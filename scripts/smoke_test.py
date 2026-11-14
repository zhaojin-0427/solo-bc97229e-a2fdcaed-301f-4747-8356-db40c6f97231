#!/usr/bin/env python3
"""End-to-end smoke test against a running seq-lake server (stdlib only).

Usage:
    python3 scripts/smoke_test.py --base http://127.0.0.1:8000 [--data-dir ./data]

Covers: happy path (both upload orders), downloads, one-upload-per-end,
concurrent duplicate, invalid FASTQ, truncated gzip, name/count mismatch,
abort of a slow in-flight peer upload, non-blocking behaviour, and input
edge cases.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import http.client
import io
import json
import os
import sys
import threading
import time
import urllib.parse

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(f"{name}: {detail}")


# ------------------------------------------------------------------ http
class Client:
    def __init__(self, base: str):
        u = urllib.parse.urlsplit(base)
        self.host, self.port = u.hostname, u.port or 80

    def req(self, method: str, path: str, body=None, headers=None, timeout=60):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            r = conn.getresponse()
            data = r.read()
            return r.status, data
        finally:
            conn.close()

    def json(self, method: str, path: str, **kw):
        status, data = self.req(method, path, **kw)
        try:
            return status, json.loads(data)
        except Exception:
            return status, data

    def upload(self, batch: str, end: str, body, timeout=60):
        headers = {"Content-Type": "application/gzip"}
        if isinstance(body, (bytes, bytearray)):
            headers["Content-Length"] = str(len(body))
        return self.req("POST", f"/batches/{batch}/ends/{end}/upload", body=body,
                        headers=headers, timeout=timeout)


# ------------------------------------------------------------------ helpers
def fastq_gz(n: int, end: int, core_names: list[str] | None = None,
             qual_delta: int = 0, seqlen: int = 50) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        for i in range(n):
            core = core_names[i] if core_names else f"READ_{i:06d}"
            seq = "ACGTN" * (seqlen // 5)
            qual = "I" * (seqlen + qual_delta)
            f.write(f"@{core}/{end} {end}:N:0:AT\n{seq}\n+\n{qual}\n".encode())
    return buf.getvalue()


class SlowBody:
    """File-like that trickles bytes: total ~ size/chunk*delay seconds."""

    def __init__(self, data: bytes, chunk: int = 8192, delay: float = 0.04):
        self.data, self.chunk, self.delay = data, chunk, delay
        self.pos = 0

    def read(self, n: int = -1) -> bytes:
        if self.pos >= len(self.data):
            return b""
        time.sleep(self.delay)
        part = self.data[self.pos:self.pos + self.chunk]
        self.pos += len(part)
        return part


def tmp_empty(data_dir: str | None) -> bool:
    if not data_dir:
        return True
    tmp = os.path.join(data_dir, "tmp")
    return not os.path.isdir(tmp) or os.listdir(tmp) == []


def batch_dir_exists(data_dir: str | None, batch: str) -> bool:
    if not data_dir:
        return False
    return os.path.isdir(os.path.join(data_dir, "batches", batch))


# ------------------------------------------------------------------ tests
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--data-dir", default=None, help="server DATA_DIR for FS checks")
    a = ap.parse_args()
    c = Client(a.base)
    dd = a.data_dir

    # 0. health
    status, _ = c.req("GET", "/healthz")
    check("healthz", status == 200, f"got {status}")

    # 1. happy path, R1 first
    r1 = fastq_gz(500, 1)
    r2 = fastq_gz(500, 2)
    s, b = c.json("POST", "/batches")
    check("create batch", s == 201 and b["status"] == "PENDING", f"{s} {b}")
    b1 = b["batch_id"]
    s, up1 = c.json("POST", f"/batches/{b1}/ends/R1/upload", body=r1)
    check("upload R1 accepted", s == 200 and up1["records"] == 500
          and up1["batch_status"] == "PENDING", f"{s} {up1}")
    check("upload R1 sha256", up1.get("sha256") == hashlib.sha256(r1).hexdigest())
    s, up2 = c.json("POST", f"/batches/{b1}/ends/R2/upload", body=r2)
    check("upload R2 completes pair", s == 200 and up2["batch_status"] == "READY"
          and up2["read_count"] == 500, f"{s} {up2}")
    s, q = c.json("GET", f"/batches/{b1}")
    check("query READY with read_count", s == 200 and q["status"] == "READY"
          and q["read_count"] == 500, f"{s} {q}")
    s, dl1 = c.req("GET", f"/batches/{b1}/ends/R1/download")
    check("download R1 intact", s == 200 and dl1 == r1)
    s, dl2 = c.req("GET", f"/batches/{b1}/ends/R2/download")
    check("download R2 intact", s == 200 and dl2 == r2)

    # 2. reverse order: R2 first
    s, b = c.json("POST", "/batches")
    b2 = b["batch_id"]
    s, _ = c.json("POST", f"/batches/{b2}/ends/R2/upload", body=r2)
    check("R2-first upload accepted", s == 200)
    s, up = c.json("POST", f"/batches/{b2}/ends/R1/upload", body=r1)
    check("R2-first pair READY", s == 200 and up["batch_status"] == "READY", f"{s} {up}")

    # 3. one upload per end, also concurrent duplicates must not overwrite
    s, _ = c.json("POST", f"/batches/{b1}/ends/R1/upload", body=r1)
    check("re-upload same end rejected", s == 409, f"got {s}")
    s, b = c.json("POST", "/batches")
    b3 = b["batch_id"]
    results: list[int] = []

    def dup():
        st, _ = c.req("POST", f"/batches/{b3}/ends/R1/upload", body=r1,
                      headers={"Content-Type": "application/gzip",
                               "Content-Length": str(len(r1))})
        results.append(st)

    t1, t2 = threading.Thread(target=dup), threading.Thread(target=dup)
    t1.start(); t2.start(); t1.join(); t2.join()
    check("concurrent same-end: exactly one accepted", sorted(results) == [200, 409],
          f"got {results}")
    s, up = c.json("POST", f"/batches/{b3}/ends/R2/upload", body=r2)
    check("batch still completes after duplicate attempt",
          s == 200 and up["batch_status"] == "READY", f"{s} {up}")
    s, dl = c.req("GET", f"/batches/{b3}/ends/R1/download")
    check("artifact not overwritten by loser", s == 200 and dl == r1)

    # 4. invalid FASTQ (quality shorter than sequence)
    bad = fastq_gz(100, 1, qual_delta=-1)
    s, b = c.json("POST", "/batches")
    b4 = b["batch_id"]
    s, e = c.json("POST", f"/batches/{b4}/ends/R1/upload", body=bad)
    check("seq/qual length mismatch -> 422", s == 422, f"{s} {e}")
    s, q = c.json("GET", f"/batches/{b4}")
    check("batch FAILED after validation error", s == 200 and q["status"] == "FAILED", f"{s} {q}")
    check("failed batch artifacts purged", not batch_dir_exists(dd, b4) and tmp_empty(dd))
    s, _ = c.json("POST", f"/batches/{b4}/ends/R2/upload", body=r2)
    check("upload to FAILED batch rejected", s == 409, f"got {s}")
    s, _ = c.req("GET", f"/batches/{b4}/ends/R1/download")
    check("no download from FAILED batch", s == 409, f"got {s}")

    # 5. truncated gzip
    s, b = c.json("POST", "/batches")
    b5 = b["batch_id"]
    s, e = c.json("POST", f"/batches/{b5}/ends/R1/upload", body=r1[:-32])
    check("truncated gzip -> 422", s == 422, f"{s} {e}")
    s, q = c.json("GET", f"/batches/{b5}")
    check("batch FAILED after truncated gzip", q.get("status") == "FAILED", f"{q}")

    # 6. read-name mismatch between ends
    names_r2 = [f"READ_{i:06d}" for i in range(500)]
    names_r2[-1] = "READ_DIFFERENT"
    s, b = c.json("POST", "/batches")
    b6 = b["batch_id"]
    s, _ = c.json("POST", f"/batches/{b6}/ends/R1/upload", body=r1)
    check("mismatch: R1 fine", s == 200)
    s, e = c.json("POST", f"/batches/{b6}/ends/R2/upload",
                  body=fastq_gz(500, 2, core_names=names_r2))
    check("name mismatch -> 422 pair mismatch", s == 422 and "mismatch" in str(e), f"{s} {e}")
    s, q = c.json("GET", f"/batches/{b6}")
    check("batch FAILED after name mismatch", q.get("status") == "FAILED", f"{q}")
    check("mismatch artifacts purged", not batch_dir_exists(dd, b6) and tmp_empty(dd))

    # 7. record-count mismatch
    s, b = c.json("POST", "/batches")
    b7 = b["batch_id"]
    c.json("POST", f"/batches/{b7}/ends/R1/upload", body=r1)
    s, e = c.json("POST", f"/batches/{b7}/ends/R2/upload", body=fastq_gz(499, 2))
    check("count mismatch -> 422", s == 422, f"{s} {e}")
    s, q = c.json("GET", f"/batches/{b7}")
    check("batch FAILED after count mismatch", q.get("status") == "FAILED", f"{q}")

    # 8. slow in-flight upload is aborted when the peer fails
    big = fastq_gz(20000, 1)  # ~2 MB gz
    slow = SlowBody(big, chunk=8192, delay=0.04)  # would take ~10 s
    s, b = c.json("POST", "/batches")
    b8 = b["batch_id"]
    r1_done: dict = {}

    def slow_r1():
        t0 = time.monotonic()
        try:
            st, data = c.upload(b8, "R1", slow, timeout=120)
            r1_done["status"] = st
        except Exception:  # server closed the connection on us: fine
            r1_done["status"] = "conn-closed"
        r1_done["elapsed"] = time.monotonic() - t0

    t = threading.Thread(target=slow_r1)
    t.start()
    time.sleep(1.0)  # let R1 get in flight
    t0 = time.monotonic()
    s, e = c.json("POST", f"/batches/{b8}/ends/R2/upload", body=bad)
    r2_elapsed = time.monotonic() - t0
    check("failing end gets 422", s == 422, f"{s} {e}")
    # The 422 is returned only after the peer exited and leftovers were
    # cleaned; a fast response therefore proves the slow peer (~10 s body)
    # was interrupted rather than waited out.
    check("peer abort did not wait for the slow body", r2_elapsed < 5,
          f"{r2_elapsed:.2f}s")
    s, q = c.json("GET", f"/batches/{b8}")
    check("batch FAILED, peer did not slip through", q.get("status") == "FAILED", f"{q}")
    check("no leftovers after abort", not batch_dir_exists(dd, b8) and tmp_empty(dd))
    t.join(timeout=30)
    check("aborted client upload unblocks", t.is_alive() is False
          and r1_done.get("elapsed", 99) < 25,
          f"elapsed={r1_done.get('elapsed')} status={r1_done.get('status')}")

    # 9. slow upload does not block other batches or queries
    s, b = c.json("POST", "/batches")
    b9 = b["batch_id"]
    slow_ok = SlowBody(fastq_gz(15000, 1), chunk=8192, delay=0.03)  # ~6 s
    bg: dict = {}

    def slow_ok_r1():
        try:
            st, data = c.upload(b9, "R1", slow_ok, timeout=120)
            bg["status"] = st
        except Exception:
            bg["status"] = "conn-closed"

    t = threading.Thread(target=slow_ok_r1)
    t.start()
    time.sleep(0.5)
    t0 = time.monotonic()
    s, b = c.json("POST", "/batches")
    b10 = b["batch_id"]
    c.json("POST", f"/batches/{b10}/ends/R1/upload", body=r1)
    s, up = c.json("POST", f"/batches/{b10}/ends/R2/upload", body=r2)
    check("other batch completes during slow upload", s == 200
          and up["batch_status"] == "READY", f"{s} {up}")
    lat = time.monotonic() - t0
    check("other batch not blocked", lat < 5, f"{lat:.2f}s")
    t0 = time.monotonic()
    s, _ = c.json("GET", "/batches")
    check("queries stay fast during slow upload", s == 200
          and time.monotonic() - t0 < 2)
    t.join(timeout=60)
    check("slow valid upload eventually accepted", bg.get("status") == 200, f"{bg}")
    s, up = c.json("POST", f"/batches/{b9}/ends/R2/upload", body=fastq_gz(15000, 2))
    check("slow batch completes", s == 200 and up["batch_status"] == "READY", f"{s} {up}")

    # 10. edge cases
    s, _ = c.json("GET", "/batches/nope")
    check("unknown batch -> 404", s == 404, f"got {s}")
    s, b = c.json("POST", "/batches")
    b11 = b["batch_id"]
    s, _ = c.json("POST", f"/batches/{b11}/ends/R3/upload", body=r1)
    check("bad end -> 422", s == 422, f"got {s}")
    s, e = c.json("POST", f"/batches/{b11}/ends/R1/upload", body=gzip.compress(b""))
    check("empty FASTQ -> 422", s == 422, f"{s} {e}")
    s, b = c.json("POST", "/batches")
    b12 = b["batch_id"]
    s, e = c.json("POST", f"/batches/{b12}/ends/R1/upload",
                  body=gzip.compress(b"@r1\nACGT\n+\n"))  # 3 lines only
    check("incomplete 4-line record -> 422", s == 422, f"{s} {e}")
    s, b = c.json("POST", "/batches")
    b13 = b["batch_id"]
    s, e = c.json("POST", f"/batches/{b13}/ends/R1/upload",
                  body=gzip.compress(b"r1\nACGT\n+\nIIII\n"))  # no '@'
    check("missing '@' header -> 422", s == 422, f"{s} {e}")
    s, b = c.json("POST", "/batches")
    b14 = b["batch_id"]
    s, e = c.json("POST", f"/batches/{b14}/ends/R1/upload",
                  body=gzip.compress(b"@r1\nACGT\n-\nIIII\n"))  # no '+'
    check("missing '+' line -> 422", s == 422, f"{s} {e}")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILURE(S):")
        for f in FAILURES:
            print(" -", f)
        return 1
    print("ALL SMOKE TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
