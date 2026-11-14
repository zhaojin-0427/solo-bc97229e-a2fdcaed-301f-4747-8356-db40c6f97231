"""Streaming gzip + 4-line FASTQ validator with O(1) memory.

feed()/finish() are blocking and are meant to be called in a worker thread
(via ``asyncio.to_thread``). Memory use is bounded by the upload chunk size
and ``max_line_bytes`` — never by file size:

* decompressed text is consumed line by line from a small carry buffer;
* read names are folded into an order-sensitive SHA-256 chain
  (``H = SHA256(len(name) || name || ...)``). Comparing the final digest and
  the record count between R1 and R2 proves the two name sequences are
  pairwise equal (up to cryptographic-hash collision), without ever storing
  the names.
"""
from __future__ import annotations

import hashlib
import re
import zlib


class ValidationError(Exception):
    """The uploaded stream violates the gzip / FASTQ contract."""


_WS = re.compile(rb"\s")


def read_name(header: bytes) -> bytes:
    """Text after '@' up to the first whitespace, trailing /1 or /2 stripped."""
    rest = header[1:]
    m = _WS.search(rest)
    token = rest[: m.start()] if m else rest
    if token.endswith(b"/1") or token.endswith(b"/2"):
        token = token[:-2]
    return token


class GzipFastqValidator:
    """Incrementally validates one gzip-compressed 4-line FASTQ stream."""

    def __init__(self, dest_path: str, max_line_bytes: int):
        self._f = open(dest_path, "wb")
        self._closed = False
        self._sha = hashlib.sha256()
        self._gz = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._max_line = max_line_bytes
        self._buf = b""
        self._state = 0  # 0=@header 1=seq 2='+' 3=qual
        self._seq_len = 0
        self.records = 0
        self.size_bytes = 0
        self._names = hashlib.sha256()
        self.first_name: bytes | None = None
        self.last_name: bytes | None = None

    # ------------------------------------------------------------------ API
    def feed(self, chunk: bytes) -> None:
        """Consume one chunk of the raw (still compressed) request body."""
        self._f.write(chunk)
        self._sha.update(chunk)
        self.size_bytes += len(chunk)
        try:
            out = self._gz.decompress(chunk)
        except zlib.error as exc:
            raise ValidationError(f"corrupt gzip stream: {exc}") from exc
        if out:
            self._feed_text(out)

    def finish(self) -> None:
        """Called once the request body is fully received."""
        try:
            tail = self._gz.flush()
        except zlib.error as exc:
            raise ValidationError(f"corrupt gzip stream: {exc}") from exc
        if tail:
            self._feed_text(tail)
        if not self._gz.eof:
            raise ValidationError("truncated gzip stream: end-of-stream footer missing")
        if self._gz.unused_data:
            raise ValidationError("trailing garbage after the gzip stream")
        self.close()
        if self._buf:  # tolerate a final line without trailing newline
            if len(self._buf) > self._max_line:
                raise ValidationError(f"line exceeds {self._max_line} bytes")
            self._line(self._buf[:-1] if self._buf.endswith(b"\r") else self._buf)
            self._buf = b""
        if self._state != 0:
            raise ValidationError("truncated FASTQ: incomplete 4-line record at end of file")
        if self.records == 0:
            raise ValidationError("no FASTQ records found")

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._f.close()
            except Exception:
                pass

    @property
    def sha256_hex(self) -> str:
        return self._sha.hexdigest()

    @property
    def names_hash_hex(self) -> str:
        return self._names.hexdigest()

    # -------------------------------------------------------------- internals
    def _feed_text(self, data: bytes) -> None:
        parts = (self._buf + data).split(b"\n")
        self._buf = parts.pop()
        if len(self._buf) > self._max_line:
            raise ValidationError(f"line exceeds {self._max_line} bytes")
        for line in parts:
            if line.endswith(b"\r"):
                line = line[:-1]
            self._line(line)

    def _line(self, line: bytes) -> None:
        state = self._state
        if state == 0:
            if not line.startswith(b"@"):
                raise ValidationError(
                    f"record {self.records + 1}: header line must start with '@'"
                )
            name = read_name(line)
            if not name:
                raise ValidationError(f"record {self.records + 1}: empty read name")
            self._names.update(len(name).to_bytes(4, "big"))
            self._names.update(name)
            if self.first_name is None:
                self.first_name = name
            self.last_name = name
            self._state = 1
        elif state == 1:
            self._seq_len = len(line)
            self._state = 2
        elif state == 2:
            if not line.startswith(b"+"):
                raise ValidationError(
                    f"record {self.records + 1}: third line must start with '+'"
                )
            self._state = 3
        else:
            if len(line) != self._seq_len:
                raise ValidationError(
                    f"record {self.records + 1}: sequence length {self._seq_len}"
                    f" != quality length {len(line)}"
                )
            self.records += 1
            self._state = 0
