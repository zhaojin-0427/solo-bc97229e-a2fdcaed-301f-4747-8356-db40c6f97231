#!/usr/bin/env python3
"""Generate sample paired-end gzipped FASTQ files for demos and tests."""
from __future__ import annotations

import argparse
import gzip
import os
import random


def make_pair(
    outdir: str,
    nreads: int,
    prefix: str = "INST:1:FC:1:1101",
    mismatch_names: bool = False,
    r2_fewer: int = 0,
    bad_qual: bool = False,
    truncate_r2: bool = False,
    suffix: str = "",
) -> tuple[str, str]:
    rng = random.Random(7)

    def rec(i: int, end: int) -> str:
        core = f"{prefix}:{1000 + i}:{2000 + i}"
        if mismatch_names and end == 2 and i == nreads - 1:
            core = f"{prefix}:9999:9999"
        seq = "".join(rng.choice("ACGTN") for _ in range(150))
        qlen = 150 if not (bad_qual and i == 0) else 149
        qual = "".join(chr(rng.randint(33, 73)) for _ in range(qlen))
        return f"@{core}/{end} {end}:N:0:ATCACG\n{seq}\n+\n{qual}\n"

    r1 = os.path.join(outdir, f"sample{suffix}_R1.fastq.gz")
    r2 = os.path.join(outdir, f"sample{suffix}_R2.fastq.gz")
    with gzip.open(r1, "wt") as f:
        for i in range(nreads):
            f.write(rec(i, 1))
    with gzip.open(r2, "wt") as f:
        for i in range(nreads - r2_fewer):
            f.write(rec(i, 2))
    if truncate_r2:
        with open(r2, "rb") as f:
            data = f.read()
        with open(r2, "wb") as f:
            f.write(data[:-16])  # cut the gzip footer -> truncated stream
    return r1, r2


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("outdir")
    p.add_argument("--reads", type=int, default=1000)
    p.add_argument("--mismatch-names", action="store_true", help="R2's last read name differs")
    p.add_argument("--r2-fewer", type=int, default=0, metavar="N", help="R2 has N fewer records")
    p.add_argument("--bad-qual", action="store_true", help="first R1 record has qual shorter than seq")
    p.add_argument("--truncate-r2", action="store_true", help="cut R2's gzip footer")
    p.add_argument("--suffix", default="")
    a = p.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    r1, r2 = make_pair(
        a.outdir, a.reads,
        mismatch_names=a.mismatch_names, r2_fewer=a.r2_fewer,
        bad_qual=a.bad_qual, truncate_r2=a.truncate_r2, suffix=a.suffix,
    )
    print(r1)
    print(r2)


if __name__ == "__main__":
    main()
