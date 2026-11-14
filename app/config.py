"""Runtime configuration (environment driven)."""
from __future__ import annotations

import os

DATA_DIR = os.environ.get("DATA_DIR", os.path.abspath("./data"))
DB_PATH = os.path.join(DATA_DIR, "lake.db")
TMP_DIR = os.path.join(DATA_DIR, "tmp")
BATCHES_DIR = os.path.join(DATA_DIR, "batches")

# Hard cap for a single FASTQ line; keeps parser memory bounded regardless of
# file size. 16 MiB is far beyond any real sequencer read length.
MAX_LINE_BYTES = int(os.environ.get("MAX_LINE_BYTES", str(16 * 1024 * 1024)))

# How long a failing end waits for the paired in-flight upload to abort and
# clean up before the batch is reported FAILED.
PEER_DRAIN_TIMEOUT = float(os.environ.get("PEER_DRAIN_TIMEOUT", "60"))
