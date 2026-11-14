"""Filesystem layout and artifact handling.

In-flight uploads live in ``TMP_DIR`` as ``*.part`` files. A validated end is
published into ``batches/<batch_id>/`` with a hard-link + unlink, which is
atomic and can never overwrite an existing artifact.
"""
from __future__ import annotations

import glob
import os
import shutil
import uuid

from . import config


def ensure_dirs() -> None:
    os.makedirs(config.TMP_DIR, exist_ok=True)
    os.makedirs(config.BATCHES_DIR, exist_ok=True)


def tmp_path(batch_id: str, end: str) -> str:
    return os.path.join(config.TMP_DIR, f"{batch_id}_{end}_{uuid.uuid4().hex}.part")


def batch_dir(batch_id: str) -> str:
    return os.path.join(config.BATCHES_DIR, batch_id)


def final_path(batch_id: str, end: str) -> str:
    return os.path.join(batch_dir(batch_id), f"{end}.fastq.gz")


def commit_temp(tmp: str, final: str) -> None:
    """Publish tmp as final atomically; raises FileExistsError instead of
    ever overwriting an existing artifact."""
    os.makedirs(os.path.dirname(final), exist_ok=True)
    os.link(tmp, final)
    os.unlink(tmp)


def discard(path: str | None) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except (FileNotFoundError, IsADirectoryError):
        pass


def purge_batch(batch_id: str) -> None:
    """Remove every artifact of a batch (final dir + in-flight temp files)."""
    shutil.rmtree(batch_dir(batch_id), ignore_errors=True)
    for p in glob.glob(os.path.join(config.TMP_DIR, f"{batch_id}_*")):
        discard(p)


def sweep_tmp() -> None:
    for p in glob.glob(os.path.join(config.TMP_DIR, "*")):
        discard(p)


def batch_dirs_on_disk() -> list[str]:
    return [d for d in glob.glob(os.path.join(config.BATCHES_DIR, "*")) if os.path.isdir(d)]
