"""Seq Lake — paired-end FASTQ ingestion API.

Workflow
--------
1. ``POST /batches`` creates a batch.
2. The two ends are streamed independently, in any order:
   ``POST /batches/{id}/ends/R1/upload`` and ``.../R2/upload``
   (raw ``application/gzip`` body). Each end is accepted exactly once.
3. Each end is validated while streaming: complete gzip stream, strict
   4-line FASTQ records ('@' header, '+' separator, sequence/quality of
   equal length). Read names (token after '@' up to the first whitespace,
   trailing /1 or /2 stripped) are folded into an order-sensitive hash chain.
4. When both ends are fully persisted and validated, the record counts and
   name chains are compared; on success the batch becomes READY and both
   originals open for download together. On any failure the peer upload is
   aborted immediately (without waiting for a slow body), both sides exit,
   leftover artifacts are removed, and only then is the batch reported
   FAILED.

Run with a single uvicorn worker (in-memory pair coordination).
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel
from starlette.requests import ClientDisconnect

from . import config, db, runtime, storage
from .fastq import GzipFastqValidator, ValidationError

log = logging.getLogger("seq-lake")

ENDS = ("R1", "R2")


class PairedAbort(Exception):
    """The paired end failed; stop receiving immediately without waiting for
    the rest of the (possibly slow) request body."""


# ------------------------------------------------------------------ models
class EndView(BaseModel):
    status: str
    size_bytes: int | None = None
    sha256: str | None = None
    records: int | None = None
    error: str | None = None


class BatchView(BaseModel):
    batch_id: str
    status: str
    read_count: int | None = None
    error: str | None = None
    created_at: str
    updated_at: str
    ends: dict[str, EndView | None]


class UploadResponse(BaseModel):
    batch_id: str
    end: str
    end_status: str
    batch_status: str
    records: int
    size_bytes: int
    sha256: str
    read_count: int | None = None


# ------------------------------------------------------------------ helpers
async def _next_chunk(stream, fail_event: asyncio.Event) -> bytes | None:
    """Next request-body chunk, None at EOF.

    Races the body read against the batch failure event: as soon as the
    paired end fails, raises PairedAbort instead of waiting for more of a
    slow body to arrive.
    """
    if fail_event.is_set():
        raise PairedAbort()
    get = asyncio.ensure_future(stream.__anext__())
    fail = asyncio.ensure_future(fail_event.wait())
    try:
        done, _ = await asyncio.wait({get, fail}, return_when=asyncio.FIRST_COMPLETED)
        if fail in done:
            raise PairedAbort()
        try:
            return get.result()
        except StopAsyncIteration:
            return None
    finally:
        for t in (get, fail):
            if not t.done():
                t.cancel()
            elif not t.cancelled():
                t.exception()  # retrieve so nothing is logged as "never retrieved"


def _maybe_pop_runtime(batch_id: str, rt: runtime.BatchRuntime) -> None:
    if rt.inflight:
        return
    b = db.get_batch(batch_id)
    if b is not None and b["status"] in db.TERMINAL:
        runtime.pop_if(batch_id, rt)


# ------------------------------------------------------------------ lifespan
@asynccontextmanager
async def lifespan(_: FastAPI):
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    storage.ensure_dirs()
    db.init()
    # Restart recovery: fail every batch that never completed, remove its
    # artifacts; successful (READY) batches and their originals are kept.
    stale = db.fail_incomplete_on_startup()
    for batch_id in stale:
        storage.purge_batch(batch_id)
    storage.sweep_tmp()
    ready = db.batch_ids_by_status("READY")
    for d in storage.batch_dirs_on_disk():
        if os.path.basename(d) not in ready:
            shutil.rmtree(d, ignore_errors=True)
    if stale:
        log.info(
            "startup recovery: marked %d incomplete batch(es) FAILED and removed artifacts",
            len(stale),
        )
    log.info("seq-lake ready (data dir: %s)", config.DATA_DIR)
    yield
    db.close()


app = FastAPI(
    title="Seq Lake — paired-end FASTQ ingestion",
    version="1.0.0",
    lifespan=lifespan,
)


# ------------------------------------------------------------------ endpoints
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"status": "ok"}


@app.post(
    "/batches",
    status_code=201,
    response_model=BatchView,
    summary="Create a batch",
    description="Create an ingestion batch, then upload ends R1 and R2 in any order.",
)
def create_batch() -> dict:
    return db.create_batch()


@app.get(
    "/batches",
    response_model=list[BatchView],
    summary="List batches",
)
def list_batches(status: str | None = None) -> list[dict]:
    if status is not None and status not in ("PENDING", "READY", "FAILED"):
        raise HTTPException(422, "status must be PENDING, READY or FAILED")
    return db.list_batches(status)


@app.get(
    "/batches/{batch_id}",
    response_model=BatchView,
    summary="Batch status",
    description="Batch status, per-end progress and, once READY, the read count.",
)
def get_batch(batch_id: str) -> dict:
    b = db.get_batch(batch_id)
    if b is None:
        raise HTTPException(404, f"unknown batch: {batch_id}")
    return b


@app.post(
    "/batches/{batch_id}/ends/{end}/upload",
    response_model=UploadResponse,
    summary="Stream one end (gzip FASTQ)",
    description=(
        "Stream one end of the pair as a raw `application/gzip` body. "
        "Each end of a batch is accepted exactly once; order between ends is free. "
        "If either end fails, the in-flight peer upload is aborted immediately."
    ),
)
async def upload_end(batch_id: str, end: str, request: Request) -> UploadResponse:
    end = end.upper()
    if end not in ENDS:
        raise HTTPException(422, "end must be R1 or R2")
    rt = runtime.get_or_create(batch_id)
    started = db.try_start_file(batch_id, end)
    if started == "no_batch":
        if not rt.inflight:
            runtime.pop_if(batch_id, rt)
        raise HTTPException(404, f"unknown batch: {batch_id}")
    if started == "not_pending":
        if not rt.inflight:
            runtime.pop_if(batch_id, rt)
        raise HTTPException(409, "batch is already finished; create a new batch to upload again")
    if started == "duplicate":
        raise HTTPException(
            409, f"end {end} of this batch was already accepted; only one upload per end"
        )

    tmp = storage.tmp_path(batch_id, end)
    proc: GzipFastqValidator | None = None
    resp: UploadResponse | None = None
    err: str | None = None
    err_status = 500
    initiator = False

    rt.enter(end)
    try:
        proc = GzipFastqValidator(tmp, config.MAX_LINE_BYTES)
        stream = request.stream()
        while True:
            chunk = await _next_chunk(stream, rt.fail_event)
            if chunk is None:
                break
            if chunk:
                # file write + gunzip + validation happen off the event loop
                await asyncio.to_thread(proc.feed, chunk)
        await asyncio.to_thread(proc.finish)
        if rt.fail_event.is_set():
            raise PairedAbort()  # late finisher is not let through

        final = storage.final_path(batch_id, end)
        meta = {
            "path": final,
            "size_bytes": proc.size_bytes,
            "sha256": proc.sha256_hex,
            "records": proc.records,
            "names_hash": proc.names_hash_hex,
        }
        await asyncio.to_thread(storage.commit_temp, tmp, final)
        outcome, info = await asyncio.to_thread(db.finalize_file_success, batch_id, end, meta)
        if outcome == "batch_not_pending":
            storage.discard(final)
            raise PairedAbort()
        if outcome == "conflict":
            storage.discard(final)
            err, err_status = "upload state conflict for this end", 409
        elif outcome == "mismatch":
            rt.fail(info)
            storage.purge_batch(batch_id)
            err, err_status = info, 422
        else:
            resp = UploadResponse(
                batch_id=batch_id,
                end=end,
                end_status="VALIDATED",
                batch_status="READY" if outcome == "ready" else "PENDING",
                records=proc.records,
                size_bytes=proc.size_bytes,
                sha256=proc.sha256_hex,
                read_count=proc.records if outcome == "ready" else None,
            )
    except PairedAbort:
        err = f"aborted: {rt.failure_reason or 'the paired end failed'}"
        err_status = 409
    except ValidationError as exc:
        err, err_status, initiator = str(exc), 422, True
    except ClientDisconnect:
        err = "client disconnected before the request body was fully received"
        err_status, initiator = 400, True
    except FileExistsError:
        err, err_status = "artifact already exists for this end", 409
    except Exception:
        log.exception("batch %s end %s: unexpected upload failure", batch_id, end)
        err, err_status, initiator = "internal error while receiving the upload", 500, True
    finally:
        rt.exit(end)
        if proc is not None:
            proc.close()
        storage.discard(tmp)

    if err is None:
        assert resp is not None
        _maybe_pop_runtime(batch_id, rt)
        return resp

    if initiator:
        # Fail the pair: signal the peer, wait until both sides have exited,
        # then remove leftovers and mark the batch FAILED. The second purge
        # closes the race with a peer whose artifact commit landed in between;
        # a peer finalizing after finalize_failure sees the batch non-PENDING
        # and discards its own artifact. Only after all of this is the
        # failure reported.
        rt.fail(err)
        db.mark_file_failed(batch_id, end, err)
        try:
            await asyncio.wait_for(rt.all_clear.wait(), timeout=config.PEER_DRAIN_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning(
                "batch %s: peer upload did not exit within %.0fs",
                batch_id, config.PEER_DRAIN_TIMEOUT,
            )
        storage.purge_batch(batch_id)
        db.finalize_failure(batch_id, err)
        storage.purge_batch(batch_id)
    else:
        db.mark_file_failed(batch_id, end, err)
    _maybe_pop_runtime(batch_id, rt)
    raise HTTPException(err_status, detail=err)


@app.get(
    "/batches/{batch_id}/ends/{end}/download",
    summary="Download an original",
    description=(
        "Download the original gzip of one end. Both ends open for download "
        "together, only after both are fully persisted and validated (batch READY)."
    ),
    responses={200: {"content": {"application/gzip": {}}}},
)
def download_end(batch_id: str, end: str):
    end = end.upper()
    if end not in ENDS:
        raise HTTPException(422, "end must be R1 or R2")
    b = db.get_batch(batch_id)
    if b is None:
        raise HTTPException(404, f"unknown batch: {batch_id}")
    if b["status"] != "READY":
        raise HTTPException(
            409, "downloads open only after both ends are persisted and validated"
        )
    info = b["ends"][end]
    if not info or not info.get("path") or not os.path.exists(info["path"]):
        raise HTTPException(410, "artifact is not available")
    return FileResponse(
        info["path"], media_type="application/gzip", filename=f"{batch_id}_{end}.fastq.gz"
    )


# ------------------------------------------------------------------ OpenAPI
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title, version=app.version, description=app.description, routes=app.routes
    )
    # The upload endpoint reads a raw streaming body; describe it explicitly
    # so Swagger UI renders a file picker.
    op = schema["paths"]["/batches/{batch_id}/ends/{end}/upload"]["post"]
    op["requestBody"] = {
        "required": True,
        "content": {
            "application/gzip": {"schema": {"type": "string", "format": "binary"}},
            "application/octet-stream": {"schema": {"type": "string", "format": "binary"}},
        },
    }
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi
