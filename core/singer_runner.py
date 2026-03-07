"""Singer subprocess runner — orchestrates tap-postgres | transformer | target-postgres.

Manages the full lifecycle: tmpfs config files, subprocess piping, STATE capture,
NestJS callback, and cleanup.
"""

from __future__ import annotations

import asyncio
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx

# Thread pool for sync subprocess fallback (avoids uvloop/Python 3.13 pipe issues)
_preview_executor: ThreadPoolExecutor | None = None


def _get_preview_executor() -> ThreadPoolExecutor:
    global _preview_executor
    if _preview_executor is None:
        _preview_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="preview")
    return _preview_executor

from core.config_builder import build_tap_config, build_target_config
from core.catalog_builder import build_catalog
from core.registry import get_tap_executable, get_target_executable

logger = logging.getLogger("etl.runner")

TMPFS_ROOT = Path(tempfile.gettempdir())
TMPFS_PREFIX = "mxf_"
PYTHON = sys.executable
# Max chars to capture from subprocess stderr (full tracebacks for connection errors)
STDERR_MAX_CHARS = 8000


class SingerRunResult:
    """Result of a Singer subprocess chain execution."""

    def __init__(self) -> None:
        self.status: str = "completed"
        self.rows_read: int = 0
        self.rows_upserted: int = 0
        self.rows_dropped: int = 0
        self.rows_deleted: int = 0
        self.singer_state: dict[str, Any] | None = None
        self.error: str | None = None
        self.duration_seconds: float = 0
        self.lsn_end: int | None = None


def cleanup_stale_tmpfs() -> None:
    """Remove leftover /tmp/mxf_* dirs from prior crashes (called on startup)."""
    for entry in TMPFS_ROOT.iterdir():
        if entry.is_dir() and entry.name.startswith(TMPFS_PREFIX):
            try:
                shutil.rmtree(entry)
                logger.info("Cleaned stale tmpfs: %s", entry)
            except OSError:
                pass


async def fetch_singer_state(
    nestjs_state_url: str,
    pipeline_id: str,
) -> dict[str, Any] | None:
    """GET singer state from NestJS internal endpoint."""
    url = f"{nestjs_state_url.rstrip('/')}/{pipeline_id}"
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("singer_state")
    except Exception as exc:
        logger.warning("Failed to fetch singer state for %s: %s", pipeline_id, exc)
    return None


async def post_callback(
    callback_url: str,
    payload: dict[str, Any],
) -> None:
    """POST sync results to NestJS callback endpoint."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.post(callback_url, json=payload)
            if resp.status_code >= 400:
                logger.error(
                    "Callback POST failed (HTTP %d): %s", resp.status_code, resp.text[:500]
                )
            else:
                logger.info("Callback POST success (HTTP %d)", resp.status_code)
    except Exception as exc:
        logger.error("Callback POST exception: %s", exc)


async def run_sync(
    *,
    job_id: str,
    pipeline_id: str,
    organization_id: str,
    source_connection_config: dict[str, Any],
    dest_connection_config: dict[str, Any],
    replication_method: str,
    source_stream: str,
    dest_table: str,
    dest_schema: str = "public",
    replication_slot_name: str | None = None,
    source_type: str = "postgres",
    dest_type: str = "postgres",
    column_map: dict[str, str] | None = None,
    drop_columns: list[str] | None = None,
    transform_script: str | None = None,
    output_column_sql_types: dict[str, str] | None = None,
    emit_method: str = "append",
    upsert_key: list[str] | None = None,
    hard_delete: bool = False,
    nestjs_callback_url: str,
    nestjs_state_url: str,
    discovered_catalog: dict[str, Any] | None = None,
) -> SingerRunResult:
    """Execute the full Singer tap -> transformer -> target chain.

    This is the core function called by the /sync route's background task.
    """
    result = SingerRunResult()
    start = time.monotonic()
    work_dir = TMPFS_ROOT / f"{TMPFS_PREFIX}{job_id}"
    tap_exe = get_tap_executable(source_type)
    target_exe = get_target_executable(dest_type)

    logger.info(
        "Starting sync: job=%s pipeline=%s stream=%s method=%s dest=%s.%s",
        job_id, pipeline_id, source_stream, replication_method, dest_schema, dest_table,
    )

    try:
        work_dir.mkdir(parents=True, exist_ok=True)

        tap_config = build_tap_config(
            source_connection_config,
            replication_slot_name,
            source_type=source_type,
        )
        target_config = build_target_config(
            dest_connection_config, dest_schema,
            hard_delete=hard_delete,
            source_stream=source_stream,
            dest_table=dest_table,
            emit_method=emit_method,
            upsert_key=upsert_key,
            dest_type=dest_type,
        )

        tap_config_path = work_dir / "tap-config.json"
        target_config_path = work_dir / "target-config.json"
        catalog_path = work_dir / "catalog.json"

        tap_config_path.write_text(json.dumps(tap_config))
        target_config_path.write_text(json.dumps(target_config))

        # Build or use provided catalog
        if discovered_catalog:
            catalog = build_catalog(discovered_catalog, source_stream, replication_method)
        else:
            catalog = await _run_discover(
                tap_config_path, source_stream, replication_method, source_type=source_type
            )

        selected_count = sum(
            1 for s in catalog.get("streams", [])
            for m in s.get("metadata", [])
            if m.get("breadcrumb") == [] and m.get("metadata", {}).get("selected") is True
        )
        total_streams = len(catalog.get("streams", []))
        logger.info(
            "Catalog built: source_stream='%s' selected=%d/%d streams",
            source_stream, selected_count, total_streams,
        )
        if selected_count == 0:
            logger.error(
                "No stream selected in catalog for '%s' — tap will produce 0 records",
                source_stream,
            )

        catalog_path.write_text(json.dumps(catalog))

        # Fetch singer state (LOG_BASED only)
        state_path: Path | None = None
        if replication_method == "LOG_BASED":
            singer_state = await fetch_singer_state(nestjs_state_url, pipeline_id)
            if singer_state:
                state_path = work_dir / "state.json"
                state_path.write_text(json.dumps(singer_state))

        # Build subprocess commands
        tap_cmd = [
            tap_exe,
            "--config", str(tap_config_path),
            "--catalog", str(catalog_path),
        ]
        if state_path:
            tap_cmd.extend(["--state", str(state_path)])

        transformer_env = dict(os.environ)
        if column_map:
            transformer_env["COLUMN_MAP"] = json.dumps(column_map)
        if drop_columns:
            transformer_env["DROP_COLUMNS"] = ",".join(drop_columns)
        if transform_script:
            transformer_env["TRANSFORM_SCRIPT"] = base64.b64encode(
                transform_script.encode()
            ).decode()
        if output_column_sql_types:
            transformer_env["COLUMN_SQL_TYPES"] = json.dumps(output_column_sql_types)
        if upsert_key:
            transformer_env["UPSERT_KEY"] = json.dumps(upsert_key)

        transformer_cmd = [PYTHON, "-m", "core.singer_transformer"]

        target_cmd = [
            target_exe,
            "--config", str(target_config_path),
        ]

        # Spawn all three processes with PIPE stdin/stdout.
        # uvloop doesn't support passing StreamReader as stdin, so we pipe manually.
        tap_proc = await asyncio.create_subprocess_exec(
            *tap_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        transformer_proc = await asyncio.create_subprocess_exec(
            *transformer_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=transformer_env,
        )

        target_proc = await asyncio.create_subprocess_exec(
            *target_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Bridge processes: tap.stdout → transformer.stdin → target.stdin
        tap_record_count = 0
        transformed_record_count = 0

        async def _pipe(reader, writer):
            try:
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        break
                    writer.write(chunk)
                    await writer.drain()
            except (BrokenPipeError, ConnectionResetError, RuntimeError, OSError):
                pass
            finally:
                with suppress(Exception):
                    writer.close()

        async def _counting_pipe(reader, writer, counter_name: str):
            """Pipe that counts Singer RECORD messages line-by-line."""
            nonlocal tap_record_count, transformed_record_count
            buf = b""
            try:
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        if buf:
                            writer.write(buf)
                            await writer.drain()
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        line_with_nl = line + b"\n"
                        try:
                            payload = json.loads(line)
                            if payload.get("type", "").upper() == "RECORD":
                                if counter_name == "tap":
                                    tap_record_count += 1
                                else:
                                    transformed_record_count += 1
                        except json.JSONDecodeError:
                            pass
                        writer.write(line_with_nl)
                        await writer.drain()
            except (BrokenPipeError, ConnectionResetError, RuntimeError, OSError):
                pass
            finally:
                with suppress(Exception):
                    writer.close()

        async def _read_all(reader) -> bytes:
            chunks = []
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)

        pipe1 = asyncio.create_task(
            _counting_pipe(tap_proc.stdout, transformer_proc.stdin, "tap")
        )
        pipe2 = asyncio.create_task(
            _counting_pipe(transformer_proc.stdout, target_proc.stdin, "transformer")
        )
        read_target_out = asyncio.create_task(_read_all(target_proc.stdout))
        read_target_err = asyncio.create_task(_read_all(target_proc.stderr))

        pipe_error: Exception | None = None
        try:
            await pipe1
            await pipe2
        except Exception as exc:
            pipe_error = exc
            logger.warning("Pipe error (will check process exit codes): %s", exc)

        target_stdout = await read_target_out
        target_stderr = await read_target_err

        for proc in (tap_proc, transformer_proc, target_proc):
            try:
                await asyncio.wait_for(proc.wait(), timeout=15)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

        tap_stderr = await _safe_read_stderr(tap_proc.stderr)
        transformer_stderr = await _safe_read_stderr(transformer_proc.stderr)

        if tap_stderr:
            logger.info("%s stderr:\n%s", tap_exe, tap_stderr.decode(errors="replace")[:STDERR_MAX_CHARS])
        if transformer_stderr:
            logger.info("transformer stderr:\n%s", transformer_stderr.decode(errors="replace")[:STDERR_MAX_CHARS])
        if target_stderr:
            logger.info("%s stderr:\n%s", target_exe, target_stderr.decode(errors="replace")[:STDERR_MAX_CHARS])

        result.rows_read = tap_record_count
        result.rows_upserted = transformed_record_count
        result.rows_dropped = max(tap_record_count - transformed_record_count, 0)

        if tap_proc.returncode != 0:
            err_text = (tap_stderr or b"").decode(errors="replace")[:STDERR_MAX_CHARS]
            result.status = "failed"
            result.error = f"{tap_exe} exited {tap_proc.returncode}: {err_text}"
        elif transformer_proc.returncode != 0:
            err_text = (transformer_stderr or b"").decode(errors="replace")[:STDERR_MAX_CHARS]
            result.status = "failed"
            result.error = f"singer_transformer exited {transformer_proc.returncode}: {err_text}"
        elif target_proc.returncode != 0:
            err_text = (target_stderr or b"").decode(errors="replace")[:STDERR_MAX_CHARS]
            result.status = "failed"
            result.error = f"{target_exe} exited {target_proc.returncode}: {err_text}"
        elif pipe_error:
            result.status = "failed"
            result.error = f"Pipe error: {pipe_error}"
        else:
            _parse_target_output(target_stdout, result)
            result.status = "completed"
            logger.info(
                "Sync completed: job=%s rows_read=%d rows_written=%d rows_dropped=%d rows_deleted=%d",
                job_id,
                result.rows_read,
                result.rows_upserted,
                result.rows_dropped,
                result.rows_deleted,
            )

    except asyncio.CancelledError:
        result.status = "interrupted"
        result.error = "Run was cancelled (pod shutting down)"
    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)[:STDERR_MAX_CHARS]
        logger.exception("Singer run failed for job %s", job_id)
    finally:
        result.duration_seconds = round(time.monotonic() - start, 2)

        # Post callback to NestJS
        callback_payload = {
            "job_id": job_id,
            "pipeline_id": pipeline_id,
            "organization_id": organization_id,
            "status": result.status,
            "rows_read": result.rows_read,
            "rows_upserted": result.rows_upserted,
            "rows_dropped": result.rows_dropped,
            "rows_deleted": result.rows_deleted,
            "lsn_end": result.lsn_end,
            "singer_state": result.singer_state,
            "error": result.error,
            "duration_seconds": result.duration_seconds,
            "source_tool": tap_exe,
            "dest_tool": target_exe,
            "replication_method_used": replication_method,
        }
        await post_callback(nestjs_callback_url, callback_payload)

        # Cleanup tmpfs
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except OSError:
            pass

    return result


async def run_discover(
    source_connection_config: dict[str, Any],
    source_type: str = "postgres",
) -> dict[str, Any]:
    """Run tap --discover and return the raw catalog JSON."""
    tap_exe = get_tap_executable(source_type)
    host = source_connection_config.get("host", "?")
    dbname = source_connection_config.get("dbname", source_connection_config.get("database", "?"))
    logger.info("Running %s --discover for %s/%s", tap_exe, host, dbname)

    work_dir = TMPFS_ROOT / f"{TMPFS_PREFIX}discover_{id(source_connection_config)}"
    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        tap_config = build_tap_config(source_connection_config, source_type=source_type)
        config_path = work_dir / "tap-config.json"
        config_path.write_text(json.dumps(tap_config))

        proc = await asyncio.create_subprocess_exec(
            tap_exe, "--config", str(config_path), "--discover",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            err = (stderr or b"").decode(errors="replace")[:STDERR_MAX_CHARS]
            logger.error("%s --discover failed for %s/%s: %s", tap_exe, host, dbname, err)
            raise RuntimeError(f"{tap_exe} --discover failed ({proc.returncode}): {err}")

        catalog = json.loads(stdout)
        stream_count = len(catalog.get("streams", []))
        logger.info("Discovery complete for %s/%s: %d stream(s)", host, dbname, stream_count)
        return catalog
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def run_test_connection(
    source_connection_config: dict[str, Any],
    source_type: str = "postgres",
) -> dict[str, Any]:
    """Run tap --test and return success/error."""
    tap_exe = get_tap_executable(source_type)
    host = source_connection_config.get("host", "?")
    port = source_connection_config.get("port", "?")
    dbname = source_connection_config.get("dbname", source_connection_config.get("database", "?"))
    logger.info("Running %s --test for %s:%s/%s", tap_exe, host, port, dbname)

    work_dir = TMPFS_ROOT / f"{TMPFS_PREFIX}test_{id(source_connection_config)}"
    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        tap_config = build_tap_config(source_connection_config, source_type=source_type)
        config_path = work_dir / "tap-config.json"
        config_path.write_text(json.dumps(tap_config))

        # --config must come before --test so tap-postgres CLI parses config correctly
        proc = await asyncio.create_subprocess_exec(
            tap_exe, "--config", str(config_path), "--test", "all",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode == 0:
            logger.info("Connection test PASSED for %s:%s/%s", host, port, dbname)
            return {"success": True}

        err = (stderr or b"").decode(errors="replace")[:STDERR_MAX_CHARS]
        logger.warning("Connection test FAILED for %s:%s/%s (exit %d): %s", host, port, dbname, proc.returncode, err[:500])
        return {"success": False, "error": err}
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def _run_preview_sync(
    work_dir: Path,
    config_path: Path,
    catalog_path: Path,
    tap_exe: str,
    transformer_env: dict[str, str],
    limit: int,
) -> dict[str, Any]:
    """Sync preview using subprocess.Popen (avoids uvloop/Python 3.13 pipe issues)."""
    tap_proc = subprocess.Popen(
        [tap_exe, "--config", str(config_path), "--catalog", str(catalog_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    transformer_proc = subprocess.Popen(
        [PYTHON, "-m", "core.singer_transformer"],
        stdin=tap_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=transformer_env,
    )
    tap_proc.stdout.close()

    records: list[dict[str, Any]] = []
    columns_set: set[str] = set()
    for line in iter(transformer_proc.stdout.readline, b""):
        if len(records) >= limit:
            break
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get("type", "").upper() == "RECORD":
            rec = msg.get("record", {})
            records.append(rec)
            columns_set.update(rec.keys())

    for proc in (tap_proc, transformer_proc):
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    return {
        "records": records,
        "columns": sorted(columns_set),
        "total": len(records),
    }


async def run_preview(
    source_connection_config: dict[str, Any],
    source_stream: str,
    limit: int = 50,
    column_map: dict[str, str] | None = None,
    drop_columns: list[str] | None = None,
    transform_script: str | None = None,
    source_type: str = "postgres",
) -> dict[str, Any]:
    """Run tap in FULL_TABLE mode piped through transformer, collect N records."""
    logger.info("Running preview: stream=%s limit=%d source_type=%s", source_stream, limit, source_type)
    work_dir = TMPFS_ROOT / f"{TMPFS_PREFIX}preview_{id(source_connection_config)}"
    try:
        work_dir.mkdir(parents=True, exist_ok=True)

        tap_config = build_tap_config(source_connection_config, source_type=source_type)
        config_path = work_dir / "tap-config.json"
        config_path.write_text(json.dumps(tap_config))

        # Discover then build catalog
        catalog = await _run_discover(
            config_path, source_stream, "FULL_TABLE", source_type=source_type
        )
        catalog_path = work_dir / "catalog.json"
        catalog_path.write_text(json.dumps(catalog))

        tap_exe = get_tap_executable(source_type)

        transformer_env = dict(os.environ)
        if column_map:
            transformer_env["COLUMN_MAP"] = json.dumps(column_map)
        if drop_columns:
            transformer_env["DROP_COLUMNS"] = ",".join(drop_columns)
        if transform_script:
            transformer_env["TRANSFORM_SCRIPT"] = base64.b64encode(
                transform_script.encode()
            ).decode()

        try:
            tap_proc = await asyncio.create_subprocess_exec(
                tap_exe, "--config", str(config_path), "--catalog", str(catalog_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            transformer_proc = await asyncio.create_subprocess_exec(
                PYTHON, "-m", "core.singer_transformer",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=transformer_env,
            )
        except (AttributeError, OSError) as exc:
            # uvloop/Python 3.13: StreamReader has no fileno — fall back to sync subprocess
            if "fileno" in str(exc) or isinstance(exc, AttributeError):
                logger.info("Using sync preview fallback (uvloop/Python 3.13 compatibility)")
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    _get_preview_executor(),
                    _run_preview_sync,
                    work_dir,
                    config_path,
                    catalog_path,
                    tap_exe,
                    transformer_env,
                    limit,
                )
            raise

        # Bridge tap.stdout → transformer.stdin (uvloop doesn't support direct pipe)
        async def _feed_transformer():
            try:
                assert tap_proc.stdout is not None
                assert transformer_proc.stdin is not None
                while True:
                    chunk = await tap_proc.stdout.read(65536)
                    if not chunk:
                        break
                    transformer_proc.stdin.write(chunk)
                    await transformer_proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError, RuntimeError, OSError):
                pass
            finally:
                if transformer_proc.stdin:
                    transformer_proc.stdin.close()

        feed_task = asyncio.create_task(_feed_transformer())

        records: list[dict[str, Any]] = []
        columns: set[str] = set()

        assert transformer_proc.stdout is not None
        while len(records) < limit:
            line = await transformer_proc.stdout.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("type", "").upper() == "RECORD":
                rec = msg.get("record", {})
                records.append(rec)
                columns.update(rec.keys())

        # Kill processes once we have enough records
        for proc in (tap_proc, transformer_proc):
            try:
                proc.kill()
            except ProcessLookupError:
                pass

        await tap_proc.wait()
        await transformer_proc.wait()
        feed_task.cancel()
        with suppress(asyncio.CancelledError):
            await feed_task

        logger.info(
            "Preview complete: stream=%s rows=%d columns=%d",
            source_stream, len(records), len(columns),
        )

        return {
            "records": records,
            "columns": sorted(columns),
            "total": len(records),
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _safe_read_stderr(stream) -> bytes:
    """Read remaining stderr from a process, returning empty bytes on failure."""
    if stream is None:
        return b""
    try:
        return await stream.read()
    except Exception:
        return b""


async def _run_discover(
    tap_config_path: Path,
    source_stream: str,
    replication_method: str,
    source_type: str = "postgres",
) -> dict[str, Any]:
    """Run --discover then build a catalog with selected stream."""
    tap_exe = get_tap_executable(source_type)
    proc = await asyncio.create_subprocess_exec(
        tap_exe, "--config", str(tap_config_path), "--discover",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = (stderr or b"").decode(errors="replace")[:STDERR_MAX_CHARS]
        raise RuntimeError(f"{tap_exe} --discover failed: {err}")

    discovered = json.loads(stdout)
    return build_catalog(discovered, source_stream, replication_method)


def _parse_target_output(stdout: bytes, result: SingerRunResult) -> None:
    """Parse target-postgres stdout for the final STATE message and row counts."""
    last_state: dict[str, Any] | None = None
    for line in stdout.decode(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg_type = msg.get("type", "").upper()
        if msg_type == "STATE":
            last_state = msg.get("value")

    if last_state:
        result.singer_state = last_state
        bookmarks = last_state.get("bookmarks", {})
        for _stream_id, bookmark in bookmarks.items():
            lsn = bookmark.get("lsn")
            if lsn is not None:
                result.lsn_end = int(lsn)
                break


# ---------------------------------------------------------------------------
# Graceful shutdown support
# ---------------------------------------------------------------------------

_active_tasks: set[asyncio.Task[Any]] = set()
_shutting_down = False


def register_task(task: asyncio.Task[Any]) -> None:
    _active_tasks.add(task)
    task.add_done_callback(_active_tasks.discard)


def is_shutting_down() -> bool:
    return _shutting_down


async def drain_running_pipelines(timeout: float = 300) -> None:
    """Wait for all active sync tasks to finish (called on SIGTERM)."""
    global _shutting_down
    _shutting_down = True
    if not _active_tasks:
        return
    logger.info("Draining %d active pipeline(s)...", len(_active_tasks))
    done, pending = await asyncio.wait(_active_tasks, timeout=timeout)
    if pending:
        logger.warning("Cancelling %d pipeline(s) after timeout", len(pending))
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
