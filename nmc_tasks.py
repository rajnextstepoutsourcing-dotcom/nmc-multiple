"""
nmc_tasks.py — NMC job task executed by RQ worker
This function is called by the Redis Queue worker.
It processes all PIN rows for a job sequentially (1 at a time).
"""

import asyncio
import logging
import os
import shutil
import time
import uuid
import zipfile
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from functools import partial

import anyio

log = logging.getLogger(__name__)

STORAGE_ROOT = Path("/tmp/nextstep")

# Hard failures — do not retry these, fail immediately
HARD_FAILURE_PATTERNS = [
    "site unavailable",
    "service unavailable",
    "maintenance",
    "503",
    "502",
    "connection refused",
    "name or service not known",
    "network is unreachable",
]

def _is_hard_failure(error_str: str) -> bool:
    """Returns True if this error should NOT be retried."""
    el = (error_str or "").lower()
    return any(p in el for p in HARD_FAILURE_PATTERNS)

def _safe_name(name: str, default: str) -> str:
    name = re.sub(r"\s+", " ", (name or "").strip()) or default
    return re.sub(r'[\\/:"*?<>|]+', "-", name).strip()

def _uk_now_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.now(tz=ZoneInfo("Europe/London"))
    except Exception:
        dt = datetime.utcnow()
    return dt.strftime("%d.%m.%Y")


def process_nmc_job(job_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main task function executed by RQ worker.
    Processes all PIN rows for a single NMC job sequentially.

    job_data keys:
        job_id      — internal NMC job UUID
        db_job_id   — central DB job ID (may be None)
        tenant_id   — tenant ID
        user_id     — user ID
        items       — list of {nmc_pin, original_filename}
        storage_path — /tmp/nextstep/{tenant_id}/{user_id}/{job_id}/
    """
    from nmc_runner import run_nmc_check_sync
    from pdf_utils import make_simple_error_pdf
    import db

    job_id = job_data["job_id"]
    db_job_id = job_data.get("db_job_id")
    tenant_id = job_data.get("tenant_id", 0)
    user_id = job_data.get("user_id", 0)
    items = job_data.get("items", [])
    storage_path = Path(job_data["storage_path"])
    storage_path.mkdir(parents=True, exist_ok=True)

    log.info("[Task] Job %s started — %d items tenant=%s user=%s",
             job_id, len(items), tenant_id, user_id)

    # Update Redis job state
    _update_redis_state(job_id, {
        "state": "running",
        "message": "Processing checks...",
    })

    checked_date = _uk_now_str()
    pdf_names: List[str] = []
    rows = []
    successful = 0
    failed = 0

    # Initialise row tracking
    for i, it in enumerate(items[:100]):
        rows.append({
            "row": i + 1,
            "status": "queued",
            "nmc_pin": (it.get("nmc_pin") or "").strip().upper(),
            "original_filename": it.get("original_filename") or f"Row {i+1}",
            "name": "",
            "expiry_date": "",
            "status_text": "",
            "pdf_filename": "",
            "pdf_url": "",
            "error": "",
        })

    _update_redis_rows(job_id, rows)

    # ── Rerun mode: only process dirty rows, merge with old rows ────────────
    is_rerun = job_data.get("is_rerun", False)
    dirty_row_nums = set(job_data.get("dirty_row_nums", []))
    old_rows = job_data.get("old_rows", [])

    if is_rerun and old_rows:
        # Start with old rows — only dirty ones get re-processed
        rows = []
        for r in old_rows:
            rnum = r.get("row", 0)
            if rnum in dirty_row_nums:
                # Find new PIN from items
                new_pin = next((it.get("nmc_pin","") for it in items if it.get("row_number")==rnum), r.get("nmc_pin",""))
                rows.append({**r, "status": "queued", "nmc_pin": new_pin.strip().upper(),
                              "pdf_filename":"","pdf_url":"","error":"","name":"","expiry_date":"","status_text":""})
            else:
                rows.append(r)
        # Replace items with only dirty ones for processing
        items = [it for it in items if it.get("row_number") in dirty_row_nums]
        _update_redis_rows(job_id, rows)

    # Process one PIN at a time
    for i, it in enumerate(items[:100]):
        pin = (it.get("nmc_pin") or "").strip().upper()
        row = rows[i]

        if not pin:
            row["status"] = "failed"
            row["error"] = "No NMC PIN found."
            failed += 1
            _update_redis_rows(job_id, rows)
            continue

        row["status"] = "running"
        _update_redis_rows(job_id, rows)
        log.info("[Task] Job %s — row %d PIN=%s", job_id, i + 1, pin)

        # Item storage path
        item_dir = storage_path / f"item_{i+1:03d}"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Run check (runner has its own MAX_RETRIES=3 for transient errors)
        # But we skip retry on hard failures
        try:
            result = run_nmc_check_sync(pin, str(item_dir))
        except Exception as e:
            result = {"ok": False, "error": str(e), "pdf_path": ""}

        if result.get("ok"):
            name = result.get("name") or pin
            expiry_date = result.get("expiry_date") or ""
            status_text = result.get("status_text") or ""
            final_name = _safe_name(
                f"{name} - {pin} - {checked_date}.pdf",
                f"NMC-{i+1}.pdf"
            )
            pdf_src = result.get("pdf_path") or ""
            final_path = storage_path / final_name

            if pdf_src and Path(pdf_src).exists():
                try:
                    if final_path.exists():
                        final_path.unlink()
                    import shutil as _sh
                    _sh.move(pdf_src, str(final_path))
                except Exception:
                    final_path = Path(pdf_src)
                    final_name = final_path.name

                pdf_names.append(final_name)
                row.update({
                    "status": "done",
                    "pdf_filename": final_name,
                    "pdf_url": f"/nmc/download/{job_id}/{final_name}",
                    "name": name,
                    "expiry_date": expiry_date,
                    "status_text": status_text,
                })
                successful += 1
                log.info("[Task] Row %d done: %s", i + 1, name)
            else:
                row["status"] = "failed"
                row["error"] = result.get("error") or "PDF not found after run."
                failed += 1
        else:
            error_msg = result.get("error") or "Check failed."
            row["status"] = "failed"
            row["error"] = error_msg
            failed += 1
            log.warning("[Task] Row %d failed: %s", i + 1, error_msg)

            # Include error snapshot PDF if available
            pdf_src = result.get("pdf_path") or ""
            if pdf_src and Path(pdf_src).exists():
                snap_name = _safe_name(
                    f"ERROR-{pin}-{checked_date}.pdf",
                    f"error-{i+1}.pdf"
                )
                snap_path = storage_path / snap_name
                try:
                    import shutil as _sh
                    _sh.move(pdf_src, str(snap_path))
                    row["pdf_filename"] = snap_name
                    row["pdf_url"] = f"/nmc/download/{job_id}/{snap_name}"
                    pdf_names.append(snap_name)
                except Exception:
                    pass

        _update_redis_rows(job_id, rows)

    # Build ZIP — for rerun, include ALL PDFs in folder (old + new results)
    if is_rerun:
        zip_name, zip_ready = _build_zip_from_folder(storage_path, checked_date)
        if zip_ready: log.info("[Task] ZIP rebuilt from folder: %s", zip_name)
    else:
        zip_name = ""
        zip_ready = False
        if len(pdf_names) >= 2:
            zip_name = _safe_name(f"NMC_Checks_{checked_date}.zip", "NMC_Checks.zip")
            zip_path = storage_path / zip_name
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for pdfn in pdf_names:
                    fp = storage_path / pdfn
                    if fp.exists():
                        zf.write(fp, arcname=pdfn)
            zip_ready = True
            log.info("[Task] ZIP created: %s", zip_name)

    # For rerun: merge processed rows back into full rows list
    if is_rerun and old_rows:
        final_rows = []
        rerun_rows_by_num = {r.get("row",0): r for r in rows if r.get("row",0) in dirty_row_nums}
        for r in old_rows:
            rnum = r.get("row",0)
            if rnum in rerun_rows_by_num:
                final_rows.append(rerun_rows_by_num[rnum])
            else:
                final_rows.append(r)
        rows = final_rows

    # Final state
    final_state = {
        "state": "done",
        "rows": rows,
        "zip_ready": zip_ready,
        "zip_name": zip_name,
        "zip_url": f"/nmc/download/{job_id}/{zip_name}" if zip_ready else "",
        "message": "" if (successful > 0) else "No PDFs generated. Check errors above.",
        "successful": successful,
        "failed": failed,
    }
    _update_redis_state(job_id, final_state)
    log.info("[Task] Job %s complete — %d success, %d failed", job_id, successful, failed)

    # Update central database
    if db_job_id:
        db.update_job_status(
            db_job_id=db_job_id,
            status="completed" if successful > 0 else "failed",
            successful_items=successful,
            failed_items=failed,
        )

    # Record token usage
    if successful > 0 and tenant_id:
        db.record_usage(
            tenant_id=tenant_id,
            user_id=user_id,
            db_job_id=db_job_id,
            successful_outputs=successful,
        )

    # Schedule file cleanup after 10 minutes
    _schedule_cleanup(storage_path, delay_seconds=600)

    return {"ok": True, "successful": successful, "failed": failed}


# ── Redis state helpers ───────────────────────────────────────────────────────

def _get_redis():
    import redis as redis_lib
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return redis_lib.Redis.from_url(url)

def _update_redis_state(job_id: str, updates: Dict[str, Any]) -> None:
    try:
        import json
        r = _get_redis()
        key = f"nextstep:nmc:job:{job_id}"
        existing_raw = r.get(key)
        existing = json.loads(existing_raw) if existing_raw else {}
        existing.update(updates)
        r.setex(key, 3600, json.dumps(existing))  # expire in 1 hour
    except Exception as e:
        log.warning("[Task] Redis state update failed: %s", e)

def _update_redis_rows(job_id: str, rows: List[Dict]) -> None:
    _update_redis_state(job_id, {"rows": rows})

def _schedule_cleanup(path: Path, delay_seconds: int = 600) -> None:
    """Schedules folder deletion after delay. Uses a background thread."""
    import threading

    def _delete():
        time.sleep(delay_seconds)
        try:
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)
                log.info("[Cleanup] Deleted: %s", path)
        except Exception as e:
            log.warning("[Cleanup] Failed to delete %s: %s", path, e)

    t = threading.Thread(target=_delete, daemon=True)
    t.start()


def _build_zip_from_folder(storage_path: Path, checked_date: str) -> tuple:
    """Builds ZIP from ALL PDFs currently in the storage folder. Returns (zip_name, zip_ready)."""
    all_pdfs = [f.name for f in storage_path.glob("*.pdf") if f.is_file()]
    if len(all_pdfs) < 2:
        return "", False
    zip_name = _safe_name(f"NMC_Checks_{checked_date}.zip", "NMC_Checks.zip")
    zip_path = storage_path / zip_name
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for pdfn in all_pdfs:
            fp = storage_path / pdfn
            if fp.exists():
                zf.write(fp, arcname=pdfn)
    return zip_name, True
