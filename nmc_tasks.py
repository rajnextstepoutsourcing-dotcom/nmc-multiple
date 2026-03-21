"""nmc_tasks.py — Child task processor for NMC."""
import os, json, logging, zipfile, shutil, threading, datetime, time, re
from pathlib import Path
from typing import Dict, Any, List, Optional

log = logging.getLogger(__name__)
STATE_LOCK = threading.Lock()
JOB_PREFIX = "nextstep:nmc:job:"
CHILD_PREFIX = "nextstep:nmc:child:"


def _get_redis():
    import redis as redis_lib
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return redis_lib.Redis.from_url(url)


def _jget(job_id: str) -> Optional[dict]:
    try:
        raw = _get_redis().get(f"{JOB_PREFIX}{job_id}")
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


def _jset(job_id: str, state: dict):
    _get_redis().setex(f"{JOB_PREFIX}{job_id}", 60 * 60 * 8, json.dumps(state))


def _cset(child_id: str, state: dict):
    _get_redis().setex(f"{CHILD_PREFIX}{child_id}", 60 * 60 * 8, json.dumps(state))


def _safe_name(name: str, default: str) -> str:
    name = re.sub(r"\s+", " ", (name or "").strip()) or default
    return re.sub(r'[\\/:"*?<>|]+', "-", name).strip()


def _uk_now_str() -> str:
    try:
        from zoneinfo import ZoneInfo
        dt = datetime.datetime.now(tz=ZoneInfo("Europe/London"))
    except Exception:
        dt = datetime.datetime.utcnow()
    return dt.strftime("%d.%m.%Y")


def _update_row(job_id: str, row_number: int, updater):
    with STATE_LOCK:
        state = _jget(job_id) or {}
        rows = state.get("rows") or []
        idx = max(0, row_number - 1)
        if idx >= len(rows):
            return None
        row = rows[idx]
        updater(row)
        successful = sum(1 for r in rows if r.get("status") == "done")
        failed = sum(1 for r in rows if r.get("status") == "failed")
        running = sum(1 for r in rows if r.get("status") == "running")
        queued = sum(1 for r in rows if r.get("status") == "queued")
        total = len(rows)
        state["rows"] = rows
        state["successful"] = successful
        state["failed"] = failed
        state["running_count"] = running
        state["queued_count"] = queued
        state["state"] = "done" if total and successful + failed >= total else ("running" if running else "queued")
        state["message"] = f"{successful + failed} / {total} completed"
        _jset(job_id, state)
        return state


def _schedule_cleanup(path: Path, delay_seconds: int = 600) -> None:
    def _delete():
        time.sleep(delay_seconds)
        try:
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)
        except Exception as e:
            log.warning("[Cleanup] Failed to delete %s: %s", path, e)
    threading.Thread(target=_delete, daemon=True).start()


def _build_zip_from_folder(storage_path: Path, checked_date: str) -> tuple[str, bool]:
    all_pdfs = [f.name for f in storage_path.glob("*.pdf") if f.is_file() and not f.name.lower().endswith('.zip')]
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


def _finalize_parent_if_complete(job_id: str):
    with STATE_LOCK:
        state = _jget(job_id)
        if not state:
            return
        rows = state.get("rows") or []
        total = len(rows)
        successful = sum(1 for r in rows if r.get("status") == "done")
        failed = sum(1 for r in rows if r.get("status") == "failed")
        running = sum(1 for r in rows if r.get("status") == "running")
        queued = sum(1 for r in rows if r.get("status") == "queued")
        if total == 0 or successful + failed < total or running or queued:
            _jset(job_id, state)
            return
        storage_path = Path(state.get("storage_path") or "")
        checked_date = _uk_now_str()
        zip_name, zip_ready = _build_zip_from_folder(storage_path, checked_date)
        state["state"] = "done"
        state["zip_ready"] = zip_ready
        state["zip_name"] = zip_name
        state["zip_url"] = f"/nmc/download/{job_id}/{zip_name}" if zip_ready else ""
        state["message"] = f"Completed {successful} / {total}" if successful else "No PDFs generated. Check errors above."
        state["successful"] = successful
        state["failed"] = failed
        _jset(job_id, state)
    db_job_id = state.get("db_job_id")
    tenant_id = state.get("tenant_id")
    user_id = state.get("user_id")
    if db_job_id:
        try:
            import db
            db.update_job_status(db_job_id=db_job_id, status="completed" if successful > 0 else "failed", successful_items=successful, failed_items=failed)
        except Exception as e:
            log.warning("[Task] DB final update failed: %s", e)
    billable_successes = sum(1 for r in rows if r.get("status") == "done" and r.get("billable") and r.get("attempt_in_current_job"))
    if billable_successes > 0 and tenant_id:
        try:
            import db
            db.record_usage(tenant_id=tenant_id, user_id=user_id, db_job_id=db_job_id, successful_outputs=billable_successes)
        except Exception as e:
            log.warning("[Task] record_usage failed: %s", e)
    if storage_path:
        _schedule_cleanup(storage_path, delay_seconds=600)


def process_nmc_child(job_data: Dict[str, Any]) -> Dict[str, Any]:
    from nmc_runner import run_nmc_check_sync
    import db

    if not job_data:
        return {"ok": False, "error": "Missing child payload"}
    child_id = job_data["child_id"]
    job_id = job_data["job_id"]
    row_number = int(job_data.get("row_number") or 1)
    pin = (job_data.get("nmc_pin") or "").strip().upper()
    storage_path = Path(job_data["storage_path"])
    storage_path.mkdir(parents=True, exist_ok=True)
    checked_date = _uk_now_str()

    try:
        state = _jget(job_id) or {}
        db_job_id = state.get("db_job_id")
        if db_job_id:
            db.update_job_status(db_job_id=db_job_id, status="running", successful_items=state.get("successful", 0), failed_items=state.get("failed", 0))
    except Exception:
        pass

    def mark_running(row: dict):
        row["status"] = "running"
        row["error"] = ""
    _update_row(job_id, row_number, mark_running)

    if not pin:
        def mark_missing(row: dict):
            row["status"] = "failed"
            row["error"] = "No NMC PIN found."
        _update_row(job_id, row_number, mark_missing)
        job_data["status"] = "failed"
        _cset(child_id, job_data)
        _finalize_parent_if_complete(job_id)
        return {"ok": False, "error": "No NMC PIN found."}

    item_dir = storage_path / f"item_{row_number:03d}"
    item_dir.mkdir(parents=True, exist_ok=True)
    try:
        result = run_nmc_check_sync(pin, str(item_dir))
    except Exception as e:
        result = {"ok": False, "error": str(e), "pdf_path": ""}

    if result.get("ok"):
        name = result.get("name") or pin
        expiry_date = result.get("expiry_date") or ""
        status_text = result.get("status_text") or ""
        final_name = _safe_name(f"{name} - {pin} - {checked_date}.pdf", f"NMC-{row_number}.pdf")
        pdf_src = result.get("pdf_path") or ""
        final_path = storage_path / final_name
        if pdf_src and Path(pdf_src).exists():
            try:
                if final_path.exists():
                    final_path.unlink()
                shutil.move(pdf_src, str(final_path))
            except Exception:
                final_path = Path(pdf_src)
                final_name = final_path.name

            def mark_done(row: dict):
                row.update({"status": "done", "pdf_filename": final_name, "pdf_url": f"/nmc/download/{job_id}/{final_name}", "name": name, "expiry_date": expiry_date, "status_text": status_text, "error": "", "billable": True})
            _update_row(job_id, row_number, mark_done)
            job_data["status"] = "done"
            _cset(child_id, job_data)
        else:
            def mark_failed_pdf(row: dict):
                row["status"] = "failed"
                row["error"] = result.get("error") or "PDF not found after run."
            _update_row(job_id, row_number, mark_failed_pdf)
            job_data["status"] = "failed"
            _cset(child_id, job_data)
    else:
        error_msg = result.get("error") or "Check failed."
        snap_name = ""
        pdf_src = result.get("pdf_path") or ""
        if pdf_src and Path(pdf_src).exists():
            snap_name = _safe_name(f"ERROR-{pin}-{checked_date}.pdf", f"error-{row_number}.pdf")
            snap_path = storage_path / snap_name
            try:
                shutil.move(pdf_src, str(snap_path))
            except Exception:
                snap_name = ""
        def mark_failed(row: dict):
            row["status"] = "failed"
            row["error"] = error_msg
            if snap_name:
                row["pdf_filename"] = snap_name
                row["pdf_url"] = f"/nmc/download/{job_id}/{snap_name}"
        _update_row(job_id, row_number, mark_failed)
        job_data["status"] = "failed"
        _cset(child_id, job_data)

    _finalize_parent_if_complete(job_id)
    return {"ok": True, "child_id": child_id, "job_id": job_id}
