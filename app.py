import os
"""
app.py — NMC Check Service (NextStep SaaS)
Child-task scheduler version.
"""
import sys, asyncio, anyio, logging, json, io, uuid, time, threading, shutil, datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger(__name__)

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from starlette.middleware.base import BaseHTTPMiddleware

from nmc_extract import extract_nmc_pin_from_bytes, parse_csv_pins, parse_xlsx_pins

APP_DIR = os.path.dirname(os.path.abspath(__file__))
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
STORAGE_ROOT = Path("/tmp/nextstep")
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
BACKEND_VALIDATE_URL = os.environ.get("BACKEND_VALIDATE_URL", "https://nextstep-backend-e75l.onrender.com/api/validate-session")
APP_DASHBOARD_URL = os.environ.get("APP_DASHBOARD_URL", "https://nextstep-backend-e75l.onrender.com/dashboard")
APP_LOGIN_URL = os.environ.get("APP_LOGIN_URL", "https://nextstep-backend-e75l.onrender.com/login")
MAX_CONCURRENT_TASKS = max(1, int(os.environ.get("MAX_CONCURRENT_TASKS", "1")))
WORKER_POLL_INTERVAL = max(1, int(os.environ.get("WORKER_POLL_INTERVAL", "2")))

JOB_PREFIX = "nextstep:nmc:job:"
OWNER_PREFIX = "nextstep:nmc:owner:"
CHILD_PREFIX = "nextstep:nmc:child:"
ACTIVE_CHILDREN_KEY = "nextstep:nmc:children:active"

_redis = None
_dispatcher_started = False
_dispatcher_lock = threading.Lock()
_local_active: set[str] = set()
_local_active_lock = threading.Lock()
_last_parent_pointer = 0

class PersistNextStepTokenMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        token = request.query_params.get("ns_token")
        if token:
            response.set_cookie(key="ns_token", value=token, httponly=True, samesite="lax", secure=True, max_age=60 * 60 * 8)
        return response

app = FastAPI(title="NMC Check — NextStep")
app.add_middleware(PersistNextStepTokenMiddleware)
app.mount("/static", StaticFiles(directory=os.path.join(APP_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(APP_DIR, "templates"))


def get_redis():
    global _redis
    if _redis is None:
        try:
            import redis
            _redis = redis.Redis.from_url(REDIS_URL, decode_responses=False)
            _redis.ping()
        except Exception as e:
            log.error("[Redis] %s", e)
            _redis = None
    return _redis


def _job_key(job_id: str) -> str:
    return f"{JOB_PREFIX}{job_id}"


def _owner_key(job_id: str) -> str:
    return f"{OWNER_PREFIX}{job_id}"


def _child_key(child_id: str) -> str:
    return f"{CHILD_PREFIX}{child_id}"


def _parent_queue_key(job_id: str) -> str:
    return f"nextstep:nmc:parent:{job_id}:queue"


def _job_to_dict(raw) -> Optional[dict]:
    if not raw:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)
    except Exception:
        return None


def _jget(job_id):
    r = get_redis()
    if not r:
        return None
    return _job_to_dict(r.get(_job_key(job_id)))


def _jset(job_id, state):
    r = get_redis()
    if not r:
        return
    r.setex(_job_key(job_id), 60 * 60 * 8, json.dumps(state))


def _owner_set(job_id, tenant_id):
    r = get_redis()
    if not r:
        return
    r.setex(_owner_key(job_id), 60 * 60 * 8, str(tenant_id))


def _owner_get(job_id):
    r = get_redis()
    if not r:
        return None
    try:
        v = r.get(_owner_key(job_id))
        return int(v) if v else None
    except Exception:
        return None


def _child_get(child_id: str) -> Optional[dict]:
    r = get_redis()
    if not r:
        return None
    return _job_to_dict(r.get(_child_key(child_id)))


def _child_set(child_id: str, payload: dict):
    r = get_redis()
    if not r:
        return
    r.setex(_child_key(child_id), 60 * 60 * 8, json.dumps(payload))


def _storage(tenant_id, user_id, job_id):
    p = STORAGE_ROOT / str(tenant_id) / str(user_id) / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _validate_via_backend(token: str):
    if not token:
        return None
    try:
        import requests
        resp = requests.get(BACKEND_VALIDATE_URL, params={"token": token}, timeout=8)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data.get("valid"):
            return None
        user = data.get("user") or {}
        tenant = data.get("tenant") or {}
        return {
            "user_id": user.get("id"),
            "tenant_id": tenant.get("id"),
            "role": user.get("role", "admin"),
            "email": user.get("email"),
            "name": user.get("name"),
        }
    except Exception as e:
        log.warning("[Auth backend] %s", e)
        return None


def _get_ctx(request: Request):
    token = request.headers.get("X-NextStep-Token") or request.cookies.get("ns_token") or request.query_params.get("ns_token") or ""
    if not token:
        return None
    ctx = _validate_via_backend(token)
    if ctx:
        return ctx
    try:
        import db
        return db.validate_user_token(token)
    except Exception as e:
        log.warning("[Auth db] %s", e)
        return None


def _auth(request: Request):
    ctx = _get_ctx(request)
    if not ctx:
        raise HTTPException(status_code=401, detail=f"Not authenticated. Please log in at {APP_LOGIN_URL}")
    return ctx


def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


def _refresh_parent_progress(job_id: str, message: str = "") -> Optional[dict]:
    state = _jget(job_id)
    if not state:
        return None
    rows = state.get("rows") or []
    queued = sum(1 for r in rows if r.get("status") == "queued")
    running = sum(1 for r in rows if r.get("status") == "running")
    successful = sum(1 for r in rows if r.get("status") == "done")
    failed = sum(1 for r in rows if r.get("status") == "failed")
    done = successful + failed
    total = len(rows)
    if done >= total and total > 0:
        state["state"] = "done"
    elif running > 0:
        state["state"] = "running"
    else:
        state["state"] = "queued"
    state["queued_count"] = queued
    state["running_count"] = running
    state["successful"] = successful
    state["failed"] = failed
    if message:
        state["message"] = message
    _jset(job_id, state)
    return state


def _save_parent_state(job_id: str, state: dict):
    _jset(job_id, state)


@app.on_event("startup")
def startup_event():
    global _dispatcher_started
    with _dispatcher_lock:
        if _dispatcher_started:
            return
        _dispatcher_started = True
        t = threading.Thread(target=_dispatcher_loop, daemon=True, name="nmc-child-dispatcher")
        t.start()
        log.info("[Dispatcher] started with MAX_CONCURRENT_TASKS=%d", MAX_CONCURRENT_TASKS)


def _recover_stale_children():
    r = get_redis()
    if not r:
        return
    try:
        for key in r.scan_iter(match=f"{CHILD_PREFIX}*"):
            child = _job_to_dict(r.get(key))
            if not child:
                continue
            if child.get("status") == "running":
                child["status"] = "queued"
                _child_set(child["child_id"], child)
        r.delete(ACTIVE_CHILDREN_KEY)
    except Exception as e:
        log.warning("[Dispatcher] recovery failed: %s", e)


def _list_parents_with_queue() -> List[str]:
    r = get_redis()
    if not r:
        return []
    job_ids = []
    for key in r.scan_iter(match="nextstep:nmc:parent:*:queue"):
        key_s = key.decode() if isinstance(key, bytes) else str(key)
        parts = key_s.split(":")
        if len(parts) >= 5:
            job_id = parts[3]
            try:
                if r.llen(key_s) > 0:
                    job_ids.append(job_id)
            except Exception:
                continue
    job_ids.sort()
    return job_ids


def _active_count() -> int:
    with _local_active_lock:
        return len(_local_active)


def _register_active(child_id: str):
    r = get_redis()
    with _local_active_lock:
        _local_active.add(child_id)
    if r:
        r.sadd(ACTIVE_CHILDREN_KEY, child_id)


def _unregister_active(child_id: str):
    r = get_redis()
    with _local_active_lock:
        _local_active.discard(child_id)
    if r:
        r.srem(ACTIVE_CHILDREN_KEY, child_id)


def _pick_next_child() -> Optional[str]:
    global _last_parent_pointer
    r = get_redis()
    if not r:
        return None
    parents = _list_parents_with_queue()
    if not parents:
        return None
    if _last_parent_pointer >= len(parents):
        _last_parent_pointer = 0
    ordered = parents[_last_parent_pointer:] + parents[:_last_parent_pointer]
    for idx, job_id in enumerate(ordered):
        qkey = _parent_queue_key(job_id)
        child_id = r.lpop(qkey)
        if child_id:
            if isinstance(child_id, bytes):
                child_id = child_id.decode("utf-8")
            _last_parent_pointer = (parents.index(job_id) + 1) % max(1, len(parents))
            return child_id
    return None


def _run_child_thread(child_id: str):
    try:
        import nmc_tasks
        nmc_tasks.process_nmc_child(_child_get(child_id) or {})
    except Exception as e:
        log.exception("[Dispatcher] child %s failed: %s", child_id, e)
    finally:
        _unregister_active(child_id)


def _dispatcher_loop():
    _recover_stale_children()
    while True:
        try:
            while _active_count() < MAX_CONCURRENT_TASKS:
                child_id = _pick_next_child()
                if not child_id:
                    break
                child = _child_get(child_id)
                if not child or child.get("status") != "queued":
                    continue
                child["status"] = "running"
                _child_set(child_id, child)
                _register_active(child_id)
                t = threading.Thread(target=_run_child_thread, args=(child_id,), daemon=True, name=f"nmc-child-{child_id[:8]}")
                t.start()
            time.sleep(WORKER_POLL_INTERVAL)
        except Exception as e:
            log.exception("[Dispatcher] loop error: %s", e)
            time.sleep(WORKER_POLL_INTERVAL)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "dashboard_url": APP_DASHBOARD_URL, "login_url": APP_LOGIN_URL})


@app.get("/health")
def health():
    r = get_redis()
    ok = False
    try:
        if r:
            r.ping()
            ok = True
    except Exception:
        pass
    return {"ok": True, "redis": ok, "twocaptcha": bool(os.getenv("TWOCAPTCHA_API_KEY")), "db": bool(os.getenv("DATABASE_URL")), "max_concurrent_tasks": MAX_CONCURRENT_TASKS}


@app.post("/nmc/extract")
async def nmc_extract(request: Request, files: List[UploadFile] = File(...)):
    _auth(request)
    if not files:
        raise HTTPException(400, "No file uploaded.")
    items: List[Dict] = []
    for file in files[:100]:
        content = await file.read()
        fname = file.filename or "upload"
        flower = fname.lower()
        if len(content) > 25 * 1024 * 1024:
            raise HTTPException(413, f"'{fname}' too large.")
        if flower.endswith(".csv"):
            for r in parse_csv_pins(content):
                if len(items) >= 100: break
                items.append({"original_filename": f"{fname} ({r['original_filename']})", "nmc_pin": r["nmc_pin"], "confidence": 95, "source": "Spreadsheet"})
            continue
        if flower.endswith(".xlsx"):
            for r in parse_xlsx_pins(content):
                if len(items) >= 100: break
                items.append({"original_filename": f"{fname} ({r['original_filename']})", "nmc_pin": r["nmc_pin"], "confidence": 95, "source": "Spreadsheet"})
            continue
        if flower.endswith(".webp"):
            try:
                from PIL import Image
                im = Image.open(io.BytesIO(content)).convert("RGB")
                buf = io.BytesIO(); im.save(buf, "PNG")
                content = buf.getvalue(); fname = fname[:-5] + ".png"; flower = fname.lower()
            except Exception:
                pass
        result = extract_nmc_pin_from_bytes(content, fname)
        pin = (result.get("nmc_pin") or "").strip().upper()
        conf = int((result.get("confidence") or {}).get("nmc_pin", 0) * 100)
        items.append({"original_filename": fname, "nmc_pin": pin, "confidence": conf, "source": "PDF text" if flower.endswith(".pdf") else "Image scan"})
        if len(items) >= 100:
            break
    return JSONResponse({"items": items})


def _create_parent_job(tenant_id: int, user_id: int, items: List[dict], db_job_id: Optional[int], storage_path: Path, reuse_rows: Optional[List[dict]] = None, old_job_id: Optional[str] = None) -> tuple[str, list[dict]]:
    r = get_redis()
    if not r:
        raise HTTPException(500, "Redis unavailable.")
    job_id = str(uuid.uuid4())
    if reuse_rows is None:
        rows = [{"row": i + 1, "status": "queued", "nmc_pin": (it.get("nmc_pin") or ""), "original_filename": (it.get("original_filename") or f"Row {i+1}"), "name": "", "expiry_date": "", "status_text": "", "pdf_filename": "", "pdf_url": "", "error": ""} for i, it in enumerate(items)]
    else:
        rows = reuse_rows

    state = {
        "state": "queued",
        "rows": rows,
        "zip_ready": False,
        "zip_name": "",
        "zip_url": "",
        "message": "Batch queued — processing starts shortly...",
        "successful": 0,
        "failed": 0,
        "queued_count": sum(1 for r0 in rows if r0.get("status") == "queued"),
        "running_count": 0,
        "parent_total": len(rows),
        "db_job_id": db_job_id,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "storage_path": str(storage_path),
        "created_at": _now_iso(),
        "source_job_id": old_job_id or "",
    }
    _save_parent_state(job_id, state)
    _owner_set(job_id, tenant_id)
    return job_id, rows


def _enqueue_child_tasks(job_id: str, tenant_id: int, user_id: int, storage_path: Path, items: List[dict]):
    r = get_redis()
    for idx, it in enumerate(items, start=1):
        child_id = str(uuid.uuid4())
        payload = {
            "child_id": child_id,
            "job_id": job_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "row_number": int(it.get("row_number") or idx),
            "nmc_pin": (it.get("nmc_pin") or "").strip().upper(),
            "original_filename": it.get("original_filename") or f"Row {idx}",
            "storage_path": str(storage_path),
            "status": "queued",
            "created_at": _now_iso(),
        }
        _child_set(child_id, payload)
        r.rpush(_parent_queue_key(job_id), child_id)


@app.post("/nmc/run")
async def nmc_run(request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]; user_id = ctx["user_id"]
    payload: Dict = {}
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
    else:
        form = await request.form(); payload = dict(form)
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(400, "No items provided.")
    items = items[:100]
    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0:
            raise HTTPException(402, "No tokens remaining. Please contact NextStep to top up.")
        if 0 < tokens < len(items):
            items = items[:tokens]
    except HTTPException:
        raise
    except Exception as e:
        log.warning("[Run] Token check skipped: %s", e)

    storage_path = _storage(tenant_id, user_id, str(uuid.uuid4()))
    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=len(items))
    except Exception as e:
        log.warning("[Run] DB record failed: %s", e)
    job_id, rows = _create_parent_job(tenant_id, user_id, items, db_job_id, storage_path)
    # move storage to actual job id path
    real_storage = _storage(tenant_id, user_id, job_id)
    if storage_path != real_storage and storage_path.exists() and not any(storage_path.iterdir()):
        try:
            storage_path.rmdir()
        except Exception:
            pass
    st = _jget(job_id) or {}
    st["storage_path"] = str(real_storage)
    _jset(job_id, st)
    _enqueue_child_tasks(job_id, tenant_id, user_id, real_storage, items)
    return JSONResponse({"job_id": job_id, "status_url": f"/nmc/status/{job_id}", "rows": rows, "queued": True})


@app.get("/nmc/status/{job_id}")
async def nmc_status(job_id: str, request: Request):
    _auth(request)
    state = _jget(job_id)
    if not state:
        raise HTTPException(404, "Job expired or not found.")
    rows = state.get("rows") or []
    done = sum(1 for r in rows if r.get("status") in ("done", "failed"))
    running = sum(1 for r in rows if r.get("status") == "running")
    queued = sum(1 for r in rows if r.get("status") == "queued")
    return JSONResponse({"job_id": job_id, "state": state.get("state", "queued"), "running": {"done": done, "total": len(rows) or 1}, "rows": rows, "zip_ready": bool(state.get("zip_ready")), "zip_name": state.get("zip_name", ""), "zip_url": state.get("zip_url", ""), "message": state.get("message", ""), "successful": state.get("successful", 0), "failed": state.get("failed", 0), "running_count": running, "queued_count": queued})


@app.get("/nmc/download/{job_id}/{name}")
async def nmc_download(job_id: str, name: str, request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]
    job_tenant = _owner_get(job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        raise HTTPException(403, "Access denied.")
    tenant_root = STORAGE_ROOT / str(tenant_id)
    file_path = None
    for p in tenant_root.rglob(name):
        if job_id in str(p):
            file_path = p
            break
    if not file_path:
        state = _jget(job_id) or {}
        source_job_id = state.get("source_job_id") or ""
        for p in tenant_root.rglob(name):
            if source_job_id and source_job_id in str(p):
                file_path = p
                break
    if not file_path:
        matches = list(tenant_root.rglob(name))
        if len(matches) == 1:
            file_path = matches[0]
    if not file_path or not file_path.exists():
        raise HTTPException(404, "Download expired or not found.")
    bg = None
    if name.lower().endswith(".zip"):
        sp = STORAGE_ROOT / str(tenant_id) / str(ctx["user_id"]) / job_id
        bg = BackgroundTask(_cleanup, sp, job_id)
    return FileResponse(str(file_path), filename=name, media_type="application/octet-stream", background=bg)


def _cleanup(storage_path: Path, job_id: str):
    import time as t
    t.sleep(5)
    try:
        if storage_path.exists():
            shutil.rmtree(storage_path, ignore_errors=True)
    except Exception as e:
        log.warning("[Cleanup] %s", e)
    r = get_redis()
    if r:
        try:
            r.delete(_job_key(job_id))
            r.delete(_owner_key(job_id))
            for key in r.scan_iter(match=f"{CHILD_PREFIX}*"):
                child = _job_to_dict(r.get(key))
                if child and child.get("job_id") == job_id:
                    r.delete(key)
            r.delete(_parent_queue_key(job_id))
        except Exception:
            pass


@app.get("/nmc/export/excel/{job_id}")
async def nmc_export_excel(job_id: str, request: Request):
    ctx = _auth(request)
    jt = _owner_get(job_id)
    if jt is not None and jt != ctx["tenant_id"]:
        raise HTTPException(403, "Access denied.")
    state = _jget(job_id)
    if not state:
        raise HTTPException(404, "Job not found.")
    rows = state.get("rows") or []
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "NMC Results"
    headers = ["#", "NMC PIN", "Name", "Expiry Date", "Status", "Result"]
    hf = PatternFill("solid", fgColor="1e3a8a"); hfont = Font(bold=True, color="FFFFFF")
    for c, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.fill = hf; cell.font = hfont; cell.alignment = Alignment(horizontal="center")
    for w, col in zip([6, 16, 30, 14, 36, 12], "ABCDEF"):
        ws.column_dimensions[col].width = w
    for r0 in rows:
        rn = ws.max_row + 1; st = r0.get("status", "")
        rl = "Done" if st == "done" else ("Failed" if st == "failed" else st.title())
        ws.append([r0.get("row", ""), r0.get("nmc_pin", ""), r0.get("name", ""), r0.get("expiry_date", ""), r0.get("status_text", ""), rl])
        rc = ws.cell(row=rn, column=6)
        if st == "done":
            rc.fill = PatternFill("solid", fgColor="d1fae5"); rc.font = Font(color="065f46")
        elif st == "failed":
            rc.fill = PatternFill("solid", fgColor="fee2e2"); rc.font = Font(color="991b1b")
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    ds = datetime.datetime.utcnow().strftime("%d.%m.%Y")
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f'attachment; filename="NMC_Results_{ds}.xlsx"'})


@app.get("/nmc/export/csv/{job_id}")
async def nmc_export_csv(job_id: str, request: Request):
    ctx = _auth(request)
    jt = _owner_get(job_id)
    if jt is not None and jt != ctx["tenant_id"]:
        raise HTTPException(403, "Access denied.")
    state = _jget(job_id)
    if not state:
        raise HTTPException(404, "Job not found.")
    rows = state.get("rows") or []
    import csv as cm
    buf = io.StringIO(); w = cm.writer(buf)
    w.writerow(["#", "NMC PIN", "Name", "Expiry Date", "Status", "Result"])
    for r0 in rows:
        st = r0.get("status", "")
        w.writerow([r0.get("row", ""), r0.get("nmc_pin", ""), r0.get("name", ""), r0.get("expiry_date", ""), r0.get("status_text", ""), "Done" if st == "done" else ("Failed" if st == "failed" else st.title())])
    ds = datetime.datetime.utcnow().strftime("%d.%m.%Y")
    return StreamingResponse(io.BytesIO(buf.getvalue().encode("utf-8-sig")), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="NMC_Results_{ds}.csv"'})


@app.post("/nmc/rerun")
async def nmc_rerun(request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]; user_id = ctx["user_id"]
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON.")
    old_job_id = (payload.get("job_id") or "").strip()
    dirty_items = payload.get("items") or []
    if not old_job_id:
        raise HTTPException(400, "job_id is required.")
    if not dirty_items:
        raise HTTPException(400, "No items provided for rerun.")
    job_tenant = _owner_get(old_job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        raise HTTPException(403, "Access denied.")
    old_state = _jget(old_job_id)
    if not old_state:
        raise HTTPException(404, "Original job expired. Please run a fresh check.")
    old_rows = old_state.get("rows") or []
    storage_path = STORAGE_ROOT / str(tenant_id) / str(user_id) / old_job_id
    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0:
            raise HTTPException(402, "No tokens remaining.")
        if 0 < tokens < len(dirty_items):
            dirty_items = dirty_items[:tokens]
    except HTTPException:
        raise
    except Exception as e:
        log.warning("[Rerun] Token check skipped: %s", e)
    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=len(dirty_items))
    except Exception as e:
        log.warning("[Rerun] DB record failed: %s", e)
    dirty_map = {int(it.get("row_number") or 0): it for it in dirty_items}
    merged_rows = []
    for r0 in old_rows:
        row_no = int(r0.get("row") or 0)
        if row_no in dirty_map:
            merged_rows.append({**r0, "status": "queued", "nmc_pin": (dirty_map[row_no].get("nmc_pin") or "").strip().upper(), "pdf_filename": "", "pdf_url": "", "error": "", "name": "", "expiry_date": "", "status_text": ""})
        else:
            merged_rows.append(r0)
    job_id, rows = _create_parent_job(tenant_id, user_id, dirty_items, db_job_id, storage_path, reuse_rows=merged_rows, old_job_id=old_job_id)
    _enqueue_child_tasks(job_id, tenant_id, user_id, storage_path, dirty_items)
    return JSONResponse({"job_id": job_id, "status_url": f"/nmc/status/{job_id}", "rows": rows, "queued": True})
