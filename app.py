"""
app.py — NMC Check Service (NextStep SaaS)
"""
import sys, asyncio, anyio, logging, json, os, io, re, secrets
import datetime, shutil, uuid, zipfile, time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from functools import partial

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger(__name__)

from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from starlette.responses import Response

from nmc_extract import extract_nmc_pin_from_bytes, parse_csv_pins, parse_xlsx_pins
from pdf_utils import make_simple_error_pdf

APP_DIR       = os.path.dirname(os.path.abspath(__file__))
REDIS_URL     = os.environ.get("REDIS_URL", "redis://localhost:6379")
NMC_QUEUE     = "nextstep:nmc:jobs"
STORAGE_ROOT  = Path("/tmp/nextstep")
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="NMC Check — NextStep")
app.mount("/static", StaticFiles(directory=os.path.join(APP_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(APP_DIR, "templates"))

# ── Redis ──────────────────────────────────────────────────────────────────────
_redis = None
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

def _jget(job_id):
    r = get_redis()
    if not r: return None
    try:
        raw = r.get(f"nextstep:nmc:job:{job_id}")
        return json.loads(raw) if raw else None
    except: return None

def _jset(job_id, state):
    r = get_redis()
    if not r: return
    try: r.setex(f"nextstep:nmc:job:{job_id}", 3600, json.dumps(state))
    except: pass

def _owner_set(job_id, tenant_id):
    r = get_redis()
    if not r: return
    try: r.setex(f"nextstep:nmc:owner:{job_id}", 3600, str(tenant_id))
    except: pass

def _owner_get(job_id):
    r = get_redis()
    if not r: return None
    try:
        v = r.get(f"nextstep:nmc:owner:{job_id}")
        return int(v) if v else None
    except: return None

# ── Auth ──────────────────────────────────────────────────────────────────────
def _get_ctx(request: Request):
    token = (request.headers.get("X-NextStep-Token")
             or request.cookies.get("ns_token")
             or request.query_params.get("ns_token") or "")
    if not token:
        return None
    try:
        import db
        return db.validate_user_token(token)
    except Exception as e:
        log.warning("[Auth] %s", e)
        return None

def _auth(request: Request):
    ctx = _get_ctx(request)
    if not ctx:
        raise HTTPException(401, "Not authenticated. Please log in at nextstep.co.uk")
    return ctx

# ── Storage ───────────────────────────────────────────────────────────────────
def _storage(tenant_id, user_id, job_id):
    p = STORAGE_ROOT / str(tenant_id) / str(user_id) / job_id
    p.mkdir(parents=True, exist_ok=True)
    return p

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
def health():
    r = get_redis()
    ok = False
    try:
        if r: r.ping(); ok = True
    except: pass
    return {"ok": True, "redis": ok,
            "twocaptcha": bool(os.getenv("TWOCAPTCHA_API_KEY")),
            "db": bool(os.getenv("DATABASE_URL"))}

@app.post("/nmc/extract")
async def nmc_extract(request: Request, files: List[UploadFile] = File(...)):
    _auth(request)
    if not files: raise HTTPException(400, "No file uploaded.")
    items: List[Dict] = []
    for file in files[:100]:
        content = await file.read()
        fname = file.filename or "upload"
        flower = fname.lower()
        if len(content) > 25*1024*1024:
            raise HTTPException(413, f"'{fname}' too large.")
        if flower.endswith(".csv"):
            for r in parse_csv_pins(content):
                if len(items) >= 100: break
                items.append({"original_filename": f"{fname} ({r['original_filename']})",
                               "nmc_pin": r["nmc_pin"], "confidence": 95, "source": "Spreadsheet"})
            continue
        if flower.endswith(".xlsx"):
            for r in parse_xlsx_pins(content):
                if len(items) >= 100: break
                items.append({"original_filename": f"{fname} ({r['original_filename']})",
                               "nmc_pin": r["nmc_pin"], "confidence": 95, "source": "Spreadsheet"})
            continue
        if flower.endswith(".webp"):
            try:
                from PIL import Image
                im = Image.open(io.BytesIO(content)).convert("RGB")
                buf = io.BytesIO(); im.save(buf, "PNG")
                content = buf.getvalue(); fname = fname[:-5]+".png"; flower = fname.lower()
            except: pass
        result = extract_nmc_pin_from_bytes(content, fname)
        pin = (result.get("nmc_pin") or "").strip().upper()
        conf = int((result.get("confidence") or {}).get("nmc_pin", 0)*100)
        items.append({"original_filename": fname, "nmc_pin": pin, "confidence": conf,
                       "source": "PDF text" if flower.endswith(".pdf") else "Image scan"})
        if len(items) >= 100: break
    return JSONResponse({"items": items})

@app.post("/nmc/run")
async def nmc_run(request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]; user_id = ctx["user_id"]

    payload: Dict = {}
    ctype = (request.headers.get("content-type") or "").lower()
    if "application/json" in ctype:
        try: payload = await request.json()
        except: payload = {}
    else:
        form = await request.form(); payload = dict(form)

    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(400, "No items provided.")
    items = items[:100]

    # Token check
    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0:
            raise HTTPException(402, "No tokens remaining. Please contact NextStep to top up.")
        if 0 < tokens < len(items):
            items = items[:tokens]
    except HTTPException: raise
    except Exception as e: log.warning("[Run] Token check skipped: %s", e)

    job_id = str(uuid.uuid4())
    storage_path = _storage(tenant_id, user_id, job_id)

    # DB job record
    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(tenant_id=tenant_id, user_id=user_id, total_items=len(items))
    except Exception as e: log.warning("[Run] DB record failed: %s", e)

    rows = [{"row": i+1, "status": "queued",
             "nmc_pin": (it.get("nmc_pin") if isinstance(it, dict) else ""),
             "original_filename": (it.get("original_filename") if isinstance(it, dict) else f"Row {i+1}"),
             "name": "", "expiry_date": "", "status_text": "",
             "pdf_filename": "", "pdf_url": "", "error": ""}
            for i, it in enumerate(items)]

    _jset(job_id, {"state": "queued", "rows": rows, "zip_ready": False,
                   "zip_name": "", "zip_url": "",
                   "message": "Job queued — processing starts shortly...",
                   "successful": 0, "failed": 0})
    _owner_set(job_id, tenant_id)

    job_data = {"job_id": job_id, "db_job_id": db_job_id,
                "tenant_id": tenant_id, "user_id": user_id,
                "items": items, "storage_path": str(storage_path)}

    # Enqueue to Redis
    enqueued = False
    try:
        from rq import Queue as RQ
        import redis as rl
        q = RQ(NMC_QUEUE, connection=rl.Redis.from_url(REDIS_URL))
        q.enqueue("nmc_tasks.process_nmc_job", job_data, job_timeout=7200, result_ttl=3600)
        enqueued = True
        log.info("[Run] Job %s enqueued %d items tenant=%d", job_id, len(items), tenant_id)
    except Exception as e:
        log.error("[Run] Enqueue failed, running directly: %s", e)
        asyncio.get_event_loop().create_task(_direct(job_id, job_data))

    return JSONResponse({"job_id": job_id, "status_url": f"/nmc/status/{job_id}",
                         "rows": rows, "queued": enqueued})

async def _direct(job_id, job_data):
    try:
        import nmc_tasks
        await anyio.to_thread.run_sync(partial(nmc_tasks.process_nmc_job, job_data), cancellable=True)
    except Exception as e:
        log.error("[Direct] %s", e)
        _jset(job_id, {"state": "done", "message": f"Job failed: {e}", "zip_ready": False})

@app.get("/nmc/status/{job_id}")
async def nmc_status(job_id: str, request: Request):
    _auth(request)
    state = _jget(job_id)
    if not state: raise HTTPException(404, "Job expired or not found.")
    rows = state.get("rows") or []
    done = sum(1 for r in rows if r.get("status") in ("done","failed"))
    return JSONResponse({"job_id": job_id, "state": state.get("state","queued"),
                         "running": {"done": done, "total": len(rows) or 1},
                         "rows": rows, "zip_ready": bool(state.get("zip_ready")),
                         "zip_name": state.get("zip_name",""), "zip_url": state.get("zip_url",""),
                         "message": state.get("message",""),
                         "successful": state.get("successful",0), "failed": state.get("failed",0)})

@app.get("/nmc/download/{job_id}/{name}")
async def nmc_download(job_id: str, name: str, request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]

    # Ownership check
    job_tenant = _owner_get(job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        log.warning("[DL] Tenant %d tried job owned by %d", tenant_id, job_tenant)
        raise HTTPException(403, "Access denied.")

    # Find file
    tenant_root = STORAGE_ROOT / str(tenant_id)
    file_path = None
    for p in tenant_root.rglob(name):
        if job_id in str(p):
            file_path = p; break

    if not file_path or not file_path.exists():
        raise HTTPException(404, "Download expired or not found.")

    bg = None
    if name.lower().endswith(".zip"):
        sp = STORAGE_ROOT / str(tenant_id) / str(ctx["user_id"]) / job_id
        bg = BackgroundTask(_cleanup, sp, job_id)

    return FileResponse(str(file_path), filename=name, media_type="application/octet-stream", background=bg)

def _cleanup(storage_path: Path, job_id: str):
    import time as t; t.sleep(5)
    try:
        if storage_path.exists(): shutil.rmtree(storage_path, ignore_errors=True)
    except Exception as e: log.warning("[Cleanup] %s", e)
    r = get_redis()
    if r:
        try: r.delete(f"nextstep:nmc:job:{job_id}"); r.delete(f"nextstep:nmc:owner:{job_id}")
        except: pass

@app.get("/nmc/export/excel/{job_id}")
async def nmc_export_excel(job_id: str, request: Request):
    ctx = _auth(request)
    jt = _owner_get(job_id)
    if jt is not None and jt != ctx["tenant_id"]: raise HTTPException(403, "Access denied.")
    state = _jget(job_id)
    if not state: raise HTTPException(404, "Job not found.")
    rows = state.get("rows") or []

    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "NMC Results"
    headers = ["#","NMC PIN","Name","Expiry Date","Status","Result"]
    hf = PatternFill("solid", fgColor="1e3a8a"); hfont = Font(bold=True, color="FFFFFF")
    for c, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=c, value=h)
        cell.fill = hf; cell.font = hfont; cell.alignment = Alignment(horizontal="center")
    for w, col in zip([6,16,30,14,36,12], "ABCDEF"): ws.column_dimensions[col].width = w
    for r in rows:
        rn = ws.max_row+1; st = r.get("status","")
        rl = "Done" if st=="done" else ("Failed" if st=="failed" else st.title())
        ws.append([r.get("row",""), r.get("nmc_pin",""), r.get("name",""),
                   r.get("expiry_date",""), r.get("status_text",""), rl])
        rc = ws.cell(row=rn, column=6)
        if st=="done": rc.fill=PatternFill("solid",fgColor="d1fae5"); rc.font=Font(color="065f46")
        elif st=="failed": rc.fill=PatternFill("solid",fgColor="fee2e2"); rc.font=Font(color="991b1b")
    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    try:
        from zoneinfo import ZoneInfo; ds = datetime.datetime.now(tz=ZoneInfo("Europe/London")).strftime("%d.%m.%Y")
    except: ds = datetime.datetime.utcnow().strftime("%d.%m.%Y")
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="NMC_Results_{ds}.xlsx"'})

@app.get("/nmc/export/csv/{job_id}")
async def nmc_export_csv(job_id: str, request: Request):
    ctx = _auth(request)
    jt = _owner_get(job_id)
    if jt is not None and jt != ctx["tenant_id"]: raise HTTPException(403, "Access denied.")
    state = _jget(job_id)
    if not state: raise HTTPException(404, "Job not found.")
    rows = state.get("rows") or []
    import csv as cm; buf = io.StringIO(); w = cm.writer(buf)
    w.writerow(["#","NMC PIN","Name","Expiry Date","Status","Result"])
    for r in rows:
        st = r.get("status","")
        w.writerow([r.get("row",""), r.get("nmc_pin",""), r.get("name",""),
                    r.get("expiry_date",""), r.get("status_text",""),
                    "Done" if st=="done" else ("Failed" if st=="failed" else st.title())])
    try:
        from zoneinfo import ZoneInfo; ds = datetime.datetime.now(tz=ZoneInfo("Europe/London")).strftime("%d.%m.%Y")
    except: ds = datetime.datetime.utcnow().strftime("%d.%m.%Y")
    return StreamingResponse(io.BytesIO(buf.getvalue().encode("utf-8-sig")), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="NMC_Results_{ds}.csv"'})

# ── Rerun (dirty rows only) ────────────────────────────────────────────────────
@app.post("/nmc/rerun")
async def nmc_rerun(request: Request):
    ctx = _auth(request)
    tenant_id = ctx["tenant_id"]; user_id = ctx["user_id"]

    payload: Dict = {}
    try: payload = await request.json()
    except: raise HTTPException(400, "Invalid JSON.")

    old_job_id = (payload.get("job_id") or "").strip()
    dirty_items = payload.get("items") or []

    if not old_job_id:
        raise HTTPException(400, "job_id is required.")
    if not dirty_items:
        raise HTTPException(400, "No items provided for rerun.")

    # Verify ownership of original job
    job_tenant = _owner_get(old_job_id)
    if job_tenant is not None and job_tenant != tenant_id:
        raise HTTPException(403, "Access denied.")

    # Get existing job state — keep non-dirty rows as-is
    old_state = _jget(old_job_id)
    if not old_state:
        raise HTTPException(404, "Original job expired. Please run a fresh check.")

    old_rows = old_state.get("rows") or []
    old_storage = STORAGE_ROOT / str(tenant_id) / str(user_id) / old_job_id

    # Token check — only charge for dirty rows being rerun
    dirty_count = len(dirty_items)
    try:
        import db
        tokens = db.get_tenant_tokens_remaining(tenant_id)
        if tokens == 0:
            raise HTTPException(402, "No tokens remaining.")
        if 0 < tokens < dirty_count:
            dirty_items = dirty_items[:tokens]
            dirty_count = len(dirty_items)
    except HTTPException: raise
    except Exception as e: log.warning("[Rerun] Token check skipped: %s", e)

    # Create new job ID — reuse same storage folder so old PDFs remain accessible
    new_job_id = str(uuid.uuid4())
    # Storage reuses old path — new job writes into same folder
    storage_path = old_storage

    # DB record for rerun
    db_job_id = None
    try:
        import db
        db_job_id = db.create_job_record(
            tenant_id=tenant_id, user_id=user_id, total_items=dirty_count
        )
    except Exception as e: log.warning("[Rerun] DB record failed: %s", e)

    # Build merged row list — dirty rows get reset to queued, non-dirty keep their state
    dirty_row_nums = {it.get("row_number") for it in dirty_items if it.get("row_number")}
    merged_rows = []
    for r in old_rows:
        rnum = r.get("row", 0)
        if rnum in dirty_row_nums:
            # Find the new PIN from dirty_items
            new_pin = next((it.get("nmc_pin","") for it in dirty_items if it.get("row_number")==rnum), r.get("nmc_pin",""))
            merged_rows.append({**r, "status": "queued", "nmc_pin": new_pin.strip().upper(),
                                 "pdf_filename": "", "pdf_url": "", "error": "", "name": "", "expiry_date": "", "status_text": ""})
        else:
            merged_rows.append(r)

    # Rebuild items for worker — only dirty ones
    rerun_items = []
    for it in dirty_items:
        rerun_items.append({
            "nmc_pin": (it.get("nmc_pin") or "").strip().upper(),
            "original_filename": it.get("original_filename") or f"Row {it.get('row_number','')}",
            "row_number": it.get("row_number"),
        })

    # Set new job state in Redis
    _jset(new_job_id, {
        "state": "queued", "rows": merged_rows,
        "zip_ready": old_state.get("zip_ready", False),
        "zip_name": old_state.get("zip_name", ""),
        "zip_url": old_state.get("zip_url", ""),
        "message": f"Rerunning {dirty_count} edited row(s)…",
        "successful": 0, "failed": 0,
    })
    _owner_set(new_job_id, tenant_id)

    job_data = {
        "job_id": new_job_id,
        "db_job_id": db_job_id,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "items": rerun_items,
        "storage_path": str(storage_path),
        "is_rerun": True,
        "old_rows": old_rows,
        "dirty_row_nums": list(dirty_row_nums),
    }

    enqueued = False
    try:
        from rq import Queue as RQ
        import redis as rl
        q = RQ(NMC_QUEUE, connection=rl.Redis.from_url(REDIS_URL))
        q.enqueue("nmc_tasks.process_nmc_job", job_data, job_timeout=7200, result_ttl=3600)
        enqueued = True
        log.info("[Rerun] Job %s queued — %d dirty rows", new_job_id, dirty_count)
    except Exception as e:
        log.error("[Rerun] Enqueue failed, running directly: %s", e)
        asyncio.get_event_loop().create_task(_direct(new_job_id, job_data))

    return JSONResponse({
        "job_id": new_job_id,
        "status_url": f"/nmc/status/{new_job_id}",
        "rows": merged_rows,
        "queued": enqueued,
    })
