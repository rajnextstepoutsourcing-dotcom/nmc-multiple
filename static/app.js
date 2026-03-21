/* NMC Register Check — app.js (NextStep SaaS) */
"use strict";

// ── Helpers ───────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
function setText(id, msg) { const el=$(id); if(el) el.textContent = msg||""; }
function setDisabled(id, v) { const el=$(id); if(el) el.disabled = !!v; }
function escapeHtml(s) {
  return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;")
    .replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;");
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function toast(msg, pos) {
  pos = pos||"top-right";
  const id = pos==="top-right"?"toastTopRight":"toastBottomRight";
  let host = document.getElementById(id);
  if (!host) { host=document.createElement("div"); host.id=id; host.className=`toastHost ${pos}`; document.body.appendChild(host); }
  const t = document.createElement("div"); t.className="toast"; t.textContent=msg; host.appendChild(t);
  setTimeout(()=>t.classList.add("show"),10);
  setTimeout(()=>{t.classList.remove("show");t.classList.add("hide");},3500);
  setTimeout(()=>{try{t.remove();}catch(e){}},4200);
}

// ── State ─────────────────────────────────────────────────────────────────────
let bulkFiles  = [];
let lastJobId  = null;
let pollTimer  = null;

function stopPolling() { if(pollTimer){clearTimeout(pollTimer);pollTimer=null;} }

function updateBulkCount() {
  setText("bulkCount", `${bulkFiles.length}/100 files selected`);
  const rows = document.querySelectorAll("#bulkList .bulkRow").length;
  setText("bulkRowCount", rows ? `${rows}/100 rows ready` : "");
}

// ── File chips ────────────────────────────────────────────────────────────────
function renderChips() {
  const wrap = $("bulkChips"); if(!wrap) return; wrap.innerHTML="";
  bulkFiles.forEach((f,idx)=>{
    const chip=document.createElement("span"); chip.className="chipFile";
    chip.innerHTML=`<span title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</span>`;
    const btn=document.createElement("button"); btn.type="button"; btn.title="Remove"; btn.textContent="×";
    btn.addEventListener("click",()=>{ bulkFiles.splice(idx,1); renderChips(); updateBulkCount(); });
    chip.appendChild(btn); wrap.appendChild(chip);
  });
}

function isSupportedFile(f) {
  const n=(f?.name||"").toLowerCase();
  return n.endsWith(".pdf")||n.endsWith(".png")||n.endsWith(".jpg")||
         n.endsWith(".jpeg")||n.endsWith(".webp")||n.endsWith(".csv")||n.endsWith(".xlsx");
}

function appendFiles(files) {
  let rejected=0;
  Array.from(files||[]).forEach(f=>{
    if(!isSupportedFile(f)){rejected++;return;}
    if(bulkFiles.length>=100) return;
    bulkFiles.push(f);
  });
  renderChips(); updateBulkCount();
  if(rejected) setText("extractBulkStatus",`Skipped ${rejected} unsupported file(s).`);
}

// ── Drop zone ─────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded",()=>{
  const dz=$("dropZone"); const inp=$("files");
  if(dz&&inp){
    dz.addEventListener("click",()=>inp.click());
    dz.addEventListener("dragover",e=>{e.preventDefault();dz.classList.add("dragover");});
    dz.addEventListener("dragleave",()=>dz.classList.remove("dragover"));
    dz.addEventListener("drop",e=>{e.preventDefault();dz.classList.remove("dragover");appendFiles(e.dataTransfer.files);});
  }
  inp?.addEventListener("change",e=>{appendFiles(e.target.files);e.target.value="";});
  $("btnAddMore")?.addEventListener("click",()=>$("files")?.click());
  $("btnClearAll")?.addEventListener("click",()=>{
    bulkFiles=[]; renderChips(); updateBulkCount();
    const list=$("bulkList"); if(list) list.innerHTML="";
    setText("extractBulkStatus",""); setText("runBulkStatus",""); setText("zipNotice","");
    _resetZipBtn(); setDisabled("btnDlResultsXlsx",true); setDisabled("btnDlResultsCsv",true);
    _hideProgress(); _hideRerun();
    lastJobId=null; stopPolling();
  });
  updateBulkCount(); setDisabled("btnDlResultsXlsx",true); setDisabled("btnDlResultsCsv",true);
});

// ── Badge ─────────────────────────────────────────────────────────────────────
function buildBadge(status) {
  const span=document.createElement("span");
  const labels={queued:"Queued",running:"Running…",done:"Done",failed:"Failed"};
  const classes={queued:"badge",running:"badge running",done:"badge clear",failed:"badge portal_unavailable"};
  span.className=classes[status]||"badge"; span.textContent=labels[status]||status; return span;
}

// ── Render bulk table ─────────────────────────────────────────────────────────
function renderBulkTable(items) {
  const list=$("bulkList"); if(!list) return; list.innerHTML="";
  (items||[]).forEach((it,idx)=>{
    const row=document.createElement("div");
    row.className="bulkRow";
    row.dataset.row=String(idx+1);
    row.dataset.originalFilename=it.original_filename||"";
    row.dataset.nmcPin=it.nmc_pin||"";
    row.dataset.dirty="false"; // track edits
    row.innerHTML=`
      <div class="bulkRowTop">
        <div class="bulkRowLeft">
          <button type="button" class="iconBtn btnRemoveRow" data-idx="${idx}" title="Remove">
            <svg viewBox="0 0 24 24" width="16" height="16"><path fill="currentColor" d="M9 3h6l1 2h4v2H4V5h4l1-2zm1 6h2v9h-2V9zm4 0h2v9h-2V9zM7 9h2v9H7V9z"/></svg>
          </button>
          <div class="bulkIndex">#${idx+1}</div>
        </div>
        <div class="bulkFields">
          <div class="fieldBlock">
            <div class="fieldLabel">Source</div>
            <div class="bulkSource" title="${escapeHtml(it.original_filename||"")}">${escapeHtml(it.original_filename||"")}</div>
          </div>
          <div class="fieldBlock">
            <div class="fieldLabel">NMC PIN</div>
            <input class="cell pin" value="${escapeHtml(it.nmc_pin||"")}" placeholder="e.g. 23B0365O">
          </div>
          <div class="fieldBlock">
            <div class="fieldLabel">Name</div>
            <div class="nameCell muted small">—</div>
          </div>
          <div class="fieldBlock">
            <div class="fieldLabel">Expiry Date</div>
            <div class="expiryCell muted small">—</div>
          </div>
        </div>
        <div class="bulkActions">
          <div class="statusWrap"><span class="statusCell"></span></div>
          <div class="dlWrap"><span class="dlCell"></span></div>
        </div>
      </div>`;
    // Mark dirty when PIN edited
    const pinInput = row.querySelector("input.pin");
    pinInput?.addEventListener("input", ()=>{
      row.dataset.dirty="true";
      row.dataset.nmcPin=pinInput.value.trim().toUpperCase();
      _showRerunIfNeeded();
    });
    list.appendChild(row);
  });
  updateBulkCount();
}

// ── Remove row ────────────────────────────────────────────────────────────────
document.addEventListener("click",e=>{
  const btn=e.target.closest?.(".btnRemoveRow"); if(!btn) return;
  const row=btn.closest(".bulkRow"); if(!row) return;
  const removedName=(row.dataset.originalFilename||"").trim(); row.remove();
  if(removedName){ const idx=bulkFiles.findIndex(f=>(f?.name||"")===removedName); if(idx>=0){bulkFiles.splice(idx,1);renderChips();} }
  Array.from(document.querySelectorAll("#bulkList .bulkRow")).forEach((r,i)=>{
    r.dataset.row=String(i+1);
    const idxEl=r.querySelector(".bulkIndex"); if(idxEl) idxEl.textContent=`#${i+1}`;
  });
  updateBulkCount();
});

// ── Extract ───────────────────────────────────────────────────────────────────
$("btnExtractBulk")?.addEventListener("click",async()=>{
  if(!bulkFiles.length){ setText("extractBulkStatus","Please add files first."); return; }
  setText("extractBulkStatus","Extracting…"); setDisabled("btnExtractBulk",true);
  const fd=new FormData();
  bulkFiles.slice(0,100).forEach(f=>fd.append("files",f));
  try {
    const resp=await fetch("/nmc/extract",{method:"POST",body:fd});
    const data=await resp.json();
    if(!resp.ok) throw new Error(data?.detail||"Extraction failed");
    renderBulkTable(data.items||[]);
    updateBulkCount();
    setText("extractBulkStatus",`Done — ${(data.items||[]).length} PIN(s) extracted.`);
    toast("Extraction complete","top-right");
  } catch(err){ setText("extractBulkStatus",err?.message||"Extraction failed."); }
  finally{ setDisabled("btnExtractBulk",false); }
});

// ── Collect items ─────────────────────────────────────────────────────────────
function collectItems(dirtyOnly=false) {
  return Array.from(document.querySelectorAll("#bulkList .bulkRow"))
    .filter(row => !dirtyOnly || row.dataset.dirty==="true")
    .map(row=>({
      nmc_pin: (row.querySelector("input.pin")?.value||"").trim().toUpperCase(),
      original_filename: row.dataset.originalFilename||"",
      dirty: row.dataset.dirty==="true",
      row_number: parseInt(row.dataset.row||"0",10),
    }));
}

// ── Progress bar ──────────────────────────────────────────────────────────────
function _showProgress(done,total,state,msg){
  const wrap=$("progressWrap"); if(!wrap) return;
  wrap.classList.remove("hidden");
  setText("progressCount",`${done} / ${total} completed`);
  const pct=total>0?Math.round((done/total)*100):0;
  const bar=$("progressBar"); if(bar) bar.style.width=pct+"%";
  const pmsg=$("progressMsg"); if(pmsg&&msg) pmsg.textContent=msg;
  const badge=$("progressBadge"); if(!badge) return;
  if(state==="queued"){badge.className="badge queued";badge.textContent="⏳ Queued";}
  else if(state==="running"){badge.className="badge running";badge.textContent="⚙️ Processing";}
  else if(state==="done"){badge.className="badge done";badge.textContent="✓ Complete";}
  else{badge.className="badge failed-all";badge.textContent="✗ Failed";}
}

function _hideProgress(){ const w=$("progressWrap"); if(w) w.classList.add("hidden"); }

// ── ZIP button helpers ────────────────────────────────────────────────────────
function _resetZipBtn(){
  const z=$("btnDlZip"); if(!z) return;
  z.classList.add("disabledLink"); z.removeAttribute("href"); z.download="";
  z.textContent="⬇ Download All PDFs (ZIP)";
}

function _enableZipBtn(url, name){
  const z=$("btnDlZip"); if(!z) return;
  z.href=url; z.download=name||"NMC_Checks.zip";
  z.classList.remove("disabledLink");
  z.textContent="⬇ Download All PDFs (ZIP)";
}

// ── Rerun button ──────────────────────────────────────────────────────────────
function _showRerunIfNeeded(){
  const hasDirty = Array.from(document.querySelectorAll("#bulkList .bulkRow"))
    .some(r=>r.dataset.dirty==="true");
  const btn=$("btnRerun");
  if(!btn) return;
  if(hasDirty && lastJobId){ btn.classList.remove("hidden"); }
  else{ btn.classList.add("hidden"); }
}

function _hideRerun(){ const b=$("btnRerun"); if(b) b.classList.add("hidden"); }

function _clearDirtyFlags(){
  document.querySelectorAll("#bulkList .bulkRow").forEach(r=>r.dataset.dirty="false");
}

// ── Update UI from poll ───────────────────────────────────────────────────────
function updateBulkUIFromStatus(data, isRerun=false){
  const running=data.running||{};
  const done=running.done||0; const total=running.total||0;
  const activeCount=data.running_count||0;
  const queuedCount=data.queued_count||0;
  _showProgress(done,total,data.state||"running",data.message||"");
  if(total) setText("runBulkStatus",`${done}/${total} completed • ${activeCount} running • ${queuedCount} queued`);

  (data.rows||[]).forEach((r,idx)=>{
    // For rerun: find row by row_number, not index
    const rowNum = r.row||idx+1;
    const tr=document.querySelector(`#bulkList .bulkRow[data-row="${rowNum}"]`);
    if(!tr) return;

    // Skip non-dirty rows during rerun UI update
    if(isRerun && tr.dataset.dirty==="false" && r.status!=="queued") return;

    const st=(r.status||"").trim();
    const statusCell=tr.querySelector(".statusCell");
    const dlCell=tr.querySelector(".dlCell");
    const nameCell=tr.querySelector(".nameCell");
    const expiryCell=tr.querySelector(".expiryCell");

    if(r.name&&nameCell){ nameCell.textContent=r.name; nameCell.classList.remove("muted","small"); }
    if(r.expiry_date&&expiryCell){ expiryCell.textContent=r.expiry_date; expiryCell.classList.remove("muted","small"); }

    if(statusCell){
      statusCell.innerHTML=""; statusCell.appendChild(buildBadge(st));
      if(st==="running"){ const bar=document.createElement("div"); bar.className="miniProgress"; bar.textContent="██████░░░░"; statusCell.appendChild(bar); }
      if(st==="failed"&&r.error){ const e=document.createElement("div"); e.className="small"; e.style.color="rgba(239,68,68,.9)"; e.style.marginTop="4px"; e.textContent=r.error; statusCell.appendChild(e); }
    }

    if(!dlCell) return;
    if(st==="running"||st==="queued"){ dlCell.innerHTML=`<div class="miniSpinner"></div>`; return; }
    if(r.pdf_url){
      const label=st==="failed"?"⬇ Error Log":"⬇ Download PDF";
      const style=st==="failed"?`style="border-color:rgba(239,68,68,.35);"`:"";
      dlCell.innerHTML=`<a class="btnSmall downloadBtn" href="${escapeHtml(r.pdf_url)}" ${style}>${label}</a>`;
    } else { dlCell.innerHTML=""; }
  });

  // ZIP button — always use latest state from server
  setText("zipNotice",data.message||"");
  if(data.zip_ready&&data.zip_url){ _enableZipBtn(data.zip_url,data.zip_name); }
  // Do NOT reset ZIP if not ready — keeps previous run's zip visible

  const anyDone=(data.rows||[]).some(r=>r.status&&r.status!=="queued"&&r.status!=="running");
  setDisabled("btnDlResultsXlsx",!anyDone);
  setDisabled("btnDlResultsCsv",!anyDone);

  if(data.state==="done"){
    stopPolling();
    setDisabled("btnRunBulk",false);
    const runBtn=$("btnRunBulk");
    if(runBtn){ runBtn.classList.remove("isRunning"); runBtn.textContent="Run All Checks"; }
    setText("runBulkStatus",`Completed ${done}/${total}`);
    toast(`Run complete (${done}/${total})`,"bottom-right");
    _showRerunIfNeeded();
  }
}

// ── Poll ──────────────────────────────────────────────────────────────────────
function startPolling(jobId, isRerun=false){
  stopPolling();
  pollTimer=setTimeout(async function poll(){
    try{
      const r=await fetch(`/nmc/status/${jobId}`);
      const st=await r.json();
      if(!r.ok) throw new Error(st?.detail||"Status failed");
      updateBulkUIFromStatus(st, isRerun);
      if(st.state!=="done") pollTimer=setTimeout(poll,2000);
    } catch(e){
      setText("runBulkStatus","Updating…");
      pollTimer=setTimeout(poll,3000);
    }
  },1500);
}

// ── Run ───────────────────────────────────────────────────────────────────────
$("btnRunBulk")?.addEventListener("click",async()=>{
  stopPolling();
  // Reset ZIP button for new run
  _resetZipBtn();
  _hideRerun();
  _clearDirtyFlags();
  setText("runBulkStatus",""); setDisabled("btnRunBulk",true);
  const runBtn=$("btnRunBulk");
  if(runBtn){ runBtn.classList.add("isRunning"); runBtn.textContent="Running…"; }

  const items=collectItems();
  if(!items.length||!items.some(it=>it.nmc_pin)){
    setText("runBulkStatus","No NMC PINs found. Please extract first.");
    setDisabled("btnRunBulk",false);
    if(runBtn){ runBtn.classList.remove("isRunning"); runBtn.textContent="Run All Checks"; }
    return;
  }

  try{
    const resp=await fetch("/nmc/run",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({items})});
    const data=await resp.json();
    if(!resp.ok) throw new Error(data?.detail||"Run failed");
    lastJobId=data.job_id;
    updateBulkUIFromStatus({rows:data.rows||[],running:{done:0,total:(data.rows||[]).length},state:"queued",message:"Job queued…"});
    startPolling(lastJobId, false);
  } catch(err){
    setText("runBulkStatus",err?.message||"Run failed.");
    setDisabled("btnRunBulk",false);
    if(runBtn){ runBtn.classList.remove("isRunning"); runBtn.textContent="Run All Checks"; }
  }
});

// ── Rerun (dirty rows only) ───────────────────────────────────────────────────
document.addEventListener("click",async e=>{
  const btn=e.target.closest?.("#btnRerun"); if(!btn) return;
  if(!lastJobId){ toast("Please run a check first.","top-right"); return; }

  const dirtyItems=collectItems(true);
  if(!dirtyItems.length){ toast("No edited rows to rerun.","top-right"); return; }
  if(!dirtyItems.some(it=>it.nmc_pin)){ toast("Edited rows have no PINs.","top-right"); return; }

  stopPolling();
  btn.classList.add("hidden");
  setText("runBulkStatus","Rerunning edited rows…");

  // Mark dirty rows as queued in UI
  dirtyItems.forEach(it=>{
    const row=document.querySelector(`#bulkList .bulkRow[data-row="${it.row_number}"]`);
    if(!row) return;
    const sc=row.querySelector(".statusCell"); if(sc){ sc.innerHTML=""; sc.appendChild(buildBadge("queued")); }
    const dc=row.querySelector(".dlCell"); if(dc) dc.innerHTML=`<div class="miniSpinner"></div>`;
  });

  try{
    const resp=await fetch("/nmc/rerun",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({job_id:lastJobId, items:dirtyItems})
    });
    const data=await resp.json();
    if(!resp.ok) throw new Error(data?.detail||"Rerun failed");
    // Rerun creates a new job_id — update and poll
    lastJobId=data.job_id;
    startPolling(lastJobId, true);
  } catch(err){
    setText("runBulkStatus",err?.message||"Rerun failed.");
    _showRerunIfNeeded();
  }
});

// ── Excel / CSV export ────────────────────────────────────────────────────────
$("btnDlResultsXlsx")?.addEventListener("click",()=>{ if(lastJobId) window.location.href=`/nmc/export/excel/${lastJobId}`; });
$("btnDlResultsCsv")?.addEventListener("click",()=>{ if(lastJobId) window.location.href=`/nmc/export/csv/${lastJobId}`; });
