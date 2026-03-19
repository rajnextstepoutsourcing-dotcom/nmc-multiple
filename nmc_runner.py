"""
nmc_runner.py — NMC Register Check OPTIMISED VERSION
=====================================================
Optimisations applied:
- Cookie consent injected BEFORE page load (~50s saved per PIN)
- Screenshots only taken when process is affected (errors/failures)
- Reduced fixed waits throughout
- 2captcha polling unchanged (external dependency)

reCAPTCHA strategy (confirmed via findRecaptchaClients() on live site):
- sitekey : 6LcVjboUAAAAAEoj5nBYsQOfmr4IZ4nZlMPULDck
- callback: ___grecaptcha_cfg.clients['0']['G']['G']['callback']
- method  : userrecaptcha (token-based V2, NOT image grid clicking)

Flow:
1. Inject CookieConsent cookies before navigating (skip dialog entirely)
2. Submit sitekey + pageurl to 2captcha /in.php (method=userrecaptcha)
3. Poll /res.php until token is ready
4. Set #g-recaptcha-response textarea value
5. Call the callback function with the token
6. Wait for CAPTCHA to clear, then continue
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

log = logging.getLogger(__name__)

NMC_URL = "https://www.nmc.org.uk/registration/search-the-register/"
RECAPTCHA_SITEKEY = "6LcVjboUAAAAAEoj5nBYsQOfmr4IZ4nZlMPULDck"
TWOCAPTCHA_API_KEY = os.getenv("TWOCAPTCHA_API_KEY", "").strip()
MAX_RETRIES = 3


def _sanitize(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return re.sub(r'[\\/:\*?"<>|]', "", s)[:120].strip() or "NMC"


def _shot(page, out_dir: Path, prefix: str, shots: List[Path]) -> None:
    """Take a screenshot — only called when process is affected."""
    try:
        p = out_dir / f"{prefix}_{int(time.time())}.png"
        page.screenshot(path=str(p), full_page=True)
        shots.append(p)
        log.info("Screenshot: %s", p.name)
    except Exception as e:
        log.warning("Screenshot failed: %s", e)


def _body_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception:
        try:
            return page.inner_text("body")
        except Exception:
            return ""


# ─────────────────────────────────────────────────────────────────────────────
# COOKIE CONSENT — inject before page load (fast path)
# ─────────────────────────────────────────────────────────────────────────────

def _inject_cookies_before_load(context) -> None:
    """Inject CookieConsent cookies into browser context before navigating.
    This skips the cookie dialog entirely — PIN input will be enabled on load."""
    for domain in [".nmc.org.uk", "www.nmc.org.uk"]:
        context.add_cookies([
            {"name": "CookieConsent", "value": "true", "domain": domain, "path": "/"},
            {"name": "CookiebotDialogClosed", "value": "true", "domain": domain, "path": "/"},
        ])
    log.info("Cookie consent pre-injected")


def _ensure_pin_enabled(page, shots: List[Path], out_dir: Path) -> None:
    """Verify PIN input is enabled after page load. Fallback if pre-injection failed."""
    pin_loc = page.locator("#PinNumber").first
    pin_loc.wait_for(state="visible", timeout=25000)

    def pin_enabled() -> bool:
        try:
            cls = pin_loc.get_attribute("class") or ""
            dis = pin_loc.get_attribute("disabled")
            return dis is None and "cookies-only-disabled" not in cls
        except Exception:
            return False

    def wait_enabled(ms: int) -> bool:
        end = time.time() + ms / 1000
        while time.time() < end:
            if pin_enabled():
                return True
            time.sleep(0.3)
        return False

    # Fast check — cookie pre-injection usually means PIN is ready immediately
    if wait_enabled(3000):
        log.info("PIN enabled (cookie pre-injection worked)")
        return

    # Fallback 1: JS submit
    log.info("Cookie pre-injection didn't work, trying JS fallback...")
    try:
        page.evaluate("""() => {
            if (window.Cookiebot?.submitCustomConsent) Cookiebot.submitCustomConsent(true,true,true);
            else if (window.Cookiebot?.submitConsent) Cookiebot.submitConsent(true,true,true);
        }""")
    except Exception:
        pass

    if wait_enabled(5000):
        log.info("PIN enabled after JS submit")
        return

    # Fallback 2: Try clicking cookie button
    selectors = [
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "#CybotCookiebotDialogBodyButtonAccept",
        "button:has-text('Allow all')",
        "button:has-text('Accept all')",
        "button:has-text('Accept')",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=1000):
                loc.click(timeout=5000, force=True)
                log.info("Cookie clicked: %s", sel)
                if wait_enabled(5000):
                    log.info("PIN enabled after cookie click")
                    return
                break
        except Exception:
            continue

    # Fallback 3: Force-enable PIN via JS
    try:
        page.evaluate("""() => {
            const p = document.querySelector('#PinNumber');
            if (p) { p.classList.remove('cookies-only-disabled'); p.removeAttribute('disabled'); }
            document.querySelectorAll('[id^="CybotCookiebot"]').forEach(e => e.remove());
        }""")
        if wait_enabled(3000):
            log.info("PIN force-enabled")
            return
    except Exception:
        pass

    # Screenshot only if all fallbacks failed
    _shot(page, out_dir, "error_pin_disabled", shots)
    raise RuntimeError("PIN input still disabled after all cookie fallbacks")


# ─────────────────────────────────────────────────────────────────────────────
# CAPTCHA DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _detect_captcha(page) -> bool:
    for frame in page.frames:
        url = frame.url or ""
        if any(k in url for k in ("recaptcha/api2/anchor", "recaptcha/enterprise/anchor", "bframe")):
            log.info("reCAPTCHA detected via iframe: %s", url[:80])
            return True
    body = _body_text(page).lower()
    for indicator in ["recaptcha", "verify you are not a robot", "verify you are human", "i'm not a robot"]:
        if indicator in body:
            log.info("reCAPTCHA detected via body: '%s'", indicator)
            return True
    try:
        if page.locator(".g-recaptcha, [data-sitekey]").count() > 0:
            return True
    except Exception:
        pass
    return False


# ─────────────────────────────────────────────────────────────────────────────
# 2CAPTCHA — get token
# ─────────────────────────────────────────────────────────────────────────────

def _get_recaptcha_token(page_url: str, notes: List[str]) -> str:
    if not TWOCAPTCHA_API_KEY:
        raise RuntimeError("TWOCAPTCHA_API_KEY not set")

    log.info("Submitting to 2captcha sitekey=%s url=%s", RECAPTCHA_SITEKEY, page_url)
    params = {
        "key": TWOCAPTCHA_API_KEY,
        "method": "userrecaptcha",
        "googlekey": RECAPTCHA_SITEKEY,
        "pageurl": page_url,
        "json": "1",
    }
    encoded = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(
        "https://2captcha.com/in.php",
        data=encoded,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp = urllib.request.urlopen(req, timeout=30)
    result = json.loads(resp.read())
    if result.get("status") != 1:
        raise RuntimeError(f"2captcha submit error: {result}")

    task_id = result["request"]
    notes.append(f"2captcha task_id: {task_id}")
    log.info("task_id=%s polling...", task_id)

    poll_params = urllib.parse.urlencode({
        "key": TWOCAPTCHA_API_KEY,
        "action": "get",
        "id": task_id,
        "json": "1",
    }).encode()

    for attempt in range(40):
        time.sleep(5)
        try:
            poll_req = urllib.request.Request(
                "https://2captcha.com/res.php",
                data=poll_params,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            poll_resp = urllib.request.urlopen(poll_req, timeout=15)
            poll_result = json.loads(poll_resp.read())
            status = poll_result.get("status")
            request_val = poll_result.get("request", "")
            if status == 0 and request_val == "CAPCHA_NOT_READY":
                log.debug("attempt %d: not ready", attempt + 1)
                continue
            if status == 0:
                raise RuntimeError(f"2captcha error: {request_val}")
            if status == 1:
                log.info("Token received attempt %d len=%d", attempt + 1, len(request_val))
                notes.append(f"Token received len={len(request_val)}")
                return request_val
        except urllib.error.HTTPError as e:
            log.warning("Poll HTTP %s attempt %d", e.code, attempt + 1)
        except RuntimeError:
            raise
        except Exception as e:
            log.warning("Poll attempt %d error: %s", attempt + 1, e)

    raise RuntimeError("2captcha timed out after 200s")


# ─────────────────────────────────────────────────────────────────────────────
# TOKEN INJECTION
# ─────────────────────────────────────────────────────────────────────────────

def _inject_token(page, token: str, notes: List[str]) -> bool:
    log.info("Injecting token len=%d", len(token))

    try:
        page.evaluate(f"""() => {{
            document.querySelectorAll(
                'textarea[name="g-recaptcha-response"], #g-recaptcha-response'
            ).forEach(t => {{ t.style.display = 'block'; t.value = '{token}'; }});
        }}""")
    except Exception as e:
        log.warning("Textarea set failed: %s", e)

    # Primary: confirmed hardcoded callback path
    try:
        page.evaluate(f"""() => {{
            const cb = ___grecaptcha_cfg.clients['0']['G']['G']['callback'];
            if (typeof cb === 'function') cb('{token}');
        }}""")
        log.info("Called hardcoded callback ['0']['G']['G']['callback']")
        notes.append("Called callback ['0']['G']['G']['callback']")
        page.wait_for_timeout(1500)
        return True
    except Exception as e:
        log.warning("Hardcoded callback failed: %s", e)

    # Fallback: dynamic walk of ___grecaptcha_cfg
    try:
        result = page.evaluate(f"""() => {{
            if (typeof ___grecaptcha_cfg === 'undefined') return 'no_cfg';
            for (const cid of Object.keys(___grecaptcha_cfg.clients)) {{
                const client = ___grecaptcha_cfg.clients[cid];
                for (const topKey of Object.keys(client)) {{
                    const top = client[topKey];
                    if (!top || typeof top !== 'object') continue;
                    for (const subKey of Object.keys(top)) {{
                        const sub = top[subKey];
                        if (sub && typeof sub === 'object' && 'sitekey' in sub && 'callback' in sub) {{
                            const cb = sub['callback'];
                            if (typeof cb === 'function') {{
                                cb('{token}');
                                return 'called:' + cid + '/' + topKey + '/' + subKey;
                            }}
                        }}
                    }}
                }}
            }}
            return 'not_found';
        }}""")
        log.info("Dynamic callback result: %s", result)
        notes.append(f"Dynamic callback: {result}")
        if result and str(result).startswith("called:"):
            page.wait_for_timeout(1500)
            return True
    except Exception as e:
        log.warning("Dynamic callback failed: %s", e)

    # Last resort: dispatch events
    try:
        page.evaluate(f"""() => {{
            const t = document.querySelector('#g-recaptcha-response');
            if (t) {{
                t.value = '{token}';
                t.dispatchEvent(new Event('change', {{ bubbles: true }}));
                t.dispatchEvent(new Event('input', {{ bubbles: true }}));
            }}
        }}""")
        notes.append("Fallback: dispatched events on textarea")
        page.wait_for_timeout(1000)
        return True
    except Exception as e:
        log.warning("Event dispatch failed: %s", e)

    return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CAPTCHA HANDLER
# ─────────────────────────────────────────────────────────────────────────────

def _handle_captcha(page, out_dir: Path, shots: List[Path], notes: List[str]) -> bool:
    page.wait_for_timeout(1500)
    if not _detect_captcha(page):
        log.info("No CAPTCHA detected")
        return False

    notes.append("reCAPTCHA detected")
    # Screenshot when CAPTCHA is affecting the process
    _shot(page, out_dir, "captcha_detected", shots)

    try:
        token = _get_recaptcha_token(page.url, notes)
        injected = _inject_token(page, token, notes)
        if not injected:
            notes.append("Token injection failed")
            _shot(page, out_dir, "error_token_injection_failed", shots)
            return False

        page.wait_for_timeout(2500)

        if not _detect_captcha(page):
            notes.append("reCAPTCHA cleared")
            return True

        # Try clicking VERIFY if still showing
        for ctx in [page] + list(page.frames):
            try:
                btn = ctx.locator("button:has-text('VERIFY'), button:has-text('Verify')").first
                if btn.is_visible(timeout=1500):
                    btn.click(force=True)
                    notes.append("Clicked VERIFY")
                    page.wait_for_timeout(1500)
                    break
            except Exception:
                continue

        if not _detect_captcha(page):
            notes.append("reCAPTCHA cleared after VERIFY")
            return True

        notes.append("reCAPTCHA still present after injection")
        _shot(page, out_dir, "error_captcha_not_cleared", shots)
        return False

    except Exception as e:
        notes.append(f"CAPTCHA failed: {e}")
        log.error("CAPTCHA failed: %s", e)
        _shot(page, out_dir, "error_captcha_exception", shots)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SEARCH RE-SUBMIT
# ─────────────────────────────────────────────────────────────────────────────

def _click_search_again(page, notes: List[str]) -> bool:
    notes.append("Re-submitting search...")
    for sel in ["button[type='submit']", "button:has-text('Search')", "input[value='Search']"]:
        try:
            loc = page.locator(sel).first
            if loc.is_visible(timeout=3000):
                loc.scroll_into_view_if_needed()
                loc.click(force=True, timeout=10000)
                notes.append(f"Re-submitted via: {sel}")
                page.wait_for_timeout(1500)
                return True
        except Exception:
            continue

    try:
        pin = page.locator("#PinNumber").first
        if pin.is_visible():
            pin.press("Enter")
            notes.append("Re-submitted via Enter")
            page.wait_for_timeout(1500)
            return True
    except Exception:
        pass

    notes.append("Could not re-submit")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# RESULTS WAIT
# ─────────────────────────────────────────────────────────────────────────────

def _wait_for_results_state(page, timeout_ms: int, notes: List[str]) -> str:
    end = time.time() + timeout_ms / 1000.0
    last_state = "unknown"

    while time.time() < end:
        try:
            if page.get_by_role("link", name=re.compile(r"View\s+details", re.I)).count() > 0:
                notes.append("Found View details")
                return "success"
        except Exception:
            pass

        body = _body_text(page)
        body_l = body.lower()

        if "your search returned" in body_l and re.search(r"view\s+details", body, re.I):
            return "success"
        if "registration number" in body_l and "pin number" in body_l:
            return "success"
        if "practitioner details" in body_l or ("registered" in body_l and "expiry date" in body_l):
            return "success"
        if any(re.search(p, body, re.I) for p in [
            r"no results", r"no matching", r"not found", r"could not find", r"0 results",
        ]):
            notes.append("No results")
            return "no_result"

        if page.locator("#PinNumber").count() > 0 and "search the register" in body_l:
            last_state = "search_page"
            if time.time() > end - 10:
                _click_search_again(page, notes)

        if _detect_captcha(page):
            notes.append("CAPTCHA during wait")
            return "captcha"

        page.wait_for_timeout(1000)

    notes.append(f"Timeout. Last: {last_state}")
    return last_state or "timeout"


# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT DETAILS
# ─────────────────────────────────────────────────────────────────────────────

def _extract_details(page) -> Dict[str, str]:
    try:
        page.get_by_text(re.compile(r"Practitioner\s+Details", re.I)).first.wait_for(timeout=20000)
    except Exception:
        pass

    try:
        dialog = page.locator("div[role='dialog']").first
        text = dialog.inner_text() if dialog.is_visible(timeout=2000) else page.inner_text("body")
    except Exception:
        text = page.inner_text("body")

    log.info("Modal text: %s", text[:300])

    name = ""
    m = re.search(r"\bName\b\s*[:\n]\s*([A-Za-z][A-Za-z .,'\-]{1,80})", text)
    if m:
        name = m.group(1).strip()

    expiry_date = ""
    m2 = re.search(r"Expiry\s+date\s*[:\n]?\s*(\d{2}/\d{2}/\d{4})", text, re.I)
    if m2:
        expiry_date = m2.group(1).strip()

    status_text = ""
    m3 = re.search(r"Registered\s*[-–]\s*[^\n]+", text, re.I)
    if m3:
        status_text = m3.group(0).strip()
    elif "suspended" in text.lower():
        status_text = "Suspended"
    elif "removed" in text.lower():
        status_text = "Removed"

    log.info("name=%r expiry=%r status=%r", name, expiry_date, status_text)
    return {"name": name or "NMC", "expiry_date": expiry_date, "status_text": status_text}


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE ATTEMPT
# ─────────────────────────────────────────────────────────────────────────────

def _run_single_attempt_sync(pin: str, out_dir: Path, shots: List[Path], notes: List[str]) -> Dict[str, Any]:
    log.info("=== NMC check PIN=%s ===", pin)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1365, "height": 768},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )

        # ── Inject cookies BEFORE loading page (skips cookie dialog) ──
        _inject_cookies_before_load(context)

        page = context.new_page()

        try:
            page.goto(NMC_URL, wait_until="domcontentloaded", timeout=90000)

            # Ensure PIN is enabled (fast path since cookies pre-injected)
            _ensure_pin_enabled(page, shots, out_dir)

            pin_input = page.locator("#PinNumber").first
            pin_input.scroll_into_view_if_needed(timeout=8000)
            pin_input.click(timeout=20000, force=True)
            try:
                pin_input.press("Control+A")
            except Exception:
                pass
            pin_input.type(pin, delay=50)

            val = pin_input.input_value(timeout=2000)
            if (val or "").strip().upper() != pin:
                pin_input.click(force=True)
                pin_input.press("Control+A")
                pin_input.type(pin, delay=60)

            notes.append(f"PIN readback: '{pin_input.input_value(timeout=2000)}'")

            search_btn = page.get_by_role("button", name=re.compile(r"^Search$", re.I)).first
            search_btn.scroll_into_view_if_needed(timeout=8000)
            search_btn.wait_for(state="visible", timeout=25000)
            search_btn.click(timeout=25000, force=True)
            page.wait_for_timeout(1000)

            captcha_handled = _handle_captcha(page, out_dir, shots, notes)
            if captcha_handled:
                notes.append("CAPTCHA handled")
                page.wait_for_timeout(2000)

            state = _wait_for_results_state(page, timeout_ms=45000, notes=notes)
            if state == "captcha":
                notes.append("CAPTCHA reappeared")
                if _handle_captcha(page, out_dir, shots, notes):
                    state = _wait_for_results_state(page, timeout_ms=30000, notes=notes)

            if state != "success":
                # Screenshot only when results page not reached
                _shot(page, out_dir, "error_results_not_reached", shots)
                raise RuntimeError(f"Result page not reached. State: {state}")

            view_link = page.get_by_role("link", name=re.compile(r"View\s+details", re.I)).first
            view_link.scroll_into_view_if_needed(timeout=8000)
            view_link.click(timeout=25000)
            page.wait_for_timeout(800)

            details = _extract_details(page)
            name = details["name"]
            expiry_date = details["expiry_date"]
            status_text = details["status_text"]

            from datetime import datetime
            checked_at = datetime.now().strftime("%d/%m/%Y %H:%M")

            # ── PDF GENERATION ───────────────────────────────────────────────
            # 1. Get href from "Print this page" link inside the modal
            # 2. Navigate to that clean registrant URL on the same page
            # 3. emulate_media("print") — applies @media print CSS
            # 4. page.pdf() — clean output with browser native header/footer

            log.info("Getting 'Print this page' href...")
            try:
                print_href = page.locator("a:has-text('Print this page')").first.get_attribute("href", timeout=10000)
                if not print_href:
                    raise RuntimeError("Print this page href is empty")
                if print_href.startswith("/"):
                    print_href = "https://www.nmc.org.uk" + print_href
                log.info("Print href: %s", print_href)
                notes.append(f"Print href: {print_href}")
            except Exception as e:
                _shot(page, out_dir, "error_print_href_failed", shots)
                raise RuntimeError(f"Could not get Print this page href: {e}")

            log.info("Navigating to registrant page...")
            page.goto(print_href, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_load_state("networkidle", timeout=30000)
            page.wait_for_timeout(1000)
            log.info("Registrant page loaded: %s", page.url)
            notes.append(f"Registrant page: {page.url}")

            # Switch to print CSS mode
            page.emulate_media(media="print")
            page.wait_for_timeout(400)

            out_pdf = out_dir / f"{_sanitize(name)} nmc check.pdf"

            pdf_bytes = page.pdf(
                display_header_footer=True,
                print_background=False,
                format="A4",
                margin={"top": "45px", "bottom": "45px", "left": "20px", "right": "20px"},
            )

            out_pdf.write_bytes(pdf_bytes)
            log.info("PDF: %s (%d bytes)", out_pdf.name, len(pdf_bytes))

            browser.close()

            if out_pdf.exists() and out_pdf.stat().st_size > 2000:
                return {
                    "ok": True, "pdf_path": str(out_pdf),
                    "name": name, "expiry_date": expiry_date,
                    "status_text": status_text, "stage": "done",
                }

            raise RuntimeError("PDF missing or too small")

        except Exception as e:
            log.error("Attempt error: %s", e)
            try:
                browser.close()
            except Exception:
                pass
            raise


# ─────────────────────────────────────────────────────────────────────────────
# SNAPSHOT PDF
# ─────────────────────────────────────────────────────────────────────────────

def _make_snapshot_pdf(out_path: Path, *, stage: str, notes: List[str], image_paths: List[Path]) -> None:
    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.utils import ImageReader

        def wrap(text: str, width: int) -> List[str]:
            words = (text or "").split()
            lines, cur = [], []
            for w in words:
                if sum(len(x) for x in cur) + len(cur) + len(w) > width:
                    lines.append(" ".join(cur))
                    cur = [w]
                else:
                    cur.append(w)
            if cur:
                lines.append(" ".join(cur))
            return lines or [""]

        out_path.parent.mkdir(parents=True, exist_ok=True)
        c = rl_canvas.Canvas(str(out_path), pagesize=A4)
        pw, ph = A4
        y = ph - 72

        c.setFont("Helvetica-Bold", 16)
        c.drawString(72, y, "NMC automation snapshot")
        y -= 28
        c.setFont("Helvetica", 10)
        c.drawString(72, y, f"Stage: {stage}")
        y -= 18

        for line in notes[:80]:
            for wrapped in wrap(line, 95):
                if y < 72:
                    c.showPage(); y = ph - 72; c.setFont("Helvetica", 10)
                c.drawString(72, y, wrapped)
                y -= 12

        c.showPage()

        for p in image_paths:
            try:
                img = ImageReader(str(p))
                iw, ih = img.getSize()
                margin = 36
                scale = min((pw - 2 * margin) / iw, (ph - 2 * margin) / ih)
                dw, dh = iw * scale, ih * scale
                c.drawImage(img, (pw - dw) / 2, (ph - dh) / 2,
                            width=dw, height=dh, preserveAspectRatio=True, mask="auto")
                c.showPage()
            except Exception:
                pass

        c.save()

    except Exception as e:
        log.error("Snapshot PDF failed: %s", e)
        from pdf_utils import make_simple_error_pdf
        make_simple_error_pdf(out_path, "NMC check failed", notes[:10])


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def run_nmc_check_sync(nmc_pin: str, out_dir: str) -> Dict[str, Any]:
    from pdf_utils import make_simple_error_pdf

    out_dir_path = Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    pin = (nmc_pin or "").strip().upper()
    if not pin:
        out = out_dir_path / "NMC-Error-Missing-PIN.pdf"
        make_simple_error_pdf(out, "NMC check failed", ["Missing NMC PIN."])
        return {"ok": False, "pdf_path": str(out), "stage": "missing_pin", "error": "Missing PIN"}

    all_notes: List[str] = []
    all_shots: List[Path] = []
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        notes: List[str] = [f"Attempt {attempt}/{MAX_RETRIES}"]
        shots: List[Path] = []
        try:
            result = _run_single_attempt_sync(pin, out_dir_path, shots, notes)
            all_notes.extend(notes)
            all_shots.extend(shots)
            return result
        except Exception as e:
            last_error = e
            all_notes.extend(notes)
            all_notes.append(f"Attempt {attempt} failed: {type(e).__name__}: {e}")
            all_shots.extend(shots)
            log.warning("Attempt %d failed: %s", attempt, e)
            if attempt < MAX_RETRIES:
                all_notes.append("Retrying in 3s...")
                time.sleep(3)

    snap = out_dir_path / f"NMC-Snapshot-{int(time.time())}.pdf"
    try:
        _make_snapshot_pdf(
            snap,
            stage=f"failed_after_{MAX_RETRIES}_attempts",
            notes=all_notes + [f"Final error: {last_error}"],
            image_paths=all_shots,
        )
    except Exception:
        from pdf_utils import make_simple_error_pdf
        make_simple_error_pdf(snap, "NMC check failed",
                              [f"All {MAX_RETRIES} attempts failed.", str(last_error)])

    return {
        "ok": False, "pdf_path": str(snap), "stage": "failed",
        "error": str(last_error) if last_error else "Unknown error",
        "name": "", "expiry_date": "", "status_text": "",
    }
