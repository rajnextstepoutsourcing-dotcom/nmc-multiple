# NMC Register Check — v2

Automated NMC register checker with 2captcha integration, bulk mode, and job tracking.

## Features
- Upload PDF, JPG, PNG, WEBP, CSV, XLSX — auto-extracts NMC PIN
- Single and bulk check modes (up to 100 at once)
- reCAPTCHA v2 solved automatically via 2captcha
- Retry logic (3 attempts per PIN)
- Progress tracking with per-row status badges
- ZIP download for bulk results
- Session management — one job at a time per user
- Auto cleanup after 15 minutes

## Environment Variables (set in Render dashboard)

| Variable | Description |
|---|---|
| `TWOCAPTCHA_API_KEY` | Your 2captcha API key (required for CAPTCHA solving) |
| `GEMINI_API_KEY` | Google Gemini API key (for scanned PDF/image PIN extraction) |
| `GEMINI_MODEL_FAST` | Fast model (default: gemini-2.0-flash) |
| `GEMINI_MODEL_STRONG` | Fallback model (default: gemini-2.5-pro) |

## Deploy to Render
1. Push this repo to GitHub
2. Create a new Web Service on Render pointing to this repo
3. Render will auto-detect `render.yaml`
4. Add environment variables in Render dashboard
5. Deploy

## Local Development
```bash
pip install -r requirements.txt
playwright install chromium
uvicorn app:app --reload
```
