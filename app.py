import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from worker import (
    load_state, save_state, find_pipeline, update_pipeline,
    start_pipeline, stop_pipeline, is_running,
    upcoming_slots, ensure_slots_for_today, reroll_slots_today,
    daily_reroll_loop, buffer_headers, get_buffer_key,
    WINDOW_START, WINDOW_END, POSTS_PER_DAY, MIN_GAP_MIN,
)

BUFFER_SERVICE = os.getenv("BUFFER_SERVICE", "http://127.0.0.1:3000")


# ------------------------------------------------------------------
# Lifespan
# ------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_slots_for_today()
    task = asyncio.create_task(daily_reroll_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="tikpost", lifespan=lifespan)


# ------------------------------------------------------------------
# CORS — allow the Vercel frontend + local dev to call this API
# ------------------------------------------------------------------
_origins_env = os.getenv("CORS_ORIGINS", "").strip()
if _origins_env:
    allow_origins = [o.strip() for o in _origins_env.split(",") if o.strip()]
else:
    # Sensible defaults for dev + your Vercel deploy
    allow_origins = [
        "https://tikpost-murex.vercel.app",
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",   # any *.vercel.app preview
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


TEMPLATES = Path(__file__).parent / "templates"


# ------------------------------------------------------------------
# Models
# ------------------------------------------------------------------
class PipelineInput(BaseModel):
    name: str
    channel_id: str
    tweet_template: str
    urls: list[str]


class BufferKeyInput(BaseModel):
    api_key: str


# ------------------------------------------------------------------
# UI (served locally; Vercel serves its own copy)
# ------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index():
    idx = TEMPLATES / "index.html"
    if idx.exists():
        return idx.read_text(encoding="utf-8")
    return HTMLResponse(
        "<h1>tikpost API</h1>"
        "<p>Frontend is deployed on Vercel. This is the API backend.</p>"
    )


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------
@app.get("/healthz")
async def healthz():
    return {"ok": True}


# ------------------------------------------------------------------
# Status
# ------------------------------------------------------------------
@app.get("/api/status")
async def status():
    s = load_state()
    slots = ensure_slots_for_today()
    out = []
    for p in s["pipelines"]:
        out.append({
            **p,
            "running": is_running(p["id"]),
            "total": len(p.get("urls") or []),
        })
    return {
        "pipelines": out,
        "window": {
            "start": f"{WINDOW_START[0]:02d}:{WINDOW_START[1]:02d}",
            "end":   f"{WINDOW_END[0]:02d}:{WINDOW_END[1]:02d}",
            "posts_per_day": POSTS_PER_DAY,
            "min_gap_min": MIN_GAP_MIN,
        },
        "slots_today": {
            "day": slots["day"],
            "times": slots["times"],
            "rolled_at": slots.get("rolled_at"),
        },
    }


# ------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------
@app.get("/api/settings")
async def get_settings():
    key = get_buffer_key()
    return {
        "has_key": bool(key),
        "preview": (key[:6] + "…" + key[-4:]) if len(key) > 12 else ("set" if key else ""),
    }


@app.post("/api/settings/buffer-key")
async def set_buffer_key(data: BufferKeyInput):
    key = data.api_key.strip()
    if not key:
        return JSONResponse({"ok": False, "error": "empty key"}, status_code=400)

    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                f"{BUFFER_SERVICE}/api/key-check",
                headers={"X-Buffer-Key": key},
            )
        body = r.json()
        if not body.get("ok"):
            return JSONResponse(
                {"ok": False, "error": body.get("error", "invalid key")},
                status_code=400,
            )
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    s = load_state()
    s["buffer_api_key"] = key
    save_state(s)
    return {"ok": True, "email": body.get("email")}


@app.delete("/api/settings/buffer-key")
async def clear_buffer_key():
    s = load_state()
    s.pop("buffer_api_key", None)
    save_state(s)
    return {"ok": True}


# ------------------------------------------------------------------
# Slots
# ------------------------------------------------------------------
@app.get("/api/slots")
async def slots(count: int = 10):
    return {"slots": upcoming_slots(count)}


@app.get("/api/slots/today")
async def slots_today():
    from worker import today_slots_utc, utc_iso, nairobi_iso, fmt_12h
    from zoneinfo import ZoneInfo
    nai = ZoneInfo("Africa/Nairobi")
    out = []
    for s in today_slots_utc():
        out.append({
            "utc": utc_iso(s),
            "nairobi": nairobi_iso(s),
            "time12": fmt_12h(s),
            "label": s.astimezone(nai).strftime("%a %d %b") + " · " + fmt_12h(s),
        })
    return {"slots": out}


@app.post("/api/slots/reroll")
async def slots_reroll():
    s = reroll_slots_today()
    return {"ok": True, "slots": s}


# ------------------------------------------------------------------
# Pipelines CRUD
# ------------------------------------------------------------------
@app.post("/api/pipelines")
async def create_pipeline(data: PipelineInput):
    s = load_state()
    pid = str(uuid.uuid4())
    p = {
        "id": pid,
        "name": data.name.strip() or "Untitled pipeline",
        "channel_id": data.channel_id,
        "tweet_template": data.tweet_template,
        "urls": [u.strip() for u in data.urls if u.strip()],
        "cursor": 0,
        "posted_count": 0,
        "failed_count": 0,
        "status": "idle",
        "next_slot": None,
        "next_slot_nairobi": None,
        "current": None,
        "upcoming_slots": [],
        "log": [],
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    s["pipelines"].append(p)
    save_state(s)
    return {"ok": True, "pipeline": p}


@app.put("/api/pipelines/{pid}")
async def update_pipeline_route(pid: str, data: PipelineInput):
    p = update_pipeline(
        pid,
        name=data.name.strip() or "Untitled pipeline",
        channel_id=data.channel_id,
        tweet_template=data.tweet_template,
        urls=[u.strip() for u in data.urls if u.strip()],
    )
    if not p:
        return JSONResponse({"error": "not found"}, status_code=404)
    return {"ok": True, "pipeline": p}


@app.delete("/api/pipelines/{pid}")
async def delete_pipeline(pid: str):
    s = load_state()
    s["pipelines"] = [p for p in s["pipelines"] if p["id"] != pid]
    save_state(s)
    return {"ok": True}


# ------------------------------------------------------------------
# Pipeline control
# ------------------------------------------------------------------
@app.post("/api/pipelines/{pid}/start")
async def pipeline_start(pid: str):
    if not find_pipeline(pid):
        return JSONResponse({"error": "not found"}, status_code=404)
    ok = start_pipeline(pid)
    return {"ok": ok}


@app.post("/api/pipelines/{pid}/stop")
async def pipeline_stop(pid: str):
    ok = stop_pipeline(pid)
    return {"ok": ok}


@app.post("/api/pipelines/{pid}/reset")
async def pipeline_reset(pid: str):
    p = update_pipeline(
        pid,
        cursor=0,
        posted_count=0,
        failed_count=0,
        status="idle",
        next_slot=None,
        next_slot_nairobi=None,
        current=None,
        upcoming_slots=[],
    )
    if not p:
        return JSONResponse({"error": "not found"}, status_code=404)
    return {"ok": True}


# ------------------------------------------------------------------
# Channels proxy
# ------------------------------------------------------------------
@app.get("/api/channels")
async def channels():
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(
                f"{BUFFER_SERVICE}/api/channels",
                headers=buffer_headers(),
            )
        return JSONResponse(r.json(), status_code=r.status_code)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ------------------------------------------------------------------
# JSON upload
# ------------------------------------------------------------------
@app.post("/api/upload")
async def upload_json(file: UploadFile = File(...)):
    import json as _json

    if not file.filename or not file.filename.lower().endswith(".json"):
        return JSONResponse({"ok": False, "error": "Only .json"}, status_code=400)

    try:
        raw = await file.read()
        data = _json.loads(raw.decode("utf-8"))
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Invalid JSON: {e}"}, status_code=400)

    urls = data["urls"] if isinstance(data, dict) and "urls" in data else data
    if not isinstance(urls, list):
        return JSONResponse({"ok": False, "error": "Need array of URLs"}, status_code=400)

    urls = [u.strip() for u in urls if isinstance(u, str) and u.strip()]
    return {"ok": True, "urls": urls, "count": len(urls)}


# ------------------------------------------------------------------
# Local dev
# ------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)