import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from db import init_pool, close_pool
from worker import (
    load_state, save_state, find_pipeline, update_pipeline,
    start_pipeline, stop_pipeline, is_running,
    upcoming_slots, ensure_slots_for_today, reroll_slots_today,
    daily_reroll_loop, buffer_headers, get_buffer_key,
    load_state_from_db, persist_now,
    _today_local_str,
    WINDOW_START, WINDOW_END, POSTS_PER_DAY, MIN_GAP_MIN,
)

BUFFER_SERVICE = os.getenv("BUFFER_SERVICE", "http://127.0.0.1:3000")
CRON_SECRET    = os.getenv("CRON_SECRET", "").strip()
MIGRATE_JSON   = os.getenv("MIGRATE_STATE_JSON", "1") == "1"


# ------------------------------------------------------------------
# Lifespan — connect to Postgres, hydrate cache, start background loop
# ------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Connect to Postgres
    await init_pool()

    # 2. Load state into the in-memory cache
    await load_state_from_db()

    # 3. Optional one-shot migration from the old state.json
    if MIGRATE_JSON:
        legacy = Path("data/state.json")
        if legacy.exists() and not load_state().get("pipelines"):
            try:
                old = json.loads(legacy.read_text(encoding="utf-8"))
                s = load_state()
                s.update(old)
                save_state(s)
                await persist_now()
                print("[db] migrated data/state.json → Postgres", flush=True)
            except Exception as e:
                print(f"[db] migration failed: {e}", flush=True)

    # 4. Ensure today's slots exist
    ensure_slots_for_today()
    await persist_now()

    # 5. Start background re-roll loop
    task = asyncio.create_task(daily_reroll_loop())

    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await close_pool()


app = FastAPI(title="tikpost", lifespan=lifespan)


# ------------------------------------------------------------------
# CORS — Vercel frontend + local dev
# ------------------------------------------------------------------
_origins_env = os.getenv("CORS_ORIGINS", "").strip()
allow_origins = (
    [o.strip() for o in _origins_env.split(",") if o.strip()]
    if _origins_env
    else [
        "https://tikpost-murex.vercel.app",
        "http://127.0.0.1:8000",
        "http://localhost:8000",
    ]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",
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
# Helpers
# ------------------------------------------------------------------
def _check_cron_secret(request: Request):
    if not CRON_SECRET:
        return
    provided = (
        request.headers.get("x-cron-secret")
        or request.query_params.get("secret")
    )
    if provided != CRON_SECRET:
        raise HTTPException(status_code=401, detail="invalid cron secret")


# ------------------------------------------------------------------
# UI + health
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
# External cron
# ------------------------------------------------------------------
@app.get("/api/cron/daily-roll")
@app.post("/api/cron/daily-roll")
async def cron_daily_roll(request: Request):
    """
    Idempotent endpoint for external cron (cron-job.org).
    Rolls today's slots only if not already rolled.
    """
    _check_cron_secret(request)

    st = load_state()
    slots = st.get("slots", {})
    today = _today_local_str()

    if slots.get("day") == today and slots.get("times"):
        return {
            "ok": True,
            "already": True,
            "day": today,
            "times": slots["times"],
        }

    s = reroll_slots_today()

    # Refresh upcoming_slots on all pipelines
    st = load_state()
    for p in st["pipelines"]:
        p["upcoming_slots"] = upcoming_slots(min(10, len(p.get("urls") or []) * 2))
    save_state(st)
    await persist_now()

    pretty = ", ".join(s["times"])
    print(f"[cron] daily-roll fired → {s['day']}: {pretty}", flush=True)

    return {
        "ok": True,
        "day": s["day"],
        "times": s["times"],
        "rolled_at": s.get("rolled_at"),
    }


# ------------------------------------------------------------------
# Settings (Buffer API key)
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
    await persist_now()
    return {"ok": True, "email": body.get("email")}


@app.delete("/api/settings/buffer-key")
async def clear_buffer_key():
    s = load_state()
    s.pop("buffer_api_key", None)
    save_state(s)
    await persist_now()
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
    await persist_now()
    return {"ok": True, "slots": s}


# ------------------------------------------------------------------
# Pipelines
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
    await persist_now()
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
    await persist_now()
    return {"ok": True, "pipeline": p}


@app.delete("/api/pipelines/{pid}")
async def delete_pipeline(pid: str):
    s = load_state()
    s["pipelines"] = [p for p in s["pipelines"] if p["id"] != pid]
    save_state(s)
    await persist_now()
    return {"ok": True}


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
    await persist_now()
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