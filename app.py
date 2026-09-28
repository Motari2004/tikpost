import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from buffer import key_check, list_tiktok_channels, BufferError
from db import init_pool, close_pool
from worker import (
    load_state_from_db, persist,
    upcoming_slots, upcoming_slots_for_pipeline,
    ensure_slots_for_today, reroll_slots_today,
    reschedule_pipeline, reroll_pipeline_slot,
    get_buffer_key, daily_roll, roll_one_slot, on_startup,
    find_due_pipelines, fire_pipeline_by_id,
    next_slot_after_pipeline,
    utc_iso, nairobi_iso, now_utc,
    _validate_hhmm,
    WINDOW_START, WINDOW_END, POSTS_PER_DAY, MIN_GAP_MIN,
)

UTC = ZoneInfo("UTC")


def _self_base() -> str:
    return os.getenv("SELF_BASE", "https://tikpost-murex.vercel.app").rstrip("/")


def find_pipeline_in(state: dict, pid: str) -> dict | None:
    for p in state.get("pipelines", []):
        if p["id"] == pid:
            return p
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_pool()
    except Exception as e:
        print(f"[lifespan] init_pool failed: {e}", flush=True)
    try:
        await on_startup()
    except Exception as e:
        print(f"[lifespan] on_startup failed: {e}", flush=True)
    try:
        yield
    finally:
        try:
            await close_pool()
        except Exception:
            pass


app = FastAPI(title="tikpost", lifespan=lifespan)

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


class PipelineInput(BaseModel):
    name: str
    channel_id: str
    tweet_template: str
    urls: list[str]
    schedule_mode: str = "random"
    manual_slot1: str = "09:00"
    manual_slot2: str = "13:00"


class BufferKeyInput(BaseModel):
    api_key: str


def _normalize_schedule(data: PipelineInput) -> tuple[str, str, str]:
    mode = (data.schedule_mode or "random").lower()
    if mode not in ("random", "manual"):
        mode = "random"
    if mode == "manual":
        s1 = _validate_hhmm(data.manual_slot1 or "09:00")
        s2 = _validate_hhmm(data.manual_slot2 or "13:00")
        return mode, s1, s2
    return "random", "09:00", "13:00"


@app.get("/", response_class=HTMLResponse)
async def index():
    idx = TEMPLATES / "index.html"
    if idx.exists():
        return idx.read_text(encoding="utf-8")
    return HTMLResponse("<h1>tikpost API</h1><p>Frontend on Vercel.</p>")


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/api/status")
async def status():
    s = await load_state_from_db()
    slots = ensure_slots_for_today(s)
    out = []
    for p in s["pipelines"]:
        out.append({**p, "total": len(p.get("urls") or [])})
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
        "last_tick": s.get("last_tick"),
        "tick_count": s.get("tick_count", 0),
    }


# ==================================================================
# CRON 1 — tick (fast, dispatches work, returns)
# ==================================================================
@app.get("/api/cron/tick")
@app.post("/api/cron/tick")
async def cron_tick():
    state = await load_state_from_db()
    state["last_tick"] = now_utc().isoformat()
    state["tick_count"] = int(state.get("tick_count") or 0) + 1

    due = find_due_pipelines(state)
    await persist(state)

    dispatched = []
    if due:
        try:
            async with httpx.AsyncClient(timeout=2) as client:
                try:
                    await client.post(f"{_self_base()}/api/internal/work")
                    dispatched.append("ok")
                except httpx.TimeoutException:
                    dispatched.append("dispatched")
        except Exception as e:
            print(f"[tick] dispatch failed: {type(e).__name__}: {e}",
                  flush=True)

    return {
        "ok": True,
        "marked_pending": due,
        "dispatched": dispatched,
        "now": utc_iso(now_utc()),
        "tick_count": state["tick_count"],
    }


# ==================================================================
# CRON 2 — daily-roll
# ==================================================================
@app.get("/api/cron/daily-roll")
@app.post("/api/cron/daily-roll")
async def cron_daily_roll():
    return await daily_roll()


# ==================================================================
# INTERNAL — work (called by tick, not by cron)
# ==================================================================
@app.post("/api/internal/work")
@app.get("/api/internal/work")
async def internal_work():
    state = await load_state_from_db()

    pending = [
        p for p in state.get("pipelines", [])
        if p.get("status") == "pending"
    ]

    if not pending:
        return {"ok": True, "fired": [], "pending_remaining": 0,
                "now": utc_iso(now_utc())}

    p = pending[0]

    state = await load_state_from_db()
    fresh = None
    for x in state.get("pipelines", []):
        if x["id"] == p["id"]:
            fresh = x
            break

    if not fresh or fresh.get("status") != "pending":
        return {"ok": True, "fired": [], "reason": "already_taken",
                "now": utc_iso(now_utc())}

    fresh["status"] = "running"
    await persist(state)

    fired = []
    try:
        result = await fire_pipeline_by_id(fresh["id"])
        fired.append(result)
    except Exception as e:
        print(f"[work] fire failed {fresh['id'][:8]}: "
              f"{type(e).__name__}: {e}", flush=True)
        fired.append({
            "pipeline": fresh["id"],
            "success": False,
            "reason": f"{type(e).__name__}: {e}",
        })

    return {
        "ok": True,
        "fired": fired,
        "pending_remaining": max(0, len(pending) - 1),
        "now": utc_iso(now_utc()),
    }


# ==================================================================
# Settings
# ==================================================================
@app.get("/api/settings")
async def get_settings():
    s = await load_state_from_db()
    key = get_buffer_key(s)
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
        email = await key_check(key)
    except BufferError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    s = await load_state_from_db()
    s["buffer_api_key"] = key
    await persist(s)
    return {"ok": True, "email": email}


@app.delete("/api/settings/buffer-key")
async def clear_buffer_key():
    s = await load_state_from_db()
    s.pop("buffer_api_key", None)
    await persist(s)
    return {"ok": True}


# ==================================================================
# Slots
# ==================================================================
@app.get("/api/slots")
async def slots(count: int = 10):
    s = await load_state_from_db()
    return {"slots": upcoming_slots(s, count)}


@app.post("/api/slots/reroll")
async def slots_reroll():
    """
    Global reroll — regenerates today's random pair AND updates every
    active pipeline's next_slot so the UI reflects the new times.
    """
    state = await load_state_from_db()
    slots = reroll_slots_today(state)

    updated = []
    for p in state.get("pipelines", []):
        if p.get("status") not in ("scheduled", "running", "pending"):
            continue
        if reschedule_pipeline(p, state):
            updated.append(p["id"])

    await persist(state)

    return {
        "ok": True,
        "slots": slots,
        "updated_pipelines": updated,
    }


@app.get("/api/random-slot")
async def random_slot(exclude: str = ""):
    return {"time": roll_one_slot(exclude or None)}


# ==================================================================
# Per-pipeline reroll
# ==================================================================
@app.post("/api/pipelines/{pid}/reroll")
async def pipeline_reroll(pid: str):
    """
    Reroll one pipeline's next slot. Only works for random-mode pipelines
    (manual pipelines have fixed times).
    """
    state = await load_state_from_db()
    p = find_pipeline_in(state, pid)
    if not p:
        return JSONResponse({"error": "not found"}, status_code=404)

    if (p.get("schedule_mode") or "random").lower() == "manual":
        return JSONResponse(
            {"ok": False, "error": "manual pipelines have fixed slots"},
            status_code=400,
        )

    if p.get("status") not in ("scheduled", "running", "pending"):
        return JSONResponse(
            {"ok": False, "error": "pipeline is not active"},
            status_code=400,
        )

    new_slot = reroll_pipeline_slot(p)
    if not new_slot:
        return JSONResponse(
            {"ok": False, "error": "no room left today for a new slot"},
            status_code=400,
        )

    p["next_slot"] = new_slot
    # Recompute the preview list from the new next_slot
    p["upcoming_slots"] = upcoming_slots_for_pipeline(p, state, 10)
    await persist(state)

    return {"ok": True, "next_slot": new_slot}


# ==================================================================
# Pipelines CRUD
# ==================================================================
@app.post("/api/pipelines")
async def create_pipeline(data: PipelineInput):
    try:
        mode, s1, s2 = _normalize_schedule(data)
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

    state = await load_state_from_db()
    pid = str(uuid.uuid4())
    p = {
        "id": pid,
        "name": data.name.strip() or "Untitled pipeline",
        "channel_id": data.channel_id,
        "tweet_template": data.tweet_template,
        "urls": [u.strip() for u in data.urls if u.strip()],
        "schedule_mode": mode,
        "manual_slot1": s1,
        "manual_slot2": s2,
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
    state["pipelines"].append(p)
    await persist(state)
    return {"ok": True, "pipeline": p}


@app.put("/api/pipelines/{pid}")
async def update_pipeline_route(pid: str, data: PipelineInput):
    try:
        mode, s1, s2 = _normalize_schedule(data)
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

    state = await load_state_from_db()
    p = find_pipeline_in(state, pid)
    if not p:
        return JSONResponse({"error": "not found"}, status_code=404)

    p["name"] = data.name.strip() or "Untitled pipeline"
    p["channel_id"] = data.channel_id
    p["tweet_template"] = data.tweet_template
    p["urls"] = [u.strip() for u in data.urls if u.strip()]
    p["schedule_mode"] = mode
    p["manual_slot1"] = s1
    p["manual_slot2"] = s2

    if p.get("status") in ("scheduled", "running", "pending"):
        reschedule_pipeline(p, state)

    await persist(state)
    return {"ok": True, "pipeline": p}


@app.delete("/api/pipelines/{pid}")
async def delete_pipeline(pid: str):
    state = await load_state_from_db()
    state["pipelines"] = [p for p in state["pipelines"] if p["id"] != pid]
    await persist(state)
    return {"ok": True}


@app.post("/api/pipelines/{pid}/start")
async def pipeline_start(pid: str):
    state = await load_state_from_db()
    p = find_pipeline_in(state, pid)
    if not p:
        return JSONResponse({"error": "not found"}, status_code=404)

    try:
        nxt = next_slot_after_pipeline(p, state, now_utc())
    except Exception as e:
        return JSONResponse({"error": f"cannot compute slot: {e}"}, status_code=400)

    p["status"] = "scheduled"
    p["current"] = None
    p["next_slot"] = utc_iso(nxt)
    p["next_slot_nairobi"] = nairobi_iso(nxt)
    p["upcoming_slots"] = upcoming_slots_for_pipeline(p, state, 10)

    await persist(state)
    return {"ok": True, "next_slot": utc_iso(nxt)}


@app.post("/api/pipelines/{pid}/stop")
async def pipeline_stop(pid: str):
    state = await load_state_from_db()
    p = find_pipeline_in(state, pid)
    if not p:
        return JSONResponse({"error": "not found"}, status_code=404)
    p["status"] = "stopped"
    p["current"] = None
    p["next_slot"] = None
    await persist(state)
    return {"ok": True}


@app.post("/api/pipelines/{pid}/reset")
async def pipeline_reset(pid: str):
    state = await load_state_from_db()
    p = find_pipeline_in(state, pid)
    if not p:
        return JSONResponse({"error": "not found"}, status_code=404)
    p["cursor"] = 0
    p["posted_count"] = 0
    p["failed_count"] = 0
    p["status"] = "idle"
    p["next_slot"] = None
    p["next_slot_nairobi"] = None
    p["current"] = None
    p["upcoming_slots"] = []
    await persist(state)
    return {"ok": True}


# ==================================================================
# Channels
# ==================================================================
@app.get("/api/channels")
async def channels():
    s = await load_state_from_db()
    key = get_buffer_key(s)
    if not key:
        return JSONResponse({"error": "No Buffer API key set"}, status_code=400)
    try:
        data = await list_tiktok_channels(key)
        return JSONResponse(data)
    except BufferError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ==================================================================
# Upload
# ==================================================================
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


# ==================================================================
# Local dev
# ==================================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)