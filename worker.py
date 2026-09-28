import asyncio
import hashlib
import os
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

from buffer import create_post, BufferError
from db import read_state, write_state

API_BASE     = os.getenv("API_BASE", "https://tiktokresolver.onrender.com")
API_KEY      = os.getenv("API_KEY", "")
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "3"))
POST_RETRIES = int(os.getenv("BUFFER_RETRY_MAX", "2"))
POST_BACKOFF = float(os.getenv("BUFFER_RETRY_DELAY", "5"))

NAIROBI = ZoneInfo("Africa/Nairobi")
UTC     = ZoneInfo("UTC")

WINDOW_START  = (6, 0)
WINDOW_END    = (23, 0)
POSTS_PER_DAY = 2
MIN_GAP_MIN   = 120


# ==================================================================
# State
# ==================================================================
def _default_state() -> dict:
    return {
        "pipelines": [],
        "slots": {"day": None, "times": []},
        "buffer_api_key": "",
        "last_tick": None,
        "tick_count": 0,
    }


async def load_state_from_db() -> dict:
    try:
        data = await read_state()
    except Exception as e:
        print(f"[db] read_state failed: {e}", flush=True)
        data = {}

    if not data:
        data = _default_state()

    data.setdefault("pipelines", [])
    data.setdefault("slots", {"day": None, "times": []})
    data.setdefault("buffer_api_key", "")
    data.setdefault("last_tick", None)
    data.setdefault("tick_count", 0)
    return data


async def persist(state: dict) -> None:
    try:
        await write_state(state)
    except Exception as e:
        print(f"[db] write_state failed: {e}", flush=True)


# ==================================================================
# Time helpers
# ==================================================================
def fmt_12h(dt: datetime) -> str:
    return dt.astimezone(NAIROBI).strftime("%I:%M %p").lstrip("0")


def _hhmm_to_12h(hhmm: str) -> str:
    h, m = hhmm.split(":")
    h = int(h)
    suffix = "AM" if h < 12 else "PM"
    h12 = h % 12 or 12
    return f"{h12}:{m} {suffix}"


def utc_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def nairobi_iso(dt: datetime) -> str:
    return dt.astimezone(NAIROBI).strftime("%Y-%m-%dT%H:%M")


def now_utc() -> datetime:
    return datetime.now(UTC)


def _minutes(h: int, m: int) -> int:
    return h * 60 + m


def _today_local_str() -> str:
    return now_utc().astimezone(NAIROBI).date().isoformat()


def _validate_hhmm(t: str) -> str:
    t = (t or "").strip()
    if ":" not in t:
        raise ValueError(f"Invalid time: {t!r}")
    h, m = t.split(":")
    h, m = int(h), int(m)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"Invalid time: {t!r}")
    return f"{h:02d}:{m:02d}"


# ==================================================================
# Slots
# ==================================================================
def _roll_slots_for(day) -> list[str]:
    start_min = _minutes(*WINDOW_START)
    end_min   = _minutes(*WINDOW_END)
    span      = end_min - start_min

    if POSTS_PER_DAY == 1:
        offsets = [random.randint(0, span)]
    else:
        for _ in range(300):
            offsets = sorted(random.sample(range(span + 1), POSTS_PER_DAY))
            if all(
                offsets[i + 1] - offsets[i] >= MIN_GAP_MIN
                for i in range(len(offsets) - 1)
            ):
                break
        else:
            offsets = [
                int(round(span * i / (POSTS_PER_DAY + 1)))
                for i in range(1, POSTS_PER_DAY + 1)
            ]

    out = []
    for off in offsets:
        total = start_min + off
        h, m = divmod(total, 60)
        out.append(f"{h:02d}:{m:02d}")
    return sorted(out)


def ensure_slots_for_today(state: dict) -> dict:
    slots = state.setdefault("slots", {"day": None, "times": []})
    today = _today_local_str()

    if slots.get("day") != today or not slots.get("times"):
        slots["day"] = today
        slots["times"] = _roll_slots_for(now_utc().astimezone(NAIROBI).date())
        slots["rolled_at"] = now_utc().isoformat()
        print(f"[slots] rolled new slots for {today}: {slots['times']}", flush=True)

    return slots


def reroll_slots_today(state: dict) -> dict:
    slots = state.setdefault("slots", {})
    today = _today_local_str()
    slots["day"] = today
    slots["times"] = _roll_slots_for(now_utc().astimezone(NAIROBI).date())
    slots["rolled_at"] = now_utc().isoformat()
    return slots


def _parse_hhmm_on(day, hhmm: str) -> datetime:
    h, m = hhmm.split(":")
    return datetime(day.year, day.month, day.day, int(h), int(m), tzinfo=NAIROBI)


def _times_for_pipeline(p: dict, state: dict) -> list[str]:
    mode = (p.get("schedule_mode") or "random").lower()
    if mode == "manual":
        s1 = _validate_hhmm(p.get("manual_slot1") or "09:00")
        s2 = _validate_hhmm(p.get("manual_slot2") or "13:00")
        return sorted([s1, s2])
    return ensure_slots_for_today(state)["times"]


def _slots_for_day(p: dict, state: dict, day) -> list[datetime]:
    mode = (p.get("schedule_mode") or "random").lower()
    today_local = now_utc().astimezone(NAIROBI).date()

    if mode == "manual":
        return [_parse_hhmm_on(day, t).astimezone(UTC)
                for t in _times_for_pipeline(p, state)]

    if day == today_local:
        times = ensure_slots_for_today(state)["times"]
        return [_parse_hhmm_on(day, t).astimezone(UTC) for t in times]

    # Deterministic per-day roll for future days — stable preview
    seed = int(hashlib.sha256(day.isoformat().encode()).hexdigest()[:8], 16)
    rnd = random.Random(seed)
    start_min = _minutes(*WINDOW_START)
    end_min   = _minutes(*WINDOW_END)
    span      = end_min - start_min

    for _ in range(300):
        offs = sorted(rnd.sample(range(span + 1), POSTS_PER_DAY))
        if all(offs[i + 1] - offs[i] >= MIN_GAP_MIN for i in range(len(offs) - 1)):
            break
    else:
        offs = [int(round(span * i / (POSTS_PER_DAY + 1)))
                for i in range(1, POSTS_PER_DAY + 1)]

    out = []
    for off in offs:
        total = start_min + off
        h, m = divmod(total, 60)
        out.append(_parse_hhmm_on(day, f"{h:02d}:{m:02d}").astimezone(UTC))
    return sorted(out)


def next_slot_after_pipeline(p: dict, state: dict, after_utc: datetime) -> datetime:
    local = after_utc.astimezone(NAIROBI)
    for day_offset in range(8):
        day = (local + timedelta(days=day_offset)).date()
        for s in _slots_for_day(p, state, day):
            if s > after_utc:
                return s
    raise RuntimeError("no slot found in 8 days")


def upcoming_slots_for_pipeline(p: dict, state: dict, count: int,
                                after_utc: datetime | None = None) -> list[dict]:
    after = after_utc or now_utc()
    out = []
    cursor = after
    for _ in range(count):
        s = next_slot_after_pipeline(p, state, cursor)
        local = s.astimezone(NAIROBI)
        out.append({
            "utc": utc_iso(s),
            "nairobi": nairobi_iso(s),
            "label": local.strftime("%a %d %b") + " · " + fmt_12h(local),
            "time12": fmt_12h(local),
        })
        cursor = s + timedelta(seconds=1)
    return out


def upcoming_slots(state: dict, count: int,
                   after_utc: datetime | None = None) -> list[dict]:
    fake = {"schedule_mode": "random"}
    return upcoming_slots_for_pipeline(fake, state, count, after_utc)


def roll_one_slot(exclude: str | None = None) -> str:
    start_min = _minutes(*WINDOW_START)
    end_min   = _minutes(*WINDOW_END)
    span      = end_min - start_min

    for _ in range(60):
        total = start_min + random.randint(0, span)
        h, m = divmod(total, 60)
        hhmm = f"{h:02d}:{m:02d}"
        if exclude and hhmm == exclude:
            continue
        return hhmm

    h, m = divmod(start_min + span // 3, 60)
    return f"{h:02d}:{m:02d}"


# ==================================================================
# Key
# ==================================================================
def get_buffer_key(state: dict) -> str:
    return (state.get("buffer_api_key") or os.getenv("BUFFER_API_KEY", "")).strip()


# ==================================================================
# Stages
# ==================================================================
async def resolve(client, url: str) -> dict | None:
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = await client.post(
                f"{API_BASE}/api/v1/resolve",
                json={"url": url}, headers=headers, timeout=20,
            )
            if r.status_code == 200 and r.json().get("status") == "ok":
                return r.json()
            print(f"[resolve] {attempt}: HTTP {r.status_code}", flush=True)
        except Exception as e:
            print(f"[resolve] {attempt}: {e}", flush=True)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(1.5 * attempt)
    return None


async def post_to_buffer(state: dict, video_url: str, source_url: str,
                         channel_id: str, template: str) -> bool:
    text = template.replace("{source_url}", source_url).strip()[:2200]
    key = get_buffer_key(state)

    if not key:
        return False

    for attempt in range(1, POST_RETRIES + 1):
        try:
            await create_post(
                key=key,
                channel_id=channel_id,
                text=text,
                video_url=video_url,
                mode="shareNow",
            )
            return True
        except BufferError as e:
            print(f"[post] {attempt}/{POST_RETRIES}: {e}", flush=True)
        except Exception as e:
            print(f"[post] {attempt}/{POST_RETRIES}: {e}", flush=True)
        if attempt < POST_RETRIES:
            await asyncio.sleep(POST_BACKOFF * attempt)
    return False


# ==================================================================
# Fire one pipeline
# ==================================================================
def _log(state: dict, pid: str, msg: str):
    line = f"{datetime.now(NAIROBI).strftime('%I:%M:%S %p').lstrip('0')} {msg}"
    print(f"[{pid[:8]}] {line}", flush=True)
    for p in state.get("pipelines", []):
        if p["id"] == pid:
            p.setdefault("log", []).append(line)
            p["log"] = p["log"][-200:]
            break


async def fire_one(state: dict, p: dict) -> dict:
    pid = p["id"]
    urls = p.get("urls") or []
    cursor = int(p.get("cursor") or 0)

    if cursor >= len(urls):
        p["status"] = "done"
        p["next_slot"] = None
        _log(state, pid, "🏁 no more URLs")
        return {"pipeline": pid, "fired": False, "reason": "queue_empty"}

    channel_id = p.get("channel_id", "")
    template = p.get("tweet_template", "")
    source_url = urls[cursor]

    _log(state, pid, f"▶ [{cursor+1}/{len(urls)}] {source_url}")
    p["current"] = {"stage": "resolve", "url": source_url}
    p["status"] = "running"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        result = await resolve(client, source_url)

    if not result:
        _log(state, pid, "❌ resolve failed")
        p["cursor"] = cursor + 1
        p["failed_count"] = int(p.get("failed_count") or 0) + 1
        p["current"] = None
        if p["cursor"] >= len(urls):
            p["status"] = "done"
            p["next_slot"] = None
        else:
            nxt = next_slot_after_pipeline(p, state, now_utc())
            p["status"] = "scheduled"
            p["next_slot"] = utc_iso(nxt)
            p["next_slot_nairobi"] = nairobi_iso(nxt)
        return {"pipeline": pid, "fired": True, "success": False,
                "reason": "resolve_failed"}

    video_url = result["download_url"]
    filename = result.get("filename", "video.mp4")
    _log(state, pid, f"✅ resolved → {filename}")
    p["current"] = {"stage": "post", "filename": filename}

    ok = await post_to_buffer(state, video_url, source_url, channel_id, template)

    p["cursor"] = cursor + 1
    p["posted_count"] = int(p.get("posted_count") or 0) + (1 if ok else 0)
    p["failed_count"] = int(p.get("failed_count") or 0) + (0 if ok else 1)
    p["current"] = None

    if p["cursor"] >= len(urls):
        p["status"] = "done"
        p["next_slot"] = None
        p["upcoming_slots"] = []
        _log(state, pid,
             f"🏁 done posted={p['posted_count']} failed={p['failed_count']}")
    else:
        nxt = next_slot_after_pipeline(p, state, now_utc())
        p["status"] = "scheduled"
        p["next_slot"] = utc_iso(nxt)
        p["next_slot_nairobi"] = nairobi_iso(nxt)
        p["upcoming_slots"] = upcoming_slots_for_pipeline(
            p, state, min(10, (len(urls) - p["cursor"]) * 2))

    return {"pipeline": pid, "fired": True, "success": ok, "filename": filename}


# ==================================================================
# tick — called by cron every minute
# ==================================================================
async def tick() -> dict:
    state = await load_state_from_db()
    state["last_tick"] = now_utc().isoformat()
    state["tick_count"] = int(state.get("tick_count") or 0) + 1

    now = now_utc()
    fired = []

    for p in list(state.get("pipelines", [])):
        if p.get("status") not in ("scheduled", "running"):
            continue

        if not p.get("next_slot"):
            try:
                nxt = next_slot_after_pipeline(p, state, now)
                p["next_slot"] = utc_iso(nxt)
                p["next_slot_nairobi"] = nairobi_iso(nxt)
                p["upcoming_slots"] = upcoming_slots_for_pipeline(p, state, 10)
            except Exception as e:
                print(f"[tick] compute failed {p['id'][:8]}: {e}", flush=True)
            continue

        try:
            due = datetime.fromisoformat(p["next_slot"].replace("Z", "+00:00"))
        except Exception:
            nxt = next_slot_after_pipeline(p, state, now)
            p["next_slot"] = utc_iso(nxt)
            p["next_slot_nairobi"] = nairobi_iso(nxt)
            continue

        if due > now:
            continue

        try:
            r = await fire_one(state, p)
            fired.append(r)
        except Exception as e:
            print(f"[tick] fire failed {p['id'][:8]}: {e}", flush=True)
            _log(state, p["id"], f"❌ fire error: {e}")

    await persist(state)

    return {
        "ok": True,
        "fired": fired,
        "now": utc_iso(now),
        "tick_count": state["tick_count"],
    }


# ==================================================================
# daily roll
# ==================================================================
async def daily_roll() -> dict:
    state = await load_state_from_db()
    slots = state.get("slots", {})
    today = _today_local_str()

    if slots.get("day") == today and slots.get("times"):
        return {"ok": True, "already": True, "day": today, "times": slots["times"]}

    s = reroll_slots_today(state)

    for p in state.get("pipelines", []):
        try:
            if p.get("status") in ("scheduled", "running"):
                p["upcoming_slots"] = upcoming_slots_for_pipeline(
                    p, state, min(10, len(p.get("urls") or []) * 2))
        except Exception:
            p["upcoming_slots"] = []

    await persist(state)

    return {
        "ok": True,
        "day": s["day"],
        "times": s["times"],
        "rolled_at": s.get("rolled_at"),
    }


# ==================================================================
# Startup
# ==================================================================
async def on_startup():
    state = await load_state_from_db()
    ensure_slots_for_today(state)

    for p in state.get("pipelines", []):
        if p.get("status") == "running":
            p["status"] = "scheduled"
        if p.get("current"):
            p["current"] = None

    await persist(state)