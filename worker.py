import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from db import read_state, write_state

API_BASE       = os.getenv("API_BASE", "https://tiktokresolver.onrender.com")
API_KEY        = os.getenv("API_KEY", "")
BUFFER_SERVICE = os.getenv("BUFFER_SERVICE", "http://127.0.0.1:3000")
MAX_RETRIES    = int(os.getenv("MAX_RETRIES", "3"))
POST_RETRIES   = int(os.getenv("BUFFER_RETRY_MAX", "3"))
POST_BACKOFF   = float(os.getenv("BUFFER_RETRY_DELAY", "10"))

NAIROBI = ZoneInfo("Africa/Nairobi")
UTC     = ZoneInfo("UTC")

# ==================================================================
# Posting window (Nairobi local time)
# ==================================================================
WINDOW_START  = (6, 0)
WINDOW_END    = (23, 0)
POSTS_PER_DAY = 2
MIN_GAP_MIN   = 120


# ==================================================================
# In-memory state cache
# ==================================================================
_STATE: dict = {
    "pipelines": [],
    "slots": {"day": None, "times": []},
    "buffer_api_key": "",
}
_STATE_LOCK = asyncio.Lock()


def _default_state() -> dict:
    return {
        "pipelines": [],
        "slots": {"day": None, "times": []},
        "buffer_api_key": "",
    }


def load_state() -> dict:
    """Synchronous accessor for the in-memory cache."""
    return _STATE


def save_state(state: dict) -> None:
    """
    Update the in-memory cache and schedule a DB write.
    Safe to call from sync code; the write is fire-and-forget.
    """
    global _STATE
    _STATE = state
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_flush_state(state))
    except RuntimeError:
        # No running loop — happens at import time; skip.
        pass


async def _flush_state(state: dict) -> None:
    async with _STATE_LOCK:
        try:
            await write_state(state)
        except Exception as e:
            print(f"[db] write_state failed: {e}", flush=True)


async def load_state_from_db() -> dict:
    """Load the state from Postgres into the in-memory cache."""
    global _STATE
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

    _STATE = data
    return _STATE


async def persist_now() -> None:
    """Force a synchronous flush (used by endpoints that must be durable)."""
    await _flush_state(_STATE)


# ==================================================================
# Time helpers
# ==================================================================
def fmt_12h(dt: datetime) -> str:
    s = dt.astimezone(NAIROBI).strftime("%I:%M %p")
    return s.lstrip("0")


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


# ==================================================================
# Buffer key
# ==================================================================
def get_buffer_key() -> str:
    s = _STATE
    return (s.get("buffer_api_key") or os.getenv("BUFFER_API_KEY", "")).strip()


def buffer_headers() -> dict:
    key = get_buffer_key()
    return {"X-Buffer-Key": key} if key else {}


# ==================================================================
# Random daily slots
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


def ensure_slots_for_today() -> dict:
    s = _STATE
    slots = s.setdefault("slots", {"day": None, "times": []})
    today = _today_local_str()

    if slots.get("day") != today or not slots.get("times"):
        slots["day"] = today
        slots["times"] = _roll_slots_for(now_utc().astimezone(NAIROBI).date())
        slots["rolled_at"] = now_utc().isoformat()
        save_state(s)
        pretty = ", ".join(_hhmm_to_12h(t) for t in slots["times"])
        print(f"[slots] rolled new slots for {today}: {pretty}", flush=True)

    return slots


def reroll_slots_today() -> dict:
    s = _STATE
    slots = s.setdefault("slots", {})
    today = _today_local_str()
    slots["day"] = today
    slots["times"] = _roll_slots_for(now_utc().astimezone(NAIROBI).date())
    slots["rolled_at"] = now_utc().isoformat()
    save_state(s)
    return slots


def _parse_hhmm_on(day, hhmm: str) -> datetime:
    h, m = hhmm.split(":")
    return datetime(day.year, day.month, day.day, int(h), int(m), tzinfo=NAIROBI)


def today_slots_utc() -> list[datetime]:
    slots = ensure_slots_for_today()
    day = now_utc().astimezone(NAIROBI).date()
    return [_parse_hhmm_on(day, t).astimezone(UTC) for t in slots["times"]]


def daily_slots_for(day) -> list[datetime]:
    today_local = now_utc().astimezone(NAIROBI).date()
    if day == today_local:
        return today_slots_utc()

    key = day.isoformat()
    cached = _FUTURE_CACHE.get(key)
    if cached is None:
        cached = [
            _parse_hhmm_on(day, t).astimezone(UTC)
            for t in _roll_slots_for(day)
        ]
        _FUTURE_CACHE[key] = cached
        if len(_FUTURE_CACHE) > 4:
            for old in sorted(_FUTURE_CACHE.keys())[:-4]:
                _FUTURE_CACHE.pop(old, None)
    return cached


_FUTURE_CACHE: dict[str, list[datetime]] = {}


def next_slot_after(after_utc: datetime) -> datetime:
    local = after_utc.astimezone(NAIROBI)
    for day_offset in range(8):
        day = (local + timedelta(days=day_offset)).date()
        for slot_local_utc in daily_slots_for(day):
            if slot_local_utc > after_utc:
                return slot_local_utc
    raise RuntimeError("no slot found in 8 days")


def upcoming_slots(count: int, after_utc: datetime | None = None) -> list[dict]:
    after = after_utc or now_utc()
    out = []
    cursor = after
    for _ in range(count):
        s = next_slot_after(cursor)
        local = s.astimezone(NAIROBI)
        out.append({
            "utc": utc_iso(s),
            "nairobi": nairobi_iso(s),
            "label": local.strftime("%a %d %b") + " · " + fmt_12h(local),
            "time12": fmt_12h(local),
        })
        cursor = s + timedelta(seconds=1)
    return out


# ==================================================================
# Pipeline helpers
# ==================================================================
def find_pipeline(pid: str) -> dict | None:
    for p in _STATE.get("pipelines", []):
        if p["id"] == pid:
            return p
    return None


def update_pipeline(pid: str, **fields) -> dict | None:
    s = _STATE
    for p in s.get("pipelines", []):
        if p["id"] == pid:
            p.update(fields)
            save_state(s)
            return p
    return None


# ==================================================================
# HTTP stages
# ==================================================================
async def resolve(client, url, log) -> dict | None:
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = await client.post(
                f"{API_BASE}/api/v1/resolve",
                json={"url": url}, headers=headers, timeout=120,
            )
            if r.status_code == 200 and r.json().get("status") == "ok":
                return r.json()
            log(f"resolve {attempt}: HTTP {r.status_code}")
        except Exception as e:
            log(f"resolve {attempt}: {e}")
        await asyncio.sleep(5 * attempt)
    return None


async def post(video_url: str, source_url: str,
               channel_id: str, template: str, log) -> bool:
    text = template.replace("{source_url}", source_url).strip()[:2200]

    payload = {
        "channelId": channel_id,
        "text": text,
        "videoUrls": [video_url],
        "mode": "shareNow",
    }

    headers = {"Content-Type": "application/json", **buffer_headers()}
    log(f"🎬 TikTok video URL: {video_url[:90]}...")

    for attempt in range(1, POST_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=120) as c:
                r = await c.post(
                    f"{BUFFER_SERVICE}/api/posts",
                    json=payload, headers=headers,
                )
            body = r.json()
            if r.status_code == 200 and body.get("post"):
                p = body["post"]
                log(f"✅ published id={p.get('id', '?')[:8]}... status={p.get('status')}")
                return True
            log(f"⚠ post {attempt}/{POST_RETRIES}: {body.get('error') or r.text[:120]}")
        except Exception as e:
            log(f"⚠ post {attempt}/{POST_RETRIES}: {e}")
        if attempt < POST_RETRIES:
            await asyncio.sleep(POST_BACKOFF * attempt)
    return False


# ==================================================================
# Per-pipeline runner
# ==================================================================
class PipelineRunner:
    def __init__(self, pid: str):
        self.pid = pid
        self.cancel = False
        self.task: asyncio.Task | None = None
        self.current: dict | None = None
        self.next_slot: str | None = None

    def log(self, msg: str):
        line = f"{datetime.now(NAIROBI).strftime('%I:%M:%S %p').lstrip('0')} {msg}"
        print(f"[{self.pid[:8]}] {line}", flush=True)
        s = _STATE
        for p in s.get("pipelines", []):
            if p["id"] == self.pid:
                p.setdefault("log", []).append(line)
                p["log"] = p["log"][-200:]
                break
        save_state(s)

    async def _sleep_until(self, target: datetime):
        while not self.cancel:
            remaining = (target - now_utc()).total_seconds()
            if remaining <= 0:
                return
            await asyncio.sleep(min(2, remaining))

    async def run(self):
        p = find_pipeline(self.pid)
        if not p:
            return

        update_pipeline(self.pid, status="scheduled")

        urls       = p.get("urls") or []
        channel_id = p.get("channel_id", "")
        template   = p.get("tweet_template", "")

        if not channel_id:
            self.log("❌ no channel selected")
            update_pipeline(self.pid, status="failed")
            return
        if not urls:
            self.log("❌ no URLs in pipeline")
            update_pipeline(self.pid, status="failed")
            return
        if not get_buffer_key():
            self.log("❌ no Buffer API key — set it in ⚙ Settings")
            update_pipeline(self.pid, status="failed")
            return

        ensure_slots_for_today()
        slots_today = _STATE["slots"]["times"]
        pretty_slots = ", ".join(_hhmm_to_12h(t) for t in slots_today)

        win_start = _hhmm_to_12h(f"{WINDOW_START[0]:02d}:{WINDOW_START[1]:02d}")
        win_end   = _hhmm_to_12h(f"{WINDOW_END[0]:02d}:{WINDOW_END[1]:02d}")

        self.log(
            f"🗓 window {win_start}–{win_end} (Nairobi) · "
            f"randomized {POSTS_PER_DAY}/day · min gap {MIN_GAP_MIN}m"
        )
        self.log(f"   today's slots: {pretty_slots} (Nairobi)")

        preview = upcoming_slots(min(10, len(urls) * 2))
        update_pipeline(self.pid, upcoming_slots=preview)
        self.log("   upcoming slots:")
        for i, slot in enumerate(preview, 1):
            self.log(f"     {i:2d}. {slot['label']} (Nairobi)")

        posted = failed = 0
        slot_index = 0

        async with httpx.AsyncClient(follow_redirects=True) as client:
            while slot_index < len(urls) and not self.cancel:
                slot_time = next_slot_after(now_utc())
                nairobi_time = slot_time.astimezone(NAIROBI)
                nai_12h = fmt_12h(nairobi_time)

                self.next_slot = utc_iso(slot_time)
                update_pipeline(
                    self.pid,
                    next_slot=self.next_slot,
                    next_slot_nairobi=nairobi_iso(slot_time),
                    upcoming_slots=upcoming_slots(min(10, len(urls) * 2)),
                )

                wait_sec = (slot_time - now_utc()).total_seconds()
                self.log(
                    f"⏳ slot #{slot_index+1} → "
                    f"{nairobi_time.strftime('%a %d %b')} · {nai_12h} (Nairobi) "
                    f"· in {wait_sec/3600:.2f}h"
                )

                await self._sleep_until(slot_time)
                if self.cancel:
                    break

                source_url = urls[slot_index]
                self.current = {"stage": "resolve", "url": source_url}
                update_pipeline(self.pid, current=dict(self.current))
                self.log(f"▶ [{slot_index+1}/{len(urls)}] {source_url}")

                result = await resolve(client, source_url, self.log)
                if not result:
                    self.log("❌ resolve failed")
                    failed += 1
                    slot_index += 1
                    update_pipeline(
                        self.pid,
                        cursor=slot_index,
                        failed_count=failed,
                        current=None,
                        upcoming_slots=upcoming_slots(min(10, len(urls) * 2)),
                    )
                    continue

                video_url = result["download_url"]
                filename  = result.get("filename", "video.mp4")

                self.current = {"stage": "post", "filename": filename}
                update_pipeline(self.pid, current=dict(self.current))
                self.log(f"✅ resolved → {filename}")

                if await post(video_url, source_url, channel_id, template, self.log):
                    posted += 1
                    self.log(f"✅ posted {filename}")
                else:
                    failed += 1
                    self.log(f"❌ failed {filename}")

                slot_index += 1
                update_pipeline(
                    self.pid,
                    cursor=slot_index,
                    posted_count=posted,
                    failed_count=failed,
                    current=None,
                    upcoming_slots=upcoming_slots(min(10, len(urls) * 2)),
                )

        status = "stopped" if self.cancel else "done"
        update_pipeline(
            self.pid,
            status=status,
            next_slot=None,
            next_slot_nairobi=None,
            current=None,
            posted_count=posted,
            failed_count=failed,
            cursor=slot_index,
        )
        self.log(f"🏁 {status} posted={posted} failed={failed}")


# ==================================================================
# Runner registry
# ==================================================================
runners: dict[str, PipelineRunner] = {}


def start_pipeline(pid: str) -> bool:
    if pid in runners and runners[pid].task and not runners[pid].task.done():
        return False
    r = PipelineRunner(pid)
    runners[pid] = r
    r.task = asyncio.create_task(r.run())
    return True


def stop_pipeline(pid: str) -> bool:
    r = runners.get(pid)
    if not r:
        return False
    r.cancel = True
    return True


def is_running(pid: str) -> bool:
    r = runners.get(pid)
    return bool(r and r.task and not r.task.done())


# ==================================================================
# Background re-roll
# ==================================================================
async def daily_reroll_loop():
    print("[slots] background reroll loop started", flush=True)
    try:
        ensure_slots_for_today()
        await persist_now()
    except Exception as e:
        print(f"[slots] initial roll failed: {e}", flush=True)

    while True:
        try:
            local_now = now_utc().astimezone(NAIROBI)
            tomorrow = (local_now + timedelta(days=1)).date()
            midnight = datetime(
                tomorrow.year, tomorrow.month, tomorrow.day,
                0, 0, 0, tzinfo=NAIROBI,
            ).astimezone(UTC)

            wait_sec = (midnight - now_utc()).total_seconds()
            while wait_sec > 0:
                await asyncio.sleep(min(60, wait_sec))
                wait_sec = (midnight - now_utc()).total_seconds()

            s = _STATE
            day_str = _today_local_str()
            slots = s.setdefault("slots", {})
            if slots.get("day") != day_str or not slots.get("times"):
                slots["day"] = day_str
                slots["times"] = _roll_slots_for(now_utc().astimezone(NAIROBI).date())
                slots["rolled_at"] = now_utc().isoformat()
                save_state(s)
                await persist_now()
                pretty = ", ".join(_hhmm_to_12h(t) for t in slots["times"])
                print(f"[slots] background rolled new slots for {day_str}: {pretty}", flush=True)
        except Exception as e:
            print(f"[slots] reroll loop error: {e}", flush=True)
            await asyncio.sleep(60)