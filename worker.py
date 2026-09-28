import asyncio
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from buffer import create_post, BufferError
from db import read_state, write_state

API_BASE     = os.getenv("API_BASE", "https://tiktokresolver.onrender.com")
API_KEY      = os.getenv("API_KEY", "")
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "3"))
POST_RETRIES = int(os.getenv("BUFFER_RETRY_MAX", "3"))
POST_BACKOFF = float(os.getenv("BUFFER_RETRY_DELAY", "10"))

NAIROBI = ZoneInfo("Africa/Nairobi")
UTC     = ZoneInfo("UTC")

WINDOW_START  = (6, 0)    # 6:00 AM
WINDOW_END    = (23, 0)   # 11:00 PM
POSTS_PER_DAY = 2
MIN_GAP_MIN   = 120
DEFAULT_MANUAL_GAP_MIN = 240   # 4h


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
    return _STATE


def save_state(state: dict) -> None:
    global _STATE
    _STATE = state
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_flush_state(state))
    except RuntimeError:
        pass


async def _flush_state(state: dict) -> None:
    async with _STATE_LOCK:
        try:
            await write_state(state)
        except Exception as e:
            print(f"[db] write_state failed: {e}", flush=True)


async def load_state_from_db() -> dict:
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
    await _flush_state(_STATE)


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


def _add_minutes_hhmm(hhmm: str, minutes: int) -> str:
    """'09:00' + 240 → '13:00' (wraps past midnight if needed)."""
    h, m = hhmm.split(":")
    total = int(h) * 60 + int(m) + minutes
    total %= 24 * 60
    h2, m2 = divmod(total, 60)
    return f"{h2:02d}:{m2:02d}"


def _derive_manual_times(first_hhmm: str, gap_min: int) -> list[str]:
    """
    Given the first slot, produce the full list of HH:MM times for the day.
    POSTS_PER_DAY=2 → [first, first+gap].
    """
    first = _validate_hhmm(first_hhmm)
    if POSTS_PER_DAY == 1:
        return [first]
    second = _add_minutes_hhmm(first, gap_min)
    return [first, second]


# ==================================================================
# Buffer key
# ==================================================================
def get_buffer_key() -> str:
    s = _STATE
    return (s.get("buffer_api_key") or os.getenv("BUFFER_API_KEY", "")).strip()


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


def _times_for_pipeline(p: dict) -> list[str]:
    """Return the two HH:MM times this pipeline should use today."""
    mode = (p.get("schedule_mode") or "random").lower()
    if mode == "manual":
        first = p.get("manual_first") or "09:00"
        gap   = int(p.get("manual_gap_min") or DEFAULT_MANUAL_GAP_MIN)
        return _derive_manual_times(first, gap)
    # random → use today's rolled slots
    return ensure_slots_for_today()["times"]


def daily_slots_for_pipeline(p: dict, day) -> list[datetime]:
    today_local = now_utc().astimezone(NAIROBI).date()
    mode = (p.get("schedule_mode") or "random").lower()

    if mode == "manual":
        return [_parse_hhmm_on(day, t).astimezone(UTC) for t in _times_for_pipeline(p)]

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


def next_slot_after_pipeline(p: dict, after_utc: datetime) -> datetime:
    local = after_utc.astimezone(NAIROBI)
    for day_offset in range(8):
        day = (local + timedelta(days=day_offset)).date()
        for slot_local_utc in daily_slots_for_pipeline(p, day):
            if slot_local_utc > after_utc:
                return slot_local_utc
    raise RuntimeError("no slot found in 8 days")


def upcoming_slots_for_pipeline(p: dict, count: int,
                                after_utc: datetime | None = None) -> list[dict]:
    after = after_utc or now_utc()
    out = []
    cursor = after
    for _ in range(count):
        s = next_slot_after_pipeline(p, cursor)
        local = s.astimezone(NAIROBI)
        out.append({
            "utc": utc_iso(s),
            "nairobi": nairobi_iso(s),
            "label": local.strftime("%a %d %b") + " · " + fmt_12h(local),
            "time12": fmt_12h(local),
        })
        cursor = s + timedelta(seconds=1)
    return out


def upcoming_slots(count: int, after_utc: datetime | None = None) -> list[dict]:
    """Global random preview (used on the window strip)."""
    after = after_utc or now_utc()
    out = []
    cursor = after
    for _ in range(count):
        local_dt = cursor.astimezone(NAIROBI)
        day = local_dt.date()
        candidates = today_slots_utc() if day == local_dt.date() else [
            _parse_hhmm_on(day, t).astimezone(UTC) for t in _roll_slots_for(day)
        ]
        chosen = next((s for s in candidates if s > cursor), None)
        if chosen is None:
            for d_offset in range(1, 8):
                day2 = (local_dt + timedelta(days=d_offset)).date()
                candidates = [
                    _parse_hhmm_on(day2, t).astimezone(UTC)
                    for t in _roll_slots_for(day2)
                ]
                chosen = next((s for s in candidates if s > cursor), None)
                if chosen:
                    break
        if chosen is None:
            raise RuntimeError("no slot found")
        local = chosen.astimezone(NAIROBI)
        out.append({
            "utc": utc_iso(chosen),
            "nairobi": nairobi_iso(chosen),
            "label": local.strftime("%a %d %b") + " · " + fmt_12h(local),
            "time12": fmt_12h(local),
        })
        cursor = chosen + timedelta(seconds=1)
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
    key = get_buffer_key()

    if not key:
        log("❌ no Buffer API key — set it in ⚙ Settings")
        return False

    log(f"🎬 TikTok video URL: {video_url[:90]}...")

    for attempt in range(1, POST_RETRIES + 1):
        try:
            result = await create_post(
                key=key,
                channel_id=channel_id,
                text=text,
                video_url=video_url,
                mode="shareNow",
            )
            pid = (result.get("id") or "?")[:8]
            log(f"✅ published id={pid}... status={result.get('status')}")
            return True
        except BufferError as e:
            log(f"⚠ post {attempt}/{POST_RETRIES}: {e}")
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
        mode       = (p.get("schedule_mode") or "random").lower()

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

        if mode == "manual":
            try:
                times = _times_for_pipeline(p)
            except ValueError as e:
                self.log(f"❌ invalid manual time: {e}")
                update_pipeline(self.pid, status="failed")
                return
            pretty = ", ".join(_hhmm_to_12h(t) for t in times)
            gap = int(p.get("manual_gap_min") or DEFAULT_MANUAL_GAP_MIN)
            self.log(f"🗓 schedule: manual · first {_hhmm_to_12h(times[0])} "
                     f"+{gap}m → {pretty} (Nairobi)")
        else:
            ensure_slots_for_today()
            slots_today = _STATE["slots"]["times"]
            pretty = ", ".join(_hhmm_to_12h(t) for t in slots_today)
            self.log(f"🗓 schedule: random · today: {pretty} (Nairobi)")

        preview = upcoming_slots_for_pipeline(p, min(10, len(urls) * 2))
        update_pipeline(self.pid, upcoming_slots=preview)
        self.log("   upcoming slots:")
        for i, slot in enumerate(preview, 1):
            self.log(f"     {i:2d}. {slot['label']} (Nairobi)")

        posted = failed = 0
        slot_index = 0

        async with httpx.AsyncClient(follow_redirects=True) as client:
            while slot_index < len(urls) and not self.cancel:
                p = find_pipeline(self.pid)
                slot_time = next_slot_after_pipeline(p, now_utc())
                nairobi_time = slot_time.astimezone(NAIROBI)
                nai_12h = fmt_12h(nairobi_time)

                self.next_slot = utc_iso(slot_time)
                update_pipeline(
                    self.pid,
                    next_slot=self.next_slot,
                    next_slot_nairobi=nairobi_iso(slot_time),
                    upcoming_slots=upcoming_slots_for_pipeline(
                        p, min(10, len(urls) * 2)),
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
                        upcoming_slots=upcoming_slots_for_pipeline(
                            p, min(10, len(urls) * 2)),
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
                    upcoming_slots=upcoming_slots_for_pipeline(
                        p, min(10, len(urls) * 2)),
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
# Background daily re-roll
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
                print(f"[slots] background rolled new slots for {day_str}: {pretty}",
                      flush=True)
        except Exception as e:
            print(f"[slots] reroll loop error: {e}", flush=True)
            await asyncio.sleep(60)