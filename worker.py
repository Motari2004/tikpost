import asyncio
import hashlib
import os
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

from buffer import create_post, BufferError
from db import read_state, write_state

API_BASE           = os.getenv("API_BASE", "https://tiktokresolver.onrender.com")
API_KEY            = os.getenv("API_KEY", "")
RESOLVE_TIMEOUT    = float(os.getenv("RESOLVE_TIMEOUT", "55"))
TRANSCRIPT_TIMEOUT = float(os.getenv("TRANSCRIPT_TIMEOUT", "90"))
POST_RETRIES       = int(os.getenv("BUFFER_RETRY_MAX", "1"))
POST_BACKOFF       = float(os.getenv("BUFFER_RETRY_DELAY", "5"))
MAX_URL_RETRIES    = int(os.getenv("MAX_URL_RETRIES", "3"))

# Inline retries within a single fire (before falling back to next-slot retry)
INLINE_RESOLVE_ATTEMPTS = int(os.getenv("INLINE_RESOLVE_ATTEMPTS", "3"))
INLINE_RESOLVE_BACKOFF  = float(os.getenv("INLINE_RESOLVE_BACKOFF", "5"))

NAIROBI = ZoneInfo("Africa/Nairobi")
UTC     = ZoneInfo("UTC")

WINDOW_START  = (6, 0)
WINDOW_END    = (23, 0)
POSTS_PER_DAY = 2
MIN_GAP_MIN   = 120

PENDING_STALE_SEC = 300


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
# Time
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
# Pipeline helpers
# ==================================================================
def reschedule_pipeline(p: dict, state: dict) -> bool:
    try:
        nxt = next_slot_after_pipeline(p, state, now_utc())
        p["next_slot"] = utc_iso(nxt)
        p["next_slot_nairobi"] = nairobi_iso(nxt)
        p["upcoming_slots"] = upcoming_slots_for_pipeline(p, state, 10)
        return True
    except Exception as e:
        print(f"[reschedule] {p.get('id', '?')[:8]}: {e}", flush=True)
        return False


def reroll_pipeline_slot(p: dict) -> str | None:
    now_local = now_utc().astimezone(NAIROBI)
    start_min = _minutes(*WINDOW_START)
    end_min   = _minutes(*WINDOW_END)
    now_min   = now_local.hour * 60 + now_local.minute

    lower = max(start_min, now_min + 2)
    upper = end_min - MIN_GAP_MIN
    if lower >= upper:
        return None

    h, m = divmod(random.randint(lower, upper), 60)
    target_local = now_local.replace(hour=h, minute=m, second=0, microsecond=0)
    return utc_iso(target_local.astimezone(UTC))


# ==================================================================
# Key
# ==================================================================
def get_buffer_key(state: dict) -> str:
    return (state.get("buffer_api_key") or os.getenv("BUFFER_API_KEY", "")).strip()


# ==================================================================
# Log
# ==================================================================
def _log(state: dict, pid: str, msg: str):
    line = f"{datetime.now(NAIROBI).strftime('%I:%M:%S %p').lstrip('0')} {msg}"
    print(f"[{pid[:8]}] {line}", flush=True)
    for p in state.get("pipelines", []):
        if p["id"] == pid:
            p.setdefault("log", []).append(line)
            p["log"] = p["log"][-200:]
            break


# ==================================================================
# Retry tracking (per-URL, across slots)
# ==================================================================
def _get_retry_count(p: dict, source_url: str) -> int:
    retries = p.get("retries") or {}
    return int(retries.get(source_url, 0))


def _bump_retry(p: dict, source_url: str) -> int:
    p.setdefault("retries", {})
    p["retries"][source_url] = int(p["retries"].get(source_url, 0)) + 1
    return p["retries"][source_url]


def _clear_retry(p: dict, source_url: str):
    retries = p.get("retries") or {}
    if source_url in retries:
        del retries[source_url]


# ==================================================================
# Resolve — with inline retries
# ==================================================================
async def resolve_one(url: str, log) -> dict | None:
    """
    Resolve a TikTok URL.

    Retries inline up to INLINE_RESOLVE_ATTEMPTS times before giving up.
    Each attempt gets a full RESOLVE_TIMEOUT budget. The first attempt
    on a cold resolver wakes it up; the second or third usually succeeds
    within the same fire.
    """
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    for attempt in range(1, INLINE_RESOLVE_ATTEMPTS + 1):
        log(f"[resolve] attempt {attempt}/{INLINE_RESOLVE_ATTEMPTS} "
            f"→ {API_BASE}")

        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(RESOLVE_TIMEOUT, connect=15.0),
            ) as client:
                r = await client.post(
                    f"{API_BASE}/api/v1/resolve",
                    json={"url": url},
                    headers=headers,
                )

            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception as e:
                    log(f"[resolve] bad JSON: {e}")
                    data = None

                if data and data.get("status") == "ok":
                    return data

                log(f"[resolve] non-ok response: "
                    f"{str(data)[:160] if data else 'unknown'}")
            else:
                log(f"[resolve] HTTP {r.status_code}: {r.text[:160]}")

        except httpx.TimeoutException:
            log(f"[resolve] timeout after {RESOLVE_TIMEOUT}s "
                f"(attempt {attempt}/{INLINE_RESOLVE_ATTEMPTS})")
        except httpx.ConnectError as e:
            log(f"[resolve] connect error (attempt {attempt}): {e}")
        except Exception as e:
            log(f"[resolve] {type(e).__name__} (attempt {attempt}): {e}")

        # Backoff between inline attempts, but not after the last one
        if attempt < INLINE_RESOLVE_ATTEMPTS:
            log(f"[resolve] retrying in {INLINE_RESOLVE_BACKOFF:.0f}s…")
            await asyncio.sleep(INLINE_RESOLVE_BACKOFF)

    log(f"[resolve] all {INLINE_RESOLVE_ATTEMPTS} inline attempts failed")
    return None


# ==================================================================
# Transcript
# ==================================================================
async def fetch_transcript(source_url: str, log) -> str | None:
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    log("[transcript] requesting…")

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(TRANSCRIPT_TIMEOUT, connect=15.0),
        ) as client:
            r = await client.post(
                f"{API_BASE}/api/v1/transcript",
                json={"url": source_url},
                headers=headers,
            )
    except httpx.TimeoutException:
        log(f"[transcript] timeout after {TRANSCRIPT_TIMEOUT}s")
        return None
    except httpx.ConnectError as e:
        log(f"[transcript] connect error: {e}")
        return None
    except Exception as e:
        log(f"[transcript] {type(e).__name__}: {e}")
        return None

    try:
        data = r.json()
    except Exception as e:
        log(f"[transcript] bad JSON: {e} (HTTP {r.status_code})")
        return None

    if data.get("status") == "ok" and data.get("transcript"):
        text = data["transcript"].strip()
        log(f"[transcript] {len(text)} chars")
        return text

    log(f"[transcript] error: {data.get('error', 'unknown')}")
    return None


# ==================================================================
# Caption builder — transcript verbatim
# ==================================================================
def _apply_tokens(text: str, p: dict, cursor: int, source_url: str) -> str:
    urls = p.get("urls") or []
    remaining = max(0, len(urls) - cursor - 1)
    slot_local = now_utc().astimezone(NAIROBI)

    tokens = {
        "{source_url}": source_url,
        "{index}":      str(cursor + 1),
        "{remaining}":  str(remaining),
        "{date}":       slot_local.strftime("%Y-%m-%d"),
        "{time}":       slot_local.strftime("%I:%M %p").lstrip("0"),
        "{day}":        slot_local.strftime("%A"),
        "{name}":       p.get("name") or "",
    }
    for k, v in tokens.items():
        text = text.replace(k, v)

    return text.strip()[:2200]


def build_caption(p: dict, cursor: int, source_url: str,
                  transcript: str | None) -> str:
    """
    Use whatever the transcript service returns, verbatim.
    Fall back to the template only if the transcript is empty.
    """
    if p.get("use_transcript", True) and transcript:
        cap = transcript.strip()
        if cap:
            limit = int(p.get("transcript_max_chars") or 2200)
            if limit > 0 and len(cap) > limit:
                cap = cap[:limit].rsplit(" ", 1)[0] + "…"
            return _apply_tokens(cap, p, cursor, source_url)

    template = p.get("tweet_template") or ""
    if template.strip():
        return _apply_tokens(template, p, cursor, source_url)

    return source_url[:280]


# ==================================================================
# Buffer post
# ==================================================================
async def post_to_buffer(state: dict, p: dict,
                         video_url: str, source_url: str,
                         caption: str, log) -> bool:
    key = get_buffer_key(state)
    if not key:
        log("[post] no Buffer API key")
        return False

    for attempt in range(1, POST_RETRIES + 1):
        try:
            result = await create_post(
                key=key,
                channel_id=p.get("channel_id", ""),
                text=caption,
                video_url=video_url,
                mode="shareNow",
            )
            pid = (result.get("id") or "?")[:8]
            log(f"[post] ✅ published id={pid}")
            return True
        except BufferError as e:
            log(f"[post] attempt {attempt}: {e}")
        except Exception as e:
            log(f"[post] attempt {attempt}: {type(e).__name__}: {e}")

        if attempt < POST_RETRIES:
            await asyncio.sleep(POST_BACKOFF * attempt)

    return False


# ==================================================================
# Due discovery
# ==================================================================
def find_due_pipelines(state: dict) -> list[str]:
    now = now_utc()
    stuck_before = now - timedelta(minutes=5)
    pending_stale = now - timedelta(seconds=PENDING_STALE_SEC)
    due = []

    for p in state.get("pipelines", []):
        status = p.get("status")
        if status not in ("scheduled", "running", "pending"):
            continue

        ns = p.get("next_slot")
        if not ns:
            continue
        try:
            due_at = datetime.fromisoformat(ns.replace("Z", "+00:00"))
        except Exception:
            continue

        if status == "running" and due_at < stuck_before:
            p["status"] = "scheduled"
            p["current"] = None
            status = "scheduled"

        if status == "pending" and due_at < pending_stale:
            p["status"] = "scheduled"
            p["current"] = None
            status = "scheduled"

        if due_at <= now and status == "scheduled":
            p["status"] = "pending"
            p["pending_since"] = now.isoformat()
            due.append(p["id"])

    return due


# ==================================================================
# Fire one pipeline
# ==================================================================
async def fire_one(state: dict, p: dict) -> dict:
    pid = p["id"]
    urls = p.get("urls") or []
    cursor = int(p.get("cursor") or 0)

    if cursor >= len(urls):
        p["status"] = "done"
        p["next_slot"] = None
        _log(state, pid, "🏁 no more URLs")
        return {"pipeline": pid, "success": False, "reason": "queue_empty"}

    source_url = urls[cursor]
    log = lambda m: _log(state, pid, m)
    slot_attempt = _get_retry_count(p, source_url) + 1

    _log(
        state, pid,
        f"▶ [{cursor+1}/{len(urls)}] {source_url}"
        + (f" (slot attempt {slot_attempt}/{MAX_URL_RETRIES})"
           if slot_attempt > 1 else ""),
    )
    p["current"] = {"stage": "resolve", "url": source_url}
    p["status"] = "running"
    await persist(state)

    # ---- 1. resolve (with inline retries) ----
    result = await resolve_one(source_url, log)

    if not result:
        new_count = _bump_retry(p, source_url)

        if new_count < MAX_URL_RETRIES:
            _log(state, pid,
                 f"⏳ resolve failed after inline retries "
                 f"(slot attempt {new_count}/{MAX_URL_RETRIES}) — will retry "
                 f"at next slot")
            p["current"] = None
            p["status"] = "scheduled"
            nxt = next_slot_after_pipeline(p, state, now_utc())
            p["next_slot"] = utc_iso(nxt)
            p["next_slot_nairobi"] = nairobi_iso(nxt)
            p["upcoming_slots"] = upcoming_slots_for_pipeline(
                p, state, min(10, (len(urls) - cursor) * 2))
            await persist(state)
            return {"pipeline": pid, "success": False,
                    "reason": "resolve_failed_retry", "attempt": new_count}

        _log(state, pid,
             f"❌ resolve failed after {new_count} slot attempts — "
             f"skipping URL")
        _clear_retry(p, source_url)
        p["cursor"] = cursor + 1
        p["failed_count"] = int(p.get("failed_count") or 0) + 1
        p["current"] = None

        if p["cursor"] >= len(urls):
            p["status"] = "done"
            p["next_slot"] = None
            p["upcoming_slots"] = []
            _log(state, pid,
                 f"🏁 done posted={p.get('posted_count', 0)} "
                 f"failed={p['failed_count']}")
        else:
            nxt = next_slot_after_pipeline(p, state, now_utc())
            p["status"] = "scheduled"
            p["next_slot"] = utc_iso(nxt)
            p["next_slot_nairobi"] = nairobi_iso(nxt)
            p["upcoming_slots"] = upcoming_slots_for_pipeline(
                p, state, min(10, (len(urls) - p["cursor"]) * 2))

        await persist(state)
        return {"pipeline": pid, "success": False, "reason": "resolve_failed"}

    video_url = result["download_url"]
    filename = result.get("filename", "video.mp4")
    _log(state, pid, f"✅ resolved → {filename}")

    # ---- 2. transcript ----
    transcript = None
    if p.get("use_transcript", True):
        cached = (p.get("resolved_transcripts") or {}).get(source_url)
        if cached:
            transcript = cached
            log(f"[transcript] cached ({len(cached)} chars)")
        else:
            p["current"] = {"stage": "transcript", "filename": filename}
            await persist(state)
            transcript = await fetch_transcript(source_url, log)
            if transcript:
                p.setdefault("resolved_transcripts", {})[source_url] = transcript

    # ---- 3. caption ----
    caption = build_caption(p, cursor, source_url, transcript)
    log(f"[caption] {caption[:100]}{'…' if len(caption) > 100 else ''}")

    # ---- 4. post to Buffer ----
    p["current"] = {"stage": "post", "filename": filename}
    await persist(state)

    ok = await post_to_buffer(state, p, video_url, source_url, caption, log)

    if not ok:
        new_count = _bump_retry(p, source_url)

        if new_count < MAX_URL_RETRIES:
            _log(state, pid,
                 f"⏳ post failed (slot attempt {new_count}/{MAX_URL_RETRIES}) "
                 f"— will retry at next slot")
            p["current"] = None
            p["status"] = "scheduled"
            nxt = next_slot_after_pipeline(p, state, now_utc())
            p["next_slot"] = utc_iso(nxt)
            p["next_slot_nairobi"] = nairobi_iso(nxt)
            p["upcoming_slots"] = upcoming_slots_for_pipeline(
                p, state, min(10, (len(urls) - cursor) * 2))
            await persist(state)
            return {"pipeline": pid, "success": False,
                    "reason": "post_failed_retry", "attempt": new_count}

        _log(state, pid,
             f"❌ post failed after {new_count} attempts — skipping URL")
        _clear_retry(p, source_url)
        p["cursor"] = cursor + 1
        p["failed_count"] = int(p.get("failed_count") or 0) + 1
        p["current"] = None

        if p["cursor"] >= len(urls):
            p["status"] = "done"
            p["next_slot"] = None
            p["upcoming_slots"] = []
            _log(state, pid,
                 f"🏁 done posted={p.get('posted_count', 0)} "
                 f"failed={p['failed_count']}")
        else:
            nxt = next_slot_after_pipeline(p, state, now_utc())
            p["status"] = "scheduled"
            p["next_slot"] = utc_iso(nxt)
            p["next_slot_nairobi"] = nairobi_iso(nxt)
            p["upcoming_slots"] = upcoming_slots_for_pipeline(
                p, state, min(10, (len(urls) - p["cursor"]) * 2))

        await persist(state)
        return {"pipeline": pid, "success": False, "reason": "post_failed"}

    # ---- success ----
    _clear_retry(p, source_url)
    p["cursor"] = cursor + 1
    p["posted_count"] = int(p.get("posted_count") or 0) + 1
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

    await persist(state)
    return {"pipeline": pid, "success": True, "filename": filename}


async def fire_pipeline_by_id(pid: str) -> dict:
    state = await load_state_from_db()
    p = None
    for x in state.get("pipelines", []):
        if x["id"] == pid:
            p = x
            break
    if not p:
        return {"pipeline": pid, "success": False, "reason": "not_found"}

    if p.get("status") not in ("scheduled", "running", "pending"):
        return {"pipeline": pid, "success": False, "reason": "not_active"}

    return await fire_one(state, p)


# ==================================================================
# Daily roll
# ==================================================================
async def daily_roll() -> dict:
    state = await load_state_from_db()
    slots = state.get("slots", {})
    today = _today_local_str()

    if slots.get("day") == today and slots.get("times"):
        return {"ok": True, "already": True, "day": today, "times": slots["times"]}

    s = reroll_slots_today(state)

    for p in state.get("pipelines", []):
        if p.get("status") in ("scheduled", "running", "pending"):
            reschedule_pipeline(p, state)

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
            p["status"] = "pending"
        if p.get("current"):
            p["current"] = None

    await persist(state)