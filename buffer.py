import httpx

BUFFER_API = "https://api.buffer.com"


class BufferError(Exception):
    pass


async def _graphql(query: str, variables: dict, key: str) -> dict:
    if not key:
        raise BufferError("No Buffer API key configured")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {key}",
    }
    payload = {"query": query, "variables": variables or {}}

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(BUFFER_API, json=payload, headers=headers)

    print(f"[Buffer] status={r.status_code} body={r.text[:400]}", flush=True)

    try:
        data = r.json()
    except Exception:
        raise BufferError(f"Buffer returned non-JSON: {r.text[:200]}")

    if "errors" in data:
        msgs = "; ".join(e.get("message", "?") for e in data["errors"])
        raise BufferError(msgs)

    return data.get("data", {})


async def key_check(key: str) -> str | None:
    data = await _graphql("query { account { id email } }", {}, key)
    return (data.get("account") or {}).get("email")


async def list_tiktok_channels(key: str) -> dict:
    data = await _graphql(
        "query { account { organizations { id name } } }", {}, key,
    )
    orgs = (data.get("account") or {}).get("organizations") or []
    all_channels = []

    for org in orgs:
        ch_data = await _graphql(
            """query GetChannels($input: ChannelsInput!) {
                channels(input: $input) {
                  id name service avatar displayName isDisconnected
                }
            }""",
            {"input": {"organizationId": org["id"]}},
            key,
        )
        for c in ch_data.get("channels") or []:
            all_channels.append({**c, "organizationId": org["id"]})

    tiktok = [
        c for c in all_channels
        if c.get("service") == "tiktok" and not c.get("isDisconnected")
    ]
    return {"channels": tiktok, "tiktok": tiktok, "all": all_channels}


async def create_post(
    key: str,
    channel_id: str,
    text: str,
    video_url: str,
    mode: str = "shareNow",
    due_at: str | None = None,
    thumbnail_offset: int = 1000,
) -> dict:
    if not channel_id:
        raise BufferError("channelId required")
    if not video_url:
        raise BufferError("TikTok requires a video")
    if text and len(text) > 2200:
        raise BufferError("Caption max 2,200 characters")

    assets = [{
        "video": {
            "url": video_url,
            "metadata": {"thumbnailOffset": thumbnail_offset},
        }
    }]

    post_input = {
        "channelId": channel_id,
        "text": text,
        "schedulingType": "automatic",
        "mode": mode,
        "assets": assets,
    }
    if due_at:
        post_input["dueAt"] = due_at

    data = await _graphql(
        """mutation CreatePost($input: CreatePostInput!) {
            createPost(input: $input) {
              ... on PostActionSuccess { post { id text status dueAt } }
              ... on MutationError { message }
            }
        }""",
        {"input": post_input},
        key,
    )

    result = data.get("createPost") or {}
    if result.get("message"):
        raise BufferError(result["message"])
    return result.get("post") or {}