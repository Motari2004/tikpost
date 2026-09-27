import os
import requests
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, static_folder="public", static_url_path="")
CORS(app)

BUFFER_API = "https://api.buffer.com"


# ------------------------------------------------------------------
# Buffer GraphQL helper
# ------------------------------------------------------------------
def buffer(query, variables=None):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('BUFFER_API_KEY')}",
    }
    payload = {"query": query, "variables": variables or {}}

    res = requests.post(BUFFER_API, json=payload, headers=headers, timeout=60)
    print(f"[Buffer] status={res.status_code} body={res.text[:400]}")

    try:
        json_data = res.json()
    except Exception:
        raise Exception(f"Buffer returned non-JSON: {res.text[:200]}")

    if "errors" in json_data:
        messages = "; ".join(
            e.get("message", "Unknown error") for e in json_data["errors"]
        )
        raise Exception(messages)

    return json_data.get("data", {})


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True})


# ------------------------------------------------------------------
# GET channels — TikTok only
# ------------------------------------------------------------------
@app.route("/api/channels", methods=["GET"])
def get_channels():
    try:
        data = buffer("""
            query {
              account {
                organizations { id name }
              }
            }
        """)

        orgs = (data.get("account") or {}).get("organizations") or []
        all_channels = []

        for org in orgs:
            ch_data = buffer(
                """
                query GetChannels($input: ChannelsInput!) {
                  channels(input: $input) {
                    id
                    name
                    service
                    avatar
                    displayName
                    isDisconnected
                  }
                }
                """,
                {"input": {"organizationId": org["id"]}},
            )
            for c in ch_data.get("channels") or []:
                all_channels.append({**c, "organizationId": org["id"]})

        tiktok = [
            c for c in all_channels
            if c.get("service") == "tiktok" and not c.get("isDisconnected")
        ]

        return jsonify({
            "channels": tiktok,
            "tiktok": tiktok,
            "all": all_channels,
        })

    except Exception as e:
        print(f"[channels] error: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
# POST create TikTok video post
# ------------------------------------------------------------------
@app.route("/api/posts", methods=["POST"])
def create_post():
    try:
        body = request.get_json() or {}
        channel_id       = body.get("channelId")
        text             = body.get("text", "")
        video_urls       = body.get("videoUrls") or []
        mode             = body.get("mode") or "shareNow"
        due_at           = body.get("dueAt")
        thumbnail_offset = body.get("thumbnailOffset")

        if not channel_id:
            return jsonify({"error": "channelId required"}), 400
        if not video_urls:
            return jsonify({"error": "TikTok requires a video"}), 400
        if len(video_urls) > 1:
            return jsonify({"error": "TikTok allows max 1 video per post"}), 400
        if text and len(text) > 2200:
            return jsonify({"error": "TikTok caption max 2,200 characters"}), 400
        if mode == "customScheduled" and not due_at:
            return jsonify({"error": "customScheduled requires dueAt"}), 400

        assets = []
        for url in video_urls:
            if url and url.startswith("http"):
                try:
                    offset_val = int(thumbnail_offset) if thumbnail_offset is not None else 1000
                except (ValueError, TypeError):
                    offset_val = 1000
                assets.append({
                    "video": {
                        "url": url,
                        "metadata": {"thumbnailOffset": offset_val}
                    }
                })

        post_input = {
            "channelId": channel_id,
            "text": text,
            "schedulingType": "automatic",
            "mode": mode,
            "assets": assets,
        }
        if due_at:
            post_input["dueAt"] = due_at

        data = buffer(
            """
            mutation CreatePost($input: CreatePostInput!) {
              createPost(input: $input) {
                ... on PostActionSuccess {
                  post { id text status dueAt shareMode externalLink }
                }
                ... on MutationError { message }
              }
            }
            """,
            {"input": post_input},
        )

        result = data.get("createPost") or {}
        if result.get("message"):
            raise Exception(result["message"])

        post = result.get("post")
        print(f"[posts] tiktok id={post.get('id') if post else '?'} "
              f"status={post.get('status') if post else '?'}")
        return jsonify({"post": post})

    except Exception as e:
        print(f"[posts] error: {e}")
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------------
# GET recent posts
# ------------------------------------------------------------------
@app.route("/api/posts/<channel_id>", methods=["GET"])
def get_posts(channel_id):
    try:
        orgs = buffer("""
            query { account { organizations { id } } }
        """)
        orgs_list = (orgs.get("account") or {}).get("organizations") or []
        if not orgs_list:
            raise Exception("No organization found")

        organization_id = orgs_list[0]["id"]

        data = buffer(
            """
            query Posts($input: PostsInput!) {
              posts(input: $input) {
                edges {
                  node { id text status dueAt shareMode }
                }
              }
            }
            """,
            {
                "input": {
                    "organizationId": organization_id,
                    "filter": {"channelIds": [channel_id]},
                }
            },
        )

        edges = (data.get("posts") or {}).get("edges") or []
        return jsonify({"posts": [e["node"] for e in edges]})

    except Exception as e:
        print(f"[posts/{channel_id}] error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    if not os.getenv("BUFFER_API_KEY"):
        print("⚠ WARNING: BUFFER_API_KEY not set in .env file")

    port = int(os.getenv("BUFFER_PORT", "3000"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    print(f"[buffer_service] starting on http://127.0.0.1:{port}")
    app.run(host="127.0.0.1", port=port, debug=debug)