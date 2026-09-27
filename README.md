# tikpost

TikTok → Buffer → Twitter pipeline with a web UI.

## Architecture

- **FastAPI** (`:8000`) — serves the UI, runs the pipeline, proxies to Buffer service
- **Flask** (`:3000`) — talks to Buffer's GraphQL API
- **tiktokresolver** (Render) — resolves TikTok URLs to direct MP4 links

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt"# tikpost" 
"# tikpost" 
