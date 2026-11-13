# antra-web

A phone-friendly HTTP front-end for this fork, so daily use doesn't need the Wails
GUI over VNC. Vanilla by design: one FastAPI file, one HTML file, **no Node, no
bundler, no framework** — nothing to rebuild when the app changes.

    ~/antra-build/.venv/bin/pip install fastapi uvicorn
    ANTRA_WEB_BIND=100.x.y.z ANTRA_WEB_PORT=8088 ~/antra-build/.venv/bin/python app.py

Endpoints: `POST /api/sync` (playlist sync), `POST /api/add` (single URL),
`GET /api/library`, `GET|POST|PATCH|DELETE /api/playlists`, `GET /api/usb`,
`POST /api/usb/sync`, `POST /api/usb/flush`, `GET /api/jobs/{id}/stream` (SSE).

Notes:
- Every download action shells out to the bundled `AntraBackend`, so fork fixes
  apply identically; it never re-implements fetching.
- Auth is a bearer token (`X-Token` or `?token=`) written to `~/.antra-web-token`.
- One job at a time: the backend shares a library state file.
- USB: detects removable mounts under `/media/$USER` or `/mnt/usb`, copies with
  `rsync -rt` (incremental, survives interrupted copies), and offers a flush.
