"""
Antra Web — HTTP front-end for the Antra fork on the macmini box.

Vanilla only: no Node, no bundler, no framework. One FastAPI file + one HTML file.

Responsibilities:
  * drive the bundled AntraBackend binary (never re-implement downloading)
  * playlist tracking / incremental sync via the fork's own --auto-sync
  * USB: detect removable media, let the user confirm, then sync the library onto it
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
import uvicorn

HOME = Path(os.environ.get("HOME", "/home/mike"))
BACKEND = HOME / "antra-build" / "antra-wails" / "runtime" / "backend" / "AntraBackend"
CONFIG = HOME / ".local" / "share" / "Antra" / "config.json"
LIBRARY = Path(os.environ.get("ANTRA_LIBRARY", "/srv/media/music"))
TOKEN_FILE = HOME / ".antra-web-token"
PORT = int(os.environ.get("ANTRA_WEB_PORT", "8088"))
BIND = os.environ.get("ANTRA_WEB_BIND", "100.120.74.69")

BACKEND_ENV = {
    "SLSKD_EXTERNAL_URL": os.environ.get("SLSKD_EXTERNAL_URL", "http://127.0.0.1:5030"),
    "SLSKD_EXTERNAL_API_KEY": os.environ.get("SLSKD_EXTERNAL_API_KEY", ""),
    "SLSKD_AUTO_BOOTSTRAP": "false",
    "SOURCES_ENABLED": os.environ.get("SOURCES_ENABLED", "soulseek"),
    "ANTRA_MAX_WORKERS": os.environ.get("ANTRA_MAX_WORKERS", "3"),
    "PYTHONUTF8": "1",
}

app = FastAPI(title="Antra Web")
_lock = threading.Lock()
jobs: Dict[str, Dict[str, Any]] = {}
_job_log: Dict[str, Deque[str]] = {}
_current_job: Optional[str] = None
_library_cache: Dict[str, Any] = {"at": 0.0, "items": []}


# ── auth ──────────────────────────────────────────────────────────────────────

def token() -> str:
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text().strip()
    t = uuid.uuid4().hex
    TOKEN_FILE.write_text(t)
    TOKEN_FILE.chmod(0o600)
    return t


AUTH = token()


def check(token_hdr: Optional[str], q: Optional[str]) -> None:
    if (token_hdr or q or "").strip() != AUTH:
        raise HTTPException(status_code=401, detail="bad or missing token")


# ── config ────────────────────────────────────────────────────────────────────

def read_config() -> Dict[str, Any]:
    try:
        return json.loads(CONFIG.read_text())
    except Exception:
        return {}


def write_config(data: Dict[str, Any]) -> None:
    tmp = CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    tmp.replace(CONFIG)


def tracked() -> List[Dict[str, Any]]:
    return read_config().get("tracked_playlists") or []


def save_tracked(entries: List[Dict[str, Any]]) -> None:
    cfg = read_config()
    cfg["tracked_playlists"] = entries
    write_config(cfg)


# ── jobs (one at a time: the backend shares a library state file) ─────────────

def _run_job(job_id: str, argv: List[str], env_extra: Optional[Dict[str, str]] = None) -> None:
    global _current_job
    _job_log[job_id] = deque(maxlen=4000)
    jobs[job_id].update({"status": "running", "started": time.time(), "events": 0})
    try:
        env = dict(os.environ)
        env.update(BACKEND_ENV)
        env.update(env_extra or {})
        env["ANTRA_CONFIG_PATH"] = str(CONFIG)
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, bufsize=1, env=env, cwd=str(HOME))
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            _job_log[job_id].append(line)
            jobs[job_id]["events"] += 1
            if '"playlist_summary"' in line or '"auto_sync_summary"' in line:
                try:
                    jobs[job_id]["summary"] = json.loads(line)
                except Exception:
                    pass
        proc.wait()
        jobs[job_id]["exit"] = proc.returncode
        jobs[job_id]["status"] = "done" if proc.returncode == 0 else "failed"
    except Exception as exc:
        _job_log[job_id].append(json.dumps({"type": "log", "level": "error", "message": str(exc)}))
        jobs[job_id].update({"status": "failed", "exit": -1})
    finally:
        jobs[job_id]["finished"] = time.time()
        with _lock:
            _current_job = None


def start_job(argv: List[str], kind: str, label: str,
              env_extra: Optional[Dict[str, str]] = None) -> str:
    global _current_job
    with _lock:
        if _current_job and jobs.get(_current_job, {}).get("status") == "running":
            raise HTTPException(status_code=409, detail="another job is running")
        jid = uuid.uuid4().hex[:12]
        _current_job = jid
    jobs[jid] = {"id": jid, "kind": kind, "label": label, "summary": None, "exit": None}
    threading.Thread(target=_run_job, args=(jid, argv, env_extra), daemon=True).start()
    return jid


def start_backend(args: List[str], kind: str, label: str) -> str:
    return start_job([str(BACKEND), *args, "--config", str(CONFIG)], kind, label)


# ── library ───────────────────────────────────────────────────────────────────

def scan_library(max_age: float = 30.0) -> List[Dict[str, Any]]:
    now = time.time()
    if now - _library_cache["at"] < max_age and _library_cache["items"]:
        return _library_cache["items"]
    items: List[Dict[str, Any]] = []
    try:
        from mutagen import File as MutagenFile
    except Exception:
        MutagenFile = None  # type: ignore
    for path in sorted(LIBRARY.rglob("*.mp3")):
        rel = path.relative_to(LIBRARY)
        item = {"folder": str(rel.parent), "file": rel.name, "size": path.stat().st_size,
                "title": path.stem, "artist": "", "album": "", "duration": 0}
        if MutagenFile is not None:
            try:
                m = MutagenFile(str(path), easy=True)
                if m:
                    item["title"] = (m.get("title") or [path.stem])[0]
                    item["artist"] = (m.get("artist") or [""])[0]
                    item["album"] = (m.get("album") or [""])[0]
                    item["duration"] = int(m.info.length) if getattr(m, "info", None) else 0
            except Exception:
                pass
        items.append(item)
    _library_cache.update({"at": now, "items": items})
    return items


# ── USB ───────────────────────────────────────────────────────────────────────

def usb_devices() -> List[Dict[str, Any]]:
    """Removable, mounted block devices — from lsblk, no extra deps."""
    try:
        raw = subprocess.run(["lsblk", "-J", "-o", "NAME,LABEL,FSTYPE,SIZE,RM,TRAN,MOUNTPOINT,TYPE"],
                             capture_output=True, text=True, timeout=10).stdout
        data = json.loads(raw or "{}")
    except Exception:
        return []

    found: List[Dict[str, Any]] = []

    removable_roots = [f"/media/{os.environ.get('USER', 'mike')}", "/mnt/usb"]

    def is_removish(mp: Optional[str]) -> bool:
        if not mp:
            return False
        return any(mp == r or mp.startswith(r + "/") for r in removable_roots)

    def walk(node: Dict[str, Any], parent_removable: bool = False) -> None:
        removable = parent_removable or node.get("rm") is True or node.get("tran") == "usb"
        mp = node.get("mountpoint")
        # udisks mounts sticks under /media/$USER/<LABEL>; that path is a more
        # reliable signal than the rm flag on virtualised/odd hardware.
        if node.get("type") in ("part", "disk", "loop") and mp and (removable or is_removish(mp)):
            found.append({
                "device": "/dev/" + node.get("name", ""),
                "label": node.get("label") or node.get("name"),
                "fstype": node.get("fstype"),
                "size": node.get("size"),
                "mountpoint": mp,
                "writable": os.access(mp, os.W_OK),
            })
        for child in node.get("children") or []:
            walk(child, removable)

    for dev in data.get("blockdevices") or []:
        walk(dev)
    return found


def dir_stats(path: Path) -> Dict[str, Any]:
    mp3s, total = 0, 0
    try:
        for f in path.rglob("*.mp3"):
            mp3s += 1
            total += f.stat().st_size
    except Exception:
        pass
    try:
        usage = shutil.disk_usage(path)
        free, capacity = usage.free, usage.total
    except Exception:
        free, capacity = 0, 0
    return {"mp3_count": mp3s, "mp3_bytes": total, "free": free, "capacity": capacity}


@app.get("/api/usb")
def api_usb(token_q: Optional[str] = None, x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    lib = dir_stats(LIBRARY)
    devices = []
    for d in usb_devices():
        mp = Path(d["mountpoint"])
        stats = dir_stats(mp)
        d["existing_mp3"] = stats["mp3_count"]
        d["free"] = stats["free"]
        d["capacity"] = stats["capacity"]
        d["needs_bytes"] = max(0, lib["mp3_bytes"] - stats["mp3_bytes"])
        d["fits"] = stats["free"] >= d["needs_bytes"]
        devices.append(d)
    return {"devices": devices, "library": lib}


@app.post("/api/usb/sync")
async def api_usb_sync(request: Request, token_q: Optional[str] = None,
                       x_token: Optional[str] = Header(default=None)):
    """Copy the library onto a removable device (incremental)."""
    check(x_token, token_q)
    body = await request.json()
    mountpoint = (body.get("mountpoint") or "").strip()
    delete_extras = bool(body.get("delete_extras"))

    known = {d["mountpoint"]: d for d in usb_devices()}
    if mountpoint not in known:
        raise HTTPException(status_code=400, detail="that device is not a mounted removable disk")
    if not known[mountpoint]["writable"]:
        raise HTTPException(status_code=400, detail="device is not writable")

    target = Path(mountpoint) / "Music"
    if not shutil.which("rsync"):
        raise HTTPException(status_code=500, detail="rsync not installed on the server")

    argv = ["rsync", "-rt", "--info=progress2", "--no-inc-recursive",
            "--exclude", ".antra_state.json",
            str(LIBRARY) + "/", str(target) + "/"]
    if delete_extras:
        argv.insert(2, "--delete")
    return {"job": start_job(argv, "usb", f"sync to {known[mountpoint]['label']}")}


@app.post("/api/usb/flush")
def api_usb_flush(token_q: Optional[str] = None, x_token: Optional[str] = Header(default=None)):
    """Flush write buffers so the stick can be pulled out safely."""
    check(x_token, token_q)
    subprocess.run(["sync"], timeout=60)
    return {"ok": True}


# ── routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return (Path(__file__).parent / "index.html").read_text()


@app.get("/api/state")
def state(token_q: Optional[str] = None, x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    running = _current_job if _current_job and jobs.get(_current_job, {}).get("status") == "running" else None
    return {
        "tracked": tracked(),
        "job": jobs.get(running) if running else None,
        "library_count": len(scan_library()),
        "config": {"output_format": read_config().get("output_format"),
                   "download_path": read_config().get("download_path")},
    }


@app.get("/api/library")
def library(token_q: Optional[str] = None, x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    items = scan_library(max_age=0)
    return {"count": len(items), "items": items}


@app.post("/api/playlists")
async def add_playlist(request: Request, token_q: Optional[str] = None,
                       x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    body = await request.json()
    url = (body.get("url") or "").strip()
    if not re.match(r"^https?://", url):
        raise HTTPException(status_code=400, detail="url must start with http(s)://")
    entries = tracked()
    if any((e.get("url") or "").strip() == url for e in entries):
        raise HTTPException(status_code=409, detail="already tracked")
    entries.append({"url": url, "name": (body.get("name") or "").strip() or None,
                    "sync_enabled": True, "last_track_ids": [], "last_sync_ts": None})
    save_tracked(entries)
    return {"tracked": entries}


@app.patch("/api/playlists")
async def update_playlist(request: Request, token_q: Optional[str] = None,
                          x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    body = await request.json()
    url = (body.get("url") or "").strip()
    entries = tracked()
    hit = False
    for e in entries:
        if (e.get("url") or "").strip() == url:
            hit = True
            if "sync_enabled" in body:
                e["sync_enabled"] = bool(body["sync_enabled"])
            if body.get("name") is not None:
                e["name"] = body["name"]
    if not hit:
        raise HTTPException(status_code=404, detail="not tracked")
    save_tracked(entries)
    return {"tracked": entries}


@app.delete("/api/playlists")
async def delete_playlist(request: Request, token_q: Optional[str] = None,
                          x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    body = await request.json()
    url = (body.get("url") or "").strip()
    entries = [e for e in tracked() if (e.get("url") or "").strip() != url]
    save_tracked(entries)
    return {"tracked": entries}


@app.post("/api/sync")
def sync(token_q: Optional[str] = None, x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    if not [e for e in tracked() if e.get("sync_enabled", True)]:
        raise HTTPException(status_code=400, detail="no enabled playlists to sync")
    return {"job": start_backend(["--auto-sync"], "sync", "playlist sync")}


@app.post("/api/add")
async def add_track(request: Request, token_q: Optional[str] = None,
                    x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    body = await request.json()
    url = (body.get("url") or "").strip()
    if not re.match(r"^https?://", url):
        raise HTTPException(status_code=400, detail="url must start with http(s)://")
    return {"job": start_backend([url], "add", "add " + url[:60])}


@app.get("/api/jobs/{job_id}")
def job_state(job_id: str, token_q: Optional[str] = None,
              x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="unknown job")
    return jobs[job_id]


@app.get("/api/jobs/{job_id}/stream")
def job_stream(job_id: str, token_q: Optional[str] = None,
               x_token: Optional[str] = Header(default=None)):
    check(x_token, token_q)
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="unknown job")

    def gen():
        sent = 0
        while True:
            lines = list(_job_log.get(job_id, []))
            if len(lines) > sent:
                for line in lines[sent:]:
                    yield f"data: {line}\n\n"
                sent = len(lines)
            st = jobs.get(job_id, {})
            if st.get("status") in ("done", "failed"):
                yield "data: " + json.dumps({"type": "job_finished", "status": st.get("status"),
                                             "exit": st.get("exit"), "summary": st.get("summary")}) + "\n\n"
                return
            time.sleep(0.7)

    return StreamingResponse(gen(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(app, host=BIND, port=PORT, log_level="info")
