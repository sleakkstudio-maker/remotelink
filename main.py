"""
Remote Control MVP - WebSocket Relay Server
==========================================
Acts as a relay between an Android device and a browser dashboard.
The server never sees decrypted content — it simply forwards frames
and input commands between registered sessions.

Architecture:
    Android App  <──WebSocket──>  This Server  <──WebSocket──>  Browser
                    (frames →)                    (→ frames)
                    (← input)                     (input →)
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Session store
# ──────────────────────────────────────────────────────────────────────────────

class Session:
    """Holds the two WebSocket connections that make up one remote-control session."""

    def __init__(self, device_id: str):
        self.device_id: str = device_id
        self.device_ws: Optional[WebSocket] = None
        self.viewer_ws: Optional[WebSocket] = None
        self.created_at: float = time.time()
        self.last_frame_at: float = 0
        self.frame_count: int = 0

    @property
    def device_connected(self) -> bool:
        return self.device_ws is not None

    @property
    def viewer_connected(self) -> bool:
        return self.viewer_ws is not None

    def info(self) -> dict:
        return {
            "device_id": self.device_id,
            "device_connected": self.device_connected,
            "viewer_connected": self.viewer_connected,
            "frame_count": self.frame_count,
            "age_seconds": round(time.time() - self.created_at, 1),
        }


# Global session registry  { device_id -> Session }
sessions: dict[str, Session] = {}


def get_or_create_session(device_id: str) -> Session:
    if device_id not in sessions:
        sessions[device_id] = Session(device_id)
        log.info("Session created: %s", device_id)
    return sessions[device_id]


def remove_session_if_empty(device_id: str) -> None:
    s = sessions.get(device_id)
    if s and not s.device_connected and not s.viewer_connected:
        del sessions[device_id]
        log.info("Session removed (empty): %s", device_id)


# ──────────────────────────────────────────────────────────────────────────────
# FastAPI app
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Remote Control Relay", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the web dashboard from /static
app.mount("/static", StaticFiles(directory="static"), name="static")


# ──────────────────────────────────────────────────────────────────────────────
# REST endpoints
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    """Redirect browsers to the dashboard."""
    with open("static/index.html") as f:
        return HTMLResponse(f.read())


@app.get("/health")
async def health():
    return {"status": "ok", "sessions": len(sessions)}


@app.get("/sessions")
async def list_sessions():
    return {"sessions": [s.info() for s in sessions.values()]}


@app.get("/sessions/{device_id}")
async def session_info(device_id: str):
    s = sessions.get(device_id)
    if not s:
        raise HTTPException(404, "Session not found")
    return s.info()


@app.post("/sessions/new")
async def new_session():
    """Let the Android app request a fresh device ID."""
    device_id = str(uuid.uuid4())[:8].upper()  # e.g. "A3F9B2C1"
    get_or_create_session(device_id)
    return {"device_id": device_id}


# ──────────────────────────────────────────────────────────────────────────────
# WebSocket: Android device endpoint
# ──────────────────────────────────────────────────────────────────────────────

@app.websocket("/ws/device/{device_id}")
async def device_endpoint(ws: WebSocket, device_id: str):
    """
    The Android app connects here.
    Protocol (binary):  raw JPEG frame bytes
    Protocol (text):    JSON control messages, e.g. {"type":"hello","width":1080,"height":1920}
    """
    await ws.accept()
    session = get_or_create_session(device_id)

    if session.device_ws is not None:
        await ws.close(code=4001, reason="Device already connected")
        return

    session.device_ws = ws
    log.info("[%s] Device connected", device_id)

    # Notify any waiting viewer
    if session.viewer_ws:
        try:
            await session.viewer_ws.send_text(
                json.dumps({"type": "device_connected", "device_id": device_id})
            )
        except Exception:
            pass

    try:
        while True:
            # receive_bytes or receive_text — FastAPI gives us whichever arrived
            message = await ws.receive()

            if "bytes" in message and message["bytes"]:
                # ── Raw JPEG frame → forward to viewer ──────────────────────
                frame_data = message["bytes"]
                session.frame_count += 1
                session.last_frame_at = time.time()

                if session.viewer_ws:
                    try:
                        await session.viewer_ws.send_bytes(frame_data)
                    except Exception as e:
                        log.warning("[%s] Failed to forward frame: %s", device_id, e)
                        session.viewer_ws = None

            elif "text" in message and message["text"]:
                # ── JSON control message ─────────────────────────────────────
                try:
                    data = json.loads(message["text"])
                    msg_type = data.get("type", "")
                    log.debug("[%s] Device msg: %s", device_id, msg_type)

                    if msg_type == "hello":
                        log.info(
                            "[%s] Device hello: %dx%d",
                            device_id,
                            data.get("width", 0),
                            data.get("height", 0),
                        )
                        # Echo device metadata to viewer if present
                        if session.viewer_ws:
                            await session.viewer_ws.send_text(json.dumps(data))

                except json.JSONDecodeError:
                    pass

    except WebSocketDisconnect:
        log.info("[%s] Device disconnected", device_id)
    finally:
        session.device_ws = None
        if session.viewer_ws:
            try:
                await session.viewer_ws.send_text(
                    json.dumps({"type": "device_disconnected"})
                )
            except Exception:
                pass
        remove_session_if_empty(device_id)


# ──────────────────────────────────────────────────────────────────────────────
# WebSocket: Browser viewer endpoint
# ──────────────────────────────────────────────────────────────────────────────

@app.websocket("/ws/viewer/{device_id}")
async def viewer_endpoint(ws: WebSocket, device_id: str):
    """
    The browser dashboard connects here to view a device.
    Text frames sent by the viewer are forwarded to the Android device as input
    commands (JSON):
        {"type":"tap",   "x":0.5, "y":0.3}          (normalised 0–1)
        {"type":"swipe", "x1":0.5,"y1":0.8,"x2":0.5,"y2":0.2,"duration":300}
        {"type":"key",   "keycode":4}                (Android KeyEvent codes)
        {"type":"text",  "text":"hello"}
        {"type":"back"}
        {"type":"home"}
        {"type":"recents"}
    """
    await ws.accept()
    session = get_or_create_session(device_id)

    if session.viewer_ws is not None:
        await ws.close(code=4002, reason="Viewer already connected")
        return

    session.viewer_ws = ws
    log.info("[%s] Viewer connected", device_id)

    # Tell the viewer whether the device is already online
    await ws.send_text(
        json.dumps(
            {
                "type": "session_status",
                "device_id": device_id,
                "device_connected": session.device_connected,
            }
        )
    )

    try:
        while True:
            message = await ws.receive()

            if "text" in message and message["text"]:
                # ── Input command → forward to device ────────────────────────
                raw = message["text"]
                if session.device_ws:
                    try:
                        await session.device_ws.send_text(raw)
                    except Exception as e:
                        log.warning("[%s] Failed to forward input: %s", device_id, e)
                        session.device_ws = None
                        await ws.send_text(
                            json.dumps({"type": "device_disconnected"})
                        )
                else:
                    # Device not connected — tell the viewer
                    await ws.send_text(
                        json.dumps({"type": "error", "message": "Device not connected"})
                    )

    except WebSocketDisconnect:
        log.info("[%s] Viewer disconnected", device_id)
    finally:
        session.viewer_ws = None
        remove_session_if_empty(device_id)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point (for direct `python main.py` runs)
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
