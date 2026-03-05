"""FastAPI server for parsebox demo mode.

Serves the static frontend and provides WebSocket endpoints for
streaming agent conversations. All agent sessions run in demo mode
(restricted tools, no code execution).
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from parsebox.agent.context import DatasetContext
from parsebox.agent.session import AgentSession
from parsebox.dataset import DatasetManager
from parsebox.identity import get_or_create_user_id
from parsebox.storage import LocalStorage

logger = logging.getLogger(__name__)

# -- Config -------------------------------------------------------------------

MAX_MESSAGES_PER_SESSION = 30
MAX_CONCURRENT_SESSIONS = 10
SAMPLE_DATA_DIR = Path(os.environ.get("PARSEBOX_SAMPLE_DATA", Path(__file__).resolve().parent.parent.parent / "sample_data"))

SAMPLE_DATASETS = [
    {
        "key": "s1",
        "name": "Startup Employees",
        "description": "Employee records from a tech startup",
        "folder": "startup_employees",
        "file_count": 10,
    },
    {
        "key": "s2",
        "name": "VC Deal Memos",
        "description": "Venture capital investment memos",
        "folder": "vc_deal_memos",
        "file_count": 13,
    },
]

# -- State --------------------------------------------------------------------

_sessions: dict[str, dict[str, Any]] = {}  # session_id -> {session, ctx, ...}
_storage = None
_manager = None
_user_id = None


def _ensure_globals():
    global _storage, _manager, _user_id
    if _storage is None:
        _storage = LocalStorage()
        _manager = DatasetManager(_storage)
        _user_id = get_or_create_user_id()


# -- App ----------------------------------------------------------------------

app = FastAPI(title="parsebox", version="0.2.0")

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/chat")
async def chat_page():
    return FileResponse(str(STATIC_DIR / "chat.html"))


@app.get("/api/samples")
async def list_samples():
    available = []
    for s in SAMPLE_DATASETS:
        folder = SAMPLE_DATA_DIR / s["folder"]
        if folder.exists():
            available.append(s)
    return JSONResponse({"samples": available})


@app.get("/api/sessions")
async def list_sessions():
    """Return active sessions that can be resumed."""
    sessions = []
    for sid, state in _sessions.items():
        sessions.append({
            "session_id": sid,
            "dataset_name": state["dataset_name"],
            "message_count": state["message_count"],
            "messages_remaining": MAX_MESSAGES_PER_SESSION - state["message_count"],
            "created_at": state["created_at"],
            "status": state["ctx"].dataset.status,
        })
    return JSONResponse({"sessions": sessions})


@app.get("/api/sessions/{session_id}/messages")
async def get_session_messages(session_id: str):
    """Return the message history for a session (for reconnect replay)."""
    if session_id not in _sessions:
        return JSONResponse({"error": "Session not found"}, status_code=404)
    state = _sessions[session_id]
    return JSONResponse({"messages": state["history"]})


@app.post("/api/sessions")
async def create_session(body: dict):
    _ensure_globals()

    if len(_sessions) >= MAX_CONCURRENT_SESSIONS:
        # Evict oldest session
        oldest_id = next(iter(_sessions))
        await _cleanup_session(oldest_id)

    dataset_key = body.get("dataset", "s1")
    sample = next((s for s in SAMPLE_DATASETS if s["key"] == dataset_key), None)
    if not sample:
        return JSONResponse({"error": "Unknown dataset"}, status_code=400)

    folder = SAMPLE_DATA_DIR / sample["folder"]
    if not folder.exists():
        return JSONResponse({"error": "Sample data not found"}, status_code=404)

    # Create dataset
    dataset = _manager.create_dataset(sample["name"], _user_id)
    dataset.source_folder = str(folder)
    _storage.save(_user_id, dataset)

    ctx = DatasetContext(
        user_id=_user_id,
        dataset=dataset,
        storage=_storage,
        manager=_manager,
        source_folder=str(folder),
        demo_mode=True,
    )

    session = AgentSession(ctx)
    try:
        await session.start()
    except Exception as e:
        logger.error("Failed to start agent session: %s", e, exc_info=True)
        return JSONResponse({"error": f"Agent startup failed: {str(e)[:300]}"}, status_code=500)

    session_id = str(uuid.uuid4())[:8]
    _sessions[session_id] = {
        "session": session,
        "ctx": ctx,
        "message_count": 0,
        "dataset_name": sample["name"],
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "history": [],  # [{role: "user"|"assistant", content: str}]
    }

    logger.info("Created session %s for dataset '%s'", session_id, sample["name"])

    return JSONResponse({
        "session_id": session_id,
        "dataset_name": sample["name"],
        "message_limit": MAX_MESSAGES_PER_SESSION,
    })


@app.delete("/api/sessions/{session_id}")
async def delete_session(session_id: str):
    """Explicitly destroy a session."""
    if session_id not in _sessions:
        return JSONResponse({"error": "Session not found"}, status_code=404)
    await _cleanup_session(session_id)
    return JSONResponse({"ok": True})


@app.websocket("/api/chat/{session_id}")
async def websocket_chat(ws: WebSocket, session_id: str):
    await ws.accept()

    if session_id not in _sessions:
        await ws.send_json({"type": "error", "content": "Session not found. Please create a new session."})
        await ws.close()
        return

    state = _sessions[session_id]
    session: AgentSession = state["session"]
    ctx: DatasetContext = state["ctx"]

    try:
        while True:
            data = await ws.receive_json()
            message = data.get("message", "").strip()
            if not message:
                continue

            # Check message limit
            state["message_count"] += 1
            if state["message_count"] > MAX_MESSAGES_PER_SESSION:
                await ws.send_json({
                    "type": "error",
                    "content": f"Session message limit reached ({MAX_MESSAGES_PER_SESSION}). Please start a new session.",
                })
                continue

            # Record user message
            state["history"].append({"role": "user", "content": message})

            # Stream agent response
            try:
                text_buffer = []
                full_response_parts = []
                async for event_type, content in session.send(message):
                    if event_type == "text":
                        text_buffer.append(content)
                        full_response_parts.append(content)
                    elif event_type == "tool_use":
                        # Flush text before tool indicator
                        if text_buffer:
                            await ws.send_json({"type": "text", "content": "".join(text_buffer)})
                            text_buffer = []
                        # Filter hidden tools
                        short_name = content.split("__")[-1] if "__" in content else content
                        _HIDDEN = {"ToolSearch", "ToolResult", "TaskStop", "TaskCreate", "TaskGet"}
                        if short_name not in _HIDDEN:
                            await ws.send_json({"type": "tool_use", "tool": short_name})
                    elif event_type == "done":
                        if text_buffer:
                            await ws.send_json({"type": "text", "content": "".join(text_buffer)})
                            text_buffer = []

                # Flush any remaining
                if text_buffer:
                    await ws.send_json({"type": "text", "content": "".join(text_buffer)})

                # Record assistant response
                full_response = "".join(full_response_parts)
                if full_response.strip():
                    state["history"].append({"role": "assistant", "content": full_response})

                # Send suggestions based on dataset state
                ctx.reload_dataset()
                suggestions = _get_suggestions(ctx)
                await ws.send_json({
                    "type": "done",
                    "status": ctx.dataset.status,
                    "messages_remaining": MAX_MESSAGES_PER_SESSION - state["message_count"],
                    "suggestions": suggestions,
                })

            except Exception as e:
                logger.error("Agent error in session %s: %s", session_id, e, exc_info=True)
                await ws.send_json({"type": "error", "content": f"Agent error: {str(e)[:200]}"})

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for session %s (session preserved)", session_id)
        # Session stays alive -- user can reconnect later


def _get_suggestions(ctx: DatasetContext) -> list[str]:
    """Return contextual suggested next messages based on dataset state."""
    status = ctx.dataset.status

    if status == "draft" and not ctx.dataset.documents:
        return []  # Agent is auto-exploring

    if status == "draft" and ctx.dataset.documents:
        return [
            "Propose a schema based on what you found",
            "What types of data are in these files?",
        ]

    if status == "schema_ready":
        return [
            "Extract a sample to preview the results",
            "Show me the current schema",
            "Add a field for ...",
        ]

    if status == "previewed":
        return [
            "Looks good! The data is ready to query",
            "Change the schema and re-extract",
            "Show the extraction results again",
        ]

    if status == "extracted":
        return [
            "Show me all the data",
            "What are the most interesting patterns?",
            "Summarize the key findings",
        ]

    return []


async def _cleanup_session(session_id: str):
    if session_id in _sessions:
        state = _sessions.pop(session_id)
        try:
            await state["session"].stop()
        except Exception:
            pass
        logger.info("Cleaned up session %s", session_id)


def create_app():
    """Factory for the demo web app."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("anthropic").setLevel(logging.WARNING)
    return app
