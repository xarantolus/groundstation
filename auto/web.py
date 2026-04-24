from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import List

import aiohttp
from aiohttp import web

from . import events as E
from .bus import EventBus, run_subscriber
from .models import GroundstationConfig
from .view import ViewModel

logger = logging.getLogger("groundstation.web")

TEMPLATE_DIR = Path(__file__).parent / "templates"


class WebService:
    def __init__(self, cfg: GroundstationConfig, bus: EventBus, view: ViewModel) -> None:
        self._cfg = cfg
        self._bus = bus
        self._view = view
        self._sockets: List[web.WebSocketResponse] = []
        self._stop = asyncio.Event()
        self._app = web.Application()
        self._app.router.add_get("/", self._index)
        self._app.router.add_get("/ws", self._ws)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._index_html: str = ""

    async def run(self) -> None:
        self._index_html = (TEMPLATE_DIR / "index.html").read_text(encoding="utf-8")
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self._cfg.web_host, self._cfg.web_port)
        await self._site.start()
        logger.info("web server on http://%s:%d", self._cfg.web_host, self._cfg.web_port)

        sub = self._bus.subscribe(
            E.PassPredicted,
            E.PassesTableChanged,
            E.PassUpcoming,
            E.PassStarted,
            E.PassEnded,
            E.RecordingStarted,
            E.RecordingCompleted,
            E.RecordingFailed,
            E.DecodeQueued,
            E.DecodeStarted,
            E.DecodeCompleted,
            E.DecodeFailed,
            E.DecodeLog,
            E.TransferQueued,
            E.TransferStarted,
            E.TransferProgress,
            E.TransferCompleted,
            E.TransferFailed,
            E.LogMessage,
            name="web.events",
            queue_size=2048,
            overflow="drop_oldest",
        )
        sub_task = asyncio.create_task(run_subscriber(sub, self._broadcast_event, "web.events"))

        try:
            await self._stop.wait()
        finally:
            sub.close()
            sub_task.cancel()
            try:
                await sub_task
            except asyncio.CancelledError:
                pass
            for ws in list(self._sockets):
                await ws.close()
            if self._runner:
                await self._runner.cleanup()

    def stop(self) -> None:
        self._stop.set()

    async def _broadcast_event(self, event: E.Event) -> None:
        if not self._sockets:
            return
        payload = {"kind": "event", "event": event.model_dump(mode="json", by_alias=True)}
        message = json.dumps(payload, default=str)
        stale: List[web.WebSocketResponse] = []
        # Snapshot — new clients can connect mid-broadcast and mutations to
        # the underlying list would otherwise skip/repeat entries.
        for ws in list(self._sockets):
            if ws.closed:
                stale.append(ws)
                continue
            try:
                await ws.send_str(message)
            except ConnectionResetError:
                stale.append(ws)
            except Exception:
                logger.exception("failed to send event to websocket")
                stale.append(ws)
        for ws in stale:
            try:
                self._sockets.remove(ws)
            except ValueError:
                pass

    async def _index(self, request: web.Request) -> web.Response:
        return web.Response(text=self._index_html, content_type="text/html")

    async def _ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30.0)
        await ws.prepare(request)
        self._sockets.append(ws)
        snapshot = {
            "kind": "snapshot",
            "snapshot": self._view.snapshot().model_dump(mode="json"),
        }
        try:
            await ws.send_str(json.dumps(snapshot, default=str))
        except Exception:
            logger.exception("failed to send initial snapshot")

        try:
            async for _ in ws:
                pass
        finally:
            try:
                self._sockets.remove(ws)
            except ValueError:
                pass
        return ws
