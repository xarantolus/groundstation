from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
from aiohttp import web

from . import events as E
from .bus import EventBus, run_subscriber
from .models import GroundstationConfig
from .skymap import SkymapService
from .view import ViewModel
from .waterfall_store import WaterfallStore

logger = logging.getLogger("groundstation.web")

TEMPLATE_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


class WebService:
    def __init__(
        self,
        cfg: GroundstationConfig,
        bus: EventBus,
        view: ViewModel,
        waterfalls: WaterfallStore,
        skymap: SkymapService,
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._view = view
        self._waterfalls = waterfalls
        self._skymap = skymap
        self._sockets: List[web.WebSocketResponse] = []
        self._stop = asyncio.Event()
        self._app = web.Application()
        self._app.router.add_get("/", self._index)
        self._app.router.add_get("/ws", self._ws)
        self._app.router.add_get("/static/{name}", self._static)
        self._app.router.add_get("/waterfalls/{pass_id}.png", self._waterfall)
        self._app.router.add_get("/skymap", self._skymap_page)
        self._app.router.add_get("/api/skymap", self._api_skymap)
        self._app.router.add_get("/api/skymap/tracks", self._api_skymap_tracks)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._index_html: str = ""
        self._skymap_html: str = ""

    async def run(self) -> None:
        self._index_html = (TEMPLATE_DIR / "index.html").read_text(encoding="utf-8")
        self._skymap_html = (TEMPLATE_DIR / "skymap.html").read_text(encoding="utf-8")
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
            E.SkymapUpdated,
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

    def _decorate_pass(self, pass_dict: Dict[str, Any]) -> None:
        pid = pass_dict.get("id")
        pass_dict["has_waterfall"] = bool(pid) and self._waterfalls.has(pid)

    def _decorate_event_payload(self, event_dict: Dict[str, Any]) -> None:
        if "pass" in event_dict and isinstance(event_dict["pass"], dict):
            self._decorate_pass(event_dict["pass"])
        passes = event_dict.get("passes")
        if isinstance(passes, list):
            for p in passes:
                if isinstance(p, dict):
                    self._decorate_pass(p)

    async def _broadcast_event(self, event: E.Event) -> None:
        if not self._sockets:
            return
        event_dict = event.model_dump(mode="json", by_alias=True)
        self._decorate_event_payload(event_dict)
        payload = {"kind": "event", "event": event_dict}
        message = json.dumps(payload, default=str)
        stale: List[web.WebSocketResponse] = []
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

    async def _static(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info["name"]
        if not _SAFE_NAME.match(name):
            raise web.HTTPNotFound()
        path = STATIC_DIR / name
        if not path.is_file():
            raise web.HTTPNotFound()
        return web.FileResponse(path, headers={"Cache-Control": "public, max-age=86400"})

    async def _waterfall(self, request: web.Request) -> web.StreamResponse:
        pass_id = request.match_info["pass_id"]
        if not _SAFE_NAME.match(pass_id):
            raise web.HTTPNotFound()
        path = self._waterfalls.path_for(pass_id)
        if path is None:
            raise web.HTTPNotFound()
        return web.FileResponse(path, headers={"Cache-Control": "public, max-age=86400"})

    async def _skymap_page(self, request: web.Request) -> web.Response:
        return web.Response(text=self._skymap_html, content_type="text/html")

    async def _api_skymap(self, request: web.Request) -> web.Response:
        sat = request.query.get("satellite") or None
        payload = self._skymap.aggregate(satellite=sat)
        payload["station"] = {
            "lat": self._cfg.location_lat,
            "lon": self._cfg.location_lon,
            "alt_m": self._cfg.location_alt,
        }
        payload["pass_elevation_threshold_deg"] = (
            self._cfg.pass_elevation_threshold_deg
        )
        return web.json_response(payload, dumps=lambda o: json.dumps(o, default=str))

    async def _api_skymap_tracks(self, request: web.Request) -> web.Response:
        sat = request.query.get("satellite") or None
        try:
            limit = int(request.query.get("limit", "30"))
        except ValueError:
            limit = 30
        limit = max(1, min(limit, 200))
        tracks = self._skymap.recent_tracks(satellite=sat, limit=limit)
        return web.json_response({"tracks": tracks}, dumps=lambda o: json.dumps(o, default=str))

    async def _ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30.0)
        await ws.prepare(request)
        self._sockets.append(ws)
        snap = self._view.snapshot().model_dump(mode="json")
        for p in snap.get("passes", []):
            if isinstance(p, dict):
                self._decorate_pass(p)
        snap["station"] = {
            "lat": self._cfg.location_lat,
            "lon": self._cfg.location_lon,
            "alt_m": self._cfg.location_alt,
        }
        snap["pass_elevation_threshold_deg"] = self._cfg.pass_elevation_threshold_deg
        snapshot = {"kind": "snapshot", "snapshot": snap}
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
