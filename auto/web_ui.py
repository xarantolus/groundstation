import asyncio
import logging
import os
import aiohttp.web
import jinja2
from typing import List, Dict, Any, Optional
import datetime
from common import Satellite, PassInfo

logger = logging.getLogger("groundstation.web")

class WebServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 80, template_dir: str = "auto/templates"):
        self.host = host
        self.port = port
        self.app = aiohttp.web.Application()
        self.runner: Optional[aiohttp.web.AppRunner] = None
        self.site: Optional[aiohttp.web.TCPSite] = None
        self.websockets: List[aiohttp.web.WebSocketResponse] = []

        # State cache for new connections
        self.last_passes_html = ""
        self.last_transfers_html = ""
        self.log_history = []  # Store last N log entries (HTML)

        # Setup Jinja2 environment
        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(template_dir),
            autoescape=jinja2.select_autoescape(['html', 'xml'])
        )
        self.template = self.jinja_env.get_template("index.html")

        # Routes
        self.app.router.add_get("/", self.index_handler)
        self.app.router.add_get("/ws", self.websocket_handler)

    async def start(self):
        """Starts the web server."""
        self.runner = aiohttp.web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        self.site = aiohttp.web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logger.info(f"Web server started at http://{self.host}:{self.port}")

    async def stop(self):
        """Stops the web server."""
        if self.runner:
            await self.runner.cleanup()

    async def index_handler(self, request):
        """Serves the main page with initial state."""
        # Use cached HTML if available, or empty instructions
        logs_html = "".join(self.log_history) if self.log_history else '<div class="log-entry">Waiting for logs...</div>'

        content = self.template.render(
            passes_html=self.last_passes_html or "<div>No upcoming overpasses found.</div>",
            transfers_html=self.last_transfers_html or '<div class="transfer-item">No active transfers.</div>',
            logs_html=logs_html
        )
        return aiohttp.web.Response(text=content, content_type="text/html")

    async def websocket_handler(self, request):
        """Handles WebSocket connections."""
        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(request)

        self.websockets.append(ws)
        try:
            async for msg in ws:
                pass  # We primarily send data, not receive
        finally:
            self.websockets.remove(ws)
        return ws

    async def broadcast(self, message: Dict[str, Any]):
        """Sends a JSON message to all connected clients."""
        if not self.websockets:
            return

        # Prune closed connections
        self.websockets = [ws for ws in self.websockets if not ws.closed]

        for ws in self.websockets:
            try:
                await ws.send_json(message)
            except Exception as e:
                logger.error(f"Failed to send to websocket: {e}")

    def generate_passes_html(self, passes: List[Any], now: datetime.datetime) -> str:
        """Generates the HTML table for passes."""
        if not passes:
            return "<div>No upcoming overpasses.</div>"

        html_parts = ['<table><thead><tr><th>Satellite</th><th>Start</th><th>Duration</th><th>Max El.</th><th>Direction</th></tr></thead><tbody>']

        # visible_passes has (sat, pass_info) tuples
        for sat, p in passes:
             if p["end_time"] > now:
                is_live = p["start_time"] < now < p["end_time"]
                start_str = p["start_time"].strftime("%H:%M:%S")
                start_class = 'class="live-pass"' if is_live else ''
                start_display = f"{start_str} (LIVE)" if is_live else start_str

                # Directions
                dirs = [
                    self._az_to_dir(p['start_azimuth']),
                    self._az_to_dir(p['max_azimuth']),
                    self._az_to_dir(p['end_azimuth'])
                ]
                dir_str = " -> ".join(dirs)

                html_parts.append(f"""
                    <tr>
                        <td>{sat['name']}</td>
                        <td {start_class}>{start_display}</td>
                        <td>{p['duration_minutes']:.1f}m</td>
                        <td>{p['max_elevation']:.1f}°</td>
                        <td>{dir_str}</td>
                    </tr>
                """)

        html_parts.append('</tbody></table>')
        return "".join(html_parts)

    def _az_to_dir(self, azimuth: float) -> str:
        # Simple helper strictly for this view
        val = int((azimuth / 22.5) + 0.5)
        arr = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        return arr[(val % 16) // 2] # Map 16 zones to 8 directions? Or just use basic 8 logic.
        # Original logic:
        # if 0 <= azimuth < 22.5 or 337.5 <= azimuth < 360: return "N" ...
        # Simplified for brevity here mirroring tracker.py logic roughly
        sectors = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        idx = int((azimuth + 22.5) % 360 / 45)
        return sectors[idx]

    async def update_passes(self, passes: List[Any]):
        """Updates the passes section."""
        now = datetime.datetime.now()
        html = self.generate_passes_html(passes, now)
        if html != self.last_passes_html:
            self.last_passes_html = html
            await self.broadcast({"target": "passes-content", "html": html})

    async def update_transfers(self, active: Dict, completed: List):
        """Updates the transfers section."""
        html_parts = []

        if active:
            for path, data in active.items():
                fname = os.path.basename(path)
                prog = data["progress"]
                html_parts.append(f"""
                    <div class="transfer-item">
                        <span class="transfer-name">{fname}</span>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: {prog}%;"></div>
                        </div>
                        <div style="font-size: 0.8em; text-align: right; margin-top: 2px;">{prog:.0f}%</div>
                    </div>
                """)
        else:
            html_parts.append('<div class="transfer-item" style="background: none; padding-left: 0;">No active transfers.</div>')

        if completed:
            html_parts.append('<div style="margin-top: 15px; font-weight: bold; margin-bottom: 5px;">Recent Transfers:</div>')
            for item in reversed(completed): # Show newest first
                status_class = "status-ok" if item["status"] == "Completed" else "status-err"
                time_str = datetime.datetime.fromtimestamp(item["time"]).strftime("%H:%M:%S")
                # Using simple HTML construction
                html_parts.append(f"""
                    <div class="completed-transfer">
                        <span class="{status_class}">✓</span> {item['filename']}
                        <span style="float: right;">{time_str}</span>
                    </div>
                """)

        html = "".join(html_parts)
        if html != self.last_transfers_html:
            self.last_transfers_html = html
            await self.broadcast({"target": "transfers-content", "html": html})

    async def log(self, record):
        """Broadcasts a log record."""
        # Simple HTML escape for the message
        import html
        msg = html.escape(record.getMessage())
        time_str = datetime.datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        levelname = record.levelname

        # log entry html
        entry_html = f'<div class="log-entry log-{levelname}"><span class="log-time">{time_str}</span>{msg}</div>'

        self.log_history.append(entry_html)
        if len(self.log_history) > 500:
            self.log_history.pop(0)

        await self.broadcast({"target": "logs-content", "method": "append", "html": entry_html})
