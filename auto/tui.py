from __future__ import annotations

import asyncio
import datetime
import logging
import os
from typing import Optional

from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .bus import EventBus
from .pass_predictor import azimuth_to_compass
from .view import ViewModel

logger = logging.getLogger("groundstation.tui")


class TUIService:
    def __init__(self, bus: EventBus, view: ViewModel, refresh_per_second: int = 4) -> None:
        self._bus = bus
        self._view = view
        self._refresh = refresh_per_second
        self._stop = asyncio.Event()

    async def run(self) -> None:
        # TUI just reads from the ViewModel — a dedicated subscriber (started
        # in main.py) keeps it updated. Rich Live refresh handles repainting.
        try:
            console = Console()
            renderable = _TUIRenderable(self._view)
            with Live(renderable, console=console, screen=True, refresh_per_second=self._refresh):
                try:
                    await self._stop.wait()
                except asyncio.CancelledError:
                    pass
        except Exception:
            logger.exception("TUI loop crashed")

    def stop(self) -> None:
        self._stop.set()


class _TUIRenderable:
    def __init__(self, view: ViewModel) -> None:
        self._view = view
        self._layout = Layout()
        self._layout.split(Layout(name="top", size=15), Layout(name="bottom"))
        self._layout["top"].split_row(Layout(name="passes", ratio=3), Layout(name="transfers", ratio=1))
        self._layout["bottom"].split_row(Layout(name="main_log"), Layout(name="decoder_log"))

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        # Snapshot under the view's lock. Everything below is immutable.
        snap = self._view.snapshot()
        self._layout["passes"].update(self._passes_table(snap))
        self._layout["transfers"].update(self._transfers_panel(snap))
        self._layout["main_log"].update(self._log_panel(snap))
        if snap.decoding:
            self._layout["decoder_log"].visible = True
            self._layout["decoder_log"].update(self._decoder_log_panel(snap))
        else:
            self._layout["decoder_log"].visible = False
        yield self._layout

    def _passes_table(self, snap) -> Table:
        table = Table(title="Next Overpasses", expand=True)
        table.add_column("Satellite")
        table.add_column("Start")
        table.add_column("Duration")
        table.add_column("Max El.")
        table.add_column("Direction")
        table.add_column("Status")

        now = datetime.datetime.now()
        past = [p for p in snap.passes if p.pass_info.end_time <= now]
        future = [p for p in snap.passes if p.pass_info.end_time > now]
        display = past[-1:] + future

        for p in display[:12]:
            start = p.pass_info.start_time.strftime("%H:%M:%S")
            is_live = p.pass_info.start_time < now < p.pass_info.end_time
            if is_live:
                start = f"[bold green]{start} (LIVE)[/bold green]"
            elif p.pass_info.end_time <= now:
                start = f"[dim]{start}[/dim]"
            if is_live:
                remaining = p.pass_info.end_time - now
                total_seconds = max(0, int(remaining.total_seconds()))
                duration_cell = f"[bold green]{total_seconds // 60}:{total_seconds % 60:02d} left[/bold green]"
            else:
                duration_cell = f"{p.pass_info.duration_minutes:.1f}m"
            dirs = "→".join(
                [
                    azimuth_to_compass(p.pass_info.start_azimuth),
                    azimuth_to_compass(p.pass_info.max_azimuth),
                    azimuth_to_compass(p.pass_info.end_azimuth),
                ]
            )
            table.add_row(
                p.satellite.name,
                start,
                duration_cell,
                f"{p.pass_info.max_elevation:.1f}°",
                dirs,
                p.status.value,
            )
        return table

    def _transfers_panel(self, snap) -> Panel:
        lines = []
        if snap.pending_decoders and not snap.decoding:
            lines.append(
                f"[yellow]{snap.pending_decoders} decoder(s) waiting[/yellow]"
            )
            lines.append("")
        if snap.active_transfers:
            for t in snap.active_transfers:
                label = t.label or os.path.basename(t.source_path)
                bar_len = 16
                filled = int(t.progress / 100 * bar_len)
                bar = "█" * filled + "░" * (bar_len - filled)
                lines.append(f"{label}")
                lines.append(f"[{bar}] {t.progress:>3.0f}%")
        else:
            lines.append("[dim]no active transfers[/dim]")

        if snap.completed_transfers:
            lines.append("")
            lines.append("[bold]recent:[/bold]")
            for c in snap.completed_transfers[-5:][::-1]:
                color = "green" if c.status == "ok" else "red"
                label = c.label or os.path.basename(c.source_path)
                ts = c.ts.strftime("%H:%M:%S")
                mark = "✓" if c.status == "ok" else "✗"
                lines.append(f"[{color}]{mark} {label}[/{color}] [dim]({ts})[/dim]")

        return Panel(Group(*[Text.from_markup(l) for l in lines]), title="Transfers")

    def _log_panel(self, snap) -> Panel:
        lines = [
            Text(f"{l.ts.strftime('%H:%M:%S')} {l.message}", style=_level_style(l.level))
            for l in snap.main_log[-200:]
        ]
        return Panel(_TailRenderable(lines), title="Log")

    def _decoder_log_panel(self, snap) -> Panel:
        lines = [
            Text(l.line, style="dim")
            for l in snap.decoder_log[-200:]
        ]
        suffix = f" · {snap.pending_decoders} waiting" if snap.pending_decoders else ""
        if snap.decoding:
            title = (
                f"Decoder log — {snap.decoding.decoder_name or 'decoder'} "
                f"({snap.decoding.pass_id}){suffix}"
            )
        else:
            title = f"Decoder log{suffix}"
        return Panel(_TailRenderable(lines), title=title, border_style="cyan")


class _TailRenderable:
    """Renders the tail of a list of Text lines so the newest content is always
    visible. Rich's default behavior truncates the bottom of a Group when the
    available height is exceeded; we want the opposite — drop the oldest lines
    and keep the panel "scrolled" to the end."""

    def __init__(self, lines: list[Text]) -> None:
        self._lines = lines

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        max_height = options.height if options.height is not None else options.max_height
        if max_height is None or max_height <= 0 or not self._lines:
            for line in self._lines:
                yield line
            return

        width = options.max_width
        # Walk from the newest line backwards, counting visual rows (accounting
        # for wrapping), until we have enough to fill the panel.
        visible: list[Text] = []
        rows = 0
        for line in reversed(self._lines):
            cell_len = console.measure(line, options=options).maximum
            wrapped = max(1, -(-cell_len // width)) if width > 0 else 1
            if rows + wrapped > max_height and visible:
                break
            visible.append(line)
            rows += wrapped
            if rows >= max_height:
                break
        for line in reversed(visible):
            yield line


def _level_style(level: str) -> str:
    return {
        "ERROR": "red",
        "WARNING": "yellow",
        "INFO": "",
        "DEBUG": "dim",
    }.get(level.upper(), "")
