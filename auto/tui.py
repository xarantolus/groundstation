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
from rich.segment import Segment, Segments
from rich.table import Table
from rich.text import Text

from .bus import EventBus
from .pass_predictor import azimuth_to_compass
from .view import ViewModel

logger = logging.getLogger("groundstation.tui")

DECODER_PANEL_GRACE_SECONDS = 5.0


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
        self._layout["top"].split_row(Layout(name="passes", ratio=3), Layout(name="right", ratio=1))
        self._layout["right"].split_column(
            Layout(name="transfers"),
            Layout(name="pending_decoders", size=1),
        )
        self._layout["bottom"].split_row(Layout(name="main_log"), Layout(name="decoder_log"))

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        # Snapshot under the view's lock. Everything below is immutable.
        snap = self._view.snapshot()
        self._layout["passes"].update(self._passes_table(snap))
        self._layout["transfers"].update(self._transfers_panel(snap))
        show_pending = bool(snap.pending_decoders) and snap.decoding is None
        self._layout["pending_decoders"].visible = show_pending
        if show_pending:
            self._layout["pending_decoders"].update(self._pending_decoders_line(snap))
        self._layout["main_log"].update(self._log_panel(snap))
        if _show_decoder_panel(snap):
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
        if snap.active_transfers:
            for t in snap.active_transfers:
                label = t.label or os.path.basename(t.source_path)
                if t.copied <= 0:
                    lines.append(f"[yellow]⏸[/yellow] {label} [dim](preparing…)[/dim]")
                elif t.total > 0:
                    pct = max(0.0, min(100.0, t.progress))
                    lines.append(
                        f"[cyan]↑[/cyan] {label} "
                        f"[bold]{pct:5.1f}%[/bold] "
                        f"[dim]({_format_bytes(t.copied)}/{_format_bytes(t.total)})[/dim]"
                    )
                else:
                    lines.append(
                        f"[cyan]↑[/cyan] {label} "
                        f"[dim]({_format_bytes(t.copied)} copied)[/dim]"
                    )
        elif not snap.queued_transfers:
            lines.append("[dim]no active transfers[/dim]")

        if snap.queued_transfers:
            lines.append(f"[dim]{len(snap.queued_transfers)} queued[/dim]")

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

    def _pending_decoders_line(self, snap) -> Text:
        if snap.pending_decoders and not snap.decoding:
            return Text.from_markup(
                f"[yellow]{snap.pending_decoders} decoder(s) waiting[/yellow]",
                justify="center",
            )
        return Text("", justify="center")

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
        active = snap.decoding or snap.last_decoding
        if active:
            finished_marker = " (finished)" if snap.decoding is None else ""
            title = (
                f"Decoder log — {active.decoder_name or 'decoder'} "
                f"({active.pass_id}){finished_marker}{suffix}"
            )
        else:
            title = f"Decoder log{suffix}"
        return Panel(_TailRenderable(lines), title=title, border_style="cyan")


class _TailRenderable:
    """Renders the tail of a list of Text lines so the newest content is
    pinned to the bottom row of the panel. Trims at the RENDERED-ROW level
    rather than the logical-line level so that a long wrapped line (or one
    with embedded newlines — e.g. an exception traceback) can be partially
    shown above the newest message instead of pushing it off-screen."""

    def __init__(self, lines: list[Text]) -> None:
        self._lines = lines

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        max_height = options.height if options.height is not None else options.max_height
        if max_height is None or max_height <= 0 or not self._lines:
            for line in self._lines:
                yield line
            return

        # Render each Text to its exact segment rows using the root console's
        # default options (which don't cap height), so a long line's trailing
        # rows aren't silently dropped by render_lines' internal islice.
        # Then collect newest→oldest until we have max_height rows.
        render_opts = console.options.update(max_width=options.max_width)
        rows: list[list[Segment]] = []
        for line in reversed(self._lines):
            rendered = console.render_lines(line, render_opts, pad=False)
            for row in reversed(rendered):
                rows.append(row)
                if len(rows) >= max_height:
                    break
            if len(rows) >= max_height:
                break

        rows.reverse()
        segments: list[Segment] = []
        for i, row in enumerate(rows):
            if i > 0:
                segments.append(Segment.line())
            segments.extend(row)
        yield Segments(segments)


def _show_decoder_panel(snap) -> bool:
    if snap.decoding is not None:
        return True
    if snap.decoding_ended_at is None:
        return False
    delta = (datetime.datetime.now() - snap.decoding_ended_at).total_seconds()
    return delta < DECODER_PANEL_GRACE_SECONDS


def _level_style(level: str) -> str:
    return {
        "ERROR": "red",
        "WARNING": "yellow",
        "INFO": "",
        "DEBUG": "dim",
    }.get(level.upper(), "")


def _format_bytes(n: float) -> str:
    if n < 1024:
        return f"{int(n)} B"
    n /= 1024
    for unit in ("KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
