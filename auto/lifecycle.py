from __future__ import annotations

from . import events as E
from .bus import EventBus
from .models import Pass, PassStatus
from .state import StateStore


async def set_pass_status(
    p: Pass,
    status: PassStatus,
    state: StateStore,
    bus: EventBus,
    reason: str | None = None,
) -> None:
    """The one way services change a pass's status: set it (with an optional
    reason), persist the pass, and announce it. Views and UIs take the status
    from PassStatusChanged only, never infer it from other events."""
    p.set_status(status, reason)
    await state.save_pass_async(p)
    await bus.publish(E.PassStatusChanged(pass_id=p.id, status=status, reason=reason))
