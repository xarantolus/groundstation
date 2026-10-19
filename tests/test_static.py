from __future__ import annotations

from pathlib import Path

from pyflakes import messages
from pyflakes.api import checkPath
from pyflakes.reporter import Reporter

AUTO = Path(__file__).resolve().parent.parent / "auto"

# Mistakes that only show up at runtime (e.g. a NameError in main._run, which
# no unit test executes). Style findings like unused imports are left alone.
FATAL = (
    messages.UndefinedName,
    messages.UndefinedLocal,
    messages.UndefinedExport,
    messages.ImportStarUsage,
    messages.DuplicateArgument,
)


class _Collect(Reporter):
    def __init__(self) -> None:
        self.found: list[str] = []

    def flake(self, message) -> None:
        if isinstance(message, FATAL):
            self.found.append(str(message))

    def syntaxError(self, filename, msg, lineno, offset, text) -> None:
        self.found.append(f"{filename}:{lineno}: syntax error: {msg}")

    def unexpectedError(self, filename, msg) -> None:
        self.found.append(f"{filename}: {msg}")


def test_no_undefined_names_in_auto():
    rep = _Collect()
    for path in sorted(AUTO.rglob("*.py")):
        checkPath(str(path), rep)
    assert not rep.found, "\n".join(rep.found)
