"""Minimal evaluator for Ab Initio ``.pset`` / ``.env`` parameter files.

These files are KornShell fragments made of ``export NAME=value`` lines. The
evaluator reproduces what ``. file.pset`` does for the constructs used in this
repo: ``$NAME``, ``${NAME}`` and ``${NAME:-default}`` expansion, applied in
order against a mutable environment. Command substitution (``$(...)``) is not
executed; it is only allowed inside a ``:-`` default that is never needed.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, MutableMapping
from pathlib import Path

_ASSIGN = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class PsetError(ValueError):
    pass


def _matching_brace(text: str, start: int) -> int:
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return i
    raise PsetError(f"unterminated ${{...}} in {text!r}")


def expand(text: str, env: MutableMapping[str, str]) -> str:
    """Expand shell-style variable references in ``text`` using ``env``."""
    out: list[str] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch != "$":
            out.append(ch)
            i += 1
            continue
        nxt = text[i + 1] if i + 1 < len(text) else ""
        if nxt == "{":
            end = _matching_brace(text, i + 1)
            body = text[i + 2 : end]
            name, sep, default = body.partition(":-")
            value = env.get(name, "")
            if not value and sep:
                value = expand(default, env)
            out.append(value)
            i = end + 1
        elif nxt == "(":
            raise PsetError(f"command substitution is not supported: {text!r}")
        else:
            match = _NAME.match(text, i + 1)
            if not match:
                out.append(ch)
                i += 1
                continue
            out.append(env.get(match.group(0), ""))
            i = match.end()
    return "".join(out)


def _strip_comment(line: str) -> str:
    in_single = in_double = False
    for i, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double and (i == 0 or line[i - 1].isspace()):
            return line[:i]
    return line


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        return value[1:-1]
    return value


def source(path: str | Path, env: MutableMapping[str, str]) -> MutableMapping[str, str]:
    """Apply every assignment in ``path`` to ``env`` (like ``. path`` in ksh)."""
    for raw in Path(path).read_text().splitlines():
        line = _strip_comment(raw).strip()
        if not line:
            continue
        match = _ASSIGN.match(line)
        if not match:
            continue
        name, value = match.groups()
        literal = value.strip().startswith("'")
        value = _unquote(value)
        env[name] = value if literal else expand(value, env)
    return env


def source_all(
    paths: Iterable[str | Path], env: MutableMapping[str, str]
) -> MutableMapping[str, str]:
    for path in paths:
        source(path, env)
    return env
