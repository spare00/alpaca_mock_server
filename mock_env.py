"""
Load ``.env`` into ``os.environ`` before argparse defaults are evaluated.

Does not override variables already set in the process environment.
"""
from __future__ import annotations

import os
from pathlib import Path


def _truthy(raw: str) -> bool:
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _strip_quotes(val: str) -> str:
    v = val.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        return v[1:-1]
    return v


def load_dotenv(path: Path | None = None) -> Path | None:
    """
    Parse a ``.env`` file and apply ``os.environ.setdefault`` for each ``KEY=value``.

    Returns the path loaded, or ``None`` if no file was found/read.
    """
    candidates: list[Path] = []
    if path is not None:
        candidates.append(Path(path))
    else:
        here = Path(__file__).resolve().parent
        candidates.append(here / ".env")
        candidates.append(Path.cwd() / ".env")

    chosen: Path | None = None
    for candidate in candidates:
        if candidate.is_file():
            chosen = candidate
            break
    if chosen is None:
        return None

    with chosen.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            val = _strip_quotes(val)
            os.environ.setdefault(key, val)
    return chosen


def preparse_env_file_arg(argv: list[str]) -> str | None:
    """Return ``--env-file`` path if present in ``argv`` (for loading before full argparse)."""
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--env-file" and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith("--env-file="):
            return a.split("=", 1)[1]
        i += 1
    return None


def env_str(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name: str, default: float | None) -> float | None:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_bool(name: str) -> bool:
    return _truthy(os.environ.get(name, ""))
