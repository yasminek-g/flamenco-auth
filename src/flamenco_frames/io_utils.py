from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import orjson


def read_json(path: str | Path) -> Any:
    path = Path(path)
    with path.open("rb") as f:
        return orjson.loads(f.read())


def write_json(path: str | Path, obj: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        f.write(orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))


def write_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        for row in rows:
            f.write(orjson.dumps(row, option=orjson.OPT_SORT_KEYS))
            f.write(b"\n")


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    rows: list[dict[str, Any]] = []
    with path.open("rb") as f:
        for line in f:
            if line.strip():
                rows.append(orjson.loads(line))
    return rows