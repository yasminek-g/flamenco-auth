from __future__ import annotations

from pathlib import Path
from typing import Any

from flamenco_frames.io_utils import read_jsonl, write_jsonl


def clip_text(text: str, max_chars: int) -> str:
    text = text.strip()

    if len(text) <= max_chars:
        return text

    clipped = text[:max_chars].rsplit(" ", 1)[0].strip()
    return clipped + " [...]"


def make_window_id(article_id: str, focal_block: dict[str, Any]) -> str:
    global_block_id = focal_block.get("global_block_id")
    unit_id = focal_block.get("unit_id")

    if global_block_id is not None:
        suffix = f"gb{global_block_id}"
    elif unit_id:
        suffix = str(unit_id).split("__")[-1]
    else:
        suffix = "unknown"

    return f"{article_id}__{suffix}"


def make_evidence_windows_for_article(
    article: dict[str, Any],
    include_previous: bool = True,
    include_next: bool = True,
    max_window_chars: int = 900,
) -> list[dict[str, Any]]:
    blocks = article.get("blocks") or []
    windows: list[dict[str, Any]] = []

    for i, focal_block in enumerate(blocks):
        focal_text = (focal_block.get("text") or "").strip()
        if not focal_text:
            continue

        source_blocks: list[dict[str, Any]] = []

        if include_previous and i > 0:
            source_blocks.append(blocks[i - 1])

        source_blocks.append(focal_block)

        if include_next and i + 1 < len(blocks):
            source_blocks.append(blocks[i + 1])

        raw_window_text = "\n\n".join(
            (block.get("text") or "").strip()
            for block in source_blocks
            if (block.get("text") or "").strip()
        )

        window_text = clip_text(raw_window_text, max_window_chars)

        article_id = article["article_id"]
        window = {
            "issue_id": article.get("issue_id"),
            "article_id": article_id,
            "article_name": article.get("article_name"),
            "article_type": article.get("article_type"),
            "language": article.get("language"),
            "window_id": make_window_id(article_id, focal_block),
            "focal_unit_id": focal_block.get("unit_id"),
            "focal_global_block_id": focal_block.get("global_block_id"),
            "page_number": focal_block.get("page_number"),
            "logical_page": focal_block.get("logical_page"),
            "focal_text": focal_text,
            "text": window_text,
            "text_chars": len(window_text),
            "source_unit_ids": [
                block.get("unit_id")
                for block in source_blocks
                if block.get("unit_id")
            ],
            "source_global_block_ids": [
                block.get("global_block_id")
                for block in source_blocks
                if block.get("global_block_id") is not None
            ],
        }

        windows.append(window)

    return windows


def build_evidence_windows(
    article_units_path: str | Path,
    include_previous: bool = True,
    include_next: bool = True,
    max_window_chars: int = 900,
) -> list[dict[str, Any]]:
    articles = read_jsonl(article_units_path)

    windows: list[dict[str, Any]] = []

    for article in articles:
        windows.extend(
            make_evidence_windows_for_article(
                article=article,
                include_previous=include_previous,
                include_next=include_next,
                max_window_chars=max_window_chars,
            )
        )

    return windows


def write_evidence_windows(
    article_units_path: str | Path,
    output_path: str | Path,
    include_previous: bool = True,
    include_next: bool = True,
    max_window_chars: int = 900,
) -> None:
    windows = build_evidence_windows(
        article_units_path=article_units_path,
        include_previous=include_previous,
        include_next=include_next,
        max_window_chars=max_window_chars,
    )

    write_jsonl(output_path, windows)