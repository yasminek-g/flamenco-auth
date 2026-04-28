from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from flamenco_frames.io_utils import read_jsonl, write_jsonl


def block_sort_key(block: dict[str, Any]) -> tuple[int, str, int, int]:
    page_number = block.get("page_number")
    logical_page = block.get("logical_page") or ""
    block_order = block.get("block_order")
    global_block_id = block.get("global_block_id")

    return (
        int(page_number) if page_number is not None else 10**9,
        str(logical_page),
        int(block_order) if block_order is not None else 10**9,
        int(global_block_id) if global_block_id is not None else 10**9,
    )


def compact_block(block: dict[str, Any]) -> dict[str, Any]:
    return {
        "unit_id": block.get("unit_id"),
        "issue_id": block.get("issue_id"),
        "article_id": block.get("article_id"),
        "article_name": block.get("article_name"),
        "article_type": block.get("article_type"),
        "page_number": block.get("page_number"),
        "logical_page": block.get("logical_page"),
        "block_id": block.get("block_id"),
        "global_block_id": block.get("global_block_id"),
        "block_label": block.get("block_label"),
        "block_role": block.get("block_role"),
        "block_order": block.get("block_order"),
        "block_bbox": block.get("block_bbox"),
        "text": block.get("text") or "",
        "text_chars": block.get("text_chars"),
        "metadata": block.get("metadata") or {},
        "review_flags": block.get("review_flags") or [],
    }


def infer_page_span(blocks: list[dict[str, Any]]) -> tuple[int | None, int | None]:
    pages = [
        int(block["page_number"])
        for block in blocks
        if block.get("page_number") is not None
    ]

    if not pages:
        return None, None

    return min(pages), max(pages)


def infer_language_from_issue(issue_id: str) -> str:
    # Candil material is Spanish in the current corpus.
    # Keep this simple for now; later make this metadata-driven.
    return "es"


def build_article_units_from_blocks(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for block in blocks:
        article_id = block.get("article_id")
        if not article_id:
            continue

        grouped[str(article_id)].append(block)

    articles: list[dict[str, Any]] = []

    for article_id, article_blocks in grouped.items():
        article_blocks = sorted(article_blocks, key=block_sort_key)
        compact_blocks = [compact_block(block) for block in article_blocks]

        first = article_blocks[0]
        issue_id = str(first.get("issue_id") or "")
        page_start, page_end = infer_page_span(article_blocks)

        full_text = "\n\n".join(
            block.get("text") or ""
            for block in compact_blocks
            if block.get("text")
        ).strip()

        article = {
            "issue_id": issue_id,
            "article_id": article_id,
            "article_name": first.get("article_name"),
            "article_type": first.get("article_type"),
            "section_label": first.get("section_label"),
            "language": infer_language_from_issue(issue_id),
            "page_start": page_start,
            "page_end": page_end,
            "n_blocks": len(compact_blocks),
            "text_chars": len(full_text),
            "blocks": compact_blocks,
            "full_text": full_text,
            "review_flags": sorted(
                {
                    flag
                    for block in article_blocks
                    for flag in (block.get("review_flags") or [])
                }
            ),
        }

        articles.append(article)

    articles.sort(
        key=lambda article: (
            article.get("page_start") if article.get("page_start") is not None else 10**9,
            article.get("article_id") or "",
        )
    )

    return articles


def write_article_units(
    block_units_path: str | Path,
    output_path: str | Path,
) -> None:
    blocks = read_jsonl(block_units_path)
    articles = build_article_units_from_blocks(blocks)
    write_jsonl(output_path, articles)