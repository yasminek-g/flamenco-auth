from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from flamenco_frames.io_utils import read_json, write_jsonl
from flamenco_frames.schemas.corpus import AnnotationUnit


DEFAULT_ALLOWED_BLOCK_ROLES = {
    "body",
    "subheading",
    "article_anchor",
}

DEFAULT_DENIED_BLOCK_ROLES = {
    "boilerplate",
    "ad_package",
    "satellite",
    "front_matter",
}

DEFAULT_DENIED_BLOCK_LABELS = {
    "header",
    "footer",
    "header_image",
    "footer_image",
    "image",
    "aside_text",
}

WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    return WHITESPACE_RE.sub(" ", text).strip()


def infer_issue_id(path: str | Path, issue_json: dict[str, Any]) -> str:
    if "issue_id" in issue_json and issue_json["issue_id"]:
        return str(issue_json["issue_id"])

    stem = Path(path).stem
    return stem.replace("_tagged", "")


def should_keep_block(
    block: dict[str, Any],
    min_chars: int,
    allowed_roles: set[str],
    denied_roles: set[str],
    denied_labels: set[str],
) -> bool:
    text = normalize_text(str(block.get("block_content") or ""))
    if len(text) < min_chars:
        return False

    block_label = block.get("block_label")
    if block_label in denied_labels:
        return False

    hierarchy = block.get("inferred_hierarchy") or {}
    block_role = hierarchy.get("block_role")

    if block_role in denied_roles:
        return False

    if allowed_roles and block_role not in allowed_roles:
        return False

    return True


def iter_page_blocks(issue_json: dict[str, Any]) -> Iterator[tuple[int | None, dict[str, Any]]]:
    for page in issue_json.get("pages", []):
        page_number = page.get("page_number")

        # Current file shape: page["result"]["res"]["parsing_res_list"]
        parsing_blocks = (
            page.get("result", {})
            .get("res", {})
            .get("parsing_res_list", [])
        )

        for block in parsing_blocks:
            yield page_number, block


def make_unit_id(issue_id: str, block: dict[str, Any], fallback_index: int) -> str:
    hierarchy = block.get("inferred_hierarchy") or {}
    article_id = hierarchy.get("article_id") or "unknown_article"
    global_block_id = block.get("global_block_id")

    if global_block_id is not None:
        suffix = f"gb{global_block_id}"
    else:
        suffix = f"idx{fallback_index:06d}"

    return f"{issue_id}__{article_id}__{suffix}"


def block_to_annotation_unit(
    issue_id: str,
    page_number: int | None,
    block: dict[str, Any],
    fallback_index: int,
) -> AnnotationUnit:
    hierarchy = block.get("inferred_hierarchy") or {}
    text = normalize_text(str(block.get("block_content") or ""))

    return AnnotationUnit(
        unit_id=make_unit_id(issue_id, block, fallback_index),
        issue_id=issue_id,
        page_number=page_number,
        logical_page=hierarchy.get("logical_page"),
        article_id=hierarchy.get("article_id"),
        article_name=hierarchy.get("article_name"),
        article_type=hierarchy.get("article_type"),
        section_label=hierarchy.get("section_label"),
        listed_in_contents=hierarchy.get("listed_in_contents"),
        contents_match=hierarchy.get("contents_match"),
        subclass=hierarchy.get("subclass"),
        block_id=block.get("block_id"),
        global_block_id=block.get("global_block_id"),
        block_label=block.get("block_label"),
        block_role=hierarchy.get("block_role"),
        block_order=block.get("block_order"),
        block_bbox=block.get("block_bbox"),
        text=text,
        text_chars=len(text),
        metadata={
            "side": hierarchy.get("side"),
            "style4_page_anchor": hierarchy.get("style4_page_anchor"),
            "resolved_page_number": hierarchy.get("resolved_page_number"),
            "group_id": block.get("group_id"),
            "global_group_id": block.get("global_group_id"),
        },
        review_flags=[],
    )


def build_annotation_units(
    issue_path: str | Path,
    min_chars: int = 80,
    allowed_roles: set[str] | None = None,
    denied_roles: set[str] | None = None,
    denied_labels: set[str] | None = None,
) -> list[AnnotationUnit]:
    issue_json = read_json(issue_path)
    issue_id = infer_issue_id(issue_path, issue_json)

    allowed_roles = allowed_roles or DEFAULT_ALLOWED_BLOCK_ROLES
    denied_roles = denied_roles or DEFAULT_DENIED_BLOCK_ROLES
    denied_labels = denied_labels or DEFAULT_DENIED_BLOCK_LABELS

    units: list[AnnotationUnit] = []

    for i, (page_number, block) in enumerate(iter_page_blocks(issue_json)):
        if not should_keep_block(
            block=block,
            min_chars=min_chars,
            allowed_roles=allowed_roles,
            denied_roles=denied_roles,
            denied_labels=denied_labels,
        ):
            continue

        units.append(
            block_to_annotation_unit(
                issue_id=issue_id,
                page_number=page_number,
                block=block,
                fallback_index=i,
            )
        )

    return units


def write_annotation_units(
    issue_path: str | Path,
    output_path: str | Path,
    min_chars: int = 80,
) -> None:
    units = build_annotation_units(issue_path=issue_path, min_chars=min_chars)
    write_jsonl(output_path, (unit.model_dump() for unit in units))