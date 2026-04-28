from __future__ import annotations

import json
from typing import Any

from flamenco_frames.prompting.family_context import load_codebook
from flamenco_frames.schemas.codebook import Codebook, Concept


ARTICLE_OUTPUT_SCHEMA = {
    "article_id": "string",
    "language": "es | en | unknown",
    "article_summary": {
        "dominant_codes": ["concept_id"],
        "secondary_codes": ["concept_id"],
        "summary": "brief article-level synthesis of frame work",
    },
    "span_annotations": [
        {
            "concept_id": "string",
            "family_id": "string",
            "decision": "present | uncertain",
            "confidence": "number from 0.0 to 1.0",
            "window_id": "string",
            "page_number": "number or null",
            "evidence_span": "exact quote from a supplied evidence window",
            "local_function": "what this frame is doing locally in the article",
        }
    ],
    "rejected_candidates": [
        {
            "concept_id": "string",
            "family_id": "string",
            "reason": "why the candidate was rejected",
        }
    ],
    "needs_human_review": "boolean",
    "human_review_reason": "string",
}


def compact_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def select_examples(concept: Concept) -> dict[str, Any]:
    positive = None
    trap = None

    for example in concept.few_shot_examples:
        if positive is None and example.is_good_example == "yes":
            positive = {
                "excerpt": example.excerpt,
                "rationale": example.rationale,
                "language": example.language,
                "example_type": example.example_type,
            }

        if trap is None and example.is_good_example == "no":
            trap = {
                "excerpt": example.excerpt,
                "rationale": example.rationale,
                "language": example.language,
                "example_type": example.example_type,
            }

        if positive and trap:
            break

    return {
        "positive_example": positive,
        "trap_example": trap,
    }


def build_compact_codebook_entries(
    codebook: Codebook,
    candidate_codes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []

    seen: set[tuple[str, str]] = set()

    for candidate in candidate_codes:
        family_id = candidate["family_id"]
        concept_id = candidate["concept_id"]
        key = (family_id, concept_id)

        if key in seen:
            continue

        seen.add(key)

        family = codebook.families.get(family_id)
        if family is None:
            continue

        concept = family.concepts.get(concept_id)
        if concept is None:
            continue

        examples = select_examples(concept)

        entries.append(
            {
                "family_id": family_id,
                "family_name_en": family.name_en,
                "family_name_es": family.name_es,
                "family_purpose": family.purpose,
                "family_do_not_use_when": family.do_not_use_when,
                "concept_id": concept_id,
                "label_en": concept.label_en,
                "label_es": concept.label_es,
                "definition": concept.definition,
                "use_when": concept.use_when,
                "do_not_use_when": concept.do_not_use_when,
                "related_concepts": concept.related_concepts,
                "positive_example": examples["positive_example"],
                "trap_example": examples["trap_example"],
            }
        )

    return entries


def build_evidence_windows(candidate_codes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    windows_by_id: dict[str, dict[str, Any]] = {}

    for candidate in candidate_codes:
        for window in candidate.get("supporting_windows", []):
            window_id = window.get("window_id")
            if not window_id:
                continue

            if window_id not in windows_by_id:
                windows_by_id[window_id] = {
                    "window_id": window_id,
                    "page_number": window.get("page_number"),
                    "logical_page": window.get("logical_page"),
                    "focal_global_block_id": window.get("focal_global_block_id"),
                    "text": window.get("text"),
                    "candidate_codes_supported": [],
                }

            windows_by_id[window_id]["candidate_codes_supported"].append(
                {
                    "family_id": candidate.get("family_id"),
                    "concept_id": candidate.get("concept_id"),
                    "rank": candidate.get("rank"),
                    "evidence_strength": candidate.get("evidence_strength"),
                    "n_supporting_windows": candidate.get("n_supporting_windows"),
                }
            )

    return list(windows_by_id.values())


def build_article_annotation_prompt(
    *,
    codebook: Codebook,
    article_candidate_row: dict[str, Any],
) -> str:
    candidate_codes = article_candidate_row.get("candidate_codes") or []

    article_payload = {
        "issue_id": article_candidate_row.get("issue_id"),
        "article_id": article_candidate_row.get("article_id"),
        "article_name": article_candidate_row.get("article_name"),
        "article_type": article_candidate_row.get("article_type"),
        "language": article_candidate_row.get("language"),
        "page_start": article_candidate_row.get("page_start"),
        "page_end": article_candidate_row.get("page_end"),
        "n_blocks": article_candidate_row.get("n_blocks"),
        "text_chars": article_candidate_row.get("text_chars"),
        "article_opening": article_candidate_row.get("article_opening"),
    }

    compact_candidates = [
        {
            "rank": candidate.get("rank"),
            "family_id": candidate.get("family_id"),
            "concept_id": candidate.get("concept_id"),
            "evidence_strength": candidate.get("evidence_strength"),
            "n_supporting_windows": candidate.get("n_supporting_windows"),
            "supporting_window_ids": [
                window.get("window_id")
                for window in candidate.get("supporting_windows", [])
            ],
        }
        for candidate in candidate_codes
    ]

    evidence_windows = build_evidence_windows(candidate_codes)

    codebook_entries = build_compact_codebook_entries(
        codebook=codebook,
        candidate_codes=candidate_codes,
    )

    return f"""You are annotating one historical flamenco periodical article.

The article is the unit of analysis.
Evidence must come from the supplied evidence windows.

Your task:
1. Decide which candidate codes are analytically present in the article.
2. Preserve internal variation by attaching each present code to one or more evidence windows.
3. Reject candidates that are only keyword matches, generic description, geography, biography, or administrative language.
4. Use trap examples as negative guardrails.
5. Return valid JSON only. Do not include markdown.

Do not assign broad article labels without evidence.
Do not invent evidence outside the supplied windows.
Evidence spans must be exact quotes from the supplied evidence windows.

The candidate ranking is retrieval-based, not ground truth. A high-ranked candidate can still be absent.
A code may appear multiple times in different windows; annotate each distinct local function only once unless the evidence shows a meaningfully different use.

ARTICLE:
{compact_json(article_payload)}

CANDIDATE CODES:
{compact_json(compact_candidates)}

EVIDENCE WINDOWS:
{compact_json(evidence_windows)}

COMPACT CODEBOOK ENTRIES:
{compact_json(codebook_entries)}

REQUIRED JSON OUTPUT SHAPE:
{compact_json(ARTICLE_OUTPUT_SCHEMA)}
"""


def build_article_prompt_from_paths(
    codebook_path: str,
    article_candidate_row: dict[str, Any],
) -> str:
    codebook = load_codebook(codebook_path)
    return build_article_annotation_prompt(
        codebook=codebook,
        article_candidate_row=article_candidate_row,
    )