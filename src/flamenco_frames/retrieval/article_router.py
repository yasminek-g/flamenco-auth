from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from flamenco_frames.io_utils import read_jsonl, write_jsonl


def clip_text(text: str, max_chars: int) -> str:
    text = clean_prompt_text(text)

    if len(text) <= max_chars:
        return text

    clipped = text[:max_chars].rsplit(" ", 1)[0].strip()
    return clipped + " [...]"


def clean_prompt_text(text: str) -> str:
    """
    Light cleanup for OCR/layout artifacts before text enters prompts.
    """
    import re

    text = text.replace("-\n", "")
    text = re.sub(r"(?<=\w)\n(?=\w)", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def candidate_key(candidate: dict[str, Any]) -> tuple[str, str]:
    return str(candidate["family_id"]), str(candidate["concept_id"])


def aggregate_candidate_score(scores: list[float]) -> float:
    if not scores:
        return 0.0

    sorted_scores = sorted(scores, reverse=True)
    max_score = sorted_scores[0]
    supporting_score = sum(sorted_scores[1:])

    return max_score + 0.3 * supporting_score


def evidence_strength(article_score: float, n_windows: int) -> str:
    if article_score >= 25:
        return "high"
    if article_score >= 10:
        return "medium"
    return "low"


def window_has_regex(window: dict[str, Any]) -> bool:
    return bool(window.get("regex_hits"))


def window_has_bm25(window: dict[str, Any]) -> bool:
    return bool(window.get("bm25_hits"))


def window_selection_key(window: dict[str, Any]) -> tuple[int, float]:
    """
    Prefer windows with explicit regex evidence, then higher score.

    This helps surface clearer windows like 'reconocido' for LEGIT_01
    instead of only high-BM25 windows.
    """
    has_regex = window_has_regex(window)
    has_bm25 = window_has_bm25(window)

    if has_regex and has_bm25:
        priority = 3
    elif has_regex:
        priority = 2
    elif has_bm25:
        priority = 1
    else:
        priority = 0

    return priority, float(window.get("score") or 0.0)


def load_articles_by_id(article_units_path: str | Path) -> dict[str, dict[str, Any]]:
    articles = read_jsonl(article_units_path)
    return {
        str(article["article_id"]): article
        for article in articles
    }


def aggregate_article_candidates(
    article_units_path: str | Path,
    window_candidates_path: str | Path,
    max_candidate_codes: int = 6,
    max_windows_per_code: int = 2,
    max_total_windows: int = 10,
    max_window_chars: int = 700,
    min_article_score: float = 3.0,
) -> list[dict[str, Any]]:
    articles_by_id = load_articles_by_id(article_units_path)
    window_rows = read_jsonl(window_candidates_path)

    grouped: dict[str, dict[tuple[str, str], list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for row in window_rows:
        article_id = row.get("article_id")
        if not article_id:
            continue

        for candidate in row.get("candidates", []):
            key = candidate_key(candidate)
            grouped[str(article_id)][key].append(
                {
                    "window_id": row.get("window_id") or row.get("unit_id"),
                    "focal_unit_id": row.get("focal_unit_id"),
                    "focal_global_block_id": row.get("focal_global_block_id"),
                    "page_number": row.get("page_number"),
                    "logical_page": row.get("logical_page"),
                    "text": clip_text(row.get("text") or "", max_window_chars),
                    "focal_text": clip_text(row.get("focal_text") or "", max_window_chars),
                    "score": float(candidate.get("score") or 0.0),
                    "sources": candidate.get("sources", []),
                    "regex_hits": candidate.get("regex_hits", []),
                    "bm25_hits": candidate.get("bm25_hits", []),
                }
            )

    article_candidate_rows: list[dict[str, Any]] = []

    for article_id, article in articles_by_id.items():
        candidates_for_article = grouped.get(article_id, {})

        candidate_codes: list[dict[str, Any]] = []

        for (family_id, concept_id), windows in candidates_for_article.items():
            all_windows_by_score = sorted(
                windows,
                key=lambda window: float(window.get("score") or 0.0),
                reverse=True,
            )

            scores = [
                float(window.get("score") or 0.0)
                for window in all_windows_by_score
            ]

            article_score = aggregate_candidate_score(scores)

            if article_score < min_article_score:
                continue

            selected_windows = sorted(
                all_windows_by_score,
                key=window_selection_key,
                reverse=True,
            )[:max_windows_per_code]

            candidate_codes.append(
                {
                    "family_id": family_id,
                    "concept_id": concept_id,
                    "article_score": article_score,
                    "evidence_strength": evidence_strength(
                        article_score=article_score,
                        n_windows=len(all_windows_by_score),
                    ),
                    "n_supporting_windows": len(all_windows_by_score),
                    "supporting_windows": selected_windows,
                }
            )

        candidate_codes.sort(
            key=lambda candidate: candidate["article_score"],
            reverse=True,
        )

        candidate_codes = candidate_codes[:max_candidate_codes]

        # Add stable rank after sorting.
        for rank, candidate in enumerate(candidate_codes, start=1):
            candidate["rank"] = rank

        # Enforce total window budget across codes.
        total_windows = 0
        trimmed_candidate_codes: list[dict[str, Any]] = []

        for candidate in candidate_codes:
            remaining = max_total_windows - total_windows
            if remaining <= 0:
                break

            windows = candidate["supporting_windows"][:remaining]
            total_windows += len(windows)

            trimmed_candidate = dict(candidate)
            trimmed_candidate["supporting_windows"] = windows
            trimmed_candidate_codes.append(trimmed_candidate)

        page_start = article.get("page_start")
        page_end = article.get("page_end")

        article_candidate_rows.append(
            {
                "issue_id": article.get("issue_id"),
                "article_id": article_id,
                "article_name": article.get("article_name"),
                "article_type": article.get("article_type"),
                "language": article.get("language"),
                "page_start": page_start,
                "page_end": page_end,
                "n_blocks": article.get("n_blocks"),
                "text_chars": article.get("text_chars"),
                "article_opening": clip_text(article.get("full_text") or "", 600),
                "candidate_codes": trimmed_candidate_codes,
            }
        )

    article_candidate_rows.sort(
        key=lambda row: (
            row.get("page_start") if row.get("page_start") is not None else 10**9,
            row.get("article_id") or "",
        )
    )

    return article_candidate_rows


def write_article_candidates(
    article_units_path: str | Path,
    window_candidates_path: str | Path,
    output_path: str | Path,
    max_candidate_codes: int = 6,
    max_windows_per_code: int = 2,
    max_total_windows: int = 10,
    max_window_chars: int = 700,
    min_article_score: float = 3.0,
) -> None:
    rows = aggregate_article_candidates(
        article_units_path=article_units_path,
        window_candidates_path=window_candidates_path,
        max_candidate_codes=max_candidate_codes,
        max_windows_per_code=max_windows_per_code,
        max_total_windows=max_total_windows,
        max_window_chars=max_window_chars,
        min_article_score=min_article_score,
    )

    write_jsonl(output_path, rows)