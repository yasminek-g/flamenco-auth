"""Build long-form CSVs from the chat-annotation result files.

Reads every `chat_annotation_packets_<periodical>/results/*_results.json`,
joins each article on the periodical's `manifest.csv`, and writes:

    data/articles.csv     one row per article (article-level fields + manifest)
    data/codes.csv        one row per emitted code
    data/possible.csv     one row per `possible_but_not_emitted` entry
    data/basis.csv        one row per `derived_analysis.basis` tag

The plot scripts only read these CSVs. Re-run this once after adding new
results.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import (  # noqa: E402
    CANDIL_DIR,
    DATA_DIR,
    JALEO_DIR,
    family_of,
    issue_to_year,
    periodical_label,
)


SOURCES = [
    ("candil", CANDIL_DIR),
    ("jaleo", JALEO_DIR),
]

CODE_RE = re.compile(r"^(AUTH|CRIT|COMM|PED|HERIT)_\d{2}$")


def load_results(periodical: str, base: Path) -> list[dict]:
    results_dir = base / "results"
    rows: list[dict] = []
    for path in sorted(results_dir.glob("*_results.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = payload.get("results") or [payload]
        for art in payload or []:
            if not isinstance(art, dict):
                continue
            art["_source_file"] = path.name
            art["_periodical"] = periodical
            rows.append(art)
    return rows


def load_manifest(base: Path) -> pd.DataFrame:
    df = pd.read_csv(base / "manifest.csv")
    keep = [
        "article_id",
        "title",
        "issue_id",
        "language",
        "article_type",
        "review_text_chars",
        "review_strategy",
        "n_trigger_windows",
        "retrieval_hint_codes",
        "packet_id",
        "packet_article_position",
    ]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()
    df["article_id"] = df["article_id"].astype(str)
    return df


def word_count(text: object) -> int:
    if not isinstance(text, str):
        return 0
    return len(re.findall(r"\w+", text))


def coerce_possible_entry(entry: object) -> dict | None:
    """Normalize a `possible_but_not_emitted` entry. Some annotations returned
    a string instead of an object; tolerate both."""
    if isinstance(entry, dict):
        code = entry.get("code")
        reason = entry.get("reason") or ""
        family = entry.get("family") or family_of(code)
        if not code:
            return None
        return {"code": str(code), "family": family, "reason": str(reason)}
    if isinstance(entry, str):
        # Best-effort: pick out the first code id from the string.
        m = CODE_RE.search(entry)
        if m:
            code = m.group(0)
            return {"code": code, "family": family_of(code), "reason": entry}
        return None
    return None


def build() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    article_rows: list[dict] = []
    code_rows: list[dict] = []
    possible_rows: list[dict] = []
    basis_rows: list[dict] = []

    for periodical, base in SOURCES:
        manifest = load_manifest(base)
        articles = load_results(periodical, base)
        man_lookup = (
            manifest.drop_duplicates("article_id", keep="first")
            .set_index("article_id")
            .to_dict("index")
        )

        for art in articles:
            article_id = str(art.get("article_id") or "")
            meta = man_lookup.get(article_id, {})
            derived = art.get("derived_analysis") or {}
            codes = art.get("codes") or []
            possible = art.get("possible_but_not_emitted") or []
            basis_tags = [b for b in (derived.get("basis") or []) if isinstance(b, str)]

            article_rows.append(
                {
                    "article_id": article_id,
                    "periodical": periodical,
                    "periodical_label": periodical_label(periodical),
                    "title": meta.get("title"),
                    "issue_id": meta.get("issue_id"),
                    "year": issue_to_year(meta.get("issue_id", "")),
                    "language": meta.get("language"),
                    "article_type": meta.get("article_type"),
                    "review_text_chars": meta.get("review_text_chars"),
                    "review_strategy": meta.get("review_strategy"),
                    "n_trigger_windows": meta.get("n_trigger_windows"),
                    "retrieval_hint_codes": meta.get("retrieval_hint_codes"),
                    "packet_id": meta.get("packet_id"),
                    "packet_article_position": meta.get("packet_article_position"),
                    "no_relevant_discourse": bool(art.get("no_relevant_discourse")),
                    "insufficient_context": bool(art.get("insufficient_context")),
                    "n_codes": len(codes),
                    "n_possible": len([p for p in possible if coerce_possible_entry(p)]),
                    "code_families_emitted": "|".join(
                        sorted({family_of(c.get("code")) for c in codes if isinstance(c, dict)} - {None})
                    ),
                    "codes_emitted": "|".join(
                        sorted({c.get("code") for c in codes if isinstance(c, dict) and c.get("code")})
                    ),
                    "legitimation_effect_present": bool(derived.get("legitimation_effect_present")),
                    "polarity": derived.get("polarity"),
                    "basis_tags": "|".join(basis_tags),
                    "n_basis_tags": len(basis_tags),
                    "derived_target": derived.get("target"),
                    "exclusion_boundary_present": bool(derived.get("exclusion_boundary_present")),
                    "right_to_define_present": bool(derived.get("right_to_define_present")),
                    "annotation_notes": art.get("annotation_notes") or "",
                    "annotation_notes_words": word_count(art.get("annotation_notes")),
                    "source_result_file": art.get("_source_file"),
                }
            )

            for c in codes:
                if not isinstance(c, dict):
                    continue
                code = c.get("code")
                if not code:
                    continue
                fam = c.get("family") or family_of(code)
                code_rows.append(
                    {
                        "article_id": article_id,
                        "periodical": periodical,
                        "periodical_label": periodical_label(periodical),
                        "issue_id": meta.get("issue_id"),
                        "year": issue_to_year(meta.get("issue_id", "")),
                        "language": meta.get("language"),
                        "article_type": meta.get("article_type"),
                        "review_strategy": meta.get("review_strategy"),
                        "family": fam,
                        "code": code,
                        "confidence": c.get("confidence"),
                        "evidence_quote": c.get("evidence_quote") or "",
                        "evidence_quote_words": word_count(c.get("evidence_quote")),
                        "target": c.get("target") or "",
                        "target_words": word_count(c.get("target")),
                        "rationale": c.get("rationale") or "",
                        "rationale_words": word_count(c.get("rationale")),
                    }
                )

            for entry in possible:
                norm = coerce_possible_entry(entry)
                if not norm:
                    continue
                possible_rows.append(
                    {
                        "article_id": article_id,
                        "periodical": periodical,
                        "periodical_label": periodical_label(periodical),
                        "issue_id": meta.get("issue_id"),
                        "year": issue_to_year(meta.get("issue_id", "")),
                        "code": norm["code"],
                        "family": norm["family"],
                        "reason": norm["reason"],
                        "reason_words": word_count(norm["reason"]),
                    }
                )

            for b in basis_tags:
                basis_rows.append(
                    {
                        "article_id": article_id,
                        "periodical": periodical,
                        "periodical_label": periodical_label(periodical),
                        "issue_id": meta.get("issue_id"),
                        "year": issue_to_year(meta.get("issue_id", "")),
                        "basis": b,
                        "polarity": derived.get("polarity"),
                    }
                )

    articles_df = pd.DataFrame(article_rows)
    codes_df = pd.DataFrame(code_rows)
    possible_df = pd.DataFrame(possible_rows)
    basis_df = pd.DataFrame(basis_rows)

    articles_df.to_csv(DATA_DIR / "articles.csv", index=False)
    codes_df.to_csv(DATA_DIR / "codes.csv", index=False)
    possible_df.to_csv(DATA_DIR / "possible.csv", index=False)
    basis_df.to_csv(DATA_DIR / "basis.csv", index=False)

    print(f"articles.csv  {len(articles_df):>5} rows")
    print(f"codes.csv     {len(codes_df):>5} rows")
    print(f"possible.csv  {len(possible_df):>5} rows")
    print(f"basis.csv     {len(basis_df):>5} rows")


if __name__ == "__main__":
    build()
