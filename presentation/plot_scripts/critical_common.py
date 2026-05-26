from __future__ import annotations

from pathlib import Path
import textwrap

import pandas as pd

from common import ROOT, code_family, family_set, load_annotation_summary, read_csv, split_labels


GOLD_CSV = ROOT / "human_gold_audit_complete copy.csv"
CANDIL_CANDIDATES = ROOT / "candil_llm_annotation_pipeline" / "pilot_v12" / "pilot_candidate_codes.csv"
JALEO_CANDIDATES = ROOT / "jaleo_llm_annotation_pipeline_v12" / "pilot_v12" / "pilot_candidate_codes.csv"
CANDIL_WINDOWS = ROOT / "candil_llm_annotation_pipeline" / "pilot_v12" / "pilot_window_candidates.csv"
JALEO_WINDOWS = ROOT / "jaleo_llm_annotation_pipeline_v12" / "pilot_v12" / "pilot_window_candidates.csv"
CANDIL_SECTIONS = ROOT / "human_gold_comparison_outputs" / "candil_section_enriched_comparison.csv"
JALEO_SECTIONS = (
    ROOT / "reports" / "human_gold_jaleo_section_linkage" / "human_gold_jaleo_with_section_candidates.csv"
)
CANDIL_ARTICLE_FOOTPRINT = (
    ROOT / "tmp" / "candil-recurring-section-kept-analysis" / "article_footprint.csv"
)
JALEO_ARTICLE_SECTIONS = (
    ROOT / "reports" / "jaleo_recurring_sections_by_title" / "article_title_section_review.csv"
)

CORE_FAMILIES = ["AUTH", "COMM", "CRIT", "HERIT", "PED", "TRAD", "WCL", "LEGIT"]


def metric_gold(gold_csv: Path = GOLD_CSV, metrics_only: bool = True) -> pd.DataFrame:
    gold = read_csv(gold_csv)
    if metrics_only:
        gold = gold[gold["final_include_for_metrics"].astype(str).str.lower().eq("yes")].copy()
    else:
        gold = gold.copy()
    gold["join_article_id"] = gold["article_id"]
    is_jaleo = gold["periodical"].eq("jaleo")
    gold.loc[is_jaleo, "join_article_id"] = (
        gold.loc[is_jaleo, "article_id"].astype(str).str.split("::").str[-1]
    )
    gold["gold_codes"] = gold["human_gold_codes"].apply(lambda value: set(split_labels(value)))
    gold["gold_families"] = gold["gold_codes"].apply(family_set)
    gold["gold_code_count"] = gold["gold_codes"].apply(len)
    gold["gold_family_count"] = gold["gold_families"].apply(len)
    return gold


def merged_gold_llm(gold_csv: Path = GOLD_CSV) -> pd.DataFrame:
    gold = metric_gold(gold_csv)
    summaries = []
    for periodical in ["candil", "jaleo"]:
        summary = load_annotation_summary(periodical).copy()
        summary["periodical"] = periodical
        summary["join_article_id"] = summary["article_id"]
        summaries.append(summary)
    llm = pd.concat(summaries, ignore_index=True)
    llm = llm.drop_duplicates(["periodical", "issue_id", "join_article_id"], keep="last")
    merged = gold.merge(
        llm[["periodical", "issue_id", "join_article_id", "all_accepted_codes"]],
        on=["periodical", "issue_id", "join_article_id"],
        how="left",
    )
    merged["llm_codes"] = merged["all_accepted_codes"].apply(lambda value: set(split_labels(value)))
    merged["llm_families"] = merged["llm_codes"].apply(family_set)
    return add_article_metrics(merged)


def prf(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return precision, recall, f1


def add_article_metrics(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        gold_codes = row["gold_codes"]
        llm_codes = row["llm_codes"]
        gold_families = row["gold_families"]
        llm_families = row["llm_families"]
        code_tp = len(gold_codes & llm_codes)
        code_fp = len(llm_codes - gold_codes)
        code_fn = len(gold_codes - llm_codes)
        family_tp = len(gold_families & llm_families)
        family_fp = len(llm_families - gold_families)
        family_fn = len(gold_families - llm_families)
        code_p, code_r, code_f1 = prf(code_tp, code_fp, code_fn)
        family_p, family_r, family_f1 = prf(family_tp, family_fp, family_fn)
        payload = row.to_dict()
        payload.update(
            {
                "code_tp": code_tp,
                "code_fp": code_fp,
                "code_fn": code_fn,
                "code_precision": code_p,
                "code_recall": code_r,
                "code_f1": code_f1,
                "family_tp": family_tp,
                "family_fp": family_fp,
                "family_fn": family_fn,
                "family_precision": family_p,
                "family_recall": family_r,
                "family_f1": family_f1,
                "family_exact": gold_families == llm_families,
                "family_overlap": bool(gold_families & llm_families),
                "code_exact": gold_codes == llm_codes,
                "code_overlap": bool(gold_codes & llm_codes),
            }
        )
        rows.append(payload)
    return pd.DataFrame(rows)


def label_metrics(df: pd.DataFrame, level: str) -> pd.DataFrame:
    plural = "families" if level == "family" else f"{level}s"
    gold_col = f"gold_{plural}"
    llm_col = f"llm_{plural}"
    labels = sorted(set().union(*df[gold_col], *df[llm_col]))
    rows = []
    for label in labels:
        pairs = zip(df[gold_col], df[llm_col])
        tp = int(sum(label in g and label in p for g, p in pairs))
        pairs = zip(df[gold_col], df[llm_col])
        fp = int(sum(label not in g and label in p for g, p in pairs))
        pairs = zip(df[gold_col], df[llm_col])
        fn = int(sum(label in g and label not in p for g, p in pairs))
        precision, recall, f1 = prf(tp, fp, fn)
        rows.append(
            {
                level: label,
                "gold_support": tp + fn,
                "llm_support": tp + fp,
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "precision": precision,
                "recall": recall,
                "f1": f1,
            }
        )
    return pd.DataFrame(rows)


def explode_set(df: pd.DataFrame, column: str, name: str) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        for value in sorted(row[column]):
            payload = row.to_dict()
            payload[name] = value
            rows.append(payload)
    return pd.DataFrame(rows)


def sources_text(paths: list[Path]) -> str:
    text = "Sources: " + "; ".join(str(path.relative_to(ROOT)) for path in paths)
    return textwrap.fill(text, width=145, subsequent_indent="Sources continued: ")
