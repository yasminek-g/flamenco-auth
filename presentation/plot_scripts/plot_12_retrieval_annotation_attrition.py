from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import (
    DROPPED_FAMILIES,
    FAMILY_FOLD,
    INK_2,
    ROOT,
    code_family,
    output_line,
    read_csv,
    save_figure,
    setup_theme,
)
from critical_common import CANDIL_CANDIDATES, CANDIL_WINDOWS, GOLD_CSV, JALEO_CANDIDATES, JALEO_WINDOWS, merged_gold_llm, sources_text


SOURCES = [GOLD_CSV, CANDIL_WINDOWS, JALEO_WINDOWS, CANDIL_CANDIDATES, JALEO_CANDIDATES, ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup", ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup"]


def normalize_family_counts(series: pd.Series) -> pd.Series:
    """Fold TRAD into AUTH and drop deprecated families (WCL, LEGIT)."""
    folded = series.rename(index=lambda fam: FAMILY_FOLD.get(fam, fam))
    folded = folded.groupby(level=0).sum()
    drop = [fam for fam in DROPPED_FAMILIES if fam in folded.index]
    return folded.drop(index=drop)


def family_counts_from_csvs(paths: list[Path], family_col: str) -> pd.Series:
    frames = [read_csv(path, sep=";") for path in paths]
    df = pd.concat(frames, ignore_index=True)
    counts = df.drop_duplicates(["issue_id", "article_id", "concept_id"])[family_col].value_counts()
    return normalize_family_counts(counts)


def accepted_family_counts() -> pd.Series:
    merged = merged_gold_llm()
    counts: dict[str, int] = {}
    for codes in merged["llm_codes"]:
        for code in codes:
            family = code_family(code)
            counts[family] = counts.get(family, 0) + 1
    return pd.Series(counts)


def matched_family_counts() -> pd.Series:
    merged = merged_gold_llm()
    counts: dict[str, int] = {}
    for _, row in merged.iterrows():
        for code in row["gold_codes"] & row["llm_codes"]:
            family = code_family(code)
            counts[family] = counts.get(family, 0) + 1
    return pd.Series(counts)


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    windows = family_counts_from_csvs([CANDIL_WINDOWS, JALEO_WINDOWS], "family_id")
    candidates = family_counts_from_csvs([CANDIL_CANDIDATES, JALEO_CANDIDATES], "family_id")
    accepted = accepted_family_counts()
    matched = matched_family_counts()
    families = sorted(set(windows.index) | set(candidates.index) | set(accepted.index) | set(matched.index))
    rows = []
    for family in families:
        for stage, series in [
            ("retrieved windows", windows),
            ("sent as candidates", candidates),
            ("accepted by LLM", accepted),
            ("exact gold code match", matched),
        ]:
            count = int(series.get(family, 0))
            rows.append({"family": family, "stage": stage, "count": count, "count_plus_one": count + 1})
    data = pd.DataFrame(rows)
    data["stage"] = pd.Categorical(data["stage"], ["retrieved windows", "sent as candidates", "accepted by LLM", "exact gold code match"], ordered=True)

    fig, ax = plt.subplots(figsize=(12.5, 6.2))
    sns.lineplot(data=data, x="stage", y="count_plus_one", hue="family", marker="o", linewidth=2, ax=ax)
    ax.set_yscale("log")
    ax.set_xlabel("")
    ax.set_ylabel("Count + 1 (log scale)")
    ax.tick_params(axis="x", rotation=10)
    ax.legend(title="family", frameon=False, ncol=4, fontsize=8)
    fig.subplots_adjust(top=0.95, bottom=0.20)
    return save_figure(fig, "fig12_retrieval_annotation_attrition", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
