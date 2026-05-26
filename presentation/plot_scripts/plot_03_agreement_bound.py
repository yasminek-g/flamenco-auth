from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import (
    INK,
    INK_2,
    INK_3,
    RED,
    RED_DEEP,
    ROOT,
    family_set,
    load_annotation_summary,
    output_line,
    read_csv,
    save_figure,
    setup_theme,
    split_labels,
)


def multilabel_micro_prf(
    gold_sets: list[set[str]], pred_sets: list[set[str]]
) -> tuple[float, float, float]:
    """Micro-averaged precision / recall / F1 over multi-label article×label decisions.

    Gold is a single reconciled reference, so this is a prediction-vs-truth evaluation,
    not inter-rater agreement — F1 is the right frame, not a chance-corrected κ.
    """
    if len(gold_sets) != len(pred_sets):
        raise ValueError("Gold and prediction lists must have the same length")

    tp = fp = fn = 0
    for gold, pred in zip(gold_sets, pred_sets):
        tp += len(gold & pred)
        fp += len(pred - gold)
        fn += len(gold - pred)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return precision, recall, f1


def build_data(gold_csv: Path, periodical: str | None = None) -> pd.DataFrame:
    gold = read_csv(gold_csv)
    gold = gold[gold["final_include_for_metrics"].astype(str).str.lower().eq("yes")].copy()
    if periodical:
        gold = gold[gold["periodical"].eq(periodical)].copy()

    summaries = []
    for name in ["candil", "jaleo"]:
        if periodical and name != periodical:
            continue
        summary = load_annotation_summary(name).copy()
        summary["periodical"] = name
        summary["join_article_id"] = summary["article_id"]
        summaries.append(summary)
    llm = pd.concat(summaries, ignore_index=True)

    gold["join_article_id"] = gold["article_id"]
    is_jaleo = gold["periodical"].eq("jaleo")
    gold.loc[is_jaleo, "join_article_id"] = gold.loc[is_jaleo, "article_id"].astype(str).str.split("::").str[-1]

    merged = gold.merge(
        llm[["periodical", "issue_id", "join_article_id", "all_accepted_codes"]],
        on=["periodical", "issue_id", "join_article_id"],
        how="left",
    )
    merged["gold_codes"] = merged["human_gold_codes"].apply(lambda value: set(split_labels(value)))
    merged["llm_codes"] = merged["all_accepted_codes"].apply(lambda value: set(split_labels(value)))
    merged["gold_families"] = merged["gold_codes"].apply(family_set)
    merged["llm_families"] = merged["llm_codes"].apply(family_set)

    family_p, family_r, family_f1 = multilabel_micro_prf(
        merged["gold_families"].tolist(), merged["llm_families"].tolist()
    )
    code_p, code_r, code_f1 = multilabel_micro_prf(
        merged["gold_codes"].tolist(), merged["llm_codes"].tolist()
    )

    rows = []
    for level, (prec, rec, f1) in [
        ("Family level", (family_p, family_r, family_f1)),
        ("Code level", (code_p, code_r, code_f1)),
    ]:
        rows.append({"level": level, "metric": "Precision", "value": prec})
        rows.append({"level": level, "metric": "Recall", "value": rec})
        rows.append({"level": level, "metric": "F1", "value": f1})
    return pd.DataFrame(rows)


def plot(data: pd.DataFrame, outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    palette = {"Precision": INK_3, "Recall": INK_2, "F1": RED}

    fig, ax = plt.subplots(figsize=(11.5, 6.6))
    sns.barplot(
        data=data,
        x="level",
        y="value",
        hue="metric",
        hue_order=["Precision", "Recall", "F1"],
        palette=palette,
        ax=ax,
    )

    # Mark the F1 bars so the headline statistic reads at a glance.
    for patch, (_, row) in zip(ax.patches, _bar_rows(data).iterrows()):
        if row["metric"] == "F1":
            patch.set_edgecolor(RED_DEEP)
            patch.set_linewidth(1.4)

    for patch in ax.patches:
        height = patch.get_height()
        if height <= 0:
            continue
        ax.text(
            patch.get_x() + patch.get_width() / 2,
            height + 0.015,
            f"{height:.2f}",
            ha="center",
            va="bottom",
            color=INK,
            fontsize=10,
            fontweight="bold",
        )

    ax.set_ylabel("Score (gold vs LLM)", fontsize=12)
    ax.set_xlabel("")
    ax.set_ylim(0, 1)
    ax.yaxis.set_major_formatter(lambda y, _pos: f"{y:.1f}")
    ax.tick_params(axis="both", labelsize=11)
    ax.legend(title="", loc="upper right", frameon=False, fontsize=11)
    fig.subplots_adjust(top=0.95, left=0.1, right=0.98, bottom=0.1)

    return save_figure(fig, "fig03_agreement_bound", outdir)


def _bar_rows(data: pd.DataFrame) -> pd.DataFrame:
    """Reorder rows to match seaborn's patch order (grouped by hue, then x)."""
    order = []
    for metric in ["Precision", "Recall", "F1"]:
        for level in ["Family level", "Code level"]:
            order.append(
                data[(data["metric"] == metric) & (data["level"] == level)].iloc[0]
            )
    return pd.DataFrame(order).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-csv", type=Path, default=ROOT / "human_gold_audit_complete copy.csv")
    parser.add_argument(
        "--periodical",
        choices=["candil", "jaleo"],
        default=None,
        help="Optional subset. Defaults to the full 163-article metric audit.",
    )
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(build_data(args.gold_csv, args.periodical), args.outdir)
    print(output_line(paths))


if __name__ == "__main__":
    main()
