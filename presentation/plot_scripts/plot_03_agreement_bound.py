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


HUMAN_HUMAN_FAMILY_KAPPA = 0.78


def multilabel_cohens_kappa(gold_sets: list[set[str]], pred_sets: list[set[str]]) -> float:
    """Cohen's kappa over article x label binary decisions.

    The audit is multi-label, so each article contributes one present/absent decision
    for every label observed in either the gold or LLM outputs.
    """
    labels = sorted(set().union(*gold_sets, *pred_sets))
    if not labels:
        return 0.0

    gold_binary = []
    pred_binary = []
    if len(gold_sets) != len(pred_sets):
        raise ValueError("Gold and prediction lists must have the same length")

    for gold, pred in zip(gold_sets, pred_sets):
        for label in labels:
            gold_binary.append(label in gold)
            pred_binary.append(label in pred)

    total = len(gold_binary)
    observed = sum(g == p for g, p in zip(gold_binary, pred_binary)) / total
    gold_yes = sum(gold_binary) / total
    pred_yes = sum(pred_binary) / total
    expected = (gold_yes * pred_yes) + ((1 - gold_yes) * (1 - pred_yes))
    return (observed - expected) / (1 - expected) if expected != 1 else 0.0


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

    family_kappa = multilabel_cohens_kappa(
        merged["gold_families"].tolist(), merged["llm_families"].tolist()
    )
    code_kappa = multilabel_cohens_kappa(merged["gold_codes"].tolist(), merged["llm_codes"].tolist())
    return pd.DataFrame(
        [
            {
                "comparison": "Human vs human",
                "level": "family",
                "statistic": "κ",
                "value": HUMAN_HUMAN_FAMILY_KAPPA,
                "source": "audit adjudication benchmark",
            },
            {
                "comparison": "Human gold vs LLM",
                "level": "family",
                "statistic": "κ",
                "value": family_kappa,
                "source": "recomputed from audit copy and v12 outputs",
            },
            {
                "comparison": "Human gold vs LLM",
                "level": "code",
                "statistic": "κ",
                "value": code_kappa,
                "source": "recomputed from audit copy and v12 outputs",
            },
        ]
    )


def plot(data: pd.DataFrame, outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    plot_df = data.copy()
    plot_df["label"] = [
        "Human vs human\nfamily level",
        "Human gold vs LLM\nfamily level",
        "Human gold vs LLM\ncode level",
    ]

    fig, ax = plt.subplots(figsize=(11.5, 6.6))
    colors = [INK_2, RED, RED]
    bars = sns.barplot(data=plot_df, y="label", x="value", palette=colors, hue="label", ax=ax)
    legend = ax.get_legend()
    if legend is not None:
        legend.remove()

    ax.axvspan(0.0, 0.2, color="#f6f4ef", zorder=0)
    ax.axvspan(0.2, 0.4, color="#efebe2", zorder=0)
    ax.axvspan(0.4, 0.6, color="#e7e0d2", zorder=0)
    ax.axvspan(0.6, 0.8, color="#dcd2bf", zorder=0)
    ax.axvspan(0.8, 1.0, color="#ccbfa3", zorder=0)
    ax.axvline(0.4, color=RED, linestyle=(0, (4, 3)), linewidth=1.8)
    ax.text(0.405, -0.42, "reliability floor", color=RED, fontsize=10, fontweight="bold")

    hatch_bar = ax.patches[2]
    hatch_bar.set_hatch("///")
    hatch_bar.set_edgecolor(RED_DEEP)

    for idx, row in plot_df.iterrows():
        ax.text(
            row["value"] + 0.025,
            idx,
            f"{row['statistic']} = {row['value']:.2f}",
            va="center",
            color=INK,
            fontsize=11,
            fontweight="bold",
        )

    ax.set_xlabel("Agreement statistic", fontsize=12)
    ax.set_ylabel("")
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(lambda x, _pos: f"{x:.1f}")
    ax.tick_params(axis="both", labelsize=11)
    fig.subplots_adjust(top=0.95, left=0.24, right=0.98, bottom=0.13)

    return save_figure(fig, "fig03_agreement_bound", outdir)


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
