from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from common import RED, ROOT, output_line, save_figure, setup_theme
from critical_common import GOLD_CSV, CORE_FAMILIES, merged_gold_llm, sources_text


SOURCES = [GOLD_CSV, ROOT / "candil_llm_annotation_pipeline" / "llm_annotation_outputs_v12_backup", ROOT / "jaleo_llm_annotation_pipeline_v12" / "llm_annotation_outputs_v12_backup"]


def build_heatmap() -> pd.DataFrame:
    merged = merged_gold_llm()
    rows = []
    for _, row in merged.iterrows():
        missed = row["gold_families"] - row["llm_families"]
        extra = row["llm_families"] - row["gold_families"]
        for gold_family in missed:
            for llm_family in extra:
                rows.append({"missed_gold_family": gold_family, "extra_llm_family": llm_family})
    if not rows:
        return pd.DataFrame(0, index=CORE_FAMILIES, columns=CORE_FAMILIES)
    matrix = pd.crosstab(
        pd.DataFrame(rows)["missed_gold_family"],
        pd.DataFrame(rows)["extra_llm_family"],
    )
    return matrix.reindex(index=CORE_FAMILIES, columns=CORE_FAMILIES, fill_value=0)


def plot(outdir: Path | None = None) -> list[Path]:
    import matplotlib.pyplot as plt
    import seaborn as sns

    setup_theme()
    matrix = build_heatmap()
    fig, ax = plt.subplots(figsize=(8.4, 7.2))
    sns.heatmap(matrix, annot=True, fmt="d", cmap=sns.light_palette(RED, as_cmap=True), cbar_kws={"label": "Articles"}, linewidths=0.5, linecolor="#efe8de", ax=ax)
    ax.set_xlabel("Extra LLM family")
    ax.set_ylabel("Missed human-gold family")
    ax.tick_params(axis="x", rotation=35, labelsize=10)
    ax.tick_params(axis="y", rotation=0)
    for label in ax.get_xticklabels():
        label.set_ha("right")
    fig.subplots_adjust(top=0.95)
    return save_figure(fig, "fig10_missed_extra_family_heatmap", outdir)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=Path, default=ROOT / "figs")
    args = parser.parse_args()
    paths = plot(args.outdir)
    print(sources_text(SOURCES))
    print(output_line(paths))


if __name__ == "__main__":
    main()
