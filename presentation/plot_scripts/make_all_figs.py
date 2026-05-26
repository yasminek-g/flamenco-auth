from __future__ import annotations

from pathlib import Path

from common import FIGS_DIR, ROOT
from plot_01_primary_mode_share import build_data as build_fig01_data
from plot_01_primary_mode_share import plot as plot_fig01
from plot_02_section_architecture import build_data as build_fig02_data
from plot_02_section_architecture import plot as plot_fig02
from plot_03_agreement_bound import build_data as build_fig03_data
from plot_03_agreement_bound import plot as plot_fig03
from plot_04_candil_temporal_modes import build_data as build_fig04_data
from plot_04_candil_temporal_modes import plot as plot_fig04
from plot_05_audit_composition import plot as plot_fig05
from plot_06_gold_label_density import plot as plot_fig06
from plot_07_any_gold_family_distribution import plot as plot_fig07
from plot_08_gold_vs_llm_family_support import plot as plot_fig08
from plot_09_family_f1_ranked import plot as plot_fig09
from plot_10_missed_extra_family_heatmap import plot as plot_fig10
from plot_11_code_failure_concentration import plot as plot_fig11
from plot_12_retrieval_annotation_attrition import plot as plot_fig12
from plot_13_length_vs_agreement import plot as plot_fig13
from plot_14_agreement_by_periodical_confidence import plot as plot_fig14
from plot_15_sectioned_vs_unsectioned_agreement import plot as plot_fig15
from plot_16_section_architecture_over_time import plot as plot_fig16
from plot_17_section_to_gold_family_heatmap import plot as plot_fig17
from plot_18_claims_evidence_matrix import plot as plot_fig18


def main() -> None:
    outdir = FIGS_DIR
    gold_csv = ROOT / "human_gold_audit_complete copy.csv"

    jobs = [
        ("Figure 1", lambda: plot_fig01(build_fig01_data(gold_csv), outdir)),
        (
            "Figure 2",
            lambda: plot_fig02(
                *build_fig02_data(
                    ROOT
                    / "tmp"
                    / "candil-recurring-section-kept-analysis"
                    / "recurring_section_summary.csv",
                    ROOT
                    / "tmp"
                    / "candil-recurring-section-kept-analysis"
                    / "article_footprint.csv",
                    ROOT
                    / "reports"
                    / "jaleo_recurring_sections_by_title"
                    / "article_title_section_review.csv",
                    2,
                ),
                outdir,
            ),
        ),
        ("Figure 3", lambda: plot_fig03(build_fig03_data(gold_csv), outdir)),
        ("Figure 4", lambda: plot_fig04(build_fig04_data(), outdir)),
        ("Figure 5", lambda: plot_fig05(outdir)),
        ("Figure 6", lambda: plot_fig06(outdir)),
        ("Figure 7", lambda: plot_fig07(outdir)),
        ("Figure 8", lambda: plot_fig08(outdir)),
        ("Figure 9", lambda: plot_fig09(outdir)),
        ("Figure 10", lambda: plot_fig10(outdir)),
        ("Figure 11", lambda: plot_fig11(outdir)),
        ("Figure 12", lambda: plot_fig12(outdir)),
        ("Figure 13", lambda: plot_fig13(outdir)),
        ("Figure 14", lambda: plot_fig14(outdir)),
        ("Figure 15", lambda: plot_fig15(outdir)),
        ("Figure 16", lambda: plot_fig16(outdir)),
        ("Figure 17", lambda: plot_fig17(outdir)),
        ("Figure 18", lambda: plot_fig18(outdir)),
    ]

    for label, run in jobs:
        paths = run()
        rel_paths = ", ".join(str(Path(path).relative_to(ROOT)) for path in paths)
        print(f"{label}: wrote {rel_paths}")


if __name__ == "__main__":
    main()
