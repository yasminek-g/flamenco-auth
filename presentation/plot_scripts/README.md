# Presentation Figure Scripts

These scripts regenerate the deck figures as seaborn/matplotlib charts and save both PNG and SVG copies to `figs/`.

Run all figures:

```bash
python presentation/plot_scripts/make_all_figs.py
```

Or run individual figures:

```bash
python presentation/plot_scripts/plot_01_primary_mode_share.py
python presentation/plot_scripts/plot_02_section_architecture.py
python presentation/plot_scripts/plot_03_agreement_bound.py
python presentation/plot_scripts/plot_04_candil_temporal_modes.py
python presentation/plot_scripts/plot_05_audit_composition.py
# ... through plot_18_claims_evidence_matrix.py
```

Default inputs and what they support:

- `human_gold_audit_complete copy.csv`: human audit composition, human gold mode/family/code labels, article word counts, audit confidence, and the gold side of all human-vs-LLM diagnostics.
- `candil_llm_annotation_pipeline/llm_annotation_outputs_v12_backup/`: Candil accepted LLM labels used for full-corpus trends and human-vs-LLM comparisons.
- `jaleo_llm_annotation_pipeline_v12/llm_annotation_outputs_v12_backup/`: Jaleo accepted LLM labels used for full-corpus trends and human-vs-LLM comparisons.
- `candil_llm_annotation_pipeline/pilot_v12/pilot_window_candidates.csv` and `jaleo_llm_annotation_pipeline_v12/pilot_v12/pilot_window_candidates.csv`: retrieval-stage candidate windows used for attrition analysis.
- `candil_llm_annotation_pipeline/pilot_v12/pilot_candidate_codes.csv` and `jaleo_llm_annotation_pipeline_v12/pilot_v12/pilot_candidate_codes.csv`: candidate codes sent to the LLM used for attrition analysis.
- `tmp/candil-recurring-section-kept-analysis/article_footprint.csv`: Candil article-level section detection and year metadata.
- `tmp/candil-recurring-section-kept-analysis/recurring_section_summary.csv`: Candil recurring-section support counts.
- `reports/jaleo_recurring_sections_by_title/article_title_section_review.csv`: Jaleo article-level title/section review and issue dates.
- `human_gold_comparison_outputs/candil_section_enriched_comparison.csv`: Candil human-gold audit rows enriched with detected section labels.
- `reports/human_gold_jaleo_section_linkage/human_gold_jaleo_with_section_candidates.csv`: Jaleo human-gold audit rows linked to section candidates.

The LLM summary loader reads directly from the two `llm_annotation_outputs_v12_backup/` JSON directories. It does not use the aggregate `llm_annotation_summary.csv` files.

Figure inventory:

- `fig01_primary_mode_share`: human-gold primary mode share.
- `fig02_section_architecture`: Candil/Jaleo section coverage and rank-size shape.
- `fig03_agreement_bound`: Cohen's kappa reliability boundary; all bars are kappa.
- `fig04_temporal_modes_by_periodical`: unsmoothed annual accepted-LLM family shares for Candil and Jaleo.
- `fig05_audit_composition`: audit inclusion and period-bin balance.
- `fig06_gold_label_density`: human-gold code/family density per audited article.
- `fig07_any_gold_family_distribution`: any-family human-gold support by periodical.
- `fig08_gold_vs_llm_family_support`: support mismatch between human gold and LLM families.
- `fig09_family_f1_ranked`: family F1, ranked for main-deck readability.
- `fig10_missed_extra_family_heatmap`: family substitutions where the LLM adds one family while missing another.
- `fig11_code_failure_concentration`: code-level false negatives/false positives/true positives.
- `fig12_retrieval_annotation_attrition`: retrieval windows to candidates to accepted labels to exact gold matches.
- `fig13_length_vs_agreement`: article length vs article-level family F1.
- `fig14_agreement_by_periodical_confidence`: agreement by periodical, label level, and human audit confidence.
- `fig15_sectioned_vs_unsectioned_agreement`: agreement inside/outside detected sections.
- `fig16_section_architecture_over_time`: sectioned article share by year for both periodicals.
- `fig17_section_to_gold_family_heatmap`: section labels as weak genre proxies using human-gold labels.
- `fig18_claims_evidence_matrix`: presentation-facing matrix separating strong, caveated, and avoided claims.
