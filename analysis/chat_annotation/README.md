# Chat-annotation evaluation: figure pipeline

This folder turns the chat-annotation result JSON for **Candil** and **Jaleo**
(under `chat_annotation_packets_<periodical>/results/`) into a reproducible
set of seaborn figures. Plot scripts are intentionally separate so the
companion notebook (`analysis_notebook.ipynb`) can stay lean and only display
PNGs.

## Layout

| file | role |
| --- | --- |
| `common.py` | palette, theme, paths, IO helpers (`load_articles`, `load_codes`, `load_possible`, `load_basis`) |
| `build_dataset.py` | reads both periodicals' `results/*_results.json` + `manifest.csv` and writes long-form CSVs under `data/` |
| `plot_NN_*.py` | one focused figure each, writes to `figs/*.png` + `figs/*.svg` |
| `make_all_figs.py` | rebuilds `data/` then runs every plot script |
| `analysis_notebook.ipynb` | display-only: imports figures, no plotting code |

## Quick start

```
./flamenco-env/bin/python analysis/chat_annotation/make_all_figs.py
```

Outputs end up under `analysis/chat_annotation/data/` (CSVs) and
`analysis/chat_annotation/figs/` (PNG + SVG side by side, matching the
presentation deck palette).

## Data tables (built by `build_dataset.py`)

* `data/articles.csv` -- one row per article. Carries manifest metadata
  (`periodical`, `issue_id`, `year`, `language`, `article_type`,
  `review_strategy`, `review_text_chars`, `n_trigger_windows`) plus
  article-level annotator output: `n_codes`, `n_possible`,
  `code_families_emitted`, `codes_emitted`, `polarity`, `basis_tags`,
  `legitimation_effect_present`, `exclusion_boundary_present`,
  `right_to_define_present`, `derived_target`, `annotation_notes`.
* `data/codes.csv` -- one row per emitted code with `family`, `code`,
  `confidence`, `evidence_quote(_words)`, `target(_words)`,
  `rationale(_words)`, plus article metadata.
* `data/possible.csv` -- one row per `possible_but_not_emitted` entry with
  `code`, `family`, `reason`, `reason_words`. Strings that the annotator
  returned without a JSON object are parsed best-effort.
* `data/basis.csv` -- one row per (article, basis) tag from
  `derived_analysis.basis` plus the article polarity.

## Figure index

### Basic statistics
1. **plot_01_coverage** -- coverage decisions: coded vs. no relevant discourse
   vs. insufficient context. Counts and shares side by side.
2. **plot_02_codes_per_article** -- distribution of codes per coded article
   (boxen + jittered points; histogram by periodical).
3. **plot_03_family_emission** -- article share vs. emission rate (per 100
   articles) for each family x periodical.
4. **plot_04_code_heatmap** -- full code x periodical emission heatmap (per 100
   articles), divided by family.

### Derived analysis & confidence
5. **plot_05_confidence_by_family** -- stacked high/medium/low shares per
   family x periodical with emission counts annotated.
6. **plot_06_polarity_distribution** -- polarity tag counts and shares per
   periodical.
7. **plot_07_basis_distribution** -- share of articles touching each
   legitimation basis, plus basis tags per article (multi-tag friendly).
8. **plot_08_legitimation_and_boundaries** -- rates of
   `legitimation_effect_present`, `exclusion_boundary_present`, and
   `right_to_define_present` among coded articles.
9. **plot_09_polarity_basis_heatmap** -- polarity x basis co-occurrence
   (counts on top row, share-within-polarity on bottom row).

### Possible-but-not-emitted, reasoning, targets, time
10. **plot_10_possible_top_codes** -- top 15 near-miss codes per periodical
    coloured by family.
11. **plot_11_emit_vs_possible** -- scatter of emit vs. possible counts per
    code with the parity diagonal; also writes
    `data/emit_vs_possible.csv` with the underlying ratios.
12. **plot_12_rationale_length** -- rationale and evidence-quote word counts
    by confidence x periodical (split violin).
13. **plot_13_target_top_terms** -- top 12 target tokens per family across
    both periodicals (stopwords + the word "flamenco" removed).
14. **plot_14_temporal_family** -- per-year family share as stacked area for
    each periodical, with yearly sample sizes printed.

### Authenticity deep dive (AUTH_01..AUTH_04)
15. **plot_15_auth_internal_overlap** -- within-article overlap among
    AUTH subtypes: counts in the lower triangle, Jaccard in the upper.
16. **plot_16_auth_family_overlap** -- AUTH x every other family at the
    article level: P(fam | AUTH), P(AUTH | fam), and Jaccard. Companion CSV
    at `data/auth_family_overlap.csv`.
17. **plot_17_auth_target_terms** -- top target tokens per AUTH subcode x
    periodical (small multiples).
18. **plot_18_auth_polarity_boundaries** -- polarity composition and
    boundary/legitimation flag rates for AUTH-emitting vs. non-AUTH coded
    articles.
19. **plot_19_auth_temporal** -- share of coded articles emitting AUTH per
    year, plus AUTH subtype mix per year, per periodical.
20. **plot_20_auth_emit_vs_possible** -- AUTH emit vs. possible counts and
    near-miss ratio per subcode.

## Notes

* Palette and theme mirror `presentation/plot_scripts/common.py` so figures
  drop straight into the deck.
* The annotator never returned `low` confidence in this batch -- the
  high/medium binary is the active signal.
* `uncertainty_reason` is also empty across both runs.
* Article ids:
  - Candil: `YYYY-MM-<page>-<col>-<slug>`
  - Jaleo : `JALEO_YYYY_MM::A<idx>`
  Year is parsed from the `issue_id` via a `(19|20)\d{2}` match.
