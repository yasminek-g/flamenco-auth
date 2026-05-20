# LLM annotation pipeline for flamenco periodicals

This folder contains the reusable annotation pipeline used for the Jaleo corpus.

It can be reused for Spanish periodicals by placing enriched Spanish article CSVs in the `Used/` folder.

The main pipeline is:

```text
enriched article CSVs
→ evidence-window extraction
→ candidate code selection from codebook triggers
→ article-level LLM prompt
→ OpenAI JSON annotation
→ summary CSV
→ result tables and graphs
```

The annotations should be treated as **provisional structured readings**, not final ground truth.

---

## 1. Folder structure

```text
llm_annotation_pipeline/
│
├── Used/
├── Rest/
├── pilot/
├── llm_annotation_outputs/
│
├── flamenco_codebook_v10.json
├── window_builder.py
├── llm_test_runner.py
├── aggregate_llm_outputs.py
├── make_result_tables.py
├── check_used_csvs.py
├── fix_missing_issue_ids.py
├── make_corpus_overview_outputs.py
├── make_code_frequency_outputs.py
├── make_top_code_explanation_table.py
├── make_period_code_outputs.py
│
└── bertopic_optional/
```

Explanation:

- `Used/` = enriched CSV files to process.
- `Rest/` = enriched CSV files not currently processed.
- `pilot/` = generated prompt files.
- `llm_annotation_outputs/` = JSON outputs from the OpenAI API.
- `flamenco_codebook_v10.json` = shared codebook used for all corpora.
- `bertopic_optional/` = optional exploratory topic modelling scripts.

The most important point is that all corpora should use the same `flamenco_codebook_v10.json`, so that Jaleo and Spanish periodical results remain comparable.

---

## 2. Required Python packages

Install the basic packages:

```powershell
python -m pip install pandas openai matplotlib
```

---

## 3. API key

Before running the LLM step, set your OpenAI API key in PowerShell:

```powershell
$env:OPENAI_API_KEY = "your-api-key-here"
```

Do not paste the API key into the code and do not commit it to GitHub.

You can check whether the key is set with:

```powershell
$env:OPENAI_API_KEY
```

If it prints nothing, set the key again.

---

## 4. Add enriched CSVs

Put the enriched CSV files you want to process into:

```text
Used/
```

Only files in `Used/` will be processed.

For example:

```text
Used/
├── 1977-08_enriched.csv
├── 1977-09_enriched.csv
└── ...
```

For the Spanish corpus, place the enriched Spanish periodical CSVs in `Used/`.

---

## 5. Check issue IDs

Run:

```powershell
python check_used_csvs.py
```

Check that every CSV has a valid `issue_id` and no `<MISSING>` rows.

Example of a good result:

```text
Rows: 15
PERIODICAL_1978_02    15
```

Example of a problem:

```text
Rows: 15
<MISSING>    15
```

If issue IDs are missing, adapt and run:

```powershell
python fix_missing_issue_ids.py
```

Important: `fix_missing_issue_ids.py` may need to be adapted to match the Spanish periodical file names. The Jaleo version assumes Jaleo-style filenames.

---

## 6. Clean old prompts and outputs before a fresh run

Before a fresh run, delete old generated prompts and outputs:

```powershell
Remove-Item pilot\pilot_outputs_* -Recurse -Force
Remove-Item llm_annotation_outputs\*.json
```

Do **not** delete outputs if you are continuing an interrupted run.  
The LLM runner skips existing JSON files, so rerunning can continue from where it stopped.

---

## 7. Run the main annotation pipeline

Run the scripts in this order:

```powershell
python window_builder.py
python llm_test_runner.py
python aggregate_llm_outputs.py
python make_result_tables.py
```

What each script does:

```text
window_builder.py
→ creates evidence-window prompts from enriched CSVs

llm_test_runner.py
→ sends prompts to OpenAI and saves JSON annotations

aggregate_llm_outputs.py
→ combines JSON outputs into llm_annotation_summary.csv

make_result_tables.py
→ creates table_1_corpus_overview.csv and table_2_code_frequency.csv
```

Expected outputs:

```text
llm_annotation_summary.csv
table_1_corpus_overview.csv
table_2_code_frequency.csv
```

---

## 8. Create corpus overview and code-frequency results

After the main pipeline has finished, run:

```powershell
python make_corpus_overview_outputs.py
python make_code_frequency_outputs.py
python make_top_code_explanation_table.py
python make_period_code_outputs.py
```

These create:

```text
compact_corpus_summary.csv
corpus_by_year.csv
article_lengths.csv
article_length_by_year.csv
top_codes_overall.csv
code_family_frequency.csv
top10_code_explanation_table.csv
code_frequency_by_period_normalized.csv
code_family_by_period_normalized.csv
```

They also create graphs such as:

```text
issues by year
articles by year
article length over time
top 10 accepted codes
code families
period-normalized code trends
```

---

## 9. Recommended result structure

For the final analysis, use the outputs in this order:

### Result 1: Corpus coverage and structure

Use:

```text
compact_corpus_summary.csv
corpus_by_year.csv
article_length_by_year.csv
```

Purpose:

```text
Show what corpus was processed, how many issues/articles it contains,
and whether coverage varies over time.
```

### Result 2: Top accepted codes

Use:

```text
top_codes_overall.csv
top10_code_explanation_table.csv
```

Purpose:

```text
Show which legitimacy codes appear most often.
```

### Result 3: Code families

Use:

```text
code_family_frequency.csv
```

Purpose:

```text
Show whether legitimacy is mostly constructed through community,
heritage, tradition, authenticity, formal legitimacy, etc.
```

### Result 4: Change over time

Use:

```text
code_frequency_by_period_normalized.csv
code_family_by_period_normalized.csv
```

Purpose:

```text
Show how the code patterns change over broader periods, normalized by corpus size.
```

---

## 10. Optional: BERTopic exploratory topic modelling

The folder:

```text
bertopic_optional/
```

contains optional scripts for exploratory BERTopic topic modelling.

This is **not** the main annotation pipeline.

The main evidence comes from the codebook-based LLM annotations.  
BERTopic is only used as a supplementary exploratory method to describe broader article-topic or article-function patterns in the corpus.

Use BERTopic if you want to ask:

```text
What kinds of recurring article topics or periodical functions appear in the corpus?
```

This is different from the codebook question:

```text
How is flamenco legitimacy constructed?
```

Go to:

```text
bertopic_optional/README.md
```

for detailed setup instructions.

---

## 11. Important methodological notes

- Use the same `flamenco_codebook_v10.json` for all corpora.
- The LLM annotations are provisional structured readings, not final labels.
- Warning flags should be manually inspected.
- `needs_human_review` is conservative and should not automatically be interpreted as failure.
- If CSV files use comma separation instead of semicolon separation, update `sep=";"` in the scripts.
- If the LLM run stops due to internet/API problems, rerun `llm_test_runner.py`; existing JSON outputs should be skipped.