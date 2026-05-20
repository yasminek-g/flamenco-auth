# Optional BERTopic analysis

This folder contains optional scripts for exploratory BERTopic topic modelling.

BERTopic is **not** the main annotation pipeline. The main project evidence should come from the codebook-based LLM annotations.

BERTopic is useful as a supplementary method to understand the broader structure of a corpus, for example:

```text
performance reviews
artist profiles
event listings
community reports
editorials
historical essays
directories/classifieds
```

It should be treated as exploratory, not as final historical classification.

---

## 1. Folder location

This folder should be inside the main pipeline folder:

```text
llm_annotation_pipeline/
└── bertopic_optional/
```

The scripts assume they are run from the main pipeline folder:

```text
llm_annotation_pipeline/
```

not from inside `bertopic_optional/`.

So in PowerShell, first go to the main pipeline folder:

```powershell
cd "PATH_TO_REPO\scripts\llm_annotation_pipeline"
```

Example:

```powershell
cd "C:\Users\Hugob\OneDrive\VS code\EPFL\flamenco-auth\scripts\llm_annotation_pipeline"
```

---

## 2. Required files before running BERTopic

Before running BERTopic, you should already have enriched CSV files in:

```text
Used/
```

For example:

```text
Used/
├── issue_1_enriched.csv
├── issue_2_enriched.csv
└── ...
```

The BERTopic scripts read the article texts from these enriched CSVs.

---

## 3. Recommended installation

BERTopic needs extra packages.

Try this first:

```powershell
python -m pip install bertopic sentence-transformers umap-learn plotly
```

On Windows, `hdbscan` may fail with pip. If that happens, install it with conda:

```powershell
conda install -c conda-forge hdbscan
python -m pip install bertopic sentence-transformers umap-learn plotly
```

If the base environment gives dependency problems, create a clean environment:

```powershell
conda create -n dh412_topic python=3.10 -y
conda activate dh412_topic
conda install -c conda-forge pandas matplotlib scikit-learn hdbscan umap-learn -y
python -m pip install bertopic sentence-transformers plotly
```

Then go back to the main pipeline folder:

```powershell
cd "PATH_TO_REPO\scripts\llm_annotation_pipeline"
```

Test the installation:

```powershell
python -c "import bertopic; import hdbscan; import sentence_transformers; print('BERTopic setup works')"
```

---

## 4. Step 1: Prepare BERTopic input

Run this from the main pipeline folder:

```powershell
python bertopic_optional/prepare_bertopic_input.py
```

This creates:

```text
bertopic_articles.csv
```

This file contains:

```text
issue_id
article_id
title
year
text
word_count
```

Check that `bertopic_articles.csv` actually contains article text, not only titles or metadata.

---

## 5. Step 2: Run BERTopic

Run:

```powershell
python bertopic_optional/run_bertopic_jaleo.py
```

Despite the filename saying `jaleo`, the script can be reused for another corpus.

This creates a folder:

```text
bertopic_outputs/
```

Important outputs:

```text
bertopic_outputs/bertopic_topic_info.csv
bertopic_outputs/bertopic_representative_docs.csv
bertopic_outputs/bertopic_article_topics.csv
```

Meaning:

```text
bertopic_topic_info.csv
→ list of topics, counts, and automatic keyword labels

bertopic_representative_docs.csv
→ example texts for each topic, used for manual interpretation

bertopic_article_topics.csv
→ each article with its assigned BERTopic topic
```

---

## 6. Step 3: Inspect the BERTopic output

Open:

```text
bertopic_outputs/bertopic_topic_info.csv
```

and:

```text
bertopic_outputs/bertopic_representative_docs.csv
```

Do not rely only on the automatic topic names.

Read the representative documents and decide what each topic actually means.

Important:

```text
Topic -1 = mixed/outlier material
```

Usually, topic `-1` should not be interpreted as a substantive topic.

---

## 7. Step 4: Create manual topic labels

The file:

```text
bertopic_topic_labels_example.csv
```

is only an example from the Jaleo corpus.

For a new corpus, create a new file in the main pipeline folder called:

```text
bertopic_topic_labels.csv
```

The file should have this format:

```csv
topic;manual_label;include_in_report;notes
-1;Outliers and mixed material;no;Texts that did not cluster clearly
0;Historical and theoretical writing;yes;Broad essays and historical discussion
1;Announcements and event listings;yes;Notices, events, workshops, performance information
```

You must adapt the labels based on the new corpus.

The manual labels should be based on:

```text
1. topic keywords
2. representative documents
3. historical/contextual interpretation
```

---

## 8. Step 5: Create BERTopic graphs

After creating:

```text
bertopic_topic_labels.csv
```

run:

```powershell
python bertopic_optional/make_bertopic_period_graph.py
python bertopic_optional/make_bertopic_topic_size_table.py
```

This creates:

```text
bertopic_outputs/bertopic_topics_by_period.csv
bertopic_outputs/bertopic_topics_by_period_pivot_percent.csv
bertopic_outputs/bertopic_topics_by_period_stacked_percent.png
bertopic_outputs/bertopic_topic_size_table.csv
```

The main graph is:

```text
bertopic_outputs/bertopic_topics_by_period_stacked_percent.png
```

---

## 9. Recommended interpretation

Use BERTopic as exploratory support only.

Good wording:

```text
As a supplementary exploratory step, BERTopic was used to examine the broader
article-topic structure of the corpus. The resulting clusters were manually inspected
and labelled using representative documents. The topic model is treated as exploratory,
not as final historical classification.
```

For Jaleo, the BERTopic result was used to support the idea that the periodical worked as community infrastructure.

For another corpus, the interpretation may be different and must be based on the new representative documents.

---

## 10. Important warning

Do not present BERTopic as replacing the codebook-based LLM annotations.

Use this distinction:

```text
LLM/codebook annotation
→ How is flamenco legitimacy constructed?

BERTopic
→ What kinds of article topics or periodical functions structure the corpus?
```