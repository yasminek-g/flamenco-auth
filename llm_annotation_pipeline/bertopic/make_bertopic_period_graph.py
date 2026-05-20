import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

TOPIC_ARTICLES = Path("bertopic_outputs/bertopic_article_topics.csv")
LABELS_FILE = Path("bertopic_topic_labels.csv")
OUTPUT_DIR = Path("bertopic_outputs")

def period_from_year(year):
    year = int(year)
    if 1977 <= year <= 1980:
        return "1977–1980"
    elif 1981 <= year <= 1984:
        return "1981–1984"
    elif 1985 <= year <= 1988:
        return "1985–1988"
    elif 1989 <= year <= 1992:
        return "1989–1992"
    else:
        return "Other"

period_order = ["1977–1980", "1981–1984", "1985–1988", "1989–1992"]

topics = pd.read_csv(TOPIC_ARTICLES, sep=";", encoding="utf-8-sig")
labels = pd.read_csv(LABELS_FILE, sep=";", encoding="utf-8-sig")

topics = topics.merge(labels, on="topic", how="left")

topics["manual_label"] = topics["manual_label"].fillna("Unlabelled topic")
topics["include_in_report"] = topics["include_in_report"].fillna("no")
topics["period"] = topics["year"].apply(period_from_year)

# Keep only labelled topics we want to interpret
plot_df = topics[topics["include_in_report"].astype(str).str.lower() == "yes"].copy()

counts = (
    plot_df
    .groupby(["period", "manual_label"], as_index=False)
    .size()
    .rename(columns={"size": "count"})
)

# Important: denominator includes all modelled articles, including outliers
totals = (
    topics
    .groupby("period", as_index=False)
    .size()
    .rename(columns={"size": "total_modelled_articles"})
)

period_topics = counts.merge(totals, on="period", how="left")

period_topics["percent_of_modelled_articles"] = (
    period_topics["count"] / period_topics["total_modelled_articles"] * 100
)

period_topics["period"] = pd.Categorical(
    period_topics["period"],
    categories=period_order,
    ordered=True
)

period_topics.to_csv(
    OUTPUT_DIR / "bertopic_topics_by_period.csv",
    sep=";",
    index=False,
    encoding="utf-8-sig"
)

pivot = period_topics.pivot_table(
    index="period",
    columns="manual_label",
    values="percent_of_modelled_articles",
    fill_value=0
).reindex(period_order)

pivot.to_csv(
    OUTPUT_DIR / "bertopic_topics_by_period_pivot_percent.csv",
    sep=";",
    encoding="utf-8-sig"
)

plt.figure(figsize=(12, 6))

bottom = None
for col in pivot.columns:
    if bottom is None:
        plt.bar(pivot.index.astype(str), pivot[col], label=col)
        bottom = pivot[col].copy()
    else:
        plt.bar(pivot.index.astype(str), pivot[col], bottom=bottom, label=col)
        bottom += pivot[col]

plt.xlabel("Period")
plt.ylabel("Percent of modelled article-level units")
plt.title("Exploratory BERTopic article-topic distribution in Jaleo by period")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "bertopic_topics_by_period_stacked_percent.png", dpi=300)
plt.close()

print("Created:")
print(OUTPUT_DIR / "bertopic_topics_by_period.csv")
print(OUTPUT_DIR / "bertopic_topics_by_period_pivot_percent.csv")
print(OUTPUT_DIR / "bertopic_topics_by_period_stacked_percent.png")
print()
print(pivot.round(1).to_string())