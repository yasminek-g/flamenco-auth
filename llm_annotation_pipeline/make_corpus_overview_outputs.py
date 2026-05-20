import pandas as pd
import matplotlib.pyplot as plt
import re
from pathlib import Path

TABLE_1_FILE = Path("table_1_corpus_overview.csv")
USED_DIR = Path("Used")

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def extract_year_from_issue_id(issue_id):
    m = re.search(r"JALEO_(\d{4})", str(issue_id))
    return int(m.group(1)) if m else None

def read_csv_safely(path):
    return pd.read_csv(path, sep=";", encoding="utf-8-sig")

def find_text_column(df):
    """
    Try to detect the column containing article/OCR text.
    Adjust this list if your enriched CSVs use another column name.
    """
    possible_text_columns = [
        "article_text",
        "text",
        "full_text",
        "ocr_text",
        "content",
        "body",
        "article",
        "raw_text"
    ]

    for col in possible_text_columns:
        if col in df.columns:
            return col

    # Fallback: choose the text-like column with the longest average string length
    object_cols = df.select_dtypes(include=["object"]).columns
    if len(object_cols) == 0:
        return None

    avg_lengths = {}
    for col in object_cols:
        avg_lengths[col] = df[col].astype(str).str.len().mean()

    return max(avg_lengths, key=avg_lengths.get)

def count_words(text):
    if pd.isna(text):
        return 0
    words = re.findall(r"\b\w+\b", str(text))
    return len(words)

# ------------------------------------------------------------
# 1. Load table 1 and create corpus summary
# ------------------------------------------------------------

overview = read_csv_safely(TABLE_1_FILE)
overview["year"] = overview["issue_id"].apply(extract_year_from_issue_id)

total_issues = len(overview)
total_articles = overview["number_of_articles"].sum()
articles_with_codes = overview["number_with_accepted_codes"].sum()
articles_without_codes = overview["number_with_no_accepted_codes"].sum()
warning_articles = overview["number_with_warnings"].sum()

warning_percent = warning_articles / total_articles * 100
accepted_percent = articles_with_codes / total_articles * 100

start_year = int(overview["year"].min())
end_year = int(overview["year"].max())

summary = pd.DataFrame({
    "Corpus measure": [
        "Periodical",
        "Periodical type",
        "Time span covered",
        "Issues processed",
        "Article-level units processed",
        "Articles with at least one accepted code",
        "Articles with no accepted code",
        "Articles with warning flags",
        "Warning-flag rate"
    ],
    "Result": [
        "Jaleo",
        "Anglophone/community flamenco periodical",
        f"{start_year}–{end_year}",
        total_issues,
        total_articles,
        f"{articles_with_codes} ({accepted_percent:.1f}%)",
        articles_without_codes,
        f"{warning_articles} ({warning_percent:.1f}%)",
        f"{warning_percent:.1f}%"
    ]
})

summary.to_csv("compact_corpus_summary.csv", sep=";", index=False, encoding="utf-8-sig")

# ------------------------------------------------------------
# 2. Issues and articles per year
# ------------------------------------------------------------

yearly_overview = (
    overview
    .groupby("year", as_index=False)
    .agg(
        issues_processed=("issue_id", "count"),
        article_level_units=("number_of_articles", "sum"),
        articles_with_accepted_codes=("number_with_accepted_codes", "sum"),
        articles_with_no_accepted_code=("number_with_no_accepted_codes", "sum"),
        articles_with_warnings=("number_with_warnings", "sum")
    )
    .sort_values("year")
)

yearly_overview["articles_per_issue"] = (
    yearly_overview["article_level_units"] / yearly_overview["issues_processed"]
)

yearly_overview.to_csv("corpus_by_year.csv", sep=";", index=False, encoding="utf-8-sig")

# Chart 1: issues per year
plt.figure(figsize=(10, 5))
plt.bar(yearly_overview["year"].astype(str), yearly_overview["issues_processed"])
plt.xlabel("Year")
plt.ylabel("Number of issues processed")
plt.title("Number of Jaleo issues processed per year")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("jaleo_issues_by_year.png", dpi=300)
plt.close()

# Chart 2: article-level units per year
plt.figure(figsize=(10, 5))
plt.bar(yearly_overview["year"].astype(str), yearly_overview["article_level_units"])
plt.xlabel("Year")
plt.ylabel("Number of article-level units")
plt.title("Number of article-level units in the Jaleo corpus by year")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("jaleo_articles_by_year.png", dpi=300)
plt.close()

# Chart 3: article-level units per issue by year
plt.figure(figsize=(10, 5))
plt.bar(yearly_overview["year"].astype(str), yearly_overview["articles_per_issue"])
plt.xlabel("Year")
plt.ylabel("Average article-level units per issue")
plt.title("Average number of article-level units per Jaleo issue by year")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("jaleo_articles_per_issue_by_year.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# 3. Article length extraction from Used/*.csv
# ------------------------------------------------------------

article_rows = []

for path in USED_DIR.glob("*.csv"):
    try:
        df = read_csv_safely(path)
    except Exception as e:
        print(f"Could not read {path.name}: {e}")
        continue

    text_col = find_text_column(df)

    if text_col is None:
        print(f"No text column found in {path.name}")
        continue

    if "issue_id" not in df.columns:
        print(f"No issue_id column found in {path.name}")
        continue

    for _, row in df.iterrows():
        issue_id = row.get("issue_id", "")
        article_id = row.get("article_id", "")
        title = row.get("title", "")
        text = row.get(text_col, "")

        article_rows.append({
            "source_file": path.name,
            "issue_id": issue_id,
            "year": extract_year_from_issue_id(issue_id),
            "article_id": article_id,
            "title": title,
            "text_column_used": text_col,
            "character_count": len(str(text)) if not pd.isna(text) else 0,
            "word_count": count_words(text)
        })

article_lengths = pd.DataFrame(article_rows)
article_lengths.to_csv("article_lengths.csv", sep=";", index=False, encoding="utf-8-sig")

# Yearly length summary
length_by_year = (
    article_lengths
    .groupby("year", as_index=False)
    .agg(
        number_of_articles=("word_count", "count"),
        mean_word_count=("word_count", "mean"),
        median_word_count=("word_count", "median"),
        min_word_count=("word_count", "min"),
        max_word_count=("word_count", "max")
    )
    .sort_values("year")
)

length_by_year.to_csv("article_length_by_year.csv", sep=";", index=False, encoding="utf-8-sig")

# Chart 4: article length distribution
plt.figure(figsize=(10, 5))
plt.hist(article_lengths["word_count"], bins=50)
plt.xlabel("Word count")
plt.ylabel("Number of article-level units")
plt.title("Distribution of article lengths in the Jaleo corpus")
plt.tight_layout()
plt.savefig("jaleo_article_length_distribution.png", dpi=300)
plt.close()

# Chart 5: median article length by year
plt.figure(figsize=(10, 5))
plt.bar(length_by_year["year"].astype(str), length_by_year["median_word_count"])
plt.xlabel("Year")
plt.ylabel("Median word count")
plt.title("Median article length in the Jaleo corpus by year")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("jaleo_median_article_length_by_year.png", dpi=300)
plt.close()

print("Created:")
print("- compact_corpus_summary.csv")
print("- corpus_by_year.csv")
print("- article_lengths.csv")
print("- article_length_by_year.csv")
print("- jaleo_issues_by_year.png")
print("- jaleo_articles_by_year.png")
print("- jaleo_articles_per_issue_by_year.png")
print("- jaleo_article_length_distribution.png")
print("- jaleo_median_article_length_by_year.png")
print()
print(summary.to_string(index=False))
print()
print("Yearly corpus overview:")
print(yearly_overview.to_string(index=False))

# Categorize article lengths
def length_category(word_count):
    if word_count <= 150:
        return "Very short (0–150)"
    elif word_count <= 500:
        return "Short (151–500)"
    elif word_count <= 1000:
        return "Medium (501–1000)"
    elif word_count <= 2000:
        return "Long (1001–2000)"
    else:
        return "Very long (2000+)"

article_lengths["length_category"] = article_lengths["word_count"].apply(length_category)

category_order = [
    "Very short (0–150)",
    "Short (151–500)",
    "Medium (501–1000)",
    "Long (1001–2000)",
    "Very long (2000+)"
]

length_categories = (
    article_lengths["length_category"]
    .value_counts()
    .reindex(category_order)
    .reset_index()
)

length_categories.columns = ["length_category", "number_of_article_units"]
length_categories.to_csv("article_length_categories.csv", sep=";", index=False, encoding="utf-8-sig")

plt.figure(figsize=(9, 5))
plt.bar(length_categories["length_category"], length_categories["number_of_article_units"])
plt.xlabel("Article-length category")
plt.ylabel("Number of article-level units")
plt.title("Article-length categories in the Jaleo corpus")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig("jaleo_article_length_categories.png", dpi=300)
plt.close()

# Chart: mean and median article length by year
plt.figure(figsize=(10, 5))

plt.plot(
    length_by_year["year"].astype(str),
    length_by_year["median_word_count"],
    marker="o",
    label="Median word count"
)

plt.plot(
    length_by_year["year"].astype(str),
    length_by_year["mean_word_count"],
    marker="o",
    label="Mean word count"
)

plt.xlabel("Year")
plt.ylabel("Word count")
plt.title("Mean and median article length in the Jaleo corpus by year")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig("jaleo_article_length_over_time.png", dpi=300)
plt.close()