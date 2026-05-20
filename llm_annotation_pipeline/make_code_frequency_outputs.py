import pandas as pd
import matplotlib.pyplot as plt
import re
from pathlib import Path

TABLE_1_FILE = Path("table_1_corpus_overview.csv")
TABLE_2_FILE = Path("table_2_code_frequency.csv")

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def extract_year_from_issue_id(issue_id):
    m = re.search(r"JALEO_(\d{4})", str(issue_id))
    return int(m.group(1)) if m else None

def code_family(code):
    return str(code).split("_")[0]

# ------------------------------------------------------------
# Load files
# ------------------------------------------------------------

overview = pd.read_csv(TABLE_1_FILE, sep=";", encoding="utf-8-sig")
codes = pd.read_csv(TABLE_2_FILE, sep=";", encoding="utf-8-sig")

overview["year"] = overview["issue_id"].apply(extract_year_from_issue_id)

# Total article-level units per year, used for normalization
articles_by_year = (
    overview
    .groupby("year", as_index=False)["number_of_articles"]
    .sum()
    .rename(columns={"number_of_articles": "article_level_units"})
)

# ------------------------------------------------------------
# 1. Top codes overall
# ------------------------------------------------------------

top_codes = (
    codes[["code", "number_of_articles_where_code_appears"]]
    .sort_values("number_of_articles_where_code_appears", ascending=False)
)

top_codes["family"] = top_codes["code"].apply(code_family)
top_codes.to_csv("top_codes_overall.csv", sep=";", index=False, encoding="utf-8-sig")

top10 = top_codes.head(10)

plt.figure(figsize=(10, 5))
plt.bar(top10["code"], top10["number_of_articles_where_code_appears"])
plt.xlabel("Code")
plt.ylabel("Number of article-level units")
plt.title("Top 10 accepted codes in the Jaleo corpus")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("jaleo_top10_codes.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# 2. Code families overall
# ------------------------------------------------------------

family_frequency = (
    top_codes
    .groupby("family", as_index=False)["number_of_articles_where_code_appears"]
    .sum()
    .sort_values("number_of_articles_where_code_appears", ascending=False)
)

family_frequency.to_csv("code_family_frequency.csv", sep=";", index=False, encoding="utf-8-sig")

plt.figure(figsize=(8, 5))
plt.bar(family_frequency["family"], family_frequency["number_of_articles_where_code_appears"])
plt.xlabel("Code family")
plt.ylabel("Accepted code appearances in article-level units")
plt.title("Accepted code appearances by code family")
plt.tight_layout()
plt.savefig("jaleo_code_families.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# 3. Code frequencies over time, normalized by articles per year
# ------------------------------------------------------------

# Issue columns are all columns except these metadata columns
metadata_cols = {"code", "number_of_articles_where_code_appears"}
issue_cols = [col for col in codes.columns if col not in metadata_cols]

long_codes = codes.melt(
    id_vars=["code", "number_of_articles_where_code_appears"],
    value_vars=issue_cols,
    var_name="issue_id",
    value_name="code_count"
)

long_codes["year"] = long_codes["issue_id"].apply(extract_year_from_issue_id)
long_codes["family"] = long_codes["code"].apply(code_family)

code_by_year = (
    long_codes
    .groupby(["year", "code", "family"], as_index=False)["code_count"]
    .sum()
    .merge(articles_by_year, on="year", how="left")
)

code_by_year["code_rate_per_100_articles"] = (
    code_by_year["code_count"] / code_by_year["article_level_units"] * 100
)

code_by_year.to_csv("code_frequency_by_year_normalized.csv", sep=";", index=False, encoding="utf-8-sig")

# Plot top 5 codes over time using normalized rates
top5_codes = top_codes.head(5)["code"].tolist()
top5_over_time = code_by_year[code_by_year["code"].isin(top5_codes)]

plt.figure(figsize=(10, 5))

for code in top5_codes:
    subset = top5_over_time[top5_over_time["code"] == code].sort_values("year")
    plt.plot(
        subset["year"].astype(str),
        subset["code_rate_per_100_articles"],
        marker="o",
        label=code
    )

plt.xlabel("Year")
plt.ylabel("Code appearances per 100 article-level units")
plt.title("Top accepted codes over time, normalized by corpus size")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig("jaleo_top5_codes_over_time_normalized.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# 4. Code family trends over time
# ------------------------------------------------------------

family_by_year = (
    code_by_year
    .groupby(["year", "family"], as_index=False)
    .agg(
        family_code_count=("code_count", "sum"),
        article_level_units=("article_level_units", "first")
    )
)

family_by_year["family_rate_per_100_articles"] = (
    family_by_year["family_code_count"] / family_by_year["article_level_units"] * 100
)

family_by_year.to_csv("code_family_by_year_normalized.csv", sep=";", index=False, encoding="utf-8-sig")

top_families = family_frequency.head(5)["family"].tolist()
family_plot = family_by_year[family_by_year["family"].isin(top_families)]

plt.figure(figsize=(10, 5))

for family in top_families:
    subset = family_plot[family_plot["family"] == family].sort_values("year")
    plt.plot(
        subset["year"].astype(str),
        subset["family_rate_per_100_articles"],
        marker="o",
        label=family
    )

plt.xlabel("Year")
plt.ylabel("Family code appearances per 100 article-level units")
plt.title("Code-family trends over time, normalized by corpus size")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig("jaleo_code_families_over_time_normalized.png", dpi=300)
plt.close()

print("Created:")
print("- top_codes_overall.csv")
print("- code_family_frequency.csv")
print("- code_frequency_by_year_normalized.csv")
print("- code_family_by_year_normalized.csv")
print("- jaleo_top10_codes.png")
print("- jaleo_code_families.png")
print("- jaleo_top5_codes_over_time_normalized.png")
print("- jaleo_code_families_over_time_normalized.png")
print()
print("Top 10 codes:")
print(top10.to_string(index=False))
print()
print("Code families:")
print(family_frequency.to_string(index=False))