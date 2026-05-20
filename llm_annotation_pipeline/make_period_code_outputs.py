import pandas as pd
import matplotlib.pyplot as plt
import re
from pathlib import Path

TABLE_1_FILE = Path("table_1_corpus_overview.csv")
TABLE_2_FILE = Path("table_2_code_frequency.csv")

def extract_year(issue_id):
    m = re.search(r"JALEO_(\d{4})", str(issue_id))
    return int(m.group(1)) if m else None

def period_from_year(year):
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

def family_from_code(code):
    return str(code).split("_")[0]

# Load data
overview = pd.read_csv(TABLE_1_FILE, sep=";", encoding="utf-8-sig")
codes = pd.read_csv(TABLE_2_FILE, sep=";", encoding="utf-8-sig")

overview["year"] = overview["issue_id"].apply(extract_year)
overview["period"] = overview["year"].apply(period_from_year)

# Articles per period
articles_by_period = (
    overview
    .groupby("period", as_index=False)["number_of_articles"]
    .sum()
    .rename(columns={"number_of_articles": "article_level_units"})
)

period_order = ["1977–1980", "1981–1984", "1985–1988", "1989–1992"]
articles_by_period["period"] = pd.Categorical(
    articles_by_period["period"],
    categories=period_order,
    ordered=True
)
articles_by_period = articles_by_period.sort_values("period")

# Convert issue columns to long format
metadata_cols = {"code", "number_of_articles_where_code_appears"}
issue_cols = [col for col in codes.columns if col not in metadata_cols]

long_codes = codes.melt(
    id_vars=["code", "number_of_articles_where_code_appears"],
    value_vars=issue_cols,
    var_name="issue_id",
    value_name="code_count"
)

long_codes["year"] = long_codes["issue_id"].apply(extract_year)
long_codes["period"] = long_codes["year"].apply(period_from_year)
long_codes["family"] = long_codes["code"].apply(family_from_code)

# Code by period
code_by_period = (
    long_codes
    .groupby(["period", "code", "family"], as_index=False)["code_count"]
    .sum()
    .merge(articles_by_period, on="period", how="left")
)

code_by_period["rate_per_100_articles"] = (
    code_by_period["code_count"] / code_by_period["article_level_units"] * 100
)

code_by_period["period"] = pd.Categorical(
    code_by_period["period"],
    categories=period_order,
    ordered=True
)

code_by_period.to_csv(
    "code_frequency_by_period_normalized.csv",
    sep=";",
    index=False,
    encoding="utf-8-sig"
)

# Family by period
family_by_period = (
    code_by_period
    .groupby(["period", "family"], as_index=False)
    .agg(
        family_code_count=("code_count", "sum"),
        article_level_units=("article_level_units", "first")
    )
)

family_by_period["rate_per_100_articles"] = (
    family_by_period["family_code_count"] / family_by_period["article_level_units"] * 100
)

family_by_period["period"] = pd.Categorical(
    family_by_period["period"],
    categories=period_order,
    ordered=True
)

family_by_period.to_csv(
    "code_family_by_period_normalized.csv",
    sep=";",
    index=False,
    encoding="utf-8-sig"
)

# Top 5 codes by overall frequency
top_codes = (
    codes[["code", "number_of_articles_where_code_appears"]]
    .sort_values("number_of_articles_where_code_appears", ascending=False)
)
top5 = top_codes.head(5)["code"].tolist()

# Plot top 5 codes by period
plt.figure(figsize=(10, 5))

for code in top5:
    subset = code_by_period[code_by_period["code"] == code].sort_values("period")
    plt.plot(
        subset["period"].astype(str),
        subset["rate_per_100_articles"],
        marker="o",
        label=code
    )

plt.xlabel("Period")
plt.ylabel("Code appearances per 100 article-level units")
plt.title("Top accepted codes by period, normalized by corpus size")
plt.legend()
plt.tight_layout()
plt.savefig("jaleo_top5_codes_by_period_normalized.png", dpi=300)
plt.close()

# Plot code families by period
top_families = ["COMM", "HERIT", "TRAD", "AUTH", "LEGIT"]

plt.figure(figsize=(10, 5))

for family in top_families:
    subset = family_by_period[family_by_period["family"] == family].sort_values("period")
    plt.plot(
        subset["period"].astype(str),
        subset["rate_per_100_articles"],
        marker="o",
        label=family
    )

plt.xlabel("Period")
plt.ylabel("Family code appearances per 100 article-level units")
plt.title("Code-family patterns by period, normalized by corpus size")
plt.legend()
plt.tight_layout()
plt.savefig("jaleo_code_families_by_period_normalized.png", dpi=300)
plt.close()

print("Created:")
print("- code_frequency_by_period_normalized.csv")
print("- code_family_by_period_normalized.csv")
print("- jaleo_top5_codes_by_period_normalized.png")
print("- jaleo_code_families_by_period_normalized.png")
print()
print("Articles by period:")
print(articles_by_period.to_string(index=False))