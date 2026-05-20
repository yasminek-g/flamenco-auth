import pandas as pd
import re
from pathlib import Path

USED_DIR = Path("Used")
OUTPUT_FILE = Path("bertopic_articles.csv")

def extract_year(issue_id):
    m = re.search(r"JALEO_(\d{4})", str(issue_id))
    return int(m.group(1)) if m else None

def count_words(text):
    if pd.isna(text):
        return 0
    return len(re.findall(r"\b\w+\b", str(text)))

def find_text_column(df):
    possible = [
        "article_text",
        "text",
        "full_text",
        "ocr_text",
        "content",
        "body",
        "article",
        "raw_text"
    ]

    for col in possible:
        if col in df.columns:
            return col

    # Fallback: choose object column with longest average string length
    object_cols = df.select_dtypes(include=["object"]).columns
    if len(object_cols) == 0:
        return None

    lengths = {
        col: df[col].astype(str).str.len().mean()
        for col in object_cols
    }
    return max(lengths, key=lengths.get)

rows = []

for path in sorted(USED_DIR.glob("*.csv")):
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    except Exception as e:
        print(f"Could not read {path.name}: {e}")
        continue

    text_col = find_text_column(df)

    if text_col is None:
        print(f"No text column found in {path.name}")
        continue

    for _, row in df.iterrows():
        issue_id = row.get("issue_id", "")
        article_id = row.get("article_id", "")
        title = row.get("title", "")
        text = row.get(text_col, "")

        rows.append({
            "source_file": path.name,
            "issue_id": issue_id,
            "article_id": article_id,
            "title": title,
            "year": extract_year(issue_id),
            "text_column_used": text_col,
            "text": "" if pd.isna(text) else str(text),
            "word_count": count_words(text)
        })

articles = pd.DataFrame(rows)

articles.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8-sig")

print(f"Created {OUTPUT_FILE}")
print(f"Rows: {len(articles)}")
print("Text columns used:")
print(articles["text_column_used"].value_counts().to_string())
print()
print("Word count summary:")
print(articles["word_count"].describe().to_string())