from pathlib import Path
import pandas as pd
import re

USED_DIR = Path("Used")

season_map = {
    "spring": "SPRING",
    "summer": "SUMMER",
    "fall": "FALL",
    "winter": "WINTER",
}

def issue_id_from_filename(filename):
    name = filename.replace("_toc_enriched", "").replace("_enriched", "").replace(".csv", "")

    # Normal monthly files, e.g. 1978-04_enriched.csv
    m = re.match(r"^(\d{4})-(\d{2})", name)
    if m:
        year, month = m.groups()
        return f"JALEO_{year}_{month}"

    # Seasonal files, e.g. spring-1986_toc_enriched.csv
    m = re.match(r"^(spring|summer|fall|winter)-(\d{4})", name, re.I)
    if m:
        season, year = m.groups()
        return f"JALEO_{year}_{season_map[season.lower()]}"

    return None

for path in USED_DIR.glob("*.csv"):
    expected_issue_id = issue_id_from_filename(path.name)

    if expected_issue_id is None:
        print(f"SKIP: Could not infer issue_id for {path.name}")
        continue

    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    except Exception as e:
        print(f"\nERROR while reading: {path.name}")
        print(e)
        raise 

    if "issue_id" not in df.columns:
        df["issue_id"] = ""

    before_missing = df["issue_id"].isna().sum() + (df["issue_id"].astype(str).str.strip() == "").sum()

    # Fill only missing/blank issue_id values
    mask = df["issue_id"].isna() | (df["issue_id"].astype(str).str.strip() == "")
    df.loc[mask, "issue_id"] = expected_issue_id

    after_missing = df["issue_id"].isna().sum() + (df["issue_id"].astype(str).str.strip() == "").sum()

    df.to_csv(path, index=False, sep=";", encoding="utf-8-sig")

    if before_missing > 0:
        print(f"Fixed {path.name}: filled {before_missing} missing issue_id values with {expected_issue_id}")

print("Done.")