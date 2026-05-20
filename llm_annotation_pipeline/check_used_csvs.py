from pathlib import Path
import pandas as pd

for path in sorted(Path("Used").glob("*.csv")):
    print("\n" + "=" * 60)
    print(path.name)

    df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    print(f"Rows: {len(df)}")

    if "issue_id" not in df.columns:
        print("NO issue_id COLUMN")
        continue

    print(df["issue_id"].fillna("<MISSING>").value_counts().to_string())