from pathlib import Path
import pandas as pd
from collections import Counter

INPUT_CSV = Path("llm_annotation_summary.csv")

OUTPUT_CORPUS_OVERVIEW = Path("table_1_corpus_overview.csv")
OUTPUT_CODE_FREQUENCY = Path("table_2_code_frequency.csv")


def split_codes(value):
    """
    Turns a cell like 'COMM_04; HERIT_10; COMM_03'
    into a Python list:
    ['COMM_04', 'HERIT_10', 'COMM_03']
    """
    if pd.isna(value) or str(value).strip() == "":
        return []

    return [code.strip() for code in str(value).split(";") if code.strip()]


def main():
    df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")

    print(f"Loaded {len(df)} rows from {INPUT_CSV}")

    # -----------------------------
    # Table 1: Corpus overview
    # -----------------------------

    overview_rows = []

    for issue_id, group in df.groupby("issue_id"):
        number_of_articles = len(group)

        number_with_accepted_codes = (group["number_of_annotations"] > 0).sum()
        number_with_no_accepted_codes = (group["number_of_annotations"] == 0).sum()
        number_with_warnings = (group["warning_count"] > 0).sum()
        number_needing_human_review = (
            group["needs_human_review"].astype(str).str.lower() == "true"
        ).sum()

        overview_rows.append({
            "issue_id": issue_id,
            "number_of_articles": number_of_articles,
            "number_with_accepted_codes": number_with_accepted_codes,
            "number_with_no_accepted_codes": number_with_no_accepted_codes,
            "number_with_warnings": number_with_warnings,
            "number_needing_human_review": number_needing_human_review,
        })

    overview_df = pd.DataFrame(overview_rows)
    overview_df = overview_df.sort_values("issue_id")

    overview_df.to_csv(
        OUTPUT_CORPUS_OVERVIEW,
        sep=";",
        index=False,
        encoding="utf-8-sig"
    )

    print(f"Saved {OUTPUT_CORPUS_OVERVIEW}")

    # -----------------------------
    # Table 2: Code frequency
    # -----------------------------

    code_counter = Counter()
    issue_code_counter = Counter()

    for _, row in df.iterrows():
        issue_id = row["issue_id"]

        # Use unique codes per article, so if the same code appears twice
        # in one article, it only counts once for article frequency.
        codes = set(split_codes(row.get("all_accepted_codes", "")))

        for code in codes:
            code_counter[code] += 1
            issue_code_counter[(issue_id, code)] += 1

    frequency_rows = []

    for code, count in code_counter.most_common():
        row = {
            "code": code,
            "number_of_articles_where_code_appears": count,
        }

        # Add one column per issue, useful for seeing change over time.
        for issue_id in sorted(df["issue_id"].dropna().unique()):
            row[issue_id] = issue_code_counter[(issue_id, code)]

        frequency_rows.append(row)

    frequency_df = pd.DataFrame(frequency_rows)

    frequency_df.to_csv(
        OUTPUT_CODE_FREQUENCY,
        sep=";",
        index=False,
        encoding="utf-8-sig"
    )

    print(f"Saved {OUTPUT_CODE_FREQUENCY}")

    print("\nDone.")
    print("Created:")
    print(f"- {OUTPUT_CORPUS_OVERVIEW}")
    print(f"- {OUTPUT_CODE_FREQUENCY}")


if __name__ == "__main__":
    main()