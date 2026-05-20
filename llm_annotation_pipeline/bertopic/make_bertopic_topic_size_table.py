import pandas as pd
from pathlib import Path

TOPIC_INFO = Path("bertopic_outputs/bertopic_topic_info.csv")
LABELS_FILE = Path("bertopic_topic_labels.csv")
OUTPUT_FILE = Path("bertopic_outputs/bertopic_topic_size_table.csv")

info = pd.read_csv(TOPIC_INFO, sep=";", encoding="utf-8-sig")
labels = pd.read_csv(LABELS_FILE, sep=";", encoding="utf-8-sig")

merged = info.merge(labels, left_on="Topic", right_on="topic", how="left")

merged["manual_label"] = merged["manual_label"].fillna("Unlabelled topic")
merged["include_in_report"] = merged["include_in_report"].fillna("no")

result = merged[[
    "Topic",
    "manual_label",
    "Count",
    "include_in_report",
    "notes"
]].copy()

result = result.sort_values("Topic")

result.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8-sig")

print("Created:", OUTPUT_FILE)
print()
print(result.to_string(index=False))