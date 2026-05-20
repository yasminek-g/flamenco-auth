import json
import pandas as pd
from pathlib import Path
import re

CODEBOOK_FILE = Path("flamenco_codebook_v10.json")
TOP_CODES_FILE = Path("top_codes_overall.csv")

top_codes = pd.read_csv(TOP_CODES_FILE, sep=";", encoding="utf-8-sig")
top10 = top_codes.head(10).copy()

with open(CODEBOOK_FILE, "r", encoding="utf-8") as f:
    codebook = json.load(f)

code_lookup = {}

CODE_PATTERN = re.compile(r"^[A-Z]+_\d+$")

def stringify_value(value):
    if value is None:
        return ""
    if isinstance(value, list):
        return "; ".join(str(v) for v in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)

def collect_codes(obj, parent_key=None):
    """
    Flexible recursive codebook search.

    Handles both:
    1. {"concept_id": "COMM_03", "label": "..."}
    2. {"COMM_03": {"label": "...", "definition": "..."}}
    3. nested family structures.
    """
    if isinstance(obj, dict):
        # Case 1: the code is stored inside the object
        possible_id = (
            obj.get("concept_id")
            or obj.get("code")
            or obj.get("id")
            or obj.get("code_id")
            or obj.get("name_id")
        )

        if isinstance(possible_id, str) and CODE_PATTERN.match(possible_id):
            code_lookup[possible_id] = obj

        # Case 2: the dictionary key itself is the code
        for key, value in obj.items():
            if isinstance(key, str) and CODE_PATTERN.match(key):
                if isinstance(value, dict):
                    entry = value.copy()
                    entry["concept_id"] = key
                    code_lookup[key] = entry
                else:
                    code_lookup[key] = {
                        "concept_id": key,
                        "value": value
                    }

            collect_codes(value, key)

    elif isinstance(obj, list):
        for item in obj:
            collect_codes(item, parent_key)

collect_codes(codebook)

def get_first_existing(entry, possible_fields):
    for field in possible_fields:
        if field in entry and stringify_value(entry[field]).strip():
            return stringify_value(entry[field])
    return ""

rows = []

for _, row in top10.iterrows():
    code = row["code"]
    entry = code_lookup.get(code, {})

    label = get_first_existing(entry, [
        "label_en",
        "label_es",
        "short_label",
        "label",
        "name",
        "title",
        "concept_label",
        "code_label"
    ])

    definition = get_first_existing(entry, [
        "definition",
        "operational_definition",
        "description",
        "meaning",
        "criteria",
        "coding_rule",
        "rule",
        "explanation"
    ])

    # Optional: include examples/triggers if definitions are missing
    triggers = get_first_existing(entry, [
        "triggers",
        "trigger_terms",
        "keywords",
        "examples",
        "positive_examples"
    ])

    family = code.split("_")[0]

    rows.append({
        "Code": code,
        "Family": family,
        "Short label": label,
        "Meaning / operational definition": definition,
        "Trigger terms / examples": triggers,
        "Number of article-level units": row["number_of_articles_where_code_appears"]
    })

explanation = pd.DataFrame(rows)

explanation.to_csv(
    "top10_code_explanation_table.csv",
    sep=";",
    index=False,
    encoding="utf-8-sig"
)

print("Created: top10_code_explanation_table.csv")
print()
print(explanation.to_string(index=False))

print()
print("Codes found in codebook:")
for code in top10["code"]:
    print(code, "FOUND" if code in code_lookup else "NOT FOUND")