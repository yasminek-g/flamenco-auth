from pathlib import Path
import json
import csv

INPUT_DIR = Path("llm_annotation_outputs")
OUTPUT_CSV = Path("llm_annotation_summary.csv")


def as_text_list(value):
    if not value:
        return ""
    if isinstance(value, list):
        return "; ".join(str(v) for v in value)
    return str(value)


def main():
    rows = []

    json_files = sorted(INPUT_DIR.glob("*.json"))

    print(f"Found {len(json_files)} JSON files")

    for path in json_files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            rows.append({
                "file": path.name,
                "issue_id": "",
                "article_id": "",
                "title": "",
                "dominant_codes": "",
                "secondary_codes": "",
                "all_accepted_codes": "",
                "number_of_annotations": 0,
                "number_of_rejected_candidates": 0,
                "needs_human_review": True,
                "human_review_reason": f"Could not read JSON: {e}",
                "warning_count": 1,
                "warnings": f"Invalid JSON: {e}",
            })
            continue

        summary = data.get("article_summary", {})
        annotations = data.get("span_annotations", [])
        rejected = data.get("rejected_candidates", [])

        dominant_codes = summary.get("dominant_codes", [])
        secondary_codes = summary.get("secondary_codes", [])

        accepted_codes = []
        for ann in annotations:
            concept_id = ann.get("concept_id")
            if concept_id:
                accepted_codes.append(concept_id)

        warnings = []

        present_ids = {ann.get("concept_id") for ann in annotations if ann.get("concept_id")}
        rejected_ids = {r.get("concept_id") for r in rejected if r.get("concept_id")}
        summary_ids = set(dominant_codes) | set(secondary_codes)

        overlap = present_ids & rejected_ids
        if overlap:
            warnings.append(f"Code both accepted and rejected: {sorted(overlap)}")

        missing_from_annotations = summary_ids - present_ids
        if missing_from_annotations:
            warnings.append(f"Summary code missing from annotations: {sorted(missing_from_annotations)}")

        rejected_in_summary = summary_ids & rejected_ids
        if rejected_in_summary:
            warnings.append(f"Rejected code appears in summary: {sorted(rejected_in_summary)}")

        for ann in annotations:
            span = str(ann.get("evidence_span", "")).strip()
            words = [w.strip(".,;:!?\"'()[]{}").lower() for w in span.split()]
            meaningful_words = [w for w in words if len(w) > 2]

            if len(meaningful_words) < 4:
                warnings.append(
                    f"Weak evidence for {ann.get('concept_id')}: {span!r}"
                )

            if span.lower() in {
                "know", "men", "man", "woman", "women", "home", "place",
                "list", "tablao", "staged", "public", "private", "origin",
                "inside", "jaleo"
            }:
                warnings.append(
                    f"Forbidden or weak single-word evidence for {ann.get('concept_id')}: {span!r}"
                )

        rows.append({
            "file": path.name,
            "issue_id": data.get("issue_id", ""),
            "article_id": data.get("article_id", ""),
            "title": data.get("title", ""),
            "dominant_codes": as_text_list(dominant_codes),
            "secondary_codes": as_text_list(secondary_codes),
            "all_accepted_codes": as_text_list(sorted(set(accepted_codes))),
            "number_of_annotations": len(annotations),
            "number_of_rejected_candidates": len(rejected),
            "needs_human_review": data.get("needs_human_review", ""),
            "human_review_reason": data.get("human_review_reason", ""),
            "warning_count": len(warnings),
            "warnings": " | ".join(warnings),
        })

    fieldnames = [
        "file",
        "issue_id",
        "article_id",
        "title",
        "dominant_codes",
        "secondary_codes",
        "all_accepted_codes",
        "number_of_annotations",
        "number_of_rejected_candidates",
        "needs_human_review",
        "human_review_reason",
        "warning_count",
        "warnings",
    ]

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved summary table: {OUTPUT_CSV}")
    print(f"Rows written: {len(rows)}")

    warning_rows = [r for r in rows if r["warning_count"] > 0]
    review_rows = [r for r in rows if str(r["needs_human_review"]).lower() == "true"]

    print(f"Files with warnings: {len(warning_rows)}")
    print(f"Files needing human review: {len(review_rows)}")


if __name__ == "__main__":
    main()