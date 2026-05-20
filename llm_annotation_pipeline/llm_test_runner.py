from pathlib import Path
from openai import OpenAI

client = OpenAI()

PROMPT_ROOT = Path("pilot")

OUTPUT_DIR = Path("llm_annotation_outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

MODEL = "gpt-4o-mini"

BAD_SINGLE_WORD_EVIDENCE = {
    "know", "men", "man", "woman", "women", "home", "place",
    "list", "tablao", "staged", "public", "private"
}


def validate_output_text(output_text):
    import json

    warnings = []

    try:
        data = json.loads(output_text)
    except Exception as e:
        return [f"Invalid JSON: {e}"]

    present_ids = {a.get("concept_id") for a in data.get("span_annotations", [])}
    rejected_ids = {r.get("concept_id") for r in data.get("rejected_candidates", [])}
    dominant_secondary = set(data.get("article_summary", {}).get("dominant_codes", []))
    dominant_secondary |= set(data.get("article_summary", {}).get("secondary_codes", []))

    overlap = present_ids & rejected_ids
    if overlap:
        warnings.append(f"Concept appears as both present and rejected: {sorted(overlap)}")

    missing_from_annotations = dominant_secondary - present_ids
    if missing_from_annotations:
        warnings.append(f"Summary codes missing from span_annotations: {sorted(missing_from_annotations)}")

    rejected_but_in_summary = dominant_secondary & rejected_ids
    if rejected_but_in_summary:
        warnings.append(f"Rejected concept also listed in summary: {sorted(rejected_but_in_summary)}")

    for ann in data.get("span_annotations", []):
        span = str(ann.get("evidence_span", "")).strip()
        words = [w.strip(".,;:!?\"'()[]{}").lower() for w in span.split()]
        meaningful_words = [w for w in words if len(w) > 2]

        if len(meaningful_words) < 4:
            warnings.append(
                f"Weak evidence for {ann.get('concept_id')}: {span!r}"
            )

        if span.lower() in BAD_SINGLE_WORD_EVIDENCE:
            warnings.append(
                f"Forbidden single-word evidence for {ann.get('concept_id')}: {span!r}"
            )

    return warnings

def run_prompt(prompt_path):
    prompt = prompt_path.read_text(encoding="utf-8")

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a careful historical text annotator. "
                    "Return only valid JSON. "
                    "Do not use markdown. "
                    "Do not invent evidence. "
                    "Reject weak or keyword-only candidate codes."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0,
        response_format={"type": "json_object"},
    )

    return response.choices[0].message.content

def main():
    prompt_files = sorted(PROMPT_ROOT.glob("**/*.txt"))

    print(f"Running {len(prompt_files)} articles")

    for path in prompt_files:
        safe_folder = path.parent.name
        out_path = OUTPUT_DIR / f"{safe_folder}_{path.stem}.json"

        if out_path.exists():
            print(f"Skipping existing: {out_path.name}")
            continue

        print(f"→ {path.name}")

        output = run_prompt(path)

        warnings = validate_output_text(output)

        if warnings:
            print("  WARNINGS:")
            for w in warnings:
                print(f"   - {w}")

        out_path.write_text(output, encoding="utf-8")

        print(f"Saved: {out_path}")

if __name__ == "__main__":
    main()