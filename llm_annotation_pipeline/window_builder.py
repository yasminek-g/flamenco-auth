#%%
import json
import re
import pandas as pd
from pathlib import Path
from collections import defaultdict

# ---- paths ----
CODEBOOK_PATH = "flamenco_codebook_v10.json"
INPUT_DIR = Path("Used")

OUTPUT_ROOT = Path("pilot")
OUTPUT_ROOT.mkdir(exist_ok=True)

TEXT_COLUMN = "text"
WINDOW_SIZE = 350
MAX_ARTICLES = 999
MAX_WINDOWS_PER_ARTICLE = 20

FAMILIES_TO_USE = {"LEGIT", "AUTH", "HERIT", "COMM", "PED", "CRIT"}


def load_codebook_triggers(codebook_path):
    with open(codebook_path, "r", encoding="utf-8") as f:
        codebook = json.load(f)

    triggers = []

    for family_id, family in codebook["families"].items():
        if family_id not in FAMILIES_TO_USE:
            continue

        for concept_id, concept in family.get("concepts", {}).items():
            for trig in concept.get("trigger_variants", []):
                pattern = trig.get("regex_or_seed")
                if not pattern:
                    continue

                triggers.append({
                    "family_id": family_id,
                    "concept_id": concept_id,
                    "label_en": concept.get("label_en", ""),
                    "language": trig.get("language", ""),
                    "pattern": pattern,
                })

    return triggers, codebook


def extract_windows_for_article(row, triggers):
    text = str(row.get(TEXT_COLUMN, ""))
    if not text or text == "nan":
        return []

    windows = []

    for trig in triggers:
        pattern = trig["pattern"]

        try:
            matches = re.finditer(pattern, text, flags=re.IGNORECASE)
        except re.error:
            continue

        for match in matches:
            start = max(match.start() - WINDOW_SIZE, 0)
            end = min(match.end() + WINDOW_SIZE, len(text))

            windows.append({
                "window_id": f'{row.get("article_id", "")}__{len(windows):04d}',
                "issue_id": row.get("issue_id", ""),
                "article_id": row.get("article_id", ""),
                "title": row.get("title", ""),
                "pages": row.get("pages", ""),
                "family_id": trig["family_id"],
                "concept_id": trig["concept_id"],
                "label_en": trig["label_en"],
                "matched_text": match.group(),
                "trigger_pattern": pattern,
                "start_char": match.start(),
                "end_char": match.end(),
                "window_text": text[start:end].replace("\n", " "),
            })

    return windows


def rank_candidate_codes(windows, max_candidates=12):
    grouped = defaultdict(list)

    for w in windows:
        key = (w["family_id"], w["concept_id"], w["label_en"])
        grouped[key].append(w)

    ranked = []

    for (family_id, concept_id, label_en), supporting_windows in grouped.items():
        ranked.append({
            "family_id": family_id,
            "concept_id": concept_id,
            "label_en": label_en,
            "n_supporting_windows": len(supporting_windows),
            "supporting_window_ids": [w["window_id"] for w in supporting_windows[:3]],
        })

    # Sort by number of windows, but keep all candidate types before cutting
    ranked = sorted(
        ranked,
        key=lambda x: x["n_supporting_windows"],
        reverse=True
    )

    # Add rank numbers after sorting
    for i, item in enumerate(ranked, start=1):
        item["rank"] = i

    return ranked[:max_candidates]

def select_windows_for_prompt(windows, candidate_codes, max_windows=20):
    selected_ids = []

    # First include supporting windows for every candidate code
    for code in candidate_codes:
        for window_id in code.get("supporting_window_ids", []):
            if window_id not in selected_ids:
                selected_ids.append(window_id)

    # Then add more windows in original order if there is still room
    for w in windows:
        if w["window_id"] not in selected_ids:
            selected_ids.append(w["window_id"])

    selected_ids = selected_ids[:max_windows]

    return [w for w in windows if w["window_id"] in selected_ids]

def make_article_prompt(article_row, windows, candidate_codes):
    
    selected_windows = select_windows_for_prompt(windows, candidate_codes)
    selected_candidate_codes = candidate_codes

    return f"""
If you output anything other than valid JSON, the answer is invalid.

You are annotating one historical flamenco periodical article.

The article is the unit of analysis.
Evidence must come from the supplied evidence windows.

Your task:
1. Decide which candidate codes are analytically present in the article.
2. The article is the unit of analysis. Do not code a frame merely because a keyword appears in one window.
3. Attach each present or uncertain code to one or more evidence windows.
4. Reject candidates that are only keyword matches, generic description, biography, geography, event listing, names of people, or administrative language.
5. Evaluate every candidate code listed. Each candidate must appear either in span_annotations or rejected_candidates.
6. Do not mark all candidate codes as present. You are expected to reject weak candidates.
7. Do not return only one code if another candidate is clearly present as a secondary frame.
8. Return valid JSON only.

Do not invent evidence outside the supplied windows.
Evidence spans must be exact quotes from the supplied evidence windows.

Every candidate code must be accounted for:
- If present, include it in span_annotations.
- If uncertain, include it in span_annotations with decision "uncertain".
- If weak, incidental, keyword-only, or unsupported at article level, include it in rejected_candidates.
The rejected_candidates list should not be empty unless every candidate is genuinely supported.

The candidate ranking is retrieval-based, not ground truth.
A high-ranked candidate can still be absent.
A lower-ranked candidate can still be important.

IMPORTANT NEGATIVE CODING RULES:

- Do not code COMM_06 merely because the article mentions "man", "men", "woman", "women", a male artist, a female artist, or a gendered noun.
- Do not code COMM_06 from publicness, audience, seminars, workshops, juergas, teaching settings, performance settings, or community gatherings unless the article explicitly frames them through gender.
- Public/private setting is not the same as gendered tradition.
- A person's gender alone is not evidence for COMM_06.
- A public performance, juerga, seminar, workshop, or audience is not evidence for COMM_06 unless the article explicitly discusses gender roles, gendered authority, gendered transmission, family/gender expectations, or gendered participation.
- If the evidence span cannot directly explain the exact concept_id, reject the candidate.
- Evidence must support the specific code, not only the general family.

BIOGRAPHY AND DIRECTORY RULES:

- Do not code a concept from simple biography alone, such as birth, death, age, career dates, names of relatives, or "started dancing at age X".
- Do not code COMM_03 from a person's long career, early training, fame, or biography unless the article explicitly frames them as an insider, authority, knower, legitimate bearer of tradition, or recognized member of a flamenco community.
- Do not code concepts from directory listings, contact information, phone numbers, addresses, cast lists, or event listings unless the article uses them analytically.
- If the article is mainly a notice, obituary, photo caption, directory, or listing, use few or no codes and set needs_human_review to true.

EVIDENCE QUALITY RULES:

- Do not use single words or very short phrases as evidence spans.
- An evidence_span must normally contain at least 4 meaningful words.
- Evidence such as "know", "men", "man", "Home", "Place", "list", "tablao", or "staged" is never sufficient by itself.
- If the only available evidence for a candidate is a single word, reject the candidate.
- A trigger word is not evidence. The evidence_span must show how the article uses that idea.
- Do not assign present or uncertain codes from isolated OCR fragments.

CONFIDENCE CALIBRATION:

- Do not use confidence 1.0 unless the evidence explicitly and unmistakably names or argues the frame.
- Most valid annotations should fall between 0.7 and 0.9.
- Use 0.9 for strong evidence.
- Use 0.8 for good but interpretive evidence.
- Use 0.6-0.7 for plausible but indirect evidence.
- Use decision "uncertain" for borderline cases.
- Do not mark a code as present if confidence would be below 0.6.
- If an annotation depends on interpretation rather than explicit wording, confidence must be below 1.0.

FINAL VALIDATION BEFORE OUTPUT:

Before returning JSON, check these rules strictly:

1. A concept_id must not appear in both span_annotations and rejected_candidates.
2. A concept_id listed in dominant_codes or secondary_codes must also appear in span_annotations.
3. A concept_id listed in rejected_candidates must not appear in dominant_codes or secondary_codes.
4. If an evidence_span has fewer than 4 meaningful words, reject that annotation.
5. Never use isolated words such as "know", "men", "man", "woman", "Home", "Place", "list", "tablao", or "staged" as evidence.
6. COMM_06 must be rejected unless the evidence explicitly discusses gender roles, gendered authority, gendered transmission, gendered expectations, or gendered participation.
7. A famous male or female performer is not enough for COMM_06.
8. A reference to Carmen Amaya, Pastora Imperio, Pilar López, Mario Maya, or any other gendered performer is not enough for COMM_06 unless gender itself is being discussed.
9. If these rules conflict with an earlier instruction, these final validation rules override the earlier instruction.

ARTICLE:
{{
  "issue_id": "{article_row.get("issue_id", "")}",
  "article_id": "{article_row.get("article_id", "")}",
  "title": "{article_row.get("title", "")}",
  "pages": "{article_row.get("pages", "")}",
  "item_type": "{article_row.get("item_type", "")}",
  "article_opening": {json.dumps(str(article_row.get(TEXT_COLUMN, ""))[:900], ensure_ascii=False)}
}}

CANDIDATE CODES:
{json.dumps(selected_candidate_codes, ensure_ascii=False, indent=2)}

EVIDENCE WINDOWS:
{json.dumps(selected_windows, ensure_ascii=False, indent=2)}

In the JSON output, copy issue_id, article_id, and title exactly from the ARTICLE metadata.

REQUIRED JSON OUTPUT SHAPE:
{{
  "issue_id": "string",
  "article_id": "string",
  "title": "string",
  "language": "en | es | unknown",
  "article_summary": {{
    "dominant_codes": ["concept_id"],
    "secondary_codes": ["concept_id"],
    "summary": "brief article-level synthesis of the frame work"
  }},
  "span_annotations": [
    {{
      "concept_id": "string",
      "family_id": "string",
      "decision": "present | uncertain",
      "confidence": 0.0,
      "window_id": "string",
      "evidence_span": "exact quote from a supplied evidence window",
      "local_function": "what this frame is doing locally in the article"
    }}
  ],
  "rejected_candidates": [
    {{
      "concept_id": "string",
      "family_id": "string",
      "reason": "why the candidate was rejected"
    }}
  ],
  "needs_human_review": true,
  "human_review_reason": "string"
}}
"""


def clean_issue_name(csv_path):
    name = csv_path.stem

    for suffix in [
        "_toc_enriched_v10",
        "_toc_enriched",
        "_enriched",
    ]:
        if name.endswith(suffix):
            name = name[: -len(suffix)]

    return name.replace("-", "_")


def main():
    triggers, _ = load_codebook_triggers(CODEBOOK_PATH)
    print(f"Loaded {len(triggers)} triggers")

    csv_files = sorted(INPUT_DIR.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in {INPUT_DIR}")

    for csv_path in csv_files:
        issue_name = clean_issue_name(csv_path)
        output_dir = OUTPUT_ROOT / f"pilot_outputs_{issue_name}"
        output_dir.mkdir(exist_ok=True)

        existing_prompts = list(output_dir.glob("*.txt"))
        if existing_prompts:
            print(f"Skipping {csv_path.name} because prompts already exist in {output_dir}")
            continue

        print(f"\nProcessing {csv_path.name} → {output_dir}")

        df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig")
        article_rows = df.head(MAX_ARTICLES)

        all_windows = []
        all_candidate_codes = []

        for _, article_row in article_rows.iterrows():
            windows = extract_windows_for_article(article_row, triggers)
            candidate_codes = rank_candidate_codes(windows)

            all_windows.extend(windows)

            for c in candidate_codes:
                c["issue_id"] = article_row.get("issue_id", "")
                c["article_id"] = article_row.get("article_id", "")
                c["title"] = article_row.get("title", "")
                all_candidate_codes.append(c)

            prompt = make_article_prompt(article_row, windows, candidate_codes)

            safe_title = re.sub(
                r"[^a-zA-Z0-9_-]+",
                "-",
                str(article_row.get("title", ""))
            )[:50]

            prompt_path = output_dir / f'{article_row.get("article_id", "")}_{safe_title}.txt'

            with open(prompt_path, "w", encoding="utf-8") as f:
                f.write(prompt)

        pd.DataFrame(all_windows).to_csv(
            output_dir / "pilot_window_candidates.csv",
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        pd.DataFrame(all_candidate_codes).to_csv(
            output_dir / "pilot_candidate_codes.csv",
            sep=";",
            index=False,
            encoding="utf-8-sig"
        )

        print(f"Saved prompts to: {output_dir}")

    print("\nDone building prompts.")


if __name__ == "__main__":
    main()
# %%