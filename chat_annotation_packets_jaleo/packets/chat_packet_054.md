Annotate the following flamenco periodical articles using the collapsed codebook.

Be conservative. Prioritize precision over recall.

Rules:
- Annotate only the visible article_text_for_review.
- Do not infer from title, metadata, periodical, author identity, or missing text.
- Default to 0–3 codes per article.
- Use 4–6 codes only when clearly distinct evidence spans support different discourse functions.
- Never assign more than 6 codes.
- Do not emit low-confidence codes.
- Keywords are not enough. A code requires a discourse function: the passage must evaluate, authorize, exclude, preserve, teach, transmit, rank, criticize, or define belonging/authority.
- Every emitted code must include family, code, confidence, evidence_quote, target, and rationale.
- If no code is clearly supported, return codes: [] and no_relevant_discourse: true.
- If the text is too short, OCR-damaged, or insufficient for judgment, set insufficient_context: true.
- Put weak or ambiguous possibilities in possible_but_not_emitted, not in codes.

Allowed families and codes:
AUTH: AUTH_01, AUTH_02, AUTH_03, AUTH_04
HERIT: HERIT_01, HERIT_02, HERIT_03
PED: PED_01, PED_02, PED_03
COMM: COMM_01, COMM_02, COMM_03, COMM_04
CRIT: CRIT_01, CRIT_02, CRIT_03, CRIT_04

Return valid JSON only, as an array with one object per article_id:

[
  {
    "article_id": "...",
    "no_relevant_discourse": false,
    "insufficient_context": false,
    "codes": [
      {
        "family": "AUTH",
        "code": "AUTH_02",
        "confidence": "high",
        "evidence_quote": "...",
        "target": "...",
        "rationale": "..."
      }
    ],
    "possible_but_not_emitted": [],
    "derived_analysis": {
      "legitimation_effect_present": true,
      "polarity": "legitimating | delegitimating | contested | mixed | neutral | unclear",
      "basis": ["authenticity"],
      "target": "...",
      "exclusion_boundary_present": false,
      "right_to_define_present": false
    },
    "annotation_notes": ""
  }
]

---

Articles:

```json
[
  {
    "article_id": "JALEO_1979_10::A10",
    "article_text_for_review": "by Bob Clifton At 38, in the undistinguished physical and spiritual shape most common to that age, having no previous dance training and limited funds, you do not take two weeks off a straight job, flabbergasting your boss, and invest $200 in a crash course in flamenco dance taught by a Hungarian-American in Bellingham, Washington, without certain misgivings and a degree of self-consciousness. I did just that and apparently survived the self-consciousness. (My boss tactfully refrained from asking how it was, perhaps secretly convinced that the trip was a cover for something more manly -- like running off with a stripper from Atlantic City) What I would like to report here is how ungrounded the misgivings were. I believe I probably speak for most of the dozen and a half students from all over the country, representing the most diverse backgrounds and various relationships to flamenco, in saying that Teo Morca's workshop was a rich and rewarding experience -- well worth the time and money expended. Hopefully, he will report what kind of experience it was for him elsewhere GREGORIO WOLFE, GUITARIST FOR TEO MORCA",
    "title": "MORCA AS MAESTRO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_10",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 185,
    "article_char_count_full": 1126,
    "article_char_count_review": 1126,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_10::A11",
    "article_text_for_review": "(Daily News, Wed., July 11, 1979, Springfield, Mass. By Richard Conway) As long as I live, I shall never forget the indescribable beauty of Isabel and Teodoro Morca dancing the most pure and holy of love songs to the strains of Pachelbel's lyrically tender \"Canon in D.\" If the Jacob's Pillow Dance Festival program this week had nothing else of worth, that alone would be sufficient reason for making the trek to Becket. Teo Morca's duo is simply one of the most moving moments I have ever experienced in the arts. Out of the most simple of movements, they have created a tone poem of love that becomes a mystical, spiritual act of worship. The adagio movements and gestures are liquid lyric lines that sing and exult of their mutual affection. At one point, they simply walk around the stage together. Oh, yes, the walk is done with more style and grace and carriage than you or I would walk; but it is a walk nonetheless. And there is such an emotion passing between the dancers and thence to us that one wants to join them, to share in their joy and discovery. Such holy beauty cannot last, and the close of the dance finds the lovers at opposite diagonals on the stage and facing away from each other. The program note quotation from Gilbran explains it all: \"Sing and dance together and be joyous, but let each one of you be alone.\" The dance is a masterpiece.",
    "title": "MORCA COUPLE CREATES PURE, HOLY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_10",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 248,
    "article_char_count_full": 1366,
    "article_char_count_review": 1366,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_10::A12",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA REVIEW Copyright C 1979 by Carol Whitney All rights reserved Mairena, Antonio. Las Confesiones de Antonio Mairena. Edición preparada por Alberto García Ulecia. Publicaciones de la Universidad de Sevilla, Colección de bolsillo, Número 53, 1976. (See end of review for publisher's address.) Antonio Mairena's book Las Confesiones de Antonio Mairena is a self-portrait by a man who has dedicated his life to the preservation, resurrection, and sometimes artistic elaboration of the cante. Reading the book has three effects on me. (1) I see flamenco's history as if through Mairena's eyes, and find the experience interesting, sometimes jolting, and sometimes beautiful. (2) As I see through Mairena's eyes, I begin to sense the man's personal qualities, so that even his recorded cante suddenly\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"deeply\"]\n\nedicated his life to the preservation, resurrection, and sometimes artistic elaboration of the cante. Reading the book has three effects on me. (1) I see flamenco's history as if through Mairena's eyes, and find the experience interesting, sometimes jolting, and sometimes beautiful. (2) As I see through Mairena's eyes, I begin to sense the man's personal qualities, so that even his recorded cante suddenly comes alive for me in a new way. (3) I'm deeply impressed by the way Mairena sees his life in relation to Gypsy life. His dedication to the cante is no more nor less than his personal struggle for survival, and he makes clear in his chapter \"La Razón Incorpórea\" that his own survival depends on survival of Gypsy tradition. \"La Razón Incorpórea (pp. 79-83) is the essential chapter of the book, because it explains Mairena's tenacity and passion in maintaining his dedication in spite of difficulties. His hope that the cante's value would one day be recognized supported him in the face of \"the incomprehension, ignorance, and humiliation that singers in the flamenco ambiente of that time suffered\" (p. 80). Mairena expresses his personal faith---his joy--in his awareness of and love for his Gypsy tradition in all its aspects. It is this tradition that nourishes and sustains him. Without it\n\n[ENDING CONTEXT]\n\nare rarely given a juvenile aspect. For his students in all levels, his assessments of suitability are instinctively reliable. Throughout his career, he has adhered closely to the authentic approach, preferring to draw upon the vast well of traditional flamenco and Spanish dance. Ángel's knowledge of the English language was sketchy when he first came to Canada. He only vaguely understood the documents he signed which made him a Canadian resident. Raymond Muller, his sponsor, explained that Ángel's signature would mean giving Canada flamenco. There are many grateful Canadian aficionados.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MAIRENA'S CONFESIONES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_10",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "16-18",
    "page_number": 16,
    "word_count": 1229,
    "article_char_count_full": 7353,
    "article_char_count_review": 2924,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "deeply"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_10::A13",
    "article_text_for_review": "PASTORA IMPERIO DIES IN MADRID: Pastora Rojas Monge, 90, better known as \"Pastora Imperio,\" one of Spain's greatest flamenco dancers, who started dancing at age 13. She danced at the world premiere of Manuel de Falla's \"El Amor Brujo\" in 1917. (from the Los Angeles Times; sent by Ester Moreno)",
    "title": "PILAR LOPEZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_10",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 50,
    "article_char_count_full": 294,
    "article_char_count_review": 294,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_10::A14",
    "article_text_for_review": "This summer, in the Teatro Reina Victoria of Madrid, there has been a long run of a new theatrical version of \"La Historia de los Tarantos\" by Alfredo Mañas. It has been presented by the Companía del Teatro Andaluz, directed by Luis Balanguer (from Cádiz). The part of Soledad, La Taranta, is played by Rosa Durán (from Jerez); her children are Candi Román (Madrid), Felipe Sánchez (Murcia), and Cristina Durán (Madrid). Camisón is played by Fernando Sánchez Polack (Madrid), his wife by Margarita Calahorra (Madrid), and daughter by La Contrahecha (Sevilla). Curro el Picao, is Felix Ordoñes (Albacete) and the two brothers are Felix Granados (Madrid) and Eduardo Montes (Jaén). The singers are Carmen Linares (Linares), Rafael Romero (Andujar) and Chaquetón (Algeciras); guitarists are Perico el del Lunar (Madrid), Curro de Jerez (Jerez), and Carlos Habichuela (Granada).",
    "title": "NEW VERSION OF \"LOS TARANTOS\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_10",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 137,
    "article_char_count_full": 874,
    "article_char_count_review": 874,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
