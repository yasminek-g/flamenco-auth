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
    "article_id": "JALEO_1984_05::A20",
    "article_text_for_review": "DIRECTORY australia FLAMENCC ENTERTAINMENT Cosmos Inc. (Adelaide) canada DANCE INSTRUCTION Maximiliano (Toronto)",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_05",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "30",
    "page_number": 30,
    "word_count": 12,
    "article_char_count_full": 112,
    "article_char_count_review": 112,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A1",
    "article_text_for_review": "\"TODAY, 80% OF WHAT IS BEING PLAYED THROUGHOUT THE WORLD IS MINE!\" He went to New York for the first time in 1940 and was there for five years; after that he went to Mexico for a period and then returned to New York where he set up his permanent residence. He was married to a Mexican woman, the mother of his two children, and later divorced. During his first trip to North America he had his idyll with Carmen Amaya, perhaps one of his greatest loves, although the maestro won't be very explicit about that: \"Bueno, mire usted, it must have been one of those childish things, because nothing ever happened [\"luego no ocurrió nada de nada\"]\". We were good friends...we were together for a few years and then I stayed in Mexico and she came back here, to Europe.\" \"I have always told people I am fifty years and a few minutes old,\" he responded when I asked his age. I believe he must be about 75 years old. Speaking of the flamenco guitar today Spain, he declares that perhaps there isn't the \"solía\" of before, but the fingers and everything else have evolved and have gained a great deal. He is proud of having done for the guitar what nobody else has done until now: He has taken it around the world, made it fashionable, and given it some classical touches. \"The flamenco guitar was played nowhere except in Spain, and only by a few; it was a very small thing. Then, since my records came out in the last 30 years, people became fans of the flamenco guitar all around the world.\" Sabicas claims to come from no school of guitar playing JALEO - AUGUST/SEPTEMBER 1984 20% DISCOUNT TO ALL MEMBERS OF JALEISTAS 1011 FORT STOCKTON DRIVE OWNER TOM SANDLER SAN OIEGO, CALIFORNIA (714) 298-8558 (Hillcrest/Mission Hills area) PRESENTS TOMAS DE CHICAGO FLAMENCO GUITARIST TUESDAY THRU SATURDAY 3110 Newport Blvd., Newport Beach. CA 92663 (714) 673-3440 Supreme strings designed for today's finest classic and flamenco guitars Tés. (212) 307-1567 757.4412 or 3255",
    "title": "SABICAS: INTERVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 346,
    "article_char_count_full": 1958,
    "article_char_count_review": 1958,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A2",
    "article_text_for_review": "For those who remember the flamenco quiz published in Jaleo a few years ago and felt it was perhaps a little too difficult, here is a chance to compare it with a quiz taken from a popular magazine in Spain; it appears to be aimed at a general, layman, flamenco audience. Some of the questions and answers seem a little vague, perhaps even questionable, but here it is: 1. In which Andalucian city did flamenco develop and also serve as the birthplace of Tío Luis el de la Juliana in the last third of the 18th century: A) Montoro, E) Jerez de la Frontera, C) Alcalá de Guadalra 2. Cante jondo is a cante of extended tones, whose beginning or \"salida\" is given in notes that are: A) Mostly higher, a) Sustained, C) Mostly lower 3. \"El jiplo\" is a melodic fragment sung: A) With a single breath, B) With interruptions, C) In two voices 4. When a copla is sung in e tone that is brighter (\"mas agudo\" - sharper) than the previous ones, it is called: A) Canto liviano, B) Levantar el cente C) Comenzar por \"afillá\" 5. From the soled, two danceable cantes have been derived. They are: A) Bulerías and martinetes, B) Bulerías and Tientos, C) Bulerías and fandangos naturales 6. Two of the following cantes have not been transformed from their original popular from. Which has been?: A) Petenera, s) Martinete, C) Soleá 7. The solea has how many lines in each poetic verse? A) 5, B) 4, C) 3 8. By what other name is the siguiriya gitana known? A) Serranas, B) Playeras, C) Tanguillos 9. which cante is typical of blacksmiths and named after the hammer they use? A) Martinete, B) Fandango, C) Polo 10. The \"carceleras\" were a song of prisoners working at forced labor. They are a variant of which cante? A) la media caña, B) Martinetes, C) Tondá 11. By what other name are the \"fandangos de Cádiz\" known today?: A) Alegrías, B) Policañas, C) Milongas 12. The \"saeta\" is based on melodies of the siquiriyas or martinetes, but how many lines does it have in its poetic verse?: A) 4-5, B) 13-14, C) 3-4 13. What creation of the gaditanos (from Cádiz) is constantly changing and is sung in the carnavals? A) Peteneras, B) Columbianas, C) Tanguillo 14. Which of the following is intended to be performed at weddings?: A) Farruca, B) Mirabrás, C) Alborea (answers on page 21) Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059 Los Angeles, Ca. A Classic Combination PACO PENA & D'ADDARIO Born in 1942 in Córdoba, Spain, Paca Peña has been playing professionally since the age of twelve and has toured Europe both as a soloist and as part of the \"Paco Peña Flamenco Company\" to wide critical acclaim. Dedicated to conserving the pura artistry of flamenco, Mr. Peña established the seminar \"Encuentra Flamenco\" offering the aficionado an intensive program of study as well as the opportunity to live in Andalucía, the heart of this musical culture. He has recorded nine albums for Decca Records including three live performances and a duo effort with Paco DeLucia, another world renowned flamenco guitarist. He has also made several highly successful tours of Australia, given recitals with the company at festivals in Hong Kong, Edinburgh, Holland, and Aldeburgh and performed to audiences in Japan and London, all to widespread enthusiasm. Paco Peña appears regularly worldwide an Television and has received extensive praise for his shared recitals with John Williams. Paco Pena uses D'Addario Strings. D'Addario E. Farmingdale, NY 11735 USA PABLO Spanish-Continental Cuisine Picasso 5254 VAN NUYS BLVD. SHERMAN OAKS, CA 91401 (818) 906-7337 Dance Expo! Incorporated Proudly Presents The Juan Talavera Spanish and Flamenco Dance Workshop For Beginner and Intermediate Dance Students case Call (213) 699-9855 For Details",
    "title": "FLAMENCO QUIZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 636,
    "article_char_count_full": 3734,
    "article_char_count_review": 3734,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJALEO - AUGUST/SEPTEMBER 1984 CABRERO» CRAZY FOR HIM\" that before he will look at the reporter, he prefers to look at the mountain, at the singing birds, or at the goats who are wandering astray and crossing the road, gambling their lives for a skimpy stubble of brush; in spite of that, one is aware of eyes that are black like pieces of coal, very black eyelashes, and dark circles around the eyes. El Cabrero does know how to read, but he has the face of a prince and smells like a man. \"Do you see this belt? It was given to me by someone who was in jail for rape.\" This thing of the jail, Cabrero, has made you famous throughout Spain. \"The whole thing had a lot of 'guasa' (it seems like he is laughing at himself). A cowboy as President of the Government and El Cabrero in jail!\" Jose\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"body\"]\n\nd smells like a man. \"Do you see this belt? It was given to me by someone who was in jail for rape.\" This thing of the jail, Cabrero, has made you famous throughout Spain. \"The whole thing had a lot of 'guasa' (it seems like he is laughing at himself). A cowboy as President of the Government and El Cabrero in jail!\" Jose Domínguez, cantaor and goatherd, thirty-eight years old, born in Aznalcollar in the province of Sevilla, was arrested, as everybody knows, for blasphemy in public. During a recital in Alcolea de Córdoba, as he began to sing \"sé la hora por el sol...\" his voice failed him---caramba, artists are only human---and the smart alecks around began to tease him. With tears of anger, with his voice cracking, José Domínguez shouted a, \"me cago en Diós\" and a commander of the Guardia Civil expedited a report that resulted, two years later, in three months in jail. \"I served twenty-two days in confinement, twenty-two days stabbed in the soul and I will never ever forget it. I feel I was innocent. Me cago en Diós!\"\n\n[ENDING CONTEXT]\n\nlit a cigarette and launched into some private thoughts: \"I bother people. I'm dangerous. For me, since my detention, there is only that. For all of 'them', my way of life is a danger. Because I don't want to be a millionaire, nor do I want my daughter to marry a famous bullfighter, nor do I want to cover my wife with jewels. If I sing, if I want money, it is to buy goats and a little ranch house where I can raise animals and my wife can give me many children. And to be able to be free of the flamenco festivals. So that they don't have to pay me ninety thousand pesetas to hear me sing.\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "EL CABRERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "6-10",
    "page_number": 6,
    "word_count": 1289,
    "article_char_count_full": 6914,
    "article_char_count_review": 2645,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "body"
      }
    ]
  },
  {
    "article_id": "JALEO_1984_08::A4",
    "article_text_for_review": "$ x_{1} + x_{2} = 2 $ (Photos: Peter Holloway) MANUELA CARRASCO",
    "title": "\"FLEGMENCO\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 12,
    "article_char_count_full": 63,
    "article_char_count_review": 63,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
