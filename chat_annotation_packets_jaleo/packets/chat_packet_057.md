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
    "article_id": "JALEO_1979_11::A11",
    "article_text_for_review": "(From: El Correo de Andalucía, August 25, 1979; by J.G.; sent by R. Reyes and La Vikinga; translated by Paco Sevilla) Quién dijo que el cante ha muerto? Martinete, no golpees más mis sienes; deja reposar mis sentimientos; déjame aceptar la realidad de esta seguirilla; déjame respirar, que el no lo hace. Cuando uno no sabe que es el cante, quizá pueda decir que ya no hay cante. Cuando uno no haya oído nunca ese lamento en \"caía\", ese ritmo \"cortao\", ese llanto del bordón.... quizá pueda decir que no hay cante. Pero cuando uno se enfrenta, se pelea con el aire,",
    "title": "QUE JONDO CALO EL CANTE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_11",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 102,
    "article_char_count_full": 565,
    "article_char_count_review": 565,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_11::A12",
    "article_text_for_review": "by Laurie Randolph (Arseguell, Catalonia) In May-June of this year, two American guitarists were invited to bring their program of Spanish music to its homeland. Ten concert halls of the Aula de Cultura (Spanish cultural organization) capped off their season's performances of \"Mano a Mano\", the two-year-old partnership of guitarists Anita Sheer and Laurie Randolph. Although their first joint tour of Spain, this was not a first visit of either artist to the country. Anita Sheer studied the flamenco guitar and cante jondo with gypsies in Andalucía and Madrid. Laurie Randolph made her first trip to Spain in 1972 when the North Carolina School of the Arts guitar class paid a visit to Segovia's home near Granada. The program, alternating classical, folkloric and flamenco solos and duos, included music of the New World with Iberian character-",
    "title": "MANO A MANO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_11",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 136,
    "article_char_count_full": 848,
    "article_char_count_review": 848,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_11::A14",
    "article_text_for_review": "(photos from Carlena Gerheim) Here are some scenes from the Colony Restaurant in Cleveland, where Spanish food is served and flamenco presented in a room called \"Tio Manolo's\". The featured attractions are currently Marina Torres and José Luis, recently married Spaniards who had been working with Los Chavales de España for several years. SINGER/DANCER CARLENA \"LA MAYA\" (GERHEIM), TOM SHEPHERD \"TOMAS PASTOR\", AND DENNIS GERHEIM \"DIONISIO\"",
    "title": "FLAMENCO IN OHIO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_11",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 66,
    "article_char_count_full": 441,
    "article_char_count_review": 441,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_11::A15",
    "article_text_for_review": "We present these photographs of Louis. Ernest Lenshaw in order to honor him on his 87th birthday (September) and mark his retirement from active flamenco participation Ernesto was well into his 50's when he began the serious study of Spanish dance, flamenco guitar, and castanet making; he achieved a remarkable degree of skill in all three areas. All photos are from the 1950's and 60's when Ernest lived in San Francisco",
    "title": "LOUIS ERNEST LENSHAW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_11",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "20",
    "page_number": 20,
    "word_count": 71,
    "article_char_count_full": 422,
    "article_char_count_review": 422,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_11::A16",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Juana DeAlva Beginning this issue Juerga reports of past juergas will appear in the following month to allow more time for preparation of photos and reports. We see cameras going off at the juergas but don't often see the results of these photos in the newsletter. Your juerga photos or other flamenco related photos would be appreciated. Please get them to us the first week of the month to be processed. SEPT. JUERGA RETURN TO OLD STAMPING GROUNDS Jaleistas returned in September to one of their first gathering places - the Alumni cottage of the National University. This was the site of the first New Year's Eve juerga, of Paco's slide presentation and where we were honored by the participation of Don and Luisa Porhen, Teo & Isabel Morca and Gary Hayes. Since our last visit to the N.U.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"duende\"]\n\neek of the month to be processed. SEPT. JUERGA RETURN TO OLD STAMPING GROUNDS Jaleistas returned in September to one of their first gathering places - the Alumni cottage of the National University. This was the site of the first New Year's Eve juerga, of Paco's slide presentation and where we were honored by the participation of Don and Luisa Porhen, Teo & Isabel Morca and Gary Hayes. Since our last visit to the N.U. cottage, we have pursued the duende in all areas of the county. We have had juergas in Point Loma, Santee, Del Mar, La Jolla, Lakeside, Escondido, Encinitas, Pacific Beach, Poway and downtown San Diego. Like anytime that we try to return to the past, there are disappointments -- things are not quite the same. On setting up before the juerga we discovered that much of the furniture and all of the table lamps had been removed and many folding chairs left in their stead. This was o.k. because less furniture means more room for dance and we were able to improvise with the existing lights to create the desired atmosphere. Next we found that part of the house had been locked up. These rooms were not used extensively in the past but in locking them we were reduced to only one access to the \"Back Room\" (a favorite of the rumba enthusiasts and site of Rosala's darkroom sevillanas lessons). This single access was through the bathroom. Beside the obvious inconvenience this created, anyone in the \"Back Room\" was in danger of being imprisoned for the entire evening if the second door to the bathroom was left locked. Hopefully this situation can be remedied on our next return. We tried also to revive an old custom of having a dinner juerga but most people are conditioned to \"tapas\" by now and don't bother looking at the \"what to bring\" section. Those who did, however, went all out. There was a giant circular loaf of home made bread, some spicy tamale pie, several cakes, including a hand decorated birthday cake in honor of one of our most active and enthusiastic jaleistas - Ernest \"Ernesto\" Lenshaw. This was another late starter in which we wondered if anyone was going to come and then about 9:00 we were suddenly inundated with fifty arrivals. The \"Sala Hundida\" was our \"Cuarto Hondo\", the \"Sala Grande\" and the kitchen were fiesta rooms and the \"Back Room\" was ma\n\n[ENDING CONTEXT]\n\n213-469-9701 Pedro Carbajal 1828 Oak St. Ester Moreno 213-506-8231 san diego... HISPANO-MEXICAN BALLET Provides a \"taste of Spain\" every third Saturday in Old Town starting Nov. 10th. Performers are dancers - María Teresa Gomez, Juanita Franco, Laura & Tina Crawford, Carmen Monzón; guitarists - Jim Owen and Rod Hollman. POSTERS WANTED: Paco Sevilla is looking for flamenco posters of all types, both Spanish and non-Spanish, promoting personalities, festivals, concerts, etc. If you have any that you don't want or would like to sell, contact Paco through the Jaleo, Box 4706, San Diego, CA. 92104\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_11",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "21-24",
    "page_number": 21,
    "word_count": 1148,
    "article_char_count_full": 7171,
    "article_char_count_review": 3926,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "duende"
      }
    ]
  }
]
```
