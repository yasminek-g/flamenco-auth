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
    "article_id": "JALEO_1978_07::A12",
    "article_text_for_review": "The following are comments overheard at flamenco dance performances. On all three occasions they were made by one elderly woman to another: \"Look how colorful they are; they're such a happy people, singing and dancing all day long!\" \"They're no good - those are $ \\underline{\\text{white}} $ people; come with me and I'll show you some real Mexicans!\" \"I hope they own their own house; you could never make all that noise in an apartment!\" Omitted Announcement: KENNETH SANDERS plays solo guitar (classical flamenco, modern) Friday and Saturday nights 6-9:00 P.M. at the Jolly Franciscan restaurant, 31781 Camino Capistrano in San Juan Capistrano, Ca. For reservations, call: (714) 493-6464. Jaleistas wishes to thank John MacDonald for the contribution of a paper cutter; it was very badly needed. Thank you John!",
    "title": "What Our Audiences Are Saying",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 130,
    "article_char_count_full": 813,
    "article_char_count_review": 813,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A13",
    "article_text_for_review": "\"THE EXCITING SOUND OF FLAMENCO - JUAN MARTÍN\" Two books: \"Zambra Mora\" and \"Brisas Habaneras\" (guajiras), $3.75; \"Mi Rumba\" and \"Aires Gaditanos\" (cantiñas), $5.00. United Music Publishers LTD, 1 Montague St., London W.C.1 Review by Paco Sevilla These books of music by the British guitarist, Juan Martin, are two more examples of the excellent music being transcribed for flamenco guitar these days. The music is taken directly from the commercially available record, \"The Exciting Sound of Flamenco\" (Argo ZDA 201) and is accurately notated in music and tablature. The \"Zambra Mora\" (D-tuning) and the guajiras are good solo pieces, made up of material from different sources (the zambra is very similar to Luis Maravilla's) with only a small dose of originality. The pieces in the other book, rumba and alegrías (E major-minor), are less effective solos, but have more originality and contain some good ideas. Available from Theodore Presser Co., Bryn Mawr, Penn. Born to Thor and Peggy Hanson, a son, Wyatt James; he has already attended his first juerga.",
    "title": "NEW FLAMENCO GUITAR MUSIC",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "article",
    "pages": "18",
    "page_number": 18,
    "word_count": 170,
    "article_char_count_full": 1060,
    "article_char_count_review": 1060,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A14",
    "article_text_for_review": "La Vikinga On April 21, 1978, Antonio de Jesús (cantaor), Roberto Reyes (tocaor), and myself, gave a concert in Northport, Long Island. We followed our usual procedure of sending flyers to aficionados in the surrounding area. It \"paid off\" as the house was packed. Roberto always tries to explain to the audience before the concert begins, a little bit about what flamenco is, and that the artists and the audience are about to embark upon an adventure. ANTONIO DE JESUS In keeping with the spirit of the evening, he invited all the flamencos in the audience to come up on stage an participate in our encore, \"fiesta por bulerías.\" George Thompson played guitar, his wife, Bernadette, and Alicia Laura danced, and Antonio and Roberto competed in letras and desplantes. The moment was captured by our \"trusty\" video-tape deck. ROBERTO REYES AND GEORGE THOMPSON",
    "title": "Juerga in Concert",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "18, 19",
    "page_number": 18,
    "word_count": 143,
    "article_char_count_full": 859,
    "article_char_count_review": 859,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A15",
    "article_text_for_review": "THE ESTHETIC & SPIRITUAL ASPECTS OF FLAMENCO WITH REFERENCE TO JUERGAS by David Blakley García Lorca, a Malagueño, when writing of flamenco and the Málaga region, spoke of flamenco as, \"the music of the people who think with their hearts.\" This article will be primarily a mini-treatise on some of the esthetic and spiritual aspects of flamenco, with reference to juergas. The breakfast juerga failed to jell. Dancing never really materialized, and, although it was guitaristically excellent with the steadfast and driving presence of Paco Sevilla, there was no singer of cante jondo. An interesting definition of flamenco states that the song is the primary element, that it alone could truly stand alone as the complete embodiment of the soul of flamenco. As the writer approached the locale of the juerga, he was very agreeably impressed by the rather surrealistic scenery of the chaparral country, which was reminiscent to him of suburban Córdoba. Consider, readers, how fortunate we have been to have each juerga set in a lovely, usually Spanish",
    "title": "June Breakfast Juerga",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "poem",
    "pages": "19",
    "page_number": 19,
    "word_count": 170,
    "article_char_count_full": 1050,
    "article_char_count_review": 1050,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A16",
    "article_text_for_review": "JALEISTAS ANNIVERSARY JUERGA - JULY 15th In honor of Jaleista's first anniversary, we are holding the July juerga at the site of our initial formative juerga a year ago - the Palmer ranch in El Cajon. Maus and Mary Palmer, parents of Juana De Alva, have been involved in the graphic and plastic arts all their lives. Maus, an artist by profession, has chosen circus and dance as the main subjects for his oil paintings (some of which may be viewed in Maus and Juana's mutual studio at the juerga). Mary, who studied theater and dance, has been active in both over the years, appearing professionally on broadway, working in little theater and teaching modern dance. Both have been long time followers of Spanish and flamenco dance and special admirers of Carmen Amaya. As last year, this will be an outdoor-indoor juerga, beginning on the lawn, patio and bar-B-que areas and moving indoors to the STUDIO as the evening cools off. Date: July 15th Starting time: 6:00 FM Location: 1721 Vista Way (end of Lisbon Lane)",
    "title": "JULY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "22",
    "page_number": 22,
    "word_count": 176,
    "article_char_count_full": 1014,
    "article_char_count_review": 1014,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
