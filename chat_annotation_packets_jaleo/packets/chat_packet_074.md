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
    "article_id": "JALEO_1980_05::A2",
    "article_text_for_review": "(from: Delegacion de Cultura del Excmo. Ayuntamiento de Sevilla; sent by Vicente Granados; translated by Paco Sevilla) If Andalucía is the synthesis of cultures, then the flamenco art, the most universal of its expressions, is the encounter of the sorrows and joys of the rhythms of the people. It is an art that has grown in two centuries, incessant creator of new forms and esthetic dimensions of \"el grito\" (the crying out). If flamenco, instead of being born in Andalucía, had been created on the southern coast of the United States, it is more than likely that the number of Spanish flamenco aficionados would be substantially greater than the number who are presently capable of feeling the \"ecos\" of Tomas el Nitri or Silverio. Flamenco, in spite of its capacity for creating so much beauty, is still contemptu- The strength of this artistic expression has been so imposing in its inspiration and presence that it has crossed its own borders and infected other forms and artistic disciplines. The \"I Bienal de Arte Flamenco -- Ciudad de Sevilla\" attempts to bring together an exhibit of all these manifestations, to join them in a single competition so that we can make a reliable check of the generative capacity of flamenco. During the two weeks, on different stages, Sevillans had the opportunity to see the voice of flamenco reflected in movies, the plastic arts, theater, and literature. The session began on April 6, 1980 with an opening speech by the poet from Granada, Luis Rosales, and a concert by the Orquesta Bética Filarmónica de Sevilla. That same day, doors opened on exhibitions of painting sculpture, ceramics and photography -- the first exhibitions of such magnitude in Sevilla. In the afternoon, Mario Maya presented his dance theater. Toward the end of the week, the movies -- from the old films of the 1940's to the television serials to the shorts and documentaries. From Monday the 14th until the closing of the \"Bienal\" there was an exhibit of flamenco records and books, including all of the most recent editions. A special book of the best photos and letras was published, along with the essays and speeches delivered in homage to Antonio Mirena in another edition. There was a contest, \"Giraldillo del Cante,\" in which six of today's most important flamenco artists (not named in the article) each sang twelve different styles of cante in a rigorous search for the most complete cantaor, the one capable of interpreting the widest variety of flamenco forms. It was a contest in which all of the participants were winners, since they were selected by all of the flamenco peñas in Spain. The \"I Bienal del Arte Flamenco -- Ciudad de Sevilla\" was an effort to universalize even more, and more profoundly, the image and presence of flamenco.",
    "title": "I BIENAL DE ARTE FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "4,23",
    "page_number": 4,
    "word_count": 468,
    "article_char_count_full": 2772,
    "article_char_count_review": 2772,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_05::A3",
    "article_text_for_review": "Dear Jaleo, Just a line from London, England, to let you know that flamenco thrives here. There are many classes for dancing and guitar; I teach the dance in three of the evening Institutes, that run classes in all subjects here and there is good enthusiasm among the students, some of whom visit Madrid when they can to study at Amor de Diós. Some of my students, in a group known as \"Los Bohemios\", will be appearing in\"The Festival of Mind and Body\" at Olympia on June 28th. We are looking forward to this event which is timed for six p.m. We are preceded by a group of Spanish children called \"Niños de Oro\" who play flamenco guitar. Their tutor is Paquita Pérez, a professional who plays regularly for a small company led by dancer María Rosa. Flamenco enthusiasts here meet the first Sunday in every month at a restaurant in east London called \"The Sultan Amet\". They have to have a meal there, but can take their own wine and usually have a juerga which sometimes starts in the afternoon and carries on in the evening at someone's home. I was interested to come across your magazine and to find that there are pockets of flamenco spread around the world. Long may it continue to be international! Sandra Escudero London, England",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 222,
    "article_char_count_full": 1235,
    "article_char_count_review": 1235,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_05::A4",
    "article_text_for_review": "By Brook Zern It occurs to me that I have not written any crank letters to you for many months. So, after reiterating how much I have enjoyed your publication, I'd like to rectify this oversight. To that end, I'll seize upon an innocent remark by your editor, in his interesting and considerate appraisals of flamenco guitar records by Americans (March issue). Paco raises the issue of playing other people's material. \"Emphasize original material,\" he advises. \"Playing other people's material is for nightclubs or, if kept to a minimum, concerts, but records should offer something original, a personal statement by the artist.\" Okay -- in the particular case he was describing, Paco's point was well taken. To replicate already-recorded material by a single and well-documented player does diminish the values of a recording. But he is touching on a broader question for guitarists, and one which has occupied me for some time.",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 152,
    "article_char_count_full": 930,
    "article_char_count_review": 930,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_05::A5",
    "article_text_for_review": "by Juana De Alva This issue of $ \\underline{\\text{JALEO}} $ will be a combined MAY-JUNE issue to get $ \\underline{\\text{JALEO}} $ back on track and into readers' hands toward the beginning of each month instead of the end. Advertisements taken out for May and June will be extended an extra month. There will be other policy changes which will be announced in upcoming months to facilitate production and cut down expenses. Special thanks to those readers who are passing on the enclosed gold subscription forms to prospective subscribers. If you will include your name on the back when passing on these forms it will enable us to express our appreciation individually to you.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 113,
    "article_char_count_full": 676,
    "article_char_count_review": 676,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_05::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nI Quincena de Flamenco y Arte Andaluzay (From: $ \\underline{ABC} $, Dec. 1979; sent by John Fulton; translated by Paco Sevilla and Brad Blanchard) We received in the mail an assortment of articles about the \"I Quincena de Flamenco y Música Andaluza\". Only one of them had a date on it and none of them gave a comprehensive description of what it was all about. However, we can make some assumptions: \"Quincena\" means \"two weeks\" (fifteen days in Spanish). So, this was a two week series of performances of flamenco and Andalusian music that began around December 1st and ended on the 14th. It was held in Sevilla's \"Lope de Vega\" theater and coincides with the theater's fiftieth anniversary. Since it is called the \"I Quincena\", we can assume that it is intended to be an annual event. We have\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Famili\"]\n\nnniversary. Since it is called the \"I Quincena\", we can assume that it is intended to be an annual event. We have fairly complete information on the second week, but only scattered information on the first week. The performances (after-noon and evening) were thematic and ran roughly as follows: Week One (Dec. 1-6): --Guitar soloists: Rafaelito Riqueni, Enrique de Melchor, Paco Cepero --The gypsies: Angelita \"La Gitanilla\", La Susi, El Biencasao, Familia Montoya, Manuela Carrasco --Flamenco piano: Arturo Pavón, Felipe Campuzano Fri. Dec. 7 --Sevilla: Chano Lobato, Curro Fernández, Manolo Mairena, Naranjito de Triana, Chiquetete, Chocolate, Paco Taranto, Manuel Domínguez, Manuel Cala \"El Poeta\", Ana María Bueno Sat. Dec. 8 --Jerez y Cádiz: Camarón de la Isla, La Paquera, Beni de Cádiz, Terremoto de Jerez, Juanito Villar, La Tati Sun. Dec. 9 --Voces Rocieras (Sevillanas): Los Marismenos, Romeros de la Puebla, Amigos de Gines, Paco Palacios \"El Pali\" Mon. Dec. 10 --Rock: \"Guadalquivir\", \"Fragua\", \"Pata Negra\" (Raimundo and Rafael Amador) Tues., Wed., Thurs (11-13) -- Juanita Reina, \"La Escuela de Danza de Caracolillo\" $ \\underline{\\text{Fri. Dec. 14}} $ --Los Grandes Maestros: Pilar López and Curro Vélez, Matilde Coral and Rafael \"El Negro\", Rosario with Juan Morilla, Enrique \"El Cojo\" Rafael Riqueni \"...strict- ly a concert artist, he began as a child to perform alone.\" Enrique de Melchor, \"...he drank from one of the purest of foun- tains -- El de Marchena.\" \"With Paco Cepero we arrive at the tempestuous Jerez of vineyards, Santiago, narrow streets, old wines, and an exceptional compás.\" MAY-JUNE - 1980 MA\n\n[ENDING CONTEXT]\n\nher. And Pilar dances! PILAR, ETERNAL PILAR There in Madrid, on General Goget Street, she locked herself in her immense coffer of memories. Encarnación closely guarded in the trunks. A thousand trophies. A fabulous cana she did in Sevilla with the unbeatable accompaniment of a gypsy from Trina: Curro Vélez. MATILDE CORAL CURRO VELEZ MATILDE AND RAFAEL A complete school in a corner of Fray Isidoro in Sevilla is an oven for cooking the bread of the art. Matilde and Rafael in a triumphant night, against the definitely flamenco background of the guitar of Manolo Domínguez. Two artists of Sevilla.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ANDALUZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "7-13",
    "page_number": 7,
    "word_count": 1443,
    "article_char_count_full": 8585,
    "article_char_count_review": 3255,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Famili"
      }
    ]
  }
]
```
