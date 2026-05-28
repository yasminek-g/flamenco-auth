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
    "article_id": "JALEO_1982_04::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[Editor: The following articles have been taken from the PISL Newsletter, published between 1968 and 1970. They present several different views on Americans and their ability to sing flamenco. We hope readers will be stimulated to send in their views on this subject and perhaps update us on the efforts of non-Spaniards in the cante.] THE CANTE AND AMERICANS (from: FISL Newsletter, July 1968) The volumes of books flamencologists have written on who's who in flamenco and where this and that came from, number it the hundreds. They have never said Americans can't sing flamenco; the question never even occurred to them; they are too involved in judging who in Andalucía can sing flamenco, since anyone born outside of that blessed region couldn't even begin to attempt it. This \"impossibility\"\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mention\"]\n\n't even begin to attempt it. This \"impossibility\" attitude has greatly stunted the growth of the art both in this country and in Spain. The first real breakthrough for Americans in the cante came in 1959 at the Córdoba Concurso de Cante Jondo. German-American Elaine Dames (Elena Marbella), entered the competition with artists of such stature as Juan Talegas, La Fernanda de Utrera, and Fosforito. She didn't win any prizes, but was given honorable mention by the judges. (For detailed description, see \"Oido al Cante\" by Anselmo Gonzalez Climent (Madrid, 1960), pp. 77-85.) Rumor has it that American-born Maureen Carnes entered the 1968 Córdoba Concurso where she went as far as the semi-finals. Maureen has spent a number of years on the other side working and studying. It has been said that she is one of the most knowledgeable people in Spain with regards to the cante. Elaine Dames' students, David Morenos, Luisa Verette, Anita Volland, Antonio de Jesus, Estela Zatania, and Maureen Carnes, together with their respective students, can be considered to represent an American school of cante flamenco. Although other teachers and singers have certainly made contributions and are part of the school, Elaine must be given the initial credit for her inspiration, methods of teaching, and, most important, her belief in the cante for Americans. $ ^{*} $ $ ^{*} $ $ ^{*} $ ON CANTE FLAMENCO AND NORTH AMERICANS (from: PISL Newsletter, Sept. 1968; translated by Paco Sevilla) by Alberto de Santiago I just finished reading, with a great\n\n[ENDING CONTEXT]\n\ncreates the manic-depressive mood of so many \"jondo\" flamencos; you need to learn the difference between the aztless, direct and penetrating statements made in gypsy verses and the crafted, polished and refined sentiments that characterize Spanish folkloric (non-gypsy) verses. You need to learn about the persecution that molded the gypsy mind. You must begin to understand the special form that Christianity has taken in Spain. You have to learn about the fatalism, the male chauvinism, the aloneness, and the strange humor of the flamencos. The verses will tell you all this. Nothing else can.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "NON-SPANIARDS AND THE CANTE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "10-12",
    "page_number": 10,
    "word_count": 2688,
    "article_char_count_full": 15765,
    "article_char_count_review": 3163,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mention"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_04::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nART IN THE FORM OF DANCE (from: ABC, Oct. 25, 1981; sent by Gordon Booth; translated by Roberto Vázquez) by Tomás Balbontín She does not hesitate. Her ideas are clear and are engraved in her spirit as if by fire. She wants to learn, to keep on learning more and more, until...Does it have an end? Ana María Bueno has now obtained her degree as a dance teacher. A degree that just the acknowledgement of years and years of total dedication, ever since the time when at four years of age, this paya from San Julian got up on the stage and became dazzled by the lights. Since then, everything has been a long apprenticeship, a continuous striving for perfection. --Ana María, what does that degree mean to you now? \"Now? Now it is time to start all over again, to begin learning anew, admitting that\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"know\"]\n\nthat just the acknowledgement of years and years of total dedication, ever since the time when at four years of age, this paya from San Julian got up on the stage and became dazzled by the lights. Since then, everything has been a long apprenticeship, a continuous striving for perfection. --Ana María, what does that degree mean to you now? \"Now? Now it is time to start all over again, to begin learning anew, admitting that you will never get to know everything. This you accept because you have heard it a thousand times from those who are recognized as'sacred giants' of the dance. One never reaches the end; it is impossible to attain the absolute perfection. One must overcome that arrogance which seems to be a part of the artist in order to impose on yourself the conviction that you have to learn something new every day. This is a competition; higher, faster and stronger. Always learn. Either you accept the challenge that the dance poses, or throw in the towel.\" --In order to acquire the professorship, a mastery of the Spanish dance, regional dance, bolero, classical and flanenco is required. Each one with its rules, each one with its techniques and fine points. And in flamenco, the inspiration, that undefinable duende. \"Each cante or each music for dancing has its exact compás, but also calls for a feeling of rhythm. In flamenco one can maintain a very exact rhythm in the dance por alegrías or por cantinas and the result will be gray, feeble, lifeless\n\n[ENDING CONTEXT]\n\nout of rules with exactness, good taste, bearing and reliability. A strictly technical woman. But, isn't there room for the intuitive, vital, gypsy dance? \"I don't believe that there is a dance for gypsies and a dance for non-gypsies. Although the racial strength of the former makes itself felt notably in the interpretation of particular styles, I believe that the dance exists only with its basic principles, with its rules. I have always tried to respect those rules.\" ANA MARIA BUE (from: ABC Oct 25 1981) photos by Ruesga Bono SEVILLANAS BOLERAS CLASSICAL SPANIS \"CORDOBA\" BY A FIFTH POSITION\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ANA MARIA BUENO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 1211,
    "article_char_count_full": 6838,
    "article_char_count_review": 3098,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "know"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_04::A8",
    "article_text_for_review": "Transcriptions by Peter Baime",
    "title": "STRUCTURE UP CLOSE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "16-17",
    "page_number": 17,
    "word_count": 4,
    "article_char_count_full": 29,
    "article_char_count_review": 29,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_04::A9",
    "article_text_for_review": "(from: New York Daily News, Mar. 7, 1982) by Joan Shepard Students and aficionados of the classical and flamenco guitar have a home at the American Institute of Guitar at 204 W. 55th St. It is the place where guitarists play, study, and just hang out. Founded seven years ago, there are about 150 private classes held every week and teachers sometimes include guitar masters Sabicas and Mario Escudero. \"All classes,\" said the institute director Perry Koplik, \"are one-on-one, except for the master classes, which number about 30.\" The institute holds concerts at 8 on most Friday nights. Admission is only $2. \"You hear pure unamplified sound here. \"This summer,\" said Koplik, \"Sabicas will hold master classes and we are going to have a flamenco festival.\" For information, call (212) 757-4412.",
    "title": "GUITAR AND THE MASTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "article",
    "pages": "18",
    "page_number": 19,
    "word_count": 131,
    "article_char_count_full": 796,
    "article_char_count_review": 796,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_04::A10",
    "article_text_for_review": "The XX Festival Flamenco and Summer Flamenco Courses will be held this year in Jerez from August 22 through September 8. As in past years, the program has four parts: I. Nightly lectures about flamenco and recitals of poetry, flamenco piano, cante, baile, and guitar; II. Tourist activities such as visits to museums, bodegas, vinyards, monuments, etc.; III. Advanced and intermediate courses in guitar and dance, from 9:00 AM to 2:00 PM each day (this year, alegrías and soleares will be the forms studied; students must already Flamenco Guitars For Sale 1962 JOSE RAMIREZ SPRUCE TDP CLASSICAL GUITAR STAMPED \"PB\" EXCELLENT CONDITION $2,800 1949 MARCELD BARBERO FLAMENCO, PEGS, CLEAR GOLPEADDRES, OUTSTANOING FLAMENCO SOUND S2,400 1974 MANUEL VELAZQUEZ SPRUCE TOP CLASSICAL GUITAR, SIGNED, EXCELLENT CONDITION $2,000",
    "title": "XX FESTIVAL FLAMENCO EN JEREZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 19,
    "word_count": 125,
    "article_char_count_full": 817,
    "article_char_count_review": 817,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
