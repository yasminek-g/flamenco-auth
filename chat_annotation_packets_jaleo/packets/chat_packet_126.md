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
    "article_id": "JALEO_1981_12::A6",
    "article_text_for_review": "\"Paco Peña and Friends,\" September 10, 1981; Thames Theatre, Slough, England: I am very \"up\" for this evening, traveling by bus to the suburb of Slough, impatiently priming myself at a rustic pub down the street. The hall is intimate and filled to three-quarters capacity; the acoustics are excellent. At last, Paco appears on stage looking formal, dignified, perhaps a little nervous. He gently coaxes tender soleá from his guitar; the tempo is very restrained. Machine-gun bursts of rasgueado erupt now and again, punctuating the mood, deepening the contrasts. He follows with tarantas, then alegria, farruca, colombianas, bulerías, guajira. His playing is very crisp, very adroit, the variations clean, clear and not particularly modern. He introduces a few selections in somewhat halting English; his tone is that of an undergraduate professor. This part of the program closes with sevillanas. Intermission follows. My feelings are very mixed. Paco's \"Friends\" (never introduced or mentioned in the program) begin the second half with a sevillanas-fandangos jumble. Two English girls dance and clatter castanets while a pair of guitarists keep the rhythm. One of them attempts cante in a thin and thoroughly undistinguished voice. His manner is aggressive and abrasive. He completely dominates everyone else on stage. Strutting to the edge of the podium, he accompanies himself in a rumba, providing his own raucous jaleo. Another dancer joins him and they move swiftly through a sloppy out-of-synch zapateado and climax with a flat tientos. What next? Paco Peña returns to the stage and patiently chords along with the others as a theatrical and totally unmoving alegrias tops off this segment. The audience is really getting into it. Everyone exits the stage except Paco and the previously almost unnoticed second guitarist. He proves to be Paco's long-time friend, an Englishman called \"Juanito Adrian.\" The man exudes happiness and warmth as he accompanies Paco in a delightful",
    "title": "PACO PEÑA: CONCERT REVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 311,
    "article_char_count_full": 1985,
    "article_char_count_review": 1985,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_12::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nINTERNATIONAL GUITAR SEMINARS CORDOBA 1981 ENCUENTRO B 27TH JULY by Ron Bray Paco Peña is to be congratulated for his choice of the picturesque Posada del Potro as the location for this year's \"I Encuentro Flamenco.\" I think it would be very difficult to find a more perfect setting in which to study flamenco. To enter the Posada you pass through two large doors which open out into a shaded and pleasantly cool reception area beyond which is a long rectangular cobbled courtyard where the evening concerts and discussions were held. At either side of the courtyard are stone stairs that lead up to a magnificent gallery that has been restored to its original condition, complete with heavy wood beams and a low red-tiled roof. There are two large rooms which were used as classrooms and several\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"place\"]\n\nnd pleasantly cool reception area beyond which is a long rectangular cobbled courtyard where the evening concerts and discussions were held. At either side of the courtyard are stone stairs that lead up to a magnificent gallery that has been restored to its original condition, complete with heavy wood beams and a low red-tiled roof. There are two large rooms which were used as classrooms and several smaller ones where students could find a quiet place to practice. The Posada is a beautiful building with a Soleares was the first rhythm studied, the cómpas first being clapped out and then played on the guitar. Paco made the point that the rhythm does not go wrong, only we go wrong; if we establish one pace and anything happens that does not coincide with the rhythm, we are out of cómpas. The best guitarist to accompany a dancer is the one that plays exactly on the beat and not slightly in front. Having learned to play the basic rhythm in cómpas, with accented beats and rasgueos, the class was shown a twelve-beat falseta that made extensive use of the thumb. Paco explained that the traditional structure for a soleares falseta is to start with a musical idea that lasts six beats, or half a compás, then repeat the half compás again with a PEPE MORALES ABOVE & BELOW: A NIGHT AT THE FLAMENCO PEÑA \"RINCON DEL CANTE\" LEFT: PACO PEÑA TEACHING all photos by Ron Bray RIGHT: LOS FARRUCOS LA FARRQUITA DANCING POR ALEGRIAS GUITAR STUDENTS IN THE POSADA DEL POTRO slightly different feeling, and finally resolve the falseta with one whole compás; th\n\n[ENDING CONTEXT]\n\nMoss lives the North-West's only genuine flamenco guitarist, Ron Bray. Ron, of Midgeland-road, is a former teacher at Blackpool and Fylde College and is now senior lecturer in graphics at Preston Polytechnic, but for the past 35 years he has also been a dedicated guitarist. Originally he studied classical guitar but always wanted to learn the flamenco style. \"It's not the sort of thing you can just copy from records -- you have to be shown,\" he says. \"There is more to flamenco than just playing the music -- it's a whole way of life. I was lucky in that I met various people who helped me.\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CENTRO FLAMENCO PACO PEÑA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "16-26",
    "page_number": 16,
    "word_count": 1152,
    "article_char_count_full": 6690,
    "article_char_count_review": 3179,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "place"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_12::A8",
    "article_text_for_review": "My family are Romany and originate from Ireland and although my grandfather played the concertina and my father the trumpet, there is no tradition of music in my family that is anything remotely like flamenco. When I was very young my father took me to the city of Leeds in Yorkshire and there I saw a shop window full of guitars. From that day, I always wanted to play the guitar. I bought my first guitar when I was 11 years old -- I grew up in Bridlington, a small fishing town in the northeast of Engalnd. There were no guitar teachers locally so I was self-taught. I first became interested in flamenco after seeing a flamenco show at my local theatre; although I can't remember the name of the group, I was absolutely knocked out with the dancing. At the age of 18 I went to college in London to study art and, while I was there, I started having classical guitar lessons. But gradually I became more and more interested in flamenco. Like most guitarists I have picked up lots of things from lots of people, like Juan Martín and a great friend of mine from Málaga, José Zamarrilla. I have also studied with Pepe Martínez for about 7 or 8 years. For the past three years I have been the guitarist with Paco Montes and Los Granados. Although I give solo guitar recitals, I prefer working with singers and dancers -- this gives me the most pleasure. We perform mainly in art centers, Spanish restaurants and have also appeared on radio and television.",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "27",
    "page_number": 27,
    "word_count": 266,
    "article_char_count_full": 1454,
    "article_char_count_review": 1454,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_12::A9",
    "article_text_for_review": "I'm still high from the exciting workshop that I shared with twenty-six beautiful people. All I can say is that it worked and we all came away with a deeper understanding of flamenco; there was a joy, energy and intensity that was, at times, unbelievable. From morning to night there was a total melting into flamenco, its technique, interpretation, and understanding. I'm always amazed by how much can be done in two weeks. Each year gets better and fuller. This year, the very beginners were moving in such a way that you thought they had been dancing two years. Each day began with technique classes at the beginning and intermediate-advanced levels. We used base rhythms of tango and soleares for a total movement class -- adding other rhythms so that the students would get used to moving and adapting the basic techniques to a variety of rhythms. By the end of the workshop, we had done technique classes to farruca, alegrias, taranto, siguiriyas, bulerías, and rumba; it was amazing to see the understanding of technique being applied and see the change in interpretation.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "28",
    "page_number": 28,
    "word_count": 183,
    "article_char_count_full": 1079,
    "article_char_count_review": 1079,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_12::A10",
    "article_text_for_review": "dance and produced something fascinating, entertaining and surprising. Guest dancer Nina Raimondo presented a gracious, balletic version of a classical bolero, and Isabel Morca, throughout the evening, offered a variety of bright solos. It was Morca, however, with his single-minded intensity and machine-gun speed who was the star. Morca's solo interpretation of J.S. Bach's Toccata and Fugue in D Minor is a classic, a unique reaction to this piece complete with flamenco footwork and castanets. Within the music, Morca found the delicate, often elusive cross rhythms which Bach wrote and managed to add a few of his own. By sometimes playing the opposite tempo of the music — slow, burning dance during fast keyboard passages — he pointed out the fire and ice in the score, recorded here on harpsichord. The combination of classical music and ethnic dance might have seemed incongruous with less perceptive talents. Morca and company made baroque and Spanish rhythms one in \"Aire y Gracias,\" set to the first two movements of Bach's Double Harpsichord Concerto in C Minor. A light touch was present, too. “El Zapatero y Las Botas Magicas” found Morca in the role of a shoemaker who discovered that flamenco shoes have a life of their own as the white boots apparently carried the unwilling character across the stage. ARCHIVO The Making of an Anthology by Caballero Bonald",
    "title": "CONCERT REVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 29,
    "word_count": 225,
    "article_char_count_full": 1375,
    "article_char_count_review": 1375,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
