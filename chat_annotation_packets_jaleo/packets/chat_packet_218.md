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
    "article_id": "JALEO_1989_03::A16",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nVIVA GYPSY! Rosa Montoya Bailes Flamencos celebrated its fifteenth concert season with the world premiere of Viva Gypsy July 15th and 16th at the Herbst Theatre. \"In a village outside the ancient city of Sevilla,\" reads the program notes, \"dusk approaches. Gypsies gather with handwoven baskets of roses. The scent of flowers mingles with wild sage, anise, and mint that grows by the roadside and the robust aroma of a feast being prepared — veal roasting over an open fire, oranges, cinnamon...\" The mood is set for this wedding celebration in dance. Rosa Montoya is the bride, guest artist Roberto Amaral the groom. Other family members include: dancers Nemesio Paredes, Carlos D. Escobar, Alma Janera, Malia de Felice, Susana Carmo, Maria Davidauskis; singers Roberto Zamora and Sevilla born\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"classic\"]\n\noasting over an open fire, oranges, cinnamon...\" The mood is set for this wedding celebration in dance. Rosa Montoya is the bride, guest artist Roberto Amaral the groom. Other family members include: dancers Nemesio Paredes, Carlos D. Escobar, Alma Janera, Malia de Felice, Susana Carmo, Maria Davidauskis; singers Roberto Zamora and Sevilla born guest artist from the San Diego area Charo Monge; guitarists (flamenco) Guillermo Rios and Juan Moro, (classical) Charles Ferguson and Timothy Lawler. Other flamenco pieces in the program included: tientos, cantinas, romeras, caña and tanguillos. Classical pieces included: Rumores de la Caleta by Albeniz, Granados' Intermadio de Goyescas and Breton's Zapateado. Rosa Montoya (photo by Charles Mullens) There were also solo, duo and trio presentations by the classical and flamenco guitars. Rosa Montoya's company which has been very active in the San Francisco Bay area for fifteen years, is deservedly supported by the California Arts Council and National Endowment for the Arts. *** BALLET FLAMENCO LA ROSA This Miami-based flamenco company was joined for two concerts in January by the Middle Eastern dancer Myriam Eli and oud player/vocalist Joe Zeytoonian, both from Oudansquerade. Together the companies presented a new, full-length version of Pasaje, a work which premiered last season. The piece illustrates the influences which centuries of Moorish rule left upon the culture of southern Spain. In Pasaje, dancers explore the rhythms and melodies both of traditional flamenco and the Middle Eastern music which forms an essential part of its history and origins. Original and traditional music for the work was composed and arranged by Joe Zeytoonian and La Rosa-Guitarist Paco Tonta. In her review of the piece last May, Herald dance critic Laurie Horn states: \"One could almost imagine that it was the 12th century. This is a flamenco of insinuating hand gesture, of embroidery in the air as fine as the alabaster\n\n[ENDING CONTEXT]\n\nFLAMENCO GUITAR VOL. III Seu logo #1 = 500000 #2 Vindale • Leendings de Heids & Algoe • La opusos de Enconel y Un Pro Buer cas ♦ a 1800 en Rhonda Llanne-er Laig • Unpro. ♦ Un Pro. TRADITIONAL FLAMENCO GUITAR VOL. II Béco de 1000 m² (Rue de Flambert) by Mariano Córdoba * Home Study Courses with Cassettes * Instructions, Exercises, Musical Selections, Techniques * Written in Conventional Music Notations and Tablature $23.00 each plus $2.00 shipping U.S.A / $5.00 outside U.S.A. Send Cashier's Check or Money Order to. Mariana Córdoba 647 E. Garland Terrace Sunnyvale, California 94086, U.S.A.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PRESS RELEASES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "40-43",
    "page_number": 40,
    "word_count": 1489,
    "article_char_count_full": 9167,
    "article_char_count_review": 3593,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "classic"
      }
    ]
  },
  {
    "article_id": "JALEO_1989_03::A17",
    "article_text_for_review": "THE MADRID CLOSES The Madrid Restaurant in Newport Beach — gathering place for performers and aficionados from the Orange County, Los Angeles and San Diego areas — closed its doors in late 1988 but the contacts made there survived and are enriching flamenco in the San Diego area. Flamenco guitarist Bruce Paterson and his wife, flamenco dancer Jaclisa have moved to San Diego and can be found performing at the Tablao Flamenco and in Tijuana. Other performers such as dancers Lourdes Rodriguez, Juana Escobar, Anna and singer Antonio Sanchez are commuting to our area on a regular basis to perform on both sides of the border. *** Lunch at Diego's in Pacific Beach. (R to L) David DeAlva & Rosa DeAlva, Dominico Caro & José Molina (barly visible in shadows). Members of the José Molina company. Guitarists José María Moreno & Carlos Rubio on right The program consisted of a mixture of orchestrated and flamenco pieces and was colorfully costumed. Other members of the company were: guitarists José María Moreno and Carlos Rubio, singer Dominico Caro and dancers Esther Suarez, Susana Webb and Anna Mercedes. Through this and similar programs (locally Paco Sevilla, Juanita Franco and Marysol Fuentes also present educational flamenco programs) we can anticipate a new generation of flamenco aficionados. —Juana DeAlvaa José Molina 1981 NORTH AMERICAN FLAMENCO DIRECTORY – COLLECTORS ITEM – STILL AVAILABLE FOR $8.50 IN THE U.S. A TRIBUTE TO A FLAMENCO DANCER, A LOVING VOLUME OF PIOTOGRAPIIS, POEMS AND ANECDOTES DEDICATED TO THE LATE DANCER ROBERTD LORCA AND TO THE TRADITIONAL SPANISH ART OF FLAMENCO DANCING, HAS BEEN COMPILED BY LUISITA SEVILLA DE PACHECO, HIS FORMER DANCE PARTNER, PROCEEDS FROM THE SALE OF THE BOOK WILL BE DONATED TO HELP PEOPLE AFFLICTED WITH AIDS. (SEE ADJOINING PAGE.) ORDER NOW! A BOOK OF POETRY, PHOTOGRAPHS AND FACTS ABOUT FLAMENCO This book is a work of love and inspiration in memory of a wonderful dancer, ROBERTO LORCA. It is put together by Flamenco dancers, singers, musicians, photographers and friends. All of whom loved Lorca and Flamenco. They contributed their talent and gifts of love to make this tribute possible. All proceeds from the sale of this book will go to help people afflicted with the disease AIDS. Please send me \"A TRIBUTE TO A FLAMENCO DANCER\" at $19.95 and $5.00 for shipping, handling and postage. Total $24.95. Name City State Zip Make check payable to: BOBBY LORCA FOUNDATION Send to: BOBBY LORCA FOUNDATION 4151 Gate Lane, Baypoint Miami, Florida 33137",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "poem",
    "pages": "44-47",
    "page_number": 44,
    "word_count": 414,
    "article_char_count_full": 2517,
    "article_char_count_review": 2517,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1990_04::A1",
    "article_text_for_review": "The program finale, “Cuadro Flamenco,” is a show in itself. Over an hour long, the entire second part of the program is devoted to the mesmerizing and indefatigable music and dance improvisations of each of the company members. The performers each take a turn — 10 to 15 minutes at a time — displaying their distinctive talents and quirks. In the process, they reveal the discipline it takes to shine in flamenco which requires great rhythmic accurateness and endurance. The dancing begins when di Palma, de Cordoba, Rodarte and Maya Tatiana make a mock fierce entrance with a proud flamenco strut. Although, each presents him or herself with flashes of bravado and clowning, they all get down to the real business: the solo performance. The others join singer Manolo Segura and guitarist Gregory Wolfe for sharp, rousing clapping and singing accompaniment. Segura not only sings with great feeling, but proves an able and comic master of ceremonies. *** Above photo: Zorongo Flamenco troup in the production of Picasso's Guernika. Left to right: Susana diPalma, Antonio Sánchez (singer), Gregory Wolfe (guitarist), Maya Tantiana, Pablo Rodarte, Luis Porcel, (dancers) and Luis Primitivo (guitarist). Photo by Karen Bowers ©1989 important to the success of improvisation. This significant characteristic lends to the constantly developing nature of the 600-year-old art. “People think flamenco — they think it stands still, but it moves,” said dancer Pablo Rodarte after the show. “We don’t do the same thing we did in 1940...thank God.” Of course, flamenco is still flamenco. While the more recent dance forms of ballet, tap, and jazz have been part of the dancers' training, each member of the group has dedicated him or herself to this specific style. The dance, like music, is characterized by a tension that exists between restraint and release in each artist. The calm upper body and hand movements are strikingly similar to classical Indian dance, with the women using every finger as a separate instrument of expression and the man revealing and concealing his palm like a well rehearsed magician. In contrast, the syncopated rhythm created by the zapateados (heel and toe clicking) and hand clapping continuously crescendos to reach an almost frenzied flurry of energy at least once in every dance. At this point, a burst of calm allows for the precariously perched members of the audience to shake out of their trance and push themselves back into their seats. In Spain, this would have been the cue for observers, who are often indistinguishable from performers, to get up and join the action, according to Brian Burt, a student who has seen flamenco in its natural setting. Despite a relatively restrained audience, Zorongo Flamenco made it well worth Laso's efforts to bring the group to Carleton. The performance consisted of two sets of the physically exhausting dance separated by an interlude, during which the musicians were given their turn in the spotlight. Although bright costumes reflected the tone of some of the pieces in the first set, at times the dancers seemed on the verge of bursting into tears or a barrage of fatal curses. After they had done enough stomping on the Concert Hall's wooden floor to send anyone with shinsplints whimpering to the lobby with empathetic pain, the dancer's and the singer exited. Wolfe's solo then kept the audience enthralled for at least ten minutes straight of the inhumanity fast and furious flamenco style guitar. After a resounding ovation, Segura joined Wolfe to perform a song about Granada, one of the most beautiful cities in the world according to the singer, because of the mixture of Christian, Jewish, and Arab culture. At times, Segura's voice was reminiscent of song used to call Muslims to prayer, and during the entire piece he seemed possessed by the music. The second set of dance began with La Madre de Flamenco (The Mother of Flamenco), an extremely difficult and long piece performed by Susana di Palma, the group's artistic director. Wearing a black dress, the ruffles of which trailed several feet behind her, di Palma actually began seated in a chair. The energy in her balletic arm movements steadily grew until she rose from the seat to continue the crescendo of energy with her entire body, even incorporating the trailing ruffles at times with a deft swoop of her legs. All the while, of course, the sound of guitar, voice, and clapping from the four others performers developed steadily as well. One of the last numbers, a solo by Rodarte, seemed to capture the playful spirit that often appeared fleetingly in other pieces during the night. Before Rodarte began his performance of what might be the human equivalent of a male peacock's mating strut, Segura told the audience \"This one's more funky — so enjoy yourselves.\" Needless to say, we did *** FLAMENCO GUITAR, DANCERS COME TO SHELDON [from: St. Louis Post-Dispatch; April 24, 1989] by Kevin Eckstrom In our society, which is largely organized by numbers, technology, and litigation, the expression of genuine passion is all too often forced into pathological modes of behavior. But in those rare instances when real passion informs artistic creativity, the result is very exciting, redeeming us",
    "title": "ZORONGO FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 862,
    "article_char_count_full": 5244,
    "article_char_count_review": 5244,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1990_04::A2",
    "article_text_for_review": "WHERE'S MY JALEO? Where's my Jaleo, you ask? Depending on when this issue appears, it will be 3-5 months late. Obviously we cannot continue this way. Our problems are the same as they have always been, but are more severe now. Basically, the magazine has become a one person operation, where it was once produced by a large staff who handled the editing, typing, layout, printing, subscriptions, advertising, photos, correspondence, back issues, accounting, labels, assembly, and mailing. At present, Paco Sevilla does the editing and Juana De Alva does all the rest — or pays to have it done. Costs have escalated to the point where a subscriber's money is used to pay for a single issue, with nothing left for the rest on his subscription. Thus, we end up waiting longer and longer to gather enough money from renewals and new subscriptions to publish each issue. We have decided, therefore, that the time has come to end our participation in the publishing of Jaleo. This decision is made with great sadness, for Jalea has been an important part of our lives for over 12 years. The magazine has kept us in contact with the world flamenco community and opened many doors for us. Juana has invested in a great deal of computer equipment in order to bring Jaleo up to its present level of quality. But, enough is enough! Before bringing everything to a halt, we would like to make a proposal to our readers. If we now could gather a group of volunteers like those who started Jaleo, the magazine could probably be put on a sound financial basis. We wonder, therefore, if there might be a group of aficionados out there who would like to take over Jalea and make it work. The potential is there. What we have lacked has been the time to investigate sources of funds — advertising, grants, sponsors, etc. A magazine cannot survive an subscriptions alone. It might be possible to get one of the high Spanish sherry companies — Domecq, Sandeman, or Harvey's — to underwrite the cost of Jaleo. If there is a group that would like to take over Jalea, Juana and I would be willing to help get it started. The magazine could be much smaller and simpler than it is now. If it come out every two months, it could become a useful advertising and promotional vehicle once more. Jaleo writes itself—readers send in all materials. Some of our readers probably picture Jaleo as a typical magazine, with an office and staff. Thus as there is no staff, there is no office. All work is done in the home, on the living room floor, by people with no particular specialized skills. Anyone can do it. So give it some thought. It is a highly rewarding activity that places you at center stage in the flamenco world and offers unique opportunities. If you think you might be interested, please contact Juana De Alva at 440-5279. It has been a wonderful twelve years for us and only the sad realization that we are not being fair to our readers could bring us to the decision to stop publishing. We fervently hope that Jaleo will continue. Each of you who decides not to become involved will understand why we cannot continue. --Paca Sevilla",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 553,
    "article_char_count_full": 3116,
    "article_char_count_review": 3116,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1990_04::A3",
    "article_text_for_review": "Writer Wants More Guitar Dear Jaleo, My name is Stephen A. Richards and I'm very interested in flamenco guitar. I play classical guitar, and this year I will be preforming in the Pepe Romero masterclass in Tampa and Tampa University. I've studied flamenco with Dr. Mark Switzer and went through the Juan Martín Volume I, although the bulerías is still giving me difficulty. I would appreciate any information as to the availability of QUALITY flamenco guitar instruction in my area. I'm interested in any correspondence or information of methods and documented transcriptions of technique and rhythmic functions of flamenco guitar. Sincerely, a troubled gringo, Stephen A. Richards Tampa, FL [Editor: We invite our readers' responses to Stephen's inquiry. Letters will be forwarded to Stephen and published where appropriate.] 林林林 Impressed With JALEO Dear Editor, I'm very impressed with the magazine. The writing is surprisingly poetic. The photography captures intensity! Jaleo has a tremendous sense of diversity and balance, to which is adds a meticulous sense of historical accuracy. I have never seen all of these qualities so well combined in a single magazine. Thanks for the valuable contribution of your time so that others may enjoy the profound beauty of flamenco! Sincerely, Paul Tnijillo Peralta, NM 取取 1349 FRANKLIN AVE BELLINGHAM, WA 98225 Ph. (206) 676-1864 12TH \"ALL FLAMENCO\" WORKSHOP. CONCERT CELEBRATION & FIESTA OF FLAMENCO \"SPIRIT\" August 6th through 18th 1990 WRITE OR CALL FOR BROCHURE",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 237,
    "article_char_count_full": 1511,
    "article_char_count_review": 1511,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
