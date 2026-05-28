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
    "article_id": "JALEO_1983_07::A14",
    "article_text_for_review": "We struggled with them during our private lessons while Luisa played the appropriate music on the guitar and did the steps with us if necessary. Every Tuesday she gave a group class where, among other things, the whole school could review all the exercises, ten times on each side or twenty times if the step changed feet. By the end of our second year, we were supposed to have mastered all the patterns, and eventually to know all of them by number. So, when we wrote down the dances, many patterns could be noted with just a number. The initial sequence of the main zapateado of the alegrías, after the llamada por castellana, for example, looks like this: <table><tr><td>#13</td><td>I</td><td>D</td></tr><tr><td>#14</td><td>I</td><td>D</td></tr><tr><td>#15</td><td>I</td><td>D</td></tr><tr><td>#16</td><td>I</td><td>D</td></tr></table> The zapateado exercises were not a complete catalogue of all the common patterns. The remate de taco, for example, is not there. But learning them all gave us a core of footwork on which to build, and by noting down the dances we learned to analyze new combinations. Most of us have dances, learned early on, such as the sevillanas or the seguidillas manchegas that we will probably never forget, but generally choreographies are easily lost. By the time a dancer has twenty-five, thirty or more dances under his belt, he can't practice them all very regularly, especially if he is supporting his dance habit with a real world job. The main purpose of the notation system was, I think, to allow us to resuscitate dances. I don't think it was supposed to be a substitute for learning dances correctly and well at the time they were being taught -- but it can almost be one. The November 1982 Jaleo ran an article I wrote on Jose De Udaeta's Sitges course which I attended. What I didn't say was that I spent the first four days of the course taking the Bar Exam in Honolulu. I was six days late arriving in Sitges. I was hopelessly behind in three classes. Five of the choreographies were new to me. The sixth, Jaleo de Jerez, I had studied before, but the notation had gotten lost between Buenos Aires and Honolulu. Other students helped me unravel what I had missed and I can't overemphasize their generosity. But I never felt caught up in everything while I was in Spain, and I had expected that I wouldn't. I decided to attend the course, arriving so late, because I was fairly confident (probably overconfident since I had not counted on needing -- much less getting -- so much help) that I could make an accurate notation to study from once I was back home and free to practice, which turned out to be several weeks after the end of the course.",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "20",
    "page_number": 20,
    "word_count": 455,
    "article_char_count_full": 2689,
    "article_char_count_review": 2689,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A16",
    "article_text_for_review": "[from: La Prensa, 1942; sent by Laura Moya; translated by Paco Sevilla] by Juan Martinez The artists' agents to which we alluded in the last chronicle -- if I can call them that, and I refer to those who were in Madrid and Barcelona in that period -- in combination with the owners of the establishments that we have named elsewhere, and some of the new dance teachers, were all in charge of discovering new and future victimiz of that lucrative, but immoral art that could have reached such a level of public development only in Spain. These agents were specialists in these things and established a system in which they only gave work to those girls who were studying in the academies where they had a special interest; this did a great deal of harm to the other teachers who did not have the same point of view with reference to the art, in fact, to the contrary, they wanted to protect it from the catastrophic and hopeless end that each day made itself known with greater strength. \"How long can this ga on?\" asked the Spanish dancers among themselves. What happened to the other dancers? I don't mean to imply that there weren't still some places where the good artists could perform, such as theaterz, clubz, cabarets, and same café cantantes, but there weren't enough to give a living to the many artists of the cante and baile who were available. Many of the theaters and movie houses had variety shows only on Saturdays and Sundays, or, where they had shows every day, they paid very little. All of the business went to the others -- the frivolous variety shows. I also have little doubt that an infinite number of dancers left Spain for foreign countries to look for a more favorable environment for their art -- among others, \"La Argentina\" and \"La Argentinita,\" who were absent from the theaters for a number of years. The public, for its part, had a preference for the beauty of the artist. (In that period there still existed no concerts of Spanish dance in Spain.) Providing expert guitar accompaniment were Benito Palacios and Marcos Carmona. The performance was fast-paced and exciting throughout. The performers sensed from the start that the audience was with them and the rapport was complete. It proved too difficult for some to hold their applause until a dancer paused. This enthusiasm continued until the troupe left the floor to a standing ovation and a shower of dollar bills (a very desirable Greek custom of showing appreciation). * * * REPORT FROM VANCOUVER by Mary Robertson Vancouver, B.C.'s Angel Monzon, longstanding performer, teacher and choreographer, has joined the faculty of the Goh Ballet Academy. The Academy held a recital on June 5th at the Northshore Centennial Theatre. The varied ethnic population in Vancouver makes for a diverse and productive dance climate -- this was especially evident in the Goh recital. Faculty members trained in Russia, China, England (and in Angel's case, of course, in Spain and Argentina) were able to pass on to their students a great purity of classical line. This was evident in fluid arm movements -- a great plus for Angel's choreography of a Zambra from Ben Amor performed to the music of Pablo Luna. Among the principals was Gabriel Monzón who showed great depth of understanding in his interpretation. Gabriel's dancing has matured beyond his tremendous technical ability, particularly apparent in his solo performance of a selection from The Three-Cornered Hat. * * * ANA MARTINEZ'S FLAMENCO [from: The Washington Post, May 18, 1982; submitted by John Fowler] by George Jackson The foundation of flamenco dancing is in the footwork, of course, but Ana Martínez builds her choreography using -- with discretion -- the entire body. Sunday night at Lisner Auditorium, she was surrounded by two singers, two guitarists, and two other dancers -- but it was her technique, presence and taste, and not her privileged position as the only woman on stage, that made her the star. To witness Martinez's skill in stamping and tapping, one must use both the eyes and the ears. She makes profound music with her feet. It is rhythmically subtle, has an incredibly wide range in volume, and is tonally pure. The motions that produce this sound are eminently clear, even at their most rapid. In the opening \"Soleá,\" she sailed onto the stage, bosom forward like a prow. In the \"Alegrias,\" the mantilla with which she toyed as if it were a bullfighter's cape and in which she wrapped herself emphasized the suppleness of the shoulders and the proud arch of her back. Throughout the repertory, the controlled action of Martínez's wrists gave her arms and hands elegance and power. Flamenco gowns, with their ruffles and long trains, are splendid creations but hide the dancer's legs and, unless lifted, also the feet. In order to show her high-step stamping and an added ronde de jambe, Martínez in the \"Garrotín\" wore a white pants suit with matching cap and put on tomboy airs. On occasion, she even danced with tresses loosened and swinging. Except for a passing smile, her manner was reserved until nearly the end of most dances. In the final flourishes, though, Martínez's temperament was allowed to surface. With such fine talents as guitarists, Małaga and José Antonio, and singers Nino de Brenes and Manplo Leiva, the use of recorded scores for some of the dances seemed unnecessary. Also, the orchestral sound overwhelmed the stamping and tapping. Manclo Rivera's dancing had ample speed and fluidity, but there was a bit of nightclub in his manner. Roberto Lorca's elbows-and-chest style of moving seemed brutal. The most unusual and haunting item on the program was the \"Carcelera-Martinete,\" in which the two singers called to Martinez from opposite sides of a darkened stage and she, barely visible, replied with the music of her feet.",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 985,
    "article_char_count_full": 5815,
    "article_char_count_review": 5815,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A17",
    "article_text_for_review": "COUNTER CLOCKWISE FROM ABOVE: 1 ERIC & YRMA HORTA L DANCING YORGO GRECIA & CAROLYN BERSER ACCOMPANIED BY BRUCE PATTERSON & MARCOS CARMONA 3 SAN DIEGO PARTICIPANTS - PILAR MORENO, CHARO BOTELLO SINGING, MICHELLE BOTELLO DANCING 4 GUITARIST BRUCE PATTERSON, DANCING - YORGE DE VALLE & CORAL CITRON, PALMAS - ARLENE SAPER & YVETTA WILLIAMS 5 GUITARISTS ROY MENDEZ LOPEZ, DAVID DE ALVA, PALMAS RAUL DE ALVA CLOCKWISE FROM ABOVE: 1 DANCING JOY PADILLA & ESTELA ALARCON GUITARISTS - CARLOS PRICE & DENNIS HANNON 2 PILAR MORENO SINGING TO GUITAR AND PALMAS ACCOMPANIMENT 3 YORGO SINGS FOR KATINA VRINOS 4 YORGO SINGS FOR IRENE HEREDIA 5 BILL FREEMAN, BRUCE PATTERSON, CORAL CITRON, RUDY MONTOYA & ARLENE SAPER",
    "title": "LOS ANGELES JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "23-25",
    "page_number": 23,
    "word_count": 117,
    "article_char_count_full": 702,
    "article_char_count_review": 702,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A18",
    "article_text_for_review": "Mystery photos from a San Francisco Renaissance Faire, north of San Francisco in the 1970s. Who are the performers? (We can identify at least one, but will let our readers in the Bay Area give us the details; sent by H. E. Huttiq). The famous pianist, Arthur Rubinstein, wrote about his visit to the gypsy caves in Granada in about 1915. \"The two guitarists, who looked like smugglers in $ \\underline{\\text{Carmen}} $, began to strum their instruments. They delighted me by their strong rhythm and the fine sonority they produced. One of them stopped playing from time to time and sang some strange coloratura cadenzas which I later learned to know as the genuine flamenco $ \\underline{\\text{canto jondo.}} $\" *Arthur Rubinstein, My Young Years*, Alfred A. Knopf, N.Y., p. 458.",
    "title": "MYSTERY PHOTOS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 26,
    "word_count": 130,
    "article_char_count_full": 777,
    "article_char_count_review": 777,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A19",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLETTER FROM OHIO Dear Jaleo, I'm writing this thank-you letter to the editors y sus amigos flamencos -- ;todos! -- on a plane bound for Dallas, armed with a pile of freshly-recorded tapes, rolls of film to be developed, and an array of happy memories of my visit to San Diego. It was my most enjoyable visit anywhere! The week was all too short -- but efficiently jammed full with superb flamenco and beautiful, down-to-earth people. I choose my occasional professional meetings with great care and was not exactly heartbroken when I found out that one was slated for San Diego. Raúl and Charo Botello and Gary and Marysol West are friends via their stint in Georgia with a former Ohioan, Marta del Cid. Martha warned me of all the constant activity and fun when I told her of my visit, and she was\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"home\"]\n\npeople. I choose my occasional professional meetings with great care and was not exactly heartbroken when I found out that one was slated for San Diego. Raúl and Charo Botello and Gary and Marysol West are friends via their stint in Georgia with a former Ohioan, Marta del Cid. Martha warned me of all the constant activity and fun when I told her of my visit, and she was not kidding! Raúl, Charo and their daughters Susie and Michelle opened their home to me; it was a delight to be able to stay with them, and share in the continual compás and warmth that are integral to their existence. It began immediately after they picked me up at the airport, with a stop in Old Town to watch Paco Sevilla, Rayna and her group (including Michelle) perform. After an unintentionally long surrender to jet lag that evening, I awoke to find it had started again...with Yuris, Paco, Pilar, Rodrigo, Remedios, Herb, Juana and more. So much happened in a single week that I find it difficult to remember who all came by on which day! These beginnings set the stage for the rest of my stay. My meetings ran until after 9 p.m. on Monday only, but Marysol had just returned from visiting Gary (on duty in the Philippines) that evening, such that her return was celebrated with more flamenco. Included in the delightful flurry of the week were visits to more performances and a rehearsal, to Yuris' Blue Guitar workshop, to Rosa's phenomenal Chilean restaur\n\n[ENDING CONTEXT]\n\nTeo Morca - Flamenco Workshops Tom Sandler - The Frame Station Peter Evans - Big Sur Juerga G. Kruells - Flamenco Etchings AIR + BUS + STEAMSHIP + AAN + DOMESTIC AND WORLD TOURS 426-6800 Specializing in Spain REYNOLDS S. HERIDT OWNER - MANAGER BEGINNING FLAMENCO GUITAR COURSE AT UCSD Pac- Sevilla will teach a beginning flamenco guitar and general flamenco survey class through the UCSD Extension program. The course will be held on Tuesdays (7-10:00 PM) beginning on September 26 and running to December 10. Interested parties should call the UCSD Extension number, 452-3430, for a free catalogue.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "27-29",
    "page_number": 27,
    "word_count": 1631,
    "article_char_count_full": 9487,
    "article_char_count_review": 3066,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "home"
      }
    ]
  }
]
```
