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
    "article_id": "JALEO_1989_03::A10",
    "article_text_for_review": "BACK TO BASICS: THE INNER AND OUTER DANCE OF FLAMENCO I have often been asked if I still get nervous before a performance, going on stage or before a class or lecture. The answer is always, \"yes\". I do not get nervous in the sense of frightened or scared but in the sense of nervous energy to \"do it right\"... to express with truth and integrity what I feel inside. I want to be sure that this inner truth and feeling and the real inner me will be expressed outwardly in my dance, in my thoughts in lecture, in my classes while I teach others. This, to me, is one of the great responsibilities of a teacher. This article relates to these thoughts: to the importance of the basic \"roots\" of technique, to the foundation and building blocks of movement that reflect our inner feeling and what it is that makes steps or movements into dances — choreographic flamenco expressions of our inner flamenco feelings. “The inner dance” cannot be reached without first creating the “way”—the technique-path of the outer dance. It is the inner dance, the inner truth, which helps guide and develop the outer technique to express the inner art, the inner essence of the dance and the dancer. It is this ping-pong, back and forth blending of the inner and outer person that brings about the “whole”—the truth—the flamenco that says something. Flamenco continues to evolve within its tradition. More and more, it is leaving the tradition of the small, intimate confined tablaos and colmaos and going onto the theater or the festival stage. More and more teachers are teaching technique along with the traditional way of teaching flamenco by dances or \"routines\". More and more dance companies are re-appearing and touring the world. There seems to be a resurgence of the Fifties and Sixties as far as the popularity of flamenco and Spanish dance is concerned. Some of the structure of the dance companies is creating a very definite discipline of training in the flamenco dancer as well as the other forms of Spanish dance. For example there are regular classes that the members of companies must take — not just for choreography, but for technique, much like a ballet company. Recently, while in Spain, my wife, my son and I were invited to classes and rehearsals of the Ballet National and the structuring and discipline was every bit as complete as a classical ballet company such as the American Ballet Theater. What this means is that the serious flamenco artist or student is faced with a discipline a bit different than years gone by when one could depend on work if one could do one or two dances well with gracia and arte. I am not talking about quantity — or that one has to have a large and varied repertoire. I am saying that the approach to that repertoire — whatever it is — has become more structured and focused. Even the hard core traditionalist is seeking to expand and grow and not just be happy with his or her “natural talent”. It is a very natural habit and a necessity for ballet dancers such as Baryshnikov, Makarova and other serious ballet dancers to take daily — or even twice daily — technique classes besides working on choreographies and performing. They are obviously polishing and developing their techniques, their bodies, their musicality, their strength and their artistry so that when they enter into choreography they will have their \"act together\" to express whatever they want to say in the dance. Their outer dance and inner dance are ready to work together. It is through this seemingly endless repetition of the basics — with that primary focus in mind — that the path to the inner dance is found.",
    "title": "MORCA: SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 634,
    "article_char_count_full": 3624,
    "article_char_count_review": 3624,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1989_03::A11",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSara de Louis dancing at Teo Morca Workshop choreography to memorize. The other dance was a medium fast bulerías or, rather, a group of bulería, in which Teo would frequently mix up the order in which they were to be performed. This was a prelude to one session on improvisation. Some of the class performed a couple of minutes each of bulerías, using previously unrehearsed routines. On the evening of the first day of classes, Teo conducted a flamenco \"rap session\". Teo defined flamenco terminology; desplante, llamada, escobilla, etc. He explained the development of a flamenco dancer, from understanding the forms and technique, through the development of skills to combine and execute the learned elements, to eventually becoming the dance and performing through feeling and expression, as\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"compás\"]\n\nrsed routines. On the evening of the first day of classes, Teo conducted a flamenco \"rap session\". Teo defined flamenco terminology; desplante, llamada, escobilla, etc. He explained the development of a flamenco dancer, from understanding the forms and technique, through the development of skills to combine and execute the learned elements, to eventually becoming the dance and performing through feeling and expression, as opposed to counting the compás and consciously executing technique. Teo explained the origin of flamenco, the Indian, gypsy and Moorish influence and the relation of the dance to the cante and guitar—the latter a relatively recent addition. We heard some war stories from his forty years of dancing. Was Carmen Amaya really that good? He traced the evolution of the dance from around the campfire to the elaborate Las Vegas-type productions found in the modern big city tablaos. According to Teo, there is valid artistic expression to be foun\n\n[ENDING CONTEXT]\n\nwith Paco de Lucía, Serranito, Manolo Sanlúcar, Camarón, Lebrijano, Enrique Morente, Paco Cepero, Enrique de Melchor, Nínio Miguel, and others. Now it's time for them to give way to the new crop of artists who are in their twenties and thirties. If you are forty years old or more, you've got a problem, let me assure you. Personally I am all for change, but accompanying any change are the problems which must be dealt with and the decisions that must be made. If you are a trend-conscious artist, here is what you must do: A) Have a drummer as part of your dance company or your guitar concert.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "16-18",
    "page_number": 16,
    "word_count": 1793,
    "article_char_count_full": 10408,
    "article_char_count_review": 2589,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "compás"
      }
    ]
  },
  {
    "article_id": "JALEO_1989_03::A13",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFLAMENCO GUITAR CENTER-STAGE Jaleo had two guitarist covers for their recent issues: Joaquín Amador, top rate guitarist and rasgueado player, husband of Manuela Carrasco and brother of the great cantaora Susi and the latest issue features Enrique de Melchor (worked with Paco de Lucia, Paco Cepero) in his own right has played for more cantaores then any other guitarist. New York itself had its own guitaristic firsts — Basilio Georges teacher, player, arranger and now the sole musical composer of the recently presented World Premiere by Carlota Santana of Hemingway's \"For Whom the Bells Toll\" (October 13th to 15th at Symphony Space, Broadway New York City). The work was fully orchestrated for two guitars, cello, flute, xylophone, piano etc... Basilio's music is in cassette. After the recent\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"the great\"]\n\nSantana of Hemingway's \"For Whom the Bells Toll\" (October 13th to 15th at Symphony Space, Broadway New York City). The work was fully orchestrated for two guitars, cello, flute, xylophone, piano etc... Basilio's music is in cassette. After the recent Pedro Bacán concert here in New York City, I had Basilio present Pedro with a cassette to take back to Spain... more about Carlota and Basilio follows — the featured guitar in the Hemingway play was the great Pedrito Cortés. The “second” guitaristic first was Greg Wolfe resident of Minneapolis (I knew him in Chicago and on his invitation I attended a three day sleep in flamenco juerga in St. Paul at Great Bear Lake). Greg composed/arranged for Susanna (Hauser) di Palma’s “Gernika” — the Picasso legend for the ballet of Zorongo Gitano. This gigantic work had its New York premiere only last week and included top quality dancing, acting and with the beautiful music — even singing by all the performers. The other guitarist for this “Twin City Venture” was another great performer and friend Luis Primitivo. THE LUCK OF THE DRAW New York City had the luck of the draw — some surprisingly beautiful dance presentations at short notice included first and foremost the José Greco Dance Company [see reviews]... Greco, himself nearly 70 years old delighted audiences with his own stage presentation, in a way introductory, to present three of his gifted children to New York City... but there was more to it — a superb efficient technical, artistic and musical co-ordination so often lacking in other companies. José Antonio, Greco's eldest son, was the musical director with some phenomenal orchestrations, solo guitar and the recorded voice of Pansequito\n\n[ENDING CONTEXT]\n\nunequalled by any other dancer in the USA. Ten years ago to the date she was interviewed and later performed with the greatest of the gypsy dancers, La Singla in a gypsy tablao north of Barcelona. Her guitarist then was Marote, who recently visited New York City with the Ballet Nacional. Yes, Susana showed us el baile gitanizada with all its nuances and had the added classical school of Merché Esmeralda, Enrique el Cojo and her other teachers added. Pablo Rodárite portrayed Bull the Picasso character role, spelling out fascistic death. Caballo was danced by Luis Porcel with interesting\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "article",
    "pages": "27-29",
    "page_number": 27,
    "word_count": 1211,
    "article_char_count_full": 7209,
    "article_char_count_review": 3333,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "the great"
      }
    ]
  },
  {
    "article_id": "JALEO_1989_03::A14",
    "article_text_for_review": "Basilio Georges is a guitarist and composer who has lived in New York City the past fourteen years. His most recent accomplishment was to write and perform \"Guerra, Amor y Campanas\" a half-hour flamenco ballet orchestrated for two guitars, and a chamber ensemble. This music was used by Spanish Dance Arts for its October 1988 production of \"For Whom the Bell Tolls\" by Ernest Hemingway. Basilio has been involved in flamenco for ten years, but came to it in a very round about way. His musical education started in Milwaukee, Wisconsin at the age of seven when his parents sent him to study classical piano. Although he played fairly well, his interest in music did not take place until four years later when he heard the Beatles and took up the guitar. Except for sporadic classical and bossa nova lessons, his study of the guitar was mainly by ear throughout high school. He went through many styles of rock while playing guitar and organ, and singing in his own groups. By his last year of high school he became involved in stage band and took a harmony/theory course. He met other students who played instruments not associated with rock and was inspired to begin composing. This involved having to relearn how to read and write music. Enrolling at the University of Wisconsin-Madison as a B.A., his interest gravitated to jazz, African music, and other ethnic musics. At this point his only contact with flamenco had been as a child when he saw Carlos Montoya. He also owned one album by Montoya and had heard one by Sabicas. Flamenco guitar was intriguing because of the technique, and the ability to sound as complete as a pianist. Finishing at UW-M with a B.A. in Ethnomusicology he moved to New York City to pursue jazz. He arrived in New York as a guitarist, but had just purchased an upright bass on a whim. After a year or so of playing and composing for workshop groups, friends suggested he would work more as a JALEO - VOLUME X, No. 4 Aurora Reyes rigor of all toques and accompanied such locals as Marcelino Sánchez, Antonio Benamargo, and Juanele de Jerez. As in any art form, a career is filled with highs and lows. His work experience as a flamenco was often frustrating in the sense that during low periods he always felt a tremendous struggle to be catching up to people who had never been involved seriously with anything but flamenco. The deeper he got into flamenco the more he left his other musical experience and talents on a back burner. Returning to New York City in the fall of 1986, Basilio renewed a relationship with Carlota Santana as guitarist for Spanish Dance Arts. The S.D.A. program of 1987 featured live music to both Classical and Flamenco segments. This gave him the opportunity to arrange guitar parts for De Falla's \"Andaluza\" and Sarasate's \"Zapateado\", which were performed with the Alborada Latina chamber ensemble. They also did Albeniz's \"Leyenda\" and Manolo Sanlucar's \"Alfarero\". For the flamenco segment Basilio was able to collaborate with Pedro Cortes Jr. and La Conja. Basilio's work on the score for the Hemingway piece began in December 1987. It involved dusting off his writing talents which had been dormant for almost five years. The result turned into a very rewarding collaboration with Pedro Cortes Jr., the Alborada Latina chamber ensemble, Luis Montero, and Carlota Santana. Aurora Reyes Aurora Reyes is a dancer/singer who lives in New York City with her guitarist husband, Basilio Georges. She was born in Brooklyn and raised on Long Island. Her mother's family were Gallegos and her father's family were Valencianos. Aurora began studying flamenco at the age of 22. Although she didn't find her true forte until beginning to dance flamenco, she was involved with music and dance throughout her teen years. She sang folk songs accompanying herself on guitar at community centers and later JANE LUSCOMBE Jane Luscombe has been associated with Spanish dance for more than twenty years. She first saw the company of Manuela Vargas in London in 1962 and then studied with Elsa Brunelleschi there for several years. With Elsa's help, Jane worked with the companies of Rafael and Manolita Aguilar in France and nearby countries and also with Rafael de Sevilla, who was based in England and toured English cities extensively. On return to New Zealand in 1972 with flamenco singer El Niño León (from La Linea), they performed in their own Spanish restaurant 'Costa Brava' for several years while Jane also established a dance school. This developed into a performing company, 'Spanish Fiesta Dancers', some members have studied with Jane for more than eleven years. In 1982, Jane met Teo Morca and his wife when they were in New Zealand and, as a result, became a subscriber to Jaleo and attended Teo's annual summer flamenco course that August in Bellingham, Washington. She returned in subsequent years and in 1985, received an Arts Council grant from the New Zealand government, which enabled study time in Spain, at workshops in Cordoba and Jerez. Since her return, she came in contact with Marina Grut of the Spanish Dance Society, who is based at George Washington University, Washington DC. Marina suggested coming to do the Society's course and learn the Junior Syllabus, so for three consecutive summers, Jane has been to Washington and has now covered the entire Syllabi—six junior grades and three senior. She is now the Society's New Zealand representative and has been passing on her knowledge to ballet teachers in other New Zealand centers to enable them to teach children in their areas and, thus, establish the society there. There has been great interest and approval of the Syllabus. It offers a constructive approach to teaching basic technique of Spanish dance, plus an amazing selection of dances in flamenco, regional, and classical styles.",
    "title": "PROFILES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "30-32",
    "page_number": 30,
    "word_count": 981,
    "article_char_count_full": 5823,
    "article_char_count_review": 5823,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1989_03::A15",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPost-Franco Fiesta Reigns from Spain [from: $ \\underline{\\text{The New York Post}} $, Wednesday, July 3, 1988; sent by George Ryss] by Clive Barnes Spanish dance with a difference arrive at the Metropolitan Opera House on Monday night for a week's season in the form of the Royal National Spanish Ballet, which offers flamenco-flavored dance with an unexpectedly institutional look. The present company isn't by any count, more than a decade old, and really dates back just five years, with the merger of two earlier companies, when it was placed under the direction of veteran Maria de Avilia. Since 1986 its director and principal dancer has been thirty-seven year old José Antonio. Merche Esmeralda of the Ballet National de España. (photo - Paco Ruiz) Guitarist Manolo Sanlucar who composed and\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"classic\"]\n\ntwo earlier companies, when it was placed under the direction of veteran Maria de Avilia. Since 1986 its director and principal dancer has been thirty-seven year old José Antonio. Merche Esmeralda of the Ballet National de España. (photo - Paco Ruiz) Guitarist Manolo Sanlucar who composed and played the music for the new theater-flamenco work Medea for the Ballet National. (photo by Karen Bowers) An attempt had been made in 1978 to start both a classical and ethnic Spanish ballet, with the ethnic side — of which this present company is the ultimate heir — under first Antonio Gades and then Antonio. The present troupe of some 50 dancers is not like any Spanish group we have ever had before. Perhaps the nearest to it has been the more elaborate manifestations of the Antonio company — in the days when it did things like Antonio's own version of \"The Three-Cornered Hat\" — or the Gades troupe, with its production of \"Carmen\" and \"Blood Wedding\". Certainly forget the kind of gypsy-flamenco abandon of Carmen Amaya, the rarefied taste of Pilar Lopez, or the artistic imagination of Luisillo. This new venture is deliberately aimed at being Establishment Spain — an image of Spain's new post-Franco respectability in Europe, and promoting, with vigor but good taste, the suggestion of a Royal tradition that is in fact non-existent. The resulting product — which I enjoyed far more than I expected — is short on what the old Gitano dancers called “duende,” the “inspiration,” but strong on technique, entertainment and impeccable stylishness, rather than style. Ever since 1921, when Serge Diaghilev introduced a “Cuadro Flamenco” into one of his company programs, classic ballet has had a love affair with Spanish dance, while treating it as an underprivileged poor relation. And this Royal Spanish Ballet is an attempt to change all that — but Madrid, no more than Rome, cannot be built in a day. The Met program is odd, but a serviceable, and likeable, introduction to the company's merits and virtues. Although oddly planned — the flamenco fiesta comes in the middle, not at the end — it works well. If the Spanish have no ancient tradition of a subsidized dance culture — any more than do we or the British — they have something the English-speaking world really lacks, a living heritage of vernacular dance. formances from Esmeralda and a rather sedate Antonio, never quite add up. Abandon seems to have been abandoned. Nor do the handclaps (the palmas) have quite the right dryness, or the finger clicking (the pitos) sufficient pistol-crack sharpness. The spirit never takes fire. Amaya could have eaten the lot of them before breakfast and still had room for cornflakes, yet in fairness Amaya-style dance is not what these Royal Span\n\n[ENDING CONTEXT]\n\na harsh, masculine voice from his instrument, and Gerardo Alcala, who gave his guitar a more lyrical, rolling sound. Offering sung comments in her deep-based flamenco style was the handsome, voluptuous Rubina Carmona, who did some stately dancing using marvelous hand movements with Morca in the encore. Her gestures, like Morca's, again remind the viewer how gypsy dancing is not very far removed from the of the Middle East and, further back in time, India. If Morca is Astaire, Carmona could be his Ginger Rogers — although this pair is earlier than that Silver Screen duo every dreamed of being!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "poem",
    "pages": "33-39",
    "page_number": 33,
    "word_count": 3179,
    "article_char_count_full": 19222,
    "article_char_count_review": 4378,
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
  }
]
```
