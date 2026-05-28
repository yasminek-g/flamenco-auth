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
    "article_id": "JALEO_1979_06::A14",
    "article_text_for_review": "After a successful four month performance, Chateau Sevilla Restaurant said goodby to \"Flamenco Fiesta\" with bailarin/cantaor, Ernesto Hernandez, bailarina, La Chiquitina, and guitarrista, Miguelito. This trio will be joining Miguel Herrero, cantaor of Cuban fame, when his new restaurant, El Cid, opens in mid-June on Le Jeune Road N.W. near W. Flagler Street. Ernesto, who hails from \"The Spaghetti Factory\" in San Francisco is \"in-between\" jobs, while Miguelito and Chiquetina are performing at The Health Nut Restaurant. Pepe Bronce and his company, \"Los de Oro\" (La Chiquetina, Rosa Elvys, bailarinas; Manolo, cantaor; Miguelito, guitarrista), the only active professional Spanish dance company here in Miami, is hoping for a four week contract at a hotel near Disney-world. They perform throughout the year at various places in Florida, including the famous Les Violins Supper Club. In December, they will push off for a South American tour, including a few weeks at the Tekendama Hotel in Bogota. The bailarin, Pepe Bronce from Argentina, studied many years with the Pericets and toured extensively throughout South America. He has mastered not only the flamenco style, but the Spanish classical and regional as well. Among Miami's steady teachers, we are lucky to have Luisita Sevilla. She is very active with her classes and has imparted her knowledge and spirit to an inestimable number of people. Luckily, she performs now and then at fiestas and ferias in Florida. José Molina resides in Miami for several months throughout the year; he gives beginning and master classes and choreographs for professionals. Roberto Lorca also gives classes occasionally and choreographs. Both Molina and Lorca teach at Luisita's studio in Miami. Rosita Segovia, Antonio's ex-partner, has also been very successful with her classes at the Conchita Espinosa Academy. The Centro Español Restaurant is presenting \"Los Chavales de España\" with dancers Orlando Romero and Micaela. Orlando, from Argentina, is an excellent bailarin and was a member of \"Los Duendes Gitanos\" years ago. El Baturro Restaurant features bailaora Carmen de Córdoba, cantaor/bailaor Cacharrito de Málaga, and Manolo Vargas, a \"hot\" guitarist from Sevilla. The Flamenco Supper Club leaves much to be desired. The only thing flamenco about the place is its name. Now and then they feature some good dancers, but rarely do the divert from their big gaudy reviews with flashy costumes and very little dancing. To each his own... Other flamenco artists that Miami boasts of at the present are: cantaor, Carlos Madrid; guitarists, Chucho Vidal, Miguel Mesa, Monty; bailaoras, Carmelita, Adela Vergara; bailaor/cantaor/guitarrista, Miguel Herrero; bailarinas, Cecilia López, Cecilia Núnez, Clarita Figuroa, Margarita; bailarín, Dario. ANNOUNCEMENTS Announcements are free of charge and will be placed for two months; they must be received by us by the 15th of the month previous to their appearance, earlier if possible. Send to: JALEO, P.O. BOX 4706, SAN DIEGO, CA. 92104.",
    "title": "MIAMI FLAMENCO SCENE IN MAY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_06",
    "year": 1979,
    "language": "en",
    "article_type": "poem",
    "pages": "25, 26",
    "page_number": 25,
    "word_count": 467,
    "article_char_count_full": 3031,
    "article_char_count_review": 3031,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_07::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nBY BROOK ZERN (This article, from $ \\underline{\\text{the village Voice}} $, June 14,1976, is particularly relevant at the present time since Agujetas is again appearing in New York -- see announcements. We thank Roberto Reyes for sending this.) One of the world's great singers -- an acknowledged master of a staggeringly difficult and demanding tradition -- is working unnoticed in New York. Or maybe he isn't in New York. He is a gypsy, and the fact that he was here last week doesn't mean he will be here next week, or even this week. But he says he will be around for a while, and that is a good omen. He is called Agujetas, and he sings flamenco. Specifically, he sings the kind of flamenco called cante jondo, or deep song -- music of such shattering intensity that those who really dominate\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"woman\"]\n\nhere next week, or even this week. But he says he will be around for a while, and that is a good omen. He is called Agujetas, and he sings flamenco. Specifically, he sings the kind of flamenco called cante jondo, or deep song -- music of such shattering intensity that those who really dominate it can be counted on the fingers of one hand. It has been a dozen years since New Yorkers last had the chance to encounter this style of singing -- when a woman known as La Ferdanda de Utrera sang at the World's Fair. But then circumstances were abominable. The formal setting, the transient and unsophisticated audience, the absurd scheduling (flamenco matinees yet), and La Fernanda's innate tendency to freeze up when appearing off her own turf made the engagement forgettable at best. Things are better this time, but that doesn't mean you can hear good cante jondo on a predictable timetable. Agujetas sings in a small restaurant called La Sangria, at 569 Hudson Street on the corner of llth St. He does perhaps three short and spread-out sets each night from Wednesday through Sunday starting at about 10:30 and finishing late, usually around 2:30 a.m. The songs are rendered while Agujeta's wife, an extraordinarily good dancer named Tibulina, pounds out the rhythms. (Actually, the footwork is the easy part; she also dances well with her arms and upper body, in accordance with the canons of female flamenco baile). always the central component in an authentic flamenco session, is subordinated to what in Spain is an intermittent and always secondary element. So hearing Agujetas really open up and sing, just him and a guitar accompanist, is a matter of luck or sheer tenacity. The first night I went early, hun\n\n[ENDING CONTEXT]\n\n-- black pants, bolero jacket. It should be noted that in most of the regional dances of Spain, women are indeed free to dance with the fullest expression of joy and vigor in movement.\" ABOUT BROOK ZERN: Manuel Amaya Cortés Heredia \"El Morucho\" is the youngest of that clan's eleven children. Born to a life of freedom, sleeping beneath the stars, he rapidly acquired great fame among his people for his prodigious mastery of flamenco song, as well as his brilliance on the guitar and his phenomenal dancing. Yet deep within him, there lay an unquiet and questing soul which could not rest content.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A Flamenco Master Sings for His Sangria",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "poem",
    "pages": "1, 24, 25, 26",
    "page_number": 1,
    "word_count": 1553,
    "article_char_count_full": 8942,
    "article_char_count_review": 3342,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "woman"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_07::A2",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDear Jaleo, Allen Yonge tells me Ansonini is returning to Seattle on Friday the thirteenth of July, for a second visit with friends. During his visit, he will be giving a lecture-demonstration, and possibly making a film and recordings, at the University of Washington. Jim Kuhn is doing the organizing, and anyone wanting further information can get it from Allen Yonge at (206) 525-8782. Allen didn't specifically mention a juerga--but you know what happens when Ansonini visits friends! I understand you're planning to publish my letter to Paco about Ansonini's first visit to Seattle, last May, along with the short article I wrote on it. I'll also be looking forward to Gary Hayes's promised article (perhaps with photos?) about that weekend, since he lasted for the entire juerga--and I hope\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"excellent\"]\n\nnow what happens when Ansonini visits friends! I understand you're planning to publish my letter to Paco about Ansonini's first visit to Seattle, last May, along with the short article I wrote on it. I'll also be looking forward to Gary Hayes's promised article (perhaps with photos?) about that weekend, since he lasted for the entire juerga--and I hope he'll also write up the coming visit. I thought his article on Rafael el Aguila (*May 78*) was excellent--do it again, Gary! In case anyone is worried about Friday the thirteenth, Ansonini told me the Spanish version is \"el martes ni te viajes ni te cases,\" so it must be all right! Sincerely, Carol Whitney Canada P.S. What happened to Juana's marvelous juerga reports? The following is Carol Whitney's personal letter dealing with her trip to Seattle. The photo of Ansonini in Seattle was sent to us by Gary Hayes. ANSONINI IN SEATTLE Dear Paco, Recently I went very unexpectedly to Seattle, and I've just written up the experience as impersonally as I possibly could, in order to concentrate on issues that seem to me to be flamenco-wide. But my trip was an immensely warm personal experience, full of little exchanges that reminded me of all of you in San Diego--so for old times' sake, here's how it went. but only over the phone! So this was my chance to meet Allen and Penelope in person. We hit it off right away, which didn't really surprise me after our conversations, but still, you never know. I was glad to have a rest, chat, wine and supper with the Yonges first, before the excitement of seeing Ansonini again. The morning after Ansonini phoned me, I phoned Allen Yonge to ask if I could stay with them. I've known Allen about five years--we've had many long conversations-- Allen has a record company (name: Voyager); he publishes jazz and classical music, though in the past he's also published traditional U.S. and Canadian music. He studied in Morón in the early sixties, on and off, I guess, for several years--and said that even then there were generally a dozen or so foreigners around at a time. It's interesting to think of Morón before the more extensive inundation (there were, I estimate, be\n\n[ENDING CONTEXT]\n\ndance. My voice was cracking with fatigue. I stayed much too long. That night as I fell asleep I could still see the Morcas dancing--it was a powerful image. I would have liked more time to talk with them, but hope for another chance some day. I made this trip for two reasons: to see Ansonini and to meet the Yonges--and I came home glowing. Allen and Penelope have become very special friends, I have some new aficionado friends, Ansonini will be taking messages to Spain for me, and my mind is full of his art and humanity--and that of the Morcas. I'm flamenco-friend-renewed. Sincerely, Carol\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "2, 3, 4, 5, 6",
    "page_number": 2,
    "word_count": 1626,
    "article_char_count_full": 9393,
    "article_char_count_review": 3800,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "excellent"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_07::A3",
    "article_text_for_review": "RAFAEL WITH CURRO TORRES (center) AND GITANILLO DE BRONCE (right) While in Spain with his family this past spring, Rafael Santillana went one night to the tablao El Arenal in Sevilla. As the evening's performances were coming to an end around 1:30 A.M. and Rafael began to get ready to leave, he noticed a number of very dressed-up people beginning to arrive; when he saw a television camera being set up, he asked what was going on. It turned out that \"La Tertulia Flamenca de Radio Sevilla\" whose director is Manuel Palomino Vaca, was presenting an award, a trophy called \"El Puente de Triana\" to Antonio Mairena. So Rafael stayed and was treated to the singing of Antonio, Curro, and Manolo Mairena, accompanied by the guitar of José Cala \"El Poeta,\" and the dancing of Matilde Coral and her husband Rafael \"El Negro\" Our faithful Jaleista, Rafael (who is a native of Málaga), was taking notes on all of this when a man sitting next to him asked what he was doing. Rafael explained that he was a Spaniard who lives in America and that he was taking notes to share with the hundreds of aficionados in San Diego. The man didn't believe him and asked him to identify the songs that were being performed. Rafael did well on the test and went on to tell his new acquaintance all about $ \\underline{\\text{Jaleo}} $ and the flamenco here. The man turned out to be Curro Torres, descended from the famous Torres family of flamencos, uncle of Agujetas and Gitanillo de Bronce, and a frequent judge in flamenco contests. Curro introduced Rafael to Antonio Mairena, who was apparently enthusiastic about $ \\underline{\\text{Jaleo}} $ and the promotion of flamenco worldwide. The following day Rafael went to visit Curro in Ecija, where he met Gitanillo de Bronce (who later went with the Santillanas to Málaga for the baptism of their son) and was treated to good food an flamenco. During the visit, Curro wrote an article for Jaleo, although still somewhat skeptical about the whole thing and suspecting it might be a joke. This article is very interesting in that it expresses a gypsy point of view -- which always brings cries of outrage from the non-gypsy flamencos. One should, therefore, read the article with the prejudice of the author in mind, although there appears to be considerable truth in what he says. For comparison we follow the English translation of the article with another author's point of view. There may be some errors in the Spanish since the author's handwriting was very difficult to read. ester luisa moreno international flamenco artist HOLLYWOOD (213) 506-8231",
    "title": "A Jaleista Meets Curro Torres",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "6, 7",
    "page_number": 6,
    "word_count": 442,
    "article_char_count_full": 2582,
    "article_char_count_review": 2582,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_07::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEnglish translation of: FLAMENCO, GYPSIES, AND HISTORY by Curro Torres. There is nothing written about flamenco that can be guaranteed; it is like a vineyard without a caretaker and ready to be harvested. On my part, I am going to attempt to give a brief impression of the facts as I understand and believe them to be. Of course, all of this is backed up by 50 years of listening to cante, defending the purity and truth of the cante grande, and above all, a love without limits -- not to mention passion and feeling for this art inherited from our ancestors. Although flamencologists say that flamenco has existed for only 150 years, it has been demonstrated that Cervantes afirms in his \\\"novela exemplar\\\" that a gypsy girl, accompanying herself on a tambourine, interpreted in the correct\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mentions\"]\n\nth of the cante grande, and above all, a love without limits -- not to mention passion and feeling for this art inherited from our ancestors. Although flamencologists say that flamenco has existed for only 150 years, it has been demonstrated that Cervantes afirms in his \\\"novela exemplar\\\" that a gypsy girl, accompanying herself on a tambourine, interpreted in the correct manner, some \\\"romances.\\\" Also, Lope de Vega, in \\\"La Talla de Sevilla,\\\" mentions that, in El Arenal, he heard a gypsy sing some coplas that were monotonous and sad, and according to Lope, the theme of the copla was tragedy and the melody was a continuous rosary of \\\"ayes\\\" and lament. If this wasn't the siguiriya, what could it have been? Leaving history aside, I assure you that 50 years ago my grandfather used to sing for me the tonå and romance that he, in turn, learned from his grandfather, and thus it was transported through families and generations - to my understanding. Flamenco is reborn and begins to spread in Jerez when, during a municipal census-taking being made in the last third of the 1700's, a man called himself Manuel Cantoral and, as his profession, he said he was a \\\"cantaó de flamenco;\\\" it is the earliest known reference that can be found on the subject. We know that Cantoral sang romances, siguiriy\n\n[ENDING CONTEXT]\n\nkingdom is not of this world. Shipwrecked and a survivor of a splendorous past; proud, but not arrogant; endowed with a great sensitivity that permits him to develop his tendencies, his art in all its various manifestations of dance, song, and bullfighting -- taking into account that the \\\"duende,\\\" plasticity, and \\\"el angel\\\" are always present when a gypsy dances, sings, or bullfights. His constant wandering, his precarious life and in many cases his misery; there is no reason why in his code there should not be a motto: It is for them, above all else, to be first a man, and then a poet!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Flamenco, Los Gitanos, y La Historia",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "7, 8, 9, 10",
    "page_number": 7,
    "word_count": 1044,
    "article_char_count_full": 6043,
    "article_char_count_review": 2931,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mentions"
      }
    ]
  }
]
```
