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
    "article_id": "JALEO_1980_01::A3",
    "article_text_for_review": "(All of the following articles are the person- al property of the individual authors and should not be reproduced without permission. All photos are courtesy of Anne Freeman.) by Jerry Lobdill This issue of JALEO focuses on Edward Freeman, a multifaceted, dynamic English musician who was the first to capture the essence of the flamenco guitar in written notation and who is now in his 71st year. The collection of articles and letters which appear here were written by people who have either studied with Freeman or are related to him. They were written in response to a request for people's impressions of Freeman. When we began this project we had no idea what the outcome would be, since no one knew what others would say. As a result, we have a composite picture which leaves some questions unanswered, and some areas unavoidably overemphasized. These paragraphs are intended to rectify this situation to the extent that available information will permit. Freeman's largest single contribution to flamenco may be that he made its melodies, rhythms, and basic forms understandable to any guitarist who could read standard musical notation. In 1953 he published an article in Melody Maker, a London music magazine for professionals, which explained for the first time to western musicians the variations of the 12 beat count on which so many flamenco forms are based. Some time later, he obtained contracts with Carlos Ramos, Mario Escudero, and later, Paco de Lucia to be their official transcriber for sheet music. All of this occurred at a time when flamenco guitarists were jealously guarding their techniques and music, and it was commonly accepted that flamenco couldn't be written. When Guitar Review published their first flamenco issue (No. 19) in the 1950's they selected Freeman's music above everything else they could find to publish. Although Freeman did not dedicate his energies to flamenco until 1952 he had been a professional musician since 1921 when he ran away from home at the age of 12. His classical guitar training, which began about 1933, provided an excellent background for his study and analysis of flamenco. Freeman was primarily a flamenco guitar teacher and transcriber of flamenco music. SEVILLA, MARCH 1953; ED FREEMAN WITH THE GROUP \"LOS TRES DE SEVILLA. (He is now retired.) He infrequently appeared as a performer in public though he was an excellent player. He would play endlessly for his students, his guests, and for professionals such as Ramos and Escudero who came to Dallas from time to time. As far as we know all of Freeman's students play the guitar as an avocation-- none have become renowned professional flamenco guitarists. As you will be told, Freeman is also an important guitar builder. His guitar design has changed over the years as his ideas have evolved. Today he is finishing his last six guitars and intends to shut down his shop. Freeman has never been a commercial guitar builder. He has been an experimentalist whose motivation has been to improve the response, volume, tone quality, and playability of his guitars. One of his guitars was made entirely of rosewood. One would expect this guitar to be dead, but amazingly, it is alive and has excellent playing qualities. His latest guitars are finished inside",
    "title": "EDWARD FREEMAN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_01",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 542,
    "article_char_count_full": 3276,
    "article_char_count_review": 3276,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_01::A4",
    "article_text_for_review": "(from: the Dallas News, February 23, 1969) By Francis Raffett In Spain or Mexico, when on of Edward Freeman's guitar students plays a throbbing bit of flamenco, the listeners will exclaim, \"Ah, el Ingles de Texas!\" They recognize the style as that of \"the Englishman from Texas.\" In 1954, at the age of 42, Freeman made a decision. \"I had played classical guitar and then jazz and now had encountered flamenco,\" he said. \"I was fascinated. But there was nothing in writing on the flamenco, no explanation. It was a folk art, not recorded.\" He decided to leave his wife and children in the U.S.A. and go to Spain and learn about flamenco. Which he did. \"I would give up my business to have nerve enough to drop everything and go alone to Spain,\" said a business friend wistfully. \"You're a bloody liar,\" answered Freeman. \"In Spain, you ask where are the flamenco artists and they answer 'What's flamenco'\" he recalled. \"They won't tell you because you're a foreigner.\" Finally, Freeman got on the trail of the real flamenco guitarists. He worked with Niño Ricardo, Ramón Montoya, Mario Escudero, Esteban de San Lúcar and Carlos Ramos. Some of the gypsies could not count to the full 12 flamenco beat and there was nothing in writing on the subject. So Freeman, occasionally strumming his own guitar, laboriously spent 18 months writing a complete flamenco library. Some numbers he did from the artists' records. When he checked them in person, he found that he had written only a very few mistakes. \"The biggest coming star I believe is Francisco Sánchez, known as Paco de Lucía,\" said Freeman. A young man and virtuosic genius, he believes. Many flamenco artists are known by their given name and their locale, he explained. Andre of Segovia, Paco of Santa Lucía, etc. Freeman, a restless type, was not satisfied with existing guitars, no matter how expensive. So he experimented with the traditional Brazilian and East Indian rosewoods, Mediterranean cypress and spruce, and came up with his own manufacturing technique. \"I invented eight different processes or procedures,\" he said, \"That's why this guitar has so much more volume.\" One sold recently for about the price of a Volkswagen. His personal emblem or trademark on each guitar is a dagger, sometimes in mother-of-pearl, on the neck and sometimes six of them inserted around the sounding hole. \"I'm no carpenter. I'm a musician,\" he insisted. \"I make a guitar only when I have to, for myself or a student.\"",
    "title": "FLAIR FOR FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_01",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 419,
    "article_char_count_full": 2467,
    "article_char_count_review": 2467,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_01::A5",
    "article_text_for_review": "by a member of the family It is not that Edward Freeman is uncooperative in explaining details that would tantalize a professional biographer -- it is that with intractable impatience, imperious calls for more tea, in a smog of smoke, he reflects abruptly: any preoccupation with the past, particularly $ \\underline{\\text{his}} $ past, is a \"bloody waste of time\". Among boxes of pictures and newspaper clippings (Maureen Freeman's treasures surviving World War II; moves from London to Belfast to New York to Los Angeles to Spain and to Dallas) a chronology exists. It defiantly awaits an organizer; it threatens years; it eludes an amateur. The $ \\underline{\\text{present}} $ is Edward Freeman's preoccupation: the student struggling to effect a correct hand position, the subtle construction of a guitar in the workshop. And the $ \\underline{\\text{future}} $: philosophical debates in the kitchen, theories about UFO's and men on Mars. Never idle, he is forever fixing, probing, discovering, designing, writing music, reflecting, and teaching. Flicks of memory in names, places, incidents: Edward Freeman, London runaway at twelve, professional violinist in pit orchestras of silent movie houses, seventeen-year-old conductor of a thirty-five member orchestra with musicians many years his senior; playing thirteen instruments, filling in when necessary with London chamber music, jazz groups, and combos (Dixie, Latin American, etc.) dance jobs, hotels, nightclubs; violin and banjo at the Savoy Cafe, Portsmouth, then with Larry Brennan in Belfast at the Plaza Palais de Dance -- two seasons long enough to meet Maureen McKeown, a blacksmith's daughter, whom he wrote to when he went to New York and played with Ricardo Giannoni; dance orchestra in Baltimore at the Summit Roadhouse near the Pimlico Racetrack; a Harlem speakeasy, engagements with Billy Lustig and the Scranton Sirens; return to London with Harry Roy at the London Paladium; then marriage to Maureen and ten years at the Savoy Hotel in London; made records with the best London musicians selected by recording producer Leonard Feather (still played on BBC in 1979!) designed \"Eddie Freeman Special 4-String Guitar\" for Selmer Music Company and composed \"In all Sincerity\", a demonstration solo; war years in Belfast playing trumpet and conducting a seven-piece Dixie combo in The Embassy Club; after the war playing trumpet in the Knightsbridge South American Club in London, always doubling with jazz guitar in the Bag O'Nails",
    "title": "ABRUPTLY BIOGRAPHICAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_01",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "8",
    "page_number": 8,
    "word_count": 387,
    "article_char_count_full": 2499,
    "article_char_count_review": 2499,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_01::A6",
    "article_text_for_review": "by Jerry J. Lobdill Freeman's library of transcriptions is organized by lessons, small sheets containing five lines of music per sheet. Thus, a piece takes up a number of consecutive sheets in the library. The music was given only to guitar students studying with Ed, and they were admonished not to copy it. It was for their own edification only and was a part of their guitar education with Freeman. Incidentally, it is legal for a teacher to teach what he knows and provide material to students for their own use in the course of instruction without fear of violating copyright law under the \"fair use\" doctrine. However, it would not be legal to offer unpublished copyrighted music for sale to the general public. Some of Freeman's transcriptions were done in cooperation with the artist, as was however, the library is so extensive that it is possible that something has escaped notice here. If anyone has knowledge of additional material- in particular, complete solos transcribed by Freeman-we would appreciate being notified so that this listing can be made accurate and complete. Unfortunately, many of the source recordings are unknown to this writer. These listings bear the notation, (U). Classical pieces in the library are indicated with the notation (C). If any of you deal with classified material in your work, please don't be alarmed by these notations. Here, then, is a bibliography of Edward Freeman's library. The order is arbitrary. CLASSICAL PIECES: 1. Carcassì Study in Am (C) 2. Carcassi Study in A Maj (C) 3. Romanza d'Amor, (Anon) (C) 4. Lágrima, Tarrega (C) 5. Adelita, Tarrega (C) 6. Recuerdos de la Alhambra, Tarrega (C) 7. Cancion Triste, F. Calleja (C) FLAMENCO VERSIONS OF POPULAR PIECES 1. Andalucía, (Lecuona) arr. by M. Escudero 2. Granada, (Lara) arr. by M. Escudero 3. La Cumparsita (Matos-Rodriguez) arr. by M. Escudero (All these pieces were on the same unknown record, i.e., (U).) FLAMENCO AND SPANISH TRADITIONAL PIECES Traditional, arranged by E. Freeman 1. Alegrias por baile 2. Soleares por baile 3. Gran Jota 4. Malagueña 5. Bulerias 6. Zambra 7. Farruca 8. Bulerías Inglesas These pieces are traditional in the legal sense of being public domain, except 8. SABICAS 1. Granadinas (viejo) (U) 2. Malaguena, Elektra, EKL 117 3. Farruca 4. Danza Árabe, (U) 5. Bulerías, Elektra, EKL 117",
    "title": "FREEMAN'S LEGACY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_01",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 386,
    "article_char_count_full": 2329,
    "article_char_count_review": 2329,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_01::A8",
    "article_text_for_review": "by Donald (and Carla) Gray I studied guitar with Ed Freeman for five years. I've forgotten who told me about Ed, but it was a fortunate recommendation. The pressures of teaching and research at the University of Texas at Dallas were wearing, and music always provided a means of relaxation. I knew nothing about Flamenco guitar, yet one meeting with Ed convinced me that this was what I wanted to learn. From my first lessons in 1972, I'm certain that Ed knew that I would never be a performer. He spoke with obvious pride of his students who could play professionally, students like Mark Volk, Tom Cotton, and Larry Fuess. But we all appreciated the important contribution Ed made in writing down for the first time many of the complicated rhythms and forms of Flamenco music. Even though I often postponed our Sunday noon sessions for lack of time to practice, Ed Freeman patiently let me proceed at my own pace. Both my wife, Carla, and I learned to appreciate Flamenco music, and we still listen for hours to tapes that Ed allowed us to make from his prized record collection. I am especially grateful for the willingness of a professional to share his knowledge and experience with a student who was not dedicated to playing professionally. Many teachers even at universities lack this willingness, giving attention only to the students who will go on to advanced studies. My own teaching has been influenced by the realization that science, like music, can be appreciated by students who will never \"perform.\" We applaude Ed Freeman for his knowledge and teaching of music, and we are grateful to both the Freemans for their friendship and generosity. *** ED AND BOB JOHNSTON, A DALLAS INDUSTRIALIST, AT THE TABLE WHERE ALL OF ED'S STUDENTS WERE TAUGHT. ED IS PLAYING ONE OF HIS SOLID BLACK GUITARS -- A FORMIDABLE INSTRUMENT.",
    "title": "A SCIENTISTS VIEW OF ED FREEMAN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_01",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "12",
    "page_number": 12,
    "word_count": 315,
    "article_char_count_full": 1832,
    "article_char_count_review": 1832,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
