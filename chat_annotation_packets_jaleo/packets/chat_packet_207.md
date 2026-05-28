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
    "article_id": "JALEO_1985_07::A9",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMORCA IN OHIO by Bruce Catalano Teo had given a two week residency at the Fairmount Spanish Dance Company. Libby Lubinger, artistic director, located in Novelty, Ohio, just outside of Cleveland. This performance occurred midway through the residency and was received enthusiastically by everyone in attendance, including the Plain Dealer critic, guests, and the Fairmount Spanish Dancers themselves. The residency itself, occurring the first two weeks of July, 1985, was tremendous. The Fairmount Dancers, a company of 15 dancers, a singer, and 3 guitarists were so impressed with Teo, not only as a consumate Spanish dancer and teacher, but with the overall warmth and genuineness of his personality. Plans are already underway to have a return engagement next year with him and hopefully his whole\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nthemselves. The residency itself, occurring the first two weeks of July, 1985, was tremendous. The Fairmount Dancers, a company of 15 dancers, a singer, and 3 guitarists were so impressed with Teo, not only as a consumate Spanish dancer and teacher, but with the overall warmth and genuineness of his personality. Plans are already underway to have a return engagement next year with him and hopefully his whole company. By the way, Teo also put in many words of support about $ \\underline{\\text{Jaleo}} $ to all of us here. I know that many of us already have subscriptions, but it never hurts to give a plug. MORCA PUTTING INTENT STUDENTS THROUGH THEIR PACES IN OHIO WORKSHOP JALEO - VOLUME VIII, NO. 3 Around the turn of the last century, as flamenco dancers were brought onto the stages of nightclubs and performance halls, dancers began to employ more complicated steps, marking out varied and complex rhythmic patterns with the heels and soles of their shoes. Some historians have speculated that these fancier steps resulted from the flamenco dancers' attempts to mimic the dynamic effect of the American tap dancers whose performances were often the highlight of European musical reviews. Whatever the inspiration, the complicated steps became integrated into the flamenco tradition, and most descriptions of flamenco today refer to \"stamping feet\" and \"c\n\n[ENDING CONTEXT]\n\nbetween to attend the Morea workshop, an annual event for the past seven years. Some, like Seattle's Sara de Luis, are professional dancers; others are beginners. The youngest participant--and one of the most talented, Teo Morca says--is Johanna Denis, ll, a Texas dancer. Flamenco has a different appeal for each participant. \"I like the strength of it, the theatrics,\" Elise Hunt of San Antonio said. \"But what interests me the most is that it represents the culture of a people.\" The workshop concludes with a celebration and dance performance. $ ^{*} $ $ ^{*} $ $ ^{*} $ WORKSHOP IN BELLINGHAM\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "TEO MORCA WORKSHOPS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "11-14",
    "page_number": 11,
    "word_count": 1768,
    "article_char_count_full": 10913,
    "article_char_count_review": 2988,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "many"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_07::A10",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[from: The Herald, Bellingham, Washington, Thursday, August 22, 1985] by Joan Connell In flamenco perhaps more than any other form, a dancer's body is his instrument, the source of rhythm and percussion as well as lyrical, liquid movement. Two weeks ago in Bellingham, as he performed a rigorous portion of flamenco repertoire under blazing skies at an outdoor festival--with not a drop of sweat on his brow--Teo Morca, 51, demonstrated again that his body is in excellent shape. After 36 years of performing, Morca still can elicit from dance critics adjectives that range from to \"magnificent\" to \"profound.\" A University of Washington medical researcher who included Morca in a study of dancers who have been performing 25 years or more, gave his X-rays and cartilage rave reviews. That discounts\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"know\"]\n\n, 51, demonstrated again that his body is in excellent shape. After 36 years of performing, Morca still can elicit from dance critics adjectives that range from to \"magnificent\" to \"profound.\" A University of Washington medical researcher who included Morca in a study of dancers who have been performing 25 years or more, gave his X-rays and cartilage rave reviews. That discounts the notion that middle age signals the end of a dancer's career. \"I know I'll reach a turning point sometime, and when I reach that point I'll deal with it. But every dancer that stays young treats his instrument properly,\" Morca said. Like his wife and dancing partner, Isabel, he is a gourmet cook, an enthusiastic eater, and a conscientious dieter. \"Lola Móntez used to have a favorite saying: 'Zippers don't lie.' I can honestly say I've never let out a pair of pants in my life,\" he said. Physical fitness is really a side issue to Morca, who was born in Los Angeles to a Hungarian-Spanish family. His performing career began at 15, as Abdullah the Gong Player in a recital at modern dance pioneer Ruth St. Denis' school in Hollywood. He received ballet training from Ballet Russe expatriates, many of whom ultimately settled in California. Flamenco training from José Greco, Jose Cansino and others built on that classical foundation. Los Angeles in the 50's was a hotbed of Spanish dance, and Morca recalls doing four or five grueling shows a night in cabarets, coffeehouses and clubs. Before long, he was leading dancer with Pilar Lopez' Baile Espanole, and toured the international circuit with her 35-member company. Concert tours in theaters were one thing, but the night-club scene in the U.S. and Europe offered steady work and created a certain kind of energy, M\n\n[ENDING CONTEXT]\n\nthis life movement--for it is theatre. Flamenco dance is total body. It is visual and it is sound, made by our own body instruments. We can adapt a soleares to the stage that will expand the wall and be power- CATALOGUE OF MODERN FLAMENCO RECORDS A collection of flamenco records from the modern era (1972–82), representing most of the important artists and including a number of unusual and rare items. Each record is described in detail and given a brief critical review. A tape library will make these records available. SEND $4.00 TO: PACO SEVILLA, 2958 KALMIA ST. SAN DIEGO, CA 92104\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1956,
    "article_char_count_full": 11250,
    "article_char_count_review": 3374,
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
    "article_id": "JALEO_1985_07::A11",
    "article_text_for_review": "NEW YORK Villa Del Parral on 213 W. 14th Street is one of the many Spanish restaurants in New York City. This exactly is where the comparison ends! Parral, owned by \"Mr. Flamenco,\" Jesus Ramos, bailaor and chef is undoubtedly headquarters for flamencos...keep it that way...get there EAT, DRINK, and PARTICIPATE...Juergas till 6:00am Sunday morning (what with two or three aficionado-cantaores at the bar). If you ignore this, and do not go, let me tell you of some of the nobility that passed through its gates in recent times: Paco de Lucía, and his two brothers Ramón and Pepe de Lucía. The greatest of the bailaoras, La Tati went there after each concert, had her meals there, participated in the juergas and had her farewell party there. Mario Maya's gitanillos frequented whenever possible. Cumbre Flamenco went there in its entirety, Carmelilla Montoya, El Güito, Serranito, Morente and all their participating artists...Sabicas and Mario Escudero make appearances--Ramos himself appears at every flamenco function and helps artists, whenever he can. Villa de Parral has as guest cantaor; Pepe de Málaga, recently returned from Florida. He is probably the finest and most presentable stage cantaor of flamenco; his guitarist at Parral is Diego Castellón, ever popular tocaor, brother of Sabicas. Fiesta de Campostella in New York City, July 25 through 28 included Estrella Morena, bailaor Orlando Romero, with Pepe de Málaga and guitarist José Ma. Moreno. Spanish Dance Arts Co. presented 4 shows at University Theatre Lorca, Santana and Marques participated. This took place the beginning of June. Liliana Morales presented a beautiful show in May. Her guest artists were two male dancers, Jesus Ramos and Orlando Romero; Arturo Martínez on guitar and Miguel de Cádiz was her cantora. Aficionados, big news for autumn, New York: Chateau Madrid is reopening at a new location with slight change in ownership. It would be Park Avenue near 29th Street. Pepe de Málaga, Estrella with Emilio Prados on guitar would form the flamenco attractions...this is an exceptionally good neighborhood for Paellas and I mean Mesa de España on 27th Street near Park Avenue -- Guitarist in-residence is Roberto Reyes at this exceptional, excellent restaurant.",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "article",
    "pages": "17",
    "page_number": 17,
    "word_count": 358,
    "article_char_count_full": 2248,
    "article_char_count_review": 2248,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_07::A12",
    "article_text_for_review": "Born in Madrid, Spain, Mr. Sanchez began his study of the guitar at the age of twelve. At the age of thirteen, he continued his studies at the Conservatory of Music in Madrid. He studied music with the professor of music, Regino Sain de la Mata. At the same time, he took private instruction with Juan Garcia de la Mata for one year. He also studied four years with the great classical guitar professor, Haurio Herrero. After six years in the Conservatory, Mr. Sanchez became attracted to flamenco music and started an extensive study of that media. His natural aptitude for classical and flamenco music enabled him to progress swiftly in its perfection. His professional career started in 1950 with a touring company in Spain. In 1957, Mr. Sanchez became the first guitarist of the company Rosario Ballet Español. He played classical solos and flamenco accompaniment for the dancers. From 1962 to 1968, Mr. Sanchez became first guitarist with Antonio Ballet de Madrid, making concert tours around the world. He travelled in Europe, Africa, Asia, the Middle East, South America, Central America and North America. Mr. Sanchez has been doing concerts in colleges and Universities in North America since 1973. He was the first flamenco artist to be invited to perform at the New Orleans Jazz Festival, which attracts New Orleans' top artists annually. He was also a faculty member of Tulane University's Music Department where he taught classical guitar. He also performed in Santa Fe's production of the Opera \"La Vida Breve\" as solo guitarist. Mr. Sanchez has made numerous recordings in Spain and South America, both solo and with the flamenco company. His latest recording which was made in North America is entitled SOUL FLAMENCO. He has performed before the King and Queen of Denmark and for Spanish royalty.",
    "title": "PROFILES: CARLOS SANCHEZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 301,
    "article_char_count_full": 1812,
    "article_char_count_review": 1812,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_07::A13",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n\"NEW\" RECORDINGS AVAILABLE by Paco Sevilla Anita Paloma sends us an article from ABC, March 3, 1985, written by a Señor Montoya. It touches on a number of subjects: Ricardo Pachón, knowledgeable in flamenco was producing a series of programs on that subject for Spanish television, to be called \"Flamenco en Vivo.\" Mr. Montoya asked him how it was going. \"...it was cancelled when we were halfway finished!\" \"Why, Ricardo?\" \"The truth is, I don't know! All I can tell you is that they told me to stop and I stopped. There must have been somebody new who came along and didn't like it much.\" Montoya then goes on to say, \"Well, that's how things go in this country, and it wasn't an isolated incident, especially when it comes to this Andalucian art. I have been told that an American named Cristóbal\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"making\"]\n\nt have been somebody new who came along and didn't like it much.\" Montoya then goes on to say, \"Well, that's how things go in this country, and it wasn't an isolated incident, especially when it comes to this Andalucian art. I have been told that an American named Cristóbal Silver [Cristóbal Dos Santos--Chris Carnes] who is a professor at Berkeley, a university in California [Chris lived in Berkeley, but is not a professor], dedicated himself to making recordings from 1961 to 1969 of fiestas that included such pure flamenco artists as Diego del Gastor, Juan Talega, Manolito el de María, El Perrate, etc., and now, recently, he offered it all, nearly three hundred hours of cante, to official offices of the government in Andalucía, such as the Junta de Andalucía or the Diputación de Sevilla, for a modest price so that they could edit and release them. The response was negative and the University in California bought them. So now you know: if you want to hear pure cante, go to California!\" The University of California did not buy the tapes. They are being offered by that very same Cristóbal under the label, Zincali Recording Co. The complete list of materials now available are to be found elsewhere in this issue. There is a series of tapes of Diego del Gastor playing for such renowned cantaores as Fernanda de Utrera, Manolito el de María, Juan Talagas, and others, a tape featurin\n\n[ENDING CONTEXT]\n\ntrue of the bulerías, which are also played in dissonant tones. In summary, I find this tape to be very listenable, without containing much that is memorable. The recording quality is okay--perhaps a bit heavy on the bass. Those who enjoy guitar solo music will probably enjoy it. As I said, the playing is quite good and authentic. There were no instructions included with the tape for obtaining a copy. On the cassette is what appears to be a phone number: (408) 372-STAR. Other than that I can only suggest writing to Guillermo Rios, care of \"María Benítez, 1617 Vuelta Place, Santa Fe, NM 87501.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "RECORDING REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 1263,
    "article_char_count_full": 7316,
    "article_char_count_review": 3027,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "making"
      }
    ]
  }
]
```
