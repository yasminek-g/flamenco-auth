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
    "article_id": "JALEO_1984_05::A4",
    "article_text_for_review": "(sent by George Ryss) Mariquita Flores is an internationally-esteemed artist, performer, and teacher in the field of Spanish dance. An acclaimed child prodigy, she launched her professional career at the age of four in zarquelas and fines de fiesta. At age fourteen, M. Flores was performing as a member of Vicente Escudero's troupe in Paris, France. A subsequent assignment as dance directress, at age seventeen, with the faculty of the Palace of Fine Arts in Mexico City formulated the embryonic stages of her deep interest in dance education. Punctuated by professional appearances and teaching engagements, her formative years were spent in exhaustive study with some of the best teachers available in Spain and France. Vivid in her memory are: her studies of baile agitando with the famous Ortega family, her studies of flamenco with the great Estampio, and her studies with Azunción Granados (an outstanding associate of the world-renowned Argentina - it was from Anzunción Granados that Miss Flores learned the complete repertoire of the famous Argentina). At the age of nineteen, Miss Flores was chosen to tour the United States of America, Canada, and Mexico to raise funds for Spanish refugees in France. While on tour, she was contracted by 20th Century Fox for two Hollywood films: \"Blood and Sand\" and \"Fiesta\". In subsequent years she appeared in various Argentinian, Brazilian, and French films. Miss Flores has given numerous command performances for heads of state including Juan and Eva Peron of Argentina, President Getulio Vargas of Brazil, Presidents Lazaro Cardenas and Miguel Aleman of Mexico, and President Vincent Auriol of France. The annals of Spanish Dance Repertoire have been indelibly inscribed with Miss Flores' choreographic interpretations and performances. With music accompaniment by La Orquestra Sinfonica Brasileira (conducted by Maestro Eleazar de Carvalho), she staged and gave an historic performance of Manuel de Falla's \"Amor Brujio\" and \"Capricio Español.\" No one could interpret my music better!\" exclaimed Ernesto Lecuona when Miss Flores (accompanied by music of the Carnegie Hall Symphony Orchestra under the capable baton of Maestro D'Artega) presented one of the original interpretations of \"Malagueña,\" \"Gitanerías\" and \"Andalucía.\"",
    "title": "MARIQUITA FLORES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_05",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 349,
    "article_char_count_full": 2283,
    "article_char_count_review": 2283,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_05::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMaría Benítez began formal dance training at the age of ten. After studying with the legendary Francesca Romanoff and Igor Schwezoff, she went to Spain to continue studies in Spanish Dance. She joined the María Rosa Spanish Dance Company as soloist and performed with the company in concert and on television throughout Spain, Europe and South America. She was first dancer with Paquita Rico Company for two winter seasons in Madrid and toured throughout Spain. María returned to the United States and continued to perform while she taught at the Boston Conservatory of Music, Verde Valley School, the University of Utah, and the Institute for American Indian Art in Santa Fe. In 1972 she and her husband Cecilio formed their own company, María Benítez' Estampa Flamenca, which has been\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"successful\"]\n\nShe was first dancer with Paquita Rico Company for two winter seasons in Madrid and toured throughout Spain. María returned to the United States and continued to perform while she taught at the Boston Conservatory of Music, Verde Valley School, the University of Utah, and the Institute for American Indian Art in Santa Fe. In 1972 she and her husband Cecilio formed their own company, María Benítez' Estampa Flamenca, which has been extraordinarily successful throughout the U.S. She is not only widely recognized as one of the foremost Spanish dancers of her generation, but as a choreographer for opera as well, including \"La Vida Breve,\" \"Carmen,\" and \"La Traviata\" for the Santa Fe Opera, \"La Vida Breve,\" starring Victoria de los Angeles, at Sarah Caldwell's Boston Opera, and production in Virginia, Tucson, and Fort Worth. Miss Benítez performed in New York City's Delacorte Dance Festival in 1975 and again in 1976; the American Dance Festival in 1977; the prestigious Brooklyn Academy of Music, in 1978; and under the auspices of New York City's Dance Umbrella Series, successively in 1980 and 1981. She is the recipient of the New Mexico Governor's Award for Excellence in the Field of Dance. The film \"Estampa Flamenca\" featuring Miss Benítez has been viewed by audiences throughout the United States, Puerto Rico, and Canada - over the PBS national television network. Her most recent television appearance was on the Perry Como Christmas Special on the ABC Network. She has also appeared with the Milwaukee Symphony and the Spoleto Festival USA in Charleston, S.C. The company's most recent major engagement was at the Akademie der Kunste in Berlin, West Germany, for a series of highly successful concerts. The company\n\n[ENDING CONTEXT]\n\n22 Sept. 24, 25, & 26 at Kennedy Center, Terrace Theatre, Washington, DC 8 PM Oct. 1-4 Denver, CO TBA Oct. 5 & 6 at 8 PM CO. Springs Pine Arts Center, CO, two master classes Oct. 4 & 6 Oct. 8 Craig, CO TBA Oct. 10 Rock Springs, WY TBA Oct. 13 at 8 PM University of CO at Greeley Oct. 16 Albuquerque, NM TBA Oct. 19 at 8 PM Fine Arts Center, School of Mines, Socorro, New Mexico Oct. 23 at 8 PM Paul Pogg Theatre for the Performing Arts, Del Rio, TX, one 1/d Oct. 22 Oct. 24 at 8 PM Angelo State Univ. Audit., one 1/d Oct. 25 San Antonio, TX, 3 workshops on same days Oct. 31 at 8 PM, Laramie, WY TBA\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MARIA BENITEZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_05",
    "year": 1984,
    "language": "en",
    "article_type": "poem",
    "pages": "6-9",
    "page_number": 6,
    "word_count": 1671,
    "article_char_count_full": 10053,
    "article_char_count_review": 3353,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "successful"
      }
    ]
  },
  {
    "article_id": "JALEO_1984_05::A6",
    "article_text_for_review": "(sent by George Ryss) Manolo de Córdoba, born in Córdoba, Spain, began his dancing career about five years ago with Estrella Morena. After four weeks of classes, Estrella gave him a chance to perform with her company at the Barbizon Plaza Theater. Two months after he performed with Estrella and Pepe de Málaga at Carnegie Hall in New York. He toured with José Greco as soloist, María Benítez, Isabel Lujan and the Boston Flamenco Ballet. He performed at the Chateau Madrid for two years and various clubs throughout the country, with such artists as \"El Agujetas de Jerez,\" \"El Pelete,\" Luis Vargas, Teo Santelmo, Domingo Alvarado, Emilio Prados and currently with La Tati. THIS SPACE RESERVED FOR YOUR CARD-SIZE AD SPECIAL OFFER $10 FOR 1 MONTH - $25 FOR 3 MONTHS PRICE APPLIES TO PHOTO READY ADS (ONE TIME $5.00 FEE IF AD DESIGN IS REQUESTED) · RARE LIVE RECORDINGS DIEGO DEL GASTOR LIVE La Fernanda de Utrera - singer Manolito de la Maria - singer Fernandillo de Moron - dancer Circulo Mercantil Fiesta, Moron de la Frontera 1964. 2x60 min. high quality, normal bias, Dolby B, mono setting tapes. Includes commentary and selected letras and translations. Please send $25 plus $3 for taxes, air mail and handling. MANOLO DE HUELVA - GUITARIST An Extremely Rare and Unique Recording Luis Caballero - singer Sevilla 1968. La Cuadra. Only known live recording in existence. One side of a 45 min. high quality, normal bias, Dolby B, mono tape and short commentary. Please send $15 plus $2 for taxes, air mail & handling. To order send check or money order to: ZINCALI RECORDING CO. 1185 CHEZEM RD. BLUE LAKE, CA 95525 Please expect 4-6 weeks for delivery. Defective tapes will be replaced.",
    "title": "MANOLO DE CORDOBA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_05",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 290,
    "article_char_count_full": 1688,
    "article_char_count_review": 1688,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_05::A7",
    "article_text_for_review": "INTERVIEWED FOR JALEO BY DAVID ALFORD This interview was obtained after Paco de Lucía's concert in Honolulu, Hawaii, with John McLaughlin and Al DiMeola. Paco was very generous to give the rushed interview because his plane was leaving soon. He impressed me as a very kind person: Paco, thank you for this brief interview. Are you familiar with $ \\underline{\\text{Jaleo}} $? We would be very interested in learning about your plans. I understand you're leaving for Australia tonight. Yes, in two hours. We have a long trip, very long. Are you planning on making any more records soon in flamenco? Yes, of course, it is a constant in my life to play flamenco, but I do other things to bring air and to open doors to my music. The reason of my music is flamenco. Do you still enjoy flamenco more than your work now in jazz? You know, my music is flamenco. I do everything for that feeling. I cannot be a jazz musician. I can't play the notes that well. I can never be a jazz musician because I am a flamenco guitarist. Do you have any time schedule in mind for your forthcoming record? Yes. Well, I don't know... I don't like to plan my future. The music comes when she wants to come. So, when the music comes I record it. Do you plan to every day more of Lorca? Garcia Lorca...no, I made a record a long time ago. Although, Garcia Lorca is still always a fountain of inspiration. Paco, do you feel it is possible to evolve more entirely new toques? Of course, but the thing is, flamenco has many traditions, many customs and its not easy to implant something new. For me it is very hard to try to evolve flamenco because they don't let it, and it's not easy. You have to do it very subtly, in a very subtle way including new chords and new concepts but without losing the feeling of the roots.",
    "title": "PACO DEL LUCIA: INTERVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_05",
    "year": 1984,
    "language": "en",
    "article_type": "poem",
    "pages": "12",
    "page_number": 12,
    "word_count": 333,
    "article_char_count_full": 1792,
    "article_char_count_review": 1792,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_05::A8",
    "article_text_for_review": "(from: $ \\underline{\\text{Hoy}} $, March 15, 1984; sent and translated by Brad Blanchard) A revolutionary \"Extremeñan guitar\" has just been invented by Juan Gilabert Sarrión, a 46 year old man from Cádiz who has lived in Badajoz for 16 years. \"My friends who have seen my innovation insist on calling it 'guitarra extrema' and I'm proud that it is so, precisely because of the support I have found among you 'extremeños' in order to accomplish what I have done.\" The Extremeñan detail, aside from Juan Gilabert's 16 years of study here, is constituted by the heart of oak (encina, equivalent to the local \"national tree\") used in the construction of the bridge. Artistic woodworker, self-employed, Juan Gilabert arrived in Extremadura 16 years ago. In love with his profession, the artist understood he was able to make a guitar, which was easy for him because of the many years that he has dedicated to artistic woodwork. \"Among the advantages of this guitar,\" Señor Gilabert tells us, \"is that the sound enters round and clean and comes out the same way, because there are no obstacles inside the guitar. The technicians who have seen it and played it have said that. Among other advantages, the sound is the most important.\" Other advantages, according to a report made by the professor of guitar in the Conservatory of Badajoz and music-critic for this newspaper, Enrique Molina, are that this guitar has \"...characteristics that distinguish it from its companions; a round body instead of a flat one, with the consequence of being able to make bar chords up to the twelfth fret, which can't be done on traditional guitars. In spite of the innovation, Señor Gilabert's guitar is totally classical in the sound and in the material used, with the front being of Canadian cedar, the sides and back of Indian rosewood, the neck of Honduran cedar, ebony and rosewood, and the detail of the heart of oak used in the construction of the bridge.\" \"A few years ago,\" continues Señor Gilabert, Señor Joaquín Ponce gave me some guitar classes and since then I've been interested in making a better guitar than those which are on the market, with the rounded body.\" Among the characteristics of this guitar...are the rounded sides instead of the usual flat ones, the absolute absence of angles, the absence of impediments for the sound circulating between the sides, front and back, and the facility in playing the instrument which practically molds itself to the musician's body. \"Both classical and flamenco guitarists who have played it have told me that it is a great innovation in the field of music.\" However, in order to achieve the first guitars, both for practice and concert, Señor Gilabert has had to destroy many others until he found the \"trick\". \"Yes, there's a trick which, logically, I'm not going to reveal. I've JUAN GILABERT SARRION Organiza: EXMO. AYUNTAMIENTO DE CORDOBA Delegación Municipal de Cultura rtística: PACO PENA Centro Flamenco Paco Reña de Córdoba 12 de Julio. 2130 noche Patio Antiguo Ayuntamiento EL CHAPARRO 13 de julio, 2270 noche, Patio Antiguo Ayuntamiento LOLI FLORES y su grupo 16 de Julho. 21'30 noche. Patio Antiguo Ayuntamiento ENRIQUE MONTOYA 18 de Julho. 21'30 nache. Patio de los Naranjos PACO PENA / ADRIAN LYNCH 19 de Julio, 21730 enche; Patio Antigua Ayantamienta JAMES TYLER y BARRY MASON 21 de Julio, 22 de noche, Patio de los Naranjos PACO DE LUCIA 23 de Julio 21730 nache Patio Antiguo Ayuntamiento JOHN WILLIAMS y BENJAMIN VERDERY 24 de Julio, 21'30 mche; Patio Antiguo Ayuntamiento EDUARDO FÁLIU 25 de julio 2130 noche Patio Antiguo Ayuntamiento Conferencia a cargo de José Rodriguez Concierto Quinteto Reginaldo Barherá 26 de Julio, 21'30 noche, Patio de los Narnijos JOHN WILLIAMS 27 de Julio. 22.00 noche. Teatro Municipal al aire libro INTI-ILIMANI 30 de Julio, 21 30 noche; Patio Antiguo Ayuntamiento CHANO 1.0BATO 31 de Julho, 21'30 nache: Patio Antiguo Ayuntamiento INMACULADA AGUILAR y su grupo 2 de Agosto. 2130 noche. Patio Antiguo Ayuntamiento VICTOR MÓNGE \"SERRANITO\" d de Agosto. 22'000 noche Teatro Municipal al aire libre GRUPO SOLERA de la Peña Flamenca de GRAN FIESTA FINAL Huelva",
    "title": "AN \"EXTREMENAN GUITAR\" IS INVENTED",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_05",
    "year": 1984,
    "language": "en",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 690,
    "article_char_count_full": 4145,
    "article_char_count_review": 4145,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
