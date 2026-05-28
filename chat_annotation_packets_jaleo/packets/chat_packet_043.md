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
    "article_id": "JALEO_1979_06::A9",
    "article_text_for_review": "We almost didn't have a juerga this month, but at the last minute a site was volunteered. It will take place at the home of Donald and Emilia Thompson on June 23rd. The address is 5931 Desert View Dr. in La Jolla. This juerga will be for members only. Bring snacks and drinks.",
    "title": "LATE NOTICE: June Juerga",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_06",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "13",
    "page_number": 13,
    "word_count": 53,
    "article_char_count_full": 276,
    "article_char_count_review": 276,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_06::A10",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFRANCISCO MILLAN (Ed. note: \"Palmas Sordas\" is a column that appears Sundays in the $ \\underline{\\text{el Correo de Andalucia}} $, a newspaper in Sevilla. From time to time we will bring you excerpts that are especially relevant to the world of flamenco in general; the following appeared in March of this year.) JUAN PEÑA \"EL LEBRIJANO,\" THE UNPERSECUTED Satisfaction and problems have been the note of the beginning of the year for Juan Peña \"El Lebrijano.\" His major satisfaction was -- we are sure -- his recitals in the Teatro Real of Madrid. It is perhaps the first time that flamenco has entered into the most important theater in the capital (Ed. note: except for Paco de Lucía's solo guitar concert). For this reason, a group of friends attended an homenaje (homage) in the pub Camacho and\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"magnificent\"]\n\nsatisfaction was -- we are sure -- his recitals in the Teatro Real of Madrid. It is perhaps the first time that flamenco has entered into the most important theater in the capital (Ed. note: except for Paco de Lucía's solo guitar concert). For this reason, a group of friends attended an homenaje (homage) in the pub Camacho and honored him with the presentation of the silver plaque of the house. There was cante in his honor, he sang, dance by the magnificent Isabel Romero, and friendly conversation. The problems were related to the mounting and casting of \"Persecución\" (Persecution), the show about which it is said that, in spite of the subsidy of a million six hundred thousand pesetas (about $23000) by the Ministerio de Cultura (Dept. of Culture), the man from Lebrija lost on the event. But he is satisfied and disposed toward preparing another show right away. Assistance, friends, professionalism, art, all of these Juan Peña has in abundance in order to continue with the responsibilities and difficulties of preparing the shows; for good reason he is the least persecuted gypsy of all time. MARIO MAYA RETURNS Mario Maya, the gypsy from Granada, is the top flamenco bailaor of the present day. Nobody can deny to us that he is the most complete. Many argue about it, especially those of his race. His training goes from the indispensable academy training to his hours of daily study and the study of other dances like negro-jazz in the United States. In New York he had a complete triumph and a few days ago, he returned to Spain. MARIO MAYA From Madrid, demonstrating his friendship, he writes\n\n[ENDING CONTEXT]\n\nSanlúcar...y regresarte (a Miguel Hernández);\" RCA PL-35201. \"La Guitarra Gitana Y Pura De Paco Del Gastor\", (with Juan Del Gastor); Discophon SC 2292. \"Mi Sangre\" El Turronero, with Paco Cepero and Enrique de Melchor; Olivo 2-27.023 \"El Cante y la Guitarra de Pedro Peña\" with Pedro Bacán; Ariola 25 643H \"Cante se escribe con L\" El Lebrijano with Enrique de Melchor and Pedro Bacán; Olivo BVL-002 \"Triana, Despierta\" Chiquetete, with Paco Cepero and Enrique de Melchor; Zafiro ZLF 833 \"Los ases de flamenco LP\" Manuel Torre and El Tenazas de Morón, accompanied by Miguel Borrull (from 78 rpm)\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PALMAS SORDAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_06",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "18, 19, 20, 21",
    "page_number": 18,
    "word_count": 1176,
    "article_char_count_full": 6888,
    "article_char_count_review": 3237,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "magnificent"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_06::A11",
    "article_text_for_review": "by R.H.Morrison (Australia) Just this wood, just these strings, just this thumb, just these fingers -- MORCA FLAMENCO WORKSHOP & JUERGA A two week workshop for beginning and intermediate-advanced dancers. Each day, the workshop will feature a morning technique class and an afternoon repertory class for each level. Evening discussions will be held on all aspects of flamenco, including dancing with singing accompaniment, costuming, use of the bata de cola, and history of flamenco. The workshop will culminate with a juerga with participation by all. Mr. Morca's credentials are known to most of Jaleo's readers: He has worked with many of the top Spanish dance companies, been featured as a soloist at the Café de Chinitas, toured widely with his own companies, and is widely acclaimed as a teacher. The course will run from August 13-25th. The fee of $200.00 ($25.00 deposit is required by June 15th to ensure a place in the class) does not include housing. For more information, write: Morca Academy, 1349 Franklin, Bellingham, WA 98225. Or call Mary Rouzer at 206-676-1864.",
    "title": "GRANAINAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_06",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 175,
    "article_char_count_full": 1079,
    "article_char_count_review": 1079,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_06::A12",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCONCERT REVIEW A group of nine Jaleistas \"van-pooled\" to Los Angeles on May 6th to see \"Flamenco Spectacular\", a concert sponsored by Iberia Concerts and presented in the La Mirada (Calif.) Civic Theater. On the way back, we decided to do a communal review of the concert. It was agreed by all that the concert was a tremendous artistic success and greatly enjoyed by all of us. It is not often these days that we are offered concert companies of this size and caliber -- three singers, two guitarists, two male dancers (soloists) and eight female dancers. No matter whether one enjoyed or disapproved or particular aspects of the performance, it was all well done; for practically everything that was criticised by some or most of our group, there was at least one of us who enjoyed that particular\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\ndone; for practically everything that was criticised by some or most of our group, there was at least one of us who enjoyed that particular aspect. The show opened with a beautifully choreographed fandangos de Huelva, danced by all of the girls, Luisa de Bernardo, Isabel Campos, Meira Fuentes, Luana Moreno, Rosal Ortega, Ana María Suárez, Laura Torres, and Valencia, with Chínín de Triana and Antonio Sánchez alternating in singing the coplas. The many varied colors of the dresses gave the appearance of flowers in motion and were very effective. La Caña, a solo number by Roberto Amaral, who was responsible for all of the excellent choreography of the group numbers, did not explore the jondo qualities of this cante or demonstrate Roberto's dancing ability as well as did some of his other numbers. Chinín sang well; he is doing a lot of improvising with the melodies in his singing; some of them are only exercises in doing something different, but others are very beautiful. He put some new twists in the \"lamento\" (\"ay\" section) of the caña that were very nice. It was during this number that most of our A DINING ADVENTURE IN SPANISH AND MEXICAN FOOD group first noticed that the second guitarist David \"El Chinito\" did not appear to be actually playing; throughout the show, he did not appear to contribute much playing which was odd, since we know him to be a capable guitarist from his performances with other groups. Benito Palacios did an excellent job, however, and there was no lack of good music and accompaniment. Pepita Sevilla's energetic singing of \"La Zarzamora\" received its usual enthusiastic response from the audience. In the singing of all of her Spanish and popular songs she is very dynamic and very Spanish. Alfonso Bermúdez danced a tango de Málaga with his usual crispness. Al\n\n[ENDING CONTEXT]\n\nlively rumba sung and danced by Pepita Sevilla, we were treated to a gorgeous guajira danced by Rosal Ortega and Roberto Amaral (choreography by Roberto). In this dance, Roberto really showed his skill and sensitivity as a dancer and choreographer. He and Rosal worked well and very expressively together and this number was the favorite of most of our group. Interestingly, this was the only major number in the concert in which castanets were used; the public may miss them, but it is nice to see some restraint in their use in flamenco (which certainly heightens their impact when they are used).\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FLAMENCO SPECTACULAR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_06",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "22, 23",
    "page_number": 22,
    "word_count": 1155,
    "article_char_count_full": 6742,
    "article_char_count_review": 3436,
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
    "article_id": "JALEO_1979_06::A13",
    "article_text_for_review": "by Paco Sevilla EL BAILE - PART III CASTANUELAS (PALILLOS) - castanets; here is a list of their parts: Concha (la) - the shell or wooden half of the castanet. Escudo (el) - the pattern on the outside of the castanet. Huevo or Corazón (el) - the hollow on the inside. Pico (el), Beco (el), Punta (la) - point of the castanet. Puente (el) - the point where the two halves come together. Orejas (las) - the \"ears\" or projections at the top of the castanet. Other terms: Carretillas (las) - rolls with the castanet. Hembra (la) - the female or right castanet. Macho (el) - the male or left castanet. COLETAZO (el) - a kick with the side of the foot to move the train (la cola) of the dress to one side. ESCOBILLA (la) - aside from a general term for the major heelwork sections of a dance, it is also used in its original sense to mean a brush or scuff step (escobilla - a little broom). FRUNCIMIENTOS DE ENTRECEJO (los) - the knitting or drawing up and together of the eyebrows for expression. PALMAS ABIERTAS (las) - loud, sharp hand-claps made by the fingers of one hand hitting the palm of the other; also called FUERTES or SECAS. PALMAS REDOBLAS (las) - countertime palmas; also called PALMAS ENCONTRÁS. PALMAS SORDAS (las) - muted or soft palmas done by hitting the cupped palms together. PALMERO (e1) - one who does palmas. PASO (el) - step, as in taking a step, or a particular \"step\" in a dance. PERICON (el) - extra large fan (abanico) used in dancing. REDOBLE (el) - used to label a number of different heelwork combinations in which each foot does two flats before changing to the other foot; often used to conclude a rhythmic phrase. RESBALAR - to slip or slide (as on a slippery floor). SENTAO - dancing in a \"seated\" position, that is, with the knees bent; usually associated with very \"heavy\" or \"jondo\" dancing. VUELTA QUEBRADA (la) - turn done with the body bent forward throughout so that the eyes or crown of the head remain fixed forward at one point. VUELTA DE RODILLAS (la) - a turn on the knees; usually done by men. WELCOME TO JALEISTAS NEW MEMBERS CALIFORNIA: Joe Laib, Jr., Bettyna Belén, Mary Freguson, Eugene, Norman, Yvette Williams, Ester Moreno. FLORIDA: Lezli \"La Chiquitina\", Bob Rauchman. ILLINOIS: R.E. Brone MASSACHUSETTS: Donna Spencer MINNESOTA: Judith Milton, Lynnel Kunde NEW YORK: Carola Goya & Matteo, Hector Antonio de Jesus, Peter A. Gallett, Dick Denton OKLAHOMA: Ronald Radford OREGON: Dennis Ellexson WASHINGTON: Greg Wolfe WISCONSIN: Robert Ruck, Tom Johnson, Steve Stone SYDNEY, AUSTRALIA: David Schell SEVILLA, SPAIN: Antonio Mairena, Curro SWEDEN: Gita Sellmann Torres MIAMI FLAMENCO SCENE IN MAY by Lezli \"La Chiquitina\" After a successful four month performance, Chateau Sevilla Restaurant said goodby to \"Flamenco Fiesta\" with bailarin/cantaor, Ernesto Hernandez, bailarina, La Chiquitina, and guitarrista, Miguelito. This trio will be joining Miguel Herrero, cantaor of Cuban fame, when his new restaurant, El Cid, opens in mid-June on Le Jeune Road N.W. near W. Flagler Street. Ernesto, who hails from \"The Spaghetti Factory\" in San Francisco is \"in-between\" jobs, while Miguelito and Chiquetina are performing at The Health Nut Restaurant. Pepe Bronce and his company, \"Los de Oro\" (La Chiquetina, Rosa Elvys, bailarinas; Manolo, cantaor; Miguelito, guitarrista), the only active professional Spanish dance company here in Miami, is hoping for a four week contract at a hotel near Disney-world. They perform throughout the year at various places in Florida, including the",
    "title": "FlAMENCO TALK",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_06",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 592,
    "article_char_count_full": 3525,
    "article_char_count_review": 3525,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
