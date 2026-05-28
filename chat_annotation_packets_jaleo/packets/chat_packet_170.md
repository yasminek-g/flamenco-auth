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
    "article_id": "JALEO_1983_06::A14",
    "article_text_for_review": "[sent by American Institute of Guitar] Outstanding guitar virtuosos from Spain and Latin America are featured artists in Spanish Accent on Guitar, a special series of four concerts at New York City's Town Hall. Players include Sabicas, Jorge Mcrel, Mariq Escudero and Manolo Sanlucar. The concerts--are every other month commencing in October--are presented by the American Institute of Guitar as part of its Second International New York Guitar Festival. Seats are $10.00 and $12.00, with a special subscription price to all four concerts offered at $36.00--a 25% saving. Tickets and information are available at the American Institute of Guitar, 2D4 West 55th Street, New York 1D019 (212) 757-3255. The first concert, on October 14, spotlights the legendary genius whom all flamenco artists call el maestro. He is Sabicas, and far nearly half a century he has been at the pinnacle of the great flamenco guitar tradition. Now a New Yarker, Sabicas recently returned from a triumphant tour of his native Spain after an absence of many years. Jarge Morel, born in Argentina, brings his warm and romantic artistry to Town Hall on December 9. Morel takes a broad approach toward repertoire, and his concerts may include his remarkable arrangements of songs from Leonard Bernstein's West Side Story as well as major works by Bach or Boccherini. But he is perhaps best known as an interpreter of works by South American composers, and these formed part of the memorable Mcrel concert which the New York Times called \"perhaps the most enjoyable recital of the season.\" Dn February 3, 1984, the brilliant Mario Escudera reveals another dimension of the flamenca guitar. Renowned for his compositional talent as well as his dazzling technical prawess, Escudero has been a major force in the development of a fresh and modern approach to flamenco guitar. Through his innumerable recordings, as well as concert tours that encompassed Moscow and Tokyo in addition to Europe's capitals, he has been a major influence on the players who followed him. Manold Sanlucar concludes the series on April 6, 1984. He is one of the most acclaimed and exciting players in Spain's new generation of flamenca virtuosos. His mastery of the instrument is complete, and he has been awarded Spain's most prestigious honor in the field--the National Prize for Flamenco Guitar. Manolo Sanlucar's innovations have helped to revolutionize the art, and he has earned a huge and devoted following in his homeland and beyond. [Not yet confirmed.] AROUND THE TOWN SAN DIEGANS VISIT L.A. JUERGA: A small group of San Diego Jaleistas went up to the May 14th juerga and had a great time. Since the L.A. contingent gives us plenty of advance notice of their juergas, perhaps we can get a bigger group together for the next one in July. FLAMENCC DUD: The newest item on the San Diego flamenco scene is the guitar duo Rodrigo and Pacá Sevilla. Although one might think that the vast difference in their styles would make this combination incompatible, the opposite seems true. When they play tagethez trading off lead or supporting roles, the blend is exciting. The sciá numbers appear to be a friendly competition in which each stimulates the other to greater virtuosity. The Duo will be appearing twice in June on Sunday the 5th and 26th at Drowsy Maggi's at 31st Street and University Avenue in North Park. It might also be mentioned that Maggi's has a unique and excellent menu and well within a flamenco's slender budget. There is no cover charge; the atmosphere is casual; a tip jar is the performer's sale renumeration -- so take along a couple of bucks for the artists' fund. FAREWELL TO DEANNA: Jaleistas gave Australian born dancer Deanna Davis a surprise going away party in April. She and her son Jessy, along with her fiancee are moving \"dawn under\" indefinitely. She says that there is a lot of flamenco in Sydney and she thinks she'll be able to find work with no problem.",
    "title": "SPANISH ACCENT ON GUITAR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_06",
    "year": 1983,
    "language": "en",
    "article_type": "article",
    "pages": "25",
    "page_number": 25,
    "word_count": 660,
    "article_char_count_full": 3944,
    "article_char_count_review": 3944,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_06::A15",
    "article_text_for_review": "* * * MAY JUNTA The May Junta meeting was attended by a small nucleus of Jaleistas. Our financial state has improved slightly thanks to a response by members to our editorial last month. We are lacking to print our June issue of Jaleo on time and get it out to our readers. The main topic of discussion was juergas, juerga sites and how to rebuild enthusiasm and participation. Please bring your fresh ideas to the June juerga meeting. $ ^{*} $ $ ^{*} $ $ ^{*} $ JUNE JUERGA Hurray! We have a juerga site for June! Guitarist Earl Kenvin has recently moved and has offered his house for the June juerga. Let's all take responsibility for contributing to the success of the evening so that we may be invited back on a future occasion. A self-employed computer programmer, Earl, a self-employed computer programmer, has been interested in and involved in music for many years. He has mainly taught himself guitar through books -- everything from classical, pop, country, and more recently, flamenco. He will be attending Paco Pena's workship in Córdoba in July. His home will lend itself perfectly to a juerga with several dance areas including an outside wooden deck, the living-room and a garage. We will also be holding elections at this juerga, so be thinking about what dynamic people you think could inject some new energy into the wilting local organization. Date: Saturday, June 25th Time: 8 PM Place: 3047 Chicago St. Phane: 273-1376 Bring: Tapas to share and whatever you want to drink. (We will not be selling wine because of the large waste problem, but soft drinks will be available.) Directions: From I-5 take Claimmont Drive EAST (short distance); take a LEFT on Denver to end and LEFT again; take a RIGHT almost immediately on Chicago. Donation: We are planning on purchasing and sealing some new dance boards. For this purpose we are requesting a $2.00 donation from members and $3.00 from non-members. -- Juara De A-va",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_06",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 334,
    "article_char_count_full": 1933,
    "article_char_count_review": 1933,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A1",
    "article_text_for_review": "The records featuring Pedro Bacán include: \"Estilos,\" with Curro Malena. Movieplay. 1973 \"Asi Canta El Perro de Paterna.\" RCA. 1974 \"Nuevos Cantes de El Perro de Paterna.\" RCA. 1974 \"Canta El Manuel de Paula.\" RCA. 1974 \"Campo Joven,\" with Manuel de Paula. Movieplay. 1975 \"Pepe Montaraz.\" Zafiro. 1975 \"Romance de Manuel Justicia.\" Manuel de Paula. Movieplay. 1976 \"Curro Malena.\" Discophone. 1977 \"Se Canta con L,\" with Lebrijano. Belter. 1978 \"El Cante de Pedro Peña.\" Ariola. 1978 \"El Cabrero.\" Belter. 1978 \"Pequeñas Cosas,\" with Manuel de Paula. Movieplay. 1979 \"A Mi Sevilla,\" with El Chozas. Belter. 1979 \"El Bienale,\" with Calixto Sanchez. Aljarafe. 1980 \"El Chozas.\" Belter. 1980 \"Curro Malena.\" Discophone. 1981 \"Vereas Negras,\" with José de la Tomasa. Belter. 1981 \"Calixto Sanchez.\" Aljarafe. 1982 Most recently Pedro has been invited to serve as artist-in-residence at the University of Washington School of Music during 1983-84. During this time he will be available for concerts and seminars.",
    "title": "PEDRO BACAN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 155,
    "article_char_count_full": 1008,
    "article_char_count_review": 1008,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A2",
    "article_text_for_review": "Summer has rolled around and with it many flamenco activities: summer concerts; flamenco performances at conventions, fairs and festivals; workshops by Paco Peña, Sanlúcar (Spain), Teo Morca, Manolo Marín, Roberto Amaral, Rosa Montoya. Let us know well in advance if you have summer or fall activities planned. Jaleo needs your input to keep its readers posted. We apologize for the fact that continued financial difficulties are causing Jalec to come out in the middle of the month. We feel that this is better than the alternative of another two month issue. You can safely add ten days to the closing dates on inside cover for submission of material for the August issue.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 112,
    "article_char_count_full": 674,
    "article_char_count_review": 674,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A3",
    "article_text_for_review": "IN SEARCH OF A SHY GENIUS Dear Jaleo Readers: I know some of you have tapes of Diego del Gastor that you are bogarting. How about making some available to us on cassette? After reading Donn Pohren's \"A Way of Life\" and Carol Whitney's articles (starting Sep. 1978 through Dec. 1978 with Brook Zen's crank, but appropriate letter in Jan. of 1979 Jaleo), I've wondered what this man sounded like. Finally, after acquiring National Geographic's The Music of Spain, Vol. 1 Andalusia, and hearing 3 cuts by the shy genius who refused to record, I know why the oldtimers raved about this unique man. Is this all my ears are destined to hear? I hope not. Sadhana Tucson, AZ APRIL-MAY ISSUE Dear Jaleo, Thanks for a particularly interesting April-May issue! We enjoyed and very much concurred with Manolo Marin's comments and we appreciated the great photos of the 8ienal, especially Natilde Coral and Angelita Vargas. Warmest regards, Rubina Carmona Los Angeles, CA CORRECTIONS CORNER Last month in our editorial it was stated that we were going to start a new policy of listing \"Jaleo Sponsors\" in the inside cover of Jaleo. We were forgetting that there were already two provisions for recognizing special contributions -- our \"Anda Jaleo\" column and the \"Sustaining Members\" provision of our bylaws. We have amended the bylaws to provide for the listing of Sustaining Members on the inside cover of Jaleo. Inside cover \"COVER PHOTO\"/identification should read \"guitarist David Serva\" not \"Manolo Marín.\" On page 6 photo caption should be \"Serva\" not \"Sevra.\" ANDA JALEO IN APPRECIATION OF THOSE, WHO THROUGH THEIR CONTINUED CONTRIBUTION, KEEP JALEO GOING. Our special thanks to Sustaining Members*, Genevieve Offner and Elizabeth Ballardo. Mrs. Offner (affectionately known to us as \"Jenny\"), is our professional typist/type-setter. She usually donates a few hours a month to us, but this month she is donating her time for the entire issue. Even though Elizabeth Ballardo has been forced to resign her post as treasurer because of other demands on her time, we feel that the many hours she spent, over the past three years, on the Jaleistas books, above and beyond the requirements of a treasurer's position, warrant her being listed as a Sustaining Member also. (*See back cover) THIRD ANNUAL An historic Indian Mpadow, nestled in the redwood forests of Big Sur is the setting for this unique event where there will be a wonderful array of musical talents. MIDDLE EASTERN FLAMENCO SWIMMING: Bring suit and towel. Peter Evans Palo Coiorado Canyon Carmel, CA 93923 Phone or Write for reservation, map and further instructions. (408) 625-2517",
    "title": "LETTERS EXTREMADURA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 433,
    "article_char_count_full": 2637,
    "article_char_count_review": 2637,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
