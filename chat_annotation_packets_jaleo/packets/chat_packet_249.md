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
    "article_id": "JALEO_1986_SUMMER::A24",
    "article_text_for_review": "[from: El País, April 25, 1986; sent and translated by The Shah of Iran] by Angel Alvarez Caballero The professional gypsy tocaor Antonio Losada presented, in connection with the Cumbres Flamences in Madrid, a new technique of his own elaboration for tuning guitars. Losada is a self-instructed guitarist who earns his livelihood as a tocaor in a tablao in Madrid. The tuning of guitars still presents problems of considerable magnitude, some of which Losada tries to obviate by employing this new method which he discovered while practicing his hobby of guitar-making. The new technique consists of modifying the frets, traditionally straight, with differing curvatures, especially those belonging to the second string of the instrument, which seems to be the most troublesome to tune. Professional guitarists, as well as guitar builders, were present in the room in which this presentation took place. Losada, after explaining the theory of his invention, gave several practical demonstrations on an instrument he had prepared for this purpose and invited anyone present who cared to, to try for himself, which several did. The musicologist Sabas de Hoces tried out [the instrument] and declared the discovery of Losada to be astonishing and of great potential. JALEO - VOLUME IX, No. 2",
    "title": "NEW TECHNIQUES FOR TUNING GUITARS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "article",
    "pages": "40",
    "page_number": 40,
    "word_count": 204,
    "article_char_count_full": 1288,
    "article_char_count_review": 1288,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A25",
    "article_text_for_review": "WOOD THAT LAUGHS, STRINGS THAT CRY: THE FLAMENCO GUITAR Countless years ago, peoples began inventing musical instruments to imitate the rhythm and music of nature and also to express and extend their feelings, to communicate their emotions, both on a human and a spiritual level. One of these genius instruments has evolved as the guitar, specifically, for this article, the flamenco guitar. I do not know of anyone who can say for certain when the first flamenco guitar was made or who made it, but, indeed, it was a strong bit of genius that so strongly captured the flamenco musical soul. The guitar was the late corner in flamenco. For centuries, songs were sung freely with rhythmical accompaniment of tapping sticks and canes and, obviously, palmas. Dance was done with bare feet against the earth or in whatever footwear was available. No fancy boots with nail-filled heels against hard wood floors; that would come later with the discovery that flamenco was destined to become a universal performing art. Although the guitar was the late corner in the total picture of flamenco art, it was this instrument and its music that unified and made cohesive this magical trilogy of music, song and dance. Bits of artfully crafted wood, six strings stretched to the tension of 3 octaves and, magically, an instrument capable of expressing the total range of human joy and sorrow, depth of feeling and emotion, and even the reason for living, when blended with the song of the soul and the dance rhythms of life. The guitar was a natural for flamenco expression, for it has a range of mood, rhythm, depth, soul, tone and melody that comes from the many races of people that gave it birth. One of the main differences between flamenco guitar and other related instruments is the pulse. A gypsy violin wants to cry, to woo to smile, to seduce. To all of this the flamenco guitar adds a pulse to mark the heart beat and rhythms of the soul. Not bow against strings, but fingers against strings, caressing the compás, the mood, the melody, the feeling of music, song, and dance, that is of the earth and spirit. In a quantum leap from the golden era of the café cantante, the playing of the flamenco guitar has gone from pluck and strum to the interpretations of countless flamenco styles and forms, both in accompaniment and as a solo instrument, with a range of technique that was unheard of a few years ago. Since there are no videos or films of this beginning era of the café cantante, when flamenco melted together music, song and dance as a performing art for the pleasure of the public and a bit of a living for the artists, and there are very few real early audio recordings of this flamenco era, it would be interesting to use a bit of \"Sherlock Holmes deductive reasoning\" to guess how this evolution of flamenco guitar accompaniment might have begun to take form with the song and dance. The flamenco guitar as we know it today seemed to develop around the same time as the emergence of flamenco as a performing art in the era of the café cantante. Up until the time of the middle of the nineteenth centuries, guitars in Spain were basically \"Spanish guitars\", and were constructed the same whether they were used for classical, folk or flamenco. It is generally credited to a few artists, such as the famed guitar maker Antonio de Torres Jurado, who constructed the flamenco guitar as we know it today in basic design. With the use of Spanish cypress for the back and sides, constructed in a thinner manner than the thicker, mellower rosewood guitar, he was able to achieve that vibrant and distinctive sound that is a natural accompaniment for the sound of the flamenco cante and has the driving rhythm capability for the percussive sounds of the dance. It is always amazing to see natural evolution occur out of necessity, and subtleties of guitar construction came along as the popularity of flamenco evolved. The traditional flamenco guitar has other differences from the classical guitar both internally and externally. One of these differences is the golpeador which protects the guitar from the finger tapping",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "40",
    "page_number": 40,
    "word_count": 713,
    "article_char_count_full": 4123,
    "article_char_count_review": 4123,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A26",
    "article_text_for_review": "Dear Jaleo: I have been here in Savannah, Georgia, since April; this is probably my last week here. This is one of America's most beautiful towns, with all its huge oak trees (with Spanish moss) and avenues of palm trees. To add to its historic value, the town planners have added twenty-six squares, \"plazuelas\". There is no flamenco here. At the end of May I was pleasantly surprised to hear that the 10th annual \"SPOLETO\" Festival (founded by Gian Carlo Menotti) in neighboring Charleston, So. Carolina, had invited the National Spanish Ballet for three performances: I was an of the few who knew what a tremendous performance they always presented. The Royal National Ballet, previously under the leadership of Antonio Gades and Antonio, had visited the USA in 1983; performing in all states. The Ballet has been regrouped and is now managed by the famous Maris de Avila, partner of Vicente Escudero, Juan Magrilla. The program itself commenced with Seis Sonatas Para La Reina de España and ending in the gigantic tragic work from Greece, MEDEA...probably the first time that a Spanish Ballet transcended its frontier; alas with spellbound success...we only hope that a film version of the MEDEA will be made available to the public. The Ballet was under the leadership of Jose Antonio, a very fine male dancer; the performance of Juan Mata, pirouette continuity of Antonio Marques and the ending bulierias by these men and especially Javier LaTorre (Valencia) stunned the audiences...The internationally known Merche Esmeralda appeared as guest dancer and only danced her \"Solea\"; her flancnco hands are probably unequalled in the business. Ana González played the title role Medes; some of the other principal roles were danced by José Greco's daughter Lsis...most of the choreography was old and treasured including that of Alberto Lorca, Pilar",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "article",
    "pages": "41",
    "page_number": 41,
    "word_count": 300,
    "article_char_count_full": 1851,
    "article_char_count_review": 1851,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A27",
    "article_text_for_review": "Heredia traveled throughout Colorado then in the Chautauqua series of the Colorado Council on the Arts and Humanities. \"Most students of flamenco guitar in Colorado are either my students or students of my students,\" he says, and, in fact, others fly in from New York City and other music capitals to study for two or three weeks with Heredia. RENE HEREDIA At this point in his life, the guitarist is his own manager and has several musical enterprises under an umbrella company called Gypsy Productions, Inc. These ventures include Flamenco Fantasy Productions; Rene Heredia, Concert Guitarist; and Rene Herdia's Flamenco-Jazz Fusion. The latter group-featuring electric bass, percussion, flute, and saxophone in addition to guitar-fuses jazz and flamenco melodies and rhythms. \"Flamenco fusion isn't jazz or salsa,\" Heredia says. \"It is unique, and we are the only ones doing it in America.\" Proud that he is a gypsy, Heredia collects gypsy memorabilia and is knowledgeable about the history of his people, their lifestyles and philosophies over the past 300 years. \"Gypsies,\" he says, \"have been persecuted because they are different in attitudes. They are religious people, usually Catholic, but they live for the spirit of happiness and have a free lifestyle. They usually don't adhere to the rules and regulations of a molded",
    "title": "PROFILES: RENE HEREDIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "42",
    "page_number": 42,
    "word_count": 212,
    "article_char_count_full": 1331,
    "article_char_count_review": 1331,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A28",
    "article_text_for_review": "FLAMENCO MAGNET IN THE PACIFIC NORTHWEST: LA ZAMBRA THEATRE by Hap Sermol Among the numerous centers of flamenco activity in the Pacific Northwest is La Zambra Studio/Theatre in Helvetia, Oregon, located a few miles west of the city of Portland. La Zambra is Portland's flamenco venta (\"hang-out\"). For more than a couple of years now, La Zambra has been the scene of exciting juergas featuring accomplished flamenco artists. Diana Solano is the director of La Zambra Studio/Theatre. She is also the director and featured dancer of Arte Flamenco Dance Company, which performs at La Zambra, local clubs, schools, festivals and private functions. Diana studied flamenco dance in Spain with Maria Magdalena, Paco Fernández, Beti Ortiz, and in New York with Azucena, Estrella Morena, and María Alba. In Madrid, Diana performed at Las Cuevas de Nemesio. In New York, she appeared at Chateau Madrid, The Alameda Room, Lincoln Center with the Boston Flamenco Ballet, La Sangria, La Paella, and Casa Miguel. DIANA SOLANA JALEO - VOLUME IX, No. 2 FALY DE CADIZ AND JOSE SOLANA In addition to performing, Diana teaches flamenco dancing at La Zambra Studio. Her dedication to this art has inspired a remarkable growth in the appreciation of flamenco dancing among her students as well as her audiences. Diana is joined by other talented performers of Arte Flamenco. The cantaora is Faly de Cadiz who grew up singing saetas from the balconies of Seville and Cadiz during the Semana Santa celebrations. She also toured Spain singing zaruelas. Other artists who perform with the company include virtuoso guitarist John Shelton, dancers Susan Ferretta, Diana LoVerso and Maria Moreno; singers Dorothy Sermol and Joan Glassel; guitarist/singer Juanito el Pollino, dancer/singer Manolo Mateo, and guitarist Roberto Lorenz. Diana's accompanist on the guitar is her husband, Jose Solano. Jose studied guitar in New York with Pedro Cortez, Guillermo Rios, and Pepito Priego. Arte Flamenco has been funded by the Metropolitan Arts Commission, the Oregon Arts Commission, and the National Endowment for the Arts. If you plan to be in the Portland area and wish to attend or participate in one of the future juergas, please contact Diana at (503) 647-5202 or write to: Arte Flamenco, Rt. 1, Box 664, Hillsboro, Oregon 97124. \"GUAJIRAS DE LUCIA\" transcribed by: M. Haas in staff notation plus tabulation Published by: Gitarren-Studio E. & M. Haas Blissestr. 54 D-1000 Berlin 31 in West Germany DM 9,50 in US $3.50 +p&p",
    "title": "PRESS RELEASES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "43",
    "page_number": 43,
    "word_count": 403,
    "article_char_count_full": 2494,
    "article_char_count_review": 2494,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
