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
    "article_id": "JALEO_1985_10::A11",
    "article_text_for_review": "JUAN VAREA IN MEMORIAM [from: Candil, Sept.-Oct., 1985] by Manuel Yerga Lancharr According to the information that I have acquired, Juan died at his home address, 19 Plaza Manuel Becerra, in Madrid, the 8th of November, 1985. Already at fifteen Juan used to spend long hours in Madrid talking to and learning from Fernando el Herrero, Perico el del Lunar, Manolo de Badajoz, Maneli, Calcetines y Pepita Caballero. All of them became his unconditional teachers as soon as they realized the excellent artistic qualities that the young man from Burriana had. He had so much interest in learning everything and in proving that he was capable of doing that it didn't take him long to start to sing with one who, later on, would become his friend; Perico el del Lunar. After that he went to Barcelona to record with the cantora, Locita Cabello, who had previously obtained the contract with the recording company. From the sixties until the present time, every chance I had to go to Madrid on my frequent political visits, if I had any free time at all, I tried to visit with Juan, if only for the enjoyment that I got just talking to him. He knew so many of the old cantaores! Juan was such a good and humble person that it used to take him no time at all to totally captivate me and take me back to the twenties. We have written many a letter back and forth through the years! How very many times I have asked him for information on this cante or thst cantaor! He was fortunate enough to know Fosforito, Chacón, La Rita, La Pastora, Mojama, El Torre and so many other great artists, and he learned something from almost all of them, thanks to his immeasurable aficion and his perfect musical ear. However on everything he sang, which wasn't a little, he imprinted his personal style, making everything seem his own. It is for this reason that many aficionados from Málega believe he created a style por malaguñas, when the truth is that he used to interpret a style by \"La Trini,\" which one day he heard Bernardo el de los Lobitos sing. He used to tell me with the goodness that filled his conversation, \"That's what they say in Andalucfa, thst I have created cantes, but in reality I have only created a couple of fandangos, one of them inspired by the cantaor and guitarist 'El Rubio Para'. That fandango, that for better or for worse is mine, is credited around Huelva to my dear friend, El Niño Leon-may he reat in peace. He, however, never had any trouble telling the truth--that he learned it from me.\"",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "27",
    "page_number": 27,
    "word_count": 451,
    "article_char_count_full": 2504,
    "article_char_count_review": 2504,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_10::A12",
    "article_text_for_review": "GUAJIRAS DE LUCIA This music transcription of Paco de Lucía's early guajiras was sent to me by its publisher, Gitarren Studio--Musikverlag (BlissestraBe 54, D-1000 Berlin 31 (West), Germany), who hold the copyrights as of 1985. Paco de Lucía's version of guajira is not, in my opinion, the definitive guajira; I think Manolo Sanlúcer, among others, went on to create solo compositions with much more aire of the guajira--light, sensuous, syncopated and Latin. However, the \"Guajira de Lucía\" has been a popular piece, is loaded with musical ideas, and certainly a technical challenge. This music is very well written, in both standard notation and tablature, as you can see from this small sample: In the United States, this music is said to be available through: Guitar Solo Inc., 1411 Clement St., San Francisco, CA 94118. by Paco Sevilla",
    "title": "JUAN VAREA: IN MEMORIUM",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "28",
    "page_number": 28,
    "word_count": 137,
    "article_char_count_full": 840,
    "article_char_count_review": 840,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_10::A13",
    "article_text_for_review": "[from: $ \\underline{\\text{New York Times}} $, Dec. 12, 1985; sent by Rodrigo] by Jack Anderson",
    "title": "GUAJIRAS DE LUCIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 29,
    "word_count": 15,
    "article_char_count_full": 94,
    "article_char_count_review": 94,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_10::A14",
    "article_text_for_review": "The group takes its name from that of its director and leading guitarist, Manuel Morao, and the program featured much of his music. Often, he was joined by his brother, Juan Morao, also a guitarist. In addition, there were songs composed by José Vargas, one of the group's vocalists. The other singers were Luis de Pacote and Lorenzo Galvez. The guitar music ranged from the ruminative to the rhapsodic. The singers' throaty wails and lamentations sounded as if they came from the depths of their souls. At times, when the spirit moved them they even burst into bits of dancing. But most of the dancing was left to Ana Maria Blanco and Manuela Carpio. The two were paired in \"Caracoles,\" \"Mirabras\" and \"Bulerias.\" But each also presented several fine solos. Miss Blanco held the attention for the way her arms moved in a serpentine manner while her feet beat crisp rhythmic patterns on the floor. She was particularly remarkable for her ability to make her heelwork murmur through long passages of trills. Miss Carpio was altogether different in manner. What made her striking was the way she would whip up excitement and threaten to dance herself out of control while, all the time, remaining in perfect charge of the situation. Indeed, one could tell that she disdained mere agitation and, although the audience kept cheering her on, she refused to acede to its demand for spectacle until she was good and ready to display her virtuosity. The choreographic sequences would turn into crescendos and the steady patter of heelwork would be interrupted by brushes and kicks to the side and sudden stamping exclamations of feet against floor. It was this sort of excitement that made El Morao seem a little company with a great heart. THE GROUP WHICH COMPRISES \"JEREZ POR EL MUNDO\" RETURNED TO JEREZ AFTER TWO MONTHS OF SUCCESS IN THE AMERICAS. Photo Iglesias",
    "title": "EL MORAO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 29,
    "word_count": 316,
    "article_char_count_full": 1857,
    "article_char_count_review": 1857,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_10::A15",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFrom the Editor: In the last issue of Jaleo, we reviewed a new record by the British (now, apparently, Spanish) guitarist, Juan Martin. The following is material that he sent us--to give another side of the story. Keep in mind that the Spanish newspaper article prints only what he says about himself. The articles from the British guitar magazines give the critics point of view. * * * JUAN MARTIN; THE SUBTLETY THAT GIVES FEELING TO THE STRINGS OF THE GUITAR [from: La Tribuna de Marbella, Aug. 11, 1985; sent by Juan Martin, translated by Paco Sevilla.] by Eduardo Palascios Juan Martín can be considered, along with Paco de Lucía, as one of the best known, internationally, of the Spanish guitarists. He has said a half million records and was number ten on the best selling record list in Great\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nardo Palascios Juan Martín can be considered, along with Paco de Lucía, as one of the best known, internationally, of the Spanish guitarists. He has said a half million records and was number ten on the best selling record list in Great Britain with his theme for the television series \"The Thornbird.\" He has given concerts in the United States, Japan, Switzerland, France, Italy and England. He has played with Al DiMecla, Santana, Miles Davis and many others. His flamenco guitar method is one of the most used in all of Europe and has also written two other books of musical compositions. Esteblished for some years in England, where he married an English woman, \"...she is very flamence, speeks Spenish with an Andelucfan accent--a bland gypsy!\" with whom he has a six year old son, this man from Mélage, bom who knows how many years ago--he won't say--in La Carihuela, desires to return to the land of his birth, for the fog of Landon doesn't allow him to feel the vital reys of the sun. Juan Martín, who has a house in Estepana, doesn't let a single summer go by without spending a few days on his Costa del Sol. His stay in his hot Spain seems to revive his aficionado for the strings and he takes advantage of all invitations that come up to delight the listener with his deeply felt strums. Last Thursday he played in Marbelle, invited by the pefia flamenca \"Sierra Blanca,\" which is organizing the \"I Curso Internacional de Guitarra Flamenca\" [The First International Guitar Course]. He believes that these undertakings will be highly positive in contributing to the efforts to give the flamenco guitar its just place within the art. For Juan Martin, the guitar, \"...has had very bad luck.\" Besides the fact that very few reco\n\n[ENDING CONTEXT]\n\n(gusjiras); Lorca's Dream (grenadinas); Goya's 3rd of Mey (seguidilla); Danza (zepateado). by Grehm Wade Juan Martin's three previous solo albums were issued in 1973, 1976 and 1977, respectively and now at last we have ever further opportunity to listen to the true guitar of this imaginative and virtuosic flamenco artist. In this recording Juan Martin both works within the traditional flamenc forms and creates his own material, emerging from an indebtedness to the great Nino. Ricardo to that moment where a guitarist in this idiom defines his own creatifs identity in a distinctive style.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "JEREZ POR EL MUNDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "30-31",
    "page_number": 30,
    "word_count": 1310,
    "article_char_count_full": 7700,
    "article_char_count_review": 3358,
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
  }
]
```
