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
    "article_id": "JALEO_1987_SPRING::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nINTRODUCTION Throughout 1984 and 1985, Brad Blanchard of Badajoz sent me articles from the newspaper Sur about flamenco in Málaga. There were a large number of interviews with artists of Málaga by Gonzalo Rojo. I never knew what to do with the articles, since the artists were largely unknown. Recently, while trying to figure what to do with the articles, I realized that, put together, these interviews presented an interesting picture of flamenco in Málaga and an insight into what it is like to try to become a professional flamenco in Spain. The flamenco of Málaga if different in many ways from that of the other provinces of Andalucía, particularly Cádiz and Sevilla. The focus is on the fandangos, especially the malagueñas and verdiales, and there is great interest in flamenco peñas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Manolo\"]\n\nthe fandangos, especially the malagueñas and verdiales, and there is great interest in flamenco peñas [clubs] and contests. I think you will also become aware of how defensive the Malagueños are about their status in the flamenco world. RECORDINGS BY ARTISTS OF MALAGA It appears that the hour has come for artists in Málaga to record. A few years ago, we only had records by Antonio de Canillas, Pepe de la Isla, Ángel de Alora, Cándido de Málaga, Manolo de Málaga, Niño de las Moras, and little more. Today, fortunately, there are many who are seeing their cassettes or albums in the windows of the record stores and dreaming of seeing themselves in the company of the great figures of the cante. Juan Casillas, Pepe Vergara, Capote de los Claveles, Talete de Ardales, Pepe de Campillos, Niño de Peñarrubia, Pepe de Cañete, Chaqueta de Fuentepiedra, and a long ecetera of young cantaores have made recordings. We have to thank the Málaga label, Fonodis, for their labors in making it possible for the artists of Málaga to have their voices recorded and be part of the many in the Andalucian flamenco concert. Nío de Churriana: A Veteran Cantaor One of the most veteran cantaores of Málaga is José Cortés Sánchez \"Niño de Churriana,\" who was born on the Zapata ranch in Churriana on April 25, 1905. Without knowing how to read nor write, he is the author of an infinite number of verses that have been sung by many artists, and he has sung with the top figures in flamenco. When did you begin in the cante? \"I was seven or eight years old and, instead of going to school, I went to the taverns to sing and they gave me a perra chica or a perra gorda [5 or 10 cένtimos; 100 cένtimos = 1 peseta], which was something, considering that my father, working in the Martinete ironworks, was earning 4 1/2 pesetas, or 18 reales [1 real = 25 cένtimos] and there were seven of us children.\" How did you become known outside of Málaga? \"It happened that El Cojo de Málaga heard me sing one night and he liked the way I did it. The\n\n[ENDING CONTEXT]\n\nthe cantaor Cândido de Málaga, quitarist Diego Vargas, and dancers Encarnita, Pepi Ortiz, and El Duende.\" Where have you performed? \"On the Costa del Sol where my ballet is highly sought after by hotels, businesses, peñas, etc. We have also been in Austria, Germany, France, Maruecos, and Switzerland, representing Spain.\" What styles suit you best? \"In reality I do all styles of dance, but I especially love to dance\" Luisa Vera Who are your favorite dancers? \"Fernanda Romero and Manuela Carrasco.\" Is there a love of the baile in Málaga? I believe so, and we have very good professionals here.\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FLAMENCO ARTISTS OF MALAGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "poem",
    "pages": "6-15",
    "page_number": 6,
    "word_count": 4789,
    "article_char_count_full": 26537,
    "article_char_count_review": 3641,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Manolo"
      }
    ]
  },
  {
    "article_id": "JALEO_1987_SPRING::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby El Tio Paco Regardless of how it is embellished, the quintessential expression of flamenco (at least in this writer's humble opinion) is found in El Cante. It is also the form most difficult to understand, particularly for a non-Spaniard (or non-Andalucian), which explains why very few cantaores have ever \"made it\" outside of Andalucía, as compared to the countless flamenco guitarists and dancers who have had extremely successful careers outside of Spain. In fact, most of the great cantaores of all times never travelled much beyond the confines of their provinces. Some never even left their hometowns. That el cante is seldom fully appreciated outside of Spain shouldn't come as a surprise. Even within Spain's \"other\" provinces there is a feeling that it is una cosa de Andalucía (an\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"expression\"]\n\nl times never travelled much beyond the confines of their provinces. Some never even left their hometowns. That el cante is seldom fully appreciated outside of Spain shouldn't come as a surprise. Even within Spain's \"other\" provinces there is a feeling that it is una cosa de Andalucía (an Andalucian \"thing\"), and it is only in this blessed land where el cante is recognized (and even then, not by everyone) for what it is, the ultimate in artistic expression. Perhaps the ability to do it (or listen to it) requires some strange genetic code passed down through the generations from times immemorial, carrying the call to prayers from the arab muezzin, the sephardic chant, or the quejio gitano, or a mixture of all. Whatever it is, for the rest of us, it provides a vicarious vehicle of expression and catharsis like nothing else does. Remember all those things you wished you could have said to your father, but he was too old and frail to understand? Well, El Piconero de Arcos does it for you. How about the feelings that welled up in your throat when you sat by your small child's crib watching him sleep? El Turronero says them better than you ever could. And the girl you fell madly in love with as a young boy, but never had the courage to speak to her? Juanito Villar speaks to her eloq\n\n[ENDING CONTEXT]\n\n\"El Fillo\" (died in Triana, 1878). Called \"the King of cantaores\". Reputedly gifted with a hoarse gravely voice that is desirable in gypsy singing. 3 Untranslatabel, but denoting an artistic stance. 4 \"Nos va\" 5 Line of singing 6 \"Con angel\" (with \"angel\") * * * OF A CONVERSATION WHICH AN 'AFICIONADO AL CANTE' HAD WITH MAESTRO ANTONIO MAIRENA [from: Flamenco; translated by El Tio Paco] by José Manrique Lopéz Aficionada -Tell me, Don Antonio What does Flamenco consist of? I have been trying to fathom it, but can't quite understand it. Maiena -Brother, dear brother, Flamenco is a mystery.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "EL CANTE FLAMENCO: THE ULTIMATE CATHARSIS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 1070,
    "article_char_count_full": 6222,
    "article_char_count_review": 2919,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "expression"
      }
    ]
  },
  {
    "article_id": "JALEO_1987_SPRING::A6",
    "article_text_for_review": "[from: ABC de Sevilla, June 12, 1987; sent and translated by El Tio Paco] He belongs to that group of young \"veterans\" of a hundred battles; he knows his business, the effort of giving himself entirely to his profession, and finding, like many others, the meager financial rewards from day to day, from dawn to dawn. Nano de Jerez is first class, even if he is not among those who make the headlines. Maybe it is a matter of luck, or not being in the clique chosen by the powerful minorities who control the flamenco scene - a scene which, unfortunately, has become somewhat stale, and one that can collapse dramatically, because the crisis in the world of flamenco is evident. Nano de Jerez, Cayetano Fernández, the \"fragua\" (blacksmith shop) of Tio Juane and his ancestors. Tio Juane is a blood relative of Adela la de Chaqueta, and those bonds express themselves through Nano's sounds. The vineyard and the salt flats. But Nana is quintessentially a \"Jerezano\"... \"I was baptized in Sam Miguel\" [Translator's Note: 17th Century Church next door to the El Carbonero's Guitar Academy, and a few steps away from Cristobal El Jerezano's Dance Studio.\" \"It was there they sprinkled the [holy] water on Manuel Torre, La Paquera, Lola Flores, Agujetas' father [the Elder]...My maternal grandmother sang and danced very well. I am in this because of a very natural reason;",
    "title": "NANO DE JEREZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 233,
    "article_char_count_full": 1367,
    "article_char_count_review": 1367,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SPRING::A7",
    "article_text_for_review": "[from: Diario De Jerez, April 6, 1986, translated by Marysol Fuentes] A cantaor from jerez, practically forgotten today, even though his singing was always at the level of the best of his time and he was a great master of the copla, was Don José Cepero. He was an artist of high quality who sang with Chacón, Manuel Torre, La Trini, La Serrana, La Niña de los Peines and other great ones, they sang everything well, especially a fandango of his own creation, and he knew everywhere and at all times how to be a flamenco \"caballero\", a true gentlemen. After Chacón, Cepero inherited his title of \"Don\" for his art and his class which set him apart from any other artist. Perhaps for his good manners and his high education, besides the fact that his interpretation of the cante was extraordinary, Don José was designated cantaor to the Spanish Royal Family and he was the favorite cantaor of general Primo de Rivera. José began singing in Jerez when he was only nine years old. He was born in the house at number nine, Calle San Onofre, in 1887, the son of a beautiful woman who was called \"La Brisa\" (the breeze). At thirty-one years of age, when he already had been singing in Madrid for a long time, he was featured with La Niña de los Peines in the first opera flamenco show ever to be organized in Spain. After that, he recorded numerous records and was become famous with the nickname \"Poeta del Cante\", due to the fact that he composed his own lyrics. Cepero established himself in the Spanish capital, traveling around the country on several occasions as the star of several shows. On one of them, in 1955, he arrived in Jerez where I had the opportunity of interviewing him, for the magazines, Digame and El Taurino in his dressing room at the Villamarta Theater, appearing that evening with his nephew-grandson, Paco Cepero, who was still a child with no idea that in time he would become the phenomenal guitarist that he is today. There, the maestro told me that he hadn't been in Jerez since 1908, that he started singing when he was nine years old -- in the Cine Escudero, in Cádiz, with the famous Chacón and Fosforito, making a salary of six pesetas a day. \"I always,\" he told me,\" admired Chacón as an all-around cantaor, one of those that is born once in a century, and Manuel Torre as a seguiriya singer.\" Who do you like best of the cantaores of this time? \"I only like Mairena and Caracol, the rest of them are nothing special.\" What other cantaores have you admired of those that you have known? \"Of the women, La Trini, La Serneta and La Serrana. Pastora could sing everything well. Of the men, Juan Varea, Vallejo and El Niño de Barbate are very good aficionados What guitarists would you mention of those that have accompanied you?",
    "title": "DON JOSE CEPERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 497,
    "article_char_count_full": 2754,
    "article_char_count_review": 2754,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SPRING::A8",
    "article_text_for_review": "In 1962, a film was made of the play. Mañas himself wrote the script and Carmen Amaya starred as the mother (she died shortly after completing the film). Yet another stage version appeared in 1980 at the Reina Victoria in Madrid, with Felipe Sánchez as the leading man (he is the choreographer in this new version) opposite La Contrahecha. Rosa Duran was his mother. By then, the flamenco sections had increased notably. The guitarists were Perico el del Lunar, Curro de Jerez, Carlos Habichuela and Luis Pastor. The singers were Chaquetón, Rafael Romero and Carmen Linares. Paco de Lucía is enthusiastic about writing his first composition for ballet. The music was arranged for symphony by Amargos, but he made every attempt to maintain the authentic flamenco flavor for A moment during a rehearsal for Los Tarantos. (photo by Ricardo Gutierrez)",
    "title": "FLAMENCO NEWS BRIEFS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 140,
    "article_char_count_full": 847,
    "article_char_count_review": 847,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
