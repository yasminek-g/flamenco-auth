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
    "article_id": "JALEO_1981_10::A7",
    "article_text_for_review": "SCORES: 1. Paco de Lucía 16.07 notes/sec. 14.81 notes/sec. 2. Miguel Vega 14.00 notes/sec. 3. Serranito 12.13 notes/sec. 4. Sabicas 5. Manolo Sanlúcar 12.12 notes/sec. Please note: THE EFFECTS OF HUMIDITY As the seasons change we must maintain the guitar's environment at a constant humidity. In my workshop, as in most modern workshops, the temperature and humidity are carefully controlled. To maintain stable wood conditions during construction, I keep the relative humidity at 50% and the temperature between 70-80 degrees Fahrenheit. Here in San Jose, the climate is almost ideal for the guitar. Only during a few hot, dry weeks in the summer does the moisture content of the air drop so that my humidifier must run in order to maintain the 50% level. During the remainder of the year the humidity in my shop naturally stays about 50%. I am originally from the East Coast and well aware that the stable conditions I enjoy are not the norm.",
    "title": "LESTER DEVOE ON GUITAR CARE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_10",
    "year": 1981,
    "language": "en",
    "article_type": "article",
    "pages": "20",
    "page_number": 20,
    "word_count": 159,
    "article_char_count_full": 944,
    "article_char_count_review": 944,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_10::A8",
    "article_text_for_review": "WHAT IS DUENDE? Have you ever performed with \"duende\"? \"Duende\" could be described as flamenco's ultimate moment, when true sound is transmitted. I would like to explore this a bit more in depth. My observations are not authoritarian conclusions, so I'd like to see any disagreement or further insight in the form of letters to the editor. The following three questions shall be the point of this discussion: 1. Who is allowed to have \"duende\"? 2. Is the \"duende\" that is transmitted equal to the \"duende\" received? 3. Is \"duende\" only possible with two or more performers? The first question is very interesting. There seem to be two evident frames of mind concerning it. Just as there are those who abdicate responsibility for their own well-being, there are those who abdicate \"duende responsibility.\" Hero worship gets its start here. In this frame of mind the feeling is that there is some higher power in control of our lives. Then it follows that we must surrender to it. In the absence of tangible evidence of the higher power, we surrender to other human beings. The other frame of mind is just the opposite. It could be summarized in the statement: \"Céllate y escucha!\" Here the frame of mind demonstrates a desire for control over the situation. Of course it is dangerous to generalize like this. Obviously there are more frames of mind than this. But to answer the question, it is the second type that is allowed to have \"duende\".",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_10",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 249,
    "article_char_count_full": 1442,
    "article_char_count_review": 1442,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_10::A9",
    "article_text_for_review": "August Juergen DUENDE APPEARS AT MIDNIGHT by Juana De Alva The home of Bart and Joan Boyer had all the requisites for a perfect juerga setting -- a Spanish style home, a lighted patio, several indoor areas for the development of duende. Many Jaleístas had gathered by nine or ten and although there was much merriment and Damian burned up his fingers playing sevillanas, things had not really gotten rolling. Jaleístas' juergas often manifest the opposite of the \"Cinderella\" phenomenon. At the stroke of midnight instead of everything disintegrating, the juerga came to life -- a group of \"tunas\" (Spanish troubadors), in costume, with guitars, mandolins and tambourines arrived followed shortly by the performers from the Ocean Playhouse and El Moro. Soon all corners of the house were bursting with music and dance. Besides the \"tunas\" there were some other out-of-towners who added to the juerga: from Mexicali, dancer, Magdalena Cardoso, from the Los Angeles area, guitarist, David De Alva, and a brand new face to our juergas, guitarist Glicerio Mera from the San Francisco area We wish to thank our hosts for offering their home and Cuadro B headed by new cuadro leader, Vicki Dietrich, and all the rest who pitched in. BELOW: TUNAS GROUP PLAYS WHILE THE ROMEROS DANCE & PILAR MORENO LOOKS ON",
    "title": "-AUGUST JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_10",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 216,
    "article_char_count_full": 1299,
    "article_char_count_review": 1299,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_10::A10",
    "article_text_for_review": "This month's juerga will be held October 17th at the Ocean Playhouse restaurant, 7pm, 691 El Cajon Blvd., tel: 714/442-8542. For further details, see Junta Report. (GAZPACHO; ) On both records Manzanita plays guitar and sings in a modern style. His material ranges from pop to salsa to flamenco. \"Libérate\" is the opening selection, a slow moving rumba with a modern vocal approach and nice guitar work. The first impression might be that Manzanita is the male version of Las Grecas. However, the next track shows he can really swing with the bulerías rhythm; \"Capricho\" is one of three bulerías on the album, but the only solo. It starts with an aire reminiscent of Enrique Melchor, later changing to a slower version of the \"Capricho Español\" by Rimsky-Korsakoff. \"Espíritu Sin Nombre\" is a poetic attempt to describe an undefinable spirit present in everything. Many records have lots of philosophy in the letra. The sleeve of this record has all the letra printed so the listener can follow along. \"Regimiento Los Gitanos\" is a very flamenco interpretation of bulerías. Manzanita turns on with plenty of \"rajo\" and some hot guitar licks. How can he perform like this and then like a pop star on the same record? I guess the best way to put it is that he has a split personality. Side one finishes with \"Yo Te Amaré,\" a happy sounding rumba, easy to dance to in a disco club. Side two begins with \"Ni Contigo Ni Sintigui,\" a slow rumba featuring brass, drums, guitar, and bass. \"Gitano\" is a fast rumba done with a salsa aire. The vocal section comes in half way through the song. \"Paloma Blanca,\" another slow rumba, features violins, synthesizer, and drums. It is followed by \"Sarairo,\" more of the same. The closing number is \"Romance Árabe,\" a mysterious sounding bulería, done in the tarantas position on the guitar. As on \"Regimiento Los Gitanos,\" here Manzanita sounds more like a cantaor than a cantante. Both Manzanita releases were winners of the coveted \"Disco De Oro\" sticker for having sold more than 50,000 copies. I'm glad he recorded the bulerías for us though. That may silence some of his critics. -- Guillermo Salazar",
    "title": "-OCTOBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_10",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "24",
    "page_number": 24,
    "word_count": 367,
    "article_char_count_full": 2139,
    "article_char_count_review": 2139,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_10::A11",
    "article_text_for_review": "by Caballero Bonald PART X - ARCOS DE LA FRONTERA Arcos de la Frontera has a very special place in the history of flamenco. Its very geographic situation brings it close to the expressive mood of Jerez and Triana and, at the same time, to the rough cantes of the mountains. In Arcos, one of the most passionate figures of flamenco, Tomás el Nitri, was born; all that remains of him today is a creative pattern -- not much is really known about his life, although it is supposed that something is known about his singing. This chapter of personal styles, referring to the great cantaores whose fame has reached us today, is very slippery. What were they definitely like, the tonás and siguiriyas of those primitive artists? Could they have been the same, exactly, as what we know of them today? Oral transmission in this sense is very confusing and, on occasion, doesn't offer a sufficient guarantee of credibility. It is very possible that this question of the authenticity of the personal creation is subject to a long process of re-elaborations and adaptations. The styles which are attributed to cantaores long-gone had, necessarily, to reach us by way of those who reproduced in their own way the characteristics of those primitive cantaores. But we cannot bring about any type of direct investigation to gauge what is true and what is false in those examples. Perhaps neither is it very important to rigorously individualize what belongs to the totality of a determined nucleus of the gypsy/andalusian people. Each cantaor interprets, in his way, the flamenco legacy; without expressive personality, everything would be converted into a mummified replica. The first useful recordings -- the immediate proof -- is possible only in the era of Manuel Torre, Chacón, Juan Breva, El Tenazas, El Gloria. Before them, everything is supposition. We had already had, in Arcos, the opportunity of hearing a very special gathering of expressive variants, mostly in what is referred to as siguiriyas, soleares, serranas and saetas por tonás. The singers were small-town people, without any relation to professionalism -- mostly day laborers. We asked about them: One had emigrated, another, Miguel el Mochuelo, for example, a mule-driver, had died a few years back. But we managed to gather, in a ramshackle restaurant, three local cantaores: Jerónimo el Abajao, Manuel Zapata and Miguel Camballá. They spoke to us about a gypsy nicknamed El Cojo, who we unsuccessfully searched for in the impressive labyrinth-like streets of the Alto district, which is hung -- higher than the eagles -- over the ravine of Guadalete. Jerónomo el Abajao, who died a few months ago, was a robust man, getting on in years, who lived by selling lottery-tickets. He sang on rare occasions, as did Miguel Camballá, an elderly tavern owner who enjoys a well-deserved local fame as a cantaor in private gatherings, although singing very little due to advanced age. Manuel Zapata, a truck driver, is the only one of them whose name has sporadically left the borders of Arcos. It was hard to get the three together; perhaps it wouldn't have happened without the aid of José María Velázquez and Antonio Murciano, experts in Arcos and in its flamenco history, and the aid of Juan Brito, a courteous aficionado who accompanied us from Sevilla. We didn't start recording until well into the early morning hours. The nocturnal silence of Arcos has a special intensity. It is as if the cante casts itself down the geological grandeur over which the village rears up, and sinks below, breaking the age-old belt of shadows. Few Andalusian villages -- Andalucía is the village -- are more beautiful and dramatic than this one. Truly, here one wouldn't be able to sing more than a heart-rending cante of terrible echoes and unforeseen emotional depths. Arcos, from this perspective, is linked more to the flamenco accent of Jerez than the varied mountain uplands...Within the foothills of the mountain system that stretches out between Arcos and Ronda, the cante approximates the popular folk styles more and more as one approaches the province of Málaga. Between the rodeña and the soleares of Arcos, for example, the geography has worked very profound changes with respect to the roots of the cante in its foundational confines. On the other hand, it is curious that these three cantaores from Arcos also use, as their usual mode of expression, the fandangos, malaguenas and the tarantas and cartageneras of the Levante. As we were leaving Arcos, with the sun high over the dusty olive orchards, we were thinking of our conversations with Miguel Camballá, Manuel Zapata and Jerónimo el Abajao. For them, the cante is like a forgotten, battered relic. There remains only traces, isolated examples, precarious signs of something that was like a sacred, untouchable manifestation of the truth of the people. It is common to find these attitudes",
    "title": "ARCHIVO: PART X",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_10",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 813,
    "article_char_count_full": 4901,
    "article_char_count_review": 4901,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
