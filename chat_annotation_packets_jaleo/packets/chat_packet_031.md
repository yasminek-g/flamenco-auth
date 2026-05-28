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
    "article_id": "JALEO_1978_09::A9",
    "article_text_for_review": "by Paco Sevilla One of the basic themes and moving forces in flamenco is romantic love. The Spanish woman is placed on a pedestal to be worshipped by the men as a romantic object -- until she marries, when she becomes a workhorse and receives a different sort of reverence as a mother; this attitude is gradually changing in Spain, and the content of flamenco verse is also likely to change -- witness the verses of Lole y Manuel as an example. In the flamenco verse of the past, there has been a very strong, almost exaggerated, preoccupation with romantic expression that has often resulted in beautiful poetry. The verses sung in the cante chico are often simple declarations of love or attempts at flattering or persuading the loved one. In addition, there may be humor in such cantes as rumba, tangos, alegrias and bulerías; the fandangos will often have a humorous or ironic twist in the final line. The romantic verses of the solea are often philosophical or deal with the frustrations of love and the suffering it can bring; the fandangos grandes also frequently deal with romantic suffering. The following are some of my favorite traditional verses; they come from many different cantes, some of which I can no longer identify or never knew. What I wish to share is the beauty of the poetic content. The English translations lose a lot of the original feeling, but it is hoped that they will be better than nothing for those who do not read Spanish. Here are two examples of romantic flattery: Al verte, las flores lloran cuando entras en tu jardin porque las flores quisieron todas parecerse a ti.(bulerías) Upon seeing you, the flowers cry when you enter into your garden, because they all wish to be just like you. Tiene el arco del cielo siete colores distintos, pero no tiene el color moreno que es color mas divino de la mujer que mas quiero! (fandangos de Huelva) The rainbow has seven different colors but it does not have the color \"moreno\" (moreno=dark skin, hair, eyes) which is the most divine color, that of the woman I love. (photo from Bob DeVore) Here are some romantic declarations: Una alcarraza en tu casa, chiquilla, quisiera ser, para besarte en los labios cuando fueras de beber A water jar in your house, girl, I would like to be, in order to kiss your lips whenever you go to drink. Cuando te veo venir a lo lejos de la calle, le digo a mi corazón que tenga paciencia y calle. When I see you coming from far down the street, I say to my heart, have patience and keep still. These declarations can become much more serious: Yo me agarro las paredes cuando te encuentro en la calle, chiquilla, pá no caerme. (solea) I grab onto the walls when I meet you in the street, girl, in order not to fall. Aunque ponga en tu puerta canones de artillería, tengo que pasar por ella aunque me cuesta la vida. (alegrias) Even if they put in your doorway, artillery canons I have to pass through it, even if it costs me my life. El verte me da la muerte y el no verte me da vida. Mas quiero morir y verte que no verte y tener vida. Seeing you causes my death, not seeing you gives me life. I would rather see you and die than not see you and have life When there are the humorous and the clever Vente conmigo. Dile a tu mare que soy tu primo! (cantiñas) Come with me. Tell your mother that I am your cousin! Yo soy uno y tu eres una; uno y una, que son dos, dos que debieran ser uno, ay, si lo quisiera Dios. I am one and you are one; one and one are two, two that should be one, ay, God willing! El querer del hombre pobre es como el del gallo enano; que en querer y no alcanzar, se le pasa todo el año. (fandangos) The love of a poor man is like that of the dwarf rooster; he passes all of the year desiring, but not being able to reach. We have already received two responses for hospitality houses (see $ \\underline{\\text{Jaleo}} $ Vol. II No. 1). Traveling, foot-weary flamencos now have a place to touch down in Seattle Washington and Downey (Los Angeles area). It goes without saying that in San Diego, the home of Jaleistas, no flamenco would be left without a place to rest his guitar or castanets. If you have s spare corner to offer a fellow member from another city, write to Juana care of $ \\underline{\\text{Jaleo}} $.",
    "title": "Romantic Verse in Flamenco",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "15, 16, 17",
    "page_number": 15,
    "word_count": 786,
    "article_char_count_full": 4248,
    "article_char_count_review": 4248,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_09::A10",
    "article_text_for_review": "(From the popular Spanish magazine \"Lectura\" (date unknown); sent by Susanne Hauser) \"That an Andalucian guitarist accompanies a flamenco cantaor is not news, but there arrived today a photo of a North American, Rodney Lee Holman, who has been in Spain six years, is married to a girl from Ronda, and accompanies flamenco cantaores on the guitar; judging from the press clippings, he does it well, as well as any other from this country.\" His \"nombre de guerra\" (artistic name) is Rodrigo de San Diego. His first record, on which he accompanies the singer Curro Lucena, came out on the \"Olympo\" label. (See $ \\underline{\\text{Jaleo}} $ Jan. 1978 for more on Rodrigo). In the photo, Rodney, alias \"Rodrigo de San Diego\" in action.",
    "title": "Un Guitarista de Flamenco Norteamericano",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "article",
    "pages": "17",
    "page_number": 17,
    "word_count": 123,
    "article_char_count_full": 729,
    "article_char_count_review": 729,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_09::A11",
    "article_text_for_review": "Michael Hauser began playing the flamenco guitar at the age of twenty, while still a student of forestry at the University of Minnesota. After working for the U.S. Forest Service in Alaska for six months and Liberia, West Africa for two years as a rubber plantation superintendent, he traveled to Spain in 1963, where he studied flamenco intensively with Luis Maravilla and later, Niño Ricardo and Justo de Badajoz. Since then, Mr. Hauser has returned to the woods only for relaxation. He has made several subsequent trips to Spain to study and work, whenever possible, mostly in the dance studios as an accompanist. He has performed many concerts in the U. S. and Canada as soloist and as a part of the guitar duo, Michael & Anthony Hauser. In their joint recitals, the Hauser Brothers perform flamenco and classical guitar duets and solos. Earlier in his career, Michael Hauser was accompanist for \"María Fernanda and her Bailes Españoles,\" and for the dance team of Alfredo y Maruja. Currently, of course, he performs with his wife Suzanne, Anthony, and María Elena \"La Cordobesa,\" with their company, \"Suzanne Marie and Trio Flamenco.\" They are a part of the Dance Touring Program of the National Endowment for the Arts and are currently under the management of Robert Gewald Associates of New York. The Hausers all teach at the Guild of Performing Arts in Minneapolis and often appear in nightclubs in that city. One of their more interesting tours took them on a two month performing and teaching visit to Alaska. Part of the time they worked with Eskimo children, teaching them flamenco rhythms. A strange place for flamenco, north of the Arctic Circle! Suzanne Marie Hauser has been dancing since she was a child. When only seven, she danced with the Schmidt Indian Band and, as a teen-ager, was winning prizes as a rock and roll dancer. One of these prizes included a tour with Ann Margaret and Donald O'Connor. Flamenco, however, was always predominant in her mind, and, until she was able to travel to Spain to study, she worked in Minneapolis with Miss Lilian Vale and María Fernanda. Teachers in Spain, and here as well, include María Alba, María Rosa Merced, Tomás de Madrid, Merche Esmeralda and others. Suzanne has worked in Spain previously in the Company of Rafael de Córdoba, taping a program for Spanish National Television, and at the Madrid flamenco tablao, \"Las Cuevas de Nemesio.\" TRIO FLAMENCO CAME, PLAYED AND CONQUERED (From the Dailey Independent \"Desert Merry-Go-Round,\" Ridgecrest, Calif., Fri., Jan. 21, 1977) This year the Indian Wells Valley Concert Association apparently can't lose for winning. Wednesday night the association did it again, by staging another concert spectacular with the appearance of the Trio Flamenco. And, people are getting smart and realizing what a bargain in entertainment they have here as the theater was almost packed. Trio Flamenco consists of Anthony and Michael Hauser, classical and flamenco guitarists, and Suzanne Marie Hauser, dancer, who is Michael's wife. it is as one guitar playing. Their repertoire went from classic guitar duets and solos to flamenco and employed music which had been written for the lute, the grandaddy of the guitar and harpsichord. When the Hauser brothers perform, their talent and coordination are so flawless that They then commanded the undivided attention of their audience as during the trio's performance you could have heard a pin drop in the theater.",
    "title": "Michael and Suzanne Hauser",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "17, 18",
    "page_number": 17,
    "word_count": 570,
    "article_char_count_full": 3455,
    "article_char_count_review": 3455,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_09::A12",
    "article_text_for_review": "EL BAILE - PART II Parts of the Dance CASTELLANA (la) - refers to a rhythmic section of the alegrías that is highly accented, beginning on the 12th beat instead of the usual 1, 2, $ \\underline{3} $, etc.; it is usually sung, although it need not be, and often occurs after the $ \\underline{silencio} $, but may be done within the escovilla. CIERRE (el) - closing; a form of $ \\underline{\\text{llamada}} $ used to close a section of dance. CONTRATIEMPO (el) - countertime; accentuation done on the off-beat, or between the beats. DESPLANTE (el) - a dancer's variation in the bulerías; there are desplantes in the alegrias and soleares, but when they occur, the rhythm is actually that of bulerías, even though it might be quite slow in tempo; a desplante is always begun with a llamada. ESCOVILLA (la) - the major footwork section of the dance; it is characterized by the footwork being the point of emphasis and sustained for a relatively long time, the absence of singing, and for the most part, melodic variations played on the guitar, rather than rasgueados (strummed rhythm). IDA (la) - the ending of a dance (also called el final); sometimes used to refer to a set, stylized section of the alegrias that can be used to enter the bulerías. LLAMADA (la) - a call; a signal used by dancers to communicate a forthcoming change in the dance; llamadas are commonly used to signal a dancer's entrance (salida), the closing of a section of dance (cierre), a major change of tempo or rhythm (as the change into the $ \\underline{\\text{castellana}} $ or bulerías in the alegrías), or the beginning of a $ \\underline{\\text{desplante}} $. MUTIS (el) - exit (hacer mutis = to make an exit); in flamenco, the ending of a dance by going off stage. PASADA (la) - a pass; a step in the sevillanas in which the partners pass by each other alegrías which is now commonly called the silencio. PASEO (el) - a walk; refers to parts of the dance where emphasis is on graceful walking and movements of the upper body and arms; sometimes used to refer to the part of the SALIDA (la) - the dancer's entrance. SILENCIO (el) - part of the alegrias where graceful arm and body movements are emphasized, with almost a complete absence of footwork; it is not sung and is commonly played in the minor mode on the guitar. SOLO DE PIE (el) - a section of footwork done without guitar or cante - usually accompanied by palmas. ⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩⑩� Jaleo has begun inserting an extra page, a calendar of events, into the copies going to San Diego members. An idea came up, at first as a joke with talk of a \"center fold\" or \"Playmate of the Month,\" but then more seriously, of using the space on the back of the calendar for an 8 X 10 photo of a flamenco performer. Since the page is not part of the newsletter, it can be removed, used as a calendar for a month and then reversed and the photo put on a wall in a studio or whatever. If we follow through with this policy, we would send the insert to all members, even though the events on the calendar are local to San Diego. To carry this out, we will first have to have a series of photos to ensure that we will be able to continue the venture once it is begun. So, if you are a professional performer and would like some free publicity, send an 8 X 10 glossy, black and white publicity photo, plus the name you use professionally, and, if you wish, a few things about yourself (at least, what you do), to Jaleo, Box 4706, San Diego, Ca. 92104; put somewhere \"for calendar\" so we can be sure that it is intended for this use. August Juerga By Gene Coates As we were approaching the bright lights of Jack and Sheryl Tempson's house, a sharp castanet $ \\underline{\\text{repique}} $ snapped through the air. A guitar $ \\underline{\\text{rasgueo}} $ followed. One of our group (a young, first-juerga female) let out a squeal of delight. In the living room, Masami Hopper, Joe Kinney and Louis Hendricks were playing to a small crowd. Juana de Alva was energetically lending palmas. Having stopped by the kitchen for wine, we stepped into the rec. room where three brightly costumed young girls were seriously performing sevillanas. Jesús Soriano was smiling broadly, animating the dancers and spectators with a strong flamenco rhythm.",
    "title": "FLAMENCO TALK",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 759,
    "article_char_count_full": 5595,
    "article_char_count_review": 5595,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_09::A13",
    "article_text_for_review": "By Gene Coates As we were approaching the bright lights of Jack and Sheryl Tempson's house, a sharp castanet $ \\underline{\\text{repique}} $ snapped through the air. A guitar $ \\underline{\\text{rasgueo}} $ followed. One of our group (a young, first-juerga female) let out a squeal of delight. In the living room, Masami Hopper, Joe Kinney and Louis Hendricks were playing to a small crowd. Juana de Alva was energetically lending palmas. Having stopped by the kitchen for wine, we stepped into the rec. room where three brightly costumed young girls were seriously performing sevillanas. Jesús Soriano was smiling broadly, animating the dancers and spectators with a strong flamenco rhythm. Stepping out the back door, we paused at the pot-luck table. Across the patio and pool was a raised platform where guitarists and aficionados were relaxing beneath lights and sky. Paco Sevilla's inspired bulerías filled the night from that stage. Like a time capsule, the platform was succeedingly a stage for sevillanas of dance students Susan Tempchin, Hazel Lent, the Barrios girls, Rochelle Sturgess, and Patty De Alva, Jesús' and Paco's playing, and Isabel Tercero and Pilar Coates in an Andalucian duet. Back in the rec. room, Luana Moreno and Ernest Lenshaw were dancing a spirited set of sevillanas. Paco examined Masami's new guitar, played a demanding piece, and declared the guitar had \"presence\". Later, out under the stars, Masami played her guitar and sang; \"Una feria es alegría, Una copa de jerêz, Un cante por bulerías, Un beso de una mujer, Los demás son tonterías.\" On Monday, August 14th, a group of Jaleistas gathered at the mutual painting and dance studio of Maus Palmer and Juana De Alva for a brief mini-juerga to welcome bailora Carmen Mora to San Diego. Carmen is commuting here from the Los Angeles area twice monthly to teach flamenco. In spite of the fact that only snacks were requested, quite a layout of food was supplied and wine was abundant. The atmosphere in the studio was warm and condusive to camaradarie. Some notable first timers were bailora Esther Moreno who drove down with Carmen and popular singer Jessy Davis who sang \"Besame Mucho\" por rumba. Carmen honored us with some bulerías graciosas danced in a Mexican peasant dress. The evening broke up about eleven with plans for a Sea World expedition the next day.",
    "title": "AUGUST JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "19, 20",
    "page_number": 19,
    "word_count": 388,
    "article_char_count_full": 2349,
    "article_char_count_review": 2349,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
