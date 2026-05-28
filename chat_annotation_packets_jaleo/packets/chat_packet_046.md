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
    "article_id": "JALEO_1979_07::A12",
    "article_text_for_review": "and emotional aspects. It is much like a guitarist taking one falseta from Paco de Lucía, another from Sabicas, two from Montoya, three from \"Fulano\" and putting them together as a solo that is music, but does not say much. Footwork should say something. As you learn, it should say something about yourself; it should be musically, visually, and dramatically a reflection of your feeling and love of flamenco. I have mentioned in other articles that footwork should be practiced while maintaining good posture and body position, using the rest of your body as you practice, so that footwork is part of the whole. When you practice slowly, lift your legs from the knees down as high as possible so that your footwork will not have that \"glued to the floor look\" when you go fast. Also, practice as if you are focused onto one small area, not so much with feet parallel, but as if they were both standing on a control point; then, by the subtle movements of the hips and your own particular \"seated position\" or bend of the knee, you will find good balance and a good position. Look for a position where you will not bounce in your footwork and your upper body will find freedom to do all of the beautiful and important artistic expressions of flamenco movement. Feel that your footwork comes from the control of the center part of your body and down through your legs. This is a thought process that I feel helps with the all-important control, mattice, and gives beautiful clean, crisp sounds. Each sound of footwork, whether a planta, or punta or tacón should be given special emphasis while practicing. It is much easier to do a planta loudly than a tacón, so emphasize your tacón. These are basic ideas in approaching footwork as a musical instrument, as a part of the beautiful whole body expressing good and exciting flamenco, expressing that particular compás that you are dancing and \"saying something\" -- saying something about yourself as an individual artist expressing a beautiful art. --- Teo Morca Flamenco Dance Classes JUANA DE ALVA NOW OPENING NEW CLASSES IN NORTH COUNTY FOR INF. CALL: 436-3913",
    "title": "MORCA... sobre el baile",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "17, 18",
    "page_number": 17,
    "word_count": 366,
    "article_char_count_full": 2112,
    "article_char_count_review": 2112,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_07::A13",
    "article_text_for_review": "By Tom Kerrigan It is definitely an old saw to suggest that there is more to the dance of Spain than just flamenco or gitano (gypsy) dancing. People like La Meri and Carola Goya, Americas experts on Spanish dancing, have been espousing this cause for literally 50 years. But on the occasion of the New York debut of Spain's three-year-old Ballet Nacional, it is a point worth reiterating. To be sure, the companies of Antonio, José Greco, Federico Ray, Pilar López and most especially the great Argentina had repertoires that showed many of the dances of Spain -- not just flamenco. Nonetheless, the image of Spanish dancing to the average person throughout the world, and, also -- interestingly enough -- in Spain itself, remains one of flamenco. It is one of the announced goals of the 40-member Ballet Nacional to enhance that image, and it is not unlikely that for many of the people who have seen the company on tour in some 14 different countries and who have cared to look closely, this image has already been altered. As Alan Kriegsman put it in the Washington Post this week, \"At the mention of Spanish dance most of us instantly think of the rhythm of a thousand little firecrackers with heels and castanets chattering away like devil's teeth. There was plenty of that when the Ballet Nacional made its local debut, but there was also a lot more that one would have no way of anticipating. For this superb troupe is unlike any Spanish dance group that has ever existed.\" Still, Mr. Kriegsman not withstanding, one cautions a careful look, because flamenco dances in all their glory do, indeed, make up a substantial part of the 35-work repertoire of the company, and many of the remaining works have -- to the untutored American eye -- the appearance of \"flamenco\". This is due in part to the use of castanets, and the mistaken idea that castanets are an exclusively flamenco accompaniment. In fact, castanets are not flamenco in origin at all, but rather distinctly Spanish. References to castanets or similar devices are ancient and are found in pre-Christian Egypt and later in Roman and Moorish civilizations. In Spain, castanets or their forerunners (crótalos) were noted during the Roman occupation of the Iberian Peninsula from 215 B.C. to A.D. 409, but the gypsies with their dancing did not appear in the Peninsula until about the 15th century, and not in great numbers until the 16th century when, at the same time, many Flemish settlers followed the Ghent-born Charles I to Spain on his ascension to the throne. The word flamenco means Flemish, and at the time of Charles I was loosely applied to anything non-Spanish, which, of course, both the Flemish and gypsies were. Hence, gypsy dancing was called flamenco dancing, and only later did it incorporate from existing Spanish dances the use of the castanets -- and then only in some forms of flamenco, not in all of them. If the castanets are not necessarily part of flamenco dance, they are certainly necessary to Spanish dance in general. The seguidilla, fandango, bolero and some forms of the jota, to name a few, all employ them. The question as to why the castanets should have become the single most pervasive and distinguishing feature of Spanish dance is apparently unanswerable. The Encyclopedia Britannica suggests, somewhat lamely, that they \"became the characteristic instrument of the Spanish peasantry\". La Meri, in a recent telephone conversation, agreed that the question has no answer, but suggested some practical considerations: economics (they were cheap) and the solo nature of much of Spanish dance (the dancer could accompany himself). Whatever the reason, castanets were used in both the folk and ballroom or social dances of 16th and 17th century Spain, and, when, at the end of the 18th century, dancers and ballet masters from Italy came to Spain bringing the conventions of Italian classical ballet, they created a new school of ballet technique which also preserved the use of the castanets. This new classical technique was known as and is still called the Escuela Bolera or Bolero School. The name was taken from the prominent social dance of the day -- the bolero -- and the steps, though altered in the sense of less turn-out of the legs and a more closed look to the positioning of the arms due to the use of the castanets, were essentially the same as the Italian steps. to Spanish music has been undertaken by the government through the creation of the Ballet Nacional. Not since the 19th century has dance in Spain been regarded so seriously, and with the opening of a new theatre for the company next year, the first for dance and opera in Madrid since the old Opera House closed in 1925, the Ballet Nacional is well on its way to institutional permanence at home and, through its tours, abroad -- a permanence that has so far escaped all other dance endeavors in Spain. ABOUT THE FESTIVALES DE ESPAÑA The Festivales de España, which is the shorthand form of the full name \"Organismo Autónimo de Teatros Nacionales y Festivales de España\" (Autonomous Organization of National Theaters and Festivales of Spain), is not unlike America's own National Endowment for the Arts. It is a federal organization which disperses funds for artistic projects through the 50 provinces of Spain. It differs from the Endowment in two important respects: first, it is technically part of the Ministry of Information and Tourism, whereas, the Endowment is an independent agency responsible to no cabinet department; and, secondly, the Festivales de España administers and, indeed, creates the arts organizations it funds. It presents theatre, music and dance in over 150 different Spanish cities and towns. (from: the dance program of the \"Ballet Nacional\" for their 1976 performance in Madison Square Garden)",
    "title": "In Spain: Castanets Si, Petipa No",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "article",
    "pages": "18, 19, 20",
    "page_number": 18,
    "word_count": 976,
    "article_char_count_full": 5801,
    "article_char_count_review": 5801,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_07::A14",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nON HORSEBACK BETWEEN TWO EPOCHS; CHACON IS THE TOP OF FLAMENCO ART AND THE BEGINNING OF ITS CORRUPTION. by Angel Alvarez Caballero (from: Madrid's ABC, Feb. 11, 1979; sent by Brook Zern) translated by Roberto Vasquez PART III THE THEATER ADVENTURE In the times just before the First World War, Chacón was hired to sing flamenco at the San Martín Theater of Buenos Aires. In the same manner as Silverio had taken the cante to the café, Chacón took it the theater, thus laying the foundation for an artistic degradation that would culminate in the flamenco \"operas\". Unconsciously, maybe by his being such a superb \"cantaor\", he opened the way to the most fateful epoch of this art. \"To those slight, slow modernizing changes that Don Antonio Chacón would introduce because of his fame, one may\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Marchena\"]\n\nhe café, Chacón took it the theater, thus laying the foundation for an artistic degradation that would culminate in the flamenco \"operas\". Unconsciously, maybe by his being such a superb \"cantaor\", he opened the way to the most fateful epoch of this art. \"To those slight, slow modernizing changes that Don Antonio Chacón would introduce because of his fame, one may attribute cause of a later eruption -- this time dangerous and decisive -- of Pepe Marchena...\" writes Gonzales Climent. therewaiting for the pompous customer made way for him with respect and practically remained at his whim, to do whatever he wanted them to do. As he was a generous person, many nights after the fiestas had ended, he would spend all he had earned, and even more, with his companions that had been less lucky than he, he would ask them to sing and dance for him and he would pay them as if he were a \"señorito\". Without doubt he was the best paid artist of his time. When they asked him \"How much money have you made?\" Manfredi puts the next answer in his mouth, \"If I tell you I made two million, I don't exagerate. For singing in public I have charged from six reales that they gave me in a baptism when I was six years old, to five thousand duros to sing at a party, hired by the King\". One time the Count of Grisal gave him five thousand duros after having listened to him for a whole night. When Chacón saw such a large amount, considering that it was excessive, he went the next day to the Count's home to return the money, thinking that it was a mistake. The aristocrat insisted that he keep all the money, but the cantaor accepted only a much smaller amount. At the juergas, Don Antonio was greatly bothered by the jokers who wouldn't listed to the cante with due respect. When he suspected something like this\n\n[ENDING CONTEXT]\n\nde la Matrona, \"because, besides possessing that personality of his, everything he heard he studied and improved, if it was possible. In his voice everything was enormous...of the ones I have met, he has been the man most honest and most respectful of his art. He didn't put out anything to the public unless it was well done. Flamenco was to him like a second religion. And I say all of this having shared in the struggle with him for twenty or thirty years of following him. This is the word, following him, because I realized what he was doing and I couldn't find it in anyone else of his time.\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "DON ANTONIO CHACON, PAPA DEL CANTE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "20, 21, 22",
    "page_number": 20,
    "word_count": 1350,
    "article_char_count_full": 7364,
    "article_char_count_review": 3427,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Marchena"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_07::A15",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nANSONINI IN SEATTLE Copyright © 1979 by Carol Whitney All rights reserved Saturday, one-thirty in the morning, my phone's raucous rural ring roused me from a sound sleep. It was Ansonini, from Seattle, asking me to come see him. Sin tabaco, how could I? But of course I did, with five U.S. dollars in my pocket. Sitting in the house of a Seattle aficionado, we exchanged news, going five years back. Pepe, of Casa Pepe, dead, Enrique Méndez, aficionado and incomparable singer of bulerías, dead, this one dead, that one dead. Joselero, well, his family well, Juan and Paco del Gastor, well, their father Pepe, well, his wife, dead. This one married, that one with children, and so forth. Five years of news in a few minutes over a glass of wine, a rushed meeting. We went for coffee, while Ansonini\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"expression\"]\n\nble singer of bulerías, dead, this one dead, that one dead. Joselero, well, his family well, Juan and Paco del Gastor, well, their father Pepe, well, his wife, dead. This one married, that one with children, and so forth. Five years of news in a few minutes over a glass of wine, a rushed meeting. We went for coffee, while Ansonini told stories about the Talgo (express train). \"Nadie habla en el Talgo,\" said Ansonini, putting on a prim and proper expression appropriate to that conveyance. He described their Gypsy picnic, the clowning, the teasing, Joselero in the middle of things, as he often is. A proper lady, pregnant, travelling alone, sitting in front of them, other passengers silent--till no one could resist the Gypsy display, and the whole carful of people was \"muerto de risa.\" Sipping espresso, Ansonini remarked on the strangeness of America, where you could go into a bar where you couldn't drink, or into another where you couldn't smoke. His remarks intensified my own feeling of strangeness. Only Ansonini himself, only the couple of friends I was visiting, were familiar to me. Being with Ansonini, in English-speaking ambiente, in plastic-and-friendly America, was strange. We moved right on, to a pleasant living-room half-filled with aficionados. People chatted in small groups. A few introductions were made, but not very many. People didn't all know each other. My experience with juergas had been largely among people I knew at least casually--in San Diego, at the Finca Espartero, in Morón itself. Here, Ansonini became a spectacle, a show, no matter how great or how solid the attending aficionón. He was alone in that livingroom--Ansonini was the foreigner, surrounded by small groups of onlookers, supported by his American accompanist, Ken Parker. Ansonini had to bring all the ambiente with him from Spain, and the oxy\n\n[ENDING CONTEXT]\n\ntimes during those years I had also heard him make the cosmic connection: He had hooked into a different energy source, opening his mouth and letting his essence gush out like -- well, never mind what it was like; it sounds silly when I try to write about it. García Lorca called it the sonido negro, the black sound. He said it could make the quicksilver of mirrors open up. I can report objectively that men wept and tore their hair, and that women fell on their knees and crossed themselves. I also recall that an electric clock stopped, but that was no doubt because of a voltage irregularity.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Gaól on Cante",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "poem",
    "pages": "23, 24",
    "page_number": 23,
    "word_count": 1210,
    "article_char_count_full": 7182,
    "article_char_count_review": 3483,
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
    "article_id": "JALEO_1979_07::A16",
    "article_text_for_review": "The last minute insertion of the juerga announcement last month provided neither time nor space for our usual \"bio\" on the host and hostess, so we will include a few lines here. Emilia Thompson, one of the most enthusiastic jaleadoras at the juergas, is a Madrileña. Her bubbley vitality is contagious to those around her and she is a believer, as her compatriot, Jesús Soriano, put it, that, \"It's never time to go home...\" (see \"Punto de Vista Jaleo, March 1978). Besides adding her energy and vitality to every juerga, she plays a major role, as distribution manager, assuring that Jaleo reaches its many destinations throughout the world. Donald Thompson, who lived and worked in Spain for fifteen years (until 1969), had many opportunities to observe flamenco firsthand; he enjoys flamenco, but not to the exclusion of other styles of dance, music, and theater. LA JOYA DEL FLAMENCO EN LA JOLLA by Alba Pickslay tuvo lugar en la elegante residencia de Emilia y Donald Thompson en La Jolla. Fue realmente un éxito y en todo momento reinó la alegría y el espíritu flamenco. Nuestra última juerga del mes de Junio La dueña de la casa, dueña también de la gran simpatía que todos le conocemos, ofició de anfitriona cordial y amable, por lo tanto se logró elclima ideal; en ningún momento decayó el ambiente y se lucieron en el tablao de la planta baja nuestros valores locales. Juanita Franco y Carolina con mucha gracia, mostraron su garbo renovado en el último viaje a \" la meca del flamenco \" Sevilla, que hicieron con motivo de la celebración de San Diego, CA 92104 \"BAJO LA LUNA GITANA\" \"LA MUERTE DE MANOLETE\"",
    "title": "JUNE JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "27, 28",
    "page_number": 27,
    "word_count": 280,
    "article_char_count_full": 1616,
    "article_char_count_review": 1616,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
