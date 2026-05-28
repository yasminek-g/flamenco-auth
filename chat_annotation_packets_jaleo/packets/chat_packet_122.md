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
    "article_id": "JALEO_1981_11::A1",
    "article_text_for_review": "I'm writing to you from Adelaide, the capital city of South Australia, because I'd like to inform your readers of what is happening in flamenco here in this part of Australia. After reading several of your magazines owned by a friend, I have decided to subscribe myself. The scope for performing flamenco here in Adelaide is very limited, as it is in most parts of Australia. We have nothing that resembles a tablao here, although there are a few Spanish restaurants, one of which has a mediocre floorshow. Flamenco in Australia is kept alive mostly by non-Spaniards. Here in Adelaide a singer and I have been instrumental in forming a flamenco group and we have been together since March 1979, which is something of a record. I'm enclosing a photo of our group and I'll briefly describe the people in it from left to right. Our group is called \"Los Flamencos\" and each member has been involved in some form of other dance or music at some stage as well as flamenco. is a specialty type of entertainment and doesn't appeal to everybody; secondly, if they have a second rate floorshow which doesn't cost a lot of money and the audience seems to enjoy it, especially if it is commercial with pasedobles in it, they prefer to keep that on rather than look for better quality floorshows with better dancers in it. This tends to be depressing and I know of several dancers in Sydney who were very good dancers in Spain for many years, but are not working because of this type of attitude. However, here in Adelaide, we are getting work and we're still very enthusiastic. We've all been to Spain at some stage and though Spain is too far to go for short trips, we find going to Sydney inspirational as far as learning and seeing good flamenco, because of the amount of talent there. Starting from the left, sitting down, is PAQUITA. By day she is a Pharmacist; she is Greek and has been dancing for about ten years. SALERA has been dancing for about four years. She has always been involved in dance since the age of four years and has progressed tremendously in the short time she has been dancing. She is Australian, and works as an art and drama teacher. RAFAEL, standing up, worked as a principal dancer in a Spanish dance company for several years, in another state. He studied under Estrella Morena in Spain and has been dancing for fifteen years. He is Australian with some Spanish blood, going back a couple of generations. He works as a professional pianist for a dance school and is a qualified hairdresser as well, which is lucky for us when we want our hair and makeup done. DOMENIC plays a little guitar, but is involving himself mostly in singing now. He and I started the group. Although he worked as a professional singer in popular music for a number of years, he now is an owner of a successful hairdressing salon in the city. He is Italian, He is learning to sing flamenco and although he is finding it difficult, especially as he doesn't speak Spanish, he is coming on very well. At present he is singing garrotín, sevillanas, verdiales and guajira. He is in the process of learning tientos and, although it's hard work, we feel he is getting there. pleased with the way he is able to accompany the dancers. From past experience I know how difficult it is for a guitarist, used to playing solo, to follow a dancer and not many of them can do it. When Paco Peña was here for our last Festival of Arts, three years ago, he was very impressed with Italo's playing and invited him to join his summer school for guitarists in France the following year. Italo went and needless to say, learnt a lot. Another Italian. ITALO, the youngest of our group (21 yrs), has been studying guitar for twelve years. He has appeared as a solo artist in various talent shows on television and has always walked away with major prizes. This is the first flamenco group he has been involved in and he is learning fast. We're especially In the centre is me, VERONICA. I've been dancing for about eight years now, starting as a hobby, but finding it like an addiction -- I can't get enough of it. I spent six months in Spain and have studied under various teachers here in Australia. I'm a secretary and Croatian with a bit of Hungarian thrown in on my mother's side. I speak Spanish fluently and teach flamenco to the children at a Spanish school here. I've been with various flamenco groups here in Adelaide but they have never lasted long. I spent two years dancing in a Spanish restaurant here, until they decided that they couldn't really afford a flamenco group and also, under new ownership, were trying to change their image. That particular restaurant was the closest to a tablao we have ever had. It was a place that had a resident flamenco guitarist/singer and people were always welcome to come in and dance a little, sing and do palmas.",
    "title": "LOS FLAMENCOS OF SOUTH AUSTRALIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 870,
    "article_char_count_full": 4838,
    "article_char_count_review": 4838,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A2",
    "article_text_for_review": "Dear Jaleistas: I wish to thank all of those beautiful dancers, guitarists, and guests who helped to make my birthday party a great success. Thank you and Salud! Ernesto Lenshaw San Diego, CA Dear Jaleo, PACO PENA, MANOLO \"EL CHINO\" (ONE OF LONDON'S PREMIER DANCERS), AND MARIO MAYA AT A PUB DURING HIS LONDON PERFORMANCE SEASON. (photos by David Bateman) EL OSITO PLAYS FOR DIANA \"LA CHOCOLATA\" AT THE COUNTRY HOME OF JENNIFER LOWE NEAR LONDON. GUITARIST ANGEL CORTEZ AND SINGER MANUEL DE PAULA LOOK ON.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 86,
    "article_char_count_full": 504,
    "article_char_count_review": 504,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A3",
    "article_text_for_review": "PACO DE LUCIA -- CABALGANDO CON EL VIENTO (RIDING WITH THE WIND) by Kenneth Sanders You can always tell that a noble prince of the guitar has arrived on the scene, when people start raving about him in a negative as well as an affirmative way. Whenever I read an ugly, unkind review concerning the work of any accomplished artist, especially Paco de Lucía, I am reminded of the German composer Hugo Wolf, who wrote a piece describing his pleasure in kicking an unpleasant critic down the stairs (to the lilting strain of a Viennese waltz)!! To me, there appear to be three kinds of people: (1) The ones like the artists, who $ \\underline{\\text{make things happen}} $. (2) The people like the fans and aficionados who $ \\underline{\\text{watch what happens}} $ and really get off on it. (3) And then there's the negative kind who $ \\underline{\\text{wonder what happened}} $ and always try to put it down because it's something new and they can't relate to it. All people are certainly entitled to their respective opinions, but the reviews, etc., that I have read in the recent issues of Jaleo concerning Paco de Lucía's latest accomplishments seem to miss their mark. I personally adore the antique, traditional flamenco, but I can also certainly dig what's happening in the 80's. I didn't have to pay $12.50 for Paco's new record. He had his record company send it to me free of charge from Spain. He was also nice enough to autograph it for me backstage after his concert with John McLaughlin and Al DiMeola at the Hollywood Bowl, CA, Aug. 9, 1981. Several regular Hollywood Bowl concert series aficionados declared Paco's concert to be the best of the season. I told Paco of Jaleo's interview with Sabicas and of Sabicas remarking that Paco had left the path of flamenco, etc. Paco didn't seem too moved. I find it amusing and somewhat disappointing that some people think they can tell Paco de Lucía how to play the guitar or try to \"classify\" him with all their little rules, regulations, traditions, etc. It's like trying to tell Bruce Lee how to fight. The man is a free spirit, riding with the wind! There was a lot of feeling at that concert and much warmth. And that wasn't only because I was with the most beautiful girl in the world either! The music was the magic that had us in \"Cielito Lindo\" (Beautiful Heaven). It seems that when Paco touches the guitar, the feeling drips from his fingers. I think the people who try to knock him are the ones with no feeling. We flamencos secretly wish that people of other styles and forms of music, dance, song, etc., will recognize what our art has to offer. John McLaughlin and Al DiMeola can't play flamenco, yet Paco embraced their art and they collaborated together in a beautiful fusion of their respective styles, each man giving something of his own. (I just wish I were half the guitarist that John or Al is, much less Paco!) It's always easier to sit on your fanny and be an \"armchair quarterback\" and criticize every new, fresh effort, saying, \"Well, it's not traditional or whatever,\" than to get up onstage and really give of yourself to the art. I can certainly recommend adding Paco de Lucía's new record, \"Sólo Quiero Caminar\" to your album collection. It's beautiful. The whole album is interesting; the more I listen to it, the more I hear. The jazz influence is present, but it doesn't dominate the flamenco at all. It's very up-to-date music. The two bulerías (\"La Tumbona\" and \"Piñonate\") are exceptional and both are solo guitar works. The columbianas (\"Monasterio del Sal\") is also a gorgeous work of art. In my opinion, Paco de Lucía's present potential is about as \"dormant\" as a tornado's when it touches the ground. Viva la Revolución del Flamenco! *** \"SOLO QUIERO CAMINAR\" -- ANOTHER VIEW by David Alford I would like to mention a different viewpoint from the one my friend Jerry Lobdill took in his record review (Jaleo Vol. V No. 1) of Paco de Lucia's recent album \"Solo Quiero Caminar.\" The heart of the matter, it seems to me, is whether an artist will come closer to 2 FOR 1 SPECIAL No Limit GUITAR STRINGS 2 FOR 1 SPECIAL EXP 1-31-82 Set FSW 100 a clear nebes Set FSB 100 w back trebles REGULAR §10 SET — SPECIAL 2 SETS §10 LESTER DeVOE – GUTTARMAKER 2436 Renfield Way, San Jose, CA 95148 Enclosed is my check or money order payable to Lester DeVoe Send sets (FSB) sets (FSW) 85 each set plus 8.50 each set post. Name City",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "7",
    "page_number": 7,
    "word_count": 786,
    "article_char_count_full": 4410,
    "article_char_count_review": 4410,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Gabriel Ruiz Madrid y su invierno en las calles de la capital española, llena de euforia para la mayoría de sus habitantes, por la caída de Alfonso XIII, y la recién instaurada República de Alcalá Zamora...Corren los días en aquel 1935. La Calle Echegaray, como siempre en las madrugadas, repleta en sus casas de vinos, bares, restaurantes y colmaos. Aunque es un invierno crudo, y ya para llegar el alba, se oye bullicio en todos los establecimientos públicos y, en algunos, palmateo y cante flamenco. En aquel tiempo, por la mencionada calle -- al igual que el Callejón del Gato, Calle la Cruz y Plaza Santa Ana -- todas las noches deambulaban por ellas casi todos los flamencos que en Madrid vivían, al igual que los aficionados al cante y al vino. Los primeros, para ganarse su vida con las\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"segundos\"]\n\nun invierno crudo, y ya para llegar el alba, se oye bullicio en todos los establecimientos públicos y, en algunos, palmateo y cante flamenco. En aquel tiempo, por la mencionada calle -- al igual que el Callejón del Gato, Calle la Cruz y Plaza Santa Ana -- todas las noches deambulaban por ellas casi todos los flamencos que en Madrid vivían, al igual que los aficionados al cante y al vino. Los primeros, para ganarse su vida con las juergas, y los segundos, para divertirse con aquellos, a los cuales pagaban por escucharles cantar y tocar en algún reserva de cualquier colmao. reservao especial, famosísimo para los flamencos de aquella época, que se le llama \"El Cuarto de la Lidia,\" por sus carteles taurinos y su decoración interior. Los Gabrieles -- ¿Quien no lo conocía, al igual que a su dueño, Don Fabían? -- con muchos pelmazos en la barra, casi todos borrachos y otros clientes en los reservaos de arriba, del segundo piso. Hay un En este cuarto se hallaban aquella noche, Don Tomas, famoso y popular médico, amigo de los flamencos, Don Gabriel -- buen aficio--nado al cante y padre de un joven tocaor, Gabrielito -- el señor Agustín -- padre de Sabicas -- Pepe el de la Matrona, Estampio, Fernando el Herrero, Fosforito, el Maestro Malagueño, el Señor Luis -- maestro de obras y amigo íntimo del de la Matrona -- Manolo Bonet, Sabicas y su hermano \"Dieguico,\" Enrique el Mellizo, y Gabrielito. Están allí, reunidos, desde las once de la noche, despidiendo al Mellizo, quien debe viajar al día siguiente para Cádiz. Ya casi va a amanecer; se ha cantado, bailado, y tocado mucho aquella noche. La fiesta, la juerga, es pagada por Don Tomas y Don Gabriel. Los mayores, algunos, están\n\n[ENDING CONTEXT]\n\nsay anything. El Mellizo, with a broken and barely audible voice, said to the youth, \"Grabielito, hijo, I am going to sing for you the last malaguena of El Mellizo!\" And that Gaditano, now old, thin, tall, flamenco, with the coloring of cypress, rests his hands on the shoulders of Gabrielito and, very faintly, almost without voice, sobbing, moisture below his nose, his breath falling on the face of the young guitarist, whispers: \"Ay...Eran las dos de la noche....Ay, Ay, y vino mi hermano a buscarme... Alevantate hermano mío, A, A, A, Ay, AAA...\" LA CUERDA IDEAL para el ejecutante más exigente\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "THE LAST MALAGUENA OF ENRIQUEEL MELLIZO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "8-10",
    "page_number": 8,
    "word_count": 1119,
    "article_char_count_full": 6383,
    "article_char_count_review": 3322,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segundos"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_11::A5",
    "article_text_for_review": "by Ron Spatz One of my lifetime ambitions was recently fulfilled when I became the proud owner of one of the \"aristocrats\" of flamenco guitars, handed to me personally by the great luthier, Faustino Conde. In order to portray the feeling I would like to share of this moment, I must allude to a rather depressing scenario. After enjoying Holy Week in Sevilla this last April, my wife Frances, our traveling companion Helen Payson, and I embarked upon a zigzag course that eventually led to Madrid (as all roads do in Spain). Armed with several letters of introduction to some celestial flamenco types and bursting with anticipation, we no more had arrived than I began bursting in other ways (i.e., a bout with the green apple quickstep). The upshot is that I lay flat on my back in an expensive hotel room for days -- sick, cold, and needless to say, miserable, comforted only by my Donn Pohren books and a copy of Washington Irving's Tales of the Alhambra. On the very last day prior to our necessary departure, I made a superhuman effort to drag my sickly body from the damp hotel room, determined that I would kill if necessary to reach the primary goal of my trip to Spain: the guitar shop of the Hermanos Conde (for those not aware of it, the three Conde brothers are nephews of the great Domingo Esteso and the exclusive builders of the",
    "title": "A VISIT TO SOBRINOS DE ESTESO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "11",
    "page_number": 11,
    "word_count": 241,
    "article_char_count_full": 1343,
    "article_char_count_review": 1343,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
