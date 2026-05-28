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
    "article_id": "JALEO_1982_05::A11",
    "article_text_for_review": "NEWS FROM OUR MEMBERS (From Bettyna Belen -- Las Angeles, CA) There's a new dancer at EI Cid Restaurant. Mer name is Laurdes. She looks like a mixture of Manuela Carrazco and La Susi. Don't miss her! oscar Nieto and his \"Mozaico Flamenco\" performed at EI Conquistador on April 10th. Some of the company members are Terry Tony and Irene Heredia. (From Angell Winston -- Phoenix, AZ) Laura Moya Institute for Hispanic Dance presented a Mexican-Spanish-Flamenco program on April 15th at the Scottsdale Center for the Arts which included Rodrigo's \"Concierto Andaluz\" choreographed by Oscar Nieta. (Juana -- San Diego, CA) Local Jaleistas kept bumping into each other in front of the Guild Theater last month. The occasion...a two week showing of Antonio Gades' film version of Blood Wedding. It was a beautiful Spanish style modern dance complete with rolling on the floor and dressing and undressing an stage, but alas -- na flamenco. (From Juan Santana, L.A.) Dancer Luana Moreno and singer Rafael Santillana are back from Florida and working at the Espartacus Restaurant on 8911 Santa Monica Blvd. (From Nartheastern Illinois University) Ensemble Español will perform May 15, 8:00p.m. at the N.I.U. Auditorium with special guest artists dancers, Victoria Xcrjhan, Edc, guitarist Mario Manuel Escudero and cataor Paca Alonso. Call 4B1-5299 or 239-7742. (From the Hausers, Minneapolis) \"Zorongc Flamenco,\" guest artist Manolo Rivera, completed a two-month, sixteen-city tour from Natchez, Mississippi, and Tampa, Florida, to Toronto, Canada. The group was well-received and will look forward to future tours with Colombia artists. Zorongo Flamenco will also perform this month in Ljublijana, Yugoslavia, and in nearby villages. Flamenco is very popular in Yugoslavia and Zorongo is honored to receive this invitation. Susana is currently in Spain, and will be joining the other members of Zorongo in Yugoslavia towards the end of May.",
    "title": "EL OIDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_05",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 20,
    "word_count": 302,
    "article_char_count_full": 1932,
    "article_char_count_review": 1932,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_05::A12",
    "article_text_for_review": "(from: FISL Newsletter, April 1969) by Morre & Estela Zatania Among the items most essential for dancers to interpret our modern style of flamenco are dance shoes. Heelwork came into the flamenco scene around the first half of the 19th century. Donn Pohren, in his book \"Lives and Legends of Flamenco,\" mentions dancers Miracielos, El Raspao, and Enrique el Jorobao as being some of the first to be remembered for the heel-tapping, foot-stomping style of dancing. Flamenco dancing had been around some years prior to this although it might be hard for some of today's flamencos to conceive of dancing without heelwork. Raised heels seemed to be the style of dress of those times, at least on the ranch where the heel was most probably innovated for riding. Female dancers also began using shoes with heels as it became stylish for woman to use heelwork. It is not improbable that they used shoes with heels before this time as they may have been the style for the upper-class ladies. It might be of interest to note that dancers danced with buttoned spats when they were in style, judging from pictures of dancers which were taken at this time. Of course we must remember that having one's picture taken in this period was a big thing and very often the clothes one wore for a picture were Sunday best or borrowed. I have seen dancers today who wear leather spats which are zipped up and worn with regular dance shoes giving the illusion of being campero boots. Dance heels are today generally made from wood and covered with a thin piece of leather with a $ k $ inch piece of leather on the bottom where tacks are placed. Once shoes have been purchased from the maker or distributor, they must be fitted with braces and tacks for the heel, rubber for the sole, and a piece of leather and tacks for the toe. If you're a professional dancer and working, you may have the money to pay a shoemaker to do this work for you. Unless it's a shop which often does this type of service you might have trouble explaining what you want and you might find the shoes won't do what they're supposed to do on the boards. Those who want to save a five to ten dollar fee per pair and are capable of and enjoying working with their hands can do the job themselves. Only girls' shoes will need braces for the heel, which keep the heel from wobbling backward and forward. These can be purchased from most 5&lD$ stores, hardware stores, or a shoemaker's outlet, in boxes of six or a dozen. Most shoemakers won't sell you a tack -- not when they can charge a couple of dollars to put it in. The angle of the brace may have to be bent one way or the other to match the angle of the heel. Nails of sufficient length",
    "title": "DANCE SHOES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_05",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "20",
    "page_number": 21,
    "word_count": 494,
    "article_char_count_full": 2691,
    "article_char_count_review": 2691,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_05::A13",
    "article_text_for_review": "This month's juerga will be held again in downtown San Diego. Los Trianeros, San Diego's flamenco youth group, will be setting up a caseta in one of the rooms and any of the cuadros wishing to do the same are welcome to do so. Cuadro \"A\" will be in charge (see names below). La juenga de este mes tendrá lugar nuevamente en el centro de San Diego. Los Trianeros, el grupo juvenil flamenco de San Diego convertirá, en casta, uno de las cuartes y cualquiera de los cuadros que seté intersado puede hacer lo miemo. El Cuadro \"A\" sstará a cargo de la juenga. (Los nombres aparecen abajo.) DATE: Saturday, May 22 PLACE: 526 Market St. - Between 5th & 6th PHONE: 232-1331 BRING: Tapas (hors d'œuvres) Doaatioas: Members & first guest of S/G Member.....$3.00 Noa-Members.....$5.00 Children 15 and under.....$1.00 Ayudantes.....Free AYUDANTES: Halpers will be admitted to the juerga free of charge. They must be current members of jalaistas and must notify the juerga coordinator one week prior to the juerga if they wish to help. Plsasa volunteer! It is not fair for one or two persons to have to man the bar or the entrance table all night. We are all members of jalaistas and should all shara in the work as well as the fun! Call Vicki Oistrich 460-6218 or 468-3755. Ayudantes serán admitidos sin cobrar. Deben de ser socios de jaleistas y necessitem avisar a la coordinadora de juergas una semana antes de la juerga si quieren ayudar. ¡Por favor, ofrencenses! No ee justo que una o dos personas esten atrás del bar o la mea de entrada todo la noche. ¡Todos somos socios y debemos, compartir no solo en la diversión para tambien en el trabajo! Llama Vicki Districh 480-6216 or 468-3755. DIRECTION: From I-5 So. take the Front St./Civic Center exit and bear left, right on 4th, left on Market. Highway 94 West empties onto F, turn left on 6th, right on Market. 8166way 163 empties on Market. Del I-6 sur toma la salida Front St./Civic Center y queda sobre su lado isquierda, valtea a la derecha en la calls \"Fourth\" y a la izquierda en Market. La 94 West se acaba en la \"F\", voltae a la isquierda en la Sixth y a la derecha en Market. La 163 eur se acaba en el Market. CUADRO \"A\" MEMBERS: Francisco and Elizabeth Ballardo, Elizabeth Jr., Juanita and Victoria Ballardo, Marilyn Bishop, Pilar Coates, Juanita Franco, Bernardo and Chela Gres, Ernest and Hilma Lenshaw, Jan Jocay, Tony and Alba Pickslay, Nina Yguerabide, Mary Ferguson, Hiroko Nagata, Juan Torra, Juerga coordinator, Vicki Dietrich 460-6218/468-3755. Cuadro \"A\" is still without a leader if you should wish to volunteer. Cuadro \"A\" sigue sin capitán. Avisano si alguien quiere ser voluntario.",
    "title": "SAN DIEGO SCENE: MAY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_05",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 22,
    "word_count": 461,
    "article_char_count_full": 2649,
    "article_char_count_review": 2649,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_05::A14",
    "article_text_for_review": "The JUNTA is the organizational board which steers the course of JALEISTAS. Meetings are held on the SECOND TUESDAY of every month at JALEO HEADQUARTERS, 1626 Fern Street, at 7:00 p.m. Our next meeting will be on May 11th. EVERYONE IS WELCOME. APRIL JUNTA MEETING The meeting was held on April 13th, with five board members and the Cuadro \"D\" Leader and Assistant Leader in attendance. TREASURER: The Treasurer submitted the monthly report, which was accepted and made part of the record. TREASURY AND INCORPORATION: A special financial report covering all phases of Jaleistas' activities (\"JALEO,\" juergas, etc.) during the past three years, along with the proposed budget for 1982, was submitted. This report is fundamental in our quest for incorporation as a non-profit organization. ELIZABETH BALLARDO is to be commended for her time and preciseness in its preparation. It will be given to the attorney representing Jaleistas in this matter. JUERGAS: The President reported on the March juerga, which was held at the Market Street site in the Gas Lamp Oistrict.\" The remainder of the meeting was devoted to discussing the pros and cons of this location. In spite of the fact, as stated before, that \"This site not only meets, but exceeds, our requirement for Juergas,\" attendance has been below normal at the two Juergas held there. The majority of Board Members present felt that this site is not suitable for Juergas because of its location; however, it was agreed that we would try once more and that the May Juerga could be held there. In the meantime, other rental possibilities are to be checked in hopes of finding a permanent Juerga site. EVERYONE'S HELP IS ASKEO IN THIS. LOCAL ENTERTAINMENT: As a reminder, it was pointed out that we can always patronize the business places in San Diego where some of our members are performing when we have no scheduled Juerga. There being no further business, the meeting was adjourned at 9:00 p.m. LA JUNTA La JUNTA es el grupo que organiza y guía el curso de JALETSTAS. Se reúne el SEGUNDO MARTES de cada mes en las oficinas de JALEO, 1628 Fern Street, a las 7:00 p.m. La próxima reunión será el 11 de mayo. TODOS ESTAN INVITADOS, REUNION DE ABRIL La reunion se celebró el 13 de abril, con la asistencia de cinco miembros de la directiva, y del Director y Sub-Director del Cuadro \"D.\" La TESORERA presentó el informe mensual de cuentas, el cual fue aceptado y hecho parte del expediente. TESORERIA E INCORPORACIÓN: También presentó la tesorera un informe especial de Finanzas que cubre todas las actividades de Jaleistas (\"JALEO,\" Juergas, etc.) durante los últimos tres años y también el presupuesto recomendado para 1982. Este informe es fundamental para nuestro proyecto de incorporación como organización no lucrativa. Felicitamos y agradecemos a ELIZABETH BALLARDO por la precisión y el tiempo que dedicó a su preparación. Se le entregará al abogado que representa a Jaleistas en este trámite. JUERGAS: La Presidenta informó acerca de la Juerga de Marzo, la cual tuvo lugar en el local de la Calle Market, en el \"Distrito de Faroles.\" Se dedicó el resto de la reunión a discutir los pros y los contras de hacer las Juergas en este lugar. A pesar del hecho de que, como dijimos antes, \"Este lugar no sólo llena, sino sobrepasa nuestros requisitos para Juergas,\" la asistencia ha sido menor que la normal en las dos Juergas que allí se han celebrado. La mayoría de los miembros de la directiva presentes opinan que este local no es adecuado para Juergas debido al lugar en el que está situado; sin embargo, se acordó que se haría la prueba una vez más y que la Juerga de Mayo se celebraría allí. Mientras tanto, se deben estudiar las posibilidades de otros lugares de alquiler con la idea de encontrar un local de Juergas permanente. SE PIDE LA AYUDA DE TODOS PARA EVCONTRARLO. PRESENTACIONES LOCALES: Para mantenerlo siempre en mente, se volvió a recordar que cuando no tengamos Juergas podemos siempre asistir a los lugares públicos de San Diego donde actúan miembros de Jalelistas. Por no tener otros asuntos que tratar, se cerró la reunión a las 9:00 p.m. AM + BUS + STEAMSHIP + HILL + DOMESTIC AND WHOLE TRUCKS (714) 276-6000 • 297 • K' STREET • CHOLA VISTA, CHOLA 92011 ESPECIALISTAS DE ESPAÑA REYNOLDS S. HERIOT OWNER - MANAGER 426.6800",
    "title": "JUNTA REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_05",
    "year": 1982,
    "language": "en",
    "article_type": "article",
    "pages": "22",
    "page_number": 23,
    "word_count": 734,
    "article_char_count_full": 4298,
    "article_char_count_review": 4298,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_06::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLOLA FLORES: INTERVIEW (from: ABC, Dec. 19, 1981; sent by Gordon Booth; translated by Paco Sevilla) by Javier de Pablo \"The greatest in the world, along with Jerez\" has returned to Sevilla, an artist who has spent many years on top without losing one iota of that overwhelming energy that inundates everything and manages to attract an audience that is always excited by her...Now, for three days, on the stage of the Lope de Vega Theater, Lola Flores, accompanied by Manuela Carrasco, performs for her people. \"My sister Carmen was going to come, but since she signed a contract with Manolo Escobar, she couldn't do it. They suggested Manuela Carrasco to me and I was very satisfied, because, to me, she is an unusual genius. I like to work with artists who leave the people warmed up, not cold.\"\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Manuela\"]\n\nthree days, on the stage of the Lope de Vega Theater, Lola Flores, accompanied by Manuela Carrasco, performs for her people. \"My sister Carmen was going to come, but since she signed a contract with Manolo Escobar, she couldn't do it. They suggested Manuela Carrasco to me and I was very satisfied, because, to me, she is an unusual genius. I like to work with artists who leave the people warmed up, not cold.\" -- What does the recital consist of? \"Manuela does the first part and I do the second. It is a show much like what I usually do, that is, with recitations, bulerías, pasodobles, costume changes, with orchestra, cantaor, guitar, and everything. I have a beautiful bulería that I sing to my children, by Antónito Gallardo, a Mexican song by Juan Gabriel, a poem by Rafael de León on the death of García Lorca, plus all of my hit numbers.\" --Is the Spanish canción still on the rise? \"Bueno, it continues at a high point in certain people. There are artists who continue to be what they were, like Juanita Reina, who will be a star until she dies. But few new artists are appearing, and those who do appear do not have personality -- they resemble Marife or Juanita, etc. But, in any case, the canción will never decline because Andalucía is a source and good people will appear.\" -- What do you do, Lola, to maintain that strength and temperament that characterizes you? \"I sleep very little, only six or seven hours -- at night I like to write poems and songs. I believe that I work a lot and give too much. I give my entire life when I go out on stage and I think that must be good for me.\" \"I think it was her idea. For me to work with Lola Flores is something -- this is the truth -- that I don't really believe yet. To perform with this artist is something difficult since she is such a complete artist. I do the first part, approximately 40-50 minutes, and dance siguiriyas, solea and bulerías.\" -- Don't you run the risk that she, with her temperament, will eclipse you and you will go unnoticed? \"I think not. It helps me to perform with Lola. You have to realize that we are very different. She is a giant in her field and, what I do, I don't do badly.\" -- Were you satisfied with your performance in the Quincena? \"I am very pleased with my part in it. The Quincena has been a marvelous thing; each day had a greater success. The audience went all out and I believe everyone enjoyed it. The artists gave everything they had in the Quincena.\" -- They say the baile of Manuela Carrasco is radical, distinctly aggressive, and profoundly reaches the public. Do you agree?\n\n[ENDING CONTEXT]\n\ntheir importance decline when she appears. She was a whirlwind and continues being an authentic seismic phenomenon. Time, which is the perfect judge, demonstrates, with facts and without words, the truth of her quality. In the Quincena, along with Matilde, Milagros, and El Güito, they formed a real dance team. And triumphed. And she continues to triumph, together with that whirlwind of color, that inexplicable woman named Lola. The \"jerezana\" has succeeded in being not only a star in her art, but a piece of the art itself. That is why Nacha Guevara and Juanita Reina were there to applaud her.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LOLA FLORES AND MANUELA CARASCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_06",
    "year": 1982,
    "language": "en",
    "article_type": "poem",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 1171,
    "article_char_count_full": 6513,
    "article_char_count_review": 4215,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Manuela"
      }
    ]
  }
]
```
