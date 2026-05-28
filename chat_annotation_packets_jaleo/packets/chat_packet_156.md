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
    "article_id": "JALEO_1982_12::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTHE ROOTS OF FLAMENCO I'm afraid Mr. Clark begs a reply to his \"Punto de Vista\" letter of Oct. 1982. There are several points to be addressed. First, I don't believe that \"personal attack\" is an appropriate interpretation of what is transpiring. When debate occurs, error is pointed out. Can one point out error without addressing the one who made the error, and why? Should we not point out error for fear of damaging fragile egos? There is something more important here, and that is that there are people reading Jaleo who perhaps are new to flamenco and want to learn more about it. Shall we direct them to Mr. Clark, Ms. del Cid, or Mr. Lodbill instead of Paco de Lucía? Are they more knowledgeable about flamenco than he is? Second, flamenco's history is much more complex than Mr. Clark\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"reference\"]\n\ndirect them to Mr. Clark, Ms. del Cid, or Mr. Lodbill instead of Paco de Lucía? Are they more knowledgeable about flamenco than he is? Second, flamenco's history is much more complex than Mr. Clark understands. Who can say when flamenco was \"born\"? What day, what hour? Also, there is little evidence to support Mr. Clark's idea that flamenco was initially non-performing and without some form of guitar accompaniment and instrumentalism. The first reference to the gypsies (the Shah Nameh, c. 1000 AD) indicates clearly that they were a performing artist caste that was invited to Persia by Prince Behram Gour in about the year 420 AD when he realized that \"his poor subjects were pining away for lack of amusements. He sought a means of reviving their spirits and of providing some distractions from their hard life...he sent a diplomatic mission to Shankal, King of Cambodia and Maharaja of India and begged him to choose among his subjects and send to him in Persia persons capable by their talents of alleviating the burden of existence and able to spread a charm over the monotony of work. Behram Gour soon assembled 12,000 itinerant minstrels, men and women.\" He gave them land and grain and seed so that they could survive and to play music at no cost to the people. They ate their grain seed instead of planting it and were expelled by Behram Gour for their irresponsible behavior. Other sources, such as the Arab historian Hamza (c. 940 AD) corroborate this account. To quote Jean-Paul Clébert, a well-known writer on the gypsies: \"With music, dancing is one of the earliest activities attributed to the gypsies...It is more than likely that the first gypsy dancers in India were professionals...The guitar is inseparable from the Gitano musician, in the same way as the violin is from the Gypsy.\" The guitar is, in Walter Starkie's words, \"the\n\n[ENDING CONTEXT]\n\nhe shouldn't be at the center of flamenco discussions at the present time, because he is doing something else. -- Paco Sevilla] Home study course with recorded cassette TRADITIDNAL FLAMENÇO GUITAR VOLUME I by MARIANO CORDOBA • 219 pages of instructions, exercises, studies, and selections • Written in conventional music notations and tablature • Selections: Sevillanas, \"Campanas de Granada\" (Zambra), Fandangos, Alegrías, Bulerías, \"Canaveral\" (Rumba Flamenca), Tango, Verdiales, \"Mi Favorita\" Write for complete details MARIANO CORDOBA 647 EAST GARLAND TERRACE SUNNYVALE, CALIFORNIA 94086, U.S.A.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 1220,
    "article_char_count_full": 7343,
    "article_char_count_review": 3479,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "reference"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_12::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nINTERVIEWED FOR JALEO BY PACO SEVILLA AND JUANA DE ALVA ROSA MONTOYA PERFORMING AT THE ZELLERBACH AUDITORIUM, UNIVERSITY OF CALIFORNIA AT BERKELEY, 1975 To get us started, will you tell us something about your family? Ramón Montoya was my father's uncle, that is, Ramón's sister was my father's mother. Carlos Montoya is my father's brother, one of three brothers, all of whom played the guitar. The other brother, Juanjo, was a professional guitarist, but he died very young--in his twenties. My father only played the guitar for himself--he spent more time with his father's family, who were livestock dealers and went from feria to feria buying and selling horses. The mother and father of the three brothers died quite young, so Carlos, Juanjo, and my father went to live with Ramón Montoya and\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"family\"]\n\nlivestock dealers and went from feria to feria buying and selling horses. The mother and father of the three brothers died quite young, so Carlos, Juanjo, and my father went to live with Ramón Montoya and his mother. I was raised by Ramón Montoya's daughter, who is like a mother to me. My father was sick, half-paralyzed, and couldn't work much, so when I was very young my brother and I had to go to live with my aunt, Ramón's daughter. All of my family is gypsy, but my aunt's mother, Ramón's wife, was a \"paya\" (non-gypsy) and, besides that, daughter of a Guardia Civil. In those times, the gypsies and the Guardia Civil did not like each other. For that reason, the gypsies did not like her at first, but she was such a good woman and did so much for the gypsies that, later, everybody came to her and praised her. So, the aunt I lived with was half-gypsy. She married a \"payo,\" a lawyer. We lived, also, with Ramón, whom I called \"abuelo\" [grandfather], and his wife. I remember that I always used to dance and Ramón didn't like it, because he didn't want any of the women in the family to be dancers. In those days, the bailaoras did not have a good reputation. But all of the men were guitarists -- Carlos, the other brother, Ramón, and one of Ramón's sons, who was also named Ramón. I recall very well when Ramón died, because it was so incredible at our house. There were long lines in the street Calle de la Cabeza, which i\n\n[ENDING CONTEXT]\n\nlovers of this much maligned art form, as good Spanish dance -- especially good flamenco -- is about as rare as hen's teeth. And the colorful Montoya troup gave just plain theater lovers their money's worth too. Not only is the company technically accomplished, but they also have a considerable amount of the right \"gitana pura\" style and spirit. This is not to say they are perfect in respect to authentic style and could make one forget an Escudero, but this finely flavored little troupe gets one closer to the meat of gypsy song and dance than, say, Jose Greco used to do with his big company.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ROSA MONTOYA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "7-20",
    "page_number": 7,
    "word_count": 4666,
    "article_char_count_full": 24753,
    "article_char_count_review": 3061,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "family"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_12::A6",
    "article_text_for_review": "DANCING WITH CONTROL Having full control while dancing seems very basic at the first look, but in reality there are many facets and faces of one's control over dance. There is a never ending search and discovery process in these many areas of control. When I speak of inner control and being -- being in control of the body, the total body as the ultimate goal, not the body controlling and guiding our dance and our feelings of dance. As initial inspiration gives way to learning good technique, the body seems very much in control, until our long study periods start to develop \"muscle memory,\" that ability to know that when you execute a turn or a factwork pattern, it will be there. Control over our basic technique is a never ending study process; as we climb to a level of expertise and put all of the isolated parts of our body together to do one beautiful flamenco turn, then immediately we begin the search for and practice of two and three turns and on and on. The same is true for all of the facets of technique -- refining, cleaning, developing a deeper and more complex total involvement. In my 1982 all flamenco workshop, we worked on some very important facets of control that I feel develop the art of the dance to a higher degree. Some of these basics are often neglected and are not given sufficient time and worth in our total study. A few concepts we worked on were: sustaining energy and focus, proper breathing; dancing slowly with power; attacking speed with power, not just winding up into it; the control of footwork with speed, not just loud, but a full range of slow-fast, loud-soft, slow-loud, fast-soft, etc, and all combinations of contrast; control over filling in the music within the campá, developing a \"flow\" within each compás that steadily interprets that particular compás; sustaining interpretation, not switching interpretation just to get the \"Rah-Rah\" ending as a performing artist, controlling the area between artist and audience; making simple things look difficult and difficult things look easy; making the whole body and spirit move as one, completely integrated for whatever interpretation message it sets out to do; the ability to stand still and exude energy and power to the back of the room. These are just some of the ideas to think about in the search for a total approach to control in dance. One of the most important approaches to good over-all control is to be in good physical \"dance shape,\" and to properly warm up and do a regular series of stretch and strengthening exercises. The body is the instrument and, the better tuned it is, the better it dances. I always suggest ballet for flamenca dancers; for many, it falls on deaf ears, but it is one of the best approaches to body control, centering, strengthening, stretching, breathing and discipline, which all can be applied to flamenca without becoming or looking like a ballet dancer. Another facet of control is sustaining energy and focus. I am speaking of that special inner tension that is like a spring, coiled, like a stalking tiger. Think of tension as flexible, not stiffness like a cement wall, but a sustained energy that you release at will. Think of your body as independent of gravity, your upper body suspended, your legs and hips free to move, your arms sustaining energy to bring them down as well as to lift them, a feeling of suspension, like moving under water. Also, along with energy is breathing, total breathing deep into the body, both with nose and mouth. It is important to find your breathing spots within your dance movement, otherwise all of the muscles in the world will not prevent the huff and puff and strain, sucking out all of the aesthetics. Along with breathing is focus, an inner focus as if you are stalking an unseen force. This focus I am speaking of is not a mystery, nor is it just a focus with the eyes, but",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 677,
    "article_char_count_full": 3869,
    "article_char_count_review": 3869,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_12::A7",
    "article_text_for_review": "THE GUITAR, THE BANDURRIA, AND THE CANTE JONDO DEVELOPED THE SPANISH DANCE by Juan Martínez [from: La Prensa, 1942; sent by Laura Moya; translated by Paco Sevilla] The guitar has seven strings in Russia and is used especially by the Russian gypsies; in Italy the guitar has up to nine strings, but in Spain, only six. In the early times, it was used only as an instrument of accompaniment, both for dancing and singing. In Andalucía the guitar took another, more important, path of development, passing rapidly from the role of accompanying the singer. In Valencia, the guitar served and still serves as an instrument to accompany the songs of Valencia, and in Aragón it completes the great \"rondallas\" [musical groups] made up of bandurrias [mandolin-like instruments] that would sound incomplete without the guitars. The bandurria, being an instrument for playing melodies, was adopted by the gypsies so that, between it and the guitar, they could harmonize the melodies they needed for the development of their dance. But the guitar couldn't stop there; it was made to reach a much higher position, and it was easy for it to achieve that position because of the role it played in the cante and baile. The early guitarists, without having anything of their own except a great ability to follow rhythms, were able to accompany whatever dance was executed in front of them. At the same time, the cante then just beginning to be very rich in variations, but lacking in titles to give recognition to the different melodies, was, like the dance and the guitar, looking for a way to classify the differences in styles, even though almost all came from the same origin -- the gypsies. MENTAL ARTISTRY IN FLAMENCO a revolutionary approach to mastery for dancers and guitarists * improve technical skill * accelerate learning * develop mental & muscular control * enhance performance",
    "title": "JUAN MARTINEZ: EL ARTE FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "22",
    "page_number": 22,
    "word_count": 315,
    "article_char_count_full": 1876,
    "article_char_count_review": 1876,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_12::A8",
    "article_text_for_review": "GUIBLE RMB TIPS ON LEARNING TO SPEAK SPANISH PART 3 This is the last installment of articles on learning Spanish. Here are some more questions that people ask: Q: Is Spanish easy or difficult? A: That is really a subjective question. Some people have difficulty with Spanish and others find it easy. When I hear someone say that it's easy, I usually interpret it as meaning that it's easier than most other languages. You do not have to learn a new alphabet, and the sounds are very predictable. This does not mean that it is easy, though. Why does this issue have to be settled? Is Spanish easy or difficult? I think the question is asked by the prospective student to size up the teacher. The student wants to hear that it is easy, since it gives credibility and authority to the teacher. If the teacher is a good salesman he can even sell the product as being arduously difficult, only for the chosen few. This approach appeals to the elitist personality. One feels that Spanish is extremely difficult for the masses, but simple for him. Q: My teacher at the university tells me not to watch the \"telenovelas\" (soap operas), but rather the news broadcasts. What do you think? A: Watch both! The newscasters speak in perfect Spanish and the soap operas have quick dialogue. You can learn plenty from the \"telenovelas\" even though the university crowd may look down on it. The content of the stories may not compare with great literature, but the reason for studying these television shows is to hear people speak with the quick dialogue. Q: What other things can I do outside of class to help to learn Spanish? A: I like this question! It shows student-generated interest and willingness. My way of teaching assumes cooperativeness, not to be confused with adulation. I highly recommend comic books since there are many pictures to serve as visual aids. Again, you may get some frowns from literatura teachers. It's \"not dignified\" enough. Q: Why do you mention this dignity issue in many of your articles? A: Much of Spanish and Latin American literature deals with the theme of dignity: the lord and the peasant, the Christian and the pagan, the beggars, the persecuted, the inquisition, the \"conquistadores,\" and the Indian. Also great numbers of articles have been published, especially in the \"New World\"",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "23",
    "page_number": 23,
    "word_count": 398,
    "article_char_count_full": 2311,
    "article_char_count_review": 2311,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
