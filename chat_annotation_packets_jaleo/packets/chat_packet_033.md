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
    "article_id": "JALEO_1978_10::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nBy Angela de la Yglesia (The following is from a Spanish gossip type magazine, the name of which is unknown to us, but probably appeared in 1976. Thanks to Brook Zern for bringing it to our attention.) Translated by Paco Sevilla and Ron Ryno This boy with the long and serious face that reminds one of Caradine, the Kung-Fu of television, has dared to say that he will make flamenco evolve (you know, flamenco: that untouchable guitar, made from the wood of centuries, that is defended from the first string to the bass by wise purists...). This Andalucian with the look of a \"hippy without marijuana\" has achieved with the guitar that which was unattainable for the Segovias, the Yepes, the Segundo Pastors, and the rest of the eminent ones: That hundreds of people cause great disturbances in the\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"classic\"]\n\no the bass by wise purists...). This Andalucian with the look of a \"hippy without marijuana\" has achieved with the guitar that which was unattainable for the Segovias, the Yepes, the Segundo Pastors, and the rest of the eminent ones: That hundreds of people cause great disturbances in the doorways of the cultural theaters of the world when they can't get in to hear him. He is adored by the gypsies in spite of the fact that he is not a gypsy. The classical musicians admire his technique. The more progressive musicians, like Eric Clapton, affirm that there is \"nobody like him\". And flamencos say that he has revolutionized the guitar. Why this boom? Who is Paco de Lucía? What does he feel? How does he think? What is true in him and what is a prefabricated product? I was almost at the point of not ascertaining, and not because I was faced with the traditional \"chase after the idol\". Paco de Lucía does not act like a great star. But he suffers - and makes suffer - his followers. Nervous and without a guitar to calm his nerves, between the airplane that took him to Palma de Mallorca and an automobile that would carry him to a concert in Oviedo, something of what he has inside floated out. Paco de Lucía has been accused of something terrible - of not being a pure flamenco guitarist. Can it be that he do\n\n[ENDING CONTEXT]\n\ngrasp first the easiest. It is like a big balloon; with what is left after it is deflated, Paco will construct that which really interests him. He only wants money in order to be free from material needs. He plays -- and why not -- at popular prices. In the Teatro Real of Madrid, where he introduced for the first time the flamenco guitar, those people who had never had the opportunity to attend a concert there, lived through a great afternoon for thirty or a hundred petas (50¢ to $1.50). On the stage they had to put six hundred extra seats, and still, a thousand people were left outside.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PACO DE LUCIA: A Discussion on his Revolution of Flamenco",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_10",
    "year": 1978,
    "language": "en",
    "article_type": "poem",
    "pages": "7, 8, 9",
    "page_number": 7,
    "word_count": 1315,
    "article_char_count_full": 7224,
    "article_char_count_review": 2941,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "classic"
      }
    ]
  },
  {
    "article_id": "JALEO_1978_10::A7",
    "article_text_for_review": "Translated from the Spanish by Paco Sevilla (The following is taken from $ \\underline{\\text{Hoja del Lunes de Madrid}} $, which appeared in 1971 or 1972. It is of interest not only because it tells a little about Teodoro Morca, but because the attitude of the interviewer is so typical and Teo handles it so well. The word \"cingaro\" refers to non-Spanish gypsies.) The stage is deserted and in the back, a dim light hardly served us for our dialogue. He's an unusual man; tall, extravagant, \"cíngaro\". He confesses to having danced flamenco for twenty years and has appeared in shows with Frank Sinatra and Dean Martin. He appears as relaxed as if, for a Hungarian to toss his hair in a tablao and to be named Teodoro Morca, it were the most natural thing in the world. \"I learned to dance in New York, Los Angeles, and also in Spain. I have had many teachers. It is a very interesting experience now to dance in a tablao with an authentic gypsy like La Chunga.\" -- Partner of La Chunga, Hungarian, concert dancer, and at times, gypsy -- Do you believe all of this makes sense? \"Flamenco is an art. I believe that anyone who feels the music can dance flamenco. Having been born to the sound of jaleo and palmas does not give one the ability to appear on the stage of a theater. Besides, we \"cíngaros\" are almost the same as \"los gitanos.\" -- Ah, very good! And you believe then that everything lies in technique? You don't feel a special \"tickle\" when you dance? \"What I enjoy and feel is the drama. I love the baile por soleares and the cante jondo.\" -- How does the public react to a Hungarian dancer? Teodoro Morca en el Caé de Chinitas \"Look, I have had a lot of luck in Spain. The people don't know what you are. I insist that, to me, being Hungarian is not very important.\" -- Listen, Mr. Tempermental, for you, what role do the palmas and taconeo play in your life? \"Flamenco is part of my life. When I dance, I feel natural.\" -- Since I, with your permission, consider you to be neutral, who are the best? \"I can't answer because I have many friend and this little world is very small. Some great dancers are Antonio Gades, Antonio, Rafael de Córdoba...\" -- And the women, what can you tell me? \"I will tell you three of the best: La Chunga because she is so pure, María Soto, and Carmen Mora.\" -- What about money? \"I have earned a lot with flamenco. I have lived many years from my dance. Now I go to the U.S.A. to do a tour. It will be very interesting. I will take flamenco to the American universities.\" Juana tells him to go onstage. In a moment he is there alone. A small world, partially internal, seems to open for him. He takes it very seriously. The shawls and castanets are asleep on the walls. A hungarian flamenco, little by little, starts to wake them up. He says it makes no difference; he could be right.",
    "title": "CASI GITANO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_10",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "10",
    "page_number": 10,
    "word_count": 527,
    "article_char_count_full": 2829,
    "article_char_count_review": 2829,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_10::A8",
    "article_text_for_review": "\"Look, I have had a lot of luck in Spain. The people don't know what you are. I insist that, to me, being Hungarian is not very important.\" -- Listen, Mr. Tempermental, for you, what role do the palmas and taconeo play in your life? \"Flamenco is part of my life. When I dance, I feel natural.\" -- Since I, with your permission, consider you to be neutral, who are the best? \"I can't answer because I have many friend and this little world is very small. Some great dancers are Antonio Gades, Antonio, Rafael de Córdoba...\" -- And the women, what can you tell me? \"I will tell you three of the best: La Chunga because she is so pure, María Soto, and Carmen Mora.\" -- What about money? \"I have earned a lot with flamenco. I have lived many years from my dance. Now I go to the U.S.A. to do a tour. It will be very interesting. I will take flamenco to the American universities.\" Juana tells him to go onstage. In a moment he is there alone. A small world, partially internal, seems to open for him. He takes it very seriously. The shawls and castanets are asleep on the walls. A hungarian flamenco, little by little, starts to wake them up. He says it makes no difference; he could be right.",
    "title": "Teodoro Morca en el Caé de Chinitas",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_10",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "10",
    "page_number": 10,
    "word_count": 226,
    "article_char_count_full": 1189,
    "article_char_count_review": 1189,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_10::A9",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPART III by Suzanne Keyser LAST DAYS - MORON AND BACK TO MADRID As I said before, the night life in Morón can be pretty hard to take if you're not a flamenco (and even if you are, because flamenco doesn't happen $ \\underline{\\text{every}} $ night). But there is always Sevilla, which is about two hours' drive away. One night Tana, Agustín, Chuck, and I piled into Tana's little old Seat, and off we went on the bumpy road to Sevilla - across the fields, headed for yet another tablao. After Madrid, we weren't exactly keen on the idea, but Tana managed to convince Agustín to tear himself away from his beloved Morón and his guitar and take her to see Manuela Carrasco, the current flamenco dance star of Spain. We had seen her in an impromptu bulerías in \"Café de Chinitas,\" and she had been\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"tablao\"]\n\nher in an impromptu bulerías in \"Café de Chinitas,\" and she had been extremely impressive then even in her casual slacks and high platform shoes (in which it is virtually impossible to dance - but dance she did!). So off we went, with Agustín in his white suit, Chuck in his gangster outfit, and Tana and I in our best dresses. We arrived early enough to have a couple of copas in the Barrio Santa Cruz where the drinks are much cheaper than in the tablao. On the whole, the show at \"Los Gallos\" (the tablao we went to in Sevilla) was much better than anywhere else, although they have the same package, with the perennial blonde rumba dancer and the raven haired sexy voiced \"typical\" gitana in the polka dots. However, the artistic quality was much, much better than in Madrid; one girl was a bit academic and only one was really offensive, but all the others were quite good and projected that special Andaluz gracia which is almost impossible to translate. Manuela Carrasco was absolutely outstanding and deserving of all the publicity and fame. This young gypsy girl (who is no more than 18), who has a face that would be considered unattractive by any accidental standard of beauty, just took over the whole tablao, even with the other dancers as good as they were. Her stage presence is powerful; her movements, although wide and encompassing, are perfectly controlled; now dynamic, now subtle. She commands the\n\n[ENDING CONTEXT]\n\nin his right mind would ever leave his province to start a business in a foreign country). As a result, most of the Spanish restaurants run by Spaniards are run by Basques, Catalans, or the hardest of the plastic set, the Madrileño. At best they will offer a silly version of a Madrid tablao, and at worst they will advertise Mexican music or Italian music as flamenco (as El Matador and Chateau Madrid in Montreal have done). They are under the conviction that what appeals to a Spanish public will appeal to the world in general, which is the reason that most \"flamenco\" restaurants fold.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Dance Experiences in Spain",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_10",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "11, 12, 13",
    "page_number": 11,
    "word_count": 1698,
    "article_char_count_full": 9444,
    "article_char_count_review": 3032,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "tablao"
      }
    ]
  },
  {
    "article_id": "JALEO_1978_10::A10",
    "article_text_for_review": "A REPORT ON EL CID IN LOS ANGELES by Paco Sevilla I finally got a chance to visit El Cid, Southern California's only surviving flamenco tablao; I was anxious to resolve conflicting reports which had said \"...not very impressive!\" or, \"...the best tablao in the country\" and also to see Carmen Mora in action after many weeks of watching her teach in San Diego. What I saw was the finest evening of club flamenco that I have experienced outside of Spain. I don't know how the club scene is in the East, but this show certainly far outdis-tanced anything I have seen in California. The \"Cid\" is very attractively decorated and well set up for performances; there are few, if any, tablaos in Spain that pay such careful attention to decor and the technical aspects of the performance, such as stage visibility and sound (some Spanish tablaos may be superior in one particular aspect, such as Torres Bermejas for decor or Café de Chinitas for lighting, but they all fall down in many areas). The P.A. system sounded better than any I have heard, with the guitar sounding very clear and very natural. The first show featured a cuadro that gave the impression of filling the stage with bodies, although there were actually only five performers -- guitarist Antonio Durán, singer-dancers Raul Martin and Concha de Morón, and dancers Juan Talavera and Liliana Morales. They opened with a spirited sevillanas, the singers taking turns, then Concha danced alegrías. Antonio played a guitar solo, tarantas-tarantos, followed by Liliana in an elegant solea sung by Concha. The rest of the show consisted of a long finale \"por bu-lerías\". Concha sings well with a modern sound and I enjoyed her very much. Raul adds a human touch with his traditional style singing and unschooled natural dancing; he also added to the show with his introductions and explaining of the numbers. After an intermission of 45-60 minutes, (I didn't keep track) the second show opened with an alegrias by Carmen Mora, Concha singing, Antonio playing guitar and Juan Talavera doing palmas. This number developed into a zapateado danced by Talavera. After a guitar solo \"por bulerías\" Carmen danced her tarantos and the group finished with bulerías. Each number flowed into the following one so that, as Carmen explained later, there was no letdown in the show. I agree with her in the sense of keeping the show going, but somehow the transition of an alegrias into a zapateado bothers me a little. I was hit by three thoughts on watching Carmen dance. First, she teaches the same things that she uses in her dances -- there is no holding back of her good stuff and teaching students other movements. Second, Carmen's style is unique; if one comes expecting to see classical traditional flamenco, one is apt to be surprised. I'm not going to try to describe her dance except to say that it is pure Carmen, full of the unexpected and a lot of drama, at times almost like modern dance, but, very flamenco. Her personal style is so strong that those who study with her will need to exercise care that they do not over-imitate her and become grotesque caricatures -- always a hazard when studying with someone who has a strong \"propio sello\" (like a Diego del Gastor or a María de la Merced). Lastly, I found that aside from the above thoughts, I was hypnotized by Carmen's dancing to such an extent that I was unaware of exactly what was happening and have very little recall of anything she did. I know I have seen good dancing when I forget to be analytical or critical and don't pay much attention to the guitarists.",
    "title": "Tablao-California Style",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_10",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "13, 14",
    "page_number": 13,
    "word_count": 622,
    "article_char_count_full": 3578,
    "article_char_count_review": 3578,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
