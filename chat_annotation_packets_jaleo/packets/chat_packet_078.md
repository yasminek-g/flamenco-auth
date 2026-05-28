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
    "article_id": "JALEO_1980_07::A8",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAREA 'JALEISTAS' KEEP ALIVE THE SPIRIT OF THE FLAMENCO (from: $ \\underline{\\text{The San Diego Union}} $, May 11, 1980) By Sue Garson Spirits livened by Spanish wines, clicking castanets, the sight of red flowers pinned to long, coal-black hair, plus the electricity of the performers' friendly rivalry produce the finest sounds of the juerga in the early morning hours. True to Spanish tradition, the boisterous din of the juerga -- a flamenco festival -- doesn't really get going until after midnight, although it officially begins at 7 p.m. Several of the 80 bystanders shout, \"Eso es, María!\" Instant imagery of suffering, of sad-eyed gypsies are conjured as María José Díaz sings the couplet, \"There is no doubt that in this world, we are born to suffer,\" the quintessence of flamenco which,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"intim\"]\n\nTrue to Spanish tradition, the boisterous din of the juerga -- a flamenco festival -- doesn't really get going until after midnight, although it officially begins at 7 p.m. Several of the 80 bystanders shout, \"Eso es, María!\" Instant imagery of suffering, of sad-eyed gypsies are conjured as María José Díaz sings the couplet, \"There is no doubt that in this world, we are born to suffer,\" the quintessence of flamenco which, like blues, reflects an intimate art that displays the primitive sounds of suffering and sentiment. By 1 a.m. most of the tapas (Spanish hors d'oeuvres) are consumed, bottles of sherry are rapidly emptying, while the wooden floor becomes a swirling mass of polka dots, ruf-fles and brightly colored fringed shawls. Thick leather heels tap to the flamenco rhythms of many guitars. and join us. It has provided me with more opportunities to work. Before we organized, no one sang flamenco here. All we had was an occasional guitarist and sometimes a dancer performing in clubs. But these juergas have triggered childhood memories of Spain and suddenly, the songs come back to them. Then, after the songs become perfected, we have singers who perform in night-clubs.\" Most of the flamenco enthusiasts in San Diego a\n\n[ENDING CONTEXT]\n\nat age 13 that he became drawn to flamenco. He continued to pursue music seriously, eventually creating his own compositions and style, and at 20 moved to Spain. Now he pursues his flamenco career here, performing in several county restaurants. His comment on his career choice: \"You don't have to be a born Spaniard to share the philosophy.\" Dawn arrives too soon as the wood-paneled walls of the cottage reverberate with the sound while people slowly drift out. Eventually, only two guitarists remain in the center of the main room, plucking strings in a tone that suggests both joy and sadness.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ART OF SUFFERING",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "18-20",
    "page_number": 18,
    "word_count": 1060,
    "article_char_count_full": 6313,
    "article_char_count_review": 2861,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "intim"
      }
    ]
  },
  {
    "article_id": "JALEO_1980_07::A9",
    "article_text_for_review": "LAST OF A 6 PART SERIES (From: Nueva Andalucía, July 21, 1978. Sent by Bettyna Belén; translated by Roberto Vázquez) by Luis Melgar Reina The flamenco figure most difficult to understand and evaluate for those of us who do not experience him directly, \"a viva voz\" (in living voice), but only through inexact recordings, is without any doubt, Manuel Torre. Manuel Torre has been glorified by all those who have studied his cante and his huge flamenco works in depth. We share the idea held by some flamencologists that Manuel Torre has surpassed human personality and has evaported into flamenco mythology. We think that his great value lies, precisely, more in his creative capacity than in his interpretive possibilities, although we don't doubt even for a moment that the latter falls far behind the former. A whole mythology has been created on true facts of Manuel Torre, as a consequence of his purifying art, so that today, it is the groundwork and even the root of an extensive gamut of cantes. Manuel Torre creates, but when he does it he is not conscious of his creation, it is more an intense and vigorous flamenco flowing than a preconcieved idea. It has been said, perhaps in an excessively disparaging tone, that Manuel Torre was a difficult cantaor, one of those who needs to be surrounded by very special conditions in order for his art to stand out and reach the heights of which he was capable. Perhaps -- without putting a blemish on him -- those who think this way are partly right. The stories of Manuel Torre's performances are full of great failures, so that at the end, when the \"duendes\" converged at certain moments, they would be transformed into the most spectacular and astonishing triumphs. That is natural in geniuses and Manuel Torre was perhaps the greatest in flamenco A premium string designed especially for the flamenco guitar. At your dealer or write to Antonio David—370 West 58th Street, New York, N.Y. 10019. Fandangos de Santa Eulalia Third in a series dealing with different styles of fandangos de Huelva. by Paco Sevilla The geographical origin of the fandangos de Santa Eulalia is not completely clear. These fandangos are sometimes called \"fandangos de Almonaster\" and the coplas sometimes refer to the city of Almonaster la Real, as well as to the Rio Odiel which runs near Almonaster. The coplas also indicate that Santa Eulalia is the patron saint of Almonaster. I am assured by a number of people from Huelva and Sevilla that there is no pueblo named Santa Eulalia, and that all references in the coplas are to Santa Olalla (O-la-ya). Santa Olalla is in a mountainous mining region about sixty miles east of Almonaster, near the eastern border of the province of Huelva, and only about forty-five miles north of Sevilla. The mention of mining in some of the coplas supports Santa Olalla as the home of these fandangos. So, while not ruling out the possibility that their may be a tiny pueblo called Santa Eulalia, we can tentatively say that this fandango style is found in both Santa Olalla and Almonaster and has been named after the patron saint of Almonaster, Santa Eulalia. The fandangos de Santa Eulalia maintain a strict bulerías type compás. Using the most common counting method for bulerías, we have the following for each line of fandangos: $ \\underline{12,1,2,3,4,5,6,7,8,9,10,11} $, etc. Another way to count would be: $ \\underline{1,2,3,1,2,3,1,2,1,2,1} $, etc. Or it could be considered as alternating measures of $ \\underline{6/8} $ and $ \\underline{3/4} $ time. The common strum patterns played on the guitar for fandangos do not accentuate in this manner, but produce instead a straight 3/4 time (1,2,1,2,1,2). The result is that, for half of each line of singing, the cante accents every third beat while the guitar accents every second beat. There would seem to be a conflict. Add to this the fact that most dance choreographies also stress the bulerias pattern and we have both the cante and baile at odds with the guitar. Remember, also, that in some other styles of fandangos de Huelva, the cante does not use the bulerías accentuation, but is consistent with the guitar accompaniment;",
    "title": "TORRE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 703,
    "article_char_count_full": 4158,
    "article_char_count_review": 4158,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A10",
    "article_text_for_review": "Á Santa Olalla he de ir con un hábito morão. (A Santa Olalla he de ir) Que un minero bien plantáo me tiene a mi que cumplir la palabra que me ha dado I have to go to Santa Olalla wearing my purple habit. A handsome miner has to fulfill the promise he made to me. * (es costumbre muy antigua) Eso de ir al Ajunquío es costumbre muy antigua. Allí se lava la cara sin toalla y sin jabón. Al río Santa Olalla. * (que le llaman Santa Oli)(Uli?) Un río en Santa Olalla que le llaman Santa Oli, donde me lavé la cara la primera vez que fuí al río de Santa Olalla * (tiene una fuente escondida) Santa Olalla la minera tiene una fuente escondida donde la(s) niña(s) soltera(s) beben el aguita fría la(s) tarde(s) de primavera. Santa Olalla, the mining town has a hidden fountain where the single girls drink cool water on spring afternoons. * Ay, una hermita chiquita a orilla de Odiel, (ay, una hermita chiquita) donde tengo yo mi fé. Es Santa Eulalia bendita patrona de Almonaster. Ay, there is a small chapel on the bank of the Odiel where I place my faith. It is the blessed Saint Eulalia patron saint of Almonaster. Caballo que a los tres años ve una llegua y no relincha (caballo que a los tres años) es que le farta cebada o es que le aprieta la cincha o es que no vale pa' nada. A three year-old horse that sees a mare and doesn't whinny (a three year-old horse) either hasn't been fed enough or his cinch is too tight or he's completely worthless! From \"Furia Amaya\" by Chato de Osuna: (te voy a decir la verdad) Al cabo de tanto tiempo te voy a decir la verdad; yo no te quiero pa' na' solo ha sio un pasatiempo, con todas me pasa igual. After so much time I'm going to tell you the truth; I don't care for you at all, you have only been a pastime, that's the way it always is with me. * Eres fría como el marmol sin sangre ni corazón; (eres fría como el marmol) eres de la condición que cuando da un desengaño gozas de satisfacción. You are cold like marble, without blood nor heart; you are the sort who when there is misfortune feels the pleasure of satisfaction. * (más que tu sepas de mi) Yo de ti se mucho más, más que tu sepas de mi y sin embargo sabrás que nadie me oyó decir el porque fué terminá I know know more about you, more than you know about me; but you must know that nobody has heard me say why we came to an end. Eso lo dijo uno que esta(ba) barando Eso lo dijo uno que esta(ba) barando en un cortijo. Desde entonce(s) le llaman lo(s) fandango(s) Desde entonce(s) le llaman lo(s) fandango(s) de Santi Ponce. Hasta en Italia se ve bailar los fandangos Hasta en Italia se ve bailar los fandangos de Santa Eulalia. (In the March 1980 article, Fandangos de Cabezas Rubias\" there is a misprint; the last line of the copla at the top of page 21 should read, \"cuando empezaba de vivir.\") (MONTOYA ) \"We had been leaving sawed-off chairs all over the country,\" Mrs. Montoya said, explaining that her husband \"likes to sit 16 inches off the floor...I used to carry around a tape measure so when concert managers would say, 'Yes, this is 16 inches,' I could pull it out and say, 'See!' \" After sawing off metal chair legs and a microphone stand -- to 28 inches -- in Jacksonville, Fla., Mrs. Montoya said, \"then we realized we had to do something.\" \"Well, we went to Alaska (on tour) and met a man there with a furniture factory and, in the course of talking, which, you know, I often do, I told him about this problem and he suggested that I send specifications and he would make one...The next year, when we arrived in Anchorage, he was at the airport with the stool.\" Before the concert in East Hampton, a woman in the audience (who confessed that she had worn a long Spanish skirt just for the flamenco performance) eyed the small bench on the stage and shook her head. \"That tiny little stool he uses,\" she whispered. \"You'd think he'd want a chair with a back to it.\" anywhere. On any stage. But when Montoya lowered himself onto the bench, hiked up a trouser leg and rested the guitar on his knee, it was clear that could feel --- with those accoutrements --- at home On stages all over the world, Montoya spreads a universal language --- music --- with his hands. A sort of international ambassador of goodwill. He has received keys to cities from Miami to New York to San Francisco. He is an honorary citizen of Winnipeg. He has been made an Authentic Sour Dough by the mayor of Anchorage. In Texas, a Stetson hat was custom made --- size 6 --- to fit his bald head. And the day he became a U.S. citizen in 1946, he played for President Truman at the Mayflower Hotel in Washington, D.C.",
    "title": "FANDANGOS DE SANTA EULALIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "23-26",
    "page_number": 23,
    "word_count": 876,
    "article_char_count_full": 4606,
    "article_char_count_review": 4606,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A12",
    "article_text_for_review": "By La Chiquitina El bailaor, José Miguel Herrero, the son of the cantaor Miguel Herrero and of the bailarina Carmelita Vázquez, has been imbued, not only in the $ \\underline{\\text{art}} $ of flamenco, but in the $ \\underline{\\text{life}} $ of flamenco since his birth. He grew $ \\underline{\\text{up}} $ \"on tour\", in dressing rooms and on stage. José Miguel has never had \"academic\" schooling in flamenco. He $ \\underline{\\text{lives}} $ flamenco. His dance style is unmechanical and unplanned. He does not preplan his choreography. His dances are filled with a duende \"of the moment\". His compás is impeccable. He achieves the very difficult merging of feeling and technique. His emotion does not get lost in his technique and his phenomenal technique does not overpower his emotion. His virile; masculine air contradicts the over-effeminate image of so many male flamenco dancers. José Miguel is a natural, \"un-made-up\", truly $ \\underline{\\text{male}} $ Spaniard. He brings to mind the spontaneous sevillanas at the ferias or at the Rocío; the untamed fury of a bystander in a café in Utrera suddenly compelled to dance.",
    "title": "JOSE MIGUEL HERRERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "28",
    "page_number": 28,
    "word_count": 180,
    "article_char_count_full": 1123,
    "article_char_count_review": 1123,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A13",
    "article_text_for_review": "Guillermo Salazar visited San Diego in May and tried to take in as much flamenco as possible. He joined in at the juerga and at the Andalucia Restaurant and those of us who had the opportunity enjoyed meeting him and hearing him play. We hope he will return many times in the future. Here are his impressions of some of the flamenco he experienced in San Diego. MAY JUERGA This month's juerga was my first one with San Diego's Jaleístas. Here are my impressions. Herb Goullabian and I arrived a little late and found lots of flamenco activity in progress. I didn't know exactly what to expect and was kind of nervous. Upon our arrival, we were greeted by Juana de Alva, who made me feel very comfortable and introduced us around. After mingling with the Jaleístas for a while, the flamenco started again. Sevillanas were sung and danced to the guitars of Rodrigo de San Diego and Miguel Ochoa who was down from Los Angeles. Remedio sang alegrias for Magdalena Cardoso and Remedio and Rodrigo's little daughter Raquel improvised to bulerías. There were many participants, including professionals, semi-professionals, amatuers, and students. The juerga proved to be good for everyone. The professionals performed and publicized themselves, while the non-professionals had a chance to perform in front of an audience and gain experience with other performers. Everyone was enjoying the juerga so much for social value and flamenco that I thought flamenco could be good therapy if it were presented this way elsewhere. If flamencos would accept each other and encourage each other once a month, we would approach a more perfect world. Maybe it doesn't eliminate all friction, but it sure helps in this direction. Later the activity moved to the jondo room. I joined in on the guitar and played for 15 minutes before losing my thumbnail. I yielded to the strong compás of Yuris Zeltins aided by Miguel Ochoa, Herb Goullabian, Tony Pickslay and Roberto Vasquez. We all enjoyed the dancing of Julia Romero from Sevilla and Magdalena from Mexico. Andalucian born Vicki Dietrich tried out her bulerías and fandangos for the first time at a juerga. After midnight Paco Sevilla arrived with his group after their show at the Andalucía. Dancer Luana Moreno and singer Pilar \"La Canaria\" were accompanied by Yuris, Miguel Ochoa and Herb. Then singer Rafael did a cana, farruca, fandangos grandes and, inimitably, bulerías. Herb and I started to tire at 3:30 a.m. and left the juerga. We both thoroughly enjoyed it and highly recommend it to Jaleistas and other flamencos around the U.S. DAVID CHENEY David Cheney is playing Sundays and Mondays at Rudy García's Mexican restaurant in Pacific Beach. I had heard of David Cheney from the old FISL newsletters and also from $ \\underline{\\text{Jaleo}} $, so I was very curious about him. When I entered, Cheney was playing a solo note for note from a Manolo Sanlúcar record, and I imagined he was a Sanlúcar imitator. I thought his second set would be more Sanlúcar material. After finishing the solo it was time for intermission and Cheney came over to the table to greet us. After an animated conversation he returned to play more. I was amazed by his selection of solos. He began with a Sabicas danza Mora from Electra Vol. 2, and then played Paco de Lucía's alegrias in D, Paco Cepero's granadinas from \"Amuleto\", Paco de Lucía's soleá from \"Fuente Y Caudal\", Serranito's alegrias in E from \"Virtuosísmo\", and Sanlúcar's \"Bulerías de las Gitanas Marquesas\". The list went on and on and, finally, when he stopped, he came over to the table again for more interesting conversation. In the back of my mind I was thinking \"This guy is a flamenco jute box.\" He plays everything recorded by everybody. As far as I could tell the fingerings were correct and the harmonies were exact. Whenever he played a certain guitarist's material, it was a very accurate interpretation. David Cheney is the Rich Little of flamenco guitar! I enjoyed his show very much. It seemed to me that the food was the attraction at the restaurant rather than flamenco. The people responded with applause, but were not paying attention for the most part. I enjoyed Cheney both as a guitarist and as a person. This unusual guitarist is well worth seeing.",
    "title": "GUILLERMO SALAZAR IN SAN DIEGO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 29,
    "word_count": 723,
    "article_char_count_full": 4257,
    "article_char_count_review": 4257,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
