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
    "article_id": "JALEO_1979_07::A17",
    "article_text_for_review": "This month's juerga will be held in the home of Deanna and Jesse Davis. Deanna has been dancing, acting, and modeling professionally since the age of fifteen. She is currently a member of the flamenco troup \"Fantasía Española\" and membership secretary of Jaleistas. Jesse Davis, who just finished a three month engagement at the Hotel del Coronado, is a singer and song writer with several albums to his credit. The Davis home is in Tierrasanta. Take Tierrasanta Blvd. east off of highway 15; then turn left (north) on Santos Rd., continuing almost to the end. Turn left again on El Comal and look for the juerga sign on your right. This is for members only. Guests are by special permission only (call Juana: 442-5362 or 444-3050). DATE: JULY 21 PLACE: 10460 EL COMAL TIME: 7:00 P.M. UNTIL?? PHONE: 277-6141 WHO: MEMBERS ONLY BRING: TAPAS (APPETIZERS) TO SHARE, FOLDING CHAIR, WARM WRAP (INDOOR-OUTDOOR AFFAIR). FRUIT PUNCH OR WINE WILL BE AVAILABLE FOR A SMALL DONATION, OR BRING YOUR OWN.",
    "title": "JULY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_07",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "30",
    "page_number": 30,
    "word_count": 168,
    "article_char_count_full": 991,
    "article_char_count_review": 991,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_08::A1",
    "article_text_for_review": "by Paco Sevilla (This article is a consolidation of information from a variety of sources. The author had a couple of conversations with Carlos in Spain in 1977. George Ryss, who suggested the article, talked with him this past spring and sent most of the factual data and publicity material. Frank Miller, a friend of Carlos in the \"early years,\" sent some of his impressions. And Brook Zern gave permission to use some material written by him. Hopefully, the resulting picture of Carlos Lomas is reasonably accurate.) I met Carlos \"Chip Bond\" Lomas, or \"El Chipi\" as he is often called in Spain, at La Boveda, an old mill-turned-restaurant on the cliffs overlooking the Mediterranean. He was doing an \"American\" type of flamenco show with the guitarist playing for two dancers and doing lots of solos. Carlos is tall and has light brown hair, but does not look out of place playing flamenco guitar. He is considered by many to be one of the top American-born flamenco guitarists and enjoys considerable popularity on the East Coast of the United States. Frank Miller says that they first met when Chip was a bass major at the Philadelphia Musical Academy in the mid-1960's. At that time Chip was really getting into flamenco guitar, playing \"... 12 or more hours a day, 7 (or more) days a week,\" and Frank warned him \"You'll flunk out of school, and your marriage engagement won't last too long!\" Apparently the engagement didn't last, \"...and he never would have made it through the school thing if he hadn't become proficient with flamenco guitar solos for his final graduation concert -- he played bass some, but it was the guitar that did it.\" In any case he earned a degree in music education; later, he was to teach at the Hoff-Barthelson Music School in Scarsdale, New York, the 92nd St. YMHA in New York City, and the Staten Island Jewish Community Center. Frank continues, \"I showed him everything I knew in less than a year -- he was spending hours with Sabicas records -- and he took lessons with Mario Escudero in New York City, where he also met a dancer, Liliana, who became his wife and partner. In the summer of '69 or '70 he went to Spain, where one of his first teachers was Juan Maya.\" According to George Ryss, Carlos considers Emilio Prados to be his \"maestro\" but I don't know when or where he studied with Emilio. A little more from Frank, \"Chip has always had an incredible ear...a blessing and a curse at the same time. Blessing: he would slide into one proper chord tone after another when playing for singing - in the beginning, way before he knew anything about accompanying -- or any Spanish. Curse: some of those far out, weird, soul-slicing chord tones of the old singers are hard to get into when you don't drink (author's note: Frank indicated that Carlos doesn't drink, but he downed several beers while I talked to him in Torremolinos) and must have your instrument precisely tuned.\" Since approximately 1972, Carlos has had an apartment on the outskirts of Málaga. He spends at least half of the year on the Costa del Sol and has worked in a number of places like La Bóveda and tablaos such as the \"Gran Taberna Gitana\" and \"Tablao de Emi Bonilla\" in Málaga, \"La Pagoda Gitana\" in Marbella, and \"El Jaleo\" in Torremolinos. He occasionally spends time in Madrid and has worked in the tablao, \"Cuevas de Nemesio.\" Carlos' dance experience is not limited to tablaos and is really quite extensive. He has played for the following dancers and dance companies: Rafael de Cordoba, Ramon de los Reyes, Antonic Santaella, Jose Molina, Maria Alba, Mariano Parra, Marcelo, Manuel Nunez, Jose Greco, Estrella Morena, Luis Rivera, Jose Antonio, Carmen Acevedo, Pinto (brother of Pansequito), and his wife, Liliana Lomas. Carlos has also accompanied a great deal of singing; some of the cantaores he has worked with professionally are: Chano Lobato, Emi Bonilla, Miguel de los Reyes, Domingo Alvarado, Luis Vargas, El Chocolate, Agujetas, Paco Torronja, and Gitanillo de Bronce. He claims to have played in one juerga with Camarón de la Isla, Juanito Villar, Turronero, and Pansequito. In 1978, he accompanied Chano Lobato in a festival in Málaga (it is rare for a non-Spaniard to play in a festival). Among the Hispavox records to surface here are many of that firm's important releases from the past two decades -- now bearing new covers and (confusingly) new copyright dates and serial numbers. Included are \"El Cante de las Minas\" by Antonio Pinana, a fine interpreter of the cante de levante (Hispavox 0-067 S); and \"Cien Anos de Cante Gitano\" (0-061 S) by Antonio Mairena. (Carlos Lomas, ) In his solo performances, Carlos plays in the modern style and includes solos by Sabicas Esteban Sanlúcar, Mario Escudero, and Niño Ricardo, as well as his own compositions. According to Brook Zern, \"His technique is formidable, but it is never displayed for mere effect. It is subjugated to an innate musicality as demonstrated in his creative and original compositions...He is a superb interpreter of flamenco.\" When He is in New York, Carlos frequently appears at the Chateau Madrid. In 1976, he gave a performance in the Carnegie Recital Hall in New York and was such a success that he was asked to return in 1977. He was the first foreign flamenco guitarist to be presented by the Spanish Institute in New York, and each year, he returns to the United States for a season of performances.",
    "title": "CARLOS BOND LOMAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_08",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "3,22",
    "page_number": 3,
    "word_count": 933,
    "article_char_count_full": 5413,
    "article_char_count_review": 5413,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_08::A2",
    "article_text_for_review": "Here it is! Issue number one of our third year. How are we doing and what does the future look like? The answers to those questions will involve many statements that we have made over and over, but it can't be helped -- they are things that need to be said. Jaleo has a readership not much larger than it had a year ago, but the make-up of that readership has changed. In the first year, many of our subscribers were not flamenco aficionados, but people who joined so that they could go to our juergas. Most of our readers were from the San Diego area and a large percentage of them did not resubscribe. Now, approximately one third of the members are from San Diego. Other cities with more than 25 members each are New York, Los Angeles and San Francisco. The remaining members tend to be quite scattered throughout forty states and a number of foreign countries. Each month, we receive 25 to 50 new subscriptions and the money for those memberships is used to print and mail the $ \\underline{\\text{Jaleo}} $; our bank account fluctuates, but essentially we have no money and are dependent upon each month's income to produce the magazine. This is not good business policy, but we have gotten by so far. Perhaps somebody with business skills will come along and get us straightened out. Meanwhile, our plea for financial help from the readers received a wonderful response We had enough extra money in June and July to fix up our work area a little bit and buy a few pencils. Also, we had an increase in new members in those months. We wish we could praise every person who contributed to Jaleo, but it is impossible; we would certainly overlook somebody and we don't want to do that. We have to thank the person who sends us $100, and the person who sends us $10; we have to thank the person who sends us a list of 20 potential subscribers and the person who sends us 1 name and address; we have to thank those who put forth considerable effort to donate items for Jaleo to sell; then there is the person who buys 5 gift subscriptions for his friends, and those who buy t-shirts, back issues, records -- all of these things mean much to Jaleo. And the people who give of their time, those who send information and articles, those who put the Jaleo together. You see, there is no way to thank each contributor. So once again, thank you everybody. And don't stop! This is a reader written and reader produced magazine and, until we get a big financial break or grow large enough, we will continue to need assistance. For example, we are almost too numerous for the Jaleo to be assembled (folded and stapled) by hand; machine assembly by the printer will be a tremendous new expense for us. There is how we stand. It is up to all of you to maintain the enthusiasm that you have shown in the last year. Given that, we can survive and progress.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_08",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 522,
    "article_char_count_full": 2841,
    "article_char_count_review": 2841,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_08::A3",
    "article_text_for_review": "LEADERS AND FOLLOWERS IN FLAMENCO The flamenco world, just like any other human world, is comprised of leaders and followers. Flamenco needs both kinds of people so that it can continue to exist. Followers could be defined as: aficionados, imitators of other artists, occasional concert-goers, record buyers, and people who hire artists for private juergas. Leaders, then, would be performers, creators, recording artists, or people who lead the flamenco life style with flamenco as an important active pursuit in their daily lives. The two groups, as I see it, have many people who overlap and are members of both groups at times. Also there are people who are clearly in one group or the other. A creator can also be an aficionado, buyer of records of other artists, and even imitate them as a hobby. Some creators of the old school only will play, dance, or sing their own material, or so they say. Some knowledgeable people in flamenco immediately make it known that they are not performers. They can then analyze the art of others and be immune from criticism. Some of these will indeed perform when they feel everything is safe, and they will get acclaim. The transition from imitator to creator is one of the most difficult things to do. Not even the greatest creators are without elements of others in their style. One should be influenced, but not dominated by others. After years of imitation, an artist is then ready to start branching off. This varies for each individual and is mainly a question of readiness. At that point one must make the sometimes painful decision to break with mentors, idols, or teachers. It's time to be one's own person.",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_08",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 284,
    "article_char_count_full": 1658,
    "article_char_count_review": 1658,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_08::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(from: $ \\underline{\\text{Cambio 16}} $, May 20, 1979; sent by Brad Blanchard and Paca Villarroel, Madrid) The approximately 25 tablaos operating in Spain are the only job security for hundreds of artists, singers, dancers, musicians and palmeros, who now, for the first time in their history are planning social vindications. The flamenco tablaos do not consist of only carnations, smiles, and rhythms. They are also the personal drama of singers and dancers, almost always underpaid, many times exploited and always unprotected. More and more each day, they are becoming a business for small entrepreneurs -- who make their money although not so much -- and for foreign tourists. As opposed to the circus, it keeps on being a living spectacle that, in spite of everything, survives and seems to\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"audience\"]\n\nmes exploited and always unprotected. More and more each day, they are becoming a business for small entrepreneurs -- who make their money although not so much -- and for foreign tourists. As opposed to the circus, it keeps on being a living spectacle that, in spite of everything, survives and seems to have a future. And there are always those with aspirations to triumph, the young artists of the guitar, and zapateo, always ready to cheer up the audience for a few pesetas. There are cases where the artists work without signing a contract. Others, where signatures are somebody else's because the artist does not know how to sign. The artist is paid for each performance and usually is thankful for the opportunity. But times change, and now they are demanding seven days annual vacation as a minimum. With pay! There are some who seek a weekly day off and some who resign themselves not to have it because on that day they can charge double. Before, if the got sick and had to leave the job, they lost their employment without recourse; with democracy, they have dared to demand that, if they are considered by the Social Security to be sick and a leave of absence is recommended, they be entitled during that time to 75% of their salary. Antonio Machado said, on one occasion, that he didn't understand how people were able to amuse themselves with flamenco and with bullfights, those being such serious things. Machado was the only one of the generation of '98 who loved flamenco because his father had compiled old songs and the theme belonged to family traditions. The intellectuals of '98, on the other hand, were in love with the arid and austere Castillian plain. For flamencos, however, not everything is tears or tradegy, not everything is the grief of children of nobody, unloved and thrown into the puddles. In the complex art of gypsies and A\n\n[ENDING CONTEXT]\n\nSpain, prizes in many art competitions, and a number of one-woman shows. \"Bajo la Luna Gitana\" is a woodcut print which measures 12\" X 26\" and the price of $125.00 includes the matte and mailing costs; this is a beautiful work for which our reproduction here does not do justice. The two Manolete bullfight prints are offset prints, 8½\" X 11\", and the price of $15.00 does not include any form of mounting. If you like to own beautiful art and would like to treat yourself and, at the same time, $ \\underline{\\text{help Jaleo}} $, this is your chance. Send a check or money order to: \"MANOLETE\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "THE OTHER SIDE OF JONDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_08",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "5-7",
    "page_number": 5,
    "word_count": 1256,
    "article_char_count_full": 7143,
    "article_char_count_review": 3483,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "audience"
      }
    ]
  }
]
```
