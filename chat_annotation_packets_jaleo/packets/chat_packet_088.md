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
    "article_id": "JALEO_1980_10::A6",
    "article_text_for_review": "Pardon the digression. Perhaps we should start again. \"WM, young...okay, not so young but not aware of it, literate (last book read: The Art of Volkswagen Repair...and I don't even own a Volkswagen), wide interests ranging from flamenco to animal husbandry in the Ukraine to collecting marimbas, seeks female with like interests to share Green Fish Gin at the Festival de la Tortilla Española\". In a more serious -- some would say ponderous -- vein, I have lived in Europe since 1964, most of the time in Germany, but the last five years have been on the beach here in Spain. I work for the European Division of the University of Maryland's University College which offers academic courses for U.S. military personnel stationed overseas. My current posting at the U.S. Naval Station, Rota, allows me to indulge my interest in f-menco, which has grown considerably over the past few years. I live, with my wife and two sparkling children (ages 4 & 10), outside of Puerto de Santa María in an apartment which has an unobstructed view of the Bay of Cádiz. Our life is a happy one, with the warmth of the sun to wake us in the morning and the singing of the waves to put us to sleep at night. I have been in love with Spain since high school when I read everything I could get my hands on about the country, her history and culture; it has been a dream to have been able to live here.",
    "title": "ABOUT GORDON BOOTH",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 252,
    "article_char_count_full": 1380,
    "article_char_count_review": 1380,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A7",
    "article_text_for_review": "SOME THOUGHTS ON THE DANCE From the very beginning, it was the power, the art, and the inspiration of the individual that shaped the growth of the many branches of flamenco, for it has always been the individual that expressed his inner feelings and emotions and life itself. That individuality has been one of the 'eyes to the evolution of flamenco. Before there was ever a thought of flamenco going on stage, when flamenco was being expressed behind closed doors and in open fields in and around the pueblos of Southern Spain, the natural creative drive of the individual, for personal fulfillment, was changing and evolving flamenco. Flamenco, like all great art forms that have no boundaries or borders, is a living, breathing, flowing creative process, very much alive -- like a flowing river, not a stagnant pool. Tradition also grows and moves. Tradition is the tap root with branches that grow and use tradition as a base for For the professional flamenco artist, good technique is a form of freedom. If you are performing nightly in club, tablao, or travelling to concert after concert, there will be many a time that you will rely on good technique, the \"craft\" of your art to get you through a performance. The human being is not a machine, neither is the flamenco dancer; and many times the old cliche \"The show must go on\", is true, even if tired, sick, etc. The ideal juerga, where the elements are all great for the profound flamenco happening, are rare, very rare. Who does not wish for the fine guitarist, superb singer, exciting dancers, great jaleo, sensitive and understanding aficionados, all with aire and gracia to set the stage for the ultimate flamenco happening. It does happen and it is beyond words in feeling and emotion, but like anything special, it happens seldom. There have been many influences in flamenco, some very beneficial, some not so beneficial. Some of the negative influences have been the \"tourist attraction\" aspect of flamenco used in Spain and other countries. This has flourished in the many tablaos as \"quantity instead of quality\". Many people have tried a short cut approach to flamenco, learning a few set routines, adding what they think is a \"sexy-sell-jazzy\" approach to it with \"cuchy-coo\" costumes and passing this off as the latest Vegas-rock-instyle flamenco; in reality, it is just a plain lazy approach and in bad taste for such a beautiful art form. The good influences in flamenco have been the serious artists who realize that flamenco is an art of the highest, most noble stature and bring forth innovative creations, taking the seeds of tradition and cultivating them with dignity, class, style and study. Examples are Vicente Escudero with his Rubina Carmona Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059 Los Angeles, Ca.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 473,
    "article_char_count_full": 2822,
    "article_char_count_review": 2822,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A8",
    "article_text_for_review": "Several years back I met some people who had just come back from a vacation in Torremolinos. During the conversation they said: \"We saw this guitarist, and he was really good!\" I asked, \"How was he good? What do you mean by that?\" They didn't know what to say except that the audience response was good, and every one of the tourists believed that he was good -- a majority rule. I have no idea who the guitarist was or how good he was, but this got me thinking. What does good mean? What are the criteria? The general public has a difficult time understanding flamenco performances since it is a foreign culture that is being presented. Put yourself in the shoes of the uninitiated. Good can mean: (1) Technically good. (2) Soulfully good. (3) The critics of newspapers say it's good. (4) Other flamenco performers say it's good. (5) The general public says it's good. (6) Advertisers with financial interest say it's good. (7) Personal friends say it's good. The uninitiated public, who attends most performances in the U.S.A., basically is looking for good entertainment. It is not interested in seeing real flamenco in a private juerga. It is interested in seeing costumes and choreographies, showmanship and finesse. It wants variety and visual stimulation. The Hollywood approach to flamenco seems especially successful: props on stage like tables in a taberna or exotic plants. People will then leave the show thoroughly entertained, which is the desirable effect.",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 247,
    "article_char_count_full": 1471,
    "article_char_count_review": 1471,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A9",
    "article_text_for_review": "(from: $ \\underline{\\text{The New York Times}} $, Sept.15,1974; sent by George Ryss) By Brook Zern I should never travel without a guitar. If I don't play the guitar for a whole day, my palms start to sweat and my mind gets foggy. But bringing a guitar to Spain -- and a flamenco guitar at that -- seemed about as redundant as toting along a box of dehydrated paella or a personal supply of olive oil. So, guitarless myself, I hung around with guitarists, and played the odd flamenco. Or I visited music stores, \"examining\" their wares. Which led me, one afternoon, to Seville's biggest music store, where I requested nothing less than the Arcángel Fernández guitar occupying the place of honor in the carefully shaded display window. To play such a guitar, of course, it is necessary to first promise that you will buy it. And afterward, of course, it is necessary to extricate yourself by explaining that you do not happen to have the money with you, but will immediately wire your bank in New York and have them transfer the funds. (This is preferable to simply saying that you have the money at your hotel, since it enables you to return on several subsequent days -- while waiting for the alleged transfer -- to retest the compromised instrument.) And so I played the Fernández. Perhaps it was Dr. Johnson who remarked of a card-playing dog that the wonder is not that he does it well, but that he can do it at all. Most Spaniards apply equally lenient standards to Americans who play the flamenco guitar, gaping in wonderment at the very ideal while retaining an unshakable conviction that such music can only be interpreted properly by one who carries it in the blood. (I learned much of my flamenco from my father, which is a point in my favor; but my father is Pennsylvania Dutch, which is at least two points against.) In any case, the net result of my playing was a few generous compliments and a complete identification of me with the Fernández guitar. who had played that same guitar and even sworn to buy it that very afternoon. \"Señor Capitán,\" I volunteered helpfully, \"I did not throw any rock through any window and steal any Fernández guitar.\" \"And who said anything about a rock, a window or a Fernández guitar?\" he said slowly. \"It says it all right there on that paper. I read it.\" \"But you could not have read it,\" he said, arising with a triumphant flourish. \"You see, the paper is facing me!\" \"So?\" \"So from where you sit, it is upside down!\" \"I can read upside down,\" I said. \"Here, put it on the desk again and I will read the whole thing out loud.\" \"Nobody can read upside down,\" said the captain. \"Besides, this is a confidential report.\" He folded it again and put it back into his pocket. More sitting and more silence. Finally I realized that my wife might be getting concerned. I told the captain that I hoped they could send someone over to the hotel to explain the situation to her. And since her Spanish was still rudimentary, I pointed out that an interpreter would be necessary. \"By all means,\" said the captain. \"I shall send the appropriate personnel immediately.\" My wife tells me that soon afterward, there was a knock on the door. She answered it, and a man in a gray suit flipped up his lapel to show a little badge or something. Simultaneously, he smiled at her and said in perfect English, \"Hot.\" \"It certainly is,\" she said. \"Is there something I can do for you?\" He looked at her again, smiled even more pleasantly, and said, \"Cold.\" And so he and his companion proceeded to go through the room and our luggage, obviously looking for something. My wife, recalling a childhood game, said that if they would simply tell her what they hoped to find, she would tell them if they were getting hot or cold, but that drew no appreciable response. Finally she realized that the interpreter, who had undoubtedly gained great prestige among fellow officers for his linguistic mastery, spoke only those two words of English. After 10 minutes of apparently fruitless searching, the men left. But on the way out, the other one pulled out my passport, pointed to my picture, and whispered reassuringly, \"O.K.\" Meanwhile, I had acquired my first ally at headquarters. He was a lieutenant whose job was to grill me further, but he knew I couldn't have done it. \"How could an American steal something like that,\" I'd heard him arguing in the hall. \"Americans are all rich. Would you steal if you were rich?\"",
    "title": "ENCOUNTER: A CAPER IN SEVILLA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 791,
    "article_char_count_full": 4438,
    "article_char_count_review": 4438,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A10",
    "article_text_for_review": "By Gordon Booth Ever wonder what the top flamenco \"stars\" get paid for performing? A recent issue of the Sunday supplement to the Spanish newspaper ABC revealed that they don't do too badly. According to the article, which dealt with Spanish performers in general and not just flamencos, Paco de Lucía receives the most dinero (as one would expect) -- 500,000 pesetas ($4,225). The top singers, people such as Juan Peña \"El Lebrijano\" and Fosforito, normally command 80,000 pesetas (slightly more than $1,000) an appearance. The vast majority of the people involved in flamenco are not only paid a substantially lesser amount but often get little opportunity to perform, besides, a fact which one should remember the next time one of these lesser known figures seems reluctant to leave the stage at a festival; while not particularly nourishing, applause is still very satisfying. DON QUIXOTE RESTAURANT A DINING ADVENTURE IN SPANISH AND MEXICAN FOOD ENTERTAINMENT CAMPBELL AT SARATOGA 206 EL PASEO DE SARATOGA SAN JOSE, CALIF. 95130 378-1545 Ernest Lenshaw San Diego, Ca. Dear Jaleo: I thought that perhaps Jaleo's readers would be interested in what the great cantaor, Antonio Mairena, has to say about Diego del Gastor in his book, Las Confesiones de Antonio Mairena (with Alberto García Ulecia, 1976). He calls Diego, \"...aquel gitanito singular de toque enigmático e inolvidable.\" (pg. 173) He also describes Diego as follows: \"Another guitarist with tendencies toward solo playing was Diego de El Gastor, a gypsy descendant of la Anica Amaya, whose family had come from the Serranía de Ronda and made their home in Morón, where Diego lived the rest of his life and died in the summer of 1973. Diego tended more toward concert style playing than accompaniment, although he was a great aficionado of the cante and knew how to accompany, as Juan Talegas and I can testify having been accompanied by him many times in fiestas. But, unlike what happened with Montoya (Ramón), he didn't favor the 'aires levantinos' (songs of the fandangos family like tarantas, graninas, etc.), but the others, more like Javier Molina, although even more pronounced in his position -- el de el Gastor used to play only por soleá, seguiriya, and bulería, and in a very personal and unusual manner. In reality, Diego's attitude could be explained, in part by the fact that he was not a professional - the major part of his life he played for friends and in private fiestas.\" (pg. 122) Paco Sevilla San Diego, Ca.",
    "title": "FLAMENCO SALERIES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 416,
    "article_char_count_full": 2494,
    "article_char_count_review": 2494,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
