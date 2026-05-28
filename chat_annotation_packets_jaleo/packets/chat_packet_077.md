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
    "article_id": "JALEO_1980_07::A3",
    "article_text_for_review": "(From: $ \\underline{\\text{Newsday}} $, (Nw York) Sept 21, 1975; sent by George Ryss) By Susan Soper The hands make the music and the music is the man. Smooth, olive-colored hands that look far younger than their 71 years. Long, slim fingers that end in perfectly shaped and rounded nails lacquered with three coats of Hard As Nails. Graceful hands that accompany outbursts of Spanish or illustrate broken English. Pampered hands that don't go near the ocean lest an overpowering wave cause them to be sprained, that don't prune shrubbery because working the clippers leaves them stiff and sore. Hands that cautiously steer a new Mercedes 280 coupe from a one-car garage at his Wainscott summer home through East End villages. Or that lovingly peel pears for young grandchildren. Before those hands create brilliantly improvised flamenco music, Carlos Montoya warms them up for about 30 minutes. Opening and closing his hairless fists. Stretching the hands from the palm out, flexing his fingers as if they were doing deep knee bends. During a performance, the hands never stop. The fingers pluck lively rhythms on six strings of a guitar polished so that the wood takes on a golden sheen. Between songs, during intermission, as his forearms hug the guitar, Montoya continues to extend the fingers in and out, in and out. One balmy summer evening after a concert in East Hampton, Sally Montoya stood in an enclosed sculpture garden at Guild Hall and watched her husband warmly shaking hands with friends and admirers. \"I'm glad you saw him here,\" she said with a smile, \"because, you know, Carlos never really comes alive without his guitar.\" With a sticker-studded guitar case in one hand and a flight bag containing a small bench in the other, Montoya travels all over the world giving more than 125 concerts a year. Inside the guitar case is a spruce and cyprus instrument made for him by Archangel Fernández, a man in Madrid who makes only 26 guitars a year. And in the flight bag is the miniature vinyl-covered piano bench with removable legs designed for him by Mel Pipton, a furniture manufacturer in Anchorage. But is was not until Montoya had been playing publicly for more than 50 years that his native country, Spain, honored him -- only four years ago -- with the Comendador del Merito Sevillano. \"The Spanish government, at this late date,\" Mrs. Montoya said sighing, \"finally realized Carlos was the best ambassador they ever had.\" It is the gypsy in Montoya that has an intuitive feel for rhythm and melody. The instinctive power which allows him to create what he feels. Not what he knows. The gypsy blood flows through his fingers over the six strings of his guitar -- an auxiliary vessel to his heart, pumping the music that begins in his blood. That is why, on one recent afternoon at the Montoya's summer home on Long Island's East End, when Montoya was asked to play \"Jota,\" a well-loved folk song from the province of Aragon, he half-heartedly strummed a few chords. Then he shook his head and said, \"Nah, nah...I no like other people commanding me. 'Oh, please, please, you play...' \" And poking a stubborn finger to his chest he added, \"I no play (if) I don't like it.\" That is the hombre rebelde side of Montoya which rejects commands. \"When his wife orders him to do something,\" Mrs. Montoya said, translating and smiling, \"that's when he doesn't give her anything.\" Montoya smiled, too, and gave his wife a loving pat on the knee. \"It has to come from within,\" she said. Things that don't come from within -- politics, reading music, newspapers -- Montoya does without. He devours \"relaxing\" books like those of Agatha Christie and Georges Simenon (Spanish translations picked up in Madrid), watches the \"Today Show\" and the noon news (while Sally has a vigorous swim in the ocean each summer day). \"He's the most brilliant man intuitively,\" Mrs. Montoya said, \"but he's very dense when it comes to books. Things that come naturally, he's brilliant. But things that have to be learned are never really learned.\" Good examples of that are reading music",
    "title": "HIS HANDS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5,26",
    "page_number": 5,
    "word_count": 697,
    "article_char_count_full": 4077,
    "article_char_count_review": 4077,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nText and photos by Candace Bevier All rights reserved His definition superbly summarizes his recent visit to Denver with his beautiful wife, the classical Spanish dancer Nana Lorca, and their enjoyable reunion with their old friend Rene Heredia. José Greco, Nana Lorca and his company, composed of husband and wife, dancer Lilliana Lomas and guitarist Carlos Lomas, ABOVE: JOSE TRIES ON RENE'S HAT. The first day in town, José was giving a live television interview to the same Denver station that was preparing a special program honoring René Heredia, one of \"Denver's Top Ten Men.\" René was being awarded for his contribution to the arts and culture of the city. The television crew made arrangements to film René's flamenco show the night José and Nana would be in the audience. José was\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"friends\"]\n\ngram honoring René Heredia, one of \"Denver's Top Ten Men.\" René was being awarded for his contribution to the arts and culture of the city. The television crew made arrangements to film René's flamenco show the night José and Nana would be in the audience. José was interviewed on the television noon news and René on the evening news. It was spring in the Rockies and the town was jumping with flamenco happenings. After settling their business the friends left for an afternoon of lunch and getting caught up. During the visit in Denver, Nana, José and René spent as much time together as their busy schedules could allow. Several afternoons were filled with long lunches when the friends laughed, exchanged news and jokes and retold old stories. René had toured two seasons with José (1964 & 65) after leaving Carmen Amaya's company. José and Nana had one free evening off when they arranged to have dinner and see the show at the club René was appearing at with his conga/bongo player. Nana told René after the show that she had not seen an audience in a club that attentive to the guitar for years. \"All these people come to listen, to really listen!\" After the shows the audience lined up to be introduced to José and Nana and to welcome them to Denver. The afternoon of the concert was spent eating, drinking, touring Rene's newly completed music studio, looking at photographs and listening to out-of-print recordings from Rene's extensive collection. The most popular records were early Sabicas and Carmen Amaya's spectacular singing and dancing. Nana was completely spellbound by the sounds made by Carmen. She totally concentrated to absorb the full impact of the recording. Rene and José exchanged insights and opinions of those days of the big companies, the brilliant dancers and exceptional guitarists and gifted singers. This was an\n\n[ENDING CONTEXT]\n\nAugust 18th-30th, 1980, the Morca Academy will be offering an intensive flamenco workshop for beginning and intermediate-advanced flamenco students. The workshop will be taught by Teodoro and Isabel Morca. The course will include a morning technique class and an afternoon reperatory class in each level, evening discussion session on all aspects of flamenco, use of the bata de cola, costuming and the juerga to be held on August 30th. The fee for the workshop is $225 with a $25 deposit due by July 15th. For more information write: Morca Academy, 1349 Franklin, Bellingham, WA 98225 206/676-1864.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "JOSE GRECO TODAY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "poem",
    "pages": "6-12",
    "page_number": 6,
    "word_count": 1098,
    "article_char_count_full": 6549,
    "article_char_count_review": 3469,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "friends"
      }
    ]
  },
  {
    "article_id": "JALEO_1980_07::A5",
    "article_text_for_review": "By Alain Faucher France is the immediate neighbor of Spain. One would imagine that this proximity would be propitious for the development of flamenco in France and encourage the presence of a great number of aficionados. What exactly is the situation? There are, in fact, two very different types of flamenco lives in France that have no relation between each other. One type is in Paris and the other is in the \"Midi\", that is to say, the southern part of France. In Paris live professional artists who are dedicated to perpetuating the tradition of genuine flamenco. There are very few of them and they lead a difficult life; in this they are really flamencos because there is almost no public for their art. The problem is that finding engagements is a daily chore for many of them who must, to earn a living, make compromises. So, we see cantaores singing rumbas or boleros of the worst taste in tablaos where people ignore everything about cante flamenco -- they would confuse a pasodoble with a bulería. We see, as well, guitarists in bars -- often not even Spanish -- playing more or less commercial things for a rather sparse audience. However, in this morose climate, there are two groups that have the merit of maintaining a very high level of flamenco in the capital city: The ensemble, \"Alma Gitana\", is constituted by cantaor Paco \"El Lobo\", bailaora Sara (formerly, first dancer in the Madrid tablao, \"Los Canasteros\"), and the guitarists, Navaro Puente and Hierbita. To these artists who live in Paris, we must add the great maestros who come from Spain for occasional performances -- in reality, much too rare. For want of public, few are willing to run the risk and there is no organizer in Paris itself. The most conspicuous events of these last years are: 1974 -- Antonio Gades Dance Company filled the Théâtre des Champs-Elysées for two weeks; success was assured by the exceptional quality of the performance. 1976 -- Paco de Lucía at the Théâtre de la Ville. 1977 -- Enrique Morente accompanied by Pepe Habichuela at the UNESCO. -- \"Camelamos Naquerar\" by Mario Maya was presented for ten days at the Théâtre Montparnasse with cantaores, \"El Piki\" and Gómez de Jerez, guitarists, Paco Cortes and Lalo Maya, and dancers, Concha Vargas and Mario Maya. -- Paco de Lucía at the Palais des Congrès. 1978 -- Paco Peña, followed by Paco Cepero at the Théâtre de la Ville. 1979 -- Manolo Sanlucar at the Théâtre de la ille. -- Paco de Lucía with John McLaughlin and Larry Coryell at the Pavillon de Paris (January). -- Paco de Lucía in Arles (near Marseille (July). -- Paco de Lucía at the Théâtre de la Porte Saint-Martin (November). 1980 -- Enrique Morente accompanied by Paco Cortés at the Casa de España and a few days later at the Théâtre des Amandiers with the \"Teatro del Arte Flamenco\". -- José Menese and Diego Clavel accompanied by Pedro Peña coming for a week to the Théâtre du Marais in October. In the south of France it is a completely different world. France is unique in Europe in that it is the only country, aside from Spain, with an important gypsy population. These gypsies settled here about the same time their brothers were arriving in Andalucía and are called \"Catalan\" gypsies because they reside in a geographic area that goes from Barcelona, capital of the Spanish Catalonia, to Mareille, the French Mediterranean capital. From their midst sprang Manitas de Plata who symbolizes a flamenco that is far from pure, but which, at least, has the merit of realizing it. Their flamenco may be in question, but their style has the evident quality of spontaneity and remains essentially gypsy. Catalán gypsies cultivate mainly the rumba. It is their pulsation, their rhythm, their life; we could almost say that their heart throbs in the rumba compás. Besides the rumba, they also sing tangos and tangos \"arumbaos\" which are rather hard to distinguish from the rumba. Here we are far from cante jondo, that cante of which the Andalucian gypsies are the exclusive trustees. However, it is quite easy to find among the French gypsies some who have a good repertory of cante grande; although they live more than 1500 kilometers from Jerez or Triana; nothing of what is gypsy can be a stranger to a gypsy, even a Catalán. Each year, around May 25th, the pilgrimage of Les Saintes-Maries de la Mer occurs,",
    "title": "FLAMENCO IN FRANCE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "13",
    "page_number": 13,
    "word_count": 750,
    "article_char_count_full": 4333,
    "article_char_count_review": 4333,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A6",
    "article_text_for_review": "By Martha Sid-Ahmed Whenever I make a presentation on flamenco, I first try to set flamenco in perspective against the music and dance of the rest of Spain, and generally introduce the subject by saying that Spain has the most richly diversified cultural heritage in the world. For a while I used to preface this remark with \"in all probability\" in an effort to avoid the inflexibility of a flat statement. But I have had much exposure to the music and dance of many countries, and after examining all of them with as objective an eye as I could manage, I have finally concluded that this statement is indeed the truth. We are looking at a country in which five distinct languages -- Gallego, Basque, Catalan, Castilian, and Caloʾ (although rapidly dying out) -- and many dialects are spoken. We hear the music change drastically from guitar to mandolin to bagpipe and drum. The quality of the singing swings from the clear out-of-doors tones of a jota to the strangely dissonant yodeling of Mallorca to the raw, back room The first thing we are struck with in a performance are the costumes, and they can subtlely determine our mood and attitude about a particular interpretation. Sometimes they can make too strong a statement and can distract from a dance -- ruffles so stiff that the dancer sounds as though she's moving through a bowl of corn flakes; the interminable bata that looks as though it should be equipped with wheels; the splashy, garish floral print where all you can notice is that large, carelessly placed chrysanthemum. You should never let a costume dominate your dance. Your earliest consideration when designing a costume should be for the mood of the dance you will be performing and the kind of atmosphere you want to create, not only for your audience but for yourself as well. We are all emotionally sensitive to color -- I never feel more Spanish than when I'm dancing in just solid black; I would find it difficult to get involved in, say, a soleá or a taranto while wearing a bright floral print. Of course, if you are performing in a tablao or (like me) you are frequently the only dancer present, you will often not have the oppor- Of equal importance in making a decision in the type of fabric used. Do you want structured-looking ruffles or soft ones? Is it machine washable or does it require dry cleaning? Since we do not have available in this country the ultra-stiff Spanish nylon (or do we? - let me know), you will most likely choose a cotton/polyester type of fabric which is offered in a large selection of colors, prints, and weights. An excellent source for large florals and sweeping patterns is drapery or slipcover materials -- they may be slightly more expensive, but often make up for it in the extra width. Also, just remind yourself what you would be spending if you were paying to have your costume made for you. I used a jersey knit to make my green dress and had to make some concessions - the dress is narrower than most (it was too soft to make fullness maneuverable) and I perked up the bottom flounces just enough by encasing the hems with traverse cord. For more body you will want to interface the ruffles, as well as line them. Pellon is good for this, easy to use, and comes in many weights. For extra stiffness you may want to go to crinoline. For the petticoat or under-ruffle, I have used nylon organdy which retains its body through washings. My friend, Char Gerheim, is successfully using a nylon rainwear/outdoor fabric (look on the large bolts at the sides or back of the store) that is available in a wide range of",
    "title": "COSTUMING FOR FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "poem",
    "pages": "14-16",
    "page_number": 14,
    "word_count": 638,
    "article_char_count_full": 3584,
    "article_char_count_review": 3584,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A7",
    "article_text_for_review": "By Paco Sevilla It is still fashionable among many (perhaps most) performing flamenco artists in the United States to criticize, ignore or condemn JALEO and organizations such as Jaleístas. Whatever their reasoning, these people are overlooking one important point. Prior to the founding of Jaleistas in 1977, San Diego had never supported regular flamenco performances in its nightclubs and restaurants; one solo guitarist managed to find enough work in a succession of restaurants. There were no flamenco singers. There were two dance groups performing sporadically. I believe Jaleístas had a lot to do with the present situation. The juergas brought people together, created new performing groups, and encouraged aficionados to support these groups. One of the more important consequences of the juergas was that they attracted the attention of Sue Garson, a free-lance writer. She attended an early juerga and wrote an extensive article about Jaleístas called \"Invitation to the Dance\". It appeared in the Reader (Oct. 14, 1977), a widely read \"alternative\" newspaper in San Diego that is especially noted for its entertainment coverage. In the last few months, Sue has really been working. In February, her article \"Viva San Diego: Flamenco's Unlikely Outpost\" appeared in $ \\underline{\\text{Applause}} $, San Diego's magazine",
    "title": "THANK YOU SUE GARSON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 203,
    "article_char_count_full": 1331,
    "article_char_count_review": 1331,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
