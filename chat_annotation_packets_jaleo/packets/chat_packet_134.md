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
    "article_id": "JALEO_1982_03::A1",
    "article_text_for_review": "Enrique Melchor performed in concert in London on June 25, 1981. Ron Bray sent us the program and the above photo. We reproduce the program notes here to let aficionados around the world know a little more about this flamenco artist and his recent activities. Born Enrique Jiménez in Sevilla in 1951, Melchor takes his name from his legendary father, Melchor de Marchena. He started playing the guitar at the age of 5 and began his professional career at 13, playing for the famous singer Manolo Caracol, in whose opinion the young Enrique was destined for greatness. In 1969, at the age of 18, he won the Golden Guitar — the top prize in the world's most important guitar competition. In 1978, the Castillete de Oro de Los Toques de Levante, and in the following year, 1979, the Grand Prize for the Best Guitarist of the Year from the Cátedra de Flamencología of Jerez de la Frontera. These three prizes are the most important in the world for the flamenco guitarist. Now, Melchor is asked to judge these competitions — he does not take part in them. Melchor has worked with many established artistes including Manolo Caracol, Antonio Mairena, José Menese, Antonio Fernández, Fosforito, Rocío Jurado, Lola Flores, Sara Lezana and Paco de Lucía. He has more than one hundred records in circulation on which he accompanies some of the most famous singers of the age. Melchor has travelled extensively with his guitar -- Japan, Germany, France, Holland, Italy, Iran, Austria, Belgium, Switzerland, Venezuela, Mexico and the U.S.A. His present performances in England are in preparation for an extensive U.K. tour to be held during the autumn. The group is Melchor's idea. Its objective is to fuse pure flamenco music with other instruments and thus to internationalize the appeal of what has been, up to now, a particularly esoteric school of music. Melchor feels that the oft repeated phrase -- \"Music is an international language\" -- is simply an empty expression -- unless done. When asked about what influences him, he says,",
    "title": "ENRIQUE MELCHOR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 342,
    "article_char_count_full": 2026,
    "article_char_count_review": 2026,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_03::A2",
    "article_text_for_review": "It has always amazed us in the past when issues of Jaleo have developed a central theme without any intentional guidance. Suddenly, we find ourselves with a number of articles dealing with the same subject and then manage to dig up a few more that have been sitting on a back shelf waiting for the right time. Never nas cnis nappeneo to a greater extent than with this issue of Jaleo. Never have so many articles dealt with the same theme, a theme that is superbly stated in Jerry Lodbill's \"Punto de Vizta.\" There is an incredible amount of food for thought in this issue and we hope that all readers will take the time to do the thinking, to read between the lines and contemplate the common thread that runs through all of the articles. We would like to thank Ron Spatz for his \"fingernail\" article, the inspiration for the in-depth look at the guitarist and his fingernails that is presented this month. Special thanks must also be given to El Chileno, who devoted an incredible amount of time to the preparation of the many valuable interviews that we have seen this year. Each interview had to be arranged, carried out, transcribed from tape, often translated into English, edited, and typed -- an awesome task. El Chileno has done all of this while working full time, studying guitar, and studying for medical axams. It is that sort of effort -- going far beyond what is expected -- that has made Jaleo possible.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 253,
    "article_char_count_full": 1419,
    "article_char_count_review": 1419,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_03::A3",
    "article_text_for_review": "Dear Jales, We would like to express our thanks and appreciation to Mr. Peter Baime who was a guest in our home last month. While doing some engagement commitments in San Francisco he was able to take some time to journey to sregon and spend an entire week with our family. During this time he performed in concert at the Oregon Institute of Technology along with some special performances at two of our local high schools. His special personality and unique way of holding the interest of his audience will long be remembered. While many JALEO readers know Mr. Beime as an outstanding artist in the interpretation of flamenco, there is another talent which was brought to light during his stay -- that of being an excellent cook. The night before he left he took over our kitchen and prepared an outstanding meal. We toasted to a new and lasting friendship. Sincerely, Dennis, Maggie and Kim Ellexson Klamath Palis, Oregon Dear Jaleo, please don't print any more articles that say no one is able to understand flamenco except 60+ year-old maestros like Sabicas! It is discouraging at least. Let's encourage flamenco to flourish! Halycyon Ida Santa Cruz, CA Dear Fellow Flamencos: Az a recent subscriber to Jaleo, I want to say that your magazine is excellent and is everything I hoped it would be. The interviews, pictures, and articles are great and I hope that you continue the superb work. Through your magazine, I was able to order, and receive, the \"Archivo de Cante Flamenco\" which I enjoyed immensely and is also an invaluable document which captures this exciting folk art ak ita source. However, I have only one complaint (which is not your fault): Neither the enclosed booklet for the disca give any clue az to who the cantsorez and tocaores are. Granted, the list of performers might suffice for those who have no special interest in flamenco, but the aficionados who are studying voice and guitar techniques would most likely appreciate having an idea as to whom they are listening. For example, Diego el del Gastor, who I had never before heard (unfortunately), but about whom many good things have been written, ia only one of 1E listed guitarriatas, and although he may be the tocaor on Record #2, Side A, playing both siguirias and soleares, I am not at all certain. For all I know, the guitarrista could be Joaquin de Farada. The point is that I do care who the tocaor iz and I have not been a student of flamenco long enough to distinguish the styles of various guitarists. Perhaps my ignorance can be understood and forgiven. After a steady diet of Carlos Montoya and Manitas de Plata (the latter having a harsh and earthy sound that I genuinely like in spite of his alleged ignorance of the compaz), I would like to become familiar with the great players of the past. For all of Paco de Lucia's virtuesity (and he is a genius), I would much rather listen to these unsung but excellent guitarists of the past. There is much to learn from them and perhaps future editions of this folk anthology treasure will at least let aficionados know to whom they are listening. Once again, thank you for your fine magazine, and keep up the good work.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 554,
    "article_char_count_full": 3158,
    "article_char_count_review": 3158,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_03::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTIME WARP a tale by Jerry Lsbdfl Because the ocean is where sonar systems are supposed to work I spend a lot of time at sea-- not a lot of time by a sailor's reckoning, but a damned sight more than an acustical physicist landlubber like me would think was healthy. I usually take an old guitar along to relieve the bresedom during the long and monotonous passages to and from the test area. Over the years I have spent many a pleasant hour playing flamenco and classical guitar (and when pressed, an occasional bawdy song) in the lounge of the R/V State Wave, a research vessel on which I frequently berth. During these relaxing interludes I have also become well acquainted with the skipper, Fred Carson, whom I know to be intelligent, sensible and reliable. I was therefore astonished when I\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Marine\"]\n\nny a pleasant hour playing flamenco and classical guitar (and when pressed, an occasional bawdy song) in the lounge of the R/V State Wave, a research vessel on which I frequently berth. During these relaxing interludes I have also become well acquainted with the skipper, Fred Carson, whom I know to be intelligent, sensible and reliable. I was therefore astonished when I recently received the following material from him: R/V State Wave c/o Tracor Marine Gallows Bay Christiansted, St. Croix, Virgin Islands 00820 December 21, 1981 Dear Jerry, It is only after a great deal of thought that I am writing to you. I am not given to flights of fancy, nor am I a daydreamer or a science fiction nut. Neither am I a believer in the supernatural. I am a practical man. I hope you believe this after our many conversations. It is therefore with great reluctance that I even consider recounting the events of the past week. I apologize in advance for asking your indulgence. Enclosed you will find a transcript of a radio interview which was part of a ninety minute recording made aboard this ship sometime between Monday and Thursday of last week JW: As performera or as afianados? P: Well, at that time it was generally believed that one could not perform flamenco unless one had been reared in the Andalucian flamenco culture, but the foreigners were, in fact, in Morón to learn to perform. JW: Apparently Mr. Pohren did not agres that only Span- iards could perform flamenco? P: That's a difficult question. Hmm... On the one hand his books lament the passing of the pure gypsy flamenco as lived and performed by the noncommercial local artists in Morón before the town was overrun by foreigners. But... on the other hand Pohren himself played flamenco guitar and was, in all honesty, the catalyst that changed Morón and the nature of flamenco there and throughout the world. $ \\underline{JW} $: And what a change it was! P: Yes, in about twenty years, from the mid 1960's to the mid 1980's, there was a complete redefini\n\n[ENDING CONTEXT]\n\nit certainly seems to be a logical possibility to me. Is it a hoax or a time warp? If you don't know, ship-mate, I'm sure I don't. A sailor once told me how to tell the difference between a fairy tale and a sea story: A sea story invariably begins thusly, \"All right, now hear this, you lubbers, and this is no $*&!\" Obviously we don't have a sea story here. By the way, I'll see you in March. Save the tape. I'd like to hear the guitar piece that follows the interview. Yours truly, Jerry Loddill Flamenco Guitar For Sale *1956 JOSE RAMIREZ * TOP MODEL *EXCELLENT CONDITION *MACHINE HEADS $1,200\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PUNTO DE VISTA: TIME WRAP",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 1285,
    "article_char_count_full": 7253,
    "article_char_count_review": 3638,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Marine"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_03::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby El Chileno Of the many talented husband-and-wife teams in the world of flamenco today, there are only a few which have achieved that wide acclaim and recognition that places them solidly among the great artists of all time. Raquel Peña and Fernando Sirvent are such a team. With a long and distinguished career in all phases of \"el arte\" as performing and recording artists, as well as teachers, they have become some of the most highly regarded and sought-after flamenco artists in the USA and Spain today. It would be indeed hard to conceive anyone being more active and deeply involved in flamenco than these artists. Nightly performances at El Tío Pepe Spanish Restaurants, where they are resident artists, frequent appearances around Washington, DC, ranging from schools and universities to\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Magazine\"]\n\nuld be indeed hard to conceive anyone being more active and deeply involved in flamenco than these artists. Nightly performances at El Tío Pepe Spanish Restaurants, where they are resident artists, frequent appearances around Washington, DC, ranging from schools and universities to the Kennedy Center for the Performing Arts, are in addition to teaching large numbers of students every week. Hardly an issue of the Washington Post of the Washington Magazine goes by without mention of their names -- to the point where they have become synonymous with flamenco in the Capitol City area. Washington is their home base of operations for most of the year. Some time prior to Memorial Day every year, Raquel and Fernando travel to Spain, where they divide their time between Madrid and Alicante until they return to Washington around Labor Day. Ostensibly a \"vacation,\" their stay in Spain is filled with professional activities as well, leaving them just enough time for rest and relaxation with family and friends over there. I had the good fortune to get to know these artists over a period of several weeks last fall while in Washington, DC. I was also able to listen to them, watch them, and study with them, all of which was one of the most fun and rewarding experiences I've had in flamenco. The artists shared many of their experiences very willingly and openly with me. I present it here for Jaleistas as a token of appreciation and admiration to this most remarkable team of artists. We opened our \"conversación\" with the subject of guitars (what else?) : FERNANDO: I always play a Ramírez (he hands it to me). The other one I have, the mate of this one, has \"clavijas,\" but this one, as you can see, has machine heads. I prefer clavijas because then the guitar weighs less and it is more comfortable for me. There are some people who cannot tell the difference, but one of the characteristics of flamenco guitar is its lightness. The type of wood used (cypress) is very light and, well, that is a characteristic you are thankful for when it comes to selecting a guitar to play flamenco. For the classical guitar, the materials are heavier. They weigh almost twice as much as a flamenco instrument. Now, in Spain, Ramírez is pushing for a new type of guitar that is made of \"palo santo\" (rosewood). He calls them \"mixta\" and they are very good for solo work. I have not decided to acquire one of those yet. You have to get used to a guitar, and that takes time. It takes about a year to feel comfortable with a new guitar. Travelling with a guitar nowad\n\n[ENDING CONTEXT]\n\nbeen consistently superb. At other engagements, however, the rest of her troupe has sometimes appeared merely to be engaged in a diversionary holding operation between Pena's solos. This time, though, the entire company was in excellent form, including Fernando Sirvent, whose guitar playing was unusually fluent and articulate; flamenco singer Manolo Leiva, whose pitch and timbre were more on a par with his expertly stylish ornamentation; the five women dancers of the troupe, looking more polished than ever; and an outstanding guest artist, an intense male dancer who goes by the name of Edo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "RAQUEL PEÑA AND FERNANDO SIRVENT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "7-14",
    "page_number": 7,
    "word_count": 3049,
    "article_char_count_full": 17326,
    "article_char_count_review": 4188,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Magazine"
      }
    ]
  }
]
```
