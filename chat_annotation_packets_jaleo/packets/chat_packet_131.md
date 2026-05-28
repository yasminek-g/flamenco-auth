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
    "article_id": "JALEO_1982_02::A2",
    "article_text_for_review": "Dear Jaleo: Firstly, thank you for all the back copies I received recently. The magazines are really excellent! In the May 1980 issue, there was an article regarding a joint effort between Jaleo and International Record Distributors to make flamenco records available. Records are nearly impossible to buy in Australia, so I am writing in the hope that you can help me obtain some. There was a list of Hispavox LP's in the May issue and I would be grateful if you could tell me how I could order them, or the records that you have received [David goes on to list about twenty records that have appeared in Jaleo since May 1980]. I thank you and sincerely would appreciate any help you can give me. Australia must be the worst place for someone who loves flamenco. Once again, thanking you, David Smith New South Wales, Australia [Editor's comments: Obtaining records is not easy for anybody living outside of Spain and is especially difficult for those living on other continents. The obvious answer is to go to Spain or have a friend buy records for you there. But, even in Spain it is not as easy as one might expect to find good flamenco records, except for those featuring the biggest stars. Even in Andalucía, one has to search for records by lesser known artists -- many records seem to come out in limited editions, so that, if you don't buy one when it first comes out, you might as well forget it! For reasons unknown to us, International Record Distributors was unable to order the records we requested; then the company went through some sort of shake-up and we lost contact. The only sources of records that we can suggest are the Musical Heritage Society (14 Park Road, Tinton Falls, New Jersey, 07724, USA) which has a few records of Sabicas and Mario Escudero, and the following address in Spain that Guillermo Salazar gave in a recent \"Gazpacho\": Casa Damas, Calle Sierpes 65, Sevilla, España. Probably the only way we can make flamenco music available is to create an international cassette tape exchange of some sort. A number of record collectors have already expressed interest in creating a catalogue of all records to be found in collections around the world and then making the catalogue available to aficionados so that tapes could be ordered. By dealing only with \"out of print\" records (most flamenco records fall into that category) and not sending tapes into Spain, we could probably avoid copyright problems. The problems standing in the way are, 1) finding someone to put the catalogue together -- I won't be able to tackle it while acting as editor of Jaleo and trying to put the Directory out, 2) making certain that all recordings are done on good equipment and on good quality tape cassettes, and 3) establishing a fair price for the tapes -- Guillermo Salazar suggests $15-20 as a necessary price to cover materials, postage and labor, a seemingly high price, but which could cover two records on a single 90 minute tape, and would help discourage mass copying by casual aficionados. Input from readers on this idea would be welcomed.] Dear Jaleo: Since two years I am a reader of $ \\underline{\\text{Jaleo}} $ and I enjoy it very much. It might seem strange, that a German subscribes to your magazine, but as far as I know, there is no publication existing in my country on the art of flamenco, comparable to $ \\underline{\\text{Jaleo}} $. Your articles are very interesting and informative. If there will be a chance for me to go to California I will try to visit one of your juergas. I'm really looking forward to that. Reading your August issue I found an article (p.24) by Paco Sevilla about new Flamenco Guitar Music available in the U.S.A. (Juan Serrano Flamenco, Concert Selections, Mel Bay Publications plus cassette tape). I tried to get it here, but I did not succeed. Here's my request. Could you find out and tell me, where I can order this collection by mail or would you make it available for me (including cassette tape)? I would be very glad if you could answer to my question positively. Sincerely yours, Peter Kaniut West Germany [Editor's Reply: It does not seem at all strange to us that we have a German subscriber -- in fact, we have many subscribers in Germany and throughout Europe. As for the Juan Serrano book, we can only suggest that you try to order it from: Mel Bay Publications Pacific, MO 63069 USA The cost is $9.95 for the book and $6.95 for the tape; you might add an extra $5.00 or so for postage. We have been trying to convince 4. Menkes' style is clean and elegant. Gallardo boots by comparison look \"chatos.\" 5. Menkes' heels produce clean sound with little effort. Gallardo's product does not favor the heel, but the planta, and produce a dull taconeo, with much effort. To the various flamencos around New York whom I have heard commiserating over the shortcomings of Gallardo and indeed to the entire American flamenco family, I gladly make known to you an alternative that has brought me satisfaction. May the following address serve you as well as it has me: H. Menkes Mesonero Romanos, 14 Madrid - 13 Tel: 232.10 36 Send 5,000 pesetas (postage included) plus measurements or size in centimeters, as well as specifications. Allow three weeks from date of receipt. Id en paz, The Shah of Iran Brooklyn, NY Dear Jaleistas: Sometimes I want to send you articles about the flamenco artists I've met, but my English is not good enough to write the articles, and always each time I wanted to send you a photo, or an article about an artist, I at once received the new Jaleo with an article about the same artist. This year, I was in Andalucía for one month. AIR • BUS • STEAMSHIP • RAIL • DOMESTIC AND WORLD TOURS (714) 426-6800 • 297 \"K\" STREET • CHULA VISTA, CALIF. 92011 ESPECIALISTAS DE ESPAÑA REYNOLDS S. HERIOT OWNER - MANAGER 426-6800",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_02",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1024,
    "article_char_count_full": 5814,
    "article_char_count_review": 5814,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_02::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nexperiences, narrated always in rich, impassionate Andalucian speech, with Mari Carmen filling in some of the details, but I hope to share with Jaleistas my glimpse into this extraordinary artist's life, one of that pure, uncompromising breed that have elevated the flamenco guitar to what it is today. Carlos speaks little or no English. \"I have always worked in places where Spanish was spoken, so I never really needed to learn English. I always meant to (learn), but time passed and I never did. The older one gets, the more difficult it becomes. When people speak to me slowly I can understand and we can communicate, but my English is still bad (para el inglés yo soy muy malo). Others have come here at a younger age (than I did), maybe married American women and have learned a little more.\"\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"listen\"]\n\nan women and have learned a little more.\" Born in Vélez, Málaga, Carlos was raised in the small town of Lagos where his father owned a \"finca\" and an oil mill (\"molino de aceite\"), Carlos was one of eight brothers and sisters. On his early years in Spain he recollects: \"I began to play when I was nine years old. My father played a little, but never tried to teach me. He was teaching an older brother of mine, and since I liked the guitar, I would listen, and when they'd leave I would pick up the guitar and play everything my father had taught my brother. One day I was playing in my room when my father called out, 'Julio, Julio!' I replied, 'He is not here.' 'Who is playing then?' he asked. 'It's me,' I said. That was the first time he found out I could play anything at all. We lived in a very small town, though, and during the season, we all had to work on the farm. In those times, to play the guitar for a living was considered a bad thing to do, done only by 'indeseables,' 'gente perdía.' True, in those times, to play the guitar, or sing, or dance flamenco was very much looked down upon, 'una cosa indeseable.' Then, instead of helping me with the guitar, they tried to dissuade me by saying there was no future in it. But I didn't care, I didn't do it to make money, I did it because I liked it. So I went on playing, but not the way I should have done it. If I had had a teacher then, or studied music, in 20 years or so I would have been a great figure. But since they paid no attention to it, given the lack of future they saw, I just bounced around with the guitar until the war broke out.\" His unusual talent did not go unrecognized in Spain, however, and from 1940 to 1948 he gave performances throughout Spain, Portugal, and North Africa with many of the outstanding artists of that time, including Pepe Marchena, Manolo el Malagueno, Cepero, Juanito Mojama, Pepe de la Matrona, Antonio el Sevillano, Juanito Varea, Manolo Leiva and Nino Almadén. With the latter he eventually did a record in New York many years later. In 1948 he joined the company of Pepe Marchena, playing second guitar to Ramón Montoya for dancer Carmen Sevilla. About Don Ramón, Carlos recalls: \"I liked him very much from the very first time I saw him play the guitar. I always thought he was the very best and he was the influence that all of us (guitarists) have. Well, it is true that other newer things have been done after him, but he was 'la fuente' of all guitarists of today. We worked together for a year, but eventually Don Ramón became ill and left me in his place in the company. After he fell ill, he never recovered and finally died in 1949 at the age of 69.\" And what kind of a man was he? \"He was a difficult man in many ways. For me, though, he was a very good friend, but in those times it was hard to make a living; all artists were suspicious of each other and he had that suspicion. I remember the first time I rehearsed with him and the company. He watched me constantly, but not in a good way at first,\n\n[ENDING CONTEXT]\n\nhe was very famous for a while over there. He did a solo entitled 'La Campana,' which went like this (he demonstrates by holding the guitar with his left hand by the neck and swinging it like a pendulum while doing ligados). That was his solo! And people ate it up! I've never seen anything like it in my life. In the flamenco guitar you can fool the whole world. On the other hand, take someone like Sabicas, who never resorts to effect; he places his hands on the guitar in such a way that he doesn't even seem to be moving them and does wonderful things with them. But that, people won't notice.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CONVERSACIONES CON...CARLOS RAMOS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_02",
    "year": 1982,
    "language": "en",
    "article_type": "poem",
    "pages": "10-15",
    "page_number": 10,
    "word_count": 2694,
    "article_char_count_full": 14725,
    "article_char_count_review": 4652,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "listen"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_02::A4",
    "article_text_for_review": "By Dean Wallace Flamenco guitar, like many arts in the folk tradition, can occasionally be elevated to the highest reaches of poetic expression. It seldom happens, it is true, but that is only because a musician like Carlos Ramos comes along so rarely. In his first San Francisco recital Saturday at Veterans Auditorium, Ramos unveiled his magic. If everyone who heard him tells one of two friends of the revelation in this remarkable man's playing, it will be SRO from now on. LIKE ETCHING Flamenco, as it is usually performed, is essentially monochromatic, somewhat analogous to an etching in deep reds and blacks. The tender, more subtle side of its tragedy is eclipsed by flaming passion, just as the lyrical possibilities of the music a generally sacrificed for flashy pyrotechnics. A couple of hours of this is enough for almost anyone. ARTIST IN PROFILE",
    "title": "CARLOS RAMOS...'A MASTER'",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_02",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 144,
    "article_char_count_full": 860,
    "article_char_count_review": 860,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_02::A5",
    "article_text_for_review": "by El Chileno Natalia, Jaleo's Washington, DC, correspondent, is an active flamenco dancer who performs regularly in the Capitol City. Born and raised in Baltimore, Natalia attributes her intense Spanish looks and artistic leanings to her Sephardic Jewish ancestry. She traces her interest in flamenco to watching José Greco dance in the movie, \"Around the World in 80 Days,\" when she was eleven years old. Formal instruction in dancing had to wait until she was fifteen or sixteen, at which time she began to study with María Morales in Baltimore, who taught her the first steps in classical, theatrical-type Spanish dance to recorded music. With flamenco still very much in her heart, she moved to Washington, DC, to attend college, and also began to study in earnest with Raquel Peña and Ana Martínez. Her first professional break came in 1976, when she was asked to substitute for Micaela Díaz at El Bodegón restaurant with none other than guitarist Carlos Ramos.",
    "title": "NATALIA MONTELEON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_02",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 160,
    "article_char_count_full": 967,
    "article_char_count_review": 967,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_02::A6",
    "article_text_for_review": "by El Chileno Occasionally in our travels we come upon a restaurant that we feel is worthy of comment for its flamenco quality. Our purpose is not to \"plug\" that particular place, but rather to give accolades when due. Of the many establishments that offer some sort of flamenco entertainment, there are a few where the management seems to be truly interested in \"el arte\" and does its best to promote it above and beyond the mere sound of the cash register. It is in such places where we feel that the Jaleista can spend a very pleasant evening and come away not only with a somewhat slimmer wallet, but with the feeling that his or her presence was truly appreciated by the artists and management alike, plus the notion that he is richer in experience and enjoyment. One such place is El Bodegón, Spanish Restaurant, located on 1617 \"R\" Street N.W. in Washington, DC. It is fitting to highlight El Bodegón in this issue which features our \"Conversación\" with ABOVE AND LEFT: P Clockwise from lower left -- 17 year-old Pansequit Pansequito. -- Enrique Melchor and h -- Paco de Lucia at home -- Tomatito (Almeria) -- Tomatito with his wif RIGHT: JUERGA IN Photos by Ga Top to bottom: -- Marta dancing to Char guitar, Mary Sol, Cha --Char and Bob enjoying --Marta and Charo turn o ABOVE AND LEFT: PHOTOS BY HO-TONG HANH, 1981 -- 17 year-old Pansequito Hijo (guitarist) and his father, Pansequito. -- Enrique Melchor and his daughter in Madrid. -- Tomatito with his wife and two daughters. RIGHT: JUERGA IN GEORGIA (SEE ARTICLE, PAGE 29) Photos by Garry West and Raul Botello -- Marta dancing to Charo; L to R: Maria, Jose, Bob playing guitar, Mary Sol, Charo.",
    "title": "EL BODEGON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_02",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "17-19",
    "page_number": 17,
    "word_count": 294,
    "article_char_count_full": 1658,
    "article_char_count_review": 1658,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
