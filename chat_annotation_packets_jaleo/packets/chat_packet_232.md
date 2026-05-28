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
    "article_id": "JALEO_1986_FALL::A14",
    "article_text_for_review": "FIRST OF IT'S KIND by Iris Miller For once, those of us in the throes of learning the wonderful art of flamenco dance have been given an opportunity to put our long hours of hard work and study to the test. Under the tutelage and direction of Raquel Lopez, flamenco dancer, choreographer and teacher, a student recital was given. Ms. Lopez began rehearsing in the fall of 1985 and, to date, is the only San Francisco Bay Area teacher known to work on and produce such an endeavor. Fifteen students were features in this very professional \"student recital\" at the Marin Community Playhouse on July 26th. They were backed up by Roberto Zamora, cante, Augustin Quintero and Juan Moro, guitarra, and Sara Olivar, palmes; Sarita Ayala was the evening's guest artist. The first half of the program, garrotín, caracoles, tangos de Málega and alegrías, were all choreographed by Matilde Coral under whom Ms. Lopez has received the majority of her dance training in Spain. The garrotín and alegrías truly brought out the varied qualities seen in the \"antigua\" style that Ms. Coral is noted for. In the garrotín there is the joy of youth and the charm of coquettishness, whereas the alegrías searches further to display more depth of the lyrical \"antigua\" style. The caracoles, performed by Sarita Ayala, and tangos de Málaga, danced by Alicia Farin, highlighted rhythmic show-stoppers that each soloist performed with fine agility and style, showing yet another aspect of Ms. Coral's choreographic talents. The second half of the program began with another dance using abanicos (fans), danced by Emilia Lorca and Carolina de la Plata in the romeras. Again, we saw the choreography of Matilde Coral. It was joyful yet full of sharp moves, performed with the clarity and ease that was seen in all of the dances. This was followed by Ms. Coral's solea, a rare beauty that Ms. Lopez pronounced with seven student dancers, bringing out both the gentle and piercing facets of this wondrous flamenco dance form. Bulerfas a dos guitarras was then superbly played by Augustin Quintero and Juan Moro and followed by Ms. Lopez' own choreography for soleá por bulerfas. In this piece, one could see the intenseness of the soleá blending into the thrill of bulerfas in a mixture expressing the languidness of the \"antigua\" style, as well as other more forceful modern styles. This fabulous choreography was equally staged, as were all the other dances, by Ms. Lopez. Ending the program, of course, was the Fin de Fiesta, featuring the entire company. Roses were showered on all the cast and a feeling of immense joy and satisfaction was felt. It was a tribute to the extensive labor and devotion given by Raqual Lopez to her students and the flamenco community. Luckily, Raquel Lopez will be presenting another student recital in June 1987. She will be offering new flamenco dance courses on the tangos de Málaga, caracoles and guajiras, beginning November 4th at the Finn Hall Cultural Center in Berkeley and Pacific Ballet in San Francisco. * * *",
    "title": "FLAMENCO IN THE BAY AREA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "poem",
    "pages": "28",
    "page_number": 28,
    "word_count": 510,
    "article_char_count_full": 3026,
    "article_char_count_review": 3026,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_FALL::A15",
    "article_text_for_review": "by Mary McConnell Is it all over at 60? What if you are over 60 and want to learn flamenco? I started a class in flamenco for the Highland Senior Center in Albuquerque, New Mexico. I discovered a lot of interest among this group and exploded a lot of myths about aging in the process. Nina Gardner, age 66, for instance, is not living in the past. She, like others in the class, speak of the stress and hard work of their early life and the lack of opportunity for creative expression. Nina raised a family and worked with her husband in Oklahoma. Now -- she does what she always wanted to do -- she dances. Nina takes dance classes at the center in tap dance, belly dance, folk dancing, everything. When I started my flamenco class, she became an enthusiastic learner, even looking Spanish with her long dark hair and gypsy skirts. She performs folk dances at senior centers, day care centers, shopping malls, nursing homes, political rallies and community events. Here is one myth exploded: that a senior dancer has no place to perform. They have more opportunities than anyone you know. Perhaps no pay, but still, they are performing their dance. Nina never says \"I can't,\" or \"I'm too old.\"",
    "title": "FLAMENCO FOR SENIORS -- OLE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 29,
    "word_count": 213,
    "article_char_count_full": 1194,
    "article_char_count_review": 1194,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_FALL::A16",
    "article_text_for_review": "by Jeanne Zvetina Suffering jet lag and flamenco saturation after three solid days of the spectacular \"Flamenco Puro\" in New York, I was hardly enthusiastic about dragging myself to the East County Performing Arts Center for the Ballet Español de Madrid on November 18. It seemed so anti-climactic. I was in for a surprise. The company was so well-trained and fresh that I was shaken out of my stupor. And about twenty minutes into the show, I was jolted by a flash of deja vu -- half-a-dozen of the dancers, including the lovely and exciting Carmen Villena, I had just seen on the videotape of Antonio Gades' \"Blood Wedding\". It was icing on the cake when Carmen Villena and Larin Diaz consented to be interviewed on stage after the show as the crew dismantled and packed the equipment: JZ: I just bought the videotape of \"Blood Wedding\" and watched it yesterday. Tonight I suddenly realized that you and four or five others on stage were in the case of \"Blood Wedding\". VC: Yes, there are quite a few of us, J7: Are you from Madrid? CV: Yes -- where are you from? JZ: Here -- but my mother was born in Mexico. This is the first time I have ever seen a dance \"cooperative.\" You must be very proud of the quality you achieve. Have you known each other and worked together a long time? CV: All of us were together in the Ballet Nacional with Antonio Gades. Gades was the director. He had a problem with Ballet Nacional and they let him go. We didn't think it was fair, so we all left with him and formed a cooperative company with Antonio Gades. That was the core of the movie \"Blood Wedding\", but after that things weren't so great among all of us, so we separated. He formed his own company and we formed ours. LD: The idea of forming our own company and splitting from Antonio Gades was that we wanted to innovate -- to give a new line to Spanish dance, not just in the theatre, but personality as individuals. This company is truly a cooperative. Apart from just dancing together, we all have responsibilities for the different aspects of a dance company, such as costumes, scenery, publicity, administration, etc. That's the way we all take care of the house. Those were our thoughts in forming a new company. JALEO - VOLUME IX, No. 3 JZ: When you go to New York I hope you will be interviewed by the New York Times and get to explain about a cooperative company. On the whole, I feel dancers have little to say about their own destiny. \"Blood Wedding\" must have been filmed about five years ago, yet you all look just as young and vital. LD: That's because we are very alive artistically. Within those five years we have put together four completely different programs of two hours each. We're living every day with the innovations in music, poetry and art -- all the new things going on in the world. That's what keeps us going. That's why we left Antonio Gades. We didn't want to continue doing \"Blood Wedding\" for five years. We wanted new challenges. The life of a dancer is very short and you have to do a lot in that time. The greatest thing a human can do is give something back to humanity. This is what gave birth to this company. We feel an obligation to give something back. CV: Excuse me now. I must leave. I'm in charge of costumes and have to take care of the packing. JZ: Thank you so much, Carmen, and good luck in San Francisco and New York. Lario, did you all study ballet? LD: Yes, and this is one of the disciplines we still have. For one and one-half hours every day we do classical ballet. We feel that this keeps the body aesthetic and elastic and gives you a feeling for everything. You have to really exercise all the cells in the body so they don't atrophy. JZ: Did you study flumenco separately? LD: Our strength, of course, is Spanish dance and flamenco is one of the branches. We also studied folkloric. We've studied with a lot of different people in Spain. JZ: Was Maria Magdalena one of them? L.D: Yes. Also some of the women dancers studied in Valeoena. I studied in Barcelona for a time. JZ: What is your non-dancing responsibility with the company? LD: I'm the administrator of the company. We've got twenty people and tomorrow we go to San Francisco. Next week we go to New York for three weeks. JZ: Thank you so much for your courtesy and good luck on your tour. (To guitarist Carlos Carmona Carmona \"El Habichuela,\" who was walking by): Aren't you one of the \"Habichuelas?\" CC: Yes. JZ: I just met two of your brothers in New York in \"Flamenco Puro\". CC: Don't tell me! What theatre are they in? We're going to New York and I don't even have their address. JZ: They're at the Mark Hellinger Theatre and the cast is (photo by Jose R. Pino) BAILET ESPANOL DE MADRID JALEO - VOLUME IX, No. 3 (photo by Jose R. Pino) TEODORO MORCA IS NOW OFFERING ON VIDEO TAPE, A COMPLETE APPROACH TO STUDYING FLAMENCO DANCE, IN TECHNIQUE, INTERPRETATION REPERTOIRE AND UNDERSTANDING. WRITE OR PHONE FOR A \"MENU\" OF TAPE SELECTIONS. 1349 Franklin Bellingham, Washington 98225 Ph. (206) 676-1864 Morca Foundation proudly presents three summer workshops: CLEVELAND, OH, June 15 to 27, Fairmont Art Center, contact Libby Lubinger (216) 338-3171; ALBUQUERQUE NM, July 18 to 26, work-shop and performances, contact Eva Enciñas (505) 345-4718; BELLINHAM, WASH, August 10 to 22, 9th All Flamenco Workshop-Celebration and Fiesta.",
    "title": "BACK STAGE WITH BALLET MADRID",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "30-31",
    "page_number": 30,
    "word_count": 963,
    "article_char_count_full": 5344,
    "article_char_count_review": 5344,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_FALL::A17",
    "article_text_for_review": "THE ART OF JALEO The art of jaleo is synonymous with the understanding of flamenco, the art and relationship of flamenco in all of its facets. The essence of the meaning of the word, jaleo, when it applies to flamenco is to encourage, to encourage the maximum of artistry and inspiration from whatever flamenco happening is taking place. Jaleo can come from all directions in a flamenco happening. It can come from the performing artists, jaleadores, singers, dancers, guitarists, aficionados, the audience, spontaneous performers, etc. Jaleo can st times make or break a flamenco happening in mood, feeling and ambiente. Picture these two simplistic stories: A cuardo flamenco (flamenco scene of fine guitarists, singers, dancers and jaleadores). The guitarist tunes up and starts to play, maybe a few chords and a few falsetas, setting the cejilla in the right place for the singer. When they finish these opening falsetas, a few subtle sayings may be heard, \"olé, así se toca\"; a singer starts to warm his voice, \"ay, ay, ay,\" a bit of soft palmas is introduced, in compás and with the proper accent and mood for the music and song. The singer sings a letra and is encouraged to sing another, \"canta ya, olé\". He sings another letra with the palmas and jaleo coming with the right accents, dynamics, tempo and compás to \"enhance\" the cante. A dancer is moved to dance, gets up from a chair and begins to move, locking into the mood, timing and feeling of the music and the flow and dynamics of the singing. As the dancer moves to the song and interprets that music, that song in movement, the palmas look into a definitive matiz of shading, tempo and accent, blending with the dance, song and music. The jaleadores match their saying of encouragement with the remates, flamadas, desplantes and the tatal dynamic interpretation of this blend of artistry that now is a well tuned, blended, balanced, orchestrational flamenco happening. It all becomes that compés, that harmony of sound, dance music, song, that melody of flamenco color and feeling that internally says what flamenco is. No one is overpowering the other. Yes, a blended orchestra of jaleo, music, song, dance that speaks the energy and rhythmical balance and instrument that is the beauty of flamenco, a true flamenco happening. Both of these little stories are simplistic. Unfortunately, the second story is very common when people are not in tune to what jaleo is all about in relation to the overall picture of a flamenco happening, whether in private juerga, tablo or theatre performance. What I am getting at is that the people giving jaleo should know and be aware of the subtleties of the art. All of the audible, rhythmical techniques, such as palmas, footwork, pitos, table tapping, cane tapping and verbal sayings, can be considered musical, rhythmical instruments in their relationship to flamenco accompaniment. This might seem a bit sophisticated, but for someone who is giving jaleo and is sitting in for that purpose, then indeed, that person's jaleo has all of the intricacies of the musicians, the singers and cancers. Often jaleo can be in the category of the cheering section of a ball game or any kind of sport where the fans on the side lines are saying \"go man go\" (this is an old 1950s saying that I remember when I was on the school gym team). This is jaleo in its most simplistic form. The starting point for giving good jaleo is to begin with a sure knowledge of all of the basic compás in relation to the guitar music, and the accompaniment for song and dance. With this knowledge comes the understanding of the many styles and dynamics of good palmas, when to play palmas, when to play loud, soft, to stop, to start, accenting, shading, counter time, good positioning and aire, along with the aesthetics of the art of flamenco. There is no quick school of this, just as there is no short cut to proper training in guitar or dance or the singing, even if you have the talent. Some of the learning involves developing a sensitivity and plain musical good sense. I make this art of jaleo s very important part of my all flamenco workshops, and when one group is performing, another group is giving good jaleo. One approach to the basics is \"when it is soft, play soft; when it is a bit louder, play louder in regards to palmas.\" This simplistic approach is valid to start. It makes obvious good sense to not try to drown out the artists with too loud palmas, even if your palmas are terrific, in compás and super in contratiempos. Palmas are to adorn, to add pulse and energy, to accent and to emphasize something exciting, something that may be building, interpreting or developing a mood. Palmas can actually be \"drawn out,\" retarded with intensity in such forms as solesres, seguirias, tarantos and other compáses that have a built-in dramatic quality. This is where a bit of body language comes in, s bit of palmas movement, a dance within itself, to give the quality of elongating. In rhythms such as bulerlas, there is a quality of explosive impulse, a release of the rhythm, as if the opening of the hands is the power and accent and interpretation of the compás. Like other facets of flamenco, it takes daing and listening and developing a sense of the music, the song forms, the song styles, and the individual singers and their singing styles. Also, to understand the dance forms, the various calls, variations, stops, the sections of dancing with the singer and the footwork sections, must be studied. They all have individuality of color and contrast far palmas.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "32",
    "page_number": 32,
    "word_count": 960,
    "article_char_count_full": 5566,
    "article_char_count_review": 5566,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_FALL::A18",
    "article_text_for_review": "forming artists. A good rule of thumb is that if you are not invited to give jaleo or are not part of the immediate flamenco happening, do not play audible palmas. Palmas, even in compás, take time to travel to the stage in a theatre and end up late and out of compás and break the intimacy of the flamenco happening--which is difficult, anyway, to create on a large concert stage and the total effect is annoyance for all. I recently went to a flamenco concert of a fine artist, where the sponsor in an opening speech, encouraged the audience to \"clap and yell\" during the concert and believe me, it turned a concert into a cheap shot, and the fine singer tried many times to stop the out-of-compás clapping, much to the loss of his spirit. This idea of respect for the artists with regard to jaleo, is just old fashion common sense and respect for the art that we want to experience as an audience. This idea can actually be applied to any audience as far as audible palmas are concerned. When a flamenco happening is going on, with people who are trying to experience what flamenco feeling is all about, then a bunch of noisy, extra palmas from the audience can blow the mood, just as if you started to yell and clap at a chamber orchestra concert. Not that there will not be times of cachondeo and fun times for all, but I am talking about flamenco in moments of juergas, get-togethers and performances that try to express the artistry of jaleo, sensitive jaleo, we are encouraging the meaning of life, of art, of feeling of beauty of the agony and ecstasy and love of flamenco. --Teo Morca 'GYPSY GENIUS' HISTORIC - EXCLUSIVE VIDEO RELEASE BY MANUEL AGUJETAS DE JEREZ For the first time in flamenco history the legendary Manuel Agujetas de Jerez performs on video cassette. The world famous maestro of the Jerez dynasty of gypsy flamenco singing gives an historic performance that wiff remain forever. Beautiful cantes por Sulea, Fandenga Grande, Sigurryas, Malagueias, Romeras, Taranto, Tentus, Bulerias. Length-90 minutes in color. This video features the special collaboration and original guitar, accompaniments of recording and concert artist RODRICO. Don't miss out on this first world release as it is a collector's item. No Studio video of this kind has ever been made. Order Beta of VI-45. Only $49.00. Send each, check or money order: to Alejandrina Holiman. 148 Taft Ave., #11, El Cajon, CA 92020. The performance took place on August 5, 1985. An educational \"must\" for guitarists and singers. Allow 3 to 4 weeks for delivery. FLAMENCO GUITAR Flamenco Rubia guitar, brand new in case. Excellent sound. Constructed by master guitar maker Pedro Maldonado of Malaga, Spain. $1600 or best offer. (818) 786-0637 R-R-R- New York Considered by many as the greatest flamenco stage performers La Tati led the Cumbre Flamenca 2 to one of the great presentations in Central Park, New York...La Tati was joined by Cristobal Reyes, La Tolea, Carmen Cortés (husband Gerardo Núñez on guitar) and the voices of Talegón de Córdoba, Gabriel Cortés, Pedro Montoya...others. Hardly had the Cumbre left for their South American tour, to return later in the year to Broadway, all the aficionados were preparing for the onslaught of Flamenco Puro, when there comes a surprise attack by another \"Regimen Gitano\"--armed with guitars--all gypsies and guitars, but not flamenco!! New York Town Hall filled to capacity with Russians listening to their songs of gypsy life, romance, horse trader and serenading their 7 string (D-G-B-d-g-b-d) folk guitar (only a poor relative of the six string instrument). The Roman Gypsy threatre led by Nikolai Slichenko gave beautiful vocal renderings with guitar accompaniment led by a player who did some outstanding 7 string accompaniments. Enough to say that many Russians since Segovia's visit in 1925 switched to the six string guitar; also both guitar movement leaders New York, Vladimir Bobri and Dr. Boris Perott in London (teacher of Julian Bream and performer on an 11 string harp guitar) were Russians. American Institute of Guitars has two such teachers! Yasha Kofman (himself pupil of Konstantin Smaga of Moscow) is one of the better known soloists on the New York scene on classical guitar...the other Igor Skoromudov, at the young age of 27 is a full fledged musician who learned flamenco guitar in Moscow, after Mario Escudero's concerts there. After studying with Paco Peña in Córdoba, playing Sabicas and Paco de Lucía variations, Igor had a special guitar built for him by Brune of Chicago. IGOR SKIDOMUDOV Igor was born in Moscow in 1959. He started playing guitar at age thirteen and became interested in flamenco guitar when he heard Mario Escudero in concert in Moscow. He first learned flamenco from tape and records and became very interested in Paco de Lucfa's style after hearing him on Moscow television in 1976. In 1978, Igor graduated from the Moscow College of Music with a major in classical guitar. He emigrated to the United States in 1981 and began guitar studies with Maria Escudero at the American Institute of the Guitar in New",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "article",
    "pages": "33",
    "page_number": 33,
    "word_count": 859,
    "article_char_count_full": 5090,
    "article_char_count_review": 5090,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
