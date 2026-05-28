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
    "article_id": "JALEO_1985_SPRING::A6",
    "article_text_for_review": "[from: Oleaje Cultural, Sept. 1984; submitted by Mary Sol West; translated by Paco Sevilla] by Pedro Luis Cabrales ...In the tavern, there in the plaza, we found El Perro—59 years of travelling the roads, carrying Paterna proudly to all parts. We talked a bit about his life and things. Marfa, his wife, smiles from the doorway of the kitchen. --How did it all begin Antonio? \"Well, you see, I have always sung. Although, as you know, what happens in this country is that nobody helps you, nobody gives you that push that is necessary to go forward. So you have to do it alone. You go to the festivals, from town to town, paid or not paid. One day, Antonio Murciano (the great poet from Arcos) got me a record deal with the Koss company. It was a single, my first one, and I dedicated it to Manuel Vallejo. It must not have turned out badly, for RCA noticed ANTONIO PEREZ \"EL PERRO\" me and I recorded eleven LP's and four singles with them. Later, I went with Belter and was contracted for four LP's, of which I still have two to be recorded. --Can you make a living with flamenco Antonio? \"Yes! Yes you can make a living, although not everybody can. You have to realize that the cantaor is pretty much restricted to the summer festivals. I presently have nineteen festivals contracted; taking into account the date, the middle of August, that is not bad. But of course, as I said, not everybody has that luck. It depends upon many things, you know? Without a doubt, the most important is that you are profitable to the agency that contracts you. If not, they hire you once and 'adios muy buena'. The dependability of the artist is also absolutely necessary if he is to work. You have to go out on stage and give all you have, destroying your throat if necessary; that is what it means to be a professional. And the audience will understand and appreciate it--even if you have a bad performance on a particular evening. The public sees how you give all you have. And then they say, 'that tio is a professional from head to toe. Also, the manager has a lot to do with it. I have worked with Pulpón [major flamenco promoter in Spain] for many years, I don't know what others think, but to me he is undeniably the best flamenco representative in this country.\" --Antonio, I know that you have received a number of awards throughout your long artistic life. Which do you feel were the most important? \"For a cantaor, all prizes have the same importance, because all of them--the smallest and the biggest--signify a recognition of your work, your efforts. For me it was a dream to receive the \"Granada de Plata\" and the first price for the granafinas in the 50th anniversary of the Festival de Granada. I am also very proud of the \"Galeote de Plata de Fuengirola,\" the \"Boquerón de Plata de Málaga,\" and the prizes in La Unión, Arcos...and, of course, the prize for the perenera--which--and its a shame--we are slowly killing, and she is the one who died. But, lets leave that subject because I would have to talk too much...\" He goes to the bar, gets two beers and, still mumbling to himself about the death of La Petenera, he goes out in the street, looks off into the distance, carressing with his eyes the whitewashed walls, deeply breathing in gulps of air that is impregnated with jasmin and mint. And there, and over there, the great murmur of long rivers in a town without rivers, the profound voice of the land, in a town that never had land. Our people put all of their inexpressible pain into the guitar. Tomorrow or someday Paterna will dedicate a street to Antonio Pérez Jiménez \"El Perro\" for his art. Its the least they can do for this man who one day, many years ago, decided to travel the highways and byways, with his voice, carrying the banner of Paterna. And he still has many more to go. Thank you, Antonio.",
    "title": "ANTONIO PEREZ \"EL PERRO DE PATERNA\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 696,
    "article_char_count_full": 3823,
    "article_char_count_review": 3823,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_SPRING::A7",
    "article_text_for_review": "WITHOUT BRILLIANCE BUT WITH DUENDE \"LOS SENDEROS DEL CANTE\" [from: El Pafs, Jan. 26, 1985; submitted by Brad Blanchard; translated by Paco Sevilla] by A. Alvarez Caballero Cante: Antonio Izquierdo \"Merenguito\" Toque: Vicente Pradal, Oscar Luis, Miguel de Cádiz The case of Merenguito is very unusual in the present day flamenco scene. He has a strange voice, different, that shocks at first and may never be pleasing to many. It is an opaque voice, mute, without brilliance; I heard somebody call it \"voz de arena\" [a gravely voice] and that seems to be a good description, but as one gets accustomed to that voice, one discovers qualities in it that must be taken into account, such as his ability in the \"quejlo\" [wailing sound]—which I must say, this cantaor often abuses—a rich variety of middle and lower tones, and the ability to search for, and find, the \"duendes de lo jondo\" [the duende in the profound cante]. This is Merenguito's first record and it is technically not very good. His cante is also erratic. For example, he makes the mistake of doing some bulerfas (I call them that only for lack of a better name) in a Latin American style with words by Alberto Cortez that are a real horror. Merenguito, who generally has better taste in flamenco, should not fall into these concessions as so many others around here do. The rest is worthwhile and some things touch on excellence, such as his fandangos de Cepero that he does with vigor and strength, bringing out greatly their intrinsically rich melody. The solea apolá is another cante that Merenguito does well, with adequate grandness for a style that demands the capacity to take it to great heights. There are also two good coplas of taranta and cartegenera. The cantaor has lately been cultivating the styles from the Levante and is progressing well in them--as evidenced by the prizes he brought home from Festival del Cante de las Minas de la Union this year and last. Peteneras, rondeña, tangos, alegrias, garrotin and another bulerias complete the recording in an acceptable tone. The accompaniment of the guitars is very uneven.",
    "title": "THE VOICE OF MERENGUITO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 358,
    "article_char_count_full": 2102,
    "article_char_count_review": 2102,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_SPRING::A8",
    "article_text_for_review": "by Brad Blanchard Paco Sevilla has written several excellent articles for Jaleo which explore the structure of different cantes, their melodies, Letras, and how they are accompanied. He also made pleas for others to contribute similar articles either in the same vein or in areas where his knowledge faltered. This article has its inspiration and model in Paco's articles and is a response to his request. It will help to illustrate a fandango which is little-known, even within Spain. Years ago I bought a tape titled \"Viva el Fandango de Huelva\" (Hispavox C-0-019). One of the songs caught my attention and I soon learned to sing it. Now in this case it is surprising because I belong to the moderate far-right as far as flamenco goes; I like to hear cante and guitar, accompanied by nothing more than el jaleo. Well, this cut is recorded with violin, bandurria, guitar and castanuelas. I didn't realize then that this the typical, the most traditional form of singing the fandangos in many pueblos of Huelva; the singer alone with the guitarist is another ramification of the fandangos de Huelva, which probably, chronologically speaking, represents a later msnifestation. I haven't yet been to Encinasola, but I know it is in those beautiful mountains in a finger of Huelva which extends up into Badajoz, almost touching Portugal. This isolation --the nearest famous fandango center is Almonaster--may explain the difference between the fandangos de Encinasola and other fandangos de Huelva. This difference is not in the instrumentation--I have heard them sung mainly in the singer-guitarist style to which we are most accustomed--but in the melody, the guitar accompaniment required by the melody, and the order in which the verses are sung. I haven't collected enough of the verses to really generalize about their content, but in risking a guess, I would place them in the more lightweight category that fandangos de Huelva often present. That is, the tragic copla is missing. They seem to be more preoccupied with the beauty of the pueblo, the girls, and the positive, lighter aspects of love. The order in which they arr sung differs substantially from other fandangos. All fandangos de Huelva that I have heard sung repeat the first and third line to turn the five-line copla into a six-line copla. The fandangos de Enrinasola always repeat the first line twice to bring the total sung to six. In one of the examples you can see that a four-line copla repeats the first line in the second and last position to make a four-line verse into six. Paco in his article divides the compás in the singing of various fandangos into two groups: those wwith alternating compás (1 & 2 & 3 & 4 & 5 etc.) and those which follow the buterías type compás. These belong to the latter group. The melody of the singing requires an accompanying of the guitar which is unique as far as I know. It is the only fandango which starts in the E7-Am tones and finishes in the phrygian mode G-C-F-E7, repeating the F-E7 part twice. You can see how this is accomplished in the analysis of the melody. (Remember that this analysis is written for guitar and is only approximate--for example, in the second bar of the first line, the singing doesn't abruptly stop on beat four as shown, but this is the best way to give both the feeling of the guitar and singing together.) The fandango de Encinasola seems to be in a similar situa- 'GYPSY GENUIS' HISTORIC - EXCLUSIVE VIDEO RELEASE BY MANUEL AGUJETAS DE JEREZ (CANTAOR) For the first time in flamenco history the legendary Manuel Agujetas de Jerez performs on video eassette. The world famous maestro of the Jerez dynasty of gypsy flamenco singiog gives an historic performance that will remain forever. Beautiful cantes por Soleá, Fandango Grande, Siguiriyas, Malagueñas, Romeras, Taranto, Tientos, Bulerías. Length-90 minutes in color. This video features the special collaboration and original guitar accompaniments of recording and concert artist RODRIGO. Don't miss out on this first world release as it is a collectare item. No studio video of this kind has ever been made. Order Beta or VHS. Only $49.00. Send cash, check ar money order to Alejandrina Hallman. 148 Taft Ave., #11, El Cajon, CA 92020. The performance took place an August 5, 1985. An educational \"must\" for guitarists and singers. Allow 3 to 4 weeks for delivery. Spanish-Continental Cuisine Picasso 5254 VAN NUVS BLVD. SHERMAN OAKS, CA 91401 [818] 906·7337 Dance Espanol Incorporated Proudly Presents The Juan Talavera Spanish and Flamenco Dance Workshop For Beginner and Intermediate A Classic Combination PACO PENA & D'ADDARIO Born in 1942 in Córdoba, Spain, Paco Peña has been playing professionally since the age of twelve and has toured Europe both as a soloist and as part of the \"Paco Peña Flamenco Company\" to wide critical acclaim. Dedicated to conserving the pure artistry of flamenco, Mr. Peña established the seminar \"Encuentro Flamenco\" offering the aficionado an intensive program of study as well as the opportunity to live in Andatucía, the heart of this musical culture He has recorded nine albums for Decca Records including three live performances and a duo effort with Paco DeLucia, another world renowned flamenco guitarist. He has also made several highly successful tours of Australia, given recitals with the company at festivals in Hong Kong, Edinburgh, Holland, and Aldeburgh and performed to audiences in Japan and London, all 10 widespread enthusiasm. Paco Peña appears regularly worldwide on Television and has received extensive praise for his shared recitals with John Williams. Paco Peña uses D'Addario Strings. 367 Ed Rep Oct. Ban Siep, Oct. 9209 619 483-2703",
    "title": "FANDANGOS DE ENCINASOLA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 944,
    "article_char_count_full": 5696,
    "article_char_count_review": 5696,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_SPRING::A9",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEZPRERBE Guille W. FLAMENCO ON THE RADIO IN SPAIN Have you ever heard flamenco on the radio here in the United States? There is a local station that plays flamenco occasionally here in Denver. Wouldn't it be nice to turn on the radio at any time of the day and find out what new recordings were coming out, and hear live performances by many different artists! Well, this is now possible in Spain! It took so many years, but finally there is at least one solid flamenco station to listen to. The following report will examine four different stations. Upon arriving in Madrid, a quick search of both the AM and FM dials proved futile. Could this be true? To come all the way from Colorado with a bulky radio-cassette and find that all they're playing is Stevie Wonder, José Feliciano, Barbara\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"commercial\"]\n\noking for flamenco. After about five attempts there it was, flamenco on the radio, one half-hour program of all flamenco. The station's name is \"Radio Popular de Msdrid\" and it is found at approximately 1000 on the AM dial. The program is called \"El Mund del Flamenco\" and begins around 8:25 at night or 8:30, when programming goes on schedule. The announcer is Pedro Sáenz and he interviews different artists live, spins their albums, and even does commercial messages to the compás of flamenco records. The program is quite interesting; too bad it's only half an hour long. However, there is a program on the same station beginning at 8:00 pm which plays all Spanish music, and some flamenco may be heard at this time also. Here's a rundown of the typical program format of \"El Mundo del Físmenco\": The program begins with some commercials, sfter which Pedro Sáenz makes a few flamenco announcements; then the guitar of Parilla de\n\n[ENDING CONTEXT]\n\nperformance reviews appeared in the form of translated reprints from a Sevilla newspaper. Apparently Miguel knows his flamenco well and has organized it all in the form of this station. Thanks to him, you can now have your flamenco all day long until two in the afternoon. If you have other things to do in Sevilla, you will hear this station as you walk around from place to place. It can be found at 90 MHz on the FM dial. There was one other station with some flamenco, \"Radio Jerez\", but I didn't get any other information about it. If you go to other cities throughout Andalucía, you still can\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "GASPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 1395,
    "article_char_count_full": 7966,
    "article_char_count_review": 2556,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "commercial"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_SPRING::A10",
    "article_text_for_review": "THE SHAH REVIEWS JOSE MOLINAS BAILES ESPANOLES PRESENTED MAR. 9. 1985 JOSE MOLINA WITH AURORA REYES CLARA MORA ESTER SUAREZ PEPE DE CADIZ GERARDO AL CALA AND BASILIO JORGES Mr. Molina's effort belonged not in Carnegie Hall, but in a cabaret. He is the only performer we have ever known to use canned music in a concert hall of such prestige and before an audience of such discernment. If one pretends to dance the music of De Falla or Albéniz in a grand concert hall, one should be dancing to a complete and competent orchestra, of which there are several in the New York area. Leaue the taped version for the rehearsal studio. This small troupe was swallowed up by the immensity of the stage, which was utterly bare of any decoration or atage set whatever. The sounds of both music and footwork were lost in the cavernous spaces of the hall. Clesrly the group was \"out of place\" in more than one sense of the phrase. Mr. Molina has proven once again that he hss no talent for choreography, not the slightest feeling for fitting appropriate movement to music. He and his three ladies danced chorus style through much of the program, all four of them executing exactly the same steps at exactly the same time in exactly the same androgynous style. Three wore dresses, one wore pants; otherwise there was little to dintinguish one from the other. Mr. Molina's stage presence is too cute and frivolous (this is a man approaching 5D years of age), his arms are weak, his hips hyperactive, and his movements independent of the requirements of the music. He might as well have been doing gymnastics. His idea of partnering appears to be a man and a woman dancing, each alone. There was no intensity in his partnering, no male supremacy, no play of tension, no drama. The ladies of the troupe were quite competent and interpreted well \"Asturias\" and \"Bolero\" choreographed by Mariano Parra. As for \"La Boda de Luis Alonzo,\" poor Luis Alonzo would never have gotten married had he known what this troupe would do to the exciting musit of his wedding celebration. The flamenco numbers were competently if unremarkably accompanied by cantaor Pepe de Cadiz and guitarists Cerardo Alcalá and Basilio Jorges. That some of the audience were moved to give this garbage a standing ovation should surprise no one. There are people in New York who will applaud a chain-snatching, and whoever paid twelve to twenty dollars to see this mess certainly got mugged. Molina reached the apogee of his talent six or seven years ago and began a slow and steady decline from which he appears unable to recover. Sic transit gioria mundi.",
    "title": "THE SHAH SPEAKITH",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 455,
    "article_char_count_full": 2608,
    "article_char_count_review": 2608,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
