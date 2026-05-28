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
    "article_id": "JALEO_1977_12::A4",
    "article_text_for_review": "OPEN LETTER FROM PACO SEVILLA Dear JALEO, I'm writing this from Kansas City, at the halfway point of the tour. We bave been all through Ohio, up to Wisconsin and Iowa, and now to several cities in Missouri and Kansas. At this point, the two separate companies come together for a three day residency at Witchita State University and then form a single company. Rayna went back to San Diego and to her work at The Matador in Los Angeles; the Mariachis return to their homes in Los Angeles and New York; the Estudintina return to Spain and Timo Lozano and Pedrito Cortez (dancer and guitarist) return to New York. Five Argentinian PACO SEVILLA- continued \"Gauchos\" dancers and Juan Cortez, a gypsy flamenco guitarist who accompanies $ \\underline{\\text{The Gauchos}} $, join us in a group that will continue for three more weeks, traveling through the south and then eventually back to the northeast. We will finish at Lincoln Center in New York City. So far, in spite of numerous car problems at inopportune times and a bizarre schedule that sometimes allows for very little sleep, we have had no serious problems. It is enjoyable doing the shows, but it would take a special kind of person to do this for very long. There is no time to practice, little opportunity for exercise, (especially for non-dancers), too little sleep and too often the food consists of hamburgers and french fries, or a steak and baked potato. I can't believe that these people once did a twenty-three week tour! One thing that I have come to appreciate more than ever is the importance of the flamenco singer to the unity of a flamenco group in performance. The song provides a reference point for both the guitarist and the dancer--allowing them greater freedom of expression, without the danger of losing compas. When the singer has good compas, the dancer does not need the guitar and everything flows. When the singer is out of compas, everything tends to fall apart. Many times when we are doing bulerias, I become disoriented with regards to compas, while following the countertimes of the dancer. But as long as the singer is present and on time, it doesn't matter if one knows where one is in the compas-- everyone just flows until the beat becomes obvious again. In those moments, one feels a tremendous sense of freedom. Simon Blasco, our empresario, brought up an interesting point: He feels that a flamenco company must have a male dancer as a central figure if it is to be successful. He says that he has never had a flamenco show without a male star that worked out well. The current tour seems to bear this out. About three-fourths of our audiences are junior and senior high school students and the attitude of the girls is hard to believe. Universally, they react toward Alfonso, our lead dancer, in the way teenage girls have usually reserved for rock and roll idols like Elvis Presley or the Beatles-- they scream and shriek at everything he does. (Without fail, a roar goes up when he removes his jacket part way through his dance). After the show they mob him, trying to touch him or even rip his clothes. I have seen girls standing trembling or crying while waiting for an autograph. Many of them are oblivious to the other performers on the stage, PACO SEVILLA- continuación PACO SEVILLA- continued and it is a little depressing to realize that the art involved is not being recognized. Even Alfonso is a bit disturbed by the fact that the response toward him is not based primarily upon his dance abilities, although, undoubtedly, the strength of his dancing is part of his image. However, there is a positive side to all of this-- thousands (hundreds of thousands, over the past few years) of young people are being left with a tremendously strong positive image of flamenco that will undoubtedly remain with them throughout their lives. No lukewarm appreciation of a quaint folk art could have anywhere near the same impact on most people. So it must be good for flamenco in the long run. That does it from your roving reporter. See you all at the next juerga. Paco This letter was received by $ \\underline{\\text{READER}} $ magazine following their two page article on $ \\underline{\\text{JALEISTAS}} $. GYPSY GYP Your evocative \"Invitation to the Dance\" (October 20) is a terrible tease. Here I am all charged up, looking for the gypsies all the way from Bonita to Ramona to Vista, and they're out of sight. They're not even in the Yellow Pages. Please, don't keep these secrets all to yourself. My heart has wept enough blood. Rudy Melton San Diego",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1977_12",
    "year": 1977,
    "language": "en",
    "article_type": "other",
    "pages": "3,4,5",
    "page_number": 3,
    "word_count": 791,
    "article_char_count_full": 4550,
    "article_char_count_review": 4550,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1977_12::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Jack Jackson Sabicas, or Agustin Castellon, is a flamenco guitarist born in the north of Spain at Pamplona, in the year 1907. For several decades he has been the leading innovative guitarist in flamenco. Sabicas has everything: uncanny technical precision, dazzling speed, perfect intonation and an absolute understanding of flamenco structure and tonality. In addition, he is highly inventive. His first guitar was a small scale copy with six strings, which he persuaded his parents to buy for him at the age of five. An uncle, who knew only chords, taught them to him; they were E (mi) and A (la). He played these two chords along with everything else he heard, and as a gypsy living among gypsies, he heard a lot of flamenco. It took Sabicas only three years to master his little guitar. When\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"teacher\"]\n\nwhich he persuaded his parents to buy for him at the age of five. An uncle, who knew only chords, taught them to him; they were E (mi) and A (la). He played these two chords along with everything else he heard, and as a gypsy living among gypsies, he heard a lot of flamenco. It took Sabicas only three years to master his little guitar. When he was eight, his mother thought that he should have guitar lessons. Sabicas says,\"She took me to the best teacher in Pamplona, who asked me to play something. I played a run (escala), and the teacher became furious. He accused my mother of trying to make fun of me and ran us out of the house.\" When he was nine, he made his debut in Madrid at the $ \\underline{\\text{El Dorado Theater}} $, with the company of La Chelito. Because he was so young, he was presented as a child prodigy and played solo wearing the short trousers of a school boy. It was a scandal. Sabicas related that, \"they made so much noise that I thought they were going to hit me.\" At eleven years of age he won first prize as the best guitarist at the $ \\underline{\\text{Monumental Cinema}} $ Theater in Madrid. At twenty he was considered the genius of the flamenco guitar and he toured Spain as a gui- tarist. In 1936, a cruel civil war erupted between the ruling Republicans and the Nationalists. Sabicas formed a flamenco company in 1937 and embarked on his own tour of South America, first playing in Buenos Aires. He was not to return to Spain for many decades because of his political views. In 1942, he accompanied Carman Amaya, one of the greatest flamenco dancers, on her first American tour. He remained with her for five years. He owned a home in Mexico City and lived in Mexico for about twenty years. Much of the money he made was gambled on horse races. He appeared in movies in South America, Mexico, and Hollywood. He also played a command performance at the White House. He moved to New York City permanently in 1957. Sabicas, o Agustin Castellón, guitarrista de flamenco, nacio en Pam\n\n[ENDING CONTEXT]\n\nof what might be coming next. The second twenty years are spent accompanying singers. This is more difficult because he must simultaneously lead and follow the singers rhythm. Then at the age of 60 the guitarist is ready to solo. After so many years absence from Spain, the Spanish government awarded him the Gold Medal of Honor for having contributed more to the cultural development of Spain than any other living man. He returned to Spain for the presentation. At the present time, Sabicas is living in New York City and is still performing solo flamenco concerts. SABICAS- continuación\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "SABICAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1977_12",
    "year": 1977,
    "language": "en",
    "article_type": "other",
    "pages": "6, 7",
    "page_number": 6,
    "word_count": 1221,
    "article_char_count_full": 6932,
    "article_char_count_review": 3635,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "teacher"
      }
    ]
  },
  {
    "article_id": "JALEO_1977_12::A6",
    "article_text_for_review": "Last week I was fortunate enough to get a few days off to accompany my wife to Mexico City on a research trip for her studies. I had proposed to myself to spend part of the time there investigating the flamenco scene and reporting to $ \\underline{\\text{JALEO}} $. Simply stated, there is definitely flamenco in the Mexican capital, but it is not as popular as you might expect in an Hispanic influenced country. A search of the white and yellow pages of the phone book revealed no references to either clubs or dance schools. However, I had lived in Mexico City many years ago and so I went to see if an old haunt still existed. I had a little trouble finding it because the new Metro has changed the geography somewhat. However, $ \\underline{\\text{Gitanerías}} $ is still going strong at 15 Calle Oaxaca, just 50 meters or so from the metro station \"Insurgentes\" (direction Observatorio). They have a complete cuadro with eight or nine persons including cantaor, declamador (reciter) and even gaiteros (bag pipists). The first show is at 11:30 p.m. and there is a 50 peso (2.50) cover. You can go earlier for dinner to the accompaniment of gypsy violinists. I was not able to actually see the show, but I did talk to the owner for a while. He said that all that the young people want is \"yeh-yeh\" music and American clothes, etc. (By the way, if you spot the noisy \"Pink Frog\" you are almost at $ \\underline{\\text{Gitanerias}} $). He also said that members of the group give both guitar and dance lessons. Another club exists by the name of $ \\underline{\\text{Corral de la Morería}} $. However, I was not able to get the address. Our tour guide, who just happened to have studied flamenco for eight years put us on to this place and said that the best male dancer in Mexico performs there. I know there are several other places with shows, but I couldn't get their names or addresses easily. At Niza 44, 3rd floor, there is a Spanish dance school. The owner, a portly Spanish lady, had no bulletins, nor could she offer detailed information to me about classes; but she did say I could come back later to observe. Around the corner at Londres 101, is a Spanish tasca, whose show consists of the clients, according to the bartender. What I want to say by all of this is that if you want flamenco in Mexico City, it is definitely there, but you may have to do some searching. Start at $ \\underline{\\text{Gitanerías}} $ and good luck! EN BUSCA DE FLAMENCO en mexico La semana pasada tuve la suerte de tomar unos días de vacaciones para acompañar a mi esposa en un viaje a México. Me propuse pasar una parte de mis momentos de ocio en investigar el flamenco allá, y en preparar un informe para JALEO. Resulta que por supuesto, hay flamenco en la capital mexicana, pero tanto como uno pensara considerando la influencia hispánica. Biscando en la guía de teléfonos no encontré ningún club ni escuela de baile que fuera evidentemente de falamenco. Sin embargo, hace muchos años viví en México y así pensé volver a visitar un club que había frecuentado. Tube un poco de dificultad en encontrarlo porque el nuevo Metro ha cambiado la geografía por allí. Pero Gitanerías sique bien en la calle Oaxaca, nc. 15, a sólo 50 metros de la estación metros de \"Insurgentes\" (dirección Observatorio), Ofrecen un cuadro completo con cantaor, daclamador y hasta gaiteros. El primer show empieza a las 11:30 de la noche y piden 50 pesos para entrar. Más temprano se puede cenar con música de violín gitano. También existe el club Corral de la morería, pero no pude conseguir la dirección. Nuestro guía turístico, que estudio flamenco cuando niño, nos mencioné este lugar y dijo que el mejor bailarín de México estaba allí. Sé que hay otros clubes pero no pude conseguir sus nombres ni direcciones. En Niza 44, piso 3, hay una escuela de baile española. La dueña, una española bastante gorda, no podía darme ninguna información escrita a observar una clase más tarde. A la vuelta en la calle Londres, no. 101, hay una tasca Española donde solo los clintes mismos que hacen el \"show,\" según el cantinero. Lo que quiero dicir con todo esto es que si desea ver flamenco en México, sera posible pero a lo mejor tendrá que buscar un poco. i Emiece en Gitanerías y buena suerte!",
    "title": "THE SEARCH FOR FLAMENCO in mexico city ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1977_12",
    "year": 1977,
    "language": "en",
    "article_type": "other",
    "pages": "8",
    "page_number": 8,
    "word_count": 755,
    "article_char_count_full": 4250,
    "article_char_count_review": 4250,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1977_12::A7",
    "article_text_for_review": "by Tony Pickslay On my very first day in Mexico City, I spotted a poster for Paco de Lucía! He was going to give three concerts, starting the very next day. \"What luck!\" I said to myself. So I hustled down to the Teatro de la Ciudad ticket office and got seats in the front row, twelve seats from the center. I knew of his reputation, but had not really heard anything by him except one or two tracks on a friend's record. I have heard many guitarists and I decided to try and go with as open a mind (ear?) as possible. My principal reaction to the concert was: \"This guy has a fantastic future ahead of him as a jazz guitarist!\" I will try to explain. The theater was about 2/3 full and the program announced a traditional concert: First part Alegrías, Taranta, Guajira, Bulería, Zapateado, Panderos Flamencos. Second part Soleá, Fandangos, Guajira, La Malagueña, Rondena, Rumba. Then a second guitarist, \"Ramón de Algeciras.\" The crowd was very attentive and Paco came out only a couple of minutes late, sat down, said nothing and began to play. Between numbers, he tunes up with a series of very modern chords. He remained silent, not even a smile throughout the entire concert! Not a word to the Spanish speaking audience! Obviously he was concentrating on the music. But the theater remained untouched, giving good applause to each piece, but hardly reacting as you would expect to the performance of the \"world's greatest flamenco guitarist.\" I found myself saying, \"Man, this guy's not that exceptional,\" and not really getting a good feeling for the music. The rhythm wasn't even getting to me. The playing was clear, precise with the amazing picado runs, but there was no duende! It was too cerebral. And so it continued, even when Paco's brother Ramón came on midway through the second half, until... the last number, Rumba. Then an incredible event took place. With his brother playing backup, Paco started out on what turned out to be a fantastic display of what I must call jazz guitar playing-- a driving, intense, technically superb series of long melody runs. I have no idea how much if any was improvised. I could not see his hands all the time, so I can not say how he did it, but I think that it was mostly picado. Mi primer día en México ví un cartel que anunciaba tres conciertos por Paco de Lucía. El primero era para el día siguiente. \"¡Que suerte!\" me dijo. Así fui en seguida a la boletería del Teatro de la Cuidad y conseguí 3 asientos en la primera fila, aunque un poco al costado. Conocía l fama de este muchacho pero nunca le había escuchado tocar con la excepción de una o dos piezas en el disco de un amigo. He escú escuchado a muchos guitarristas y decidi ir con el mínimo posible de preconceptos. Mi reaccón principal después del concierto fue: \"¡A este tipo le espera un futuro maravilloso como guitarrista de jazz!\" Trataré ce explicar. El teatro estaba 2/3 lleno y el programa anunciaba un concierto tradicional: Primera parte Alegrías, Taranta, Guajira, Buleria, Zapateado, Panderos flamencos. . Segunda parte Soleá, Fandangos, Guajira, La Malagueña, Rodena, Rumba y segundo guitarrista \"Ramón de Algeciras.\" El público estaba muy atento y Paco apareció sólo unos minutos tarde. Se sentó no dijo nada y empezó a tocar. Entre las piezas afinaba la guitarra con unos acordes muy modernos. ¡Quedó mudo por todo el concierto! ¡Ni una palabra a la audiencia hispano-parlante! Obviamente se concentraba en la música. Pero el público quedo bastante indiferente casi, aplaudiento bien pero sin reaccionar como uno esperaría frente al \"mejor guitarrista flamenco del mundo.\" Me decía a mi mismo, \"Este tipo no es tan extraordinario\" y no me entraba la musica. Ni el ritmo me agarraba, tocaba bien, nitido, preciso, con eses secunias de picado, tan increibles;! pero no había duende! Todo era demasiado cerebral. Y así continuaba aun cuando su hermano \"Ramón\" se junto a Paco a la mitada de la segunda parte, hasta...la última selección, Rumba. Entonces transcurrió un evento increíble. Con el harmano acompañando, Paco empezó lo que resultó una muestra fantástica de le que tengo que llamar música de guitarra de jazz; una serie de notas increíblemente intensa, pujante, magnífica tecnicamente. No puedo decir si alguna parte fue improvisada Ni sé bien como lo hacía porque sus manos me estaban escondida muchas veces; pero creo que fue picado principalmente. El público reaccionó a todo esto y era evidente que Paco mismo gozaba. Y seguía una secuencia maravillosa tras otra, por toda PACO DE LUCIA- continued The audience reacted strongly to it, and you could see that Paco was enjoying it. He just kept going, one tremendous run after another, all over the fingerboard, and Ramón dept right with him. I shall never forget it. My only regret was that I didn't have a tape recorder with me. At the end I was immediately on my feet with most of the audience (something I rarely do, being a timid sort). And Paco finally smiled. He even returned a Cordoba style sombrero to it's owner with a flick of the wrist. One encore was all we could manage to get and it was a continuation of that unforgettable Rumba. He really digs that kind of playing. Back in San Diego, a friend told me that in the $ \\underline{\\text{Guitar Player}} $ magazine, an article appeared on Lucía and one of the things mentioned was that he was experimenting in other areas, including jazz. All I can say is that those eight or ten minutes made the whole trip to Mexico City worthwhile!!!",
    "title": "PACO DE LUCIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1977_12",
    "year": 1977,
    "language": "en",
    "article_type": "other",
    "pages": "9, 10 ",
    "page_number": 9,
    "word_count": 953,
    "article_char_count_full": 5488,
    "article_char_count_review": 5488,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1977_12::A8",
    "article_text_for_review": "\"Awaken, my son, the new dawn has come to wash your face with the morning frost; and gather your thoughts, for our journey is long in search of the virgin valleys of Lost Dreams.\" \"Papa, is there such a place? Is there, Papa? Is there?\" \"Yes my son, there is such a place, nestled in the Sierras of southern Spain, a place so beautiful that even the eyes of a thousand eyes can't capture all of the wealth that lies beyond the reach of men. Where the Sierras touch the sky of magic. Where one is protected from the world of tragic Where the flowers of Spring blossom to the serenading of whispering pines. Where the hummingbird showers Life with the nectar of love. Where the goddess of Nature smiles freely upon you. Where the sun comes into being, in a moment of silence. Where men can roam free, away from the land of violence. Where the sun fades away into yesterday and the night life runs and plays for free. Where one can rest in peace to the serenading of the summer breeze.\" \"But, Papa, why must we travel so far? Why, Papa?\" \"Why? Why is there darkness, my son? For we are gypsy; gypsy living in Darkness.\" Living in the shadow of the Payo. Living in the forgotten valleys of yesterday's thoughts. Traveling the burning roads of no return, looking for peace to mend our burns. Watching in sadness the Bailaor dancing to broken strings of lost music, surrounded by the ring of fire. Listening to the Cantor sing his songs of lost words, lost in yesterday's darkness. Where the Guitarrista plays notes of loneliness looking for chords that will fill the heart with laughter. Looking for a handshake with the grip of friendliness.\" \"Papa?\" \"Yes, my son.\" \"When I grow up I want to be a King. I would wave my magic wand so the flowers of Spring can sing to you.\" I would give you the Golden Key; the Key of Life that unlocks the door to the Valley of Dreams. I would make it rain to wash away your pain, so you can live in your Valley of Dreams without shame. Papa, I love you.\" \"We must begin our journey, my son, for the road is long and the day is short. Vama-arre.\" To understand a gypsy, One must be a gypsy; But to understand a gypsy as a friend, One only must be a friend. Para entender a un gitano, Uno debe ser un gitano; Pero para comprender a un gitano como amigo, Uno necesita solamente ser un amigo.",
    "title": "GYPSY IN DARKNESS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1977_12",
    "year": 1977,
    "language": "en",
    "article_type": "other",
    "pages": null,
    "page_number": 11,
    "word_count": 435,
    "article_char_count_full": 2318,
    "article_char_count_review": 2318,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
