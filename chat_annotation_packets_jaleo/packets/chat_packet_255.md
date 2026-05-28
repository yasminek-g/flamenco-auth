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
    "article_id": "JALEO_1986_WINTER::A5",
    "article_text_for_review": "I thought Jaleo readers, unfamiliar with the Feria of Sevilla, might like to hear a bit about this year's event. In spite of lots of advice from friends, I know that I was unprepared both for the nature and the magnitude of this unique annual ritual in which the whole city and much of the surrounding area enjoys several days of something approaching the medieval concept of \"days of misrule.\" Day is turned into night and the present mingles with the past as elegant Spanish matrons spread their heirloom mantones over the balconies of the Plaza de Toros, horsemen in cropped jackets, with wide Cordobés hats, guide their mounts through the narrow passages of Barrio Santa Cruz in search of friends or the best local tapas. One doesn't have to visit the fairgrounds to be reminded that this is Feria week--the reminders are all over town. The festivities actually begin much earlier, in the salons and workshops of Sevilla's dressmakers and costumes designers, who work overtime creating the most elaborated and befrilled dresses since Scarlet O'Hara's garden party frock--the \"trajes flamencos.\" While the basic patterns are traditional, each year's styles are slightly different--a little longer ruffles, a different kind of trim, so that fashionable ladies insist on a new dress for each Feria. Last year's dresses can be picked up for a song by canny readers of the local classified ads. For those whose budget's don't allow for custom tailoring, hardly a shop in the city is without at least a small selection of ready-made dresses, ranging from the offerings of Corte Inglés and fashionable boutiques to the racks of the corner \"tiendas de todo.\" These dresses are not to be confused with the professional costumes seen on dancers in tablaos or flamenco concerts--the skirts are stiffer, the sleeves short and puffy, the ruffles more innocent and fliry--no matter what age of the woman wearing one, she looks as if she'd fit in at a deb ball or birthday party. Still, as the traditional costum of Andalucía, the outfit is perfect for dancing sevillanas, which is, after all, what the Feria is all about. In the weeks before the Feria one cannot walk into a shop or home in Sevilla, or down any street, without hearing sevillanas coming from one or more sources--it is the regional muzak, heard in supermercados, taxis, and elevators. But it is also a rhythm in the air--tapped out by old men's canes, clapped by groups of young people walking down the sidewalk, hummed by almost everyone. The sound of sevillanas becomes a part of the ambient noise of the city--like the traffic, the swallows, the shouts of playing children. The weekend before the fair begins, workmen scurry around the fairgrounds (located on the outskirts of town, next to the barrio of Los Remedios), constructing the structures known as 'casetas', which will be the focal point of the following week's festivities. They also provide public services adequate for crowds estimated at up to a million people a day, refreshments stands and curio displays just outside the fair itself, and a large amusement park. Once the casetas are constructed, the decorating crews take over to festoon them with special wallpaper printed to resemble azulejos (Spanish tiles), lace panels, ceilings made up of balls of tissue paper wired onto clothesline, posters, paintings, fans, photographs, and even antique mirrors or other furnishings in the more elaborate structures. Each caseta is operated by a particular group or individual; over 500 applications are waiting for an opening for one of the approximate 300 places available. Some are run by political parties, district governments, trade unions, or public agencies, but the majority belong to private clubs, associations, businesses, or just to individuals or groups of friends who can raise money for the annual fee. Monday night, at midnight, the huge arch at the front gate of the fairgrounds is illuminated; the surface is covered with hundreds of lights bulbs and the arch itself casts a glow that can be seen all the way across the river. More lights are strung across the streets so thickly that one has to peer up through them periodically to be reminded that it is not daylight lighting up the casetas and streets between them. Traditionally, each caseta has a private party for its members or special invited guests on Monday evening, opening up to a wider audience later in the week. Beginning on Tuesday, each day naturally divides into two parts--the morning--roughly from 11:00AM to 4:00PM, and the night and dawn--from 11:00PM until 6:00 or 7:00 in the morning. In fact, there is hardly any time PAGE Published by M Haas in staff notation plus tabulation. Published by: Gitarren-Studio Musikverlag E.M. Haas, Blissestr. 54, D-1000 Berlin 31 WEST GERMANY. Direct orders are welcome. Please wait for Pro-Forma invoice.",
    "title": "LA FERIA DE SEVILLA, 1987",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 802,
    "article_char_count_full": 4852,
    "article_char_count_review": 4852,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_WINTER::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nIn Madrid and Seville the Nightclubs Called Tabaos Feature the Soulful Gypsy Dance [from: $ \\underline{\\text{The New York Times}} $, March 22, 1987; sent by George Ryss] by Hubert Saal Flamenco, according to Federico García Lorca, its poet laureate, was \"born of the first cry and the first kiss.\" Flamenco is elemental: the women are hot, the men cool, and the songs sung in voices that spring from the earth, tasting of tobacco and wine. The songs came first. Like the blues of black America, the songs are lonely cries from the heart. There is as enormous body and variety of flamencan songs and dances, light as well as dark, saucy as well as sad, quick and slow, heated and calm, from the bulerías of Jerez de la Frontera and the malagueñas of Málaga to the bounty of Seville, including the\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"places\"]\n\nely cries from the heart. There is as enormous body and variety of flamencan songs and dances, light as well as dark, saucy as well as sad, quick and slow, heated and calm, from the bulerías of Jerez de la Frontera and the malagueñas of Málaga to the bounty of Seville, including the sevillanas corraleras, which has become Spain's national dance, and the soleares of Triana, Seville's gypsy quarter, which often are anguish distilled. The customary places to see flamenco in Spain are nightclubs, called tablaos. There the cuadro, the group that usually consists of a dozen guitarists, singers and dancers, stands or sits around the rear of a small stage while one or more of the company dances front and center. They are all jaleadores, clappers, capable of the softest or most jarring sound of palm against palm, and who in tight, syncopated rhythms intensify the beat. They also shout encouragement to the dancer, and the audience joins in enthusiastically: \"Olé,\" \"Elé,\" \"Guapa\" (\"you, pretty girl, you\") or \"Así se baila\" (\"That's the way to dance.\") The flamenco in tablaos is frowned upon by purists. Commercialism has compromised the authenticity of flamenco, they say. And they're right. At the heart of the flamencan experience is the mysterious duende, which Lorca called black sounds, a ki\n\n[ENDING CONTEXT]\n\nGuadalquivir. Be sure to reserve ahead; then get there early and hang on to your seats so you don't land behind a pillar. In Triana, and in the adjoining Los Remedios, are a series of bars in which the old traditions of the cafe concertante flourish. Each has its stage and every night volunteers, amateurs and professionals from the audience, play the music, sing the songs and perform the dances, all extemporaneously. Stop in at La Garrocha on the Calle Salado or, nearby, Las Trés Faroles or La Canela Pura, which means, literally, pure cinnamon, and idiomatically, super. That's about right.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "THE BLUES OF SPAIN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "poem",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1401,
    "article_char_count_full": 8177,
    "article_char_count_review": 2925,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "places"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_WINTER::A7",
    "article_text_for_review": "[from: Correo de Andalucía, May 21, 1986; sent and translated by The Shah] by Luis Caballero \"The light is buried by chains and noises In unholy defiance of science without roots. The people of the neighborhoods dazed and tottering Like sleepwalking refugees from a ship wrecked in blood.\" --García Lorca I don't know why a copy of $ \\underline{\\text{The Poet in New York}} $ accompanied me during that trip to Granada, but I do know that on one of its pages I left written, deeply impressed by the events, these few lines that today return me to the happy memory of those unforgettable days in the company of the great dancer, Pilar López. \"Today has been, for me, perhaps the happiest day of my thirty-five years of existence. I visited Fuente Vaqueros with Pilar López and listened to Doña María talk about her cousin Federico (Garía Lorca), I visited the house and the room in which the poet was born, and finally the mountain where they shot him. We left him some carnations, some magnolias and some tears.\" June 29, 1954. All this began because, along with the guitarist Eduardo el de la Malena, the enthusiastic Rafael Delmonte, and half the age I presently possess, offered two ladies of the high Spanish aristocracy a long recital in a beautiful room of the old headquarters of the National Radio in Seville. The younger lady, very elegant, announced to me at the end, \"You, Sir, are going to go to the Festival of Music and Dance in Granada.\" And I went. In a blessed hour I went. myself. Dancing were Paquita Rodriquez, Teresa Amaya, Adela Borja, Delia Montenegro, Curro Veléz, Paco Aguilera, Fernando Terremoto, and Mariano. Guitars were Luís Maranilla, Juan Hidalgo, Manolo Amaya, Miguel el Santo and Juan Amaya. Amongst us were the cantaores Juan Varea, Luis el de las Marianas, El Pili, Ramón de Loja, Pepe el Culata, Albaicín and \"Granada, serene and refined, encircled by her mountains and definitively anchored,\" enclosed as always in her color and warmth of Upper Andalucia, but for those days of the third Festival of Music and Dance of 1954, she was distinguished with a solemn aura. JALEO - VOLUME IX, No. 4 From among the organizers of the flamenco portion of the festival, I struck up a friendship with a great personality, Andalusian par excellence, intelligent aficionado and pretigious doctor of medicine Fernando Lastra -- a friendship which endures to this day. He, along with my old companion of escapades and adventures and myself went up to the Hotel Washington Irving, where Doña Pilar was waiting to practice with Fernando the \"Baladilla de los Tres Ríos\" with the music for this being por tientos. Doña Pilar, I only had known, from a spectator's seat, as a dancing figure. I had enjoyed the serene delicacy of her dance and her sense of choreography. Now we were going to speak together, perhaps even to strike up a friendship. After a very short practice, a long conversation revealed to me a cultivated woman, simple, elegantly flamenca, and the owner of a sharp sense of humor. She had finished performing in the festival, but remained for several days in Granada, savoring it to the last drop like an exquisite cup of nectar. She spoke of her sister Encarnación (La Argentinita), of Lorca, of Sáchez Mejías, of baile... Baile flamenco with depth and art is from the lowlands. In the mountainous regions, they dance upwards, vertically, with less use of arms -- the arms are the part of the body that dances -- and more footwork... Federico could make a drama out of any historic occurrence, using a handkerchief for a curtain... Federico was a born musician, a genius even when speaking of the most simple, mundane, and commonplace.\" \"Doña Pilar,\" I said, \"it so happens that tomorrow I'm going to Fuente Vaqueros. I want to see the village where Garía Lorca was born.\" She thought for a moment and said for me to wait. That night in the Arabian Corral del Carbón, we debuted along with an unannounced guest artist dancing the Baladilla de los Tres NEXT TO THE SPOT WHICH MIGHT BE THE EXACT GRAVE OF FEDERICO GARCIA LORCA, WE POSED, FROM LEFT TO RIGHT: LUIS CABALLERO, ANTONIO DIAZ, PILAR LOPEZ AND JAIME CARRION - MONTES DE VIZNAR 1954",
    "title": "PILAR LOPEZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "poem",
    "pages": "14",
    "page_number": 14,
    "word_count": 723,
    "article_char_count_full": 4176,
    "article_char_count_review": 4176,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_WINTER::A8",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRíos. \"Pilar danced this accompanied by the very flamenco composition and well executed interpretation of Fernando Lastra, a student at the time. A tremendous success it was!\" The public that filled the Corral that night contained such sociopolitical importance that the nest day, ABC featured on its front page a large photograph, at the foot of which read: \"The wife of the head of state, in Granada. Partial view of the Corral del Carbón, during the fiesta of cante and baile offered to Doña Carmen Polo de Franco, with the presence of the ministers of Finance and of National Education, the undersecretary of state, the ambassadors of Great Britain, Germany, the Netherlands, and Switzerland, the mayor of Granada and other authorities who happen to be in Granada.\" But above all this bombastic\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"maintained\"]\n\nf the head of state, in Granada. Partial view of the Corral del Carbón, during the fiesta of cante and baile offered to Doña Carmen Polo de Franco, with the presence of the ministers of Finance and of National Education, the undersecretary of state, the ambassadors of Great Britain, Germany, the Netherlands, and Switzerland, the mayor of Granada and other authorities who happen to be in Granada.\" But above all this bombastic sounding assembly, I maintained one fixed and unwavering idea -- to visit the village and house in which Federico Garía Lorca was born. The next morning there were seven of us who boarded a taxi and headed towards Fuente Vaqueros; Pilar, the journalist Miguel Utrillo, the psychiatrist Urquijo, Lieutenant Jaime Carrión of the Air Force, Fernando Lastra, Antonio Díaz and myself. The inhabitants of Federico's hamlet were accustomed to the most diverse types of outsiders enquiring after him and his traces, and though \"nobody knew anything\" some elderly ladies showed us the entire house. The most ancient of these ladies repaired, not without a certain affection overcast with sadness, to a certain room, showing us where on the fifth of June, 1898 at twelve midnight, doña Vicenta Lorca brought into the light of the morning star a child which thirty-eight years later would astonish the world with love, culture and art by his death and his work. After visiting doña María\n\n[ENDING CONTEXT]\n\nvisit by \"cantaor\" Chiquetete who announced that Camarón would not sing. The rowdy audience burst into shouts and whistles protesting Camarón's absence. Price of tickets was 1000 pesetas and was to be donated to the \"Carbonería\", a long time Sevilla establishment. —Guillermo Salazar FLAMENCO GUITAR Flamenco Rubia guitar, brand new in case. Excellent sound. Constructed by master guitar maker Pedro Maldonado of Malaga, Spain. $1600 or best offer. (818) 786-0637 PACO PENA “Live in Munich” $14.95 us. Postage & Handling U.S. and Canada - $1.50 Other Countries - $3.00 The Blue Guitar Workshop\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "poem",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1410,
    "article_char_count_full": 8055,
    "article_char_count_review": 3031,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "maintained"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_WINTER::A9",
    "article_text_for_review": "SEVILLANAS ARTE, AIRE Y GRACIA What! Over one hundred sevillanas clubs in Bilbao, the heart of the Basque country. One hundred and fifty sevillanas clubs in Barcelona, the heart of Catalonia and hundreds more from Madrid to Galicia, from Asturias to Cádiz. This all seems a bit amazing considering that twenty years ago while touring Spain in the \"Festivales de España,\" with Pilar López, we had to have three different concert programs depending where we were in Spain, as Spain is one of the most regional countries in the world and it was jotas in the north and flamenco in the south and all of the different regional dances for the specific regions that we went to. Sevillanas in the 1980's has become one, if not the one, universal popular dance of not only the festivals and ferias, but of the social populus, the doctors, lawyers, merchants and housewives and everyone else. This phenomena, after experiencing dancing sevillanas does not seem so amazing, as sevillanas, with all of its built-in \"aire y gracia,\" has captured the new mood of an international Spain that crosses all regional borders and gets to the heart of all of the people of Spain. One of my most memorable experiences dancing sevillanas happened in the middle 1950's. José Greco and company had come to Los Angeles for a series of concerts. This was one of his first tours of the U.S. and with his superb company of artists, he was enjoying that tremendous success that only a few artists have experienced in the world of dance. It was Spanish and flamenco dance that was making waves of popularity wherever they went. This particular day began for me when I went to see a matinee of Greco's concert at the Wilshire Ebell Theatre in Los Angeles. Following this concert I was invited to a party given by a friend of mine for the Greco Company. I had been studying for a few years and had learned a few \"dances\". That is the way that I started out when I began to study with José and Eduardo Cansino, Paco Lucena, the Trianas and others in Los Angeles. They mainly taught \"routines\" and did not deal much with technique or what could be considered \"flamenco\" flamenco, but mainly school dances. Of course one of these dances taught by José Cansino was a nice old version of four coplas of sevillanas. The Cansinos were very famous in their days in the theatre, and sevillanas was one of their main dances. I had learned it by following along and learning the steps, for that was what was taught, steps, not much explanation about aire, technique, history, interpretation, just steps and hopefully the awareness of what they all were supposed to express. It all worked, for there was a lot of joy in class, and everyone in the big Saturday classes really got into it. began to dance. The mood was happy and alive and the atmosphere had a speciol energy that was affecting everyone. All of a sudden Teresa Maya was standing in front of me with that fantastic smile and said, \"Quieres bailar?\" My knees almost buckled. Before I knew it I was dancing sevillanas with her. To this day, all I remember was that I never felt so good, like I was finally dancing the way I always wanted to feel it. She just looked at me while we were dancing and by some magic the sevillanas danced me and I danced it and this whole combination of dancing with a great artist, dancing this magic dance that symbolizes the cradle of flamenco, was the combination that let me know that I was a dancer. This was the inspiration, to search for that feeling all my life. I was addicted. From that time on I have always had a special feeling for sevillanas and have never gotten tired of dancing them. Over twenty years later, while dancing at the Café de Chinitas in Madrid, I walked up to Teresa Maya who was featured there and she looked at me for a minute and asked if she knew me; I reminded her of that day in Los Angeles and she smiled and I asked her to dance sevillanas with me and it was still as exciting as ever. I was excited to be invited to the party where all of these fine dancers and musicians of the Greco Company were the guests of honor. After seeing these professionals in concert a few hours before, I had been left in awe. Each artist was great and exciting, and getting to meet Teresa and Juanele Maya, who were real standouts in the concert, was especially exciting to me, for they were the best flamenco dancers that I had ever seen. After every one had eaten and had a few drinks, one of the guitarists took out his guitar and, in that beautiful late afternoon in my friends garden, he began to play, to play sevillanas. Some of the dancers got up and What is sevillanas; why is it a very special dance in Spain and how does it fit into the world of flamenco? First of all, sevillanas have gone through a lengthy evolution to arrive at what they are today -- a dance, song and music that not only captures a flamenco mood, the total aire and gracia of all Andalucía, but the hearts and feelings of Spain and all who experience Spain. Sevillanas today are heard with almost any type of musical arrangement, from jazzy renditions with organs, flutes and orchestras of all sorts, to the tapping of a stick, to the solo singing of the coplas. All of this originated from a more classical form known as seguidillas, seguidillas sevillanas and forms of the seguidillas boleras which",
    "title": "MORCA: SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 969,
    "article_char_count_full": 5348,
    "article_char_count_review": 5348,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
