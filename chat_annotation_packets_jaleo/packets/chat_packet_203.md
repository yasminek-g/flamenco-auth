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
    "article_id": "JALEO_1985_01::A10",
    "article_text_for_review": "by Guillermo Salazar In Triana, right across the bridge from Sevilla, a school for flamenco guitarists will open under the direction of Mario Escudero and his \"profesor adjunto\" Sami Martin. The following interview was taken in December of 1984 for Jaleo. Jaleo: By what date will the school open? Escudero: Bueno, we don't have a set date, but I don't think it will be too long, because the remodeling of the location is on the verge of being finished. So I think we'll begin around the first part of January. J: You mentioned before that it will be a school, not just in the sense of private lessons. In what way? E: First of all, naturally, we're going to start by forming different grades, from the \"principiante o primario\" through the \"intermedio\", \"avanzado\" and \"profesional\". Then, we are going to teach about the correct way, the most possibly adequate way, for the soloist, and for the accompanist in the two branches which are the \"cante\" and the \"baile\". J: Other than yourself will there be other teachers? E: We intend to have other teachers, but at the moment I don't know any names or how many there are going to be, because everything is going to happen as things get underway. J: Will this school be open all year round? E: We hope so. J: If students want to learn classical guitar, will it be offered? E: Maybe some fundamentals, but for the advanced classical guitarist there is already a conservatory here where there are good teachers. They can contact América, Martínez, a good friend of mine, who is the \"catedratico\" and a very good teacher. J: Here in Sevilla? E: Yes. Here naturally we're going to teach basic knowledge applicable to flamenco or classical playing. For example, we will present scales in their totality, because in my personal opinion there is only one school of guitar. Although in flamenco we do have certain techniques not used by classical guitarists, like \"rasgueos\", the use of \"pulgar\", and certain types of \"tremolos\". J: What if someone wants to come to the school for private classes? E: Certainly they may have them, but the conditions would be different. It would be more expensive for private because the attention you give in a group is not the same as you give to one person. We can give also semi-private classes of two people as well as the group and private classes. J: Could you tell us a little about your assistant professor Sami Martin? E: Bueno, Sami has been a professional flamenco guitarist for many many years. He has a perfect knowledge of what he interprets, and for me he is a professional to the fullest extent of the word. J: So, other teachers may be invited to teach occasionally? E: I hope so, there will be possibly another \"professor adjunto\" who will teach sporadically, maybe for a season, but we haven't studied this matter to any depth yet. Could we say that up to the moment we don't have a phone installed at the studio because we haven't finished remodeling. But at the moment anyone interested can call my \"compañero\" Sami Martin at 51 50 25. If anyone wants to call me directly at home the number is 61 06 36 in Sevilla. Of course dialing long distance they would have to dial the appropriate area codes, or go through the operator. If they want to write, the address is \"Centro Triana de Guitarra Flamenca\", Calle Rodrigo de Triana, 46 esquina Victoria, 20, Sevilla, España. J: To finish this interview, Mario, is there anything you would like to say to the many \"aficionados\" in the United States, and other parts of the world where $ \\underline{\\text{Jaleo}} $ is",
    "title": "\"CENTRO 'TRIANA' DE GUITARRAFLAMENCO\" TO OPEN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "article",
    "pages": "21",
    "page_number": 21,
    "word_count": 626,
    "article_char_count_full": 3556,
    "article_char_count_review": 3556,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_01::A11",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE: Bueno, I hope that they keep in mind their \"afición\" of the flamenco guitar, this marvellous instrument; and also keep in mind that they can come here to Andalucía, in this case Sevilla, where we will do everything possible to make the classes worthwhile to all students. Finally I wish \"mucha felicidad\" to everyone, and in particular to the \"aficionado a la guitarra flamenca\". ENSENANZA GENERAL DE LA GUITARRA FLAMENÇA ENSEÑANZA GENERAL DE LA GUITARRA FLAMENCA Niveles o Grados: 1-Principiente o primaria 2-Intermedio 3-Avenzado 4-Professional CURSOS COMPLETOS: Solists Acompañamiento del cante Acompañamiento del baile MONTAJE DE NUMEROS MUSICA: SOLFEO Y CIFRA CLASES COLECTIVAS E INDIVIDUALES Apertura Curso: Enero 1985 Teléfono: 515D25 y 6iD636...Sevilla GuilleRemé HUNTING FOR FLAMENCO\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"know\"]\n\nGUITARRA FLAMENÇA ENSEÑANZA GENERAL DE LA GUITARRA FLAMENCA Niveles o Grados: 1-Principiente o primaria 2-Intermedio 3-Avenzado 4-Professional CURSOS COMPLETOS: Solists Acompañamiento del cante Acompañamiento del baile MONTAJE DE NUMEROS MUSICA: SOLFEO Y CIFRA CLASES COLECTIVAS E INDIVIDUALES Apertura Curso: Enero 1985 Teléfono: 515D25 y 6iD636...Sevilla GuilleRemé HUNTING FOR FLAMENCO RECORDS IN SPAIN Believe me, flamenco is not dead. How do I know? You've just been down to the local record shop and they told you there were not any flamenco albums except Al Dimeola's. Well obviously you live in the United States, right? You knew flamenco comes from Spain, the south, Andalucía, remember? Not New York, not California, not Mexico, not Burgos nor even Bilbao! Dut wait, isn't flamenco from Madrid too? Let's find out. Call your travel agent and book the next flight to Madrid. Tell her you want to arrive early in the morning so you can be at the door of the \"Corte Ingles\" when it opens at 9 a.m. Tell her that many of the flamencos from Andalucía have recorded albums in Madrid; tell her you want a special taxi cab with a ecof rack waiting at the Barajas airport. The cab is to escort you for a day and load all records and tapes in the roof rack, and then drive you back to the airport to catch the evening flight home. You've got to be at the office bright and early for week the next day. Duce in Madrid you go directly for the cab. You tell the drivee, \"Coete Ingles, por favor.\" He replies, \"Which one? There are foue locations.\" Anyway, this account may be slightly exaggerated but the rest of this article will deal with the most interesting record shops of Madrid, and a few in Sevilla. It may save you some shoe leather and time. If you get a hold of the Madeid yellow pages at the local library you can plan where you want to go; but Madrid is such a spread out city, and you may want to do many other activities on your trip so... below is a list of shops sure to have flamenco records: EL CORTE INGLES-This is a chain of seven-story-building department stores. I didn't find out until the end of my one month stay that the location with the most complete selection is the one on calle Princesa, a short distance from the Plaza España. This store must have a very good department manager, or maybe it has a better selection due to its proximity to the \"Ciudad Universitaria.\" Perhaps there are mo\n\n[ENDING CONTEXT]\n\nthe following series were available: LO MEJOR DEL CANTE ANTIGUD, EMI-ODEON This series was originally released on LP records by the EMI-ODEON company. Although I saw a few of this series still available in record form, the great majority of it was abundant in cassette form under two different labels which bought the rights from EMI-ODEON: Ark and Amalgama. There are more than thirty volumes featuring some of the following cantaores: Manuel Vsileja, Don Antonio Chacón, El Niño de Gloria, José Capero, Manuel Torre, El Tenazas de Morón, AngelIilo, El Cojo de Huelva, Niño de La Ruerta, Niños de\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1876,
    "article_char_count_full": 10516,
    "article_char_count_review": 4038,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "know"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_01::A12",
    "article_text_for_review": "CONTRA TIEMPO, RHYTHM'S LIFE FORCE There is nothing that wakes up a person's soul as well as good rhythm doused with an abundance of exciting, predictable and unpredictable caunter-rhythms--contra-tiempos, in flamenco terms. More people have been turned on to dance--especially flamenco dance, music and aong--while listening to \"well placed counter-rhythms. Flamenco encompasses the full range of rhythmical and musical force and has been an inspiration for countless composers throughout the world. People like Michael Jackson have taken the traditions rhythms of Latin American, Afro-Cuban and other 4/4 rhythms and, by adding a new twist to exciting counter rhythms, have created a world wide popularity explosion of their recorded music. They have said millions of records with much thanks to their rhythmic explorations. Rhythmical pulse and counter-rhythms seem endless in their variety, which indeed they are. Another great master in country point was J.S. Bach whase 300th birthday is celebrated this year. His works are more alive and vital today than when he was alive. His works have inspired every type of musician, dancer, chareagrapher and artist in their quest for musical inspiration. Flamenco is very unique in its dance in that the dancers not only have many instruments within their own bodies but have a variety of rhythms ta work with. Rhythm by itself can be boring if it is sa steady and unadorned that it has the affect of a dripping water faucet. Both visual and audible rhythms come alive with accents, dynamics, flexing pulse, s pushing and pulling effect and, of course, beautifully placed jewels of counter-rhythm. Rhythm is one of the main faundatians of all music and its variety of pulse challenge one to awaken these pulses with a variety of counter-play. Rhythm and counter-rhythm have played a most important role in man's expression and communication with nature, religion, life and art since the beginning of time. Flamenco is really a history of, and a picture of, all of mas's expressions and communications with life in all of its rhythms. Flamenco can transcend the 4/4 world which dominates much of the world's listening. Oancers just beginning their study of flamenca will discover layer upon layer of rhythmical exploration. Most of the various compas in flamenca, the various rhythmical structures in flamenco fall into the basic 4/4 rhythms and the 12 count rhythms. There are various 6/8 rhythms also and all of these unwritten forms have various base accented rhythms that give it a particular form, style, peranality, flavor and interpretation. Needless to say, an important requirement for studying flamenca dance--or any other dance or musical form for that matter--is to have a good sense of rhythm. I mean a real musicality of rhythm and a deep understanding of what the pulse of the rhythm is. All rhythm has an underlying steady pulse and this is the core of the rhythmical structure. All flamenca forms have a definite underlying pulse, no matter how strong the base accents. Keeping a very steady beat may seem very basic but that is why the finest musicians use a metronome at times so that they may develop a self control in expressing rhythms with and against a steady pulse. The pulse of a flamenco compás is like a steady heart beat underlying every counter rhythm, falseta, melody and expression and this exists no matter what the tempo or how many silences are used or how many dynamics are used. It is literally \"thst\", \"the pulse or basic rhythmical beat of that particular form.\" Any flamenca form can be tapped out in a steady beat. This is very important to understand in approaching a deeper understanding of expressing flamenco since and understanding flamenco music all-together.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "24",
    "page_number": 24,
    "word_count": 612,
    "article_char_count_full": 3752,
    "article_char_count_review": 3752,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_01::A13",
    "article_text_for_review": "The condition of flamenco in Mexico City can be described in one word - naribund. The three institutions reviewed herein present the total earthly flamenco production in Mexico. Additionally, there is a ghost tablao. It's the moat fabulous, the locals say. Some say it's located in Colonia Polanco. Some say in Calle Vetrarra. Others say somewhere else, but they are not quite sure. It is our personal belief that this magnificent tablao is located in the Twilight Zone. Three weeks of reasonably diligent investigation produced no clue to its terrestrial whereabouts. High altitude, acarce oxygen, smog, congestion, devalued currency, popular apothy, dull quotidian grind - perhaps these conditions of Aztec life combine and permeate the spirit. Perhaps they weigh upon, warp and distant nightly dreams and daily reality, producing a shimmering fantastic vision, a magnificent mirage tahlao. Or perhaps this peripstetic tablas without certain name or fixed address is the product of people too courteous to tell you that what you seek does not exist. A Mexican, when asked an address he does not know will, after all, give you incorrect directions rather than to appear unobliging. Londres 161, Local 20a Zona Rosa The first show starts more or less on time at 12 midnight. Three youthful guitarists sing Spanish popular numbers and entertain pretty well. They yield to four ladies who commence an assault on the sevillanas. Two of them tan almost dance. In due course, a reasonably good guitarist holds forth, as does a cantaor. This latter This strange palace is celebrating its fiftieth anniversary this year by attempting to present performances of inordinate excellence. And so we arrive at the subject of this endeavor; s very fine program denominated \"Aires y donaires\" featuring two very fine Spanish artists. Pilar Rioja needs no further introduction here. In our judgment, she is the finest all-around female Spanish dancer performing today. The critics of \"The New York Times\" hold her in similar high esteem. Miss Rioja's partner in this performance, Mati Mistral, is every bit as remarkable as Miss Rioja but is unknown to general American audiences. Miss Mistral sings and declares and does both superbly. Her rendition of poems by Cervantes, Garcia Lorca and Machada, among others captured the audience's attention and breath. The highlight of the entire program was her cante, recitatian and dance of \"Prisoners in Algiers\" by Cervantes. This number should be introduced into the regular flamenco repertoire and placed alongside the \"Danaa Mora.\" The lovely moorish melody was accompanied by finger cymbals and sinuous movements; the audience's appreciation was spectacular! Miss Rioja presented some of the same courtly dances she brought to the United States last year and executed them with her perpetually correct technique. She presented a guajira, and a tarantos that was received with great enthusiasm. While we salute Mias Rioja's artistry, we also award her the prize for the nadir of the concert. This came in the form of an inane pantomime called \"The Nuns\" in which she alternately draped and undraped both herself and a piano stool in a white sheet. This piece of nonsense was choreographed by name other than the great Manolo Vargas of longago and distant memory. We justifiably would expect greater things from him. The unusual accompaniment consisting of a chamber group, Indian sitara, flute, piano, guitar both flamenco and classic, as well as the participatian of the excellent flamenco singer Chiquito de Triana will give the reader some idea of the diversity of this fine program. We close this review by correcting two errors of fact. Miss Rioja is listed in the program as having had \"outstanding success in the greatest theaters of New York.\" Outstanding success, yes, but in the theater of the Repertorio Español which, with fewer than 150 seats is not considered one of New York's greatest. We wish, of course, that she would bring her art to bigger and better-known theaters. We invite her to do so and to repart the fact accurately. Miss Mistral similarly claims to have performed \"in all the important cspitals of Europe and the U.S. with enormous success.\" We congratulate her on the tremendous accomplishment in keeping this success so secret. We only know her through the medium of Spanish network television. . THE FLAMENCO'S GASTRONOMICAL GUIDE TO MEXICO CITY Bellinghausen 95 Calle Landres Despite its name this, this olace provides very fine",
    "title": "THE SHAH SPEAKITH",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 726,
    "article_char_count_full": 4494,
    "article_char_count_review": 4494,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_01::A14",
    "article_text_for_review": "THE CLASSICAL DANCES OF SPAIN COME TO THE CORNELL CAMPUS [from: The $ \\underline{\\text{Ithaca Journal}} $, May 3, 1984; sent by Michael Fisher] by Lee Scott At the age of 3, Jose Espadero was dancing on the docks of Alicante for American sailors, in exchange for Chicklets. Today, in his early 40s, he is one of Spain's best known classical dancers. He's preserving the folklore of every region and presenting it to audiences all over Europe and the United States. Artist in residence at Cornell University this semester, Espadero will give a weekend dance concerts as a grand finale to his stay in Ithaca. Three solo works choreographed by Espadero are on the program, scheduled for 8:15 p.m. Friday and Saturday and 2:30 p.m. Sunday in the Wilard Straight Theatre. Tickets are available at the Straight Box Office, 256-3421. Espadero's dancing career began even earlier than those impromptu performances on the docks. He was a child sensation at 2½, touring Alicante (on the southeastern coast of Spain) and the surrounding provinces as \"El Gran Pepito.\" JALEO - VOLUME VIII, No. 1 JOSE ESPADERO In 1965, Espadero was named to the Chair of Classical Dance at Alicante's Conservatory of Music. This was the first time classical Spanish dance had a niche of its own at the conservator. Previously, it was taught only as an adjunct to classical ballet. Espadero created a curriculum which adapted classical ballet techniques to the particular characteristics of Spanish dance - and to its three types of expression - flamenco, bolero and regional dance. Bolero, a courtly dance with origins in the 18th century, is performed in soft shoes and punctuated with castinets, he explained. Flamenco, characterized by handclapping and rhythmic toe and heel tapping, has been danced almost as long as bolero, but grew from the people, with gypsy influences from Andalucia in the south of Spain. For several years, Espadero has been traveling throughout Spain, documenting regional dances, talking to older residents who perhaps know the dances in a purer form. Some of the dances are performed by Espadero's 23-member dance company. But most are not, he said, because they are not theatrical enough to please most audiences. Classical Spanish dancing as a profession is well supported in Spain today. Since the creation of a school of national dance, young artists need not form their own companies as Espadero and Garcia were forced to do. Instead, they have a place to apprentice and teachere to coach them. During his stay in Ithaca, Espadero is living at Telluride House; paying for hospitality with occasional informal dance lessons. This is his first trip to the United States. He said he's impressed with the green panoramas of the Finger Lakes region and with the architecture of Ithaca homes. He said he loved the Maid of the Mist boat trip he took at Niagara Falls, and revealed in the atmosphere of New York City's Broadway. After seeing a production of the musical \"42nd Street,\" Espadero insisted on buying a pair of tap shoes, and plans to teach himself to tap dance by watching old Fred Astaire and Ginger Rogers movies.",
    "title": "ESPADERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "27-28",
    "page_number": 27,
    "word_count": 521,
    "article_char_count_full": 3126,
    "article_char_count_review": 3126,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
