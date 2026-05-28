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
    "article_id": "JALEO_1980_05::A12",
    "article_text_for_review": "Thanks to New York flamencos, Gene St. Louis, Roberto Reyes, and La Vikinga, Jaleo has embarked on a cooperative effort with International Book and Record Distributors (IBR). Jaleo will be helping IBR to select records for importation and to promote them in this country. The hoped for end result will be the availability of Spanish flamenco records in this country. To help this plan succeed, there are several things that aficionados can do: First, we must buy records; if we want the records to be available, then we must demonstrate it by purchasing them. The second thing that will help is for readers to send us their opinions of the records they buy. That will help others to know which records they wish to buy. Thirdly, we need to encourage record stores to stock flamenco records; speak to your local store owners and let them know they will be able to sell a certain number of records. The address to contact is: International Book & Record Dist. 40-11 24th St. Long Island City, New York 11101 Here are the stores currently stocking these records: CALIFORNIA: Tower Records in San Francisco Sacramento, Los Angeles, Anaheim, West Covina, Cambel, and San Diego. Also, Bernard H. Hamel (Spanish books and records) in Los Angeles. CHICAGO: $ \\underline{\\text{Rose Records}} $, $ \\underline{\\text{Sounds Good}} $. C. Song: Rose Records, Sounds Good. CAMBRIDGE, MASS: Harvard Co-op, Discount Records, Strawberries. MIAMI, FLORIDA: Capital, Hi-Fi, Spec's NEW YORK CITY: King Karol, Sam Goody WASH. D.C.: Record & Tape Ltd., Serenade Record Shop, Disc Shop The first list of records has come in -- all Hispavox label and many of which have been in the stores for some time now. There are many records that are not flamenco, so the buyer must be careful. Many of these records are unknown to us so we can only give some general indications of content here. Also, we are not certain how many of these records will be available at each store: Sevillanas (especially valuable for dance teachers and singers looking for new coplas): \"Sevillanas de Oro\" (HH 10381) Hermanos Reyes, Hermanos Toronjo \"Sevillanas de Oro\" V.2 (HHS 10413) Romeros de la Puebla, Marismenos \"Sevillanas de Oro, V.3 (HHS 10426) Romeros de la Puebla, Los Duendes \"Sevillanas de Oro, V.4 (HHS 10437) Marismeños, Hermanos Reyes \"Sevillanas de Oro, V.5 (HHS 10446) El Pali, Amigos de Gines, Marismeno \"Sevillanas de Oro, V.6\" (HHS 10466) Romeros de la Puebla, El Pali \"Sevillanas de Oro, V.7\" (HHS 10475) Romeros de la Puebla, El Pali \"Amigos de Gines, De la Feria al Rocío\" (HHS 10439) \"Amigos de Gines, Nuevo Amanecer\" (HHS 10469) \"Los Marismeños, Fandangos, sevillanas, rumbas\" (HH 10351) \"Los Marismãos, Fandangos, sevillanas, rumbas\" (HHS 10444) \"Los Marismãos, Lo Mejor\" (HHS 10452) \"Los Marismãos, Nuestra Andalucía\" (HHS 10471) \"Los del Rio, Los Choqueros, Sevillanas Mana a Mano\" (0064) Recommended for lovers of traditional cante: ✓ \"Manolo Caracol, Una Historia del Cante Flamenco\" (0034) ✓ \"Canta Jerez, Terremoto, El Borrico, Diamante Negro, El Sordera\" (0050) \"Enrique Morente, Homenaje a Antonio Chacón\" (181380/1) ✓ \"Enrique Morente, Cantes Antiguos del Flamenco\" (S 20047) L \"Enrique Morente, Homenaje Flamenco a Miguel Hernández\" (181251) Guitar Solo: \"Sabicas, Flamenco Virtuoso\" (HX 00003) \"Victor Monje, Serranito, Virtuosismo Flamenco\" (S 20047) \"Antología de la Guitarra Flamenca, Ramón Montoya, Manuel Cano, Serranito (HH 10326)\" (Bienal - ) ously considered by many to be something belonging to savages, drunks, and lazy good-for-nothings. In the wake of the writers of '98, the voice of the poets has been the only defender of the dignity of the cante and has proclaimed its importance. It is precisely this voice, and that of many faithful aficionados, that have secured a higher position for the \"lamentos y alegrías\" of the artists -- who are admired more and more all the time and valued for their rightful dimension as creators. The strength of this artistic expression has been so imposing in its inspiration and presence that it has crossed its own borders and infected other forms and artistic disciplines. The \"I Bienal de Arte Flamenco -- Ciudad de Sevilla\" attempts to bring together an exhibit of all these manifestations, to join them in a single competition so that we can make a reliable check of the generative capacity of flamenco. During the two weeks, on different stages, Sevillans had the opportunity to see the voice of flamenco reflected in movies, the plastic arts, theater, and literature. The session began on April 6, 1980 with an opening speech by the poet from Granada, Luis Rosales, and a concert by the Orquesta Bética Filarmónica de Sevilla. That same day, doors opened on exhibitions of painting sculpture, ceramics and photography -- the first exhibitions of such magnitude in Sevilla. In the afternoon, Mario Maya presented his dance theater. Toward the end of the week, the movies -- from the old films of the 1940's to the television serials to the shorts and documentaries. From Monday the 14th until the closing of the \"Bienal\" there was an exhibit of flamenco records and books, including all of the most recent editions. A special book of the best photos and letras was published, along with the essays and speeches delivered in homage to Antonio Mirena in another edition. There was a contest, \"Giraldillo del Cante,\" in which six of today's most important flamenco artists (not named in the article) each sang twelve different styles of cante in a rigorous search for the most complete cantaor, the one capable of interpreting the widest variety of flamenco forms. It was a contest in which all of the participants were winners, since they were selected by all of the flamenco peñas in Spain. The \"I Bienal del Arte Flamenco -- Ciudad de Sevilla\" was an effort to universalize even more, and more profoundly, the image and presence of flamenco.",
    "title": "RECORDS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 968,
    "article_char_count_full": 5883,
    "article_char_count_review": 5883,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_05::A13",
    "article_text_for_review": "By Adela El Mesón Español is a charming, quaint restaurant in the heart of \"Little Havana\" in Miami, a cozy spot with all the flavor of Spain. The customers are made to feel at home by the very friendly proprietor, Agustín Fernández, who likes to mingle with the patrons and pass the wine bota to all. The singer of the show, Arturo de Ronda, keeps everybody in good spirits with his spontaneous sense of humor. Arturo speaks with an unmistakable Andalusian accent, so when he explains he was born in South America it comes as a surprise. Formerly a dancer, he is today mainly a singer of Spanish songs, from flamenco to all the regional folk music. And when it comes to reciting García Lorca he is a master. The guitarist is Pepe Menéndez from Madrid, also a former dancer. He has been involved with flamenco all of his life and has a long background as a guitarist and accompanist for different troupes. Having lived in Cuba for several years, he really absorbed that rumba technique and can play with it delightfully. The shows, with dancer Adela, range from flamenco grande to chico with highlights of",
    "title": "MIAMI FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "poem",
    "pages": "24",
    "page_number": 24,
    "word_count": 196,
    "article_char_count_full": 1105,
    "article_char_count_review": 1105,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_05::A14",
    "article_text_for_review": "MARCH JUERGA by Stphanie Levin The first two hours of the March juerga did not look too promising. There were perhaps a dozen people milling around wondering if a juerga were going to take place. A few of us began to think that the juerga was going to flop before it had a chance to conviene. Lack of people proved to be the least of our problems on this cold chilly night. Held at the National University Cottage, the water lines had broken the day before the juerga. In true bureaucratic tradition someone had neglected to inform the Jaleistas of this event. We decided that the lack of water was not a good enough reason to discontinue our juerga. Around 10:30 p.m. people began appearing at the door and by 11:00 our juerga was complete with around 50 members. It was small and delightful, intimate and full of guitarists. The ambiente was ripe and the juerga proceeded in gaiety and fun. Juana de Alva, Magdalena Cardoso; a guest from Mexicali and Julia and her daughter gaily danced to the guitars of Yuris Zeltins, Miguel Ochoa, Joe Kinney, Roberto Vásquez and Herb Goularian. Someone built a cozy fire in the fireplace which added to the warm glow of the room. By the end of the evening it seemed everyone had participated in the juerga through dancing, singing, guitar playing and spirit. I think everyone that attended the March juerga would agree that it was perhaps the smallest juerga Jaleistas has put on, but in many ways the most delightful. April Juerga by Brad Blanchard The April juerga took place in surroundings familiar to many Jaleistas, the National University Alumni house. By 9:00, the house was fairly full but the atmosphere was all too quiet for a juerga; people were quietly conversing and concentrating on the tapas and fairly good selection of tortillas Españolas. Then suddenly someone was playing fandangos de Huelva, Juanita Franco was tempestuously beating out their rhythm on the tablao in the central room, and everything had changed into a fast paced evening of flamenco that didn't let up until 2:00 in the morning. Rodrigo, Remedio and María José arrived and performed a few fast rounds of bulerías; throughout the night Alvaro, Julia Romero and María Clara could be found in one part or another dancing sevillanas, fandangos de Huelva and at one point, a long set of guajiras; Juanita Franco followed María Solea in dancing por siguiriya later in the evening as María Jose sang. In between songs, Benito rounded up all of the rumba aficionados and treated us with rumbas, and throughout the night Juana de Alva could be found dancing and/or singing por alegrías. At one point we were treated to a set of sevillanas danced by Marvila and Marina Madrid. One good thing about the juerga was that the aficionados who are learning the baile were not shyed away by the ability of those who have studied the art longer. Those who danced sevillanas and rumbas the best they could, received the encouragement they deserved and helped give a good ambiente to the juerga. Things stopped early -- at about 2:00 a.m. -- but those who attended agreed that it was a success. Repeat performance at the National U. Alumni cottage. \"CUADRO A\" not \"CUADRO D\" will host this juerga. Cuadro A members are: (leader) Juanita Franco, Juanne Zvetina, Tony & Alba Pickslay, Antonio & Elda Delgado, Nina Yguerabide, Ruben Varteressean, Thor & Peggy Hanson, Bernardo & Chela Gres, Gene & Pilar Coates, Bianca Almanza, Francisco & Elizabeth Ballardo, Amparo Oliva, Marilyn Bishop, Jose Roldan, Robert & Hazel Lent, Regla & Vincent Dee. to offer your assistance call Juanita at 481-6269 or Elizabeth Ballardo at 454-4086. DATES: May 17 PLACE: National University Alumni Cottage 4141 Camino del Rio South TEO MORCA FLAMENCO WORKSHOP Teo Morca will be presenting his flamenco workshop again this year, from August 18th to the 30th. There will be a morning technique class and an afternoon repertory class at both the beginning and intermediate-advanced levels. The fee is $225 for the two week session, with a $25 deposit due by July 31st (the deposit is not refundable after the 31st). Write to: Morca Academy 1349 Franklin Bellingham, Washington 98225 Phone: (206) 676-1864",
    "title": "JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_05",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 711,
    "article_char_count_full": 4185,
    "article_char_count_review": 4185,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A1",
    "article_text_for_review": "By Guillermo Salazar Cândida Gilotti was born March 24th, 1953 in North Adams, Massachusetts. She is of Italian heritage. At age 16 she began singing rock, soul, and blues. She would sing along with records of Aretha Franklin, Janis Joplin, and Grace Slick. Later she branched off into her own material in what she calls \"new age rock\". While still living in the East, Cândida became lead singer for the rock group, \"Brandywine\", and spent two years, 1973-4, touring in the Northeast. In 1976 Cândida moved to Denver, where she heard flamenco cante for the first time. The following short interview will introduce Cândida to the readers of Jaleo: GUILLERMO: What interested you in flamenco? CÁNDIDA: I was aware of flamenco dancing, but not cante. I never imagined that I would sing it. In 1976 I heard an album by Lole y Manuel. I was stunned by Lole's voice. GUILLERMO: When you first heard it, did you think you could do it? CÁNDIDA: It sounded impossible at the time. GUILLERMO: What was your technique for learning the cante? CÁNDIDA: I learned Lole's material from her records. I wrote the words out phonetically and sang along with the records line by line. Around the same time I learned a sevillanas from Simón Serrano. GUILLERMO: Your diction sounds excellent. Do you speak Spanish? CÁNDIDA: I am learning Spanish and had a lot of help with Andaluz pronunciation from several flamencos. GUILLERMO: At this point, what are some of the cantes in your repertoire? CÁNDIDA: Solea, peteneras, bamberas, tangos, alegrías, bulerías, and sevillanas. My favorite is bulerías, and I have seven or eight bulerías in my repertoire at the moment. These are by Lole and Camarón de la Isla. GUILLERMO: Whose material is the peteneras and bamberas? CÁNDIDA: I sing a peteneras by Porrinas de Badajoz and the bamberas, of course, is Niña de los Peines. Actually the letra of the bamberas is Juanito Villar's and is done in a soleares rhythm. GUILLERMO: What new cantes are you learning? CÁNDIDA: Seguiriyas is next on my list, maybe fandangos de Huelva. Also I'd like to learn more jondo bulerías and soleares. I want to get more unto the nucleus of flamenco, since I still feel on the outside looking in. There's still plenty of work ahead. GUILLERMO: Have you been to Spain? CÁNDIDA: Last spring I visited Sevilla, Ronda, Morón, and Malaga. There I got plenty of inspiration. I went to the Arenal in Sevilla and was impressed by a woman named Rocío, who sang and danced simultaneously. The highlights of the trip were a performance in Morón by Juan Peña \"El Lebrijano\", and the following night by Joselero and Juan del Gastor. GUILLERMO: Where have you sung flamenco? CÁNDIDA: I started out singing at Lawrence Phipp's flamenco parties for fun. I have performed with Vicente Romero and Lydea Torea, and with a trio called \"Grupo Paella\". More recently I sang in concert with you at the First Unitarian Church in Denver.",
    "title": "CANTAORA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 498,
    "article_char_count_full": 2914,
    "article_char_count_review": 2914,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_07::A2",
    "article_text_for_review": "Dear Jaleo, Your articles are all wonderful and my life line to my passion for Spanish dance and flamenco. Particularly I enjoyed the piece on José Greco in your latest issue. It reminded me of my brief meetings with the great man. I was leaving Málaga airport on a flight to the U.S. via Madrid. In the preflight lounge I spotted a gapper gentleman with a handkerchief around his neck. It was my hero José Greco. As a novice student of classic Spanish dance I was a bit shy about approaching him, but he was most pleasant. He introduced me to his guitarist who was travelling with him and suggested we do a few pasos in the concourse of the Madrid airport during our layover. Unfortunately, life has played one of its foul tricks on me. The day before departure I had bought a new pair of boots, trying on only the right one. The day of the trip I discovered to my horror that BOTH boots were right ones and I was crippled for the entire trip! I couldn't walk, much less DANCE! Mr. Greco met his wife in Madrid and I didn't see him until we landed in New York. There, he came up to me and gave me a postcard with his address and phone number in Nueva Andalucía, inviting me to come out to his school there. I was overwhelmed!",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_07",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 234,
    "article_char_count_full": 1226,
    "article_char_count_review": 1226,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
