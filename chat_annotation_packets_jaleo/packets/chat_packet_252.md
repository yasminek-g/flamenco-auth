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
    "article_id": "JALEO_1987_SUMMER::A8",
    "article_text_for_review": "Tío Paco With the death of two of the greatest exponents of the purest of \"El Cante\", Tia Juana la del Pipa and Tia Anica la Piriñaca occurring within days of each other, a Golden Chapter in history of Flamenco comes to a close. The following is a translation of an article by A. Alvarez Caballero which appeared in El País (Madrid) on Friday November 6, 1987. TIA ANICA LA PIRINACA DIES AT 88. Like the gypsies, she believed that good singing comes from sorrow. Ana Blanco Soto, better known by her stage name, Tía Anica la Piriñaca, was buried yesterday afternoon in the Jerez cemetery after dying at the age of eighty-eight in her home in the Barrio Santiago, where she had been moved at her own request from the hospital where she was being treated. Tía Anica was a legend in the world of flamenco; her best years were in the 1950's when she was backed by Antonio Mairena. She excelled in her ability to interpret the difficult \"palos\" in flamenco, the siguirias, the bulerías, and the bulerías of Jerez \"al golpe\". Tía Anica la Piriñaca is dead. She was without doubt the oldest voice we had left in el cante. She had just turned eighty-eight on April 11. Just a few days before, Tía Juana la del Pipa, died too. Now without them, the cante and the dance of Jerez will never be the same. I saw them together for the last time in Sevilla in September of 1984, and the image of Tía Anica balancing herself on her cane to do a few steps \"por bulerías\" is etched in my mind. Ana Blanco Soto was not a gypsy -- only one eighth to be precise-- but she looked it. She spent her entire life among gypsies in the Santiago Quarter and married a gypsy $ ^{1} $. Her cante was pure gypsy. When she sang she would hold a handkerchief in her left hand, with which she would wipe her lips every now and then. It was a gesture which reminded me of Louis Armstrong when he played the trumpet. Armstrong at times would draw a bloodstained handkerchief away from his lips. Tía Anica la Piriñaca said once to Caballero Bonald: \"When I sing well my mouth tastes of blood\". She believed, as did Juan Talegas and many other geniuses of \"el jondo\", that good cante is the one that hurts, the one that comes from sorrow. She learned the cante as a little girl, in the fields, when everyone was together and there was always someone who sang, and from the great Jerez masters, Manuel Torre, Antonio Frjones, and above all, from Tio José el de la Paula. It can be said without doubt, that in the history of the cante there has been no woman's voice that could express and transmit better the gypsy siguiriya. \"I sing por siguiriyas\", she said to Manolo Herrera, \"because I hurt at my husband being gone, and because I hurt from the troubles my children have caused me; I have hurt because of all those things and I've sung always remembering those sorrows. And there are words that really make me cry. And I have to get hold of myself because tears run down my face from what comes into my heart, the despair that possesses me; because it is true, because there are words that reach that cross I carry in my heart, or that thing I had within me. And I've come out singing, and I've come out crying\". \"Mother and father of tears, the poet called her, because there can be no sadder way to sing than the way that woman sang por martinetes, por siguirias, por soleares. Even bulerías, being mostly party-like airs, had in her voice a well of sorrow. Caballero Donald told of seeing her many times in the streets of Jerez, in some out-of-the-way bar, almost begging for a few coins in exchange for a cante. And he added: \"She could have been an incomparable source of teachings and rarely has she been considered what she really was: an ignored and magnificent specimen of human truth and drama in el cante\". Obscure genius, elementary, but with that rare intuition for \"el arte\" of those few privileged beings that are born with the gift denied to most mortals. Because, as she said, \"that runs in your blood\". Without Tía Anica la Piriñaca, the cante of Jerez will be poorer, because many others went her way. Unique beings, irreplaceable, who contributed so much towards making flamenco an enigmatic and fascinating art, besides being truly exclusive of the Spanish South. Without going very far into the past, I think of those of recent menory, los Teremoto², los Borrico³, Los Sernita⁴ los Parrilla⁵..., and that Tía Juana la del Pipa, who with just one round por bulerías could make all hearts stand up. There will never be another Tía Anica la Pirañaca, because flamenco art is not what it used to be, and beings like her disappear without leaving possible successors. We are left with her memories, her voice in the microgroove, the rare echo of her cante, which really, seems to have a taste of blood. (1) Reportedly she did not begin to sing in earnest until she was well into her sixties or seventies and following the death of her husband, who apparently did not approve of her singing. (2) Fernando Fernandez Monje, \"Terremoto\", died in Jerez in 1981, age forty-seven. His sister, María Soleá, currently considered foremost exponent of \"cante puro de Jerez\". (3) Gregorio Manuel Fernandez, \"El Tio Borrico\", died in Jerez in Tia Anita la Piniñaca in front with cane. In background is Tia Juana la del Pipa. (See Jaleo Volume VI-2.) photo by Pablo Julia",
    "title": "PASSING OF AN ERA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 966,
    "article_char_count_full": 5339,
    "article_char_count_review": 5339,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SUMMER::A9",
    "article_text_for_review": "by George Ryss It is 11 a.m. at St. Malachy's, The Actors Chapel on West 49th Street in New York City. The church is filled to capacity with aficionados, bailadores, and friends; those that danced with him, knew him, enjoyed his company...a priest speaks about Orlando Romero, his art, his work...the memorial has begun with the haunting tones of the saeta (a palo seco)... the music reverberates in the dome of the church the pierced saeta of death in the name of Orlando Romero...The Saetero our own respected Domenico Caro...\"Ay, el divino rey Orlando Romero quiere verte.\" Next to the cantaor are seated three guitarists...three guitars each playing their solo laments to Orlando, who so frequently had danced to the guitar. We hear Basilio Georges \"Por Siguiriyas\"...Carlos Rubio (all the way from Philadelphia) \"Por Soleares\". and them the classic guitar of Scott Jackson Wiley in the beautiful rendition of Albeniz' \"Layenda.\" The audience heard the priest in a prayer; then came the bailaores TIA ANICA (continued from facing page): (4) Manuel Fernandez Moreno, \"Sernita de Jerez\", died in Madrid in 1971, age fifty. (5) Outstanding flamenco family in Jerez. Among them guitarists (father and son), and dancer Ana Parrilla. JALEO - VOLUME X, No. 2 Upon returning to his native Argentina, Orlando continued his studies and eventually landed a position as first dancer with Oscar Segovia Spanish Dance Company. As first dancer it was only a matter of time before he was discovered by another great in the flamenco world - guitarist Mario Escudero. They worked together for over three years. But Orlando began to feel the need to have his own company, to choreograph his own pieces, and so \"Los Duendes de España\" was formed. After a very successful debut here in New York, the company went on the road and toured North, South and Central America, the Caribbean, and Europe. *** Murder of a Well-known Flamenco Dancer Investigated [From: Clarin (Buenos Aires) Sept. 7, 1987; sent by Juan Maleoba; translated by Paco Sevilla.] Personnel of the Homicide Division of the Federal Police carried out intensive investigation yesterday into the death of Orlando Romero, 46 years old, a well-known flamenco artist whose handcuffed body was found in his apartment in the central zone of the city; there were evident signs that he had been strangled. Romero, dedicated for many years to the art of flamenco, was the head of a show called \"Andalucia Canta y Baila con Alegria,\" which is at the Olimpia theater in this city. Last night sources said that a person was being detained by the police with regard to the homicide, but that could not be confirmed. The crime was discovered by the sister of the victim, Norma, who had gone to the artpment at 465 Carlos Pellegrini, third floor. When she got no response to her repeated knocks, she called the concierge to open the door. In this way she came upon an impressive sight: In underclothes, hands tied, and with visible marks on his neck, lay her brother. The police, who came immediately, found no signs of struggle. Apparently, the furniture had not been moved; everything was in order and there were no signs of violence in the area. As we said, the police immediately began a wide investigation, under the assumption that robbery was not the motive of the murder. The news, which spread rapidly, dazed and brought forth profound grief among the relatives and close friends of the victim, who was held in great esteem. Argentine by birth, Romero had triumphed not only in this country, but also in Spain and the United States. \"This crime is absurd; nobody could have hated Orlando to the extreme,\" was all one of his fellow dancers could say last night. Born in the city of Rosario, Orlando Romero was attracted to flamenco dance from the time he was very young. His first studies were with the legendary Enrique el Cojo in Spain; he returned to Argentina with the wealth he had received from the great maestro. He teamed up with Alba del Rosario and they went to Uruguay, where they performed in various nightclubs, theaters, and on television. Upon returning to Buenos Aires, Romero joined the dancer, Oscar Segovia, and they took their art to the port theaters, Ovpera, Avenida, Astral, and Metropolitan, sharing the stage with José Marrone, Dringue Farias, Pepe Arias, Mario Fortuna, Miguel de Molina, Angelillo, Juan Carlos Mareco, and Pablo del Rio. After completing a period with Mario de la Vega, he went to the United States in 1966 to make his home and form his own company. He was presented in shows with such celebrities as Sammy Davis Jr., Johnny Holiday, Silvie Vartan, Diana Dors, Tony Bennett, and Mamie van Doren. He would only return to his native land to work. Slowly but surely he developed his artistic career based only on its legitimate merits.",
    "title": "ORLANDO ROMERO MEMORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 816,
    "article_char_count_full": 4816,
    "article_char_count_review": 4816,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SUMMER::A10",
    "article_text_for_review": "Roberto Lorca, Spanish dancer and founder and director of the Spanish Dance Arts Company, died of an AIDS-related illness on Tuesday at Bellevue Hospital Center. He was forty-nine years old. Born in California, Mr. Lorca received his early dance training from Antonio and Luisa Triana in Los Angeles. At sixteen, he became a soloist with the José Greco Dance Company. He danced with the companies of Maria Alba, Ximenez-Vargas, Luisa Triana, José Molina and Alberto Lorca, and performed as the partner of Maria Benitez. He also appeared in the original Broadway production of \"Flower Drum Song: and Josephine baker's 1971 show at the palace Theater. Mr. Lorca served as the director of the School of Spanish Dance at Harkness House from 1973 to 1977. Up to the time of his death he directed and choreographed for the Spanish Dance Arts Company in New York City, which he founded in 1983. *** MEMORIAL SERVICE FOR ROBERTO by George Ryss Probably one of the USA's greatest male dancers died on October 13, 1987 at ten minutes to five. His own company, Spanish Dance Arts Company was at the time rehearsing for their opening on October 15th at the Symphony space Theatre in New York City. Among the out-of-town people who came to see the performances - a living memorial to a great dancer -- were Dame Libby Komaiko Fleming for whom Bobby had danced, taught, and done the choreography for Dame Libby Fleming's Ensemble Español in Chicago. Attending the memorial service at the church was Luisita Sevilla Pacheco from Miami, who remembered Lorca as a dance partner some twenty-five years ago. The actual memorial service in the church was well attended by flamenco aficionados, professionals and simply the many friends that he had. Proceedings included singing of a saeta by the cantaor Domenico Caro. A priest eulogized the deceased. The siguriyas, as guitar solo, was played by Basilio Georges, the cantaora, La Conja, rendered beautiful flamenco numbers and was joined by Domenico Caro and the guitarist \"por fandangos.\" Individual church eulogies to Roberto Lorca were given by Luisita Sevilla (of Miami) and the dancers Liliana Morales and Carlota Santana who so successfully presented Spanish Dance Arts Company in New York City only days after his death; indeed the company had performed \"To the Memory of Roberto Lorca.\" A reception was held after the conclusion of the church service in a nearby Mexican restaurant, as Carlota Santana explained \"the way Bobby would have wanted it.\" As a final note: Bobby's favorite song was \"Send in the clowns\" from the Broadway musical — Domenico, and many other of the flamencos had often visited Bobby in the hospital — Domenico was going to sing \"Send in the Clowns\" for Bobby at his next hospital visit, namely October 14 but Bobby died on the 13th (and Domenico was not notified)...Domenico sang Bobby Lorca' favorite song in church that day... *** DANCE BY ROBERTO LORCA: MORE THAN FLAMENCO [from The New York Times, Oct. 18, 1987] by Jennifer Dunning There is theater that stands out for its artistic vision and for the pleasure of the performers' company. The Spanish Dance Arts Company offered such theater on Thursday at the Symphony Space, in an evening of music and dance that was like sharing a glass of wine with old friends. There have been better and worse explorations of flamenco and classical Spanish dance. Roberto Lorca, who founded the company in 1983 and taught Spanish dance at Harkness House, created dances and programs here beyond the usual offerings. There was a revelatory offering of music by Alberto Ginastera, played luminously and with wit by the Alborada Latina Chamber Ensemble. And the choreography by Mr. Lorca and Luis Montero suggested more than passing knowledge of 20th-century ballet and modern dance. Mr. Lorca's \"Luz y Sombra,\" set to a score by Manolo Sanlúcar and traditional music, hauntingly explores the light and dark side of love. In the first of two duets, La Meira and Manolo Rivera are young lovers with unclouded horizons, eager and inseparable in their joyous, powerful union. A man facing death is seduced by a deathlike lover in the second duet, whose characters were etched in acid by Pablo Rodarte and Carlota Santana. Their honed and passionate theatricality offered a perfect counterpoint to the purity and invention of the dancing of Mr. Rivera and the lovely La Meira. Mr. Montero's new \"Andaluz-Asturias,\" performed to music by Manuel de Falla and Isaac Albénez, is a lyrical, almost dreamlike excursion into Spanish classical dance that is notable not just for its crisply articulated footwork and abrupt changes in pace and direction but for a flowing continuity one associates with ballet. It was danced by Miss Santana, Mr. Rivera, Mr. Rodarte and La Meira to a score performed by the ensemble with the guitarists Basillio Georges and Rafael Aragón. The program was completed by \"Zapateados,\" choreographed by Mr. Lorca with Mr. Rivera, and a traditionally fiery and discursive flamenco cuadro. The evening was an infectiously joyful celebration of music and dance and of Mr. Lorca, who died on Tuesday. The company also included the singer La Conja, Aurora Reyes and the delightful Maria Constancia. The Spanish Dance Arts Company performs through tonight at symphony Space, Broadway and 95th Street.",
    "title": "ROBERTO LORCA DIES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "poem",
    "pages": "19",
    "page_number": 19,
    "word_count": 878,
    "article_char_count_full": 5313,
    "article_char_count_review": 5313,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SUMMER::A11",
    "article_text_for_review": "[The following is a long promised biography and photos of the famed dancer/choreographer/guitarist Juan Martinez who died of cancer in 1961. The article is from an unidentified newspaper.] Juan Martinez, dancer, choreographer and an authority on Spanish and classical ballet, died Monday of cancer in St. Vincent's Hospital. He was sixty-five years old. Mr. Martinez, who also taught private dance classes in New York, lived at 315 West Fifty-fourth Street. His most recent project was choreography done last summer for the Spanish dance company for Mariano Parra. A Castillian by birth, Mr. Martinez made his dance debut with his family in Portugal in 1902. A tour of Europe and the Middle East took the family to Russia in 1917. In Russia, Mr. Martinez was given a gold medal by the czar's sister. He was then arrested by the Bolsheviks. Until 1922 he was forced to dance in such unlikely places as box cars and trucks. He was also conscripted for a time as a policemen. Afterward, he was engaged by the Paris Opéra as a choreographer and did two ballets for Argentina, the dancer. In 1929, Mr. Martinez formed a dance company called Ballet Espagnol de Juan Martinez. The company set off on a tour of Europe, Latin America and the United States. It was during this tour that he received his second decoration, his one from King Albert of Belgium. Juan and Antoinita Martinez JALEO - VOLUME X, No. 2 Juan and Antonita Martinez COLECTORS GUITARS FOR SALE SANTOS HERNANDEZ 1931 OWNED BY CELEBRATED GUITARIST RAMON MONTOYA AFTER RAMON GARCIA ENRIQUE GARCIA 1906 OWNED BY F. TARREGA AFTER COTTIN (PARIS) AND ANDRE VERDIER GOOD CONDITION · WITH CASES R. Dussart. Rue des Jonquilles 51, 7384 Onnezles, Belgium MODERNFLAMENCO-RECORDS-VOL MODERN FLAMENCO RECORDS VOL II is now available. It is all new, more complete, reviews records in greater detail, and includes many more records than Vol I. The $8.00 price includes future updates and supplements. Vol I is still available for $4.00. Send for free information on videos, books, and \"The Living Flamenco Anthology.\" PACO SEVILLA BOX 4706 SAN DIEGO, CA 92104",
    "title": "JUAN MARTINEZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "20",
    "page_number": 20,
    "word_count": 351,
    "article_char_count_full": 2104,
    "article_char_count_review": 2104,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SUMMER::A12",
    "article_text_for_review": "by Nanette Hogan STUDYING IN SEVILLA After Madrid, Sevilla is the second biggest center for the study Spanish dance. If you're looking to study jota, this is not the place to come, for Andalucía is the land of flamenco, and Sevilla is its capital. There is one major difference between studying in Madrid and studying in Sevilla. In Sevilla, you study with one teacher only. It is apparently considered the height of disloyalty to divide your time between two teachers. You will be dropped if one finds out about the other. This doesn't seem to be the case in Madrid, where no one notices from whom you take, or whether you study with more than one teacher at a time. In Madrid, then, you can study with a number of teachers if you so desire. In Sevilla, you would be wise to choose your teacher carefully and plan on sticking with him or her. SEVILLANAS There seem to be entire dancing schools devoted to teaching nothing but sevillanas, the traditional set of dances of Sevilla. There is even such a thing as a Teaching Certificate to qualify one as a teacher of sevillanas. In Spring, before the Feria, the dancing schools are full of six and seven years olds (urged on by proud Spanish mamas), who begin very early to learn this famous set of dances. In the last year or so, the sevillanas dances have become all the rage in the discos all over Spain. You would do well to learn them -- in the night clubs of Sevilla, everyone knows them and does them over and over, all night long! New albums of just sevillanas music are put out each year by popular groups in Sevilla. In 1986, there were about forty new sevillanas albums available for sale. (They can be ordered from Ann Fitqgerald's catalogue -- see below). JALEO - VOLUME X, No. 2 Maya Fajardo c/ Pie Mallol 14. Tel. 41-52-09. (Flamenco) Menjibar de la Cruz, c/a. San Gariel Bl 23. Tel 33-55-92. (Flamenco -- in Huelva c/ Nicola Orta) Rios Amaya c/ Castellar 29. Tel. 38-31-72. (Flamenco) Vilches Ciscares. c/Virgen de la Cancalación. Ciscares, c/Virgen de la Consolacion 24. Tel. 86-13-04, Utrera, (Flamenco) Renshaw Gonzalez, c/p. Damien 1, Tel. 45-77-73. (Flamenco, baile español clásico) Alonso Pavón, c/ Manuel Arcillano, 20, Bda. Sta. Ana, (en la Guorderla). Tel. 34-16-87. Sevillanas Classes: No. 5 Placentines. Al Lado de la Giralda. Tel. 22-55-27. OTHER RESOURCES IN SEVILLA FLAMENCO MAGAZINES Puerta de Sevilla, a weekly magazine, often has articles of interest to aficionados, as well as listings of current events. On sale at newsstands around Sevilla. A man associated with the Institute of Flamencology in Jerez gave me these magazine titles and contact people, but alas, no addresses, and I did not see them for sale at the time I was visiting Sevilla. Manuel Herrera, \"Sevilla Flamenca\" Los Palacios, Sevilla Emillo Jimenez Diaz, \"El Correo de Andalucía,\" Sevilla Tours I received a flyer from Lorins Tours, 2332 Cedar, Berkeley, CA 94708, Tel (415) 845-8325, last winter, advertising a ten day tour to Sevilla and the Feria, led by a dancer. The land price was $1295, and included juergas, visits to dance studios and the Feria, hotels, dinner and breakfast. I don't know who the dancer might be, but if you can afford it, and assuming it happens again next year, it sounds like a great introduction to Sevilla. Tablaos El Arenal, Rodo 7, Tel. 21-64-92, 10 p.m. to 1:30 a.m. La Trocha, Rondo de Capuchinos, 23. Tel. 35-50-28 35-12-72. 11p.m. to 3 a.m. Los Gallos. Plaza de Santa Cruz, Tel. 21-69-81. In the Barrio Santa Cruz. 9:30 p.m. to 1:30 a.m. Patio Sevillana. Paseo de Cnlon, 11. Tel 21-41-20. 9:30 p.m. to 2 a.m. THE FERIA, FESTIVALS The famous Feria in Sevilla (covered) in detail in a well-written article by Carol Bangs in Jaleo Vol IX-4), takes place a week or two after Ester Sunday and continues for a week. Refer to the above article for further information. Be advised that no dance classes take place during this time, and it is very difficult to find accommodations anywhere near Sevilla during Holy Week and the Feria; make your plans well in advance. During the entire month of September, in even-numbered years (1986, 1988, 1990, etc.), Sevilla hosts a tremendous festival of Flamenco. The theme of this biennial festival (\"La Bienal\" in Spanish), rotates from dance to guitar to singing each time the festival is held. The next one, in September 1988 will focus on dance. There are major concerts nearly every night. Tickets are relatively cheap and you can buy a pass for the entire festival. Video cameras are no longer tolerated, although audin taping and flash cameras didn't seem to upset anyone. In addition, some of the dance teachers offer special short courses in dance during the festival to accommodate visiting dancers. In 1986 both Matilde Coral and Manolo Marín offered short courses. Next bienal, there may be more, since the focus of the festival is the baile. There doesn't seem to be any way of finding out much about this festival in advance. You just have to show up and keep asking questions.",
    "title": "DANCE STUDENTS' CORNER",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 866,
    "article_char_count_full": 5011,
    "article_char_count_review": 5011,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
