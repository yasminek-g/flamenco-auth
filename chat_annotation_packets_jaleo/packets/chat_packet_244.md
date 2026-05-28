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
    "article_id": "JALEO_1987_SPRING::A16",
    "article_text_for_review": "FLAMENCO IN OHIO ,3rd All Flamenco Workshop in Ohio Exciting serious flamenco in Novelty, Ohio? You bet your compás there is. It is a common thought that all the flamenco action is in the big cities like New York and Los Angeles, Chicago or San Francisco. The real facts are that flamenco is found throughout the U.S. in many smaller communities and it is taken quite seriously. Libby Lubinger at the Fairmount Center for the Arts in Novelty, Ohio, which is about 40 miles east of Cleveland, has, in the last seventeen years, done a terrific job of developing a great core group of dancers who are doing Spanish and flamenco dance, music, and song. She has developed two performing groups -- The Fairmount Spanish Dancers and a junior group. Both perform in concert, recital and cultural enrichment programs. There are many local people who make this an important part of their lives. They have had classes from many fine artists, including María Alba, José Greco, Nana Lorca and have developed a well rounded program in a very fine school and setting. This was my third visit to give my \"All Flamenco Workshop\" and it proved to be the most intense, in that they were all so much more accomplished than the year before and that pushed all of us to new levels of excitement and awareness. I had a superb group of students, most of whom had taken the classes before and, this year, I had some mothers and daughters which was fun to see. Many of the students took both levels, which meant that they took two full Teo Morca and ten year old Hilary - Sevillanas Some of the interesting highlights: three of the ladies who took the workshop were from Spain and this was their introduction to flamenco. Teresa, from Malaga is so into flamenco that she is taking the workshop in Jerez de la Frontera this year. Marija Temo is coming out to Bellingham to participate in my \"All Flamenco Workshop\" in August and hopefully many will come to my winter ski-lodge workshop the week between christmas and new years. Lydia Torea - rumba served her notoriously delicious and potent sangria, and Carlos Calleros offered bottles of vino tinto from the Rioja region of Spain. The festivities were held under the stars around a portable dance floor on the patio. Because of the warm temperatures, most guests dressed very casually; for example, Lydia relaxed in white shorts and Liliana de Leon Guy Frankel singing fandangos, Luis Campos on guitar The range of interests of the participants enabled them to intersperse the traditional flamenco with other cultural contributions and styles. For example, Liliana de Leon danced a spectacularly energetic rumba that featured modern dance techniques. Also, with help from the guitarists and from Roy Jones on saxophone, Pamela Driggs sang a Brazilian bossa nova in Brazilian Portuguese. Well after midnight, the dancing, singing, and guitar playing drifted off, and participants began to leave. Some from out of town, however, stayed for the night, and many of those who stayed ended the juerga by soaking in the hosts' pool and hot tub. Phoenix area flamencos hope to host another juerga in the Lileana Ruiz - rumba JALEO - VOLUME X, No. 1 Bob Campos giving guitar workshop at Calleros home in Phoenix YOUR HOST ANTOINE HAGE FLAMENCO ENTERTAINMENT TUESDAY NIGHTS (619) 298-2010 1349 Franklin Bellingham, Washington 98225 Ph. (206) 676-1964 TEODORO MORCA IS NOW OFFERING ON VIDEO TAPE, A COMPLETE APPROACH TO STUDYING FLAMENCO DANCE, IN TECHNIQUE, INTERPRETATION REPERTOIRE AND UNDERSTANDING. WRITE OR PHONE FOR A \"MENU\" OF TAPE SELECTIONS.",
    "title": "WORKSHOPS & JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "poem",
    "pages": "40-44",
    "page_number": 40,
    "word_count": 603,
    "article_char_count_full": 3565,
    "article_char_count_review": 3565,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SPRING::A17",
    "article_text_for_review": "Las Noches de Cádiz In June of this year a theater-flamenco production was presented at the Centro Cultural de la Raza in Balboa Park by a group of local flamencos. \"Las Noches de Cádiz\" was based on the original concept and script by guitarist and Jaleo editor Paco Sevilla. Guitarist Rodrigo and singer Remedios Flores assisted in writing the script, Carla Heredia assisted in direction and all of the rest of the cast contributed ideas and were involved in different aspects of producing the work. The dances and songs of the production were woven into two typical scenes in Cádiz -- a tavern called La Pravidilla and a gypsy encampment at the outskirts of town. Each member of the cast played a character in the production representing a typical personality in the flamenco scene. Part I, in the taverna, delto primarily with non-gypsy flamenco music that is public and festive. The second part dealt with the more gypsy flamenco, characterized by complex percussion and cante jondo. (photos by Edward Reuss; copyright 1987; used by permission) Below: Moro dances por bulerías in the gypsy camp accompanied by the rhythm of Ana, Juaquina, Frasquito and El Rubio. CAST OF CHARACTERS Don Vicente (Victor Soto): A \"señorito,\" a wealthy gentleman who enjoys flamenco and dabbles in bullfighting. Doña Enriqueta (Juana Escobar): Lady friend of Don Vicente. Señora Carlota (Carla Heredia); Enriquetas\"s good friend. Maria Antonia \"La Caramba\" (Juanita Franco): A colorful figure in the nightlife of Cádiz and well known for her dancing of tangillos. Consuelito (Erika López): Daughter of María Antonia El Rubio de San Fernando (Rodrigo): Gypsy from San Fernando, a town not far from Cádiz. He comes frequently to Cádiz to earn what he can as a guitarist. Aniya la Gitana (Remedios Flores): The wife of El Rubio. Frasquito Ortega (Paco Sevilla): Senior member of the Ortega family. Although he often finds work as a guitarist, he supplements his income by working in the slaughterhouse outside of Cádiz. Soledad Ortega \"La Gamba\" (Marysol Fuentes): Member of the Ortega family. Manuel Ortega (Miguel Arnot): A member of the Gypsy Ortega family; works occasionally as a waiter in La Privadilla. Antonio Vargas \"Moro\" (Victor Soto): Head of the Vargas clan, a family of Gypsies who go from fair to fair, where Moro works as a tratante, a go-between in the selling of livestock. Manuela la de Vargas (Carla Heredia): Wife of Moro. Carmela Vargas (Tanya Rodriquez):Daughter of Manuela and Moro JALEO - VOLUME X, No. 1 Juaquina (Juana Escobar) dances siguiriyas. native region to the performance. She has appeared in concert with such important artists as Teodoro Morca and Spain's Manolo Marin. Paco Sevilla has dedicated his life of eh art of the flamenco guitar. During extensive periods in Spain, he studied and performed with some of flamenco's best know artists. In the USA and Mexico he has toured widely, performing in virtually every state and appearing in such prestigious halls as New York's Lincoln Center and Los Angeles' Dorothy Chandler Pavilion. Miguel Arnot lived for thirteen years near Puerto de Santa María (Cádiz). His mother played flamenco guitar and he was immersed in flamenco with radio, records, and home fiestas. In the United States, he became more serious and studied baile with Juanita Franco and cante on his own. During the last year, he has performed frequently at Tablao Flamenco. Erika Lopez, nine years old and of Spanish decent, has demonstrated special dance ability after only two years of study. She has already performed professionally as well as at many functions of the San Diego Casa de España. Canada, and England. Tanya Rodriquez, eight years old, acquired the Spanish temperment of her paresnts, who are from Puerto Rico and Ecuador. Like Erika, she has studied dance for two years with Juanita Franco, performed professionally and for Casa de España functions. Jennifer Villanueva, eleven years old and of Mexican-Portuguese background must have had a past life as a Gypsy, for her dance appeared spontaneously. Her only formal training has been the during the few weeks before the production of \"Las Noches de Cádiz.\" Soledad Ortega played by Marysol Fuentes. Frasquito (Paco Sevilla) sets his guitar asside to do a few steps of tango accompanied by (L to R): Aniya, El Rubio, Soledad, Luisa, Moro and Manuel. Rodrigo's guitaristry is well-known in Andalucía. Besides recording and concertizing in Spain and America, he is director of Sounds-Vision Records -- a leading distributor of flamenco in the USA. Juanita Franco began her dance training at age six in Sevilla. Her teachers included Enrique el Cojo and Maestro Realto; by are fourteen, she was a full-time professional. Now a San Diego resident, she has had her own companies, bveen a frequent guest artist, and has been artistic director and featured dancer at Tablao Flamenco for the past three years. Left: Frasquito (Paco Sevilla) and El Rubio (Rodrigo) pack up their guitars and prepare to leave at the closing of the first act. (photos by Edward Reuss; copyright 1987; used by permission.) Below, left to right: Aniya ILa Gitana (Remedios Flores) and El Rubio accompany the baile of Luisa Vargas (Juanita Franco) while Soledad (Marysol Fuentes) and Moro look on.",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "45-48",
    "page_number": 45,
    "word_count": 863,
    "article_char_count_full": 5268,
    "article_char_count_review": 5268,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A1",
    "article_text_for_review": "OR TO BE BORN WITH DUENDE [from: Dominical, May 11, 1986; sent by Brad Blanchard; translated by Paco Sevilla] by Nieves San Martin photos X.M. Albán Blanca del Rey, one of today's best flamenco dancers, gives the best she has to give every night in the tablao \"El Corral de la Morerfa.\" She has performed on stage in Russia, Japan, Italy, Argentina, Jordan, Holland, etc. But her natural environment is the tablao. There, she has been shaping herself into an artist since she was twelve years old. She always says she was born dancing and has never set foot in a dance school. She never wanted to be anything but a dancer. After an interruption of some years due to her marriage, Blanca returned to the tablao in 1980 with a maturity and control of movement that astonished the critics of the time. 5poradic appearances on the television program \"La Danza\" and an occasional recital have provided the only opportunity for a wider audience than that which frequents the tablaos to witness the magic of the dance of Blanca del Rev. --What qualities should be combined to form a good flamenco dancer? \"First you have to feel, to live deeply what you are doing. Without that inner world you can't even begin. Then, you must have the ability to express that inner world, to transfer it to your body. You can learn a technique, but you can't learn the ability to make your feelings appear on your face, in the hands feet and body.\" --50, technique is not enough? \"Technique can be learned, but there are those who do not study it; rather, they have inner drives that move their hands and body. Later, they gradually move toward perfection by looking inward. There are people who use a mirror to study, but I cannot. It destorts me, intimidates me, and makes me feel observed and criticized by myself. I have to see myself from the inside, and I know that, if what I see inside looks good, then I am doing well on the outside. Now, videotapes have demonstrated that I was right.\" BLANCA DEL REY IN THE TABLAO \"CORRAL DE LA MORERÍA\"; GUITARIST: DAVID JONES, FELIPE MAYA, CURRO DE JEREZ JALEO - VOLUME IX, No. 2 \"They talk about complete or incomplete bailaoras. The decisive point in a good dancer is not whether she is more or less complete, or has a better or worse technique, although all of this may be important. The audience forgets all of that when a dancer has other virtues that make them forget about those details. Those virtues are \"el alma\" [soul], art, duende, and angel.\" --What makes for purity in flamenco dance? \"In the first place, purity is feeling and living what you are doing. Secondly, it is dancing things that are worth the trouble and then not to corrupt them. For example, you can't dance a siguiriya and end it por bulerfas. The two things don't go together. Neither can you surpress the role of the cante. The siguiriya was born as a cante and you must dance the song; you must respect the terrain of the cantaor and the rules demanded by a siguiriya. You can't subdue the cante and the guitar in order to be more entertaining to the gallery, to get an easy applause. You have to do things as they are. If not, the artist knows he is not giving authenticity.\" --What are \"duende\" and \"angel\"? \"I would say that duende is a very special internal way of being when you are dancing. When the duende is with you, you feel a..how do I explain it to you..a special kind of 'gracia'. I believe that duende and ángel are the same thing, although ángel could mean 'gracia' or 'aire' and duende something more profound. But, basically they are the same, a pleasant state of being where you feel no obstacles, where you can dance freely with no sensation of body weight; it is a state of harmony so great that you begin to create and you are capable of creating a thousand new things and dance all night long without stopping, without tiring; your body floats, your hands and arms move by JALEO - VOLUME IX, No. 2 DON'T LET FLAMENCO RUIN YOUR LIFE! This is a true confession of how I and others have become entangled and eventually infected with flamenco. In 1965, I attended my wedding as did several others. One of them was named Bobby Turner, who at that time could play a hot butlerias. (CAUTION: The buterias is one of the most contagious forms of this disease.) I was infected from that moment until this very day. Bobby became cured as have others, like Joe Trotter, but these cases are all too few. Once, in the San Francisco area, I attended a party with Agustin, Freddie Mejia and a nice young woman named Lynn Write. Someone asked Lynn if she was a dancer and she replied that she was NOT. But when the music started, she began shaking her skirt and marking time. Then she started singing and I knew that she had an advanced case of flamencoitis. I eventually found out that she and her husband, Benji, had sold their house in Morón de la Frontera to Donn and Luisa Pohren. The house was used as an institution for those poor infected souls, but eventually it closed. However, Donn has not given up and has written several books warning people about the flamenco way of life. He is now conducting what he calls \"Unusual Tours\" of the infected area. Jaleo magazine is kept together by an over-worked staff who cares about English speaking people who are suffering from this infection. Of course, there is more information available in the Spanish language. So, if you have an article that you want to share with the rest of us, please try to get it translated before sending it in. You might think it's enjoyable, even fun, to sit around the house playing guitars, singing and dancing all day, but remember: Don't let flamenco ruin your life! This nice young man could have been a butcher or attended barber college, but no, he became obsessed with flamenco. --Sodhana SABICAS WITH PEPITA LLA SE, MADRID 1930",
    "title": "BLANCA DEL REY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 1050,
    "article_char_count_full": 5829,
    "article_char_count_review": 5829,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A2",
    "article_text_for_review": "THE GREAT FLAMENCO MYSTERY Dear Readers, I'm writing in response to two letters in the last Jaleo: Joe Bubes, Pittsburgh, PA and Radoffa Acedo, Pirtleville, AZ. (Mr. Acedo, I would like to recommend Grag Stitt, P.O. Box 44014, Tucson, AZ 85733. He is an excellent guitar instructor and has studied with Mariana Córdoba; see Jaleo Vol. VII No. 5.) I have lived in Arizona for many years and recently spent some time in Pittsburgh. Of course, I'm always in search of other flamencos. I have read in Jaleo about juergas in Akron, Ohio, a short drive from Pittsburgh and was annoyed at these readers for not listing a number. I phoned all the dance studios and guitar shops listed in Pittsburgh's yellow pages; NOTHING! So for the next four months I had to content myself with one person juergas. Ugh!",
    "title": "EDITORIAL LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 140,
    "article_char_count_full": 797,
    "article_char_count_review": 797,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A3",
    "article_text_for_review": "MANITAS DE PLAYA Y DIENTES DE ORO When I hear a non-flamenco say \"I have A flamenco album,\" inevitably it will be one of three artists: Carlos Montoya, Sabicas, or Manitas de Plata. The latter of the three was not even mentioned in Donn Pohren's book, _Lives and Legends of Fismenco_. I have heard so many bad things about this guitarist that it was ten years after I started collecting flamenco records that I had the good fortune to actually hear one of his old albums in which he accompanied a singer. Since then I have collected several and it occurred to me that, if Manitas de Plata doesn't play with a tight compás, it's because he doesn't want to. I wonder why it is O.K. for someone like Prince to display showmanship and twirl around when playing his guitar, but it's not okay for Manitas de Plata to do his thing. (I must admit, however, that I have never seen him perform.) Someone once told me a story of a person who asked Manitas de Pista what it was he had that other guitarists didn't have. His answer: \"A Rolls-Royce!\" -Sadhana 20% DISCDUNT TO ALL MEMBERS OF JALEISTAS 1011 FORT STOCKTON DRIVE OWNER TOM SANDLER SAN DIEGO. CALIFORNIA (714) 298-8558 (Hillcrest/Mission Hills area)",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 214,
    "article_char_count_full": 1197,
    "article_char_count_review": 1197,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
