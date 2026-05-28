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
    "article_id": "JALEO_1985_SPRING::A11",
    "article_text_for_review": "BECOMING PROFESSIONAL. BEING PROFESSIONAL The word \"professional\" usually signifies someone who has put in a great deal of time and effort to become an \"expert\" in his or her chosen field. The true meaning of professionalism goes far beyond just getting paid for what you do. It is a responsibility towards what you do. One's profession is synonymous with spending a great deal of time with that profession, studying the profession, perfecting and refining it and dealing with the business of a profession so that it will be a benefit to all concerned. In the case of flamenco, like all performing arts, the benefit goes beyond the artist, beyond the sponsors, buyers and public and even beyond the art form, for it should benefit the \"feeling,\" the meaning of the art of flamenco as a whole. Flamenco dance as a profession holds a very special place in the world of dance. Although flamenco dates way back, it has been only in the last century, more or less—since the riae of the café cantante—that flamenco dancers have been performing in a professional manner with set-type shows, receiving some sort of salary, and performing in front of a public that has come to see skilled artists perform with knowledge and understanding of the art. Flamenco also holds the destination of being one of the few dance performing arts froms that can be performed in almost any space, from a very small \"tablao\" style of performing, to concert halla, in the round, and in literally any space that can hold chairs for the guitarist, singers and space to move a bata and hopefully have a resilient, wooden type floor. This has been both good and not so good for the art form, for a few important reasons that I will explain as this article unfolds. Art, by its very definition, is of the highest level of human expression; the art of flamenco is one of these highest levels of human expression and communication. To approach flamenco as a profession, is a great responsibility, for you are a representative of this great art form that is so often misunderstood. The very flexibility of the performing possibilities of flamenco have unfortunately drawn many people who are not ready to represent this beautiful art to its fullest. Many beginners feel that, after a short period of study, they can put on the costume, dance their routines and cliches, and think that the public will not know the difference. This sounds very basic and to the point, but too many people fall short of representing flamenco because they have performed while their talents were \"too green.\" The proper approach to the profession of flamenco is no different than that of any other serious art or profession. It requires great respect for the art. This is one of the keys to crossing over to the professional level of the art--\"respect for the art.\" The profession of flamenco or any art requires a primary use of me's time, of one's life and focus. This is too often lacking. Flamenco in many of the tablaos in Spain has been used primarily for tourist attraction and the cliches of flamenco are encouraged, often with mediocre talent. Some tablaos emphasize the bevy of gorgeous girls in slinky leotards parading a few pasos of soleares or alegrías. Others emphasize the fast and furious stomping of the floor and other fast cliches, again basically tourist oriented, passing this off as flamenco puro. God, how we miss the old tablao, \"La Zambra,\" where they had flamenco with dignity, class and integrity. An important thing to realize is that the general public may not understand the compás or the art form itsrlf, but I do not underestimate the public's inner feelings of what is good and what is a mediocre representation of flamenco. This has been learned the hard way, especially by dancers who have come to the USA and thought that the Americans did not know the difference between good and mediocre. This attitude has only hurt the art form and the potential for all the fine artists who are trying to keep flamenco a living, breathing, vital art form, with as many opportunities for the artist and public as in other vital art forms.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 710,
    "article_char_count_full": 4107,
    "article_char_count_review": 4107,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_SPRING::A12",
    "article_text_for_review": "FLAMENCO PASEO by Paco Sevilla The following is from the publicity blurb for flamenco guitarist Marcos, who lives and performs in England: \"Marcos began playing the flamenco guitar at the age of eleven and has been described as \"a master in both technique and expression and among the best flamenco guitarists\" (Guitar Magazine). He learned his art in Seville, one of flamenco's most important centres, with the maestro Pepe Martinez and has subsequently pursued a highly successful solo career performing at leading international Festivals including eight Edinburgh Festivals and numerous guest appearances on radio and T.V. programmes. \"His album for Stoptime Records \"Flamenco Hotizons\" was extremely well received - \"...an impressive debut from someone very dedicated and capable who will undoubtedly travel down the road which is perhaps the most technically sophisticated and distiplined that a guitar player can take\" (Martin Simpson, Southern Rag). \"I found this most enjoyable, full of excitement and interesting ideas. Highly recommended.\" (Steve Marsh, Classical Guitar). \"On 'Flamenco Paseo' Marcos has captured the sounds and photo by Studio Edmark MARCOS Atmosphere of a summer's evening stroll through a typical Andalucian city - the distinctive ambience and rhythms that emerge in the night over Seville, Jerez, Cadiz, and Ronda. Side One is uncompromising flamenco for the 80s - the 'duende' of new Andalucia with Marcos supported by another guitarist of the younger generation 'Pastorito', while Side Two features traditional compositions. From the opening ex Side Two begins with a rumba in which Marcos is joined by the guitarist John James. This track in my opinion, spoils an otherwise excellent record. The music is corny and seems to be an unsuccessful attempt at the Paco de Lucia style of performance. The next track is \"Malagueña Horizons\" by Esteban Sanulcar. This is an attractive piece composed by a guitarist who, in the 1930s, was considered to be a phenomenon by other guitarists and was the Paco de Lucia of his time. The complex rhythmic structure of the next piece, \"Siguirfya,\" is handled with great dexterity by Marcos. This is followed by \"Zoronga Gitano,\" an old Andalucan melody which Lorca rediscovered in his search for the lost flamenco forms. The final two tracks, a tientaa leading into tangos and a rodeña, complete a most entertaining record. The sleeve notes are brief and to the point and it makes a pleasant change to be told what kind of guitar is being played. (Even the make of strings used is given.) Highly recommended. --Steve Marsh * * * THE FRAME STATION The Finest in Custom Picture Framing 20% DISCDUNT TO ALL MEMBERS OF JALEISTAS 1011 FORT STOCKTON DRIVE SAN DIEGO, CALIFORNIA OWNER TOM SANDLER (714) 298-8558 (Hillcrest/Mission Hills area) A NEW RECORD FROM CAMARON \"VIVIRE\" [Philip 822719-1; 1984] [from: E1 País, November 24, 1984; submitted by Brad Blanchard; translated by Paco Sevilla] by Angel Alvarez Caballero cante: Camarón de la Isla :oque: Paco de Lucía, Tomatito; with Carlos Benavent (bass), Jorge Parão (flute), and Rubén Dantas (percussion). A new record by Camarón that certainly adds nothing to the recent recordings of this unusual cantator. I mean to say that he is again and again repeating the formula that gives him so much success—lively rhythms of tangos and bulerías done with his aire, the aire of Camarón that is so unorthodox, yet with a \"rajo cantator\" and the enchanting voice of this man who is a truly exceptional case in the modern art of flamenco. Camarón continues following to the letter the line of his previous record, \"Calle Real\" as well as the one before that, \"Como el agua.\" I cannot say that he enriches this series, because it seems to me that he includes less variety of styles than on the previous two. Of the eight cantes that make up \"Viviré,\" seven have a strong similarity to each other, bringing on the risk of monotony. The eighth theme, \"Campanas del Alba\" breaks away completely from this universal sound with some very beautiful melodies of siguiriya in which the cantaor, although not sticking to the traditional form of doing this style, achieves a dramatic \"jondura,\" a spine tingling lamentation. For me, this theme is by far the most valuable thing on this record. Romero Sanjuán is not a flamence cantor in the strict sense of the term. It is more appropriate to consider him as a singer/songwriter. But he has a beautiful voice for the cante and, although he gives up the jipio [the breaking of the voice in flamenco] in favor of pure melody, he is rewarding to listen to. Finally, one must not overlook the excellent collaboration of Rafael Riqueni on guitar, Manuel Soler with percussion, and a group of jazz musicians that includes personalities who have been frequently associated with flamenco, namely Carlos Benavent and Jorge Pardo. * * * ANOTHER POSTHUMOUS RECORD OF ANTONIO MAIRENA | from: El País, Nov. 27, 1984; submitted by Brad Blanchard; translated by Paco Sevilla| by A. Alvarez Caballero \"Cantes en Londres y en la Unión\" [Pasarela PRD-107; 1984] cante: Antonio Mirena toque: Manuel Moreno \"Morao de Jerez\" and Paco de Lucia The XII Congress de Actividades Flamencas that was held at the end of September in Cáceres has brought out a new record of Antonio Mairena that recovers for the discography of the deceased cantaor some previously unreleased cantes and some that were recorded a long time ago but are particularly impossible to obtain. The proceeds from this record will go to a fund to build a mausoleum for Maestro Antonio Mairena in the town of his birth.",
    "title": "RECORD REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "20-22",
    "page_number": 20,
    "word_count": 926,
    "article_char_count_full": 5607,
    "article_char_count_review": 5607,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_SPRING::A13",
    "article_text_for_review": "SPANISH DANCE SOCIETY USA [sent by Marina Keet] Mr. José de Udaeta, from Barcelona, will take part in the 20th Anniversary celebrations of the Spanish Dance Society in October in Washington, D.C. On Friday October 18th at 8:00 p.m., a free performance will be given at the Marvin Theater sponsored by the Spanish Embassy, the George Washington University Alumni Association and the Spanish Dance Society. The syllabus of the Spanish Dance Society is taught for credit at George Washington University. On Saturday, October 26th at 8:00 p.m. and Sunday, October 27th at 3:00 p.m. at the Smithsonian Institute's Baird Auditorium, lecture-demonstrations with musicians and dancers under the direction of Marina Keet, will present a survey of Spanish dance under the title \"Exploring Spanish Dance.\" Dances from every region of Spain will be represented in their glorious costumes: Jotas, Seguidillas and Fandangos, classical and flamenco dances accompanied by the instruments from those regions. Influences on and by Sapnish dances will be discussed and demonstrated. From Monday, October 21st until Friday, November 1st, José de Udaeta will conduct daily classes that will include a Soleares. Anyone interested in participating in these events can write to: The Spanish Dance Society 4201 Cathedral Ave. NW Suite 814 E Washington, D.C. 20016 JALEO - VOLUME VIII, NO. 2 Rivera Spanish Dance Company, as well as forming and touring her own companies. She has been hailed by the $ \\underline{\\text{New York Times}} $ as \"...one of the wonders of the Spanish dance world, a dancer of consummate skill and style...\" These talents were evidenced in her master classes as she addressed the process of inspiring and giving her knowledge to her students. Aspects of footwork, castanets, and arms, as well as details for projecting the Spanish dance aura were explored. Classes were on the beginning and intermediate levels and were attended by devotees from as far as Rockford, DeKalb, and Sycamore, Illinois, and Gary, Indiana. Partial funding for these classes was provided by Productos Preferidas of Chicago. Besides these classes, new choreography was set for Las Preferidas, the Spanish Dance Company in residence at the Ballet Arts Studio. Two special dances--one, a siguirias for bata de cola is to be performed as a solo for Teresa or Lila Dole, Associate Professor of Dance at Northern Illinois University; and the second is an Habanera, to be performed by Sue's-in Emig, a teacher of Oriental dancing at Purdue University. * * * ATTENTIVE STUDENTS IN MARIA'S WORKSHOP CLASS ADELA CLARA WORKSHOP [sent by Viviana] A 3-day Spanish Dance Workshop by Adela Clara, sponsored by Viviana Orbeck, took place March 15, 16, 17 in Viviana's Lakewood Center for the Arts studio in Lake Oswego, Oregon. Adela Clara, founder of Theatre Flamenco of San Francisco, is a sought-after and inspirational teacher, choreographer and soloist. Her academic teaching credits include major residencies at the University of Utah; Master classes at Sonoma State College, San Jose State College and Dominican College in CA; University of Colorado; New Mexico State University; Jefferson High School, Portland, OR. She has been dancer/soloist for Maria Alba Company, Teresa y su Compañía and New York City Opera in New York; Fiesta Flamenca in Provincetown, MA; Seattle Opera Company and Heritage Family Theatre in Seattle, WA. Ms. Clara's inventive and stunning choreography, created for Theatre Flamenco of San Francisco, gathered numerous choreographic awards from the National Endowment for the Arts as well as for private funding foundations. In Madrid, Adela Clara studied with Maria Rosa Merce, Martin Vargas and Victoria Eugenia; in New York with Mario de Bronce, Roberto Ximenez, Manolo Vargas, Pepa Reyes, Azucena Vega and Mariquita Flores. Her background also includes modern dance, jazz and comprehensive ballet training.",
    "title": "PRESS RELEASES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 612,
    "article_char_count_full": 3902,
    "article_char_count_review": 3902,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_SPRING::A14",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMONTOYA'S CLICKING HEELS [from: The San Francisco Sunday Chronicle, March 24, 1985; sent by Frank Campbell] by Beverly Mann Rosa Montoya can remember the time years ago when she and her flamenco dance troupe were on their way to a performance in Victoria when their bus got stuck in a snowbank. \"The dancers tried to dig the bus out with shovels,\" she said. \"Finally, without food in our stomachs and a sold-out audience, awaiting, a tourist bus rescued us and took us to the theater.\" The company arrived 45 minutes late. \"My feet were so frozen that I could not even feel my toes. My castanets did not even sound. But we danced to an enthusiastic audience. It was some night.\" Since the 1960s, Montoya, along with the renowned flamenco dancer Circo, has had a few such nights. She has toured\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Marilyn\"]\n\ng, a tourist bus rescued us and took us to the theater.\" The company arrived 45 minutes late. \"My feet were so frozen that I could not even feel my toes. My castanets did not even sound. But we danced to an enthusiastic audience. It was some night.\" Since the 1960s, Montoya, along with the renowned flamenco dancer Circo, has had a few such nights. She has toured throughout the continents and performed onstage with such divas as Beverly Sills and Marilyn Horn. Niece of the famous classical guitarist Carlos Montoya, and possibly the only \"gypsy\" flamenco dance artist teaching in the United States, Montoya has been recognized nationwide by the critics. \"Bursting with energy at 100 heel clicks per minute, Rosa Montoya is almost a one-woman show within a show,\" wrote Pamela Gaye of Dance Magazine. For 12 years, Madrid-born Montoya has been sharing her art form with students at her San Francisco Mission studio. She now prepares herself and her company, Bailes Flamencos, for a DANCING DYNAMO ROSA MONTOYA series of monthly performances at San Francisco's Music Hall Theater starting this Saturday. \"I've tried to recreate the authentic 'cafe cantantes' or 'tableau flamenco' such as in Spain. Instead of a theatrical stage or concert setting, a feeling of intimacy between the audience and dancers will be established in a dinner-club atmosphere,\" said Montoya. During lunch at a cafe, the 4-foot-11-inch dynamo of a dancer was in perpetual motion. \"Flamenco is not just gypsy dancing with fancy footwork and a rose between one's teeth. It's a feeling - an expression. It must have good cantes [singing], good palmas [clapping] and good guitar accompaniment to complement the 'tiempo' or rhythm of the dancer. T\n\n[ENDING CONTEXT]\n\nreveres as the greatest flamenco artist in the world, is playing in a jazz-fusion ensemble. Even though flamenco has become a way of life for him, Radford does not limit his interest in the guitar to flamenco. He listens to and admires classical guitarists such as John Williams and Julian Bream, and he has studied under Spanish classical guitar master Andres Segovia in Spain. He listens to jazr-rock guitarists such as Al Dimeola and John MRLaughlin. Chet Atkins, a former idol of Radford's back in his Silvertone days, is now a big fan. \"On the road, I listen from everything from Beethoven's\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "25-30",
    "page_number": 25,
    "word_count": 3318,
    "article_char_count_full": 19961,
    "article_char_count_review": 3340,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Marilyn"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_SPRING::A15",
    "article_text_for_review": "APRIL JUERGA Joaquin and Lisa Feliciano offered their studio \"The Long Beach Dance Academy - Studio 2000\", 727 South St., Long Beach, for the April 13, 1985 Los Angeles area juerga. There was a good attendance with many familiar faces and many new ones, which we are always glad to welcome. We had lots of food and drink and everyone had a good time. The following are some photos of the juerga. KATINA VRINOS RUMBA GITANA FIRST LADY NANCY REAGAN JOINS FLAMENCO CLASS IN MADRID",
    "title": "LOS ANGELES JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "31-33",
    "page_number": 31,
    "word_count": 85,
    "article_char_count_full": 477,
    "article_char_count_review": 477,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
