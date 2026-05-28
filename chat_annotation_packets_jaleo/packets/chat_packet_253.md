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
    "article_id": "JALEO_1987_SUMMER::A13",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTHE FLAMENÇO WORKSHOP About 55 years ago, Ted Shawn started Jacob's Pillow in Lee, Massachusetts. This was the summer and, at times, winter home of his all-male dance company. They worked on many styles of dance and began doing summer afternoon lecture-demos for the local people and the few that came up from New York. This was the start of what is now the oldest dance festival in the United States. This was also the beginning of the \"dance camp\", where professional dancers and students would come and stay for certain lengths of time and devote their entire time to the study of dance. There were others such as Perry-Mansfield in the mountains of Colorado. Little by little these dance camps blossomed, mostly for modern and classical styles of dance. Up until a few years ago, the study of\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"private\"]\n\nstudents would come and stay for certain lengths of time and devote their entire time to the study of dance. There were others such as Perry-Mansfield in the mountains of Colorado. Little by little these dance camps blossomed, mostly for modern and classical styles of dance. Up until a few years ago, the study of flamenco along with other dance styles in Spain, were mostly in the class room. The classes were usually group classes on a set day or private classes set by teacher and student. The idea of a two, three or four week seminar-workshop in flamenco was very rare. Also, there appears to be a correlation in the fact that until very recently, summers were very lean times in the study of dance, including the performance of dance, especially touring and concert work. The very fact that there was so much spare time in summer gave way to the idea among many schools of dance, dance companies, festivals, dance camps, that this would be a good time to capitalize on the people that may want to study dance or music in depth. This started to include forms like flamenco, and the many other forms of Spanish dance. Within the last ten or fifteen years, workshops and seminars have blossomed throughout the world, especially in the United States, Spain, and a few other countries, and now they are offered throu\n\n[ENDING CONTEXT]\n\naire. Lili Castillo did a dramatic work, \"Dos Mujeres\", in story form. Yaclisa did a beautiful soleares with much solero. I did my interpretation of Bach's Toccata and Fugue in D-Minor (Bach was very flamenco). Lydia Torea and her company did the finale of the first half, a super and fun rumba flamenca, showing what sexuality in flameco is all about. A very strong highlight of the concert was Eva Encinias choreography for six male dancers in martinete. This was arte puro and unique in its superb individuality. Eva Encinias did a very special caña, with power and drama. Lydia Torea and I did\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "23-25",
    "page_number": 23,
    "word_count": 2003,
    "article_char_count_full": 11198,
    "article_char_count_review": 2943,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "private"
      }
    ]
  },
  {
    "article_id": "JALEO_1987_SUMMER::A14",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA MIXED BAG This is a mixed bag of flamenco; lets start with the sad news first: Orlando Romero died under the most tragic circumstances in Buenos Aires (all details included -- a photo, his cartel, and our memorial for Romero at the Actors Chapel)...Miguel Céspedés, popular guitarist (also South America and Uruguay) was mugged in New York subway -- broken leg...Paco Montes is organizing a homenaje for October 4, 1987 at the Casa de España. Paco Montes, the well known cantaor and cantante is now appearing at Meson Asturias in Queens...Esther Suarez is dancing in the tablao and Basilio Georges is on guitar there. At Bloomindales' Spanish and Catalan week in New York City, Bloomingdales featured \"Los Paquiros\" for September 21 and 24. This included cantaor Paco Ortiz, dancer Mara and Arturo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Martinez\"]\n\n- broken leg...Paco Montes is organizing a homenaje for October 4, 1987 at the Casa de España. Paco Montes, the well known cantaor and cantante is now appearing at Meson Asturias in Queens...Esther Suarez is dancing in the tablao and Basilio Georges is on guitar there. At Bloomindales' Spanish and Catalan week in New York City, Bloomingdales featured \"Los Paquiros\" for September 21 and 24. This included cantaor Paco Ortiz, dancer Mara and Arturo Martinez...The queens Bloomingdales had Roberto Reyes as solo guitar... It is my pleasure to include photo and resume of Lisa Bottalico...told me that flamenco had changed her life...first appeared on stage at the age of four...has been in Opera...Tango dancer, Lisa has worked with many of the artists. Closely associated with Manolo Rivera, Lisa has duende -- duende like nobody else on stage. I have also included smaller photos of Barbara and Estafania with Domenico Caro and Arturo Martinez...This group performed on numerous occasions this summer on an outside stage at Lincoln Center and later at Fordham University in NYC. October 12 at the Avery Fisher Hall, NYC, Mario Maya Flamenco Gypsy Dance Theatre perform Falla's \"El Amor Brujo\". Paco Montes at 14th Street Fair I have included a paquete by Daniel de Córdoba. (The\n\n[ENDING CONTEXT]\n\nemotionally thrilled audience; mention should be mad of the ever popular Mari Constancia dancing Albéniz' \"Leyenda\". A final encore to this great Flamenco Homenaje...the dancing Liliana, Mariano Parra, Manolo de Córdoba, La Conja, Meira, José Antonio...all por bulerías with the cante of Luis Vargas, Domenico Caro and joined in the end by Fernando Guisado. The guitarists were Arturo Martínez and Basilio...this homenaje should be classed as one of the greatest ever. Miscellaneous news bit: Russo-Spanish Plisetskya (of Bolshoi Ballet) is to become artistic director of the Ballet National España.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "article",
    "pages": "26-30",
    "page_number": 26,
    "word_count": 1319,
    "article_char_count_full": 8068,
    "article_char_count_review": 2911,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Martinez"
      }
    ]
  },
  {
    "article_id": "JALEO_1987_SUMMER::A15",
    "article_text_for_review": "JOSE LUIS RODRIGUEZ This young figure of the flamenco guitar was born in the city of Ceuta in 1967. He was attracted to the guitar when he was nine years old and studied with various teachers until he met Maestro Mario Escudero in 1983. He has studied with Escudero since then. He won second prize in the Bienal de Sevilla and a third in the Concurso Nacional de Jerez and studied in the Conservatorio Superior de Música in Sevilla. The stages of Brussels, London, Paris, New York, Madrid, and throughout Andalucía have been witness to his success as a concert guitarist. Now he is touring the United States and visiting Washington D.C., where he will make a record for the O.E.A. José Luis makes guitar arrangements for the Coral de Santa María de la Rábida, based on themes of Garcia Lorca and the folklore of Huelva. He considers himself to be a follower of the school of Mario Escudero and, according to those who should know, a promising figure of the Spanish Guitar. PROGRAM 1. \"Fantasía Onuvense\" (fandango 2. \"Exodo Gitano\" (taranta) 3. \"Fiesta en Cádiz\" (alegrías) 4. \"Quelaja\" (bulerías) by José Luis Rodríguez by Mario Escudero by Mario Escudero by José Luis Rodríguez Ⅱ 1. \"Malagueña\" 2. \"Corazón de guitarra\" (granaina) 3. \"Para Amina\" (guajira) 4. \"Llanto de Boabdil\" (danza Arabe) by E. Lecuona by Mario Escudero and José Luis Rodríguez by Mario Escudero by José Luis Rodríguez Luisita Sevilla (a.k.a. Karen Louise Pacheco) Drawn to the difficult art of flamenco dancing at an early age in Denver, she was told by José Greco that in order to be and authentic Spanish dancer she had to study in Spain. At the tender age of fifteen, she left the Denver Ballet Theatre School of Dance, travelled to Seville, Spain and enrolled in the Academy of Eloisa Albaniz, and following an intense year of study, continued with the great Enrique el Cojo. The Spaniards could not pronounce her American first name Karen, and because of her youth they finally settled on Luisita, or little Louise. She was given the name of the city she trained in, and she entered the world of professional dancing with the name Luisita Sevilla. After touring Europe with various companies she returned to the United States, where she eventually joined the famous touring Spanish Orchestra, Los Chavales de España. She left the troupe after a year to form her own flamenco dance act with the talented Roberto Lorca and toured the country for four years. The act was hired to return with Los Chavales, and she remained with the troup for another five years touring Europe, the Orient, South America, the Caribbean, Canada and the United States. With her marriage to Ferdie Pacheco, a Miami physician, Luisita settled down to teach in the academy she founded to perpetuate the art of Spanish dance, both flamenco and classical and gave annual concerts. During this time, she choreographed dances for the opera company, principally the flamenco scenes in Carmen and from time to time appeared in various clubs and television shows. She is currently teaching at \"Ballet Concerto\" in Miami and chorcographed a flamenco version of Carmen along with her partner Paco del Puerto October 17. *** Luisita Sevilla",
    "title": "PROFILES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "poem",
    "pages": "31",
    "page_number": 31,
    "word_count": 541,
    "article_char_count_full": 3184,
    "article_char_count_review": 3184,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SUMMER::A16",
    "article_text_for_review": "Andrea Del Conte, dancer/choreographer/teacher is the founder and artistic director of The American Spanish Dance Theatre and a leader in the creation of contemporary Spanish dance in America. Born in the United States and trained in Spain, her work is a demonstration of the merging of the two cultures. Ms. Del Conte formed her company in 1978 and has toured extensively both as a solo performer and with the ensemble. Anton Dolin first discovered her while she was performing the Spanish variation from Tchaikovsky's \"Nutcracker\" at the age of sixteen. He strongly encouraged her to become a Spanish dancer and soon after she went to Spain to begin her training. Her teachers include Paco Fernandez, Carmen Mora, Merche Esmeralda and Luis Montero. She has also had extensive training in modern dance, ballet and jazz and has performed with the companies of Maria Alba, Estrella Morena and the New York City Opera. Ms. Del Conte holds a Master's Degree in Spanish literature from New York University and is on faculty at Third Street Music School as well as the Executive Director of the School of the Ballet de Puerto Rico. Her company has been in residency at Penn State University and has performed at numerous colleges and dance festivals throughout the United States. JALEO - VOLUME X, No. 1 RELEASES Luisa Triana Photo above left: Internationally renowned Spanish dancer Luisa Triana is also a talented visual artist. Her character studies and portraits reflect Ms. Triana's understanding of inner discipline and self awareness. With a dancer's sensitivity to the human form, the artist imbuies her subjects with a captivating dignity. Flamenco dancers are caught at the peak of their self-involvement as they perform the intricacies of the dance. Having studied painting under such masters as Will Foster and Nicolai Fechin, Ms. Triana is able to share through another medium her understanding and love of dance. SPANISH DANCE SOCIETY Marina Keet, Spanish dance lecturer at George Washington University in Washington DC, spent the summer examining for the Spanish Dance Society in Great Britain, Malta and Italy. She visited the Royal National Ballet of Spain in Madrid and José de Udaeta in Sitges, Barcelona, where many members of the Society gathered to celebrate the 15th anniversary of his summer courses. Sr. Udaeta was the recipient of this year's dance prize in Germany, for his contribution to Spanish dance in that country. Photo below left: José de Udaeta with members of the Spanish Dance Society in Spain. Marie-Louise Ihre from Italy, Marina Keet, Paula Durbin and Ziva Nir (2nd from right) from George Washington University in DC, Nancy Ruyter from University of California, Irvine, Irina Campbell from Virginia and Laura Know from Birmingham, Alabama. Joana Del Rio Photo to right: Joana del Rio the first American to examine Spanish dance overseas, when she examined in London in December 1986 for the Spanish Dance Society. Joana received her Masters in dance at George Washington University, where she was trained in Spanish dance by Marina Keet. She has her Instructor de Baile examination from the Spanish Dance Society. JALEO - VOLUME X, No. 1 From left to right: Manuela Carrasco, Rosario Montoya \"La Farruquita\", Angelita Vargas, Pilar Montoya \"La Faraona\" L to R: Angelita Vargas, José Cortes \"Bien Casáo\", Antonio Montoya \"El Farruco\", \"La Faraona\", \"Guito\", Manuela Carrasco, Rosario Montoya \"La Farruauita\" TRADITIONAL FLAMENCO GUITAR TRADITIONAL FLAMENCO GUITAR VOL. 1 VOL. II Fugue filha de Venterro. Bumbe. Fannau. filha. Reuanda. Madre. de aqueles Serrano. Aldo. Flamino. (de-lares) Squercas. Pesta. Fatina. (Huari-si) Ses danses #1 • Se-filles-#2 Malgré la classe d'une base Zapata de Colombia • Carrier Follogos de Vimenta • Farrera Duvos Vivienn TRADITIONAL FLAMENCO GUITAR VOL. III Sev.Bance #1 • Sev.Bance #7 V.cobas. • Faufaumg. de l'Ingr. N.g.de. • Campeche de Gronda l'Empleo P.de.f.o. • conyera di Runda Flammera 1 Ngo. 5 Ngo.de. • N.g.o.e.r.m. by * Home Study Courses with Cassettes * Instructions, Exercises, Musical Selections, Techniques * Written in Conventional Music Notations and Tablature $23.00 each plus $2.00 shipping U.S.A / $5.00 outside U.S.A. Send Cashier's Check or Money Order to: Mariano Córdoba 647 E. Garland Terrace Sunnyvale, California 94086, U.S.A.",
    "title": "PRESS RELEASES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "32-35",
    "page_number": 32,
    "word_count": 687,
    "article_char_count_full": 4323,
    "article_char_count_review": 4323,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SUMMER::A18",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFLAMENCO! An Evening of Flamenco Dance and Music One of the most stirring flamenco concerts I've ever attended was held in what was perhaps the most incongruous of settings, sunny and placid Carmel, specifically Mission Ranch, The Bam, 26270 Dolores, Carmel. The poster's reference to The Barn had proved to be more than a simple literary allusion. It was in fact a converted barn in what looked like a converted ranch. The barn had been outfitted with flood lights, a sound system, and most importantly, a raised stage. The concert began with a spirited tango gitano. Continuing in this vein, Sara and Diana artfully performed a lively alegrias. While Rubina accompanied in song with Timo clapping in rhythm, the two dancers flanked each other and spiritedly counterpointed the other. Juan played\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"powerful\"]\n\narn in what looked like a converted ranch. The barn had been outfitted with flood lights, a sound system, and most importantly, a raised stage. The concert began with a spirited tango gitano. Continuing in this vein, Sara and Diana artfully performed a lively alegrias. While Rubina accompanied in song with Timo clapping in rhythm, the two dancers flanked each other and spiritedly counterpointed the other. Juan played in bright major keys using a powerful rasgueado stroke which seemed to animate the dancers even more. The audience, warmed by the opening cante chico, was a mixture of the curious, as well as the dedicated, which apanned all age groups. The next piece, a rumba gitana, was performed by Rubina who danced as well as sang. The rumba seemed to borrow its sensual quality from the Latin American rumba while keeping its gypsy roots. As this number came to an end, the crowd became increasingly enthusiastic and receptive to the performers. This I mused to myself, would be a great show. Timo Lozano then took center stage for a solo farruca, which consisted of lively zapateado. Timo, in sharp contrast to Diana, Sara and Rubina, who wore colorful and ornate dresses, was dressed in black trousers and vest, accented only by a white shirt. It seemed as if all his emotions and flair which normally would be expressed in the clothes worn, were perfectly synchronized. Timo danced with great concentration and barely suppressed emotion. Each turn of his head suggested a certain grace of form, a majesty so evocative of the Spanish soul or alma. As Timo took his bow, the audience broke into fervent applause. Timo's smile seemed to confirm that the audience, too, had felt what he had. At the end of this number, a single chair and microphone were set up on the stage in preparation for Juan Moro's guitar solo. Until this point, Juan had been playing in several different keys using a cejilla. But for his number, Juan elected to use a second guitar without the cejilla. This second guitar, unlike the first, was a more traditional classical guitar, one that, it seemed, has waged many campaigns. As Juan played, I couldn't help but marvel at the combination of strength and gracefulness that he employed, interspersing occasional golpes to the side of the guitar. As he completed the piece, the crowd quickly added its approval, evoking a subtle smile from Juan. Sara, next performed one of the more challenging dances, a siguiriyas. This dance brought out the darker, more emotional side of flamenco music. Her black gown helped conv\n\n[ENDING CONTEXT]\n\nstage, and slowly, carefully climbs her way to the floor...her parchment moon, like a large, white lit tamborine hovers with her. She freezes in statue stillness, combining primitive sexuality with the naive elegance of a child...Preciosa is about to encounter the \"ingleses\", and her fate will take a fearful turn with the Wind. Though Philadelphia cannot be said to be a flamenco center, the city's most discerning aficionados were treated this east winter to an extraordinarily creative and innovative interpretation of Federico Garía Lorca's haunting poem \"Preciosa y el Aire\". It was the work\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "poem",
    "pages": "39-47",
    "page_number": 39,
    "word_count": 2614,
    "article_char_count_full": 15741,
    "article_char_count_review": 4179,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "powerful"
      }
    ]
  }
]
```
