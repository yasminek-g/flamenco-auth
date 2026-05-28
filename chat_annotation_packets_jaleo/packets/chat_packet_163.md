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
    "article_id": "JALEO_1983_03::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTwo forms of the petenera are commonly heard today. One of them, the \"Peteneras Boleras,\" would normally be of little interest to flamencophiles, because it is piano music belonging to the escuela bolera. It does creep into the repertory of some flamenco guitarists, though, probably because it is considered \"safe.\" An unsettling characteristic of the flamenco song form is that it has a sinister connotation for some artists who believe that performing it will bring on disaster. I have run into the superstition on a couple of occasions. The first time was in 1975, a sort of \"por las dudas\" situation. A couple of years later, I went to a party where a group of flamenco musicians was entertaining. I had seen them before, and had thought they were gypsies, although I never verified this. They\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mention\"]\n\nforming it will bring on disaster. I have run into the superstition on a couple of occasions. The first time was in 1975, a sort of \"por las dudas\" situation. A couple of years later, I went to a party where a group of flamenco musicians was entertaining. I had seen them before, and had thought they were gypsies, although I never verified this. They asked for requests and, of course, I requested the petenera. The singer told me I should not even mention the name because anything could happen (\"puede caer el acensor\"). Needless to say, the group picked another song. ATTITUDE FROM THE PETENERA (photo by Stanley Kanetake) The connection between the singer and the petenera is rejected by some, implicitly as having no basis in fact, so, even in general terms, it is probably not the last word. Nothing seems to be the last word on the petenera. Since the petenera is not performed very often, for one reason or another, a discussion of the choreographic range of the dance is almost impossible. Unlike the music, the dance has no particular pattern as does, say, the soleares and the seguiriya, and the choreographer is somewhat freer to improvise. The possibility that the music might be rooted in ancient, exotic sources, does lend an aura of glamour to the petenera. An effective choreography taught by Luisa Pericet emphasizes this possible antiquity by using very basic footwork and many compases of walking. The only adornment to the dance is a mantón de Manila held so the long fringe veils the face of the dancer as she enters the dance. The costume is plain; there are no catchy movements, no castanets, no steps identifiable as belongi\n\n[ENDING CONTEXT]\n\nManuel Mairena, Rafael Romero (well-known for his peteneras, one of his preferred cantes) and, of course, Camarón de la Isla. The same trend can be seen among guitarists: there are many guitar solo versions by non-gypsies -- Carlos Ramos, Faquito Simón, Manuel Cano, Paco de Lucía -- and not quite so many by gypsies -- Carlos Montoya, Melchor de Marchena. I don't know of any recorded versions by Sabicas or Mario Escudero, both gypsies who have explored just about every other flamenco form, but avoided this one. In any case, the superstition makes for interesting concert or record album notes.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LA PETENERA IV QUINCENA DE FLAMENCO Y MUSICA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 1240,
    "article_char_count_full": 6925,
    "article_char_count_review": 3278,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mention"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_03::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nVIERNES 3 CANTE Y BAILE DE SEVILLA Cantaores EL CABRERD MANDLO MAIRENA JDSE DE LA TOMASA PACO TARANTO MIGUEL FUNY GUITARRAS: Manolo Domínguez y Antonio Sousa. BAILE: Pepa Montes acompañada a la guitarra por Ricardo Niño. SABADO 4 y DDMINGD 5 MANOLO ESCOBAR y su Espectáculo Drquesta de acompanamento LUNES 6 CUADROS GITANOS CUADROS GITARRO LOS MDNTOYA LOS FARRUCOS ANGELITA VARGAS y EL BIENCASADO, acompañados en el cante por El Boqueron y la guitarra de Quique Paredas. MARTES 7 Funciones 7,30 tarde y 10,30 noche ROCK FLAMENCO TRIANA MIERCOLES & 1 tarde MARTES 30 - 8,30 tarde PIAND FLAMENCD FELIPE CAMPUZAND CONFERENCIA-PREGDN a cargo del escritor y periodista JUAN TEBA, que será presaniado por el novelista savilla-no JULIO M. DE LA ROSA JUEVES 9 NUEVAS FORMAS DEL FLAMENCO MANZANITA acompeñado\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"MAESTRO\"]\n\nacompañados en el cante por El Boqueron y la guitarra de Quique Paredas. MARTES 7 Funciones 7,30 tarde y 10,30 noche ROCK FLAMENCO TRIANA MIERCOLES & 1 tarde MARTES 30 - 8,30 tarde PIAND FLAMENCD FELIPE CAMPUZAND CONFERENCIA-PREGDN a cargo del escritor y periodista JUAN TEBA, que será presaniado por el novelista savilla-no JULIO M. DE LA ROSA JUEVES 9 NUEVAS FORMAS DEL FLAMENCO MANZANITA acompeñado por su grupo musical MIERCDES 1 - 6,30 y 10,30 MAESTROS DE ACADEMIAS SEYILLANAS DE BAÍLE Intervienen, por orden alfabético, acom- panados por una representación de sus elumnos: ANGELITA MILLA EUGENIA y JOSE JUANITO DIAZ MANOLO MARIN MARGARITA y MANCILLA (Gilanillos de Bronce) PEPITA RABAY ROCIO ALBENIZ VIERNES 10 JUAN PEÑA \"LEBRIJANO\" PEDRO BACAN Orquestra de Música Andeluza dei Conservatorio de Teluán Programe: FLAMENCD y MUSICA ANDALUSI y ARABE TRADICIDNAL CANTAORES: Curro de Triane y Curro Fernández GUITARRISTAS: Sami y el Niño de Romerito. SABADD 11 CANTES Y BAILES DE CADIZ Cantaores: JUEVES 2 CAMAR\n\n[ENDING CONTEXT]\n\nyear. The guitar and dance courses, along with the many recitals and the flamenco festival, will be held from Aug. 1-13 and will cost 20,000 pesetas (less than $200). Details are not yet available, but reservations must be made by July 1, 1983 -- no address for mailing has been indicated yet. SPECIAL OFFER Expires 9/30/83 G U I T A R S T R I N G S COMPLETE SETS $ \\frac{Retail}{Sit.00} $ $ \\frac{SPECIAL}{6.00} $ Specify Black or Clear trable Minimum order $12.00 Postage Paid California residents add 6.5% sales tax Make checks payable to: Lester DeVoe-Guitarmaker Box AA, San José, CA 95151\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ANDALUZA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "12-16",
    "page_number": 12,
    "word_count": 1135,
    "article_char_count_full": 6732,
    "article_char_count_review": 2636,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "MAESTRO"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_03::A6",
    "article_text_for_review": "$ ^{2} $ \"The Stah of Iran\" — A Few Remarks — In response to Jimmy Crowell's most welcome exhortation that we develop a flamenco syllabus, I have devised a system of musical notation that represents on paper the actions necessary for producing the sounds of zapateo. This notation records the percussion of feet. It is not a system of choreographic notation, and does not attempt to show, for example, where one foot is in relation to the other, or where the two feet are under the sun. Frozen in these few scratches below is the basic bulería step of Don Enrique El Cojo, peace be upon him. \\[\\underbrace{1 As a reward for reading this paper through to the end, this step is yours. No need for you to buy a ticket to Sevilla, no need to read a lengthy, nebulous verbal description of the steps. A few strokes of the pen covering two or three linear inches in a few seconds will capture and deliver this great man's work to anyone who understands this code. Nor need any footwork henceforth be lost to the dark recesses of memory. What flamenco dancer has not heard of the celebrated zapateado of Juan El Estampio? Yet, who alive today can tell me how it was danced? - Part the First - This symbol represents the foot, either foot, without distinction: Toe (Ball) Left A line abutting the foot-symbol at midsection and pointing LEFT means stomp with the WHOLE LEFT foot: A similar line at midsection, but pointing RIGHT means stomp with the WHOLE RIGHT foot: A line abutting the foot-symbol at the TCP and pointing LEFT means stomp with the LEFT BALL of the foot. similarly, such a line at the TOP, but pointing RIGHT means stomp with the RIGHT eALL: A line at the BOTTOM pointing RIGHT indicates a RIGHT heel beat: A stab by the POINT of the left foot is written in this manner: And the same motion performed by the RIGHT POINT is written, of course: $$ \\text{ 十 }+\\text{ 十 } $$ As any duckbill platypus can plainly see, it's our old friend the simple redoble. Since these four figures constitute a combination, let's tie them together with a bracket. Logical, n'est-ce pas? $$ \\overline{ 冊 } $$ $$ \\widehat{H\\mathrm{H}} $$ - part the Second - A scrape or a sliding motion is indicated by adding an arrow to any of these lines in the following manner. Thus, a toe scrape with the left foot: The same for the right foot: A left heel scrape: Is a right heel scrape: - part the Third - All the foregoing motions originate with the entire foot being picked up and the indicated part of the foot or the whole foot being smitten against the floor. Consider these two special instances: I. Suppose we wish to pick up only a part of the foot such as the left heel and strike with it, leaving the ball undisturbed on the floor. II. Suppose again we have just struck the left ball (7) and wish to follow immediately with the left heel. Then we must use the very handy symbol we shall call the \"ligado chico.\" The ligado chico is nothing more than a small circle attached to any of the above lines, and it ties the action to which it's attached to the immediately preceding action as in case II above, or it indicates that only a certain part of the foot (usually the heel) is picked up by itself and stomped with. For case I, consider this redoble which we all know: Left heel, right foot (planta), right foot (planta) left foot (planta). We render it thus: $$ \\sqrt{ 訂 }H $$ $$ \\boxed{ 丁 \\boxed{ 丁 } $$ because we picked up $ \\underline{\\text{only}} $ the heel ( ) anō not the whole foot ( ). For case II, that of left ball followed by dropping the left heel, we have $ \\boxed{\\alpha} $. If we were to write this as $ \\boxed{\\gamma} $ without the ligado chico, we would have to strike first with the left ball, then pick up the left foot, then strike with the left heel ( $ \\boxed{\\gamma} $). That's not the step we have in mind.",
    "title": "FOOTWORK NOTATION",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 713,
    "article_char_count_full": 3821,
    "article_char_count_review": 3821,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_03::A7",
    "article_text_for_review": "by Ken Sanders \"Most of it comes from playing the guitar for a long time.\" -- Paco de Lucía Playing in compás with a clean sound is what good tachnique is all about. How this is achieved is up to each individual. One of the best ways to master technique is through many hours of practice. Using a metronome during the practice of scales, arpeggios, tremolos, legados, etc., helps smooth out these studies and results in accurate rhythm. SCALES In his study of diatonic major and minor scales, Andres Segovia mentions that, through the patient study of scales, guitarists will correct faulty hand positions, gradually increase the strength of the fingers, physical beauty of sound, etc. \"In one hour of scales may be condensed many hours of arduous exercises which are frequently futile. The practice of scales enables one to solve a greater number of technical problems in a shorter time than the study of any other exercise.\" These truths can, of course, apply to the flamenco guitar. In general, I have learned that the easiest way to produce that \"clean sound\" with the left hand, is for the fingers to attack the strings right on their tips, close to the frets. If the hand is slanted slightly towards the tuning pegs (to the left for right-handed guitarists), it is easier for the little finger to curl right into the string, on its tip. The thumb of the left hand (positioned on the neck opposite the index and middle fingers), drops lower and lower on the back of the neck as you ascend towards the body of the guitar. When the thumb reaches the body, it slips right underneath, on the bottom of the fingerboard (over the body), giving support to notes played above the 12th fret. When playing scales above the 12th fret, more support is given to the fingers when the thumb slides towards the fingers as you play up the scale, going across the fingerboard, and slides away from the fingers as you descend. With the right hand picado and free stroke, I get the most power when the movement comes from the knuckle. With free stroke, the second joint of the finger bends enough to clear the next string, but the whole movement of the finger, the stroke, still comes from the knuckle. The right hand thumb, as a general rule, rests on the 6th string and drags up the body of the guitar as the fingers play down the scale. It acts as an anchor or point of reference, keeping the hand from moving around. Scales should be practiced rest stroke and free stroke with the i and m fingers. Also, in order to strengthen the fingers for tremolo and arpeggio studies, it is suggested to practice rest and free stroke using the m and a fingers, and/or i and a. Many professionals also recommend practicing scales with the thumb, rest and free stroke. Scales should be primarily practiced using the rest stroke with the i and m fingers, in order to develop a strong, powerful picado. \"You must feel the comás like a drum beating inside you.\" -- Benito Falacios",
    "title": "GUITAR: TIPS ON TECHNIQUE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "article",
    "pages": "18",
    "page_number": 18,
    "word_count": 526,
    "article_char_count_full": 2952,
    "article_char_count_review": 2952,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_03::A8",
    "article_text_for_review": "IN SEARCH OF INDIVIDUALITY One of the most exciting searches in a person's life is the search for himself, especially the individual-creative self. We are all on this earth with some basic creative expression of life itself. Just like an apple tree does not try to bear changes or a rosebush does not desire to become a carnation plant, so too, the human being, deep down inside of spirit and soul, wants to express true self, the individual self. Just as there are no two people who lack exactly alike, not even identical twins, there are no two people who have the exact same \"stamp of life\" within their being. Flamenco, being one of the true solo dance forms in the world, sold in its personal expression, is a perfect artistic and creative expression for finding one's individuality. A person who is moved to study flamenco, to absorb, to learn and live the art of flamenco, has an unlimited space to search for the individual self. A teacher of flamenco has a great responsibility to encourage the individuality of his students, right from the beginning and not to encourage and create clopes of himself. There are many teachers who have an established way of teaching, a way of teaching the movements, techniques and styles of flamenco. I feel that it is important to stress a person's individuality within the tradition and within a teacher's style and approach to all the facets of technique and movement. The discovery and awakening of a person's individual feelings are an inspirational discovery for both student and teacher. If a teacher stresses only his personal style and states that this is \"the\" way to approach flamenco, then that teacher is stressing an ego trip that will ultimately be limiting for both teacher and student. Discovering your own individuality is like planting a beautiful garden. The beginning seeds, nurtured with a good solid technique that looks good on your body, blended with what your emotions and feelings say to you, along with knowledge of the music, the cante, the compás and interpretation, will grow to be you, an individual expression of what you truly feel about flamepco. I have stressed individuality in almost all of my articles because we are in a day and age of very little \"black and white,\" an age of mostly \"grey,\" figuratively speaking, where there is sameness wherever you go. Society is stressing uniformity, whether a McDonald's hamburger stand in every corner of the world, square buildings that look alike and dancers who look alike -- like every other dancer, with a few titillating desplantes and mannerisms that are popular for the moment. There have always been more sheep than shepherds, but it is time to have ccjopes and \"be thyself\" in expressing art, flamenca art, find yourself! Love it and enjoy it and let us break away from the boring assembly line, in flamepco, at least! Since the beginning of time, people have copied other people who they felt were \"special\" and yet we are all special individuals and capable of doing something unique. No ape can be us and we cannot be anyone else. Why get a Beatle haircut if it does not fit you or you do not like it, or wear clothes that do not look good on you even though they have been put into fashion. Why try to dance like someone else when you yourself look better on you and feel better to you. Within the art and tradition of flamenco there is unlimited room for self-expression. All of the basics of the age-old traditional movements can be molded to individual personality if approached with that ip mpd. As you learn various facets of technique, for example, see and feel how they look on you. YES, it is It is very exciting and almost awesome in feeling when one truly realizes that he is a unique individual, a one of a kind creative expression of this universe as we know it. To copy is really not a short-cut, but just a copy, never as unique as the original. To lose oneself within is to truly find oneself; applied to flamenco, it is a very rewarding experience. To find your creative individuality with so vast an art as flamenco is a worthwhile search, a finding of a treasure within the soul. --Tec Morca GAZPACE DE GuilleRemo MORE BURIED TREASURE \"Buried Treasure\" was the title of an article about used flamenca records. (See Jalea issue of August, 1981, Vol. IV No. 12.) Since then I have continued visiting the used and \"recycle\" shops and have turned up these records of interest: \"Flamenco,\" Ethnic Folkways Library, Folkways Records FE 4437 (1956) \"Carlos Montoya, Aires Flamencos,\" Montilla FM-LD 10 (w. Triarita) \"A Dynamic Program of Authentic Flamenco,\" Los Flamencos De España; Somerset Album P-4100 (recarded in Denmark 1957) \"Guitarra Flamenca,\" Antonio Albaicín; Alhambra C 7039 reprint of original Columbia AL-7039 \"Flamenco Virtuoso,\" John Philip Lee; Reiffusion Stereo ZS 127 (1972) \"Flamenco Guitars,\" Juan Campalargo (Manolo Labrador); Request Records SRLP 10114 (1972) \"An American in Spain, Peter Evans\": RCA Stereo LP-3306 \"Jaime Grifão/Ninho Marvino, Dos Flamencos,\" Liberty LST 7147 (J. Fawcett and M. Walker) \"Flamenquistas,\" Stinson SLP 33, vol. 3 \"Olé Flamenco,\" Harmony (Columbia) EL 7015 \"Lecuona Plays Lecuona,\" RCA Victor LPM-1055 (1955, 1964) \"Flamenco Español,\" Capitol T10033 \"Flamenco, Curro Amaya,\" Somerset SF-12000 (w. Juan De La Mata and Domingo Alvarado) \"Dolores Vargas, El Terremoto Gitanc,\" Decca DL-74019 (w. Pepe Castellón and Sabicas)",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 913,
    "article_char_count_full": 5423,
    "article_char_count_review": 5423,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
