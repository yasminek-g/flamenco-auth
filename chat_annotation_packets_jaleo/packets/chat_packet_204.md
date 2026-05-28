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
    "article_id": "JALEO_1985_01::A15",
    "article_text_for_review": "The twenty-third version of the \"Cursos Internacionales de Verano: Flamenco in Jerez,\" sponsored by the Cátedra de Flamencología de Jerez de la Frontera will take place this year from August 1 through the 17th. The dance course, taught by Teresa Martínez, will feature tangos and peteneras and cost 25,000 pesetas; in addition, Angelita Gomez will teach a course in bulerías, at a cost of 15,000 pesetas per student. Guitar--tango and bulerías--will be taught by Parila de Jerez and Pepe Moreno; cost: 25,000 pesetas. There will be nightly performances and lectures by various artists and flamencologists. Dance students will be divided into groups with two hours of teaching per group. Guitar students will receive twenty minutes individually. Reservations may be made by sending 5,000 pesetas in the form of a bank draft or money order. The remainder is due by July 1. Only those students who have some experience with flamenco that is, intermediate or advanced, will be accepted. Spanish will be the language of instruction. For reservations or information, write to: Câtedra de Flamencologia Apartado 246 Cale Quintos, 1 (Edificio Domecq) Jerez de la Frontera Spain, PACO PENA \"Live in Munich\" $14.95\\text{ us.}$ Postage & Handling U.S. and Canada - $1.50 Other Countries - $3.00 Guitar Studios, Inc. 40 Clement St San Francisco, CA, 94118",
    "title": "XXII INTERNATIONAL COURSES IN SPAIN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 29,
    "word_count": 215,
    "article_char_count_full": 1343,
    "article_char_count_review": 1343,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_01::A16",
    "article_text_for_review": "[from: Variety] by Bask The recently formed Ballet Espanol De Los Angeles, under the artistic direction of Roberto Amaral, brought some 40 dancers, singers and musicians onto the Wilshire-Ebell stage for two performances this past weekend. GUEST ARTIST LOURDES RODRIQUE2 GUEST ARTIST CRUZ LUNA GYPSY TRIO FROM BALLET ESPAÑOL DE LOS ANGELES (program photo)",
    "title": "BALLET ESPANOL DE LOS ANGELES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "29-30",
    "page_number": 29,
    "word_count": 54,
    "article_char_count_full": 355,
    "article_char_count_review": 355,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_01::A17",
    "article_text_for_review": "NEW YORK (NOVEMBER) Extraordinary months af flamenco presentations had their shawings here in New York. Pilar Rioja from Mexico had an extended season. A concert of the guitar virtuoso, Pedco Bacán...Mario Maya's are night portcaval of the plight of the gypsies in full company at the Carnegie Hall...La Tati considered by aome as the greatest boilora, an stage. To top it off Cumbre Flamenco flown specially ta New York for the Lstin Festival (included guitarist Serranito, bailaor El Giita, cantaor Enrique Morente and Familia Montoya)...and there is more... America's own Maria Beniter has seven shows projected at the Joyce Theatre on Eighth Ave., New York City starting next week. Just to show, how great an artist she is, she is paying a special tribute to José GRECO wha will be with Maria an stage. GRECO, the man who has given so much ta the Spanish dance, is returning ta Spain to form another company. Ms Beniter has been featured on the caver af this manths Dnce Magazine. Latest on Antonio Gadez is that he will be presenting \"Carmen\" and \"Bodas de Sangre\" at Carnegie Hall the beginning of 1985. Jasé Molina aftec the completion of his USA touc will be appearing at La Columbia Tampa, Fla.--Manolo Rivera is touring with Susans Hauser. El Msestro Mario Escudero will concertize on Feb. 3, 1985 at the YM-YWHA on Lexington Ave., his cantaor will be Luis Vargas, cantaor far Benitez (Augustine Strings, sponsor). Paco Peña will appear on December 2, 1984; guitar solos at Meckin Concert Hall, 129 W 67 Street, NYC (sponsored by D'Addaria strings). Peña will also play at Carnegie Recits! Nall May 13, 1985. His cantaor at that recital will be Chano Lobata. Philsdelphia's flamenca enthusiast Julia Cleerfield advises that the exceptional danter Orlando Romero, with cantaar Miguel de Cádiz and guitarist Carlos Rubio, will be appearing at the Don Quijate Restaurant on November 17 and 24, Philadelphia. Carlos Montoya concertizes at the Carnegie Hall, Nov. 24. Otherwise, on the local scene, the tableos are not active: Villa del Parral, Rincón de España, Mesón Asturias, have no flamenco; the Mesón still features guitarist Adonis Puertas; Mesa de España has guitarist Roberto Reyes.",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "article",
    "pages": "31",
    "page_number": 31,
    "word_count": 362,
    "article_char_count_full": 2196,
    "article_char_count_review": 2196,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_01::A18",
    "article_text_for_review": "FUEGO ESPANOL, II by Michael Fredrics The sensational Fuego Español, 11 Spanish dance concert was performed before a sell-out audience on December 15, 1984 at the Mayer Kaplan Jewish Community Center Theatre in Skokie, Illinois. The combined companies of Teresa y Las Preferidas, in residence at the Ballet Arts Studio of Wilmette, and the Northern Illinois Repertory Dance Company from the Theatre Arts Department at Northern Illinois University in DeKalb performed with a rare combination of polish and exuberance. In recognition of the fine quality of their concert work, partial funding for the performance was provided by grants from Productos La Preferida of Chicago and the Illinois Arts Council, a state agency. Both Act I and Act II featured dance works by the world renowned choreographers Maria Alba, Nana Lorca, La Tati, FUEGO ESPAÑOL A dramatic dance inspired by Federica Garcia Larca's play \"Yerma\" unfortunately involved little more than mapping and quarreling. And, although a program note stated that some acolara believe the ward \"flamenca\" derivez fram an Arabic expreaaian meaning \"fugitive peazant,\" the choreographer miaed the appartunity to have their program demonstrate how flamenco may have been affected by Middle Eastern influences. Instead, they provided simply an assortment of dances in two styles. Yet the dancing, accompanied by recorded scares and live muaic, was often lively. The musicians were Ricarda Amadar, Joe Zeytoonian, Hagi Tekbilek, Higuele de Cádiz, Kemal Ates and Harald Hagapisa. $ ^{*} $ $ ^{*} $ $ ^{*} $ SPANISH DANCE SOCIETY The Case de España of Washington DC and the George Washington University, co-spanared the Spanish Dance Society's production FIESTA ESPAÑOLA at the Marvin Theater in December 1984, featuring Marina Keet's \"stunning\" new ballet BDLERO, to an American composer Richard Trythall's percussion scare. [from: The Washington Paat, Dec. 17, 1984.] by Suzanne Levy Marina Keet is one of the unheralded treasures of the Washington dance scene. A choreographer and teacher here since 1981, she has a passion for Spanish dance and a mission to make everyone else passionate about it too. Keet has spent 35 years collecting the regional and classical dance of Spain, and it is Washington's fortune that she is disseminating them here. In her Fiesta Española at Marvin Theatre an Saturday, Keet presented bath traditional forms and original charea-graphic works, all stunningly rostumed. That she is foremost an educator was evident in her presentation of regional dances from Basque, Galicia, Andalusia, Castile, Old Castile, Estremadura and Catalonia. These dances praved a revelation in the richness and diversity of styles that fall under the rubric \"Spaniah.\" The old folk dances with their filigree of the lower legs seem closer to Celtic step dancing than ta flamenco, although the Spanish dances display a looser torso in their attention to epaulement. Diversity was also the impulse behind Keet's \"Gran Via,\" a colorful 19th-century Spanish street scene that served as the frame far demonstrations of a variety of dances by the patrons of a café. Keet's \"Bolero (Percussion Variations)\" is a stunning essay in rhythmic and visual counterpoint for 15 dancers. Originally choreographed for South Africa's Danza Lorca, the \"Bolero\" is a complex work in which badly and spatial patterns of circularity play against the hypnatic drumming score of Ameriran composer Richard Trythall. The exuberance of Keet's company proved winning. Gharo Linares, a guest dancer from London, brought her own considerable talents and expressive verve to the Aldeana, the Casteltersol, the Old Madrid Jota and, most particularly, the Rumba Finale. First-rate also was the musical support, particularly by guest flamenco guitarists Paco de Malaga and Manuel Racca, and singer Domenico Caro. ★ ★ ★ ROBINSON DANCERS TRUMPH [from: Rocky Mountain Newa, Jsn. 12, 1985] by Irene Clurman After being on the verge for a long time, the Gleo Parker Robinson Oance Ensemble showed definite signs of coming of age in Friday's concert at the Arvada Center. Unlike most of the company's concerts, the evening was programmed tightly, with no disappointing dead spaces. The trupe'a remarkable energy instesd was devoted to fair very",
    "title": "PRESS RELEASES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "32-33",
    "page_number": 32,
    "word_count": 666,
    "article_char_count_full": 4263,
    "article_char_count_review": 4263,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_01::A19",
    "article_text_for_review": "Adela Clara, founder of Theatre Flamenco of San Francisco, conducted a Master Class for beginning and intermediate Spanish dancers on November 17th, in the Lake Dswega, Oregon studio of Viviana Orbeck. The invitational session included soleares, alegrios, rumba, with emphasis on technique. Young guitarist Sheila Swoja played. This was the first of several special classes and workshops planned through the coming year by Viviana. In March, Adela Clara -- famed as soloist, inventive choreographer, teacher and lecturer -- returns far a 3-dsy Workshop, with limited registration. Flamenco, neo-classic dance and regional material will be presented. Dates, times and fee ta be announced later.",
    "title": "REVIEWSFLAMENCO IN OREGON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_01",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "34",
    "page_number": 34,
    "word_count": 103,
    "article_char_count_full": 693,
    "article_char_count_review": 693,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
