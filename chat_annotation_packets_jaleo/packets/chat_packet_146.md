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
    "article_id": "JALEO_1982_07::A4",
    "article_text_for_review": "JEREZ DE LA FRONTERA The Catedra de Flamencologia de Jerez de la Frontera notifies us that the annual \"Festival Flamenco en Jerez\" -- including master classes, recitals, concerts and movies -- will take place from August 23rd to September 6th. It is our understanding, from the information provided, that the entire course, including room and meals, is 15,000 pesetas ($150.00). For more information write to the Excmo. Ayutamiento de Jerea de la Frontera, SPAIN. SCHEDULE OF EVENTS August 23 - Inaugural Glass \"The Cante Greats of Jerez\" by J.M. Caballero Bonald Master Classe on cante de Axietae de Jerez August 24 - Master Glass on Dance by a professor to be determined August 25 - Master Class on Guitar by Manuel Cacao August 26 - Recital of Andalusian Flamenco Poetry by Pepe González August 27 - Open Air Performance Flamenco Jerezano August 28 - Cantes y Bailes of Carmellys Montoyn and Family August 30 - Movie Forum including zhowingz and lecture August 31 - Guitar Class and concert - Parrilla de Jernz Septem 1 - Gante Glass and recital - Beni de Cádiz Septem 2 - Dsence Claas and recital - Solera de Jerez Septem 3 - Open Air Performance Flamenco Jerezano Septem 4 - Possible concert or recital Septem 6 - Grnduation and presentation of diplomas XVI Fiesta de la Bulería Septem 7&8 Tablaos Flamencos de la Vendimia del Sherry A premium string designed especially for the top line of flamenco guitars—the choice of many leading guitarists, classical as well as flamenco. At your local dealer or contact Antonio David Inc., 204 West 55th Street, New York, N.Y. 10019 — (212) 757-3255 and (212) 757-4412.",
    "title": "FESTIVALES 1982",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_07",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 276,
    "article_char_count_full": 1614,
    "article_char_count_review": 1614,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_07::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Ron Spatz Can'til-late (kan'ti-lāt) To recite by intoning or chanting. -- Funk and Wagnalls Some cantaores sing, others cantillate. Chinin de Triana cantillates. Whether performing cante jondo with just the properly timed silence, or cante chico with a fast shuffle across the stage at the perfect moment, Chinin dues it with class. He can sound one moment like a muezzin calling the faithful to prayer...the next like Pavarotti doing Wagner. Talent with class is the best way I know to describe this fiery little gypsy. Add enthusiasm--on stage or off, he never seems to be shut down. I've never met a person more consistently wired. Chinin was immersed in the ambience of flamenco his entire life. Jose Greco discovered him in Madrid and immediately signed him into his company. He was featured\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nlike Pavarotti doing Wagner. Talent with class is the best way I know to describe this fiery little gypsy. Add enthusiasm--on stage or off, he never seems to be shut down. I've never met a person more consistently wired. Chinin was immersed in the ambience of flamenco his entire life. Jose Greco discovered him in Madrid and immediately signed him into his company. He was featured on several global tours with Greco, then moved on to perform with many of the flamenco greats of this century -- Vicente Escudero, Mario Escudero, Estebán de Sanlúcar, Carlos Montoya, Jerónimo Villarino, and many others. He has appeared on network television shows -- including Ed Sullivan, Arthur Godfrey, Steve Allen, and Camera Three at Carnegie Hall. He has recorded for Occa, Fantasy, and Folkways records. Chinin has made his home for many years in the Los Angeles area, in and around Hollywood, which by his own admission he \"loves the best.\" One rainy January afternoon, I sat in Chinin's cozy Burbank apartment, sipping coffee and asking questions, surrounded by walls papered with flamenco memorabilia. Strains of flamenco music (what else?) permeated the atmosphere. The following interview is the result of that enjoyable afternoon. JALEO: How did you become interested in the cante? CHININ: My father was also a singer. He used to go to the local ferias and perform. As a boy, I would accompany him as often as possible. This gave me an opportunity not only to listen to my father, but to all of the local gitanos as well. At this time there were many great cantaores such as Manolo Caracoi, Pepe Marchena, La Niña de los Pienes and her brother, Tomás Pavón. These artists influenced me very much, and I studied their styles very seriously. JALEO: Why did you decide to become a professional? CHININ: Naturally, I started singing, myself. All the local people, neighbors, etc., told me that I had a very good voice and that I should seriously seek a career as a cantaor. JALEO: What were some of your first professional performances? CHININ: Well, I started performing locally. Then I went to Madrid where I worked in several tablaos. I joined a group performing on the radio. The program was called \"Piesta En El Aire.\" From there we travelled all over Spain, performing in all the theaters and radio stations. JALEO: Why did you decide to come to America? CHIHIN\n\n[ENDING CONTEXT]\n\nwith a most interesting stage personality. Miss Garcia was most successful with the more abandoned type of number in which she could fling herself excitedly around the stage. In the quieter gances, her movements seemed a lot stúrd. PUERTAS and Palacios were as good as any Plamenco guttatosis I've ever heard and father more inventive than many of them. The one weak spot was when Puertas strayed out of this pasture and gave us a harsh, insensitive reading of the Ahenz \"Leyenda\" —Jack Loughner. S. F. News-Call Bulletin CHININ WITH GUITARIST JUAN PERRIN AND CLARITA IN REHEARSAL WITH JUANA ESCOBAR\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CHININ DE TRIANA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_07",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "6-12",
    "page_number": 6,
    "word_count": 2233,
    "article_char_count_full": 13368,
    "article_char_count_review": 3987,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "many"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_07::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPASCUAL OLIVERA AND ANGELA DEL MORAL (from: $ \\underline{\\text{Dancemagazine}} $, Feb. 1980, sent by Irina Campbell) by Ann Barzel Pascual Olivera and Angela del Moral are probably the most popular dancers in the Chicago area. Audiences invariably greet their work with \"olés\" and rise in standing ovations, as they did at the performances I attended in the First Theater in Chicago and the Drake Theater in Lake Forest. Their program has the intrinsic color and excitement of Spanish dance, also the drama and humor, plus the attractiveness of the dancers' outgoing personalities. To the proud carriage of the national dance style and the authenticity of their dances, they add ease and nimbleness. Ballet training shows in del Moral's exquisite footwork and in Olivera's swift turns and elevation\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"CLASSIC\"]\n\nhe drama and humor, plus the attractiveness of the dancers' outgoing personalities. To the proud carriage of the national dance style and the authenticity of their dances, they add ease and nimbleness. Ballet training shows in del Moral's exquisite footwork and in Olivera's swift turns and elevation in jotas. The pair is versed in several categories of Spanish dance and the program currently Flamenco Guitars For Sale 1962 JOSE RAMIREZ SPRUCE TOP CLASSICAL GUITAR STAMPED \"PB\" EXCELLENT CONDITION $2,800 1949 MARCELO BARBERO FLAMENCO, PEGS, CLEAR GOLPEADORES, OUTSTANDING FLAMENCO SOUND $2,400 1974 MANUEL VELAZQUEZ SPRUCE TOP CLASSICAL GUITAR, SIGNED, EXCELLENT CONDITION $2,000 ITINERARY Each seminar consists in two weeks intensive work on the guitar. There will be two sessions each day (not Sunday): from 10.30 to 1.00 and from 3.00 to 5.00. Flamenco comprises of a number or toques (styles or forms) most of which are played a compés i.e. with a strict rhythmic structure, whilst some are libre, i.e. with a more free flowing rhythmic structure. The course will cover the following: 1) The campás i.e. the time in flamenco music. 2) The falseta (variation). 3) Besic toques a compás: Saleares, Seguiriγas, Tientas, Buterías. 4) Other toques related to the basic toques: Alegrias, Serranas, etc. 5) The Fandangos family: Toques Libres, i.e. Fandangas, Malagueñas, Tarantas, etc. and Taques a campás, i.e. por H\n\n[ENDING CONTEXT]\n\nand friendship in an evening of flamenco music and fun. Our host and owner of Mexico City Restaurant, Jimmy Jauregui, has graciously consented to remain open late for our August juerga. Come as early as you like and enjoy an excellent Mexican dinner before the juerga. The juerga will begin about 8:45pm when most of the regular dinner customers are gone. The bar and food service will be available during the evening. No reservations. Large parking lot. The restaurant is located at 1147 South St., Long Beach. For more information call Ron Spatz 213/883-0932 or Yvetta Williams 213/833-0567.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CONCERT REVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_07",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "13,14,15,16,17,18,20",
    "page_number": 13,
    "word_count": 2111,
    "article_char_count_full": 14235,
    "article_char_count_review": 3041,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "CLASSIC"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_07::A8",
    "article_text_for_review": "IN APPRECIATION OF THOSE, WHO THROUGH THEIR CONTINUEO CONTRIBUTION, KEEP JALEO GOING. This month we wish to acknowledge PENELDPF MADRID for her continued contribution to Jaleo. Penelope serves in several capacities; as Correspondence Secretary -- answering the myriad of letters, requests or complsints; as Distribution Secretary -- keeping records updated, sending out renewal notices, forwarding updated information to Tony Pickslay who produces the mailing tables; as Advertising secretary -- corresponding with and billing out advertisers. All this, and more, she does for the preservation of Jaleo and her love of flamenco -- accepting not a penny. How she manages to squeeze all this in, along with running a household, hunting for a job, practicing the guitar and taking her daughters to dance classes, remains a mystery.",
    "title": "ANDA JALEO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_07",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 126,
    "article_char_count_full": 828,
    "article_char_count_review": 828,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_07::A9",
    "article_text_for_review": "NEWS FRDM OUR MEMBERS Working in New York as of April 27th: Dancers Estrella Morena and Manolo de Córdoba with guitarists Pepe de Málaga and Emilio Prados at the Flamenco Room of the Chateau; dancers Hara Sultani and Liliana Moralea with guitarists Pedro Cortez and singer Paco Ortiz at El Rincon de España, Friday and Saturday, B2 Beaver St., 212/344-5228; guitarist Paco Juanas and Paco Montes (pop and flamenco singer), and dancer Mari Carmen Rubio at El Castellano Restaurant, 79th St. and Roosevelt Ave. in Queens, Friday, Saturday, Sunday. A pena has juat been opened on 14th St. between 7th and Bth Avenues for flamencos and friends. Diego Castellon and Miguel Cespedes (Arrieta) are usually there Friday and Saturday; its called \"Piedmonte.\" (from Lillana Morales) Evanston, ILL: Ridgeville Cultural Arts Program offers fismenco classes year round with Teresa and master classes with Edc Cie, Victoria Korihsn, Nana Lorca, Maris Alba ett. For further information call: 312/869-564D. (from Jane Cole) San Diego, CA: Dancer Marlene Gael and guitsrist/singer Carlota Hernandez will perform July 3rd at the Wing Cabaret, 2753 B Street in Golden Hills at 8:00 and 10:00pm. Vegetarian food - $3.00 cover, phone 280-464B. (from Kathy Kajimy) Paco de Lucia will be in the States part of September and October touring with pianist Chic Corea. (from Ken Sanders) Read interviews with Segovia, Tomas, Romeros, Pujol, and many more. Find out about instrument builders, festivals, competitions, and master classes. Play our new music and lute tablature. Find out what is happening around the world in guitar and lute through- guitar & lute Magazine 1229 Waimanu Street Honolulu, Hawaii 96814 Send for Free Brochure. $2.00—sample copy, $10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.00—$10.0",
    "title": "EL OIDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_07",
    "year": 1982,
    "language": "en",
    "article_type": "poem",
    "pages": "19",
    "page_number": 19,
    "word_count": 275,
    "article_char_count_full": 5781,
    "article_char_count_review": 5781,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
