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
    "article_id": "JALEO_1983_02::A10",
    "article_text_for_review": "As of the printing of this issue, the February juerga information has not been firmed up. We are hoping to have the cantaor, Manuel Agujetas, who is visiting the West Coast join us. A post card with juerga information will be sent to California Subscribers or call 440-5279.",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_02",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "27",
    "page_number": 27,
    "word_count": 48,
    "article_char_count_full": 274,
    "article_char_count_review": 274,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_02::A11",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHere is a list of back issues of Jaleg and their contents. Only major articles are listed. Each issue also contains such things as letters to the editor, Punta de Vista, Morca Sobre el Baile, Gazpacho de Guillermo, Lester DeVoe on Guitar Care, concert, record and music reviews, flamenco dictionary and juerga reports from around the country. Order through the Jaleo P.O. Box and send: $1.00/copy for Vol. I, Nos. 1-6; $2.00 for all other issues through Vol. IV; $2.50 for each copy of Vol. V. We are running out of some issues and do not plan to reprint. (Add $1.00 copy for overseas mailing.) Stresses, and Compas; Facio de Lucia - Revolution of Flamenco; Casi Gitano - Morca; Dancing in Spain; Diego del Gastor; Flamenco Stories. No. 4: Café de Chinitas; Senores, When?; Lole y Manuel; Diego del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"Purist\"]\n\n. 1-6; $2.00 for all other issues through Vol. IV; $2.50 for each copy of Vol. V. We are running out of some issues and do not plan to reprint. (Add $1.00 copy for overseas mailing.) Stresses, and Compas; Facio de Lucia - Revolution of Flamenco; Casi Gitano - Morca; Dancing in Spain; Diego del Gastor; Flamenco Stories. No. 4: Café de Chinitas; Senores, When?; Lole y Manuel; Diego del Gastor; Flamenco Stories. No. 5: The Juerga; Flamenco: For the Purist Its a Ritual, Not a Spectacle; Esteban de Sanlúcar; Compás por Solea; Diego del Gastor; Flamenco Stories. No. 6: Bulerías; Daniel Maya, Raquel Reyes in Spain; Bulerías...The Most Elusive Rhythm; Llamadas and Desplantes; Paco de Lucía; A Flamenco for the Music of Falia. No. 7: Camarón de la Isla; Pepe de la Matrana; Caló - A Dying Language; Interview with Ramón M�ntcya. No. 8: La Zambra; Renald Radford; Enrique El Cojc; Reviews of Mario Escudero; Matruja Vargas; Teo Morca & Carmen Morca. No. 9: Granada;\n\n[ENDING CONTEXT]\n\nManola Sanlúcar Summer Guitar Course. $ B_{a}, 12 $ Cacharito de Málaga (autobio. letter); Teo Morca (interview plus photos); Index Vols. 1-1V, Nos. 1-12 each; More on Syncopation; Marina Keet: South Afrika's Lass--America's Gain (olo.); Sanchez Perria: Composer of Famous Themes, but an Unknown; A Masterful Lessor by Maestro Sabicas. Expires 9/30/83 G U I T A R S T R I N G S COMPLETE SETS Specify Black or Clear trable Retail $11.00 SPECIAL $6.00 Minimum order $12.00 Postage Paid California residents add 6.5% sales tax Mabe checks payable to: Lester DeVne-Guitarmaber Box AA, San Jose, CA 95151\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "BACK ISSUES OF JALEO AVAILABLE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_02",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "28-29",
    "page_number": 28,
    "word_count": 1074,
    "article_char_count_full": 6944,
    "article_char_count_review": 2592,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "Purist"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_03::A1",
    "article_text_for_review": "[Special thanks to George Ryss for making this article possible.] Libby Komaiko Fleming was born in Chicago, Illinois, and is the daughter of Musicologist/Pianist Robert Komaiko and pianist Dorothy R. Komaiko. Libby began her formal dance training at the age of ten with Elisa Stigler, Director of the dance program at the Chicago Musical College of Roosevelt University, Chicago, Illinois, studying classical ballet and character dance. Her childhood theatrical education includes studies in tap and jazz dance, drama, instrumental and vocal music, theatrical history, technical design -- sets, lighting, costuming and make-up artistry. She began her performance career at the age of thirteen in the United States' number one rated Evanston Township High School Musical Theater Program. She was extremely active throughout her four years at the school in dance, drama, musical and theatrical productions as choreographer, dancer, corps member, technical designer, crew and stage director. Upon graduation from high school, Ms. Fleming was appointed Assistant to Elisa Stigler and inaugurated her teaching career. Intensification of her studies in Spanish dance, music and culture ensued during this period. In March 1969, she became the recipient of the José Greco Scholarship. This scholarship included personal training by world renowned artists José Greco, Nana Lorca and Paul Haakon and culminated in professional performance. The following season, upon the invitation of the department chairman, she majored in dance at the nationally acclaimed dance program of Butler University. Travel and studies throughout Spain and additional study and work with Lola Montes followed.",
    "title": "LIBBY KOMAIKO FLEMING",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "3",
    "page_number": 3,
    "word_count": 248,
    "article_char_count_full": 1679,
    "article_char_count_review": 1679,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_03::A2",
    "article_text_for_review": "BE A JALEO CORRESPONDENT One of the aims of Jaleo is to keep its subscribers posted on what is going on in the flamenco community. Not having funds to station correspondents around the country, we must depend on you, the readers, to help us gather and report this information. We need concert dates, newspaper articles and performance reviews, who is currently performing and teaching in your area, where dance and guitar supplies may be obtained. If you enjoy writing, you might consider interviewing a local personality, writing juerga or performance reviews, sharing a personal experience or a trip to Spain, etc. To be listed as a \"CORRESPONDENT\" for your area (there may be more than one) we need an update at least every other month. To be listed as a \"contributing writer\" of the staff we need a commitment for six articles a year. At this time we wish to welcome a new columnist, guitarist Ken Sanders, who will be offering tips on guitar technique and Paul Durbin on dance and other contributors to this issue; Juana Ballardo from San Diego; \"The Shah of Iran\" -- New York; Ron Spatz -- Los Angeles; Fatri Nadir -- Northern California; George Ryss -- New York; Marina Keet -- Washington, DC and others. --Juana De Alva",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 213,
    "article_char_count_full": 1227,
    "article_char_count_review": 1227,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_03::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nVIDEO CASSETTE Dear Jaleo, Peter Baime has produced a second video cassette even more spectacular than the first! This one has the professional touch of lead-in scenes/music, fade-in/fade-out, and subtitles. Mr. Baime was dressed in his concert attire this time with a very colorful background. The program started with a \"Serranito\" alegrias (por rosas) followed by a \"Sabicas\" farruca, both played first at performance speed and then slowed down with close-ups. Very enjoyable for learning or just plain entertainment. The next phase was the teaching of various rumba strum patterns from elementary to complex, concentrating on the right hand but including some left hand tips. This seemed to be his \"specialty\" although his playing was excellent throughout the tape. Again he ended with a little\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"public\"]\n\nin entertainment. The next phase was the teaching of various rumba strum patterns from elementary to complex, concentrating on the right hand but including some left hand tips. This seemed to be his \"specialty\" although his playing was excellent throughout the tape. Again he ended with a little party (this time it was Christmas) celebrating the hard work that had been completed. Peter indicated that he would make some video tapes for sale to the public in the near future. Yours truly, Bill Brinda Huntsville, AL FLAMENCO DANCE CLASSES NEW BEGINNER CLASSES FOR CHILDREN AND ADULTS ONE BLOCK OFF 3DTH NEAR FWY 94 IN SAN DIEGO CALL JUANA (619) 440-5279 instructor of dance on the faculty of the Department of Music at Northeastern Illinois University and has the unique distinction of an academic degree in Hispanic Dance. Ms. Fleming has studied in Spain and the United States with Maria Alba, Nana Lorca, José Greco, Lola Montes, Elisa Stigler, Paul Haakon, Manolo Vargas, María Magdalena, Pedro Azorín and Ciro. Ms. Fleming has choreographed the majority of her company's extensive repertoire and is known for\n\n[ENDING CONTEXT]\n\nCarter and Fleming sizzled. The program also brought two attractive newcomers to the spot-light who clearly deserve a place in this company. An important yet unsung member of this company is Ann Rosi, who serves as the manager, costume designer and wardrobe mistress. Ms. Rosi is a standout in all of these areas. Her costumes -- a fashion show in themselves -- are all exquisitely executed, filling the stage with stunning textures and colors and enhancing the moods of each dance. If variety and excitement in dance are what you're looking for look no further -- Ensemble Español offers it all.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 1399,
    "article_char_count_full": 8762,
    "article_char_count_review": 2739,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "public"
      }
    ]
  }
]
```
