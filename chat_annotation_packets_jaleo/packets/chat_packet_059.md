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
    "article_id": "JALEO_1979_12::A6",
    "article_text_for_review": "\"Absolutely! There has not been an interference in my private life by my artistic halo. Effectively, there are those who speak of me as if I were a myth. I don't believe that is so. In my case, I would be considered a living myth, which is doubly gratifying.\" --Genius and celebrity to the grave, Antonio has already chosen his funeral home; he already has his tomb in the cemetery of Sevilla. Why dig the grave now? Why choose Sevilla as the site of your eternal rest? \"Being from Sevilla, where else could I feel at home? From there I left and to there I wish to return. I have made my grave for one simple reason: I wanted to know for sure where I will be placed when I die. Imagine, if you died in a strange country and it took years to bring you back to your land. This would create for me a state of insecurity that has nothing to do with religion or metaphysics, I assure you. I like Sevilla a lot, especially her cemetery, and I have no other plan but to be buried there. You don't have to look for a special preoccupation with the theme of death in this decision. I do it for strictly'material' reasons. Also, I have the advantage of being able to place my mother in my tomb; she is temporarily buried in another niche of the cemetery.\" --Let's return to life. What does the baile of Antonio have in it from Andalucía? A critic has said that it is Andalusian in temperament and European in the rational, the rehearsal and study of technique. \"My dance is genuinely Andalusian, for the simple reason that it has all of its roots in Andalucía. What happens is that, since I was very young, back around 1936, I was in America for a number of years, and there I matured professionally, assimilating the concept of American technique, the manner of --Does the dancer, the artist, fear becoming involved with politics? \"Fortunately, I have little time. I don't lose any sleep over politics, and it doesn't interest me in the slightest. I don't dedicate myself to talking about politics. I lack political ideals and the only thing I do is admire those who do it best at a particular moment.\" --If an investigation were made into your personality -- almost all the investigations have emphasized, in the first place, your \"genius\", and, in second place, appear adjectives like \"controversial\" and \"argumentative\"... \"It is just that people can't live without talking about somebody. Until now, dancers have not been given the star treatment, that is, treated as famous. I have broken the spell and, as I have said before, I have, in a certain sense, dignified this profession. For many, what I do or say is as important as that which a movie star or pop singer does or says. I differ from previous dancers in that I am not only an artist and artistic, but also popular. If I go on a trip, they photograph me; if I don't go, they photograph me anyway.\" --Your bachelorhood is one of the most well-known and widely discussed facets of Antonio. Does it have some significance in relation to your work? \"Absolutely! My bachelorhood is not really an artistic choice, but that is as good a reason as any. Antonio, 'dancer', could have been perfectly married, a widower.\" --You have almost always been artistically oriented toward the classical, although always with the desire to reinnovate it, to renew it. Today there is rejection by this generation of the classic, the traditional. Does that worry you? \"All lovers of the art should worry about everything that endangers this tradition. But first I want to make clear that I don't focus basically on the classical. In fact, the show that I am going to present in my two farewell seasons is authentically flamenco, based exclusively on guitars, cantaores, and spoken poetry. It is a show that has nothing to do with the classical; it is a creation, an advanced image of the future of the new flamenco presentations. Of course, in this clearly flamenco atmosphere, there exists some classical choreography adapted to the rhythm demanded by flamenco.\"",
    "title": "ANTONIO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "26-27",
    "page_number": 26,
    "word_count": 708,
    "article_char_count_full": 3996,
    "article_char_count_review": 3996,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_12::A7",
    "article_text_for_review": "(from: $ \\underline{\\text{The New York Times}} $, Oct. 19, 1979; sent by George Ryss) by Raymond Ericson Sabicas, the Spanish gypsy guitarist, will be performing solo, as he always does, on October 21 at 8:00 in the Avery Fisher Hall in Lincoln Center. It will be his first New York performance in a year and a half, his Lincoln Center debut and a welcome opportunity PROGRAM <table><tr><td>1. HOMENAJE A GRANADA</td><td>Granadina</td></tr><tr><td>2. RAPSODIA FLAMENCA</td><td>Farruca</td></tr><tr><td>3. MANATIAL GITANO</td><td>Soleares</td></tr><tr><td>4. NOSTALGIA CASTELLANA</td><td>Castellanas</td></tr><tr><td>5. INSPIRACION</td><td>Nuevo Son Flamenco</td></tr></table> INTERMISSION <table><tr><td>6. TORREMOLINOS</td><td>Malaguena Siglo XIX</td></tr><tr><td>7. VARICONES DE ALEGRIAS</td><td>Alegías</td></tr><tr><td>8. RECUERDO A CARMEN AMAYA</td><td>Garrotín</td></tr><tr><td>9. FANTASIA ARABE</td><td>Danza Mora</td></tr><tr><td>10. SEMANA SANTA EN SEVILLA</td><td>Seguirillas</td></tr></table> INTERMISSION 11. VERDIAL MALAGUÊN...............Verdiales 12. PIROPO A GALICIA.....Aires Gallegos 13. RITMOS GITANOS.....Tientos Zambra 14. CANA DE AZUCAR.....Guajira Flamenca",
    "title": "SABICAS IN NEW YORK",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "28-29",
    "page_number": 28,
    "word_count": 116,
    "article_char_count_full": 1179,
    "article_char_count_review": 1179,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_12::A8",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Michael E. Fisher (from: $ \\underline{\\text{B.M.G.}} $, September, 1955; sent by Michael E. Fisher) Whilst in Madrid this summer (1955), I took the opportunity of visiting again, Marcelo Barbero, world renowned maker of flamenco guitars. As I called on him frequently, I was able to watch many of the stages in the construction of a flamenco guitar. Perhaps readers will be as interested as I was in the process of construction which in many respects was both surprising and fascinating. I had never realised that the back of a guitar is attached $ \\underline{\\text{after}} $ the front and sides have been fixed to the neck or that the last thing to be glued on is the bridge! Each of Marcelo's guitars is individually hand made in the tradition of the great masters such as Domingo Esteso,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"known\"]\n\nfixed to the neck or that the last thing to be glued on is the bridge! Each of Marcelo's guitars is individually hand made in the tradition of the great masters such as Domingo Esteso, Ramírez and Santos Hernández, all of whom also worked in Madrid. Since I saw him last Marcelo has taken on an assistant, but even now he personally wood one wants for the face. The reserved pieces proudly display the pencilled names of the customers -- guitarists known and unknown all over Spain. Ideally the grain should be very fine and straight; complete freedom from knots is, of course, essential. When viewed end-on the grain should cut perpendicularly through the face. The closeness of the grain usually varies across the piece of wood which is so cut that the coarser regions are towards the sides of the guitar. When glued the roughly-cut face is thinned to correct thickness and sandpapered smooth. The correct shape of the guitar is outlined on it in pencil -- about a $ \\frac{1}{4} $ inch is left to spare all around. The position of the sound-hole is marked and further concentric circles are drawn to indicate the position of the decoration that will surround it. The diameter of this decorative ring is about 5 inches whilst the hole is about $ 3\\frac{1}{2} $ inches in diameter. Dimensions such as these are standard in Spain and are observed by all guitar makers. THE MOSAIC The decoration is quite remarkable in itself and, although it adds nothing to the performance of the guitar, it is most pleasing to the eye. It consists of an elaborate mosaic of small coloured squares and strips of wood inlaid into the face of the guitar. In a typical pattern there may be as many as 4,000 separate bits of wood! Needless to say these thousands of pieces are not placed in one at a time! In fact the method of construction is most ingenious. Flat strips of wood (less than 1 mm. thick, but an inch or two wide) are throughly dyed to various bright colours -- blue, black, red, green. The strips are then carefully glued together in different orders to make a multi-coloured 9 or 10 layer \"sandwich.\" Fine slices are cut off this sandwich across the layers -- each slice being as thick as the original strips, but being\n\n[ENDING CONTEXT]\n\nand extra tools are kept for the more difficult art of repairing guitars. The last important step in the construction is the fitting of the bridge. This is fashioned in rosewood in the traditional \"Spanish style\" form. The distance between 1st and 6th string is set at slightly less than $ 2\\frac{1}{4} $ in. The bridge is centred with precision and firmly glued in position. Over the years the lengths of the open guitar strings have become longer. Marcelo favours a string length of a full 26 in., about $ \\frac{1}{4} $ in. longer than that used by Esteso and Santos Hernandez. FINISHING STEPS\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "THE MAKING OF A BARBERO GUITAR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "article",
    "pages": "30-34",
    "page_number": 30,
    "word_count": 1303,
    "article_char_count_full": 7412,
    "article_char_count_review": 3833,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "known"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_12::A9",
    "article_text_for_review": "by Bill Regan Someone once said that when you hear flamenco for the first time, it all sounds the same. Then, after a while, you get to the point where you are able to tell the difference between soleares, siguiriyas, etc. Then, after a few years, it all begins to sound the same again. The truth is that some people get bored and either start to play other kinds of music or draw on their creativity within flamenco. So we have a new wave of creativity in flamenco in Spain, led by people like Paco de Lucía, Camarón, Lebrijano, Manolo Sanlúcar, Enrique Morente, Enrique Melchor, Paco Cepero, and Niño Miguel. Not everything new is good, but neither is everything old. Here are my thoughts on two new records: \"LA LEYENDA DEL TIEMPO\" (Philips 63 28 255) Camarón de la Isla with El Tomatito and Raimundo on guitars. This record, Camaron's tenth not counting re-releases or single tracks, was quite a surprise since it seemed so different. It was the same feeling as when I heard \"Almoraima\" for the first time. The combination of cante with electric guitars, flamenco guitars, organ, sitar, drums, palmas, and baile, was tastefully done, but it took about three listenings for me to accept it. It was interesting to hear Camaron without Paco de Lucía. Quality musicians and recording make this record a must for the record collector. Camaron, after having recorded nine discs of cante, is not going to sit back and sing the same cantes with different letras for the rest of his days. A good way to describe it is, \"doble cara hacia adelante y hacia atras.\" Camaron has not forgotten the roots, but doesn't want to get caught in the rut of cliche either. The letras for this record come from García Lorca material. \"PERSECUSION\" (Philips 91 13 004) -- Juan Peña \"El Lebrijano,\" with Enrique de Melchor and Pedro Peña. Excellent record! Incredible vocals by one of the best. Again the recording was well done and the musicians were high quality. Lebrijano's emotion pours forth as he sings the shocking letra. It's obvious that he means what he says and is not singing trite cuples. The theme of the record is the persecution of the",
    "title": "RECORD REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "poem",
    "pages": "35",
    "page_number": 35,
    "word_count": 374,
    "article_char_count_full": 2130,
    "article_char_count_review": 2130,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_12::A10",
    "article_text_for_review": "(From: $ \\underline{\\text{The}} $ $ \\underline{\\text{Denver}} $ $ \\underline{\\text{Post}} $, Oct. 19, 1979; sent by Candace Bevier) By MAX PRICE Flamenco guitarist Rene Heredia has performed with a flutist, a cellist, a violinist and with an orchestra. During his current engagement at the Slightly Off Center Theater he is strumming up a flamenco storm with a percussionist, Bataki Cambrelen of New York, known here for his appearances at the Bonfils Theater and with the Cleo Parker Robinson Dance Company. At their performance Thursday night, Heredia and Cambrelen put on an impressive demonstration of musical communication. Wherever Heredia led, Cambrelen followed; the artistry of each complementing the other. There was no program. Since Heredia is appearing in an extended run at the Slightly Off Center Theater, he preferred it that way, allowing him to vary the pieces according to his mood. He warmed up with a rhythmic clapping number and then moved quickly into his own \"Gypsy Jam\" that underscored the links between flamenco and jazz. A piece with an Arabian flavor provided a nice change of pace, and then Heredia picked up the tempo again with a lively treatment of the familiar \"Malagueña\" and a song from \"Black Orpheus.\"",
    "title": "HEREDIA PROVIDES ARTISTRY PLUS STRUMMING UP A STORM",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "36",
    "page_number": 36,
    "word_count": 199,
    "article_char_count_full": 1239,
    "article_char_count_review": 1239,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
