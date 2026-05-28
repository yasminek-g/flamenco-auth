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
    "article_id": "JALEO_1981_06::A7",
    "article_text_for_review": "(from: Blanco y Negro, Sept. 1979; sent by Manuela de Cádiz) At the age of ninety, the great bailaora and actress, Pastora Imperio, has died (1979). Pastora made her debut in 1902, when she was thirteen years old. Her artistic name was given to her by Don Jacinto Benavente after seeing her dance, although during the course of her long career, she had several nicknames -- \"La Giralda de Sevilla,\" \"La Faraona,\" etc. Her success was as great in films as on the stage; she appeared in a number of films that found immediate success -- \"La Danza Fatal,\" \"María de la O,\" \"La Marquesona,\" \"Canelita en Rama,\" \"El Amor Brujo,\" and \"Duelo en la Cañada,\" the last filmed in 1959. Pastora died on Friday, September 14, the result of heart failure. With her passing went one of the best bailaoras in the Spanish arts. PASTORA IMPERIO: CASI UN SIGLO DE ARTE",
    "title": "OF ART",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_06",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 152,
    "article_char_count_full": 849,
    "article_char_count_review": 849,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_06::A8",
    "article_text_for_review": "In 1974, while I was a student of the classical guitar, I heard my first flamenco guitar concert. I fell in love with the music Mariano Córdoba played that night. It was something I never heard before and I was attracted to the rhythms and the feeling of the music.",
    "title": "LESTER DEVOE ON GUITAR CARE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_06",
    "year": 1981,
    "language": "en",
    "article_type": "article",
    "pages": "19",
    "page_number": 19,
    "word_count": 50,
    "article_char_count_full": 265,
    "article_char_count_review": 265,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_06::A9",
    "article_text_for_review": "Donn Pohren is going to conduct a sixteen day flamenco tour of Andalucía -- from September 3rd to the 18th (1981). It is an opportunity to experience flamenco in its native habitat under the experienced guidance of author-guitarist Pohren. Travelling in a Mercedes van that will hold nine people, the tour leaves from Madrid (and ends there) and will visit scenic and historic sites, flamenco bars and penas (clubs) throughout Andalucía. There will be two nights dedicated to the flamenco contest and festival of Mairena del Alcor (among the most serious flamenco events of the season), and three days in Jerez during the grape harvest celebration (vendimia) with its associated flamenco activities -- including entrance into flamenco casetas. Throughout the tour, there will be extensive contact with flamenco artists and aficionados. The cost of the tour is 43,000 pesetas (as of April 1981, about $500) and includes transportation, accommodations (double room, private baths) and tour guidance. Not included: food, drinks, entrance fees, or other incidentals. To make reservations (haste is advised), contact: Society of Spanish Studies Calle Victor Pradera 46 Madrid 8, Spain.",
    "title": "SEPTEMBER FLAMENCO TOUR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_06",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "20",
    "page_number": 20,
    "word_count": 183,
    "article_char_count_full": 1180,
    "article_char_count_review": 1180,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_06::A10",
    "article_text_for_review": "by Caballero Bonald PART VI - MAIRENA DEL ALCOR translated by Brad Blanchard Mairena del Alcor is a small, shining pueblo surrounded by olive and orange orchards. The plaza is like a patio with its baseboards painted rose and indigo and a central garden filled with fragrant flowers. The name \"Mairena\" suggests a phonetic effect of beach and sea, seen later on a horizon that seems to cap some unsuspected marine landscape. But all that is a product of the imagination. We are in the heart of the agrarian plains of the Andalusian basin. Mairena del Alcor is proud that Cervantes should remember it in $ \\underline{\\text{El coloquio de los perros}} $ (a novel); a carved stone gives witness to the fact: \"And before daybreak I was in Mairena, which is a place four leagues from Sevilla\" (from $ \\underline{\\text{El coloquio}} $). We arrived in the middle of the afternoon, with a furious sun beating against the white village. It was Antonio Mairena who took us to his pueblo. Antonio is, naturally, the favorite son of Mairena. His ample popularity as a cantaor could only be produced in places like this, inhabited by almost only one numerous family and where even daily life is like a humble communal expression. In more populous places, one is never a grand cantaor for the many, but rather for those nuclei of aficionados who always constitute a more or less ample minority. But Antonio Mairena, to his pueblo, is a symbol of artistic lucidity and a permanent source of pride. It is impressive, when one is not used to it, to see him cross these wide, flowered streets followed by the respect and admiration of all the people. We believe that, within the always restricted popularity of an authentic cantaor, it would be difficult to repeat a similar scene in any other corner of Andalucía. A FINE SELECTION OF GUITARS at the American Institute of Guitar With what is probably the largest selection of Classic and Flamenco Guitars in New York City, Antonio David has established a sales office at the American Institute of Guitar, 204 West 55th Street, New York, N.Y. 10019. • Telephone (212) 757-4412. Read interviews with Segovia, Tomas, Romeros, Pujol, and many more. Find out about instrument builders, festivals, competitions, and master classes. Play our new music and lute tablature. Find out what is happening around the world in guitar and lute through- Before arriving in Mairena del Alcor we had made a special call in Dos Hermanas, a step away from Sevilla and another from Utrera. Juan Talega was waiting for us in a modest bar near the plaza. Although a resident of this agricultural area that borders the marshes of the Guadalquivir -- \"nazarenos\" they are called -- the cante and the life of Juan Talega are linked to Alcalá de Guadalra. Dos Hermanas, in spite of its location in the geographic cradle of flamenco, belongs to other expressive spheres. Juan Talega, now almost in his eighties, is the son of Augustín Fernández, a great but anonymous cantaor of the past century, and nephew -- as was Manolito el de María -- of Joaquín el de la Paula, the unforgettable craftsman of the soleares which bear his name. Heavily built and proud, with noble gypsy bearing, he represents to perfection the so often mentioned and almost lost group of great cantaores that can be found in their native regions. Juan Talega in this sense is an exceptional example. Faithful preserver of the old styles of Alcalá and Triana, of Jerez and Utrera, he is one of the two or three greatest present day exponents of the cante -- when considered according to its truest and most rigorous historical roots. Talega is an ultimate example of dramatic clear-sightedness and expressive wisdom. Each one of his cantes constitutes a supreme lesson in sobriety, in pathos, in the exact measuring of the compás, and in emotive tension. No one today could give us more direct and precise human and artistic data than that which was given to us by this faultless heir of the most illustrious branch of the gypsy creators of Alcalá -- that of the \"houses\" of the Talegas and the Paulas. His cante is the expression of his life. When the day comes that he can no longer sing, a whole important cycle of the history of flamenco will have been closed. guitar & lute Magazine 1229 Waimanu Street Honolulu, Hawaii 96814 Send for Free Brochure. $2.00—sample copy, $10.00-4 issues",
    "title": "ARCHIVO: PART VI",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_06",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 752,
    "article_char_count_full": 4364,
    "article_char_count_review": 4364,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_06::A11",
    "article_text_for_review": "1. Oxygen: Try to listen to one side of a flamenco record without breathing. You won't make it, excluding hyperventilation and 45 rpm. 2. Physical Freedom: If you are bound with ropes you will not be able to be on time for the sevillanas at the nightclub. 3. Warmth: Try hitch-hiking on the Autobahn in Germany in January. After one half-hour without getting a ride, I'll guarantee you will temporarily forget about flamenco. 4. Water: You can go without food for a while, but water is needed in some form daily. Try a competition fast between water and flamenco to see which you go for first. 5. Food: For the body to move its muscles and function in all ways nourishment is a prerequisite. Side two starts with \"Despertar,\" a rondeã. Synthesizer, cymbals, and drums open the piece and play for a whole minute before the guitar enters. It is a long and drawn out rondeã, with much repetition and monotony. Monotony is to some extent part of the hypnotic attraction of flamenco however. Halfway through this eight-minute rondeã it starts to pick up with a 3/4 beat. The whole group joins in the repetitive melodies until the end of the piece. The tarantas, \"Serena Calma,\" begins with cymbals, jingle bells, and the sounds of wooden percussion instruments. The guitar does a traditional introduction to this nice tarantas. This must be something more recent for Diego, since he didn't play tarantas when I met him, at least not to my recollection. Even Diego del Gastor didn't play tarantas, but rather the tarantos, which is not the same thing, especially with regard to the cante. The second bulerías is called \"Recovecos.\" This is interpreted much faster than the other, but still with a good Morón aire. The record ends with \"Sueños Rotos,\" a media granaína. Once again this must be something Diego picked up in Madrid, since few in Morón would play such things. He does give it a Morón sound -- until the synthesizer joins in. Then he does traditional type granadinas until the end of the record. What is it about this record that is hard to accept? For me it is the primitiveness of the Morón style, mixed with the futuristic synthesizer interpretations. Of course, it all depends on what the word \"jondo\" means to the listener. If it means a closeness to the earthy events of life, such as birth and death, then the trained flamenco listener may be disappointed by some things on the record. However, if the listener accepts that even non-flamenco people can be close to the earth in their own way, there may be tolerance, if not acceptance, of non-flamencos. As I have said in several previous reviews, most hard core flamencos will not appreciate this kind of record. On the other hand, as most acid trippers and peyote button poppers would assure us, the earthiness of birth and death is common to all human beings. Flamenco has no monopoly on true \"duende.\" Flamenco has no monopoly on true \"duende.\" --Guillermo Salazar",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_06",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 509,
    "article_char_count_full": 2931,
    "article_char_count_review": 2931,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
