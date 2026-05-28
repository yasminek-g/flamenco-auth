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
    "article_id": "JALEO_1981_11::A6",
    "article_text_for_review": "SPANISH LESSON One of my favorite things about the Spanish language is the subjunctive mood. English does have subjunctive, but it is used far less than in Spanish. Let's observe some of the uses of subjunctive and see if there is any way to apply this to our appreciation of the music and lifestyle we love -- flamenco. The specific uses of the subjunctive that are the most interesting to me are the following. It is used after verbs of wanting, wishing, hoping, commanding, insisting, and verbs of emotion. For example: \"Espero que vengan.\" The word \"vengan\" is properly put into the subjunctive. This sentence is rendered, \"I hope that they come.\" The technical formula for any sentence of this type could be written: a) verb of wishful thinking or emotion b) que c) change of subject d) subjunctive mood in second verb. Especially the verbs of wishful thinking show a desire for control over events or other human beings. The subjunctive is a clever way to eternally remind us that wishing for something doesn't make it so. Consider the sentence, \"Espero que vengan.\" I say this to myself when waiting for students to show up for their guitar lessons. It never works to say it, even a hundred times. How does all this tie in to flamenco? Well, let me do it in story form. Once I went to a flamenco guitar concert with some flamenco friends. My friends were staunch followers of the great Diego del Gastor and were all guitarists. In the lobby before the concert, I observed on the face of each one an irritated look of displeasure. I asked, \"What's wrong?\" One said, \"I wish this were a juerga\" (See the English subjunctive?). Another said, \"I hope Diego del Gastor comes out when the curtain opens.\" Then I replied, in the indicative: \"The fact is that this is not a juerga, and Diego surely will not appear.\" Needless to say, the friends didn't enjoy the concert and looked for faults to attack. Well, what is the point of all this? Maybe it is this: Many times people do not do what we want, wish, demand, petition, order, or prefer: Especially flamenco people.",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "12",
    "page_number": 12,
    "word_count": 367,
    "article_char_count_full": 2069,
    "article_char_count_review": 2069,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A7",
    "article_text_for_review": "(from: $ \\underline{La} $ $ \\underline{Voz} $ $ \\underline{de} $ $ \\underline{la} $ $ \\underline{Frontera} $, Oct 3, 1981) Dos Bailarines Muertos en un Accidente de Tránsito TORREON, COAH. OCTUBRE 2 (EXCELSIOR).- En un accidente automovilístico ocurrido esta madrugada en la carretera Saltillo-Torreón, fallecieron los bailarines españoles Carmen de Mora y Félix Ordóñez y otros cinco integrantes de este grupo resultaron lesionados. Según las primeras investigaciones, el vehículo era conducido a exceso de velocidad y volcó en una curva cerca del sitio denominado \"La Cuchilla\". Los otros cinco artistas españoles lesionados fueron identificados como Gregorio Cortijo, Rosa María Morell, Francisco Izquierdo, Ricardo Quezada y José López. El Agente del Ministerio Público de esta ciudad, licenciado David Gómez, informó que los artistas viajaban en un automóvil último modelo rumbo a esta ciudad en dónde deberían presentarse en un espectáculo. (English translation) TORREON, COAH; Oct. 2 -- In an automobile accident that occurred at dawn on the highway between Saltillo and Torreón, Spanish dancer Carmen Mora and Felix Ordóñez died and five other members of the group were injured. An agent of the Ministerio Publico of that city, the lawyer David Gómez, said that the artists were traveling in a late model automobile toward this city (Torreón) where they would be presenting a performance. According to preliminary investigations, the vehicle was being driven at excessive speed and overturned on a curve near a place called \"La Cuchilla.\" The other five injured Spaniards were identified as Gregorio Cortijo, Rosa María Morell, Francisco Izquierdo, Ricardo Quezada, and José López. * * * CARMEN MORA Carmen Mora was born in Madrid, into a non-dancing family. She studied first with Ramón Ontín and began her professional career at age 15. With her own and other companies she toured throughout Europe and the Orient with much critical acclaim. In 1962 Carmen came to the United States with \"José Greco and His Spanish Ballet.\" She also was principal dancer with Alberto Lorca's \"Ballet Lorqueana.\" With her husband, Mario Maya, and the dancer El Guito, she formed \"Trio Madrid,\" a very popular dance trio that was awarded the \"Premio Nacional de Flamenco\" by the Cátedra de Flamencología de Jerez de la Frontera in 1971. In 1977 Carmen Mora was the featured bailaora with the \"Ballet Nacional Festivales de España,\" performing her tarantos and bulerías, as well as appearing in several classical numbers. With this company she came to the USA for the second time. Summer 1978 found her in California for what was to be a stay of almost a year. During that time she taught extensively in Los Angeles and San Diego, performed regularly at the El Cid restaurant, formed a company in early 1979 to present concerts in both Los Angeles and San Diego, and learned an impressive amount of English. Also during that time she was asked to go to Mexico City to do special performances for Spanish King Juan Carlos and President José López Portillo. CARMEN MORA -- SOME THOUGHTS by Paco Sevilla I was in contact with Carmen Mora at infrequent intervals over a period of about a year; I didn't know her intimately, so I didn't know all sides of her, mostly her professional personality. And yet there was so much that was unique about her -- even when viewed from such a narrow perspective. When I first met her -- she was coming to San Diego to give classes -- she didn't know anything about me; I was just a local gringo from San Diego, an out-of-the-way border town north of Tijuana. Yet she greeted me with respect and always addressed me as \"maestro.\" During classes she always took the blame when the guitar and dance did not go together properly, even when it was my fault! At the end of the class she shook my hand and thanked me. These may seem like small things, but in my many years of accompanying the classes of many different teachers, I had never been treated that way before, so it made an impression. At the end of that first class, she watched me getting into my dirt-encrusted, rusted, junk-filled 1965 Mustang and said, \"Es tuyo?\" When I said it was, she replied, \"Que gitano eres!\" And so my car became \"El Coche Gitano\" and to this day I think of her whenever the car gets really dirty (which is quite often). Carmen Mora was not only a memorable person, but she accomplished what every flamenco artist strives for and only a few achieve -- the creation of her own, unique and very personal style. Her style, which combined traditional Spanish movement with a strong gypsy element, modern dance, and even a touch of karate, was so personal that I felt that much of it did not look good when imitated by others. The style was so strong that it became a caricature if imitated too exactly. That does not mean that she was not a good teacher. Thanks to her generosity -- she taught her best material -- dancers all over the world will continue enjoying material that she taught them. CARMON MORA WHEN SHE WAS WITH JOSE GRECO IN 1962",
    "title": "CARMEN MORA DIES IN MEXICO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "poem",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 858,
    "article_char_count_full": 5047,
    "article_char_count_review": 5047,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A8",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(The following is the summary of a number of articles sent by Bob Clark of Ohio. They come from La Voz del Sur, Sept. 8, 1981, and Hoja del Lunes, Sept. 7, 1981.) by Paco Sevilla On Saturday, September 5, 47-year-old Fernando Fernández Monje \"Fernando Terremoto,\" was not feeling well when he performed in the festival in Ronda. But nothing could fore-tell what was to occur. His doctors said he was in good health and not suffering from his old hepatitis infection. On the way home from Ronda he was feeling badly, and three hours after arriving home at Calle Dolores, Number 10, in the \"Barriada Asunción,\" at six o'clock Sunday morning, Terremoto died of a heart attack in the bathroom of his home. Manuel Morao, who worked often with Fernando, said, \"He died singing -- it couldn't be any other\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"body\"]\n\nng about how Fernando had been feeling in previous days, Morao said, \"He was so special, so much his own person, with such a sense of propriety, that you never knew if he was feeling well or badly. He just was!\" The news was out immediately and spread rapidly by word-of-mouth, the only topic of conversation on that festive Sunday (The Fiesta de la Vendimia). Cantaores, bailadores, and tocaores went immediately to the house in Asunción to see the body of their deceased idol. Those who found themselves away from Jerez, returned as soon as possible. It is hard to think of any artist from Jerez who wasn't there. The house was filled with artists, friends, and relatives who came to offer condolences to the family -- Terremoto had three children. The vigil of Sunday was impressive and by dawn of Monday, there was room for no more people. The coming and going was incessant, but at any one time there were over five hundred people. It was incredible, but true -- as true as the gypsy genius of Fernando Terremoto. Monday morning the streets of the Asunción neighborhood were filled with over three thousand people, while the barrios of San Miguel and Santiago (gypsy neighborhoods) were practically deserted. The crowd was truly impressive, with women, children and old people crying as they recalled episodes in the life of the great cantaor... \"He was unique. When not performing he hardly ever left his barrio. He always used to play cards with us, with those who loved and idolized him. There was nobody like Terremoto.\" The small church could not hold everyone for the funeral. In the immense crowd could be seen the great artists of the cante -- Beni de Cádiz and his family, the Pansequitos, the Chiquetetes, the Turroneros, the...everybody you can think of, and representative\n\n[ENDING CONTEXT]\n\nguitarists are Juan Parrilla, Pepe Moreno, Gerardo Nunez and Antonio Jero. CARMEN AMAYA AT THE BEACHCOMBER, 1941 (p 16/17) (from: Picture Post, London, June 14, 1941; sent by Phil Coram.) The quality of these photos is very poor, but they are real collector's items. The caption on page 17 reads: \"To the Beachcomber Night Club in New York flock the bored, the indolent, the tired, the idle. They come to see the most amazing exhibition of vibrant agility New York has to offer--the spectacular gypsy dancing of a nineteen-year-old Spanish girl.\" Carmen was actually twenty-seven at the time.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "TERREMOTO DIES SUDDENLY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "15-18",
    "page_number": 15,
    "word_count": 1059,
    "article_char_count_full": 6132,
    "article_char_count_review": 3407,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "body"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_11::A9",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTERREMOTO TRIUMPHS IN FESTIVAL \"NOCHES DE LA IN PUERTO SANTA MARÍA (from: $ \\underline{ABC} $, July 8, 1981; sent by Gordon Booth; translated by Penelope Madrid) ...The organization showed understanding. The order of appearance of the artists on stage was decided by a drawing of lots. It was an experience that will serve as an example. It eliminated delays and gave a sense of order. The show was divided into two parts, each opened by the appearance of an aficionado. In the first, Manolo Simón from Jerez demonstrated that he is on the rise; his cante por soleá showed the unmistak-able stamp of his region. The enthusiasm reached its peak with the appearance of a top artist like El Lebrijano. In his generous offering, we recall best the bulería por soleá that he interpreted with the\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"pure\"]\n\ne of order. The show was divided into two parts, each opened by the appearance of an aficionado. In the first, Manolo Simón from Jerez demonstrated that he is on the rise; his cante por soleá showed the unmistak-able stamp of his region. The enthusiasm reached its peak with the appearance of a top artist like El Lebrijano. In his generous offering, we recall best the bulería por soleá that he interpreted with the uninhibited boldness of the most pure cantes de Lebrija. The performance of Camarón de la Isla followed, with the unmistakable stamp of the \"eco\" of his melodic voice; bulerías and fandangos brought down the house. María Vargas, with the enchantment of the extremely flamenco emphasis of her cante, delighted the audience. The exquisite cantaora from Sanlúcar succeeded in carrying the compás with the refinement and style of art that characterizes her. The first half had a rhythm of quality that continued to improve as it developed. Then purity broke out in an uproar. The presence of Terremoto, with his very gypsy manner of interpreting the cantes, stood out from the rest. Fernando never let down for an instant -- he had delivery, determination and was feeling \"a\n\n[ENDING CONTEXT]\n\nby Miguel Acal The past festival of Ecija lit the flame. And, sadly, the start of the festival could not have been more discouraging. When the show was over, we witnessed a haggling that should never happen again. It is obvious that, if the public fills the house, nothing unpleasant would happen, because expenses would be covered and a little money might be earned. But when there is not a capacity crowd, the artists shouldn't suffer the consequences. The organizers of ANTONIO MAIRENA AND PEDRO PEÑA EL POTAJE GITANO DE UTRERA (photos by Pozo-Boie) (photos by Pozo-Boje) ROMERITO DE JEREZ\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "JOSE GRECO IN SANTA FE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 1033,
    "article_char_count_full": 6080,
    "article_char_count_review": 2798,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "pure"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_11::A10",
    "article_text_for_review": "by Allan N. Pearson Six dancers, including the famed Jose Greco and Nana Lorca, two guitarists and one cantaor form the exciting \"Jose Greco Spanish Dance Company,\" currently performing a month-long engagement at the former Casa-blanca nightspot in La Fonda. The room is now designated El Greco Room in honor of Don Jose Greco, knighted by the Spanish government as \"Cruz de Caballero del Merito Civil\" for his worldwide contribution to the culture and performing arts of Spain. This dance show, both in quality and quantity, is the biggest and best show of its kind Santa Fe has seen in the last decade; and it suggests a new adage: \"Old flamenco dancers never die -- especially when they come to Santa Fe.\" Jose Greco has been in the business for a long time; so long, in fact, that one hears the question around town: \"Is he still dancing?\" Lay your doubts to rest! Not only is Greco still dancing, and dancing enthusiastically and well, but he has gathered around himself and his wife Nana Lorca some of the best young talent in the country. The whole show is an exquisite blend of refreshing youth and professional experience. It has an honesty to it that demands respect, and it provides an extremely entertaining evening of dance. The show begins slowly. Guitarists Carlos Lomas and Lorenzo Villa establish their technical mastery at the very start, as does cantaor El Pelete. (Both Lomas and El",
    "title": "FESTIVALES 1981",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "poem",
    "pages": "22",
    "page_number": 22,
    "word_count": 242,
    "article_char_count_full": 1402,
    "article_char_count_review": 1402,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
