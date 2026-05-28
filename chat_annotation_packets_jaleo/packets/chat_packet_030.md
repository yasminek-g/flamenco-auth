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
    "article_id": "JALEO_1978_09::A3",
    "article_text_for_review": "(Part of a letter from Rodrigo de San Diego, a guitarist who lives and works in the Málaga area of Spain. See article on page 17) ...The man who's responsible for the new wave in flamenco is Camarón de la Isla. He's changed the singing -- all singing, including the Grecas and other modern singers. And he has changed the guitar because he's responsible for Paco's playing -- everything but the technique of course. When Camarón changed his singing, Paco changed his guitar aire to what it is now. He's also changed the dance because one dances to the verse or letra. But here's the interesting part; who's responsible for Camarón? Well, there is a gypsy family that lives in La Linea (Cádiz), very close to Algeciras. They are cousins of \"Los Habichuela\" from Granada. One of them, a man named Joaquín \"El Canastero\", around 40 or 45 years of age, developed the Moorish and melodic style of singing which Camarón has cleaned up and improved by having lived in Ceuta, Melilla, and Tangiers (Spanish cities in Morroco) for years. Also, a man called \"El Rubio de La Linea\" (a gypsy) also developed this style. Joaquín \"El Canastero\" is basically a letra writer, and the last news I heard about him was that he was in Paris selling material, clothes, etc., and was thrown out of his hotel for having run the faucet all night in his room (being high on hashish) because it inspired him to write some verses. Camarón has given little or no credit to him, which infuriates the family. I worked in a small club in Fuengirola for 3 months with Joaquín's son (a Jehovah's Witness like his father) and you sure can tell Camarón's influence. Very interesting experience! I played tangos and rumbas for three months; now I am satisfied to play solos at my own pace. Among the aficionados who are young and not professional, flamenco has few singers. Camarón is easy and a cheap, pretty way to sing, but it is not powerful flamenco singing, which is what affected me when I was 14-15 years old -- and it never will! El Oido is written by Deanna. If you have news of club members for this column, call Deanna at 277-6141 or drop a card to her care of Jaleo.",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 387,
    "article_char_count_full": 2143,
    "article_char_count_review": 2143,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_09::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Carol Whitney Copyright © 1978 by Carol Whitney All rights reserved (From the Editor) For those who are unfamiliar with the \"Morón phenomenon,\" a word of explanation, since two articles in this issue deal with flamenco in that area and assume that the reader knows something about it. Morón de la Frontera is a town in the Seville area. In the early 1960s it was made known to American flamencos through Donn Pohren's book \"The Art of Flamenco,\" in which he extolled the virtues of its resident, non-commercial flamencos. In Morón, flamenco was a way of life for a number of flamenco artists of very high caliber who did not perform in commercial surroundings (tablaos and concerts) nor make recordings, but lived by either working at other trades or from what they could earn in private juergas;\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"the great\"]\n\nAmerican flamencos through Donn Pohren's book \"The Art of Flamenco,\" in which he extolled the virtues of its resident, non-commercial flamencos. In Morón, flamenco was a way of life for a number of flamenco artists of very high caliber who did not perform in commercial surroundings (tablaos and concerts) nor make recordings, but lived by either working at other trades or from what they could earn in private juergas; these included Juan Talegas, the great singer of soleares and seguirias, Manolito de la María, and the guitarist Diego del Gastor (all of these people are now dead). Visitors to juergas in Morón often included people of the caliber of the Mairenas and Fernanda and Bernarda de Utrera With the appearance of the Pohren book began the pilgrimage of Americans to Morón, mostly guitarists to study with the great Diego del Gastor, whose unique style of playing has become known as the \"Morón style\". Later, Pohren opened a ranch in Morón where foreigners could come to experience and learn real flamenco. So the Americans came and went by the dozens, many of them taking home miles of tape recordings of the juergas and Diego's playing. Diego became somewhat of a \"God\" to many guitarists. DIEGO DEL GASTOR (photo by G. Tsuge) For more information on this subject one can consult Donn Pohren's $ \\underline{\\text{The Arte of Flamenco}} $ and $ \\underline{\\text{Lives and Legends of Flamenco}} $, and await his forthcoming book th\n\n[ENDING CONTEXT]\n\nand for what reasons? I had no way of knowing the answers. Sometimes, maybe, artists and aficionados overstate things. I found Diego, as reported, creative in an original sense. Flamenco, after all, follows certain rhythmic and modal structures; its framework is an ideal base for improvisation. Today's toque, like yesterday's, is based on what went before, and Diego's was of course based on toque he had heard. But his interpretations, extensions, tags, or entire falsetas, as well as his rasgueo, had a unity of character that made them his own. This unity is what people call propio sello.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Diego del Gastor: Flamenco Stories",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "5, 6, 7",
    "page_number": 5,
    "word_count": 1341,
    "article_char_count_full": 8035,
    "article_char_count_review": 3072,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "the great"
      }
    ]
  },
  {
    "article_id": "JALEO_1978_09::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Suzanne Keyser PART II - MORÓN DE LA FRONTERA We finally got to Morón, entering through the cement factory road, and immediately on arrival taking a room in the only pension (boardinghouse) in town, the Fonda Pascual. We take the room in the old part; a new part has been added since Chuck was here last; the fonda is aspiring to be a Holiday Inn. There is no water in the shower, but the landlady tells us not fret, that there will probably be some tomorrow between 9 and 11 A.M. when the city turns on the power again. Apparently there is always a shortage this time of year. So we try to improve the state of our ripe, traveled bodies as best we can, using water from the drinking jug. That afternoon, Chuck takes me up the cobblestoned hill to the plaza next to the old church, to Bar Pepe,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"Jaleo\"]\n\ntly there is always a shortage this time of year. So we try to improve the state of our ripe, traveled bodies as best we can, using water from the drinking jug. That afternoon, Chuck takes me up the cobblestoned hill to the plaza next to the old church, to Bar Pepe, the flamenco heart of town. As we climb the three steps into the bar, we immediately encounter Donn Pohren, with Ansoninni on one side sporting his silk ascot, and Joselero (see July Jaleo for article on Joselero) in his Sunday best on the other. They're recuperating from last night's fiesta over a quiet \"menta\" beside the pinball machine. Both gypsies are men in their sixties; both famous as flamencos -- Joselero is a singer, and Ansoninni is a dancer. Ansoninni is from Lebrija, one of the small towns in the area. The creme de menthe is for Ansoninni's throat, as too many cigarettes an ate nights have irritated his throat beyond the repairing capacities of Veterano cognac. Pohren, the faithful aficionado (author of the book Art of Flamenco) is encouraging them that, although there is one last fiesta tonight, the Feria week (which we missed, dammit) is over, and they can rest tomorrow (as if either of them really wanted to!). After a moment's hesitation, Pohren recognizes Chuck (it had been five years), and a moment later so did Joselero and even Ansoninni, with whom Chuck had had little contact. Introductions all around for Susana, and a round of menta, fino, or veterano. We all part company after a little while, only to return that evening. Up the hill again, Chuck's anticipation is almost unbearable - \"Will my old friends be there - will they remember me - what will the reception be - etc.\" The bar is buzzing with action, as is usual this time of night. A few villagers are recognized, and, as we are trying to relax with a glass of fino, Pepe, the bar owner calls out to someone on the step. In comes a young, lean gitano, with thick unruly hair, a white suit, frilly shirt, and a sparkle in his eye. The moment he lays eyes on Chuck, it's like Christmas morning, New Year's Eve, and his 21st birth\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_02 | trigger=\"voices\"]\n\nnursing his throat with his scarf wrapped around his neck. Also present are friends of the artists, relatives, and aficionados, sitting and standing around, exchanging pleasantries. An occasional burst of palmas and jaleo break out - Agustín tunes his guitar, plays a couple of chords, sets it down, and tastes a tapa that his mujer (woman) Tana (an American from San Francisco) has just brought in on a big tray. A verse to rumba is heard over the voices, and pretty soon everyone begins to feel the warmth of the wine. Agustín begins strumming por bulería, and the palmas beat more regularly; timidly at first, and then stronger, as more aficionados join in. The sound of the palmas sordas becomes strong, and Agustín plays with more gusto, with his typical Morón style inherited from Diego del Gastor; strong thumb technique and rasgueado, interspersed with whimsical, imaginative improvised and/or rem\n\n[ENDING CONTEXT]\n\nmade things a whole lot easier all around. Chica started to improvise little verses, and Milagro would improvise a step and encourage me to do the same; she cheered me on when I did something good, and scolded me (in a friendly manner) when I lost compás or did a particularly non-flamenco movement. as being the most fun rhythm around. Like António Gades says in the film \"Los Tarantos,\" \"Me voy a bailar la Bulería de la Gloria.\" When I was in Morón, I felt like those should be my last words too (I still do, when I hear Chuck practicing bullerías in the next room). (photo by Phil Watson)\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Dance Experiences in Spain",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "8, 9, 10, 11, 12, 13, 14",
    "page_number": 8,
    "word_count": 3599,
    "article_char_count_full": 20062,
    "article_char_count_review": 4684,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "Jaleo"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voices"
      }
    ]
  },
  {
    "article_id": "JALEO_1978_09::A7",
    "article_text_for_review": "Susana \"La Ceniza\" was inveigled into flamenco by a flamenco guitarist of ill repute (her husband, Carlos \"Las Gafas\"), and began with two years of intense training in compas and artistic survival. She began formal dance with a brief study with Mercedes Leon, and was helped by some very fine young artists, especially Sandra Fernandez (LA), María Teresa Carbajal, Roberto Gales, Milagro Ríos, and Marcela Del Real (Spain). She also studied briefly with María Magdalena in Madrid.",
    "title": "Suzanne Keyser",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 77,
    "article_char_count_full": 480,
    "article_char_count_review": 480,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_09::A8",
    "article_text_for_review": "(appeared in New York Times, Wednesday, May 17, 1978) By Jennifer Dunning The María Benítez Spanish Dance Company does not seem to perform for those aficionados with their Spanish dance. No attempt was made to survey the different styles of flamenco at a concert on Monday at the Spanish Repertory Theater. The heelwork and castenet-playing were articulated and subtle enough to provide the necessary dynamic variation in this harshly flamboyant gypsy dance form. But the softening influence of modern dance and ballet was evident in Miss Benítez's carriage and in the choreography she contributed to the program. What the company presented instead was pure theater; exciting, pleasant, bawdy, and dramatically lit, with an intimate give-and-take between audience and performers. Nowhere was this theatrical element more striking than in Miss Benítez's new two-part solo. \"Tribute to García Lorca,\" set to music by Rodrigo and Luna. With its simple stage patterns, its lyrical manipulation of a cape and almost breathless heelbeats, the dance evokes all the earthy melancholy of Lorca's famous elegy to his dead friend, the bullfighter Ignacio Sanchez Mejías, on which the solo was based. Miss Benítez was joined by the buoyant Manola Rivera and Victoria, a partner of such intensity that \"Asturias,\" a duet with Miss Benítez, had a delicately smoldering seldom seen in dance. The program will be repeated this afternoon and tonight.",
    "title": "Dance: Spanish Flavor",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "poem",
    "pages": "14",
    "page_number": 14,
    "word_count": 225,
    "article_char_count_full": 1433,
    "article_char_count_review": 1433,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
