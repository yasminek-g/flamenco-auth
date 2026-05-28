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
    "article_id": "JALEO_1982_04::A11",
    "article_text_for_review": "(from: ABC, Dec. 19, 1981; sent by Gordon Booth; translated by Paco Sevilla) One of the most ambitious recording productions to be attempted in Spain to date, has been completed. Based on an idea by the cantaor Juan Peña Lebrijano, Manolo Sanlúcar has composed the first Andalucian opera based on the Gospel according to Saint Matthew. Manolo is the producer, composer, director, and performer, along with the singer, Rocio Juardo, of this \"Gypsy Gospel\" that presents all of the poetic wealth inherent in being Andalucian.",
    "title": "EL EVANGELIO GITANO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 20,
    "word_count": 85,
    "article_char_count_full": 523,
    "article_char_count_review": 523,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_04::A12",
    "article_text_for_review": "(from: ABC, Dec. 19, 1981; sent by Gordon Booth; translated by Paco Sevilla) The old \"Posada de Potro,\" an example of the civil architecture of the 14th Century, has again opened its door in the center of Córdoba. It is now converted into a center for popular culture...For an opening, the new \"Sala Municipal de Arte\" is presenting an exhibition entitled \"Flamenco in Present Day Art,\" in which more than twenty well-known artists are participating. Among the artists: Francisco Moreno Galván, Pepi Sánchez and Juan Valdés from Sevilla, Antonio Bujalance, Juan Hidalgo del Moral, and Ángel López-Obrero from Córdoba, Eduardo Carretero and Miguel Moreno Romera from Granada, Juan Gutiérrez Montiel from Jerez, Manuel Mingorance from Málaga, Fausto Olivares and Antonio Povedano from Jaén, along with Venancio Blanco, Antonio Campillo Párraga, Joaquin García Donaire, Hipólito Hidalgo de Caviedes, Elena Lucas, César Montana, Gregorio Prieto, José Torres Guardia, and Francisco Zueras. All of those painters and sculptors, both figurative and abstract, express with their art, the emotion of that indefinable duende that hides and liberates our arte flamenco. An example is this oil painting, \"Por Bulerías,\" by Juan Valdes. The government of Córdoba has published a collection of the art works on exhibition. It has an introduction by Antonio Gala and notes by Luis Quesada. ROCIO JURADO MANOLO SANLUCAR \"POR BULERIAS\" \"COLO",
    "title": "EL FLAMENCO EN EL ARTE ACTUAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 20,
    "word_count": 220,
    "article_char_count_full": 1424,
    "article_char_count_review": 1424,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_04::A14",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFLAMENCO FOR NON-FLAMENCO DANCERS I have often suggested to dancers who dance flamenco that they study other forms, such as ballet, for control, for feeling of using space, for line in relation to performance focus. I have also written a few past articles with suggestions relating to the study of other facets of dance that would enhance the total art of flamenco dance. In this article, I would like to talk about what flamenco dance can do for dancers whose primary focus is in other styles of dance, such as various styles of modern dance, ballet, jazz or any other performing dance art. I am often asked to teach master classes in universities, festivals, private schools, and other situations where I would be teaching non-flamenco dancers, and I am invariably asked what they could possibly\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"experience\"]\n\nco dance. In this article, I would like to talk about what flamenco dance can do for dancers whose primary focus is in other styles of dance, such as various styles of modern dance, ballet, jazz or any other performing dance art. I am often asked to teach master classes in universities, festivals, private schools, and other situations where I would be teaching non-flamenco dancers, and I am invariably asked what they could possibly gain from the experience of studying the movements of flamenco dance. Also, I am often told that \"The dancers do not have flamenco boots, shoes, or castanets, and, anyway, what will they ever do with footwork stomqing and castanet clacking?\" When I answer that there will be a minimal amount of footwork, that they do not need castanets and that bare feet are fine, as we will be working on the \"essence\" of flamenco movement, I am often stared at in amazement, as this cliche of \"stomping feet and clacking castanets\" has almost reached every earer of the non-flamenco dance world, and that is a large part of the dance world! \"The essence of flamenca movement\" usually gets me to first base, but what is this essence that can indeed enhance the all-around dancer? What can I make them aware of besides the obvious? I feel that the obvious basics that can be learned fran flamenco are an exciting posture, a posture that is the essential ingredient of \"stage presence,\" a use of the back, arms and torso that is unique to flamenco, but essential to any study of using your upper body in an expressive way, no matter what dance form. The flowing movement of the torso, leading with the torso into other movements such as turns, leading with the upper part of the body and following through with the lower part of the body, is one element. Another is the coordination and isolating movements in flamenco that are unique for their totality, especially where there is desire for extreme opposition in movement. Walking is one of the most difficult facets of movement to master, stage walking, that is; flamenco \"dance walking\" has got to be the most beautiful, powerful and sensual walk, when done well. This style of walking as a learning experience is another obvious advantage of studying flamenco dance. One of the most important obvious advantages of studying flamenco is an important way of using energy, steady sustained energy, so that there is a feeling of \"non-gravity\" in movement. You float over the dance floor, y\n\n[ENDING CONTEXT]\n\njumping at the opportunity and buying up the supply. The Bureau must be astounded at the popularity of that item because they ksep restocking it. This is the only flamenco anthology that is still commercially available and when it goes off the market it will mean tbs end of a whole flamenco era for the aficionado who does not own any of the old anthologies. To order, send $12.99 plus $2.25 handling (and appropriate tax if you live in N.Y. or N.J.) to: Publisher's Central Bureau Department 124 1 Champion Ave. Avenel, NJ 07131 Ask for the record set, \"History of Cante Flamenco,\" item #S436D1.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "20-21",
    "page_number": 21,
    "word_count": 1260,
    "article_char_count_full": 7428,
    "article_char_count_review": 4089,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "experience"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_04::A15",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFLAMENCO IN SOUTH AFRICA Guitarist Michael Fisher, of New York, just returned from the Cape Town area in South Africa and sends us the following newspapers. He adds that the motivating force behind this program is South Africa's Mavis Becker, who danced professionally with Luisillo's company. Michael adds to the newspaper review with a few comments. In reacting to the criticism of the local dancers in a jota aragonesa, he says, \"...the jotas aragonesas were danced with spirit by all and in good style by quite a few dancers. They were refreshingly traditional, since the rest of the program was, to my mind, overly \"modern!\" Some pieces, such as \"Llanto\" were set to modern electronic music. About the review, he writes, \"What the reviewer totally fails to mention -- perhaps she was quite\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"known\"]\n\nfreshingly traditional, since the rest of the program was, to my mind, overly \"modern!\" Some pieces, such as \"Llanto\" were set to modern electronic music. About the review, he writes, \"What the reviewer totally fails to mention -- perhaps she was quite unaware of it -- was that the 'flamenco section' was most untraditional, indeed merely 'impressions of flamenco' -- the impressions being far removed from the real thing.\" José Antonio became well-known during his years with the companies of María Rosa and Antonio. With his wife, Luisa Aranda, prima ballerina with Antonio for ten years, he formed the company \"Siluetas\" that was known for its \"modern\" stylings. Rafael Aguilar is a Spanish choreographer who created \"El Rango\" for the Ballet Nacional de España in 1979, \"Retrato de Mujer\" for Manuela Vargas in 1981, and now \"Lanto\" for the performance reviewed here. Emilio de Diego is known internationally for his guitar playing; the program lists him as: \"Guitar virtuoso, professor, and composer, who left a permanent engagement with Gade's company to devote himself to composition. Paco el Lobo is a guitarist/singer who has his own Paris based company, \"Horizontes.\" $ ^{*} $ $ ^{*} $ $ ^{*} $ SPANISH STARS IN CT (from: The Argus, Jan. 22, 1982, sent by Michael Fisher) by Glynnis Underhill Cape Town's\n\n[ENDING CONTEXT]\n\nchoreography, tones down the garish lighting effects, finds buttons that stay buttoned and zippers that stay zipped -- he'll command as much audience respect as excitement. Saturday, however, his venture represented a last-minute victory of basic talent over monumental self-indulgence and bad taste. * * * FLAMENCO SUPREME A premium string designed especially for the top line of flamenco guitars—the choice of many leading guitarists, classical as well as flamenco. At your local dealer or contact Antonio David Inc., 204 West 55th Street, New York, N.Y. 10019 — (212) 757-3255 and (212) 757-4412.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "poem",
    "pages": "22-23",
    "page_number": 23,
    "word_count": 1592,
    "article_char_count_full": 9895,
    "article_char_count_review": 2938,
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
    "article_id": "JALEO_1982_04::A16",
    "article_text_for_review": "IN SEARCH OF THE PERFECT JUERGA SITE The March juerga was the second in our new location on Market Street. We were hoping for a larger crowd, but unfortunately, we were in conflict again with a Casa de España function the following day. We missed many of our Spanish friends who add so much animation to our juergas. The thirty-five members and guests who participated seemed thinly spread in the numerous rooms at our disposal. Los Angeles guitarist, Yvetta Williams, came down bringing Japanese flutist, Yuri Yashida, and classic guitarist, Stephen Jones. Besides playing classic duets with Stephen, Juri picked up some sevillanas on the spot and accompanied some of the dancing. Another new face was Manuel Estrada, a student of Juanita Franco. He came early and pitched right in to help set-up. Guitarist Louis \"Luís\" Hendricks returned after three years absence and dancer Victor Gill is really developing his style under the tutelage of Roberto Amaral. The \"ayudante\" system does not seem to be working very well. It appears that people would rather pay an entrance fee than commit themselves to help in some capacity. Our thanks to Rafael Díaz who came early to set-up, manned the bar almost all evening and stayed to clean-up at the end of the juerga. A poll was taken during the evening of members present to see if our new site was acceptable. Feelings seemed pretty evenly split between those who preferred our new site on Market to those who preferred private homes. The big advantage of a permanent site to those of us who put on the juergas is that it is much less work -- no transportation of boards or supplies, no furniture to move and clean-up is easier. (Of those who preferred private homes, no one felt that they could handle the transportation of juerga boards.) It is true that if our turnout continues to be so small we will be lost in the spaciousness of the Market Street site. (We do have a tip on a smaller site that we plan to check out.) We will continue to be on the lookout for alternatives and will appreciate your suggestions. EN BUSCA DEL LOCAL FERFECTA La juerga de Marzo fue la segunda en el nuevo local en la calle Market. Esperábamos tener un grupo más grande pero",
    "title": "MARCH JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "24",
    "page_number": 25,
    "word_count": 387,
    "article_char_count_full": 2203,
    "article_char_count_review": 2203,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
