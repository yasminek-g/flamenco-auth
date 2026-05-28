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
    "article_id": "JALEO_1992_04::A2",
    "article_text_for_review": "IN NEW MEXICO... 6th Annual FESTIVAL FLAMENCO 92 presented in conjunction with UNM Center for Regional Studies, Center of Southwest Culture, Eva Encinias and Ritmo Flamenco CONCERT, June 25, 26, 27, 28 WORKSHOPS, June 15 - 24, college credit available A) 9:00-10:20 - Intermediate Flamenco, Jose Greco II, $200 B) 10:30-11:50 - Escuela Bolera, Jose Greco, $200 C) 10:30-11:50 - History of Flamenco, Eric Patterson, $200 D) 12:00-2:00 - Flamenco Guitar, Pedro Cortez, $200 E) 12:00-1:50 - Flamenco Costuming, Pablo Rodarte, $200 + material F) 2:00-4:00 - Advanced Flamenco, La Tati, $375 G) 4:00-6:00 - Flamenco Repertory, La Tati, $375 H) 6:00-7:20 - Beginning Flamenco, Pilar Serrano, $200 I) 7:30-9:30 - Flamenco Song, Dominico Caro, $200 YOUTH WORKSHOP, June 15 - 19, Age 13-t8 Y) 9:00-12:00 - Beginning Flamenco, Joaquin Encinias, Juanito, $100 SEVILLANAS COMPETITION: JUNE 19 Open to beginner through advanced dancers.",
    "title": "Solera Flamenco Dance Company",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 143,
    "article_char_count_full": 923,
    "article_char_count_review": 923,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1992_04::A3",
    "article_text_for_review": "IN CANADA... Studio Flamenco will hold a summer school July 20 - August 1, 1992 in Calgary, Alberta with guest teacher Claudia Carolina. Three levels of instruction for adults will be offered as well as childrens' classes. In addition, guest guitarist Peter Knight will conduct a guitar workshop July 24 - 27, 1992. Contacy Marilyn Malinsky (403) 283-2785 to receive further information in write Studio Flamenco, 2221 6th Ave. N.W., Calgary, Alberta Canada T2N 0X1. FLAMENCO FENCING ORNAMENTAL IRON Safeguard Fence, Inc. Serving North County (619) 745-4846 CA Contractors's Licence #374198",
    "title": "Dance Reviews",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 91,
    "article_char_count_full": 589,
    "article_char_count_review": 589,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1992_04::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTHE SOLERA FLAMENCO DANCE COMPANY DEBUTS IN LOS ANGELES On Sunday, May 24, 1992 the Solera Flamenco Dance Company will make its Los Angeles debut at the Fountain Theatre, having had to cancel its scheduled May 3 performance in the wake of the recent Los Angeles riots. Under the artistic direction of dancer Yaelisa and guitarist Bruce Patterson, this performance will feature guest artists Jose Valle \"Chuscales\" and Roberto Amaral. Other artists include singers Antonio de Jerez, Marisol Fuentes and Pilar Moreno, and dancers Lourdes Rodriguez, Linda Andrade, and Cynthia King. The Solera Flamenco Dance Company, San Diego's first and only Flamenco dance company, was founded in 1990 to promote and present the art, dance and music of Spain in its most popular form, Flamenco. The Company made its\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"grant\"]\n\ne work based upon a poem by Federico Garcia Lorca. Under the directorship of dancer and choreographer Yaelisa and Musical Director Bruce Patterson, the Solera Flamenco Dance Company intends to focus its presentations on Flamenco dance theatre. The \"new wave\" in Spain today, dance theatre is becoming increasingly popular in the U.S. In 1992, the Company was recognized by the Commission on Arts and Culture of San Diego and received its first major grant. The Company has recently been quite active in the Southern California area. Several sold out performances in April at the Better World Galleria in San Diego were followed by appearances at the \"Together Again\" AIDS benefit at the Civic Theatre alongside much of California's top talent, a performance at the Barclay Theatre at UC Irvine, and scheduled performances in July as part of the Kaleidoscnpe Dance Festival at California State University, Los Angeles. One of the more interesting recent projects is the premiere of a work conceived and choreographed by Yaelisa and dancer/choreographer John Malashock of Malashock Dance & Company. The piece, titled \"Laberinto del Caballo Verde\", will be premiered on May 15-17 at the Don Powell Theatre, San Diego State University as part of the Malashock Dance & Company Spring season. A collaboration rather than a fusion of choreographic styles and dance genres, \"Laberinto\" utilizes a blend of modern dance, traditional flamenco dance and highly stylized flamenco movements. An integral part of the work, the musical accompaniment ranges in form from traditional to stylized modern flamenco, using the music of Manolo Sanlucar, Enrique Morente and Bruce Patterson. Here is an excerpt of an article in the San Diego Union by Anne Marie Welsh, Dance Critic, Sunday, May 10, 1992: ... Yaelisa and Bruce Patterson came for jobs in Tijuana at a new flamenco club, Corral Sevillano, and soon opened a combined flamenco studio-art gallery at Eighth and K streets. Both projects met sad ends. \"The little club was doing very well, but one night some kids brought a gun onto t\n\n[ENDING CONTEXT]\n\nthe outside in.\" Her biggest influence, she says, is her guitarist husband. \" He encouraged me to look for my own way. I realized I could put to use all the years I spent as a child listening to Indian music, the music they brought to Spain, that was influenced by the character of the Andalusian people, by the Moors, by Jewish secular music. The gypsies are the originators of flamenco. The dancers and singers are tremendously rhythmic.\" Explaining the name of her company, Yaelisa said, \"When you make red wine in Spain, the drop that is the essence, the thing that starts the wine is solera.\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Book Reviews",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "poem",
    "pages": "5",
    "page_number": 5,
    "word_count": 1141,
    "article_char_count_full": 6845,
    "article_char_count_review": 3697,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "grant"
      }
    ]
  },
  {
    "article_id": "JALEO_1992_04::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nReview, March 26, 1992 The New York Times FUSING SPANISH AND LATIN AMERICAN STYLES By Jackie Anderson The Carlota Santana Spanish Dance Company, which opened a week's eogagement at the Joyce Theater on Tuesday night, is interested not only in dance in Spain, but also in the way the Spain's dance traditions have spread throughout the Americas. The group devoted the entire first half of its program to \"El Encuentrn de Dos Mundos\" (\"The Encounter of Twn Worlds\"), a trilogy that attempted to show the fusion of Spanish and Latin American styles. \"Ecos de Plata,\" choreographed by La Meira, was the most powerful of the three pieces. This duet for Miss Santana and Gabriel Heredia, a guest artist with the group, blended Spanish dancing with steps recalling those of the Argentine tango. The\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"gesture\"]\n\nthat attempted to show the fusion of Spanish and Latin American styles. \"Ecos de Plata,\" choreographed by La Meira, was the most powerful of the three pieces. This duet for Miss Santana and Gabriel Heredia, a guest artist with the group, blended Spanish dancing with steps recalling those of the Argentine tango. The choreography also had some of the austerity of early American modern dance. There was a sense of suppressed erotic violence in every gesture by Miss Santana and Mr. Heredia, and they appeared to be locked in a battle that neither would every totally win. Nevertheless, when Miss Santana stole Mr. Heredia's hat, she suggested that she had robbed him of a symbol of power. \"Aires del Caribe,\" choreographed by Mannio Rivera, who also performed as a guest dancer, attempted to be a comie sketch about a Caribbean cafe. Much of the choreography was oddly subdued. In contrast, when Mr. Rivera made an effort to be jaunty in the role of a country humpkin his antics looked forced. But Miss Santana made a good impressinn as an amusingly icy chaperone, and Mr. Heredia was convincing as a gapper fellow who took obvious pride in being a man about town. La Conja's \"Brisas Andinas\" paid tribute to the folk traditions of the Andes. It was a festive piece, although one wished the production had been given a setting that would have made it appear unmistakably Andean. Spain inspired the works on the second half of the program. Watching Miss Santana's brooding solo \"Quehrantos\" was like taking a peep into the deepest recesses of somenne's mind and heart. Miss Santana was surely contending with invisible private demons. But when she raised an arm in defiance, it was clear that solitude had not driven the woman she portrayed into total despair. The evening concluded with \"Tablao Flamenco,\" a suite that included solos by La Meira, Aurora Reyes, La Conja, Mr. Heredia and Mr. Rivera. Unfortunately, the stage was so heavily amplified that the sounds of the musicians and the dancers' footwork, instead of being rhythmically crisp and clear as the ought to be i\n\n[ENDING CONTEXT]\n\nThey have even given credence to the fact that even though flamenco has roots, it is changing as aoy living thing will. It is an art form that almost demaods some appreciation of its history. This book and cassette are better than well done. They are simply excellent. It was prepared with love and respect. The Press at CSU, Fresno is to be complimented for its contribution to the literature of flamenco. The price they are charging represents a value of real sigoificance to those of us interested in building out knowledge of the art and its place in history. -- Contributed by Homero Cates\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "\"Juerga Sevillana\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 1543,
    "article_char_count_full": 9006,
    "article_char_count_review": 3691,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "gesture"
      }
    ]
  },
  {
    "article_id": "JALEO_1992_04::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n\"JUERGA SEVILLANA\" Felix del Valle, 1940 Translated by Eric Patterson In crystal thimbles, the living golden water of the vine. Hands that are dedicated to \"palmas,\" making rhythms with a chatter of quick, nervous, maddening claps. There is a sort of rapture in the atmosphere. I am enveloped, enclosed completely in this flamenco circle. I am a fluid within the fluid. And I know no way to make it concrete or to escape it. If I think of some form of liberatinn, it disappears at once. It is intoxication -- not only from wine but also from witchery. It is the total nullification of the personality which has been built up through social norms: it is to be laid here, without additions or artifices. Something foreign and confusing fits and conforms me to this powerful zigzagging of sensuality,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"imitat\"]\n\nn -- not only from wine but also from witchery. It is the total nullification of the personality which has been built up through social norms: it is to be laid here, without additions or artifices. Something foreign and confusing fits and conforms me to this powerful zigzagging of sensuality, which comes from the cloudy glance of the \"cantaor\" to the foot of the dancer -- a hammer of satin which underlines the rage, accenting the desperation and imitating the twisting of a single wing in the violent fall of a wounded bird. When the \"juerga\" arrives at its culmination, it becomes full of its own character: hizarre flourishes, anxieties, vchemencies, sweetness, fatigue, and inexplicable rivalries. A guitar in the hands of a dark man -- hard, dry metallic. Instrument of his existence, loved like a bride, full of secrets untranslatable to foreigners, sensible to the touch, rich to the ear, sensual to a view obsessed with the sound hole -- a navel in the middle of the truncated hips of this miniature woman that is the flamenco guitar. It sets free memories, deepens martyrdoms, mystifies or eliminates concerns of virtue, and stimulates, procures or erases unworthy propositions. It sickens the cnid, it inflames and vibrates like a woman in bed, in the hands of a guitarist -- more than her love, he is her exploiter who punishes her when necessary. He pampers her, he caresses her, he hears her, putting his ear lightly to the neck to feel its secrets and enjoy it himself before it emits its sounds for the other\n\n[ENDING CONTEXT]\n\nartist. What is interesting about flamenco is that it has become professionalized, and it is this that we are discussing. A curious feature of flamenco is how modern it is, a music of the 20th century, and how professionalized. Because of this it has progressed and is today what it is. JOSE LUIS DE CARLOS: I would like to point out something about the flamenco-payo controversy. Flamenco is a musical tradition and we are considering it as such. Social entities manifest flamenco in a different way than individuals do. There exists a race which makes of flamenco its own thing. On the other hand,\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sur Express - Debate",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 2436,
    "article_char_count_full": 13736,
    "article_char_count_review": 3152,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imitat"
      }
    ]
  }
]
```
