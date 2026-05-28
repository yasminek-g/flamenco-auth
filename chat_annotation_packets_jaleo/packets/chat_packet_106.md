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
    "article_id": "JALEO_1981_05::A12",
    "article_text_for_review": "MOSAICO FLAMENCO MAKES ITS DEBUT IN SAN DIEGO by El Chileno Those of us who still mourn the untimely demise of the Andalucía Restaurant have since been anxiously looking forward to that small, intimate flamenco place where we can create some \"ambiente\" and find inspiration. The El Moro Restaurant has partially filled that need but I have always felt that the large Hispanic (and Hispanophile) community in the Southland could support more than one such place. Therefore, as soon as we heard that MOSAICO FLAMENCO, a group made up of dancer Deanna, singer Pilar Moreno, and guitarist Paco Sevilla would be making its debut at two different restaurants in San Diego the first week in April, I rushed to both places to investigate. An account of my experience follows. The Ocean Playhouse Restaurant The Ocean Playhouse in El Cajon is a small neighborhood type of place; it is pleasant and unassuming with separate bar and dining areas and a third area off the dining room with a dozen or so cooktail tables and a small stage with a mirrored background. On opening night, a relatively large crowd (for a Thursday) nearly filled the room to watch the perform- San Diego magazine has said, \"...when we die, we'd like to do time in this dark, classy haven...\" I would too, but I don't plan to wait that long. As somewhat of a contrast to the earthiness of the Ocean Playhouse, the Blue Parrot is a hangout for the designer-jean clad, young (or would-like-to-be-young), beautiful people of La Jolla and other equally affluent communities. The location of the restaurant, at the bottom of a three-tiered shopping area near the ocean adds to the quaintness of the surroundings. Indoor and outdoor dining areas, as well as a bar are provided, where the discriminating customer will find not only beer and wine, but Pernod and Galliano as well (sorry, no Tio Pepe or Cardenal Mendoza!). The menu includes mostly continental dishes ranging in price from $8.00 to $12.00. A two-drink minimum exists. The small stage can be seen quite well from nearly every table. The service is excellent and the quality of the food quite good, although a little over-priced -- in keeping with the going rates in La Jolla. The shows were flawlessly executed. Paco Sevilla's solo peteneras was particularly impressive, as was his accompaniment of Pilar's moving tientos. Deanna, an exceptionally beautiful and talented dancer was again hampered by a slippery stage, a deficiency which I expect will also be taken care of in future appearances. The Blue Parrot is located at 1298 Prospect Street in La Jolla. Performances take place on Sunday beginning at 8:00pm. In summary, I came away very gratified after watching Mosaico Flamenco in its double debut in San Diego. I have always felt we are indeed fortunate to have much flamenco talent in San Diego, and I am encouraged -- and hopeful -- to see \"el arte\" flourish, as it jolly well should. Paco Sevilla is a highly developed artist who has put together a show of superb professional quality. In a style somewhat reminiscent of Paco Peña's, he quickly captivates and involves the audience in the performance with a well-balanced and thought-out sequence of selections. Pilar \"La Canaria\" is a classy and beautiful singer, who has a vast repertoire, which she executes with considerable refinement and emotion, complementing Paco's guitar rather well. Deanna is an artist who has evolved considerably. Her style combines solid technique with a lot of feeling, and I look forward to even greater development as time goes on. The Ocean Playhouse and the Blue Parrot are two additions to the world of flamenco in the Southland which should provide aficio-",
    "title": "-FLAMENCO MEANDERINGS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 618,
    "article_char_count_full": 3676,
    "article_char_count_review": 3676,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_05::A13",
    "article_text_for_review": "HOSTS TONY AND ALBA PICKSLAY WITH JUANA DE ALVA (LEFT) THE RIGHTS OF SPRING The Jaleistas celebrated the coming of spring by having one of the best juergas ever at the home of Tony and Alba Pickslay in Del Mar. I would like to take this opportunity to thank them for being such gracious hosts and to congratulate and thank all the members of Cuadro \"A\" as well. We were also honored to have appearing as guest artists Marcos and Rubina Carmona from Los Angeles who sang and danced and played so well all night. It just happened to be one of those nights where everything worked and everyone good showed up -- for example, some of the great local talent there Paco Sevilla, Yuris Zeltins Deanna Davis, Rodrigo de San Diego, who also delighted everyone by showing us that he is more than a brilliant guitarist and treated us to his own rendition (dancing) of a bulería. Remedios Flores sang so well, María José sang and did a fiery dance, Rosala, long in absence and on a visit from Spain danced, Juana de Alva (por supuesto) it wouldn't be a juerga without her and, of course, Juanita Franco, dancing and captivating everyone there. If there wasn't something happening upstairs, it was happening downstairs, but it was definitely happening all night and what a great night it was! Special thanks to Mary Ferguson, the Ballardos, Jack Jackson, Nina and everyone in Cuadro \"A\". LEFT: MAGDALENA CARDOSO IS ACCOMPANIED BY YVETTA WILLIAMS, YURIS & MARCOS. ABOVE: JUANA DE ALVA, EL CHILENO, MARCOS (SEATED) RUEINA & ERNEST LENSHAW (STANDING). Remedios Flores (singer): \"For me it was very impressive and in that moment I felt that I was in Spain. There was a lot of \"Pellisco\" in the dance. Everyone who collaborated in the fiesta deserves an olé. The ambiente was full of duende.\" María José (singer): \"For me it was stupendous! Muy alegre! I hadn't felt so content in the flamenco juerga in a long time. Rubina seemed so nice and everyone collaborated. It was a treat for everyone. It was also a great surprise for me to see Rodrigo dance bulerías.\" (LEFT) EL CHILENO & LA CAMARONA, (BELOW) RUBINA, REMEDIOS FLORES & MARIA JOSE JARVIS SING FANDANGOS ABOVE: RUBINA CARMONA DANCING RIGHT: JUANITA FRANCO DANCING TO ACCOMPANIMENT OF RUBINA & REMEDIOS Yuri Zeltins (guitarist): \"I felt that I missed their (Marcos and Rubina) performance. It was covered in the deluge of people. They had so much more to offer than they were able to do -- than circumstances would allow. I really enjoyed the energy level that they generated. It was nice to see that many guitarists play. I didn't feel that I had to play for once.\" BELOW & RIGHT: PACKED IN DOWNSTAIRS ROOM ARE GUITARISTS ROY LOPEZ, YURIS ZELTING & MARCOS CARMONA (SEATED) & RON RYNO, EL CHILENO, THOR HANSON, JUANITA BALLARDO & RUBINA (STANDING). Juanita Franco (dancer): \"I really enjoyed it a lot. I thought that it was a really good juerga. I only wish that we could have heard more of the guest performers. I was enjoying listening to them and then people were trying to get me to dance. I would like to see others participate more, especially the student guitarists and dancers. But it was a really neat juerga.\" Thor Hanson (guitarist): \"I thought it was really neat. There was a lot of energy. Having new people fires everybody up -- gives an added learning experience. I enjoyed learning and wish we could have guests at more juergas. Sometimes there were so many guitarists playing, though, that it degenerated into a lot of noise. The throb of the beat could be heard but not all the subtleties. It is a difficult thing because people want to play along to learn but maybe they should try to take turns more of the time.\" CAROLINA MOURITZEN & MARY FERGUSON Paco Sevilla (guitarist): \"I think that it was the best fiesta (it definitely was not a juerga) that we've had in a couple of years. With such a large number of artists there was always something happening. When one room became jammed with performers and spectators, to the bursting point, it was possible to slip away to a quiet room and start fresh. The only thing I regret was not being able to full enjoy the artistic abilities of our guest artists. We had an incredible guitarist who was reduced to pounding out barely audible accompaniment and a cantaora capable of singing cante jondo who was limited to singing sevillanas, fandangos, tangos and alegrias. With a little less noise we could have enjoyed Marcos and Rubina even more.\"",
    "title": "-MARCH JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "25-28",
    "page_number": 25,
    "word_count": 780,
    "article_char_count_full": 4448,
    "article_char_count_review": 4448,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_05::A14",
    "article_text_for_review": "UTRERA AND JEREZ NIGHT This month's juerga will be held in San Carlos at the home of Barbara Novak. Barbara is a friend of Ernest and Hilma Lenshaw and has attended several juergas. She was born in Poland, but became familiar with flamenco music and grew to love it in Argentina where she spent fifteen years. She reports that flamenco is played regularly on the radio there and frequently on T.V. also. (We should be so lucky!) Barbara enjoys doing international folk dances and so far has learned to play the cast-tanets to accompany the Spanish music she says. The two other members of the household who may be present at the juerga are Jessie (who has attended one previous juerga) and Helen. There is no theme for this month but for those of you who have been reading the \"Archive series in Jaleo we will be playing tapes of the singers written up the last two months: from Utrera- sisters Fernanda and Bernarda, Miguel el de Angustias, El Perrate and from Jerez de la Frontera- Tía Anica la Piriñaca, Juan Romero Pantoja and Manuel Borrico. Anyone having other examples of any of these singers, please bring them to share. DATE: Saturday May 23rd PLACE: 6620 Golfcrest PHONE. 461-0990 TIME: 8:00pm to? BRING: Tapas (Hors d'oeuvres) GUESTS: By reservation only--call Thor or Peggy Hanson at 488-4139. Donation: $5.00 for guests (non-members or non-Jaleo subscribers). Exempt are subscribers who live over 100 miles from San Diego or first guest of member holding single-plus-guest card. (Two guest limit.) $ \\underline{\\text{Directions}} $; (From freeways 5, 15, 163 or 805) take Hwy 8 east, north on College Avenue, Right on Navajo, Right on Jackson and right on Golfcrest. FLAMENCADA Two week tour through the cradle of flamenco, September 5th to the 20th. See last month's $ \\underline{\\text{Jaleo}} $ for more details. For further information and reservations call Reynolds Heriot 714/426-6800. (Please note: phone number last issue was incorrect.)",
    "title": "-MAY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 29,
    "word_count": 324,
    "article_char_count_full": 1957,
    "article_char_count_review": 1957,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_05::A16",
    "article_text_for_review": "TEMPLE(el) - the singer's salida or warm-up. TERCIO (EL) - a single line or phrase of song; this usually coincides with the poetic line (verso) of the verse (copla or letra) but need not do so. TIEMPOS (LOS) - beats TIRANDO - free or unsupported plucking strokes. TOCAOR (EL) - a flamenco guitar player TOCAR - to play a musical instrument TONO (EL) - pitch or key; \"buscar el tono\" - to look for the singer's pitch on the guitar; the keys in Spanish are: La (A), Si (B), Do (C), Re (D), Mi (E), Fa (F), Sol (G). TOQUE (EL) - flamenco guitar playing TRAJE CAMPERO (el) - Ranch clothes; these are work clothes and should not be confused with the more formal \"traje corto\". TRAJE CORTO (el) - The formal Andalucian ranchwear of the past; now worn by both men and women on festive occasions and in dancing certain flamenco dances; the name comes from the short jacket. TRAJE FLAMENCO(el) - flamenco costume; most often used to refer to the full-length dress worn by Andalusian women for dancing in the ferias and for flamenco. TRASTES (los) - The fretsof the guitar. TREMELO (EL) - a treble melody sustained with the fingers while the thumb plays a bass melody; the most common sequence of plucking in flamenco is thumb, index, ring, middle, index (repeat). TRIANA - the old gypsy quarter located across the river from Sevilla; Sevilla has now grown out and around Triana to such an extent that the barrio is no longer very distinct from the rest of the city. VARNIZ (el) - The finish on the guitar. VENTA (la) - country inn; a bar along a high-way where flamencos can sometimes be found and hired for juergas. VERSO (EL) - a literary term referring to a single line of poetry. VOLANTES (los) - The large ruffles on the traje flamenco; sometimes the word \"frun-ces\" is used. VUELTA (la) - a turn; there are many different types of turns used in flamenco dance. VOZ (LA) - voice; there are certain terms commonly used in describing voice quality: \"voz rajá\" is the very hoarse and rough voice common to gypsy cantaores and considered ideal for the cante jondo; \"voz afillá\" is similar to \"rajá\" and was derived from Diego El Fillo, a singer who had this type of voice (the term \"rajo\" is also heard in this context); \"voz natural\" and \"redonda\" are more natural and clear singing voices and more suited to singing the non-gypsy cantes, although there are many excellent cantaores with this type of voice (usually they can call forth a little \"rajo\" when needed); \"voz bonita\" is a negative term among flamen-cos and refers to the very sweet, opera-tive type voices more common among the popular pseudo-flamenco singers in Spain. ZAPATEADO (el) - footwork; more specifically, the striking of the different surfaces of the foot against the floor. ZAPATOS (los) - shoes. BULK RATE U.S. POSTAGE PAID La Mesa California Permit 368 TIME VALUE RETURN POSTAGE GUARANTEED",
    "title": "DIRECTORY OF FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "32",
    "page_number": 32,
    "word_count": 503,
    "article_char_count_full": 2858,
    "article_char_count_review": 2858,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_06::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n\"THE BLONDE GYPSY BRINGS HOME TRADITION\" (from: $ \\underline{\\text{Arizona}} $, Feb. 22, 1981) by Maxine Olmsted The women entered the sprawling, U-shaped building in the northwest Phoenix and walked along a short hallway. They could hear castanets purring and clicking heels rapping and hands clapping. It was not the staccato sound of applause, but fuller tones, more purposeful. A class in Spanish dance was warming up. Lydia Torea was at her desk, finishing a phone conversation. Someone said she thought of hand-clapping as, well, just hand-clapping, and didn't know there could be so much to it. Miss Torea offered the expert explanation: \"There are two kinds of hand claps in flamenco dance.\" Not counting the applause kind, of which Miss Torea has had her share. \"One is the sorda. The\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"close\"]\n\nr tones, more purposeful. A class in Spanish dance was warming up. Lydia Torea was at her desk, finishing a phone conversation. Someone said she thought of hand-clapping as, well, just hand-clapping, and didn't know there could be so much to it. Miss Torea offered the expert explanation: \"There are two kinds of hand claps in flamenco dance.\" Not counting the applause kind, of which Miss Torea has had her share. \"One is the sorda. The fingers are closed and the palms of both hands are cupped so that when they slowly strike together a low, muffled sound results.\" That was one sound coming from her studio. \"The other is the abierta,\" Miss Torea continued. \"The palms don't meet. Instead, the closed fingers of one hand, forming a paddle, rapidly strike the hollowed palm of the other, causing a higher, sharper sound.\" The Lydia Torea Dance Conservatory has the traditional equipment of a ballet studio: wooden floors, barres and a mirrored wall. \"Ballet is a necessary background for so many physical activities,\" Miss Torea said. \"It's my understanding that some athletic coaches even give their teams exercises comparable to ballet's barre and center floor work. Many years ago flamenco dancers, in particular, as opposed to classical Spanish dancers, wouldn't waste time on ballet training. Eventual\n\n[ENDING CONTEXT]\n\nof dance. Maria Alba, Roberto Amaral, Maria Benítez, Felipe de la Rosa, Roberto Lorca, Teodoro Morca, Manolo Rivera, to mention a few. All of these are American-born. We studied in Spain at a time when that country was in its heydey of great teachers. We have maintained those standards.\" $ ^{*} $ $ ^{*} $ $ ^{*} $ $ ^{*} $ Manuela de Cadiz phone: 213/837-0473 10620 Esther Avenue LOS ANGELES, CALIFORNIA 9C064 PRIVATE & GROUP LESSONS FLAMENCO DANCES CLÁSICO (Escuela Andaluzay) PANADEROS (Escuela Bolera) JOTAS (Aragón) MUNEIRA (Galicia) LAGARTERANA (Toledo) $ ^{*} $ $ ^{*} $ $ ^{*} $ $ ^{*} $\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LYDIA TOREA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_06",
    "year": 1981,
    "language": "en",
    "article_type": "poem",
    "pages": "3-7",
    "page_number": 3,
    "word_count": 1580,
    "article_char_count_full": 9010,
    "article_char_count_review": 2927,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "close"
      }
    ]
  }
]
```
