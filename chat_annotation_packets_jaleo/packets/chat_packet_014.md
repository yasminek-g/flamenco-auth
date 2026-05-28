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
    "article_id": "JALEO_1978_03::A14",
    "article_text_for_review": "In the late 1950s and early 1960s, there was a relatively large amount of printed flamenco music available in this country as well as in Spain. One could find single pieces or collections of solos by guitarists like Luis Maravilla, Nino Ricardo, Mario Escudero, and Jose Azpiazu, along with many others. Much of the music wasn't worth much, being either written down incorrectly or very inferior material to begin with. However, some of it was useful, especially to material-starved beginning guitarists in this country. Two things have happened to change the situation with regard to written flamenco music. First, most of the above mentioned material has disappeared and, second, flamenco has changed. Fortunately, what little music is now being published is being written down more accurately than in the past (in most cases). This may be due to the fact that it is being done more and more by non-Spaniards, who have had to really understand the structure of flamenco in order to learn it, rather than relying on intuition as does the Spaniard who learns the music by absorbing it from his surroundings (contrary to popular Spanish belief, the feeling for flamenco does not originate in the blood). The second change, that of flamenco itself, has, in one sense, made almost all written flamenco music obsolete. It is as if one were to study the be-bop music of the early 1950s in order to learn to play modern hard rock; one would end up playing in a very outdated manner. Supposing, however, that one has no alternative method of study, there are some positive aspects to learning this way. One is at least learning something that will form a basis for later learning. In both rock and flamenco, the modern forms are relatively complex and an understanding of the earlier simpler music can be helpful in learning and understanding the more complex. Also, some of the early music by people like Sabicas, Mario Escudero and Carlos Ramos is very beautiful and worth playing. I have found that most Spaniards and many American flamencos under the age of twenty-five are astounded upon hearing a rendition of a Sabicas solo -- they recognize that the style is different, perhaps less complex than that of Paco de Lucía, but it is too beautiful to be overlooked and they have never heard it before; what was once considered old-fashioned and worn out may soon be brand new again! Sabicas and Escudero, Selected Solos for Gui- tar by Sabicas and Escudero, c.$2.50, Hansen Publications, N.Y., 1962. Five solos in music only. The danza mora by Sabicas is good but his alegrias is from Flamenco Puro (see below) and totally useless; tientos, rondeña and \"Danza Cale\" by Escudero are excellent. Intermediate-advanced. Pepe Martinez, $ \\underline{\\text{Flamenco Guitar Album No. 3, As Played by Pepe Martinez,}} $ transcribed by Ivor Mairants, Belwin Mills Ltd., London. Rondeña, alegrías, and tan-guillo in music only. Okay for Intermed-advanced. Emilio Medina, Complemento del Metodo para Guitarra Flamenca, Album 1, c. $4.00, Ricordi, 1961. Rosas, danza mora, fandangos de Huelva, malagueña, jota; in music only. Okay for intermediate. Carlos Montoya, $ \\underline{\\text{Flamenco Guitar Solos by Carlos Montoya, c.2.00, Hansen Publications, N.Y., 1957. Well written examples of six Carlos Montoya solos. Music only. Good for all who enjoy his style, especially if used with discretion.}} $ Pepe Martinez, $ \\underline{\\text{Flamenco}} $ - $ \\underline{\\text{Six Pieces for Guitar by Pepe Martinez}} $, $ \\underline{\\text{transcribed by John Magarshack, Scholt & Co. Ltd., 48 Great Marlborough St., London, England. Six very short solos in music only. Poor for intermediate.}} $ Richard Rightmire, $ \\underline{\\text{Flamenco Without Tears, and More Flamenco Without Tears,}} $ William J. Smith Co., N.Y. Each volume has six solos in music and tablature. In the first book, the pieces are very simple and very short; for beginners only. $ \\underline{\\text{More Flamenco}} $ has more material in each piece; mediocre for beginner-intermediate. Jack Buckingham, $ \\underline{\\text{Flamenco Guitar - Music of the Andalusian Provinces of Spain, c.200}} $, Carl Fischer Inc., 62 Cooper Square, N.Y. 10003, 1966. Thirteen solos that are more advanced than those in his first book (see flamenco method books next month). In music only. Poor for beginner-intermediate (due mainly to his lack of feeling for what flamenco should sound like).",
    "title": "FLAMENCO MUSIC IN PRINT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_03",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "13, 14",
    "page_number": 13,
    "word_count": 709,
    "article_char_count_full": 4427,
    "article_char_count_review": 4427,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_03::A15",
    "article_text_for_review": "The March juerga will be held at the home of Isabel Tercero in Del Mar on Saturday, larch 18th. Isabel says she is inviting flamencos from as far north as San Francisco and is making paella. The address is 482 15th St. (phone: 755-9409). To get there, take Interstate 5 north to Del Mar Heights Road, about 0 miles from San Diego. Here is the food key for this month....f your last name begins with: A - E bring a main dish F - J bring a salad K - O bring a main dish P - T bring a dessert U - Z bring bread or chips & dips Please fulfill your food commitment and ring drinks (alcoholic or otherwise according to your taste); we could use more non-alcoholic drinks. See the map below for directions.",
    "title": "MARCH JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_03",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 137,
    "article_char_count_full": 699,
    "article_char_count_review": 699,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_04::A1",
    "article_text_for_review": "by Maria Teresa Gómez The English translation of this article follows the original Spanish version. Sevilla personifica la semblanza más universal del espíritu español por la variedad de su riqueza artística, el atractivo de su fisionomia urbana, la gracia de sus moradores, y, en suma, ese conjunto de originalidades sorprendientes que ha dado en llamarse eltipismo de España. Pocas veces una urbe puede mostrar en su recinto, monumentos tan singulares como La Giralda, torre almohade con corona cristiana, el Alcázar, palacio mudéjar poblado de leyendas medievales, y la Catedral, tercero en magnitude entre todos los templos del orbe cristiano. Ni gozarse en la particularidad de sus barrios, de calles estrechas y tortuosas, y plazuelas diminutas. También, Sevilla es el esplendor de la fiesta taurina, a la que aporta su ejecutoria ganadera de reses bravas, la belleza de su plaza de La Maestranza, la fama de sus toreros vernáculos siempre iluminada por la memoria de Joselito y Belmonte - y un estilo de torear (del que suele decirse que es a la fiesta lo que fue a la historia del arte, el estilo griego o, si se quiere, el barroco) de inimitable gracia estética. Este concierto de color, de dramatismo y de gracia, ha ido definiendo la personalidad de Sevilla y acumulando figuras y hechos que han determinado su carácter universal, hasta el punto de ser una de las ciudades del mundo que ha merecido más elogios. A ello ha contribuido, a partir de la época romántica, el hecho de haber sido Sevilla patria de grandes artistas, así como la circunstancia de que no pocos autores españoles y estran- inhabitants, it is the sum of all the unique qualities that typify Spain. Few cities can claim, within their boundaries, such singular monuments as the Giralda, Moorish tower with Christian dome, the Alca-zar, Muslem palace peopled with midieval legends, and the Cathedral of Sevilla, third largest temple in the Christian world. Nor can one often find such quaint, winding, narrow streets and tiny plazas. Sevilla is also the pride of the bullfight season, contributing the brave bulls of its unsurpassed cattle ranches, and is famed for its bull-fighters (illuminated by the memory of Joselito and Belmonte) and a style that gave to the art of bullfighting what Greek style gave to the visual arts, an inimitable esthetic grace. This concert of color, drama, and charm gives Sevilla its personality and its historical figures and deeds have determined its character, making it one of the most eulogized cities in the world. Another contributing factor to Sevilla's notoriety, is that it has been the birth place of many great artists and the setting of many popular novels and dramatic works. \"Don Juan,\" the most popular play in Spanish literature, transpired in Sevilla, as did the novel, \"Carmen,\" written by Merimee and later immortalized in opera form by Bizet; Beaumarchais and Rossini also picked Sevilla as the scene of their celebrated, \"Barber of Seville.\" Current bullfight figures, along with those of flamenco such as dancer, Antonio, and singer, Mairena, from Sevilla, have also promoted the prestige and popularity of their homeland. If one adds to all this, the special charm charm of Sevilla's warm spring nights, its wrought-iron grillwork and flowers, the fiery eyes of its dark-haired women, the dazzling sight of its religious processions, the charm and gaiety of the Feria, the beauty of secluded patios, then one has the classic image of Sevilla for which the traveller searches and will not be disappointed. damenco Regional = Classical RAYNA'S SPANISH BALLET 1510 HARBISON AVE., NATIONAL CITY, CA. 92050 (714) 475-4627 Rayna DIRECTOR",
    "title": "Sevilla",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "poem",
    "pages": "1, 2",
    "page_number": 1,
    "word_count": 598,
    "article_char_count_full": 3667,
    "article_char_count_review": 3667,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_04::A2",
    "article_text_for_review": "Dear $ \\underline{\\text{Jaleo}} $, Highest compliments to your excellent newsletter. Its all good, but I especially liked your educational articles, \"Rhythm of the month\" and \"Flamenco Music in Print,\" These articles provide information that is hard to find these days. Also, your monthly juergas are a welcome sight to some of us who can't always afford the ever-rising prices of the high-class dinner-clubs which \"offer\" flamenco. Sometimes at these juergas we see performances of far better quality than those of the expensive places because the feeling is friendlier and there is more of a relaxed atmosphere which always adds to the \"magic\" of the evening. Respectful appreciation is extended to those thoughtful people who are kind enough to host these oc-",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 121,
    "article_char_count_full": 762,
    "article_char_count_review": 762,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_04::A3",
    "article_text_for_review": "JUERGA...OR PARTY? Has the time come for some reevaluation of our goals for Jaleistas? Our original goals were to draw together the different flamenco elements in San Diego County by giving them a monthly gathering place and a line of communication through the newsletter. The newsletter passed our expectations long ago. It has become not only a local line of communication but an entertaining and educational publication for which we are receiving nationwide participation in the form of subscriptions, letters and articles. Locally we have grown by leaps and bounds If judged by sheer numbers we are a great success. Seventy-five turned out at our meeting in June of 1977. Three months later Juerga attendance was up to one hundred on a regular basis. Last month our numbers jumped to an estimate of one hundred and sixty. The general concensus was that the March juerga was a successful party - the food was excellent, the drink ample, the ambient gay and noisy all evening. But was it a $ \\underline{\\text{Juerga}} $? Was it a $ \\underline{\\text{flamenco}} $ party or just a party? Out of one hundred and sixty people forty-six were members and one hundred and fourteen were guests. As flamenco enthusiasts ourselves, we are anxious to introduce others to the organization and expose them to the duende. But can Jaleistas absorb one hundred new arrivals in one evening (many of whom have never been exposed to flamenco) and still retain the essence of a Juerga? Is it time to begin to be more selective with our invitations - to limit the number of guests that any one member shall bring to a single Juerga - to limit our invitations to people we sincerely feel will have a continuing interest in Jaleistas?",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 4",
    "page_number": 3,
    "word_count": 292,
    "article_char_count_full": 1713,
    "article_char_count_review": 1712,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
