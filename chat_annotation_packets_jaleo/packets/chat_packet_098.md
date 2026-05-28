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
    "article_id": "JALEO_1981_02::A11",
    "article_text_for_review": "ADIOS NATIONAL U. The National University Alumni cottage, the site of many memorable JALEISTAS juergas, is being converted into classrooms. The following photos were taken at our final N.U. juerga. photos by Frank Campbell MARI PILI DANCES SOLEA RAFAEL DIAZ, MARIA JOSE JARVAS DO PALMAS FOR BILL HARDIN ON THE GUITAR, CONNIE HARDIN CENTER RIGHT: GUITARISTS KATHLEEN JONES & P. SEVILLA REMEDIOS FLORES & JOSE PICA MAGDALENA WITH PARTNER IN SEVILLANAS",
    "title": "DECEMBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "24",
    "page_number": 27,
    "word_count": 71,
    "article_char_count_full": 449,
    "article_char_count_review": 449,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_02::A12",
    "article_text_for_review": "Francisco and Elizabeth Ballardo celebrated their 32nd anniversary with a juerga on December 20th. There was a large gathering of friends and flamenco enthusiasts who made merry into the morning hours. photos courtesy of Mary Ferguson ELIZABETH BALLARDO (SEATED) WATCHES WHILE DAUGHTER ELIZABETH DANCES BULERIAS FRANCISCO BALLARDO (STANDING CENTER) .IIANA DE ALVA DANCING ALEGRIAS",
    "title": "ANNIVERSARY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 28,
    "word_count": 54,
    "article_char_count_full": 380,
    "article_char_count_review": 380,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_02::A13",
    "article_text_for_review": "\"LOOKING FOR A PLACE TO HAPPEN\" In February, as in January, we are in search of a location for our annual meeting-juerga. We need to talk over the past year, elect officers and raise a bit of \"jaleo\". Cuadro \"C\" has offered to take charge again this month but they need members' assistance in finding a site for February 21. If you have a house to offer or other location to suggest contact Brad or Paca Blanchard at 281-8447. Remember garages make great flamenco caves; condominium recreational facilities are another possibility DATE: February 21st TIME: 7:00 to? PLACE:? BRING: Tapas of your choice",
    "title": "FEBRUARY MEETING-JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 28,
    "word_count": 103,
    "article_char_count_full": 601,
    "article_char_count_review": 601,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_02::A15",
    "article_text_for_review": "LLAMADA (la) -- a call; a signal used by dancers to communicate a forthcoming change in the dance; llamadas are commonly used to signal a dancer's entrance (SALIDA) the closing of a section of dance (CIERRE) a major change of tempo or rhythm as in CASTELLANA or the change to bulerías in the alegrías, or the beginning of a DESPLANTE. LIGADO (el) -- slur or tied note; notes that are played with the fretting hand alone, that is, without plucking the string with the right hand. LUNARES (los) -- polka dots. MACHO (EL) - a personal ending or \"remate\" which is tacked on to the end of the cante. MADRID - outside of Andalusia, but the most active site of commercial flamenco and home of many top flamenco artists; there is plentiful instruction in all areas of flamenco, but not much atmosphere; inhabitant = madrileno(a). MALAGA - a city on the Mediterranean \"Costa del Sol.\" Not a lot of gypsy flamenco, but here were developed the malagueña, jaberas, verdiales, and a style of tangos; inhabitant = malagueño(a). MANO(la) - hand; right hand = derecha; left hand = izquierda. MANTILLA(la) - Spanish veil made of lace; worn on the head, often with the peineta MANTÓN(el) - Spanish shawl used in dancing. MÁSTIL (el) - The neck of the guitar, also called EL MANGO. MAYOR (el) -- the major mode, as in Amajor. \"A\" Box 4706 San Diego, CA 92104 MEDIA PLANTA (la) -- half-sole; the striking of the ball of the foot against the floor; also called GOLPE. MENOR (el) -- the minor mode, as in A minor. MESÓN (el) - a bar-restaurant where people can gather to sing and dance such things as sevillanas, fandangos, and rumbas; occasionally the site of more serious flamenco. MORÓN DE LA FRONTERA - a town in the Sevilla area that became famous in the 1960's when it, and its resident genius guitarist, Diego del Gastor, were exposed to the world by the writings of Donn Pohren. Many foreign guitarists made pilgrimages to the pueblo and the style of guitar playing has come to be known as \"Morón style.\" MOSAICO (el) - The mosaic around the sound hole; the whole design is called LA EMBOCADURA. MURALLA REAL - literally, the \"Royal Wall;\" refers to the ancient wall around Cádiz and is frequently mentioned in verses of the alegrías. MUTIS (el) - exit (hacer mutis = to make an exit); in flamenco, the ending of a dance by going off stage. NOTA (la) -- a musical note. PALILLOS (los) -- the Andalucian or flamenco term for castanets. PALMAS (las) -- handclapping used to accompany flamenco singing and dancing. PALMAS ABIERTAS (las) -- loud, sharp handclaps made by the fingers of one hand hitting the palm of the other; also called FUERTES or SECAS. BULK RATE U.S. POSTAGE PAID La Jolla, California Permit 421 TIME VALUE RETURN POSTAGE GUARANTEED",
    "title": "DICTIONARY OF FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "28-31",
    "page_number": 31,
    "word_count": 482,
    "article_char_count_full": 2734,
    "article_char_count_review": 2734,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_03::A1",
    "article_text_for_review": "by Paco Sevilla icente Escudero was born somewhere between 1882 and 1895; the truth probably lies in the middle somewhere. As with his birthdate, it is difficult to separate fact from fiction with much of Vicente's early life. It is clear that he was born in Valladolid, a large city that lies far north of Madrid, and is the capital of the province of Castilla la Vieja (old Castile). In spite of Vicente's appearance and the myths that surround him, it is fairly certain that he was not a gypsy and his parents did not come from the gypsy caves of the Sacromonte in Granada; Vicente's father is said to have been pure Castillian and wanted very much for his son to enter the printing business. However, young Vicente had other ideas and spent much of his time hanging around the gypsy neighborhoods and decided he wanted to be a flamenco dancer, an unheard of thing in that part of Spain. One report says that, \"The young Vicente's first heel beats were practiced on the metal manholes in the streets of Valladolid...When the police chased him from manhole to manhole, he landed finally on the smooth surface of a tree trunk that had been thrown across a river as an improvised bridge. Here he practiced on the resounding wood, and -- as it was narrow -- he received many ablutions in the water below. In Vicente's own words, it was at this time that he established an unshakeable balance -- for fear of being soaked again. This balance was to support him and secure the posture for which he was famous in his majestic style of dancing.\"⁴ As young as nine Vicente was dancing wherever he could, in town squares and at local fairs where money was earned by passing a hat. Donn Pohren writes that he, \"...consistently ran up against flamenco's big problem of old: secrecy. No one would teach him the fundamentals of the dance, such as the compás, palmas, etc. (He calls this not being 'enterao,' or 'clued in'). Around the age of seventeen he began to be hired in cafés cantantes, but always lost the jobs as soon as it became evident that he was not 'enterao.' \"5 VICENTE ESCUDERO (CENTER) IN THE SACROMONTE Vicente was fortunate to meet the great dancer, Antonio de Bilbao, who must have seen the potential in the boy and taught him the basic things he needed to know (perhaps Antonio, being also from the north, empathized with Vicente's situation). Vicente then began to work in the cuadros of the cafés cantantes of Madrid and was soon making a name for himself. But he found that he did not like the atmosphere of the café cantante, the lack of respect for the artists, and so he left them to work with touring companies that performed in theaters, between films or as part of variety shows. Eventually Vicente went to Portugal where he, \"...commenced his off-beat flamenco, firstly because he could not find a guitarist there who knew the rhythms, later because he began to enjoy the liberty gained by dropping the compás. \"Next stop, Paris, where Vicente was to build such a reputation for himself that his name leapt the Pyrenees and became known throughout Spain. Shortly after Vicente's first Paris recital, in 1922, he became strongly influenced by the Dadaistic and surrealistic schools of painting, to such an extent that he took up painting himself and, what for us is more significant, began applying these concepts to his flamenco dancing. With an entire philosophy to back up his own instinctive feelings, Vicente really let himself go. He began giving concerts to the clashing of two orchestras going separate ways, or to the humming of dynamos set at different pitches. This, he states in his book, was the most delightfully creative period of his career. He went so far as to rent a",
    "title": "VICENTE ESCUDERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_03",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 652,
    "article_char_count_full": 3703,
    "article_char_count_review": 3703,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
