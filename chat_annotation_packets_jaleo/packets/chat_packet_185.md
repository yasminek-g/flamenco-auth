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
    "article_id": "JALEO_1984_01::A14",
    "article_text_for_review": "Paco Sevilla and Reynolds Heriot are pleased to announce that arrangements are underway for a trip to Spain designed specifically for flamenco enthusiasts. The flamenco activities will take place in Madrid and Andalucía and will include visits to tablaos, dance schools, flamenco bars, festivals, private juergas, as well as special trips to the best places to buy flamenco records, shawls, castanets, fans, etc. It will be an intensive three weeks of flamenco. Jaleistas and their friends will leave Los Angeles onboard a special flight of IBERIA Airlines on Tuesday, July 24th... and return from Madrid on Monday, August 13th. The three week \"package\" which includes air, hotels, with breakfast daily, numerous Tours, including meals while on tour, etc., is value priced at $1,497. - per person double...is limited to 30 participants. ...Some openings are still available...on a first come, first serve basis. A refundable $400 deposit is required to sign up...so act now! Send the deposit/registration: Name, address, phone, check or money order to: CHULA VISTA TRAVEL CENTER 297 \"K\" Street Chula Vista, California 92011 Telephone: (619) 426-6800 Full particulars are forthcoming.",
    "title": "JALIESTAS TRIP TO SPAIN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 182,
    "article_char_count_full": 1183,
    "article_char_count_review": 1183,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A15",
    "article_text_for_review": "THIRD INTERNATIONAL SAN LUCAR FLAMENCO GUITAR COURSE The International Sanlucar Flamenco Guitar Course is held in August in Snalucar de Barrameda, Spain (province of Cadiz. Manola sanlucar offers instruction to guitarists from over a dozen nations in his technique, as well as flamenca culture and his compositions. Translation into English is provided. to be a participant an intermediate level of any style of guitar playing is required. No previous knowledge of flamenco is required. Beginners may be accepted as auditors, as space permits. The level of participants ranges from intermediate level student to professional concert guitarist. Anyone desiring such information or having any questions about the course or interest in applying should contact: Irene Kessel 32 Arcadia Road Natick, MA 01760 (617) 653-4609 IV ENCUENTRO FLAMENCO The Centro Flamenca \"Paco Peña\" is holding the fourth Encuentro Flamenco in Córdoba. John Williams will direct classes in classical guitar from July 18-28; Paco Peña will teach flamenco from July 9-21; Manuel de Palma, who plays in the style of Diego del Gastor will teach beginners from July 9-21 as well as July 23 - August 4; Victor Monje \"Serranito\" will teach from July 23 - August 4; Loli Flares of Sevilla will teach dance from July 9-18 and Inmaculada Aguilar will teach from July 23 - August 1. Course A 9th July Paco Paña Course C 18th July John Williams Course D 9th July Loli Floras Course BB 23rd July Manuel de Palma COURSE FEES: Flamenco or classical guitar course 20.000 pesetas Auditors 14.000 pesetas Beginners (flamenco guitar) 14.000 pesetas Denca course 15.000 pesetas APPLICATIONS: You must enclose 4,000 pesetas with your application form for registration costs. The remainder must be paid on arrival at the seminar. The method of payment is by Eurochaque or by postel or bankers Drder to \"Centro Flamenco Paco Peña\" and send to: Plaza del Potro 15, Cordoba, Spain. Please send your application form as soon as possible to the above address and not later than 1st July 1984. Hostel accommodation will be provided at your request at around 500 to 800 pesetas per person per night. Unless otherwise stated we will assume that you have no objection to sharing with other fellow-students. If you prefer an alternative type of accommodation please tell us your requirements and the Centre will make your reservations. Please tick: HOSTEL. ☐ OTHER Meals can be organized by the Centre. There is also a wide choice of restaurants and bars all over Cordoba at reesona-bie prices. There will be concerts by the teachers as well as Camarón de la Isla and Tomatita El Sardera, Enrique Montoya, Enrique Melchor, Chano Lobata and others during the period of the classes. Write to: Centro Flamenco \"Paco Peña\" Plaza del Potro, N. 15 Córdoba, Spain",
    "title": "FLAMENCO SEMINARS IN SPAIN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 459,
    "article_char_count_full": 2797,
    "article_char_count_review": 2797,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A16",
    "article_text_for_review": "CANTES DE CORDOBA \"A MI TIERRA, CORDOBA\" Cantaor: Fosfarita Guitarrista: Enrique de Melcher y Pedro Blanco Púliyo Written by Angel Fernández Caballero, drawn out of ye spanish by ye Shah of Iran This recording, to my judgment, is of great interest for two reasons: The artist is Fasfarito, the ever-perfect exponent of the styles he renders and who, hailing from Puente Genil deserves our attention. Secondly, the recording is dedicated to the cantes of Córdoba -- with some liberties, certainly, since it includes styles not strictly cordovan -- cantes not well-known, but genuinely typical. The cante of Córdoba is distinguished, generally, by a tone of unusual gravity, a great solemnity -- indeed, at times a certain grandiloquence. Shall we also mention a certain \"Senequis mo\"? Another peculiarity is a predominant \"payismo\"; few cardavan gypsies have gone down into the history of the cante, and by contrast the list of non-gypsies is considerable. With styles, the same occurs. Those more clearly of gypsy origin (siguiriγas, bulerias) have not managed to take root in Córdoba, whilst the camperos, serranos, alegrias, fandangos, etc., take the lion's share. Fasfarita offers us in this record three cordoban variations of the fandango: That of Córdoba itself, that of Cabra, and of Lucena. The fandangos of this area are always vigorous, with quick rhythm, and short tercios. They are dignified forms which arise above the usual vulgarity of this form. Fasfarita embraiders them literally in this record, curiously enough, with that which is not proper to his own village, also known as Zángano. A highly attractive cante is the roas-alboreás of Córdoba, a wedding song as is the traditional alboreá, which here has a campás and \"senorío\" of great beauty. \"Like the alegrías or rosas de Córdoba, a variation of slow, sluggish alegrías, some call these alegrías tristes and with good reason. The carceleva is also different-- a desolate, pathetic dirge of difficult execution which the cantar resclves brilliantly despite not being at his best. Let us mention also the cantes de trilla, a pure delight. The serrana and the hirana are more common cantes; Fosforito makes beautiful versions of these. Finally, I refer to the great soleares de Córdoba, for me the best cut of this record, and truly meritáriquos. If the saleá is always a category of enormous difficulty, this more salemn, more intense cordovan version is of an overwhelming beauty. *Pertaining to the philosophy and teaching of Lucius Anneus of Seneca the younger, staic philosopher and politician (c. 4 b.c. -- 65 a.d.) who was a native of Córdova. Seneca committed suicide at the suggestion of Nero.",
    "title": "RECORD REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 433,
    "article_char_count_full": 2673,
    "article_char_count_review": 2673,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A17",
    "article_text_for_review": "MARIANO CORDOBA IN CONCERT by Frank Campbell (\"El Chileno\") \"...Sonority and its infinite shadings are not the result of stubborn will power but spring from the innate excellence of spirit.\" --Andres Segovia On Friday, February 3rd, Mariano Córdoba appeared in concert at the Douglas Beach House in Half Moon Bay in a memorable performance. Mariano, \"un maestro entre maestros\" not only delighted the large and musically sophisticated crowd, but reminded all of us what good guitar music (flamenco included) can be like. The Douglas Beach House, home of the Bach Dancing & Dynamite Society, is a large, two-story building in an idyllic setting by the Miramar Beach, a few miles north of Half Moon Bay on Highway 1. The society, in spite of its deceivingly frivolous title, is a group of (very) serious music lovers who sponsor weekly performances of jazz, folk, as well as classical music by world renowned artists in its locale by the beach. (Classical guitar greats Abel Carlevaro and Michael Newman are booked for April 1984.) Performances are usually preceded by dinner which sets the mood for a most enjoyable evening. On this occasion, Mariano Córdoba shared the stage with Adela Vergara and her company in a high quality, well-balanced performance which placed flamenco squarely where it belongs among the greatest musical forms in the world. The first half of the program consisted of solo guitar by Mariano followed on the second half by a full \"cuadro\" with Adela Vergara, dancers Jaime Valenzuela, Ricardo Orellana, and supporting guitarist Robert Dale. In the solo portion, Mariano not only demonstrated his quality as a performer, but also as an innate teacher, by preceding each number with a brief explanation of the origins and meaning of the \"toque,\" which greatly increased the understanding and appreciation by the audience. The solo numbers included a tanguillo (\"Castillito de Arena\"), malaguenas (de baile and flamenca), guajiras, zambra (entitled \"Dos Madres,\" and written for Mariano by a cousin of his in honor of his \"two mothers,\" Spain and America), granadinas, soleares, and danza mora. MARIANO WITH LESTER DEVOE AND CREATION ADELA VERGARA, MARIANO AND JAIME VALENZUELA Following the intermission, the group performed sevillanas, zorongo gitano con siguiriyas, alegrias, zapateado, tango, farruca, bulerías, and rumba. As a soloist, Mariano Córdoba has that pure, dignified, clean style that is shared only by a handful of masters such as Sabicas, Carlos Ramos, Mario Escudero, Juan Serrano, and a few others. All of the elements of the classical flamenco guitar are there, in perfect balance and harmony, with none of the circus-like atmosphere which is all too often presented as \"modern flamenco\" to the public. His mastery of \"el arte\" was also evident with the entire cuadro. It is not often that one has the opportunity to watch a master of the flamenco guitar on stage with dancers and singers, most having chosen the solo, concert style of appearances. Mariano Córdoba's performance was a stimulating, refreshing experience, which reiterated oftentimes forgotten standards of excellence for flamenco. A note of interest is that Mariano played a brand new guitar specially made for him by Lester DeVoe. Lester has emerged in an amazingly short time as a master \"guitarero flamenco\" in the world. His guitars are now among the most sought after, competing (and many times beating!) the old established Spanish stand-bys. Mariano's new proud possession is not only a beautiful instrument, but has a quality that mixes clarity, brightness, persistence, and mellowness of sound, in the most perfect combination I have ever seen. It was in all a magnificent evening, and we look forward with anticipation to Mariano Córdoba's next appearance at the Beach House. * * * TWO GUITARISTS OFFER A CHOICE OF TRADITIONS (Sent by George Ryss, source not given) by Jon Pareles In the technology of music, one of the most influential devices of all time is a machine that doesn't even have to be plugged in. It is the acoustic guitar, a design marvel of protability, adaptability and harmoniousness. It is a quiet, humble instrument, but whenever it enters a musical culture, it has a way of shifting traditions and spawning its own, from the blues to the bossa nova and from country music to Kenyan \"dry guitar.\" Two traditions that would not exist without the guitar can be heard in Manhattan this weekend. One of the most influential performers in the modern, virtuoso flamenco Jonathan Hillyer Mr. Escudero will perform tonight at 8 at Town Hall. FLAMENCO: ESCUDERO (Sent by George Ryss, source not given) by Jon Pareles In the solo guitar style that was spawned by flamenco music, melodies vie with the age-old rhythms established by poets and dancers. Mario Escudero, a major force in flamenco for 25 years, played a recital Friday at Town Hall that included both classical-style suites based on flamenco material and more improvisatory, dancelike pieces. In both forms, Mr. Escudero demonstrated a master's technique. For the classical pieces, he would often bring out a delicate tremolo melody above a complex accompaniment, playing it more quietly but more intensely than the arpeggio: surrounding it. And he could make rapid passages of strumming sound like spitfire syncopations or a fine mist of note: But the highlights of the concert were three dance pieces - a garrotín, a fandango and a tientos. They resembled sets of variations on a few chords or a bit of melody, and Mr. Escudero dug into them, changing tempos and textures with miraculous fluency. The echoes of ancient gypsy songs and dances were as clear as Mr. Escudero's ever-exact finger-picking. * * *",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 928,
    "article_char_count_full": 5697,
    "article_char_count_review": 5697,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A18",
    "article_text_for_review": "RAQUEL PENA SPANISH DANCE COMPANY Three honors have been bestowed: 1) The Raquel Peña Spanish Dance Company, with Fernando Sirvent, guitarist, has been asked by Young Audiences of the District of Columbia to perform in their program for area schools. (This is a new chapter of the nationwide Young Audience Organization.) 2) The Washington Performing Arts Society honored Raquel Peña and Fernando Sirvent in January 1984, with an award for their \"outstanding performance in the arts in Washington\" and for their 12-year participation in the \"Concerts in Schools\" program. 3) The Metropolitan Dance Association of Washington, D.C. has named Raquel Peña to its Advisory Board. The new Spanish Dance Center Company performed a full-length concert on February 26 at Gunston Theater in Arlington, VA. This Student Company will be allowed to perform some of the professional company's repertoire. At the same time, Ms. Peña will choreograph special themes just for the students. There already are several new pieces in the works that will be presented later this year. The debut of the Spanish Dance Center Performing Company was a full-length concert on January 29 at the prestigious \"Dance Place\", where they presented a demanding program which included the \"zapateadof of Juan El Estampio. Seattle,WA July 9th-14th, 1984 RESERVE YOUR PLACE NOW FOR THIS EXCITING WEEK OF FLAMENCO DANCE INSTRUCTION WITH ONE OF THE JEWELS OF THE FLAMENCO WORLD, LUISA TRIANA. BEGINNING AND INTERMEDIATE/ADVANCED LEVELS - $90 EACH. FOR MORE INFORMATION, PLEASE CALL OR WRITE: MARIA LUNA, WORKSHOP COORDINATOR P. O. BOX 22127 SEATTLE WA 98122 SEATTLE, WA 98122 206/625-0604 or 206/323-2629 \\( \\text{♦♦♦♦ THE FRAME STATION The Finest in Custom Picture Framing 20% DISCOUNT TO ALL MEMBERS OF JALEISTAS 1011 FORT STOCKTON DRIVE OWNER TOM SANDLER SAN DIEGO, CALIFORNIA (714) 298-8558 (Hillcrest/Mission Hills area)",
    "title": "PRESS RELEASE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 294,
    "article_char_count_full": 1886,
    "article_char_count_review": 1886,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
