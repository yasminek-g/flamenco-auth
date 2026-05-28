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
    "article_id": "JALEO_1978_02::A3",
    "article_text_for_review": "JUERGAS... \"IT'S NEVER TIME TO LEAVE\" As the day of the juerga approaches, I find myself getting more and more impatient. The reason why something that a year ago wasn't even in my imagination has become so important in my life, is one of those mysteries that I don't fully understand. Maybe they are feeding us something in the food, or perhaps flamenco itself is habit-forming. But whatever the reason, the truth is that I enjoy myself so much that I look forward to the juergas as I do to my vacation: eager, impatient, and restless. Juergas can be approached from many different points of view; they are for the aficionado, for the performer, and for the spectator. They have something for everybody. To begin with, and above all, I enjoy the people. There is such a variety of personalities, such a diversity of backgrounds, that it is almost impossible not to find something interesting in everyone of them. People from all over the world, with all the possible accents and pronunciations, all of them with the common interest of flamenco. I enjoy the dancing too; the \"formal\" and the \"improvisations.\" The force of flamenco is such that sooner or later persons who never danced in their lives start feeling \"ants\" creeping along their toes and up and down their spines, and logically enough, they are unable to resist the call of the guitars. I find this terribly amusing, and some of the best performances that I have witnessed have started like that. There must be something in flamenco that touches places inside of us that we didn't know existed. And I enjoy the music; the guitars that play, sing, weep and laugh; the flamenco guitars, so serious and so light, so soft and so vibrant, so powerful. When the guitar plays, the notes fill the air and something unique is created; a statue of sound that dissipates in the atmosphere. PALMAS I would just like to say that last month's article on palmas was one of the most interesting articles I have read yet. It was very informative and beneficial to me and I hope that more articles will be written on different techniques in the dance, guitar, and cante. One suggestion to subscribers to $ \\underline{\\text{JALEO}} $ who attend the juergas; if you are interested in joining in during the dancing, there are guit-arists, dancers, and singers who can teach you the basic palmas for whatever rhythm is going on at the time, so don't be afraid to ask. Thank you very much again Rayna and $ \\underline{\\text{JALEO}} $ for such a nice article. Rosala",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "4, 5",
    "page_number": 4,
    "word_count": 437,
    "article_char_count_full": 2506,
    "article_char_count_review": 2506,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A4",
    "article_text_for_review": "La Vikinga and Roberto Reyes Tibu's (see announcements) classes are UNIQUE! For the first time in N.Y.C., she teaches steps and dance patterns like a guitarist learns falsetas. It's up to the individual to use this material, placing the remates, pais-cos, and closings at the right moment when Agujetas sings. Since Agujetas hardly ever sings the letra the same way twice, both the guitarist and the dancer become accustomed to creating musical and graceful phrasing \"on the spot,\" as opposed to the routines that we are accustomed to learning, using the counting system and making it very difficult for non-Spaniards to understand and appreciate the subtle complexities of the music. In other words, Tibu will show the entire class a bit of choreography suited for the letra, varied combinations of heelwork, and several llamadas and closings. Once the general class has digested the several possibili- ties, students will come out, one at a time, and use their interpretation to try to make a conversation between guitarist, singer, and dancer.",
    "title": "UNIQUE DANCE CLASSES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 169,
    "article_char_count_full": 1046,
    "article_char_count_review": 1046,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRhythm of the Month Cádiz (Reprinted from the FISL Newsletter, Vol.2, No.1, Jan, 1969.) From the animated city of Cádiz, \"Queen of Gracia,\" comes alegrías with its song and dance now popular in all the principle flamenco cities. Every singer, dancer, and guitarist is familiar with some form of the alegrías. Alegrías and soleares are the most \"done\" rhythms in the flamenco repertoire and utilize basically the same 12 beat compás, accenting the 3rd, 6th, 8th, 10th, and 12th beats. Experienced artists, however, are aware of subtler shades of accentuation which could be felt as in the following comparison: soleares: 1 2 $ \\underline{3} $ 4 5 6 7 $ \\underline{8} $ 9 $ \\underline{10} $ 11 $ \\underline{12} $ alegrías: $ \\underline{1} $ 2 $ \\underline{3} $ 4 5 $ \\underline{6} $ 7 $ \\underline{8}\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"compás\"]\n\ne 12 beat compás, accenting the 3rd, 6th, 8th, 10th, and 12th beats. Experienced artists, however, are aware of subtler shades of accentuation which could be felt as in the following comparison: soleares: 1 2 $ \\underline{3} $ 4 5 6 7 $ \\underline{8} $ 9 $ \\underline{10} $ 11 $ \\underline{12} $ alegrías: $ \\underline{1} $ 2 $ \\underline{3} $ 4 5 $ \\underline{6} $ 7 $ \\underline{8} $ 9 $ \\underline{10} $ 11 12 Naturally, accentuation changes from compás to compás within a single piece and there are no set rules. Alegrias is also played somewhat faster than soleares and in the gay major key (A-E⁷ or E-B⁷) rather than the minor (A-B⁵ or E-F) of soleares. In the flamenco repertoire, alegrías represents the Castillian-Aragonesan influence, having its origin in the jota(1) of these northern provinces. Julian Pemartin theorizes that the jota was brought to Cádiz by the Aragoneses during the War of Independence (1808-1814) and, according to Ricardo Molina and Antonio Mairena, alegrías was first sung around 1808 in Cádiz as a type of light rhythmic jota which, in time, developed into alegrias as we know it. So our authorities are in agreement that alegrías: 1. developed out of the jota Aragonesa; 2. wa\n\n[ENDING CONTEXT]\n\n1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3 and so on, rather than 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12. This section is never sung. 6. Build-up for llamada into bulerías and several bulerías desplantes to end. La Meri thinks that the bulerías finale was added during the transition of alegrías from barrio to stage. This section is often sung and sometimes the guitarist will switch to the sol-eares key. Some dancers do a brief section known as the ida de baile which is a sort of formal and contrived way for changing from alegrías to bulerías rhythm; not too many dancers do this anymore.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ALEGRIAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "5, 6, 7",
    "page_number": 5,
    "word_count": 1112,
    "article_char_count_full": 6504,
    "article_char_count_review": 2833,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "compás"
      }
    ]
  },
  {
    "article_id": "JALEO_1978_02::A6",
    "article_text_for_review": "Personality of the Month - Deanna Davis Deanna Davis, who is a newcomer to the flamenco scene and part of Juana de Alva's dance company \"Fantasia Espanola,\" is this month's personality. As a young girl, Deanna had always liked dancing. She first became attracted to Spanish dancing after finding a pair of castanets belonging to her mother, who is of Spanish descent and who comes from a long line of dancers, being herself a ballet dancer. Deanna's uncle was a flamenco dancer and so Deanna grew up in Australia among performers. Deanna began dan-cing when she was sev- en years old, studying ballet and other styles She started working professionally at the age of fifteen, doing chorous line work. At seventeen, she joined a dance company as a show girl and toured three times to the Far East, including such places as Viet Nam. She danced and performed this way for almost ten years. While in Australia, she took her first flamenco lessons from two male dancers whose names she doesn't recall, for about four or five months. Deanna married singer Jesse Davis, who is well known throughout the show business world in Las Vegas, Lake Tahoe and Europe. He brought Deanna to San Diego, where she has been living for the past five years. When she first arrived she started lessons with Juana de Alva, but had to stop because of illness. She has now been dancing again with Juana for about one and a half years in the company \"Fantasia Espanola.\" She is also contracted to tour with Jose Luis Esparza's dance company \"Music and Dances of Spain,\" which will be performing in the border towns of Mexico. Her goals are to continue dancing; specializing in flamenco. But because she has a small baby, Jesse Jr. (who is eight months old and takes up much of her time), she would like to continue working in areas close to home. Her favorite rhythms to dance are bullderrias and rumba; bulderias because she feels that her personality can be shown more freely and rumba because... well, I think once you know and see Deanna, you will know why this is her favorite. She feels rumba is a very coquettish dance and very sexy. Is there much more to say?",
    "title": "LA LUZ ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "8",
    "page_number": 8,
    "word_count": 379,
    "article_char_count_full": 2141,
    "article_char_count_review": 2141,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A7",
    "article_text_for_review": "This article, written by Anna Kissel-goff, first appeared in the $ \\underline{\\text{New York Times}} $, Dec. 10, 1977. DANCE COMPANY FROM SPAIN PLEASES CARNEGIE HALL CROWD A Spanish dance company without the emotional quality of duende is a Spanish dance company without soul. It can, however, be a Spanish dance company made up of technicians that keeps an audience more or less happy, which is what the Ballet Nacional Festivales de Espana accomplished with no effort Thursday night at Carnegie Hall. When the troupe made its debut here exactly a year ago, its potential strong point turned out to be its weakness. Traditionally, the Spanish dance company has been a troupe centered around a charismatic personality. In this way the organization of Spanish theatrical dancing has paralleled the development of American modern dance. Personalism came first and although Spanish dance used traditional forms, the star of the Spanish dance company played the same role as the dancer-choreographer in modern dance. The company was built around a specific artist. This particular company was different. It was organized from the top by the Spanish Government. It had no Escudero, no Argentina, no Carmen Amaya or any other great name to head it. Instead, it was ostensibly conceived to reflect the panorama of dancing in Spain. This was the virtue its organizers could have exploited with sophistication-- the regional diversity of Spain offers a wealth of folk material, and the tenuously preserved indigenous academic schools of the 19th cen- tury coupled with contemporary idioms, could easily provide a varied evening of fascination. Yet, this year, as last, the Ballet Nacional Festivales tends to confuse diffuseness with an artistic profile. It is a company without a signature of its own. What is different, however, is the improved quality of the dancing. The change can be traced largely to a wide shake-up in personnel, but it is also rooted in a welcome attempt to get away from the revue-like quality of such numbers as the Ravel Bolero, repeated from last year, along with the similarly unimaginative compositions to the zarzuela music of La Boda de Luis Alonso. Where the company finds its center at present is in the colorful folk dances and in the new suite of \"school\" dances, mainly of the Bolero school, which dates from the end of the 18th century. For the first time the company introduced as guest artists four members of the Pericet family, a dynasty that has specialized in the Bolero school for more than 150 years. Here is classical ballet, Spanish style. Yet unlike other countries in Europe, Spain never gave ballet the national or royal patronage that was found in Russia, France, Denmark and elsewhere. There was no state lyric theater to protect an idiom and technique that soon lost out to an indigenous art form, the operetta-like zarzuela. What is remarkable about these dances, however, is their similarity to recent reconstructions of French and Danish ballet in the 19th century. There is the same emphasis on precision, leg beats and unisex choreography for the man and woman. The broad sweep of Russian classical ballet as it came in with Marius Petipa-- who incidentally knew the Bolero school from firsthand experience--will not be found here. 50% DISCOUNT TO MEMBERS OF JALEISTAS HOFF CLEANERS CLEANING - PRESSING - ALTERATIONS 4940 EL CAJON BLVD., SAN DIEGO, CALIF. PHONE 583-4636",
    "title": "Ballet Nacional Festivales de España",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "8, 9",
    "page_number": 8,
    "word_count": 560,
    "article_char_count_full": 3421,
    "article_char_count_review": 3421,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
