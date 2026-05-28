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
    "article_id": "JALEO_1983_06::A8",
    "article_text_for_review": "CREATION OF A DANCE STYLE Countless centuries ago, before we had categories of dance and music such as folk, classical, modern, post modern, jazz, neo-classical, flamenco, etc., people from all corners of the world expressed their feelings and emotions in movement and sound. Little by little, categories and styles crystallized in different cultures, with different people expressing their emotions in a great variety of movements. Spain, by its very location and history, has become a rich melting pot of cultural and artistic variety. Over the centuries, each corner of Spain has developed and evolved its unique and different regional folk dance style. As societies formed with various levels of social station, there developed the pre-classical dance styles, the court dance; in the courts of France, Italy, and Spain this would lead to the birth of classical ballet. In Spain, this style of classical dance became known as the \"bolero era\" or escuela bolera. These were to evolve into many traditional classical techniques, and a unique Spanish flavor. In Southern Spain, there was the birth of a most unique style of music, song and dance expression and, of course, that is what we know as flamenca. This article has not so much to do with a detailed explanation of all of the dance forms and styles found in Spain, as it has to do with a very exciting form and style of dance that grew out of and used all the ingredients of the styles of dance found in Spain and, even styles that evolved in other countries. The birth of a new style of dance is exciting, like a new island growing out of the sea after a very emotional volcanic earthquake. In Spain, from the rich blend of regional folk dance, classical dance, flamenco dance, and a super rich dose of individual creativity, came a form, a style of contemporary modern theatre dance that is unique in the dance world; it is, at the same time, in this day and age of overcategorization, a much misunderstood, beautiful hybrid of the dance world. I would like to try to clear the air of some of the misunderstanding of this most exciting and interesting of dance styles, this modern Spanish theatre dance. I would also like to correlate this style to other contemporary styles born this century in the United States and other countries, a phenomenon which seems to be in a form of cycle that is basic in the arts. In the early part of the 20th Century the Denis-Shawn Dancers were spreading dance throughout the United States and the world, and giving birth to many artists who were taking traditional movements from many dances of many countries and adapting them to the concert stage. They elato took much from classical dance, pre-classical forms and then, with a hunger for a deep personal approach to individual expression, these artists -- such as Martha Graham, Doris Humphrey, Jose Limon Cunningham, Nawkins and Jack Cole, and scores of others -- created or evolved what came to be categorized as modern dance, modern jazz, and other forms that use the word \"modern,\" which basically signifies new, creative, not crystallized in the old tradition. This was also happening in other countries, such as Germany where Mary Wigman, Joos, Kreutscherg and others were turning their backs on what one called \"confined tradition.\" Their artistic, creative expression was an attempt to free their inner choreographic feeling and not to be bound by age-old tradition of any particular form or style. Now, in the 1980's, almost any dance style that does not fit into basic classical ballet, tap, folk dance, flamenco, or other forms, is categorized as modern dance. Modern dancers are a loose collection of individuals expressing their ideas of dance, with some very strong individual artists actually creating unique ways of moving and feeling all their own. They have actually created a bit of tradition themselves, such as the \"Martha Graham School,\" etc.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_06",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 658,
    "article_char_count_full": 3912,
    "article_char_count_review": 3912,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_06::A10",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTHE ACADEMIES OF SPAIN WERE TURNED INTO DANCE FACTORIES [from: La Prensa; c. 1941; sent by Laura Moya; translated by Paco Sevilla] by Juan Martinez The second decadent period was initiated about 1920; although it could be noticed even earlier, it didn't make itself seen until that year and up to 1925 and early 1926. That decadence can be attributed to various causes, but, from my point of view, to three in particular: The frivolous, the lack of good dance teaching, and the need to look for a way to make a living. The same thing happened in Spain with the frivolous as had happened in ancient Greece; there arrived a time when vice was so great that, not only were they criticized by other countries, but Catholic Rome sent troops to combat and defeat for once and for all such a degraded\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"roots\"]\n\nee in particular: The frivolous, the lack of good dance teaching, and the need to look for a way to make a living. The same thing happened in Spain with the frivolous as had happened in ancient Greece; there arrived a time when vice was so great that, not only were they criticized by other countries, but Catholic Rome sent troops to combat and defeat for once and for all such a degraded generation. But, by that time, the Greeks had planted their roots in almost all parts of the world, including Rome where, after victory with its troops and the abolishment of vice, years later there appeared a Rome with even greater and stronger vice than that which had existed in Greece. Although the historic passage has nothing to do with what we are speaking of here, I found it so appropriate as a comparison, I could not but include it here to support the truth. As I mentioned in speaking of the first period, the frivolous was conquered by the great dance artists, but this time it returned with much more boldness and power. Who wasn't familiar with El Paralelo de Ciudad Condal and certain variety shows like El As, El Royalty, La Gran Peña and others of this type whose bosses had the nerve to call \"variety shows?\" In these places, although the police came often, they couldn't stop them, because the doormen warned everybody in time with their buzzers [doorbells]. In the programs at these places could be seen numbers of all colors (perhaps that is why they were given the name \"varietés\n\n[ENDING CONTEXT]\n\nin their city to teach, write to Paco Sevilla, care of Jaleo, for further details. There won't be time for more than four cities, but perhaps some of those who have expressed interest will cancel. MANOLO SANLÚCAR Summer Guitar Course The 2nd International Manolo Sanlucar Flamenco Guitar Course will be held in August in Sanlucar de Barrameda, Cádiz, Spain. The course will be taught by Manolo with emphasis on technique. There will be two, two-week sessions and a translator will be provided for English translation. For further information write Irene Kessei 32 Archadia Rd. Natick, MA 01760\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "JUAN MARTINEZ\" EL ARTE FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_06",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "18",
    "page_number": 18,
    "word_count": 1214,
    "article_char_count_full": 6840,
    "article_char_count_review": 3108,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "roots"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_06::A11",
    "article_text_for_review": "A RECORD REVIEW So I approached this music with a negative outlook. But then there were the impressive album cover notes by none other than Brook Zern, a knowledgeable aficionado and performer of flamenco [Was he bribed, or did he write the liner notes for classical guitar fans, or is his praise deserved?]. Dennis studied extensively with Mario Escudero from the age 14, performed as accompanist with several flamenco dance companies including that of Mariquita Flores, and spent some time in Spain. I won't list Dennis' classical guitar training but it is extensive -- he seems to have good credentials. He has concertized extensively, including two performances in Carnegie Hall in 1975 and 1977. The flamenco opens with \"Joyas de la Alhambra,\" a grandina by Sabicas. Surprisingly, Dennis has good flamenco attack in his technique -- very strong and clean. If you don't have a Sabicas record to listen to, you won't go wrong listening to this. \"Homenaje a Ramón Montoya,\" a rondeña by Mario Escudero, is well-played and sounds like the playing of a young Mario Escudero -- more flamenco, more strength and bite than Mario's current style of playing. I felt that \"Soleares de los Maestros,\" an arrangement by Dennis Koster, was the least effective of the flamenco pieces, being a hodge-podge of familiar themes, mostly those of Sabicas, without much attention to composition. The next piece is",
    "title": "THE TWO SIDES OF DENNIS KOSTER",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_06",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 230,
    "article_char_count_full": 1396,
    "article_char_count_review": 1396,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_06::A12",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTRIUMPH OF CALIXTO SANCHEZ AND PEDRO BACAN IN PARIS by Pierre Lauroua (translated by Paco Sevilla) Calixto Sánchez and Pedro Bacán were in Paris on February 27 and 28 to give two recitals in the Carré Silvia Montfort; the recitals were organized by the association, Flamenco in France. Pedro Bacan came on stage first, warmly applauded, and, while the rain beat savagely on the awnings of the great \"chapitel,\" gave us a splendid toque por soleá, embellished with ascents to the sky and resplendent bursts, a toque that set off the enthusiasm of the audience. Then Calixto arrived, with an immediate ovation, an elegant Calixto, with a red tie, a Calixto visibly moved by this first Parisian recital. The audience was conquered even before he began. Nevertheless, on those two nights the two artists\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"public\"]\n\nhe great \"chapitel,\" gave us a splendid toque por soleá, embellished with ascents to the sky and resplendent bursts, a toque that set off the enthusiasm of the audience. Then Calixto arrived, with an immediate ovation, an elegant Calixto, with a red tie, a Calixto visibly moved by this first Parisian recital. The audience was conquered even before he began. Nevertheless, on those two nights the two artists reinforced the admiration. The Parisian public is accustomed to remaining silent throughout the length of a cante, showing their approval only in the final applause -- something that often disorients the Andalucian artists who are used to immediate and spontaneous reactions. But this time the audience collaborated with the cante, greeting certain accents in the cante of Calixto and the falsetas of Pedro with olés. Throughout the two recitals Calixto was excellent, particularly in the granainas, tangos, martinetes, and fandangos. Clearly, the mastery of Calixto was not a surprise for the aficionados of cante in Paris, but these two nights revealed to the people of the capital the interesting personality of an extraordinary guitarist, Pedro Bacán. The toque of Pedro Bacán is completely personal, different from that of any other man in this art. Perhaps the most important point, and very rare today, is this personality. In the toque of P\n\n[ENDING CONTEXT]\n\nwhat a wonderfully varied selection of coplas: one very Sabicas, a couple very traditional, another of Peruvian influence, and the last, capricious and very much his own. I was feeling such pride -- for Paco, for flamenco, for this marvelous audience. I don't ever remember an evening of flamenco guitar that has left me feeling so fully gratified. The Reception Juan Moran is the gruff but thoroughly charming sevillano who is the owner of Don Juan's, the only restaurant in Atlanta which serves traditional Spanish cuisine. I had been so pleased a few weeks earlier when he had agreed to host a\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CONCERT REVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_06",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 982,
    "article_char_count_full": 6030,
    "article_char_count_review": 2984,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "public"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_06::A13",
    "article_text_for_review": "MARCH JUERGA by Ron Spatz and Yvetta Williams The layout of the \"Intersection Folk Dance Center\" is excellent for spontaneous dancing. The entire middle of the restaurant area is all dance floor, surrounded by tables and booths along the walls. In addition, there is a mezzanine with more tables overlooking the dance floor. The food is very good and inexpensive. Athen Karras, the owner, is a very amiable host. In order for the restaurant to accommodate us, a couple of experimental factors had to be introduced: a) Friday night instead of Saturday. b) Alternating with Greek/Balkan folk dancers. The first of these appeared to affect attendance somewhat, and probably accounted for a level of exhaustion more noticeable than usual. The second situation, while adding an interesting diversion, also prevented any really acceptable level of flamenco ambience from developing. Needless to say, we will try to avoid either of these situations in the future. In spite of these aspects, we still managed to have a very enjoyable evening. Maria Morca worked tirelessly at getting dancers to participate. Oscar Nieto dropped by and provided some dazzling alegrías, both song and dance. Marlene Gael danced some rarely performed (at our juergas, at least) farruca and fandanguillos. Also dancing were Sharlene Moore, Annette Pratte, Joy Padilla, Susanne Mathews, Carolyn Berger, Melanie Kareem, and others. Providing guitar accompaniment were Benjamin A FORMER SAN DIEGO RESIDENT, MARLENE GAEL, DANCES AT L.A. JUERGA (photos by Dick Williams) SHARLENE MOORE SUZANNE MATHEWS Shearer, Bill Freeman, Dennis Hannon, Yvetta Williams, and Ron Spatz. Pictures were provided by Dick Williams, Gary Cooper, and Ron Spatz. We would like to mention here that in the heat of all the things happening at these gatherings (there are more logistics involved in the planning and operating of things than most would imagine), there are those who perform, sometimes brilliantly, and we do not have an opportunity to catch their names or perhaps are not present at the moment. We fervently hope that these people are not offended when we fail to mention them. (One good way to be sure of recognition is to assist us in the operation of our juergas. We could certainly use the help.) We would like to thank all of those in attendance for taking heed of the donation can, and for participating in another enjoyable night of flamenco in Los Angeles. * * * JULY JUERGA AND JALEO BENEFIT by Yvetta Williams Saturday, July 9, 1983, will be the next Los Angeles area juerga at Long Beach Dance Academy -- Studio 2000. Joaquin and Liza Feliciano and Liza's parents, Oscar and Virginia Robles, will host the juerga. It will begin at 8 PM with a half-hour palmas (the art of using hands in clapping rhythms) and workshop conducted by dancer-teacher Maria Morca at 8:15. We encourage all who would like to know more about palmas to come to the workshop and increase their skill and knowledge on palmas. The palmas workshop will begin the fun-filled evening of good flamenco music and fellowship. Anyone with an interest in flamenco and performers at all levels and ages are invited to participate. Bring your instruments, castanets, dance shoes, wear your costumes and plan to participate. Please bring tapas to share, and your own drinks and a donation for juerga expenses. Coffee and tea will be provided. Long Beach Dance Academy Studio 2000 727 South St. Long Beach, CA 90805 Phone 213-423-9886 Take the San Diego Freeway to the Long Beach Freeway North to Del Amo Blvd., turn E. to Atlantic, turn North on Atlantic to South St. (3 blocks past Market -- between 56th and 59th St.), page 70 -- 1D Thomas Map book. ● ● ● We have decided to make this juerga a benefit for the Jaleo magazine which we all appreciate so much and which currently finds itself in financial difficulty. We were astounded to learn that it costs over twelve hundred dollars a month to put out this informative magazine which we all enjoy and take for granted. We hope that everyone will bring a generous donation to help keep Jaleo afloat. All proceeds from this juerga will go to Jaleo. We want to extend a special invitation to San Diego area flamencos to join us also since Long Beach is a little closer to them than some of our juergas. Rubina Carmona Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059 Los Angeles, Ca.",
    "title": "LOS ANGELES JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_06",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "22-24",
    "page_number": 22,
    "word_count": 740,
    "article_char_count_full": 4392,
    "article_char_count_review": 4392,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
