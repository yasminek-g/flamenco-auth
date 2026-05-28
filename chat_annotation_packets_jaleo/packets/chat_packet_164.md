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
    "article_id": "JALEO_1983_03::A10",
    "article_text_for_review": "THE MODERN FLAMENCO BAILADORES DO NOT KNDW AS MANY DANCES AS DID THOSE OF THE PAST [From: La Prensa, c. 1942; sent by Laura Moya; translated by Paco Sevilla] by Juan Martinez The \"chufla\" was applicable to those dancers that lent themselves to humor and parndy, but always within the compás of the tango. The chufla was danced with genius by Manolo Arias, \"El Afilaor\"; he was called \"El Afilaor\" because of an imitation that he created: He would come out with a chair on his shoulders, doing steps with his legs twisted oddly and making funny faces; then, stopping in the center of the stage, putting down the chair, and assuming the posture of the knife sharpener, he would imitate with his mouth the exact sound of the knife blade on the gridstone. Later, he wquuld sing some comical coplas and finish with a series of steps, each more grotesque than the previous and always por tangas. El Canela created imitations of the soldiers of different provinces. El Estampio created \"El Picador,\" a cmical",
    "title": "JUAN MARTINEZ: EL ARTE FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 175,
    "article_char_count_full": 1001,
    "article_char_count_review": 1001,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_03::A11",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFlamenco Guitarist Delights Community Concert Audience [from: Clearwater Tribune (Idaho); April 8, 1982] Community Concerts were true in form on Tuesday, March 30, when a marvelous evening of listening was enjoyed by mare than 200 Orofino concert-goers. Ronald Radford, an Oklahoma man who was'smitten' at 17 years of age by the sound of Flamenco guitar, presented a superb concert. A most personable fellow, be shared the background of each piece prior to his playing it. It was obvious to all that Mr. Radford truly loves his chosen field. The program included pieces of many feelings; the Zambra, of Moorish Arabic influence; a Malaguena tune with a familiar melody; a Guajira that spoke with rhythmic patterns of the Caribbean; Solerares, a deep song from the Gypsies, and a Tarantas, Mr.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"instructor\"]\n\nIt was obvious to all that Mr. Radford truly loves his chosen field. The program included pieces of many feelings; the Zambra, of Moorish Arabic influence; a Malaguena tune with a familiar melody; a Guajira that spoke with rhythmic patterns of the Caribbean; Solerares, a deep song from the Gypsies, and a Tarantas, Mr. Radfard's favorite setting for this music. It was of love that he spoke so often in regard to Flamenco and its art. He told of an instructor in Spain who stressed the love both that a performer has for his music and that that a listener has for the music. Love is the key element in the art of Flamenco. He said that a listener is to listen with his or her heart, not to analyze how it is being played. by Tom and Judy Dixon Watching a Flamenco guitarist is almost as enjoyable as listening to one. The strumming, finger rolls, and finger percussion offered interesting visual effects. Surely, there were many in the audience who, if they were not Flamenco fans, are now. Mr. Radford exemplified the purpose of a Community Concert membership. It is to be a time of listening to a master musicians presenting their art to an audience. In all, the concert was informative, enjoyable...memorable. As there is no written music to follow each guitarist offers his own version of a piece. In the process of learning, and astute knowledge of the variety of emotions\n\n[ENDING CONTEXT]\n\nwith flamenco in eight minutes, but what is possible, they did! I'm sorry to report that flamencos were not near as well-represented in the audience as, for example, the Polez (including my wife), who brought down the house (not including my wife). Now that the readers of Jaleo are aware of the festival's existence, maybe we will have a more respectable turn out next year (the seats are relatively inexpensive). I also hope that, next time, the Germans will be represented with something a little more ethnic than aristocratic Viennese waltzes -- like some accordians and liderhosen maybe?!?\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1370,
    "article_char_count_full": 8096,
    "article_char_count_review": 2998,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "instructor"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_03::A12",
    "article_text_for_review": "\"IT IS SAD THAT EVERYBODY THINKS IT IS ALL RUMBITA\" [from: Guia del Ocio, January 10-16, 1983; sent by David Tamarin; translated by Paco Sevilla] by Maite Contreras J. Manuel Cano, 56 year-old guitarist from Granada, about whom it could be said that he was practically born with a Spanish guitar under his arm, last week obtained the only existing professorship of the flamenco instrument, after an examination which took place in the Conservatorio Superior de Música de Madrid. Manuel Cano will go into the history of the flamenco guitar as the first professor to be involved in the official teaching of music. The position was earned in a competition in the Conservatory of Córdoba. He has spent more than thirty years as a concert artist-ambassador representing flamenco in the world. He has had to make a number of trips around the world in order to accumulate the honors that verify, with facts, that nobody \"is a prophet in his own land\" [recognized in his own land], which is more true here than anywhere else. -- Don't you feel somewhat bitter about having to wait so long and the general lack of interest? \"Flamenco has always been considered a minor art. The reality is painful in spite of the fact that flamenco has been the bastion of Spanish representation in the world.\" -- What does it mean to you to be a professor at this level \"Continuing with the guitar and my students. I intend to bring flamenco to a methodical level of teaching, giving it prestige and the musical tradition that it deserves.\" --When Andrés Segovia was asked which country best understood the flamenco guitar, he responded with Japan. Do you agree? \"Fues, si! Thirty-five percent of my students are foreigners. And in the tours that I have made, I have also come to that conclusion. The Japanese, among foreigners, best assimilates flamenco in all of its aspects and best performs it. The Germans, French, and English all end up tending to have a form of expression that reveals their origin--even when they speak Spanish, they do it with an accent. The Japanese do not. In playing the guitar they reach the point of being transformed, of assimilating their learning into a spontaneous form. There is a hypothesis circulating that the primitive Japanese culture has connections, in its Hindu influence, with the gypsies.\" --The patriarch of the cante, Mairena, insists that the essence of flamenco must not be lost... \"I go along totally with what he says. The guitar has had a great evolution, in both musical forms and in technique. The evolution is good, but not the false affectations that destroy the purity of flamenco. You have to carry it within you...It is sad that the people think that flamenco is just rumbitas.\" --What projects do you have planned? \"To continue as I am doing and to prepare the more advanced lessons. The course of study is planned for five school years, with some compensation made for those who already know something. Each group will have a maximum of six people. Only in this way will the flamenco guitar continue its evolution and provide for each student to adapt to it their own stimulus and spirituality. Each student is a genius in his own personality, and it is necessary to go to great lengths to look for that genius, almost like the process that allowed Andrés Segovia, at the beginning of this century, to take this instrument of the people to the great concert halls of the world. The Spanish guitar will continue its evolution, faithful to tradition.\" MONTHLY OLES IN SAN JOSE by Patri Nader",
    "title": "MANUEL CANO: \"IT IS SAD THAT EVERYBODY THINKS IT IS ALL RUMBITA\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "24",
    "page_number": 24,
    "word_count": 608,
    "article_char_count_full": 3526,
    "article_char_count_review": 3526,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_03::A13",
    "article_text_for_review": "At last there is a central point where guitarists, singers and dancers of all stages of development can meet each other, perform together, exchange ideas, relax and have fun in a beautiful setting. In the few months of its existence, the Society has grown rapidly and yet has maintained the warm and intimate ambiente which is so much a part of a good juerga setting. As a teacher of many years, I am thrilled to be able to support the ideas of the Society in giving students the opportunity to gain experience in such a setting, and it delights me to watch the progress and development of young artists. At the same time, as a professional dancer, the opportunity to be in touch with other professionals helps to further the scope of us all with opportunities for working together which might otherwise not exist. Bravo to the Society founders and the hard-working people behind the scenes who have made all this possible. Juergas are held on the last Wednesday of every month at 8:00pm at Acapulco Mexican Restaurant, 1299 Lawrence's Expressway, Santa Clara, CA. Further information write: Anita Sheer, 5088 Lcne Hill Road, Los Gatos, CA 95030 or call: Dona Reyes 415-851-7286 Luis Angel 408-578-3323 1. LUIS ANGEL 9. RUBINA 10. ADELA VERGARA 2. ROSA MONTOYA 3. PATRI NAOER 11. EMIRA 3. PATRI NAUER 4. DONA REYES 12. MANOLO JURADO 13. GLICERIO MERA 5. DIANA ALEJANDRE 6. CRUZ LUNA 14. LAZARO GUERKE 15. RICARDO DRELLANA 7. PATRI NAOER 8. ANITA SHEER 16. MARIANO COROOBA FLAMENCO SOCIETY OF NORTHERN CALIFORNIA photos by Curtis Fukuda collage by Don Simpson",
    "title": "FLAMENCO SOCIETY OF NORTHERN CALIFORNIA: JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 267,
    "article_char_count_full": 1558,
    "article_char_count_review": 1558,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_03::A14",
    "article_text_for_review": "by Julio Clearfield The January '83 juerga in Philadelphia was so successful and the response was so great that it was necessary to have another the following week. Julia López, dancer/owner of the Meson Don Quixote restaurant in the heart of Old Philadelphia, hosted the festivity, joined by Jorge Navarro, renowned flamenco dancer, and Carlos Rubio with his magic flamenco guitar. When these three combined to perform a siguiriya, the energy, excitement and passion of the dance brought the enthusiastic aficionados to their feet with shouts of \"ole\". Other dancers, Julio Clearfield and Lynn Wozniak, performed sevillanas and fandangos de Huelva with spirit followed by José Termine and Elena Frankel in an alegrías danced with feeling and flair. Accompanists were Shirley Martin, Joe Nonomine, and Paul Izell. Then it was time for the guitarists to take the stage by themselves. Carlos Rubio and Paul Izell played a zapateado. Howard Hoffman performed alegrías and a danza mora. Frank Miller played a tarantos and then Carlos Rubio took the spotlight once more for a solo bulerías which went right to the heart. Dancers new to flamenco, Dolores Luis Gunther and Anna Lisa Mandell, performed sevillanas and fandangos de Huelva. This was followed by Julio Clearfield in a solo alegrías and another performance by Julio López and Jorge Navarro, this time a rumba flamenco with Julia also singing some popular verses with Carlos Rubio. Manoli Mansfield also sang and danced a tientos. The turnout for Philadelphia's first \"double-juerga\" demonstrates once again that Philadelphia is a great flamenco town. The next Philadelphia juerga is scheduled for March 14, 1983, at the Meson Don Quixote, 110 Chestnut Street, Philadelphia. (photos by Lynne Wozniak) CLOCKWISE FROM ABOVE RIGHT: JULIA LOPEZ AND JORGE NAVARRO DANCING TARANTO; JULIA LOPEZ -- SIGUIRIYAS; PACO ALONSO SINGING TARANTOS TO GUITAR OF CARLOS RUBIO AND PALMAS OF JULIA LOPEZ AND JULIO CLEAR FIELD SPANISH DANCE SDCIETY Three pioneers of Spanish dance in the USA have honored the recently formed Spanish Dance Society: La Meri, doyenne of Spanish and ethnic dance in America has agreed to be Patron, and the renowned Carola Goya and Matted have accepted the important roles of Honorary Presidents. Their wisdom and experience will be of inestimable value to the work of the Society in the USA. The Spanish Dance Society was founded in 1965 to promote good teaching of this dance form outside Spain. The American Society was founded in 1982 in Washington, D.C. The Society has formed a method of teaching Spanish dance technique and has evolved a syllabus and an examination system. The first examination in the USA was held last year in Washington at George Washington University, where this method has been incorporated into the dance faculty. Marina Lorca was the external examiner and together with Margarita Jova and Emilio Acosta who came from Spain, they joined the students in three performances, where the work of the Society was demonstrated at the Marvin Theatre. --Marina Keet MENTAL ARTISTRY IN FLAMENCO a revolutionary approach to mastery for dancers and guitarists * improve technical skill * accelerate learning * develop mental & muscular control * enhance performance",
    "title": "PHILADELPHIA'S FIRST DOUBLE-JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_03",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "26-28",
    "page_number": 26,
    "word_count": 520,
    "article_char_count_full": 3246,
    "article_char_count_review": 3246,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
