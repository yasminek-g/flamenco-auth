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
    "article_id": "JALEO_1985_10::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[from: $ \\underline{\\text{Europa}} $ $ \\underline{\\text{Viva}} $, Sept. 1985; submitted and translated by Brad Blanchard] \"I was born in La Isla de San Fernando, in Cádiz, the fifth of December in 1950; I grew up there. My name comes from an uncle of mine, because when I was young I was so small and white that he called me Camarón, which is like a tiny shrimp, that's what they call it there. My family was a poor one. I'm poor, I can't read or write, I've always looked to make my living on the street. My father had a blacksmith shop, and I went there to work. The only thing I've done since I was small is work and sing. My mother used to sing, but artists in my family...I'm the only one. And the cante, I think my mother started me in it. When I was born, I already had it in me. I've always\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"tablao\"]\n\ning. My mother used to sing, but artists in my family...I'm the only one. And the cante, I think my mother started me in it. When I was born, I already had it in me. I've always liked it; it's my life and my world, even though when I was young I wanted to be a bullfighter, but I was too afraid.\" \"When I was 16, I started singing professionally, with Miguel de los Reyes, and later I went to Madrid with Dolores Vargas and worked a long time in the tablao Torres Bermejas. There I sang solos, and, finally, it was there that I met Paco de Lucía. He would go to see me, he liked me and we recorded my first record, \"Rintintín.\" Then, later, would come \"Rosa María,\" \"Arte y Majestad,\" \"Castillo de Arena\"... I was in Madrid for 15 years. Now I live in La Linea with my wife, Dolores, my two daughters and my son.\" In a hotel on the Paralelo in Barcelona, it is only two hours before his next performance. In a small room, which he shares with José Fernández \"Tomatito\", the guitarist and friend who accompanied him for years, it smells damp; it is in the low part of the city next to the sea. From the window we see a landscape of patios and uralite roofs which don't help diminish the heat. \"It's hot here, isn't it?\" complains Camarón. \"I would've liked a nicer hotel.\" Sitting across from him, face to face on the beds, with the tape recorder on a table, the man is seen openly. The sunken cheeks, prominent cheekbones, and wide chin, accent the genuinely gypsy features of Camarón. The hot, clear eyes, at times insecure, speak of a delicate man, timid and deeply human. Tiredness and pain are reflected in his face. He has come from Zamora today, and in Zamora he couldn't sleep. A sprained ankle from a stupid fall bothers him constantly and a cold forces him to continuously take Bisolvón, aspirins, decongestant drops and Nolotil for the pain. CAMARON DE LA ISLA AND TOMATITO IN 1982 JALEO - VOLUME\n\n[ENDING CONTEXT]\n\n(from the latest record of the same name): \"Viviré, mientras que el alma me suene, aquí estoy, para marir cuando llegue\" (I'll live as long as I hear my soul, here I am, to die when it comes). What do these words refer to? \"Viviré is an answer to what people are thinking, since they say I do this and that...Also, I did it for my love towards my mother, because people tell her many things about me and she's old now, 'Viviré' is: here I am now to die when it comes, not when I want...out how could I want to die with such beautiful children and my life before me? Now is when I have to struggle,\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CAMARON DE LA ISLA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 1435,
    "article_char_count_full": 7771,
    "article_char_count_review": 3532,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "tablao"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_10::A2",
    "article_text_for_review": "A SECOND OPINION ON MOLINA CONCERT Dear Jaleo, I am shocked and extremely disappointed in your editorial staff far printing the so-called \"review\" of Jose Molina's Bailes Españoles in your most recent issue (Vol. VIII No. 2). I could rebut this disgusting piece of slander point for point, such as when the \"critic\" cites the use of taped music in Carnegie Hall as belonging in a cabaret or rehearsal studio. I would remind him that taped music was also used by the renowned Ballet Nacional and by Mario Maya. However, to rebut the piece in this manner dignifies it way beyond its merit. I will simply say that when a person who does not even identify himself elevates himself to the stature of \"critic\" and uses a forum such as Jaleo from which to make personal attacks upon respected artists it is the publication and its readers who lose. The publication damages its credibility and the readers around the country, indeed around the world, receive irresponsible and incorrect information. What the so-called \"Shah of Iran\" wrote was certainly not a review but a vengeful personal attack on Mr. Molina, the orinting of which is a disgrace. As a member of the audience the night of the performance I would like to reassure the readership of Jaleo that Mr. Molina and his company gave the kind of exciting and dynamic performance we have come to expect and I am sure New Yorkers look forward to his concert return with much anticipation. Regards, Shauna Hankoff N.Y., N.Y.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 255,
    "article_char_count_full": 1472,
    "article_char_count_review": 1472,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_10::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[from: HiFi/Steres Review, January 1966; sent by Guillermo Salazar] [Editor's Comments: The article, written in 1966, not long after the appearance of Donn Pohren's $ \\underline{\\text{The Art of Flamenca}} $, is a condensed version of that book, with some additional material included. It is interesting to read a point of view expressed twenty years ago and then compare it with the present state of flamenca. For example, Donn expresses the complaint that artists were imitating the past and not being creative enough! Today, he would certainly complain of the opposite -- that artist are being too creative and not sticking to tradition.] The art of flamenco is a great deal more than a flashy style of Spanish music and dance, as is commonly believed in this country. It is an expression of a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nstate of flamenca. For example, Donn expresses the complaint that artists were imitating the past and not being creative enough! Today, he would certainly complain of the opposite -- that artist are being too creative and not sticking to tradition.] The art of flamenco is a great deal more than a flashy style of Spanish music and dance, as is commonly believed in this country. It is an expression of a way of life, of the day-to-day activities of many of the inhabitants of southern Spain. To be a \"flamenco,\" one does not have to be a performer -- a flamenco is anyone who is emotionally and actively involved in this unique approach to the problems of living. To understand the art of flamenco, it is absolutely necessary to understand that it is an outward artistic representation of the flamenco way of life. The traditional flamencos are natural actors. Their preferred life is in the streets and cafes, where they can see and be seen, admire and be admired. They enjoy being nattily dressed, and each of them has an indestructible sense of being somebody unique. Flamencos are at once expansive, authoritative, friendly, condescending, formal, dignified, and above all, individualistic. They are not ambitious, and are capable of living happily with only the basic necessities. The concepts and developments of progress are reprehensible and incomprehensible to them. They scorn the rest race and its participants, together with such abnoxious modern phenomena as demanding traffic lights, motorcluttered\n\n[ENDING CONTEXT]\n\nSUMMER COURSE FOR TEACHERS AND DANCERS presented by George Washington University to learn The SYLLABUS of the SPANISH DANCE SOCIETY This structured method includes: a. A detailed syllbus with notes of all exercises and dances. b. Theory covering classical, regional and flamenco Spanish dancing. c. Specially transcribed music in printed book or recorded on cassettes with piano and guitar. Monday June 16th - Thursday July 3rd For further information write to: Prof. Nancy Diers-Johnson Department of HKLS Building K, 817 23rd St, NW George Washington University Washington, DC 20052 (202) 676-6629\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "6-10",
    "page_number": 6,
    "word_count": 3180,
    "article_char_count_full": 19503,
    "article_char_count_review": 3137,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "many"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_10::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[from: $ \\underline{\\text{Lookout}} $, Oct. 1985; sent by El Chileno] by Donn Pohren [Editor's Note: This article, the major part of which was taken from the author's book, A Way of Life, gives us Donn Pohren's point of view some twenty years after the publication of his first book and the article reprinted in the issue of Jaleo. Donn has been pretty much out of the flamenco scene for a number of years, but has resurfaced in order to update his book. This article is followed by a reader's comment from a subsequent issue of Outlook (a magazine for Americans living in Spain); the \"Ugly American\" still lives!] THE LAST 'JUERGA' Whirling dancers showing shapely legs, snap turns calculated to send flying the flowers from their hair, fancy footwork, castanets, a guitarist executing technical\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"tablaos\"]\n\nstill lives!] THE LAST 'JUERGA' Whirling dancers showing shapely legs, snap turns calculated to send flying the flowers from their hair, fancy footwork, castanets, a guitarist executing technical miracles in the background, a singer wailing something or other further in the background... that is nearly everyone's idea of flamenco, be they Spanish or foreign. In fact, it is nearly the only type of flamenco to be found today, the type prevalent in tablaos, night clubs, theatres, even in the flamenco festivals. You might be wondering what other type of flamenco there could possibly be. A key word here is \"sophistication.\" The flamenco described in the above paragraph is a highly sophisticated version of the traditional flamenco of the pueblo. The sophisticated dancer is largely academy trained, often in several styles of dance (classical, flamenco, regional, perhaps jazz), a highly polished technician who eventually transcends the flamenco of down home. Modern flamenco guitarists go the same route, becoming so musically and technically sophisticated they can with ease incorporate universal influences into their flamenco which are recognized and acclaimed by today's international audiences. \"All right,\" I am often told, \"so flamenco is changing with the times. That is necessary for its development.\" That argument does not hold water, for the sophistication and internationalization of flamenco a\n\n[ENDING CONTEXT]\n\nthe latter part of those riotous years (1965) that we decided to open a flamenco centre, in a farm outside of Morón, designed to show international aficionados the real thing. We held some three juergas a week (starting around midnight and lasting until sunrise, if not considerably longer), offered flamenco lessons, and in general helped our guests experience the best Andalusia had to offer. Diego, of course, was a mainstay, both in lessons and many of the juergas. We remained open eight wild years, closing only with the decline of the flamenco life-style, when the artists' fees soared out of\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FLAMENCO: THE WAY IT WAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "11-14",
    "page_number": 11,
    "word_count": 2874,
    "article_char_count_full": 17037,
    "article_char_count_review": 3040,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "tablaos"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_10::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAN INTERVIEW by Guillermo Sajazar Background: Pablo Rodarte, a native of Denver, Colorado, studied under the direction of the Lillian Cushing School of Ballet, was a dancer with the original compsny of the Denver Civic Ballet, and left for New York in 1965 at the age of 18 to pursue a career in Spanish dance and flamenco. After a few months of tutorship under the Jeoffrsy & American Ballet Theatre Schools, he assumed his pursuit for knowledge in the traditional academies of Spain, where he has lived, studied and danced for the past twenty years. He began his training under the tutorship of Antonio Marfn, Mercedes y Albano (family of La Quica y Frssquillo), Pedro Azorln (authority on the Jota Aragonesa), and Miss Karen Marie Taft (Dsnish Instructor in the Bournoville School of Ballet).\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"original\"]\n\nhip of Antonio Marfn, Mercedes y Albano (family of La Quica y Frssquillo), Pedro Azorln (authority on the Jota Aragonesa), and Miss Karen Marie Taft (Dsnish Instructor in the Bournoville School of Ballet). Within a year, Pablo was auditioned and contracted by Rafael de Córdoba to perform as soloist with Mr. Córdoba's company in the \"Festival of Picasso,\" Toulouse, France. The \"Three Cornered Hat\" was the ballet and the company was attired in the original costumes of Pablo Picasso under the auspices of the Paris Opera. Rafael de Córdoba then returned to Spain to resume a tour of the Iberian peninsula appearing in summer \"Festivales de Espana.\" Spain then unaugurated its national television network, and Mr. Rodarte was engaged as lesd dancer in the telecast of its first jazz ballet under the direction of Sandra LeBrgsus, International English Choreographers and \"Ex-Bluebell Girl,\" known for her work in South American and English television. Baing of true pioneer stock, Pablo then returned to the opposite and of the globe, to Australis where he began instructing Spanish Dance for the British Ballet in Sydney, Australia. In due time, the Sydney Symphony Orchestra was to play works of Manuel de Falla conducted by Moshe Ashman, and Mr. Rodarte was asked to dance and choreograph these works, furthering his popularity in Australia, which led to cabaret, theatre and television performances. Also, through his endesvor, Sydney audiences were to witness the opening of the first Spanish Cabaret in Australia. Such was his success that six-month tour of the Fsr East was offered him and a ballet of four, taking them to Indonesia, Singapore, Malaysia, the Philippine Islands, Hong Kong and Macao, also performing in cabarets, theatre and television. Upon receiving news that Spain was to install natsinwide color television, Alberto Lorca, choreographer and director of the company \"Antologia de Is Zarzuela,\" contracted Psbio for one year bringing him back its Spanish soil. Thus, began a new chapter in his artistic career. Submerging himself in the art of flamenco, Pablo Rsdarta began its work the flamenco tablao circuit, performing in the tableos of Los Cabaes and Torres Bermejas in conjunction with RauI, a famous dancer. It was during this time that Mr. Rodarte was to begin studying under Angel Torres, a very well known flamencologist, for whom Mr. Rodarte, to this day, is totally indebted, for the knowledge and insight acquired under his direction. As one of Spain's leading flamenco authorities, Angel Torres has coached such artists as Antonio Gades, Mario Maya and Carmen Mors. Angel Torres has also performed and instructed on the Continent, Australia and in the United States. A Bradway musical then took Mr. Rodarte to the Scandinavian countries. Mr. Bent Medding, Denmsrk's\n\n[ENDING CONTEXT]\n\nmy base has been Madrid, yeah. That's where show business goes on, that's where you're going to get your contacts, that's where you're going to be able to work, right? So almost everybody who wants to pursue flamenco as a career, you know, they come from Andalucía and they always end up in Madrid because that's where the nucleus... that's where you're going to get the...work. Do you ever get employment out of Spain? P: Oh yeah, all the time. I've traveled all over the world through Madrid. I've been all over, I've been as far as Australia, I've been in the Far East, I've been all over Europe.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PABLO RODARTE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_10",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1482,
    "article_char_count_full": 8609,
    "article_char_count_review": 4432,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "original"
      }
    ]
  }
]
```
