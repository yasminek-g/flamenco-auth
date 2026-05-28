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
    "article_id": "JALEO_1982_06::A2",
    "article_text_for_review": "GUITARIST FINDS HOSPITALITY IN NEW YORK Dear Jaleo; Several months ago while in New York City for a few weeks of seminar meetings, I used some of my spare time to \"rediscover\" the guitar. This happened under the best of circumstances, as I also had discovered, through this newsletter, how to reach Mario Escudero and The American Institute of Guitar. Yet another discovery, a most personal one and the happiest of all, was Mario's capacity as a teacher. Great performer and fine teacher are not always the same person, but with Mario, it is so. Here was an opportunity to gain back some of the facility I had learned in the Bay Area with another excellent instructor, Mariano Córdoba, now out of my reach since I left California. I must mention, too, the warm hospitality shown by Antonio David and other staff members at the Institute in New York. There is nothing artificial there -- you feel at home. In my experience, the place definitely reaches its goal of a \"sharing atmosphere conducive to proper learning.\" Here in Montana, we flamenco enthusiasts are separated by hundreds of miles: The lasting encouragement provided by teachers such as Escudero and Córdoba is vital nourishment. sincerely, Autikmy Beltramo Missoula, Montana TRIBUTE TO PACO SEVILLA Dear Jaleo: Just a quick note to let you know that Faco's work as editor of Jaleo has been a constant source of inspiration for many of us. The service of putting people in touch with people is greatly appreciated. To me, Jaleo magazine provides an historical record of flamenco artists to be collected and treasured as are Don Pohren's books. Yes, writing about an art is not doing the art; however, writing is sharing an art. To be a good sports announcer one needs to know a lot about that sport. Just think where sports fans would be without announcers. There are few people with the knowledge of flamenca who are willing to take the time to write. I'm hoping Jaleo readers will make a greater effort to send usable material (to ensure the survival of Jaleo) now that Paco has stepped down. Thank you, Paco, Sadana, Tucson, AZ Tucson, AZ TIME WARP PRICKS ACHILLES HEEL Dear Jaleo: As ever, thanks for the magazine (April issue). My god! There was certainly some response over the Jerry Loddill article, wasn't there? It just goes to show that, when the pin finds the right Achilles-heel all the stones in the desert creak on their hinges as the inhabitants blink at the sunlight, get off their..., and contribute something of their own point-of-view...All is healthy, of course, whether you agree or not to the opinions expressed because response is the life-blood of a specialist magazine. It was sad to read the editorial re Paco Sevilla's demise as editor but, I expect that it's rather like working behind a bar...it would be nice to get time to enjoy the customer's side once in a while. Of course, no one but the regular editorial staff knows what effort goes into a magazine of Jaleo's quality...Paco will obviously be missed but perhaps we can look forward to a few less \"Tallyrandish\" contributing articles in the future. Good luck Paco! To Juana De Alva, of course, it is hoped that she gets all the necessary help and encouragement (via una copa de vino, or two or several) to keep this magazine afloat. Much luck Juana. Regards, Phil Coram London, England",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_06",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 575,
    "article_char_count_full": 3333,
    "article_char_count_review": 3333,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_06::A3",
    "article_text_for_review": "IN SUPPORT OF \"TIME WARP\" by Marta del Cid Find the common thread we did (Editorial, March '82 Jalea). Jerry Loddill's brilliantly satirical \"Time Warp\" makes a statement cogent enough to satisfy any of the traditionalists among us. He could not have chosen a medium of expression that better amplifies our apprehensions concerning the direction, or detour, that flamenco seems to be taking. Oddly enough, the brief opening piece on Enrique Melchor came across as satirical, too, with an implied warning even stronger, because it was unintentional, than Jerry's. Melchor is quoted as saying, with innocent and enthusiastic sincerity, \"Maybe if I add everything, I will eventually reach every- one!\" -- in an attempt to \"internationalize the appeal\" -- so that it will \"play in Peoria.\" But what is it that will play in Peoria? Reach everyone with what? Certainly not flamenco. I can appreciate an artist's need to stretch and expand himself, to test and challenge his creative resources. This is the definition of a living art. But flamenco is not a popularity contest. No one can be all things to all people -- your image to others becomes fuzzy and indefinite and soon even your own self-image is blurred and indistinct. Ultimately you render yourself ineffective. This is what I see as happening to flamenco if we are not very careful. Flamenco has made its influence felt in many other artistic areas. It has proved an inexhaustible source of inspiration for most of Spain's classical composers -- De Falla, Rodrigo, Albeniz, et al. -- who created a highly identifiable nationalistic style based on its cadencies. Miles Davis' \"Sketches of Spain\" is a jazz foray into the emotional psyche of flamenco. Teo Morca, whom I finally caught a piece of on Sunday mid-afternoon television, can take the baile, with technique and dynamics faithfully intact, superimpose it on a Bach fugue, and make it work with stunning success. And how many of you have found that you can dance a perfect bulerías, complete with desplantes, to the song \"America\" from \"West Side Story?\" Yes, flamenco is a generous contributor. But flamenco cannot absorb. Nor does it need to. It comes equipped with a spare but complete system of built-in dynamics. It is perfectly balanced for the job. As soon as anything extraneous is added (percussion, choral back-up, bassoons, etc.), dilution sets in. More is less. Some of it may be very pleasing to listen to -- I find Paco de Lucía's \"Almoraima\" very decorative and beautiful, but the addition of the oud and the overall tone of the piece makes me respond to it as Arabic music, not flamenco, but then again, it isn't really Arabic, either! I guess the difference is this -- I listen with my head instead of my gut -- I intellectualize instead of reacting. I remember first hearing Eric Clapton's version of \"I Shot the Sheriff\", -- good song, catchy beat. But what a difference when I later heard the original version by Bob Marley. Clapton is a good storyteller and a good musician, but Marley told us the truth -- you really believe he shot the sheriff. And the unadulterated reggae beat, clipped and plugging at the same time, has a dual complexity and delicacy that is shattered in the hands of a rock band. It is an intimate back room pulse that simply vanishes in the big air of an amphitheater. Flamenco, while potent and full-bodied, is similarly very fragile. The more stripped down it is, the more effective it becomes. Less is more. There are those of us who say, \"Do your own thing!\" eut for many of us, this is not our own thing to do with as we wish. It is $ \\underline{\\text{their}} $ thing. We have taken occupancy in a borrowed art form and have an obligation to guard and respect its origins and traditions. As for the true flamencos, for whom art and life are one and the same, I would only hope that they are aware of their responsibility to the future. If they are going to alter the art, what happens when the next wave of artists takes that mutation as a launching point for yet another metamorphosis? Strength of tradition will, as Rebs are fond of saying, fade faster than a Yankee tan. So if you must do your own thing, go ahead and do it. But don't call it flamenco. Call it something else. 一色/色/色一",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_06",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 741,
    "article_char_count_full": 4249,
    "article_char_count_review": 4249,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_06::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[Ed: The idea for this article and interview, as well as the initial contact with Ana Martínez and Paco de Málaga, came from Stan Peters.] by Henry Jova Ana Martínez and Paco de Málaga are a husband and wife team who have been living and performing in the Washington, DC area since 1973. Ana was born in Málaga, but her entire family is from Brenes, in the province of Sevilla. Paco, who was born in Teba in the province of Sevilla, is of a family from the Málaga area. Both came from families with considerable afición for flamenco. Paco's father, uncle and grandfather were competent amateur guitarists; Ana's father was Niño de Brenes, a well-known singer in his time, as well as a dancer, while her mother was a professional dancer and singer, and her sister is currently a dancer. At the age of\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"interpret\"]\n\nlaga area. Both came from families with considerable afición for flamenco. Paco's father, uncle and grandfather were competent amateur guitarists; Ana's father was Niño de Brenes, a well-known singer in his time, as well as a dancer, while her mother was a professional dancer and singer, and her sister is currently a dancer. At the age of nine Ana won an open competition dancing flamenco in Paris and was then contracted by the dancer Mariemma to interpret Manuel de Falla's \"El Amor Brujo\" at the Salle Chopin Pleyel in Paris. She continued dancing in her teen years in France and throughout Europe with her father's dance company and performed on the BBC of London, at the Opera comique, the Salle Chopin Pleyel and the Palais de Chaillot. The company went to South America and performed primarily in Argentina and Brazil, where her father now lives. During her artistic career Ana has worked with notable flamenco artists in and out of Spain including among others: Rafael el Negro, Paco de Ronda, Matilde Coral, Trini de España, El Parruco, Antonio Mairena, and Fosforito. In the fairs of some of the Andalucian towns she has been accompanied by such singers as Romerito de Jerez and Chocolate. Paco began to study the guitar in Algeciras at the age of thirteen with Antonio Sánchez, Paco de Lucía's father. Ramón de Algeciras was a fellow student at the time. The family moved to São Paulo, Brazil, a few years later where Paco was much in demand among aficionados of the Spanish colony. During the next few years he worked with numerous artists in South America and then in Spain. These included La Chunga, Carmen Sevilla, Joselito, Angelillo, Juanito Valderrama, Niño de Utrera, as well as Mario Maya, El Chocolate and Chiquito de Triana. While in Brazil, he recorded two records as Paquito el Malagueño, one of solo g\n\n[ENDING CONTEXT]\n\nin the last few years has changed tremendously. With today's playing style, one dances better. It is more rhythmic and stronger. The guitar has changed so much that comparisons are unfair. Not that many who play today are necessarily better than earlier guitarists, but the rhythmic style played now is something completely different. JALEO: Paco, what are your comments on guitars? What kinds do you play and own? PACO: I have an excellent classical/flamenco guitar made by Pedro Contreras (no relation to Manuel Contreras of Madrid) who works for Ramírez. We became friends in Brazil, where he\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ANA MARTINEZ AND PACO DE MALAGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_06",
    "year": 1982,
    "language": "en",
    "article_type": "poem",
    "pages": "6-14",
    "page_number": 6,
    "word_count": 1139,
    "article_char_count_full": 6530,
    "article_char_count_review": 3455,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "interpret"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_06::A5",
    "article_text_for_review": "GUEST: I don't really have any interest in that. You know that flamenco is something that you do; talk is cheap. All of us love the art of flamenco but why do so many of us get hung up with all of this talk? GUILLERMO: One of the great problems that flamenco has had is the lack of information about it. Jaleo seems to deal with that problem at least. The dissemination of information can never hurt flamenco. GUEST: one of the things I am curious about is, \"Why do many of the American flamcoss go around masquerading as Spanish?\" Do they have some kind of identity problem? Don't they like themselves and their true identities? GUILLERMO: I can't speak for everyone. In my own case it was a question of marketability of a product. The particular chemistry worked so much better that I decided to stay with the name change. That doesn't mean that everyone should change names, nor does it mean that those who do necessarily have an identity crisis; not at all. GUEST: Don't you think it is a social mask? GUILLERMO: It can be if taken too seriously, sure. GUEST: What about the problem of identity then? GUILLERMO: That's a huge question, isn't it? Let's investigate and explore it. It has tremendous ramifications and is not to be brushed aside as \"philosophical\" by anyone serious about flamenco. It isn't a question of philosophy or religion, just common sense. GUEST: The positive side of talking about something is that it helps clear up things. Communication is valuable, I see that. GUILLERMO: When a baby is born, it has no \"orgullo.\" It does not identify with historical events or characters. GUEST: Wait a minute. Are you saying that \"orgullo\" is bad? Be very careful what you say. You know professionals will read this and laugh if you write things that are irresponsible. \"Tonterías\" have their place but don't overdo it. Besides you don't want to hurt anyone. GUILLENNO: What is it that gets hurt when someone claims to be hurt? GUEST: Their soul. GUILLERMO: I don't think so. GUEST: What then? GUILLERMO: Probably the identity, the ego, the image, the concept. It isn't the soul, that's for sure. People get great comfort in knowing who they are, which is an accumulation of facts and events from history told to them by their parents and teachers. Later on this is added to the person's own contributions. This happens all over the world, so then you see the possibility of differing traditions in confrontation with each other. GUEST: That's where the question of purity comes in. Is there any such thing or was there ever any such thing that is pure? GUILLERMO: I think purity is a bad thing with a good name. The purist of today is completely different from the purist of 40 years ago. The purist of today clings to his own experience, even though in his own mind he identifies with his \"antepasados.\" As the old purist dies off, the contemporary purist incorporates. The problem with historians is that usually there is no one alive to disagree with them. GUEST: That would explain why Ramon Montoya was scorned by many as being revolutionary, and now many view him as being true and pure. GUILLERMO: If purity were such a good thing then there would be no intermarriage whatsoever. Black with Black, White with White, you know the old Ku Klux Klan excuse: \"Racial",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_06",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 577,
    "article_char_count_full": 3284,
    "article_char_count_review": 3284,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_06::A6",
    "article_text_for_review": "FLAMENCO SOLO GUITAR PERFORMED BY RODRIGO (from: $ \\underline{\\text{The New York Times}} $, Sunday, May 2, 1982) by Theodore W. Libbey, Jr. A flamenco guitarist who performs under the name of Rodrigo appeared at Carnegie Recital Hall on April 24, offering 12 selections characteristic of the flamenco solo guitar repertory. The recital was admittedly something of a sampler, conducted in an atmosphere more formal than that of a high-spirited juerga; nonetheless, the playing had immediacy and a high level of technical sophistication, and generated a fair measure of enthusiasm among the listeners. The flamenco style is one that, like many forms of specialisation, is limiting as well as liberating. The rapid passage work and complex rhythmic structures lend the idiom unmistakable vitality, through certain things that the guitar can do very well -- particularly in melodic usages, and in the areas of color and nuance -- are not emphasized. Highlights of the program were two rumbas, the Moorish-sounding granainas, and the two concluding works, a seguidilla and piece written by the performer entitled \"Alameda Gitana,\" both of which came across in a highly personal and spontaneous fashion. The first enrore, Malaguena, received a vivid treatment. $ ^{*} $ $ ^{*} $ $ ^{*} $ MORCA IN PASADENA by Ron Spatr There may have been larger dance groups in the southland in recent history, and possibly better ones, however, I for one, cannot remember when. The entire performance was carried off by Teodoro and Isabel Morca providing the dancing, Gerardo Alcalá on guitar, and aubina Carmona cantillating. The program opened quietly with Gerardo playing some nice smooth bulerías of the Paco de Lucía vintage. As the program developed, Gerardo provided a mixture of traditional and progressive toques and postures to suit. (It will be interesting to observe, twenty years from now, how many of the guitarists, that have adopted the cross-legged position, have had their right legs amputated from lack of circulation.) His compás and coordination with the other members was absolutely impeccable. The cantes of Rubina were remarkably moving. She is living proof that one does not have to be Andalucian to become a good cantaora if the desire and ability are present. Isabel in her beautiful costumes was a joy to watch, and Teo has that something in his movements that cannot be explained...only experienced. The size of the stage and the not so good acoustics detracted slightly and rendered a thinness to the otherwise excellent quality of Gerardo's guitar. All in all, the rapport with the audience and the overall ambience was perfect. In fact, the only serious criticism I can muster is that only one performance was scheduled in the Los Angeles area. Maybe this situation will be remedied next time. $ ^{*} $ 泰 泰 GUITARIST SERRANO: ELEGANCE, TASTE AND THE PULSE OF LIFE (from: Chicago Sun Times, April 13, 1982) by Robert C. Marsh Juan Serrano, flamenco guitarist, in a program of his music at International House of the University of Chicago Monday. During part of my mispent youth I could be found nearly every night at a table in a Portuguese nightclub. I appreciate that, despite many common strands of history and culture, Portugal is Portugal and Spain is Spain, especially when it comes to music. But the memory of those weeks in",
    "title": "CONCERT REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_06",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 549,
    "article_char_count_full": 3341,
    "article_char_count_review": 3341,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
