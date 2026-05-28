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
    "article_id": "JALEO_1992_04::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nthere is the music apart. Manuel de Falla is not flamenco but be is tied in the flamenco tradition. ENRIQUE MORENTE: For me, the music is the interesting thing, and I think that we have all come here to talk about music and not race problems...and you know that I like the gypsy race... QUICO RIVAS: Marin brought up an idea that I find very interesting. He spoke of the professionalization of flamenco. Do you all feel that it has been commercialized well, and well developed? Do you think that the possibilities inherent in its energy have been utilized or is this something for the future? DIEGO MANRIQUE: When I spoke earlier about how strange I felt was the case of Pata Negra, it was because I could see, easily, how they were beginning to connect with the gypsies of central Europe, like\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nis the emergence of a \"Prince\" in flamenco art. Prince is the first black artist to have put together a large company and to have the power to produce his own ventures. Something that has always caught my attention about the rumba is that the majority of the musicians who do it are not thinking in terms of the sound, they are thinking in terms of the songs, and they sing what an arranger or producer has made for them. The results are spiendid in many cases, and I don't have a complaint. But what I'm waiting for is the emergence of an artist, whether gypsy or not, whn has a complete vision of what product he wants to create, an artist who thinks not only in terms of his tradition, but of how things work in the market-place. ENRIQUE MORENTE: Ynu are speaking of nine person who does it all, no? Sells the tickets, opens the door of the theater, puts make-up on the artists...? (Laughter) JUAN VERDU: Seriously, now. Ynu were speaking of initiative in the part of the artist. I have never seen more than Las Grecas have. From the moment I saw them, I imagine them with a fusion of earlier music — well, okay, this seems to me to be a small problem and in any case, for this you have the producer, to help in the sketching out of things, to connect things and enable things to be done in the manner which he thinks is best. I think it is a question of time. We\n\n[ENDING CONTEXT]\n\n- wood student $20. PROFESSIONAL GRANADILLO $46 & $56. Victor Galliano P.O.R. Earrings, hats & mantons available. Send stamped self-addressed envelope for price list and ordering details to THE SEA, 305 N. Harbor Blvd., San Pedro, CA 90731 or telephone 310/831-1694. Mariano Córdoba by Mariano Córdoba Home Study Courses with Cassettes Instructions, Exercises, Musical Selections, Techniques Written in Conventional Music Notations and Tablature $25.00 each plus $2.00 shipping Send Cashier's Check or Money Order to: Mariano Córdoba 647 E. Garland Terrace Sunnyvale, California 94086, U.S.A.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Questions and Answers",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1279,
    "article_char_count_full": 7225,
    "article_char_count_review": 2981,
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
    "article_id": "JALEO_1992_04::A8",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nQuestions and Answers Contributed by Paco Sevilla I receive so many interesting questions in the mail that I thought I would share some of them with Jaleo's readers and invite you to send questions of your won via the Jaleo address. I am best at technical and historical matters, but I can probably find information on most subjects except for current events in Spain. Here are a few questions for starters: Q: I know that the rumba and the guajira come from Cuba and that the milonga is said to be based on the milonga of Argentina, but what is the background of the colombianas? A: You are observant in singling out the colombiana as a mystery. Carmen Amaya helped to popularize this cante outside of Spain who she recorded it with Sabicas, but did she learn it in Columbia? Apparently not. Here\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"origin\"]\n\nare a few questions for starters: Q: I know that the rumba and the guajira come from Cuba and that the milonga is said to be based on the milonga of Argentina, but what is the background of the colombianas? A: You are observant in singling out the colombiana as a mystery. Carmen Amaya helped to popularize this cante outside of Spain who she recorded it with Sabicas, but did she learn it in Columbia? Apparently not. Here are two stories about the origin of the colombianas. The guitarist Sahicas tells us that, before the Spanish Civil War, he used to hang out with Pepe Marchena, the most famous and influential cantaor of the first half of this century. Marchena created the opera flamenco period and reigned as its king from the 1920's into the 1950's. He created the fandango grande from the fandanguillo de Huelva and left his mark on all of the cantes libres (without compas) and the Latin American influenced cantes \"de ida y vuelta.\" Sahicas tells us that Marchena was obsessed with changing the cante, with creating new styles and new ways of singing. Marchena \"used to go from bar to bar humming music, trying to develop new ideas and give them his own stamp.\" On one occasion, Sahicas heard something new and asked, \"What is that?\" Marchena thought for a moment and then replied, \"They are colombianas.\" And that is wha\n\n[ENDING CONTEXT]\n\nled to the development of the tientos. It is hard to imagine how the tientos could have evolved from a four-count rhythm (tango), but if you play a traditional tanguillo in the major mode (E'-A), then change to Phrygian mode (B'-A), and theo play slower and slower, you will find yourself playing tientos. Tientos is nothing more than a slow tanguillo in tango tones. The confusion between the two tango rhythms and the tientos continued well into the 1960's. We used to have to deal with all sorts of classifications of the cante: tientos, tientos clásicos, tientos por tango, tientos por zambra,\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Retratos",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 1183,
    "article_char_count_full": 6658,
    "article_char_count_review": 2958,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "origin"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_FALL::A1",
    "article_text_for_review": "Paco de Lucía is preparing a record with Manolo Sanlúcar and will begin a short tour [now completed] of the Soviet Union that will keep him busy in July, performing six or seven days in Moscow and Leningrad. \"I have wanted to visit Russia for some time, but for one reason or another, I haven't been able to. A few months ago I was going to go and then the Chernobyl accident happened; the other members of my group and I were frightened by that and decided to postpone the performances. Even now, two members of my group won't go to the Soviet Union because they haven't gotten over their fear. But I have to go, because I have a contract and won't postpone again, so I just have to face up to it.\" --You say you have an interest in visiting the USSR. Why? \"Because I am curious, and I like to know how everybody lives. I have been in several of the communist countries, such as Poland, Hungary, Yugoslavia and Cuba, but Russia is the center of them all. From what I have seen in those countries so far, as in all places, there are good things and bad. There is nothing perfect.\" Some years ago, flamenco stuck its nose out of the darkness of the cave/tavern, passed quickly through the period of intimate and closed circles, and now occupies the stage in great halls. Performers of jazz and other kinds of music are getting together with flamenco artists to fabricate a mixed product, something different and, in many cases, fascinating. \"I don't know if I would call it a fad. But, definitely, there is a desire among flamencos to unite with other musicians, because, up until now, they have been, to a certain extent, relegated to remaining in a clan, almost a family, in Andalucía, where there was no place for any other type of music. But JALEO - VOLUME IX, No. 3 --So it was a real adventure... \"Exactly! It was to go out on stage without ever knowing what was going to happen. These guys go out before you and play an amazing solo, perhaps an inspired one, and then it is your turn and you know you have to do at least as well, not only for the audience, but for yourself, and for the others, those who are performing with you. Then there is the obligation to motivate each other, to inspire each other in a healthy competition in which the music is the winner.\" --Have any of the purist critics seen the value of these things? \"No, as a rule they think I am crazy or pretentious! Or, some believe I don't like flamenco or don't want to be a flamenco. I imagine there are all kinds of opinions. At first I was very much affected by the critics, because they would say that this music sounds like this or that, that it isn't music, that we sound like insects. Finally, after much thinking, I decided I had to be honest with myself and do what I really wanted and what I thought was best for me. I felt that in no way was this music going to confuse me and would, in fact, open me up and be very positive for me and for flamenco. And I do feel an obligation toward flamenco.\" He is a fanatic when it comes to soccer--every week he plays a game with friends--and he admits to being fascinated by impressionist music: \"Debussy, Ravel, Falla are part of this, no? And Bela Bartok, who is too much.\" He left school for the guitar: \"My father said, one day, I don't have enough money to continue paying for school, much less for you to learn some profession; you know how to read, write and do arithmetic; you know the basics, now devote yourself twelve hours a day to the guitar; because, at eleven years old, you have a chance to do at least one thing well in life, and I believe it was a wise decision.\" But he has never stopped educating himself: \"There was a period when I really liked philosophy, when I was about twenty or so. But I began to get very serious, because I realized that, through the use of logic, you arrive at an ambiguity in which nothing is anything, that is, you are left hanging because everything can be, as the saying goes, according to the eyes of the beholder. That brought me to a state of distress in which I would talk to my friends, getting very deep, and there came a time when nobody was right. It was serious and depressing, and I began to suffer greatly. I have always been a person who has been alone a great deal of the time and I have suffered the neurosis of loneliness, where you believe you are the center of the world and nothing else. So I left philosophy and became interested in other kinds of books, and in elevenness, in clever and witty people, not in books, but in real life, because cleverness is not always in what is said. In literature, Oscar Wide would perhaps serve as an example. Paco de Lucía has never involved himself politically because he says that he doesn't understand politics much, although he has on idea of what is right and wrong and a natural socialist tendency, according to his definition which places freedom above all else. \"For me, freedom is the most important, because, although you can survive eating weeds, you can't do it with a hammer held over your head, threatening to smash you if you move. Dictatorships are unacceptable in any form, no matter what their ideology proclaims. A human being has to be free.\"",
    "title": "PACO DE LUCIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "3,4,6",
    "page_number": 3,
    "word_count": 965,
    "article_char_count_full": 5193,
    "article_char_count_review": 5193,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_FALL::A2",
    "article_text_for_review": "DISTRAUGHT IN TEXAS Dear Editor: This morning (Saturday, Jan. 3, 1987) I awoke to the music of the International Folkways program on the local public radio station, KUT-FM, emanating from my clock radio. In recent years the program has gravitated away from the folk music of Europe and South America to that of Africa and Eastern and Middle Eastern countries. This morning I was treated to a recording of percussion sounds from Ghana. At the end of this twenty minute performance the announcer invited listeners to phone in a request. I thought it would be nice to hear some flamenco guitar music for a change on the program, so I phoned in a request for \"flamenco guitar music.\" The announcer said, \"Hmmm...That might be, er..., refreshing. I'll see what I can come up with.\" In a few minutes he obliged me with his idea of \"flamenca.\" In the next twenty minutes I heard about thirty seconds of genuine flamenco guitar--from the guitar of Ramon de Algeciras (Paco de Lucia's brother) on the record \"Misa Flamenca.\" The rest was random \"pipipipi\" passages in the Phrygian and minor modes with occasional sloppy continuous roll rasgueados and picado runs thrown in. This boring stuff was at times accompanied by bongo or conga drums. Some of the guitar part was played on an electric guitar, and electronic echo effects were used. I'm not going to say who the announcer identified the \"artists\" as or what the names of the albums were. The point is that this supposedly erudite University of Texas public radio station folk music expert thought this was flamenco--\"flamenca,\" he called it. Humph! \"Flamenca,\" Indeed! Next time I will ask specifically for Sabicas or Escudero, or early Paco de Lucía, or Ramon Montoya, or Niño Ricardo or Carlos Ramos. I'm sure he won't know what the hell I'm talking about, but at least he will learn that there once was a folk music known as flamenco which bears very little relation to what he (and most of the new generation) know as \"flamenca\" or whatever they call it. I suppose I'll never understand why it was necessary to destroy the identity of the old forms by insisting on connecting this new music (I'll be generous to flamenco, Why not let it die a noble, dignified death instead of smearing out even the memory of its unique character in this way? To me it is a very sad end. Jerry Lobdill Austin, TX * * * JALEO CORRESPONDENT FOR BOSTON Dear Jaleo, Since no one else seems to relish the task, I humbly offer myself as your Boston area correspondent. To that end, I enclose current information on the Boston scene, such as it is. I would also like to attempt a continuing informational column entitled \"The dance students' corner,\" or something similar, which will include items of interest to those of us trying to increase our knowledge of the baile. My grateful thanks for publishing Jaleo. I know it is no easy task since I published a Middle-Eastern dancers' newsletter here in the Boston area for a couple of years. Looking forward to writing for you. Sincerely, Nanette Hogan [Editor: We want to welcome Nanette aboard as correspondent and columnist and encourage others to act as correspondents for their areas. We need updates on establishments that have flamenco entertainment, give lessons or carry flamenco supplies with area codes and phone numbers for the directory, concert reviews, juerga or performance photos, etc.] JALEO - VOLUME IX, No. 3",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 585,
    "article_char_count_full": 3404,
    "article_char_count_review": 3404,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_FALL::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDear Editor, I have recently picked up my guitar for some serious practice after a period of relative abstinence. The impetus has been the new publication of Paco's Guajiras (see my review elsewhere in this issue). I have been off in a reverie for about a week now, reflecting on the issue of flamenco guitar notation and publication. Let me share some of my thoughts with the readership. In my 28 year love affair with the flamenco guitar I have witnessed a few geniuses of the instrument fall by the wayside, their music unpublished except in audible form on media which probably will not survive for a century. I am reminded of the story of a great and prolific composer/performer for the baroque lute, Bakfark, who, on his deathbed, ordered all of his manuscripts burned. Only a few pieces by\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"master\"]\n\nof my thoughts with the readership. In my 28 year love affair with the flamenco guitar I have witnessed a few geniuses of the instrument fall by the wayside, their music unpublished except in audible form on media which probably will not survive for a century. I am reminded of the story of a great and prolific composer/performer for the baroque lute, Bakfark, who, on his deathbed, ordered all of his manuscripts burned. Only a few pieces by this master survived the holocaust to titillate modern ears when rediscovered by Julian Bream. I suppose there are many reasons why most of the flamenco music may never be published. First, there is the attitude of flamenco players that it is meaningless to write down flamenco guitar music because it is improvisatory and is constantly evolving. They are forever changing what they play themselves, so whatever is set in concrete on a sheet of paper cannot capture the ephemeral essence of flamenco. In addition, there once was--and may still be--the tendency to guard\n\n[ENDING CONTEXT]\n\nThey were at one with Paco. They understood his music, its complexities. Even those who had never heard a fandango could relate. Such are the powers of an artist. Las Porches de Cádiz AUTHENTIC FLAMENCO IN TURN OF THE CENTURY CADIZ DIÁRECTO BY PÁCO SEVILLA & CARLA ENRIQUE JUNG 7 & 14 at 2:00 and 7:30 P.M. THEATER OF THE CENTRO CULTURAL DE LA RAZA 2004 PARK BLVD. 235-6135 (Ample parking in the Navy Hospital across the street) PRODUCED BY CRESCENDO PRODUCTIONS SPONSORED BY EL CENTRO CULTURAL DE LA RAZA (San Diego, CA) YOUR HOST ANTOINE HAGE FLAMENCO ENTERTAINMENT TUESDAY NIGHTS (619) 298-2010\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_FALL",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "5-7",
    "page_number": 5,
    "word_count": 1390,
    "article_char_count_full": 8055,
    "article_char_count_review": 2636,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "master"
      }
    ]
  }
]
```
