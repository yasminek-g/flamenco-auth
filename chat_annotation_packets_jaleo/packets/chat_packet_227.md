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
    "article_id": "JALEO_1992_01::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAngeles cannot be summarized in writing nor in this article. An artist herself as well as a businesswoman, Ms. Lawlnr uniquely understands, from the inside out, the soul of an artist. A sensitive, earthy woman, Ms. Lawlor has acted as a producer, presenter, manager, mother, ehauffer, confidant, and adoring fan to almost everyone who crosses her path. She literally loves what she does, and likely does not realize fully her contribution. The flamenco community in Los Angeles is quite fortunate to have in its midst a presenter of this caliber; a lady whose first concern is her artists, whose second concern is in presenting the art of flamenco well, and that's it. 1992 will likely see another outstanding year of flamenco in Los Angeles. Viva tu, Deborah! To learn flamenco you have to listen\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"Access\"]\n\ne literally loves what she does, and likely does not realize fully her contribution. The flamenco community in Los Angeles is quite fortunate to have in its midst a presenter of this caliber; a lady whose first concern is her artists, whose second concern is in presenting the art of flamenco well, and that's it. 1992 will likely see another outstanding year of flamenco in Los Angeles. Viva tu, Deborah! To learn flamenco you have to listen to it! Access to the world of flamenco awaits you in four volumes that review most Spanish recordings released 1970-1990. Send for information about catalogues, \"The Living Flamenco Anthology\", books, and more. Special offer for teachers. JOSE MOLINA BAILES ESPANOLES Ambassador Auditorium, Pasadena, CA. Nov. 24, 1991. The farewell performance of Jose Molina and company found the \"a gusto\" Molina delightful and generous on the closing afternoon performance. I found the program well balanced and choreographically charming. Molina danced two solos, a \"Caña\" and a \"Tangos de Malaga\" as though they were his last. The first half of the program was devoted almost entirely to the Spani\n\n[ENDING CONTEXT]\n\nare a lot of Americans who get involved in flamenco for the same reasons? I find that here, especially in San Francisco -- I don't know where else -- the profession is hard because you can't really be compensated economically; you can't really take it on as a profession. So people really can't go for it. I find it really hard to survive here with flamenco. So people do have to take it on as a hobby -- it's a part time job, it's not their all the way interest. They have other jobs, they don't have time to go every day. To really learn you've got to put a cruple hours into it every day, non-\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Interview: An American in Spain: Tania Leullieux",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_01",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "10-13",
    "page_number": 10,
    "word_count": 1631,
    "article_char_count_full": 9134,
    "article_char_count_review": 2752,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "Access"
      }
    ]
  },
  {
    "article_id": "JALEO_1992_01::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nstop, and people don't have the time for it. I think that they like it, and they would like to, but there's no way out of it. There's nowhere to work. ... What you've seen so far since you've been back in this country...how do you find flamenco here in America? I find it needs to be more innovative. I think flamenco here is kind of normal -- it's thousands of miles away. You can get a few videos, and not that many people have access to them, and not that many shows come through here. There a lot of things going on in Spain with flamenco -- a lot of innovation, a lot of things being introduced like different dance styles -- it's becoming more theatrical. There are still the flamenco puras, which is when they just go out and dance without thinking about the lighting or choreography. Since\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"adapt\"]\n\nke it more theatrical so that it's more visual. Not only that, musically, the flamenco guitar has advanced so much, it's incredible. It's really modernized, and that has made the dance change. The dance is really influenced by the guitar, and now the guitar tries to work with the dancer, and you try to make a whole musical piece out of a dance, not just a dance. What we try is to make an escobia musical and the guitar to go along with it and you adapt with the guitar and the guitar adapts to you. The whole piece in the end is a whole montaje -- it's a whole musical piece and a dance piece, with each thing having meaning -- each step, the lighting -- it's a whole concept. It's really interesting how it's all developing. -- In your opinion, what are some of the most interesting innovations you've seen over the last few years? Basically, what I was telling you about -- working out a structure that's related to the guitar, to the lighting, to the way of dancing, introducing more expressive movements, bigger movements and not so limited to traditional flamenco. -- Does anything that you've seen really stick in your mind as really interesting? Well, I've seen it in different ways. I saw Cristina Hoycos' show, and she's a renl modern dancer. Her dances are incredible because she's got a Int of aire and she's very flamenca, and at the same time she'll do things all of a sudden that are really modern, and still her show is very traditional. The lighting is beautiful, it's kind of like Gades -- she worked with him a lot -- she has a lot of knowledge about the lighting, it's really fantastic. But she is sticking to traditional flamenco -- the costumes, she's not putting any other type of music of instruments in it. Her dancing is very modern. Joaquin Ruiz is also very innovative. He's got a pretty good imagination. He's very modern too. He listens to a lot of different kinds of music and introduces different kinds of beats -- he gets a lot going. His shows are really interesting, too. He int\n\n[ENDING CONTEXT]\n\nit can go really far because you can theatricalize it, and you can do many incredible things with it. But it doesn't have enough support. There's nobody who will really back it up. I can understand that, too, because it's not easy to deal with flamenco, especially if you deal with the gypsies. It's hard to make a real production of it. -- You feel there's not enough support in Spain or in the world? In the world. There aren't any hackers or people who are willing to put money into it. But it's been proven that whenever there's been a company with strong backers... they've made money. All\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Review: Videos de la Luz",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_01",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 1185,
    "article_char_count_full": 6243,
    "article_char_count_review": 3635,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "adapt"
      }
    ]
  },
  {
    "article_id": "JALEO_1992_01::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTHE LANGUAGE OF SPANISH DANCE By MATTEO Marcellus Vitucci By Marcellus Vittucci With Carola Goya Foreword by Richard Cragun Drawings by Louis Gioia Flamenco Guitar Music by Peter Baime Piano Arrangements by Marc Saint-Germain “Although we’ve been watching Spanish dancing for decades, we must confess that what we don’t know about it could fill a book. Recently, it has, in fact: a big, remarkable volume from the University of Oklahoma Press.”—The New Yorker. “Your book, The Language of Spanish Dance, is an extraordinary accomplishment—not only because of your profound and untiring investigation on the subject, but also because it is so easy to understand. Congratulations! I am certain that future Spanish dancers and dance teachers will forever be grateful to you.”—José Greco. “It is a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"teacher\"]\n\nact: a big, remarkable volume from the University of Oklahoma Press.”—The New Yorker. “Your book, The Language of Spanish Dance, is an extraordinary accomplishment—not only because of your profound and untiring investigation on the subject, but also because it is so easy to understand. Congratulations! I am certain that future Spanish dancers and dance teachers will forever be grateful to you.”—José Greco. “It is a valuable resource for dancers, teachers, critics and aficionados. It embraces all aspects of Spanish dance with clearly defined dance terms, drawings and unusual photos.”—María Benítez. “A great gift from America to Spanish culture! So wonderfully researched that I learned many things. When, oh when will it appear in Spain?”—Pilar Rioja. “A wonderful addition to the small world of books on Spanish dance: A big ole! to MATTEO.”—Teo Morca. \"With a flick of a wrist, the rotation of a foot, the high arching of th\n\n[ENDING CONTEXT]\n\nwas a giant por solea and siguiriya. So, for me, it was a great honor to play for Mairena. I began to think and asked myself, \"Bueno, how would this man like me to play for him?\" I thought of Melchor and said to myself, \"Melchor would play for him this way, so I will play for him the same way.\" I believe the guitarist has to accompany the cante and make the cantaor feel comfortable, and in that moment the way to make Antonio Mairena feel comfortable was to play for him the way Melchor used to do it. So I thought of Melchor, put on the face and hands of Melchor, and played like Melchor.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Confessions of Paco de Lucia",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_01",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "15-18",
    "page_number": 15,
    "word_count": 2112,
    "article_char_count_full": 11646,
    "article_char_count_review": 2549,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "teacher"
      }
    ]
  },
  {
    "article_id": "JALEO_1992_01::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nI was filled with anger, not because it supposed an insult to me, but because it was an insult to flamenco. At that instant I recalled all the suffering of my father, how badly flamenco had always been treated, and I thought that, if this concert had been held in any other country, without exception, the names of the artists would have all been the same size. And this was happening in my country, in Sevilla. The home of flamenco! We are the ones who must support flamenco, for it is a music of great strength, a music I have fought for around the world, something I don't do for money nor for fame -- because I have enough money to live and more fame than I want. I wasn't born in be a star. I was born to be a spectator. My personality is not that of a star, that is, I don't want fams and I\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"historic\"]\n\nght for around the world, something I don't do for money nor for fame -- because I have enough money to live and more fame than I want. I wasn't born in be a star. I was born to be a spectator. My personality is not that of a star, that is, I don't want fams and I want that made clear, because I believe that Antonio Burgos wrote the other day about this event that I didn't play because of excess vanity, something that is not true. I rebelled for historical reasons. Flamenco has always been badly treated and continues to be without reason. We should be proud of flamenco because it is ours and because it is one of the most important musics in the world. If I am an important figure in flamenco, if I am up here, and they announce me that way, how would they announce others? That is, this says that my music, the music I represent, does not have sufficient importance to be announced in the same manner as that of Julio Iglesias. I am convinced that if I were a melodic singer, with the same fame that I have now, it would have been announced the same as the others. They only presented me that way because I am a flamenco. That is how I took it, and I said I would not play. And the most indignant part is that this happened to me in Sevilla. The Andalucian people have two main traits: one is genius and the other is superficiality. The middle-class\n\n[ENDING CONTEXT]\n\ncalled Candela. It is a type of venta where the gypsies gather to sing, dance, and play, and spend many nights there. I believe the last time was about a month ago. -- Do you recall any particularly fond moments in your career, or perhaps the opposite, something you would like to forget? Neither. Hombre, it gives me great pleasure to have known Camaron, to have lived with people of that type; with Favorito I also spent a beautiful period, and with Lebrijano. In general, I have had extraordinary moments with all of the people in flamenco, for I have spent time with them all and I have played\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Dice Don Quijote",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_01",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 1269,
    "article_char_count_full": 6838,
    "article_char_count_review": 2983,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "historic"
      }
    ]
  },
  {
    "article_id": "JALEO_1992_04::A1",
    "article_text_for_review": "CONTENTS Bruce Patterson..... Editor Lora Gorton..... Typing Contributors: George Ryss Eric Patterson Paco Sevilla Homero Cates Yaelisa Cover: Special thanks to Luisa Triana Jaleo Magazine is a sole proprietorship, published 4 times yearly. Subscription is $25 per year for bulk mailing (allow 2-3 weeks), $30 per year for First Class, Canada and Mexico; $30 for Europe (surface) and $35 Europe for First Class. ANNOUNCEMENTS, with the exception of classified ads, are free of charge to members. For further info on ad rates, send for brochure. BACK ISSUES OF JALEO are available only through Juana de Alva. Please write to: 1721 Lisbon Lane, El Cajon, CA 92019 or call (619) 440-5279. Inquiries regarding past subscriptions should be addressed to Juana de Alva.",
    "title": "Avisos",
    "periodical": "jaleo",
    "issue_id": "JALEO_1992_04",
    "year": 1992,
    "language": "en",
    "article_type": "other",
    "pages": "1-2",
    "page_number": 1,
    "word_count": 121,
    "article_char_count_full": 762,
    "article_char_count_review": 762,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
