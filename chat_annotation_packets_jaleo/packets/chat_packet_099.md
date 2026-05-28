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
    "article_id": "JALEO_1981_03::A2",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nIf you haven't noticed, IT has finally happened! We were forced to increase all of our subscription-membership rates by three dollars (also known as $3.00). We know what you are thinking, but contrary to popular belief, the editors of Jaleo are not, I repeat, not riding around in Mercedes or Rolls Royce automobiles, using Gucci toilet paper, nor snacking on crab legs and truffles while our massive staff assembles your next issue. At the present time, most of your money -- including contributions, advertising revenue, and money for back issues -- is used to type, print, and mail Jaleo. All of those expenses have increased dramatically in the last year. We now use a professional typist; the printers have raised prices several times, and all international rates and U.S. bulk-mailing rates\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"Jaleo\"]\n\nmbles your next issue. At the present time, most of your money -- including contributions, advertising revenue, and money for back issues -- is used to type, print, and mail Jaleo. All of those expenses have increased dramatically in the last year. We now use a professional typist; the printers have raised prices several times, and all international rates and U.S. bulk-mailing rates have gone up. We hope you agree with us that $ \\underline{\\text{Jaleo}} $ is still a bargain and only made possible by the immense amount of time contributed by volunteers who send us articles, encourage potential subscribers, do our correspondence and financial record keeping, use \"borrowed\" computers to do our directory and mailing labels, put together the magazine, and do the final assembly and mailing. To all those people, let us say, thank you! (ESCUDERO continued) could be realized. So Escudero came to America on his own in early 1932, presenting his first concert on January 17, and was extremely successful, so much so that he returned for a second tour late the same year. \"Reporters swarmed to the boat to meet him and trailed him to his hotel suite. Columns of copy began appearing about his fear of dying at sea and being thrown to the fish, the exploits of his pet cat, and other trivia. \"His arrogant disdain for formal trappings of theater and his flair for improvisation captivated even the most conservative concert goers, and his American tours d\n\n[ENDING CONTEXT]\n\na flamenco fiesta. Ramón Montoya, the great guitarist developed the style of the farruca, which was previously only sung. The name, 'farruca' originated in the region of Galicia, and in the guitar variations one finds musical motifs native to the region. \"In the farruca, the dancers begin to insert acrobatic steps and infect it with the tempos and other peculiarities of other dance forms...I successfully danced the farruca for many years, but I abandoned this dance after I discovered its origins and mystifications.\" \"The zapateado is danced from the waist down; the arms are not brought into\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_03",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "4-11",
    "page_number": 4,
    "word_count": 1443,
    "article_char_count_full": 8639,
    "article_char_count_review": 3079,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "Jaleo"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_03::A3",
    "article_text_for_review": "Alcalá de Guadaira, situated a few kilometers from Sevilla, is a pueblo of deep and vigorous personality and has been a decisive stage for many important chapters in the history of the cante. We naturally had to enter its twisting and The memories of Manolito el de María relative to the cante, were vague and disorderly. That's what usually happens. Rarely will a cantaor agree with another when speaking to us about his flamenco experiences. The most frequent thing is that, after weighing and contrasting judgements, we find ourselves with a greater abundance of indecisions. Manolito el de María supported his ideas about the cante with memories of his own life. He always alluded to the journeys he had to make through these fertile lands of poor farmers where he worked, as God had made him understand that he should, in humble, sporadic occupations. Outside of the geographic environment in which he evolved, his knowledge of the cante was very incomplete. He spoke to us more of cantaores than of cantes -- most of all, of his uncle Joaquín el de la Paula, who had also lived in the caves on the castle's slope. Joaquín created his own exemplary style of solares, elaborated with fragments of other local cantes and enriched with that impressive artistic intuition that the gypsies possess. Flamenco, for Manolito el de María, was like a way of being, like a commandment of his race. It's not important to sing the cante \"to the letter.\" One must feel a \"pellizco\" inside and cry out, calling to one's own self. The cante of the non-gypsies is something else; the non-gypsy sings by ear. The gypsy creates for his own kind, Enrique el de la Paula speaks in dark thrusts of memory about the life and miracles of his father, of the people who made pilgrimages to his cave to hear him -- then in the last years of his sickly, wandering life -- of the famous flamenco stock of Alcalá. Enrique knows the cante of his father -- which is the most pure and genuine local style -- but he can't express it; his voice seizes up in a painful and useless effort that barely reveals the deteriorated outline of the prodigious soleares of Joaquín. It's almost the opposite of what has happened to his sister Merced, who possesses an undeniable expressive capacity, but who has forgotten the noble and incomparable gypsy lesson of Alcalá. 新華社北京3月5日電 MARIO ESCUDERO IN LOS ANGELES (Editor's note: This article was turned in to Jaleo in November by El Chileno. We did not print it because we were anticipating an interview with Mario Escudero; that interview did not materialize yet, so we decided to go ahead with this other material.) by El Chileno Mario Escudero appeared at El Camino College in a solo performance on November 7, 1980. The overall well-balanced program was based mainly on his own arrangements of traditional flamenco pieces, all played in the clean, crisp, unmistakably \"Escudero\" style. The elements of classical guitar technique that are evident in maestro Escudero did not detract at all from his clear flamenco message. His mastery of the instrument is com- MARIO ESCUDERO WITH EL CHILENO (SHAKING HANDS) AND GINO D'AURI. This student came away with the feeling that development of technical skills in the guitar, however important, cannot by itself make you a good (or even average) flamenco guitarist if the \"aire\" or feeling is not there. Is it perhaps that the essence of flamenco goes beyond the guitar alone? After the class, Mario Escudero experimented for a few minutes with Gino d'Auri's electronic guitar, which provided a delightful surprise to everyone - including the maestro himself. MARIO ESCUDERO",
    "title": "ARCHIVO: PART III",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_03",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "12-15",
    "page_number": 12,
    "word_count": 617,
    "article_char_count_full": 3627,
    "article_char_count_review": 3627,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_03::A4",
    "article_text_for_review": "(from $ \\underline{\\text{The}} $ $ \\underline{\\text{Denver}} $ $ \\underline{\\text{Post}} $, November 15, 1979; sent by Guillermo Salazar)",
    "title": "STYLE OF PLAY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_03",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 17,
    "article_char_count_full": 137,
    "article_char_count_review": 137,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_03::A5",
    "article_text_for_review": "The concert life is ideally suited to Mario Escudero. Where is home base? \"The world,\" he laughed. \"I am a gypsy.\" He really is, too, for how else could one absorb the flamenco playing for which he is so noted? \"I started very young -- my father taught me -- and in flamenco, each person is supposed to have his own personality and style. At the beginning you must learn the traditional style, but when you are grown, you begin to impose your way and your feeling. What is my style? I think, like all guitarists, we add certain harmonic ideas, syncopations, developing the phrases more, more intricate; but, of course, to maintain the cadence.\" In fact, Escudero began at age 7. By age 14 he had begun performing with some of the gypsy troupes. \"The only way to learn flamenco,\" he said firmly, \"is to play for dancers. The real flamenco guitarist is supposed to know how to accompany singers and dancers. But today, some young people sometimes play alone, but don't recognize the style of singers and don't know how to accompany the singers or the dancers. Their knowledge is mediocre.\" While the flamenco world recognizes a great range of individuality, it also insists on following traditional rules within the form. Each type of flamenco style, such as soleares, alegrias, malaguena, bulerías, fandango, siguiriya and the like, defines both a locale within the gypsy territory and a set of rules for playing. As Escudero explained very briefly -- \"to explain more, I think, would take a book\" -- the rules of flamenco govern both accent and compás, which is a kind of rhythmic phrase. For a fandango, the accent is on the second beat of a three/four measure, but the compas is over a four measure phrase, or 12 beats. bought and sold. They are unlike material possessions in that once sold the original possessor still retains possession also. In the second case, an artist has invented a passage and refuses to show it to you because he wants credit for it. If he shows it prematurely, others can then say that they invented it. Or it will be taught to others who then spread it around and no one gives credit to the original creator. Most flamencos have made up their minds that holding back is either good or bad. Just like other issues, they want it to be black and white. I can't make up my mind about this and maybe never will. On one side, the people say that holding back is bad because the world is being denied something of beauty. It could be lost forever as in the case of the secret Stradivarius violin finish, if there was such a thing. You can cite the case of the great Manolo de Huelva, who was so eccentric that a whole school of \"toque\" has been virtually lost. On the other hand, the people say, \"Manolo de Huelva had the right idea; I don't blame him a bit. We flamencos work hard to invent material, and others steal it and call it their own. Then recording companies rip us off, don't honor contracts, etc.\" Of course, there is another side to this whole matter and that is what I call antagonism value. It's a very common technique used in the flamenco world, and it's the worst kind of \"guasa\" there is. Here's how it works, but don't make a habit of doing this. You go to Spain and send a postcard to a guitarist friend of yours saying something like the following: \"Arrived Tuesday and found a place to stay. Met a guitarist named Juan who is unbelievable. His stuff is incredibly profound. I'll show you when I come back.\" Then when you come back, you play hard to get a hold of. \"I'm awfully busy; let's get together soon. I'll let you listen to my Rubina Carmona Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_03",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 664,
    "article_char_count_full": 3675,
    "article_char_count_review": 3675,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_03::A6",
    "article_text_for_review": "The XIX Festival and Flamenco Courses in Jerez de la Frontera have been changed to a new date so that they will coincide with the \"Fiesta de la Vendimia de Jerez (Sherry)\" and the \"Tablaos Flamencos\". The two week session will be held from August 24th to September 9th. The first week will be dedicated to seminars and lectures by eminent specialists in flamenco; the second week will feature recitals and concerts by important artists in the cante, baile, and guitar. The climax of the session will be the \"Tablaos Flamencos de la Vendimia\" on the 8th and 9th of September in the bullring of Jerez. Dance classes will be taught by Teresa Martínez and Tomás Torre, with the guitar accompaniment of Gerardo Núñez. Guitar will be taught by Parrilla de Jerez and Pepe Moreno. All classes are at the intermediate level and only those with a general knowledge of their subject should register. Students must send a brief resume of their previous studies along with their registration. Registrations will be accepted until July 24, 1981. The fee for the courses, including performances and Festival activities, is 16,000 pesetas; without the classes, the fee is 5,000 pesetas (roughly $250 and $80 respectively). The address to contact is: Catedra de Flamencología de Hamencologia Apartado 246 Calle Quintos, 1 Jerez de la Frontera, Spain For a complete tour package leaving from the USA, contact: Vicente Granados c/o Wings Corp. 34 8th Ave. New York, NY 10014 辛",
    "title": "VERANO DE ARTE FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_03",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 245,
    "article_char_count_full": 1457,
    "article_char_count_review": 1457,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
