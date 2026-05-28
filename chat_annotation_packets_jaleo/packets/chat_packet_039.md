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
    "article_id": "JALEO_1979_05::A3",
    "article_text_for_review": "Very Appreciated Artists and Friends It is a pleasure to direct myself to you, to congratulate you for your steady and magnificent enthusiasm, as well as for your good information, the reports and material about so many good flamenco artists and aficionados. I thank you sincerely for publishing the photograph of the gypsy, TERE MAYA, a great friend and companion in many companies and periods of performing; among them were the three months in the El Chico in New York, with the trio \\\"Los Majos\\\" (ANGEL MONZON, LOTY ESCUDERO, and JOSE MARQUEZ), and in Buenos Aires, Argentina, where TERE MAYA and her brother, JUANELE MAYA, were working with us in the Teatro Avenida for more than a year, as well as in the now-vanished flamenco \\\"colmaos\\\" headed by EL NINO MARCHENA, JESUS PEROSAN, the guitarist ESTEBAN DE SANLUCAR, CARMEN AMAYA, and all of the great stars of the art of flamenco...it would be interminable to name them all. Later, there was our travels through South America and Europe with the company of Jose Greco; TERE MAYA is deserving of the place you have given her in  $ \\\\underline{\\\\text{Jaleo}} $",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_05",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 189,
    "article_char_count_full": 1115,
    "article_char_count_review": 1115,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_05::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Carol Whitney Copyright © 1979 by Carol Whitney All rights reserved Before long I will be offering transcribed fragments from the cante of Manolito el de la María, who died in 1966, and achieved some fame shortly before his death. I have always considered myself extremely fortunate to have met this man, to have heard him sing, and to have taken some (very) informal lessons with him--and chatted with him over many a glass of wine. Manolito's singing was, to me, truly incredible. Also, his very presence, whether in juerga or just sitting around and chatting, gave me a great sensation of joy--because Manolito had a bubbly irrepressibility about him which was totally contagious. As I write about it now, nearly thirteen years after his death, I still feel the joyful quality that he\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Manolito\"]\n\ny) informal lessons with him--and chatted with him over many a glass of wine. Manolito's singing was, to me, truly incredible. Also, his very presence, whether in juerga or just sitting around and chatting, gave me a great sensation of joy--because Manolito had a bubbly irrepressibility about him which was totally contagious. As I write about it now, nearly thirteen years after his death, I still feel the joyful quality that he bestowed. Because Manolito had this quality, I feel I can't offer you transcriptions of his songs unless I first do my very best to capture something of his character for you. I will try, too, to allow for a certain perspective in the portrait I am about to paint, though, as I think about it, I don't know why I should, unless it is to acknowledge that Manolito, like the rest of us, was a human being. Perhaps that is all the perspective needed. Fortunately for me, I attended my first real, Spanish, fiesta flamenca before I first heard Manolito sing. He sang in the second fiesta I attended (these were both at the Finca Espartero, Morón de la Frontera, 1966). I was so bowled over by the experience of the first one that I could hardly grasp what was going on. But by the second time, I was really able to listen. Of course when Manolito sang, I had no idea that his singing was anything particularly unusual by flamenco standards--still, I was nearly breathless with incredulity at the beauty of his cante. Frankly, I didn't find his voice \"foggy,\" as Pohren described it, though I can imagine why it might be called that. I was entranced not only by the beauty of his song, MANOLITO EL DE LA MARIA PHOTO BY C. WHITNEY by his control, his total domination of it\n\n[ENDING CONTEXT]\n\nsober. It turned out that Donn and Luisa had heard the whole thing from upstairs. They said they had never heard Manolito sing so well. I thought about it. It was true that he had sung really extremely well. And there I had been, working hard to accompany well--and still not familiar enough with the guitar and the cante to drop all consciousness of my playing, and so hear all the greatness of Manolito's singing. My tape recorder had sat upstairs the whole time. I had thought, once, of bringing it down, but had found the situation one of those in which one does not interrupt that way.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Manolito de Maria: Impressions",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_05",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "4, 5, 6, 7",
    "page_number": 4,
    "word_count": 2133,
    "article_char_count_full": 11764,
    "article_char_count_review": 3312,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Manolito"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_05::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMANOLITO AND SOLEARES Copyright © 1979 by Carol Whitney All rights reserved Before I start: from now on, you will see an odd-looking device in my column, and perhaps occasionally in my other articles. Sometimes I've referred to earlier columns, or said I'd deal with some subject later. I do this in order to avoid unnecessary repetition and allow some depth of coverage. If what I write here is to be of any real use, cross-reference is important. But the references interrupt and take up space. So to keep them short, and easy to pass over, I'll enclose the minimum information in asterisks and parentheses. References to my column will carry only month and year: (*January 1979*). Those to my other articles will show short title, month and year: (*Diego, October 1979*). Those to anything else\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"Jaleo\"]\n\ne depth of coverage. If what I write here is to be of any real use, cross-reference is important. But the references interrupt and take up space. So to keep them short, and easy to pass over, I'll enclose the minimum information in asterisks and parentheses. References to my column will carry only month and year: (*January 1979*). Those to my other articles will show short title, month and year: (*Diego, October 1979*). Those to anything else in Jaleo will show author's last name, short title if necessary, month and year: (*Lobdill, Bulerías, January 1979*). Where the situation warrants, of course I'll give more extended information. I'll confine \"asterisk\" references to anything that has appeared in Jaleo. Please let me know if you object to this procedure. * * * * Some of the examples of soleares I'll be including here are taken from those of Manolito él de la María. Donn Pohren includes a biography of Manolito in his Lives and Legends of Flamenco; he certainly captures the flavor of what the man was like. Incidentally, Pohren told me that Manolito was the nephew (not the cousin, as stated in Lives and Legends) of the great singer of soleares, Joaquín él de la Paul\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_02 | trigger=\"listener\"]\n\nes on two occasions when he sang. I've transcribed three fragments of his cante: a salida and two coplas. While transcribing one of the coplas, I had great difficulty with one short passage, and I slowed the tape to half speed, in order to hear more clearly. If you are a guitarist, and have slowed tapes of your own playing, you know very well how your faults are magnified by this ruthless treatment. Slowed singing has a quality of its own, and a listener really has to get accustomed to the mooing sound. I'm used to it, but this time, I sat up in my chair, astounded. Manolito's voice, slowed to half its normal speed, didn't sound like a cow. Instead, I could hear a very distinct and steady alternation, on a long note, between the tonic and the note a little less than half a tone above it. Manolito was employing something like a vibrato, to give his sound a color--and his control over his voice wa\n\n[ENDING CONTEXT]\n\nwith different letras, a number of times, before singing a cambio; thereafter, he would either return to the main melody, or change to another style of the soleares (commonly, those of La Serneta of Utrera). Here, I have barely scratched the surface of the soleares, which, as you may have guessed by now, are one of my many favorite flamenco forms. Coming up (*later*): the promised examples, with some analysis, a beat sheet with accompaniment patterns for guitarists, and more detail on how styles are distinguished. Mode is a subject for advanced study, and will come later still. 1000000\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Gaól on Cante",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_05",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "8, 9, 10",
    "page_number": 8,
    "word_count": 1239,
    "article_char_count_full": 7552,
    "article_char_count_review": 3782,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "Jaleo"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "listener"
      }
    ]
  },
  {
    "article_id": "JALEO_1979_05::A6",
    "article_text_for_review": "PHOTO BY JACK MITCHEL and the lovely daughters of the famous ballet teacher Alfredo Corvino, proved that the ethnic dance world and the ballet are very closely related. In a classical Spanish number called \"Of a Long Time Past\", along with Cartagena, they proved the point emphatically by their fine ballet technique and their lovely interpretation of the classical Spanish dance. Adon's Puertas, one of the greatest guitarists in the world, nearly stole the show with his gorgeous interpretations of \"Malagueña\", Romanza\", and the \"Miller's Dance\" from De Falla's wonderful ballet \"The Three Cornered Hat\". He is a warm, gentle, and fine gentleman with a golden touch. Luis Vargas, the flamenco singer, is no stranger to Cape audiences. His style is all his own and indeed added greatly to the success of the dancers. They seemed to adore him and the feeling was mutual! I can only repeat what the whole audience expressed to the Roberto Cartagena Dance Company -- \"OLÉ\", \"BRAVO\", and please come back many more times! * * * ETHNIC DANCE FESTIVAL POPULARITY GROWS From the $ \\underline{\\text{Sunday}} $ $ \\underline{\\text{Cape}} $ $ \\underline{\\text{Code}} $ $ \\underline{\\text{Times}} $，July 24，1977，by Judith Provost. Barnstable -- More and more people seem to be finding their way to the Summer Festival of Ethnic Dance as was evidenced Friday night by the sizeable crowd that turned out to see the Roberto Cartagena Dance Company at The Barnstable Village Hall. Accompanying the lithe Cartagena was a diverse group of artists including dancers Carlota Santana, Andra and Ernesta Corvino, and special guest Azucena Vega; Luis Vargas, singer; and Adonís Puertas, flamenco guitarist.",
    "title": "Roberto Cartagena",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_05",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "10, 11",
    "page_number": 10,
    "word_count": 268,
    "article_char_count_full": 1685,
    "article_char_count_review": 1685,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_05::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nON HORSEBACK BETWEEN TWO EPOCHS; CHACON IS THE TOP OF FLAMENCO ART AND THE BEGINNING OF ITS CORRUPTION. by Angel Alvarez Caballero (from: Madrid's ABC, Feb. 11, 1979; sent by Brook Zern) translated by Roberto Vasquez PART I \"But apon the arrival of Chacón,\" write Molina and Mairena, \"the most perfect and virtuoso of the great masters, we enter another period, which he inaugurated -- the theatrical period -- for which the illustrious French flamencologist, Hilaire, has called him the brilliant fountain of 'bad flamenco'\". In effect, Don Antonio Chacón can serve us as a link between two well defined epochs of flamenco: the classical or Golden Age, and the theatrical, in which the cante advanced one more step toward the conquest of greater audiences. This was Chacón's task. In January 1979\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Marchena\"]\n\nthe brilliant fountain of 'bad flamenco'\". In effect, Don Antonio Chacón can serve us as a link between two well defined epochs of flamenco: the classical or Golden Age, and the theatrical, in which the cante advanced one more step toward the conquest of greater audiences. This was Chacón's task. In January 1979 it will be a half century since his death. Before, Silverio Franconetti had taken the cante from the tavern to the café cantante. Then Marchena would close the cycle, taking flamenco to a total vulgarization with the opera flamenca. Three payos (non-gypsies) lead the way in these three decisive operations in the evolution of flamenco. Chance? Coincidence in a way of visualizing the art? A greater vision of the future? DON ANTONIO CHACÓN the guitarist, Javier Molina, who was very close to the cantaor in his adolescence and his first years in the art, seems to sug-gest otherwise when he says, referring to a date maybe one or two years later, \"...then we talked to Junquera about Antonio Chacón so that he would also hire him. But Junquera did not want him because Chacón wasn't worth much at that time and all the singers that he had were better than he. Through the force of our pleas and recommendations, he hired him for a few days, but had to discharge him because he wasn't liked, in spite of being from Jerez.\" At any rate, that meeting in Junquera's must have taken place, but I believe some years later for the reasons that we will see. Next to Hermosilla were nothing less than the cantaores Enrique el Mellizo -- who had a close friendship with the bullfighter, and Joaquin Laserna, also called Lacherna, a gypsy from Jerez who was Manuel Torre's uncle. All of the people present were amazed at the young man's singing, especially Enrique el Mellizo, who, according to the somewhat imaginative version of Manfredi, \"ri\n\n[ENDING CONTEXT]\n\ntime the old master of Vélez replied, \"I am telling you from this Café de Chinitas that has heard so many malagueñas, that you sing better than I this new malagueña.\" This is the way Manfredi Cano narrates it, and this episode was left imprinted in a copla that is still being repeated over and over: En el Café de Chinitas canto una copla Chacón, y le contestó Juan Breva, cantas tu mejor que yo esa malaguena nueva. At the Café de Chinitas a new copla was sung by Chacón to which Juan Breva responded, you sing better than I, this new malagueña. (Next month: Part II, \"ON THE ROADS OF ANDALUCIÁ\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "DON ANTONIO CHACON, PAPA DEL CANTE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_05",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "12, 13, 14",
    "page_number": 12,
    "word_count": 1075,
    "article_char_count_full": 6179,
    "article_char_count_review": 3475,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Marchena"
      }
    ]
  }
]
```
