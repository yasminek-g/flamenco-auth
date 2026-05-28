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
    "article_id": "JALEO_1988_09::A15",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMaría Benítez Spanish Company [from The New York Times, December 10, 1987; sent by George Ryss] by Jennifer Dunning The María Benítez Spanish Dance Company presented a compelling program on Tuesday at the Joyce Theater, 175 Eighth Avenue. But there was one moment that stood out. It came with a farruca performed by Eduardo Montero. Choreographed by José Luis Ayuste to traditional music, the solo gave Mr. Montero, a guest artist with the company, a chance to let loose with steely, lacy fusillades of foot beats of such exquisitely precise timing and dynamic range that one watched breathlessly. And near the end of the solo, Mr. Montero extended his arms to the audience, his face crumpling, just a little like a singer drawing in his listeners triumphantly as he hits a final, exhilaratingly\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"gesture\"]\n\nic, the solo gave Mr. Montero, a guest artist with the company, a chance to let loose with steely, lacy fusillades of foot beats of such exquisitely precise timing and dynamic range that one watched breathlessly. And near the end of the solo, Mr. Montero extended his arms to the audience, his face crumpling, just a little like a singer drawing in his listeners triumphantly as he hits a final, exhilaratingly exhausting high note. It was a fitting gesture, so challenging a virtuosic tightrope did Mr. Montero probe, at one point seeming to \"play\" his feet like a complex musical instrument. In those few seconds, Mr. Montero offered a perfect, living thesis on artistry and the relationship of the artist and the audience. He spun. He glided. He drifted across the stage on the velvet texture of his footwork like the sleepwalker in George Blanchine's \"Sonambula.\" Then suddenly, at the end, he slowed to an amble, shrugged and walked off the stage in the best boulevardier tradition. Mr. Montero epitomized the cool in the fire-and-ice art of Spanish dancing. And there is a good deal of cool in the performing of Miss Benítez and her company, who will be at the Joyce through Sunday. At first the slightly distanced quality of Miss Benítez's performing was puzzling. No tearing the stage to tatters here, except in the most reasoned of ways. But that distance is one of thoughtful good taste and a kind of purity that connotes great seriousness about one's art. That was evident in Miss Benítez's dances, among them \"Andaluza,\" a new solo choreographed by Rosita Segovia to music by Manuel de Falla. Here and throughout the program, there was much to admire in her long line and lyric articulation of hands and fingers, as well as in the lean and hungry way she moved into the dances, a quality that was nicely enhanced by Mr. Montero's genial and courtly partne\n\n[ENDING CONTEXT]\n\nMartínez from the Conservatory of Granada, and of Michael Lorimer, a student of the former. The flamenco guitarist on the program was none other than the young winner of the Premio Nacional de Flamenco awarded in Sevilla in 1984, José Luis Rodríguez who, if our math is right, must have been all of seventeen years of age at the time. The artists who appeared in this program were the best news of the festival. The second best news of the festival was that the admission for most events was free, for which reason we withhold fire from the heavy artillery. And the hand having written, moves on...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1988_09",
    "year": 1988,
    "language": "en",
    "article_type": "poem",
    "pages": "45-51",
    "page_number": 45,
    "word_count": 3773,
    "article_char_count_full": 22437,
    "article_char_count_review": 3491,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "gesture"
      }
    ]
  },
  {
    "article_id": "JALEO_1989_03::A1",
    "article_text_for_review": "[from: Interviu, Sept 1988, translated by Paco Sevilla] by Maite Arnaiz Three years have passed, but here we are again! Thus, Manuel broke the parentheses of time. Lole had a different story: I want to dedicate this and give thanks to Ilim, the omnipotent and omnipresent; thank you Jesus of Nazareth. Thank you Master! There was an eloquent silence and then Lole burst into song, to the compás of Manuel — as always, like the angels. The more than four thousand people who were congregated in the bullring of Puerto Banús received them by doing palmas por bulerías, the way the flamencos have of showing their affection for their idols, especially when they are gypsy artists like Lole Y Manuel, unique in their art. Nevertheless, something had changed during their period of absence. Now the couple is made up of a respectful scepter and a brand new convert who preaches the existence of God with the words of a knowledgeable theologian, a knowledge she acquired, possibly, during the time that she abandoned her music. Lole was quick to correct me on this latter point. I have abandoned nothing, neither artistically nor emotionally, but I am not going to speak about my feelings. What I do is sing, and that's it! I don't have to sell anything of my private life to any magazine, nor tell anybody about myself. We don't want to get into that material, Lole, but your absence from singing for three years is an obvious fact. I needed to think, that's all! Of course, each person is free to think what to wishes — but only think. Don't forget the saying, 'keep your tongue from evil.' It is also evident, because you show it, that you have had an encounter with the faith of Christ. I say that in public because I want to give my inspiration. I am a new Lole; I don't want to even think about the old me. Also, it is said that drugs had a lot to do with your transformation, and that is a serious matter. I know that has been said. Aside from being serious, people have a nerve to say such things. I have never taken drugs, neither to sing, nor to live. I don't need them. They also said, when I was first starting out, that I was an invalid and blind, because I sang with my eyes closed. There is always some idiot—and people don't know what to invent next—but nobody has had the nerve to say these things to me. What people have asked me is whether I had separated from Manuel and I have answered with the first thing that came to my head because I don't go asking people about their domestic problems. I am a normal woman. I just happen to be famous, but my life belongs to me. At times the price of fame is hard to take! The price of fame is that I agree to give an interview, like this one with you, and sign autographs. But I don't have to account for myself, nor put up with stories about my daughter, Alba, in magazines, stories that say that she is the fruit of a broken home. My daughter, was very sad when she saw that; she is old enough to understand such things. Alba is the fruit of a living couple, a loving couple, because I have always been in love with my husband. Lole is tiny, but not fragile, and she becomes a giant on stage, the equal of a bearded poet: \"Proof of how much I love you is my agreeing to be just a hair pin in your hair.\" Manuel makes the bulería into a delicate artwork when he sings by himself: In reality, I am just a frustrated cantaor. Manuel, your mood has had ill effects when it came to composing? Yes, although I have written quite a bit, and I tried to make a record. But I didn't like anything I was doing, because I have always composed for Lole and, by myself, the possibilities are completely different because I can't do what she can do. So, Lole is absolutely necessary for you?",
    "title": "LOLE REUNITED WITH MANUEL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 699,
    "article_char_count_full": 3733,
    "article_char_count_review": 3733,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1989_03::A2",
    "article_text_for_review": "AN APOLOGY As production editor of Jaleo I want to personally apologize to two of our members for errors in our last issue (Volume X-3). Jacqueline Hegedus contributed a fine article on Elenita Brown Flamenco in Montana (page 38) which somehow was transferred from the typing folder to the proofed folder without having been proofed in any fashion — the consequence being, as Jacqueline took the time to point out to me, was twenty-two typos! On page forty, the announcement of Rosa Montoya's concert was partially gobbled up by the computer omitting names of important participants. This also went to press without being rectified. We will include a repeat of that write-up in this issue. In a third instance, a whole paragraph was omitted by the typist in a record review by our editor Paco Sevilla (page 31). Following the reference to \"cantiñas\" in the seventh paragraph, it should have read, \"Paco de Lucia's record Almoraima,\" followed by the omitted paragraph reviewing the soleares and rumba. My apologies to Jacqueline, Rosa and Paco and to anyone else past or present who have been embarrassed by errors in Jaleo. With your continued feedback and support we hope to continue to improve our quality and content. —Juana DeAlva",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 206,
    "article_char_count_full": 1234,
    "article_char_count_review": 1234,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1989_03::A3",
    "article_text_for_review": "LETTER FROM PARIS Dear Jaleo, I am very glad and surprised to read about Chucales in Jaleo because he is a good friend of mine. I have not heard from him since the day he left Mario Maya's ballet and went to Toronto. I remember at the Café Moca 1987, Mario Maya asked me if I wanted to be his new guitarist to take the place of Paco Cortez. I went to Mario's house for a rehearsal and there I met Chucales. He was open, joyful and very interested in Bossa music. He put a lot of Bossa chords in his toques. He played fantastically when he came to Paris with Mario Maya. We spent good Chucales in Madrid, 1982. FUNDACIÓN ANDALUZA DE FLAMENCO Dear Juana, Though we've never met, your name was given to me by some Los Angeles flamenco enthusiasts as being connected with the Jaleo magazine. In case you hadn't heard about it yet, I thought I'd write and share some information about the new Fundación Andaluzas de Flamenco. The Fundación opened in Jerez de la Frontera last May and is a fabulous institute and resource center. I am including a xerox of their brochure, but of course the xerox can't do justice to how truly beautiful the facility is. On the ground floor are several gallery rooms to display art on the subject of flamenco or have exhibitions on flamenco artists. The next level up has a one hundred seat auditorium for performances and lectures, and runs an on-going slide documentary on flamenco. On the next level there are incredible library facilities. Books to be read on the premises and, most wonderfully, fantastic videos to be viewed on flamenco artists as far back as La Argentina and Carmen Amaya to the most contemporary. On the top floor there are guitar and dance studios. At the moment there are no permanent class offerings but rather, special workshops are offered from time to time. The function of the Fundación is to promote research into, and awareness of, flamenco. They offer research awards. They are a terrific organization run by delightful people. It would be worth your while, I think, contacting them and getting on their mailing list for events and activities. I told them about the magazine and they were most interested in hearing from you. They also sell books and recordings and I'll include a list of the texts available. The Fundación sponsored an international conference in June: Dos Siglos de Flamenco. I had the good fortune to attend the conference and have all their lecture notes from the different presentations. I have started a series of \"Tertulias Flamencas\" roughly one month, where a translation of one of the lectures is presented. I would be happy to have anyone attend who would be interested. They can contact me for more information at (818) 994-5781. Niva Flamenco! Jo Anna Parmelee Los Angeles, CA [Editor: The Fundación is a truly beautiful facility. It is unfortunate that the originally planned collaboration with the Catedra did not take place. Now, there is a competition in one small town, between the two organizations — one with the money and government backing and the other with the knowledge and tradition. I was impressed by the facilities of the Fundación when I was there this September, but not by the organization. Hours were irregular; people working there were extremely ignorant of flamenco—the girls seemed like they should be working in Burger King; the official in charge at that time was completely uninterested in Jaleo; the tape archive was inoperative and the library was not open to the public. Let's hope that my experiences were not representative or that conditions will change. —Paco Sevilla] Cassettes Available GUERRA, AMOR Y CAMPANAS by Basilio Georges Music from the Spanish Dance Arts production of \"For Whom the Bell Tolls\" Send $12.00 check or money order payable to: Spanish Dance Arts Co. 1 University Place New York, NY 10003 (718/626-3185 or 212/473-4605)",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 663,
    "article_char_count_full": 3866,
    "article_char_count_review": 3866,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1989_03::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA GUITAR-PLAYER Crossing the Border of Flamenco at the Zenith of His Life by Gerhard Klingstein Popularity, acknowledgement and fame are a very personal and possibly transient aspect of artistic work. What counts for pos-terity, not only in flamenco, is whether the artist develops the kind of style that becomes tradition in the true sense of the work — that be-comes part of our heritage. The artist's work has to be style-setting to accord it a musical dimension. Up until now only three guitar players of Andalusian folk music can be credited with these qualities: Ramón Montoya Salazar (1880-1948), Manuel Serrapi Sánchez \"Niño Ricardo\" (1904-1972), and Agustín Castellón \"Niño Sabicas\" born in Pamplona 1913. These three stand out from all other guitar players by having contributed to the\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"creative\"]\n\n\"Niño Ricardo\" (1904-1972), and Agustín Castellón \"Niño Sabicas\" born in Pamplona 1913. These three stand out from all other guitar players by having contributed to the musical evolution of flamenco and for their technical innovation. Important developments would have been unimaginable without their collaboration. The fragmentary biographical information on Paco de Lucía is not intended to make a myth out of him, but to illustrate his incredibly creative power and his influence on contemporary flamenco. He has performed on at least 100 LP's, to my knowledge, and has worked with some of the greatest musicians of our time: Antonio Mairena, Camarón de la Isla, Fosforito, Niño Ricardo, La Singla, and Antonio Gades in flamenco; in Jazz and Rock, with P. Itura de, Chick Corea, John McLaughlin, Larry Coryel, Al diMeola, Carlos Santana, Steve Morse and, not least, Ravi Shankar and, recently, the Greek musician Dalaras. In Algeciras, on Calle San Francisco No. 6, opposite from the forest \"La Almoraima\", Francisco Sánchez Gómez was born on Decemb\n\n[ENDING CONTEXT]\n\nrock guitarist, Steve Morse, in Greece Dalaras and, in the USA, Al DiMeola, with whom he recorded the title piece \"mediterranean Sundance\" for the album Elegant Gypsy. These circumstances may have inspired his manager, Barry Marshall, to put these three greats together to form a trio: American Larry Coryel, (later replaced by Al DiMeola) the exceptional guitarist John McLaughlin, and Paco de Lucía. At concerts throughout the world, they celebrated the triumphant success of the acoustic guitar. They sold a million recordings of a concert staged on a Friday evening in December in San Francisco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PACO DE LUCIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1989_03",
    "year": 1989,
    "language": "en",
    "article_type": "other",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 1556,
    "article_char_count_full": 9185,
    "article_char_count_review": 2678,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "creative"
      }
    ]
  }
]
```
