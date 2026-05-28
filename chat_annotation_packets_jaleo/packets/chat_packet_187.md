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
    "article_id": "JALEO_1984_03::A1",
    "article_text_for_review": "What is a man like this, payo and born in a marginal flamenco province, whose family didn't even have Andalucian roots, doing in the cante? \"Well, it's true that nobody sang in my family, but my father liked the cante very much. He was a cartwright; he owned horses and was always going to different places and sometimes he was in contact with cantaores... but he himself didn't sing.\" When he was just an adolescent, Juan went to Barcelona and there he started to frequent flamenco circles, some bars owned by the guitarists Miguel Borrull and Dorado, also a guitarist. The first day, that his friends announced that he knew how to sing and he sang, he was so scared that his cigarette fell from his hands. He has always been like that, a man gripped by fear in the moment of truth. The Cátedra de Flamencología de Jerez has just awarded him the Premio Nacional a la Maestría, in recognition of his long career, his honesty and dignity. \"I truly appreciate this award, but a maestro? I really am not a maestro at anything. I have sung the best I could, learning from all the good artists, because there have been very great artists, and also putting into it what was my own if it was necessary, if it made sense.\" Actually, Juan Varea has created cante, especially some forms of fandangos. There are some fandangos attributed to Niño León, that in reality were created by Varea. \"En lo alto de la loma/ quién tuviera una casita...\" It doesn't bother me that they have been",
    "title": "VETERAN CANTAOR RECEIVES TRIBUTE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_03",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 266,
    "article_char_count_full": 1473,
    "article_char_count_review": 1473,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_03::A2",
    "article_text_for_review": "[from: $ \\underline{\\text{El País}} $, Feb. 6, 1984; sent by Brad Blanchard; translated by Mary Sol West; by Fernando Quinoes Diminutive and lean, with a contracted face, as though to disappear inside his own unassuming person, as though to retreat to the interior fires of his cante, Juan Varea Segura has his finest hour tonight. Surrounded by a flamenco court opposed of all the most important representatives of the toque, cante, and baile today, the most important will be the smallest \"er mas chiquito\" as Manolo Vargas and Pericón used to call him in Madrid in the now closed sanctuary, La Zambra. Naturally they were only talking about height. Because from that smallness of dignity and sad temperament emanates today, after 76 February's, as it always did, a big outpour of solid cante, delivered with, great effort, knowledge, delivery, and enamsiled with Chacon's power or with the good ancient metals of Triana, Cádiz, or Jerez. Born away from the cante areas, in Burriana de Castellon, Juan Varea exemplifies one of those cases - not so few - in which the flamencn are becomes living and veement flesh in somebody who, for geographical reasons, it wasn't supposed to. But destiny is bigger than reason. And there, in his cante, in his more than half a century full of flamenco truths, are the orly reasons for this Juan Varea to whom Madrid pays homage tonight in the Monumental. And while he is still alive, the way it should be. JALEO THANKS THE FOLLOWING CONTRIBUTORS: Marilyn Perrin L.A. Jaleistas Yvetta Williams Gerry England - Gift Subscriptions - Donation - Jaleo Sales - Donation",
    "title": "JUAN'S NIGHT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_03",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 270,
    "article_char_count_full": 1601,
    "article_char_count_review": 1601,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_03::A3",
    "article_text_for_review": "A POEM SHAREO Dear Jaleo, Many years ago I found a little booklet in a used book store. It was entitled \"Thirty Spanish Poems of Lova and Exile.\" The poems were translated into English. I think I paid $0.50 for it. At the time I recall being very impressed by one of the pnets. Time went by, and the little book became lost among my many shelves of books. I was recently delighted to find it again and reread the poem that impressed me most of all. Before the little booklet disappears again I want to share that poem with the reader-ship of Jalen. POEM It is not true, sorrow, that I have known you. You are the nostalgia of a good life, The solitude of a somber heart, A boat without shipwreck and without star. Like a lost dog, wandering, Sniffing and hunting aimlasesly For his road, without a road, like A child on a holiday night Lost among the crowds, The dusty air, the flickering Candles, stunned, his heart drunk With music and hurt, So I go, drunk and melancholy, Lunatic guitarist, poet, A poor man in a dream, Hunting for God in the mists. Antonio Machado (1875-1939) To me this is incredibly powerful stuff although I am not a religious person. I'd be willing to bet that Sr. Machado was at least a closet guitarist himself. Jerry Lobdill SHOE PRICES INCREASE Jalao, Hi, a brief note to let you know that Menkes has raised its shoe price. Now 5500 Ptas + 1000 for shipping = 6500 Ptas, total for women's dance shoes. Don't know about the men's. As it costs $7.90 for the bank draft - I had to send a second one when they wrote about the increased price - it's worth it to know the correct cost! The shoes, incidentally are fine. Shirley Orbeck \"Viviana\" Portland, OR BOOKS SOUCHT Dear Jalo, Could you please give me an address to write to to purchase Lives and Legends of Flamenco by Donn Pohren and The Flamenco Dance by Juisa Pohren? G.B.W. England Wellington, New Zealand [Editor: Reader response to this letter would be appreciated]",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_03",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 357,
    "article_char_count_full": 1950,
    "article_char_count_review": 1950,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_03::A5",
    "article_text_for_review": "Dear Editor, I enclose details of a Summer Course in Spain which is being organized with the support of our Peña. We would be grateful if you would give it some coverage in your next issue. Hoping you will be able to help, with many thanks, Yours sincerely, Richard Fletcher, Chairman, Peña Flamenca de Londres. London, England Maribel la Manchega, the Spanish flamenco dancer who has been working and teaching in London for some time, is holding her first dance course in Spain this Summer. Maribel is officially-appointed professor to the Andalucian peñas in London and also teaches dance classes almost daily for Spanish and English students. She is principal dancer in her own and other flamenco companies and recently filled the 2,500-seat Barbican Centre in one of the most successful flamenco shows seen in London in recent years. The course, which is supported by the Peña Flamenca de Londres, will be held at Estepona (about 1 hour by bus from Malaga) for two weeks from 6th to 18th August. Students will be divided into 2 groups - Beginners and Intermediate/Advanced. Class sizes will be limited to about 12 students. Teaching will take place during the day but informal sessions will be organized every evening, where local singers and musicians will be invited to join in. All students who want to will be encouraged to take part in these 'juergas', whether beginners or advanced. Dances to be taught include sevillanas for beginners, and bulerías and siguirias for those more advanced, but all will have the opportunity to practice and perform other dances that they may know - such as alegrías, fandangos, tangos, etc., particularly in the evenings. Maribel attaches great importance to rhythm compas and style. The steps she intends to teach will be relatively simple, leaving plenty of time for sessions on compas, palmas, pitos, etc., and posture - particularly correct positioning of the arms, hands, head and back. There will be a resident guitarist throughout the course. The cost of the course will be £60 sterling per week. This does not include board and accommodation, for which students will make their own arrangements. Details of reasonably-priced accommodations will be sent on request. As the number of places is limited, students wishing to enroll should send a deposit of £30 without delay to Maribel la Manchega, 47 Hamilton Crescent, London N.13., (tel. D1-886 2141), from whom further information can be obtained. The balance of the course fee, £90, should be sent by 30th June. NEW ZEALANDER IN SEARCH OF HOSPITALITY IN THE U.S. Dear Jaleo, The main reason I am writing is to ask your advice and recommendations regarding an idea I had recently. As you can well imagine the flamenco scene in New Zealand is practically non-existent and consequently it is hard to maintain interest and inspiration. It has always been my wish to visit the U.S. at some stage and compared with Spain it is a lot easier and cheaper to get to. I was thinking that it may be possible to correspond with someone in the U.S. on a more personal basis, with a view, perhaps, of staying with them for a short while. That is to say a flamenco enthusiast like myself. Due to the nature of my work I would only be able to be away for about a month and I have to plan a trip like this well in advance. I would be most grateful for any help or advice with this matter. Best of luck with the continued success of your magazine. Warmest regards, Gerry England Wellington, New Zealand (Editor: Anyone wishing to correspond with Gerry may write to Jaleo for his address.)",
    "title": "EL COJO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_03",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 612,
    "article_char_count_full": 3570,
    "article_char_count_review": 3570,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_03::A6",
    "article_text_for_review": "(Sant by Georgia Ryss) Dominico Caro studied the art of flamenco singing in Madrid, Sevilla and Jerez de la Frontera, learning his trade from the graat masters such as Antcnic Mairena, Fosforito, Bernarda y Fernanda from Utrera and accompanied by the guitar of the late Diego Del Gastor. He has lived with the gypsies in Morcn and Jerez and feels that this experience has been invaluable to his interpretation of flamenco cante. On his frequent visits to Spain, Dominico has sung for such noted teachers and choreographers as Maria Rosa Merced, Ciro, Paco Fernández and La Tati. As a singer, and a dancer when inspired at \"juergas\", Dominico has become known for his vast knowledge of his art and the clarity and power of his round, mellow voice (voz redonda). He has toured nationally with José Molina, ending both seasons at Carnegie Hall. Dominico is the resident singar with both Rosario Galán and her Ballet Español, who recently shared the bill with Dame Margot Fonteyn at Jacob's Pillow and with José Molina since 1981. When not on tour Mr. Caro sings for the flamenco workshops in the Harkness House of Ballat, makes Yago Sangria commercials and is often seen in New York's famous night spot, from the Chateau Madrid to the Waldorf Astoria, the Sheraton, the Hilton and on Broadway. A highlight in Dominico's career has been touring with legendary José Greco. He has appeared on television with Steve Lawrence and Eydie Gorme, has sung on the Johnny Carson's \"Tonight Show\", and has worked with David Frost and Liza Minelli at the Philharmonic Hall at Lincoln Center in New York City and the Kennedy Center in Washington, D.C. At the moment he finds himself with the ambitious project called the New Jersey Art Academy of which he is Director and teacher as well. RARE LIVE RECORDINGS DIEGD DEL GASTOR LIVE La Fernanda de Utrera - singer Manolito de la Maria - singer Fernandillo de Moron - dancer Circuito Mercantii Fieste, Moron de la Frontera 1964, 2x60 min. high quality, normal bias, Dolby B, mono setting tapes. Includes commentary and selected tetras and translations. Please send $25 plus $3 for taxes, air mail and handing. MANOLO DE HUELVA - GUITARIST An Extremely Rare and Unique Recording Luis Cabaifero - singer Sewilia 1968. La Cuadra. Only known live recording in existence. One side of a 45 min. high quality, normal bias. Dolby B, mono tape and short commentary. Please send $15 plus $2 for taxas, air mali & handling. To order send check or money order to: ZINCALI RECORDING CO. 1185 GHEZEM RD. BLUE LAKE, CA 95525 Please expect 4-6 weeks for delivery. Defective tapes will be replaced.",
    "title": "DOMINICO CARO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_03",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 446,
    "article_char_count_full": 2612,
    "article_char_count_review": 2612,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
