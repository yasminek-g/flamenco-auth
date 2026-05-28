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
    "article_id": "JALEO_1982_07::A10",
    "article_text_for_review": "JUNE JUERGA by Yvetta Williams and Ron Spatz While not as heavily attended as the April juerga, this one was certainly so less enjoyable. Held in the private audio home of Dr. and Mrs. Colbert (Manuela de Cádiz), there were about 60 attendees. Outlying areas of L.R. were certainly well-represented. General Littleton was down from Bakersfield to grace us with his presence; Halcyon and Joann, a song and dance team from Santa Cruz; Pilar Moreno, Victor Soto, Mirchya and Carlota, up from San Diego; and Carmen Fenoy from Redlands. We were a little light in the guitar department, due at least partly to professional commitments (it's always good to hear people are wozking). However, we managed just fine. In fact, the situation allowed persons who might otherwise hang back, to rise to the occasion. The dancing area was better represented. Juana Escobar was her usual excellent self, and a real spazkplug to the activities. Katina Vrinos gave us a touch of the gypsy caves with her elegant moves; and in watching the young Yrma Horta and Eric Cortes work as a team, one gets a warm, comforting feeling that the future of flamenco dance is in good hands (and feet). Thera were as many stylea as there were dancers; Victor Soto (great alegríaa), Coral Citron, Cristina Pastor, Maria Shippen, many others — all exciting to watch. Pilar Mozeno and Rudy Montoya provided us with excellent cantes. We hope to see the same crowd and more at the August 7th juerga, to be held at the Mexico City Restaurant -- 8:45pm -- 1147 South Street, Long Beach (213) 423-0495. Come early for a great Mexican dinner. Also, there will be an \"End of Summer\" juerga held on the second Saturday of September, back at the Manuela de Cádiz studio -- 10620 8ther Ave. -- 8:00pm. 1. Carmen Fenoy (Redlands) 2. Hostess Manuela de Cádiz 3. Host Roman Colbert 4. Singer Pilar Moreno (San Diego) 5. Virtor Soto (San Diego) 6. Juana Escobar (also #23) 7. Yvetta Williams (also #18) 8. Cristina Pastora 9. David De Alva (also #12 & #20) 10. Eric Cortez 11. Yrma Horta 13. Coral Citron (also #22 & #25) 14. Katina Vrinos 15. Maria Shippen (also #16) 17. Rudy Montoya (also #24) 19. Guy Wrinkle 21. General Littleton (Bakersfield)",
    "title": "JUERGAS IN LOS ANGELES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_07",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 382,
    "article_char_count_full": 2196,
    "article_char_count_review": 2196,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_07::A11",
    "article_text_for_review": "JULY JUERGA JALEISTAS WELCOME MEMBERS FROM AFAR This month we will be honored to have members from some distance joining us at our juerga. Dancer and Jaleo author Marta del Cid and her family from Georgia and as many of those who attended her thanksgiving juerga last November as are able. (See Romeria Aloharetta, Jaleo February 1982.) Teo Morca may be down this way around those dates, guitarist Herb Goullabain is back from Germany and Paco S. and Marfa may be back from Spain. We hope that everyone will turn out at our new juerga location at Gateway Castings. If you have an old shawl or poster to stick up on the wall, bring it along to add more amhiente. Cuadro C has no leader and is almost nan-exis-tent so we ask that everyone plan on pitching in. It is not the juerga-coordinator's job to set up, sit at the entrance all evening and clean up at the end. Date: Saturday July 17 Place: Gateway Casting - 525 West B Street Phone: 234-7611 (night of juerga only) Bring: Tapas DIRECTIONS: Going south on I-5 exit on Front Street, right on Ash, left on Columbia to corner of West B Street. Going north on I-5 exit on 6th Avenue, right on Ash, etc....Freeway 163 south runs into Ash. DIRECIONES: Del I-5 sur, se sale por Front Street, derecho en Ash y izquierdo en Columbia hasta la esquina con West B Street. Del I-5 norte se sale por 6th Avenue, derecho en Ash, etc.... El autobieta 163 hacia el sur sale en Ash Street. ns: Membera & firat guest of S/G Member.....$3.00 Non-Members.....$5.00 Children 15 and under.....$1.00 Ayudantes.....Free AYUDANTES: Helpers will be admitted to the juerga free of charge. They must be current members of Jaleistzas and must notify the juerga coordinator one week prior to the juerga if they wish to help. Please volunteer! It is not fair for one or two persons to have to man the bar or the entrance table all night. We are all members of Jaleistzas and should all share in the work as well as the fun! Call Vicki Dietrich 460-621B or 468-3755. Audantes serán admitidos sin cobrar. Deben de ser socios de jaleistaz y necesitan avisar a la coordinadora de juergas una semana antes de la juerga si quieren ayudar. ¡Por favor, ofrencenses! No es justo que una o dos personas esten atrás del bar o la mesa de entrada todo la noche. ¡Todos somos socios y debemos, compartir no solo en la diversión pero tambien en el trabajo! Llama Yicki Dietrich 460-6218 or 468-3755. e alliopes Greek Taverna Serving Fine Greek Cuisine Join in our lively taverna atmosphere featuring etank dancing nightly: FLAMENCO SHOWS Thursday beginning 7:30 Reservations 281-2610 2927 Meade Ave. (1 block north of E Cajon 8rd. at 30th St.)",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_07",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "22",
    "page_number": 22,
    "word_count": 469,
    "article_char_count_full": 2650,
    "article_char_count_review": 2650,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_08::A1",
    "article_text_for_review": "(Submitted by George Ryss, translated by Juana De Alva [Editor: Juan Pacheco \"Cacharrito de Málaga\" is a Spanish cantaor living and working in the United States primarily on the East Coast. The following are excerpts from an autobiographical letter written by Juan at Mr. Ryss' request.] Although my name is Juan Pacheco, I am known artistically by \"Cacharrito de Málaga.\" I was born in the province of that beautiful Andalucian capital -- to be more exact -- in Alhaurín de la Torre of Málaga. I have my father to thank for my knowledge and love of this difficult art and being the great aficionado that he was, he taught me. He was a guitarist in his prime and thanks to his good teaching I elected \"el cante\" as my profession. I first came to the United States under contract with that genial bailaor, Ciro, working together with Rosa Montoya in the Chateau New Orleans. There I became acquainted with Antonio Vega, another great professional of our flamenco dance. I went with Antonio to Washington, D.C., where we performed for several months at La Taverna and later at Tio Pepe's and at El Bodegon with the agreeable Carlos Ramos. I continued to become oriented in the flamenco ambiente of this great country. I moved to New York where I was introduced to other great professionals such as Domingo Alvarado, Simón Serrano and Marcelo and his Spanish Ballet, with whom I toured Central and South America. I returned to Spain for a long period -- \"to my land\" (Malaga) then came back to New York where I worked with various companies and in various places such as the Segovia Restaurant, Torremolinos, La Paella, La Sangría, etc. One of the companies I worked with was that of María Alba with whom I did my first tour of the United States. In Chicago I also had the opportunity to work with other groups and on several television programs. From my present residence in Miami, Florida, I was contracted by Pascual Olivera and Angela del Moral, perhaps one of the best dancing couples with whom I have worked as cantaor. I also worked previously with another great artist and professional -- Gisela and her Fiesta Flamenca. However, it is Pascual and Angela with whom I have toured most extensively, having the opportunity to sing accompanied by that great professional of the guitar, Juan Serrano. What the newspapers say (sources unidentified): \"Then there is flamenco singer, Cacharrito de Málaga, slim, quick and bursting with energy and impudent humor. He sings, he dances, he banters with the audience in Spanish accompanied by universally understandable gestures and shrugs. With a bit of urging he'll add 'Cabaret' or 'If I Were a Rich Man' in comedy-spiced Spanish.\" \"As a final point I'll talk of Cacharrito, that many-faceted gypsy from some Sacromonte cave, animator, comedian and dancer; a real jewel who with his bulerías, his beautiful voice and great charm will provide you, as did me, with a most delightful evening -- a little piece of Spain in America.\"",
    "title": "CACHARITO DE MALAGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_08",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 509,
    "article_char_count_full": 2974,
    "article_char_count_review": 2974,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_08::A2",
    "article_text_for_review": "FLAMENCO ON THE AIR Dear Jaleo: KPFK, the public supported Los Angeles radio station, featured a program of flamenco music July 7, 1982, from 12 noon to 1:30pm. The program was hosted by Bili Davila. Bill's guests were flamenco guitarist, David De Alva, and flamenco dancer, Dscar Nieta, Oscar explained the origins of flamenco and told about some of the different flamenco farms and played palmas for David's bulerías. David De Alva played three squics beautifully — zapateado, soleares and bulerías. David is a very accomplished 19-year-old guitarist. He plays solo flamenco well and also skillfully accompanies flamenco dance. Bill played flamenco records demonstrating cuadro flamenco, solo flamenco with Carlos Mortoya and Sabicas, Misa flamenco -- Pacó de Lucía, flamenco in classical music -- Torroba's Fandanguilla and ended with Pacó de Lucía and jazz musician Al Dimiola. It was a very enjoyable program. Yvetta Williams Los Angeles, CA FLAMENCO VIOEO CASSETTES? Dear Jaleo, This is the age of the VCR (video cassette recorder). Every large city has a \"video station\" with movies to rent. I wish that someone would make some video cassettes of flamenco dancing/guitar for sale or rent! The price of video recorders keeps dropping and used video recorders are becoming available. I purchased a used machine several months ago (VHS) and have really enjoyed it. One night I rented a camera, got my guitar playing friends together and recorded their classical guitar technique...the results were quite good! I wish that someone would do this for flamenco and make it available for those of us who do not have flamenco guitar teachers or dancers in our areas!! (I will donate my classical-guitar cassette if a rental or lending library is established.) Yours truly, Bill Erinda 3612 Adair St. NW HSV, AL 3581D P.S.: The Walt Disney movie \"Sign of Zorro\" has an interesting flamenco dance interlude in it (although it is somewhat short) for anyone interested. ADVICE ON TRANSCRIPTIONS Dear Jaleo: I would like to tell the Jaleo readers not to buy the Paco de Lucía written music printed in Spain. It's not because it's a bad work, but because the transcriptions are not available for guitar. I suppose the musician who did the transcriptions doesn't know about flamenco guitar with capo. This once happened to Sabicas' \"Flamenco Puro.\" For those who want to know the taques of Paco de Lucía, you are invited to buy the videotape \"Paco de Lucía\" produced by Carl Fisher, recorded at TV1 London (1977). You will enjoy the virtuosity and the fineness of his guitar. For the readers who are interested in the toques of El Serranito, there are two books of toques played by Serranito in the record Victor Monge Serranito (Columbia Estereo TXS 3054). The transcriptions are made by El Serranito himself and Jose Luis Navarro, so there are no problems. Write to: Ediciones Músicales Regueros, 8, l° Izqda Madrid 4 or Gerona, 176 Barcelona Ho Tong Hang Paris, France",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_08",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 490,
    "article_char_count_full": 2962,
    "article_char_count_review": 2962,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_08::A3",
    "article_text_for_review": "PAN O VINO? [Editor: Professor Blanchard's \"Funto De Vista\" is another spin-off from the controversy sparked by Jerry Loddill's \"Time Warp\" fantasy (Jaleo, Feb. 19B2). Never has a single article elicited such reader response (five months of letters and conflicting opinions). We hope that Jerry or another one of our contributors has an article up his or her sleeve which will stir up our readers' juices before the furor from this one dies down.] I would like to offer some observations on the polemic that seems to be raging around Paco de Lucia's, Enrique Melchor's, et al., attempts to introduce musical structures and instruments belonging to other traditions into the flamenco tradition. There seems to be no real problem here other than (1) a problem of terminology and (2) a difference of personal likes/dislikes. The first problem revolves around the two camps in which aficionados in the eighties find themselves: either in the traditionalist camp (I don't like hearing a sitar accompanying cante jondo\") or the evolutionist camp (\"Art consists in change, therefore flamenco music needs to be revolutionized in order to maintain its vitality\"). The approximately two hundred years of flamenco's documented existence defines flamenco music as a certain style of singing often accompanied by guitar and jaleo. Within this same context, flamenco music has undergone many transformations. One good example is the guajira, a Cuban folk song which Spanish soldiers returned from Cuba singing after the Spanish-American War and which later became incorporated into the flamenco repertoire. Many present day artists feel inspired to renovate flamenco music within the traditional context: Exrique Yorente, a master of the cantes antiguos, also has created very different, very personal melodies for the cante, but even with their striking differences, his alegrías, for example, are done 100% within the spirit of the traditional alegrías. My point is this: when you leave the traditional framework of an art, it is no longer the initial art but something else. Oil painting permits a tremendous variation, but when you start gluing bits of metal or ceramic onto the canvas, it becomes a type of graphic art, but it is no longer oil painting as such. In the same way, the \"new flamenco\" is not truly flamenco although it is modern music heavily inspired by flamenco music as such, in the same way that country blues differs tremendously when played by a rock band; it then becomes blues-rock but is too longer country blues. This may be hard news in an age which no longer wishes to respect farm or tradition, nevertheless, it is the case. To sum up: if a certain kind of music appeals to you regardless of its relation to tradition, then as they say in California, \"Go for it!\" But don't call everything flamence that uses the Phrygian scale; as they say here in Badajoz, \"Hay que llamar al pan, pan y vino, vino.\" Brad Blanchard Badajoz, SPAIH “One must call bread and wine wine.”",
    "title": "PUNTO DA VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_08",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 493,
    "article_char_count_full": 2984,
    "article_char_count_review": 2984,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
