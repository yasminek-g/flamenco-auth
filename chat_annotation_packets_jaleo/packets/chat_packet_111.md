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
    "article_id": "JALEO_1981_07::A7",
    "article_text_for_review": "This past year has been such a joyous one for Ohio's flamencos, that we would like to share some of those great moments with you. They include last year's fantastic July bash at the home of Joan and Larry Temo, the mini-juergas, and a surprise anniversary party thrown by Denny Gerheim for his wife, Carlena, for which he brought from New York, the cantaor, Paco Ortiz, and guitarist, Reynaldo Rincon. (photos and information sent by Marta del Cid) 4 FLAMENCO VIOLINIST ART KRISTIN TEMO PAGE 1) CARLENA & MARIJA TEMO DANCING, SINGER PACO ORTIZ, GUITARIST REYNALDO RINCO (CALLED, BY DOMINIC CARO OF N.Y., \"ONE OF THE TOP ACCOMPANISTS IN THE COUNTRY\") 2) MARIJA, PACO & REYNALDO 3) JOSE LUIS GIMENEZ SINGER, GREG WOLFE GUITARIST, MARTA DEL CID PALMAS 4) CARLENA & BOB CLARK WORK ON CANTE 5) CARLENA GERHEIM",
    "title": "FLAMENCOHIO MONTAGE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 139,
    "article_char_count_full": 804,
    "article_char_count_review": 804,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A8",
    "article_text_for_review": "(Editor's note: Guillermo submitted this column more than five months ago -- before our recent series of interviews with flamenco artists. Any similarities with those interviews is therefore due to coincidence or Guillermo's insight into interviews in general.) The following is a fictitious interview with the greatest living exponent of flamenco guitar. Let's call him Senor Duende, a composite of greatness: INTERVIEWER: Mr. Duende, where were you born? DUENDE: In Andalucía, of course. My grandfather was Moorish and my grandmother was Christian. On the other side of my family, my grandmother was Gypsy and my grandfather was Jewish. I'm one of the few truly qualified interpreters of flamenco music. INTERVIEWER: Do you read music? DUENDE: Flamenco music is not written, so it's not necessary to learn to read music. I play with my heart and all my compositions are my own. Reading music is for lesser talents. INTERVIEWER: If you are recognized as the foremost guitarist of all times, then why do several others sometimes get referred to as \"el mejor del mundo?\" DUENDE: (laughs) Hay muchos mundos! INTERVIEWER: What specifically is the difference between a \"genio\" and a \"fenómeno?\" DUENDE: I, for example, am a \"genio.\" The flamenco world is full of \"fenómenos.\" When I was a boy there was another \"genio\" who could also sing. He died before his thirty-first birthday. INTERVIEWER: What kind of strings do you use? DUENDE: It doesn't matter -- the feeling comes through with all brands. Once when I broke a string I continued playing to the astonishment of the audience. Six or five strings, what's the difference? Besides, why should I endorse any string company? What have they done for me? INTERVIEWER: In your concert last night there were certain moments that I just wanted to cry. How do you get that mandolin sound? DUENDE: You must mean \"trémolo.\" Mine is special and I don't mind revealing the fingering, since no one can properly duplicate it. It goes p, m, i, a, m, a, on the three treble strings simultaneously. INTERVIEWER: What do you think of North American guitarists? DUENDE: I don't respect them at all. First of all, they use a pick which is detrimental in two ways. The flesh doesn't come in contact with the strings, so there is loss of feeling. Also a pick limits the use of the right hand. It would be like typing with one finger. Secondly, there is hardly any tradition to draw from. I'd like to hear the guitarists a hundred years from now. Then maybe there would be something of value that might catch my attention. INTERVIEWER: Do you listen to other flamenco guitarists? DUENDE: No, because they just take material I invent and ruin it. INTERVIEWER: What do you do when you are not practicing? DUENDE: I don't practice more than ten minutes before a concert. Other than that I don't practice at all. The \"duende\" cannot possibly come through if a guitarist practices. We are not machines. To answer your question, I spend most of my time with my family and friends. INTERVIEWER: What is your opinion of the classical guitar? DUENDE: Much execution with very little feeling. Most of the older guitarists all play the same repertoire, and the younger ones are searching for identity with their new \"compositions.\" I'm glad they aren't attempting flamenco. INTERVIEWER: Mr. Duende, I respect you very much, but you sound so negative. DUENDE: The truth is the truth. One must play from the heart, not from a piece of paper. Flamenco guitarists are not musicians, they are extensions of the culture. INTERVIEWER: How can you play both traditionally and inventively at the same time? Rubina Carmona Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059 Los Angeles, Ca. JALEO - JULY 1981. Side two begins with \"Sevillanas de las Cuatro Esquinas.\" It is overdubbed with two or three guitars in a nice relaxed aire. Sanlúcar has been known to overdub guitars, even on cante records (one I have with this effect is with La Paquera). \"Soleá Pasito a Paso\" is a slow soleares, very nicely interpreted. \"Guajira Merchelera\" is a happy sounding guajira; this piece also appears on Sanlúcar's later recording, \"Sentimiento,\" as a multiple tracked rendition. \"A Don Ramón Montoya,\" a rondeña, is the only track in the album that didn't suit my taste. It seems long and drawn out to the point that I lost interest in listening. The record ends with \"Noches de la Ribera,\" an alegrías. I believe this is the most interesting and well-composed track and is a nice way to put a finishing touch on the album. If the record isn't in your local store, try ordering it. It's a shame that some people refuse to buy it because it proclaims Sanlúcar as king. Most of them are missing hearing a fine record. --Guillermo Salazar $ ^{*} $ Manuela de Cadiz phone: 213/837-0473 10620 Esther Avenue LOS ANGELES, CALIFORNIA 90064 PRIVATE & GROUP LESSONS FLAMENCO DANCES CLÁSICO PANADEROS (Escuela Andaluzay) (Escuela Bolera) JOTAS (Aragón) MUNEIRA (Galicia) LAGARTERANA (Toledo) $ ^{*} $ $ ^{*} $ $ ^{*} $ $ ^{*} $",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 854,
    "article_char_count_full": 5043,
    "article_char_count_review": 5043,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A9",
    "article_text_for_review": "ROBERTO AMARAL AND COMPANY -- SOME FIERY FLAMENCO AND MUCH, MUCH MORE (from: $ \\underline{\\text{Daily News}} $/Los Angeles, June 16, 1981) by Gillian Rees Ole! Flamenco is alive and well and living in Los Angeles. Roberto Amaral and his sizzling company of dancers and musicians proved it at their Wilshire Ebell concert on Sunday night. This is Spanish dance at its most professional. The dashingly handsome Amaral knows how to provide variety and depth in a program. Sunday's opened with a suite of classical Spanish dances, included some fiery flamenco and ended with an innovative contemporary section. Even though the four-act show lasted almost two hours with no intermission, the pace was swift and the staging original enough to make even the simplest of dances, such as the opening \"Castilla,\" look interesting. A black, wrought-iron gate at the back of the stage created the focal point for the Concierto Flamenco. Here Amaral opted for the familiar structure of virtuoso solos and duets performed to the insistent strumming of guitars, fiercely sensual vocals and rhythmic clapping of the dancers. Performed atop a low platform, his solo \"Zapateado\" brought this section to a tremendous close and showed off his quivering heel work and animalistic attack. Act III, a tavern scene, introduced some love and jealousy among the patrons -- always good for extra spice -- some fine singing by Rubina Carmona, and a steamy duet (entitled \"Sensualidad,\" of course) for Laura Torres and Amaral. The dancers changed from one set of magnificent costumes to another: Pink ballet-inspired gowns (\"Castilla\"), shimmering gold and black dresses (\"Ritmo\"), irridescent blue and yellow flamenco skirts (\"Canastera\"), and everything from ruffled black shirts to embossed leather for the slim-hipped Amaral. A FINE SELECTION OF GUITARS at the American Institute of Guitar With what is probably the largest selection of Classic and Flamenco Guitars in New York City, Antonio David has established a sales office at the American Institute of Guitar, 204 West 55th Street, New York, N.Y. 10019 • Telephone (212) 757-4412 Read interviews with Segovia, Tomas, Romeros, Pujol, and many more. Find out about instrument builders, festivals, competitions, and master classes. Play our new music and lute tablature. Find out what is happening around the world in guitar and lute through- And then came Gitanos Modernos, a selection of dances set in a contemporary night-club -- three guitarists, a conga player and two background-vocal girls stood on platforms behind the dancers, whose black and silver costumes and shiny fans dazzled the eyes. Amaral sings as well as he dances, but the traditional flamenco clapping which continued throughout his songs, \"Para Ti,\" \"Please,\" and \"El Garrotin\" seemed out of place in this setting. * * * RODRIGO AND COMPANY (The following is not intended as a critical review and was written by an acquaintance of some of the performers.) by Yvetta Williams Rodrigo and the Cuadro Flamenco group, including Juana de Alva, Diego Robles, Remedios Flores, María José Diás, El Yuri, Maria \"La Sevillana\" and Carmen Manzón performed at the Wilshire Ebell Theater in Los Angeles, Calif., May 15, 1981, at 8:00 p.m. Rodrigo played solo guitar demonstrating his technical ability and mastery of the instrument. He played solo arrangements of zambra, tientos, tangos, graninas. After an intermission the Cuadro Flamenco group opened with El Yuri accompanying the dancing of Diego Robles and the great singing of Remedios Flores in an alegrías. El Yuri has a fantastic sense of compás and his strong, yet melodic accompaniment style brings out the best in all the performers. He accompanied the complete Cuadro Flamenco section. María José Diás sang her original Servillanas de la Virgen de Guadalupe. She has a strong, exciting voice and her spirit guitar & lute Magazine 1229 Waimanu Street Honolulu, Hawaii 96814 Send for Free Brochure. $2.00-sample copy, $10.00 - 4 issues",
    "title": "CONCERT REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "24",
    "page_number": 24,
    "word_count": 636,
    "article_char_count_full": 3984,
    "article_char_count_review": 3984,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A10",
    "article_text_for_review": "APRIL 27 (Editor's note: we regret that this article could not be included in the June issue but it still serves to let us know that flamenco is doing well in Philadelphia.) by Lynn Wozniak Julia López and guitarist Carlos Rubio have done it again -- another wonderful juerga at Meson Don Quixote. It is amazing how each juerga is better than the last. Nearly 150 enthusiastic aficionados of flamenco attended. Many started lining up at the door an hour early. Frank Miller, Paul Ezell, Howard Hoffman, Shirley Martin and Peter McPherson performed on guitar, while special guest \"Chic,\" (Editor: does the writer refer to Carlos \"Chip\" Lomas?) played works of Moorish and Spanish influence on the oud. While the music played, spirits ran high as dancers gathered at the stage to complete the juerga festivities. Dancers included Julio Clearfield, Lynn Wozniak, Elaine Frankil, Edwardo Bellamy, Carmen, Jose Termine and, dancing at her first juerga, Bacia Zadroga. The finishing touch was provided by gourmet Spanish food, beautifully prepared by Enrique López and served buffet style for the reasonable price of $7.00. Music, passion, and palmas filled the air late into the night. The next Philadelphia juerga will be June 8th at 7:30 p.m. For information, call Julia at 215-925-1889.",
    "title": "PHILADELPHIA JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 209,
    "article_char_count_full": 1284,
    "article_char_count_review": 1284,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A11",
    "article_text_for_review": "by Paco Sevilla \"Fosforito\" is a name that appears frequently in the pages of $ \\underline{\\text{Jaleo}} $. It represents a cantaor who is one of the most highly esteemed in Spain today, seldom absent from the important festivals, and frequently mentioned when important recordings are being discussed. Antonio Fernández Díaz \"Fosforito,\" a non-gypsy, was born in Puente de Genil in the province of Córdoba (near Lucena, about 45 kilometers from the city of Córdoba) in 1932. His nickname, \"Fosforito,\" should not cause him to be confused with the Fosforito of the past, who was from Cádiz. Don Pohren says about Fosforito: \"His entrance into prominence was sensational; he swept all prizes in the 1956 Córdoba contest (for non-professionals), which brought him a flood of recording and performing opportunities and subsequent fame.\" $ ^{5} $(p.162) In the late 1950's, Fosforito made a number of records (Philips), working with guitarists like Vargas Aracelli, Alberto Vélez, and Juan Serrano. In 1963, Ricardo Molina wrote in the daily newspaper, Córdoba: \"Fosforito is, today, a glorious name in the history of the cante. We haven't known anything like it since the times of Cayetano Muriel, \"Niño de Cabra,\" who, we should mention, was not as complete nor as profound as Fosforito. Because, Antonio Fernández has achieved in his youth, a maturity, mastery, and knowledge of the cante in all of its forms. This year we experienced him in the Colegio de la Merced and in the Jardines del Alcázar where he did cantes of Joaquín la Cherna, Diego el Marrurro, and Parrilla el Viejo; we heard polos and malgueñas, alegrías de Cádiz, cantiñas, and fandangos, and in each cante he has showed his mastery and unmistakable personality.\"⁴ Pohren adds: \"Fortunately, Fosforito is a purist and an excellent cantaor...[he] sings a wide range of cantes. His depth within particular cantes is also good, considering his relative youth. He tends strongly towards the serious cantes, as his voice, temperament and demeanor are all of a solemn nature. A non-gypsy, he seems equally at home in both the gypsy and andaluz-inspired cante, although a basic influence of the Córdoba school of cante can be detected in many of his interpretations.\" (p.163) According to Pohren, Fosforito suffered from a throat condition in his early recording years which reduced his resonance and strength; an operation corrected the problem and Fosforito was able to sing at full strength. In the 1960's, Fosforito began to appear on Belter records; one of his records was \"El Cante de Fosforito\" with guitarists, Juar \"Habichuela,\" Juan Maya, and Alberto Vélez. Then, in 1968, he was awarded the National Prize for cante by The Cátedra de Flamencología de Jerez de la Frontera. During this period, Fosforito began a whole series of recordings with Paco de Lucía which culminated in the four-record \"Selección Antológica.\" Later, in the 1970's, he began to make records that specialized in the cantes of particular regiones, as, for example, \"Fosforito en los cantes de Malaga\" (tangos de Piyayo, several malagueñas, jabegotes, polo de Tobalo, jaberas, fandango abandolás de Vélez, etc.) and \"Fosforito en el rincón de Cádiz\" (malagueña de El Mellizo, bulerías de Cádiz, tango de Cádiz, alegrías, peteneras, etc.). Fosforito has been considered by many to be one of today's most complete cantaores and destined to inherit the throne of Antonio Mairena. Here are the views of some flamencologists:",
    "title": "FOSFORITO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 26,
    "word_count": 558,
    "article_char_count_full": 3461,
    "article_char_count_review": 3461,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
