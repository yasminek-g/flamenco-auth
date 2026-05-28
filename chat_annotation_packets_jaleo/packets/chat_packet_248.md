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
    "article_id": "JALEO_1986_SUMMER::A19",
    "article_text_for_review": "[from: El Pais, May 15, 1986; sent and translated by The Shah of Iran] by Francis Luis Cordova The National Competition of Flamenco Art of Córdoba which was celebrated from the 17th to the 23rd of May has reached its thirtieth anniversary laden with a prestige and respectability unforseen by its creators in 1956. The Grand Theatre played a special role in the special tribute City Hall paid these events. On its renovated stage, this artistic coloseum was inaugurated officially with a recital commemorating the 30th anniversary of the National Flamenco Competition which featured the participation of Antonio Fernández Díaz \"Fosforito\", Victor Monge \"Serranito\", Matilde Corral and Paco Cepero. This national competition has 133 participants; 91 in cante, 23 in dance and 19 in guitar. This massive enrollment denotes the transcendency and enlargement of this contest in the general flamenco world. Ever since Fosforito opened the list of winners in 1956, the most illustrious names in cante, baile and toque have been included in this triannual event. Among these have been Antonio Mairena who received the \"golden key\" for cante, José Menese, Paquera de Jerez, El Lebrijano, Chano Lobato, El Cabrero, Mario Maya, Carmen Albéniz, Paco de Lucía, Manolo Sanlucar, Paco Cepero, Habichuela, and Merengue de Córdoba. The creation of this contest has its precedent 24 years earlier in the competition celebrated in Granada in 1922 brought about by the efforts of Manuel de Falla and Federico Garcia Lorca. The City Hall of Córdoba took up this torch which was extinguished the same year, and with the help of some aficionados, particularly Ricardo Molina in whose memory a journalism prize has been created in conjunction with the contest, called the first edition of this contest in order to impregnate the traditionally festive Cordovan May with Andalusian art, every three years. The contest establishes, besides the award for the most complete cantaor, six distinct prizes in the category of cantes, four in baile, and two in guitar. In order to underline the artistic diversity of flamenco, the different palos, seguirias, soleares, alegrías, tarantas, peteneras, bulerías, fandangos, serranas, among others are expressly acknowledged. Besides the possibilities of career advancement the festival offers the participants, there are 12 prizes of 100,000 pesetas and one of 200,000.",
    "title": "A FRAME FOR FLAMENCO COMPETITION",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "poem",
    "pages": "37",
    "page_number": 37,
    "word_count": 373,
    "article_char_count_full": 2383,
    "article_char_count_review": 2383,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A20",
    "article_text_for_review": "[from: E1 Pals, April 18, 1986; sent by Brad Blanchard; translated by Charlene Gerheim.] by A. Alvarez Caballero This year's Jornadas Flamencas de Fuentabrada turned out to be two very interesting sessions. The first was dedicated to the café cantante, with a warm exchange of conversation with Romualdo Molina. Then a video from a program on T.V.E. was shown that featured La Rubia and El Cunaria. Gabriel Morena was accompanied by the outstanding guitar of Carlos Pardo. Gabriel Moreno was a perfect choice for the video. Not",
    "title": "II JORNAS FLAMENCAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "37",
    "page_number": 37,
    "word_count": 87,
    "article_char_count_full": 527,
    "article_char_count_review": 527,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A21",
    "article_text_for_review": "[from: El Pafis, April 23, 1986; sent by Brad Blanchard; translated by Nancy Lee Ruyter] by A. Alvarez Caballero India Gitana (cante) and Juan Salazar (toque). Fernanda and Bernarda de Utrera (cante) and Paco del Gastor (toque). Los Paríle de Jerez: Manuel and Juan (toque) and Ane (baile), with Antonio de Malena (cante). Le Paquera (cante) and her family, Los Méndez (cante and baile): Tía Dolores, Luise Torrán, Tío Eduardo, Tía Pili, Paco Ruiz, Manuela de Jerez, Refael Agarrado, Pepe Méndez, La Margari, Paqui Flores, Alonso Flores, Joselito Méndez, Antonio Méndez, Rubichi and Alonso Flores. Niña Jero (toque). Medrid, teatro Alcalá Palace, April 24. Before the fiesta, there was a round of soleares, and that was good because the soleé is the mother of the bulerfa and because Le Fernanda was there--if that one does not sing por soleé, it is as if we are left orphans. It was without doubt Fernanda's voice that was the most impressive of the evening, even though to the majority, it did not seem so. This voice was the most handa/janda, profound, without brilliance, muted, going with much struggle to where it wanted to arrive--when it managed it. But from the beginning \"ey!\", already it was producing in us a twist of shivering and chill that I will never know how to explain. Before that, El India Giteno had sung. He is also almost always great in eleares, and he was on this occasion; afterwards, in the jaleos de Badajoz (almost the equivalent of the buterfes), worried that a piece of scenery was about to fall on him, Ej India was already a bit thrown off the track. La Paquera came out to leave us speechless with that peculiar personality of hers, and she did leave us speechless of course, in a manner completely opposite from La Fernanda. She has a sharp voice, ability, and can permit herself the luxury of leaving the microphone when she feels like it. She first sang bulerfa por soleá and afterwards played captain to all of her family, Los Méndaz, and all of those whs stamped on the stage with their martial, aggressive, outrageous and captivating \"aire\" which the public received in a friendly manner. Ana Parrilla danced soleares and bulerías por soles. i had been wanting to see her, but never before had had the chance to attend more than inconsequential performances. Pay attention to Ana Parrilla! There is in her a baileora of the old style who reminds us of some women of the baila jondo, of which there hardly remains anything today but the legend. With not one hair out of place, without the flower falling from the hairdo, without a stamp, with no shuddering, Ana Parrilla left us with some of the sequences of feminine flamenco dance that are the most beautiful i remember. And right after, the rest of the program was a magnificent \"buleariera\" racital in which the art of La Bernarda was especially brilliant. The written word has its limitations, certainly, and there are things that definitely cannot be explained. It was one of those demonstrations in which I think that we were ebie to come close enough to a flamenca art that is still authentic and in a pura statala. El Tía Pili makes a short thing of singing and dancing, but has fabulous gracia. Tía Eduardo sings par bulertas marvelously. Manuela also performed the song and the dance of course, with somewhat disquieting duende in her performance. The bays Paqui and Joselita delivered some styles that many professionals would like to have for themselves. Many of those who sang and danced there are not professional artists. They learned to sing and dance like that in their homes, in family fiestas, in the gypsy barrido of Santiago and San Miguel in Jerez. The dance of Luisa Torran is true and emotional. The dance and song of Tía Dalares has nobility and an unusual solemnity. The playing was another fiesta. Fiva great guitarista, some truly exceptional; precisely because of the coming together of so many of such quality, perhaps they did not find sufficient space for each one to offer all that he knows in the bulerías family of flamenco music that can be more, much more, A great fiesta of bulerías, the most beautiful fiesta of bulerías that I remember to have witnessed.",
    "title": "POR BULERIAS...;Y OLE!",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "38",
    "page_number": 38,
    "word_count": 723,
    "article_char_count_full": 4185,
    "article_char_count_review": 4185,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A22",
    "article_text_for_review": "Each Biennale of Sevilla comes up with a new and more interesting program. This year they seem to be organizing the performances by farms of cante. The result is a real flamenca feast. Here is the schedule of events. It makes very interesting reading if you know something about the artists. We look forward to some reviews of these performances and thank Ann Fitzgerald of Seville for sending the program. Sept. 8: Opening Ceremony of the Biennal Sept. 9: \"SEVEN DAYS OF SONG CYCLE\" THE BEAT OF THE FESTIVAL Singers Aurora Vergas, Bernorda de Utrera, Boquerón, Nano de Jerez, Romerita de Jerez, Turranera, José Mercé, Juan Villar, Juana la de Revuele Dancers Juana Ameya and Peco Valdepeñas Guitars Diego de Marón, Paco Cepero and Parrilla de Jerez Sept. 10: THE ORIGINS Singers Agujetae, Cepilla, Manuel Mairena, Miguel Vargas, Tlo José el Negro and Tomasa Dancers Angelita Vargas and los Biencasaos Guitars Enrique de Melchor, Paco del Gastor and Pero Peña Sept. 12: FOR CANTINA Singers Beni de Cádiz, Chano Lobato, Curra Malene, María Vargas and Pansequito Dancer Ana Parrilla Guiters José Luie Postigo, Manolo Dominguez and Quique Parades Sapt. 13: THE ENDLESS NIGHT The Fernández Family and the Vargas Family (Jerez-Seville), The Peña Family (Lebrija) and tha Pinini Family (Utrera) Sept. 15: FANDANGOS Singers El Cabrero, Laonor Díaz, Luis Caballaro, Paco Toronja, the Flamenco Peña of Huelva, Pereji and Pies de Ploma Dancers The Flamenco Peña of Heulva Guitars Azuaga, Juan Diaz end Menala Brenes THE EASTERN PART OF THE SOUTH Sept. 16: THE EASTERN P. Singers Canillas, Carmen Linares, Curro Lucena, Diego Claval, Encarnecón Fernández, Piffana and Tía Marina Sept. 17: SONGS OF \"IDA Y VUELTA\" Singars Ana Reverte, Chano Labato, Gebrial Moreno, Luis de Córdoba and Naranjito de Triana Dancar Pepa Montea Guitare Manolo Franco, Rafael Riquani and Ricardo Mito Sept. 1B: LDS MONTES SING Verdiales groups from the mountains of Májega, Troubadours from Las Alpujarras, Fandango group from Lucena and \"Baile el Mante\" Sept. 19: Singera Camarón de la Isla and Manuel Molina Guitars Manuel Molina and Tomatita Sept. 20: Paco de Lucía Sept. 22: ANTHOLOGY OF SEVILLANAS Singera \"Corraleras\" de Lebrija, El Pali, Los Romeros",
    "title": "IV BIENNAL FLAMENCO DE SEVILLA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "38",
    "page_number": 38,
    "word_count": 359,
    "article_char_count_full": 2222,
    "article_char_count_review": 2222,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SUMMER::A23",
    "article_text_for_review": "[Submitted by: Matteo, The Foundation for Ethnic Dance, Inc., 17 West 71st St., New York, NY 10025, (212)877-9565, and Nancy Lee Ruyter, Dance Department/School of Fine Arts, University of California, Irvine, CA 92717, (714)856-7284] Spanish dance instructors and students from the United States, Colombia, New Zealand, and West Germany spent June (LEFT TO RIGHT): VISITING EXAMINER LUISA CORTEZ, SANORALABY, CHELA JACOBO, MATTEO, CAROLA GOYA, JANE LUSCOMBE, INSTRUCTOR MARINA KEET, NANCY LEE RUYTER,",
    "title": "SPANISH DANCE SOCIETY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SUMMER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "39",
    "page_number": 39,
    "word_count": 72,
    "article_char_count_full": 500,
    "article_char_count_review": 500,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
