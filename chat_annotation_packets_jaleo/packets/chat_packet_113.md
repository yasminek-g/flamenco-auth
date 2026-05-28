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
    "article_id": "JALEO_1981_08::A2",
    "article_text_for_review": "by Paco Sevilla If there is dominant theme in the sparse amount of mail we receive at $ \\underline{\\text{Jaleo}} $, it is the constant inquiry about sources of flamenco materials--records, books, guitar music, dance supplies, guitars, etc. I would like to answer these questions all at once, since we cannot respond to each letter. We are unaware of any place in the United States that sells substantial amounts of flamenco dance equipment (fans, shawls, castanets, etc.). We occasionally hear rumors about such places, but readers do not send in details. Watch Jaleo announcements and ads for the sale of these items. Neither do we know of any sources of books, nor reliable sources of records; anything we learn about, we publish immediately. We get frequent requests for more guitar music to be printed in Jaleo. As editor, I have said before that I do not feel it is a proper function of this magazine to supply guitar music. There are several reasons: 1) We couldn't devote enough space to make it really worthwhile; 2) Copyright considerations; 3) Nobody sends music for us to print; 4) There are many good books of flamenco available (see review in this issue and information in future issues) and individuals who deal in tablature; 5) Flamenco guitar is not best learned from books -- records are a much better source. We also get requests for more technical articles, like the one on \"Malaguenas.\" But nobody will write them. In four years of publishing, almost all of the articles of this nature have been written by one person--me! There have been a few other articles on non-guitar subjects, and we thank people like Carol Whitney, Teo Morca, and Marta del Cid for their efforts. But where are all the articles by the hundreds of people who know a lot more than I do? Lately, we have been receiving a good deal of valuable biographical material. Let's keep that up, but maybe some of you out there who have specialized knowledge could take the time to share it with the rest of us.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_08",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 346,
    "article_char_count_full": 1993,
    "article_char_count_review": 1993,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_08::A3",
    "article_text_for_review": "Dear Jaleo: With the aid of the information in your June issue, I purchased the \"History of Cante Flamenco\" from Publishers Central Bureau. I am very happy with the album and appreciate your making it possible for me to acquire it, but I find one problem: I received no information about which artists are involved in each selection. There is a list of all the singers and guitarists, but no detailed accounting of each song. Can you tell me how to obtain this information. I continue to enjoy $ \\underline{\\text{Jaleo}} $ very much. Sincerely, John W. Fowler Santa Monica, CA (Editor's reply: We are glad to hear that you took advantage of the offer; we can assume that many aficionados did so since the offer has not been repeated. With regard to identifying the artists: Each time this album has been released, it seems to have come with different information. The original had the booklet that we are translating from in our \"Archivo\" series. A later version had the words to the songs, but did not identify the guitarists. Later versions had nothing. Therefore, in a future issue of Jaleo, we will attempt to bring you a complete listing of cantes, cantaores and guitarists.) Rubina Carmona Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059 Los Angeles, Ca.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_08",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 220,
    "article_char_count_full": 1291,
    "article_char_count_review": 1291,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_08::A4",
    "article_text_for_review": "(Information sent by George Ryss) Estrella Morena was born in Madrid, but apparently moved back and forth between Spain and New York, for her mother, Consuelo Moreno, was well known in New York as an interpreter of light Spanish songs and had appeared on programs with Carmen Amaya -- for whom Estrella danced when she was eleven years old. Estrella studied flamenco with Paco Reyes, jota with Pedro Azorín, and Spanish folk dance with Juanjo Linares. By the age of fifteen, she was appearing in the cuadro of Manolo Caracol's tablao, Los Canasteros, with guitarists Melchor de Marchena and Paco Aguilera. At sixteen she worked in the cuadro of Torres Bermejas and became acquainted with dancers like La Tati and Faiquillo, singers such as Pepa de Utrera, Beni de Cádiz, and Fosforito, and guitarists Juan Carmona \"Habichuela\" and Juan Maya \"Marote.\" When she was eighteen, Estrella became a featured artist, working with Alejandro Vega in the tablao, El Duende. Her cantaores included Talegón de Córdoba, José Salazar, and Juan Peña \"Lebrijano.\" Following this period, she was prima bailarina with Rafael de Córdoba in the Teatro de la Zarzuela and then went on tour with Rafael's company, which included cantaor Pepe de Málaga and guitarists, Antonio \"Pucherete\" and Pepe Priego. While with Rafael de Córdoba, Estrella did a number of television appearances and made one film, as well as touring a number of countries. In New York, Estrella met Antonio Ruiz Soler \"Antonio\" and was signed to his company as first dancer. For the next four years, from 1969-1972, she toured with that company -- which included singers Chano Lobato, Pepe de Lucía, and Sernita de Jerez, and guitarists, Manuel Moreno \"Morao,\" Curro de Jerez (Sernita's son), and Pepe Vallecano (José Jiménez, who has performed at the Chateau Madrid in New York). The company travelled throughout Europe, the Orient, and Australia. Since taking up residence in New York, Estrella and her husband, Pepe de Málaga, have performed widely and successfully, both in the Chateau Madrid in New York and in numerous concerts--including in Carnegie Hall. In addition, Estrella is widely renowned as a teacher. Dances of Possession (From $ \\underline{\\text{Other}} $ $ \\underline{\\text{Stages}} $，July 26，1979) by Linda Small There is a documentary about flamenco dancer María Benítez in which she mentions that when she dances she becomes a person The lacy arm work of flamenco, she said, comes from Middle Eastern belly dance, the steps from the folk dances of Andalucía in southern Spain, and the songs from gypsies and Arabs, and from Jewish synagogue chants. The word flamenco is thought to come from an Arab word meaning fugitive, since Arabs, gypsies and Jews were fugitives from the Inquisition, she added. A flamenco performance generally opens with a guitar section, followed by a lament without words to warm up the singer's voice, then a dancer's llamada, or call, a signal for the singer to start his song. \"The song takes an important part because it explains the heartbreak, the happiness and the sorrow of the people of Spain. We hear the words, we get inspired by the words. We cannot do good flamenco without a singer,\" Morena declared. States, she did heelwork too, and now women combine armwork and footwork. Most of the women in flamenco were very broad -- maybe that's why they didn't do heelwork -- but Carmen was thin enough to fit into men's costumes. She wanted to make People feel her strength as a man.\" Usually, flamenco dancers can be heavier than classical dancers, Morena noted. \"Since flamenco is so earthy, the pressure is on the thighs and they get very big. \"Ballet is up,\" she said, rising to her tiptoes, \"and flamenco is down,\" she concluded, bending her knees and stamping her feet. \"Being heavier gives the dancers more strength in the thigh to stamp.\" Morena, who has danced internationally with the companies of José Greco and Antonio and currently teaches and performs in New York, then switched from technical issues to the indefinable quality the Spanish call duende. \"When you say someone has duende, that means the person has something special, has a soul. Not everybody has that, not even in Spain.\" \"When a person is giving what they feel to you, and making you feel what they feel, that's duende. It has nothing to do with technique. If someone is dancing 100 miles per hour, that doesn't mean he has duende.\" \"But a little girl in Andalucía can do something in her dance that drives you off your seat. You have to stand up and say olé. That's duende.\"",
    "title": "ESTRELLA MORENA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_08",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "5-7",
    "page_number": 5,
    "word_count": 765,
    "article_char_count_full": 4558,
    "article_char_count_review": 4558,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_08::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(quotes selected, edited and translated by Paco Sevilla) DOMINGO MANFREDI CANO ( $ \\underline{\\text{Cante y Baile Flamencos}} $, 1973. p.48): \"La Sonanta\" is the guitar -- the flamenco guitar. Today almost all of the good guitarists are half maestros of the classical guitar and half of the flamenco, but in the past it was different. In the first third of the 19th century, the cantaores almost always sang \"a palo seco\" -- without the guitar -- for the sole reason that guitarists were scarce; the few that there were, says Fernando el de Triana, \"were'muy cortos tocando' (could play very little) and the cejilla was unknown.\" Until the cejilla was invented, the cantaores had to have a hoarse voice, which was and continues to be the classic style in the best history of the cante jondo. It\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"tablaos\"]\n\nways sang \"a palo seco\" -- without the guitar -- for the sole reason that guitarists were scarce; the few that there were, says Fernando el de Triana, \"were'muy cortos tocando' (could play very little) and the cejilla was unknown.\" Until the cejilla was invented, the cantaores had to have a hoarse voice, which was and continues to be the classic style in the best history of the cante jondo. It seems that with the appearance of the café de cante (tablaos of the 1800's), the cantaores and guitarists became professionalized when the invention and perfection of the cejilla made possible the adjustment of the music to the voice. Famous maestros of the guitar were \"el senor Patiño\" and Antonio Pérez, the former Sevilla and the latter from Cádiz. Their extraordinary disciples began the true history of the flamenco guitar: Francisco Sánchez, Paco el Barbero; from their school came later Javier Molina and Antonio Sol. Fernando el de Triana, who was a witness to almost everything, tells us that Paco el Barbero was the first flamenco guitar soloist to perform in public. Not being able to res\n\n[ENDING CONTEXT]\n\na lot of practice to detect the inflexion in the voice that portends the change. You are not helped if the singer himself doesn't properly understand the compás, and loses time. That happens often enough these days. But the guitarist always gets the blame. And so it is with the range of flamenco music -- the canas, malagueñas, tarantas, alegrías and so forth -- there are seemingly endless variants on each of them, and they have their special accompaniments. The guitarist's job is to know them all, and to be able to \"carry\" the singer right through without faltering, or losing \"compás\" (time),\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LA SONATA ENRIQUE EL COJO: HALF A CENTURY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_08",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "8-13",
    "page_number": 8,
    "word_count": 2385,
    "article_char_count_full": 14030,
    "article_char_count_review": 2722,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "tablaos"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_08::A6",
    "article_text_for_review": "(from: $ \\underline{ABC} $, April 21, 1981; sent by Gary Hayes; translated by Roberto Vazquez.) When we arrived at the Calle Espíritu Santo, Enrique, guitar in hand, goes over the fandangos for a few young girls -- neither children nor yet grown up -- who turn with gracia to the beat of the music, with their school uniforms as improvised flamenco dresses. \"I am a magician,\" he tells me, \"they have had less than twelve lessons and look how they dance already!\" When talking about dance schools, Enrique El Cojo puts his guard up. I am apart from the academies. I am the dean, and I have much experience. I don't belong to those academies, where one must have a degree in order to set one up. I will not go to school now, as you will certainly understand. When somebody calls me to tell me that one does not have a degree and one should not work, I don't get involved, because I earn money and I want everybody to earn it. Moreover, just look, if I am separate from the academies, then in the quincenas and dances that the city council organized, the schools presented their girls, but not I; I danced with winners of national awards. This is not an academy. I teach whoever I want and I do it almost as a hobby. Here money does not matter; I do it almost free. I teach only the children of families who want them to learn to dance. I do it all year round, but not as a business. This, for me, is a hobby. I couldn't live without doing it. Here they learn everything that is flamenco: sevillanas, rumbas, fandangos, alegrías, bulerías, siguirias, tientos...I create constantly and, even if I wanted, I could not teach two people in the same manner. I only teach professionals who can understand me. Like, for example, Cristina Hoyos, Manuela Vargas. I have also given lessons to Lucero Tena. Enrique El Cojo likes to teach. His patience is well-known, his kindness in repeating once and again the step until the pupil gets it. For him, the little room on Calle Espíritu Santo is a temple: \"Here is where I have my greatest joys and satisfactions. No, this cannot be considered a school. And let's not insist on it anymore!\" With Enrique it is better to let him do the talking. Ask him a question and let him talk because, besides being a personality of the dance, he is a living book of the history After the success at Zapico, Enrique went for a try-out at the Kursal-Internacional that was located where the Palacio Central is now. \"I had to introduce myself, and my tongue got stuck, because I didn't know how to talk. Now I know how. There, I danced with Malena, Las Pompis and Enrique el Lillo. The day when I was presented there, I was seated because at that time the dancers used to come out like that, and I saw everybody laughing. Since they had not seen my lameness, I said to myself, 'What are they laughing about?' And it was, as I was told, because my pants were unbuttoned. I had a great success. While at the Kursal I had the chance to go to Rosales, to the walls of the Macarena, but I didn't go because the war broke out.\" In those days Enrique formed a trio with some girls from the \"Posá del Lucero\" with whom he performed in a cabaret, Variedades, which is now the Trajano movie theater. \"There they announced the performances on a blackboard, as if they were fish. The girls from the \"Posa del Lucero\" made some pretty costumes for themselves. I made them for myself with much trouble, and always in a normal style so I could use them later for street wear. I remember that Enriquetita DON QUIXOTE RESTAURANT 206 EL PASEO DE SARATOGA SAN JOSE, CALIF, 95130 378-1545 Flamenco Entertainment MARIANO CORDOBA GUITARIST PILAR SEVILLA DANCER",
    "title": "OF DANCE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_08",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 668,
    "article_char_count_full": 3658,
    "article_char_count_review": 3658,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
