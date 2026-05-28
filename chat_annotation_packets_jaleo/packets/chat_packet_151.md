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
    "article_id": "JALEO_1982_10::A7",
    "article_text_for_review": "\"WHAT IS IT?\" by Maria Marca What is the Zambra Mora? That's a question that dance students inevitably ask me. Some found out last June 20th at a successful and well-attended workshop on Zambra Mora which I gave in Los Angeles. Zambra Mora, or dance of the Moors, is an exciting link between Middle Eastern and flamenca dance. Over 700 years of Arabic influence on the culture of Spain is still strongly preserved in this dance form. The three main rhythms that are used in the Zambra are Arabic rhythms: wahdah, sufiyan and du-yak. The dance also incorporates Middle Eastern movements -- broad undulations, exaggerated hip and zaric movements, lattice skirt and foot work. It's danced barefoot or with shoes, and with or without finger cymbals. Arabic culture came into Spain through the Moors. During the 7th century A.D., the Arabian Empire struck relentlessly across North Africa to the Atlantic. In 711 A.D. the Arabs crossed the Strait of Gibraltar from Morocco and gained control of Spain. By the 13th century, under Moorish occupation, Spain reached the height of her glory in the arts, sciences and education and rivalled Damascus and Baghdad as a culture of learning. Córdoba became the chosen capital of Arab Spain, by then the most powerful dynasty in the Moslem world. The cultures of many people played an important part in the development of Arabic music and dance. Directly and indirectly through invasions, conqusts, communications, trade and commerce, Turks, Kurds, Persians, Greeks, Arameans, Syrians, Phoenicians, Carthgians, Egyptians, Jews, Gypsies and others blended and fused their spirit over many centuries into what became the Arabian Classical School. Flamenco, though, was born in Spain and came, in part, from the gypsies. By nature a mystical and passionate people, the gypsies evolved their own unique style of music. They took from Arab Spain as well as from other cultures as they migrated from place to place over the centuries. Like Middle Eastern music, flamenco sprang from a spiritual and esoteric base, evolving through song, music and dance, and was established on ancient musical theories and principals. The purpose of flamenco itself is to release both the body and spirit to experience various moods, emotions and sensuous responses, to realize a harmony with nature and the universe. Various forms were evolved to allow this. Some could be used to check passion, or arouse it --cr to relax or dispel fear. One form, such as the alegrias, served to banish depression. Another, the saleares, to find unity within one's self. A third, farruca, to express masculine qualities while yet another, the zambra, expressed the farruca's feminine counterpart. A First Flamenca! Singing/Palmas/Compás Spanish Classical Choreography ◀ Beginners Classes ^Guitar Lessons ▲ Mens Classes Price Per Dance Class 8 Classes - $40.00 12 Classes - $48.00 To speak about the Montoya family is to speak of flamenco. Rosa Montoya was born into this proud and illustrious family. She began dancing at the age of eight and was performing professionally by age sixteen. She has toured successfully throughout Canada, Japan, Australia, and Europe. Today Rosa is acknowledged as one of the most skilled teachers of flamenco in California. Conducting vibrant classes for adults and children on all levels, she inspires in her students the skill and fire of flamenco dance. For Information ~ 415/239-75/10 San Francisco, CA. 94110/824-5044",
    "title": "ZAMBRA MORA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_10",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 554,
    "article_char_count_full": 3452,
    "article_char_count_review": 3452,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_10::A8",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nBULERIAS, VIVA TU! Of all the flamenco forms, bulerías has the most and fewest \"rules.\" The more one listens to bulerías and sees and hears it expressed in dance and song, the more one realizes its endless variety of mood, expression, expansiveness, rhythm and feeling. The incredible balance of rhythmic energy and pulse make for what I call an ongoing natural search for more -- not so much more variety, of which I will get to later, but of more depth, of more involvement within the form, of true self-expression finding itself. Although there are many other flamenco forms sharing the same compás, there are few others that use it with such variation of pulse, of accent, of energy, of drive, nuance and mood. Some of the common questions that dancers ask are: How do I get into the rhythm? On\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"know\"]\n\nnot so much more variety, of which I will get to later, but of more depth, of more involvement within the form, of true self-expression finding itself. Although there are many other flamenco forms sharing the same compás, there are few others that use it with such variation of pulse, of accent, of energy, of drive, nuance and mood. Some of the common questions that dancers ask are: How do I get into the rhythm? On what beat do I enter? How do I know when to staff or start? When do I make the llamada, or desplante? When does the singer come in or out? When do I pasear or play palmas? When is the best time or place to do the footwork section or how long should I carry on the footwork? How do I go from the base rhythm to the paseo? etc. Guitarists basically ask the same questions, although related to dance accompaniment or even understanding bulerías as a solo form: When do I play the base accent? What do I play when the dancer is doing footwork? Are there many different kinds of llamadas and ways of calling them? What is the difference between a llamada and a despiante? When do I play a faiseta? How do I know when the singer is coming in or finishing? What d\n\n[ENDING CONTEXT]\n\nand desplantes \"inside of you\" and at the next fiesta, rehearsal or juerga, trust your spui to bring out what is not done, not done by your body and conscious mind. Let the spirit of bulerías move you in what can be called \"becpming the dance.\" True improvisation is letting all that you have learned trickle down into the soul, as I mentioned before, like the water nourishing the roots of a beautiful flower, and then letting the roots push up and slossom into a beautiful bouquet. God bless bulerías, one of the beautiful flowers from the bouquet of art we love, cslled fiamenco. -- Tep Morca\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_10",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "12",
    "page_number": 12,
    "word_count": 1515,
    "article_char_count_full": 8492,
    "article_char_count_review": 2796,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "know"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_10::A10",
    "article_text_for_review": "MANOLO SANLUCAR HAS COMPOSED AN OPERA (from: Semana, May 15, 1982; submitted and translated by Paco Sevilla) Manolo Sanlúcar will be presented in Madrid next Monday. His recital will be one more attraction for the festivities of San Isidro, although it is still not certain where he will perform. He will interpret his current melodies, as well as traditional flamenco themes, accompanied by his brother Isidro, who plays flamenco guitar, and other musicians who are in charge of the acoustic guitar, the flute, and percussion. At this time, Manolo Sanlúcar has a lot of work. Aside from having just finished last week his latest LP, \"Al Viento,\" for his new recording company [see following article] and the song \"Esmeralda\" on a 45 RPM record, he has other projects. The most important of these is the flamenco opera that he has composed, using a full orchestra and based on the idea by El Lebrijano, called \"Un Gitano Llamado Mateo,\" although it seems that the work will be entitled \"Ven y Sigueme\" (Come and Follow Me). Sanlucar says: \"It has taken me a year to write this opera, which I consider very ambitious, perhaps the most risky undertaking of the many I have completed up to now. It was conceived with the idea of being presented in the theater, and the role of Mary Magdalene has been assigned to Rocío Jurado -- if it ever reaches the stage. As for me, I will also play one of the characters, although I still don't know which. We shall see.\" The recording of the opera features more than one hundred people and will be released as two separate albums [it is unclear whether a \"double\" album is intended]. Manolo continues, \"I have tried to avoid doing something pompous, but rather, simple and reflecting, above all, the restlessness of Andalucía -- and I believe I have succeeded. The work has its roots in the songs of Andalucía, and it has come out very beautiful. I am optimistic.\" Aside from his new album and the flamenco opera, Manolo Sanlúcar will direct, in August, the first \"International Course of Guitar\" organized by the Government of Sanlúcar. \"It will be a cultural month, with concerts of chamber music, symphonies, and classical music, painting exhibitions, and lectures. The courses will be held each year, from now on, and this summer, I will spend a month preparing for it. We have a great number of students from other countries who have signed up already, so I think it will be a success.\" * * * HABLAR CON LA GUITARRA (from: Guía del Ocio, May 16, 1982; submitted and translated by Paco Sevilla) It is not necessary to introduce Manolo Sanlúcar. Twenty-eight years playing the guitar, fourteen LP's, and the general opinion that he is one of the best flamenco guitarists of all times, are sufficient proof of his popularity. But Manolo Sanlúcar is something more than that: above all, he is a man so in love with music and so in touch with reality that he is capable of giving a concert in the Teatro Real, in a jail, in a Japanese theater, or in a tent as he will do next Monday in Las Salesas, in the middle of the Fiestas de San Isidro [Madrid]. -- How is the Madrid audience, with respect to your music? \"I base my music on feelings, on the spirit, and this reaches everybody, independent of where they are from. I would say to the Madrid public that they can be sure that, good or bad, my music is authentic.\" -- You have characterized yourself as searching constantly for new paths in flamenco. How do you define your music? \"My music has two sides. One part is that of traditional flamenco, with its classical canons. At the same time, I am continually being influenced by that which is around me, including other kinds of music. That are not flamenco, I try to mold these influences into the music that I do. Flamenco is a style, but it is not something boxed in or closed.\" -- You are from Cádiz. What does it mean to you to be Andaluz? \"When thinking of Andalucia, one always thinks of the sun and the tourists, that we are always drinking and dancing. We are very hospitable and we want our guests to have a good time when they are there. But when the guest is gone, the Andaluz works like nobody else, and the laborer once again fights with the land, under that same sun, but it burns him. With respect to the music, to flamenco, there have been good people who were not Andalucian, but being there has an effect. The air and sea are different, and that makes you work in a different way.\" MANOLO SANLUCAR TO RECORD AN ALBUM WITH PACO DE LUCIA (from: Diario 16, Dec. 14, 1981; submitted by Carlos Mullen; translated by Paco Sevilla) by Horacio Otheguy After the realization of the first Andalucian opera, Manolo Sanlúcar leaves RCA Victor for a contract that makes him a millionaire [in pesetas] many times over with Philips, the third most important company in Europe, after Shell and Siemens. The depression of this artist, who has a tendency towards melancholy, evaporated upon reaching agreement with the new recording company on some clauses in the contract that are exceptional in the area of Spanish music today: \"You know how distrustful I am of the international companies; they can be very dangerous, imposing unusual clauses that later on become a real threat to creativity. But this time, it has been, frankly, extraordinary. They have given me all the facilities imaginable, putting into my hands a thousand and one resources for the creation of four records during the period of four years, one of which will be carried out with the magnificent participation of the genius, Paco de Lucía.\"",
    "title": "MANOLO SANLUCAR IN THE NEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_10",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 981,
    "article_char_count_full": 5554,
    "article_char_count_review": 5554,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_10::A11",
    "article_text_for_review": "PASSION, POWER AND PELLISCOS by Juana De Alva I had the opportunity to visit the Espartacus Restaurant in Beverly Hills in August and the flamenco show there thoroughly lived up to my expectations. The setting was beautiful -- running fountains, shawl-covered walls, a low intimate state which allowed the audience to be in close proximity to the performers and best of all a group of artists whose combined talents resulted in a most exciting performance. Without exception, each performer projected his or her personality with strength and individuality. The show was fast-paced and escalated throughout leaving one emotionally drained and wondering why the performers weren't. Every number was opened by the powerful and passionate cante of Talegón from Córdoba with the inspiring support of the guitars of Paco Arroyo Leon from Barcelona and Manolo Cantalejo. Irene Heredia performed a soleá with nice gypsy style. Her dancing has matured and become more subtle since I last saw her perform in San Diego. Alegrias was executed by Lurdes who has a \"cara de picaro\" (impish face) as though she has some mischief planned. And she surely did -- for the \"castellara\" section and \"final,\" she exploded into torrents of \"pellispos\" which caught the spectators totally off guard. Pilar \"La Cubaniza,\" who comes by her name from her Cuban father, was born in Jerez. Her style was more natural but equally as strong as the others with powerful \"contratiempos.\"",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_10",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 236,
    "article_char_count_full": 1454,
    "article_char_count_review": 1454,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_10::A12",
    "article_text_for_review": "(from: Naticias del Yundo, July 2, 1982; sent by Vicente Granados; translated by Paco Sevilla) by Miriam Fernández-Soberón One of the most embotional fiestas that the Casa de España [of New York] has celebrated, and I have seen many, was held this past Sunday for the purpose of paying homage to Maria Escudero and the flamenca guitar. This activity was organized by a group of artists: Augustín Castellán (Sabicas), Domingo Alvaradb, Emilio Prados, Estrella Morena, Imre Witzner, Juan Orazca, José Manuel Herrida, Manalo Barón, Manuel Herrida, Marianq Baguena, Paco Ortiz, Pepe de Málaga, Pepe Ruso, Vicente Granados, Juan de la Mata, and Brdbk Zern. Among the organizations that collaborated are: Casa de España, American Institute of Guitar, Antonio David, Inc., Augustine Strings, Cámara de Comercio de España, Casa Galiria, Casa Paco Restaurante, Centro Español, Chateau Madrid, Daniel Mari Strings, Flyknings Travel, Guitarreria Crozco, Marc Simont, Mesbn Bdtin Restaurante, Oficina de Turismo de España, Pamplona Restaurante, Club Taurino de Nueva York, Rincón de España Restaurante, Sebastian Castro, Society of the Classic Guitar, Teatro Real Español, Restaurante El Mariachi, E.O. Mari, Inc. (La Bella Strings), Merino Music, and Restaurante Finisterre. It is very difficult to organize an homage of this type, and even more difficult when all of those who perform are in the same field, in this case, flamenco artists. It was very beautiful to see all the \"flamenco artists gathered there to pay tribute to a great master of the guitar, to Mario Escudero. Domingo Alvarado, who was more or less the musical director -- without wanting to be -- gave the welcome speech and then asked for some words from the person being honored. Marid Escudero, with the smile that is characteristic of him, and a little emotional and nervous, spoke: \"Speaking is not my thing; this makes me a little nervous, so I will just say thank you.\" panied by Manolo Barbr. who has been retired from the stage for ten years. Then Pacb Ortiz, to the rhythm of the guitar of Mario Escudero, Jr., who also played a solo; it was pleasing to see the satisfied face of Maestro Escudero. The first part was opened by Pepe de Málaga, Estrella Morena, Emilia Prado, and Manblc de Córdova -- a student of Estrella's. They were followed by Domingo Alvarado accom- When I arrived at the Casa de España I was speaking with him [not clear who \"him\" refers to; unless part of the article is missing, it is most likely Pacc Ortiz] -- he had been one of my dance teachers some years ago -- and he said to me, with the simplicity of the great artists, \"We'll see how it comes out; it has been a long time since I have danced, but I couldn't fail here.\" He told me that the reason for his retirement was the lack of places to work, such as there were years ago. PACO ORTIZ WITH MARIO ESCUDERO JUNIOR LEFT TO RIGHT: PACO MONTES, CARMEN RUBIO AND GUITARIST PACO JUANAS DOMINGO ALVARADO SANG AS NEVER BEFORE AND DEDICATED ONE OF HIS SONGS TO THE CHILDREN LISTENING ATTENTIVELY. photos by Juan Ruiz",
    "title": "FLAMENCO HOMAGE TO MARIO ESCUDERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_10",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 520,
    "article_char_count_full": 3060,
    "article_char_count_review": 3060,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
