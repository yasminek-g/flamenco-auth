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
    "article_id": "JALEO_1983_10::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(the following articles were sent to us by Phil Coram from England, Gordon Booth from Germany, and Charo Botello of San Diego; translated by Paco Sevilla) ANTONIO MAIRENA, MAESTRO OF CANTE FLAMENCO DIES OF A HEART ATTACH IN SEVILLA YESTERDAY (from: El País, Sept. 6, 1983) by Alfred Relano Antonio Cruz García, 74 years old, known in the flamenco world as Antonio Mairena, died yesterday at 7:00 PM in the Residencia Garcia Marato de la Seguridad Social in Sevilla, victim of a heart attack. Mairena, who had been having heart trouble for some years, suffered a relapse while in his home in Sevilla. In early August, he had spent some time in the Virgen del Rocio sanitarium. His body was taken to the town of his birth, Mairena del Alcor (near Sevilla), where it was put on display in the town hall\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"originally\"]\n\no Mairena, died yesterday at 7:00 PM in the Residencia Garcia Marato de la Seguridad Social in Sevilla, victim of a heart attack. Mairena, who had been having heart trouble for some years, suffered a relapse while in his home in Sevilla. In early August, he had spent some time in the Virgen del Rocio sanitarium. His body was taken to the town of his birth, Mairena del Alcor (near Sevilla), where it was put on display in the town hall -- although originally it had been planned that he would be placed in the chapel of the brotherhood Cristo de la Cárcel, to which he belonged. Watching over the coffin were his brother, Curro, and sister, Dolores. * * * Antonio Mairena, considered the greatest cantavor of all time, died yesterday in Sevilla, victim of a heart attack. For all practical purposes, away from his profession since 1974, due to the illness that has finally taken him to the grave, he received, in 1979, an homage from his countrymen and all of the flamenco world for the half century that he has dedicated to dignifying the cante. A bronze bust and a plaque placed in the doorway of the house in Mairena del Alcor where he was born 74 years ago, describes forever, \"the creative genius of the most pure cante jondo,\" that\n\n[ENDING CONTEXT]\n\nperformed? \"Along with Mairena, there was Juanito Varea, Platerito de Alcalá, and me in the cante, and the cuadro \"Peria de Abril\" with Farruco dancing and Chocolate singing for him. Each of us had to sing three cantes por tona or martinete, three pcor siguirijas, and then one of our own choice.\" --In summary, what do you expect from this Congress? \"There are many important topics to deal with here, aside from the \"Llave\". We have to look out for the artists of the 'tercera edad' [the 'Third Age'].\" ANTONIO MAIRENA, RECOGNISED BY ALL AS ONE OF THE GREATEST FIG- URES OF FLAMENCO OF ALL TIME\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ANTONIO MAIRENA 1909-1983",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_10",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "8-13",
    "page_number": 8,
    "word_count": 3260,
    "article_char_count_full": 18500,
    "article_char_count_review": 2869,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "originally"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_10::A4",
    "article_text_for_review": "by Paula Joann Durbin PART I AN INTERVIEW WITH MERCEDES AND ALBANO Mercedes and Albano's studio is located on the Plaza Tirso de Molina in Madrid. There, they continue the flamenco tradition of Mercedes' mother, La Quica, one of the loveliest dancers of her time. In the summer, they teach the intermediate and advanced flamenco classes in José de Udaeta's Curso Internacional de Baile Español in Sitges. Albano also provides guitar accompaniment. Choreography is one of this couple's strongest areas of expertise, and the dances they create together are usually discussed in superlatives. PD: Can we begin by hearing something about your mother, Mercedes? MERCEDES: What can I say about my mother? That she was a marvel, that she danced very well and was a wonderful teacher. She started dancing at the age of five and continued until she was sixty-one, when she died. And I am going to tell you something else. My father was also a great teacher. His name was Frasquillo, and for many years he taught Antonio, and other famous dancers as well: Enrique el Cojo, José de Udaeta, José Greco, Manolo Vargas Luisillo, Lina Amparo--many well-known artists studies with my parents. PD: Where were you born? ALBANO: I was born in Madrid. MERCEDES: Me, in Malaga, but I was raised in Sevilla, and when I was ten, we moved to Madrid. PD: When did you start dancing? ALBANO (LEFT) AND MERCEDES WITH JOSE DE UDAETA MERCEDES: At five or six, in the group class my parents taught. For them, I was just another student, the one they bothered with the least. They paid attention to everyone else but me. ALBANO: I began when I was nine with a very good, very famous teacher named Antonio Bilbao. After he died, I studied with Antonio Triana, who is now in California. When the war started, I stopped dancing, but halfway through the war I went back to it and I have been dancing ever since. PD: How did you get started professionally? MERCEDES: I began when I was seven at a benefit for a torero who had been gored by a bull. I went to dance with my parents. From that day on, when my parents worked, they took me along to dance with them. ALBANO: I began when I was eleven but for a long time I performed children's parts because I was small for my age. PD: Where did you meet? ALBANO: When we were seventeen, there was a dancer, Manuela del Rio, one of La Quica's students, who was organizing a show which was to tour Europe. She told La Quica that she needed a male dancer, so they all went to see me dance at the Teatro Calderón in Madrid. She hired me and put Mercedes and me together as partners. After many years of dancing with each other, we got married. MERCEDES: We have been married for thirty-four years. ALBANO: And we are still married and still dancing, something rare these days. It is very difficult because we are together twenty-four hours a day, practicing, traveling, working. Difficult, but one can do anything in life. PD: What was your dance career like? ALBANO: Our first contracts were for a series of concerts in Germany, Yugoslavia, Checkoslovakia and Poland. When we returned to Madrid, we continued to dance, just the two of us, and we worked in many theaters there. Later we went to Portugal and Scandinavia. Upon our return, we found we had a contract to work in Buneos Aires, and we danced for nine months in the Teatro Avenida, then did the summer season in Chile before returning to Buenos Aires for another season there. After that we went to other South American countries and the Carribean. We no sooner got back to Madrid than we had a contract for the United States where we spent several years. During that time we were under contract also to the Ice Capades. We performed with them at Madison Square Garden and traveled all over. PD: Did you have your own guitarist on those trips? ALBANO: In Buenos Aires, where there was a lot of demand for flamenco, we did, and also in Chile. But usually we didn't have a guitarist or a cantaor. You see, in those days flamenco wasn't as popular as it is now. We usually danced to a full orchestra, doing the jotas, Basque, and other regional dances, and \"clásico español.\" PD: Do you mean escuela bolera? ALBANO: No. I'm talking about the music of the Spanish composers. Albéniz, Granados, Breton, Turina. We created choreographies to their works with great love and great honesty. We put our heart and soul into them and the results were always positive. PD: What do you mean when you say \"honesty\"? Choreographing Albéniz' \"Castilla\" as a seguidillas? ALBANO: No. Honesty means we always danced the same, authentically, wherever we were-in Madrid, in Lisbon, in New York. We never changed our dances because the audience was foreign. We never thought, for example, that for an American or South American audience we should make the dances more commercial or faster or crazier. We danced the same for everyone. PD: That shows you had a lot of respect for your audience. ALBANO: Yes, and the audience deserves respect, because it makes the artist, applauds him, pays him. You must respect your audience. PD: When did you leave the stage? ALBANO: Twenty-two years ago. We had a daughter, and she always traveled with us. She was eight years old when we took our last tour. We had to think seriously in terms of her education. We didn't want to leave our only child in Madrid while we went traveling all over the world so we decided not to travel anymore and to start teaching in our own academy. I think we must be among the youngest dancers to retire because we were only 35 or 36 when we left the stage to be with our child.",
    "title": "A THREE PART SERIES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_10",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 998,
    "article_char_count_full": 5592,
    "article_char_count_review": 5592,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_10::A5",
    "article_text_for_review": "INTERVIEW FOR $ \\underline{\\text{JALEO}} $ BY CHARO BOTELLO --Which guitarists have inspired or influenced you the most? \"Niño Ricardo and Pedro Peña, my cousin, since he was the first guitarist in Lebrija...until I took my own direction.\" --What do you think of the Morón style? \"The two styles are very different, that of Morón and of Lebrija...I knew Diego del Gastor; he merits my greatest respect and I think that he did not come to be well known... Diego was a man of great contrasts, occasionally when he was sought he would go into hiding. On the other hand, with people he liked, he opened himself up in an incredible way... I don't believe that Diego was simply a guitarist, but more a person who influenced people who were around him...to me that's the way it was. There is a \"romance\" written about him by Pedro Peña which I like very much and in which I accompanied him on the guitar.\" -- With which cantaor do you feel the most relaxed when it comes time to accompany with the guitar? \"That is difficult. For their understanding of flamenco: Miguel Funi, Pedro Peña. In the aspect of more showmanship, there are others such as Calixto Sánchez and Curro Malena. With them I feel more open. Another person who has a sense of rhythm beyond comparison is Lebrijano, when he is relaxed. The compás of alegria and bulería; that is very typical of Lebrija. My grandmother, Fernanda de Pinini, daughter of Pinini, one of the creators of the cantina, had that compás...very rapid.\" --What do you think of the integration of other instruments never before used in flamenco until now? \"Above all, I'll tell you, I am a purist. Now, if one of those who does this modern style has the quality of the old, I would have to say old. But, there are only a few for the masses and their styles are not fresh...the importance of flamenco is the freshness which comes with direct proximity.\"",
    "title": "PEDRO BACAN: INTERVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_10",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 335,
    "article_char_count_full": 1884,
    "article_char_count_review": 1884,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_10::A6",
    "article_text_for_review": "-- In special case, where you accompany a good cantanr nr, a better example, a gypsy whose cante is reaching ynu deeply, if ha goes out of compás what is your reaction? \"Well, some cantes such as a malagueña, granaina, liviana, serrana, and taranto, are done in great depth. Although they fall out of cnmpás, that is acceptable. However, there are other cantes where this is not permitted for example, the cantía; no matter how well ynu kaow it, you can't inse the compás. For me, after depth comes compás.\" -- Are you a pure gypsy? \"I am a gypsy on both sides. On my mother's side is my aunt, La Perrata, Lebrijano, Turronern, Pedrin Peña. On my father's side, whosa name is Sebastian Peña Peña, are Fernanda y Barnarda who are cousins of my father, my grandmother, Fernanda de Pinini and Miguel Funi.\" -- Do you like to play for dance? \"I don't like to and I have no interest except in very spontaneous situations. Furthermore, I think that in artual flamenco, what is worst is dance. There are, however, some exceptions. For example: Mario Maya. On another level, I like Funi and Anzonini.\" \"I think that dance has never been as it has for the last three years. There was a style of dance here that has been lost. Wow it is very methodical. To be precise, the manner of dancing the sevillano way has disappeared. If there is anything left, it is not at the high professional level. I believe that it is entering into a new phase which does not interact me. To me the dance must start on the tip of the knot or shoe and finish on the tip of the toe and not spend half an hour clicking the heels.\" -- Do you feel that people today take flamenco more seriously than before? \"Not in Spain it has never been taken seriously. Only a few have done so. However, it is better known in the sense of the masses, but it is less understood.\" \" Do you think that flamenco in the U.S. is respected and well known? \"I will tell you that there are parts of the U.S. where flamenco is respected more than here in Spain. I have very good friends in America and here, and I will venture to even say that it is better known there, as in the case of Cristobal Carnes, David, and many others. **Do ynu like teaching foreigners?** \"If and when I see a genuine interest and they show respect for flamenco.\" (To Be Continued) $14.95 us Postage & Handling U.S. and Canada - $1.50 Other Countries - $3.00 Guitar Studios, Inc. 1411 Clement St. San Francisco, CA, 94118 \"Live in Munich\" sent by Vicente Granados In October of this year Marin Maya opened his Center de Actividades Flamencae in Sevilla. The Center will present recitals by the best falmenon artists in Spain and offer classes in dance and guitar. Mario will create choreographies für select artists when he is not busy with his group. Vicente Granados, our renrespндent, says, \"This Center, given the well-earned prætige of Marin as a bailanr, choreographer, and director of stage and music, would seem to be headed für great success thrnughout Andalucía and Spain.\" Maric Maya has formed a new group which had its debut in Paris on November 5. In December they will be at the Festival Mundial de la Raza Gitana in Hamburg, Germany, where gypsy artists from around the world will participate. Following that, in January and February he will be with his group in Hong Kong, Australia, and Japan. Manuel Morao, the great gypsy guitarist from Jerer has recently opened a flamenco dance academy in Jezez and should do wall, given the well-deserved fame of Manuel. Mario Escudero has moved to Sevilla, where he will give concerts and has thought of opening a guitar academy. Padro Bacán will give a recital in Mew York on Novembez 18 in the Spanish Institute in honor of a visit by the president of the Junta Autonómica de Andalucía. \"NO COMMENT\" by Shirvanian STANDARD OILER / MARCH-APRIL 1980 sent by Marilyn Bishop",
    "title": "NOTICIAS FROM SPAIN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_10",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 693,
    "article_char_count_full": 3850,
    "article_char_count_review": 3850,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_10::A7",
    "article_text_for_review": "[The following is the first in a series of articles on flamenco in Chicago selected from material sent to us by correspondent George Ryss.] PART I: CELLARCHINO by George Ryss This is an introduction to a man who has made flamenco stay in the Windy City. Lou Chino, ex-bailaor, guards and treasures \"el arte.\" He took over the basement of the Toledo Restaurant from Jose Garcia, calls it Cellarchino and told me that Cellarchino stays packed on Fridays and Saturdays. Chino has molded an exceptionally good cuadro. The amazing sotry is that the same artists have been together for months and quality has excelled. With superb teamwork, they improve with each performance. Manolo Segura, from Jaen in Spain, leads the Andalucian pathos. He is \"la figura principal\"--a good cantaor who lives his part. Exceptionally dynamic Maya does the ultimate siguiriγas. It could not have been created better (except, perhaps, in Andalucia). Arturo Martinez and Manolo are the supporting musicians. Mirna Maldonado, the joy of Andalucia, is another exceptional bailarina. Sergio Bahamondes, the male dancer, has very good body movements and gives a superbly polished performance. His forte is alegrias. The cuadro units for sevillanas, fandangos and rumbas. Cellarchino is in the basement of the Toledo Restaurant at 1935 North Sedgwick in the Lincoln Park-Old Twon area of Chicago. CELLARCHINO CUADRO (LEFT TO RIGHT STANDING) SERIO, MANOLO, ARTURO MARTINEZ, MANOLO SEGURA (SEATED) MIRNA MALDONADO, MAYA, MANAGER LOU CHINO MAYA DANCING SIQUIRIYAS MIRNA AND SERGIO",
    "title": "FLAMENCO IN CHICAGO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_10",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 243,
    "article_char_count_full": 1548,
    "article_char_count_review": 1548,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
