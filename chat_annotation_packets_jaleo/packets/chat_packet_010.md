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
    "article_id": "JALEO_1978_02::A8",
    "article_text_for_review": "A record review which first appeared in the Madrid paper, $ \\underline{\\text{Informaciones}} $. It was written by Antonio Villarejo. \"THE FLAMENCO GUITAR OF ENRIQUE DE MELCHOR\" After a first stage of consolidation of the concert style of flamenco guitar playing, in which Paco de Lucía, Manolo Sanlúcar and Victor Monje (Serranito) played a major role, a wide panorama is now seen in this specialty of flamenco art. In this new generation of flamenco soloists, Enrique Melchor stands out with absolute brilliance. The son of the famous guitarist, Melchor de Marchena, he has made his first record as a soloist, and this is cause for much happiness among flamenco enthusiasts due to the exquisiteness and good taste of his playing, the perfection of his compositions, and his profound feeling for flamenco. The only instrument on the record is his guitar which he overdubs on some cuts. In one theme he is accompanied by bongos and in others by palmas. He plays the following numbers, all of which he composed: columbianas, solea, farruca, tarantos, twobulerías, tangos, serranas, rondeñas, and rumba. థృథృథృథృథృథృథృథృథృథృథృథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథథ�",
    "title": "ENRIQUE DE MELCHOR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "9",
    "page_number": 9,
    "word_count": 179,
    "article_char_count_full": 2467,
    "article_char_count_review": 2467,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A9",
    "article_text_for_review": "JALEISTA BEHIND THE SCENES TAKES LEAVE OF ABSENCE Often it is the little known person, quietly doing a job behind the scenes, who is a most essential element in creating and maintaining an organization. Stan Schutze is such a person in Jaleistas. Since he will be leaving us for two years to take a job overseas with Bell Heliocopter International in Tehran, we are dedicating this issue of $ \\underline{\\text{JALEO}} $ to him. Neither a guitarist, singer or dancer, Stan was introduced to flamenco through a friend. He was present at the initial formative juerga in June of 1977 and at that time, offered to help in any way he could. His talents and knowledge turned out to be considerable. One area in which they were applied was the production and progressive upgrading of Jaleistas' newsletter. He brought to JALEO his great interest and expertise in graphics. He researched stores and references for ideas, obtained drafting equipment needed for layout and educated himself about and explored printing methods to obtain the highest quality product at the most economical price. He unselfishly turned over his house for bi-monthly newsletter meetings and his office for staff use during the day. At meetings he has been an \"idea machine\", cranking out suggestions, sketches and solutions to problems. Those who have received the newsletter since its conception, have had the opportunity to see the results of many of those ideas. What is not appreciated by the uninitiated, is that each small change represents hours of thought and work. Stan's seemingly boundless energies have not been limited to the production of the newsletter. He has also created a reference library to which the JALEO staff can turn in his absence. He established the JAL-EISTAS accounting records and bank account. He has supplied the juerga site on three occasions. At juergas, he usually arrives early to help set up, facilitates during the evening where he is needed and stays until the last styrafoam cup has been thrown away. Is this any indication of how Jaleistas has grown since June 18th, 1977? ___ At the next juerga, if you see a sandy-haired fellow with a mustache, dozing off in a corner in the largest available easy chair, with a can of beer nearby, Don't awaken him! He is catching a much deserved rest and recharging his batteries. Just quietly say, \"Thank you Stan. We appreciate all you've done for us.\" We may not be seeing Stan for two years but can be certain that he will continue to work behind the scenes. We have it on good authority that he has packed his \"T\" square and portable drawingboard, so it won't be a surprise if thick envelopes from Tehran begin to appear in our JALEO post office box. 2 MISCELLANY Gypsies having a fiesta in the cave home of Manolito de la María in Alcalá. (photo by Robert DeVore)",
    "title": "ADIOS!",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "10",
    "page_number": 10,
    "word_count": 482,
    "article_char_count_full": 2816,
    "article_char_count_review": 2816,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A11",
    "article_text_for_review": "A Delightful Spanish Recipe! POTATO AND ONION OMELET To serve 4 to 6 1 cup plus 3 tablespoons olive oil. 3 large potatoes (about 2 lbs), peeled and sliced into 1/8-inch-thick rounds. 2 tablespoons salt. 1/2 cup finely chopped onions. 4 eggs. In a heavy 10- to 12-inch skillet, heat one cup of olive oil over high heat until hot but not smoking. Add the potatoes, sprinkle them with one teaspoon of the salt and turn them about in the pan until they are well coated with the oil. Reduce the heat to moderate and cook the potatoes for 8 to 10 minutes, turning them over occasionally; then stir in the onions. Continue cooking over moderate heat for about 10 minutes, stirring every now and then until the potatoes are tender and golden brown. Transfer the entire contents of the skillet to a large sieve or colander and drain the potatoes and onions of all their excess oil.",
    "title": "Tortilla De Patata",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "11",
    "page_number": 11,
    "word_count": 158,
    "article_char_count_full": 872,
    "article_char_count_review": 872,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A12",
    "article_text_for_review": "... NEWS OF OUR JALEISTAS Congratulations to Valentín Cabeza and his new wife, Lola... Jesus and Melody Soriano have a new son, Alonso... Thor and Peggy Hansen are anticipating a visit from la cigüeña... dancer, Juanita Franco, with Frankie and Angela Gigletto and guitarist Joe Kinney performed at La Casa de España in Balboa Park... dancers Deanna Davis (see \"La Luz\"), and Carmen Monzón with guitarist, Tomás Reineking, will perform at Tom Ham's Lighthouse for the Propeller Club of San Diego",
    "title": "EL OIDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "11",
    "page_number": 11,
    "word_count": 81,
    "article_char_count_full": 495,
    "article_char_count_review": 495,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A13",
    "article_text_for_review": "de Parilla de Jerez Parilla de Jerez, winner of the \"Premio National de Guitarra\" in 1973, is related to Manuel and Juan Moreno, Los Morão, two guitarists also from Jerez and especially re-knowned for their accompaniment of the cante. Parilla's guitar style is similar in some respects to the playing of Manuel Moreno, although he has a light delicate touch which differs radically from the hard driving playing of \"Morão\" (on recordings, this characteristic is less noticeable than in live performance). Parilla's style of playing, a style common to many players from the Jerez area, is characterized by long sequences of single notes which frequently emphasize rhythm rather than melody. Rather than develop a singing type of melody, the runs of notes work within the compás to create accents and counteraccents, surprize tones and sudden stops; the falsetas often move from one compás to the next without regard to normal stopping and starting points. The playing is usually improvizational and often a falseta will appear to come to close, only to burst out in another direction. Many of these traits are found to some degree in the playing in other areas, such as Morón de la Frontera, but the Jerez style has its own unique flavor. Parilla is currently active on the festival circuit where he is the favored accompanist of many cantaores from Jerez, such as Curro Malena and Terremoto de Jerez. He seems to be serious in his approach, not exhibiting the flambouyance of such accompanists as Paco Cepero and Juan Carmona \"Habi-chuela,\" who frequently appear on the same program with him. He is not too serious to dance a few desplantes por bulerías in the fin de fiesta (grand finale). The following falseta, por alegrías, is used by Parilla as an introduction and is a good example of the flowing Jerez style of guitar playing. It should be played fairly fast for the best effect and careful attention should be paid to the accent marks as there is a lot of countertime. The following symbols are used: <accented beat - - stacatto or note stopped with left little finger or right hand (chords p - note played with the right thumb - first, second, third, fourth fingers of the left hand",
    "title": "ALEGRIAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "12",
    "page_number": 12,
    "word_count": 374,
    "article_char_count_full": 2191,
    "article_char_count_review": 2191,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
