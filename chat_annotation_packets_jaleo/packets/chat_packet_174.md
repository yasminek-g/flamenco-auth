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
    "article_id": "JALEO_1983_08::A1",
    "article_text_for_review": "by Don Simpson Anita Sheer was born in New York City of Russian and Rumanian parentage. She started her musical career as a child of 5 years old, when she was enrolled in the School of Musical Education. Anita graduated 11 years later in their first graduation class. As a pre-teen, Anita placed first in the U.S. Piano Competitions for 5 years and gave numerous recitals at Carnegie Recital Hall. At 13, Anita entered the High School of Music & Art with fellow students Diahann Carroll, Peter Nero & Peter Yarrow of Peter, Paul and Mary. During the High School years, Anita built an enormous repertoire of union songs, blues and American and International folk songs in 14 languages. As a teenager she started performing professionally in WQXR and at the Potting Shed and Music Barn in Tanglewood, Mass. At 17, Anita entered Oberlin College as a music major and later became President of Mummers, the theatrical society. After 2 years, she transferred to Columbia University where she earned a Bachelor of Fine Arts Degree. While attending Columbia, Anita saw Carlos Ramos perform at La Zambra in New York City and became fascinated with flamenco music. Anita had an extensive collection of Carlos Montoya records and knew of his illustrious uncle, Ramón, and his career with La Argentina and La Argentinita. On a wild hunch she looked in the Manhattan telephone directory and found a Carlos Montoya. She called the number and, to her astonishment, it was the residence of THE Carlos Montoya. Anita did not speak Spanish then and had a lengthy badinage with Sally Montoya. The telephone conversation was one discouragement after the next such as \"Senor Montoya does not speak English,\" \"He does not teach,\" \"flamenco guitar is not for a woman,\" \"If you read music you can't play flamenco.\" Undaunted, Anita spent the next six weeks teaching herself to speak Spanish from records, tapes, books. Again, she called the fateful telephone number and Montoya himself answered. It was the moment-of-truth and the Maestro granted Anita one audition. Anita showed up at the appointed time with her pitiful cigar box of a folk guitar under her arm. The Maestro was gracious and charming and after the social amenities lifted a gleaming golden guitar from a plush velvet case and proceeded to play with spellbinding virtuosity. \"Now you do it,\" he said. After gaining her composure, Anita told the Maestro she would practice it at home and then play it for him. JALEO - AUGUST/SEPTEMBER 1983 ANITA AS A TEENAGER IN LENOX, MASS. flamenco puro, Anita went on a quest to find one of her idols, the legendary \"La Niña de los Peines\". Her queries were met with the rebuttal that the \"great cantaora\" died years ago. Happily though, Anita did find the very much alive and well La Niña de los Peines living with her restauranteur husband, Pepe Pinto, in Sevilla. During their merienda, La Niña admitted to becoming reclusive and not performing for years because people were unappreciative of the pure flamenco. While in Sevilla, Anita was introduced to the monumental accompanist, Manolo de Huelva, who never wavered from the traditional style of the \"cafe cantante\", the heyday of flamenco puro. His command of this pure form without all the pyrotechnics and flamboyance was so compelling that a multimillionaire from Belgium came regularly to hire Manolo for juergas. This patronage was so substantial that Manolo de Huelva was able to live his entire life without ever commercializing. While in Spain, Anita bought her first flamenco guitar from Arcangel Fernández, the former apprentice of Marcelo Barbero. Anita informed Arcangel that she wanted to learn flamenco singing and he set up an appointment for her with Pepe Pavón. When Anita showed up for her first lesson, there was Pepe with his whole family and entourage. The lesson turned into a private juerga of Pepe's entire repertoire. When she returned for her second lesson, she asked Pepe to go over one song for her. Much to her surprise, he did it differently every time. Finally Anita asked him to go over just one line until she learned it. From that point on, Anita and Pepe taught each other to be teacher and pupil of flamenco singing. On subsequent trips to Spain, Anita continued her singing lessons with Rafael Romero (El Gallina) and Pericón de Cádiz of La Zambra in Madrid. On her way back from Spain, Anita spent time in England where she made new friends of John Williams, the now famous classical guitarist and Theodore Bikel, the actor and international folk singer. Theo was helpful in getting Anita started on her career. He got Anita appearances on the BBC and when back in the States gave her her first night club job at the Cosmo Alley, his club in Los Angeles. Theo introduced Anita into the International Folk Music set in New York where she met Alex Hassilev (the Limelighters), Alan Arkin, and other luminaries in the Coffee House circuit and flamenco scene.",
    "title": "ANITA SHEER",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 835,
    "article_char_count_full": 4928,
    "article_char_count_review": 4928,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_08::A2",
    "article_text_for_review": "Our apologies again for the late appearance of this issue. We appreciate your indulgence -- there have been only a few distressed \"Where are my Jaleos?\" calls. October-November will also be a combined issue and December will close out volume VI with number '12' allowing us to start the new volume at the beginning of the new year. To alleviate any fears or rumors about the eminent demise of Jaleo: my enthusiasm remains high (nurtured by the encouragement of our readers -- see typical letter page 6 from Hasmira) and Paco has a backlog of many issues-worth of material. We are in great need of translators, however. If any of our bi-lingual subscribers would like to contribute to Jaleo in this way please let us know. Translation would be, of course, from Spanish to English (occasionally from French or German). Clear handwriting or typing is a prerequisite. Finally, our special thanks to the Los Angeles Jaliestas for their continued juerga donations in support of $ \\underline{\\text{Jaleo}} $ and those who have become sustaining members, bought gift subscriptions for friends or sent in contributions. --Juana DeAlva LETTERS DIEGO DEL CASTOR ON RECORD Jaleo, Today came the $ \\underline{\\text{National Geographic}} $ record, which is a joy to hear and a pleasure to see. In a presentation visual as well as audio, Diego is with us again, \"one of the great guitarists of this century.\" Sadhana, bless him, was right on! Hastily, John S. Lucas Winona, MI EDITOR'S NOTE: John refers to the record \"The Music of Spain,\" which features two bands of Diego del Gastor playing bulerias -- two really remarkable examples of flamenco guitar playing. If you write and request \"704 The Music of Spain,\" they will bill you $6.95 plus postage and handling. Send to: National Geographic Society, P.O. Box 1640, Washington, D.C. 20013. The \"History of Cante Flamenco\" (catalogue #S43601), a five record set of authentic flamenco is available for $12.99 plus $2.40 postage and handling from: Publishers Central Bureau, Department 239, 1 Champion Ave., Avenel, N.J. 07131 JALEO - AUGUST/SEPTEMBER 1983 IN SEARCH OF FLAMENCO BOOKS Dear Sirs: I am writing you in hopes that you may assist me in locating a couple of publications relating to flamenco music which I would like to obtain. The publications are: 1. $ \\underline{\\text{Art of Plamenco}} $, by Don Pohren 2. Mundo y Formas de Flamenco, by Antonia Morena, (in Spanish) I would like to purchase the above mentioned books. I would appreciate any assistance on locating a source for these. Thank you for any and all assistance you may give me on these. Sincerely yours, Doran E. Smout Rancho Cordova, CA [EDITOR: As usual we refer such inquiries to our readers who are a good source of information and gives us an opportunity to share the responses with others.] Supreme strings designed for today's finest classic and flamenco guitars At your local dealer or contact Antonio David Inc., 204 West 55th Street New York, NY 10019 USA Tels. (212) 307-1567 • 757-4412 or 3255 CATALOGUE OF MODERN FLAMENÇO RECORDS A collection of flamenco records from the modern era (1972-82), representing most of the important artists and including a number of unusual and rate items. Each record is described in detail and given a brief critical review. A tape library will make these records available. SEND $4.00 TO: PACO SEVILLA, 2958 KALMIA ST. SAN DIEGO, CA 92104",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 566,
    "article_char_count_full": 3395,
    "article_char_count_review": 3395,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_08::A3",
    "article_text_for_review": "1983 CURSO INTERNACIONAL DE BALLE ESPAÑOL by Paula Durbin During the second week of the course, Udaeta and his lovely wife, Marta, who administers the program on a year-round basis, gave a garden party in their castle home for the entire group of students. At sunset, our host joined Emma Maleras in a performance of castanet accompaniment to classical music. Maestra Maleras gave a particularly brilliant rendition of Chabrier's \"Espana.\" This could only be followed by a duet, and it was: Udaeta and Maleras then engaged in a vivacious palillo dialogue to the prelude of \"La Revoltosa.\" Artistic inspiration was to be found also when Channel 7 re-broadcast the fourth episode of Margot Fonteyn's \"The Magic of Dance.\" It featured another of the Sitges teachers, Eloy Pericet, and his sister Carmen in the escuela bolera dance, \"El Bolero de la Cachucha\" (see Jaleo, January, 1983). Inspiration came, however, not so much from the filmed version of the dance, as it did from sitting in the packed sala of the Hotel Montsarrat with Eloy himself among us. For avid dance fans, the French film, Les Uns et Las Autres, directed by Claude Lelouche, was playing in Barcelona. This otherwise conventional work ends with the entire ballet which Maurice Bejart set to Ravel's \"Bolero,\" danced by Argentine primer bailarin, Jorge Donn. Students who were able to remain in Sitges after the course ended had the opportunity to enjoy Narcisco Yepes in concert on August 14, the Ballet Español de Madrid at Barcelona's Teatro Grecn JALEO - AUGUST/SEPTEMBER 1983 JALEO - AUGUST/SEPTEMBER 1983 ABOVE: ELOY PERICET (WHITE SHOES AT EDGE OF STAGE) AND ESCUELA BOLERA EXERCISES BELOW: ALBERT SANS TEACHING CATALONIAN DANCES (PHOTOS BY FRANCESC COMOS) ELOY PERICET TEACHING ARM EXERCISES (PHOTO BY FRANCESC COMOS) August 15, 16, and 17, and eventually the new movie, \"Carmen.\" This film, by the way, is another Antonio Gades-Carlos Saura effort and was recently feted at Cannes. In addition to Cristina Hoyos, it features Paco de Lucía and Laura Del Sol. Very different from \"Bodas de Sangre,\" and to me, not as uniformly successful, it is not the rehearsal of a ballet but the story of the conception of a flamenco work to Bizet's opera, and Gades' eventual \"metejón\" with Del Sol, whom he picks to play the lead much to Hoyos' filmed chagrin. Carmen has perhaps too much dialogue and too little dance, but it is rescued by some excellent flamenco sequences. One is the scene in which Carmen/Del Sol kills her rival/Hoyos. This scene epitomizes the weaving of the film between art and reality, so that we are not really sure of where we are. Another memorable part of the film is the exquisite \"Habanera\" danced by Gades and Del Sol. Paco de Lucía's flamenco guitar adaptation alternates with Joan Southerland's voice accompanied by a full orchestra and this alone is worth the price of admission. According to ABC, the ballet, Carmen, danced by Gades and Hoyos, had its Spanish premier as a ballet at Santander this August. I hope someone who saw it will send Jaleo a commentary. The Sitges course ended again with a gala performance of the dances learned and played to an SRO audience in the Prado Theater. The show opened with an introduction by Udaeta, who served as the master of ceremonies throughout, and by Senora Ana María García de Orti, president of the support organization for the local school for the handicapped, which benefitted from the proceeds of advertising and ticket sales. Guest of honor, Angel Pericet, heretofore incognito as just another vacationer at the seaside resort, was seated in the box of the Mayor of Sitges who presided over the event.",
    "title": "JOSE DE UDAETA'S",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "7-10",
    "page_number": 7,
    "word_count": 613,
    "article_char_count_full": 3652,
    "article_char_count_review": 3652,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_08::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n* * * JOSE DE UDAETA TO VISIT HAWAII Jose de Udaeta will visit the Jones-Ludin Dance Center in Honolulu, Hawaii, November 1-19, 1983. During his stay, he will give classes at the Center and will visit ten public and private elementary schools as part of the \"Artists in the Schools Program\", sponsored by the Department of Education and the State Foundation for Culture and the Arts. A one-man concert, scheduled for November 18 and 19 at the Center, will give others a chance to enjoy Udaeta's virtuosity on the castanets and his flamenco artistry. The highpoint of his visit, and an important event for all of dance, will be the gala \"Dance Celebration\" on November 4 and 5 at Leeward Community College in Pearl City. The production, directed by Bstty Jones and Fritz Ludin, will include as well\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"masterpiece\"]\n\nce to enjoy Udaeta's virtuosity on the castanets and his flamenco artistry. The highpoint of his visit, and an important event for all of dance, will be the gala \"Dance Celebration\" on November 4 and 5 at Leeward Community College in Pearl City. The production, directed by Bstty Jones and Fritz Ludin, will include as well the Halau Hula o Rukunackala, a group which performs ancient Hawaiian hula, and the reconstructed \"Invention\", a modern dance masterpiece choreographed by Doris Humphrey. Support for this last project, which will feature New York artists and will be notated and video taped, has been provided by the Rockefeller Foundation, the National Endowment for the Arts and the State Foundation for Culture and the Arts. Betty Jones and Fritz Ludin are highly respected, well known modern dancers. Jones, for example, danced the lead role of Desdemona in the world primier of Jose Limon's \"The Moo\n\n[ENDING CONTEXT]\n\nevasive gypsy duende. The students of Amor de Dios, on one of those narrow streets that were walked by Cervantes and Lope de Vega, are the center of flamenco activity in Madrid, both for professionals and aficionados. Even before you arrive, you hear the zapateado that resounds like a machine-gun against the floors and mixes with the endless lament of guitars that are held in the underground rooms, most of which are in a terrible state of disrepair. The place pulsates with a contained and nervous energy, and there is a humid odor of sweat that seems to come from a boxing gym two floors up.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "DE BAILE ESPANOL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1262,
    "article_char_count_full": 7416,
    "article_char_count_review": 2541,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "masterpiece"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_08::A6",
    "article_text_for_review": "Los Angeles, CA: Lourdes Rodriguez and Francine Russelle-Chasambaliz hosted a Flamenco show starring LOURDES and featuring INNIN OE TRIANA and BENITO PALACIOS on Tuesday evening, October 4, at The Fez Nightclub on Vermont Ave. On August 30, Rodriguez and Chasambalis hosted the first of these events at The Fez. The evening honored visiting Flamenco star Manolo Marin, who was in Southern California teaching master classes in the art of Flamenco dance. Also on stage at that event were Antonito, Antonio Duran, Marcos Carmona, Pepita de Sevilla, Robina Carmona and Lourdes. Thare will be a series of Flamenco Evenings at The Fez scheduled for the first Tuesday of each month. The Fez features live Cabaret entertainment on Friday and Saturdays, an Arabic Folkloric Night on Thursday and will soon have a Greek Night on Wednesdays. (from Francine Russelle-Chasambalis)",
    "title": "JOSE DE UDAETA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "13",
    "page_number": 13,
    "word_count": 137,
    "article_char_count_full": 868,
    "article_char_count_review": 868,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
