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
    "article_id": "JALEO_1978_08::A11",
    "article_text_for_review": "EL CANTE - PART I A PALO SECO - singing without musical accompaniment. CANCIÓN(la) - song; a popular or composed song with fixed verses, beginning and ending; not usually used to refer to the cante. CANTAOR(a) - flamenco singer, the title implies the ability to sing \"cante jondo\" (a non-flamenco singer is a \"cantante\"). CANTE(el) - The song; specifically, flamenco song, as distinguished from \"canciones.\" CANTE CHICO - light festive cante, as for example, alegrías, fandangos de Huelva and verdiales; many of these songs, especially bulerías and tangos, are often called cantes \"por fiesta.\" CANTES DE IDA Y VUELTA - cante that has gone and returned, or \"made the round trip;\" refers to songs that were taken to Latin America by early Spaniards, underwent changes and then were brought back to Andalucía by later Spaniards (especially gypsies like Carmen Amaya) where they were further changed and incorporated into flamenco. The most popular of these are rumba and guajiras (from cuba) columbianas (Columbia) and milonga (Argentina). CANTE JONDO - deep song; usually used to refer to serious gypsy cante such as si-guiriya, soleares, toná and martinetes. There are those who disagree with this classification and feel that almost any cante can be \"jondo\" if the singer feels it that way; This is especially true of such potentially jonde cantes as mala-gueñas, tarantos, tientos and fandangos grandes. CANTE P'ALANTE(cante para adelante or de adelante) - singing done \"in front\" or as a solo. CANTE P'ATRAS(cante para atras or de atras) - singing done \"behind\" as accompaniment for dancing. CUPLE(el) - a popular (non-flamenco) song sung in a flamenco rhythm (usually tan-gos or bulerías). ROMANCE(el) - a story sung in flamenco song form. SALIDA(la) - singer's entrance or \"tune up;\" also called \"temple\" from the verb \"templar\" (to tune). TEMPLE(el) - see \"salida.\"",
    "title": "FLAMENCO TALK",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_08",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "9",
    "page_number": 9,
    "word_count": 300,
    "article_char_count_full": 1871,
    "article_char_count_review": 1871,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_08::A12",
    "article_text_for_review": "ANNIVERSARY JUERGA CELEBRATED IN COSTUME by Jack Jackson We ate and we drank and we enjoyed the company of those around us...then the music started! The senior member of our flamenco club, Ernest Lenshaw, was our first dancer. He seems to have the most enthusiasm of anyone and he dances his sevillanas as well as he paints his Spanish gypsies. His first partner was Luana Moreno who has a fiery gypsy look and her dancing is equally wild and free Suddenly there were children! More children than we have ever seen before! It must be that their enthusiasm for flamenco has been inspired by their last year of instruction. All were in costume, many fresh from the \"feria de Sevilla.\" Their multicolored ruffles swirled through paseos de sevillanas and desplantes de bulerías. When their young enthusiasm waned the second line of defense took their positions. We were also treated by Julia Romero's Alegrias accompanied by Paco. Toward the the end of the evening Joe Trotter came to see our juerga for the first time. This guitarist is an accomplished performer and teacher at San Diego State University both in classical and flamenco. The night was then very late and while the clean-up brigade made the moutains sparkle, a small group retired with Joe to the \"studio' where, I'm told, a mini-juerga continued into the wee hours.",
    "title": "JULY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_08",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "10",
    "page_number": 10,
    "word_count": 226,
    "article_char_count_full": 1328,
    "article_char_count_review": 1328,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_08::A13",
    "article_text_for_review": "This month's juerga will be held at the home of Jack and Sheryl Tempchin. Jack is a rock guitarist and singer who writes and plays his own songs. He has a solo album coming out at the end of this month titled simply $ \\underline{\\text{Jack Tempchin}} $. Sheryl has enjoyed flamenco music for years and has studied different types of dance, but has only recently begun studying flamenco and become a member of jaleistas.",
    "title": "AUGUST JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_08",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "10",
    "page_number": 10,
    "word_count": 74,
    "article_char_count_full": 419,
    "article_char_count_review": 419,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_09::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMichael Hauser Flamenco dancer Suzanne Hauser left Minneapolis, Minnesota early this spring for Spain for which was to be a short stay of only six weeks duration. Instead, she was seen dancing in the Amor de Dios studios in Madrid by Ciro and the gypsy dancer, La Singla. La Singla then asked Suzanne to join her company for a tour of the Costa Brava (the northeastern coast of Spain). It was a difficult decision, for it meant leaving her daughter, Bridget, and her guitarist-husband, Michael, at home in Minneapolis until her return in mid-September. However, everyone decided this would certainly be an invaluable experience, so the decision was made. All reports from Spain to the homefront certainly indicate that Suzanne is indeed having the flamenco experience of a lifetime. Not only does\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"home\"]\n\nr daughter, Bridget, and her guitarist-husband, Michael, at home in Minneapolis until her return in mid-September. However, everyone decided this would certainly be an invaluable experience, so the decision was made. All reports from Spain to the homefront certainly indicate that Suzanne is indeed having the flamenco experience of a lifetime. Not only does she feel her dancing to be much improved, but her Spanish as well. In fact her last letter home mentioned that she had not spoken a word of English in a month. The company is a small one, comprised, with the exception of Pepa Coral (dancer and sister of Matilde Coral of Sevilla) and Suzanne, of flamenco gitanos. La Singla is the featured dancer. Flamenco aficionados would remember her from the movie \"Los Tarantos\" as the young gypsy girl whose lover belonged to the enemy gitano family. There is a male dancer, Isidro, who along with Pepa Coral do whatever choreography is used. Then there are two singers in the troupe. One of these is Andorrano from Morón (see article by Susana Keyser in this issue). The guitarists are the renowned Juan Maya \"Marote\" and his seventeen year old son, who seems to be closely following in his father's footsteps. Suzanne's adventure with her new job began with three weeks of intensive rehearsals, four to five hours each day, with perhaps no more than a five or ten minute break. Suzanne has commented on the high level of energy that these gypsy performers constantly put forth and demand from the members of the troupe. Even in working out the simplest rumba, La Singla danced alongside Suzanne until every step and move\n\n[ENDING CONTEXT]\n\na job in one of the smaller tablaos in Madrid by about five minutes on his last visit to Spain. As many Americans know, and Spaniards are coming to realize, some of the top performers in flamenco today are not necessarily Spanish. Hopefully, as in the case of Suzanne Hauser, ability and quality will continue to be the main criteria for the flamenco artist's chances for employment in Spain, whether that person is Spanish, American, or any other nationality. Jaleo Newsletter Meetings are held at the home of Maria Solea. See calendar for schedule and call Maria at 565-2202 for directions.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "SUZANNE HAUSER TOURS WITH GYPSIES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2, 3",
    "page_number": 1,
    "word_count": 1261,
    "article_char_count_full": 7452,
    "article_char_count_review": 3238,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "home"
      }
    ]
  },
  {
    "article_id": "JALEO_1978_09::A2",
    "article_text_for_review": "Dear Jaleo, Each issue of $ \\underline{\\text{Jaleo}} $ seems better than the last and I think that the August issue is the most exciting one yet! I was especially happy to find an article and a letter from Carol Whitney whom I remember so well from the \"leaner\" days of flamenco in San Diego when Carol lived here. She really added a lot to our small juergas. The new calendar of events for the month is such a fantastic idea so you can tell at a glance what is going on for the entire month. And --- the index will be so helpful to many of us who refer back to different articles in Jaleo. The entire production of Jaleo from newsletter to \"juerga\" represents many, many hours of hard work on the part of editors and assistants and results in a very successful \"operation.\" the name of the hostess be mentioned in the article? Marilyn Bishop Escondido, California Editor's comment: Thank you for the kind comments concerning Jaleo. The omission of the hostess' name is due to two factors. First, we don't have control over how a writer choses to do the juerga report. Second, due to the very rushed process of getting out each Jaleo, many credits have been overlooked; we hope to do better in the future. Meanwhile, here are some of the people who deserve mention: -- Many of the juerga photos in the July issue were taken by John MacDonald; the rest were taken by Jesús Soriano, who did all of the procéssing. -- The drawing of Sabicas on the July front page was done by Pilar Coates, a member of Jaleo staff. -- The photograph of Joselero in the August issue is by Ángel de Morón. -- The hostess for the June juerga was Marilyn Bishop. RE: $ \\underline{\\text{Jaleistas & Jaleo Survive a Year}} $, July, 78. Blasphemy!!! Take the author of this article and hang him by the thumbs! \"Stan Schutze, a person with no real interest in flamenco...\" My boiling brain waves can surely be felt in San Diego! ±@**!'#&@ Stan Schutze Tehran, Iran Editor's comment: Now, now, Stan - there may be children reading. Glad to see you are learning to write in Arabic! WELCOME TO JALEISTAS - NEW MEMBERS San Diego: Lee & Helen Pierce, Brad Blanchard, Jesus Benayas, Raul, Charo, Susan & Michelle Botello, Benito Garrido, Michelle Martin Devigne, Manuel Ramirez, Ruth Cigledy, Armando Lopez, Jesus Benayas; Sunnyvale, Ca: Mariano Cordoba; San Francisco: Irving Shore, Carla Cruz, Aurora Sauceda, Jose Ramon, J. Benetti; Jose Serrano (Ariz.), Allen & Penelope Yonge (Wash.), Doris Dieu (Illinois), Joe Fischer (Mo.), Barbara Bartosz; Linda Small (New York). About Camaron De La Isla... By Rodrigo De San Diego",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_09",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 4",
    "page_number": 3,
    "word_count": 454,
    "article_char_count_full": 2590,
    "article_char_count_review": 2590,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
