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
    "article_id": "JALEO_1990_04::A16",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFlamenco flourishes in the Northern California Bay Area. From San Jose in the South Bay to San Francisco in the North Bay, the rhythms of flamenco can be heard at public performances and private juergas. Supporting and nurturing this interest are several wonderful teachers who live in the Bay Arca. Rosa Montoya is one of the finest and most dedicated teachers. Her studio is located in San Francisco. Drawing from an extensive life in flamenco (see Jaleo Vol. VI No. 3 for information on the career of Rosa Montoya) she instructs students in baile and cante. Her classes span the range from beginning to advanced, classes for men and women. In addition to her own studio, Rosa also teaches flamenco at San Francisco State University. Studying in Rosa's class in not merely the learning of\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"deeper\"]\n\nudio is located in San Francisco. Drawing from an extensive life in flamenco (see Jaleo Vol. VI No. 3 for information on the career of Rosa Montoya) she instructs students in baile and cante. Her classes span the range from beginning to advanced, classes for men and women. In addition to her own studio, Rosa also teaches flamenco at San Francisco State University. Studying in Rosa's class in not merely the learning of technique. One also gains a deeper understanding of the feeling behind the art of flamenco. Many of Rosa's students are accomplished enough that the possibility of public performance is now a reality. At the close of last year Rosa organized \"Flamenco Ole!\", a student dance concert, which also featured Rosa's cante class and the \"Rosa Montoya Bailes Flamencos\" — her professional troupe. The concert took place at the Mission Cultural Center in San Francisco to a sold out audience. Singer Charo Monge came up from Southern California to provide additional cante. [Below] Joyous celebration by the dancers at the end of the first half of the concert. Left to ri\n\n[ENDING CONTEXT]\n\nif this is the first and last dance that she or he will ever do. This is also what is happening to the singer and guitarist. The dancers whole body is being moved, shaped and accented by the interpretation of the cante and guitar. This is what it is to \"become the dance\" — as you, the dancer, are also singing within (not words) but the feeling and soul of the cante. You do not have to think of steps. The steps happen in complete harmony with the soul and spirit of the cante if you stay sensitive to it and have a good knowledge of your basic flamenco dance technique and style. --Teodoro Morca\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "•ROSA MONTOYA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "38-39",
    "page_number": 38,
    "word_count": 1114,
    "article_char_count_full": 6317,
    "article_char_count_review": 2704,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "deeper"
      }
    ]
  },
  {
    "article_id": "JALEO_1990_04::A17",
    "article_text_for_review": "What especially ties Pilar Rioja to the gifts of the late Argentinita and to some extent to the earthier, more extraverted talents of Carmen Amaya is her consummate technical clarity. Whether performing dances of the classical escuela bolero, the flamenco or such boisterous Spanish folk dances as the canario, folias, villano, and seguidillas, or dances of her own devising, Miss Rioja typically displays an admirable command and virtuosity of the feet and a mesmerizing control of the upper body, with arm and hand movements charged with utmost expressiveness. \"Yes, I have been inspired by Argentinita and Carmen Amaya, but even more so by Antonia Mercé, who was known as La Argentina,\" says Miss Rioja, \"I have never seen La Argentina perform — she danced during the 20's and 30's — but I have read much about her and was deeply impressed by the fact that she elevated Spanish dance to a great art form, that she took it out of the gypsy music hall and made it live on the concert stage. More than anything, La Argentina proved that Spanish dance had class and elegance. And she imbued it with a sense of history.\" Miss Rioja, her face an impassive mask of nobly chiseled features, offers the image of Spanish restraint and pride. Yet, there is fire beneath her aristocratic mien. \"Of course, the problem with Spanish dance is that there is no academy for it, such as exists in classical ballet,\" she says. \"There is no school that trains the Spanish dancer in its many techniques and disciplines — in its history. There is simply no cultural perspective. Because of that, Spanish dancing has often been vulgarized, and the art has fallen into banalities. My hope is to correct this false impression.\"",
    "title": "•PILAR RIOJA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "40",
    "page_number": 40,
    "word_count": 293,
    "article_char_count_full": 1705,
    "article_char_count_review": 1705,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1990_04::A18",
    "article_text_for_review": "“To witness the phenomenon of a performance by the La Conja is to experience the soul of Spain itself.” —Editor, Cymbal Magazine At the age of ten, Conja Abdessalam became fascinated by flamenco dance. With talent and determination young Conja pursued her dream, and at thirteen, won a scholarship to study and perform with the José Greco. At sixteen, La Conja emerged as a flamenco artist of ability and fire. She commuted from America to Spain enhancing and refining her art with Mario Maya other teachers. She also performed with Carmen Mora, then the first dancer with the Ballet Nacional de España. In 1975 La Conja became involved with the flamenco singing art: the \"cante\". As dancer and singer her horizons expanded. She appeared in American and Spanish films and television, including \"The Great Santini\" with Robert Duvall, and \"Studio Abierto\" in Spain. La Conja toured the United States appearing at the International Hotel with Danny Kaye, El Nido in New Mexico, and La Colombia in Florida. She performed in Europe and Asia, returning to Spain to instruct at the renowned Amor de Dios studio in Madrid. La Conja was the only America chosen to join an all-Spanish troupe that performed throughout Japan. She joined the Spanish dance company of Jose Molina on a tour of the United States and Canada, which culminated in a",
    "title": "•LA CONJA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "41",
    "page_number": 41,
    "word_count": 225,
    "article_char_count_full": 1332,
    "article_char_count_review": 1332,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1990_04::A19",
    "article_text_for_review": "[from: The New York Times, June 15, 1989; sent by George Ryss] by Jennifer Dunning The sketchily handwritten sign reads simply \"Fazil's.\" Two dark and narrow flights up, the stairway opens out into a small room with a desk and chairs. The sound of castanets and a tinkling tango floats out from the battered rehearsal studios that surround the front desk. Some legends live on, however. While much on New York's theater history has vanished, including once-bustling rehearsal studios like Alexandro's and Rudy's, Fazil's has survived, becoming, in the process, a part of the city's theater lore. A Who's Who of Dancers A Broadway chorus boy named Jimmy Cagney once climbed its stairs to attend tap classes in the studios. Paul Draper and Eleanor Powell rehearsed there, and so did the Condos Brothers and the Nicholas Brothers, José Greco and Carmen Amaya, and the hoofers from several Broadway shows. Hanya Holm taught modern dance in one studio; Honi Coles taught Dick Cavett to tap in another. Alvin Ailey worked with his first, small company at what is now Fazil's Dance Center. And memories of the studio gave Gregory Hines the idea for Sonny's, the dance studio that is home to the hoofers in the film \"Tap.\" Fazil's has remained a center for flamenco, Middle Eastern and tap classes and rehearsals, with some ballroom dancing. Performers from the Broadway musical \"Black and Blue\" work out there, and so did Twyla Tharp at the start of her work in \"Singin' in the Rain.\" “Sporadically, we get a lot of clog dancing,” said Maria Alba, a flamenco performer and teacher. “The old-time hoofers still come in. They come early, when it’s quiet, and just move together and riff.” There is riffing of another kind on Fridays and Saturdays, fall through spring, in a little nightclub on the building's second floor that accommodates Middle Eastern dancers and their families and protégés, and, occasionally, tap and flamenco get-togethers. “It’s a throwback to the kind of Middle Eastern cabaret that used to exist here in the mid-1960’s,” Ibrabhim Farrah said. Mr. Farrah, the publisher of Arabesque magazine, performs Middle Eastern dance and teaches it at the studio. “Fazil’s has had its ups and downs,” Mr. Farrah said. “It survives. There is a sense of history as soon as you walk in the doors. Places to practice dance are always in the sleaziest parts of cities, especially in third-world countries. But they’re always a world within a world.” A Varied Clientele And so it is today. A Meryl Streep lookalike in a business suit appears at the desk to register for a Middle Eastern dance class. A dark-haired woman who looks like a gypsy stops for a little conversation about friends in Spain before her flamenco session begins. A priest hands over his money almost wordlessly. \"I think he takes the ballroom class,\" Miss Alba said. Miss Alba first came to the studio in the early 1960's as a performer with the Ximénez-Vargas Company, when the company was rehearsing there for an American tour. Performing on her own, Miss Alba continued to work A minuscule theatrical seamstress named Chiquita holds court in a tiny, top-floor room crammed with costumes and signed photographs, the successor to such nondance tenants as a Tarot reader and seller of magic potions. Small studios rent for $11 an hour and large studios for $13 and hour. 'There are mostly Middle Eastern people on the first floor of studios,' Miss Alba said. 'The tap people are all over the place. Then you have flamenco companies. We're not allowed on the first floor. Most studios don't want flamenco dancers because you ruin their floors. And if not floors, the ceilings below.' She does all her rehearsing at Fazil's. 'It's either there or the sub way,' she said. Studio rentals elsewhere tend to run a good deal higher. “These days, I don’t know how anybody manages to rehearse,” Miss Alba said. “You can’t afford to go in and make mistakes at those prices.” Neither she nor Mr. Farrah is impressed with the sleek newer studios. \"They're depressing,\" Miss Alba said. \"They're so clean. With palm trees and things. Everyone is there in their dancer designer togs. This is not serious, folks.\" \"They're so quiet,\" Mr. Farrah said of today's more modern studios. \"So many rules and regulations. No feeling of relaxation. And that's as most a statement of today's art. Their guts are lacking.\" Maria Loreta and José Molina. (photo by Karen Bowers)",
    "title": "•LEGENDARY DANCE STUDIO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "42-43",
    "page_number": 42,
    "word_count": 748,
    "article_char_count_full": 4419,
    "article_char_count_review": 4419,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1990_04::A20",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nILLUSTRATED ENCYCLOPEDIC DICTIONARY OF FLAMENCO DICCIONARIO ENCICLOPÉDICO ILUSTRADO DEL FLAMENCO [IN SPANISH. TWO VOLUMES. 860 PAOES. 10 x 13 x 3 INCHES. HARD COVEA. JOSA BLAS VEGA AND MANUEL RIOS RUIZ. EDITORIAL CINTERO. MADAID, SPAIN. 1988. PTS. 25,000 (APPROX. $220 US)] by George Hollenberg The history of flamenco, like that of Spain itself, is one of individuals. Perhaps the durability of flamenco may be due to its capacity to permit great personal expression, albeit superimposed on an established framework. This work, without doubt the most extensive of its type ever written, is more than a dictionary of flamenco; it is a dictionary of flamencos — brief biographies of more than 5,000 individuals. In these pages the famous, the gitano, the foreigner, the experimenter, all are given\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\n. Perhaps the durability of flamenco may be due to its capacity to permit great personal expression, albeit superimposed on an established framework. This work, without doubt the most extensive of its type ever written, is more than a dictionary of flamenco; it is a dictionary of flamencos — brief biographies of more than 5,000 individuals. In these pages the famous, the gitano, the foreigner, the experimenter, all are given their due, including many whom fate had rightly left in obscurity. These biographies, their length proportional to the authors' opinion of the renown of each individual, are alphabetically arranged, usually using the artist's stage or nickname. There follow their real name (nombre y apellidos), place of birth and dates. A capsulated biography is then given and, if applicable, extracts of commentary by better known flamencologists (too often taken whole-hog from record jackets). Most striking is the wealth of photographs, many never before published. The Dictionario Enciclopédico represents the most modern, complete and objective compilation of the lives of those who made and nurtured flamenco. Although it is in the tradition of other similar works it is, by and large, devoid of their defects: Arte y Artistas Flamencos-panyergic; Mundo y Formas-polemic; Li\n\n[ENDING CONTEXT]\n\nworks by an author. José L. Romanillos, a guitar builder himself, must have submerged himself in research for many years in order to come up with all of the fragments of sketchy information that he needed in order to put together a still somewhat sketchy picture of Torres' life from interviews with family members and obscure documents like birth certificates, church records, debtor's records, attorney records, and correspondence. I found the description of Torres' Sevilla period (1844-66) to be fascinating and filled with information about the guitar scene of that time; we meet\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "BOOK REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "44",
    "page_number": 44,
    "word_count": 1108,
    "article_char_count_full": 7017,
    "article_char_count_review": 2905,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "many"
      }
    ]
  }
]
```
