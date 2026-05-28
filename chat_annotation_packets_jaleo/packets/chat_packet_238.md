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
    "article_id": "JALEO_1986_SPRING::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTHE MOST COMPLETE CANTAOR OF ALL TIME [from: Diario de Jerez, Jan. 20, 1986; sent by El Chileno; translated by Paco Sevilla.] by Juan de la Plata Born on Calle de Sol, number 60, and possibly baptized in Bornos, his father's town, Antonio Chacón García, son of a shoemaker named Antonio Chacón Rodríguez and María García Sánchez of Jerez, still holds the secret of the place of his birth, some fifty-seven years after his death in the capital of Spain [Madrid]. We have to believe he was from Jerez, since he always claimed it and confirmed it in private and public, saying that he had been born in Jerez in 1865. The first time Chacón sang in public was in a baptism. He was still a little boy wearing short pants. That night he came home at four o'clock in the morning and had to leave running\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"apprentice\"]\n\ndrid]. We have to believe he was from Jerez, since he always claimed it and confirmed it in private and public, saying that he had been born in Jerez in 1865. The first time Chacón sang in public was in a baptism. He was still a little boy wearing short pants. That night he came home at four o'clock in the morning and had to leave running because his father was waiting for him with a shoemaker's strap. At ten years of age, he began to work as an apprentice in the Refige barrel factory in Clavel Street, where he was always hidden among the piles of barrel staves, singing quietly to himself. Later, he would also learn his father's profession as a shoemaker, when his parents went to live on Cazón Street, in front of the Guardia Civil barracks. Today, there is a plaque at that address, in his memory, placed there by the Government THE GREAT ANTONIO CHACON, IMMORTALIZED BY THE PAINTER CAPUCETTI JALEO - VOLUME IX, No. 1 of Jerez, under a proposal by the councilman, Manuel García Mier, in a session of the full city government on December 27, 1929, one week after the death of the greatest master of the cante of Jerez. Chacón was fourteen when he earned his first salary for singing in public. It was in Jerez and he was given six reales [1½ pesetas]. Years later, with the passage of time\n\n[ENDING CONTEXT]\n\nHe has recorded nine albums for Decca Records including three live performances and a duo effort with Paco DeLucia, another world renowned flamenco guitarist. He has also made several highly successful tours of Australia, given recitals with the company at festivals in Hong Kong, Edinburgh, Holland, and Aldeburgh and performed to audiences in Japan and London, all to widespread enthusiasm. Paco Peña appears regularly worldwide on Television and has received extensive praise for his shared recitals with John Williams. Paco Peña uses D'Addario Strings. D'Addario E Farmingdale NY 11735 USA\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "DON ANTONIO CHACON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1372,
    "article_char_count_full": 7891,
    "article_char_count_review": 2920,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "apprentice"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_SPRING::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nYOU'LL EARN A LIVING WITH YOUR VOICE [Sent and translated by Brad Blanchard.] The following is a translation of the last chapter of a book published by the regional government of Extremadura called Gitanos Extremenos. The entire book -- filled with fine color photographs -- deals with the history and lifestyle of the gypsies in Extremadura, who probably in all of Spain, are the gypsies most reluctant to abandon the old ways. For example, Extremadura is the last region in Spain where you still can find nomadic gypsies who earn their living buying and selling horses, cows, and mules. The author of the publication, \"un gitano por los cuatro costados,\" is author of various works, including the script for \"Ay, Jondo\" of Mario Amaya and \"Amargo.\" His name, Francisco Suarez, and along with his\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\n.\" His name, Francisco Suarez, and along with his profession of writer and artist, he works in the post office here in Badajoz. Porrinas de Badajoz was a fine singer who has never been fully accepted in Andalucía, in part because of his unorthodox style of singing. He was very famous here in Badajoz — everyone, gypsy or payo, who was living here when he was alive has some anecdote to tell about him. But I'll let the article speak for itself; his many recordings speak for this cantaor extremeño: before with Porrina. The next morning, as he was passing by Cafetería Colón, he saw the cantaor having his shoes shined. Like all the bad flamenco aficionados, he asked him to sing a fandango. Porras was happy to accept. A former mayor of Badajoz had been \"de juerga\" the night The shoe-shine boy was moved as he listened. When he finished singing, the mayor put a 1,000 peseta bill in his hand, and José Salazar Molina accepted it graciously. And with his unique personality he offered it to the shoe-shine boy, saying \"For you: the 1,000 pesetas and the fandango.\" They say the mayor never again went \"de juerga\" with Porrina de Badajoz. El Porra accepted an invitation from the Marqués de Villaverde to go to a fiesta in his home. the Marqués also explained to two pol\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_01 | trigger=\"moving\"]\n\nlona whose son was ill approached him and begged him to sing a saeta in his name, and he would pay him 25,000 pesetas for it. El Porra sang the saeta to the Virgen and then wouldn't accept the check, saying, \"I don't sing to my Virgen for money.\" One night during the summer in Sevilla, there was a fiesta in Casa de Pilatos. The Duquesa de Alba, who was with her friends in the central patio, noticed the cantaor who, without calling attention, was moving his chair back out of the circle -- where the aristocrats were having an animated discussion -- into the second row. With exquisite discretion and friendliness, the Duquesa asked about his move. El Marqués answered, \"I'm sitting here to sing.\" Doña Cayetana didn't understand anything at that moment. But at daybreak, as he bid goodbye to his hosts, Porrina presented a bill for 75,000 pesetas. Then she understood his answer. In an interview with J\n\n[ENDING CONTEXT]\n\nare like gentlemen from another time.\" His shield was a carnstion, glasses and the Ace of Clubs: the carnation to sweeten the air, the glasses to see what he wanted to see, and the card to give \"porrazos\" (a difficult play on words related to his name)... One day a gypsy womsn read his future: \"You'll do little with these hands, and less with your head, but you'll earn a living with your voice.\" When he was born on calle Atocha in Badejoz, in front of the Guidiana River, it was pouring rain. In January of that year, Saturn passed through Capricorn quickly and Venus didn't shine that morning.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PORRINA DE BADAJOZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1142,
    "article_char_count_full": 6254,
    "article_char_count_review": 3868,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "many"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "moving"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_SPRING::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFOR OVER TWENTY YEARS, SHE HAS BEEN BLIND AND ALONE WITH HER MEMORIES Manolita de Jerez, the cantaora who travelled almost the entire world. [from: Diario de Jerez, March 2, 1986; sent and translated by El Chileno] by Juan de la Plata It had been nearly twenty-five years since I last saw this woman who is now in front of me. A woman who was a beautiful cantaora, whom I took one day to sing in front of the microphones of Radio de Jerez, as she herself reminds me when I went to visit her. An artist from Jerez, about whom I wrote in my book \"Flamencos de Jerez,\" published in 1961, \"a young and beautiful cantaora, who since 1950 travels the entire world, carrying on her lips the best and purest of our cente.\" Her coplas have feeling, melody, and have their own seal of fina quality. She excels\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Manolita\"]\n\nI went to visit her. An artist from Jerez, about whom I wrote in my book \"Flamencos de Jerez,\" published in 1961, \"a young and beautiful cantaora, who since 1950 travels the entire world, carrying on her lips the best and purest of our cente.\" Her coplas have feeling, melody, and have their own seal of fina quality. She excels mostly in the mslagueñas of Chacón, and in the cante por fandangos. She is from the Barrio de Santiago, and her name is Manolita Cauqui. Manolita Cauqui Benftez, who carried throughout all the world continents the name of \"Manolita de Jerez,\" as a triumphant flag of the srt, has not sung fsr over twenty years because of an eyesight silment that the best doctors could not cure deprived her of sight forever, forcing her to abandon the stage, the applause, and the world of flamenco that had been her entire lifs since the age of fifteen, when she sang for the first time at the Villamarta in a comedy featuring Lili Murati. A year later, Manolita would win a Saets contest in Radis Jerez. One thousand pesetas and a bouquet given to her personslly by the Mayor. Manolita de Jerez prefers to be called Manuala, because she says she is no longer a young girl. Since her retirement from the stage, she lives in a quiet home in the neighborhood of La Plata, in the company of her sister Juanits, who looks after her day and night. It is there where she meets me, surrounded by memories and anecdotes, listening to her transistor radio which fills her empty hours, listening to all of the good programs, and staying up to date on all of the new voices in flamenco that make her relive her glorious artistic past. \"In nineteen hundred and forty-eight I left Jerez with my first show, called then \"Flamenco Opere,\" which included Manolo Vallejo, Juan Varea, Carscollillo de Cái, who was a dancer, and the bailaora \"La Pilina\" among others. With Fsrina and Porrina\n\n[ENDING CONTEXT]\n\ndeed from thoughtless people. Nevertheless, she still has a few tapes of her records, and others recorded live, which she lets ma listen to in the intimacy of her sitting room, while Rafael Iglesias shoots photos. The voice of Manolits de Jerez surges forth like a miracle, -brillisnt, powerful, for s few moments, while we remember the great cantaora, who carried the name of Jerez throughout the entire world, and today completely forgotten, in this peaceful corner of La Plats. She forgets no one though, and speaks with enthusiasm of Terremoto, La Paquera, Tla Anica la Pirifeca, Tlo Borrico...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MANOLITA DE JEREZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 1526,
    "article_char_count_full": 8343,
    "article_char_count_review": 3517,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Manolita"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_SPRING::A8",
    "article_text_for_review": "[from: Diario de Jerez, January 11, 1986; sent by El Chileno; translated by Mary Sol West.] by Juan de la Plata In the last Fiesta de la Bulería there was a singer from Jerez, practically unknown to the new generation of aficionados, who stood head and shoulders above the other singers who, with varying degrees of success, performed that night. That cantaor, that great artist from Jerez, was none other than Fernando Gálvez, rescued, if only for a few hours, from the diaspora where he resides to give us a few hours, a few moments in which we could taste the classic school of Jerez in his pure cante. There are many other artists like Fernando Gálvez, who one day left Jerez looking for new and more ambitious horizons. They are the flamencos from Jerez in the diaspora. Fernando has been residing in Madrid since 1964. He was just 24 years old when he left Jerez. Fernando was born to a gypsy family in the Barrio de Santiago, in a house on Calle de la Sangre, August 4, 1940. There were no known artistic predecessors in his family, even though his grandfather and his uncles were very good aficionados. The great cantaor tells his beginnings like this:",
    "title": "FERNANDO GALVEZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "22",
    "page_number": 22,
    "word_count": 207,
    "article_char_count_full": 1160,
    "article_char_count_review": 1160,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SPRING::A9",
    "article_text_for_review": "[from: El Pais, April 21, 1985; sent by Brad Blanchard; translated by Paco Sevilla.]",
    "title": "JOSELERO DE MORON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "23",
    "page_number": 23,
    "word_count": 14,
    "article_char_count_full": 84,
    "article_char_count_review": 84,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
