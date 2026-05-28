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
    "article_id": "JALEO_1980_03::A2",
    "article_text_for_review": "Dear Jaleo: I hope that you print this letter, because I feel strongly that those who give so much should be recognized, and I truly thank you for finally having Juana De Alva on the cover and featured in an article. I write these notes, not only because I am privileged to have her as a friend, but because Juana is one of those rare people in the arts who has paid her dues, kept her humility, and has given to others as much as she takes from life. She is a beautiful artist, not only of flamenco, but of dance, period! Whatever she pursues, I am sure that she will continue to inspire others, and her students, I am sure, will be inspired and grow from her giving art. I salute you, Juana, and thank you for your friendship, your giving kindness, and your art. The world of flamenco is richer with people like you in it. Abrazos, Teodoro Morca Dear San Diegans: Have been in Spain for seven months now, living and playing tablao flamenco in Torre-molinos near Málaga. I'm learning a lot about flamenco. I also worked picking olives and lived with gypsies in the caves of Guadix near Granada. The best flamenco I have seen was in Sevilla. It is getting cold in Andalucía, so I am heading south to the Canary Islands. Give my regards to all who remember. Charlie Blankenship \"Don Carlos de San Diego\" LETTERS TO THE EDITOR ARE ALWAYS WELCOME.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 248,
    "article_char_count_full": 1344,
    "article_char_count_review": 1344,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_03::A3",
    "article_text_for_review": "MOSAIC OF COMMENTS IN HONOR OF FIFTY YEARS (From: $ \\underline{\\text{Nueva}} $ $ \\underline{\\text{Andalucía}} $, Sept 11, 1979, sent by Roberto Reyes and La Vikinga, translated by Paco Sevilla) Manuel Ríos Ruiz: \"Antonio Mairena has always been ready to go after prestige and recognition for flamenco. It is something he has achieved, through the force of his will and his wisdom as a cantaor, at the same time that he set himself up as the most complete and masterful cantaor of his time, dominator of all existing styles, many of them saved from extinction by his brilliant and intelligent re-creations.\" Ricardo Molina: \"In a period when authentic flamenco was unknown or given little value, Antonio Mairena remained fervently true to the gypsy tradition. His attitude was transcendental for the history of the cante. To Mairena, more than any other cantaor, is due the salvation and diffusion of the glorious legacy. For this, his name is worshipped and revered by many small groups throughout the world and he is considered the greatest cantaor of our time.\" Camacho Galindo: \"Antonio Mairena is, in my judgement, one of the contemporary cantaores best suited to the task of revolutionizing, or at least, bringing about the evolution of the cante. Even more, I believe that without him intending to and even against his will (perhaps due to his reverential respect for the tradition of his race) he has created his own cantes that he, for the reasons just mentioned, makes appear to be originals of his famous predecessors and teachers.\" Antonio Mirena: \"I consider myself to be the greatest transmitter of all the geniuses, past and present, and I believe that if it weren't for Antonio Mairena much of our musical wealth that was known only by name would have disappeared; now we have it recorded for history and the future. Consequences of Mairenismo: A new way of singing, of developing the cante, the hope that a new era will arise, an era of authenticity, purity, absence of the artificial in the presense of only the purely essential.\" Emilio Jiménez Díaz: \"Gypsies -- they will always live in misfortune -- the voice of Antonio Mairena is the voice of liberation, of culture, and of sustenance. His is the \"Key\" and his is his mastery. Fifty years of cante, \"grito\", and roots are many years of teaching efforts, many years of being in touch with the duendes, and centuries, authentic centuries of knowledge.\" Antonio Fernández Díaz \"Fosforito\": \"Antonio Maierena is the greatest cantaor in the history of flamenco.\" \"In the grand design of the cantaores, Mairena is the vertebral column upon which the errant flamencology depends and we owe him for no less than twenty modalities (flamenco song styles) that were lost and then re-covered by him.\" Alfredo Arrebel: \"He is Antonio Mairena, 'Maestro Primero', because he has known how to bring the cante from a shameful, almost embarrassing position to an artistic category accessible to the highest levels of culture. The cante of Mairena is not pretty, nor cute, nor artistic. Mairena's cante is tragedy and magic, vibrant joy and solemn rebellion in the face of destiny.\" Luis Nelgar: \"There has not been a cante that resisted him because Mairena has been the cante itself. His immense power and profound and constant vigilance have caused him to search and examine carefully all corners of Andalucía\"",
    "title": "ANTONIO MAIRENA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 560,
    "article_char_count_full": 3365,
    "article_char_count_review": 3365,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_03::A4",
    "article_text_for_review": "(From: Nueva Andalucía, Sept 11, 1979; sent by Roberto Reyes and La Vikinga; translated by Roberto Vázquez) Nueva Andalucía, through its weekly \"Rincón Flamenco\", wishes to join with complete enthusiasm and interest, with its best \"palmas a compás\", the deserved homage that this year all the flamenco geography of the world dedicates to a man who has given fifty years of his life to the cante in a daily teaching, and who has lived in this half century -- almost a whole life -- only for and by the cante, dignifying it to the maximum in each one of its branches. Antonio Cruz García de Mairena, this \"rey de la tribu de la yerbabuena\", as Antonio Murciano defined him, receives in this singular dedication a profound homage, despite the fact that in its intimacy the best homage that life has given him is the quiet homage that he receives daily when he hears in other throats and with other \"ecos\" a great part of his splendid legacy, watching, every dawn, how many of the singers who make in our days the history of the cante, breathe his efforts through all the pores of their bodies. For me it is much more difficult to have to say something, even though it might be just a little about a man to whom, from the first contact -- without knowing him yet personally -- I was united by the bonds of the land, the imperceptible pull -- more than imperceptible, strangely deep and imprecise -- impressionably pure, that returned me anew, in a music that was said to be of the Andalucian people but that no Andaluz has assimilated, to the ancestral roots, to the first inexact contours -- although immensely vast -- of the interior landscape of my own being.",
    "title": "OUR BEST \"PALMAS A COMPAS\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 298,
    "article_char_count_full": 1658,
    "article_char_count_review": 1658,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_03::A5",
    "article_text_for_review": "Translated by Brad Blanchard From the Sept. 2 edition of the daily paper Córdoba, we are reproducing this article by our dear friend and companion Ángel Marín Rújula, inserted in the flamenco page \"Arte, Genio y Duende\": There are various superlatives applied to Antonio Cruz García: Maestro Mairena, pontiff of the cante, maestro of the alcores (\"hills\", referring also to his home town of Mairena de Alcor), etc. I would give him the qualifying title of \"Immortal Teacher\". Antonio Mairena is a solemnly noble man with exquisite manners -- naturally -- in his relations, that gives us an impression of timidity in spite of the fact that he represents flamenco's greatest living work of art. I think that without Mairena, the cante would not occupy the social and cultural stratum that it now does. He, with his uncontainable spirit, his gypsy sensitivity, his portentious voice and his overwhelming expressiveness, has known how to dedicate his entire life to continuing investigation and continual sacrifice in order to appraise in just measure the so important legacy of our ancestors, selecting and assimilating its artistic richness in order to offer us the full magnitude of this inheritance. He has known how to recover and restore many styles that have been ignored, with his own interpretation imposing a demanding standard of correctness on the important list of professional interpreters of the cante. Together with our beloved poet, Ricardo Molina, he put together the work $ \\underline{\\text{Mundo y Formas del Cante Flamenco}} $, a fundamental classic for every student and aficionado of our art. I think that his primary discographic work is \"La Gran Historia del Cante Gitano Andaluz\", although very important are \"Cien Años de Cante Gitano\", \"La Llave de Oro del Cante Flamenco\", \"Honores a la Niña de los Peines\", \"La Fragua de los Mairena\", \"Recuerdos de Manuel Torre\", \"Cante de Antonio Mairena\", \"Noches de la Alameda\", \"Duendes del Cante de Triana\", and a great number of other volumes that would be out of place to relate here,",
    "title": "AN IMMORTAL TEACHER",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "7",
    "page_number": 7,
    "word_count": 335,
    "article_char_count_full": 2051,
    "article_char_count_review": 2051,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_03::A6",
    "article_text_for_review": "(From: $ \\underline{\\text{Nueva}} $ $ \\underline{\\text{Andalucía}} $, Sept. 11, 1979; sent by Roberto Reyes and La Vikinga; translated by Carolyn Tamburo) In the last edition of the Festival de Alcalá, I dared to request a few words from the great cantaor from Puente Genil, Antonio Fernandez Diaz \"Fosforito\", so that they might be presented here in this special issue of Nueva Andalucía. Fosforito, a gentleman, on as well as off stage, was enthusiastic about the idea and as proof of it, here are his thoughts, set forth with his usual sincerity, because perhaps no one else knows so well nor so much about the maestro from \"Los Alcores\" (Antonio Mairena) as does the maestro from Puente Genil. And perhaps no one esteems Antonio Mairena as much as he does. Our deepest thanks to Fosforito for having submitted the following, which he has entitled \"Example to Follow\". It is a lasting payment of homage, both of friendship and affection, from one master to another: sure for me to speak of this unparalleled genius whom I love and admire so much. \"When it was suggested to me that I write about Antonio Mairena on the occasion of his more than 50 years of confronting all types of cante \"por derecho\" (in the correct manner), I was delighted because it is a plea- \"Antonio Mairena sets an example for us as a firm and immovable pillar. Fixed in his ideas and tireless labor of research, he salvages and often re-creates cantes, thus, molding and enriching our cultural repertoire of flamenco. Before such a stance, Don Antonio, I cannot help but discover myself and extend to you my most sincere admiration.",
    "title": "THE HONEST WORDS OF FOSFORITO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "8",
    "page_number": 8,
    "word_count": 277,
    "article_char_count_full": 1610,
    "article_char_count_review": 1610,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
