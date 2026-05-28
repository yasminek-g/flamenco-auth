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
    "article_id": "JALEO_1984_08::A5",
    "article_text_for_review": "SPONTANEITY, IMPROVISATION AND ZEN As children we were spontaneous. We were able to be ourselves, improvising in play and make-believe. We showed honest emotion and genuine delight in discovering ourselves, our bodies, as they learned to move, our minds and feelings as our emotions awakened in laughter and crying. Our \"mental cup\" was empty, ready and eager to accept new experiences, our \"beginners mind,\" ready to learn and become aware of who we and what we feel. Honesty, spontaneity, freshness, inspiration and yes, enlightenment, is returning to our empty cup--our \"beginners mind\"--as it is called in zen. Zen and flamenco are really one without trying. The reason that I mention zen is that the essence of both zen and flamenco springs from the same roots. From both zen and flamenco spring, not a philosophy, doctrine, rules or stone walls but spontaneity of each moment. Each breath is a way of just becoming ourselves, becoming life, so that our dance can dance, so that our breath can breathe, so that our feelings can feel, so that our enlightenment of art is art, so that our individuality is unique yet part of the whole, like a river blending with the sea. Zen is basically enlightenment of who we are. We are we, dance is dance, life is life--no deep philosophy, just moment by moment, living each day full with spontaneity and calm. Our oneness is oneness with all--no separation or duality of physical and mental--just being. There have been many books and articles written about zen and its relationship to creative endeavor (motorcycle repair, archery, martial arts, jogging, tennis, golf, etc.). These basically are paths to know thyself and in reality you can use any creative discipline to become at one with yourself, your total self.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "13",
    "page_number": 13,
    "word_count": 294,
    "article_char_count_full": 1761,
    "article_char_count_review": 1761,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A6",
    "article_text_for_review": "(from: $ \\underline{\\text{The New York Times}} $, Monday, August 20, 1984) by Jennifer Dunning",
    "title": "FLAMENCO PROGRAM AT PUBLIC's",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 14,
    "article_char_count_full": 94,
    "article_char_count_review": 94,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A7",
    "article_text_for_review": "Most exciting was El Güito, a leading flamenco dancer. Gifted with extraordinary articulation and command of the heel and toe in his foot beats, El Güito even more impressively has a stage presence that is both unassuming and mesmerizing. He has the gift, too, of stillness, so that the quick shifts in dynamics that are so exciting a part of flamenco were even quicker and more surprising here. There is a kind of roughness to his dancing that is refreshing after the slick pizzazz that one so often encounters on the Spanish dance circuit. And his great physical control did not rob El Güito of warmth. To watch him working so closely with his guitarists and singer, uncredited in the program but superb, was one of the evening's most memorable qualities. At the end, El Güito summoned the two guitarists for brief solos of their own, the older one dancing a timid but ENRIQUE MORE NTE artists in el baile. The interest and debates that are stirred up by each new contribution of Morente has gone beyond that of the strictly flamenco, since he is also acclaimed as a singer and composer by directors of film and stage, including such prestigious figures as Marsillach, Borau, and José Luis Gómez. He has received some of the most important awards, among them the National Prize for Cante awarded by the Cátedra de Flamencology de Jerez in 1975, and the National Prize for a recording, by the Ministry of Culture in 1978. He has travelled the world with his cante, performing on such famous stages as those of the Olympia in Paris and Lincoln Center in New York. His outstanding records include: - \"Cantes antiguos de flamenco\"-HISPAVOX S 20049 -\"Homenaje flamenco a Miguel Hernández\"-HISPAVOX CLAVE 18-12516 -\"Se hace camino al andar\"-HISPAVOX CLAVE 18-13425 -\"Homenaje a Don Antonio Chacón\"-HISPAVOX 18-24791/2 - \"Despegando\"-C.B.S. S52868 -\"Sacromonte\"-Zafiro ZL-552 -\"Cruz y Luna\"-Zafiro ZL-594 El Güito was born in Madrid. Since he was a child, this gypsy has danced in movies, theaters and tablaos. He was part of the company of Pilar López in those years that included other young dancers--Antonio Gades, Mario Maya, etc--who would later become representative artists in the baile español. Considered from the beginning as a master of the genuine essences of the baile flamenco, he was still quite young when he was awarded the \"Premio de las Naciones\" in Paris in competition with some of the best dancers in the world. El Güito has performed in many countries with his own company and was a guest artist with the Ballet Nacional Español and the Ballet Español de Madrid. His creation \"por solea\" is one of the important milestones to be achieved in the baile.",
    "title": "FESTIVAL LATINO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 450,
    "article_char_count_full": 2669,
    "article_char_count_review": 2669,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A8",
    "article_text_for_review": "[from: El País, July 22, 1984; submitted by Brad Blanchard; translated by Paco Sevilla]",
    "title": "MANUELA VARGAS: THE FOAL WHO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 14,
    "article_char_count_full": 87,
    "article_char_count_review": 87,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A9",
    "article_text_for_review": "\"I began to dance when I was five years old. At eleven, in Sevilla, I danced in private fiestas. Until I was sixteen, I never went to a dance school. I danced without training because we had no money. The first nightclub where I danced was El Guajiro. I began there with an extraordinary group, including El Farruco, Cristina Hoyos, Fosforito, Lebrijano, and Chocolate. I was one of those 'chiquillas' that arise in Sevilla spontaneously and you could say that I was born dancing, as if I were a young foal. I was twelve years old when a man named Roberto Nobles, of the El Clarín, came from Buenos Aires and was given an homanaje here. He came to the tablao and gave us about 12,000 pesetas, and from then on I was able to go to the school of Enrique el Cojo, my only teacher. Now, in ballet, I have teachers, but before, never. \"In 1963 I was finally able to form my own company. That year, José Monleón set up a show for me in The Theater of Nations in Paris. I was there among ten other stars. Based on that show, I had a great success. They gave me an award that could have gone to any of them because we were all very good artists, but I received it. Later, London, Paris, Italy, New York... I never stopped.\" MANUELA VARGAS ( ) \"Did I have to give up much? A great deal, a very great deal! When I went to New York, the William Morris agency contracted me for eleven years. I wanted to marry and have children, like all women. So I married in 1970 and left dancing. I was retired for six years. I had twin girls, but realized that children didn't fulfill me completely. I left my husband and dedicated myself to co-زاتف وکف وکفات. I was a large, rather plain looking girl and they laughed at me. This you never forget, because of the age at whirh it happened. \"What I want to do now is to create an authentic Andalucian ballet, one that would be purely flamenco and \"salvaje\" [wild, primitive]. I will not die without seeing a first class performance of the art of Andalucía.\" Flamenco Guitar For Sale 1932 Santos Hernandez Excellent Condition $3,500 Tel. 408/733-1115 FLAMENCO FENCING ORNAMENTAL IRON SAFEGUARD FENCE CO. SERVING NORTH COUNTY 619/745-4846 CA Contractor's Licence #374198",
    "title": "BECAME A BAILAORA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "16,26",
    "page_number": 16,
    "word_count": 402,
    "article_char_count_full": 2193,
    "article_char_count_review": 2193,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
