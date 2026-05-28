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
    "article_id": "JALEO_1981_12::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nThree elderly women in shapeless black costumes potter about, cackling mirthlessly, exchanging raucous gossip and banter. One stands by a sink of stone and brick, up to her elbows in washing. The water is cold and there's no tap or pump. When she smiles you realize with a start that she is quite toothless, apart from a single brownish tusk protruding from a lower gum slightly to the left of center. Peña begins to work at the guitar strings and the little courtyard fills with the plangent melodies of a gypsy dance from Cádiz. The sun shines, a thin black cat hurries, frightened, from a doorway and escapes to the street, the old women stand and listen. and up at the open sky, and shrugs. \"It is a hard life,\" he concedes, \"but beautiful music came out of it.\" The conflict remains in the air\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nplangent melodies of a gypsy dance from Cádiz. The sun shines, a thin black cat hurries, frightened, from a doorway and escapes to the street, the old women stand and listen. and up at the open sky, and shrugs. \"It is a hard life,\" he concedes, \"but beautiful music came out of it.\" The conflict remains in the air unresolved. Affluence, he is suggesting, brings television and transistors and motorcycles. Flamenco thrives on poverty and simplicity Peña is 35, a tiny man with a pale, slightly sallow face and black hair which he wears severely parted and plastered close to the scalp. He is one of the world's leading flamenco guitarists, a man whose main ambition is to give his art a respectability it has not enjoyed before. He plays regularly in concert with John Williams, lectures annually at the famous international guitar seminar at Castres, and has appeared in the Royal Albert Hall with Victoria de los Angeles. Although he keeps a modest house in a narrow lane just behind the Flaza del Potro he has shifted his base to Kentish Town, whence he conducts concert tours around the world. There has been times when his efforts to become a truly international figure have had ludicrous results. Once, for instance, he found himself playing, together with Mary Hopkins, during Briti\n\n[ENDING CONTEXT]\n\nvery wild so I tried to organize my playing. I wanted to give it a classical dimension while I kept the feeling and the authenticity as well.\" He has succeeded to such an extent that he is now in demand world-wide, both alone and with his troupe, \"Flamenco Puro.\" He seems to have played everywhere, from Ronnie Scott's to the MacEwan Hall, Calgary, always promoting the concept of true, authentic flamenco. As he himself would admit, with pride, the best place to hear it is in its homeland. Córdoba comes behind Seville and Cádiz as a flamenco centre and yet it is in the very heart of Andalucía.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PACO PEÑA: FASINATING RHYTHMS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "3-5",
    "page_number": 3,
    "word_count": 1061,
    "article_char_count_full": 6131,
    "article_char_count_review": 2915,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_12::A2",
    "article_text_for_review": "by Paco Sevilla For the few aficionados who do not know of Paco Peña, we can say that he is the most widely known, popular, productive and respected flamenco guitarist who lives and works primarily outside of Spain. Born in Córdoba, where he still has a home and family, he has lived for many years in London, England, where he has performed widely, recorded extensively and been a source of inspiration for guitar students through his teaching and published guitar music. In America, he is known primarily through his records which, for many years, were the only decent records (aside from an occasional Sabicas album) that we could buy here -- although as far as I know, only a few of his many records were released widely in this country. His clean and very flamenco style of playing made him a favorite among aficionados and many a dance routine has been set to the music he recorded with his group of singers and dances -- students of dance enjoy Paco Peña because of his crisp compás and sensitive accompaniment. Without ever having met nor spoken with Paco Peña, I assume him to be a highly intelligent and imaginative person; these qualities are essential for someone who has undertaken such a wide variety of projects and made a worldwide name for himself with quality music and hard work, rather than through the use of gimmicks and phoney hype. His music shows these qualities also. Paco has what I call a \"styleless style,\" that is, while he has not developed a \"propio sello\" that is as strong and distinctive as a Sabicas, Diego del Gastor, Morao de Jerez, Paco Cepero, or Paco de Lucía, he has created a way of playing that is his and combines the best of many different styles. On an early record (1966) that Paco made with El Sali, his playing, like that of most guitarists, consisted of primarily falsetas by other guitarists -- Niño Ricardo, Serranito, Sabicas, etc. -- with the dominant sound being that of Sabicas. He still has a Sabicas-like sound -- clean, logical, \"cuadrao\" style playing -- but it has kept up with the times and been influenced by modern playing. A great many, perhaps a majority of Paco's falsetas can be traced to other guitarists or to specific cantes. Listening to a bulerías, for example, one can say, \"That one came from Sabicas, that one originated with Paco de Lucía, there's a Serranito with a Diego del Gastor ending...!\" If that is true, then what is so special about Paco's playing? It is what he does to and with those falsetas. First, he changes and develops the melodies until they become something new; he takes different parts and recombines them to create new progressions that are often superior to the original. Paco Peña has an incredible rhythmic sense and often his best ideas and real contributions are of a rhythmic nature, making great use of counter-time. For example, in a columbianas (\"Nuevo Día\") he will take a melody that Sabicas played with single picado notes, add bass notes and syncopation, and come up with a new and very musical composition; the same with the theme of \"El Emigrante\" (perhaps from a Niño Ricardo Version) in a rumba.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 544,
    "article_char_count_full": 3113,
    "article_char_count_review": 3113,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_12::A3",
    "article_text_for_review": "Dear Jaleo: I have often wanted to write to say how much I have enjoyed $ \\underline{\\text{Jaleo}} $ and that I have passed it on to my pupils. Two years ago, while in Spain and taking lessons at Amor de Dios #4, it was fun talking to other dancers who had come from Chicago, New York, etc., and were all members of $ \\underline{\\text{Jaleo}} $. I'm not much of a writer, but I was inspired by the article by Carola Goya, a dear friend -- as was Mateo, to whom I passed on a Latin American number he wanted (this goes way back). My brother, Emilio, was Carola's pianist on a world tour and, at that time, I was dancing in the \"El Chico\" night-club in New York and taking lessons so that I would be good enough to do concerts in the future with my brother. Eventually, that is what we did; from October to May every year for fifteen years we toured from New York to California, doing from 65 to 70 concerts a season. It was hard work doing one-night stands, but we were young and loved our work. Over the years my working partners and friends have included Carola, Vicente Escudero, Argentinita, Pilar López, Carlos Montoya, Manolo Vargas, José Greco, Carmen Amaya, and Antonio. So good to see Ernesto still dancing sevillanas. He was the first dance enthusiast I met when I came from New York to San Francisco to \"semi-retire.\" I'm still kicking.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "7",
    "page_number": 7,
    "word_count": 248,
    "article_char_count_full": 1346,
    "article_char_count_review": 1346,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_12::A4",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHis work has taken him from Ronnie Scott's Jazz Club to a concert performance of La Vida Breve with Victoria de los Angeles at the Royal Albert Hall. His shared recitals with John Williams have been a great success both in England and on the continent. In 1972 Paco Peña was the first flamenco musician to play in a Spanish Conservatory of Music. His recordings sell in many countries and he himself visits most of them in the course of a busy year, which includes many T.V. appearances all over the world. summed up in the Guardian's music critic's review after a London recital: \"Last night the Queen Elizabeth Hall was full to the brim of young, stalwart, cheering fans of Paco Peña, a guitarist to win fans if ever there was one, and a great artist.\" * * * JALEO: First of all, may I thank you\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"family\"]\n\n. summed up in the Guardian's music critic's review after a London recital: \"Last night the Queen Elizabeth Hall was full to the brim of young, stalwart, cheering fans of Paco Peña, a guitarist to win fans if ever there was one, and a great artist.\" * * * JALEO: First of all, may I thank you on behalf of all Jaleistas for agreeing to talk with me today. I'd like to begin by asking you about your early life in Córdoba. Do you come from a flamenco family? PACO PEÑA: Yes...I only have one brother; I have seven sisters, but only one brother, who is quite a bit older than I am, and played the guitar before I did. My father was a very good singer in his younger days, but he didn't want to be professional -- he didn't like the life; so in a way my family is a flamenco family, but not professional. My brother still plays the guitar but he never cared to do it professionally either, and I should say that I grew up with him and my sisters who all used to sing and dance. So it is a flamenco family in the sense that flamenco is the music of the people and it was what people did, so my family did it too. JALEO: How did your interest in the guitar start? PACO: From my brother Antonio; he had a guitar when I was about seven or eight and I started picking little tunes on his guitar. Hearing him and the radio is what started me. My brother helped me when he saw that I had interest; he showed me lots of things. Later I joined the \"Rondalla\" in my school. Rondalla is a musical group of children playing both folk music and light classical. There was a teacher there who showed me the basic chords and so on. I got very interested. I started to develop quite fast within that small world and I was, in fact, playing professionally when I was eleven or twelve years old. JALEO: When did you begin to develop the compás in your playing? PACO: Well, I joined a group when I was very young -- a government thing; they have groups all over Spain to help promote folk-art and traditions. On relevant occasions these\n\n[ENDING CONTEXT]\n\nelse excited too. So the thing got under way and by Christmas, Karin (Paco's wife) and I had all the written copy and information ready to be printed. I got a lot of enthusiasm from all angles and I got more and more excited about the idea. JALEO: Who did? JALEO: Who did you approach? PACO: The Ministry of Culture, who were JALEO: Have there been many applicants? PACO: There are seventy people registered to take part and some of them are repeating courses; all that amounts to about 100 people which is about as much as I want for the moment, because I will be doing it on my own this year.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PACO PEÑA: INTERVIEW FOR JALEO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "8-14",
    "page_number": 8,
    "word_count": 3256,
    "article_char_count_full": 17440,
    "article_char_count_review": 3636,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "family"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_12::A5",
    "article_text_for_review": "by Scott Davies",
    "title": "PACO PEÑA DISCOGRAPHY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 3,
    "article_char_count_full": 15,
    "article_char_count_review": 15,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
