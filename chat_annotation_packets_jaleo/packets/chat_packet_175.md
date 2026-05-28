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
    "article_id": "JALEO_1983_08::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLA MERI An Interview by Patricia Mahan La Meri, a world renowned ethnic dancer and recognized expert in the ethnic dance field for many decades, was born in 1896. She came to New York City in the early twenties and toured the world as a Spanish dancer before branching out to other areas of ethnic dance. She travelled extensively performing, and eventually studying with teachers who were famous in their field. She went on to write seven books over her area of expertise (particularly Spanish and classical East Indian dance). There is hardly an ethnic dancer in the field today who does not know her reputation. Throughout her long and fascinating career, she has been responsible for the excellent training and development of many well known Spanish dancers in this country. Artistic director of\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nfamous in their field. She went on to write seven books over her area of expertise (particularly Spanish and classical East Indian dance). There is hardly an ethnic dancer in the field today who does not know her reputation. Throughout her long and fascinating career, she has been responsible for the excellent training and development of many well known Spanish dancers in this country. Artistic director of her own company, Ethnic Dance Arts, for many years, she also collaborated with Ruth St. Denis in a school they founded in the early fifties in New York City. La Meri's thorough knowledge and expertise in Spanish dance enables her to elucidate on such topics as were discussed recently in an interview done at her home in Cape Cod, Massachusetts this summer. P: From your first hand experience, how has Spanish dance evolved since you began performing it on American concert stages? L M: Patricia, you're talking about 60 years ago. Spanish dances in the early twenties and thirties were of four primary groups: the regional dances, neoclassic, fla\n\n[ENDING CONTEXT]\n\na developing a theme or conceptual suggestion comparable to works of modern dance choreographers. This has been done to some degree by some notable Spanish dance companies, but could be far more developed, drawing from the rich source of Spanish literature, modern day themes, which could be just as valid a presentation if done with a sense of good judgment and artistry. I stand convinced Patricia that any art form stagnates if it doesn't evolve in some way, but we must always come from a respect for its tradition. TUESDAY THRU SATURDAY 3110 Newport Blvd., Newport Beach, CA 92663 (714)673-3440\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "TO VISIT HAWAII",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "14",
    "page_number": 14,
    "word_count": 1068,
    "article_char_count_full": 6293,
    "article_char_count_review": 2684,
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
  },
  {
    "article_id": "JALEO_1983_08::A8",
    "article_text_for_review": "John: Well, how else might I say it? Stephanie: Maybe, when did you become famous, or very well known? Mario: Famous, I don't know I am famous. (all laugh) John: Not really famous, what I'm wondering is when your teachers, when your elders, when did they really believe that you really had arrived, as we might say in this country. Mario: Oh, tc he a professional? John: Yes. Mario: That I can't say because, we think sometimes we are ready, but sometimes we are not. It is very hard to say. I thought I was ready to be a professional when I was 15 years old. I can't tell you if I am sure in a real way or not. But of course my first teacher was my father, and also Ramón Montoya; he was the patriarch of the (solo) flamenco guitar. Ramón Montoya is uncle to Carlos Montoya. He is very famous guitarist in this country. I think you know maybe. John: Oh yes. Mario: When I take lessons with Ramón Montoya, also my first knowledge in classical, I take lessons with Daniel Fortea. He was one of the last students from Francisco Tárrega. And of course in this time, when I take lessons Mr. Fortea, he was 70 or 75 years old. I mean I don't know how long he lived; probably he died many years ago. And I live more my young age I was in Madrid, and I know many many good teachers. I was in ambience constantly, in ambience of the guitar, and I know I have much knowledge, very good knowledge in classical. Even if I am not considered a classical guitarist, I think if I play something in classical, I am sure it's correct. John: Do you include classical works in your concert? Mario: Sometimes, but I try to do them separately from the program, because I want to play strictly flamenco concert. And I take the opportunity to show my knowledge in classical in the encores. John: I had a terribly astute question that I was going to ask, but it just left me. What was that? As far as what I was asking you before when you really have, as we might say in this country, arrived, the nice thing about your art, I think is, you never have really arrived. You're always growing, your art is always growing, is it not? I mean the concert you're going to do tonight is probably going to be better than a concert you did last week. Mario: Well, every time we try to do the best we can, right? But who knows. (laughs from both) John: But don't you feel that you are better this year as a guitarist than you were 5 years ago? Mario: Well, all the time. I think, every day it is possible to learn something new. Every day! John: Of course. You talked about your debut when you were 14. That was a rather auspicious debut, wasn't it? You appeared with Maurice Chavalier. Mario: Well, with Maurice Chavalier it was not really my debut. But I tell sometimes because a funny things happen; my father and mother, and my aunt, they play in the show with Mr. Chevalier who has a big company. Then I was in the room with my father and mother, and I tell to my father: I want to play in the stage. And my father says: \"Be Quiet, what is this?\" Then I start to cry, you know. Then Mr. Chevalier is coming from the corridor, and he saw me to cry, he say: \"What, what the boy is..?\" \"Oh, don't worry Mr. Chevalier, he wants to go stage.\" \"Why not?\" Then he take me by the hand and he introduce me to the stage. Of course I play one little song, and in this time I have short pants because that is very usual in European country in this time. Then this my first experience in the stage but it's really not professional yet, and I come later when I was 14 or 15 years old to be really professional. John: Where are you concertizing now besides the United States? Do you have dates throughout Europe?",
    "title": "THE FASCINATION OF",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 704,
    "article_char_count_full": 3667,
    "article_char_count_review": 3667,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_08::A9",
    "article_text_for_review": "$ \\overline{X} = \\text{Each tick of the metronome (to keep the arpeggio even)} $",
    "title": "FLAMENCO EL OIDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "16",
    "page_number": 16,
    "word_count": 14,
    "article_char_count_full": 80,
    "article_char_count_review": 80,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_08::A10",
    "article_text_for_review": "If these examples are practiced on a regular basis, incorporating the suggestions mentioned earlier, (finger curling forcefully into the palm, pull from the knuckle, etc.), you will discover your arpeggios improving tremendously. By practicing all the time with a metronome, the arpeggios becomes round and even. As usual, practice slowly at first, and then, when you can hit all the notes clearly and smoothly, in correct time with the metronome, gradually increase the speed. This also applies when tackling the arpeggio section of your concert pisce. Increase the speed only when you are confident and comfortable at a faster tempo. Don't be afraid to push yourself, etc. The whole purpose of these articles is to suggest a way of developing strong, powerful technique, so the guitar will be easier to play. One of the greatest joys is to be able to pick up a concert piece by Sabicas, Paco de Lucía, or any of the great virtuosos and be able to play it, without having to sweat, strain and struggle so much, because of weak, undeveloped technique and potential. You don't get discouraged as often and can feel the composer's inspiration coming to you. You also begin to understand their styles and how much these great musician-composers have given, through their music, to the art of flamenco and the world. If you wish to contribute something additional, helpful, etc., pro or con, please respond to this article. That's how we all learn and benefit. --Ken Sanders WE APPRECIATE OUR ADVERTISERS PLEASE PATRONIZE THEM The Blue Guitar Workshop Bouzy Rouge Cafe Rubina Carmona Chula Vista Travel Antonio David - Supreme Strings Lester DeVoe - Flamenco Supreme Strings Tom Sandler - The Frame Station Guitar Review JALEO THANKS THE FOLLOWING CONTRIBUTORS: Katina Vrinos - Donation Dennis Hannon - Donation Dan Dibona - Donation \"The Shaw if Iran\" Los Angeles - Donation - Donation - Gift Subscription Lana Yonaguni - Gift Subscription Kim Oberdick MORCA ... sobre el baile FLAMENCO IN CONCERT The down cycle of the popularity of Spanish dance has often been blamed on flamenco. Either too much flamenco in a concert or not enough classicals, regional or other form of Spanish dance has been stated over and over as the reason for the demise of Spanish dance as a popular art form, an art deserving its past fame in today's age of dance, an age which in toto has been called \"the dance explosion.\" Almost every article that I have read and discussions that I have had covering a myriad of opinions on the subject have usually been on a negative note, directly or indirectly blaming some facet of flamenco for its own destruction and the destruction of Spanish dance in general as a popular concert attraction. Recently I read a quote in a magazine from a quote in another magazine that said: \"Some of the century's greatest Spanish dance performers have attributed the recent decline in popularity of their dance form to poor teaching and bad technical performances that emphasized only flamenco while ignoring classical and regional dances.\" This article is not so much a rebuttal to this often brought-up subject as it is another opinion. I am a very positive thinker when it comes to flamenco, as I am with every other art form and all facets of the arts that the human races express as a creative part of themselves. I am not positive just to be positive, but to also state some facts that can be thought over without a sense of simplistic, flippant emotionality. Flamenco is not responsible for the demise of Spanish dance as a popular art form. There are good teachers and bad teachers in every art form, yet other arts go on in their natural cycles of popularity. Flamenco is high art, a complete art form that can stand on its own two feet and can easily fill a concert on its own if it meets the \"basics\" of having, good talent, good programming, good interpretation, good professionalism, good staging, costumes, etc. Does a symphony orchestra have to play jazz or other styles of music to rake a full, varied and popular performance? No! Does a Chinese restaurant have to serve Mexican food to be popular? No! Does a ballet company have to perform tap, modern or flamenco to have a fulfilling evening of dance? Again, a big no! Each facet of art, and that goes for restaurants too, is unique and complete on its own. If someone wants to do a varied evening of all types of Spanish music and dance, fine! But classical Spanish is one thing, regional dances another, flamenco another, theatre composition another. They are each an artistic expression on their own and do not necessarily need the support and variety of the other if they meet the basics which I mentioned previously. We are talking art, not politics, and there is so much variety in the art of Andalucía that, if done with sensitivity, would fill many evenings of concert without needing dances of other regions. We must realize that flamenco is expressing flamenco, not all of Spain. There is a public for good talent and exciting art forms and flamenco is one of them. It has never been responsible for the demise of the popularity of Spanish dance. As I mentioned before, there are good and bad teachers in every art form. Yet if the public is in a cycle of that art form and wants it to be popular, they will find the good teachers, the good artists, the professionals with class, integrity, talent, showmanship, promotion, etc. There are many great artists who became famous after death. They lived in poverty and lacked recognition because the public was not ready for them. Sponsors who buy talent and basically represent the public in dance, music and other arts, look for what sells and they look to the public for evidence of popularity.",
    "title": "LA MERI GASPACHO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 986,
    "article_char_count_full": 5723,
    "article_char_count_review": 5723,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_08::A11",
    "article_text_for_review": "Complete sets with black or clear trebles Retail $11.00 SPECIAL OFFER $6.00 Minimum order $12.00 Postage Paid California residents add 6.5% sales tax Make checks to Lester DeVoe - Guitar Maker Box AA, San Jose, CA 95151 Offer expires January 31, 1984",
    "title": "DE GUILLERMO GUITAR:",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_08",
    "year": 1983,
    "language": "en",
    "article_type": "article",
    "pages": "18",
    "page_number": 18,
    "word_count": 42,
    "article_char_count_full": 250,
    "article_char_count_review": 250,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
