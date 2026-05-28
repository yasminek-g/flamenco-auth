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
    "article_id": "JALEO_1978_01::A1",
    "article_text_for_review": "When flamenco is at its best, a guitarist and singer working together as one, or an entire cuadro of dancers, singers, guitarists, and jaleadores joined together in creating a work of art, there is sometimes a special mood or spirit present that flamencos call \"duende.\" It is this moment when the artists and the audience become one with the spirit of the music, that flamenco artists and aficionados constantly strive for. The arrival of this moment is awaited by everyone at the juerga, but it doesn't always come. There are countless stories of juergas in Spain that never got off the ground. A guest artist might never feel the \"mood\" or perhaps not care for the atmosphere and, therefore, not perform at all or only half-heartedly. In the tablaos, the true professionals always put on a decent show, but those magic moments when they are truly inspired to do their best occur only rarely. I don't claim to be an authority on the subject of duende, but would like to offer some observations and suggestions to both flamencos and aficionados, with special reference to our local juergas. A personal experience lead me to start thinking about the conditions that increase the chances of a little duende making its appearance at a juerga. A year ago, several of us from San Diego attended a gathering, in Minneapolis, of flamencos from all around the country. The purpose was a week of study and juerga. Most activities took place outdoors and included classes, performances, and informal gatherings. For one week the juerga never really got off the ground as far as developing spirit or duende. Then, late the last day, after many people had gone home and the last fifteen or twenty of us were packing cars and cleaning up the cabin, guitars began playing at two different locations, people were attracted, and soon all were inside the cabin where dancing had begun. It was crowded and hot in the room; people were pressed body against body, and the air was filled with smoke. Soon a real juerga was in progress, and for a couple of hours there was a great deal of spirit and good flamenco. What was there in these last moments that had been lacking all week when we really needed it? The following conditions for a successful juerga are suggested by this example: DUENDE- continued Good juerga performance always appears where it is least expected and cannot be planned or demand-ed. All that can be done is to set up the best possible conditions and then hope something happens. It is for this reason that the aficionado must have a great deal of patience; it may require several hours for expectations to die out and allow spontaneity to arise. Therefore, good flamenco will most often occur late in the juerga, as it usually does in a typical Spanish juerga where the really \"heavy\" cante may not happen until the early hours of the morning. At the gathering in Minneapolis, after a week of frustration, suddenly the proper conditions appeared; there were sufficient artists, including a singer, and all were relaxed due to fatigue or drinking beer; we were indoors for essentially the first time all week (there were too many people earlier in the week) and crowded together; most important, there were no expectations since we all thought it was over and we were going home. Then it just happened. To apply these principles to our juergas in San Diego (or anywhere else) it is necessary to set up the following conditions: 1. Whenever possible hold juergas indoors. 2. Start early enough so that people have time to finish eating without being rushed. 3. Encourage as many artist as possible to attend and seek out new artists--- there is nothing like a new performer to spark an evening. 4. Encourage participants to reduce their expectations. This is especially important for aficionados and non-flamencos who are attending juergas with the hope of seeing some good flamenco. They must realize that no two juergas will be alike. If a person enjoys a juerga and then tells his friends about it and brings them to the next one, they are all likely to be disappointed. Each juerga will be different and perhaps only one out of three or four will have a high level of flamenco, and then it may not develop until very late in the evening; a lot of the good flamenco at our juergas happens after midnight when most people have gone home. This freedom from expectations means that each person must come to the juerga prepared to either leave if it looks like nothing is going to develop, or else enjoy some other aspect of the party until the mood changes or the evening comes to an end. Hopefully, with most of these conditions met, we will have lots of good flamenco at our juergas and maybe, from time to time, a little \"duende.\"",
    "title": "DUENDE AND THE JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2",
    "page_number": 1,
    "word_count": 826,
    "article_char_count_full": 4736,
    "article_char_count_review": 4736,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_01::A2",
    "article_text_for_review": "Dear JALEO, I want to express my happiness at the pleasure I've received from the newsletter. I look forward to it all month and read it avidly from end to end. And of course, the juergas and other club activities are also a lot of fun and a very positive part of my life. Thank you all so very much for your efforts, especially Juana De Alva, who must be recognized as the driving force behind Jaleistas May you all have a wonderful Christmas and a super 1978. A grateful aficionado, Jess Nieto P.S. My renewal check is enclosed.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 3",
    "page_number": 3,
    "word_count": 99,
    "article_char_count_full": 530,
    "article_char_count_review": 530,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_01::A3",
    "article_text_for_review": "This is the first of what is hoped to be an irregularly appearing series dealing with Americans who have found success working as flamencos in Spain. — by Paco Sevilla \"While a group of silent people listen, the flamenco singer's wailing, piercing voice bounces off the walls of a dimly lit tavern. Listeners sip glasses of wine and utter occasional approving 'Oles.' \"Sitting behind the singer, the guitarist's fin- gers dance up and down the instrument, in ti-entos, seguiriyas, and soleares. A typical Andalusian flamenco party? \"Not quite. The guitarist is a Californian from San Diego. \"Rodrigo de San Diego has spent the last few years of his career proving what many die-hard flamenco aficionados refuse to accept: that a foreigner, armed with patience, a will to learn, and talent, can master the art of flamenco guitar.\" This quote from the Iberian Daily Sun (Dec. 16, 1976), introduces Rodney Lee Hollman, a guitarist now in his late twenties and living in the Málaga area of Spain. Rod became interested in the guitar at an early age and played many styles including rock and roll. He began to study flamenco during his early teens with various instructors in the San Diego area, one of whom reports that he was a very quick learner and learned as much on his own as from lessons. It is significant that at a time when Sabicas and Mario Escudero were the reigning kings of the flamenco guitar, Rod was attracted to the styles of Melchor de Marchena and Los Moráo de Jerez (Manuel and Juan Moreno) and began to imitate and learn from their playing. He feels that these guitarists taught him the \"Whys\" of flamenco. After a detour into the world of rock and roll, Rod returned to flamenco and had the opportunity to go to Spain as teacher and interpreter for one of his guitar students. After a brief stay in Granada, they ended up living in Ronda, a city which, although not known for its flamenco, does lie deep in flamenco country. Their house in Ronda quickly became a gathering place for local flamencos and before long, Rodrigo was playing for singers in local events including a flamenco mass. During the next couple of years he played for many singers, including the now very famous Turronero, in fiestas and private juergas. He also met and married Remedio, a native of Ronda of gypsy background who has a beautiful singing voice. After the birth of his daughter, Raquel, Rodrigo brought his family to San Diego for a visit with his parents. Finding the flamenco scene discouraging, a year later he was back in Spain, living on the Costa del Sol in the Málaga area. A-gain in the Iberian Daily Sun (Dec., 1976) we find that, \"Decisive in furthering his career was meeting well-known flamenco singer Curro del Lucena at a flamenco party. Curro, impressed by Rod's talent, invited him to accompany his singing at several flamenco festivals (normally off limits to foreign performers).\" The newspaper Sur (July 1976), reports one such festival in its flamenco section--Rodrigo de San Diego participated in the IV Noche Flamenco del Campo Andaluz in Lucena with Pepe Sanlúcar, Juan Carmona Habicuela, Pepe Sancristán, Bení de Cádiz, El Chozas, and Curro Lucena, among others. Soon after, Rodrigo accompanied Curro on a recording for Belter Records. The event was announced in the newspaper Sol de España in an article titled, \"Por primavera vez en España--un norteamericano acompañara a la guitarra en una grabación.\" The article stated that in this recording with Curro Lucena,... \"for the first time in Spain, a flamenco singer will be accompanied by an American guitarist.\" The record which contains bulerías, tangos, bulerías por solea, siguiriya, three styles of fandangos, la caña, jabera, serranas, malagueña de Chacón, and marinas, is said to have some innovations in the tangos, bulerías and caña.",
    "title": "RODRIGO de SAN DIEGO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 4 ",
    "page_number": 3,
    "word_count": 644,
    "article_char_count_full": 3820,
    "article_char_count_review": 3820,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_01::A4",
    "article_text_for_review": "The following is selected from a transcription of the siguiriya played by a leading flamenco recording artist. A future issue of $ \\underline{\\text{JALEO}} $ will contain a collection of siguiriya falsetas as played by Diego del Gastor. About Peter Baime Peter Baime is a resident of Milwaukee, Wisconsin where as a guitarist he is involved in many projects, including solo concerts, accompanying dancers, and working on federally funded projects. He is currently looking for a professional dancer who would like to work with him in Milwaukee (see want ads). Drawing on his wide knowledge of the flamenco guitar, both old and new styles (his familiarity with the Moron style of playing comes from study with Diego del Gastor in Morón de la Frontera), he has transcribed a substantial amount of music by most of the top flamenco guitarists. This material, some of which is written in standard notation, and some in cifra or tablature, is offered for sale, and a catalogue may be obtained by writing to: Peter Baime, 1030 W. River Park Lane, Milwaukee, Wisconsin, 53209. RECORDS DONATED On behalf of Jaleistas we'd like to express our most sincere appreciation to Robert DeVore for his contribution of a collection of flamenco records to the San Diego Flamenco Association. Among the records are albums of solo guitar music by Sabicas Carlos Montoya, and Bernabé de Morón, plus some hard-to-find records of Ramón Montoya and others. There are also a number of 78 rpm discs with singing accompaniment by Niño Sabicas, Miguel Borrull, Jerónimo Villarino, and Ramón Montoya. These records will be placed, along with other collections, in a location where they can be enjoyed by all interested parties. Details will be forthcoming in future issues of $ \\underline{\\text{Jaleo}} $.",
    "title": "Transcribed by Peter Baime Seguiriya",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 289,
    "article_char_count_full": 1774,
    "article_char_count_review": 1774,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_01::A5",
    "article_text_for_review": "On behalf of Jaleistas we'd like to express our most sincere appreciation to Robert DeVore for his contribution of a collection of flamenco records to the San Diego Flamenco Association. Among the records are albums of solo guitar music by Sabicas Carlos Montoya, and Bernabé de Morón, plus some hard-to-find records of Ramón Montoya and others. There are also a number of 78 rpm discs with singing accompaniment by Niño Sabicas, Miguel Borrull, Jerónimo Villarino, and Ramón Montoya. These records will be placed, along with other collections, in a location where they can be enjoyed by all interested parties. Details will be forthcoming in future issues of $ \\underline{\\text{Jaleo}} $.",
    "title": "RECORDS DONATED",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 110,
    "article_char_count_full": 689,
    "article_char_count_review": 689,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
