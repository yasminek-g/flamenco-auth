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
    "article_id": "JALEO_1978_06::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby J.M. Caballero Ronald translated by Tony Pickslay (Appeared originally in the Spanish magazine, $ \\underline{\\text{Cartas de Espana}} $, in 1977) Flamenco, which has always been a sort of undirected protest, has lately begun to search for direction. What I mean is that something important has been changing, in a general way, in the ideological roots and the social ramifications of flamenco. If one considers the remote and enigmatic historical development of the cante, there is no doubt that this change is practically an unexpected phenomenon and therefore worthy of the most objective examination possible. It is hardly necessary to remind ourselves that the trajectory of flamenco, in its most genuine social aspect, has been an intimate one of a people under long and tenacious\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"famili\"]\n\ndevelopment of the cante, there is no doubt that this change is practically an unexpected phenomenon and therefore worthy of the most objective examination possible. It is hardly necessary to remind ourselves that the trajectory of flamenco, in its most genuine social aspect, has been an intimate one of a people under long and tenacious subjugation. Those distant creators of flamenco, who were without exception members of a few Andalusian gypsy families, converted the cante into a comunal heartrending manner of expressing their dramatic intimacy. None of these original cantaores narrated anything that did not spring from a pathetic collective experience, full of hunger, persecution, jails, and death. The basis of flamenco, viewed as a system to communicate the tragedies of an outcast people, becomes a type of private ceremony or secret rite rarely found outside the gypsy family environment. Little by little, that hidden, clandes-tine, aspect of what we might call prehistoric flamenco, is going to start losing\n\n[ENDING CONTEXT]\n\nMembership-Subscription is $8.00 per individual and $10.00 per family or couple. Announcements are free of charge to members and businesses may display their cards for $6.00 per month or $15.00 per quarter. JALEO is published 12 times yearly by Jaleistas, the Flamenco Association of San Diego. © 1978, by Jaleistas, all rights reserved. 0 WELCOME TO JALEISTAS - NEW MEMBERS Carol Whitney (Canada), Jerry Lobdill (Tex.) Nilo Margoni (Downey, CA), and from San Diego: Karen Anderson, Pat Hurd, Juliana Hicks, Pat, Mark, Christine, & Scott Cummins Don, Tom, & Raquel Latham, and Petra Hernandez.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FLAMENCO and its NEW AUDIENCES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_06",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2, 3",
    "page_number": 1,
    "word_count": 1024,
    "article_char_count_full": 6364,
    "article_char_count_review": 2636,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "famili"
      }
    ]
  },
  {
    "article_id": "JALEO_1978_06::A2",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby PACO SEVILLA This article does not pretend to be a definitive biography of Paco de Lucía. The author has met Paco, but does not know him personally. The intent of the article is to bring together much of what has been written about Paco, translating Spanish material into English, and to try to make some sort of logical sequence out of the many scattered facts. This type of report will necessarily contain many errors due to misprints, misquotes, promotional exaggeration, and outright lies. However, it is all we have until somebody writes an authorized biography. The sources of information are listed at the end of the article and include books, magazines, record jackets, and television interviews. \".And now Paco de Lucía! Paco de Lucía - what is he? Is he a myth, a legend, a perfect\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nattered facts. This type of report will necessarily contain many errors due to misprints, misquotes, promotional exaggeration, and outright lies. However, it is all we have until somebody writes an authorized biography. The sources of information are listed at the end of the article and include books, magazines, record jackets, and television interviews. \".And now Paco de Lucía! Paco de Lucía - what is he? Is he a myth, a legend, a perfect lie...many things have been said around the world about Paco de Lucía. I recall a critic who said about him, more or less, that such men occur once per century to ressurect the phenomena of a Paganini or a Liszt...\" (from a Spanish television interview). Paco is \"...the most imitated guitarist in the flamenco idiom...\" (Island record, \"Paco de Lucía\"). \"The flamenco guitar of Paco de Lucía is simultaneously deep-rooted (Paco de Lucía, cont. from page 1) and forward looking, nourished in the depth of tradition and, at the same time, creator of new forms and freedom, permeated with past centuries and, at the same time, adventure, calcified by its ancestors and, at the same time, sumptuously rich in inventive vehemence. Never has the Andalusian guitar sounded so original and, at the same time, legible and remote... There is in the music of Paco de Lucía a tumultu\n\n[ENDING CONTEXT]\n\nin Amsterdam, Holland, where Paco's records are produced, in order to avoid the publicity problems they would have had in Spain. at least two with Fosforito, and at least one with his brother, Pepe; we do not have titles and numbers for these records.) \"De Sevilla a Cádiz\" (with El Lebrijano and Niño Ricardo) Columbia CS 8002 With Camarón de la Isla: \"El Camarón de la Isla\" Ph. 58 65 026 \"Canastera\" Ph. 63 28 076 \"Arte y Majestad\" Ph 63 28 166 \"El Camarón de la Isla - Disco de Oro\" (a collection from other albums) Ph 63 28 190 There are four or five more records with Camarón de la Isla.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PACO de LUCIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_06",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 12, 13, 14, 15, 16",
    "page_number": 1,
    "word_count": 2073,
    "article_char_count_full": 12139,
    "article_char_count_review": 2932,
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
    "article_id": "JALEO_1978_06::A3",
    "article_text_for_review": "Dear Friends, I can't tell you how happy I am to discover that there is, once again, a publication dedicated to flamenco. I was sad to witness the demise of the FISL Newsletter years ago. Since that time there has been nothing to take its place. I was beginning to think that flamenco was really dead except in the hearts of a few nuts like me....my company is preparing to publish some pieces by Paco de Lucía and Ramon Montoya. The music will be in standard notation... If any of your readers have material they wish to have published, I invite them to submit it for prompt consideration. Anything submitted will be held in strict confidence and returned if agreement cannot be reached on terms for publishing. Yours truly, Jerry Lobdill Southwest Waterloo Publishing Co. Austin, Texas Dear $ \\underline{\\text{JALEO}} $, ... my left pinky is still in a cast since I dislocated it trying to play that alegrias falseta which you misprinted in the March issue. Just wanted to add one thing to Paco's excellent column on flamenco methods. The guitarist Paco Peña, a fine player and an articulate guy who runs the London scene (its okay, he's really Spanish), has put out a record that's interesting in itself - plus a book that really transcribes each solo exactly, in notes and cifra. Lots of solid, traditional material, plus some interesting surprises (including a hot columbianas, and a very catchy rumba). Cost of these English imports is $7.50 for the record, another $8.00 for the book (Editors note: these prices have risen to $8.00 and $9.00 respectively), and $1.45 for shipping. A great buy, available from an outfit called The Bold Strummer, Box 4116, Grand Central Station, N.Y. 10017. Also, Chuck Keyser has collected some very interesting falsetas which he offers apart from his methods. I've seen his alegrías, siguiriyas, soleares, and bulerías (which has a lot of Moron material via Diego's nephews). The cifra is clear and an accompanying cassette makes it even clearer. It's not exactly cheap (I don't know current prices), but it's worthwhile. Keep up the good work with $ \\underline{\\text{Jaleo}} $. I'll be mentioning your existence in a forth- coming issue of $ \\underline{\\text{Guitar Review}} $ (which itself has featured flamenco in #43, an article focusing on Manolo de Huelva; #42, Segovia on flamenco; and #41, a full flamenco issue. All available at 6.00 a copy from the Society of the Classic Guitar, 409 E. 50th St., New York, N.Y. 10022.). Regards, Brook Zern (N.Y.) $ \\underline{\\text{RHYTHM OF THE MONTH}} $",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_06",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 4",
    "page_number": 3,
    "word_count": 427,
    "article_char_count_full": 2541,
    "article_char_count_review": 2541,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_06::A4",
    "article_text_for_review": "(from the FISL Newsletter, Dec. 1969) The tangos-tientos family is another one of the many areas of flamenco which is ridden with contradictions, unanswered questions and paradoxes. The first question we are faced with is which form gave birth to the other (they are obviously closely related). It is easy to see from the title of this article what the writer's opinion is, and I am in the company of many eminent flamencologists in believing that the force ful, primitive tangos preceded the slow, sophisticated tientos (although, there are many who believe it to be the opposite). It seems to be consistently true, in flamenco as well as other musical forms, that the light, rhythmic form gives birth to the more serious or \"jondo\" form. Then there is the problem of origin. Unlike Antonio Mairena, who seems to think all cante is gypsy-inspired, or Hipolito Rossy, who seems to think all cante is Andaluz-inspired, let's be realistic and say that this, and all cante, is the result of the combination of the two cultures. However, Mairena may have the edge on this one, since the musical key of the tangos and tientos is the same as that of the great pillars of cante gitano, namely, the soleá, siguiriya, and bulería. Yet, there is one very distinct difference between tangos and those other cantes. The compás of tangos is what is known as binary, that is, counted in twos, fours, or eights, unlike the others which are counted in threes, sixes or twelves. This suggests a strong Arabic influence which is perhaps more obvious in zambra and danza mora -- close relatives of the tangos. However, let's not ignore the few names which actually represent real structural differences in the cante such as: Tango del Piyayo, Tango de Jerez, and Tango de Repompa. These tangos contain different \"letras,\" different melodies, and different guitar accompaniments from the general tango-tiento group, which is generally known as tangos de Cadiz. In tangos and tientos, the melody style, about 7 or 8 in all, and letras are completely interchangeable, although certain melodies and letras are more characteristic of one or the other. The following is a common copla for tientos: Me voy a meter en un convento que tenga rejas de bronce pa'que tu pases fatigas y de mi cuerpo no goce. A copla for tangos: Si alguna vez vas a Cái pasa por Barrio Santa María y tu verás a los gitanos como se bailan por alegría The poetic form of the letras is the common form of cante flamenco - 3 or 4 lines of 8 syllables each. \"Estribillos\" or \"coletillas\" (little endings) may be added on to the coplas, much as in alegrías. The following estribillo is very common: Vales mas millones que los clavelitos grana que asoman por los balcones! Tangos and tientos are traditionally played \"por medio\" using the progression A,Bb, Dm, and C7, although they can be done \"por arriba\" using E, F, Am, and G7. There is one misfit of a copla which requires an E7 chord (when playing \"por medio\") or a B7 chord (when playing \"por arriba\"). The melody of this particular copla is easy to recognize once you have heard it a few times, and it is more commonly heard in tientos than in tangos. \"por tango\" just as soleares may be ended \"por bulerías.\" Manuela Vargas made one of the most successful attempts at a serious tientos on a recording. Most other dancers use it as a substitute for rumba. The dance of tangos and tientos is said to have been created, or at least developed by the dancers, Faico (1880-1938) from Triana and Joaquin el Féo (1880-1940) from Madrid. Few dancers realize the great potential of these rhythms. The tango is sensual and exciting, yet with greater subtlety and depth than a rumba flamenca. Tientos can be a true \"jondo\" dance - dignified and sensual with a withdrawn and ritualistic quality. Tientos may be ended The dance contains llamadas (dancer's calls or closings) that are 1 or 2 compases long, marked on the first beat of a compás and similar to the closings used in farruca. ly unrelated tangos de la Repompa de Málaga, also sometimes referred to as \"tangos de Málaga.\" Piyayo's tangos have recently been repopularized, and there exist recordings by La Paquera, Antonio Mairena, Miguel Gálvez, and many others, including a \"gracioso\" version by a local specialist from Málaga on the record, \"Cafe de Chinitas: Selección de los Cantes de Málaga.\"",
    "title": "Tangos-Tientos",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_06",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "4, 5, 6",
    "page_number": 4,
    "word_count": 751,
    "article_char_count_full": 4347,
    "article_char_count_review": 4347,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_06::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Paco Sevilla The preceding articles on tangos were written nine years ago; tangos have undergone considerable change since that time. This article will attempt an update on this rhythm and present some other bits of information. Molina and Mairena recognize four distinct traditional styles of tangos: Gaditanos (from Cádiz), Jerezanos, Sevillanos, and Malagueños. Those of Jerez and Málaga are usually thought of as the personal creations of Frijones in Jerez and Piyayo in Málaga, rather than highly developed and varied styles. The style from Cádiz is generally recognized as the tangos gitanos. The only version of the tangos de Triana (Sevilla) that I have heard is on the London \"Anthology del Cante Flamenco,\" sung by Rosalía de Triana; it is done very slowly and sounds much like tientos.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"Arte\"]\n\ntangos gitanos. The only version of the tangos de Triana (Sevilla) that I have heard is on the London \"Anthology del Cante Flamenco,\" sung by Rosalía de Triana; it is done very slowly and sounds much like tientos. Tangos de Málaga are of two types: those of Piyayo were covered in another article; the other is done in the minor key (A-E $ ^{7} $) and is often sung to accompany the baile \"por farruca.\" Camarón sings a similar tangos on the record \"Arte y Majestad\" which he calls \"Tangos del Titi.\" One other style of tangos that has recently appeared on records is called tango estremeño. Judging from the two versions I have heard, it seems to have a strong rumba element in the melody, working down the chords of the phrygian cadence, much in the manner of a rumba chorus. In recent years, the confusion caused by the many different names used for tangos and tientos (see previous article by Estela Zatania) has been pretty much eliminated, and the terms \"tango\" and \"tientos\" have each come to have a distinct and definite meaning - most of the other name variations having disappeared. Speed or tempo is not a good criterion for distinguishing between tangos and tientos. Usually, tientos are thought of as a slow and serious rhythm, while the tangos are considered to be lively and gay. Normally, tientos are slow, but they may be done quite fast, especially when danced (often slipping into tangos for the really fast parts), while tangos are capable of a wide range of interpretation, from fast and lively to slow and almost monotonous, from the deep and pensive to a trivial light-heartedness, in a rhythm practically that of rumba. In keeping with the flexibility of this latte\n\n[ENDING CONTEXT]\n\ntoday, gone tomorrow! The catchy melodies are easily assimilated, but soon lose their charm, and a new one must come along to replace the old and used-up. Writers and singers search constantly for new gimmicks that will make their song a hit; they add more instruments (electric guitars, basses, drums, orchestras), echo chambers, and vocal choruses, and they change the song in every way possible. All of this results in a headlong plunge into \"who-knows-where\" - hopefully, not into chaos and destruction of the song form! expect the answers from this writer - I'm running as fast as anybody else!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Tangos Today",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_06",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "6, 7, 8",
    "page_number": 6,
    "word_count": 966,
    "article_char_count_full": 6419,
    "article_char_count_review": 3316,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "Arte"
      }
    ]
  }
]
```
