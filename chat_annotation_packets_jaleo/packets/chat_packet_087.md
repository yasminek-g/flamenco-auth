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
    "article_id": "JALEO_1980_10::A1",
    "article_text_for_review": "(from: $ \\underline{\\text{Guidepost}} $, Jan. 14,1966; sent by Marylin Marilyn Bishop. Note from the editor: Although this article was published in 1966, the points made are equally valid today.) By George N. Clements Of the many foreigners who come to Spain in hopes of seeing the pure flamenco music they have read about or heard on records, most leave in a state of disillusionment. If they travelled in Andalucía, they were probably told that the best singers and dancers find work in Madrid. Arriving in Madrid, they were guided to a series of expensive clubs which present tablaos, or formally staged flamenco entertainment. At some clubs they saw a few highly reputed singers and dancers, who performed sometimes with enthusiasm, sometimes with complete boredom; at others they saw a most cynical display of commercial night club acts, adverstised as \"flamenco puro\". For the foreigner of average means, or for the well-to-do Spaniard, the tablaos remain the only presentation of flamenco music that he can regularly hope to see. Out-of-the-way flamenco cafés just don't exist in Madrid; neighborhood bars invariably display the motto, \"singing and dancing prohibited\". Those who can afford to may hire musicians to perform at private parties; but these musicians, if they are good, will normally be members of a commercial tablao and will probably give their accustomed tablao performance. RECENT DEVELOPMENT. For better or worse, flamenco music in Spain centers around the Madrid tablaos and their performers. Yet these clubs are a recent development; before 1950 there were no flamenco tablaos in Madrid. Flamenco music was held in low esteem by the more cultured classes; visiting private flamenco parties was considered \"slumming\". The music could be heard occasionally in theaters, where most singers would appeal basely to the tastes of the mass public. Beginning in the years just after 1950, there arose a gradual interest in flamenco as a highly-developed native art, largely among foreigners who had heard recordings being made at this time, and then among the educated Spanish public. Coinciding with this was the development of tourism in Spain. Gypsy caves; Spanish dancers, and flamenco guitar music were found to be irresistible tourist attractions. These two forces coincided to create the flamenco club. The flamenco clubs of the present are a mixture in varying proportions of the night club and the café cantante of the 19th century. The café cantante was originally just that: café's in which the best local singers were hired to perform for the enthusiastic patrons. But business became so profitable that the café's began to compete among themselves, each presenting a bigger, \"better\" show. Dancers and dance arrangements were introduced. Soon the music had become so distorted that the audiences lost interest, and one by one the café's closed down. Are the Modern clubs heading the same way? Musically, they are starting where the café cante left off. No matter how well-intentioned a club manager may have been at the start, he has had to face a largely uncomprehending public made up of the curious, the fashionable, and the bored -- the same elements that destroyed the cafe cantante. To stay in business he has had to please the public to some degree always at the expense of the music. As each club's audience contains these elements, a certain standardization marks the presentation from one place to another. The first hour to hour-and-a-half invariably features the gran cuadro, \"great picture\", the most colorful event of the night with the greatest tourist appeal. It is normally composed of two or three cantaores (singers), two or three tocaores (guitarists), and usually six to eight bailaoras (female dancers). The dancers, taking turns, will dance solo numbers or perhaps in pairs. The dancing and singing during this part is gay, featuring the rumbas, alegrías, bulerías, and the standard soleares. Following this and on into the night are the soloists and smaller groups. It is in these more \"serious\" performances that the quality of the presentation varies most from club to club. Those which are able and willing will present some of the long-established masters of the art and promising younger talent; others will present one or more acts which better belong to second-rate night clubs; the lower-budget clubs will bring back the members of the cuadro, one after another. The best presentations, visually and musically, seem to be the simplest and most natural ones -- the singers and dancers who somehow give the illusion that they are performing for an intimate group of friends. Excessive shouting and arm-waving, applause milking, elaborate choreography, and complicated footwork belong more to the theater than to spontaneous musical expression. (But there are exceptions; a few highly trained dancers with great technical command, such as Antonio, are able to convey genuine flamenco emotion with their dance.) Arthus Frommer advises his five dollar-a-",
    "title": "FLAMENCO TABLAOS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 802,
    "article_char_count_full": 5013,
    "article_char_count_review": 5013,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A2",
    "article_text_for_review": "Dear Jaleo Many, many thanks for the latest issue of Jaleo...the Diego issue. It's terrific! I am pleased that you found some of the things I sent you to use. The cover looks great! I did enjoy the photographic section; is it going to be a regular feature in the mag? I particularly enjoyed the Chris Wilson pictures, especially the toothless granny who seems to defy the passage of time with a \"How do them apples grab ya, honey?\" attitude. Believe it or not, although I have had the Feria y Fiestas articles for some few years now, this is the first time that I have read them (I can't read Spanish). Carolyn Tamburo seems to have done a sympathetic job of translation; please thank her from this reader. Much luck with $ \\underline{\\text{Jaleo}} $; please keep up the fine work because it's a bit of a lifeline, you know! Phil Coram London, England Dear Jaleo, Last August I attended Teo Morca's flamenco workshop in Bellingham, Wa. It was a great experience! Isabel and Teo are very hospitable and friendly people. They, along with Mary Rouzier, were always helpful. The workshop was very well organized. Teo's approach toward what was learnable in a short two week period was realistic. He did not try to throw every part of a complex art form at us at once. On the other hand, he did not neglect stressing the fine details of armwork, correct footwork, body movement and music comprehension that make the dance beautiful. Equally appreciated his encouraging attitude, his patience and his willingness to give the knowledge that he has.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 269,
    "article_char_count_full": 1541,
    "article_char_count_review": 1541,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A3",
    "article_text_for_review": "by Marta del Cid I was very excited. We were planning a summer visit to relatives in Massachusetts and perhaps I could arrange for classes with him while we were there. I had been on my own as teacher and performer for some time and was long overdue for a working over. I wrote Ramón at the Conservatory and, after a few weeks passed with no response, I received a call from his manager in Boston saying that Ramon was sorry for not contacting me sooner, but he had been out of town and had only just collected his correspondence at the Conservatory. He then gave me Ramón's phone number with instructions to call as soon as we were in town. FIRST CLASSES WITH RAMÓN Ramón was very friendly and approachable on the phone and we roughly determined what would be covered in class and established a schedule. He was waiting as I entered the lobby of the Conservatory's dance annex and greeted me with that mixture of warmth and reserve that is so charmingly characteristic of the Spanish. He was obviously an artist -- that preoccupation was present -- and something else was there too -- humility and the need to communicate. I liked him immediately. I was to be impressed with Ramón as a teacher. He was very generous with both his Class was again tremendously rewarding, clearing up a lot of odds and ends concerning technique -- questions I had as a teacher concerning the men's baile. Ramón was extremely helpful and free with suggestions, and the two hour class flew by. As we prepared to leave, I asked about the interview and Ramón said next morning would be fine and invited us to come to his apartment where it would be more comfortable. So the next day we entered the mausoleum for the second time and this time went up to the top floor. What could have been a cold and vault-like hall was saved by the fact that most of the large doors to the apartments had been painted with creations ranging from pastoral Wyeth-like scenes to geometries and cartoons. Ramón's door was still blank, but he told us that apartments are leased with priority to artists, and that By age fourteen he had performed professionally with Rafael Farinas, Manolo Caracol and Pilar López, and at sixteen Manolo Vargas invited him to join the Ximénez-Vargas Ballet His next partner was his present wife, Claire, a beautiful woman of Philippine, French Canadian and Dutch ancestry. With lush hair down-to-there, she is a lovely dancer whose natural charm projects from the stage with ease. \"Claire started as my student -- she had studied ballet since she was a child, but she was really born with the There were other problems his first year in Boston. \"Students of former teachers of Spanish dance here would come to me requesting certain dances they wanted to learn, and I would have to keep explaining that if they wanted to study with me it would have to be on my terms -- that they start at the beginning like everyone else. Sometimes they would threaten to complain to the president, and again I would explain that I am in charge of my classes and that the school supports this. I must make note of the fact here that the general caliber of Spanish dance instruction prior to Ramón's arrival was not very high, and that his insistence on this policy was in the best interests of his students. Other problems developed with Boston's Hispanics who, in spite of Ramón's stature within his art, felt competitive about his presence among them. Of great assistance here was the support and friendship of Raffael De Gruttola, the talented coordinator of a program promoting bi-lingual arts in the public schools, who provided Ramón the opportunity to work with the Spanish speaking community and other ethnic groups as well as to make presentations within the schools. \"I could have stayed in New York,\" Ramón said, \"but everyone is in New York. I like living here very much -- I saw more opportunity for myself here and I hope my work will bring more ambiente to Boston. I hope someday we can have our own Spanish arts center here, like what Tina Ramírez is doing in New York. Of course, it is hard work -- not just the dancing, but all the business and paper work",
    "title": "BOSTON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5-9",
    "page_number": 5,
    "word_count": 739,
    "article_char_count_full": 4145,
    "article_char_count_review": 4145,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A4",
    "article_text_for_review": "Ramón de los Reyes had been in Boston just about a year when he began organizing a resident performing company, his Spanish Dance Theatre. Until that time he had been doing a few shows with some of his students when he heard from Dini Román, a friend from the days when both dancers were working with María Alba. Dini was appearing as featured artist with Theater Flamenco in San Francisco, but had family in Boston and was interested in what Ramón was trying to do there. Ramón, on his part, was delighted at the prospect of having another artist with whom to share performing and choreographic responsibilities. And not only did Dini join him, but guitarist Teo Greso, as well, of Theater Flamenco. The other members are all \"home grown\", students that Ramón has developed and coached and polished to a remarkably professional level in the short period of time the company has been in existence: Clara Ramona (Ramón's wife), Malena del Mar, Pamela Mora, Sara Olivera, and in a jota, Juan Seager. Fernando de Málaga is the cantaor who, as Ramón puts it, \"I found in the street\". Originally from Málaga, he is a house painter by trade and was just singing for his own enjoyment when he met Ramón, who recognized the unique qualities that made his voice ideal for flamenco and who set about teaching him the cante himself. Others in the company were temporarily absent -- a couple of dancers, an Israeli guitarist -- and Ramón announced with pride that he would be presenting the debut of a promising student, a 14 year old boy, in the near future. dawn-like revealing costumes in deep conservative hues \"reminiscent of a Goya painting\". The flow and pacing of this piece was very moving and beautifully executed. Dini Ramón's interpretation of \"Playeras\" was equal to the lyricism of that work and was illuminated by fragile little accents like the soft playing of castanets off her shoulders. The suite ended with all the women dancing \"Rondalla\", a refined but spirited number that looked like a period court dance gone country. Next, in a very flamenco zapateado \"Las Campanas\" (credited to Estampio), Ramón demonstrated his superb domination of this technique, playing his feet in an ever-changing variety of patterns and shadings punctuated by brisk vueltas and sharp poses. Teo Greso soloed with a Moorish-sounding \"Rondeñas\", and the entire company brought the first half of the program to a close with a lively, effortless appearing jota Aragonesa that was introduced by Ramón's wonderful robust singing. Following intermission, the second half was opened with a truly potent and dramatic siguiriyas as created by de los Reyes and Ramón, the latter entering first in a long, soft flow of red, just barely restrained in a large white shawl which was later discarded when her partner joined her, black enveloped in black. There were many striking moments and surprises -- his cape forming a temporary backdrop for her armwork, Ramón snatching her bata from the air as it came flying out of a vuelta. DINI ROMAN AND RAMON DE LOS REYES and enticing him into dance with her which at times threatened to get the better of both of them, as when they suddenly found themselves immobilized nose to nose at the finish of a particularly energetic vuelta. Dini Román's caracoles, with characteristic fan work, was full of lighthearted grace and charm with nice touches of inventiveness, as when making her exit, she walked around to lay her fan delicately on the end of her bata, and then danced into the wings. Somewhere in the midst of all this Teo Greso climaxed the evening with alegrías, his most exciting piece of the evening. He was a man totally involved in his dance, barely containing himself, constantly setting up check points for himself, bursting through them momentarily, then containing himself again conserving energy until the next eruption. Masterful music from his feet was interspersed with swift and nifty back heel turns and strong dominating arm work. This artist certainly has total knowledge of his vocabulary and his instrument and communicates eloquently his joy in his life's work. Ramón de los Reyes came to Boston three years ago to teach at the Boston Conservatory of Music. Within a year he had formed the Ramón de los Reyes Spanish Dance Theater featuring himself plus four women with Spanish names (if not all with Spanish ancestry), two guitarists, and a flamenco singer. In the two years since its debut, the company has developed into a fine troupe, presenting as theatrical an evening of Spanish dance as is to be found north of Seville. De los Reyes is the center of it all, one man stamping out the",
    "title": "THEATER: TWO REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "10-12",
    "page_number": 10,
    "word_count": 788,
    "article_char_count_full": 4635,
    "article_char_count_review": 4635,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A5",
    "article_text_for_review": "By Gordon Booth Fans of the guitar who happened to be in the general area of the Bay of Cádiz on the 7th of August had a unique opportunity when, as part of the series \"Festivales de Cádiz\" (14 days of cultural presentations including plays, ballet, light opera, and, on one evening, even a full-blown flamenco festival), Paco de Lucía gave one of his rare recitals --- rare, at least, in Andalucía. While Paco often tours much of northern Europe as well as northern Spain, he has not appeared on stage in his native habitat for many years. In fact, a year or so ago, when he was busy performing in Barcelona and Madrid, one of the local papers questioned, \"Why not Sevilla?\" Well, if the sevillanos are still waiting, at least the gaditanos had their chance. The concert was held in the open-air Teatro José María Pemán, a perfect setting. The theater is located in a small but quite lovely park where fancifully-cut fur trees and other exotic vegetation dance to the accompaniment of ocean breezes while in the background can be heard the moorish murmurings of a multitude of fountains. Some badly-needed restoration work on the theater had just been completed (when Manolo Sanlucar had played there last year a section of the structure just above the stage had appeared ready to topple on his head at any moment) and it looked quite elegant in its fresh coat of white. Even though the ticket prices were more than reasonable -- 400 pesetas ($6) -- the theater was less than 3/4 full...which is probably why Paco doesn't perform much in this part of Andalucía. On what to blame the lack of \"sold out\" signs? Quien sabe? Who knows? Publicity is always approached haphazardly no matter what the event. If one doesn't happen to buy the right newspaper on the right day or drive past the right wall -- the one where the poster has been pasted up -- the event comes and goes without one's knowledge.",
    "title": "PACO DE LUCIA IN CADIZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "13",
    "page_number": 13,
    "word_count": 337,
    "article_char_count_full": 1896,
    "article_char_count_review": 1896,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
