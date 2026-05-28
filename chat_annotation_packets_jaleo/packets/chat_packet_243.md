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
    "article_id": "JALEO_1987_SPRING::A9",
    "article_text_for_review": "(David Hollowell: Olé, Flamenco Guitar) Part I: Record Review . by Paco Sevilla It is always difficult to review solo guitar records by non-Spanish artists. One can praise or destroy a recording made in Spain by a Spanish artist and realize that it makes little difference. But An American will feel two consequences, both personally and, perhaps, economically (to very small extent), of a record review. The reviewer, therefore, doesn't know whether to make special allowances for an American player and try to appreciate whatever he has accomplished through many hours and years of sweat, expense, and suffering, or just call the cards as they lie. This reviewer has a difficulty in not generally being very impressed with any solo guitar effort — it just is not a very high priority in my view of flamenco. I realize that there are many who do not agree with me. Every time I do a review, I vow not to do another — too many people are mad at me already, but...here goes! Let's start by saying that David Hollowell's record \"Olé, Flamenco Guitar,\" does a fine job in accomplishing what it is obviously supposed to do — appeal to a popular, commercial, non-flamenco market. The title, the selection, of pieces, and the program notes tell us it has that purpose. The playing is technically adequate, the recording quality is good, and the music is pleasant and often fast enough to capture the emotions of the general listener. That said, let's move on. The reader of Jalco deserves a review from a flamenco perspective. We begin with the repertoire. There are five pieces credited to Sabicas. In many reviews, I have stated my belief that a record In general, David shows these strong points on his debut album: a) lots of knowledge and variety, b) enthusiasm, pizzazz, and lave for flamenco guitar, c) good musical ear, d) potential for creativity on a larger scale, e) good tone and flamenco sound, f) good flamenca thumb and rasgueado, and g) clean arpeggios. On the other hand, my feeling is that David, a diamond in the rough, needs more seasoning before returning to the recording studio. He certainly has the makings of a fine flamenco soloist, but on this album he has the following shortcomings, in my opinion: a) failure to play within his limitations of technique, b) reciting as opposed to interpreting, plus e) occasional rushing. Overall, I like David's album and would recommend it to any serious student of solo flamenco guitar who might be thinking about making a record. Probably, a record like this would not be released in Spain, since promising guitarists usually do not produce thier own records. It is much more desirable to catch on with a record company and have them do the recording and subsequent promotion. But the non-Spanish flamenco guitarist would do well to promote himself and gain entrepreneurial skills. So, if the listener must use comparison in judging this or any album of a non-Spaniard, compare him to yourself instead of to Sabicas. How does the album sound when you compare it to some of your tapes? If you don't have any tapes, make some and compare them. The serious players know what I am talking about. It's the pseudo-sophisticated or the isolated \"big fish in a small pond\" that would use David Hollowell's album as a dart board! David, who has lived in Spain and now resides in Austin, Texas, could take heart by listening to the albums of one of his teachers: Guillermo Ríos. Notice how Ríos improved by his second recording — not that the first one isn't good. Also, I'd like to welcome David Hollowell to the group of non-Spanish flamenco guitarists who have recorded albums or cassettes either on their own or commercially. Some of them: Guillermo Ríos, Dennis Koster, Gene St Louis, Peter Evans, Anita Sheer, Ronald Radford, Michael and Anthony Hauser, Guillermo Salazar, James Fawcen and Martin Walker, Philip John Lee, Chris Carnes, Ismael Barajas, Juan Martín, Gino D'Auri, Rodrigo, Ian Davies, Willie Champion (El Curro), Antonitas d'Havila, Carlos Lomas, Gerardo Alcalá, David Serva (David Jones), Agustín de Mello, the \"cantaora\" Elena Marbella (Elaine Dames), and the dancer José Greco. Part III: With NínoMiguel In Huelva I arrived in Sevilla, May 1981, after hanging around the Madrid Bar, Moca, Amor de Diós Studios, and Peña scene for four months. I studied mostly with David Jones and he suggested that I should head south to see the flamenco festivals. I had heard of Manolo Marín from the \"Madrid Three\", meaning La Cintia, Charo and Concha, so I ended up at his old studio, where I stayed and played daily for the next seven months. At that time, a great Japanese guitarist named \"Taketo\" was number one accompanist, so I sat beside him learning to play for Manolo's dances. That summer, Manolo was teaching general dance classes at a peña in Huelva, where the legendary guitarist Niño Miguel was also teaching a general guitar class. Manolo invited Taketo and me to commute with him twice weekly so we could meet and study with Miguel. Although Manolo drove his new Renault like a lunatic, we knew that watching Niño Miguel would well be worth the risk. After getting over my initial complete and total astonishment from watching Niño Miguel play up close, I was able to learn some",
    "title": "DAVID HOLLOWELL: \"OLE, FLAMENCO GUITAR\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 897,
    "article_char_count_full": 5254,
    "article_char_count_review": 5254,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SPRING::A11",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nG R Z P R E H B DE G U I L L E R M E FLAMEN(CO CONFUSION After visiting a few places in Cataluña, a friend of mine commented that the people there were the closest thing he has ever seen to Americans. \"The Catalans are the 'americanos' of Spain\", he emphatically declared. That got me to thinking about a few things: What place do Americans have in flamenco, if any at all? Wouldn't most American flamencos be much more comfortable studying dance and guitar in a place like Barcelona, Valencia, or even Torremolinos? Most Americans seem to separate themselves from the gypsies or the so-called real flamenco, be it gypsy or andaluz. In their psyches there is a feeling that theirs is not the true or real item, and of course the gypsies seem to encourage this. Some Spaniards even refer to us\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"friends\"]\n\ncalled real flamenco, be it gypsy or andaluz. In their psyches there is a feeling that theirs is not the true or real item, and of course the gypsies seem to encourage this. Some Spaniards even refer to us American flamencos as \"los flamenquillos\", a term that shows both contempt and affection; contempt since the \"flamenquillos\" may compete for contracts, and affection since the \"flamenquillos\" are the concert goers, record buyers, students, and friends of the Spanish flamencos. Obviously flamenco has enriched the life of the ordinary \"flamenquillo\". Conversely, the \"\"flamenquillo\" has affected, if not enriched the life of the flamencos both of Spain and transplanted Spaniards. Let's use the word \"affected\" instead of \"enriched\", since many Americans to into flamenco as a rejection of their own way of life. So, what are the consequences of this American involvement and marriage with flamenco? It has got to show up somehow in the thinking of the Spaniards or somewhere in the flamenco world at large. Here is a list of things that the American and the Spanish flamenco seem to have in common. Let's remember that the American psyche is an eclectic blend of ancient cultures with modern philosophies and technology. The American generally feels that he personally has come up with all this, and is living his life in the manner that he has chosen: 1) The Bible a) Jewish sacred texts: among the many examples that could be cited, one stands out: David and Goliath! A good example of this is the case of Diego del Gastor. Diego, while not a guitarist in the f\n\n[ENDING CONTEXT]\n\nstudy flamenco, and who also stick out like a sore thumb to the older Spaniards, but are indistinguishable from many of the young Americanized Spaniards. Now how can it surprise anyone that flamenco is changing? Flamenco is a part of Spain's whole movement of change, so like it or not, it will move along too. If \"El Cabrero\" can mention Ronald Reagan in one of his cartes, then what is next? Perhaps Abraham Lincoln, George Washington, the Spanish American War, Thomas Jefferson, Davy Crockett, apple pie and Cheverolet? Might as well toss in the Easter Bunny and Santa Claus! --Guilermo Salazar\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "GAZPACHO DE GUILLEROMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 26,
    "word_count": 1083,
    "article_char_count_full": 6308,
    "article_char_count_review": 3191,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "friends"
      }
    ]
  },
  {
    "article_id": "JALEO_1987_SPRING::A12",
    "article_text_for_review": "DANCE AND YOUR DIET Last spring, while visiting Spain, we noticed the usual invasion of McDonald's, Wendy's, Burger Kings, and other fast food places. We had an interesting surprise while we were in Sevilla, \"the cradle of flamenco.\" We found a \"health food restaurant.\" This restaurant, located in the heart of the city, featured veggy salads, health soups, carrot cake and fresh fruit juices. This was very interesting in the country of delicious tapas soaked in olive oil, strong coffee, lots of fried food, porras, churros, and a large diet of wine and cigarettes. While I have indulged myself with years of smoking (I quit in 1971), plenty of wine (a bit with dinner nowadays) and lots of years of eating tapas and delicious Spanish food (fried and otherwise), I have, in the last 15 years, developed a great respect for taking care of my body, especially my dancer's body. It seems that young people with young bodies have great resilience with regard to all kinds of food abuse, sleep abuse and many forms of dissipation. One sees many young dancers, ballet, flamenco, jazz, modern, you name it, barely out of grade school, living on cigarettes, dict cokes and, in Spain, cafe solo, wine and sweet rolls. Not that many do not eat well, but it is a common sight to see young dancers working their bodies and not being too concerned about what they eat. I will not get into drugs in this article, but suffice to say that dancing well and taking dance as a serious art is not compatible with drugs. Drugs are anti-dance, anti-life. Bodies change over the years and, just about the time a dancer has studied to a point of arriving at a mature performing artist, his or her body is changing. The body becomes less resilient to abuse and the metabolism begins to cry out for reform. This age of change is different for each individual, but let us say late 20's through the 40's to start. I remember that I could eat anything, including thick milk shakes, lots of beef, pastries, and all types of fattening foods and easily keep my waistline and my weight in check. When I was around 28 years old, things started to change a bit. I noticed that I had to \"work\" at keeping trim and work at keeping my weight where I wanted it to be. I feel that a dancer, a flamenco dancer owes it to himself or herself and the audience, if the dancer is a performing artist, to have an aesthetically expressive body. I know that there are many fine, heavy or thin dancers of fine artistry and many have different shapes and sizes, so I am not only talking just shape, but am talking about a healthy body as well as a body that is trim, with a flamenco line. Flamenco, like other dance forms, is expressing moving sculpture as well as the feelings and emotions and artistry of the dancer. A body should express the art, feeling, the craft and technique, along with an emotional outlet that is true. It showed a huff-puff out of shape body. When one starts to take care of the body, of the self, your body usually will tell you what is good for it, if only you listen. Discipline can surely play a part in this. Sometimes when your body and mind say, \"you have smoked enough\", it takes will power to stop, not just a weak desire from an uncomfortable body, but a real desire for self improvement. It may not be easy to change your eating habits for the better, but that is when your priorities come in to play. The excitement of a three-day juerga, with jerez and other booze flowing on and on and the room full of smoke, may be a special; occasion that brings out the best in flamenco, but somewhere along the line the body rebels. (I know from these juergas.) Somewhere along the line the dancer's body rebels, which in reality is saying, \"a bit of moderation please, if I am to function as a healthy dancer's body, able to perform the way that you want, with its full potential\".",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "27",
    "page_number": 27,
    "word_count": 703,
    "article_char_count_full": 3863,
    "article_char_count_review": 3863,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SPRING::A13",
    "article_text_for_review": "NEW YORK The end of May should be busy with flamenco dates here: Manolete will concertize at the Casa de España from May 27 to May 22; at the same venue the 1986 \"Cumbre Flamenca\" presentation can be seen in video. Other big news is a program in honor of the fiftieth anniversary of Argentina's death -- May 21st at the Museum of Natural History in New York, Matteo will present his orchestra of castanets in a diversified program. Jerane Michel (teacher's listing) will play the \"Playera, Spanish Dance #5\" of Granados as recorded by Argentina in her honour. MAY May 27th was the night of New York flamenco -- Manolete at the Casa de España -- the Casa seldom mentions flamenco but this was a special guest...all packed to capacity, no standing room...Ramón \"El Português\", cantaor from Badajoz, muy moderno, a little like Camarón, maybe Pepe de Lucía...and the inventive gypsy guitar of Felipe Maya; Maria José Velasco of Madrid completed the cuadro. Such a tremendous technician this Manolete...originally from Granada, cousin of Mario Maya, brother of guitarist Marote and friend of El Guito, but never had partnered the famed Pilar López. Manolete and Felipe did a farmica, modern, very gypsy with none of the interpretive melancholy or longing of the northern Spaniards working in the South -- abrupt and dramatic; later Manolete danced the cantinas to the coplas of El Português and the modern flamenco guitar. \"Manolete!\", \"Felipe!\" the public was shouting in approval of Felipe's falsetas. María Velasco did two well coordinated dances, an alegrias, siguirias, before intermission we had a bulerías and El Português sang por tangos with the guitar. The only possible comment I heard from the public was the lack of facial expressions of the dancers. After the show, I had the pleasure to present to Manolete a copy of Jaleo (Summer 1986) which had featured him on the cover. JUNE It was June 14th — Flamenco time again; the venue Rojas-Lombardi's \"The Ballroom\" on West 28th Street. — \"Flamenco in Concert\" with the participation of the two great dance artists Maria Alba and Vietorio Korjhan. This was to prove that, after \"Cumbre Flamenco,\" after Farruco, El Chocolate, Femanda, the Amadores and Habichuelas, we were capable of yielding superb flamenco. There was no soleares, but there was a dazzling alegrias, by María, with the full voice of El Malagueño Paco Ortiz. There was the spice of beautiful tangos de Málaga for the superb dancing of best dressed dancer Victoria in his farruca and bulerías. They were not all New Yorkers. Ex-Chicago, ex-Pilar Rioja, popular tocaor Arturo Martínez, in combination with the extraordinary (ex-Granada) tocaor José Chucales, the outpost guitarist at Restaurante Don Quijote from \"Toronto de la Frontera\". Chucales who played for La Tati, Farruco, and others there. These two guitarists combined as has seldom been heard in this city...The Californian cantaora La Conja, now very active locally, singing mainly for Victoria's baile and his dapper feet. Add to all this, the Cabaret, what an ideal place for a flamenco venue!! I Suppose we should start at the beginning and turn the clock back fifty years...Madrid 1936...creation of Victoria, as he portrays the soldier \"Paco\" returning from the battlefields, tired, disillusioned; in full spoken dialogue he stresses his main three desires \"El vino, el amor y el arte\"; - \"Paco\" dances a farruca, but",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "article",
    "pages": "28",
    "page_number": 28,
    "word_count": 560,
    "article_char_count_full": 3404,
    "article_char_count_review": 3404,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1987_SPRING::A15",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJOSE MOLINA José Molina is one of the world's most acclaimed Spanish dancers. Born in Madrid, he started dancing at the age of three. He began formal studies at the age of nine as a student of Clasico Español with Pilar Monterde. After a year of training he took up the study of flamenco as well as classical. For the next four years he proceeded to attend daily classes of intensive studies in flamenco and clasical simultaneously. At the age of fourteen, Mr. Molina auditioned for the role of second dancer in the company of the famed \"Soledad Miralles\" and was hired. He performed throughout Spain the following year and then became first dancer of \"Brisas de España.\" In his seventeenth year he began a tour of the major cities of Europe. Still in his teens, Mr. Molina arrived in the United\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"memory\"]\n\nance world and has attracted legion fans not ordinarily cognoscenti of the dance. In annual tours across North America, José Molina Bailes José Molina *** Spanish Dancers Superb S[from: The Calgary Sun, April 28, 1986] by Stephani Keer This is no time to mince words. The José Molina Bailes Españoles at the Centre last night was superb. Surely, the four dancers must be the most beautiful and most flexible troupe to grace a Calgary stage in recent memory -- to say nothing of the most exuberant. All four, as well as guitarists Miguel Ochoa and José Maria Moreno, and flamenco singer La Conja, excelled. The three women in Molina's company -- Ester Suarez, Susana Aranda and Carla Ochoa -- never lost the elegance that is so innately a part of Spanish dance, although they were able to vary the mood. Suarez, in particular, in Bolero superimposed a twinkling ease on the rigorous ballet-derived dance. All three were charming and frothy in Seguidillas and, with Molina in the traditional Serranas, Aranda and Ochoa were moodily aloof during the first part, passionate and sensual as the dance accelerated. But, make no mistake about it, the evening was Molina's from beginning to end. He showed the almost magical talent that made him a professional dancer by the time he was 14, and most of the dances were clearly carefully selected to showcase his impeccable and intricate footwork. His sharp heel-and-toe beats made a music of their own, and blended with the flamenco guitars and claps of La Conja until they were inseparable. All he had to do was walk onto the stage to deliver a feeling of supercharged energy and excitement. *** José Molina Bailes Españoles [from: Backstage, April 5, 1985] by Jennie Schulman There had been a dearth of Spanish Dance on the New York scene. Then suddenly, in rapid succession, there came the Maria Benitez Company and Ballet Antonio Gades. The latter company recently appeared in Carmen at the City Center. José Molina, who has not been seen here for about seven years, brought his company into Carnegie Hall for a one-night stand recently. Since the company has a large following, not alone among the Spanish population, but also among aficionados of Spanish dance generally, their one appearance was sufficient to turn the cool gold and white interior of the hall into a blazing inferno. There was only four dancers involved -- Molina, Aurora Reyes, Clara Mora and Ester Suarez, accompanied by flamenco singer Pepe de Cadiz, and guitarists Gerardo Alcala and Basilio Jorges. Not a large company but they manage to fill the immense Carnegie Hall with their abundant warmth. If you have seen the Be\n\n[ENDING CONTEXT]\n\ndisplaying their tight ensemble, jocular castanet technique and superb, complex legwork. No wonder it brought accolades of bravos. It was enjoyable, by the way, to be in a nearly packed house which was characterized by the presence of many people to whom the company is a new experience. This was a performance in which La Chiqui made a new and visible effort to display the talents of each of her dancers. Solos such as Diana Vidal's \"Cádiz\" and Rocío's \"Córdoba\" were very lyrical, though in markedly contrasting ways. Vidal's delicate fanwork evoked high emotional sensitivity, while Rocío's\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS & PRESS RELEASES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SPRING",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "30-39",
    "page_number": 30,
    "word_count": 3789,
    "article_char_count_full": 22963,
    "article_char_count_review": 4261,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "memory"
      }
    ]
  }
]
```
