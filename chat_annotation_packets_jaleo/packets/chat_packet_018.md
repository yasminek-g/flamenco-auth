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
    "article_id": "JALEO_1978_05::A1",
    "article_text_for_review": "Many American aficionados are familiar with the playing of Diego del Gastor, the non-commercial genius of Morón de la Frontera. Few, however, have heard of Rafael el Águila who held a place analogous to Diego's in the flamenco $ \\underline{\\text{ambiente}} $ of Jerez de la Frontera. Along with the better known $ \\underline{\\text{payo}} $ guitarist, Javier Molina, Rafael was the fountainhead of the Jerez school of guitar playing... In the spring of 1975, I was dropped off at a roadside $ \\underline{\\text{venta}} $ near the Barrio Santiago in Jerez. I had come in search of the legendary gypsy guitarist, Rafael el Águila, then a very old man with (as it later turned out) but little time left to live. He was the grand old maestro of the $ \\underline{\\text{toque}} $ of Jerez who had accompanied the great singers of his day and started out many now-famous guitarists (Parilla de Jerez for one) with lessons. He was also well known for his eccentricity (stating it mildly), and Donn Pohren, in his book, $ \\underline{\\text{Lives and Legends of Flamenco}} $, reports that he was interned in the same insane asylum in Cadiz with the singer Macandé, where the two, \"brought off some fine flamenco according to attendants and visiting friends.\" Not having an address, I set off, clutching my guitar case, into the blistering sun of mid-afternoon (anyone who has spent a summer in Andalusia knows what I mean). I suspected that I would find Rafael living in the Barrio Santiago which is well known as the gypsy quarter of Jerez, but when I questioned a few bar owners and patrons there, I found out that Rafael actually lived across town somewhere, in a place called Barriada del Chicle. (continued on page two) Off I trudged, my guitar case growing heavier and the sun hotter, asking directions all the way. At last I found myself walking on dirt paths amid the ramshackle dwellings of some of the poorer citizens of Jerez. Each person I questioned assured me that Rafael was a \"fenómeno,\" a \"monstruo,\" and a \"genio,\" but also \"muy raro,\" very eccentric and very much a bohemian. I was beginning to feel that I was getting close when I nearly bumped into an old man coming around a corner. He was quite dishevelled looking -- bearded (a rarity in Spain), wearing the most threadbare of clothes, and was holding a bag full of bloody fish heads. I asked him if he could direct me to the home of Rafael el Águila. Looking me over, he drew himself up to his full height and said, \"I am Rafael el Águila. What can I do for you?\" A bit taken aback, I replied that I would like to take some lessons from him. Pretending to be a bit perturbed, he told me, \"You've come at a bad time -- I haven't had my breakfast yet!\" As it was by then 5:00 in the afternoon, this pronouncement came as quite a surprise to me. He then asked me if I could come back in an hour or so and, after he had shown me the little shack where he lived, I went to a bar across the way to pass the time until my lesson. It turned out that the bar owner was a great aficionado who knew all the Jerez flamencos and had photographs of the all over the walls, including one of Rafael -- not playing the guitar as in most guitarist photos, but reading a book! The dueño informed me that Rafael stayed up all night and slept all day, clarifying for me the comment about breakfast. Naturally, at this point the guitar had to be brought out. I played some bulerias and the owner recogniz- some falsetas of Diego del Gastor who he said had come there once to play with Rafael. The dueno's son showed up and played some guitar as well, the tiny bar quickly filling up with patrons. At one point, one of the locals, hearing the flamenco coming from the bar, came in with his tape-recorder - radio combination, of which he was inordinately proud, and wanted to play for us all something he had recorded from the radio. An old man sitting in the corner spoke up gravely, \"Machines -- the ruin of the artist! As the time approached for my lesson, I packed up my guitar and walked over to Rafael's shack where I found an assembly of little boys sitting around outside picking and strumming away on guitars in preparation for their lessons. Rafael's lessons, it seems, were usually about five or ten minutes long, the students coming daily. When I went inside for my lesson, I saw the Rafael's hut was dirt-floored, divided into two small rooms, and had for furniture a small cot, some rickety chairs, and a table with a single-burner hotplate on it - the kitchen. The rest of the space was literally packed with books and newspapers from floor to ceiling; they were shelved and stacked up everywhere. Rafael was as enamored of reading as he was of the guitar and his books covered a wide range of subjects, including philosophy, politics, religion, and, of course, music. He lived alone and was obviously very poor, in spite of an homenaje and beneficio given for him earlier in the year at which nearly all of the flamenco greats of Jerez performed in his honor. I asked him about some of the Jerez guitarists of note. It seemed that for all of them he replied that they, \"... came to me in short pants! As we began the lesson, por soleares, it was at once apparent that Rafael now had great difficulty playing, his hands being stiff with old age and arthritis. But, slow, labored and sloppy as it was, what came from his guitar had the unmistakable primordial echo of gypsy  duende, and I shall always treasure the privilege that was mine of partaking from that pure and noble fountain of inspiration.",
    "title": "FINDING THE EAGLE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_05",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2",
    "page_number": 1,
    "word_count": 1003,
    "article_char_count_full": 5553,
    "article_char_count_review": 5553,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_05::A2",
    "article_text_for_review": "by Chuck Keyser When all is said and done, one of the most significant aspects of flamenco is its myriad of rhythm patterns, and to participate in the art, you'll have to become, in a small sense, a musician yourself (which is really not all $ \\underline{\\text{that}} $ unrewarding in the Grand Scheme of Life). The rhythm accompaniment to flamenco is traditionally the arts of $ \\underline{\\text{palmas}} $ (handclapping), $ \\underline{\\text{pitos}} $ (fingersnapping), knocking on tables, and vocal encouragement (kind and unkind) to the performers; all this action taken together is called $ \\underline{\\text{jaleo}} $ when performed in a flamenco way. Palmas are an extremely important part of flamenco and can be a percussive art in their own right; some performers in Spain (called jaleadores) even specialize in accompaniment. The first step in understanding flamenco is the ability to do palmas correctly. However, a sense of rhythm doesn't come naturally for anyone; it has to be developed. The guy sitting next to you who picked up the palmas so fast was probably a child prodigy on the violin before he became a construction foreman, so don't get discouraged -- and if you invest a little time and effort, you'll find flamenco a lot of fun. $ \\underline{I} $. $ \\underline{Basics} $ There are two kinds of palmas -- $ \\underline{palmas} $ (Palmas - cont. from page one) sordas (muffled palmas), performed with cupped hands, producing a hollow sound, and palmas secas (\"dry\" palmas), performed by clapping with the three fingers of your right hand into the palm of your left, producing a sharp dry sound. Palmas Sordas Palmas Secas II. Basic Rhythm Of course it is important to clap in rhythm. The best way to begin is to tap your foot in a steady beat and coordinate your clapping with your foot, with two claps to each beat or foot tap:  Fundamental to flamenco is the concept of $ \\underline{\\text{compas}} $, which basically means cyclic rhythm, as expressed in the phrasing of the music. There are many flamenco compas structures, but they can be divided into two main families: the 4/4 rhythms and the 3/4, 6/8 rhythms. III. $ \\underline{4/4} $ $ \\underline{Compas} $ 4/4 compas means that the musical phrases are expressed in multiples of 4; if a basic count is a quarter note, then a phrase of music in 4/4 time is equal to 4 quarter notes (4 x 1/4 = 4/4). Practically speaking, it means that you count in 4's, tapping your foot on counts 1 and 3: $$ \\begin{array}{ccc|ccc}{{{1}}}&{{{2}}}&{{{3}}}&{{{4}}}&{{{\\begin{array}{cc}1}}}&{{{2}}}&{{{3}}}&{{{4}}} \\\\{{{\\hline\\mathbf{F}}}}&{{{\\mathbf{F}}}}&{{{\\mathbf{F}}}} \\\\\\end{array}\\end{array} $$ etc., The basic palmas to the rhythm are performed by clapping on counts 2, 3, and 4, while leaving the first count silent: $$ \\begin{aligned}&\\left.\\begin{array}{ccc}\\mathbf{C}&\\mathbf{C}&\\mathbf{C}\\\\\\mathbf{1}&\\mathbf{2}&\\mathbf{3}\\\\\\mathbf{F}&&\\mathbf{F}\\\\\\end{array}\\mathbf{4}\\quad\\left|\\begin{array}{ccc}\\mathbf{C}&\\mathbf{C}&\\mathbf{C}\\\\\\mathbf{1}&\\mathbf{2}&\\mathbf{3}\\\\\\mathbf{F}&&\\mathbf{F}\\\\\\end{array}\\mathbf{4}\\right.\\end{aligned} $$ This really takes coordination, but you will really be a flamenco if you can do it. There are many other variations and lots of other things to know (like when to come in and what the music sounds like), but that only comes with listening, participation, and exposure to the art -- but that's the fun of it! Anda flamenco! Vamo' ya! Chuck Keyser is director of the Academy of Flamenco Guitar which was founded in 1971. A graduate of the University of California with a double major in mathematics and philosophy, he has devoted his life to the art of flamenco. He studied intensively under Diego del Gastor and Agustin de Morón, the legendary masters of traditional flamenco in Morón de la Frontera. As an accompanist, he was first guitarist of the Ballet Iberia, touring nightclubs throughout Spain, and as a concert flamenco guitarist, he has played in restaurants and nightclubs in the United States. He has taught highly successful classes in flamenco appreciation and flamenco guitar for the University of California Extension and the Adult Education program in Santa Barbara, California. He has also written articles for Guitar Player magazine and the Guitar Review.",
    "title": "FLAMENCO PALMAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_05",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 11, 12",
    "page_number": 1,
    "word_count": 648,
    "article_char_count_full": 4277,
    "article_char_count_review": 4277,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_05::A3",
    "article_text_for_review": "Dear $ \\underline{\\text{JALEO}} $, Enclosed is my check for membership and the newsletter. I did not know you existed until I received a complementary copy of the newsletter in the mail -- sent to me from a list of names submitted to you by Roberto Reyes of N.Y.C. While we are on the subject of Roberto Reyes, who as you know is a contributing writer, I would like to say that his sending to you the list of names is consistent with his general enthusiasm in promoting interest in flamenco. I know of no other person who has done more for the cause than he. In the past several years, he has encouraged amateur dancers, singers, and guitarists to perform in his shows whenever the opportunity presented itself. As a matter of fact, he was for years virtually the only flamenco \"entrepreneur\" available in the area who would give anyone a chance. The result of this is that there are many excellent artists who have grown out of that early experience with him. Roberto Reyes has never waived in his attempts to stimulate interest in flamenco throughout the New York Metropolitan area and sees the world as one big potential juerga. Who knows, with you on the west coast and people people like Roberto Reyes on the east coast, maybe his dream will come true, and yours. Editor's comments: Thank you for the excellent suggestions. There are several reasons that we have not printed reviews in the past. Our local newspapers do not seem to be interested in reviewing these types of concerts so we have not been able to draw on that resource. We of the JALEO staff have been reluctant to do critical reviews of artists who are our friends, since honest reviews would very likely have alienated some of them. Your suggestion to do a descriptive review in those instances is a good one. The most important reason, however, is the fact that the JALEO staff has consisted of two people during the past few months. The burden of getting the news letter to the readers has been tremendous and there has been no possibility of doing extra things like reviews or even proof-reading our articles (as some of you may have noticed). Hopefully this situation will improve in the near future as we get more organized, and we will try to stimulate interest in doing reviews. The readers in areas outside of San Diego should keep in mind that we would like concert reviews from their areas also. The reviewer need not be an expert on flamenco, since a review by a layman is just as valid in its own way as one by a flamenco authority; it tells the artists what they are communicating to the general public. * * * The following letter is from flamenco guitarist, Charlie Blankenship, and his wife, Vanessa, who left San Diego a year and a half ago to make their way around the world on minimum wages and a love of flamenco and adventure. After travelling extensively in Mexico, visiting most of the countries of Central America, and island-hopping through the Caribbean, they landed on St. Croix in the U.S. Virgin Islands, where they were stuck for the better part of a year. Recent word says that they finally found a yacht sailing for Europe and are now on their way to Spain. -- Jack Jackson ... So what's been keeping us on this little island, only 20 miles long -- lots of things. Like it says on the V.I. license plate, \"American Paridise.\" Tropical climate, hidden beaches, rum $1.00 per quart, rainforests, sailing to Buck Is. to visit the coral gardens... all attract tourists, which of course have to be entertained... there are many small clubs featuring live music, so there's lots of association with musicians of all types. A close associate is Carl Bernstein who, among many other credits, studied with Segovia. He's first class and is earning $300 a week on this island (never mind what I earn).",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_05",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 4",
    "page_number": 3,
    "word_count": 673,
    "article_char_count_full": 3792,
    "article_char_count_review": 3792,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_05::A4",
    "article_text_for_review": "From the Editor of $ \\underline{\\text{JALEO}} $: Dance Magazine's April issue featured a look at Spanish dance, including a history of the Spanish dance touring companies, a look at the Spanish dance today, especially in the United States, an interview with Spanish dancer, Jose Udaeta, a review of ethnic dance companies (including Spanish) that appeared in New York in the past year, and a brief look at Spanish dance on the West Coast. Especially interesting were the abundance of photographs of many of the top Spanish dancers currently working in this country. The following points of view are taken from letters being sent to $ \\underline{\\text{Dance Magazine}} $ and are in response to the article, \"Fanning the Spanish Fever\" by Lois Draegin. One of the points made in this article is that Spanish dance is at a low point in popularity, and one factor responsible for this situation has been an overemphasis on flamenco with a neglect of Spanish regional and classical dance. Dear Editor $ \\underline{\\text{(Dance Magazine)} $, The article that appeared in the April issue, \"Fanning the Spanish Fever\" by Lois Draegin, was very much appreciated. It is by far the most complete written work on Spanish dance and Flamenco that I have ever encountered. I thoroughly enjoyed the format: the history was accurate, the photographs were fascinating, and the interviews offered much \"food for thought.\" However, I disagree with Walter Terry's statement, \"Most of the other parts are gone now, except for brief excerpts, and the entire accent is on flamenco.\" Entire accent on flamenco!? The real problem is that companies have been performing in the U.S. mostly Spanish classical and regional dances, with the accent on flamenco fantasies. One prime example is the popular \"duet-syndrome\" which is geared for the unknowledgeable public. Flamenco cannot be truly danced with more than one dancer at a time. It's time that the representatives of all Spanish dance exercise respect for the audience of today that is far more sophisticated than the audiences of the 20's, 40's, or 60's. We're anticipating quality flamenco. As of today, no one person is an expert in Spanish classical dance, regional dance, escuela bolera, and flamenco. Like any other discipline, each requires a lifetime of love, dedication, and study. Yet, in every company, dancers are expected to dance all these styles... \"Jack of all trades, master of none!\" It is no wonder that there are 50 many negative reviews. There is a new generation of young aspiring artists trying to make their way in the \"Spanish dance scene.\" But those who are interested in flamenco, won't have time to take part in Spanish classical, escuela bolera, regional, and other dance forms. They'll be too busy learning the different styles of the song and the guitar music. In the words of Dance Magazine's perceptive Linda Small, \"Flamenco dancer, singer, and guitarist are in touch with the source of religious and secular passion, a place in the soul where love and suffering are one.\" Robert Reyes ... Lois Draegin's article, \"Fanning the Spanish Fever,\" was brilliantly executed. In her interview with Luis Rivera, she quotes him as saying he's \"... trying to get away from a complete program of polka dots and ruffles.\" What does he mean by that? That there has been too much flamenco? If so, I disagree! I've seen every Spanish dance company, flamenco recital, and night club performance in New York City in the last three years. There has been far too much make-believe flamenco. •... Hidden away in the various studios throughout the world, like caves in Granada the \"renaissance\" of flamenco is alive. I hope that $ \\underline{\\text{Dance Magazine}} $ will continue to play a relevant role in keeping the public informed of the current trends and changes in flamenco and Spanish dance. La Vikinga Dear Jaleistas: ... It would serve Spanish dance and flamenco very well if everyone would write to the editor (of Dance Magazine) of their interest in this subject, and, hopefully, Dance Magazine will continue their coverage more often than every 6 or 7 years. If you are a professional dancer, send in your picture, resume, and material about what you've been doing. It's time that the media be made aware of the \"new generation\" of flamencos. Note: the deadline is the 10th of every month. Sincerely, La Vikinga and Roberto Reyes",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_05",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 726,
    "article_char_count_full": 4383,
    "article_char_count_review": 4383,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_05::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJune 25, 1977 The trip to Alhaurín de la Torre had taken us about four and a half hours -- first a train ride from Arroyo de la Miel, a white-washed pueblo on the Costa del Sol, and then a long wait for a bus in Málaga. Alhaurín is a small pueblo, about ten miles inland from the city of Málaga, and was to be the site of the fourth \"Torre del Cante\" or, \"Festival de Cante Jondo.\" We had spent the previous day finding out how to get to Alhaurín (it seems that most people in Málaga hadn't heard of the place, or else confused it with another town, Alhaurín Grande) and going there to buy our tickets. We had found the town in the midst of a fiesta with everybody out to watch a parade, and had made our way to the ticket booth set up in the town plaza. We bought the best tickets, which cost about\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"Field\"]\n\ning there to buy our tickets. We had found the town in the midst of a fiesta with everybody out to watch a parade, and had made our way to the ticket booth set up in the town plaza. We bought the best tickets, which cost about $7.50 and were very formal with seat numbers designated and the program included (the cheapest tickets didn't cost much less at $4.50). It was 11:00 p.m. when we arrived at the Campo Municipal de Deportes (Municipal Sports Field) and entered into a walled-in area. The biggest surprise was the high level of organization and the beauty of the setting; the stage looked permanent with a whitewashed back wall containing arched windows and doorways, there were potted plants all over the stage, and the lighting was very well done. Along the sides of the seating area were arrayed a dozen huge speaker cabinets, and folding chairs had been set up in numbered rows to seat over two thousand people, with standing room and a bar in the back. We found our excellent seats in the 20th row and settled back to watch the arriving crowd. Perhaps one third of the audience were gypsies, dressed in their finest with the women wearing multicolored dresses, beautiful shawls, and flowers in their pony-tails, and the men in fashionable, expensive looking suits. Many people, like us, carried large bags of food and drink to see them through the night. We wondered how we enjoy listening to nothing but flamenco singing for hour after hour, especially since, the night before we had witnessed a flamenco singing contest in Arroyo de la Miel that had bored us to death within a f\n\n[ENDING CONTEXT]\n\nbeing slightly \"star-struck,\" I couldn't think of the many things that I should have asked him. He also is quite tall, at least six feet, which surprised me. The American guitarist, Chip Bond (Carlos Lomas), was there and introduced us to Pepe \"Tomatito,\" who was at that time Camarón's favorite accompanist. We later saw them at a festival in Madrid and found Tomatito to be an awesome guitarist (the Madrid festival had none of the charm of the one described here due to the strictly enforced seating arrangement and the fact that every other spectator seemed to be a policeman or Guardia Civil).\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FESTIVAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_05",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "6, 7, 8",
    "page_number": 6,
    "word_count": 1671,
    "article_char_count_full": 9636,
    "article_char_count_review": 3218,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "Field"
      }
    ]
  }
]
```
