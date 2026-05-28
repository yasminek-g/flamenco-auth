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
    "article_id": "JALEO_1978_07::A2",
    "article_text_for_review": "Well, we did it -- and in pretty good form! This is our twelfth issue of $ \\underline{\\text{Jaleo}} $, and it looks like we will be around for awhile longer. In the poverty stricken, disorganized, ego-sensitive, highly factioned, anti-organization world of flamenco, it is extremely unlikely that a venture like this would be conceived or given birth, let alone survive and grow. The existence of Jaleistas and Jaleo is due to a whole set of circumstances that are not likely to occur very often. First, several of us in San Diego had just spent time in Spain and were fired up to do something. Juana de Alva returned first to San Diego and started everything with a juerga at her home and an inspirational speech aimed at initiating regular juergas, that was delivered from atop a large rock. She received a positive response and the monthly juergas were begun. The juergas were important to Jaleo in two respects: First, they gave it reason to come into being as a means of communication for people attending the juergas. Second, the juergas financially supported the newsletter, and still do to a large extent, since most of the early members were primarily interested in juergas, not an educational magazine. The cost of starting a decent newsletter without the juergas would have been prohibitive for any of us. Juana de Alva should also be thanked for all of the time she has put in over the course of the year; she has been the center of all activity, around which everything has revolved, and without her constant presence, the whole thing would have failed long ago. But there were others who were of critical importance, especially in the early stages. Betty (Jobe) Reineking put a tremendous amount of work into the issues of the first six months, practically single handedly organizing and typing each issue, as well as writing articles and editorials and doing artwork. Stan Schutze, a person with no real interest in flamenco, became enthused with the newsletter idea and was responsible for most of the graphics ideas and the layout of Jaleo, as well as organizing the business end of Jaleistas; without him, Jaleo would certainly not have anywhere near the appearance it does. We also have to thank him for donating a typewriter when he left to work in the Middle East. Paco Sevilla came back from Spain loaded with ideas for articles and was the main force in starting the spread of Jaleo to areas outside of San Diego in January. We have to thank the many San Diegans who, in spite of feelings of inadequacy concerning knowledge of flamenco, educated themselves and wrote articles so that we could publish something besides announcements. We owe a special debt of gratitude to Peter Baime of Milwaukee, who jumped into the battle at an early stage and allowed his name to be associated with what was still just a local venture. And just when we were beginning to despair of ever arousing any interest in the outside world, Roberto Reyes and La Vikinga of New York gave us strength to go on, with articles, announcements, and lists of people and businesses to contact. There are so many to thank now -- Chuck Keyser, Brook Zern, Gary Hayes, and many more. So, thank you to all who have assisted in any way -- including sending in for a subscription or contributing money (we have had many generous contributions). Each of you should feel pride in what we have all created together and realize how special it is and how unique were the circumstances that brought it about. --reports and photos of local juergas. --guitar music written in either tablature or standard notation; we can rewrite it for you, but be sure it is written the way you want it. --names and addresses of potential subscribers or advertisers. These are a few ideas -- you come up with the rest! You can sit back sipping your sangria as you peruse your monthly issue of $ \\underline{\\text{Jaleo}} $ while mumbling to yourself, \"Very interesting, good job, good job!\" and perhaps watch the newsletter get thinner, become bi-monthly, or fade away completely, as the odds say it should have long ago. Or you can do a little something every now and then and watch $ \\underline{\\text{Jaleo}} $ grow bigger and better. There $ \\underline{\\text{is}} $ a choice! Box 4706 San Diego, CA 92104 X STAFF: Juana de Alva, Paco Sevilla. TECHNICAL ASSISTANCE: Bill Martin, John MacDonald, Jesus Soriano (photography) ALSO IN THIS ISSUE: Brook Zern, Jerry Lobdill Roberto Reyes, La Vikinga, David Blakley, Gene St. Louis. The goal of Jaleistas is to spread the art, the culture and the fun of flamenco. To this end we publish the $ \\underline{\\text{JALEO}} $ newsletter, have monthly juergas and sponsor periodic special events. Membership-Subscription is $8.00 per individual and $10.00 per family or couple. Announcements are free of charge to members and businesses may display their cards for $6.00 per month or $15.00 per quarter. JALEO is published 12 times yearly by Jaleistas, the Flamenco Association of San Diego. © 1978, by Jaleistas, all rights reserved. 0 WELCOME TO JALEISTAS - NEW MEMBERS San Diego: Elizabeth & Francisco Ballard, Sheryl Tempchin, Jessie & Don Johnson, Mr. & Mrs. & Lisa Estala, Greg & Jo Mellon, Victor & Margarita Urganda; Canada: Angel Monzón, Mrs. A.M. Robertson, Tom Patton; Herbert Goullabian (Colorado), Robert Weisenberg (Wisc.), Suzanne & Michael Hauser (Minn.), Johnny Beard (Okla.), Michael Fisher (N.Y.), Teodoro & Isabel Morca (Wash.)",
    "title": "Jaleistas & Jaleo Survive a Year",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2, 3",
    "page_number": 1,
    "word_count": 921,
    "article_char_count_full": 5448,
    "article_char_count_review": 5448,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A3",
    "article_text_for_review": "Dear Editor: (re: comments about the alegrias falsetas contributed by Roberto Reyes in the May issue of $ \\underline{\\text{Jaleo.}} $) When I first met Roberto, he taught me a lot about compas, just as you indicated in your, \"it is perhaps even more important to realize that the following falsetas can be accented in several different ways.\" And that is the way Roberto used to practice. BUT, it never sounded relaxed or had any \"aire.\" You were commenting about flamenco compas. That has nothing to do with the \"breaking down\" of a falseta into doublets, triplets, etc. My many years as a concert pianist (piano major in college) taught me that basic technique is the same, whether for guitar, piano, tuba, etc. In $ \\underline{\\text{learning}} $ a falseta in triplets, one must accent the first note of each triplet, or the first note of each quadruplet, or the first of every four notes in doublets. Of course, when Paco de Lucía or Sabicas play these kinds of runs at high speed, you cannot hear these accents, but believe you me, they know that they are there. Sincerely, La Vikinga Comments by acting editor, Paco Sevilla: The omission of some of Roberto's comments on this point was due to my not then, nor now, understanding the value of the point you are making. In my years of teaching hundreds of people to play flamenco guitar, I have never come across a person who did not $ \\underline{\\text{naturally}} $ accent the first note of each triplet, that is, the note which falls on the beat or the foot tap. Perhaps I am missing the point, but I do not understand how one could play any other way. On a related subject, I found an interesting point in the valuable article on Manolo de Huelva by Virginia de Zayas in the recent $ \\underline{\\text{Guitar Review}} $ #43. The point was made that, \"When the guitarist plays $ \\underline{\\text{falsetas}} $, the ligated notes give light and shade of three strengths.\" I was puzzled by that comment for awhile, until I decided that the author must be referring to triplet ligado passages played with the thumb, as in the example given here. In each beat there are three different note, a non-plucked note, and an unaccented plucked note. The author goes on to say that this effect is lost in modern playing where so much picado is used; with picado, there are either two sounds, accented and unaccented, or one homogeneous sound with emphasis on the major compás beats. A minor, but interesting point. Dear Jaleo, ...Los felicito y haré todo lo que de mi parte esté para que $ \\underline{\\text{Jaleo}} $ lo conozcan por esta parte del Canada. Espero que me comuniquen, si quieren que los envíe algún material de estas actividades que por aquí llevo realizando. El día 2 estuve a ver al amigo Morca y su Sra., así como al guitarrista, Gary Hayes. Las alegrias de Morca son magníficas, bueno, como maestro y amigo, me pareció muy bien. Hasta pronto amigos, Angel Monzón Vancouver, B.C. Canada (The following is from one of the founders of $ \\underline{\\text{Jaleo}} $, now living in Iran) You should be preparing for the next juerga by the time this letter arrives. I suspect that the newsletter is in the final stages of frantic last-minute preparations. Jesus is prompt with his photographs, as usual, and perhaps writing articles. Paco is beating the bushes for the \"meat\" of flamenco and the contents of the next $ \\underline{\\text{Jaleo}} $. And Juana, the hardest working Jaleista of them all, is generously providing help in all other areas where there is a need. I can envision your contingent of Spanish ladies, with their castanets softly purring, like cars with their motors running, in anticipation of their mid-month communion with fond memories of Spain. Sumptuous food is being prepared and costumes made ready. Jack Jackson is carefully selecting an itinerary of music to make sure that the evening starts in the right mood. María Teresa Gómez will arrive with her troup of pint-sized gitanas to inspire and warm the hearts of everyone in the room. Ernest Lenshaw will squeeze more ladies than the rest of us put together. Yuris won't say much, but is always present, articulately expressing himself on the guitar. More people arrive, the air thickens, the wine flows. Duende sparks as the evening reaches critical mass. There are many new faces and new members. Deanna is uninhibited and sexy. Jesús is cruising around in seventh heaven. Juana is managing to greet and entertain 175 new arrivals, plus dance and singing and collect money on the side. At midnight, Rosala takes the floor and captures those of us who are weak of heart with her \"sensual intensity.\"",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 4",
    "page_number": 3,
    "word_count": 796,
    "article_char_count_full": 4633,
    "article_char_count_review": 4633,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A4",
    "article_text_for_review": "HOW PURE IS PURE? I have a bone to pick with the holier-than-thou flamenco purists. The attitude that \"my flamenco is more pure than your flamenco,\" sometimes really rubs against the grain. Any ethnic dance, when removed from its original setting (usually the plaza of a small pueblo) and put on stage, is no longer \"authentic.\" There must be special lighting the music is often canned or amplified, and choreography must be set to insure a uniform performance. I remember once seeing a company from India that advertised completely authentic, unadulterated, folk dances. The dancers filed through the same monotonous pattern umptine times, and it was the biggest bore imaginable. Flamenco is the product of enculturation by some of the most creative and innovative people on earth, a people who have borrowed from many different countries and cultures. When I am told that castanets or the bata de cola have no place in flamenco, that women should not do heelwork, duo dances are a \"no-no,\" etc., I say, \"Let's carry this theme to the ridiculous, ultimate extreme! How about shoes? Shouldn't we go back to dancing barefoot? Let's get really authentic and eliminate plastic earrings and nylon ruffles (we can go back to ironing and starching each cotton ruffle for hours; I can remember a time when I spent more time ironing ruffles than I did performing on stage). How about the music? How authentic are the new falsetas and techniques? Why don't we eliminate the guitar altogether and go back to palmas and other percussion instruments? One of the reasons that the San Diego Flamenco Association has flourished is that we have avoided, thus far, the elitest snobbery that inhibits participation and stifles creativity. We have a mixture of Spaniards and non-Spaniards, professional performers and beginning students, and long-time aficionados as well as those just recently introduced to flamenco. There is an attitude of sharing and tolerance at the juergas which allows people the freedom to participate. If the non-Spaniards felt that they were being criticized by the Spaniards for their lack of authenticity, or students by professionals for their imperfect technique, or newcomers by the aficionados for their limited knowledge of flamenco, everyone would be immobilized by the fear that they might be doing something \"wrong\" and the juergas would come to a screeching halt. One of the difficulties in teaching flamenco and the beauties of executing it, is that there is no \"right\" way and, therefore unlimited possibilities of expression. At the juergas there are no two alike: Rayna speaks articulately with her heelwork, Rosala and Luana with the sinuous movements of their arms and torsos, Julia Romero, with one shake of her shoulders, has the whole room in the palm of her hand, and Juanita Franco has the entire bag of pellizcos at her disposal. I believe that there is room for both the old and the new, for the traditional and the innovative, and that, as a thriving and evolving art form, flamenco has room for and can encompass a great variety of styles, approaches, and techniques. Participants should be judged on their own unique merits and not by someone's imaginary or antiquated measuring stick! Juana De Alva * * * * * BULERÍAS OF THE 70's In May, Paco Sevilla offered a class in bullerías for intermediate and advanced students. Fourteen people showed up for the class which began at 7:00 p.m. Paco began this class with a brief discussion of the Andalusian region of Spain (cradle of flamenco) and explained where many of the most noted cantaores and guitarristas come from. This flamenco geography lesson made it clear that there are various regional flamenco styles and that they are interrelated. For example, Paco described the flamenco atmosphere of Madrid as being a flamenco melting-pot, and the guitar playing is characterized by \"tricks\" -- \"You've got to be tricky to play in Madrid.\" Tricky means using many unusual techniques, falsetas and compas structures. This trickiness seems to be the result of many flamencos from different regions competing and trading their respective \"trucos de tocar.\" aside from those listed above, these included Manuel Molina, Pedro Peña, Dieguito de Morón, Juan \"Habichuela,\" Parilla de Jerez, and Paco Cepero. -- to expose the participants to modern singers by having most of the recorded examples feature the guitarists as singing accompanists.",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "4, 5, 6",
    "page_number": 4,
    "word_count": 723,
    "article_char_count_full": 4418,
    "article_char_count_review": 4418,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A5",
    "article_text_for_review": "(The first in a series of articles about \"upgrading the flamenco market.\") La Vikinga and Roberto Reyes $ \\underline{\\text{New York Times}} $: Anna Kisselgoff wrote this about the Ballet Nacional Festivales de Espana, \"...the company now has flamenco artists of authority. By choice, they opt for the popular over the deep, but that is their privilege.\" New York Post: dance and drama critic, Clive Barnes, observes, \"Now we are reduced to smaller, somewhat less distinguished groups, such as that of the Madrid-born Jose Molina...There was a lack of what the Spaniards call DUENDE, a lack of that personal sense of matador-tragedy that is the essence of Spanish dance at its noblest and most ennobling.\" Dance Magazine: Critic, Linda Small, remarked about the Ballet Nacional Festivales de Espana, \"...the Andalusian taranto, alegrias, and bulerías are not museum pieces and cannot be danced well without feeling and without the spontaneity of the moment.\" For all artists in any art form, the future is never secure....but right now, flamenco is at its lowest ebb. People expect to see the Spanish stereotype in flamenco, regardless of the abilities of the performers. When the day comes that we can introduce flamenco with the artist's real name, Joyce Roth, rather than the traditional Juanita Reyes, flamenco will have come a long way. To give you an idea how far we are from that, guitarists are still busy learning millions of falsetas and not taking enough time to learn and study flamenco dance. Sure, we're doing all the obvious things, ending llamadas together, going into bulerías, etc., but accompanying requires much more than that. Top-notch accompanists such as El Marote and Habichuela see a dancer for the first time and they can tell immediately from what teachers, if any, the dancer has studied. For example, the straight modern arms of Paco Fernández and María Merced are quite different from the flowing arms of Matilde Coral and Flora Albaicín. But more importantly, each dancer or teacher has his/her favorite way of creating rhythm patterns within a given rhythm. In soleares, Carmen Amaya felt more comfortable and creative working in fast triplets and accenting from the 12th beat. Paco Fernández and María Merced love to work in quadruplets, starting from the 12th beat. On the other hand, Manuela Vargas and Jose Quintero like to work the accent and feeling coming from the 1st beat, mostly in triplets. It is with this kind of knowledge that quality creating can be done at the moment. Knowledge of the song and the styles of different singers is even more crucial. ing the immortal Ramon Montoya (uncle of Carlos) and beginning to create his own music in a new but very flamenco style. More importantly, he had gained an uncanny mastery of $ \\underline{\\text{compás}} $. In flamenco, $ \\underline{\\text{compás}} $ means measure; it refers to the often complex rhythmic cycles which both underlie and define most traditional forms. To say it is indispensable is to understate the case, because, without $ \\underline{\\text{compás}} $, these distinctive forms dissolve into aimless, nameless, non-flamenco meanings. Sabicas has internalized these flamenco rhythms and he manipulates them brilliantly. It was this ability, rather than his unbelievable digital dexterity, which led Carmen Amaya to seek out Sabicas to accompany her amazing footwork. It was $ \\underline{\\text{compás}} $ that helped clinch the title of $ \\underline{\\text{número}} $ uno for Sabicas.",
    "title": "FOR LACK OF KNOWLEDGE, FLAMENCO LANGUISHES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "6, 7",
    "page_number": 6,
    "word_count": 560,
    "article_char_count_full": 3492,
    "article_char_count_review": 3492,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_07::A6",
    "article_text_for_review": "On May 20, 1978, Roberto Aguilar (Bob Strack), an ardent flamenco aficionado and solo flamenco guitarist in New York City, died at the young age of 47. He was widely known among flamencos here and abroad for his enthusiasm and encouragement. As a flamenco personality, he was unique. In the professional flamenco world, more often than not, flamencos express negative statements toward one another. Roberto, on the other hand, always had something good to say about the different artists, continually reminding of the beauty of each individual way of performing. He made a point of supporting flamenco by attending concerts and frequenting the local flamenco \"hang-outs.\" The \"house guitarists\" would always invite him to play. For me and the proprietors, he was a welcome sight....such a healthy, good-looking man who silently commanded attention just by his over 6 foot 4, about 350 pound frame. When I worked at La Sangria Restaurant on Hudson St., he was a regular customer, ever ready to play a couple of guitar duets and solos. If it was a good night, he would sing and play in any language requested (his second love of music was country-western). He created an ambiente that everyone could share. Seated next to his lovely wife, Karin, he seemed SO powerful, yet he would delicately strum the guitar with the greatest finesse and sip a screwdriver, being careful not to moisten his majestic \"Vandyke.\" by Roberto Reyes in death, Roberto exudes the same powerful elegance. Looking at his face, so many memories come flooding back. He had that same expression of concentration that he had some 15 years ago, on a summer Sunday afternoon, sitting on a small wooden stool in the middle of Washington Park, playing bulerías. With thousands of people crammed around different musicians playing repertoire from country to classical, ethnic to popular, it must have been predetermined that I would stop to listen to Roberto Aguilar. After admiring his music, I asked if he would give me guitar lessons. He graciously agreed. I started to study flamenco with him as a hobby, but Roberto inspired me to become a professional and suggested that I study flamenco dance as well (I heeded his advice and studied with Mariquita Flores at Ballet Arts in Carnegie Hall). Roberto warned me that it was very difficult for a non-Spaniard to learn flamenco. He taught me at an early stage that there's a lot of mystery attached to flamenco, and that he didn't, by any means, have all the answers. During the mid-60's, Roberto Aguilar played solo guitar for four years in the renowned Jai-Lai Restaurant on Bank Street in Greenwich Village when flamenco was in its heyday. What recollections I have of waiting in line outside the restaurant, where people waited patiently for the good food and a table near the guitarist. Roberto always greeted everyone warmly, making a point of remembering their names and professions. And what magnanimity! He continually invited visiting guitarists to play, whether they were professional or aspiring students....a rare occurrence in the paranoiac world of flamenco! He made other dreams come true. The wish of every flamenco guitarist is to find a vintage Spanish-constructed guitar, and Roberto always had a good collection (I bought my first guitar from him). When we shared a cab coming home from the Jose Molina concert, I told him that I had solved some of the mysteries of flamenco that he had spoken of 15 years before. At the Sabicas concert, he approached me in the lobby of Town Hall and said that he couldn't sleep for days, thinking about the enigma I had uncovered. We promised to get together real soon. Unfortunately, now, I'm too late! MORE ON ROBERT STRACK by Gene St. Louis It is with sincere sadness that I announce the death of one of our members (Robert was one of the first in the New York area to become a member of Jaleistas) and a dear friend, Robert G. Strack, flamenco guitarist, in New York City, on 5/22/78. He leaves behind his loving wife, Karin, and daughter, Barbara, by his first marriage. With him goes the memory of a gifted entertainer who pursued the life of a flamenco by immersing himself in the art to an enviable degree. His knowledge of flamenco, his storehouse of falsetas, and his willingness to share these were paramount in establishing him as one of the most popular and knowledgeable flamencos in the area. To me he was someone special because ten years ago he was my introduction to New York City and the flamenco nightlife. From that point on we became the best of friends, and he offered me inspiration and guidance in both my guitar and personal life. One of his unique qualities was his uncanny ability to mix with people and get them into a festive mood. He could draw the shyness right out of a person and have him function as part of a group. This one quality, more than any other, made him not only popular, but valuable. This is being written not only for myself, but for all those who were fortunate enough to have been touched by this huge man of gentle ways.",
    "title": "One Magnanimous Flamenco Lost",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_07",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "8, 9",
    "page_number": 8,
    "word_count": 868,
    "article_char_count_full": 5044,
    "article_char_count_review": 5044,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
