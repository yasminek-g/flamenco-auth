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
    "article_id": "JALEO_1978_11::A15",
    "article_text_for_review": "by Raul Botello Jr. There was little doubt which house the fiesta was in. The castanets sounded sharply in the cool night air as we walked closer to the \"juerga\". Inside I found the familiar hustle and mood of enthusiastic discourse, guitars straining to the flamenco rhythms and glasses clinking as they were being filled. Dancers were cheered and applauded by spectators sitting on the floor surrounding the dancing area. Hands clapped sharply with the tempo. Often I ask myself the same questions at these fiestas. How can these beautiful people who for the most part are not Spaniard or even Hispanic, be so delighted and incensed by the flamenco art? Smiling eyes in an expressive environment follow the movement and pattern of a musical force so physical that it impels as well as it compels. Performers erupt in heated spontaniety. This mystic sound captures the unwary soul so completely, while at the same time it eludes others. sure was serene like a bird gracefully gliding through a clear blue sky. Her music was uncluttered, methodical, and refreshing. Diego Robles accompanied by Raquel She sat loosely in a low chair in the dim light, a dark cigarette protruding from her small, firm mouth. Miniature, delicate hands moved feverishly up and across the guitar strings. Her small, dark eyes gazed from one side to another. I observed how she avoided direct eye confrontation with anyone while she played. She was detatched, cool, and in total control of her own world. She played unceasingly for what seemed hours compressed into a fleeting moment. It had been a glimpse of the artist passing by. Later in the garden a quartet of guitarists hunched over their instruments, their nimble fingers plucking at the strings, tightly then loosely, struggling for unison. Fragments of sevillanas started and ended discordantly. Then fragments of bulerías and tanguillos filled the air. Chattering castanets sounded from a corner. My daughter Michelle, agile and somewhat talented, entered the inner circle of dancers. In the damp evening chill she swirled one way and then another, the white polka-dotted blue ruffles of her flamenco dress rising. Her hands moved like serpents above the yellow flower in her hair. I've learned to participate in my own small way. By feeling out the rhythms and tempos, it has not been difficult to develop hand clapping in unision with the music. In Spain I often found myself hedged in the clans on the \"feria\" streets. The singing and dancing pace was hectic and endless. I felt committed to being a part of the action and learned some of the hand clapping methods. Ironically, I learned objectively in order to develop an art which ultimately is performed spontaneously and instinctively. I am not professionally capacitated, but can at least enjoy some measure of participation.",
    "title": "SEPT. JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_11",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "19, 20",
    "page_number": 19,
    "word_count": 467,
    "article_char_count_full": 2821,
    "article_char_count_review": 2821,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_11::A16",
    "article_text_for_review": "THE HOSTS' POINT OF VIEW by Francisco and Elizabeth Ballardo It was with the greatest of pleasure that we, my wife and I, offered our home for the Oct. 21st juerga, which I believe turned out to be one more successful flamenco festival. Since the juerga, we have had a few days to reflect on the positive and few negative aspects evidenced in the preparation and the event itself. In the interest of maintaining a successful continuity of the monthly juerga, we would like to share some thoughts with all members and interested friends of flamenco so that other Jaleistas may become better informed and perhaps consider offering their homes for a juerga. First and most important was the experience we gained in the preparation and the enjoyment of the juerga itself. The preparation process was indeed a systematic plan worked out to the last detail by Juana de Alva, assisted by a few Jaleistas. The chronology of events covering only the highlights was in three steps: Some three weeks prior to the juerga a survey was made to determine the facilities, including parking, the areas the hosts wished to provide for use, and a general discussion is held to ensure that the hosts can safeguard their domestic tranquility. The second visitation about a week prior to the juerga was made by Juana and it's main objective was to translate the previous survey into a working pro- Sevillanas being taught by Juana de Alva and Julia Romero to Tony Heller and Yolanda France at the October juerga. cess. Here we were to observe the micro- details such as space utilization, geographic layout; tables where and how many, impro- visation of bars for wines and beer, light- ing effects, the placement of the tables and seating accommodations with eating utensiles, and a multitude of minor details--by the time she had completed this \"milestone\" we had the feeling that she knew our home bet- ter than we did, at least in its total utili- zation. The last phase took place a few hours prior to the juerga--here again it was Juana with her \"van\" equipped with the danc- ing \"tablas\" and many of the other provisions required to give the final touches to make the juerga possible. At this point we gasp for air and looking in retrospect we wonder if juergas of this scale (150 people) could be possible without the dedication, tenacity, and love for the flamenco art of a few jaleistas like Juana. During the juerga, the only negative incident of any import were the discomforts of the few babies not of walking age who missed the sevillanas class and the fact that it was all over by 5:00 A.M. The bottom line of our participa- tion as hosts and the experience gained con- vinces us that a repeat performance will be favorably considered--and to all the jaleistas, we say OLE! JUNTA MEETING NEW DIRECTIONS FOR JALEISTAS? Since June of this year, junta meetings have been held each month on the Wednesday following the juerga. The junta was formed in response to the need for some sort of organization to steer the course and share the work of Jaleistas. It is comprised, at present, of any and all interested members who wish to be involved. The structure is loose. Some appropriate Spanish titles were established for jobs that are specific to juergas, the Jaleista organization and Jaleo. V. Excluding small children from the juerga was proposed and discussed. Reasons were possible injury or mischievousness. It was pointed out that up until now there has been little or no problem and to seek member's feedback. VI. Prohibiting hard liquor was discussed and abandoned for the same reason; thus far there has been no problem. VII. New Year's Eve juerga: It was proposed that the December juerga be held on New Year's Eve as last year, and be combined with the Casa Espana club since many of our members are associated with both clubs. VIII. Raising of guest admission fees to hold down juerga size and add to Jaleista revenues. 3, 4, and 5 dollars were proposed. Further discussion needed. IX. Need to elect traditional officers; President, Vice President, etc. to meet the requirements of corporation rules. Proposed that Juana De Alva be acting president and Francisco Ballardo be acting vice-president until elections take place. X. The need to promote JALEO advertising: JALEO revenues need to be raised so that juerga income is no longer usurped for production of the newsletter and also to enable us to put some of the JALEO staff on salary. Mickie Ann Jackson volunteered to help with add promotion. There are still many positions unfilled and help needed, in the following areas: JALEO: $ \\underline{\\text{Managing editor-}} $(coordinates the production of JALEO) $ \\underline{\\text{Copy Editors -}} $(involves all manner of creative and technical jobs related to layout and assembly of the newsletter) $ \\underline{\\text{Reportero/a -}} $(In charge of writing or soliciting writers to cover juergas and other local events.) JUERGAS: $ \\underline{\\text{Decorador/a - (Sets up before juergas - decorates, arranges}} $ furniture and lighting for best atmosphere.) $ \\underline{\\text{Cantinero/a - (Keeper of the}} $ cantina - food and drink area) $ \\underline{\\text{Recogedor/a - (Picker-upper -}} $ clean up after juergas) Come to the junta meeting if you wish to $ \\underline{\\text{play}} $ a part in the future of JALEISTAS. For location of next meeting contact Carolina Mouritzen at222-5700.",
    "title": "DETOBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_11",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "20, 21, 22",
    "page_number": 20,
    "word_count": 901,
    "article_char_count_full": 5393,
    "article_char_count_review": 5393,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_11::A17",
    "article_text_for_review": "This month's juerga will be held at the home of Stefano Putzolu. Born in Rome, Italy, he spent his 45 day vacations in the U.S. for five consecutive years and \\\"finally decided to stay\\\". Trained in jewelry appraisal and sales, he will be opening a clothing botique for men and women in La Jolla at the end of this month called Fiorucci. Stefano's interest in flamenco sprang from his father, Giovanni, who studied flamenco dance in Spain and performed for several years. \\\"My father sent me to Madrid with directions to some of the good salas de fiestas,\\\" he says. \\\"I cried the first time I saw flamenco there.\\\" He was introduced to Jaleistas by singer Rafael Satillana. Other house members are: Dr. Riggs Roberts who studies piano, Rafael de la Barera, born in Peru of Spanish descent and Greg De Lira, a graduate in business. All are friends of Rafael Satillana and have enjoyed Rosala's dancing at previous parties in their home. There will also be two young ladies present who will be helping out; Kathline Tushinsky, a Polish friend of Stefano and Nancy, a friend of Riggs who is doing all the artistic indoor signs for us. Stefano's home has great juerga atmosphere. There will be three indoor areas, La Sala Hundida, La Sala Safari, and La Cantina and one outdoor area. It is in the same area as the September juerga. Take the Garnet turn off of freeway 5, take a right on Ingraham which will curve left becoming Foothill and bear left again on Turquoise. Turn right on Dawes and right again on Archer. Don't forget to provide food and drink for your guests or tell them what to bring. Date: November 18th\\nPlace: 1148 Archer, Pacific Beach\\nTime: 7:00 p.m. to?\\nPhone: 488-7020\\nBring: Food according to guide below and what ever you like to drink.\\nGuest donations: $2.00\\nFood guide according to first letter of last name:\\nA - De - Dessert\\nDf - J - Bread or chips and dip\\nK - M - Main dish\\nN - Se - Salad\\nSf - Z - Main dish",
    "title": "NOVEMBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_11",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "22",
    "page_number": 22,
    "word_count": 343,
    "article_char_count_full": 1942,
    "article_char_count_review": 1942,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_05::A1",
    "article_text_for_review": "The March 1978 issue of $ \\underline{\\text{Jaleo}} $ included a \"miscellany\" photo of gypsies partying in the cave home of Manolito el de la María; the unnamed guitarist was Agustín Ríos de Morón who is now living in Mill Valley and performing in the San Francisco area. Agustín, a gypsy whose guitar-playing is familiar to those who have spent time in Morón de la Frontera or listened to the many tapes of juergas there, is a nephew of the legendary Diego del Gastor and plays a mixture of his own and Diego's material. Now 30 years old, Agustín toured Europe in 1972 with La Singla and for the last two years has been performing concerts in the United States. He appears in the film \"La Vida Flamenca\" (16mm, 20 minutes long, now available for distribution in this country) along with relatives and friends such as his sisters Milagro and Eugenia, brother Pepe, Ansonini, La Chica, La Pili, Pura and others. He is currently working on a record, teaching, and doing concerts and fiestas; a recent performance paired him with dancer-singer Ansonini who is visiting in the Bay area. For concert or film information, contact Patrice Thompson: (415) 387-8403; we thank Patrice for this information. (For some experiences with Agustín, see \"Dance Experiences in Spain, Part II\" by Suzanne Keyser, Jaleo, September 1978) EDITORIAL by Paco Sevilla Last month we told you about the glowing, optimistic side of the situation with $ \\underline{\\text{Jaleo}} $. However, there is another side: $ \\underline{\\text{Jaleo}} $ has some serious problems and if they are not dealt with, there may be no newsletter to enjoy the exciting things that are happening. I hope that every reader will seriously consider the following: At the present time, if I were to leave Jaleo (a constant temptation, for the purpose of getting back to normal life), it would very likely cease to exist in its present form. If Juana de Alva were to quit and return her life to sanity, Jaleo would cease to exist. This is not a desirable situation for a publication. We are fortunate to have our faithful typist María Soleá (many hours of unpaid work), Emilia Thompson and Elizabeth Ballardo doing the very difficult and tedious chore of getting Jaleo to the readers (collate, fold, staple, address, arrange zip codes in order, bundle, weigh, mail -- with all sorts of special mailing requirements), and Deanna Davis taking care of all the subscription hassles and letter writing. If any of these people gets sick of the hassle, we are in trouble. And you may have noticed that our borrowed type-writer needs work -- if it goes, we are really in trouble! The actual layout of each issue is being done by one or two people and takes weeks -- which is why we are behind in mailing. The burden of supplying material for us to publish has fallen on the shoulders of just a few people. Financially Jaleo is not well. We spend almost all of our money on the layout, printing, and mailing. We are a much better newsletter than we should be (larger and more photos) for the amount of money we take in. At any time, we could find ourselves broke and unable to go on.",
    "title": "AGUSTIN RIOS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_05",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2",
    "page_number": 1,
    "word_count": 543,
    "article_char_count_full": 3119,
    "article_char_count_review": 3119,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_05::A2",
    "article_text_for_review": "In 1961, I went to Granada to study flamenco with the gypsies. That fact alone should fully demonstrate the depth of my ignorance at the time. I was unaware of the geographical constrictions on the art, which tend to confine the good stuff to the Seville/Jerez axis and environs thereof. But God takes care of drunks, children and befuddled flamenco freaks. I had come to learn guitar, and after reluctantly rejecting some tempting offers in unrelated areas, I found Pepe Tranca. Pepe--Jose Maldonado Cortes, technically speaking -- was considered a bit strange by some of the other Gypsies. He had actually taken the time and trouble to learn good guitar, despite the fact that people apparently preferred bad guitar. (Actually, people didn't prefer the guitar at all. In the caves, the lead melody was generally taken by the so-called Portuguese guitar -- an instrument that uses doubled steel strings, as I recall, and sounds like an overgrown mandolin.) Pepe worked in the cave of Maria La Canastera, and was married to a striking young woman who may have been her daughter. (My Spanish was pretty bad at the time; for that matter, so was Pepe's -- his accent was all but impenetrable.) They evidently lived in the caves, and I took lessons in a small room carved into the Sacromonte hillside. The air inside was delightfully cool. Unfortunately, the flies appreciated this as much as I did, and at times there would be one on each finger as I tried to mimic Pepe's music. Every few minutes he would call a fly break and we would waggle a white sheet from the back of the room toward the door to the evident amusement of the flies themselves, who had learned to hide in the brasswear until the humans finished their mysterious ritual. Pepe was my first real live Gypsy, and I was evidently his first real live $ \\underline{\\text{discipulo extranjero}} $. He had light skin, which puzzled me a bit. And perhaps he noticed, for one day he looked at me intently. \"I may be white,\" he said, \"but my heart is black.\" Two years later I came back to Spain and headed for Granada to find Pepe. I did, but things had changed for the worse. Torrential rains had crumbled many of the habitable caves, and while the showplace caves were open for business most of the Gypsies had been relocated to flimsy, tin-roofed, windowless quonset huts beneath the Sacromonte. I can't imagine what they were like in the winter cold, but in summer the roof functioned as a broiler. It was hot. During that time we had often wondered what had become of Pepe and his family. I was hardly optimistic as we headed up the narrow road toward the Sacromonte, squeezing the dinky Seat sedan into doorways to dodge the convoys of tourist buses. Our children were still excited from the afternoon spent wandering through the Alhambra. The gypsies, heedless of my dashing and clearly Continental appearance, inexplicably took me for an American. \"You want see dancing girls, see real flamenco show?\" they hollered in English as they ran alongside the car. \"No,\" I said pettishly as I continued along the road. \"I'm looking for Pepe ESTER MORENO, NOW TEACHING IN SAN DIEGO ON SATURDAYS; CALL HER OR CONTACT PACO SEVILLA: 292-2837 ester luisa moreno international flamenco artist seek a compromise on that amount. Pepe insisted that many extranjeros -- mostly French -- were paying the going rate. Still he knew that America had fallen on hard times, and besides I had bought some nice things for his children when they were little, and what the hell -- just this once! Was it worth it? Absolutely! First, I had the chance to tie up a few loose musical ends that had been bothering me for a decade and a half. I also had a chance to evaluate Pepe's playing from a broader perspective: an interesting, hard-driving style which seems impossible given his lack of fingernails; and occasional imaginative falsetas, although not as many as I had hoped for considering the high quality of the material he gave me years ago. And it was worth it because of the financial contre-temps as well, I thought. After all, anyone can rip you off -- but only a few gifted gitanos can do it in such a way that you're grateful to them, and feel you've gotten the best of the deal. A modern-sounding falseta for its time; while the G minor usage is quite old (Javier Molina used it very early in this century) its use in relation to F is progressive. Diego liked Tranca's falseta, and I showed it to him in 1970. Soon afterwards, he was playing his own version in his own inimitable style. The beginning is relatively unchanged (I frequently didn't even recognize his transformations of material I showed him) but the end is pure Diego. SIGUIRIYAS - GRANADA 1961 Interesting tonality which recalls Melchor de Marchena's siguiriyas.",
    "title": "Granada By Brook Zern Recollection, Reunion, and Remuneration",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_05",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "1, 17, 18, 19",
    "page_number": 1,
    "word_count": 833,
    "article_char_count_full": 4777,
    "article_char_count_review": 4777,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
