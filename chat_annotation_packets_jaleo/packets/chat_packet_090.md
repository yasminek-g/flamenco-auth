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
    "article_id": "JALEO_1980_11::A2",
    "article_text_for_review": "It was 1960. It was opening night for me and our group \"Los Flamencos\" at the Casa Madrid in Los Angeles. Typical first night nerves were there but with great artists like Pepe Segundo singing and guitarists Benito Palacios and Rogelio Roguera, I was looking forward to a month of fun and exciting flamenco. For the opening show, I was scheduled to dance my alegrías. We went out on stage. It was a full house. Also adding to the nerves was the fact that we were following a long engagement of Carmen Amaya and Company and, needless to say, following such an artist was rough on the nerves. We opened with sevillanas and when we finished, I heard some ring-side jaleo coming from a familiar voice. It was Carmen Amaya! My knees almost buckled. Carmen had never seen me dance, and for me to dance my alegría, for this person who had revolutionized that compás, was a bit much. She gave me a big smile and I knew she was there to enjoy. Pepe started to sing and I began to dance. I don't remember a thing, but I guess it went well for this time in my life. After the performance, I joined her table and she passed me her glass of champagne. I will never forget that evening. She was a great lady and I feel so fulfilled that I was able to see such a great artist and to know her as a great and giving person. I will never forget taking my mother to see her concert and seeing my mother moved to tears. That kind of feeling was a lesson; it was spontaneous feeling. I feel that Carmen Amaya in the lesson of her sincerity, her energy, her feeling, her emotion, and her living passion for the art of flamenco, has left us all a legacy. In every art there are the few originals, the inventive creators who inspire others, who change the art or redirect its natural, traditional evolution. Carmen Amaya was a very powerful force in the art of flamenco. Her originality inspired many and her artistry became international flamenco, traveling to all corners of the world. When she came to Los Angeles in the mid-1950's after a long absence, Carmen was returning at a time when Spanish and flamenco were reaching a very high popularity throughout the U.S.A. I had been studying dance for about six years when I first saw Carmen Amaya. I had seen many fine artists and intuitively felt that I had some knowledge of my direction in dance and already knew that it was a definite part of my being. I was not prepared for one of the concerts that I saw of Carmen Amaya, a concert that changed my life. The curtain opened and, after an opening number by the company, Carmen Amaya walked out on stage, rather she stalked out to center stage where she just looked out at the audience, and received a thunderous standing ovation. And this was just the opening! I was awe-struck with the energy she radiated in just walking, with that tiger-like way of movement; the energy, power, emotion and total spontaneous control were things that immediately etched into my being and it is the essence of that totality of artistic, individual force that so inspired me when I first saw her.",
    "title": "INNOVATOR, & CREATIVE FORCE\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_11",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 561,
    "article_char_count_full": 3061,
    "article_char_count_review": 3061,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_11::A3",
    "article_text_for_review": "Sit back for a minute and consider. Why are you in flamenco, anyway? How did you get mixed up in this art form? Remember, you weren't born in Andalucía; you weren't breastfed to the compás of bulerías palmas; you had no ambiente to draw upon when you were in your youth. So, why? Everyone's reasons vary slightly. There is something in the music that attracted you. How does this wonderful music attract people outside of Spain and change their lives forever? Let's examine the question closely by making a list of possibilities. Check the following if they apply to you: —I heard records and was compelled to find out more. I am of Hispanic descent and feel it's part of my culture. ___I visited Spain and saw flamenco. ___Spanish flamencos were performing in my town. ___I like all kinds of music and art. ___I wanted to be different. $ \\underline{\\text{Friends}} $ introduced me to it. I love the E chord (seriously!). I am a gypsy from New York. I am a guitarist and enjoy all guitar styles I am a dancer and like all dance. $ \\underline{\\text{}} $ a singer and like most kinds of songs. $ \\underline{\\text{}} $ I saw flamenco books in the library or bookstore. I saw flamenco on television (Johnny Carson Ed Sullivan, etc.). ___I heard Vicente Gómez play on the radio years ago. There are endless reasons why people get into our art form. One of the most interesting things is that there is a rebel, non-conformist side to almost everyone involved in flamenco. Those of us not having the advantage of being born in Spain chose flamenco. This is a very important point, since we did not have flamenco to grow up with. Many in Spain also chose to be flamenco, but many were chosen naturally since it was in the family. They cannot go back and eliminate the constant exposure, the ambiente advantage. You might say that flamenco chose them. The way I see it, after years of observation, is that to be flamenco means to have primary control over your life. Not all flamencos have this control, but this is the ideal they are striving for. Something in the music says \"I am the boss\", \"I do things my way\". Most flamenco people want to be at the helm. The exceptions are the rookies and the people who get into flamenco as a hobby. Even these have a hint of this desire to be in primary control. So then the answer is very simple: Why flamenco? To escape the control of others or to have the fantasy of being in control of our lives if we can't actually escape. It all goes back to the persecution of the gypsies, Arabs, Jews, and other \"undesirables who helped create flamenco. Thanks to this ugly chapter in history, flamenco developed into a meaningful art form. Even today, some still maintain this 500 year chip on their shoulders. However, beyond this is the desire for that primary control. Wake up when you come to; eat when you're hungry, come and go as you please; be free from supervision. If what I am saying is true, then flamenco will never die, even though it may change slightly in the coming centuries. * * *",
    "title": "GUILLERMO,\"WHY FLAMENCO?\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_11",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 26,
    "word_count": 547,
    "article_char_count_full": 3025,
    "article_char_count_review": 3025,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_11::A4",
    "article_text_for_review": "I am happy to own this fine album by Paco del Gastor, even though it is not at all what I expected. The Morón influence is detectable in a few bulerias but, for the most part, Paco plays in concert style. Sources tell me that it is due to his experiences in Madrid with his friend Paco de Lucía. Apparently, he went to Madrid and blew everyone's mind years ago. Many tabbed him as the future messiah of flamenco guitar. What happened was that he was blown away too, to the point that he was influenced much more than he influenced others. \"Tabaco y Té\" is the opening number, a zambra attributed to Sabicas. The orchestra of José Valero joins Paco for his rendition. Outside of a few passages I didn't recognize it as being distinctly Sabicas. \"Sales de Cádiz\" is a concert style alegria in E. It is nicely done by any standards; a few variations are by Mario Escudero, taken directly from an old Folkways album. Juan del Gastor joins his brother in \"Recuerdo a Diego del Gastor\", a funky Morón bulería at last. The two are great together and get lots of aire since they are in their own salsa. The orchestra again accompanies Paco in \"Cal de Morón\", a creative bulería. Paco shows that he is capable of his own kind of creativity and it's excellent. The orchestra does not enhance, but is bearable.",
    "title": "PACO DEL GASTOR\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_11",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 26,
    "word_count": 238,
    "article_char_count_full": 1299,
    "article_char_count_review": 1299,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_11::A5",
    "article_text_for_review": "By Marta del Cid Before we actually get into the technical end of this segment, I would like to ask your indulgence while I digress for a moment. In Skirts Part I (August Jaleo) I discussed skirts that would be suitable for class and practice use and described the construction of gathered ruffles and tiered skirts. I have since heard from Jaleo's editor, who opined that he is one who sometimes prefers that type of costume, worn with a tie-front gypsy blouse, to the more modern circular ruffled dress commonly seen today. I did not mean to give the impression that this type of skirt is entirely obsolete as a performing costume -- as Paco says, it is spectacularly effective when worn by a gypsy style dancer. It does seem that some dances, by nature of their compás, are more conducive to barefoot interpretation, namely those more primitive or Arabic feeling rhythms such as tangos, zambra, tientos, tarantos and others, which are characterized by a strong, self-propelling drive occasionally climaxed by desplantes. They have a very different feeling of flow and pacing from the 12 count compases and seem to adapt more naturally to the funky, mellow sounds of flesh on wood. Because of all these elements, and with less happening in the legs, the \"La Chunga\" type of skirt, with its soft, old fashioned flow, enhances in a way that the more sophisticated costumes never could. The only 12 count baile that feels right at home with this technique is the siguiriyas, although there are some who don't consider it in terms of 12 counts -- indeed there seem to be as many ways to count siguiryas as there are Basic Skirt You may start with a basic pattern of 4, 6, or 8 gores or sections depending on the amount of fullness you want. A narrower 4 seamed skirt gives a cleaner, more tailored look where most of the emphasis would be in the ruffle, which should be more structured with interfacing and cord to make it stand out. Anything softer on this style won't be as effective. If you go with a fuller 8 section skirt, which is the most popular, you can use any weight ruffle and it will look fine. Here are a few suggestions for commercial 8-gore skirt patterns in the November books: Simplicity: #9502 Butterick: #6898; 6860; 3137 #7757 Some of these patterns are fuller than others, but whatever you get will need alteration. 1) As before, first figure the total length of the finished skirt. Second, figure the finished depth of the ruffle, subtracting from this the width of whatever trimming you are planning for the bottom. I would recommend no more than about 11\" for total ruffle depth. Next subtract the ruffle depth and you will have the measurement of the length of the skirt top -- to this add about $ \\frac{1}{2} $\" to cover for waist and lower seams (I like to use small seams -- if you want more add it in). 2) Next you need to determine the amount of fullness you want at the bottom of the skirt where the ruffle will attach. I would suggest no more than 18\" at the bottom of each section (for a total circumference of 144\"). Anything more than that gets pretty cumbersome. *The exception here is if you decide to eliminate the ruffle altogether and simply extend the basic skirt to calf or ankle length where you can apply a ruffled or pleated trim. In this instance you may want to increase the sections to 24\" or more. (example below) 3) Cut each circle open on one side on straight of fabric. These 2 cutting edges will be seams. 4) Connect all circles with narrow $ \\frac{1}{4} $ \"seams, leaving the last one open for final adjustment. 5) Follow steps 2-4 with lining fabric. 6) Follow steps 2-4 with interfacing (optional). *This is the point at which the lower trim should be applied. For reasons of space, suggestions for trims, pleats, cordings, etc. will be dealt with in the next issue. 7) Place your fabric and lining strips of ruffles right sides together and pin around the bottom. If you are interfacing, layer them: interfacing, outer fabric, lining. 8) Pour yourself a glass of wine. 9) Put on your favorite anthology or have a friend talk or sing for you, play the guitar, or read from A Way of Life. 10) Start on your journey stitching around the bottom of the ruffle until you're about half-way around. 11) Get another glass of wine and have your friend make palmas while you dance bulerías. 12) Finish stitching the rest of the bottom. Turn right side out, pin and press and baste stitch around the top seam line. 13) Pin and stitch ruffle to bottom of skirt. If you are using more than a $ \\frac{1}{4} $\" seam you will need to slit in to the stitching all the way around so the ruffle is released better. Sometimes occasional little tucks or pleats are taken as the ruffle is attached to in-",
    "title": "PART II",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_11",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "27-29",
    "page_number": 27,
    "word_count": 851,
    "article_char_count_full": 4737,
    "article_char_count_review": 4737,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_11::A6",
    "article_text_for_review": "(from: $ \\underline{El} $ $ \\underline{Pais} $, June 24,1980; sent by Suzanne Hauser; translated by Paco Sevilla) by J.M. Costa Last Saturday, in the Monumental de Las Ventas (Madrid), there was a recital by Paco de Lucia and Lole y Manuel. The audience half filled the bullring; over the loudspeakers' rock background music was playing and, from time to time, a hard and insulting announcement for bluejeans. The sponsor began by saying that the music was not in conflict with the evening's entertainment and that popular songs can be art; he added that people were sneaking in without paying and that the ring had been left in good shape by the audience that had seen Lou Reed the night before. Actually, the atmosphere was not very exciting; the heterogeneous mix of families, youths from good families, lovers of flamenco, and other youths who were somewhat drunk, prevented that magic anticipation, that communal feeling of all waiting for the same thing. But, from the first song by Lole y Manuel, you could see how art can penetrate into any atmosphere, just as beauty can be seen from any viewpoint. Lole sang very well, with that hoarse voice of hers that goes anywhere -- when one expects that it will break at any moment; it breaks when she wants it to, and",
    "title": "FLAMENCO EN LA PLAZA DE TOROS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_11",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "30",
    "page_number": 30,
    "word_count": 220,
    "article_char_count_full": 1268,
    "article_char_count_review": 1268,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
