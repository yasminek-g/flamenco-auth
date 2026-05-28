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
    "article_id": "JALEO_1983_01::A3",
    "article_text_for_review": "EL CABRERO INCARCERATED Dear Jaleo: In the Name of Allah, all-merciful, all-compaseionate, Greetings. I set pen to paper to inform your illustrious readership of the sad plight of José Domínguez Múnoz, \"El Cabrero,\" who now languishes in an Andalusian jail, doing time for.....BLASPHEMY! The particulars are as follows: Two years ago while performing in Córdoba, El Cabrero -- may his flock increase -- found he could not sing, as the result of an argument that had freshly occurred. His audience, anxious to hear this most popular cantaor, insisted that he sing anyway, even if he had to resort to sign language. He complied, and his new efforts met no greater success than his first, whereupon his kind audience began to bleat in imitation of goats (Cabrero is, as his name indicates, a professional goatherd). At this point, a common Spanish oath burst forth from the artist's most understandable frustration and he quitted the stage. Two years of appeals have served to reduce his sentence from five months to two. The last week of October past, all legal recourses exhausted, El Cabrera reported to the local jail, while the artistic community of Andalucía vainly clamored and campaigned for his pardon. The Spanish Minister of the Interior, García Ahóveros, allowed that if this crime were prosecuted scrupulously and uniformly, eighty percent of the Spanish population would be serving life sentences. In my own humble but valid opinion, a simple public flogging would have sufficed. Peace unto you, The Shah of Iran Braoklyn, NY VISIT WITH RAQUEL PEÑA Dear Jaleo: As my husband and I were planning a brief vacation to the Washington, D.C. area I came across your article on Raquel Peña and Fernando Sirvent. Having been interested and exposed to Spanish music and dancing since childhood I wondered if there was any possibility of taking three or four dance lessons from Raquel. My first phone call in August was answered by her secretary who explained to me that Raquel and Fernando were in Spain until September. From this conversation it became ostensibly clear that their artistic engagements kept them continuously occupied. I began to doubt as to whether she would make time for a stranger from Michigan who wanted but a few lessons. When I called in September and talked to Raquel, I was pleasantly surprised that, although she did not know me at all, she was so helpful providing me with all kinds of information regarding Washington, D.C., places where flamenca was presented, where to find a good seamstress for a costume, etc., plus she said she would try to assist me as much as she could once we arrived to the area.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_01",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 443,
    "article_char_count_full": 2637,
    "article_char_count_review": 2637,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_01::A4",
    "article_text_for_review": "LIFE STYLE DUES OR LIFE STYLE BLUES? Every generation of every civilized culture has had them. They are a very disturbing element to those who don't understand them. Their names? Bohemians, the lost generation, beatniks, subterraneans, heaven's minstrels, wandering scholars, hippies, hcbcs, bums, flamencos, and of course, the perennial gypsies (all gypsies are not necessarily flamencos, nor are all flamencas gypsies). There are many other names, given to these life styles, but there is an underlying theme present in the conscious (or unconscious) philosophy of them all: the refusal to accept civilized (?) societies' established success values. They always represent a small portion of the particular society to which they belong. But, if one were to consider the \"closet Bohemian\" (those who secretly envy and support those of this particular persuasion, but feel obligations that preclude joining them), the percentage swells, until most of those who are left are not very exciting to be around (e.g., rednecks, three piece suiters, ivy leaguers, preppies, etc.). When one closely examines the artifacts that are treasured from ancient civilizations, it isn't their banking practices or their status symbols. It is overwhelmingly their art, music, and philosophical meanderings. These items are rarely created by persons enslaved to the success syndrome. This is not to say that there is something wrong with becoming rich from cultural endeavors, but that it is a rare occurrence and, in most cases, a residual effect resulting from a person's true goals. In any case, I suppose those hardly souls still with me are wondering if there is a point to be made here, or if this is just the ravings of one who has been long exposed to law radiation emanations from computer terminals. The truth is, I began giving serious consideration to this subject about a year and a half ago while sitting in a warm bus or a cold rainy day in Granada. I was watching an old gypsy hawking tourist toys. His teeth were bad and probably hurt him. He was there at least as long as an average work day and was not having much luck. I began thinking about his life style versus mine. I work a steady job for eight hours a day doing intellectually satisfying work. After I have paid these \"dues,\" I go home to comfortable privacy for the remaining sixteen hours. I think what I want and pretty much do what I want. If my teeth hurt, I have dental coverage. I don't spend even five minutes a day worrying or planning to be certain that I will have something to eat. In a few more years, I will be able to retire and will have the whole twenty-four hour day to do what I want. Who is the happier? This question isn't rhetorical. I'm really not sure, and I'm certainly not trying to be smug. I can only say that in my present situation, I'm not ready to trade places with the old gypsy. However, this is not to say with any certainty that he is not more content. Who decides? One thing is certain...whether a closet Bohemian or totally committed, these life style values are deeply seated among those who embrace them, as the following quotes testify: Donn Pohren quating a gypsy in his back El Arte Flamenca: \"I have no desire to own a house or a car, or to go to work every day like a half brain. It seems to me that the payo (nan-gypsy) works all of his life for things that he does not really want or need. He sits in a closed office dreaming of open fields and mountains and beaches and, when he finally is allowed a vacation he travels to a resort area, mills with people and pushes his way around for two weeks and spends his savings. He lives in fear and anxiety of his employer, a possible depression or war, old age, and a thousand other things either completely beyond his control or not worth the effort. But we, in our simple existence, have everything we need to be happy. I have a wonderful, talented family. If we feel like spending the summer on a beach or in a mountain forest, we do so. We have friends and relatives in all parts of Spain. Of course there are hardships -- the rain and the cold, occasional hunger -- but",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_01",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 731,
    "article_char_count_full": 4122,
    "article_char_count_review": 4122,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_01::A5",
    "article_text_for_review": "JALEO THANKS THE FOLLOWING CONTRIBUTORS: <table><tr><td>Antonio David</td><td>- 2 Gift Subscriptions</td></tr><tr><td>Robert Dwyer</td><td>- 1 Gift Subscription</td></tr><tr><td>Marianne Gabriel</td><td>- 1 Gift Subscription</td></tr><tr><td>Elun Gabriel</td><td>- 1 Gift Subscription</td></tr><tr><td>Richard Kurth</td><td>- 1 Gift Subscription</td></tr><tr><td>Raquel Scheier</td><td>- 2 Gift Subscriptions</td></tr><tr><td>Katina Vrlinos</td><td>- 2 Gift Subscriptions</td></tr></table> THIS SPACE RESERVED FOR YOUR CARD-SIZE AD SPECIAL OFFER $10 FOR I MONTH - $25 FOR 3 MONTHS PRICE APPLIES TO PHOTD READY ADS (DNE TIME $5.00 FEE IF AO DESIGN IS REQUESTED)",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_01",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "7",
    "page_number": 7,
    "word_count": 67,
    "article_char_count_full": 660,
    "article_char_count_review": 660,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_01::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(photo Villarín) dedicated his time almost exclusively to the fiesta. I would go to a place where he worked, almost every night, and sometimes he would go out, and sometimes not, because sometimes there wasn't any fiesta; every night I was with him, and he taught me a lot of cante. JALEO: And how did you eat during that period? ENRIQUE: Hombre, I used a spoon! [Laughter] No, it wasn't that; I went from pueblo to pueblo looking for cante. When you are working and you find yourself with a person who knows more than you, you go to that person and learn from him. JALEO: Oh, I see. You were learning after you started to work professionally. Tell us, where has your own cante come from? ENRIQUE: Well, it has come with the passage of time. Time goes on and you start creating your own form --\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"many\"]\n\nd you find yourself with a person who knows more than you, you go to that person and learn from him. JALEO: Oh, I see. You were learning after you started to work professionally. Tell us, where has your own cante come from? ENRIQUE: Well, it has come with the passage of time. Time goes on and you start creating your own form -- without realizing it; soon, you have your own way of singing. And you take some fandango, and you see that fandango has many possibilities. There's no reason for it to always be the same fandango, sung the same way. And then you add to it the different tones of the guitar and -- well, that's the character of flamenco. By the way, I'm very pleased that when you saw me sing, you understood it -- it's a stimulus for me because not everyone feels the same. But I love singing the cante clásico too. Take an alegrías, for example, like the one that goes [He sings and taps out the compás on the table]: Grande locura es negarlo. Que es verdad que te he querido Grande locura es negarlo. Pero tú, hay a quien hundes. Y así viví, era cien años... You take that one...It's alegrías, but it's normal. Then, one day, instead of singing it like this: Es grande locura negarlo... Like that, the way Aurelio would sing, \"que es verdad que te he querido...\" because that's how he would sing. Instead of singing like that, you sing [He sings the m\n\n[ENDING CONTEXT]\n\nThis is the first time that Enrique Morente has visited Mérida. \"At first I was turned off when I arrived here and saw these ruins. I asked myself, 'How can they do theater here when everything is falling apart?' Broken columns, seats falling apart, and all the rest. Little by little I began to grasp it and finally convinced myself that it is a true marvel. Later, I realized that the acoustics are unique; I don't know of any theater, open or enclosed, that has such perfect acoustics. In reality, there is no need for a microphone, but you have to work with the music and balance the voices.\"\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ENRIQUE MORENTE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_01",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "8-10",
    "page_number": 8,
    "word_count": 1270,
    "article_char_count_full": 7046,
    "article_char_count_review": 2985,
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
    "article_id": "JALEO_1983_01::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Charles Teetor Forty years ago I discovered flamenco in, of all places, st. Louis, Missouri. The experience was so exciting that even now my pulse quickens to think of it. I was a provincial boy from Indiana with absolutely no Spanish, Gypsy or even Mediterranean blood in my background. My only exposure to anything foreign had been one year of high school Spanish, a modest exposure at best. It all started one rainy night in January, 1943, when, with nothing to do, I found myself standing in front of the St. Louis Concert Hall. The posters proclaimed Carmen Amaya and her company would perform in a few minutes. I bought a ticket. The next four hours changed my life. It is impossible to explain to a non-believer the euphoria that Carmen and her company produced in me that evening. Looking\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"family\"]\n\nh nothing to do, I found myself standing in front of the St. Louis Concert Hall. The posters proclaimed Carmen Amaya and her company would perform in a few minutes. I bought a ticket. The next four hours changed my life. It is impossible to explain to a non-believer the euphoria that Carmen and her company produced in me that evening. Looking back, it may have been one of the best all flamenco touring companies. In addition to the sizeable Amaya family, the company contained the best Spain had to offer. It had all been packaged by Sol Hurok, who knew how to package a dance company. Costumes, orchestra, scenery and staging were of a scale that we do not see today. When the curtain finally fell, I refused to let go of the spell the Amayas had woven. For the first (but not the last) time in my life, I found the stage door and waited. I simply had to see those magic people again. The first two artists to come out were Antonio Triana and Sabicas. It was Fate that made them ask me where to get a taxi to the railroad station. In my high school Spanish, I replied that it was only a two-block walk and that I would gladly guide them. By the time we arrived at the station we were sufficiently acquainted that they asked me to come in for a coffee. Within a few minutes the entire company had gathered in the Fred Harvey lunchroom waiting for the midnight train to Chicago. The war was on and transportation was difficult. To my everlasting joy the train was two hours late. During that two hours I discovered several interesting things. First, and most important, was that true flamences are exhilarated but constrained by stage performances and frequently can't wait to get off stage and start performing \"free form.\" The tile floor of the lunchroom and the marble-topped tables perhaps were somewhat reminiscent of the cafes of Andalucía and, while waiting for that train, I experienced my first juerga. Sabicas' guitar came out, the girls sang, and there was much knuckling of the tables. Antonio Triana was an electric spark that got everything and everybody going. Such was the magic of the moment that, when the train finally arrived, they asked me to come with them to Chicago. To my everlasting discredit, I let my sense of responsibility prevail. Their final words in parting were, \"Come see us in New York.\" By the time the college term had ended some months later, I had acquired the only two phonograph records of Spanish guitar for sale in the United States at that time. One was Segovia's \"Tales of\n\n[ENDING CONTEXT]\n\nhinterlands and on the college campuses. Flamenco is one of the truly great folk musics and hopefully will be preserved forever by those who, like me, became intoxicated by it, but who, unlike me, had the talent necessary to perform it. Keep that light burning so that my children and my children's children will know the excitement I have known. MENTAL ARTISTRY IN FLAMENCO a revolutionary approach to mastery for dancers and guitarists * improve technical skill * accelerate learning $ ^{*} $ develop mental & muscular control * enhance performance CONCHITA PIQUER; TEATRO AVENIDA, BUENOS AIRES.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "THE FLAMENCO SCENE IN NEW YORK IN THE FORTIES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_01",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "11-13",
    "page_number": 11,
    "word_count": 2034,
    "article_char_count_full": 11477,
    "article_char_count_review": 4147,
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
  }
]
```
