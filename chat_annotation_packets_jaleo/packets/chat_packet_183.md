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
    "article_id": "JALEO_1984_01::A3",
    "article_text_for_review": "POSTAL SURVEY AND OTHER ITEMS Our postal survey announced in the December issue (page 15) as to speed of delivery of First Class versus Bulk Mail, is thus far inconclusive. We received seven responses -- bulk which arrived in San Frnacisco in seven days and six First Class which arrived as follows: 2 days - Virginia and Michigan 3 days - Wisconsin and? 4 days - California (Costa Mesa) 14 days - Maryland We also received two post cards, presumably from the same person, saying \"Please don't cancel First Class.\" We are going to continue our survey for one more issue so please drop us a post card with: type of subscription, date of arrival, city and state and which issue of Jaleo it was. * * * Because of recent delays in publishing and combines issues subscribers are wondering \"are we going to get any Jaleos at all? And if so, how often?\" The answer to the first question is, \"Yes\". The answer to the second is more nebulous as it depends on several factors: 1) if we have enough money to publish, 2) if we have enough material to publish, and 3) if we have enough time, strength, man/woman power to publish. The latter two factors have been the main cause of this month's delay. Contributions of articles, etc. have dwindled way down and the opening of the new tablao here in San Diego and other performing commitments have meant that more energy has been going towards rehearsing and performing than to editing and layout. It has been suggested that we continue to publish every month but make every other month a thin issue with mainly announcements and updates so that members can, at least, keep up on current events. This is still under consideration. So, the how often is still uncertain but it would be safe to expect eight to ten issues in 1984 and we will attempt to get a consistent pattern going so that we will not try your patience any more than necessary. Guillermo Salazar says that he is going to take a vacation from GAZPACHO for a while. We wish to thank him for his dedicated contribution to so many issues of $ \\underline{\\text{Jaleo}} $ and look forward to further articles when he gets renewed inspiration. --Juana DeAlva",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 385,
    "article_char_count_full": 2152,
    "article_char_count_review": 2152,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A4",
    "article_text_for_review": "LOS ANGELES FLAMENCO DANCE LINE Hi: The listing L.A. Hotline is not correct. The correct title is The L.A. Flamenco Dance Line. The line is a 45 second message containing 6 items a week. What's happening, where to buy, whose teaching, whose working, etc. It's free. The other side of the coin is heavy promotion to the media as a source of information about Flamenco dance. I plan to expand the Dance Line to 1 minute. On Friday the tape runs continuously from 4 to 8 the record unit is shut off. The rest of the week the unit goes to 4 rings. Usually, I answer, but if I don't you get the message plus an opportunity to make comment. I think that other cities could do the same. It would introduce Flamenco to many people. Respectfully, Leo Markus Los Angeles, CA CATALOGUE OF MODERN FLAMENCO RECORDS A collection of flamenco records from the modern era (1972-82), representing most of the important artists and including a number of unusual and rare items. Each record is described in detail and given a brief critical review. A tape library will make these records available. SEND $4.00 TO: PACO SEVILLA, 2958 KALMIA ST. SAN DIEGO, CA 92104 Supreme strings designed for today's finest classic and flamenco guitars Ai your local dealer or contact Antena David Inc., 204 West 5517 Street New York, NY 10019 USA Te.s (212) 307 1567 • 757 4412 or 3255 GUITAR REVIEW WORLD WIDE CLASSIC GUITAR QUARTERLY ACADEMIC YET YOUTHFUL BLOSSOMING FORTH WITH NEW IDEAS NEW WRITERS · NEW REVIEWERS · NEW DEPARTMENTS USA Subscription 4 issues: $24.00 postage included Send to: GUITAR REVIEW, P.O. Box 5375, F.D.R. Station, New York, NY 10150",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 284,
    "article_char_count_full": 1625,
    "article_char_count_review": 1625,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby The 'Shah of Iran' -PROLEGOMENA- one recent afternoon as I idled recumbent in my chambers, slipping free a glass of spiced pomegranate juice received from the perfumed hand of a favorite concubine, I turned my languid eyes upon a dusty sheaf of documents recently retrieved from some undistinguished mound of detritus, one of many such archaeological tells that decorate my abode. Great ves my delight when this sheaf of yellow - Patinsted pages yielded up - lo and behold! - an article that I had written for \"Jaleo\" in March of 1981, but which I had neglected somehow or other to mail. So, with little further dalay or comment, I expedite a copy to your worships. The paper may be a bit stale, but its words are not. There may be those efficient souls who fain would accuse my shahship of sloth\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"compas\"]\n\nnt, I expedite a copy to your worships. The paper may be a bit stale, but its words are not. There may be those efficient souls who fain would accuse my shahship of sloth or procrastination. I have resolved to disprove them by contracting an ulcer or hypertension. When I get around to it. But first, another sip of spiced pomegranate juice. Sluuurrrp! March 16, 1991 Jelmo Box L706 San Diego, CA 92101 In the name of Allah, the all-merciful and all-compassionate, Greetings. In the matter of the demise of the late and lamented Vicente Escudero, I take the liberty, by your leave, to present the following particulars. Don Vicente expired on either the 5th or 8th of December past [1980] in Barcelona at the age of ninety-two. While his passing went unnotad in the United States, it generated considerable comment in the Spanish press, and for three or four days the Madrid papers devoted abundant space to features on Escudero, and in particular on the miserable circumstances in which he passed his final years. At the time of his death, the greatest bailaor of our age owned no more than a folding cot provided through the generosity of two elderly ladies who had attended him during his final year and who had graciously supplied him free room and board and his meager maintenance. He had spent the past seventeen years soliciting some pension or similar provision from various government agencies, all to no avail. Nevertheless, his emacisted corpse was deposited with great honor and cersmony in the Pantheon in Barcelona. This is the first time such an honor has been accorded a flamenco and the representative of this lovely art was laid to rest among the illustrious only after hot debate on the prpristy of his presence ther\n\n[ENDING CONTEXT]\n\nescaped with the guitarist who was also ripe for rebellion. The two continued to perform with indiffrent succese, travelling on foot, stopping at iron manholds whers Vicente might practite his heal-work. Now it was the guitarist who collerted the money but who failed to abide consistently by the agreed principle of \"fifty-fifty\". The partnership broke up at almost the same instant that Escudero broke his companion's guitar. At twelve, Escudero was invited to dance in a Cuadro Flamenco in a Madrid cafe cantante. Out of this appearance grew a demand for his extraordinary services. During the\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ON THE PASSING OF VICENTE ESCUDERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 1062,
    "article_char_count_full": 6146,
    "article_char_count_review": 3360,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "compas"
      }
    ]
  },
  {
    "article_id": "JALEO_1984_01::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nnext eight years, Escudero performed throughout Spain; in Portugal he danced for the first time in a legitimata theatre, and was acclaimed a coming genius of the Spanish dance. At the age of twenty, he left for Paris and set the city aflame with the unvarnishad purity of his Flamenco style and the elegant insolence of his stage presence. Famous painters and musicians lauded him; he became the caoter of an adoring cult. In 1932, Escudero made his American debut upon the invitation of impresario S. Hurok and he became an immediate and electric sensation. Such arrogance and primitive power had never before been seen in a Spanish dancer; such complex and subtle explosions of rhythm coming from all parts of his quivering body - even from his long fingernails - had never been heard. He made\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"close\"]\n\n932, Escudero made his American debut upon the invitation of impresario S. Hurok and he became an immediate and electric sensation. Such arrogance and primitive power had never before been seen in a Spanish dancer; such complex and subtle explosions of rhythm coming from all parts of his quivering body - even from his long fingernails - had never been heard. He made three American tours, and during the Spanish Civil War and World War II, sileoce closed him in. Until last summer, he made sporadic appearances in Spain and elsewhere on the continent. His newest enthusiasm had become painting, and at this he waxed prolific. His subjects are mostly impressions of the Spanish dance; he has had numerous exhibitions in Paris and disposes of his works as fast as he can paint them. He has had an elaborate book published on the Flamenco dance, is also something of a vintner, bottling his own brand of Flamenco wine and designing his own labels. True to the fabled concept of the gypsy, he has no permanent home; he is a chronic hotsl-dweller. Escudero is probably the oldest male dancer performing professionally in the western world and creating excitement wherever he appears. Ha is 62, virile, raw-nerved and unorthodox. He shuns the gloss of modern showmanship and deploras the growing adulteration of the Spanish dance and the use of mechanical devices for effects. Effects, he says, should come from the human body alone. He recently published in Paris a \"Decalogue\" enumerating ten commandments for Spanish mala dancers who would escape the growing plague of impurities and mannerisms in the Spanish dance. Point one says: \"Dance like a man.\" Escudero is said to come closer to the garminal sources of the Spanish dance than any other performer alive. Flamenco, from which pure Spanish dance derives, is the genre in which he stakes his reputation. Several years ago, in Seville, Escudero flung a challenge at all male dancars to come forward and match the purity of his Flamenco style. There were no contenders. Escuge\n\n[ENDING CONTEXT]\n\n\"rondenas\" and \"guajiras,\" which I was unable to hear. Bacán proved himself the master of his instrument and his music. He has a fluid technique, a vital sense of rhythm, a sure grasp of the music's varying temperaments. Indeed, Bacán plays in a seamless manner, one note flawlessly linked to the other, each phrase part of the whole yet never devoid of individual expression. Flamenco music can move very quickly with endless passage work, requiring not only nimble fingers but also an ability to make something that can be routine emotionally into something that is charged and exciting.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PEDRO BACAN IN USA FOUR TEACHERS OF SPANISH DANCE: PART III",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "poem",
    "pages": "7",
    "page_number": 7,
    "word_count": 1220,
    "article_char_count_full": 7369,
    "article_char_count_review": 3642,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "close"
      }
    ]
  },
  {
    "article_id": "JALEO_1984_01::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPD: Do you remember your grandfather? EP: Mall, I was very small when he died, but I remember that by then he gave only castanet classes. He taught sitting down facing the student. He always wore a suit and a tie, and he changed into a fresh shirt for his afternoon class. PD: How did you, his grandchildren, begin to dance? Luisita also learned to dance as a very little girl, but she never liked performing on stage. By the time she was fourteen or fifteen she was my father's assistant. EP: Carmen as a toddler danced the same as she does now, I think. She opened the door to the studio one day when she was maybe two years old, and began to do everything the students were doing. That's how she learned, by watching. When she was three and a half she was invited to be in a recital given by some\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"school\"]\n\na very little girl, but she never liked performing on stage. By the time she was fourteen or fifteen she was my father's assistant. EP: Carmen as a toddler danced the same as she does now, I think. She opened the door to the studio one day when she was maybe two years old, and began to do everything the students were doing. That's how she learned, by watching. When she was three and a half she was invited to be in a recital given by some dancing school. I remember that my grandmother made her several little cestumes, one for a zambra, one for Amparo was ten years old when my parents moved to Argentina, and she didn't know how to dance yet. But whan Angel put on his production of \"Goyescas\" for the second time, she decided she wanted to perform too, and she began to learn with Luisita and my father. JALEO - JANUARY/FEBRUARY 1984 EP: For the time being, na. We think it is much better that Angel nat bring us, that is, his own company, into the Ballat. He should work with the Ballat as it is. It has its full complement of dancers chosen through open auditions which w\n\n[ENDING CONTEXT]\n\non a play by Antonio Gala. This is not official, but I think I can safely mention it because he is a very good friend. Among the many congratulations that I received upon my appointment as director of the Ballet, his is the one I treasure the most. He is currently the most important playwriter and \"literato\" that we have in Spain, and the most popular as well. He also writes newspaper articles and editorials which are very widely read, to the point where people buy a certain paper just for his articles. If he becomes involved in the Ballet, we will have to find a composer just as important.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ELOY AND ANGEL PERICET",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "8-10",
    "page_number": 8,
    "word_count": 1779,
    "article_char_count_full": 9607,
    "article_char_count_review": 2705,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "school"
      }
    ]
  }
]
```
