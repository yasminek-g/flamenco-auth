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
    "article_id": "JALEO_1990_04::A21",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nGIPSY KINGS: THE SOUND OF ROOTLESS ROOTS by John Milward The Gipsy Kings, who played to a packed house at the Ritz on Wednesday night, give new meaning to the notion of a guitar band. Standing six abreast, and strumming six acoustic guitars in front of four backing musicians, they wove a densely propulsive sound keynoted by the keening vocals of Nicolas Reyes and the spindly guitar figures of Tonino Ballardo. The result was a heady brew that can be called nothing less that Heavy Flamenco. For followers of world-beat music, Wednesday's show was another stop on the musical globe. But while the gypsy clan to which the Kings are connected is based in southern France, the peripatetic nature of the lifestyle naturally accommodates other regional influences as well as cosmopolitan connections.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"audience\"]\n\nis based in southern France, the peripatetic nature of the lifestyle naturally accommodates other regional influences as well as cosmopolitan connections. In fact, one factor that has given the group its cachet is that their music inspired a recent collection by influential French designer Christian Lacroix. The Kings, however, are not fashion plates. The six wore black peg-legged pants, shiny black boots and crisp dress shirts. By contrast, the audience at the Ritz was a stylish crowd whose members were just as likely to speak French, Spanish or Italian as English. But when band members punctuated the music with handclaps, fans echoed the beat in the international language of rhythm. One good thing about a band of gypsies is that they are unlikely to fall into the rock-star trap of writing songs about the rigors of the road. Of course, as Reyes sings in Gitane, a language that mixes Spanish, French and various gypsy dialects, the lyrics are academic to all but the multilingual. For novices to the intriguing sound of the group's eponymous Elektra debut, the only melody that rang a bell was \"A Mi Manera,\" the group's rather unlikely version of Frank Sinatra's signature tune, \"My Way.\" The\n\n[ENDING CONTEXT]\n\nde Triana, cantaora/bailaora La Conja, cantaor/guitarist El Pollino, bailaora Liliana Morales, guitarist Arturo Martínez, and special guest guitarist Diego Castellón (brother of Sabicas). Highlights of the evening were the rumba \"Pan y Chocolate\" by El Pollino, the soleares by Liliana Morales, and the guitar solos of Diego Castellón. These, blended with the strong footwork and lyrical voice of La Conja, the emotive cante of Fernando de Triana, and the excellent food and decor at Maximiliano made for an exceptional evening in a city where good flamenco can be hard to find. (Terry Setter ---\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "PERFORMANCE REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "45-50",
    "page_number": 45,
    "word_count": 1367,
    "article_char_count_full": 8357,
    "article_char_count_review": 2832,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "audience"
      }
    ]
  },
  {
    "article_id": "JALEO_1990_04::A22",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNew York Greetings to all aficionados and joining me in salutations is the Lady from \"Sol y Sombra\" — Loretta [see Personalities page 32] beautiful, spellbinding performer of both flamenco and the classical dance. She is no stranger to readers of JALEO — was correspondent for our magazine in company with La Vikinga, Roberto Reyes and yours faithfully...Hauntingly gorgeous dancer, she hypnotizes her audiences and her fellow performers... She has now regrouped and strengthened her dance Company \"Sol and Sombra\" — the biggest Spanish dance company on the island (already well-known) — she is busy organizing for performances, especially in the rich Hampton areas of Long Island... Another beautiful dancer of New York joined with a group of Zarzuela performers...here in New York — She is Jerane\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"Duende\"]\n\nizes her audiences and her fellow performers... She has now regrouped and strengthened her dance Company \"Sol and Sombra\" — the biggest Spanish dance company on the island (already well-known) — she is busy organizing for performances, especially in the rich Hampton areas of Long Island... Another beautiful dancer of New York joined with a group of Zarzuela performers...here in New York — She is Jerane Michel. Jerane's dance program Zarzuela and Duende is a first for the Andalucians themes in the Zarzuela. The extraordinary dancer performed the homenaje to the great Argentina on the 50th anniversary of her death at the American Museum of Natural History — the program highlight was the rendition of Argentina's castanograph of \"Playera\" by Granados, danced and played by Ms. Michel. Carmen Rubio appeared with her Spanish Dance Theatre at the Thalia Spanish Theatre in Sonnyside, New York on consecutive Fridays... Carmen was joined by partner Jorge Navarro in Manzanita's creation Liberate. Carmen danced in Fandango de Doña Francisquita and at the very end in Cañas — Jorge excelled in the performance of Romeras and a Farruca — Loretta danced beautifully costumed performed La Leyenda del Beso and the Garrotin — Wila gave an exquisite performance \"por Solea\" — Rosa Rey in her first performance danced Alegrias. Arturo Martinez was the guitarist (he was joined by Marco and Bill) — the new cantaor was Fernando de Tr\n\n[ENDING CONTEXT]\n\nalgerias — yes, the only dancer to have recorded with Paco de Lucia's guitar. The flamenco fusion program continued with the guest appearance of Michel Camilo and his players, who at a later stage were joined by Raúl, Pardo and the other artists — an unforgettable night of music. PROGRAM NOTES Flamenco Fusion The art of flamenco has evolved over the centuries from the fusion of music from ancient and different oriental and occidental cultures. In the seventies, a generation of jazz musicians began combining the guitar, the singing and dancing of pure flamenco with jazz. This new genre borrows\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "article",
    "pages": "51-52",
    "page_number": 51,
    "word_count": 1027,
    "article_char_count_full": 6171,
    "article_char_count_review": 3056,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "Duende"
      }
    ]
  },
  {
    "article_id": "JALEO_1990_04::A23",
    "article_text_for_review": "Los AngelesJuerga Scene from the Los Angeles Juerga. Gisela dancing. Art Valdez of Valdez Guitar Shop at 7420 West Sunset Blvd. Los Angeles, California, offered his shop for a Saturday night juerga on Feb. 4, 1989. After a slow start, the juerga really began when a group of dancers arrived after their rehearsal for a show. By that time many of the early group had left, but there were still enough guitar players, dancers and singers to have a good party. It was nice to see everyone helping each other to understand and improve on compás, coplas, dance steps, etc. There was lots of good food and drink brought in by everyone. Bill Freeman, Eugene Cordero and his music partner, Benito Palacio, Mickey Kane, Richard Ogelby, Rick Colman, Paul Donn, Gabriel Reyna, Victor, Ed Valenzuela, Michael Long, Yvetta Williams and several other guitarists who are friends of Art Valdez were there. Left to Right: Yvetta Williams, Carolyn Burger, Richard Ogilby *** MEMORIAL JUERGA FOR GENE FOSTER by Ron Spatz On the evening of December 3rd at the North Hollywood home of Greg Miller, a memorial juerga was held for Gene Foster. Gene, a dear friend and aficionado, left us in July of 1988 shortly after suffering a major stroke. Why a juerga? —they are supposed to be fun. Well, Gene wouldn't want it any other way. I have never met anyone who loved flamenco more than Gene. He never tired of discussing it or experiencing it. One time at one of our private juergas where things were really coming together, Gene remarked to me, \"Ron, I live for these moments.\" I am happy to report that he would not have been disappointed. The chemistry was about as near to perfect as you can get with a crowd of 50 or so aficionados. The participation was near 100%. As usual, with that many people, it would be nearly impossible to capture the names of all the participants. Most of Gene's close friends were present, plus several less familiar (to me), but most welcome, faces. It was great to have Stamen Wetzel back among us after his change of heart about taking up residence in Hawaii (too much sand, too little flamenco). Likewise having Coral Citron back from Spain. Marcos and Rubina Carmona who recently moved to Seattle could not be there, but called the night before the juerga to offer their condolences about Gene and to wish us all well (Seattle's gain is L.A.'s loss). Yvetta and myself (both suffering from juerga arranging burnout) have received some welcome relief in the form of the rejuvenating efforts of Rick Coleman, who arranged the location for this juerga through his friend Greg Miller. If one were to design a house specifically for a juerga, it would probably come out looking much like this one. Gene's wife, mother, stepfather, and oldest son were present, and from all appearances, enjoyed themselves immensely. There was a lot of video and audio taping taking place throughout the evening, which hopefully captured the moment for posterity. Dancing, we had Juan Talavera, Gisella Lorca, Alonzo Hanoln, Gilberto Cesar, Kathy Danelski, Carolyn Berger, Katina Vrinos, Coral Citon, Carmen Torres, and Mardi. Beautiful cante was provided throughout by Susan Duckett. Participating guitarists were Mickie Kayne, Ben Shearer, Stamen Wetzel, Bill Freeman, Gabriel Rufz, Paul Donn, Richard Ogilby, Yvetta and myself. I'm sure there were others that I missed, because there were several happenings in different areas of the house, plus my memory span isn't what it used to be. Gene was a sensitive, loving person who liked people, poetry, ballet, classical guitar, all aspects of flamenco, and philosophy. I have never known anyone before with whom I had so many things in common and I feel that I knew Gene well enough to state with complete authority that this juerga was the most appropriate memorial that he could wish for. —Yvetta Williams ZONA DEL RIO TIJUANA TEL. 44 75 02 AVE. DE LOS HEROES NO. 10001 LOCAL 15 Y 16 PLAZA FIESTA Andrea Regjo, one of Rosa Montoya's youngest students. (photo by Curtis Fukuda)",
    "title": "JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "poem",
    "pages": "53-55",
    "page_number": 53,
    "word_count": 683,
    "article_char_count_full": 4018,
    "article_char_count_review": 4018,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1991_07::A1",
    "article_text_for_review": "Single Issues: Single issues will be made available to non-subscribers at a cost of $7.00 per issue which includes postage and haadiiog. Due to the high cost of mailing out one issue at a time, this cannot be avoided. Since a yearly subscription is $25, we feel this is not an unreasonable cost. Punctual Mailing and Check Processing: The delay in processing your checks came as a result of getting things off the ground and set up to run the magazine. Though we were slow on this first volume, in the future the processing time should be 2-3 weeks at most. The mailing schedule for the first volume is July 15, 1991, October 15, 1991, January 15, 1992, and April 15, 1992 and our goal is to be on schedule. $ \\underline{\\text{Videos:}} $ There are a few instructional videos out there. We have one source in this issue, and we hope to hear from other professionals who have developed instructional videos. (Note: subscribers...if you are aware of sources for flamenco videos, please let us know who and where.) I am presently looking into a source for commercial videos of flamenco performances from Spain which can be converted to the VHS system. More on this in the next issue... Advance News of the Festivals in Spain: See the ioformation in this issue on the Bulletin of the Fundacion Andaluz del Flamenco de Jerez. Future issues will contain as much timely information as is possible about the schedules of the festivals in Spain and around the world. Correspondents: Jaleo Magazine is looking for a \"few good men and women!\" We need correspondents in areas across the country to cover local flamenco events, concerts and news and to let us know about it. You must be a fair to decent writer, you cannot represent or be a part of a special interest group; you should have a good knowledge of the arts of Spain, especially flamenco; preferably you have visited Spain several times and perhaps speak the language. Anyone interested in becoming a correspondent should write directly to me at Jaleo, P.O. Box, San Diego, CA 92164.",
    "title": "Letters and Avisos..... 2-",
    "periodical": "jaleo",
    "issue_id": "JALEO_1991_07",
    "year": 1991,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 354,
    "article_char_count_full": 2032,
    "article_char_count_review": 2032,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1991_07::A2",
    "article_text_for_review": "Jaleo Back on Track The first issue of Jaleo Magazine is out. Jaleo is back. As the new editors of Jaleo, we are dedicated to preserving and improving the magazine which brought flamenco to your doorstep for twelve years. Getting the first issue into the mail took a little longer than we anticipated and I know we tested the patience and good will of more than one of you, but you stuck it out as did we, and here we go! A couple of months ago I sent a thank you letter to those from whom I had received subscriptions because at that point, this project became feasible. Since that time, additional response has given the future of Jaleo a fairly solid foundation. We have a wonderful tool at our disposal and I am delighted to be charged with its direction. I see one factor in particular which makes Jaleo something a little more than just a newsletter or special interest newspaper. As small as the circulation is, it's international. At the date of this issue, we have \"family\" in three countries and within the coming year I expect the total to be five. In the United States, there are subscribers in 11 states including Florida, California, Washington and New York. One of the goals I would like to achieve with Jaleo will result in a solid link with some aspect of the flamenco world in Spain. I stated in my letter of February 1, 1991 that, in my opinion, much can be accomplished to enrich the \"flamenco experience\" in America in ways such as scholarships, residence exchange programs and grup travel rates. Perhaps the possibilities of residence exchange have not lit a fire yet but consider this--for example, if I were to receive a letter from a guitar player, from Tokyo perhaps, who proposed that he and and I exchange abodes (read \"flamenco environment\") for two weeks, nor any workable duration, would I consider it seriously? I sure would. What if that exchange were to take place between \"Anywhere U.S.A.\" and Spain? Perhaps a young member of a flamenco family in Spain was interested in spending a year in school in America. Would any of us consider opening our home to such an opportunity? I believe some of us would. There is tremendous potentiality in such a program and Jaleo can be an important tool in establishing the necessary communication. The fact that Jaleo reaches flamenco enthusiasts and artists throughout the country and beyond is significant but the heart of it is, after all, the art itself. We are involved in flamenco at a time when all aspects of the art are evolving and to one degree or another, we are a part of it. Why not grasp an opportunity to become involved just a little more than we are now? -Editor- More articles and information about flamenco and not so much \"who is doing what this summer to America\". You will enjoy the Jaleo \"Workshop\" on dance, guitar, and singing with information coming from the most authoritative sources I can solicit. To begin a \"workshop\" however, I need questions from readers who want to learn. Inquiries can be on technique, style, interpretation, guitar accompaniment, singing for dance, dancing to the singing, etc. What's In Store The Flamenco Directory of North America is very outdated and is being offered as a collector's item. I believe an updated Directory would be a good project for the future--again, it's up to you. There will be more coming from Spain as contacts there become more \"facit\". The regular interview with an interesting and artistic personality. Perhaps the personalities will not be as famous as some of those seen previously in Jateo, but they will be colorful. The \"Flamenco Directory\" always seen on the backs of previous Jaleo issues needs current information. Please send us names and telephones of instructors (and what they teach) and establishments in your area which caters to flamenco or Spanish dance. Please send this information to us on a 3x5 card. Thank you! -Editor- Thank you. Bruce Patterson Editor AVISOS ..in Los Angeles Carolina Russek will present her show at the Lobrero Theatre in Santa Barbara on August 2 and 3, 1991 with Juan Talavera as guest artist. Tickets can be purchased through the Lobrero Theatre Box Office. The Fountain Theatre will present Roberto Amaral and his \"Fuego Flamenco\" three consecutive weekends starting September 13, 1991 with shows Friday through Sunday. The series will run from Sept. 13-29. Showtimes are 8:00 p.m. Fridays and Saturdays and 3:00 p.m. Sundays. The Fountain Theatre is located at 5060 Fountain Avenue. Seating capacity is 80 and reservations are advised. There is a parking lost adjacent to the theatre. For more info call (213) 663-1525. La Masia Restaurant presents flamenco shows each week on Sundays and Tuesdays. Three shows nightly with the combined talents of Maria Bermudez, Pepita Sevilla, Antonio de Jerez and Benito Palacios result in fine flamenco entertainment. Located at Santa Monica Blvd. and Doheny. For info call (213) 273-7066. Casa Rafael in the L.A. suburb of Torrance offers flamenco entertainment every Saturday with shows at 7:30 and 9:30 p.m. Benito Palacios provides the guitar accompaniment for dancers Vivian, Maria Rojas and the cante of Antonio Alcazar. Call (213) 322-1287. ...and in New York... The Ararat Restaurant in Manhattan features a weekly flamenco showcase produced by Andrea Del Coote and the American Spanish Danee Theatre, a program featuring both established and up-and-coming flamenco artists every Wednesday evening. Some of New York's finest dancers can be seen there in a program hosted by Ms. Del Conte and with fine musical accompaniment of Arturo Martinez on guitar and Paco Ortiz, cantaor. The American Spanish Dance Theatre has been producing this event since January and will be there through the summer. A complete dinner plus the show is $20 or standing room at the bar for $10. Ararat Restaurant is located at 1076 First Avenue (58th Street) in Manhattan.",
    "title": "Cover: Carmen Amaya, oil on wood, by Luisa Triana",
    "periodical": "jaleo",
    "issue_id": "JALEO_1991_07",
    "year": 1991,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 996,
    "article_char_count_full": 5896,
    "article_char_count_review": 5896,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
