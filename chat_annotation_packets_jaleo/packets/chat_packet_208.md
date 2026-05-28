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
    "article_id": "JALEO_1985_07::A14",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSPANISH FIRE IN THE BAY AREA [From: $ \\underline{\\text{The Daily Californian}} $, March 1, 1985] by Michaela Schneider Flashes of red and legs, gutteral cries, and above all the driving rhythm. No, this is not a Broadway sleaze parade for depraved voyeurs. The synthesis of guitar, voice, and dance that is flamenco reaches sensual heights that defy the boundaries of the average American sensation seeker's experience. But neither is this powerful and subtle art food for ostentatious aesthetics. Rather, flamenco speaks from the old and strangely tortured gut of Andulusia, the southeastern region of Spain. Some say Arab, Indian, and Jewish in origin, it is associated primarily with gypsies. But to define this very living art form in terms of its pedigree is to miss the thrill of feeling it.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"experience\"]\n\nneither is this powerful and subtle art food for ostentatious aesthetics. Rather, flamenco speaks from the old and strangely tortured gut of Andulusia, the southeastern region of Spain. Some say Arab, Indian, and Jewish in origin, it is associated primarily with gypsies. But to define this very living art form in terms of its pedigree is to miss the thrill of feeling it. \"Feel\" is a more appropriate word than to \"see\" or \"hear\" in describing the experience of being caught up in this pulse of sound and movement. To achieve this captivating tension that is flamenco's core, all performers must work together. In this way flamenco is comparable to a tight jazz ensemble. But while jazz is played around an initial melodic theme, flamenco is structured on a series of set rhythmic patterns. Tension comes from the variations of tone and melodic color which change from exhuberant to brooding to dissonant. A familiar parallel is the sound of Miles Davis: repetitious and soulful with painful breaks. Visually flamenco is the ultimate mating dance. Anyone who has seen the dance movie \"Carmen\" cannot deny the erotic appeal. The alignment, arched back, and suggestive gestures are willfully and organically sexual. Women and men accentuate their differences and attractions in the dance-play. Like the music there are set stock phrases to be filled out according to\n\n[ENDING CONTEXT]\n\nsounds of drums and bugles. \"Romantic: Anonymous.\" The achingly lovely strains of this piece evoke the sweet passions and plaintive joys of romantic love. Not well known, the tune is nonetheless one of those you carry around with you for days. Mr. Radford interspersed warm, colorful commentary on such subjects as the history and tradition of flamenco music and his own experiences in Spain among his stunningly played musical pieces. His virtuosity on the finely handcrafted flamenco guitar was apparent even to the most inexperienced listener. All pieces were played from memory since flamenco\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CONCERT REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 1758,
    "article_char_count_full": 10866,
    "article_char_count_review": 2995,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "experience"
      }
    ]
  },
  {
    "article_id": "JALEO_1985_07::A15",
    "article_text_for_review": "MIGUEL BERNAL TO APPEAR IN BLOOD WEDDING Michael Bernal, will be appearing in California in a modern adaptation of Lorca's \"Blood Wedding.\" He will portray the role of the woodcutter. A demanding rold that requires him to be on stage during the entire run of the play, incorporating acting, singing and dance movement. Mr. Bernal's career started at the age of seven. Dance classes became his passion whether tap, jazz, ballet or Spanish, which became his forte. Acting came later only after being offered a featured role in Ross Hunter's movie musical production of \"Lost Horizon,\" dancing and singing with Liv Ullman and Bobby Van. He then concentrated on his acting craft under such teachers as Squire Fridell, Carmen Zapata and special seminars on acting with Lucille Ball. His acting has lead to roles in various stage and television productions. He recently completed an original musical directed by Gene Nelson, and is slated for a guest appearance on HOB's \"Not Necessarily the News.\" Though much time and concentration was devoted towards his acting classes, Mr. Bernal, kept his dance studies going in-teady progress, with teachers like Inesita and Carmelita Maracci. His studies in Spain and Mexico have brought him in contact with unknown artists such as Rosario & Antonio, Pilar Lopez, Ballet Folklorico of Mexico and Jose Greco. After a few months of study with Jose Greco, Mr. Bernal was invited to work as a soloist with Greco and Nana Lorca, performing in concerts and symphony programs. The highlight was performing with the \"Boston Pops Symphony\" during their New Years Eve nationally televised celebration. See \"Flamenco Calendar\" for dates and locations of appearances. EL OIDO Minneapolis, MINN--Between October 21 and November 19, $ \\underline{\\text{Zorongo Flamenco}} $ will tour east to west and south to north in the U.S., plus side trips to Canada. The initial engagement in Kellogg, ID, will set the tone for cries of \"Ole\" that will continue into Vancouver, BC, and later in Las Vegas and San Rafael, CA. With the close of the fall tour, Zorongo will round out with a special fall performance at University of Minnesota's St. Paul Student Center, November 21-22. CAROLYN BERGER (photo by Alan Thewlis) JALEO - VOLUME VIII, NO. 3 A Classic Combination PACO PENA & D'ADDARIO Born in 1942 in Córdoba, Spain, Paco Peña has been playing professionally since the age of twelve and has toured Europe both as a soloist and as part of the \"Paco Peña Flamenco Company\" to wide critical acclaim. Dedicated to conserving the pure artistry of flamenco, Mr. Peña established the seminar \"Encuentro Flamenco\" offering the aficionado an intensive program of study as well as the opportunity to live in Andalucía, the heart of this musical culture. He has recorded nine albums for Decca Records including three live performances and a duo effort with Paco DeLucia, another world renowned flamenco guitarist. He has also made several highly successful tours of Australia, given recitals with the company at festivals in Hong Kong, Edinburgh, Holland, and Aldeburgh and performed to audiences in Japan and London, all to widespread enthusiasm. Paco Peña appears regularly worldwide on Television and has received extensive praise for his shared recitals with John Williams. Paco Peña uses D'Addario Strings. TOMAS DE CHICAGO FLAMENCO GUITARIST TUESDAY THRU SATURDAY 3110 Newport Blvd., Newport Beach, CA 92663 (714) 673-3440",
    "title": "PRESS RELEASES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "22-24",
    "page_number": 22,
    "word_count": 554,
    "article_char_count_full": 3434,
    "article_char_count_review": 3434,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_07::A16",
    "article_text_for_review": "CHENIN DE TRIANA ACCOMPANIED BY EDUARDO AGUERO photos by Dick Williams JANUARY by Yvetta Williams & Ron Spatz This Juerga found us at a new location--the recreation room of a large condo complex in Canoga Park. Our host was Steven Schade, the manager of the complex. The place is ideal for juergas and we have been invited back. In the normal feast or famine syndrome, we had practically every guitarist in Los Angeles there, plus the great cantaor Chinin de Triana, and would you believe, three dancers. On top of a hard day of rehearsals, Coral Citron and Marlene Gael danced their hearts out. (God bless them.) Later on LEFT TO RIGHT: YVETTA WILLIAMS, EDUARDO AGUERO, CHENIN, CORAL CITRON, MARLENA GAEL JALEO - VOLUME VIII, NO. 3 LEFT TO RIGHT: YVETTA, EDUARDO, BILL FREEMON, CORAL, BUY WRINKLE, BENJAMIN SHEARER AND MARLENA Katina Vrinos pitched in. Even with so few dancers, there was tremendous ambience added by Chinin and different guitarists taking turns accompanying him. Guitarists present were Eduardo Aguero, Stamen Wetzel, Benjamin Shearer, Guy Wrinkle, Dennis Hannon, Bill Freeman, Yvetta and Ron. JANUARY '86 The next Los Angeles Juerga will be Saturday, January 11, 1986 from 7:00p.m. to midnight in the recreation room at 13900 Fiji Way in Marina del Rey. Bring your favorite pot luck dish and drink. Coffee and tea and paper plates, etc., will be provided. Rosita McCool has graciously reserved the recreation room for a flamenco party. Everyone who would like to participate is encouraged to come and share in an evening of flamenco music, dance, song and fellowship. We encourage this to be a time of sharing, helping each other to understand and appreciate all the various segments of flamenco and a chance for guitarist, dancers, and singers to work together, get to know each other better, and find ways to promote flamenco. We also encourage those who want to see a flamenco show to go to El Cid and enjoy their show and not to come to the juerga for we have no intention of putting on a show. If you want to be a critic please offer your skills elsewhere and don't come. This juerga is hopefully a place where beginning as well as professional dancers and musicians can get together to share without feeling they are on center stage in a performance. This is a party and it could be a workshop and if a dancer wants to stop in the middle of a dance to show the guitarist where the llamada comes, they can feel free to do this, and we hope this will happen. In the spirit of sharing, encouraging and enjoying a flamenco experience we do hope you will attend and bring friends and family who enjoy being around flamenco music and people. Teachers feel free to come and bring your students. There are visitor parking spaces by the tennis courts and apartments and additional parking at the Fisherman's Village. Take the Marina Expressway to Lincoln then to Admiralty Way to Fiji Way. It is near the Fisherman's Village. For more information call Yvetta Williams (213) 833-0567 or Ron Spatz (818) 883-0932.",
    "title": "LOS ANGELES JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 523,
    "article_char_count_full": 3028,
    "article_char_count_review": 3028,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_07::A17",
    "article_text_for_review": "AROUND THE TOWN San Diego guitar and dance students have taken the initiative to form a small flamenco group to gain performing experience. Their troupe, called \"A Touch of Spain\" has been bringing flamenco to senior citizen homes throughout San Diego. TABLAO FLAMENCO 3567 Del Rep Q81 Osan Diego, Ga. 92109 619 483-2703 JALEO - VOLUME VIII, NO. 3 ABOVE LEFT TO RIGHT: ELENA, MARISA, JERONIMO, CHARO, \"EL PINTOR,\" LISA MELLIZO, CECELIA BELOW: ELENA, LISA AND CECELIA DANCE FANDANGOS WINTER FLAMENCO DANCE WORKSHOP WITH TEO MORCA DECEMBER 26 THROUGH NEW YEARS EVE IN ICICLE CANYON, LEAVENWORTH An in-depth approach to technique, repertoire, and interpretation for a total understanding of the art of flamenco. Five days of classes for the beginner-intermediates starts Thursday the 26th at 1:00p.m., Sunday is free. The workshop will be held in Harriet Bullitts wilderness, forest home -- COPPERNOTCH -- located in the Cascade Mountains four miles from Leavenworth. Everybody will stay in the house with bedding and home cooked meals provided. With luck we will be able to ski outside the door! Enjoy fireside chats around the stone fireplace and dance in the New Year. One overnight guest per dancer is invited for New Years Eve potluck. Return home on New Years Day. The fee is $300 all inclusive. Space is limited to ten dancers, so reserve your space by sending a $100 deposit to Harriet Bullitt. Make checks out to COPPERNOTCH DANCE WORKSHOP. A map and more information will be sent out upon receipt of the deposit. Send deposit to: Harriet Bullitt Coppernotch Dance Workshop 222 Dexter Ave. N. Seattle, WA 98109 wk (206) 682-2704 hm (206) 329-4462 For more information contact Harriett or Kari Glass: wk (206) 382-1141 hm (206) 524-8985",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "26-28",
    "page_number": 26,
    "word_count": 287,
    "article_char_count_full": 1741,
    "article_char_count_review": 1741,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1985_07::A19",
    "article_text_for_review": "australia canada DANCE INSTRUCTION Maximiliano (Toronto) 463-8948 spain FLAMENCO SHOES H. Menkes (Madrid) 232-1036 FLAMENCO COSTUMES Ann Fitzgerald (Sevilla) 423298 JALEO - VOLUME VIII, NO. 3 Columbia Restaurant (Tampa) 813/248-4961 Costa Brava (Fort Lauderdale) 305/565-9015 Costa Vasca (Miami) 305/261-2394 Marbella Restaurant (SW 8th St. 31st Av) DANCE INSTRUCTION Academia de Baile la Rosa (Miami) 305/444-8228 Luisita Sevilla 576-4536 Josita Molina 576-4536 Roberto Lorca 576-4536 Rosita Segovia 642-0671 La Chiquitina 442-1668 Maria Andreu 642-1790 minnesota GUITAR INSTRUCTION Michael Hauser (Minneapolis) 333-8269 Michael Ziegahn 612/825-2952 DANCE INSTRUCTION Suzanne Hauser 333-8269 FLAMENCO COSTUMES Jo Ann Weber 612/291-2889 illinois FLAMENCO ENTERTAINMENT Cellarchino (Chicago) DANCE INSTRUCTION Ridgeville Park District (Evanston) Teresa (Wilmette) 312/869-5640 312/256-0749 texas FLAMENCO ENTERTAINMENT La Mansion del Rio (San Antonio) 512/225-2581 DANCE INSTRUCTION Anita Mills-Barry (Dallas) 214/357-8802 Ricardo Hidalgo (Dallas) 214/352-6798 Teresa Champion (San Antonio) 512/927-9029 Rogelio Rodriguez (Houston) 713/780-1796 Gisela Noriega (Brownsville) 512/541-8509 Ricsado Villa - Dance Center (Corpus Christi) 512/852-4448 DANCE SUPPLIES Casa de Danza (San Antonio) 512/922-0564 Dance Center (Corpus Christi) 512/852-4448 new mexico FLAMENCO ENTERTAINMENT El Nido Restaurant (Santa Fe) 505/988-4340 DANCE INSTRUCTION Eva Enciñas (Albequerque) 505/345-4718 Tamara Spagnola (Santa Fe) 505/983-2914 GUITAR INSTRUCTION Rene Heredia 722-0054 Guillermo Salazar 333-0830 oklahoma <table><tr><td colspan=\"3\">GUITAR INSTRUCTION</td></tr><tr><td colspan=\"2\">Ronald Radford (Tulsa)</td><td>918/742-5508</td></tr><tr><td colspan=\"2\">DANCE INSTRUCTION</td><td></td></tr><tr><td colspan=\"2\">Jimmie Crowell</td><td>946-2158</td></tr></table> washington FLAMENCO ENTERTAINMENT G Note (Seattle) DANCE INSTRUCTION Maria Luna (Seattle) Morca Academy (Bellingham) Josela Del Rey (Seattle) La Romera (Seattle) GUITAR INSTRUCTION Gerardo Alcala (Bellingham) Joel Blair 206/783-8112 206/323-2629 206/676-1864 206/325-2967 206/283-1368 206/676-1864 206/671-6268 oregon <table><tr><td colspan=\"2\">FLAMENCO ENTERTAINMENT</td></tr><tr><td>Norton House Rest. (Portland)</td><td>223-0743</td></tr></table> <table><tr><td>Pablo Picasso (Sherman Oaks)</td><td>818/906-7337</td></tr><tr><td>The Intersection Folk Dance Center Rest.</td><td>213/386-0275</td></tr><tr><td>Sevilla Restaurant</td><td>213/328-2366</td></tr><tr><td>JUERGAS</td><td></td></tr><tr><td>Yvetta Williams</td><td>213/833-0567</td></tr><tr><td>Ron Spatz</td><td>818/883-0932</td></tr><tr><td>ACCOMPANIST FOR DANCE & CANTE</td><td></td></tr><tr><td>Eduardo Aguero</td><td>213/660-0250</td></tr><tr><td>Marcos Carmona</td><td>213/660-9059</td></tr><tr><td>DANCE INSTRUCTION</td><td></td></tr><tr><td>Roberto Amaral</td><td>213/785-2359</td></tr><tr><td>Pedro Carbajal</td><td>213/462-9356</td></tr><tr><td>Rubina Carmona</td><td>213/660-9059</td></tr><tr><td>Manuela de Cadiz</td><td>213/837-0473</td></tr><tr><td>Concha Duran</td><td>213/223-1784</td></tr><tr><td>Carmen Heredia</td><td>213/862-1850</td></tr><tr><td>Maria Morca</td><td>213/386-0275</td></tr><tr><td>Oscar Nieto</td><td>213/265-3256</td></tr><tr><td>Sylvia Sonera</td><td>213/240-3538</td></tr><tr><td>Juan Talavera (Whittier)</td><td>213/699-9855</td></tr><tr><td>Linda Torres (San Gabriel)</td><td>213/262-7643</td></tr><tr><td>Elena Villablanca</td><td>213/828-2018</td></tr><tr><td>GUITAR INSTRUCTION</td><td></td></tr><tr><td>Marcos Carmona</td><td>213/660-9059</td></tr><tr><td>Gene Cordero</td><td>213/451-9474</td></tr><tr><td>Gabriel Ruiz (Glendale)</td><td>213/244-4228</td></tr><tr><td>Benjamin Shearer</td><td>818/348-4023</td></tr><tr><td>CANTE INSTRUCTION</td><td></td></tr><tr><td>Rubina Carmona</td><td>213/660-9059</td></tr><tr><td>Concha Duran</td><td>213/223-3796</td></tr><tr><td>Chinin de Triana</td><td>213/240-3538</td></tr><tr><td>FLAMENCO COSTUMES</td><td></td></tr><tr><td>Rubina Carmona</td><td>213/660-9059</td></tr><tr><td>CASTANETS</td><td></td></tr><tr><td>Jose Fernandez (Reseda)</td><td>213/881-1470</td></tr><tr><td>Yvetta Williams (Imported)</td><td>213/831-1694 or 213/833-0567</td></tr><tr><td>san diego</td><td></td></tr><tr><td>FLAMENCO ENTERTAINMENT</td><td></td></tr><tr><td>Old Town (Bazaar del Mundo - Sun. noons)</td><td></td></tr><tr><td>Tablas Flamenco</td><td>619/483-2703</td></tr><tr><td>JUERGAS</td><td></td></tr><tr><td>Rafael Diaz</td><td>619/474-3794</td></tr><tr><td>DANCE INSTRUCTION</td><td></td></tr><tr><td>Barbara Alba</td><td>619/222-1020</td></tr><tr><td>Juana de Alva</td><td>619/440-5279</td></tr><tr><td>Juanita Franco</td><td>619/481-6269</td></tr><tr><td>Maria Teresa Gomez</td><td>619/453-5301</td></tr><tr><td>Rayna</td><td>619/475-4627</td></tr><tr><td>Julia Romero</td><td>619/583-5846</td></tr><tr><td>GUITAR INSTRUCTION</td><td></td></tr><tr><td>David De Alva</td><td>619/440-5238</td></tr></table> BOX 4706 SAN DIEGO, CA 92104",
    "title": "DIRECTOR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_07",
    "year": 1985,
    "language": "en",
    "article_type": "poem",
    "pages": "30-31",
    "page_number": 29,
    "word_count": 327,
    "article_char_count_full": 5026,
    "article_char_count_review": 5026,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
