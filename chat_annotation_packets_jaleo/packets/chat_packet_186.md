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
    "article_id": "JALEO_1984_01::A19",
    "article_text_for_review": "MIAMI On a recent \"escape the cold weather\" vacation that took me to Florida, I was able to see the flamenco presentations available - very few, for a completely spanish speaking Miami. The end of the year is not the time to see flamenco: La Tati, the fabulous dancer was back in Spain; the tablao of Gitanilla de Bronze had left Tampa a week earlier and Rosita Segovia Antonio's partner for many years was in Barcelona and was not returning to Miami before January 1984. EL CID on LeJeune, near Flagler Street in Miami is the top spot for Spanish entertainment. This is where La Tati performed and is returning for 1984. The group had Jose Miguel Herrero (bailaor) as its leader. There was Elena (bailaora) with guitarist Pedro Cotres Jr., one of the best modern style players. COSTA VASCA--La Taberna, 5779 SW8th Street in Miami has the tablao of Cacharrito de Malaga with Paca de Malaga (bailaora) and Don Pelayo on guitar. Between the flamenco acts Cacharrito is a continuous show of humour. The tablao was packed to capacity and there was king Cacharrito. For the very last number the trio performed Tangos Canasteros which was dedicated to my presence there. COSTA BRAVA--2525 N. Federal Hwy., Fort Lauderdale. Here the tablao was led by Adriano de Alessandro (bailaor, cantaor) with Patricia Cortés and Liliana Morales (bailaoras) with the guitars of Manolo Vargas and Pedro Cortes Sr., Adriano is a very good entertainer and had known Carmen Amaya. These artists presented a full and good show. THE COLUMBIA RESTAURANT is located in the historic Ibor City, the old Spanish town of Tampa. The restaurant was first founded at the beginning of the century and has been owned by the same family, which I believe came from Cuba at the turn of the century. After Jose Molina's exquisite show, the owner Don Cesar entertained his guests with some beautiful violin playing. Gabriel Cortes and his wife La Gitanilla de Bronze had left for Spain a week earlier. The Columbia Restaurant PRESENTS \"Jose Molina Spanish Dance Co.\" \"THE BALLROOM\"--253 W. 28th Street--after tapas and vino, full scale flamenco presentation headed by Carlota Santana Melinda Marques and Jorge Navarro (bailaores), cantaor Luis Vargas, guitarist is Ricardo Amador--at present six shows have been scheduled for the weekends. RESTAURANT ESPAÑA 46--on 46th Street (near 9th Ave.) has Mara Solanis and Manolo de Cordoba (bailaores), Agujetas, cantaor and Miguel Céspedes on guitar -- 2 or 3 nights on the weekends. VILLA DEL PARRAL (old Bilbaina) on 14th Street has Jesus Ramos and La Tata (bailaores) and Diego Castellon, ever popular brother of Sabicas, on guitar, the cantaor is Domenico Caro. RINCON DE ESPAÑA, still has the same artists, namely Carmen Rubio and Jorge Navarro (bailadores), cantaor Paco Ortiz, Paco Juanas (guitarist). MESON ASTURIAS on 83rd Street at Elmhurst in Queens has Pepe de Malaga as cantaor, Liliana Lomas (bailaora) and guitarist Reynaldo Rincon. The great Sabicas has left c/o W. J. Adams 53c Lewis Bay Rd. Hyannis, MA 02601",
    "title": "THE RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "article",
    "pages": "21-23",
    "page_number": 21,
    "word_count": 503,
    "article_char_count_full": 3027,
    "article_char_count_review": 3027,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A20",
    "article_text_for_review": "DECEMBER The December juerga found us back at the Sevilla Restaurant in Torrance. The juerga got under way with a sevillanas workshop headed by Rudy Montoya. We passed out sheets containing the words and had a lot of participation. Raul Barrios sang some solos, accompanying himself on the guitar. There was a group from Spain present that added greatly to the ambient. Pilar Moreno, up from San Diego, added her considerable talents in the way of song and dance. Guitarists participating were Benjamin Shearer, Roy Mendez Lopez, David De Alva, Dennis Hannon, Ron Spatz, and Yvetta Williams. Dancers present (those identified) were Marlene Gael, Katina Vrinos, Raul Barrios, Pilar Moreno, Mary Jane Shippen, Louise Carmody Yrma Horta, Eric Cortez, Elissa Forrest, and Elizabeth Wagner. MARY JANE, ELISSA FOREST, ELIZABETH WAGNER DANCING: RAUL BARRIOS AND UNIDENTIFIED JALEO - JANUARY/FEBRUARY 1984 MARIA MORCA AND ENRIQUE VALDEZ GUITARS: DENNIS MCLAUGHLIN, MICKEY KAYNE, BEN SHEARER; DANCING: MARLENE GAEL PILAR DANCING APRIL The April Juerga will be held back at the Long Beach Academy - Studio 2000, hosted by Juaquin and Liza Feliciano, and Oscar and Virginia Robles. 727 South Street, Long Beach. Phone (213) 423-9886. When? Saturday April 21, 1984. 8 p.m. until? JALEO - JANUARY/FEBRUARY 1984 BELLY DANCE: Fine Art of the East An Instructional Video Tape By Marta Schill Co-author and co-star of Universal's Video Disc on Belly Dance. U.S. Rubayat presents A NEW VIDEO EDUCATIONAL SYSTEM, \"BELLY DANCE: FINE ART OF THE EAST,\" AN INSTRUCTIONAL TRILOGY OF VIDEO, AUDIO CASSETTE, AND A WRITTEN PUBLICATION Marta's tape introduces comprehensive instruction featuring beginning and advanced techniques for Beledi, Veil Dance, Tagseem, Floor Dancing and Drum Solo — topped by a Cabaret performance displaying your newly acquired talent! Have a Workshop at Home! Presenting Fourteen Chapters covering every aspect of Cabaret dance, including finger cymbal instruction and complete choreography for a six-part performance. Enjoy months of training condensed into sixty minutes, on your choice of VHS or Beta-Max. Taped with PANAVISION system sound stage. at Sound track recorded at Tele-Music. Produced by U.S. Rubayat. Featured in the April, 1983 edition of \"American Cinematographer\" magazine. <table><tr><td colspan=\"2\">ORDER FORM\\n—Please print—</td></tr><tr><td>Name</td><td></td></tr><tr><td>Address</td><td></td></tr><tr><td>City</td><td></td></tr><tr><td>State</td><td></td></tr><tr><td>Phone</td><td>Zip</td></tr><tr><td>VHS Tape</td><td>Amount</td></tr><tr><td>Beta-Max Tape</td><td>$65.00</td></tr><tr><td>Cassette of Dance Music</td><td>$65.00</td></tr><tr><td>Corresponding Publication</td><td>$5.00</td></tr><tr><td></td><td>TOTAL</td></tr><tr><td></td><td>All prices include shipping\\n(Please allow 6 to 8 weeks for delivery)</td></tr><tr><td></td><td>Send check or money order payable to:</td></tr><tr><td></td><td>U.S. Rubayat</td></tr><tr><td></td><td>P.O. Box 204</td></tr><tr><td></td><td>Rosemead, Ca. 91770</td></tr></table>",
    "title": "LOS ANGELES JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "24-26",
    "page_number": 24,
    "word_count": 381,
    "article_char_count_full": 3043,
    "article_char_count_review": 3043,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A21",
    "article_text_for_review": "SAN DIEGO'S NEW TABLAO FLAMENCO February 1st the Tablao Flamenco celebrated its grand opening with an over-flow crowd of well wishers and flamenco enthusiasts. The opening of the Tablao represents the realization of a long time dream and much hard work on the part of the Ballardo family. Francisco Ballardo, who has been an active member of Jaleistas (including serving as vice president) designed and built the restaurant and the adjacent office building -- giving up his job a year ago to oversee the construction. Elizabeth Ballardo (past treasurer of Jaleistas) handles all the bookkeeping and accounting for the restaurant and staff. Daughters Juanita and Victoria keep things running smoothly by filling in as hostesses, bartenders or waitresses. The restaurant, which serves Spanish food, was built around the semi-circular stage. The dining area is on the upper floor or mezanine and tapas and drinks are served downstairs in the \"patio\". From the performers point of view this is a very nice arrangement because it keeps traffic in front of the stage to a minimum during performances and one does not have to watch a customer chomping on a chicken leg in the middle of ones soleares. The stage is visible from almost any point in the restaurant. The current show, which runs Tuesday through Sunday con- JALEO - JANUARY/FEBRUARY 1984 A FAMILY AFFAIR by Juana De Alva photos by Jack Jackson sists of five dancers, two singers and one guitarist on weekends (two less dancers on weekdays). The dancers are: Juanita Franco, who is in charge of organizing the cuadro, Angela D. Barbara Alba, Carla Heredia and myself. Paco Sevilla is the guitarist. The singers Maria Jose Diaz and Rosa Spires, do double duty working both in the restaurant and on the stage. Rosa is the main chef so she does not join the cuadro until the later part of the evening when most of the dinners have been prepared. As with most construction projects, there were many hold ups and decorative details both on the exterior and the interior remained to be completed after the opening. It has been fun to watch the decorations unfold. Each week has brought new surprises as a bas relief is added to an exterior panel or an arabesque molding to an interior arch. We have gotten into the habit of looking around each day when we come to work to see what new touch has been added. All is scheduled to be completed in April when the press will be officially invited to visit the tablao. Jaleistas intending to come to the Tablao should be forwarded that there is a dress code (no blue jeans) and a $10.00 minimum consumption. Reservations are suggested. Call (619) 483-2703. 1) -TABLAO OWNERS ELIZABETH AND FRANCISCO BALLARDO 2) -DAUGHTERS JUANITA AND VICTORIA BALLARDO WITH OTHER STAFF MEMBERS ROBERTO AND FRANCOIS 3)-DOING DOUBLE DUTY, SINGER MARIA JOSE DIAZ WITH DANCER JUANITA FRANCO 4)-CHEF/SINGER ROSA SPIRES WITH ROBERTO 5)-FRANCISCO BALLARDO CELEBRATING THE OPENING OF THE TABLAO ON STAGE JUANA DE ALVA, CARLA HEREDIA CUADRO OPENING NIGHT - JUANITA FRANCO DANCING {photo by Ray Svetina} photos by Marilyn Perrin A BRIGHT BECON IN THE SAN DIEGO NIGHT $14.95 us Postage & Handling U.S. and Canada - $150 Other Countries - $3.00 Guitar Studios, Inc. 410 Clement St San Francisco, CA. 94118 FLAMENCO RECORDS I WOULD LIKE TO TRADE OR BUY FLAMENCO RECORDS AND MUSIC, IN USA AND FOREIGN COUNTRIES: write to: M. Sherbanee 5329 Norwich Ave. Van Nuys, CA 91411 U.S.A. FLAMENCO DANCE CLASSES NEW BEGINNER CLASSES FOR CHILDREN AND ADULTS ONE BLOCK OFF 3DTH NEAR FWY 94 IN SAN DIEGO CALL JUANA (619) 440-5279",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "27-31",
    "page_number": 27,
    "word_count": 605,
    "article_char_count_full": 3576,
    "article_char_count_review": 3576,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A22",
    "article_text_for_review": "Ventura, CA: Dancer SILVIA SONERA and guitarist FREDI DIONISIO presented a valentine flamenco concert titled CONCIERTO D'AMOR on Sunday, February 12th at the Ventura College Theater. (from B.H. Enterprises, Inc.) Washington, D.C.: RAQUEL PENA presented a solo concert with guitarist FERNANDO SIRVENT and singer PEPE DE MALAGA January 28th & 29th at The Dance Place. (from D.C. Wheel Productions) San Francisco, CA: ROSA MONTOYA BAILES FLAMENCOS gave two benefit performances February 24th & 25th at the Footwork Dance Studio to commemorate its tenth anniversary. (from Charles Mullen) New York, NY: Guitarist MARIO BSCUDERO gave a solo concert at Town Bell on February 3rd. (from the American Institute of Guitar) Seattle, WA: PEDRO BACAN presented three concerts on the West Coast - one in Seattle, Washington January 29th the other in Berkeley, California February 12th and Chico, California February 11th. He is scheduled to give another concert and seminar in Los Angeles, California in March. (from Jill Snow)",
    "title": "EL OIDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "32",
    "page_number": 32,
    "word_count": 157,
    "article_char_count_full": 1014,
    "article_char_count_review": 1014,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_01::A23",
    "article_text_for_review": "The seventh annual competition for ethnic dance choreographers will be held at the Theater of the Riverside Church in New York City on May 26, 1984, from 12 noon until 5 P.M. Cash prizes will be awarded at a reception in the Fall to the first, second and third place winners. The event is sponsored by Ethnic Dance Arts, Inc., headed by La Meri, one of America's greatest pioneers in this field of dance. The awards encourage ethnic dance choreographers to depart from traditional presentations and use the specific techniques of their particular genre in a purely creative way. It is to be noted that Jazz and Tap dancing are both considered forms of American ethnic dance. Each entry should not exceed fifteen minutes in length, and though costuming is not required, it is advisable to create the maximum theatrical effect. The choreographers will be evaluated by a panel of five prestigious judges (critics/dancers) headed by La Meri. The emphasis of the scoring will be on originality of concepts and its realization. If it is not possible to present the work in Hew York in May, works can be entered on VHS videocassette. May 10, is the closing data for submission. Ethnic choreographers wishing to compete may send for entry blanks and further information to the following: EAST COAST - La Mari, 77 Circuit Ave., Byannis, MA 02601 WEST COAST - Dr. Charles Miller, 2121 Bonsallo Ave., Los Angeles, CA 90007 NEW YORK CITY AREA - Mariano Farra, 3 Sheridan Square, New York, NY 10014 CORRECTIONS CORNER October/November '83, page 17 photo caption should read \"Callarchino cuadro (left to right standing): Serio, FELIPE LOPEZ, Arturo Martinez, Manolo Segura",
    "title": "RUTH ST. DENIS AWARD",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_01",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "32",
    "page_number": 32,
    "word_count": 278,
    "article_char_count_full": 1658,
    "article_char_count_review": 1658,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
