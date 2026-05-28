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
    "article_id": "JALEO_1982_12::A9",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nGYPSY MUSIC RECORDED AT THE FESTIVAL DF STES, MARIES DE LA MER, 1955 (LONDON TWB 91127) RECORD REVIEW by David Carl Blakley (David Carl Blakley is an adventurer, thinker, and aficionado, as well as a gypsy at heart, ever on the move. He sends us some impressions of flamenco experiences he had while in Washington, D.C. The writer has recently been submitting himself to repeated hearings of what is certainly the finest recording of an \"agrupación gitana\" be has ever had the great good fortune of having heard. Discs in general are quite disappointing when such \"juergas\" are recorded. Also, that extra measure of \"penetración intensivo,\" which causes bystanders and/or fellow performers to exclaim \"¡Salero!\" fails to \"come-off sufficiently\" to justify (let alone necessitate) repeated\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"public\"]\n\nn \"agrupación gitana\" be has ever had the great good fortune of having heard. Discs in general are quite disappointing when such \"juergas\" are recorded. Also, that extra measure of \"penetración intensivo,\" which causes bystanders and/or fellow performers to exclaim \"¡Salero!\" fails to \"come-off sufficiently\" to justify (let alone necessitate) repeated listenings. But that disc was damned hard to put down. And it was damned hard to go back to the public library with it — as Ray Charles (a fine singer of cante jondo of a different flavor) sang in the tune \"Busted\": \"Lord! I am no thief, but a man can go wrong...\" To reflect my sentiments, I might only add, \"...when he has such a non-duplicable musical'momenta de verdad' in his possession.\" Agreeing heartily with García Lorca's dictum \"flamenco is close to liturgy,\" I approach this review with some misgivings, because I take to heart Hemingway's dictum, \"A writer must have the prcbity of a priest of God.\" Indeed, \"these arc the times that try men's souls.\" The record proved extraordinary from diverse aspects, both major and minor. The introduction on the jacket states: \"In recording this Festival, it was hoped to find some link between flamenco-type voice-production and the songs of the wandering Bauls from Hengal, whose vocation\n\n[ENDING CONTEXT]\n\nhambre y con frío, sin echarle cuenta, Maria llamaba. Y en su vientre, cercao de lirios, al Cristo de uvas y espigas llevaba. Y espigas llevaba, a postigos, de tierras dormías, de paro y de mieo, María llamaba. Retablo flamenco de nochebuquay por_Antonio Murciano Esta noche nadie sin hijo y sin casa, sin vino y sin lumbre, sin zurrón ni manta. Esta noche, todos, su amor y su hogaza, su copla en los labios su paz en el alma. Porque es Dios quien Níño, nacio entre esas pajas, yo os juro que es hoy la noche más santa, la noche más niña, la noche más casta, la noche más bella, la noche más alba.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "poem",
    "pages": "24-30",
    "page_number": 24,
    "word_count": 2322,
    "article_char_count_full": 13656,
    "article_char_count_review": 2914,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "public"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_12::A10",
    "article_text_for_review": "(from: Sevilla Flamenca, Dec. 1980; translated by Paco Sevilla) by Manuel Alvarez López [Editor's Note: Joaquín el de la Paula is one of the legendary figures in flamenco history. He played an important role in developing the cantes de Alcalá -- the solea de Alcalá is the style of solea most often heard today. His cante was carried on by his cousin Mançita el de la María and his nephew Juan Talegas.] For Joaquín and his two children, Enriquillo and Iliniesta, that Christmas Eve would be passed in the same poverty and sadness as so many others in that harsh winter of 1930. On the tattered oilcloth that covered the small table as a sort of tablecloth, Iliniesta had placed all of the food that would comprise their frugal meal: some pieces of bread, three skimoy pieces of herring, a small pot of olives, and a handful of chestruts that Joaquín had been given by El Moreno, the owner of a small stand in El Duque, a place where Joaquín went daily to sit for a few hours in the heat of the small stove where El Moreno toasted his delicious fruit from the mountains. Joaquín was lamenting, as Enriqueillo agreed with light movements of his head, about how badly things were going, how it had been more than a month since there had been an animal to shear, and it seemed that the \"senoritas had lost their desire for the cante, for he had not been called to perform in a juerga since the end of the summer. Suddenly, interrupting his dark monolague, Joaquín directed his gaze toward the dark roof of the cave and, reaching out his arms, exclaimed pathetically, \"Is it possible, my God, that you can allow some to have so much and us to have not even a drop of brandy to toast the happy birth of your son?\" Joaquín had not uttered his last word when, from the broken door of the cave he heard the shrill voice of Indigena, the waiter at the Venta de Platilla, shouting his loudest from halfway up the hill, \"Joaquín, El Plata says you should come down to the Venta, there are some men who want a juerga!\" A shiver went through Joaquín upon hearing that; he jumped to his feet, pulled his hat down to his ears, raised",
    "title": "CHRISTMAS WITH JOAQUIN EL DE LA PAULA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "31",
    "page_number": 31,
    "word_count": 391,
    "article_char_count_full": 2118,
    "article_char_count_review": 2118,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_12::A11",
    "article_text_for_review": "NEWS FROM OUR MEMBERS EL OIDO Minneapolis, MN: Zorongo Flamenco with Susana and Michael Hauser were joined at their regular engagement at \"George Is in Fridely\" by dancer Manolo Rivera and singer Dominic Caro. Zorongo also preformed in concert in Dallas, Texas, and at the Janet Wallace Fine Arts Center in Minneapolis. (from Suzanne Hauser) Chicago, IL: Ensemble Español presented Spanish Dance in Concert the last weekend in October and the first weekend of November under the direction of Libby Komaiko with guest artists dancer Victoria Koijhan, guitarists Greg Wolfe and Tomás de Utrera, and singer Pepé Culata at Northeastern Illinois University. (from NIU) New York City: Cantaor Agujetas is back in New York at the Rincon de España with his dancer-wife Tibu and guitarist Roberto Reyes. (from George Ryss) Chicago: Cantaor-guitarist Jesus Ribon presents a varied flamenco program at the Toledo Restaurant with three dancers and two guitarists. (from George Ryss) Washington, D.C.: Following the successful summer appearances of Ana Martínez and Paco de Málaga, Ana was engaged as choreographer and solo dancer for the Houston Grand Opera production of Bizet's \"Carmen\" at the John F. Kennedy Center for the Performing Arts. (from Ana Martínez) THE IDEAL STRING for the most demanding guitarist A premium string designed especially for the top line of flamenco guitars—the choice of mony leading guitarists, classical as well as flamenca. At your local dealer or contact: Antonio David Inc., 204 West 55th Street, N.Y.C. 10019—(212) 757-3255 (212) 757-4412 REYNOLDS S. HERIOT OWNER - MANAGER Gift Subscriptions to Jaleo GIVE A GIFT AT CHRISTMAS AND HELP JALEO TO DOUBLE ITS MEMBERSHIP AND BECOME SELF-SUSTAINING. * * Take advantage of the old rates before they go up in January. * * A notice of your gift will be sent to the recipient. * * Subscription will begin in January. JALEO SUBSCRIPTION name telephone country",
    "title": "EL OIDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_12",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "32-33",
    "page_number": 32,
    "word_count": 308,
    "article_char_count_full": 1924,
    "article_char_count_review": 1924,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_01::A1",
    "article_text_for_review": "required of the dancer. The rhythm of the dances of the \"escuela\" is marked by castanets, not by the feet as it is in flamenco. The sound patterne are more varied than in most of the regional dances, except possibly those from Andalucía. Each castanet sound corresponds precisely to a foot or body movement. Accompaniment is constant during a dance, and for this reason bolero dancers often prefer cloth castanets (palillos de tela plastificada) as they are less likely to break during a performance. The dances of the \"escuela\" are usually done in \"zapatillas\" (ballet slippers). Some, however, which focus on patterns created by partners of lines of dancers, rather than on jumps, beats and extensions, give the female dancer the option of wearing supple, low-heeled pumps. A few dances, which developed later in the nineteenth century, require the dancers to wear \"eapatos\"; sometimes called \"teatrales,\" these dances combine the simpler aspects of the \"escuela bolera\" and the flamenco zapatado. The \"soleares de Arca\" is typical of this type of choreography, often claimed by the \"escuela\" but performed by both bailarina and bailaora. The name Pericet has been synonymous with the \"escuela bolera\" for many years. Angel, Eloy, Luisa, Carmen and Amparo, the fourth generation of this remarkable family, are all accomplished in the flamenco and regional dances of Spain and the folk dances of Latin America; their choreographies of the works of Spanish and foreign composers have been performed in the Teatro Colón, Carnegie Hall and on other major stages. But the family is best known for the careful preservation of the Spanish classical dances, exactly as their grandfather and great aunts performed them. Angel Pericet Carmona (1877-1944) organized the steps and exercises of the \"escuela\" into a progressive course of study, consisting of a preparatory level followed by three \"cursos,\" each subdivided into three groups. The student, as he progressas through the syllabus, also learns the \"escuela\" dances which correspond to his level. So many well-known Spanish dancers have studied the Escuela #olera de Angel Pericet that it is almost easier to name those who have not. Luisa Pericet, who lives in Buenos Aires, has carried on this teaching tradition. She allows seven years for completion of her program in Spanish dance, which requires mastery of her grandfather's course as well as zapateado and castanet technique, dance notation, some pedagogy, knowledge of the origins of the dances and of bolero, flamenco, folk and contemporary choreographies. Even for a dancer who intends to do only flamenco, there are advantages to be gained from this type of instruction. Familiarity with the \"escuela bolera\" at the very least gives a dancer more cultural depth, a desirable attribute, since authenticity is a primary goal in Spanish dance. Systematic training increases flexibility, agility, stamina, and caetanet coordination. Furthermore, the \"escuela\" shares many steps in common with the Andalucian dances usually learned by students of flamenco. The sevillanae, verdiales, and fandango de Huelva use the \"sease y contra sease,\" \"pas de vasco,\" \"padebure,\" \"matalarana,\" jerezana alta\" and other steps included in the \"escuela.\" Even \"pure\" flamenco dances require mastery of the various turns and escobillas. These movements are not always studied in isolation in most flamenco classes, the way the components of a zapteado might be, and the \"escuela\" class often provides the only opportunity to polish them. Finally, the \"escuela\" provides additional material for the dancer who wants to choreograph the music of the contemporary Spanish composers. \"Escuela bolera\" choreographies such as the \"sevillanas boleras,\" \"La maja y el torero\" or \"El ole de la Curra\" are a charming addition to any program of Spanish dance. There can be, though, a problem in performing them for the very audiences who would be most interested in seeing them. Once considered feats of incredible skill, bolero dances are now often termed museum pieces. They are, however, very lovely museum pieces, and their preservation is part of their beauty. No one drastically alters the choreography of style without losing the essence of the dances. The negative side of this fidelity to a tradition is that the \"escuela bolera\" has been comparatively static in its development, while ballet technique has reached an astonishing level of refinement and virtuosity. To spectators accustomed to good ballet, there is the risk that the \"escuela\" dance might look like crudely done ballet. The potential for comparison can be reduced somewhat only by clever staging, meticulous performance, and emphasis on the Spanish characteristics of the dance, rather than on the classical.",
    "title": "ESCUELA BOLERA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_01",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 749,
    "article_char_count_full": 4761,
    "article_char_count_review": 4761,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_01::A2",
    "article_text_for_review": "JALEO FIGHTS INFLATION The $ \\underline{Jaleo} $ staff wishes to thank its readers for the many Christmas cards and kind words of support which it received in December. Also, we are happy to announce that even though we advertised an impending raise in subscription rates for this month, we find that an increase is not essential at this time. Readers can help us to maintain current rates by encouraging flamenco-oriented businesses to advertise in Jaleo (restaurants, dance and guitar suppliers and teachers, etc.) and by helping to increase circulation. We welcome Sandra Nicht as Baltimore/D.C. area correspondent (see \"El Oido\" column) and thank George Ryss (New York) and Ron Spatz (Los Angeles) for their continued updates and contributions to Jaleo. There are still many areas unrepresented. Those interested in being correspondents for their area please drop us a line. We also wish to thank the many new writers who have contributed articles to this issue. This kind of participation is what continues to make Jaleo a vital publication. --Juana De Alva",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_01",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 171,
    "article_char_count_full": 1062,
    "article_char_count_review": 1062,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
