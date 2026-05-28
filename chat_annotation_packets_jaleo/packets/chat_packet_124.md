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
    "article_id": "JALEO_1981_11::A11",
    "article_text_for_review": "Sent by John W. Scott For the past two years, the members of the dance company, DANZANTE, have given flamenco aficionados in central Pennsylvania a unique opportunity to become involved in this Andalusian tradition of the juerga. Our latest, and probably most effective, took place at the home of John W. Scott, a dancer with the company. We regretted the absence of two of our performers, Paco, our guitarist, and Virginia Loria, dancer. Paco (Frank Miller) was away in Maine and we missed his golden fingers. Virginia (visiting San Francisco?) armed with her copy of Jaleo, contacted local dancers, teachers, and performers and took a class with Rosa Montoya. She was also able to see several performances in the area. 15 Mosaico Flamenco IN CONCERT Pilar Moreno, Deanna and Paco Sevilla PLUS SPECIAL GUEST ARTISTS: bailaora JUANITA FRANCO concert guitar soloist RODRIGO bailaora PAULA REYES AND MORE in a full performance of flamenco music and dance. Sunday, November 22 at 3:00 PM at the Educational Cultural Complex, 4343 Ocean View Blvd., in San Diego. For more information or tickets, call 282-2837.",
    "title": "AUGUST JUERGA IN HARRISBURG, PENN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 181,
    "article_char_count_full": 1106,
    "article_char_count_review": 1106,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A12",
    "article_text_for_review": "On September 27th, Jaleístas and San Diego folk dancers joined in celebrating the 89th birthday and the beginning of the 90th year of \"Ernesto\" Lenshaw. Ernesto, originally from Denmark, has been involved in folk dance and especially Spanish dance and music most of his long life. He has done everything from painting pictures of gypsy themes, to making castanets, playing the guitar and dancing. After moving to San Diego fifteen years ago, Ernesto immediately got in touch with local flamencos and offered his services as guitarist, artist, castanet maker or in any other capacity. He has been a friend and supporter of flamenco here ever since. His unfaltering enthusiasm and participation has been an inspiration to us all. In a way, because of his dual nationality and his devotion to the Spanish culture, Ernesto embodies the spirit of Jaleístas -- that growing international community of flamenco aficionados. We salute you Ernesto and the rest of you Jaleístas from England, Germany, France, Spain, Finland, Canada, Mexico, Australia, Sweden, Dominican Republic, Japan...and, of course, across the United States. Keep the faith!",
    "title": "SAN DIEGO SCENE-SPIRIT OF JALEISTAS ENTERS 90TH YEAR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 179,
    "article_char_count_full": 1136,
    "article_char_count_review": 1136,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A13",
    "article_text_for_review": "The juerga will be held in the language school of Juan's friend, Silva Jurich. Silva, of Yugoslav descent, has danced and taught folk dances for thirty years. Her language school is centrally located on Fairmount, a block off El Cajon Blvd. It has been converted from a house and the atmos-",
    "title": "-NOVEMBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 26,
    "word_count": 51,
    "article_char_count_full": 290,
    "article_char_count_review": 290,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A14",
    "article_text_for_review": "H. E. HUTTIG Flamenco circles in Miami lament the passing of Curro Zamorano of Cádiz, an accomplished dancer and a cantaor of encyclopedic knowledge of cante chico. We met him through Miguel Mesa, a local tocaor and had the CURRO ZAMORANO IN JUERGA AT THE HOME OF H.E. HUTTIG CURRO DANCING WITH ELENA MARTINEZ",
    "title": "MURIO CURRO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "27",
    "page_number": 27,
    "word_count": 54,
    "article_char_count_full": 309,
    "article_char_count_review": 309,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_11::A15",
    "article_text_for_review": "by Caballero Bonald PART XI -- PUERTO DE SANTA MARIA translated by Brad Blanchard The cante of Puerto, like that of Arcos, and in spite of the geographic characteristics of the area, is also closer to the flamenco embryo of Jerez than the stylistic nucleus of Cádiz. That, naturally, does not mean that the old tradition of cantinas gaditanas -- alegrías, romeras, mirabras, caracoles -- has not had in El Puerto de Santa María (and in Puerto Real, San Fernando and Sanlúcar de Barrameda) a number of important interpreters of that style. But the most typical local cante is tied to the ancient origins of the flamenco of Jerez and Triana. Today it is very difficult, clearly, to find some representative \"port style\" which we could call the \"old flamenco school.\" This is typical -- as we have already insisted -- in all the native zones of the cante, except for some notable exceptions. We had news, however, of some solitary survivors of those primitive, anonymous cantaores of the Puertos -- close to here, in Puerto Real, Francisco Ortega, \"El Fillo,\" one of the most important names among the interesting fundamental cantaores. We made contact with José Reyes el Negro, and through him, with Dolores la del Cepillo and with her brother Alonso -- all old gypsies. Juan de la Plata, Manuel Ríos Ruiz and Joaquín Piserra served as experienced guides for us on the expedition to the lost horizons of flamenco. José Reyes is a nervous, livid man of undefined age, with a type of moral ruin showing in his collapsing physique. He lives in a shack in the old part of Puerto, a very characteristic zone of fine homes and hovels. José Reyes occupies only one miserable room, into which his whole family is heaped. He is surprised that we have come to find him; he distrusts our intent. Only the possibility of earning some money tranquilizes him. At times he works at shining shoes and at others he works in vain, trying to find a job. He speaks in a disordered manner, in a very peculiar jargon, mixing old slang terms with a series of almost unintelligible Andalusian phonetic corruptions. Until recently, José Reyes led an errant life through pueblos on the coast. From a sociological point of view, this fugitive gypsy is a human symbol, representing the fateful history of flamenco. their miserable social structure. We know that many \"tonadas\" and \"romances\" of Castilla were sung since the sixteenth century by gypsies wandering through the country. Our picaresque literature alludes frequently to the fact. These gypsies mastered -- as is normally the case -- the songs they heard around them, modifying them according to their likes and conveniences and they offered them as a street spectacle. Estébanez Calderón, in his Escenas Andaluzas, speaks concretely about some \"romances\" or \"corridas\" still interpreted by gypsies in the first half of the nineteenth century. This modality of \"gypsified romance\" has been almost completely lost in our times, although certain examples are still preserved in popular Andalusian tradition. The gypsies, it is clear, didn't respect themes nor the original editing of those \"romances,\" confusing and mixing styles and episodes. The one called \"Bernardo del Carpio,\" for example, nobly dusted off by Antonio Mairena, demonstrates a previous error of attribution. We have always heard that it is an old interpretation of a primitive \"romance\" known throughout Andalucía, when in reality it is -- with a few insignificant variations -- a take-off on one of the most traditional versions of the \"romance\" of Roldán in which the French hero has simply been substituted for by the Spanish hero. We allude to this because the disorder is in this respect as usual as it is pardonable. José Reyes sang for the Archive a corrida in which is mixed, in a confusion that is almost delirious, fragments of romances alluding to Gerineldo and to the Count Partinuplés with unrecognizable segments, undoubtedly of some spontaneous and anarchic incorporation. Even if only for this habitual ballast, the corrida of José Reyes el Negro was for us -- even within its nonsensical reasoning -- a subject of particular interest. This recording, along with that of Dolores and Alonso del Cepillo, was done during our second trip to Puerto de Santa María. We got together in one of the rooms of a large, noble house converted into apartments. An abundant group of gypsies gathered. In this room lived an old bailaora, Pepa Campos, who enjoyed a certain \"festero\" fame in her younger years. The preparation of the fiesta was difficult. Dolores la del Cepillo sang a strange romance, which we have been unable to place geographically, but whose historic flavor seemed to concede an evident hold in tradition. This gypsy -- whose advanced age allowed her to only sketch out the cante -- called this romance \"nana de Alejandría.\" Her brother Alonso told us that all of the old cantes -- without guitar, of course -- were tonás, nanas, and cambios de siguiriγas. The logical thing, of course, it to suppose that the gypsies, in private fiestas, used that initial exclusive repertoire of tonás and corridas in order to adapt it to each and all their expressive necessities.",
    "title": "ARCHIVO: PART XI",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_11",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "28-29",
    "page_number": 28,
    "word_count": 870,
    "article_char_count_full": 5187,
    "article_char_count_review": 5187,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
