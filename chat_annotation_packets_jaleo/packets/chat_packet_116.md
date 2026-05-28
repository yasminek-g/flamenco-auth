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
    "article_id": "JALEO_1981_08::A18",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTranslated by Brad Blanchard The trip from Puebla de Cazalla to Morón de la Frontera is a few kilometers of pleasant landscape and an extremely bad road. Morón is a lofty, prosperous pueblo, dominated by the Moorish castle from which it takes its name and from which both humble dwellings and noble ancestral homes seem to slip down the hillside. Like Alcalá, Jerez and Utrera -- and like Triana, naturally -- Morón is another of the undisputed cradles of the cante. Let's remember, among other illustrative episodes in this respect, that around 1850 Diego Bermúdez, el Tenazas -- exceptional \"solearero\" -- was born here, and that here lived Silverio Franconetti -- born in Sevilla in 1831 -- that very personal cantaor who emigrated to Argentina and who returned to his land, bringing with him a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"public\"]\n\nber, among other illustrative episodes in this respect, that around 1850 Diego Bermúdez, el Tenazas -- exceptional \"solearero\" -- was born here, and that here lived Silverio Franconetti -- born in Sevilla in 1831 -- that very personal cantaor who emigrated to Argentina and who returned to his land, bringing with him a new expressive register and new social dimension for the cante. Silverio was the first to try to put flamenco within reach of the public, liberating it, in part, from its legendary minority beginnings. Silverio was not a gypsy and neither was his cante; what he lost of that secret racial nature, he gained by making flamenco accessible. We consider of greatest interest, within the historical process of flamenco, this specific contribution of Silverio to the future of the cante. The ancient traditional survivors through gypsy families, the subterranean transmission of styles, the deep ritual of some forms of expression that rarely left their racial borders, was projected as a performance. The drama of a subjugated people was then offered, with very complex moral and materia\n\n[ENDING CONTEXT]\n\ncoarse and impersonal unfurling made us suspect that the pedigree of cantaores of Morón, defined by the shadow of Silverio and El Tenazas, had been interrupted in the latest generation. Andorrano expressed his bulerías in an artificial and bothersome manner, as if adapted to some dull incitements of modern rhythms. Although these couldn't figure in our \"Archivo,\" other cantes festeras sung by unexpected participants would merit inclusion, perhaps, because they faultlessly represented the true and intimate style of singing of the gypsies who are not, in the exact sense of the word, cantaores.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ARCHIVO: PART VIII",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_08",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 1170,
    "article_char_count_full": 7161,
    "article_char_count_review": 2728,
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
    "article_id": "JALEO_1981_09::A1",
    "article_text_for_review": "by Lisa Biggs Tamara Spagnola, of Santa Fe, New Mexico, was hooked on flamenco from the first instant she heard René Heredia play and watched his sister Carmen dance a flashing alegrías. She began to study with María Benítez, an inspiration as a first teacher--she calls it out of you--and danced professionally with Benítez at the famous Santa Fe Opera at age fifteen. For the next six years she performed in traditional tablao settings with such artists as gypsy singer El Pelete, his wife, dancer Isabel Lujan, and guitarist Bruce Patterson, and in concerts with René Heredia. She traveled and studied in Spain with María Rosa. Támara is a small woman with waist-length waves of brown hair and a pretty face that clearly shows the Mediterranean in her heritage. Her father Gian Spagnola's roots are in a Sicilian village close to Palermo, occupied at one time by the Spanish, which is exactly what Spagnola means in Italian. Her mother comes from one of those rare pockets in America that got a bit lost in time: the isolated, antiquated culture of northern New Mexico, where settlements date from conquistador expeditions, and centuries of mixing with Indian blood have not erased the predominantly Spanish language and customs. Támara's mother, María Severa Lucrecia Elisaida Gallegos Spagnola, remembers her great-uncle Francisco singing the old Spanish folk song \"La Tarara\" that Lorca included in his collection. Gypsies once sang and danced through the night near her adobe in El Gauche, where her grandfather caught a gypsy man stealing chickens outside as a gitana busily read his wife's palm in the kitchen. The Spanish Penititente brotherhood celebrated their secret rites of self-mortification in the mountains of El Gauche; Elisaida remembers their sharp eerie wails on Good Friday. Her Uncle Francisco's voice was among them. Támara's dance has an authority that comes from inside. Her face and body can radiate an intense stillness, then she suddenly bursts out with a driving compás that is at once inventive and unerring. Steps cut across the beat in startling combinations complementary to the music, somehow surprising and traditional at the same time. With Támara, flamenco is a family thing. Támara feels that her husband, guitarist Peter Culbert, has taught her more than anyone, taught her \"how to sing, how to listen to singing, how to appreciate the music\" all its subtle differences. She also feels that having a strong family provides a source of strength in her dancing. For the future? \"I like to do concerts. René is wonderful to work with. His knowledge and sensitivity to",
    "title": "LA TAMARA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_09",
    "year": 1981,
    "language": "en",
    "article_type": "poem",
    "pages": "3",
    "page_number": 3,
    "word_count": 427,
    "article_char_count_full": 2605,
    "article_char_count_review": 2605,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_09::A2",
    "article_text_for_review": "FLAMENCO DIRECTORY (Since the writing of this editorial, response has increased considerably and it appears we will be able to publish. The points made here still apply, however.) In a sense, the flamenco enthusiasts of North America have voted \"No\" on a directory. Fewer than 20% of Jaleo's subscribers have responded, and probably fewer than 5% of the flamenco performers and serious students have sent in information for the Directory. This is not to say that the response we have received is insignificant. Most of the big flamenco companies have sent their information and a great many of the well known artists (the ones who know how to earn a living with flamenco) have responded. The material we have received is extremely interesting and any aficionado would enjoy reading it; the photos alone are a real treat. But we are reluctant to go ahead with publication for several reasons. First, with the number of people involved, we might not be able to sell enough copies to cover costs. Second, the picture it gives of flamenco in North America is truly distorted. Places like Denver, Colorado, and San Diego, Calif., came out looking like hotbeds of flamenco (because interested individuals made a point of getting everyone to send in a form -- or did it for them). While the real flamenco centers -- New York, Texas and Los Angeles, are barely represented.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_09",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 231,
    "article_char_count_full": 1365,
    "article_char_count_review": 1365,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_09::A3",
    "article_text_for_review": "Dear Jaleo: Looking through my collection of partially read or forgotten back issues today, I realized that what Paco Sevilla says in his article last month is quite correct. I enjoy Jaleo each month tremendously, and this is partly to express gratitude for the efforts of all who contribute to it. If I were anywhere nearly as knowledgeable as Paco, or if I were in a position to catch a live flamenco performance, my note of interest would be fast forthcoming. But, alas, I see (except for records at fleamarkets) nothing of flamenco in Arkansas. Some day I will go to Spain, but for now Jaleo is my juerga when I receive it. It is much appreciated. And lest any of you people who share knowledge through it doubt, I am glad you can see your way to write such an informative and enjoyable \"newsletter\". Claudia Fayetteville, AK We recently received this photo with the announcement of the marriage of cantaor, Chínín de Triana to dancer Sylvia Sonera. \"Congradulations,\" Chínín and Sylvia!",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_09",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 172,
    "article_char_count_full": 991,
    "article_char_count_review": 991,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_09::A4",
    "article_text_for_review": "GISELA \"Gisela and Her Flamenco Fiesta\" will soon be tour-in the United States under the auspices of Columbia Artists/Community Concerts. Unfortunately, it is one of those whirlwind tours, here today, in a different state tomorrow, and it will be hard to find out when and where the concerts will be held. Presently based in Texas, Gisela was born in Mexico City. She studied with Tarriba, Manolo Vargas, and Antonio Cordova, and later in New York with the Joffrey Ballet and the American Ballet Theatre. Her performances have included a debut at the Jacob's Pillow Dance Festival in 1972 and a return engagement in 1976. She has also been a guest artist with First Chamber Dance Company, the St. Paul Opera, the Miami Philharmonic, and symphony orchestras in Mexico City and Jalapa. Here is what one reviewer said of a past \"Gisela\" concert: \"The opening feature for the Darien Community Concerts Association season provided a highly entertaining and gifted glimpse of flamenco\" dancing, a veritable travelogue of Spain, by Gisela in her 'Flamenco Fiesta.' Dancing with the celebrated artist, with her clicking castanets and expressive heels, was William Carter. Gisela brought colorful gowns, deftly woven into the patterns of her wonderful movement through raised skirts and a long train tossed nimbly out of the way for a fast turn. Her artistry was supreme and captivatingly gay, so that her audience enjoyed every portion of the varied dances. They depicted flirtatiousness, sorrow, and and air of Spanish triumph. Luis Vargas sang robustly and in good voice, adding emphasis and color to several of the dance duets and individual solo numbers. Last, but not least, concert guitarist Emilio Prados captured the spirit of the dance program with musical artistry and a Spanish sense of high humor. This and his playing brought great enjoyment for the audience in his special musical interludes.",
    "title": "GISELA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_09",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 308,
    "article_char_count_full": 1898,
    "article_char_count_review": 1898,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
