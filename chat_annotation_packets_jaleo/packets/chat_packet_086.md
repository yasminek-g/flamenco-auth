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
    "article_id": "JALEO_1980_09::A18",
    "article_text_for_review": "By Marta del Cid We were adults and we were children. We were men and women, boys and girls. We were social workers, students, systems analyst, artist, airline employee, interior decorator, doctor, steel worker, chemist, and candy man. We were from Ohio, Pennsylvania, Michigan, Minnesota, Georgia, and Spain. But these were only surface statistics that no one particularly cared about, for we were together for flamenco. Joan and Larry Temo opened their home and their hearts for what may become the traditional Independence Day weekend juerga. Their home and pool in a secluded woodsy location made for a perfect flamenco retreat, with a tablao set up on the huge back deck and another tablao in the basement. The whole weekend was one constant flow of friendship and creative sharing that was marked, in my mind, by three high points, all of which were achieved through the extremely sensitive and dynamic toque of Greg Wolfe. We were blessed with the presence of no less than ten marvelous guitarists who each contributed in his or her own special way to the ambiente of this amazing experience. I was keenly aware of each one of them and would have detected an absence immediately had one had to leave. A juerga is such a communal effort that it usually is impossible to single out one participant, but I know that everyone was profoundly affected by Greg's playing. Friday evening was a little slow to get started with everyone still getting acquainted, so we had the first showing of the film \"Flamenco\" while we visited and got settled. Later there were sessions on the deck and then in the living room when the competition of fireworks next door and rain proved too much and the tablao was moved inside. The first magic occurred when I slipped away from all the gaiety to discover Greg in the basement working with a singer who had emerged at the last juerga, José Luis Giménez, who is from the mountainous area east of Granada. José was on a beautiful run, going through serranas (his specialty), peteneras, tientos, bulerías and fandangos. Greg's accompanying was thoughtful and stimulating with quick stops and slow spaces. I never wanted it to end, but gradually others were discovering our hiding place and the background noise proved too distracting. The singing stopped, guitar was put away.",
    "title": "AN AKRONISM: JUERGA IN OHIO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_09",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "30",
    "page_number": 30,
    "word_count": 391,
    "article_char_count_full": 2307,
    "article_char_count_review": 2307,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_09::A19",
    "article_text_for_review": "by____? REPORTER $ \\underline{\\text{NEEDED}} $: to cover San Diego Scene! It has, again, been brought to our attention, that local Jaleistas are dissatisfied with the coverage (or lack of it) given to San Diego flamenco in JALEO. This column is being established to fill this void. Our policy, all along, has been \"If you'll write it, we'll print it.\" We appeal, again, to local members who attend juergas and enjoy them - who attend or participate in local performances - who have a few friends over for the evening and a mini-juerga develops - jot down your impressions and send them in to JALEO. If you are interested in becoming a member of the $ \\underline{\\text{JALEO}} $ staff as our local roving reporter or would like to revive the defunct EL OIDO column which contained personal tidbits such as marriages, births, trips abroad etc., let us hear from you. JUNE JUERGA UNDER SUN AND STARS By Juana de Alva The June juerga was our first experiment in an extended weekend campout. Jaleístas trekked from as far away as central California and Nevada in modern day gypsy wagons to set up camp in the semi-desert terrain of Bob and Vicki Dietrich's finca in Tecate, U.S.A. This was not a juerga for everyone. The less hardy souls who had planned to retreat to the comfort of a hotel room were disappointed to learn that the only hotels nearby were across the border which closes at midnight. (Who ever heard of leaving a juerga at eleven-thirty?) The distance factor made it prohibitive for most of our performing singers, dancers and guitarists to drop in although Paco Sevilla did manage to spend a few hours with us on Saturday and Sunday between performances. MAGDALENA & HOSTESS VICKI DIETRICH IMPROVISE TO SOLEARES (top) GATHERED AROUN CAMPFIRE (right) ARE: EL CHELENO, JUANA & DAVID DE ALVA, VICTOR GILL & CUADRO LEADER, BENITO (below) LOS ANGELES & SAN DIEGO YOU YOUNGSTERS JOIN IN SEVILLANAS, L-R PILAR, MARIA, TRISHA AND MICHELE 1984年10月16日",
    "title": "SAN DIEGO SCENE: JUNE JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_09",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "31-32",
    "page_number": 31,
    "word_count": 334,
    "article_char_count_full": 1953,
    "article_char_count_review": 1953,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_09::A20",
    "article_text_for_review": "Appologies to our local members and the August juerga hosts for the absence of a juerga notice. It was inadvertently removed with some other material because of lack of space. Our thanks to Diego Robles and Chuck Thompson for their hospitality and efforts to notify members. There home made an excellent juerga site. Juerga report and photos will appear in next issue.",
    "title": "AUGUST JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_09",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "33",
    "page_number": 33,
    "word_count": 62,
    "article_char_count_full": 368,
    "article_char_count_review": 368,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_09::A21",
    "article_text_for_review": "In September JALEISTAS return to the top of Mount Soledad (not Solea, but almost), to partake of the exquisite view - to stroll through befountained patios and gardens and raise 'jaleo' at the home of Francisco and Elizabeth Ballardo. Francisco and Elizabeth are two of our most stalwart JALEISTAS. Besides opening their home for the third year in a row for a summer juerga, they sponsor and promote flamenco in San Diego in every way they can. They frequent the shows of local performers, are active JUNTA members, have donated supplies and equipment to the JALEO magazine and contributed financially to the JALEISTAS organization. Elizabeth is actively studying flamenco dance and Francisco is absorbing it through his pores and becomes more proficient in his improvizations every juerga. This will be another Sunday juerga with CUADRO A in charge. (See JUNTA REPORT for CUADRO A members.) It will be held on the fourth Sunday instead of the third, to avoid conflict with the Cabrillo Festival. One highlight of the juerga will be the showing of a recent T.V. presentation on JALEISTAS. Members are encouraged to be selective in their guest invitations. The two guest limit per member and twenty guests per juerga mains in effect. DATE: Sunday September 28th PLACE: 6271 Soledad Mountain Rd., La Jolla PHONE: 454-4086 TIME: 4:00pm to? BRING: Warm wrap for evening hours and food corresponding to the 1st letter of your last name: A - E Main Dish F - L Desert (fresh fruit) or Chips & Dip M - Se Main Dish Sf- Z Salad $ \\underline{\\text{and}} $ Bread GUESTS: By reservation only. Call Thor or Peggy Hanson 488-4139",
    "title": "SEPTEMBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_09",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "33",
    "page_number": 33,
    "word_count": 274,
    "article_char_count_full": 1615,
    "article_char_count_review": 1615,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_09::A23",
    "article_text_for_review": "A PALO SECO -- singing without musical accompaniment; as in martinates, saetas, deblas. ABANICO (el) -- fan; often used in theatrical presentations of flamenco dance (especially caracoles and guajiras); the extra large fans are called PERICONES. ACTUAR -- to perform; ACTUACION = performance. AFICIONADO (a) -- an enthusiast or fan who does not perform; also used to refer to an artist who does not perform professionally. AFINAR -- to tune the guitar; AFINADA = in tune; DESAFINADA = out of tune. AIRE (el) -- the style, air, or flavor of one's performance. AL AIRE -- open playing position, without the use of a cejilla. ALPARGATAS (las) -- canvas-topped shoes with rope soles, used for dancing jotas and other regional dances. ALZA PUA (el) -- means literally \"to lift the pick;\" refers to a guitar technique in which the right thumb strums chords and plucks individual notes in various rapid combina-tions, sometimes accompanied by simultaneous tapping with the ring finger. ALMERIA -- part of the \"Levante\" on the border of the flamenco region of Andalucía; from here comes a form of danceable fandango and the mining songs of the tarantas- tarantos family. ANDALUCIA -- the southernmost region of Spain, where flamenco originated and was developed; the people are called ANDALUCES (singular is ANDALUZ or ANDALUZA). APOYANDO -- using the rest or supported stroke in guitar playing; from the verb, APOYAR. ARETES (los) -- earrings. ARPEGIO (el) -- arpeggio; pluking the notes of a chord singley, in succession rather than simultaneously. AROS (los) -- the sides of the guitar; usually made of cypress or rosewood. BAILAOR (a)-- flamenco dancer. BAILAR -- to dance. BAILE (el) -- the dance. BARCELONA -- a major city in Cataluna books (northern Spain) which is outside of the flamenco region of Spain; however, the gypsy population of this city has produced a number of outstanding flamenco artists, including Carmen Amaya, La Chunga, and La Singla. BARRAS (las) -- the braces inside the guitar. BATA DE COLA (la) -- the full flamenco dance dress with its long train of ruffles; it is an elaboration of formal wear of the past. BOCA (la) -- the soundhole (mouth) of the guitar. BOTAS (las) -- boots; low-topped boots worn by male dancers are often called BOTINES. CARMEN AMAYA ISSUE We hope to dedicate the November issue of Jaleo to Carmen Amaya. If you have any items of interest on the subject, please send them to us before the end of September. We are interested in such things as articles, photos, newspaper clippings, programs, etc. It is up to you readers to make this a memorable issue. BULK RATE U.S. POSTAGE PAID La Mesa California Permit 368 TIME VALUE RETURN POSTAGE GUARANTEED",
    "title": "FLAMENCO DICTIONARY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_09",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "36",
    "page_number": 36,
    "word_count": 450,
    "article_char_count_full": 2694,
    "article_char_count_review": 2694,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
