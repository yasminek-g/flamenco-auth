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
    "article_id": "JALEO_1982_11::A8",
    "article_text_for_review": "Aficionados of terpsichore and flamenco alike gathered at the Athenian Gardens Restaurant in Hollywood this summer to pay homage to a great lady of the Mediterranean arts. At 62, Greek born Katina has finally \"thrown in the towel\" and as Sarita Heredia quipped, \"Become now like a true gypsy because she doesn't work anymore.\" LEFT TO RIGHT: ANA MARIA GUTIERREZ, GISELA LORCA, LUCIA DE LA ROCHA AND IRENE VILLAGRIN HELENA VLACTRON - ORIENTAL DANCER with envy. One particularly unbelievable feat was the demonstration of her ability to flip over every other coin from a row of quarters laid across her abdomen. SARITA HEREDIA",
    "title": "JUERGA FOR KATINA VRINOS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_11",
    "year": 1982,
    "language": "en",
    "article_type": "poem",
    "pages": "16,24",
    "page_number": 16,
    "word_count": 103,
    "article_char_count_full": 624,
    "article_char_count_review": 624,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_11::A9",
    "article_text_for_review": "Several questions were raised about triplet rasgueados following the last article on caracoles. I began with the intention of writing a brief clarification of the fourth variation, by Cepero, but it soon became too involved to be a minor clarification. In that same example (which, incidentally, begins on beat 12), I began the triplet rasgueado with a down-stroke, as was common in the early development of this technique, using the fingering given in example 1: [Ed: We assume that Petér means by the i/m designation, the use of the whole hand in a downward stroke; for many guitarists, the fingers that actually hit are m/a rather than i/m.] Since then, another fingering has become popular and is one that I also use; it begins with an up-stroke with the thumb (see example 2). It is obvious that once you begin repeating this pattern with the right hand several times it really won't make any difference because, no matter how you begin this type of triplet, you are using the same pattern. What is different though is how you come out of it. If you begin the pattern with a down-stroke you will come out of it with a down on the beat following the triplets. Similarly, if you begin with an up-stroke you will come out of it with an up-stroke (see examples 1 and 2). Now you can, if you wish, begin a triplet rasgueado section with an up-stroke and come out of it with a down-stroke, but that means breaking up the right hand pattern which is literally a blur of motion. Not easy! The question of how you come out of these is important because it may mean, for example, that your beat 3 in bulerías or alegrías is going to be played with an up-stroke if you begin these triplets in that direction. That is not only going to change your sound, but maybe throw you off on any up and down strokes that may follow. So it's a good idea to really think and learn these patterns with deliberation. Example 1.",
    "title": "STRUCTURE UP CLOSE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_11",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 346,
    "article_char_count_full": 1906,
    "article_char_count_review": 1906,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_11::A10",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nGUILLE RME TIPS ON LEARNING TO SPEAK SPANISH PART II Last month's article did not cover everything involved in learning Spanish. This article will add a few other tips, but will still leave many things untouched. If you get time send some of your helpful hints to $ \\underline{\\text{Jaleo}} $, or send letters of disagreement and why. Q: I can't seem to memorize the vocabulary words you give. Is there a secret to help memorize them? A: I don't recommend memorizing anything. Don't forget, there are two kinds of memory, short term and long term. Memorization generally provides short term memory, which is good for tests. After the tests are over, the pressure is off, and there is no need to retain anything. Q: I'm not totally convinced that memorizing can't be an effective tool. Even though\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"memorize\"]\n\nthe vocabulary words you give. Is there a secret to help memorize them? A: I don't recommend memorizing anything. Don't forget, there are two kinds of memory, short term and long term. Memorization generally provides short term memory, which is good for tests. After the tests are over, the pressure is off, and there is no need to retain anything. Q: I'm not totally convinced that memorizing can't be an effective tool. Even though you say not to memorize, I will do it anyway. A: That's fine! You have to see it for yourself. Don't just obey! Another problem with memorization is that it is a form of will power. So it creates the battle of \"you against it.\" First there is resistance, then there is willpower, then there is victory. Is that it? If there is resistance, it means that you feel you \"should know Spanish.\" I feel that it is alright not to know Spanish. Later if you have a passion for it, not a short-lived enthusiasm, you will find that learning Spanish is entirely different than you imagined. Q: But your function as a teacher is to generate interest, isn't it? A: Yes, but not to do it by guilt or reward. Giving grades is a form of reward and punishment. Once the grade is given, it reduces a dynamic quality to a static one, much like a phonograph record freezes a performance. Q: I think grades may be beneficial for some people. Don't you? A: Yes, for the people who get good grades. For the others it does not really help. In either case it is assumed that the student i\n\n[ENDING CONTEXT]\n\ncovers. For those familiar with previous attempts to present Spanish or Andalucian music on piano by Arturo Pavón, Campuzano is not in the same mold. I would describe his style as more modern with brass and percussion giving it mass appeal. In fact there are violins, bass guitar, oboe, tenor, baritone and soprano sax, flamenco dancers, flamenco guitar, and \"palmeros\" to make this quite a big production. The flamenco guitarist has a sound similar to Manolo Sanlúcar, but a check of the cast verifies him to be Rafael Morales. Another guitarist also plays rhythm guitar, José Jiménez Jiménez.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_11",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 1305,
    "article_char_count_full": 7456,
    "article_char_count_review": 3120,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "memorize"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_11::A11",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nANDALUCIAN SOUL, GYPSY SPONTANEITY, AND MOORISH SENSUALITY IN THE BAILE ANDALUZ (from: $ \\underline{\\text{La Presna}} $, c. 1940; submitted by Laura Moya; translated by Paco Sevilla) by Juan Martínez The complete success of the baile Andaluz in the Opera of Paris resulted in a flood of visitors coming to Andalucía from other regions of Spain and from other countries to study the dance and served as a stimulus for those who taught, encouraging them to add steps and armwork from the classical school, as well as castanets. The baile gitano took awhile longer to make its appearance, in spite of containing the essence of the Andalucian dance, because it was considered inferior to the castanet dances [note the constant separation of castanets and flamenco in these articles], not being as\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\nserved as a stimulus for those who taught, encouraging them to add steps and armwork from the classical school, as well as castanets. The baile gitano took awhile longer to make its appearance, in spite of containing the essence of the Andalucian dance, because it was considered inferior to the castanet dances [note the constant separation of castanets and flamenco in these articles], not being as complete as these latter forms and being more primitive. On the other hand, the gypsies, who usually danced bare-foot and among their own people, did not worry about whether or not their dances would one day be theatrical and didn't suspect that, with time, they would come to be duly exposed and appreciated on stage and in front of strange audiences. The gypsies only used their dance to entertain themselves and to entertain those who watched them, or to receive a little money for their effort; what they did was so interesting, however, that it attracted an infinite number of curious people to the places where they lived and won them legions of admirers. Since they could make a living from their dances, word spread among the gypsies and an enormous number of gypsy tribes were attracted to Andalucía. There, in natural competition, they each tried to do better things than the others and, what was in the beginning spontaneous and irregular, gradually became transformed into true dances that were finally completed by the Moorish influence and the musical part (guitar and bandurria) that came to take an active place in the gypsy gatherings. Little by little the gypsies became implanted in Andalucía and established their dances. As I have said, the bulerías is the dance most often performed in every villa\n\n[ENDING CONTEXT]\n\nof the other flamencos. The name \"flamenco\" remained and, wherever gypsy dances were performed, they were called \"baile flamenco,\" when the fact is that they are pure baile gitano. SPECIAL OFFER Expires 6/30/83 COMPLETE SETS OR SINGLE STRINGS 1st Rectified Clear or Black Nylon 2nd Rectified Clear or Black Nylon 3rd Rectified Clear or Black Nylon 4th Silver Plated - Balanced 5th Silver Plated - Balanced 6th Silver Plated - Balanced A COMPLETE SET Minimum order $12.00 p.p.d. California residents add 6.5% sales tax Make checks payable to: Lester DeVoe - Guitarmaker Box AA, San Jose, CA 95151\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "JUAN MARTINEZ: EL ARTE FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_11",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 19,
    "word_count": 1000,
    "article_char_count_full": 6043,
    "article_char_count_review": 3335,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_11::A12",
    "article_text_for_review": "EMOTION IN MOTION -- FOLKS FLOCK TO LEARN FLAMENCO (from: $ \\underline{\\text{The Bellingham Herald}} $, Aug. 27, 1982) by Dale Folkerts Some will say flamenco is dying. But in Bellingham for the past two weeks, the Spanish folk dance, described by its teacher as emotion in motion, has found life in the soles and souls of 34 dance students drawn here from around the world. It is a dance of life and of death; of the good times and the bad. And it is Teo Morca who is teaching how to blend emotion into the sweep of a hand and the click of a heel that is flamenco dance. It is this interpretation that makes Morca's class different, his students say. Absent from Morca's sessions is the rote learning of individual dance routines. Instead, he cements the technical blocks of style into minds that can use his teachings as the foundation for individual expression. The method is special enough to have attracted dancers from Chicago, Atlanta, Washington, Los Angeles, Seattle and New Zealand and Canada. Beverly Christie of Seattle said she has tucked away a hope to perform flamenco. But the last night spot in that city hiring flamenco dancers stopped doing so more than a year and a half ago. \"I think maybe public interest has gone way down, but dancers' interest is going 'way up,\" Christie said. She is studying flamenco in Seattle, but came to the Bellingham seminar for something more. \"I'm learning style -- he's got a lot of character, a lot of unspoken personality,\" she said. \"Teo can make you cry or make you laugh just by the way he dances.\" Eva Encinias teaches flamenco; she had to skip her first week of classes at the University of New Mexico to learn from Morca. \"What draws me to him is his total body usage,\" Encinias said. \"So often when you work with people they use isolations of arms, legs or hands. It's not that there's a shortage of teachers, there is a shortage of teachers who have the real in-depth technical skills that he has.\" Although flamenco may not be packing in bar crowds, Encinias said it is popular among dance students in New Mexico. Several of Morca's students were not dancers -- in the professional sense. They're just professionals -- engineers, nurses and real estate agents -- who like to dance. Morca offered two-week beginning and advanced classes. Both were packed. Sean Gallagher, in the beginning class, teaches karate in Sellingham. \"It's very similar to karate in the internal aspects of it, the way the hands move,\" said Gallagher, who started in jazz dance classes taught by Morca and his wife Isabel at the Morca Academy. \"Now I trade him dance lessons for karate lessons.\" Morca said many of the people in his classes have no illusions of becoming professional dancers. \"Most are in it for the love of flamenco. But why not just do something if you love it?\" * * * SABICAS GIVES GUITAR FESTIVAL ROUSING START by Tim Page New York's first international Guitar Festival got off to a rousing start Friday evening when the man known only as Sabicas, an acknowledged master of flamenco guitar for more than 40 years, performed for an overflow crowd at Cami Hall. Sabicas, who was born in Pamplona, Spain, in 1917, has played throughout the world and has made more than 50 recordings. The audience greeted him with a rapturous ovation and punctuated his performance with spontaneous outbursts of enthusiasm. Sabicas confined himself entirely to his own compositions. These were amiable display pieces -- improvisatory studies calculated to exploit the artist's formidable technique. Although they might have sounded amorphous to some, they admirably fulfilled their virtuosic intent. In any case, Mr. Sabicas has the gifts to transcend his material, and his meticulous attention to contrapuntal line would serve him well in any music he chose to play. SABICAS PLAYS DE VOE GUITAR We congratulate Lester DeVoe, guitar marker and frequent columnist for Jaleo, on the feature article about him in Frets magazine (October 1982). Artists such as Mariano Cordoba, Ricardo Peti, and Sabicas play his instruments and we wish him continued success. George Ryss of New York sends us the program of Sabicas' October 8th concert in New York. The guitar listed as the one used in the performance was made by Lester DeVoe of San Jose, CA. We felt that Lester wouldn't mind if we printed a portion of a letter he sent last January telling about how Sabicas came to play his guitars. He wrote: \"Briefly, I shipped a couple of demonstration guitars to Antonio David at \"The American Institute of the Guitar\" in New York City and asked him to show them to guitarists passing through his store. Sabicas played them one day and then came back another day and played one for a full three hours. Last fall, my wife and I toured the USA for two months in our VW camper and met Sabicas at the Institute. He is a charming and gentle man. He played my guitar for us and spoke highly of it. Later, Antonio David shipped the guitars back to me. But when Sabicas let me know he was coming to San Francisco in November [1981], I knew he was interested in my guitar. When he arrived, we had him over for dinner on a Saturday, were guests at his concert the next day, and, on Monday, he agreed to use my guitar as his concert and recording instrument.\" Once more: Congratulations and continued success, Lester. Rubina Carmona Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059 Los Angeles, Ca. FLAMENCO GUITAR INSTRUCTION NEW CLASSES NOW FORMING —BEGINNING (CHILDREN) —BEGINNING (ADULT) — INTERMEDIATE —ADVANCED TECHNIQUE —ACCOMPANIMENT OF SONG & DANCE (INTERMEDIATE LEVEL)",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_11",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 968,
    "article_char_count_full": 5623,
    "article_char_count_review": 5623,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
