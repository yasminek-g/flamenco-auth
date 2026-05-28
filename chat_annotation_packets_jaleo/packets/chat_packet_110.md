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
    "article_id": "JALEO_1981_07::A2",
    "article_text_for_review": "(Jaleo thanks George Ryss for his perseverance in soliciting the material for this and the previous article by Señor Barón.) edited by Paco Sevilla Quotes from unidentified newspaper articles: --Another newcomer is Carol de los Reyes, a real gypsy who dances with the fury and fierceness that make flamenco art exciting. --Carol de los Reyes...combined a stunning face and ample figure with a deceptive grace and speed of performance. A really dazzling entertainer. --Both were splendid, technically, but actually failed to stir the audience as much as Carol de los Reyes, a voluptuous, sexy creature who danced with both Greco and Vargas. She seemed to have been poured into her costume and if her back had been arched any further in relation to her perfectly straight, symmetrical body, she would have been doing an adagio. --Of the covey of gypsy women, the most extraordinary is Carol de los Reyes. I use the word extraordinary advisedly. Picture, if you can, a Modigliani woman burning with a hard, gem-like flame, and you have her facial idiom. From her neck down she is Renoir, poured into a skin tight sheath, calf-deep in ruffles. De los Reyes does not do much. But when she raises an arm or crooks a finger, it is a movement of magnificance, and when she kicks her yards of ruffles behind her, it is an earth-shattering event. A cold Northern audience may be inclined to giggle at her--but not as long as she has those burning eyes and glistening teeth aimed in its direction. Her's is an utter victory of style and strength over a foreign lack of understanding. HE CAME, HE SAW...SHE CONQUERED (from: $ \\underline{\\text{San Diego Union}} $, Sunday, March 10, 1968) by Syd Love The couple stepped from a taxi in a very old part of Madrid, a sector called Arches of Knife-Makers. A destitute neighborhood, and a little sinister, it was not a place the two would usually spend an evening. To reach the darkened stairs that led to the rathskeller took them only a few strides. And then they were inside the cafe, and it was dark and smokey. And it was small and quite crowded. They found a table. The man, tall for a Latin, helped his dark, attractive young woman companion to a chair. They sat there and each sipped a creme de menthe, and another, as they watched a boy in a dance routine. \"In two weeks she had signed a contract, although she wrote her own terms. She was concerned about transportation, back to Madrid from the U.S., in case she was not a success. I had no doubt of her success. But we fixed the contract the way she wanted.\" Greco's young woman companion the night he found the 21-year-old Miss de los Reyes was Nana Lorca, 24, a native of Murcia, Spain. Miss Lorca is Greco's chief dancing partner in his new ballet. \"Miss Lorca and I had been to dinner to discuss contract terms,\" Greco said. \"Each fall I look for new talent in the cafes, the countryside, the native festivals. Everywhere. WITH GRECO (GUITARISTS TONY BRAND & M. BARON) One day she packed all her belongings, paid for her trip by dancing for the ship's passengers, and arrived in the land of flamenco to conquer the country. And indeed she has. At present she is living in Palma. But her home is now in the heart of Andalucía, where she lives in a gigantic, rambling house just outside Seville. From her home on a hill overlooking the Guadalquivir she can see the Giralda, the Torre del Oro, the Cathedral, and the Puente de Triana. From her first moment of arriving in Spain, she felt completely at home. Everyone encouraged her to become a professional gypsy dancer, and she continued the studies she had begun in New York. \"But I learned the real essence of flamenco by dancing at gypsy 'juergas,' where everything is improvised, free and spontaneous. There is nothing like it anywhere in the world! To understand it, one has actually to live it entirely.\" For a moment Carolina was lost in thought. PHOTOS FROM CAROLINA'S SCRAP BOOK \"All right,\" she smiled. In a moment she, Manolo and Bernardo were on their feet, beginning the rhythmical clapping which is the prelude and accompaniment to flamenco. I watched them as they slowly became part of the rhythm and their bodies moved with it. Suddenly Bernardo broke into a cante jondo, and in a few seconds Carolina, in tune with his singing, moved with it. Every part of her body was called into action, and her hands especially, in an intricate, sensual, Oriental manner, revealed her great command of gypsy dancing. At the same time, her lithe body, bronzed by the sun, responded subtly and magically to the music. Manolo's guitar interpretations were very sensitive and profound. His tremolo has been compared with Andrés Segovia's. -- What is the secret, Carolina? Her eyes flashed, and her gypsy earrings swayed with the movements of her body. \"You see, it is really a dance of the soul. It is something you feel intensely. Of course, it is enhanced by the deep chords of the guitar and the profound melancholy of the cante jondo. After this kind of expression, which is so full of passion and vitality, one feels that something is missing in other kinds of dancing.\" -- You mentioned that you would spend six months in the Caribbean, and that you had been touring very often. When are you ever home? \"Whenever tours permit...I'd like you to see the house for yourself one day. It's called the Chalet Grande, which is the name given it by the Countess who use to own it. And it's in Cerro Alegre, just a mile from Seville. \"It means a big house in a happy mountain and often we think of what the Countess would feel if she could see all the gypsy dancing there when we are home!\"",
    "title": "CAROLINA DE LOS REYES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "poem",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 997,
    "article_char_count_full": 5632,
    "article_char_count_review": 5632,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A3",
    "article_text_for_review": "Over the years it had become a traditional event which aficionados of the guitar eagerly looked forward to attending. Occasionally the playing of this guitarist or that one, the decision of the judges in one year or another would cause a lively spark of controversy. Unfortunately just such a spark -- this time one that ignited into a royal donnyebrook -- has put the future of this grand competition into doubt. It happened this way: It was a sunny, quiet Sunday afternoon in September 1979 as lovers of the guitar began filing into a pavilion in Jerez's fairgrounds for the VIII Certamen de Guitarra Flamenco, sponsored by Gonzalez Byass Sherry and organized by the Peña Los Cernícalos. The stillness was broken as friends greeted friends, sherry bottles began making the rounds, and conversations sprang up about events at the competition during years past. Everyone had a story about being present the year that so-and-so played so beautifully that no one took a breath for the entire set or when such-and-such jerezano so magnificently captured the essence of his land that, some swore, sherry evaporated from corked bottles. sounds of the flamenco guitar filled the canopied room and spilled out into the street. Ah, such music! It was grand, everyone agreed. The guitarists themselves were of all ages, from early teens to middle aged, and represented both sexes. A general description would be that they were clean-cut, nicely dressed, with neat haircuts, all scrubbed and shiny. Their efforts with their guitars were uniformly rewarded by generous applause. As the time passed and the competition was nearing completion, several people whispered to their neighbors about the fellow from the north. What had happened to him? Perhaps, some suggested, he had been scared off by what he had heard so far today. Parilla Chico, one said, would obviously take home the prize.",
    "title": "COMPETION IN JEREZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 309,
    "article_char_count_full": 1878,
    "article_char_count_review": 1878,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A4",
    "article_text_for_review": "- SOAP, CHALK AND PEGS More and more flamenco guitars are being fitted with tuning machines. It does not surprise me, because most pegged guitars are not properly maintained and as a result become difficult or impossible to tune. You probably have experienced a guitar with pegs that was hard to tune and may have a negative attitude toward them. I much prefer pegs; if maintained properly, tuning and string changing can be very easy and the guitar maintains its traditional balance and flavor. A peg should twist easily and grab as you push in, just short of emitting a creaking sound. To get your pegs to work smoothly, try this simple procedure: Take off the string and remove the peg. Clean the peg shaft and the tapered hole in the guitar head. Use fine steel wool or scrape gently with a knife or razor blade, being careful not to remove any wood. Two items you need are dry bar soap and chalk (blackboard). The soap bars that crack if not used regularly work best; these have no cream in them. Try twisting the peg in its hole. If it grabs and creaks, strike on a very small quantity of soap (one or two strokes). Try it again and if it still creaks, stroke on a little more soap. If it won't grab with reasonable pressure, then stroke on a little chalk. Apply chalk for more friction or soap for easier turning. Tie on the string and try tuning.",
    "title": "LESTER DE VOE ON GUITAR",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "article",
    "pages": "14",
    "page_number": 14,
    "word_count": 249,
    "article_char_count_full": 1354,
    "article_char_count_review": 1354,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A5",
    "article_text_for_review": "by Ron Bray Some people believe that to play flamenco guitar, you must be Spanish. While not wishing to get involved in that argument, many such people who have been fortunate to hear Peter Holloway perform, have wasted no time in admitting how very wrong they were. Born in Bristol, England, Peter was first introduced to flamenco, at about nine years of age when his parents took him to see Antonio and Rosario in London. According to his mother, young Peter was absolutely transfixed\" and stood up in his chair through the entire show, staring at the dancers in absolute wonder. The Holloway family moved to Preston in the northwest of England and, although it wasn't until Peter reached the age of fifteen that he started to learn the guitar, the seeds of interest in flamenco had already been sown. Peter's passion for flamenco was further kindled when he bought a record by Pepe Martínez who, coincidentally, at that time was making one of his visits to England and gave a recital in Preston. Peter went back-stage and met Pepe and, for the next five years, was his pupil, having guitar lessons both at home in Preston and at Pepe's flat in Seville. About five or six years ago, Peter started to become very frustrated with the progress he was making with his playing. He had reached the stage where, as a soloist, he could play technically well, but he felt there was still something missing. Peter decided to give up his job as a guitar teacher and go to Spain, originally intending to stay for only about a year or so and then return to England. He left for Spain and, eventually, after a brief period of working as a teacher of English at a language school in Bilbao, arrived in Sevilla. Peter rented a little flat on Archeros, one of the typical narrow streets in the ancient quarter of the Barrio Santa Cruz. He got a job teaching a few English classes and started playing for dancers in the flamenco dance schools of Carmen Albéniz and Manolo Marín. After a couple of months of doing this two or three times a week, his playing began to change. His compás grew much stronger and, with more opportunity to accompany singers and dancers, Peter's playing began to develop more \"aire\" -- that special magic quality which gives flamenco guitar playing its essential meaning. Shortly after Peter arrived in Seville, he met and started working with the young Sevillano guitar virtuoso, Rafael Riqueni. (Rafael won the International Guitar competition in Córdoba at the age of fifteen!) \"Fali\" has had quite a profound influence on Peter and has encouraged him to stop playing other people's falsetas and to start creating and improvising original material, with the result that today Peter has developed his own individual and unique style. Peter is currently working at Los Gallos, one of Sevilla's best known tablaos, playing guitar accompaniment for the dancers. Carmen Albéniz, Juan Manuel, singers La Ruina, Manolo Sevilla, Jesus Heredia and Paco Gil, together with the guitarists José Cruz and Manolo Rodríguez. He has taken part in the flamenco festivals, \"El Potaje de Utrera\" and \"El Gazpacho de Morón,\" appearing with the gypsy family of Curro Fernández and Juana Amaya. Peter writes regularly for the English magazine, $ \\underline{\\text{Guitar}} $, describing the flamenco scene in and around Sevilla. Peter has appeared on both British and Spanish television and, in flamenco circles in Sevilla, he is known simply as \"EL INGLES.\" PETER HOLLOWAY (LEFT TO RIGHT): CURRO FERNANDEZ, RAMON AMAYA, JUANA A PAGE ESPERANZA FERNANDEZ, DAUGHTER OF CURRO.",
    "title": "\"EL INGLES\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "15-17",
    "page_number": 15,
    "word_count": 603,
    "article_char_count_full": 3564,
    "article_char_count_review": 3564,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_07::A6",
    "article_text_for_review": "(translated by Brad Blanchard) La Puebla de Cazalla is a humble, secluded place crouched in a corner of its immense expanses of pastures and single-crop fields. Raised above the ancient body of a Roman settlement, the present village must have emerged no later than the eighteenth century. The personality of Puebla de Cazalla is linked very directly to the deep-rooted outlines of the troublesome agrarian situation of Andalucía. Its residents -- who are called \"moriscos,\" a name which would demand a detailed commentary -- have emigrated on a large scale. It is normal in this land of poor tenant farmers and seasonal workers. The majority have gone to Germany, and lately, to Ibiza as impromptu construction workers. Francisco Moreno Galván -- who is, in our judgement -- the person who at present has lived with most human penetration the historic truth of the cante -- served us as an exceptional guide through the social reality and the flamenco atmosphere of his pueblo and later, of Morón. La Puebla de Cazalla has been the birth-place of some good, although anonymous, cantaores. A few years ago Alvaro Triguero died here, having been a great perpetuator of some of the most interesting mixtures of flamenco and liturgic music of Byzantine origin. We refer to the \"pregones sagrados,\" a variation of the primitive tonás which were sung since ancient times -- although not now -- in the parochial church of Puebla in the early morning of Holy Thursday. The \"pregón\" -- we prefer to call it \"toná litúrgica\" -- is a cante of very special expressive force, solemn and sober like very few, and considerably difficult to execute. After Alvaro Triguero, no one dared to sing them in public. The tradition is being lost. We were particularly interested in finding someone who would at least try to reproduce the noble grandeur of these dazzling \"pregones.\" After many comings and goings, we found a worker named Montesino who belongs to a family of good flamenco aficionados known by the nickname, \"Lobos.\" Montesino has only sung a few times in private get-togethers. He was resisting our request while we were drinking the evening \"slow glass\" in a tavern. The \"pregón,\" like the saeta of Puebla (another derivation of the ancient tonás) demands faculties and a tonal power of exceptional quality. Montesino didn't think he had those qualities and anyway, he hadn't sung for a long time. Only his good wishes moved him to participate in the \"Archive,\" after our friendly, but stubborn explanations.",
    "title": "ARCHIVO: PART VII",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_07",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 417,
    "article_char_count_full": 2503,
    "article_char_count_review": 2503,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
