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
    "article_id": "JALEO_1980_10::A11",
    "article_text_for_review": "Donn and Luisa Pohren have made a number of tours of the United States in recent years, including several visits to San Diego. For those readers who are not familiar with the contributions they have made to flamenco, we present the following brief summary. Donn Pohren was born in Minneapolis and attended Westminster College and the University of Minnesota. After a stint in the U.S Army, he used the G.I.Bill to study in Mexico and Madrid. In 1954 he was enjoying his first flamenco experiences in Spain, and by 1956 he had married Madrid born dancer, Blanca Luisa Bergasse, and they were living in Sevilla, in the home of the Pavón family. Donn studied guitar, while Luisa worked on her dancing with Carmen and Eloisa Albéniz. In the late 1950's, the Pohrens performed professionally using the artistic names, Daniel and Luisa Maravilla. In 1958, they opened a flamenco café in San Francisco. By 1962, the Pohrens had experienced a great deal of flamenco in the Sevilla area and Donn published his first book, The Art of Flamenco, which was eventually printed in a number of languages and became the important source of flamenco information for non-Spanish aficionados.",
    "title": "DONN POHREN & LUISA MARAVILLA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "21",
    "page_number": 21,
    "word_count": 197,
    "article_char_count_full": 1172,
    "article_char_count_review": 1172,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A12",
    "article_text_for_review": "(from: ABC, Aug. 16, 1980; sent by Brook Zern; translated by Paco Sevilla) by El Conde de Montarco An irreplaceable interpreter of cante flamenco has left us; in Pepe\"el de la Matrona\" was combined interpretive art, exhaustive knowledge of the cante, and personal human quality. When UNESCO sponsored the creation of the Centro de Estudios de Musica Andaluzay de Flamenco, with goals of investigation and conservation of purity in these arts, some fifteen years ago, I was appointed director -- more for having promoted the idea in Paris than for a profound knowledge of this art whose birth and interpretation have been the sources of much controversy. The first person I asked to be part of the advisory board of that center was Pepe Matrona -- as he is called by everybody -- and with him included other personalities who would be valuable in the study of flamenco, such as Caballero Bonald, García Matos, Fernando Quinones, Manuel Rios, Arcadio Larréa, Manuel Gutiérrez, Elias Terés, Ricardo Pachón, Manuel Cano, José Blas Vega, Luque Navajas, and Juan de la Plata But when Pepe Matrona -- who didn't use to talk much in our gatherings -- spoke, he was listened to with great respect; he always had something important to say and he would say it in a manner that was simple, graphic, and amusing. However, it was in the Figon de Santiago, at a table with los cabales (knowledgeable aficionados), animated by Vitoriya, that he best exercised his mastery. Here, throughout the night and dawn, we used to hear his opinions and judgements, supported by his interpretation of the cante under discussion. At that time he had passed seventy years of age, but the vigor of his voice and his vitality gave to his cantes an unequaled artistic value.",
    "title": "PEPE \"EL DE LA MATRONA\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "22",
    "page_number": 22,
    "word_count": 297,
    "article_char_count_full": 1743,
    "article_char_count_review": 1743,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A13",
    "article_text_for_review": "AUGUST JUERGA The juerga at Diego and Chuck's was a lot of fun with plenty of room to spread out. There was no written juerga report; the pictures are worth a thousand words, as they say, so thanks to Gene for the great photos. We will include here the background on our hosts that was omitted previously. Diego Robles lived in Spain and studied Spanish and flamenco dance as a child. In the States he branched out to ballet and jazz and performed with a variety of professional companies including the American Jazz Dance Company, Music and Dances of Spain (both of which toured Mexico), The California Ballet Company and Fantasia Española. Charles 'Chuck' Thompson, although not involved in flamenco, has been an active behind-the-scenes JALEISTA, setting up for juergas and collating the JALEO magazine along with Diego. MICHEL & PACA - SEVILLANAS VICKI & RAFAEL - SEVILLANAS PILAR SINGS TO GUITARS OF HERB, JOSELITO & YURIS YURIS, JOSELITO, MARIA JOSE & HERB MARIA JOSE & SANDRA AGUAYO LOS CHAMURRO",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "22-24",
    "page_number": 22,
    "word_count": 170,
    "article_char_count_full": 1002,
    "article_char_count_review": 1002,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_10::A15",
    "article_text_for_review": "BEMÓL -- flat, as in B flat or Bb. BÉTICA -- an old name for part of Andalucía; sometimes used to refer to someone or something from the Sevilla area. BRACEO (el) -- the movement of the arms in flamenco dancing; often used to refer to parts of the dance where the arms are the focus of attention. CABAL (el) -- the aficionado who has a deep knowledge of the cante. CABEZA (la) -- the head; the head of the guitar; sometimes called \"EL CLAVIJERO\" because it holds the \"CLAVIJAS\" (pegs). CÁDIZ -- the oldest city in Europe, located on a peninsula on the southern Atlantic coast of Spain. The name comes from the word \"Gades\", the Roman name for the city, and the inhabitants are still called GADITANOS. Cádiz and nearby towns were the site of development of the alegrías, cantiñas, mirabrás, romeras, tientos, tangos, tanguillos, and jaleos, plus distinct styles of bulerías, soleares, siguiriyas, and malagueñas. CAFÉ CANTANTE (el) -- cafés where cante flamenco was presented in the last half of the 1800's and the early 1900's. CAJA (la) -- box; the body of the guitar. CALANES (el) -- \"el sombrero calanés\"; a circular, brimless hat that has small balls on the top, usually worn over a scarf; worn by horsemen on festive occasions, and sometimes with the traje corto or campero when dancing zapateado; not to be confused with the beret or \"boina\". CAMISA RIZADA (la) -- ruffled shirt. CANASTERO (a) -- Spanish gypsy; in the strict-est sense, this name refers to gypsies who lead a wandering existence, but it is commonly applied to any gitano. Box 4706 San Diego, CA 92104 CANCIÓN(la) - song; a popular or composed song with fixed verses, beginning and ending; not usually used to refer to the cante. CANTAOR(a) - flamenco singer, the title implies the ability to sing \"cante jondo\" (a non-flamenco singer is a \"cantante\"). CANTE(el) - The song; specifically, flamenco song, as distinguished from \"canciones.\" CANTE CHICO - light festive cante, as for example, alegrias, fandangos de Huelva and verdiales; many of these songs, especially bulerías and tangos, are often called cantes \"por fiesta.\" CANTES DE IDA Y VUELTA - cante that has gone and returned, or \"made the round trip;\" refers to songs that were taken to Latin America by early Spaniards, underwent changes and then were brought back to Andalucía by later Spaniards (especially gypsies like Carmen Amaya) where they were further changed and incorporated into flamenco. The most popular of these are rumba and guajiras (from Cuba) columbianas (Columbia) and milonga (Argentina). CANTE JONDO - deep song; usually used to refer to serious gypsy cante such as si-guiriya, soleares, toná and martinetes. There are those who disagree with this classification and feel that almost any cante can be \"jondo\" if the singer feels it that way; This is especially true of such potentially jonde cantes as mala-gueñas, tarantos, tientos and fandangos grandes. BULK RATE U.S. POSTAGE PAID La Mesa California Permit 368 RETURN POSTAGE",
    "title": "FLAMENCO DICTIONARY",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_10",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "28",
    "page_number": 28,
    "word_count": 499,
    "article_char_count_full": 2981,
    "article_char_count_review": 2981,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_11::A1",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nThe following is an account of Carmen Amaya's life, compiled from many scattered sources and bits of information. There is no way to be certain of the accuracy of this material, and there is a great deal of conflicting information, especially where dates are involved. For example, different accounts have her marrying in 1947, 1951, and 1952, and some articles give detailed accounts of Carmen growing up in the caves of Granada, which is completely false. In other instances facts are undoubtedly exaggerated and blown out of proportion. However, Carmen Amaya was practically a mythical figure, so great was her fame and her effect on the flamenco world, and it is not entirely out of place to present a biography that may, in some details, be somewhat mythical in nature. We would like to thank\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Marilyn\"]\n\na, which is completely false. In other instances facts are undoubtedly exaggerated and blown out of proportion. However, Carmen Amaya was practically a mythical figure, so great was her fame and her effect on the flamenco world, and it is not entirely out of place to present a biography that may, in some details, be somewhat mythical in nature. We would like to thank La Vikinga of New York for her research that added so much to this article, and Marilyn and John Bishop for the material they contributed. Carmen Amaya y Amaya was born on November 2, 1913 in the poverty stricken gypsy barrio of Somorrostro on the outskirts of Barcelona. Her home was a one-room hut that opened onto the beach and the sea. Both sides of the family were Amaya who had come from the caves of the Sacromonte in Granada. Her father, José Amaya \"El Chino\", was a guitarist and a dealer in old clothes; her mother, a dancer and singer, had married at fourteen and had ten children, six of whom lived to adulthood and became flamenco artists. \"One day a fountain was inaugurated. 'A small fountain on a column of bricks supporting a lead spout. There were festivities and José joined in with his guitar and his daughter; the father, with his foot resting on the bowl of the fountain, played, Carmen danced, and the people hurt their hands applauding. One of flamenco's brightest lights was born\" One story tells of Carmen's first public appearance as a dancer at age four. Her parents were performing in a Barcelona theater and had left little Carmen sleeping in the dressing room. She awakened, climbed out of her cot, wandered on to the stage and started to dance. The surprised audience broke into applause. $ ^{15} $ Another source says, \"It is not legend, but the absolute truth, that she was dancing in public at the age of four. She was completely (and always remained) untaught, and in those days in the wine taverns around the port of Barcelona, patrons were already enjoying her performance, not as a child prodigy, but as a bailaora in her own right.\" $ ^{4} $ \"Her father, a serious and authentic flamenco guitarist, amazed at the fiery temperment and magnificent abilities of the child, took her with pride around the coffee-houses and other places when the flamenco art was applauded and appreciated. First at 'El Chirinquito', then at 'El Cangrejo Flamenco' and 'Villa Rosa', the little gypsy started a revolution. Her fire, passion and charm, the perfection of her dance had never been seen before. The public went mad with enth\n\n[ENDING CONTEXT]\n\n$, \"Carmen Amaya\", Souvineer Booklet (15) Sugrue, Francis, \"The Greatest Gypsy Dancer\", $ \\underline{\\text{New York Herald Tribune}} $, Nov. 20, 1963 (16) Terry, Walter, \"Carmen Amaya Closes Season at Carnegie Hall\", $ \\underline{\\text{New York Herald}} $ $ \\underline{\\text{Tribune}} $, May 18, 1942. \"Dance World: Flamenco Stars\", $ \\underline{\\text{New York Herald}} $ Tribune, Oct. 9, 1955 (17) $ \\underline{Williams} $, Peter, \"The Art of Carmen Amaya\", $ \\underline{Dance and Dancers} $, June 1959 (18) Williams, Peter, \"La Capitana\", $ \\underline{\\text{Dance and Dancers}} $, Jan. 1964\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "CARMEN AMAYA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_11",
    "year": 1980,
    "language": "en",
    "article_type": "poem",
    "pages": "3-24",
    "page_number": 3,
    "word_count": 7615,
    "article_char_count_full": 44710,
    "article_char_count_review": 4145,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Marilyn"
      }
    ]
  }
]
```
