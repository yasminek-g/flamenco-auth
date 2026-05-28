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
    "article_id": "1987-07-23-left-ablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nIV CONCURSO DE CANTE JONDO «CUITAT DE L'HOSPITALET»\n\nOrganizado por la Peña Flamenca «Antonio Mairena» de Hospitalet (Barcelona), ha sido convocado el IV Concurso de Cante Jondo, en el que podrán participar todos los cantaores de ambos sexos, aficionados y profesionales no consagrados.\n\nPara esta edición se han establecido dos grupos de cantes y los participantes estarán obligados a ejecutar dos cantes de cada grupo.\n\nEste concurso estará dotado con siete premios, siendo el primero de 125.000 pesetas.\n\nEl concurso se celebrará durante la primera quincena del mes de noviembre de 1987.\n\nPara más información los interesados pueden dirigirse a la Peña Flamenca Antonio Mairena, calle Mina, 16, Hospitalet (Barcelona).\n\nI PREMIO DE INVESTIGACION DE LA FUNDACION ANDALUZA DE FLAMENCO\n\nBASES\n\n1.º.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\na ejecutar dos cantes de cada grupo. Este concurso estará dotado con siete premios, siendo el primero de 125.000 pesetas. El concurso se celebrará durante la primera quincena del mes de noviembre de 1987. Para más información los interesados pueden dirigirse a la Peña Flamenca Antonio Mairena, calle Mina, 16, Hospitalet (Barcelona). I PREMIO DE INVESTIGACION DE LA FUNDACION ANDALUZA DE FLAMENCO BASES 1.º. Objeto: El premio se concederá al mejor estudio monográfico o trabajo de investigación no publicado sobre el arte flamenco que se presente. 2.º. Concursante: Podrán con- currir cuantas personas lo deseen. 3.º. Presentación: Los originales por triplicado ejemplar, sin límite de extensión, mecanografía do por una sola cara en lengua castellana. Deberán ser remitidos a la Fundación Andaluzas de Flamenco, calle Correderas, núm. 53. 11402 Jerez, antes del día 10 de diciembre de 1987. El trabajo se presentará bajo seudónimo o lema, sin figurar el nombre ni fir\n\n[ENDING CONTEXT]\n\nde la comprensible satisfacción que produce al Grupo Candil la indicada distinción, queremos significar la valía personal, el rigor profesional y el sentido cotidiano de lucha del escritor ecijano, en defensa siempre de las esencias flamencas. Es cierto que, en ocasiones, sus juicios no alcanzan la mesura de Solón, y que el fervor y furor de los profetas se contiene en aquéllos. Pero es que cuando se lidia contra la degradación, el nepotismo y, en general, los contenidos espúreos, es necesario ese arrojo, ese sentido crítico, aunque resulte hiriente.\n\nEnhorabuena, MANUEL, y adelante!.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1987-07",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 1454,
    "article_char_count_full": 9135,
    "article_char_count_review": 2588,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  },
  {
    "article_id": "1987-07-24-left-flamenca-placas",
    "article_text_for_review": "Por: Manuel Yerga\n\nNOTA: En nuestro número anterior y en la sección Discografia Flamenca (Placas), por un error de imprenta aparece guitarrista R. Nogal, cuando en realidad debería haber dicho R. Nogales. Pedimos disculpas a nuestro buen amigo Yerga y a los lectores de CANDIL.",
    "title": "Discografía flamenca (Placas)",
    "periodical": "candil",
    "issue_id": "1987-07",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 45,
    "article_char_count_full": 277,
    "article_char_count_review": 277,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-09-3-right-editorial",
    "article_text_for_review": "Editorial\n\nLOS SECUNDONES\n\nE n ocasiones, los clamores del éxito o el reconocimiento expreso de iniciados o no iniciados, se corresponde con la realidad. El más puntual ejemplo suele referirse, por sus recientes biógrafos, a la vida de don Antonio Chacón. Maestro incuestionable, engrandecedor de estilos, tutela de los mejores ecos, gozó de la admiración de sus contemporáneos, y desde la Corona hasta el último campesino, degustador del cante, rindió pleitesía al maestro de Jerez.\n\nEntre otros ejemplos, más cercanos, el del malogrado Antonio Mairena, está en la mente de quienes, personalmente, nos beneficiamos con su magisterio y asistimos, con orgullo, al público reconocimiento que las más altas instituciones del Estado hicieron de su arte y talento inconmensurables. Pero estas dos dignísimas citas son sólo excepciones. Han existido otros vates del jondismo, depositarios de entrañables esencias, que no han merecido ni una sencilla recensión por parte de la historiografía flamenca. No es éste el momento de analizar las claves del éxito en este arte, las cuales estimamos no difieren sustancialmente de las de cualquier otra manifestación artística. Factores sociológicos, culturales y hasta políticos intervienen en el grado de recepción del cante, en un momento dado.\n\nNuestra reflexión viene al hilo de la constatación de un fenómeno frente al que, tal vez, quepa alguna suerte de reacción. Un gran número de artistas, viejos y jóvenes, con una trayectoria, plena de fidelidades al jondismo, se extinguen, se mustian, en el mejor de los casos, frente al olvido inmerecido, frente a la indiferencia, incluso, de quienes se supone son garantes de la verdad flamenca. A esta injusta ceremonia de la confusión, contribuyen el prurito de instituciones por erigir en mitos vivos a profesionales que, siendo dignos de nuestro respeto y admiración, no pueden ni deben reputarse guías espirituales del flamenco. Por eso, las distinciones de ayuntamientos, diputaciones, Junta de Andalucía, etc., deben administrarse con enorme cautela y prudencia, de manera que no se penalice el quehacer auténtico de otros sin acceso a la amistad de líderes políticos del momento ni posibilidad de ilustrar, en reuniones acotadas, a señorías, excelencias o conspicuos sanedrines.\n\nEs obligación de todos, cualquiera que sea el foro del que se disponga, reivindicar el derecho de esos, peyorativamente, llamados secundones a un juicio crítico honesto, aunque, a veces, deba emitirse contra corriente, incluso subrayando el mérito o desmérito de los denominados presuntos maestros. Acaso no consigamos para aquéllos honores oficiales y distinciones, pero al menos quedará constancia para las generaciones futuras, de que quienes tuvimos alguna suerte de participación en el flamenco, lo intentamos y de que nuestra torpeza, en la apreciación de lo jondo, no llegó, al menos, a las elevadas cotas de nuestros gobernantes.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 446,
    "article_char_count_full": 2910,
    "article_char_count_review": 2910,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-09-4-right-el-romancero-tradicional-gitano-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPONENCIA:\n\nXV CONGRESO NACIONAL\n\nBENALMÁDENA, 1987\n\nManuel Martín Martín\n\nocasiones en que el Romancero Tradicional Gitano ha merecido la atención de este Congreso. Por el contrario, la mayoría de las veces insistimos abrumadoramente en el estudio y debate de proposiciones, contradictorias y benevolentes, que poco o nada aportan a la génesis y evolución de este arte, argumentos que, entre otros básicos, debieran sustanciar estas asambleas anuales.\n\nEl objeto, por tanto, de esta pónencia no es más que presentar una incursión por el Romancero Tradicional Gitano reflejado en la obra del maestro Antonio Mairena. En este punto, permítaseme recordar una pregunta que Manuel Yerga formulaba en tono algo pesimista (1): «¿Cuánto tiempo estará en nuestra memoria el gran don Antonio Mairena?».\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\násicos, debieran sustanciar estas asambleas anuales. El objeto, por tanto, de esta pónencia no es más que presentar una incursión por el Romancero Tradicional Gitano reflejado en la obra del maestro Antonio Mairena. En este punto, permítaseme recordar una pregunta que Manuel Yerga formulaba en tono algo pesimista (1): «¿Cuánto tiempo estará en nuestra memoria el gran don Antonio Mairena?». Pienso, sinceramente, que mientras haya en la tierra un hombre que cante siempre tendrá por guía y norte a Antonio Mairena. ¿Por qué?, se preguntarán algunos. Primero porque es el ejemplo a seguir, y en segundo lugar porque, como dijo Félix Grande, «En la Universidad que se llama don Antonio Maire- na habita todo el cante. Cuanto no recuerde Mairena no lo recordare- mos nunca». Es por ello que en los preliminares de esta ponencia la insobornable lealtad al amigo y consejero me obliga a eternizar en la agenda de nuestra historia el día 27 de junio de 1983, fecha trascendental para el Arte Flamenco en la que su Majestad el Rey Juan Carlos I le imponía la Medalla de Oro de las Bellas Artes\n\n[ENDING CONTEXT]\n\nespañola, y don Antonio Mairena es uno de los pocos exponentes del romancero en este campo del flamenco, tan falseado a veces por unas concepciones del pseudo-andalucismo».\n\nEste romance fue cantado por Antonio Mairena en el Palacio de Quintanar de Segovia, el día 15 de julio de 1980, y con la guitarra de Juan Antonio Muñoz Pacheco. En esa fecha, en un acto presidido por don Diego Catalán, Antonio Mairena recibía el primer Diploma «La Nave de Arnaldos», concedido por la Cátedra Menéndez Pidal de la Universidad Complutense y dentro del Curso de Música Tradicional y Romancística Española.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 4251,
    "article_char_count_full": 25475,
    "article_char_count_review": 2704,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombre"
      }
    ]
  },
  {
    "article_id": "1987-09-8-left-duende",
    "article_text_for_review": "Duende\n\nReunidos entre cuatro paredes\n\ncabalgamos potros de vino.\n\nLa noche persigue aullidos de sangre\n\ny atenaza quejidos de muerte.\n\nEnigmático visitante\n\ntantas veces buscado\n\ntantas otras esquivo.\n\nYo te invoco con la agridulce\n\nllama sonora del cante!\n\nTe rodeo en mi pecho,\n\nte agarro, te exprimo\n\ny entonces... ;Oh, cometa\n\nde cola caprichosa!\n\nNos abrasamos contigo\n\nen tus mares de lava.\n\nMordemos mariposas de sangre\n\npor cada segundo de tu eléctrica presencia.\n\nSe rompen las copas,\n\nse abren los pechos y tu semilla se abonda\n\nen el surco abierto por tu rayo.",
    "title": "Poema: Duende",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 93,
    "article_char_count_full": 572,
    "article_char_count_review": 572,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
