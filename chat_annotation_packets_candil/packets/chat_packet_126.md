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
    "article_id": "1986-01-23-left-discograf-a-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTITULO: «POR CANTIÑAS» CANTAN: Gabriel Moreno, Perla de Cádiz, Curro de Utrera, El Chaqueta, Pericón de Cádiz, El Flecha de Cádiz, Fernanda de Utrera, Ramón Medrano, Chocolate y Enrique Morente.\n\nTOCAN: Félix de Utrera, Manuel el Morao, Manuel Cano, Perico el del Lunar, Andrés Heredia, Juan Maya «Marote», Melchor de Marchena y Pepe Habichuela. REFERENCIA: HISPAVOX (50) 150112. Madrid, 1985.\n\nNos encontramos ante la aparición de un nuevo disco de la serie lanzada por la casa Hispavox —con mucho acierto—, que vienen a completar las dos magníficas antologías editadas por esta casa discográfica; la primera a mediados de los cincuenta y la segunda, en 1982.\n\nEste disco está dedicado exclusivamente al grupo de las cantiñas, con la excepción de las alegrías —por haber sido editado uno\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\na completar las dos magníficas antologías editadas por esta casa discográfica; la primera a mediados de los cincuenta y la segunda, en 1982. Este disco está dedicado exclusivamente al grupo de las cantiñas, con la excepción de las alegrías —por haber sido editado uno enteramente a este estilo— y en el mismo se recoge las diferentes formas de hacer los cantes de este grupo, con la personalidad que a los mismos le han dado las figuras de nuestro arte, así como la matización de diversas zonas cantaoras de nuestra Andalucía. Así, una vez más, Curro de Utrera, deja patente la matización o derivación que de las cantiñas gaditanas se hace en la provincia cordobesa. Fernanda de Utrera rememora a su abuelo «El Pinini», con los clásicos ecos de la zona cantaora de Utrera-Lebrija. Enrique Morente, recordando a Chacón, expone en esta grabación de los caracoles, la revalorización que del estilo hizo el cantaor jerezano. La maestría y la personalidad de los buenos cantaores como Pericón y El Flecha, deja patente una vez más los ecos gaditanos y la gracia de estos dos cantaores en Mirabrás y Cantiñas, respectivamente. Dos grabaciones «por Romeras», en dos voces bastantes diferentes, tanto en el compás y en el tono, como son las de Antonio «Chaqueta» y Antonio «El Chocolate», que vienen así a sumarse a la variedad que se quiere plasmar en el disco y que de hecho se consigue. El linarense Gabriel Moreno aporta su personalidad con Cantiña y Romera. Un aporte más a la profundidad y solera de nuestro arte es la grabación de Ramón Medrano con el cante de «Las Mirris». Sin embargo, escuchamos a la Perla de Cádiz en Cantiñas, quizás con falta de matización en el estilo y se observa a la vez, la reiteración de incluir a esta intérprete en esta serie de discos —dedicados a Cádiz— cuando pienso —sin menospreciar el arte de La Perla—que cantaores como Aurelio, Manolo Vargas, etc., van por este estilo con más profundidad. En cuanto a las guitarras, conocida es la valía de todos ellos, teniendo en cuenta la compenetración con los cantaores, por haber formado pareja durante muchos años. REFERENCIA: PASARELA. PRD - 140. Sevilla, 1985. TOCAN: Pedro Bacán, José Luis Postigo, Enrique de Melchor, Manolo Franco, Paco del Gastor y Rafael Riqueni. TITULO: «CANTES DE IDA Y VUELTA». CANTAN: Calixto Sánchez, José el de la Tomasa, José Meneses, Luis de Córdoba, Gabriel Moreno y Chano Lobato. La consejería de Cultura de la Junta de Andalucía, sumándose en un acto más a la conmemoración del V Centenario del Descubrimiento de América, acaba de editar el disco arriba referenciado, el cual viene así, a revalorizar —¿por qué no?— los cantes conocidos como de «ida y vuelta». Y escribo «revalorizar», porq\n\n[ENDING CONTEXT]\n\ncomo las que posee José. Otro tanto sucede con José Menese, pero quiero seguir reiterando, que el tratamiento y la interpretación de ambos me parecen buenas. Por otra parte, Gabriel Moreno también da un tratamiento adecuado a la milonga.\n\nEn cuanto a las guitarras, todos hacen bien su trabajo de acompañamiento, luciéndose, como es lógico, los que tocan las rumbas —Pedro Bacán y Manolo Franco— porque el estilo así lo propicia.\n\nEn definitiva, una grabación digna; con la que el flamenco quiere sumarse a los actos conmemorativos del V Centenario del Descubrimiento de América.\n\nRafael Valera\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1986-01",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 991,
    "article_char_count_full": 6010,
    "article_char_count_review": 4315,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  },
  {
    "article_id": "1986-01-23-right-buz-n-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSobre «Malagueñas» y malagueños\n\nbienda de que algunos de mis dichos carecían de certeza plena, la cual —pensaba—podía venir a ser desvelada por la sapiencia en estas lides del distinguido Yerga. Y en esto radicaba el «quid» de mi exposición; una especie de señuelo para que picase, espulgase y se viera el grano «libre de polvo y paja». Algo así como un intento de acicate, procurándole «material en bute», que lo enfrascase en abundantes párrafos esclarecedores por el estupendo arte que nos apasiona.\n\nAfectuosamente al amigo don Manuel Yerga Lancharro\n\nSé, amigo Yerga, que en esta parcela de títulos, rótulos, cantes, creadores o simples «decidores» en añejas placas, tú sabes más que Lepe, por escudriñador pacienzudo y tenaz en este terreno ingrato.\n\nTranquilo, sereno, concienzudo, sonriente\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\napasiona. Afectuosamente al amigo don Manuel Yerga Lancharro Sé, amigo Yerga, que en esta parcela de títulos, rótulos, cantes, creadores o simples «decidores» en añejas placas, tú sabes más que Lepe, por escudriñador pacienzudo y tenaz en este terreno ingrato. Tranquilo, sereno, concienzudo, sonriente y satisfecho, «he aguantado el rapapolvo» que, en las páginas 37 y más, de nuestra singular revista flamenca «CANDIL» número 42, «me zampa» el gran aficionado e investigador don M. Yerga Lancharro, a guisa de respuesta a mis anteriores puntualizaciones sobre «malagueñas» y malagueñeros. En efecto, lo esperaba a sa- Yo siempre he creído que las auténticas «malagueñas» chaconianas, no pasarían de ocho o nueve y que tú las tendrías hoy en tu poder (así lo reseñé en el penúltimo párrafo de mi escrito a ti en el número 40). De las otras, hasta quince, se me fue la mano adrede, expectativo, consciente de q\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\nombre determinado, pero considerando que lo mismo que don Manuel Yerga Lancharro entendió perfectamente a la persona que me refería, creo que en estos casos es más caballeresco no mencionar repetidas veces nombre de persona alguna, ya que lo mismo que él, todos los lectores de Revista CANDIL lo entenderían de igual forma. Referente a las biografías a las que me refería, quiero recordarle al señor Yerga Lancharro las dudas que él mismo tenía del lugar de nacimiento de don Antonio Chacón, ya que según él se convenció en Jerez de la Frontera sin gran certeza de su procedencia en otras cosas. Referente a lo que dice del padre del «Rojo el Alpargatero» y de un disco de los años 30 y que él ya interpretaba por esas mismas fechas estos cantes de Levante. Le diré que yo no pongo en duda de que el padre del «Rojo el Alpargatero» fuese cantar o tratante, lo que no quiere decir que el hijo pudiera ser una cosa u otra, pero lo que sí está claro es que\n\n[ENDING CONTEXT]\n\nque mi afición, ya que fui por mi cuenta y riesgo, me llevó a dicho congreso donde también pude observar en la ponencia que tanto se aplaudió de la señora Génesis García en la que omitía el nombre entre otros del Maestro Piñana cuando hablaba de los cantes de esta tierra, nombrando a otros señores que yo les preguntaría donde estuvieron durante 60 años que no se habló de dichos cantes, lo que justificaba la acogedora aceptación de la ponencia de la señora Génesis, ya que en todos sus términos le estaba adjudicando algo que no les pertenecía en toda su extensión.\n\nGabriel Rodríguez Villegas\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Buzón flamenco",
    "periodical": "candil",
    "issue_id": "1986-01",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 1064,
    "article_char_count_full": 6089,
    "article_char_count_review": 3554,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  },
  {
    "article_id": "1986-01-24-left-placas",
    "article_text_for_review": "DISCOGRAFIA (Placas)\n\nPor Manuel Yerga\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65",
    "title": "Discografía placas",
    "periodical": "candil",
    "issue_id": "1986-01",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 35,
    "article_char_count_full": 219,
    "article_char_count_review": 219,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-03-3-right-editorial",
    "article_text_for_review": "Concurso Nacional de Arte Flamenco Córdoba, 1986\n\na undécima edición del Concurso Nacional de Arte Flamenco de Córdoba debe suscitar algunas reflexiones en quienes, desde distintas perspectivas, estamos atentos al fenómeno flamenco. Nadie cuestiona la importancia, dentro de la más reciente historiografía jonda, de aquel certamen que promoviera Ricardo Molina y el Ayuntamiento de Córdoba en el año de 1956, y que se ha institucionalizado en la hermosa ciudad de la Mezquita. Ya dijimos, en otra ocasión, que el concurso cordobés marcaba un hito, un antes y un después, de suerte que con el mismo se determina una nueva sistemática de los grandes períodos que configuran la historia del flamenco. La etapa que bien pudiera denominarse de los Festivales Flamencos, arranca del concurso cordobés. Y si bien sus organizadores se propusieron como objetivo exclusivo la dignificación del flamenco, es lo cierto que secuelas de diversas naturaleza trascienden hasta nuestros días y, en cierto modo, determinan el auge de dos décadas —sesenta y setenta— y, quién sabe, si también el declive de los ochenta. Pero estas afirmaciones requieren algunas precisiones.\n\nCon toda probabilidad, a los organizadores cordobeses de 1956, les vino la inspiración del concurso granadino de 1922. Pero las diferencias entre uno y otro certamen son notables. Sin ánimo de reiterar las contradicciones en que incurren los escritores, músicos y pintores de la generación del 27, extremo éste sobre el que ya existe una abundante bibliografía, no me sustraigo a la tentación de realizar algunas puntualizaciones sobre el concurso granadino en relación al concurso cordobés de 1956. Así, del careo de uno y otro certamen, podría inferirse lo siguiente: En 1922 se intenta recuperar la expresión jonda allí donde sólo existe en forma precaria. Se excluyen los calificados como «artistas», auténticos depositarios de este patrimonio cultural; en 1956 se sabe dónde está el flamenco y se pretende proyectarlo a sectores populares por la vía de su dignificación, es decir, afirmando los estilos más puros y menos conocidos, frente al reinado fandangueril y cupletero tan del gusto de la época. En Granada se siente el flamenco como fuente de inspiración, como manojo de ancestros, de misterios, sobre los que pueden construirse hermosas teorías literarias y musicales, de indudable contenido artístico pero desconocedores de la verdad flamenca. En Córdoba, con ausencia de elucubraciones, sin deseño gratuito de teoría, se aprehende lo jondo, se contacta con la realidad humana de los intervinientes, se configura una nueva forma de comunicación con el gran público de la que se va a originar, a la postre, el éxito de los Festivales Flamencos. En 1922 se idealiza sobre el flamenco, sin apenas conocerlo; en 1956 se habla, se profundiza en el mismo con absoluto rigor. Y no es que se minusvaloren los estudios de investigación que se realizan con ocasión del concurso de 1956, sino que éstos manejan datos precisos, de relevancia historiográfica, más que líricas exégisis. En cierta medida, el entorno específicamente jondo se desvela en el concurso cordobés y ello condicionará la aparición de toda una pléyade de artistas, de genuino cuño, que oficiarán de maesfros de las más recientes generaciones, con lo que se asegura, fidedignamente, la transmisión del legado flamenco. A ello hay que añadir el interés que suscitan los estudios monográficos que con ocasión del concurso cordobés se realizan (Anselmo González Climent, Ricardo Molina...) y que tendrán la virtualidad de incentivar la investigación del fenómeno flamenco a toda una generación de intelectuales andaluces.\n\nLa historia del flamenco, sus protagonistas, le deben a Córdoba, en términos de estricta justicia, este reconocimiento.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 588,
    "article_char_count_full": 3769,
    "article_char_count_review": 3769,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-03-3-right-concurso-nacional-de-arte-flamen",
    "article_text_for_review": "Para Rosario López\n\nRecuerdo, prima, los días de duquelas y destierros, recuerdo el pulso a la muerte, recuerdo, prima, recuerdo...\n\nFatiguitas que no matan que es un veneno muy lento; quererte y que no me creas, querer como estoy queriendo.\n\nQué buena tierra la tuya y qué ingrato fue aquel árbol que ni fruto o sombra daba, y si daba, daba engaños.\n\nLa gente que te murmura y que murmura a mi paso, otro tiempo murmuraron del señor crucificao.\n\nSalgo a la mar y me pienso que no tengo ya mujer que rece por si no vuelvo.\n\nLástima que aquel hombre, de naide tiene calor; y no he caído en la cuenta que aquel hombre era yo.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "4-4",
    "page_number": 4,
    "word_count": 117,
    "article_char_count_full": 623,
    "article_char_count_review": 623,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
