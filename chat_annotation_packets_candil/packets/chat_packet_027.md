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
    "article_id": "1981-05-20-left-discograf-a-de-artistas-flamenco",
    "article_text_for_review": "DISCOGRAFIA DE ARTISTAS FLAMENCOS Por Manuel Yerga Lancharro\n\nMANUEL CENTENO",
    "title": "Discografía de artistas flamencos",
    "periodical": "candil",
    "issue_id": "1981-05",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 10,
    "article_char_count_full": 76,
    "article_char_count_review": 76,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-07-3-right-editorial",
    "article_text_for_review": "Editorial\n\nEn fechas próximas, se va a celebrar la novena edicción del Congreso de Actividades Flamencas, que Almería ha organizado con exquisito rigor y notable eficacia.\n\nCreemos que un congreso no es un aula, ni su finalidad primordial es aprender algo, entre otras razones, porque lo aprendible en el flamenco, en términos de absoluta objetividad, es bien poco. Nos preguntamos sobre cuál es el centro de interés sobre el que convergen las inquietudes de los congresistas. Formalmente, habría que decir que es la conservación de la pureza del flamenco, concibiéndose tal pureza desde la perspectiva intimista de cada aficionado. Nos olvidamos de la desnuda objetividad, como si el cante fuera sólo sentimiento, vivencia, y no tuviese virtualidad suficiente para desgajarse de sus amantes estudiosos.\n\n¿En qué momento de su historia es el cante puro? ¿Qué es en consecuencia lo que debemos conservar? Para tales interrogantes no creo que exista respuesta idónea. Entiendo que lo que hay que conservar es la pureza de los aficionados, es decir, su capacidad de contemplación ante esa expresión artística, porque el cante seguirá siendo «puro» mientras no se malogren sus contempladores. En este sentido el noveno Congreso de Actividades Flamencas puede cumplir un primordial objetivo: enriquecer, fomentar, mediante el encuentro de los aficionados, esta contemplación.\n\nLos organizadores han asumido, perfectamente esta realidad y su labor ha incidido en la solución de importantes problemas que afectan el mismo sustratosaciológico del cante. Para muchos, tal vez, este trabajo se refiere sólo a los aledafios del cante. Para nosotros es esta una tarea conmovedora que indirectamente va a incentivar la mencionada facultad de contemplación. Va, en definitiva, a mejorar el cante, en las personas que lo hacen posible, contribuyendo a que estos sistemáticos encuentros dejen de estar poblados de bizantinos debates, cultas e ineficaces disquisiciones, requiebros culturales de unos colectivos hacia otros.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 304,
    "article_char_count_full": 2007,
    "article_char_count_review": 2007,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-07-4-right-documentos-flamencos-para-una-in",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Manuel Urbano\n\nQuede el siguiente medio centenar de letras, todas ellas anteriores a 1936, por toda una interpretación de ciento veinticinco años de la historia de España y sea el lector quien saque sus propias conclusiones de los testimonios que, a mi juicio, contienen una verdadera significación política estricta. En otro número daremos entrada a varios centenares de letras, igualmente anteriores a la guerra civil, de incuestionable significado político-social. Ambas colecciones son muestrario y parte de la antología de un próximo libro nuestro.\n\nEn la medida de lo posible he intentado documentar las letras que siguen, a la vez que he efectuado unas ligeras anotaciones sobre los personajes y acontecimientos históricos que aparecen en las mismas. El lector, al que supongo conocedor\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"histórico\"]\n\no significado de la época, en el que es fácil advertir todo un sentimiento de identificación nacional y libertad. De esta copla escribió Demófilo: «Llama el pueblo baluarte invencible a la Isla de León (Isla de San Fernando) porque los franceses no pudieron penetrar en ella durante la guerra de la Independencia, consiguiendo sólo entrar en dicha plaza el año 23, cuando acaudillados por el duque de Angulema fueron llamados por Fernando VII; hecho histórico a que alude la palabra traición de este cantar.» || Er día que en capiya metieron a Riego los suspiros que daban sus tropas yegaban ar sielo. Ⅲ Salgan los santitos, de San Juan de Dió a peí limosna p'al entierro de Riego, que va de por Dió. IV Mataron a Riego, ya Riego murió; como se biste, se biste de luto toa la nasión. Estas tres coplas, de los primerísimos años del flamenco histórico, hacen concreta referencia a la muerte de Riego, tr\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_04 | trigger=\"hombres\"]\n\nrrijos baliente ¡Miren qué doló! VI El día en que mataron a Torrijos el valiente grandes guerrillas s'armaron y hasta el cielo se nubló y los güenos liberales juraron aquel día: Yo también me vengaría pero matarte a ti no. Desde luego, esta cantiña recogida por José Blas Vega de Aurelio Sellés —«Conversaciones flamencas con Aurelio de Cádiz»— es más incompleta y de gusto inferior a la antecedente que recogiera Demófilo. Torrijos, otro de los hombres más significados del liberalismo español, víctima de una provocación fue atraído a las playas malagueñas, en las que desembarcó junto con otros cincuenta y dos compañeros, todos ellos ejecutados por las fuerzas absolutistas en 1831. VII Cúchares para torero, y pá goberná la España ¡Don Baldomero Espartero! Recogida por Luis Coloma en «Cuadro de costumbres populares», donde describe las fiestas del carnaval sevillano de 1867, y retrata, con mal disimulado desprecio, el ambiente de la taberna «La Cita», de cuyo interior oyó salir la copla desde una voz aguardentosa y junto el «clásico palmoteo con que suele el pueblo andaluz acompañar sus cantos, alternando con las más soeces interjecciones.» Díficil resulta resumir en unas líneas la larga vida política del liberal general Espartero, sin duda alguna, uno de los militares españoles más populares del pasado siglo. VIII Con esa mata de pelo y esa cara de sandunga tiene usted más hombres muertos que tiene Isabel Segunda. En este texto, recogido por Demófilo y publicado en la «Revista de Literatura», el pueblo utilizó su guasa y el ambivalente piropo para calificar a Isabel II. IX Viva Prim y el gran Topete y toitas sus legiones. Viva Emilio Castelar que es contrario a los Borbones. Que a la mar que te vayas... Estas alegrías de Tío José el Aguila hacen referencia, a mi entender, a la revolución del 19 de septiembre de 1868, cuando la escuadra concentrada de la bahía de Cádiz se subleva al grito de «¡Viva Es\n\n[EVIDENCE WINDOW 3 | retrieval_hint=HERIT_02 | trigger=\"inscripciones\"]\n\norte de «Carnaval», nos parece muy ocasional y falto de la médula característica de las canciones flamencas. Díaz del Moral nos servirá para situarlo históricamente: «Cuando en octubre de 1868 se dividieron los demócratas en monárquicos y republicanos, la mayoría de los cordobeses se afiliaron a este bando, y, al amparo de las nuevas libertades, don Francisco Leyva y sus amigos organizaron procesiones cívicas, en las que se paseaban banderas con inscripciones alusivas al triunfo de la República y mítines y manifestaciones de propaganda (...). El 3 de diciembre, una numerosa manifestación republicana, dirigida por Leyva, atravesaba las calles de Montoro para celebrar una reunión en la que el jefe cordobés había de dirigir su palabra a la numerosa muchedumbre. Al pasar por las Casas Consistoriales sonaron vivas y mueras, y la fuerza pública que custodiaba el edificio, creyendo ser agredida, disparó sus\n\n[ENDING CONTEXT]\n\ninvencible, Isla de León, donde se rindió el coloso Napoleón Bonaparte y allí perdió su victoria y en Waterló.\n\nA pesar de la referencia final al desastre del emperador francés, esta letra atribuida a Chiclani-ta nos parece una variante de la marcada con el número 1.\n\nXLIX\n\nNapoleón con su escolta no pasó del balneario de la Victoria.\n\nL\n\nQue vengan pronto los francesitos pa que los desengañen los gitanitos. Que venga pronto Napoleón pa que le den en Cádiz la extramaunción.\n\nCompra-Venta de Coches Usados y Nuevos\n\nAvenida de Granada, 15\n\nJ A E N\n\nTeléfono 91 24 05\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Documentos Flamencos para una interpretación política de la Historia de España",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "4-8",
    "page_number": 4,
    "word_count": 3982,
    "article_char_count_full": 23673,
    "article_char_count_review": 5488,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "histórico"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombres"
      },
      {
        "window": 3,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "inscripciones"
      }
    ]
  },
  {
    "article_id": "1981-07-8-right-pedagogia-de-un-arte",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n¿Puede el arte flamenco situarse al mismo nivel de cualquiera de las artes que componen nuestra cultura popular?\n\nNo sería desacertada la idea de que dentro de cincuenta años o quizás un siglo, en el interior de un museo de folklore se leyera la siguiente inscripción: «El arte flamenco, una expresión popular de los hombres de ciertas regiones de España, que no sólo perdieron el modo de manifestar su amor y sus tristezas por medio del cante, sino que además perdieron sus raíces y origen por la falta de una correcta enseñanza.»\n\nPor Antonio Piñana (padre) y Juan Ruipérez Vera\n\nEsta situación, un tanto ilógica, según el ángulo en que se mire, hace imperativo recordar las palabras de Luis Millet recogidas por Emilio Gutierrez Torralde, de la Escuela Superior de Música Sagrada de Madrid, en su\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"alma\"]\n\nángulo en que se mire, hace imperativo recordar las palabras de Luis Millet recogidas por Emilio Gutierrez Torralde, de la Escuela Superior de Música Sagrada de Madrid, en su estudio, fechado en Alicante en el año 1970, sobre «El canto popular y la liturgia», que decía: «El canto popular, esta cosa única y virginal del espíritu humano, simple y bella, vive de la sinceridad bellamente adornada en la simplicidad, canta para solaz desinteresado del alma y por lo mismo, sale con inocencia juvenil mostrando ingenuamente la fisonomía característica de la raza, aquella fisonomía primordial sobre la cual los genios levantaron los monumentos que señalan los límites de la potencia suprema del intelecto humano; este canto, consuelo de sabios e ignorantes, tiene la virtud de mostrar con pura transparencia el fondo de la belleza con la misma evidencia para los eruditos y sutiles que para los humildes ignorantes.» Estas palabras de Luis Millet nos sitúan, aunque sea en hipótesis, en el nacimiento del flamenco como manifestación popular. A pesar de que el flamenco surge como la necesidad imperiosa de expresión de un pueblo, que lleva consigo la fisonomía característica de su raza, y a pesar de que su origen es simple y bello a la vez, él, con el paso del tiempo, se transforma en un arte que es sólo, y por desgracia, privilegio de unos pocos. Para nosotros, sus valores no han sido aceptados con la integridad que enaltece a cualquier otro arte. Estos hechos nos inducen a recapacitar e intentar hallar las circunstancias que han hecho que el flamenco no haya encontrado su plataforma estable dentro del contexto de las Be- llas Artes y, por lo tanto, su proyección futura libre de enajenaciones impropias que disminuyan sus\n\n[ENDING CONTEXT]\n\nen los que radican toda la temática flamenca, como son: la bibliografía, sección y archivo de música, descubrimiento de valores, peñas, recitales, festivales, escritores, poetas, artistas, etc. Estos aspectos fundamentales, individualmente, son manantiales inagotables de donde emanarían las materias que, después de una perfecta programación, se impartirían tras una eficiente enseñanza oficial a distintos niveles.\n\nServicio: HERMANOS BARRANCO B O D A S B A N Q U E T E S B A U T I Z O S CONVENCIONES\n\nESMERADO SERVICIO - SELECTA COCINA\n\nPlaza de Belén, 1 - Teléfono 22 47 89 - J A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Pedagogía de un arte",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1023,
    "article_char_count_full": 6168,
    "article_char_count_review": 3345,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "alma"
      }
    ]
  },
  {
    "article_id": "1981-07-9-right-los-extremos-se-tocan",
    "article_text_for_review": "Por Luis CABALLERO POLO\n\nLa confusión debió iniciarse con el rebuscamiento absolutamente profesional de «creaciones» ya creadas. Debió partir de una forzada necesidad de distinción a veces brillantemente lograda —a pesar de los pesares— por el artista nato y otras disparatadamente desviada de las más mínimas exigencias ortodoxas. Fueros aquellos cantaores que se autoproclamaban nada menos que creadores por acompasar una canción por bulerías o descomponer la composición de un cante ya perfectamente construido por el alambique de la inspiración colectiva y la madurez del tiempo.\n\nHoy no se especula con el atrevimiento conceptual de CREACION, pero sí con el de innovación. En el caso que nos mueve, creación más innovación es igual a arreglo; aflamencamiento de algún aire musical, tantas veces ajeno al flamenco. Exactamente lo que hicieron aquellos desviados artistas del cante que desde hace veinticinco años comenzaron a ser tachados de «impuros» por tantos y tantos de los que hoy aplauden rabiosamente sones rítmicos interminablemente monocordes y distorsiones guturales desagradablemente extrañas a la naturaleza armónica del cante.\n\nCualquiera que haya penetrado ligeramente en la hondura del flamenco sabe que éste, por su origen folklórico, rechaza de plano modas y modismos. Sin embargo, este origen sólo es sostén y proyección en su calidad de raíz-base. Ya en sus ramas vivas de arte bulle y crece una lógica y natural permanencia evolutiva que, en definitiva, explica el misterio del cambio y engrandecimiento del flamenco al correr de los tiempos. El cante, el toque y el baile, a medida que avanzan se ensanchan como los grandes y largos ríos, pero avanzar y ensancharse no es salirse de madre, desnaturalizarse, perderse sin cauces ni destino. Sabemos que el equilibrio, desde el que anda sobre un alambre hasta el que anda sobre la filosofía, resulta minoritario y transitoriamente milagroso, pero ha de servir, con más o menos fuerza eficaz, la voz de la cordura en su madura acumulación de valores sólidos y vivos ejemplos de palpable presencia: La recreación obedece más al tiempo y sus circunstancias sobre la experiencia y la riqueza heredada que a propósitos personales de revolucionar, lo que por su naturaleza es delicadamente evolutivo. Ni la guitarra debe mecanizarse, violentándola en un esfuerzo de ejecución malabarista que recuerde la superación circense del «más difícil todavía», ni el baile convertirse en una exhibición gimnástica, ni el cante en una urgente búsqueda de novedades.\n\nAyer fue lo híbrido-melodioso hasta extremos almíbaradamente cársiles. Hoy es el ritmo insípido, bastardo, coreado y adobado incluso con instrumentos extraños al flamenco. Si ayer las voces profesionalizadas del cante andaluz buscaban los dulces aires suramericanos, hoy los desgarrados lamentos, vengan o no a cuento, chacacanean un «tan, tan» selvático con la etiqueta de un gitanismo confundido.\n\nLos extremos se tocan, pero... ¿Quién puede negar la libertad de expresión musical e inquietud renovadora a ningún pueblo y menos al andaluz, que en su parcela de cante grande lo hace al más alto nivel artístico? Nadie que inteligente-mente haga uso de la razón. Sea, pues, la inteligencia y la razón, con conocimiento de causa suficiente del tema, quienes distingan públicamente lo aceptable de lo absurdo.",
    "title": "Los extremos se tocan",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "9-9",
    "page_number": 9,
    "word_count": 517,
    "article_char_count_full": 3331,
    "article_char_count_review": 3331,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
