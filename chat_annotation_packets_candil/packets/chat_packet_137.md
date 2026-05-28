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
    "article_id": "1986-09-4-right-reflexiones-sobre-la-voz-y-la-co",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJean Paul Tarby\n\nesde los últi- mos decenios,\n\nnumerosas investigaciones han tratado de la significación de las coplas flamencas desde un enfoque literario o sociológico. En cambio, pocos investigadores se han interesado por el estudio de la estética vocal flamenca. No se debe olvidar que el flamenco es una forma de expresión artística de índole oral-cantado, que privilegia las modalidades de la voz frente a la mera dimensión textual de la copla. Desde un punto de vista poético global, la ejecución (1) de la copla importa más que su contenido lingüístico. Hay una «economía» específica del discurso poético flamenco, en la medida en que, gracias a la voz que lo canta, introduce un registro sensorial tan amplio y tan rico de connotaciones, que acaba en el estadillo de las palabras que lo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradición\"]\n\no de vista poético global, la ejecución (1) de la copla importa más que su contenido lingüístico. Hay una «economía» específica del discurso poético flamenco, en la medida en que, gracias a la voz que lo canta, introduce un registro sensorial tan amplio y tan rico de connotaciones, que acaba en el estadillo de las palabras que lo constituyen. Por consiguiente, el análisis de la copla cantada exige que no se la disocie de su función social, de la tradición de que se inspira, y sobre todo, de las circunstancias de su emisión. Recordemos lo que dijo Demó- filo al final del siglo pasado: «Las coplas populares no están hechas «Las coplas populares no están hechas para venderse, ni aun para escribirse, por tanto, es imposible juzgarlas bien no oyéndolas cantar,...» para venderse, ni aún para escribirse, por tanto, es imposible juzgarlas bien no oyéndolas cantar, toda vez que, no sólo la música, sino el tono emocional, les da una significación, una expresión que meramente escritas no pueden tener (...) no es que la copla se pone en música, es que la copla, cuando nace, verdaderamente real y espontánea, nace ella misma cantándose» (2). De acuerdo con la afirmación del autor de la preciosa Colección de cantes Flamencos, sólo el estudio de la «copla cantándose», es decir de la ejecución flamenca, permitirá alcanzar la honda significación de la poesía flamenca. Hay que acabar con la actitud crítica que sigue considerando la poesía flamenca como si fuera poesía escrita, empeñándose en buscar la significación de las letras únicamente a partir de las coplas copiadas en las antologías. No hay que cometer otra vez el error que consiste en aplicar al estudio de la letra flamenca —poesía genuinamente cantada— los métodos críticos de la literatura culta escrita. La copla cantada es el elemento estético y significante que debe llamar nuestra atención, mientras que la copla escrita constituye, fuera de s\n\n[ENDING CONTEXT]\n\nse responde hoy a algo tan lejano, oculto y sólo ayer, mucho debió pesar ese algo ayer para significar tanto hoy.\n\n«El misterio no cabe en la palabra». Ni en la palabra ni en el pecho del que se gusta gustando. Así es, o puede ser, cuando no se me-caniza el Flamenco, cuando el único e íntimo impulso que nos mueve a la ciega sinrazón de la entrega total es como un viento que no vemos pero que lo sentimos cargado de sugerencias espirituales. Entonces sí es posible que se produzca ese «salto al vacío donde la razón humildemente tiene que atreverse a no ver, a no explicar, sola-mente a gritar».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Reflexiones sobre la voz y la copla Flamenca",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "4-8",
    "page_number": 4,
    "word_count": 3503,
    "article_char_count_full": 21355,
    "article_char_count_review": 3542,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradición"
      }
    ]
  },
  {
    "article_id": "1986-09-6-right-consideraciones-en-torno-al-flam",
    "article_text_for_review": "Luis Caballero\n\nQue nadie espreencontrar\n\nen esta humilde y breve disertación en torno al Flamenco el menor síntoma de erudición académica. Estais ante un andaluz más, que canta el llamado Cante Flamenco y que después anda preguntando y preguntándose qué es lo que canta y qué es el Flamenco. Por lo demás, ya mi edad puede atestiguar la sencilla verdad del transcurrir y discurrir de mis inicios: primero, nacer flamenco o para el Flamenco, crecer en la tierra del Flamenco e ir encontrándolo día a día en\n\ntu propia casa, por los rincones de alguna taberna, en la troupe pueblerina, en el campo y en tu voz, hasta... por fin, llegar a esa primera noche sin guitarra, pero ya en el cuarto, donde te va a juzgar el oído de la sabiduría mientras la voz de la intransigencia va a corregirte, aconsejarte y bautizarte como hijo de un misterio que más tarde habrá de asombrarte. Era entonces ese uno de los caminos naturales que conducían al laberinto del dolor cantado, compañero de las seis lágrimas derramadas por cosas lejanas y los brazos que ponen caireles en el aire. Ni libros, ni conferencias, ni tan siquiera la aprobación de la sociedad y la propia familia. Eras tú sólo obedeciendo la llamada de la sangre, penetrando irremediablemente en un mundo extraordinario dentro de tu ordinario mundo diario. Y todo según el impulso natural de una étnica consecuencia geográfica tan nuestra como la cal, el toro y el olivo.\n\nSin embargo, cualquier día, templado ya de vivencias inmediatas, puede surgir la pregunta, acuciarte la interrogante idea flamenca que nos mueve cargados de curiosidad frente al misterio: el Flamenco, un enigma histórico-espiritual que comienza a serlo, precisamente, desde el propio significa",
    "title": "Consideraciones en torno al Flamenco",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "6-6",
    "page_number": 6,
    "word_count": 287,
    "article_char_count_full": 1717,
    "article_char_count_review": 1717,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-09-8-right-el-concepto-de-justicia-en-el-pr",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nQuisiera comenzar el siguiente\n\ntrabajo, que no es sino una íntima consideración sobre la valoración que la Justicia merece al cante Flamenco, con una máxima gitana del siglo pasado que reza así: «A liri es ye crayí micabó a liri es calé», lo que traducido viene a significar: «La ley de los reyes ha destruido\n\nJosé Luis Buendía López\n\nla ley de los gitanos». Sabrosísima reflexión que nos llevará a una serie de consideraciones generales que no deben resultar escandalosas a nadie puesto que se basan en una experiencia vital en la que se suman siglos de impotencia, desigualdades sociales evidentes y síntomas de postración en unos seres que ven cómo la Justicia común, la elaborada por las clases dominantes, no es lo suficientemente ecuánime para equiparar a todos ante el supremo tribunal de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nrosísima reflexión que nos llevará a una serie de consideraciones generales que no deben resultar escandalosas a nadie puesto que se basan en una experiencia vital en la que se suman siglos de impotencia, desigualdades sociales evidentes y síntomas de postración en unos seres que ven cómo la Justicia común, la elaborada por las clases dominantes, no es lo suficientemente ecuánime para equiparar a todos ante el supremo tribunal de la dignidad del hombre. ¿Quién sabe?, quizá la vieja copla gitana en la que se alude a la decadencia de esta raza esté impregnada de tan amarga experiencia, y sea consecuencia funesta de la sustitución de aquellas ancestrales leyes que dic- tara el clan por la fría aplicación de los decretos palaciegos: En un tiempo los gitanos gastaban medias de seda, ahora para su desgracia gastan grillos y cadenas. Queremos que se entienda perfectamente que si hemos comenzado con un texto gitano no pretendemos con ello aludir a que sea precisamente este grupo humano el único que ha padecido en sus carnes la amarga experiencia de la injusticia; tampoco en modo alguno intentamos atribuir el profundo caudal del cante jondo a exclusivo patrimonio gitano, como en otros estudios han asegurado voces más apasionadas. Nosotros, que tenemos suficientemente claro que el cante en sus orígenes es patrimonio andaluz, una comunidad formada por etnias muy diversas, y que incluso hoy nos atrevemos a decir que pertenece a casi toda España, somos conscientes de que los problemas que sufre un pueblo son compartidos por todos los grupos humanos que lo forman, aunque, no nos engañemos, nunca en iguales proporciones. Hay casos constatados en la historia en los cuales la balanza de la equidad se ha inclinado de forma escandalosa hacia los núcleos sociales más favorecidos, y que juegan el dudoso papel de clases dominantes. Si hemos arrancado con la susodicha frase, referente a los gitanos, es como muestra de que, en numerosas ocasiones, amplios sectores del pueblo, prefieren sus propios arbitrajes, por duros y hasta discutibles que éstos puedan resultarnos, antes que un conjunto legal que perpetúa más aún la división y el enfrentamiento social, y, sobre todo, que es poco sensible a las auténticas necesidades de ese pueblo. Y es que, por ejemplo, ¿cómo justificar la contradicción que se encierra en el hecho de que, a\n\n[ENDING CONTEXT]\n\nlos cerrojos de las mentes más obtusas y, ya que no puede liberar a los que sufren por formar parte de la inmensa legión de los desposeídos de la tierra, a través de su impulso vital exhorta a que, de una vez por todas, se ponga fin a tan triste situación, a la vez que ilumina con la fuerza del rayo la figura triste, arrumbada, del que se enfrente en solitario a la tremenda máquina del Poder, que, más tarde o más temprano, acabará por devorarlo. Sólo así cobra sentido el cante marginal de nuestro pueblo.\n\nYo soy como el árbol solo que estaba al pie del camino dándole sombra a los lobos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El concepto de justicia en el primitivo cante Flamenco",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 3956,
    "article_char_count_full": 23415,
    "article_char_count_review": 3965,
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
    "article_id": "1986-09-12-left-flamencas",
    "article_text_for_review": "LET RAS\n\nEs difícil el arreglar chiquilla los desperfectos que el tiempo había ocasionao en este cariño nuestro\n\nLa tarde se despereza, abren los brazos los chopos y los olivos bostezan. Transparente como el aire, dando vueltas por mis sueños y aumentando mis pesares.\n\nEl recuerdo es como el aire, pero a veces tiene esquinas cuando el cuerpo se rebela y el alma se arremolina. En aquella esquina, junto a aquel rincón se ha roto la cuerda en el relojillo de mi corazón.\n\nEn el Suspiro del Moro Boabdil se puso a llorar, le dijo adiós a Granada y no pudo aguantar\n\nLa ropita de tu cuerpo desparrará por la alcoba me parecía de pronto una bandá de palomas.\n\nMe gusta el verte bajar con el mandilito blanco camino del olivar\n\nCorre peligro el que abre su corazón en Granada, que se le puede enfriar con los suspiros del agua.\n\nComo racimos de uva me sabían ayer tus labios bajo la luz de la luna.",
    "title": "Letras Flamencas",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 165,
    "article_char_count_full": 895,
    "article_char_count_review": 895,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-09-12-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAunque no quepa en el papel\n\n(1886-1969)\n\nde José Blas Vega EDITA: MINISTERIO DE CULTURA. MADRID, 1985 José Luis Buendía López\n\nDendita sea la memoria interminable de los flamencos. De no ser por la suerte de contar con un nutrido grupo de incansables aficionados y estudiosos, que nos prestan el servicio impagable de transmitirnos sus vivencias, muchos de los que hemos arribado más tarde a la playa del flamenco, no podríamos hacernos del mismo sino una imagen lejana y triste, ajada por la pátina implacable del tiempo, con ese color sepia que muestran los daguerrotipos que conservamos de los viejos eventos jondos.\n\nPero afortunadamente, este grupo de generosos rememoradores, entre los que deseo citar a Yerga, González Climent, Caballero Bonald, Ortiz Nuevo, Quiñones, Blas Vega y un\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"memoria\"]\n\nradores, entre los que deseo citar a Yerga, González Climent, Caballero Bonald, Ortiz Nuevo, Quiñones, Blas Vega y un larguísimo etcétera, nos están legando el goteo parsimonioso, de sus vivencias en un caso, o del fruto de sus investigaciones en el otro. Gracias a unos y a otros la cara oculta del flamenco aparece transparente ante nosotros; a unos nos descubre lo mucho que ignoramos, a otros les ayuda a recordar, les cuadra los aposentos de la memoria en aquellos aspectos más castigados por los imperativos del tiempo. Una de estas entregas es este precioso librito que ahora, con apoyo de esas instancias oficiales por las que siempre hemos clamado, nos confía José Blas Vega sobre la personalidad cantaora y humana de Bernardo Alvarez Pérez, Bernardo el de los Lobitos. (Foto del libro «La magna antología del Cante Flamenco»). al que una letra aprendida de memoria de un montañés, y que él interpretara por bulerías, le harían ser para la historia oral e invisible del cante, ya definitivamente Bernardo «el de los lobitos». Anoche soñaba yo que los lobitos me comían y eran tus ojitos negros que miraban y me decían: Por Dios no me desampares que yo he perdió la calor de mi pare y de mi mare. El autor realiza un recorrido total, desde su nacimiento, hace un siglo, junto al viejo castillo de Alcalá de Guadaira, hasta su paso por los cafés cantantes, primero el Novedades de Sevilla, y más tarde en Madrid («que es la Corte») en los nuevos\n\n[ENDING CONTEXT]\n\ntemas que ya han sido analizados y expuestos con anterioridad de manera fraccionada y dispersa por diferentes autores bien conocidos de los lectores de flamenco. Así, los nombres de Ricardo Molina, Rossy, García Martos, Caballero Bonald y algunos más, constituyen un obligado punto de referencia en la elaboración de los capítulos sucesivos. Forma la mencionada nómina una especie de diccionario de autoridades en las que Arrebola apoya todas y cada una de las opiniones que se vierten sobre los distintos aspectos temáticos abordados; eso sí, en todo caso el autor empeña su cuarto a espadas y fi-\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1004,
    "article_char_count_full": 6055,
    "article_char_count_review": 3074,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "memoria"
      }
    ]
  }
]
```
