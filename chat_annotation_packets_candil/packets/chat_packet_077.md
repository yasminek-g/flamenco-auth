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
    "article_id": "1983-07-19-left-las-letras-flamencas-de-alfonso-",
    "article_text_for_review": "¡Ay! una sombra... Que ya no era un cuerpo humano que era tan sólo una sombra. Minero fui con mi hermano y ya está en su santa gloria; las minas matan temprano.\n\n¡Ay! mi caballo... De una pata mal herío y sangrando mi caballo. Pero su gran poderío me salvó de los disparos que iban a mí dirigios.\n\nEl carburo me se ha aguao y no lo puedo encender. Nadie sabe lo que veo, cuando no se puede ver.\n\nNo me separo de ti aunque la mina esté lejos. Siempre miro tu retrato entre relevo y relevo.\n\nSe ha hundió esa galería y está mi hermanico dentro. ¡Quién sabe si estará vivo, o estará mi hermano muerto!\n\nBusqué en la Luna el abrigo. En el Sol la libertad. Por los caminos del monte me encontré con la verdad.\n\nYo no rezo ni maldigo ni quiero yo criticar. Lo que hago es practicar la verdad de lo que digo Cuando yo me esté muriendo se han de hacer ocho caminos: Siete pa que pase el fuego, uno pa que pase el frío. Guerra ¡Que no quiero guerra! que la paz la luz la temple y que el trabajo la envuelva y que los años la sellen.\n\nAnda y llévala al agua, luego a la sombra, esa yunta yuntera lucera roja.\n\nAyer trilló centeno, hoy trilla avena; y entre las dos parvas muchas fanegas.\n\nCuando suba la marea la tierra se mojará y borrará tu verea.",
    "title": "Las letras flamencas de Alfonso López Martínez",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 244,
    "article_char_count_full": 1239,
    "article_char_count_review": 1239,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-07-19-right-a-antonio-mairena",
    "article_text_for_review": "A ANTONIO MAIRENA\n\nLos labios impacientes, la luz tumultuaria de la noche, ya tan lenta, tan voraz al insondable tránsito de las lágrimas, te cuidan el olor de los musgos, los votivos lamentos de patéticas redes, donde acaso encendimos alguna mordedura de relámpago. También las voces, macilentas heridas que nacen más allá de la frontera alígera del fuego, y que por ti rompieron su quejumbre acuchillada, sus abisales gritos o recintos dulcísimos o desolada imprecación. Te cuida el son volcánico de un pueblo que acrisolan los yunques, lóbrega potestad que nos convoca a una memoria de tiniebla, a un alimento aún no ingerido de iracundias proscritas. Y te cuida el dolor de quienes viven con un hierro alevoso en las entrañas y vómito en los ojos hambreados; la tremenda pasión de estirpes desterradas y vivientes en la angostura sólida de un hombre; y gestos tan hermosos, apenas balbucidos y vulneradas bocas y frentes veneradas que cruzaron un enorme rebaño de improperios. Y yo mismo te cuido en la memoria, y caliento tu voz, la recupero de los blancos tapiales, de la cal, del alfarero barro o de la fragua; y de esa magnitud, como una culpa, de las atroces quejas, la culpa de estar vivo y de que tú te me hayas muerto.",
    "title": "A Antonio Mairena",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 213,
    "article_char_count_full": 1230,
    "article_char_count_review": 1230,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-07-20-left-un-solo-ritmo",
    "article_text_for_review": "Por Ricardo Rodríguez Cosano\n\nA L leer el artículo TIERRA SOBRE TIERRA (Aproximación a la Flamencología de J. M. Ca-ballero Ronald) (II), de José Luis Buendía, que, si no recuerdo mal, tuve la suerte de poder estrechar su mano en el Congreso de Jaén, se nos brinda la oportunidad de bucear en una profunda investigación de donde se podrán sacar notas para el constante aprendizaje que suponen nuestras vivencias en el Cante.\n\nPues bien, en la página 11 del número 26 (Marzo-Abril) de CANDIL, podemos leer lo siguiente: «Los cuatro ritmos o variantes fundamentales del flamenco son, en síntesis, estos: siguiriya, soleá, tango y bulería. Son la base en torno a la cual gira todo el universo musical del resto de los estilos. Hay, sin embargo, un ritmo propio para el cante y otro muy distinto que es el que marca la guitarra, aunque suelen superponerse y lograr la unión íntima de intereses artísticos que dará auténtico valor a la interpretación. Creemos, con Caballero Bonald, que en este caso el ritmo interno correspondería al cante y el externo a la guitarra; ambos consiguen el ritmo único, total, que se transmite en la emoción indescriptible que antes explicábamos; si falla alguno de ellos o si domina el otro de manera demasiado notoria, el aficionado notará el desfase y la atmósfera decae inevitablemente».\n\nSin ánimo de sentar cátedra, sino con afán de diálogo —pues es uno de los objetivos de CANDIL— en el deseo de poder aclarar algunos puntos, que al parecer quedaron difusos, he aquí algunas consideraciones:\n\nAl principio se nos habla de «cuatro ritmos o variantes». Después, de ritmo interno y ritmo externo. Ello puede dar lugar a confusión, ya que una vez, ritmo se refiere a «palo» o estilo de cante, y otra, aparece como elemento inseparable de un compás.\n\nSin embargo, en lo que discrepamos es en aquello de «...el ritmo interno correspondería al cante y el externo a la guitarra». Entendemos que hay un solo ritmo que marca el tocaor (acomodación del tiempo en los distintos compases de la falseta) y que a veces el cantaor puede variar de aire con un simple gesto. De esta manera, el ritmo será el vehículo que transporte al cantaor y tocaor por caminos ignotos de falsetas con feliz retorno; aquí el ritmo podíamos imaginarlo como un punto melancólico que se mueve por los sucesivos compases de la falseta. Lo que ocurre en el Cante es que dos estructuras diferentes, la del cantaor y la del tocaor, suelen acomodarse en cada tercio (de ahí el arte de acompañar), pero siempre ambos con el mismo ritmo. Esto es válido para todos los cantes; lo que ocurrirá es que el cantaor irá más encorsetado por el compás en unos cantes que en otros.\n\nLo que pasa con frecuencia es que el cantaor no acude a la llamada del tocaor al final de las falsetas de entrada por falta de decisión o por no encontrar la letra en el momento apropiado. Otras veces, el cantaor, fuera de sitio, entra en un instante de inspiración en medio de una false-ta. Finalmente, y esto ocurre por la prisa de ciertos sectores del público festivalero, el cantaor comienza sin la debida preparación de los toques de entrada a los cantes. En estos casos y en otros más, el guitarrista tiene que «enmendarse»; es decir, al quedar partida la falseta, no hay otro remedio que comenzar de nuevo otra, para seguir en unión del cantaor por los sucesivos compases, con un solo ritmo.\n\nBar TOMAS\n\nAPERITIVOS SELECTOS\n\nEspecialidad en\n\nPLANCHA\n\nMesones, 18 Teléf. 23 40 46\n\nJ A E N",
    "title": "Un solo ritmo",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 602,
    "article_char_count_full": 3459,
    "article_char_count_review": 3459,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-07-20-right-xi-congreso-nacional-de-a-flamen",
    "article_text_for_review": "Programa de Actos\n\nLUNES, DIA 12-IX:\n\n19 horas.—Inauguración de la Exposición de Pintura. Lugar: Palacio de la Madraza. C/. Oficios.\n\n19,30 horas.—PRIMERA CONFERENCIA. Tema: «García Lorca y el Cante Jondo». Conferenciante: Excelentísimo señor don Antonio Gallego Morell, Rector Magnífico de la Universidad de Granada. Lugar: Sala de Caballeros Veinticuatro del Palacio de la Madraza. C/. Oficios.\n\n20,30 horas.—Inauguración de las exposiciones de: «Guitarras Flamencas» y «Carteles de temas flamencos». Presentación del libro de Eusebio Rioja: «Guitarras Granadinas». Copa de vino. Lugar: Sala de Exposiciones de la Caja Rural Provincial. C/. Gran Vía de Colón, 48.\n\n22 horas.—Primera Sesión de las I Jornadas de Cine Flamenco. Lugar: Auditorio de la Caja Rural Provincial. C/. Gran Vía de Colón, 48.\n\n20 horas.—Asamblea de la Institución para la Tercera Edad de Artistas Flamencos (ITEAF). Lugar: Sala de Caballeros Veinticuatro del Palacio de la Madraza. C/. Oficios.\n\nMARTES. DIA 13-IX:\n\n19 horas.—Inauguración de las exposiciones de: «Humor Flamenco», de Martin Morales, y «Caricaturas de Personajes Flamencos», de Carlos Belda. Lugar: Sala de Exposiciones del Centro Cultural «Manuel de Falla». Recinto de la Alhambra.\n\n22 horas.—Tercera Sesión de las I Jornadas de Cine Flamenco. Lugar: Auditorio de la Caja Rural Provincial. C/. Gran Vía de Colón, 48.\n\nJUEVES, DIA 15-IX:\n\n9 a 12 horas.—Retirada de documentación en el Centro Cultural «Manuel de Falla». Recinto de la Alhambra.\n\n20 horas.—SEGUNDA CONFERENCIA. Tema: «Testimonios Literarios del Cante Jondo. Siglos XIX y XX». Conferenciante: Dr. Andrés Soria Ortega, Catedrático de Literatura de la Facultad de Filosofía y Letras de la Universidad de Granada. Lugar: Sala de Caballeros Veinticuatro del Palacio de la Madraza. C/. Oficios.\n\n22 horas.—Segunda Sesión de las I Jornadas de Cine Flamenco. Lugar: Auditorio de la Caja Rural Provincial. C/. Gran Vía de Colón, 48.\n\n13 horas.—Inauguración de la Estafeta Temporal con Matasellos Conmemorativo. Lugar: Palacio de la Madraza. C/. Oficios.\n\nMIERCOLES, DIA 14-IX:\n\n13,30 horas.—Recepción de Congresistas en el excelentísimo Ayuntamiento de Granada. Plaza del Carmen.\n\n14 horas.—Copa de vino, ofrecida por el excelentísimo Ayuntamiento de Granada. Muestra de Trovos Alpujarreños por el Grupo de Murtas. Lugar: Corral del Carbón.\n\n10 horas.—CUARTA SESION DE TRABAJO. Lugar: Sala de Congresos del Centro Cultural «Manuel de Falla». Recinto de la Alhambra.\n\n17 horas.—Visita a la ciudad por los acompañantes de los señores congresistas.\n\n17 horas.—PRIMERA SESION DE TRABAJO: Elección de Mesa, Exposición y examen de los mandatos del X Congreso. Inicio de las Ponencias. Lugar: Sala de Congresos del Centro Cultural «Manuel de Falla». Recinto de la Alhambra.\n\n23 horas.—Encuentro de Guitarras Flamencas. Lugar: Patio de los Arrayanes de la Alhambra.\n\nVIERNES, DIA 16-IX:\n\n12 horas.—Proyección de un vídeo sobre el Congreso y copa de vino a los acompañantes de los señores congresistas. Lugar: Establecimientos Sánchez. Avda. José Antonio, 98.\n\n9,30 horas.—Visita turística a la Alpujarra por los señores acompañantes de los congresistas. Salida: Fuente de las Batallas. Almuerzo: Capileira.\n\n17 horas.—ULTIMA SESION DE TRABAJO. Conclusiones y elección del Comité Ejecutivo del XII Congreso. Lugar: Sala de Congresos del Centro Cultural «Manuel de Falla». Recinto de la Alhambra.\n\n10 horas.—SEGUNDA SESION DE TRABAJO. Lugar: Sala de Congresos del Centro Cultural «Manuel de Falla». Recinto de la Alhambra.\n\n14 horas.—Copa de vino ofrecida a los señores congresistas. Lugar: Peña de Arte Flamenco «La Platería». Placeta de Toqueros, 7.\n\n17 horas.—TERCERA SESION DE TRABAJO. Lugar: Sala de Congresos del Centro Cultural «Manuel de Falla». Recinto de la Alhambra.\n\n22 horas.—Cena de Clausura ofrecida por la Excelentísima Diputación Provincial. Lugar: Hospital Real. C/. Hospicio, s/n.\n\n23 horas.—Festival Flamenco a beneficio de la Institución para la Tercera Edad de Artistas Flamencos (ITEAF). Lugar: Anfiteatro de los Jardines del Generalife.\n\nSABADO, DIA 17-IX:",
    "title": "XI Congreso Nacional de A. Flamencas",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 602,
    "article_char_count_full": 4067,
    "article_char_count_review": 4067,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-07-21-left-jose-cepero",
    "article_text_for_review": "J OSE Cepero nació en Jerez de la Frontera, allá por el 1888. Desde su infancia comenzó su carrera artística, circunstancia que él resaltaba: «Fui el más joven cantaor del siglo —comentaba—. Comencé a cantar a los ocho años. Fosforito fue mi compañero». A tan temprana edad, José Cepero alternaba igualmente con cantaores de la talla de don Antonio Chacón. Sobre 1917, José Cepero alcanzó tal fama en Sevilla, que era uno de los cantaores que más cobraba por sus actuaciones. En 1921 se presentó en Madrid donde consiguió la Copa de Oro del Cante, en la cual se insertaba una dedicatoria de su paisano Primo de Rivera. Tras infinidad de giras artísticas y grandes estancias en las más importantes ciudades de Andalucía, murió en Madrid, a los setenta y dos años de edad, en 1960.\n\nSelecciona: Rafael Valera\n\n«Su cante estaba en consonancia con su gran humanidad. Su voz era agradable, musical y clara», nos dice Ríos Ruiz. «Y sus fandangos eran pájaros que volaban con majestad. Servía su cante de recreo para el oído y para el espíritu. Vivió en señor del cante, dándole su verdadera importancia. Y aunque el fandango fue el estilo que más interpretó, tal vez porque el público de su tiempo se lo pedía —continúa Ríos Ruiz—, Cepero fue un completísimo cantaor, pues ejecutaba con poderío y buen compás soleares y siguiriyas, como puede comprobarse en la abundancia discografía que legó».\n\nAl igual que otros muchos intérpretes, José Cepero gustaba de presumir de su arte y capacidad creativa, sobre este aspecto él comentaba: «Nadie puede imitarme, nadie\n\nen una juerga puede cantar después de mí, porque lo mío es inspiración». Dicha inspiración del artista, autor de muchas de sus letras, le valió el sobrenombre de «poeta del cante». Sobre esta su virtud literaria, Cepero decía: «Una comedia de los Quintero que les llevaba ocho meses de trabajo, yo la reducía al pronto en una sola quintilla».\n\nComo queda arriba dicho, sobresalió por fandangos, soleá y siguiriyas, e incluso jugó con la frontera de las malagueñas y granaínas, creando una personal. Quizás el autor que mejor definió su cante y su persona fue González Climent, el cual dice: «Cantaor que rondó lo que había de jerarquía y maestrazgo en la llamada por él “época de oro del cante”. Concurriendo con elementos básicos, no llegó a alcanzarlo... No pudo acaudillar discípulos por ser un curioso ejemplar de artista abstracto. Profundo, pero sin raíces individuales, cumplió decididamente su vocación de neoclásico. Su estilo objetivo, privado del juego de un yo claro, no suscitó antagonías ni excesivas admiraciones. Cernido en gris neutralidad flamenca, Cepero fue un respetable solitario». Por otro lado, González Climent vuelve de nuevo a citar al cantaor: «José Cepero es un cantaor generacionalmente desubicado, indeciso. Hay en el total de su personalidad una nota de desequilibrio histórico evidente (no exento de interés en ocasiones). Su indecisión se hace a la larga reiteración, estilo “personal”, resbalando hasta la sequedad. La profundidad de muchos cantes cobra en él el triste sabor a oficio y disciplina. Lo fogoso se hace correcto. Es un cantaor inmovilizado, monocorde, en transición».",
    "title": "José Cepero",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 520,
    "article_char_count_full": 3173,
    "article_char_count_review": 3173,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
