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
    "article_id": "1985-11-4-right-una-visi-n-del-gitano-andaluz-en",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\na cita del profesor Le-blon con la que encabe-zamos este trabajo, pretende ser tan sólo una llamada de atención encaminada a que, de una vez por todas, concedamos al pue-blo gitano la importancia que su presencia cultural y étnica ha tenido y tiene en nuestra península, desde su incierta llega-da a ella en el siglo XV. Y todo ello al margen de visceralismos culturales (prefiero pensar que no se trata de prejuicios raciales) de los que, o conceden al gitano todo el crédito ilimitado que les dicta su fantasía y los transforman en seres cuasi mitológicos, o los que, llevados a su vez por otro tipo de espasmos intelectuales rebajan la cultura gitana a niveles de pura delincuencia y carne de carroña a extirpar.\n\nPara muchos, los gitanos son un componente básico en la conformación étnica y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nes cuasi mitológicos, o los que, llevados a su vez por otro tipo de espasmos intelectuales rebajan la cultura gitana a niveles de pura delincuencia y carne de carroña a extirpar. Para muchos, los gitanos son un componente básico en la conformación étnica y cultural andaluza, para otros ni siquiera han sabido inventar algo por lo que merezca la pena recordarlos. La polémica en torno a su mayor o menor aportación al cante flamenco puede reflejar, mejor que ninguna otra cosa, lo que aquí estamos exponiendo. Por José Luis Buendía La misma confusión que existe a nivel de opinión común sobre el tema gitano, está también presente a niveles literarios; la literatura española, desde el hecho mismo de la llegada de las primeras tribus a España, comenzó a dividirse en opiniones encontradas: Alabanzas (bastante pocas) y denuestos, que se contaban por miles. El período literario que hoy llamamos del Siglo de Oro fue pródigo en este tipo de debates: Podemos afirmar que el gitano estuvo de moda, de una rabiosa actualidad sólo repetida con la misma intensidad en el período romántico, en el cual cautivaba (y no sólo a los españoles, sino a la nómina básica de los escritores europeos) lo exótico de su aspecto y su deslumbrante y anárquica forma de perseguir la libertad. La plana mayor de los escritores de los siglos XVI y XVII analizaron, con mayor o menor profundidad, el fenómeno gitano: Miguel de Cervantes sería el caso más notable por su calidad y número de obras en las que estudia, a su manera, a esta raza: La Gitanilla, Pedro de Urdemalas, el Coloquio de los Perros, etc. Pero también Lope de Rueda, el insigne abuelo del teatro español, Sancho de Moncada, Jerónimo de Alcalá, González de Céspedes y Meneses, Juan de Timoneda y un larguísimo etcétera que haría interminable la relación, echaron su cuarto a espadas sobre el tema, incurriendo en visiones pintorescas, injustas, o claramente racistas y discriminatorias que no vamos a analizar en detalle porque la mayoría ha sido objeto de los correspondientes estudios. Tópicos como los de gitanos asesinos, caníbales, raptores de niños y demás lindezas, descalifican por sí mismos a quienes los difundieron. Pero junto a estas aberrantes opiniones existen otras que también fomentaron el es- Vicente Espinel músico, poeta y autor de la «Vida del escudero Marcos de Obregón», escribe allá por el reinado de Felipe III cuando éste publica su famosa Pragmática antigitana tereotipo gitano, pero con más mesura y agudeza de observación: Yo quiero detenerme hoy en el estudio de una de las más importantes novelas del siglo XVII español: Se trata de la Vida del Escudero Marcos de Obregón, escrita por Vicente Espinel, nacido en el pueblo de Ronda y fines del año 1550. Dicho autor, músico y poeta notable, de vuelta ya en la vida y con una notable carga de experiencias encima, comienza a escribir en torno a 1610 la novela a que hacíamos referencia y que debió de concluir hacia 1615 ó 1616. Reinaba entonces en España el rey Felipe III, y toda la visión del tema gitano, acertada o equivocada, que Espinel recoja en esta su obra máxima, ha de contemplarse forzosamente a la luz de los desatinos y arbitrariedades vertidos en aquel reinado sobre dicha raza: El 28 de junio de 1619 (aún fresca la tinta del Marcos de Ob\n\n[ENDING CONTEXT]\n\nLÁZARO CARNETER, F.: Lazarillo de Tormes en la Picaresca. Barcelona, 1972. Estilo barroco y personalidad creadora. Madrid, 1974.\n\n— LEBLON, Bernard: Les gitans dans la littérature espagnole. Toulouse, 1982. Les gitans d'Espagne. París, 1985. — MOLHO, Maurice: Introducción al pensamiento picaresco. Salamanca, 1972.\n\n— PARKER, Alexander: Los pícaros en la literatura. La novela picaresca en España y Europa. Madrid, 1971.\n\n— RICO, Francisco: La novela pircaresca y el punto de vista. Barcelona, 1982 (2. $ ^{a} $ edic.).\n\n— TIERNO GALVAN, E: Sobre la picaresca y otros estudios. Madrid, 1974.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Una visión del gitano andaluz en un libro del siglo de oro",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 3982,
    "article_char_count_full": 23924,
    "article_char_count_review": 4895,
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
    "article_id": "1985-11-8-left-copias-del-portal",
    "article_text_for_review": "Iban por la serranía los tres Reyes del Oriente. ¡Desde qué lejos venían!\n\n* * *\n\nY cuatrocientos pastores se pusieron a contar las gracias de Su hermosura; no pudieron acabar.\n\n* * *\n\n* * * ¡Viva la Virgen y el Niño, viva el Señor San José; que esta noche hay festolina en el Portal de Belén!\n\nPastores aburrios tocar las parmas que las fiestas se han hecho pa disfrutarlas.\n\nSalió primero el Rey Negro, salió un pastor a bailar: Salió toda la grandeza de la corte celestial.\n\nVuelven por la serranía los tres Reyes a su Oriente al amanecer del día.",
    "title": "Coplas del portal",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 103,
    "article_char_count_full": 550,
    "article_char_count_review": 550,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-11-8-right-un-disco-sobre-los-cantes-de-ida",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nUn disco sobre los cantes de ida y vuelta\n\nPor: Francisco Vallecillo\n\nEL NOMBRE GENERICO\n\nEn el sentido más generalizado se habla de Cantes de Ida y Vuelta, acaso la denominación más impropia, pues está por demostrar que estos cantes hayan sido realmente de ida; de ida han podido serlo otros, que al menos en su denominación, existen en América, tales el fandanguillo y la caña (Colombia). Más propio parece el nombre de Cantes de Importación, aunque nosotros propondríamos un término semánticamente más apropiado (cantes de adopción), aunque, evidentemente, no resulta muy consonante con el Flamenco. Tampoco el nombre de Rebote puede ser suficientemente definitorio y como, en definitiva, el lenguaje lo hace el pueblo y la Real Academia Española admite con magnanimidad rayana en el derroche\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"ORIGEN\"]\n\nntes de adopción), aunque, evidentemente, no resulta muy consonante con el Flamenco. Tampoco el nombre de Rebote puede ser suficientemente definitorio y como, en definitiva, el lenguaje lo hace el pueblo y la Real Academia Española admite con magnanimidad rayana en el derroche manirroto cuantos vocablos se le ponen a su alcance, valga pues el apelativo de Ida y Vuelta por ser de más común uso y, consecuentemente, de más fácil entendimiento. LOS ORIGENES Aunque no fuera más que en sus denominaciones específicas, salvo la colombiana (que no existe en Colombia), resulta evidente que guajira, milonga, rumba y vidalita son canciones existentes en determinados y concretos países, aunque las estructuras musicales no coincidan aquí exactamente. Tampoco puede asegurarse que la voz guajira, femenino de guajiro (campesino blanco) se arregle con una canción determinada: más bien deba definirse como canción campesina por antonomasia, con influencias evidentes de la rumba, el punto y el son cubanos. Blas Vega, uno de nuestros más eminentes investigadores en el reducido campo del flamenco, se refiere a los primeros indicios de popularidad de estos ritmos y canciones, que a mediados del siglo pasado se popularizaron en pliegos de cordel cantados por ciegos; entre otros menos relacionados con el flamenco, el tango americano y Calixto Sánchez. Luis de Córdoba. las décimas nuevas para cantar por el punto de La Habana. (¿Atención a la guajira, de diez versos octosilábicos, que citaremos más tarde, una pura décima o espinela compuesta). Ni los pliegos de cordel ni sus difusores desmienten, con la excepción que antes hemos referido, el origen americano de la guajira y la rumba ni de la milonga y la vidalita. CANTES E INTERPRETES HISTORICOS Colombiana: Por su estructura musical, cabe pensar que se trata de una variante de la rumba, hecha en España. Se ha escrito que es una composición musical de origen indígena, negroide y español, pero entre el f\n\n[ENDING CONTEXT]\n\nresultado de la experiencia, siempre al margen de su motivación fundamental que no requiere de mayores explicaciones, resulta favorablemente, será de esperar que alguien se decida a reactualizar y en determinados casos rescatar del olvido, otras coplas flamencas inusuales en la presente época: garrotín, farruca, incluso serranas, son formas poco cultivadas que están aguardando la inspiración creadora que siempre será válida en estilos de esta libertad de acompasamiento; que los inventos del órgano eclesial y el bongo y el jazz mal se compadecerán siempre con los cantes básicos fundamentales.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Un disco sobre los cantes de ida y vuelta",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1404,
    "article_char_count_full": 8761,
    "article_char_count_review": 3582,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "ORIGEN"
      }
    ]
  },
  {
    "article_id": "1985-11-10-left-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "Pensamiento político en el cante flamenco. (Antología de textos desde los orígenes a 1936. De José Luis Ortiz Nuevo). PENSAMIENTO POLÍTICO EN EL CANTE FAMENCO (ANILOGÍA DE LEXIOS desde los orígenes y 1936)\n\nPor José Luis Buendía López\n\nP osiblemente lo que mas esté perjudican- do a la bibliografía flamenca en los últimos años sea el desmesurado afán de publicar mucho y pronto sobre este arte nuestro que tan huérfano ha estado durante mucho tiempo de estudios serios que profundicen en los mil y un entresijos de estas simas de pasión, de esas intimidades sonoras que electrizan a quien las escucha y enervan a los que tenemos la suerte de asomarnos a sus contenidos.\n\nPero la solución no es publicar a la ligera ni «porque sí». De un arte del que se sabe tan poco, y lo poco que se sabe, a veces, está condicionado por tópicos y muletillas de la tradición oral, no conviene abusar como si fuera urgente segar la hierba que crece a su alrededor o imprescindible secar las fuentes de que se nutre.\n\nNo tenga cuidado el amigo Ortiz Nuevo, el cante no se va a acabar por muchos agoreros que le salgan al camino. Desde que Demófilo anunció su defunción ha pasado más de un siglo y todavía caminamos; con más o menos vacilaciones, pero caminamos seguros de que esas raíces son hondas y la semilla profunda; no van a acabar con ellas los pájaros del sendero, a pesar de ser muchos y voraces los que planean de un tiempo a esta parte por los predios del flamenco.\n\nVienen estas reflexiones a cuento del libro que hoy reseñamos, del que es preciso señalar que no es ni bueno ni malo, sino sencillamente innecesario y redundante. A estas alturas venir a contarnos que a la Tía Anica la boca le sabe a sangre, que los gitanitos del Puerto fueron los más desgraciaos o que los ingleses se llevaban las ganancias de los pobreticos mineros, viene a ser como descubrir de nuevo la pólvora y anunciarlo en los periódicos.\n\nConocemos la brillante trayectoria investigadora de Ortiz Nuevo, y lo que es más importante, su eficaz divulga-ción periodística (en el recuerdo del buen aficionado quedará siempre su libro sobre Pepe de la Matrona). Por eso sólo nos explicamos este título como fruto de un encargo y la urgencia por complacerlo. Una colección popular y de gran difusión como es esta Biblioteca Básica Andaluza, precisaba de ensayos con más peso específico en cuanto a novedad de tratamiento, o en caso contrario, como acertadamente se ha hecho, de obras clásicas, como la re-dición de La Copla Andaluza de Cansino Asséns.\n\nLa presente obrita sólo se justifica por su subtítulo de «Antología». Pero una empresa de tales características ha de hacerse con un mayor rigor en la selección de las letras y sobre todo, en los comentarios que acompañan a cada grupo de las seleccionadas; pero aquí, pese al interés de los diferentes apartados (presidio, servicio de armas, la mina, la miseria, etc.) dichos aspectos explicativos no pasan de ser un catálogo de los ya comentados tópicos, contenidos en libros de fácil adquisición sobre la misma materia (Félix Grande, Manuel Urbano, Belade y Gelardo, etc.) y que el autor, con la honradez intelectual que siempre le ha caracterizado, no se olvida de citar en la bibliografía que acompaña a tan exiguo texto.\n\nPor lo tanto ni bien ni mal, como deciamos al principio. Solo escaso para las posibilidades investigadoras y divulgadoras de su autor, que, nos consta, no ha escatimado nunca esfuerzos para el mejor conocimiento de esta materia flamenca que lleva cogida al alma.",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 604,
    "article_char_count_full": 3507,
    "article_char_count_review": 3507,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-11-11-left-acerca-de-la-memoria-una-vez-m-s",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor: María Rosa Fiszbein y Carlos Arbelos\n\nHoy se puede capturar en un disco o cassette y liberarse luego a transistor, eléctrica y hasta electrónicamente. Antes, hace pocos años relativamente, no se quiso hacer masivamente y antes aún no se pudo.\n\nNos referimos a guardar testimonio del cante flamenco.\n\nPor eso, de doscientos años de cante, es mínimo lo que hasta nosotros ha llegado y llega, y máxime, el esfuerzo que hay que hacer para desenterrar a la manera de sufridos arqueólogos, lo que se pueda.\n\n¿Y antes aún de esos doscientos años? Calla, cruel, la prehistoria.\n\nEntendemos que estas pautas, debieran mover a reflexión a los más jóvenes para impulsarles en la noble tarea del rescate que ejemplificó tan no-blemente Antonio Mairena.\n\nCuando se trata de memoria histórica, mal o bien\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\ny llega, y máxime, el esfuerzo que hay que hacer para desenterrar a la manera de sufridos arqueólogos, lo que se pueda. ¿Y antes aún de esos doscientos años? Calla, cruel, la prehistoria. Entendemos que estas pautas, debieran mover a reflexión a los más jóvenes para impulsarles en la noble tarea del rescate que ejemplificó tan no-blemente Antonio Mairena. Cuando se trata de memoria histórica, mal o bien se la ha dejado escrita; mal o bien el hombre fue dibujando sus mapas perfectos o imperfectos y las músicas escritas en partituras no se han perdido, pero la música popular —letra y compás— es rara y poca la que ha llegado hasta nosotros, si tenemos en cuenta que el hombre ha cantado y ha hecho música siempre, desde el principio de los tiempos. MEMORIA DEL FLAMENCO Trasladándonos al terreno del cante jondo topamos con todos los inconvenientes y muy pocas y sutiles pistas. El flamenco, nacido como arte popular de minorías, dos «buenas» razones como para ser ocultado y perseguido o ignorado, no se ha escrito nunca en partituras y sólo muchos años después —casi providencialmente— comenzaron a recogerse sus letras. Lo único que quedó como vehículo leal, por lo menos en la intención de sus recoge\n\n[ENDING CONTEXT]\n\ny prueba de agradecimiento, a la vez que destinando esto mismo a esos jóvenes artistas, queremos recordar que los milagros son a veces también de «ida y vuelta», de «toma y daca».\n\nEl público que escucha con delectación a los «Ultimos de la Fiesta» cree vivir un milagro en el tiempo. Ellos, los artistas del espectáculo también, pero en el espacio, cuando al final de su vida de lealtades al cante, se ven incorporados dignamente a un escenario y no cantándole por un mendrugo, al señorito de turno.\n\nBar TOMAS\n\nAPERITIVOS SELECTOS Especialidad en PLANCHA\n\nMesones, 18 Teléfono 23 40 46\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Acerca de la memoria una vez más",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 1128,
    "article_char_count_full": 6609,
    "article_char_count_review": 2831,
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
  }
]
```
