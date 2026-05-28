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
    "article_id": "1993-03-25-right-todos-los-festivales-flamencos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAlmería\n\nAgosto\n\nMartes 24.—ALMERIA. XXVII Festival Flamenco (1). Pendiente de programación. La Alcazaba. Organizan el Ayuntamiento y la Peña El Taranto.\n\nMiércoles 25.—ALMERIA. XXVII Festival Flamenco (y II). Pendiente de programación. La Alcazaba. Organizan el Ayuntamiento y la Peña El Taranto. Septiembre\n\nViernes 3.—ADRA. XXI. Festival de Cante Grande. El Cabrero, José Zorroche, María del Mar Berlanga, Paco del Gastor y Manolo Franco. Caseta Municipal. Organiza el Ayuntamiento.\n\nSábado 11.—DALIAS. Festival de Cante Flamenco. Pendiente de programación. Piscina Municipal. Organiza el Ayuntamiento.\n\nCádiz\n\nAgosto\n\nSábado 7.—PUERTO REAL. Final del XVII Festival del Concurso de Livianas. Calixto Sánchez y finalistas del concurso. Pabellón Municipal. Organiza la Peña Flamenca de Canalejas de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"memoria\"]\n\nniza el Ayuntamiento. Sábado 11.—DALIAS. Festival de Cante Flamenco. Pendiente de programación. Piscina Municipal. Organiza el Ayuntamiento. Cádiz Agosto Sábado 7.—PUERTO REAL. Final del XVII Festival del Concurso de Livianas. Calixto Sánchez y finalistas del concurso. Pabellón Municipal. Organiza la Peña Flamenca de Canalejas de Puerto Real. SAN FERNANDO. Final del III Certamen Nacional de Cante Flamenco Isla de San Fernando. Dedicado a la memoria de Camarón de la Isla. Pansequito del Puerto, Rancapino, Chano Lobato, José Parra, Tomatito y Moraito Chico. Organiza Tertulia Flamenca de la Isla y patrocina el Ayuntamiento. Miércoles 11.—PUERTO SERRANO. Festival de Cante Flamenco. El Cabrero, Calixto Sánchez, La Susi, Paco del Gastory Pedro Bacán. Caseta Municipal. Organiza el Ayuntamiento. Octubre Sábado 23.—ALGECIRAS. V Palma de Plata. Homenaje a Fernanda y Bernarda de Utrera. El Cabrero, José Menese, José de la Toasa, Miguel Funi, Inés y Luis, Paco del Gastor y Pedro Peña. Cine Florida. Organiza la Sociedad de Cante Grande. Noviembre Sábado 6.—JEREZ DE LA FRONTERA. XVI Noche flamenca (I). Homenaje a Fernando Gávez. Organiza Peña Buena Gente. Sábado 13.—JEREZ DE LA FRONTERA. XVI Noche Flamenca (II). Homenaje a Manuel Valencia, «El Diamante Negro». Organiza Peña Buena Gente. Sábado 20.—JEREZ DE LA FRONTERA. XVI Noche Flamenca (y III). Homenaje a Miguel Bernal Gavira, «Canalejas de Jerez». Organiza Peña Buena Gente. Córdoba Agosto Jueves 5.—AGUILAR DE LA FRONTERA. XIII Noche Flamenca. Curro Malena, José Mercé, Carmen Linares, Elu de Jerez y el Callí. Al baile: «Al Compás de Utrera». Al toque: Niño Carrión y Paco Cortés. Sábado 7.—LA RAMBLA. XVIII Festival Botijo Flamenco. El Cabrero, Curro Malena, Miguel Vargas, El Calli, Inmaculada Aguilar y su cuadro flamenco, Paco del Gastor y Manuel de Palma. Caseta Municipal. Organiza la Peña Flamenca La Bulería y colaboran el Ayuntamiento y firmas comerciales. 23,00 horas. BAENA. XVII Salmorejo Flamenco. Pendiente de programación. Salón de Verano. Organiza la Peña Flamen\n\n[ENDING CONTEXT]\n\nde Flamencología y Estudios Folklóricos Andaluces, correspondiente a 1979. Manuel Ríos Ruiz ha escrito sobre su arte: «Enrique de Melchor, dentro del panorama guitarristico-flamenco actual, es una indiscutible primerísima figura en sus diversas facetas: compositor, solista y acompañante. Las composiciones melchorianas tienen la virtud de la justeza, son piezas bien medidas en su duración, para que el tema o el leitmotiv no se diluyan, ni tampoco resulte reiterativo, sino para que se quede en bien lo percibe como una impresión sencillamente inolvidable».\n\nTOCAORES DE HOY\n\nEnrique de Melchor\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Todos los festivales flamencos",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "25-27",
    "page_number": 25,
    "word_count": 2414,
    "article_char_count_full": 16566,
    "article_char_count_review": 3670,
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
  },
  {
    "article_id": "1993-03-26-right-qu-date-con-el-cante",
    "article_text_for_review": "De lunes a viernes, de 9 a12 de la noche.",
    "title": "\"Quédate con el Cante\"",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 10,
    "article_char_count_full": 41,
    "article_char_count_review": 41,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-05-3-left-editorial",
    "article_text_for_review": "Editorial\n\nA propósito de otros análisis que nuestra línea editorial hizo del Concurso Nacional de Córdoba, formulamos en ediciones anteriores propuesta de sistematización de la joven historiografía del Flamenco, adicionando a las ya conocidas y comúnmente aceptadas, una etapa postrera que denominamos del «Festival Flamenco». Razonábamos tal propuesta de que el criterio mantenido para ese acotamiento —hermético, de los cafés cantantes, etc.— fue siempre el grado de recepción en la sociedad del fenómeno flamenco, al que no era insensible el entorno en que se producía y el vehículo de expresión utilizado; y es lo cierto que, desde finales de la década de los cincuenta, el festival se erige como forma generalizada de comunicación y divulgación del Flamenco. A ese auge contribuyeron, de manera decisiva, las aportaciones o subvenciones que las instituciones locales, provinciales y autónomicas, dispensaron a promotores o colectivos de aficionados, más por motivaciones de oportunidad política que por argumentos estrictamente culturales. Así, la afirmación de valores ge\n\nnuinos andaluces, pasaba por el subrayado de aquello que constituye señas de identidad de este Sur jondo, esto es, el Flamenco. Todo ello ha determinado el que se produjera una profusión de festivales durante los últimos diez años, en la práctica totalidad de los pueblos de Andalucía. Festivales promovidos en la mayoría de los casos por gentes sin convicción, desinformadas, que en su imprudente demanda de artistas por afán exclusivo de competir o por provinciano localismo, han reventado al alza el presupuesto de los mismos. Con ello no estamos objetando me-\n\njoras en los honorarios de los artistas, propósito éste que tornamos a reiterar y que, desde cualquier punto de vista, se defiende mejor con una progresión lenta, realista, sin interrupción ni tensiones inflacionistas. Porque lo que se temía que sucediera está ya sucediendo. Passado el fervor de las afirmaciones autonómicas, quebrantado por la crisis, el erario de las instituciones y sobrevalorado el cachet de muchos artistas, los festivales entran en picado, desaparecen poco a poco. En el año pasado no llegaron a celebrarse casi una cincuentena de festivales de los producidos en el 1991. En el presente año, con respecto del año anterior, la regresión, cuando menos, será la misma. Tal vez sea pronto para pronosticar la muerte súbita del festival flamenco, y «la aparición de una nueva fórmula de escenificación», como apuntaba en prensa, hace pocas fechas, Manuel Martín Martín, pero lo que no ofrece la menor duda es que hemos estado asistiendo, acaso irreversiblemente, al cenit de los festivales flamencos, punto tras el cual sólo resta el lento pero imparable descender.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 423,
    "article_char_count_full": 2729,
    "article_char_count_review": 2729,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-05-4-left-silverio-franconetti-aproximaci-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nQuiero ocuparme un poco de la figura de uno de los cantaores que pienso ha sido básico y fundamental en la historia y el desarrollo del arte flamenco: Silverio Franconetti. Figura cantaora y creativa considerada como el gran genio del cante del siglo XIX.\n\nEn el caso de Silverio, como en el de otras grandes figuras, empezamos por encontrar problemas a la hora de fijar la fecha de su nacimiento. Se viene situando en la de 1831, que es la que «Demófilo» da en su libro publicado en 1881 sobre el cante flamenco, y en el que le dedica un apéndice biográfico en el que resalta la figura y la importancia que este cantaor tenía sobre el resto de los cantaores del siglo pasado. Esta fecha es la del 10 de junio de 1831, y se daba el dato de que se había bautizado en la Parroquia de San Isidoro de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"según\"]\n\nonto to —erudito sevillano—, fue a buscar esa partida, y los datos no aparecieron en la citada iglesia por ninún lado. Han aparecido también otras fechas que nos hacen tomarlas en cuenta en cuanto al momento de fijar cuál es la exacta de su nacimiento. 1829 es año a considerar como el de su nacimiento, teniendo en cuenta el dato que refleja su partida de defunción, en la que se señala que muere con 60 años; como es sabido, murió en el 89, luego, según la citada partida de defunción, había nacido en el 1829. También aparece la fecha de 1834 en el certificado de su segunda boda y en el archivo del cementerio. Pero, de momento, mientras no se encuentre el documento fehaciente, tendremos que considerar la del 10 de junio de 1831 como la más fidedigna. Posiblemente la clave genealógica de Silverio está en América. Esta aseveración, aunque pueda parecer un poco extraña, es real. Coincidentemente, a través de unos aficionados que vivían en Argentina, he conseguido algunos documentos de Silverio. Parece ser que algún pariente, a principios de siglo, se marchó a América con un importante lote de documentos que yo, a pesar de haber recurrido a varios amigos, no los he podido conseguir plenamente, aunque sí uno muy importante que nos sitúa históricamente a Franconetti en Sevilla. Este documento corresponde a la partida de casamiento de los padres y está fechado el 7 de diciembre de 1809. Existe una copia de la misma en la Parroquia de Santiago el Mayor, de Alcalá de Guadaira, donde tuvo lugar dicho enlace matrimonial. Con este dato creí que había conseguido algo importante y que, a través de los documentos de la iglesia, iban a salir otras pistas posibles, pero resulta que, con la guerra, esta parroquia desapareció y se perdió completamente el archivo. P or tanto, tenemos el dato de 1809 como año de asentamiento de los Franconetti en Sevilla. Es un matrimonio compuesto por Nicolás Franconetti Chesi, que era soldado en aquel momento\n\n[ENDING CONTEXT]\n\nla reconstrucción moderna. Por otro lado, esta jabera antigua y la rondeña del Negro, también se han conservado en Madrid a través de una línea que parte de Silverio y que corresponde al Chato de Jerez, con discípulo de Silverio y compañero en sus tournés, y que este hombre que murió en Madrid, en sus últimos años enseñó a varios cantaores de Madrid como El Mimi.\n\nHasta aquí, parte de las investigaciones que he venido efectuando sobre la vida y obra de Silverio Franconetti, y que una vez completadas quedarán reflejadas en un libro (*).\n\n(*) Dicho libro actualmente se encuentra ya en prensa.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Silverio Franconetti. Aproximación a su vida y a su cante José",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "3-9",
    "page_number": 3,
    "word_count": 8028,
    "article_char_count_full": 47535,
    "article_char_count_review": 3577,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "según"
      }
    ]
  },
  {
    "article_id": "1993-05-10-left-el-disparate-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nConfieso que en ocasiones me he quedado sobrecogido ante la variada sucesión de registros y temas que aparecen en las letras del cante flamenco.\n\nCon toda justicia se ha insistido en el carácter mayoritariamente trágico y amenazante de la mayoría de ellas, como corresponde a la problemática de unas comunidades primitivas en las que dicho arte se desarrolló, teñidas de desgarros e infortunios sin cuento, en las que el elemento jondo va a actuar como notario de agravios, levantando un acta puntual de los sufrimientos y malos tratos recibidos.\n\nTambién, en algunas de nuestras investigaciones, hemos querido penetrar en los aspectos contrarios, muy frecuentemente olvidados por los analistas, y que hacen referencia a las luces del flamenco en vez de a sus sombras; esto es, bastantes letras en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"grandes\"]\n\nue el elemento jondo va a actuar como notario de agravios, levantando un acta puntual de los sufrimientos y malos tratos recibidos. También, en algunas de nuestras investigaciones, hemos querido penetrar en los aspectos contrarios, muy frecuentemente olvidados por los analistas, y que hacen referencia a las luces del flamenco en vez de a sus sombras; esto es, bastantes letras en las que la alegría rebosa por doquier y en las que se festejan los grandes acontecimientos que actúan como motores positivos en la vida del artista, ya sea la feliz llegada a puerto en la singladura amorosa, la contemplación de una naturaleza bella y equilibrada o, simplemente, hechos familiares gozosos como el nacimiento de un hijo o esa mancha de color que explota de vitalidad en las bodas gitanas. Para cada ocasión el cante viste de negro o se alhaja con colores de alegría. Pero hoy quiero hacer referencia a un aspecto bastante más desaparecido y que tiene que ver con la falta de sentido racional de muchas letras. Normalmente se re- fieren a argumentos de los que catalogábamos como menos trágicos y en los que, por tanto, tienen cabida un tipo de licencias que no cuadran demasiado con la gravedad de temas tan angustiosos como el amor, la cárcel o la persecución, por citar tres grandes núcleos argumentales de dichas letras. Sin embargo, ante la falta de coherencia y aparente trabazón lógica de la poesía flamenca, es preciso estar muy alerta y tomar precauciones acerca de la posible irracionalidad de las letras, que, muchas veces, no lo es tanto, sino que su sentido se nos escapa a causa de la refinada sutileza de los razonamientos en ellas contenidos, y que se vierten en un hermoso juego de alusiones y elusiones que despistan al que no esté muy avezado en los recovecos ambiguos de la poesía popular. Así, yo mismo estuve un largo período de tiempo enajenado por el hermetismo de esta copla en la que se manifiesta un violento rechazo hacia la destinataria: Anda vete, esaboría que el renglón que a ti te falta lo tiene la letanía. Retahíla que se me antojaba la resolución imposible hasta que un día se me hizo la luz, cuando estudiaba la importancia que se concede a la virginidad de la mujer soltera y comprendí que ese «renglón» que tiene la letanía, y que faltaba a esa mujer, es la palabra «virgo», reiteradamente mencionada en la letanía mariana. Pero, pese a los posibles sentidos ocultos de la copla, no cabe la menor duda de la total falta de lógica de muchas de estas composiciones populares, por más que, en muchas de ellas, se exagere el supuesto de curso racional, de forma que todo resulte más que evidente, como en este tanguil\n\n[ENDING CONTEXT]\n\nde vista que el emisor de tan atípicos mensajes, tal y como hacía Rubén Darío en «Tierra Soleares» cuando se enfrentaba a fenómenos ininteligibles exigiendo «exégetas andaluces» para su correcta lectura. Juzguen si no la divertida sentencia con la que cerramos nuestro trabajo y que ya llenaba de perplejidad a Hugo Schuchardt, cuando en 1881 redactara «Die Cantes Flamencos», obra en la que asegura haber oído, transcrita al caló, esta simpática sentencia:\n\nLa fortuna del cabrito no se la dé Dios a nadie: o morir cuando chiquito o ser cabrón cuando grande. Rosario López\n\nTeléfonos (953) 253139\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El disparate flamenco",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 1086,
    "article_char_count_full": 6448,
    "article_char_count_review": 4274,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "grandes"
      }
    ]
  }
]
```
