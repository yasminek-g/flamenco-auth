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
    "article_id": "1980-11-6-right-lo-jondo-en-salvador-rueda",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Manuel URBANO\n\nSi bien se viene aceptando que la influencia cultural del cante flamenco en la literatura y, sobre todo, en las artes, es manifiesta desde el Romanticismo, resulta sorprendente, cuando no paradójico, que su etapa de mayor y mejor difusión pública —la larga crisis de finales del siglo XIX— aparezca en las referencias de los estudiosos como una época vacía de atenciones intelectuales, cuando no de clara hostilidad por parte de artistas y escritores. Algo que, a mi parecer y de ningún modo, se corresponde con la realidad. Como tampoco es cierto ese manido latiguillo de la aversión general de los del noventa y ocho al cante —recordemos, pongamos por ícaso, la afición de Valle Incián y Antonio Machado—, ni, menos aún, que los modernistas se quedasen truncados con una\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"superficial\"]\n\nde atenciones intelectuales, cuando no de clara hostilidad por parte de artistas y escritores. Algo que, a mi parecer y de ningún modo, se corresponde con la realidad. Como tampoco es cierto ese manido latiguillo de la aversión general de los del noventa y ocho al cante —recordemos, pongamos por ícaso, la afición de Valle Incián y Antonio Machado—, ni, menos aún, que los modernistas se quedasen truncados con una Andalucía edulcorada, lumínica y superficial, cuando no estereotipada y panderetera —recordemos la hondura de Manuel Machado, junto a numerosas flamenquísimas páginas de Salvador Rueda, Francisco Villaespesa etc.; anotemos a Santiago Rusinol publicando en «Luz» (1.895) una terrible sátira contra la juerga flamenca y, en 1.922, entre los principales colaboradores del Concurso de Granada—. Sinceramente, se impone recobrar la memoria, sin ella será imposible efectuar el estudio profundo y colectivo que todo el arte flamenco reclama a voz en grito ante la sordera general que han motivado los más distintos intereses, algunos de ellos verdaderamente leoninos. Posponiendo para una nueva ocasión el irrenunciable análisis de las causas que originaron tanto olvido en materia fundamentalísima de la cultura andaluza durante la crucialísi\n\n[ENDING CONTEXT]\n\na Manuel Machado el haber sido, históricamente, el inaugurador lírico del tema flamenco»; añadiendo a renglón seguido y en no muy claro párrafo: «No olvidemos que el malagueño Salvador Rueda (antecesor de tantas cosas) y otros poetas menos importantes de fin de siglo habían dado, con anterioridad al autor de Cante hondo, brillantes notas líricas con sabor flamenco». Estimo, por las fechas mencionadas en este artículo, que la primacía no es del poeta sevillano. Pero pospongamos enojosas prelaciones cronológicas, la cultura no se mide por su punto de salida, sino por su meta de llegada.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Lo jondo en Salvador Rueda",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "6-8",
    "page_number": 6,
    "word_count": 1854,
    "article_char_count_full": 10992,
    "article_char_count_review": 2871,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "superficial"
      }
    ]
  },
  {
    "article_id": "1980-11-8-right-una-broma-de-mal-gusto-dada-en-1",
    "article_text_for_review": "Allá por el año 1905 cuando se deshizo la compañía de Fernando el de Triana, Sebastián Muñoz Beigveder, «El Pena», cantaor de primerísima categoría por malagueñas como está mandado en un hijo de Alora, fue contratado por Ignacio Maroto y su esposa Trinidad Navarro Carrillo «La Trini» y diariamente actuaba en el Ventorrillo que éstos tenían en La Caleta.\n\nPor M. Yerga Lancharro\n\nUn mal día La Trini y Sebastián se disgustaron y el cantaor quedó despedido, motivo por el cual regresó a Alora donde estableció una Taberna.\n\nSebastián sabía que entre el matrimonio Maroto-Navarro y don Antonio Chacón existía una muy buena amistad, tanto es así que raro era el año que el maestro jerezano no visitaba a sus amigos en La Caleta.\n\nNuestro Sebastián pensó cómo gastar una broma a La Trini y concibió la idea de enfrentar a ambos artistas, idea que no tardó en poner en práctica.\n\nSebastián, en compañía de su inseparable guitarrista Joaquín Rodríguez el hijo del ciego, se trasladó a Barcelona donde grabó varias caras de discos (caras de discos se decían entonces). En una de ellas —y he aquí la broma— Sebastián dice antes de iniciar el cante: «Cartageneras de Chacón cantadas por el Pena, acompañado a la guitarra por Juaquinillo el hijo del ciego» y lo que grabó fue un cante de La Trini.\n\nComo es natural los profesionales contemporáneos al escuchar el disco se dijeron sin previa averiguación: «¿Cómo es posible que Chacón se atribuya un cante cuya creación sabe todo el mundo que es de La Trini?».\n\nAl tener conocimiento la artista malagueña de tal atribución, localiza un ejemplar del disco y al escucharlo, tanto ella como Ignacio montaron en cólera y se pusieron en contacto con don Antonio Chacón, quien al ser informado quedó sorprendido por la osadía de Sebastián y se llevó un enorme disgusto. Así me lo relató su ahijada Ana Ariza. El matrimonio Maroto-Navarro quedó convencido de la inocencia de don Antonio y cargaron sobre Sebastán todo el peso de su ira. Este viéndose acosado se obligó públicamente a ordenar a la casa discográfica «La Voz de su Amo» que cesara en la tirada del disco, además de comprometerse a retirar del mercado todo ejemplar que llegase a sus manos. Esto era irrealizable, era utopía.\n\nPor fin La Trini y su esposo dieron por concluso el pleito cuando quedaron convencidos de que todo había quedado aclarado ante la afición y principalmente ante los profesionales que era su mayor preocupación.\n\nPero la broma perdura hasta nuestros días. El pasado año dijo un cantaor ante un numeroso público: «Señores, voy a cantar para uste-des unas cartageneras de Chacón». Al terminar su interpretación no hubo ni un solo aficionado que se percatase del error, por lo que una vez terminado el espectáculo localicé al artista y me atreví a decirle que lo que había cantado no era cartagenera de Chacón, sino un cante de La Trini. El cantaor, sin más ni más, rechazó estoicamente mi «lección» diciendo que él estaba seguro de haber interpretado un cante de Chacón porque tenía una prueba contundente con la que podía demostrar que el equivocado era yo. (La prueba, señores, no era otra que el disco de Sebastián).\n\nY no termina aquí mi sorpresa. Hace unos meses llegó a mis manos un disco microsurco donde aparece grabada una de las dos «carta-generas de Chacón»:\n\nEn San Antón me prendieron conducido a Murcia fui...\n\nEspero que este escrito sirva para que en el futuro nadie vuelva a picar en el anzuelo que hace muchos años nos puso el bueno de Sebastián el Pena.",
    "title": "Una broma de mal gusto dada en 1905, persiste en la actualidad",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 600,
    "article_char_count_full": 3490,
    "article_char_count_review": 3490,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-11-9-right-mi-encuentro-con-aurelio-en-c-di",
    "article_text_for_review": "Por Alejandro Fernández Cotta\n\nCuando recuerdo el día en que conocí a Aurelio Sellés y contemplo ahora toda esa numerosa cohorte de historiadores, catedráticos y entendidos en flamenco, me resulta difícil no pensar que en este asunto, como en tantos otros, no estemos también asistiendo al reinado —ojalá sea efímero— de lo superficial y advenedizo. A la vuelta de cualquier esquina, te encuentras hoy con una autoridad en la materia.\n\nNo conservo en la memoria el año exacto. Debió ser allá por el 52 o el 53, cuando fui a Cádiz, ya entrada la primavera, en compañía de un tío mío, Juan Cotta, hermano de mi madre, en cuyo bufete de Abogado estaba yo haciendo mis primeras armas.\n\nEra éste una persona, una personalidad, de aquellos tiempos. Amante incansable del arte flamenco, profundo conocedor de todos los secretos del cante y la guitarra —aunque fuera incapaz de entonar la más sencilla frase—, de él aprendí lo poco que sé sobre ese misterioso arte, que siempre me ha parecido tan complejo e inabarcable. Todavía no alcanzo a explicarme cómo podía reconocer la más disimulada falseta, la más desapercibida inflexión de la voz, y juzgar en ellas su pureza de ejecución o la creación del artista.\n\nLlegados a Cádiz en el tren de la mañana, pudimos resolver en menos de una hora el asunto profesional que allí nos llevaba, y nos encontramos sin nada que hacer hasta la hora del tren que nos traería de vuelta a Sevilla. En vista de lo cual, mi tío me propuso que intentáramos localizar a Aurelio, «para echar un rato con él y que nos cantara alguna cosa».\n\nAquello me pareció descabellado. No el buscarlo y charlar, porque sabía por referencias que era un buen conversador, sino que nos pudiera cantar a esa hora, a las once de la mañana, y completamente en frío. Pero cuando se lo hice notar, me contestó:\n\n—No hombre. Ya verás. Nos tomamos un café en cualquier parte que a él le guste, empezamos a hablar, y ya verás como se anima.\n\nPor fin, después de varias averiguaciones y recados en uno y otro sitio, vino Aurelio. Vestía con modestia y con una gran corrección en su porte. Más bien enjuto, en su rostro se apreciaban esos rasgos de nobleza y elegancia que pronto se hicieron proverbiables.\n\nDespués de preguntarle si prefería que fuésemos a algún sitio determinado, nos indicó una venta situada en las afueras de Cádiz, un poco más allá de lo que hoy es el Hotel Playa, y allí nos encaminamos en un coche de caballos.\n\nLa venta era como tantas otras. El cuartito en el que entramos, también: Gruesos muros blanqueados, una mesa camilla y un ventanuco a la medida. Estábamos los tres solos, y allí nos sirvieron un lento y venerable «café de maquinilla», aquel que preparaba el paladar goteando ceremoniosamente ante los ojos.\n\n¿De qué hablaríamos en aquellos primeros momentos? De su vida; eso sí lo recuerdo. Pero ¡Qué lástima de palabras perdidas! De aquel momento solamente conservo el rastro de su aroma, y su lección de humildad, que es la que me mueve a contarlo:\n\nHabría pasado algo así como una media hora, cuando mi tío le pidió, como quien no quiere la cosa, que le cantase una «alegría», y precisamente la suya, la que comienza, rompiendo por arriba:\n\n«Estoy etico de pena, Nadie se arrime a mi cama, que estoy etico de pena».\n\nRepito que serían las doce de la mañana, que estábamos tres personas solas, sin guitarra y con un café por delante, que él era AURELIO, y que no se encontraba con nosotros cumpliendo ningún compromiso, sino como amigo. ¿Alguien piensa que se negó o se resistió? Rezongó un poco, esa es la verdad —¡Hombre, Juan, mira que las cosas que se te ocurren...!—, pero en seguida carraspeó, se entonó, y la emprendió con el cante. ¡Aquella voz suya! ¡Aquella manera suya...!\n\nY aquí vino mi sorpresa. Porque no había terminado siquiera la primera estrofa, cuando mi tío, levantando su grueso dedo índice, le interrumpió bruscamente: —No, no y no.\n\nAnte tan —a mi modo de ver— desconsiderada intervención, y sabiendo lo «delicados» que son muchos artistas y lo que gustan de acentuar esa nota, casi di por seguro que mi pequeña historia iba a terminar en aquel mismo instante.\n\nNo fue así. Aurelio, con la mayor naturalidad del mundo, se concentró, volvió a entonarse y comenzó de nuevo. Esta vez su intervención no llegó tan lejos. No pudo pronunciar más que el segundo verso, cuando se vio interrumpido. Pero mucho más corto fue su tercer intento, ya que se vio silenciado en cuanto dijo «etico de pena».\n\nLa escena duró poco más. Aurelio había aceptado aquel desafío. Y lo había aceptado —pude darme perfecta cuenta de ello— con la satisfacción de que tenía frente a él a un Juez digno de su arte.\n\nVolvió a la carga y lo intentó un par de veces más, sin que recuerde su número exacto; pero ahora le entraba a la dificultad por derecho, suprimiendo la palabra «estoy», de modo que salía diciendo «eticio de pena», y ahí se paraba, ya sin necesidad de que mi tío se lo indicase.\n\nHasta que al fin, a la cuarta o quinta vez, aquel dedo índice se abatió con un jahora, ahora!, que, con entusiasta admiración, terminó cortando el cante en la misma palabra en la que antes lo había interrumpido con su disconformidad.\n\nY eso fue todo. Como si ambos se hubieran puesto de acuerdo, la sesión de cante se dio por concluida con ese único verso, con esas únicas tres palabras, para volver, sin más explicaciones, a recoger el hilo de la tranquila charla del principio.\n\nNi entonces, ni nunca, le he escuchado cantar a Aurelio esa alegría por entero. Ni se lo habría pedido, además, no ya por no man-char la pureza de un recuerdo tan original, sino porque todavía me siento algo avergonzado de no haber podido captar los matices que aquel día trazaron la frontera entre lo bueno y lo mejor.\n\nExiste hoy mucho bueno. ¿Pero existe lo mejor? ¿Existe la humildad suficiente, el sacrificio suficiente, que Aurelio quiso mostrarnos, para alcanzar la mayor grandeza del arte flamenco?",
    "title": "Mi encuentro con Aurelio en Cádiz",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 1036,
    "article_char_count_full": 5911,
    "article_char_count_review": 5911,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-11-10-right-las-letras-flamencas-de-fernando",
    "article_text_for_review": "MARTINETES\n\nYendo yo en la conducción de Carmona pa Jeré, en llegando a Las Cabezas se me escollaron los pies. Ven p'acá, mira p'alante y dime lo que estás viendo. La maldá, que no se acaba, y el tiempo que se va yendo. No impresas aún, forman parte del espectáculo «Andalucía en pie», estrenado en el teatro «Lope de Vega» de Sevilla el 13 de Noviembre de 1980, dirigido por José Tamayo, con libreto de Fernando Quiñones y música de Juan A. Castañeda y José Torregrosa.\n\nSIGUIRIYAS\n\nUn mar de tormenta me jierre en la sangre. Como reniego de las maldaitas / que en el mundo miro y no me oye nadie. La aguja y el hilo llevan la puntá como me llevas pa un lao y pa otro de tu voluntá. Capilla del Cristo en rayando el día, yo me sentaba en su murallita pensando en la mía. Reniego de tí y del diña cuatro Ágosto, prima, que te conoci.\n\nSOLEARES\n\nVuélvete p'al otro lao y da una cabeza que ya tu gusto has lograo. Tú me has cogió en la horita tonta y ya tu gusto has lograo. La gente por las esquinas estaba quieta y callá. Sogas pa los pensamientos no se han podió inventá. Mandá que mande el que quiera, que yo con mandá en mí mismo estoy de la parte 'afuera.\n\nTanto mandá, tanto mandá, han puesto al mundo según está.\n\nBULERIAS\n\nPlacita de Las Palmeras, Jaén de mi corazón, Madalena, El Cadiato, Taberna del Gorrión.\n\nYo vendía con mi hermano naranjitas en invierno, caballitas en verano.\n\nNo me llames la atención que es que estoy ya más loquito que el relojito de la estación.\n\nJunto a la pescadería me miraste y me dejaste cavilando pa tó el día.\n\nHoy tó aquel que s'escantilla, más ligero que en el cine sin quitarle los zapatos le mangan los calcetines.\n\nAyé fuí al teatro, no me gustó ná; pa teatro, las que arma en mi calle Pepí La Pirá.\n\nFANDANGOS DE HUELVA\n\nCallejón de los Tumbaos es el que tiene mi Huelva, Callejón de los Tumbaos, que el más derecho «laea» al salir p'al otro lao cuando el vino lo marea.\n\nLleva medio mundo encima. Paquillo el de las postales lleva medio mundo encima y toavía no s'ha movió de dos calles y una esquina... Paquillo el de las postales.",
    "title": "LAS LETRAS FLAMENCAS DE FERNANDO QUÍNONES",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 393,
    "article_char_count_full": 2080,
    "article_char_count_review": 2080,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-11-11-left-guitarra",
    "article_text_for_review": "Encantado matraz. Cuna ambulante de los genios del Sur. Torre sin ley de gravedad, rendida. Ojo de buey sobre el mar telegráfico del cante.\n\nParto siamés. Caminos de bramante por los que vaga errático, Undivé y sacrifican sus picos de carey las palomas del tacto.\n\ndescarnada pupila. Caja huera donde el viento redondo se desliía y ruge ante el serrallo, eunuco y vano,\n\nal ver cómo da a la luz la cuerda austera y cómo se cristiana la armonía en la concha andaluza de una mano.\n\nN. R.⁹- «Candil», excepcionalmente y camo homenaje póstumo, se honra en reproducir este soneto del joven y excelente poeta sevillano recientemente fallecido.\n\nJosé Luis Núñez",
    "title": "Guitarra",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 111,
    "article_char_count_full": 654,
    "article_char_count_review": 654,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
