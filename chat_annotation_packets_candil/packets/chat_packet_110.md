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
    "article_id": "1985-03-20-right-buz-n-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA mi amigo don José Márquez Cabello\n\nHa sido en mi poder la revista CANDIL número 37 y no sabes cuánto me he alegrado al leer tu nombre.\n\nEfectivamente, querido José, existe error al ofrecer a los aficionados, lectores de esta revista, una malagueña como creada por Francisco Lema, cuando todo el mundo sabe que es del Canario.\n\nEstos pequeños errores son inevitable y más cuando, como yo, se hace una relación amplia de cantaores y sus cantes confiando en la memoria.\n\nAunque se trata de pequeños errores, yo le doy bastante importancia, porque puede llevar a la equivocación a cualquier iniciado en el gusto por el arte flamenco.\n\nEn la misma revista leo, en la sección de placas, el nombre de Antonio Renget, cuando es Rengel. También leo mi comunicación a mi amigo Vallecillo y me han cambiado\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"escuela\"]\n\ncosa. Yo titulé mi trabajo así. A francisco Vallecillo Pecino, sobre su trabajo titulado ¿Por qué payo? Y en la revista aparece de esta forma, que ni se le parece: A Francisco Vallecillo Pecino, sobre un tratado ¿Por qué payo? En fin, estos errores de imprenta sí que carecen de importancia. Para justificar el mío, por supuesto que involuntario, te ruego que leas el número 18 de esta revista y verás la clasificación que hice por malagueñas y su escuelas. «Mi vía por aborrecerte», grabada por el «Niño de la Isla», «Niño de las Marianas» y otros. «Después, y no obstante mis gestiones practicadas, no he localizado ninguna otra creada por el «Gallegiño». «Si de tí pudiera vengarme» grabada por la «Niña Romero», «El Mochuelo», Manuel Centeno y otros. Francisco Lema creó tres estilos. ¿Quiénes los conocían? Se conocía solamente uno. Yo ofreció a la afición los tres que conservo en mi archivo. Como asimismo ofreció los tres de La Trini, cuando unicamente se conocía uno. Querido amigo, mal aficionado será aquel que no conozca la malagueña de Juan de Reyes Osuna, «El Canario de Alora». Yerga Lancharro tiene la suerte de conocerlo y otro que mis antepasados se lo atribuían a dicho cantaor perote. Ambos los conservo en mi archivo. Sin intentar darte lecciones, líbreme Dios de ello, te recuerdo que Fosforito nos legó las siguientes letras con estilos distintos: Sepas que me cabe el honor de haber ultimado un amplio trabajo de investigación en el campo de las malagueñas y sus congéneres, bastante más enriquecido que el que publiqué hace unos años. Desde que yo a tí te conocí», grabada por el «Niño de la isla» y otros. Me dices que si soy capaz de cantar una malagueña de «El Canario». ¡Pues claro que soy capaz! ¿Es que, acaso, ignoras que los artistas flamencos, para poder ofrecer todo lo grandioso y su- blime de su arte, tienen forzosamente que cantar de los cincuenta en adelante, que es precisamente cuando tienen la voz hecha y cuando cantan mejor? Aprovecho esta oportunidad para contestar también a lo que me decías, en revista anterior, respecto de la malagueña: «Porque ando me desmayo». Esta malagueña, si es verdad lo que dijo José Muñoz «Pena hijo», que la grabó en Regal con el título de: «Malagueña de la Trini», fue creada por ella\n\n[ENDING CONTEXT]\n\nconfirmación: El tío Borrico de Jerez, Juan de la Plata, Terremoto, Antonio Murciano y Antonio Fernández Díaz «Fosforito» y todo esto por sus cantes desconocidos y únicos, oriundos de Cartagena y La Unión.\n\nBar TOMAS\n\nPero lo que no entiendo es como uno de los testigos presenciales de aquel acto, 17 años después, dice en unas declaraciones en la revista CANDIL todo lo contrario (noviembre y diciembre 1984) contestándole a quien le preguntaba sobre el tema.\n\n¿Señores esto es aclarar entuertos o ampliarlos?\n\nAPERITIVOS SELECTOS\n\nMesones, 18 – Teléf. 23 40 46 Especialidad en PLANCHA\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Buzón Flamenco",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 2775,
    "article_char_count_full": 16127,
    "article_char_count_review": 3885,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "escuela"
      }
    ]
  },
  {
    "article_id": "1985-03-23-right-discograf-a-flamenca",
    "article_text_for_review": "Siempre ha sido motivo de controversia entre aficionados y cantaores la diferenciación de estos cantes, la Taranta y El Taranto, por ser dos estilos de la misma familia de los cantes mineros de levante.\n\nEs difícil escuchar una grabación en la cual no se mezclen estos dos estilos. Por eso que en este LP ni están todos los que son, ni son todos los que están. Nos viene ahora a la memoria intérpretes de cantes mineros como Jacinto Almadén y Canalejas de Puerto Real.\n\nUno de los cantaores que más ha sabido diferenciar estos palos, a nuestro juicio, ha sido «Fosforito».\n\nOtro LP más de la serie Grabaciones Históricas, esta vez, «Por Fandangos». En este disco nos encontramos con intérpretes tan dispares como: Hermanos Toronto, Manolo de la Ribera, «El Chaparro», Pepe El Pinto, Juan de la Loma, Gabriel Moreno, Angel de Alo-ra, Antonio Piñana, «El Sordera», Pepe de la Matrona, Flores «El Gaditano», Curro de Utrera y Antonio Ranchal.\n\nNo entendemos los criterios que ha seguido Hispavox en la selección de artístas para esta grabación; carente, en muchos casos, de jondura, que poco pueden aportar al verdadero aficionado.\n\nComo dice José Blas Vega, realizador de esta grabación: «Los ecos del cante jerezano siguen con crespones negros. Primero el Sernita, luego Terremoto, ahora El Borrico, Estamos en la hora de las reflexiones, de los reconocimientos, del recuerdo. Y este recuerdo nos lleva a Jerez». De Jerez escuchamos esta hermosa voz de «El Borrico» en Bulerías por Soleá, Soleares, Siguiriyas, Alegrías, de nuevo Bulerías por Soleá, Tangos, repitiendo por Soleares, Bulerías y Siguiriyas. Cantes hechos con esa jondura que sólo los privilegiados peseen, y Tío Gregorio no cabe duda que lo era.\n\nCon el Vapor de mi aliento empaño yo los cristales...\n\nEn líneas generales, ésta es una grabación aceptable, donde sobresalen las interpretaciones de Antonio Mairena, Juan Varea, Antonio Piñana y Pepe «El Culata».\n\nTITULO: Por Tarantas y Tarantos. SERIE: Grabaciones Históricas. CANTAN: Antonio Mairena, Gabriel Moreno, Barnarda de Utrera, Chocolate, Chiquitos de Algeciras, Pepe de la Matrona, Antonio Piñana, Pepe Pinto, Curro de Utrera, Pepe El Culata y Juan Varea.\n\nREFERENCIA: Hispavox (50) 150 103.\n\nCreemos que esta casa grabadora tiene «Master», «Por Fandangos» con más entidad que el que ahora nos presenta.\n\nPor ser el Fandango el cante más popular de todo el árbol genealógico flamenco y, «la voz más directa al pueblo», este LP siempre tendrá aceptación entre un público menos exigente.\n\nTITULO: Por Fandangos. SERIE: Grabaciones Históricas. REFERENCIA: Hispavox (50) 150 104.\n\nDOSCANDIL\n\nEstamos ante una obra que no debe faltar en la discoteca del aficionado más exigente.\n\nTITULO: Homenaje a Tío Gregorio «El Borrico de Jerez». SERIE: Grabaciones Históricas. CANTA: «El Borrico de Jerez». TOCA: Paco Cepero. REFERENCIA: Hispavox (50) 150 105.",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 457,
    "article_char_count_full": 2868,
    "article_char_count_review": 2868,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-05-3-right-editorial",
    "article_text_for_review": "Los responsables de la programación andaluza de T.V.E., han decidido suprimir, durante los tres meses del estío, los espacios donde tenían una exigua acogida los temas flamencos. Precisamente, en un tiempo en el que toda la geografía cantaora se estremece y raro es el lugar, importante o desconocido, que no organiza su propia comunicación flamenca. La noticia, que por sí sola pudiera reputarse irrelevante, se conecta a una larga cadena de despropósitos y de carencias de sensibilidad, que merecen, a nuestro juicio, un enérgico comentario.\n\nNo es este el momento de enjuiciar el tratamiento que tanto a nivel estatal como, especialmente, en nuestra comunidad autónoma, T.V.E. ha dado al flamenco. Hay que huir de las descalificaciones globales y de los análisis generalizadores; aun a riesgo de ello, nos atrevemos a afirmar que si bien han existido producciones de innegable categoría, el balance final es claramente negativo. Todo se reconduce, a nuestro entender, a un problema de captación, T.V.E., salvo las puntuales excepciones a que hemos hecho referencia, sólo ha captado del flamenco su dimensión exótica, cuando no intranscendente o frívola; en algún sentido, se ha hecho eco de indignas versiones de lo jondo, en la más reciente tradición de folklóricas degradaciones, en las que el grito o la dentellada, se aliviaba con una carcajada de payaso.\n\nLa proyección cultural del flamenco como signo de la especificidad andaluzahah quedado desdibujada, cuando no maltrecha y vilipendiada, como música de fondo de cómicos andaluzados que tan generosa acogida reciben de T.V.E. Por fuerza hemos de resaltar la incalificable actitud de los responsables de la programación andaluzahah. Una decisión de tal naturaleza es impensable en la Comunidad Autónoma de Cataluña o del País Vasco. Si la reivindicación cultural constituye el índice de todo tipo de reivindicación, no sólo quienes contemplamos el flamenco como un genuino fenómeno cultural, sino cualquier persona con un mínimo de sensibilidad social o política, tiene que sentirse zaherido por este tipo de decisiones arbitrarias, absurdas y carentes de todo respeto al pueblo andaluz.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1985-05",
    "year": 1985,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 333,
    "article_char_count_full": 2147,
    "article_char_count_review": 2147,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-05-4-left-el-tema-flamenco-y-2",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor: José Luis Buendía López\n\na poesía es sensible al «duende» desde García Lorca y está abierta al influjo mágico del cante. A nadie se le ocurre ahora fustigarlo, como hizo Eugenio Noël, hace cuarenta o sesenta años. A la antipatía de la generación del 98 ha sucedido una actitud de comprensión y simpatía de los escritores. La rehabilitación ha repercutido también por fortuna en la esfera social. Del prólogo del libro de Ricardo Molina y Antonio Mairena «Mundo y Formas del Cante Flamenco»\n\nLa primera pregunta que surge, inevitable, al abordar el panorama poético andaluz de postguerra, es la siguiente: ¿se puede hablar con propiedad de una poesía andaluza, distinta por sus caracteres formales y contenidos te-máticos al resto de la producción poética del país? Ardua pregunta sobre la que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"guia\"]\n\nrdo Molina y Antonio Mairena «Mundo y Formas del Cante Flamenco» La primera pregunta que surge, inevitable, al abordar el panorama poético andaluz de postguerra, es la siguiente: ¿se puede hablar con propiedad de una poesía andaluza, distinta por sus caracteres formales y contenidos te-máticos al resto de la producción poética del país? Ardua pregunta sobre la que se ha vertido mucha tinta y juicios extremos en la mayoría de los casos, más bien guiados por una concepción visceral del asunto, además de plan-teamientos normalmente extraliterarios, llá-mense grupúsculos, capillas poéticas o radícales enfrentamientos ideológicos, que enturbian la actitud desapasionada con que, creemos, de-be afrontarse el problema. Sin embargo, poco a poco se han ido serenando los ánimos, y las opiniones haciéndose cada vez más científicas y argumentadas, lo que no impide la multiplicidad de las mismas: así, por ejemplo: António Sánchez Trigueros opina: «Espero que no se me tache de chauvinista si afirmo que no creo que pueda haber otra región o lugar del planeta con tanta calidad y cantidad poética como Andalucía», en clara alusión al carácter diferencial de nuestra producción poética. Jorge Urrutia señala que: «Durante la postguerra, la lírica andaluza se mantiene, mas no con la preponderancia dentro de las letras españolas Manuel Ríos Ruiz que es manifiesta en épocas anteriores», en tanto que Luis Jiménez Martos, en su famosa Antología del 1963, Poetas del Sur, rebaja más aún nuestra aportación poética a la producción nacional afirmando que: «tras la guerra de 1936, la veleta de la poesía señalaba Norte», lo que a nosotros nos parece a todas luces injusto si nos fijamos en que, precisamente en una publicación norteña editada por la revista leonesa Espadaña «Antología parcial de la poesía española. 1936-1946», la cuarta parte de los cuarenta y ocho autores representados son poetas andaluces. Por su parte Pérez Ortega, al referirse al mencionado carácter diferencial de esta poesía meridional, afirma con rotundidad: «Lo que sí me parece cierto es que hoy y entre los más jovenes poetas del Sur, no existe una poética que se distinga de la realizada en los otros pueblos de las España, al menos en su aspecto formal». Por nuestra parte afirmaremos, como ya lo hicimos en anteriores trabajos sobre la narrativa andaluz, que preferimos hablar de poesía hecha en Andalucía, o por andaluces, toda vez que la clasificación rigurosa regional, en base a la utilización de rasgos lingüísticos propios del andaluz, parece no poder hacerse por el momento, puesto que nuestro idioma hablado, con sus giros y peculiaridades propias apenas ha asomado a los libros de poemas de nuestra región, salvando algunos tímidos intentos, entre los que cabe destacar la utilización del lenguaje román en el granaño Heredia Maya o el popularismo rural del jiennense Alfonso Hortal, ejemplos estos que tampoco son lo suficientemente completos para hablar de una literatura poética «en andaluz». Pero dejando aparte el carácter andaluz o castellano de estas producciones, lo que nadie puede negar en nuestros días es la aportación riquísima con la que Andalucía ha contribuido al desarrollo de nuestra lírica en los últimos cuarenta y cinco años. Con ello no se hace\n\n[ENDING CONTEXT]\n\nel grupo «El Olivo» en el panorama poético de los últimos quince años. Jaén, 1985.\n\nPEREZ ORTEGA, M. U., Andalucía en el testimonio de sus poetas. Madrid 1976, Antología consultada de la nueva poesía andaluzay (1963-1978). Sevilla, 1980.\n\nSAINZ DE ROBLES, F. C., Historia y antología de la poesía castellana. Madrid, 1967.\n\nSIEBENMAN, G., Los estilos poéticos en España desde 1900. Madrid, 1973.\n\nPolígono «LOS OLIVARES» - Teléfonos 22 30 00 - 22 30 04 - J A E N\n\nDISTRIBUIDOR OFICIAL DE:\n\nVIDRIO LAMINAR DE SEGURIDAD - ACRISTALAMIENTOS EN GENERAL\n\nTRABAJOS DE ALUMINIO PARA OFICINAS Y TERRAZAS\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La poesía andaluzas de postguerra: El tema flamenco (y 2)",
    "periodical": "candil",
    "issue_id": "1985-05",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "4-9",
    "page_number": 4,
    "word_count": 5819,
    "article_char_count_full": 35240,
    "article_char_count_review": 4872,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "guia"
      }
    ]
  },
  {
    "article_id": "1985-05-9-right-esteban-sanl-car",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCompañeros y amigos\n\n(Imagen y anécdota)\n\n(II)\n\nPor: Luis Caballero\n\nPues sí señor, los tiempos han cambiado y mucho; naturalmente no para el gusto de todos ni todo para un mejoramiento general de la vida, pero sí en tantos aspectos como para que no podamos negar —los que conocimos los años 20 y 30— que, entre otros problemas, el del flamenco ha pasado, si no del medievo al renacimiento, sí, al menos, de la indiferencia al reconocimiento, de un bajo concepto vulgar, ordinario y mal visto a un respeto de índole cultural más —aún— minoritariamente elitista que popular (esperemos tener la suerte de que se consolide y amplíe el curso de este giro redentor). Pero háíamos hecho mención al flamenco de allá por los finales de los años 20 y principios de los después trágicos, sangrientos y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"recuerdo\"]\n\ns cortados en la ciudad tan distantes y distintos del modesto drill acatetado. ¡Y aquellas corbatas!, sobre todo las corbatas, ese elegante signo de distinción tan desacostumbrado en los pueblos andaluces de entonces. Me atrevería a asegurar que era lo primero que adoptaban cantaores y guitarristas en cuanto ganaban los primeros cinco duros. Hasta el propio Manuel Torre alternó la corbata con el flamenquísimo pañuelo de cuello. Pues bien; ahora recuerdo cómo una noche flamenca de verano antiguo me quedé prendado de una corbata preciosa y unos elegantes zapatos combinados en blanco y negro. Los lucía un muchacho alto, de pelo perfectamente ondulado y que con chaqueta azul y pantalón blanco, más parecía el capitán de un yate que un tocaor de guitarra. Era Esteban Delgado, Niño de Sanlúcar. Mi gran afición y curiosidad por este atractivo, dramático y picaresco mundo del flamenco profesional, me hacía apurarlo —desde la barrera— hasta el último momento. Aquella noche escuché a Pastora. ¡Aquella Pastora envuelta en su mantón bordado! A Pepe Pinto, recién casado con la de los Peines y, precisamente, peinado —entonces— a lo Rodolfo Valentino; tal vez a Mazaco, con aquel vozarrón por siguiriyas. Escuché de tocar al grueso Antonio Moreno, de contar cosas, con aquella gracia, a Carlos Franco, y..., no sé a cuantos más. Los escuché y los despedí admirado y en silencio mientras se embutían en tres taxis —aquellos taxis neg\n\n[ENDING CONTEXT]\n\nEsteban dijo, como si no hubiese salido de la «Europa»: «Totá, que pa hablá con el Poeta hay que vestirse de Pizarro».\n\nA su lado se sigue viviendo y sintiendo la más genuina Andalucía de los profesionales clásicos del mejor arte flamenco. Más de cuarenta años en América demostrando cómo es su guitarra, la nuestra, esa que «Llora por cosas lejanas», y cómo es el andaluz universal, ese que se enfrenta al mundo sin más armas que su alma, su gracia y su arte.\n\nJ. A. PULPON\n\nESPECTACULOS INTERNACIONALES\n\nO'Donnell, núm. 3-4.º\n\nTeléfís. 22 20 58 - 21 69 20\n\nPARTICULAR:\n\nSEVILLA\n\nTeléfono 27 80 78\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Guitaristas conocidos: Esteban Sanlúcar",
    "periodical": "candil",
    "issue_id": "1985-05",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 1102,
    "article_char_count_full": 6533,
    "article_char_count_review": 3057,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "recuerdo"
      }
    ]
  }
]
```
