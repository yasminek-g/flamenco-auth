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
    "article_id": "1992-05-5-right-leo-aguilar-y-ram-n-porras",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen...\n\nSobrino nieto del que fuera cantaor José Cepero; discípulo de Rafael del Aguila, y, como ellos, tocaor y músico nato. De toque largo, virtuoso y gitano, que revela un extenso conocimiento técnico e interpretativo, con dominio absoluto del compás, como buen jerezano; es un placer escuchar su guitarra, que presta al cante, como decimos, toda la justeza y el sentido de la medida que en cada caso se requiere. Paco Cepero estuvo en la Peña Flamenca de Jaén y rebosó arte y jondura en la noche del viernes 24 de abril, con ocasión de su VII Semana de Estudios Flamencos.\n\n—En primer lugar, Paco, háblanos de tu tío abuelo ʾosé Cepero... —Yo le conocí siendo ya muy pequeño, tendría unos siete u ocho años. Por aquel entonces, se me quedó grabada en la memoria un\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionado\"]\n\nrtizta»... En el transcurso de los años, me parece que estas palabras marcaron mi destino artístico, ya que desde entonces el flamenco y la guitarra hicieron huella profunda en mí. —Cante, toque, baile. ¿Es ese el orden o no debe de plantearse ninguna prioridad? —Me quedo con cualquiera porque ellos son los componentes principales de la denominación «flamenco». Pero, ¡ojo!, que los tres se encuentran en los cá-nones que la establecen... —Como aficionado, ¿con qué te quedas, con el ayer o con el boy del flamenco? —Sin el ayer no existiría el presente. Creo —es mi opinión personal— que el flamenco ha ganado muchísimo en técnica, pero poquísimo en creatividad. La prueba de ello es que la mayoría de los cantaores/tocadores de hoy en día, lo hacen de oído. Yo he estado en Japón y he escuchao a un nativo imitar a Camarón, al Chocolate, a Juan Talega, y lo hacen perfectamente. Eso demuestra que se estudia la técnica. Y eso mismo está pasando aquí. Antiguamente todos los cantaores tenían su sello propio, aunque fuera un fandanguillero: «el de la Calzá» «el Palanca», etc.; hoy en día, salvando a Camarón, q\n\n[ENDING CONTEXT]\n\nquedado en el tintero? Tus vivencias, tus inquietudes, tus proyectos... Te ofrecemos las páginas de «Candil» por si deseas comentar alguna cosa más.\n\n—Sólo daros las gracias por todas las atenciones recibidas y por el bien que hacéis al flamenco; éste que está tan arraigado a nuestra cultura, creo que tanto las Peñas Flamencas como vuestra Revista, hacéis vuestro el objetivo de estudiar, analizar y difundir el Arte Flamenco. También, comentaros que este año vuelvo a tocar en los Festivales Flamencos, pues necesito el contacto con los buenos aficionados. Gracias de nuevo y, ¡hasta siempre!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Leo Aguilar y Ramón Porras",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "5-7",
    "page_number": 5,
    "word_count": 1983,
    "article_char_count_full": 11471,
    "article_char_count_review": 2740,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionado"
      }
    ]
  },
  {
    "article_id": "1992-05-7-right-cr-nica-de-la-vii-semana-de-estu",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJosé Ruiz Pérez «Pepe Polluelas», el artista jiennense que supo aglutinar a un colectivo de aficionados de esta tierra, tuvo el reconocimiento debido gracias a la programación que la Peña Flamenca de Jaén enmarcó en la VII Semana de Estudios Flamencos. Desde un principio parecía que los actos adolecían de la brillantez que suelen darle a este tipo de eventos flamencos la presencia en el programa de las figuras más en candelero de este arte. Sin embargo, conforme se fueron sucediendo los acontecimientos, la semana cultural fue adquiriendo prestancia, calidad y enduendadas noches.\n\nEste humilde cronista abría la serie de intervenciones orales con una sucinta charla sobre la vida, personalidad y dimensión artística del homenajeado. En la misma aludía a cómo se fue haciendo el cantar, allá\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"cercan\"]\n\nen candelero de este arte. Sin embargo, conforme se fueron sucediendo los acontecimientos, la semana cultural fue adquiriendo prestancia, calidad y enduendadas noches. Este humilde cronista abría la serie de intervenciones orales con una sucinta charla sobre la vida, personalidad y dimensión artística del homenajeado. En la misma aludía a cómo se fue haciendo el cantar, allá por mediados de los años treinta, haciéndolo en cortijos y ca- serías cercanas a Jaén. Y cómo posteriormente, merodeando en torno al elevado número de artistas que vivieron en nuestra capital tras el inicio de la contienda nacional, José Ruiz Pérez «Pepe Polluelas», se fue enamorando de los ecos de José Cepero, Pepe Palanca, Pepita Caballero, Antonio «El Sevillano», Pepe Pinto, Canalejas de Puerto Real o Pepe Marchena. O cómo una vez hecho el cantaor, éste adopta una postura bohemia, ci- catera en salir al exterior y apocada en su proyección artística. Y cómo en los últimos años de su vida, el aficionado cantaor aglutina a un grupo de aficionados jiennenses, se cobija artísticamente en la Peña de Jaén y su proyección artística conoce espacios más amplios que los locales. La ilustración cantaora de esa noche estuvo a cargo de Manuel Pérez Mesa «Canalejas hijo», con el acompañamiento de la guitarra de Paco Aguilar, la cual, si bien mantuvo momentos en los que el recuerdo de su padre fue acertado, adoleció de la brillantez que el jiennense suele darle a veces a sus interpretaciones. Todo esto acaecía en la noche del martes, 21 de abril. Pocos estudiosos de nuestro arte poseen actualmente el currículum de investigador sobre el flamenco que arrastra José Blas Vega. Serio, comedido, riguroso y amante de verter la exactitud de los datos, fueron las características más sobresalientes de su intervención en torno al te\n\n[ENDING CONTEXT]\n\nCádiz para abordar los estilos «ida y vuelta», en esta ocasión vidalita por tangos.\n\nY como remate de la noche y de la VII Semana de Estudios Flamencos, el singular baile de Josefa Bastos «Pepa Montes» y su grupo. Tiempo hacía que los jiennenses no teníamos oportunidad de deleitarnos con la composición de figura, la fuerza del taconeo, el sinuoso y arabesco movimiento de brazos y el marcado compás que en esta clausura nos ofreció la sevillana bailaora. En el recuerdo de todos perdurará la calidad de las soleares y las alegrías de la ganadora de los premios «Juana La Macarrona» y «La Malena».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica de la VII Semana de Estudios Flamencos",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "7-9",
    "page_number": 7,
    "word_count": 1197,
    "article_char_count_full": 7252,
    "article_char_count_review": 3435,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "cercan"
      }
    ]
  },
  {
    "article_id": "1992-05-10-left-manuel-cano-andaluz-universal-no",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEl 28 de febrero de 1992 ha sido un grandía para la guitarra flamenca y para el flamenco. En efecto, la Junta de Andalucía otorgó a título póstumo, una medalla de plata al desgraciadamente fallecido Manuel Cano Tamayo, en reconocimiento a su labor de difusión en pro de la guitarra y del arte flamenco. Queremos recordar con el presente estudio la importante labor de investigación y difusión de este andaluz universal. Para ello ofrecemos a continuación un comentario sobre parte de los datos del corpus sobre guitarra flamenca que estamos elaborando, datos que nos servirán a recordar y situar en su justo contexto a una de las figuras señeras de la guitarra flamenca.\n\nDiscografía\n\nLa forma más utilizada actualmente por los músicos para dejar constancia de su arte, sigue siendo el disco (aunque\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"Guía\"]\n\norando, datos que nos servirán a recordar y situar en su justo contexto a una de las figuras señeras de la guitarra flamenca. Discografía La forma más utilizada actualmente por los músicos para dejar constancia de su arte, sigue siendo el disco (aunque el vídeo está ocupando cada vez más espacio). En una entrevista al diario «Ideal» (1), Manuel Cano manifestaba que había grabado 17 Lps. Hemos podido localizar 33 grabaciones, ayudándonos de la «Guía del Flamenco», de Arcadio de Larrea, y del libro de Jiménez Díaz (2). En ellos encontramos grabaciones en solista, en dúo, colaboraciones en antologías, acompañamientos a cantaores, recitador y soprano. Si la mayoría de los temas son composiciones propias, hecho habitual en los guitarristas flamencos, hallamos también con arreglos de canciones andaluzas (tonadas) o composiciones de músicos españoles (Barrios, Pedrell) u otros guitarristas (Ramón Montoya), así como las canciones andaluzas rescatadas y armonizadas por García Lorca. Bibliografía A nuestro juicio, Manuel Cano es autor del libro más completo realizado hasta ahora sobre guitarra flamenca. Se trata de «la guitarra, historia, estudios y aporta- ciones al arte flamenco» (3), imprescindible para todos los que deciden emprender investigaciones sobre la historia de la guitarra flamenca. Compendio de todas las investigaciones que realizó sobre el instrumento a lo largo de su vida, él mismo comentó los seis capítulos del libro en un artículo de la revista «Sevilla Flamenca» (4). Destacaremos que ofrece la originalidad de incluir varias partituras y dos cintas con grabaciones comentadas de su colección particular de discos de pizarra. ¡Hecho poco frecuente, por no decir casi inexistente, en las publicaciones sobre flamenco! Echamos de menos una sola cosa: la falta de un estudio sobre los guitarristas de la escuela moderna. (El análisis de Cano cubre desde los primeros guitarristas hasta Sabicas). Notoria es la participación de Manuel Cano en congresos y otros actos de esta índole, aportando siempre documentadas y reflexivas comunicaciones y participando activamente en la organización de los mismos. Consultando la documentación que dispone sobre los congresos la Fundación Andaluza de Flamenco, hemos podido localizar los trabajos del guitarrista (5), y en las actas, vista reflejada su presencia en diferentes mesas. También participó Manuel Cano en otros actos como el primer congreso de folk-lore andaluz celebrado en Granada en 1986, o en la conferen- Manuel Cano, congressista cia internacional sobre flamenco celebrada en Jerez en 1988 (6). Si bien sus trabajos están centrados en la aportación de la guitarra en el flamenco, también estudia aspectos históricos de la música popular andaluza o reflexiona sobre el estado actual del arte flamenco, desde un punto de vista guitarristico. No obstante, sus ideas y las conclusiones que emanan de sus investigaciones se verán\n\n[ENDING CONTEXT]\n\ntitulada «Un siglo de la guitarra granadina» y realizada por la Caja de Ahorros de Granada. Dada la ya señalada escasa producción bibliográfica sobre guitarra flamenca, animamos a los responsables a realizar las oportunas reediciones.\n\nPara terminar, nos congratula-mos por la decisión acertada de la Junta de Andalucía, que entregó, a título póstumo, el 28 de febrero de 1992 una medalla de plata a Manuel Cano. El pertenece a este grupo de andaluces universales que entregaron y entregan su vida a la dignificación y reconocimiento de la guitarra andaluza, y encarna el mejor ejemplo a seguir.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Manuel Cano, andaluz universal Norberto",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "9-11",
    "page_number": 9,
    "word_count": 2946,
    "article_char_count_full": 18569,
    "article_char_count_review": 4531,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "Guía"
      }
    ]
  },
  {
    "article_id": "1992-05-12-left-el-taranto",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTaranto de Oro\n\nA lcalá Venceslada en su Vocabulario andaluz define la voz taranto como «natural de la província de Almería», y recoge también la frase «ese es un taranto que vino de minero y hoy está rico». En el Diccionario Enciclopédico Ilustrado del Flamenco: «Taranto, [de taranta] cante similar a la taranta, o modalidad de ella, de la que se distingue por obedecer su toque a la tendencia acompasada de la zambra, ejecutado en el mismo无一no que la taranta y la cartagenera // 2.° baile flamenco que se acompaña del cante del mismo nombre». Y usamos parte de esta definición, ante la omisión que para esta voz tiene el D.R.A.E. que registra la de taranta como «canto popular propio de los mineros». Hay otra cita que relaciona al taranto con las minas. La escritora almeriense Colombine, en su\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Granada\"]\n\nbra, ejecutado en el mismo无一no que la taranta y la cartagenera // 2.° baile flamenco que se acompaña del cante del mismo nombre». Y usamos parte de esta definición, ante la omisión que para esta voz tiene el D.R.A.E. que registra la de taranta como «canto popular propio de los mineros». Hay otra cita que relaciona al taranto con las minas. La escritora almeriense Colombine, en su novela En la sima, habla de los tarantos como mineros de Almería y Granada que llegaban a Linares para trabajar en las minas. Saltando estas definiciones, no existen hasta ahora otras pruebas que avalen documentalmente la existencia del taranto como estilo del siglo XIX, ni apenas motivos o razonamientos válidos para su formación y desarrollo, y esto se aprecia leyendo a los teóricos del tema, donde aparecen las dudas del confusionismo, mezclando geografías, equivocando variantes y estilos, y aportando interpretaciones fuera de los tecnicismos y de la realidad artística. Hoy en día, cuando el taranto es ya todo un estilo, consolidado dentro del fascinante y maravilloso mundo del cante y del baile flamenco, queremos contribuir a precisar algunos detalles que ayuden a dar valor a su sentido histórico y a su formación artística. En varias ocasiones ya hicimos un rastreo sobre luces y sombras del taranto, confirmando en su génesis la relación taranta-taranto como «cante de Almería», y para su base musical señalamos ahora como documento vivo una antigua grabación como fuente primitiva de esta relación. Se trata del disco grabado por don Antonio Chacón en 1913, acompañado a la guitarra por Ramón Montoya (Gramphone 3-62.359), y que bajo el título de Minera 1, canta la siguiente letra: Qué madrugá madrugar y trabajar subir y bajar la cuesta a mí me dan poco jornal eso no me trae cuenta a la mina no voy más. e esta grabación señalamos ve rias características: no es un estilo habitual, cuyas cadencias melódicas, de procedencia\n\n[ENDING CONTEXT]\n\nla temporada de 1941-1942, con un espectáculo de quince números, y teniendo como empresario al famoso Sal Hurok. El número nueve de la segunda parte decía: El taranto, Carmen Amaya, figurando como autor de la música Sabicas.\n\nTambién el recuerdo para los pioneros del nuevo cante por taranto: Manolo Manzanilla, Leonor Amaya, Juan Varea, Antonio Mairena, Jarrito, Gabriel Moreno..., y alguien definitivo, Fosforito, que a raíz de su triunfo total en el Primer Concurso Nacional de Córdoba de 1956, marcó una nueva línea en la atención y escucha de cantes poco habituales. El taranto le debe mucho.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El taranto",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1041,
    "article_char_count_full": 6127,
    "article_char_count_review": 3552,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "Granada"
      }
    ]
  },
  {
    "article_id": "1992-05-13-right-los-pasos-del-maestro-silverio-e",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nN o existe más documenta- ción ni encontramos otros datos, que los que nos proporciona la prensa de aquella época, sobre la gran afición al arte flamenco que despertara desde su aparición por Córdoba el gran cantaor Silverio Franconetti.\n\nEmpecemos por analizar qué afición había al Flamenco, anterior a la llegada de Silverio.\n\nEn Córdoba el Flamenco empieza por calar en el pueblo y en la burguesía a través de la guitarra. ¿Que cómo fue? El movimiento musical del pasado siglo XIX tuvo como pilares: El «Círculo de la Amistad», que junto al Liceo Artístico y Literario constituyeron una sola sociedad: el Real Centro Filarmónico, las escuelas Pías, entre otras instituciones que organizaban conciertos y en cuyos programas se incluía constantemente a la guitarra.\n\nEntre los guitarristas asiduos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"escuela\"]\n\novimiento musical del pasado siglo XIX tuvo como pilares: El «Círculo de la Amistad», que junto al Liceo Artístico y Literario constituyeron una sola sociedad: el Real Centro Filarmónico, las escuelas Pías, entre otras instituciones que organizaban conciertos y en cuyos programas se incluía constantemente a la guitarra. Entre los guitarristas asiduos a estos conciertos está Julián Arcas, que en un mismo año dio dos conciertos, el primero en las escuelas Pías y el segundo en el «Círculo de la Amistad», ocurriendo esto en el año 1858. Destaca la prensa local, que llamaba poderosamente la atención, su interpretación junto a piezas clásicas y de música popular, sus «soleares» y «peteneras». Al no existir profesionales, ni haber una comunidad gitana significada en esta afición, son pocos los datos sueltos que nos llegan de actos flamencos. Pero sí ocurre un hecho significativo y que queremos resaltar. Tuvo lugar el 15 de septiembre del año 1862 con motivo de la visita que la reina Isabel II hace a la ciudad, dentro de su recorrido por Andalucía en compañía de los infantes. Con este motivo y dentro de los muchos actos que se le ofrecen a la familia Real se incluyen: Una danza a cargo de unos labriegos del pueblo de Ob\n\n[ENDING CONTEXT]\n\nbien Córdoba no tuvo profesionales como artistas flamencos, sí mantuvo a través de los tiempos, una gran afición, en los dos núcleos flamencos importantes como la propia capital, y en la provincia Lucena y Puente Genil. Pero no hay duda que quien más contribuyó a cimentar esta afición, fue el gran maestro Silverio Franconetti, de quien celebramos el centenario de su muerte.\n\nCórdoba quiso perpetuar su gratitud y reconocimiento al gran maestro, instituyendo un premio especial en su concurso nacional de arte flamenco, al cantaor más completo, con el nombre de «Premio especial Silverio» ■\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Los pasos del maestro Silverio en la ciudad de Córdoba",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 2286,
    "article_char_count_full": 13275,
    "article_char_count_review": 2852,
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
  }
]
```
