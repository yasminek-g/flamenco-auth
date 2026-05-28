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
    "article_id": "1987-11-19-left-i-encuentro-de-cr-ticos-de-flame",
    "article_text_for_review": "Durante los días 11 y 12 de diciembre y promovido por el Grupo Candil y la Peña Flamenca de Jaén, se ha celebrado en la capital jiennense el I Encuentro de Críticos Flamencos, al que fueron presentadas distintas ponencias, posteriormente debatidas.\n\nEsta primera reunión con los responsables de la Crítica Flamenca, ha cristalizado en siete conclusiones que son una modesta, pero neta declaración de principios y punto de partida para proyectos más ambiciosos. Singularmente, la voluntad de los asistentes, expresada por unanimidad, de constituirse en Asociación, ha de ser puntualizada en el sentido de que una Asociación no es un fin en sí misma, sino instrumento, de cuya idoneidad no nos cabe duda, para la consecución de unos objetivos. Objetivos que conectan con el deseo de ofrecer a la audiencia de cualquier medio, una crítica honesta y veraz, basada en adecuados conocimientos del hecho flamenco, y en la medida que ello sea posible, didáctica.\n\nOfrecemos las conclusiones de este Primer Encuentro de Críticos de Flamenco, con la esperanza de que las mismas se conviertan, prontamente, en realidad.\n\nCONCLUSIONES\n\nPrimera\n\nEsta primera reunión asume como denominación que mejor expresa la realidad que representa «En cuentro de Críticos de Flamenco».\n\nSegunda\n\nEntendemos como Crítico de Flamenco, aquella persona que desde un medio de comunicación, periódicamente, emite juicios de valor sobre el hecho flamenco.\n\nTercera\n\nInstamos a los medios de comunicación a que al frente de los espacios que versen sobre el flamenco, se destinen personas, que con conocimiento y veracidad, sean capaces de enjuiciarlo.\n\nCuarta\n\nConsideramos que es necesario confeccionar un censo de medios, espacios de flamenco y críticos responsables de los mismos.\n\nQuinta\n\nPara la realización de este objetivo, se crea una comisión integrada por todos los participantes en este primer encuentro.\n\nSexta\n\nEstimamos conveniente la creación de una Asociación de Críticos de Flamenco.\n\nSéptima\n\nAceptamos el ofrecimiento de Cádiz y Jerez, como sede de un próximo encuentro, en el que se redactará el borrador de los Estatutos de la «Asociación de Críticos de Flamenco», cuyo acto de constitución se realizará en Jaén.\n\nJaén, a doce de diciembre de mil novecientos ochenta y siete.\n\nASISTENTES\n\nMiguel Acal Jiménez Carlos Arbelos José Luis Buendía López Francisco Carrillo María Rosa Fiszbeing Aurelio Gurrea Chalo Juan Antonio Ibáñez Manuel Martín Martín Juan de la Plata Ramón Porras Paco del Río Rafael Salinas Pedro Sánchez Ortega José Luis Solera Rafael Valera",
    "title": "Primer encuentro de críticos de flamenco",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 396,
    "article_char_count_full": 2547,
    "article_char_count_review": 2547,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-11-19-right-loor-a-fosforito",
    "article_text_for_review": "En esta feria de vanidades que es\n\nCORRESPONSAL\n\nvanidades que es actualmente el mundo del flamenco en lo artístico en el que cada cual y salvo rarísimas excepciones se adjudican números, títulos y grandezas en la medida en que puede a su gusto satisfacerlas, resulta tan relevante como gratificante un hecho que por insólito y del que me he sentido ocasionalmente excepcional testigo, estimo que me releva de las promesas de silencio y discreción hechas al propio interesado.\n\nParece que en el XV Congreso Nacional de Actividades Flamencas, al que por obligaciones profesionales no me ha sido posible asistir, en una ponencia presentada por la Federación de Peñas Flamencas de Madrid, se solicitaba y defendía una, en la que se pedía al Congreso, solemnizara con su aprobación, la concesión de la Llave de Oro del Cante para don Antonio Fernández «Fosforito» sin previa consulta al interesado.\n\nNada hay que oponer ni objetar a tan excelente como apasionada idea, provinente según mis informes de la Peña Flamenca «Fosforito» del Puente de Vallecas, porque nada hay más justo que la pasión con que su presidente trataría de defender la candidatura de su titular para tan singular galardón.\n\nAlarmado y creo que justamente «Fosforito», por cuanto artísticamente se le podría venir encima de prosperar esta propuesta, recurrió a los buenos oficios de nuestro siempre querido y admirado Paco Vallecillo, quien a su vez y confiando excesivamente en los míos, requirió mi intervención para que tratara de disuadir a don Juan Fernández, creo que principal promotor de la idea, accediendo a realizar esta gestión, aunque con el convencimiento de que nada conseguiría.\n\nMi inicial gestión no dio en principio ningún resultado, porque el interesado sobre el que había de desarrollarse, debía encontrarse ya en Benalmádena, ya que todo ello ocurría en la víspera de la inauguración del Congreso, haciendo tan innecesaria como infructuosa mi intervención, encaminada exclusivamente a complacer a un gran y querido amigo y también a una gran persona y excelente artista como en este caso resulta el desinteresado «Fosforito».\n\nEn mi breve pero grata conversación mantenida con «Fosforito» desde Alhaurín, donde lo había llamado para informarle de la inutilidad de mi deseo por complacerle, dada la ausencia del promotor y defensor de la ponencia, pude darme cuenta de la serie de virtudes que concurren y adornan la personalidad de este hombre para quien mi admiración y adhesión han crecido considerablemente por su modestia, honradez e inteligencia al renunciar conscientemente no a un honor como sería la consecución de la Llave de Oro, sino al desarrollo de su propuesta, figurándome lo trabajoso que habrá resultado a «Fosforito» conseguir su propósito de que la ponencia fuese retirada, ya que a sus muchas virtudes habrá que agregar una más, cual es la disuasión diplomática que habrá tenido que poner en juego para, con su convencimiento, eludir la responsabilidad que posiblemente con más pasión que meditación iban a imponerle, de haber prosperado la ponencia.\n\nEs precisamente por donde al caminar por esa andadura de consciencia, caballerosidad y honradez, tanto artística como personalmente, por donde se consigue el respeto y la admiración de la afición, entre la que ya ha comenzado usted a disfrutar, consolidando la categoría de gran figura en la que por méritos propios, se está convirtiendo.\n\nY aunque no se me ha pedido ni opinión ni consejo, permítame el que de mi admiración brote la osadía de pedirle que continúe en esa línea, ya que con esa alteza de miras que justamente se ensalza en estas modestas líneas y que tan llanamente resaltan de su personalidad, le continuarán proporcionando premios y títulos que agregar a sus muchos méritos.\n\nY al concluir lo que para mí he considerado una obligación, con mi admiración y respeto, le envío, si me lo acepta, maestro, un cordial abrazo.",
    "title": "Loor a Fosforito",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 631,
    "article_char_count_full": 3898,
    "article_char_count_review": 3898,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-11-20-left-cantaores-conocidos-compa-eros-y",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLuis Caballero\n\nque Emilio Jiménez Díaz y Triana son una misma cosa? A mí así me lo parece después de algunos años auscultándole como hombre, amigo y trianero.\n\nGritar viva Triana desde Triana es en mucho autovitorearse; desde fuera es como sentirla a través de la inevitable nostalgia. Desde cerca y desde lejos Triana siempre será el poema típico y tópico de la exaltación y la gracia en el sentimiento y el recuerdo de lo sufrido y lo gozado. Pero yo diría que el trianero Emilio Jiménez Díaz la ve, la abraza y la analiza desde mucho más hondo y desde mucho más alto. Sobre los barandales de sus altas nubes líricas la contempla milenariamente laboriosa y pobre, desde la hondura de sus barros alfareros como vientre fecundo de un decir espiritual lastimado por el arte de la pena.\n\nAl arte de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"técnica\"]\n\nfareros como vientre fecundo de un decir espiritual lastimado por el arte de la pena. Al arte de la pena en su raíz más profunda y trianeramente libre me ha llevado Emilio de la mano del cante. Ha descubierto, para la delicadeza del buen entender, la escondida joya de una larga cadena de soleares engarzadas sobre las piedras preciosas de los poemas del pueblo. Así confieso haber penetrado un tanto más, de manera milagrosa y gracias a la mágica técnica del sonido grabado, en el barroco lirismo musical de uno de los trianeros más preclaros y personales del cante de su parcela. Aún vive el fino autor del hallazgo; trianero, alfarero, solearero y tocayo de mi introductor al deleite auditivo del premio. Gracias a ti, amigo Emilio Jiménez, y gracias a usted don Emilio Abadía de Triana y al cante. Gloria a la digna humildad de los que hacen camino andando sobre su propio barro, a los que no empuñaron otro cetro que el de la bondad regalando la riqueza espiritual de su arte a los cuatro vientos de la amistad, a los que escribieron, en el aire flamenco de Triana, el libro más profundo de su historia, ¡páginas ya perdidas, borradas, rotas, mal leídas cuando no menospreciadas por los que nunca supieron leer! Don Emilio Abadía. Siempre lo he venido conociendo con retraso. Yo no nací ni crecí en Triana, pero sí viví alguna vez con mi familia en la calle pureza y la plazuela de «Santana». Sobre cuarenta años conviviendo con conocidos, compañeros y amigos de ese viejo y sufrido pueblo hijo y víctima de su río. Nunca en mis incursiones flamencas por las tabernas de ese venerado solar tuve la fortuna de encontrarme con tan significativo cantaor, y puedo presumir de conocidos, compañeros y amigos cantaores de Triana: Entre otros fue Manuel Oliver, el primero y del que algo aprendí; después Antonio y Joaquí\n\n[ENDING CONTEXT]\n\nsirvieron los ejemplos prácticos de sus discos. Mi viejo amigo Camilo Murillo —¡cómo han pasado los años!— lo define casi lapidariamente: «Tomás no era hombre de esta época. Para él cantar requería la amistad, la conversación, el cigarrillo liado con medio papel de fumar (se lo había ordenado así el médico) y ese impagable \"estar a gusto\" de los andaluces. Tomás era un ser de otro planeta mucho más bello... Tomás era sentimiento y delicadeza puros. No me cabe duda de que está en el cielo; en el que haya. Se me saltan las lágrimas recordándolo».\n\nDoctor Arroyo, 12\n\n- Teléfono 210058 - J A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantaores conocidos, compañeros y amigos: Emilio Abadía. Tomás Pavón",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1854,
    "article_char_count_full": 10814,
    "article_char_count_review": 3446,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "técnica"
      }
    ]
  },
  {
    "article_id": "1987-11-21-right-iscografia-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRef.: PSD - 5032 PASARELA\n\nP ASARELA, una vez más, acaba\n\nEntre terrones y pedruscos (así llama un cantaor granadino con garganta de gata rabiosa a las voces gitanas), entre la vitalidad ancestral y la recia canoridad de una tierra bendita, surge este gitano tocado de una estrella de cinco puntas, el eco atávico de un joven jerezano que se resiste a pactar con el vanguardismo flamenqueril. Él dispone de todos los componentes que están rotundamente vedados a la mayoría y, asimismo, tiene la facultad de no soñar con imposibles. Sabe que su garganta de oro, sus mágicas condiciones y su ordenada cabeza no están desnudas como la piel del aire, de ahí que se revele orgulloso y acomodado en\n\nde acertar. Su labor arroja un saldo favorable (a excepción, entre otros, del «Aromo» sin perfume de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"expresión\"]\n\nay que tragar lenta y suavemente. Se propuso irrumpir en el flamenco con un claro afán de reivindicación de la espesa jondura y ahora lo consigue con José Mercé, quien se ha ido afirmando en este último lustro como el más prometedor de los cantaores. Y es que el jerezano sabe lo que quiere, sin vacilaciones de ningún tipo, y se desvive en la grabación presente por los surcos que marcaron los innegables caminos del cante. Manuel Martín Martín la expresión gitano-andaluza. Con ella sufre, goza, construye y ofrenda a diario el estandarte sonoro del pueblo que lo vio nacer. Por eso su cante encuentra inspiración en los más altos ideales de la gitanería cantaora. Mas hay algo que merece una reflexión. Jerez de la Frontera es pródiga en cantaores y artistas ilustres. Independientes unos, ortodoxos los más, se han ido multiplicando a lo largo de la geografía cantaora, pero siempre al calor humano y pluralista de la tierra, es decir, todos fueron/son seres nacidos para la libertad, como así llamó el maestro al insigne Manuel Torre. Ello explica el que Mercé aúna la agresividad expresiva con el sosiego interior, a la par que rezuma la tranquilidad del vuelo de una paloma. O mejor aún, la estela de un pájaro con grandes alas que con su pecho herido besa el viento y con su trino recibe, saluda y canta a la libertad. Es como aquella bandada de pájaros que incitaron al poeta indio Tagore a definir su canto como «el eco de la luz del alba en la tierra».\n\n[ENDING CONTEXT]\n\na los que anexiona la coda que popularizara Manolillo el Herrao.\n\nEstamos, por tanto, ante un importante trabajo discográfico que marca el patrón para el flamenco venidero, indispensable para el investigador, profesional de la información o aficionados en general y donde Luis de Córdoba se encuentra a sí mismo, muestra su logrado equilibrio emocional y consigue la evolución del flamenco clásico con una maestría rayana en la perfección. Así, con el deleite de los dos últimos discos de Luis de Córdoba, se justifica el que la afición apueste fuerte por esta valiosa voz que sonará largamente.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1392,
    "article_char_count_full": 8159,
    "article_char_count_review": 3086,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "expresión"
      }
    ]
  },
  {
    "article_id": "1987-11-22-right-noticiario-flamenco",
    "article_text_for_review": "FALLADO EL CONCURSO DE LETRAS DE VERDIALES EN MALAGA\n\nJosé Márquez Cabello consiguió los dos primeros premios del concurso de letras para verdiales que, con motivo del Encuentro de Verdiales, ha organizado en su segunda edición la Cámara de Comercio de nuestra provincia. Márquez consiguió el primer premio con la letra de la copla titulada «Marengos», por la que obtuvo 50.000 pesetas. Asimismo, este autor se adjudió el segundo premio, de 30.000 pesetas.\n\nEl tercer premio correspondió a Carmen Gonzalgo Castellano, quien recibirá 20.000 pesetas por su trabajo. El jurado decidió conceder tres accésit, de cinco mil pesetas cada uno, a Antonio Brenes Infantes, María Dolores Bandera Hernández y al trabajo remitido por la parroquia de San Félix.\n\nEl jurado estuvo compuesto por: Alfonso Canales, Antonio Mata, Juan Cepas, Federico del Alcázar y el presidente de la comisión de Cultura de la Cámara de Comercio, Rafael Pérez-Cea Soto. Los premios serán entregados durante el Pregón de Verdiales, que tendrá lugar en el Teatro Cervantes.\n\nEn esta segunda edición del concurso la participación ha sido notable, por lo que los responsables del mismo se encuentran muy satisfechos. Al existir la posibilidad de que un mismo autor pudiera entregar varias letras, es por lo que el ganador del concurso también se adjudicó el segundo premio del certamen.\n\nEn la ciudad de Jerez de la Frontera, siendo las 12,55 horas del día 16 de octubre de 1987, tiene lugar la reunión del jurado calificador del I Certamen de Pintura Flamenca, con la asistencia de los siguientes miembros:\n\nJURADO\n\n1. Don Antonio Reyes Ruiz, teniente de alcalde delegado de Cultura del Ayuntamiento de Jerez de la Frontera, por delegación del vi- 4. Don Antonio Miguel González Sánchez, artista plástico. Director de la Escuela de Artes Aplicadas de Algeciras.\n\n5. Don Manuel Muñoz Ce- brián, artista plástico.\n\n3. Don Mariano Ruiz Carretero, subdirector general de la Caja de Ahorros de Jerez y miembro del Consejo Rector de la fundación.\n\n6. Don Antonio Hurtado Egea, artista plástico. Profesor de la Escuela de Artes Aplicadas de Cádiz.\n\nExcusa su ausencia don Manuel Ríos Ruiz, escritor y asesor de la fundación, por encontrarse en ca- ma tras un accidente.\n\n2. Don Félix Grande Lara, escritor. Experto en temas flamencos. Asesor de la fundación.\n\nConstituido el jurado, acuerdan otorgar los siguientes premios:\n\n1. Primer premimo, dotado con 500.000 (quinientas mil) pesetas y diploma de honor, a don Juan Valdés, de Sevilla, por su obra «Tríptico de la soleá».\n\n2. Segundo premio, dotado con 300.000 (trescientas mil) pesetas y diploma de honor a don Ángel Hurtado de Mendoza, de Madrid, por su obra «Mi jaca galopa y cor- ta el viento».\n\ncepresidente 1.º de la fundación, don Pedro Pacheco Herrera, alcal- de de Jerez. Actúa como presiden- te del jurado.\n\n3. Tercer premio, dotado con 200.000 (doscientas mil) pesetas y diploma de honor a doña Cándida Garbarino, de Cádiz, por su obra «Aire» (díptico).\n\nSin otro asunto que tratar, se le- vantó la sesión a las 15 horas del día y fecha citados.\n\nJerez, 16 de octubre de 1987.\n\nSeñor don Salvador Castro, de la tertulia flamenca de Badalona. Estimado amigo:\n\nLe agradezco sinceramente le gustaran los cantes flamencos que hice en Cornellá. Al mismo tiempo que le ruego comprenda mi deficiente manera de explicarme y sobre todo los hábitos adquiridos como profesional del cante, en unos tiempos en que el que no se enfrentaba con el público como «creador» no se «comía una rosca».\n\nDe todas formas la malagueña en cuestión la creo basada en la de Baldemoro Pacheco. Usted sabe que aunque cantemos los cantes de otros nunca los interpretamos exactamente igual, y menos si tratamos de arreglarlos a nuestro modo.\n\nCreo que yo no he debido decir las cosas tan detalladamente como el caso requería, por lo que pido perdón a usted y a todos los que hayan podido sentirse desorientados.\n\nLes saluda cordialmente, Enrique Orozco. Sevilla.",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 650,
    "article_char_count_full": 3950,
    "article_char_count_review": 3950,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
