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
    "article_id": "1986-11-4-right-la-poes-a-popular-de-manuel-balm",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n0 uisíéramos co- menzar nuestro\n\nJosé Luis Buendía López\n\nestudio con una reflexión de Gustavo Adolfo Bécquer, el cual, en el prólogo al libro Soledad de Augusto Ferrán, afirmaba entre otras cosas altamente interesantes para nuestras investigaciones: «El pueblo ha sido, y será siempre, el gran poeta de todas las edades y de todas las naciones». Tal aserto, que compartimos todos los que, de una manera u otra, nos dedicamos\n\nUno de esos individuos privilegiados fue sin duda Manuel Balmaseda, trabajador de los ferrocarriles, que en el año 1881, el mismo en que vería la luz la Colección de\n\nal estudio de este tipo de poesía, se complica extraordinariamente en cuanto nos cuestionamos quién es el verdadero creador en el pueblo, puesto que parece obvio que éste, entendido como colectividad o\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"comunidad\"]\n\nente en cuanto nos cuestionamos quién es el verdadero creador en el pueblo, puesto que parece obvio que éste, entendido como colectividad o suma de individuos, no compone nada por sí mismo, y que ha de ser un número reducido de habitantes de aquél el que, debido a su mayor sensibilidad, o tener más agudizado un sentido innato para captar matices líricos o rítmicos, construya de forma individual aquellas composiciones que más tarde el resto de la comunidad, si las encuentra en sintonía con sus inquietudes lúdicas y anímicas, harán patrimonio común y con el paso del tiempo, pasarán a ser tenidas por populares. Cantes Flamencos, recogida por Demófilo, daría a la imprenta un Primer Cancionero de Coplas Flamencas Populares, según el estilo de Andalucía, que fue editado en Sevilla en la imprenta y librería de E. Hidalgo y Compañía. El librito no pasó desapercibido, sino que, debido a la sensibilización ambiental, tuvo unos inusitados valedores; no debemos de olvidar que en 1882, es decir un año más tarde, Rodríguez Marín publicaría, también en Sevilla, sus Cantos Populares Españoles y Antonio Machado Alvarez ponía en marcha su revista «Folk-lore andaluz» en la que participaron nombres tan prestigiosos como los hermanos Guichot, Luis Montoto, Antonio Sendras, etc. Concretamente entre Montoto y Demófilo se estableció una curiosa correspondencia, acerca del librito de Balmaseda, en la que el primero le ruega al segundo no deje de adquirir la obra del ferroviario y al mismo ti\n\n[ENDING CONTEXT]\n\ncría lana, que las pierecitas de la calle, Imare\n\nlas tengo por cama y que, sin embargo, mantiene el orgullo decidido y arrogante de toda una raza, la de los desposeídos que pueblan el mundo; la de aquellos que nada tienen y que no obstante son conscientes de haberse hecho a sí mismos, de haber sostenido un pulso con la vida de los que se saben perdedores, pero no por ello dejan de recordar con la cabeza erguida la grandeza de la gesta que un día acometieron desde la triste desventaja de su inferioridad:\n\nYo soy como el arbolito desde chico ladeé, nadie pudo enderezarme, yo solo me enderecé.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La poesía popular de Balmaseda",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 3321,
    "article_char_count_full": 19359,
    "article_char_count_review": 3119,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "comunidad"
      }
    ]
  },
  {
    "article_id": "1986-11-8-right-cantaores-conocidos-compa-eros-y",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCantaores\n\nconocidos,\n\ncompañeros y\n\namigos\n\nPero en 1933 los maireneros que yo conocía me hablaban del «Niño de Rafael», de cómo cantaba aquel niño de aquel pueblo que era\n\nbola a su propio cantaor —de tenerlo— como bandera local, y es bonito, noble y digno que cada cantaor, como tal enseña, ondee al aire de la fama el nombre del lugar de su nacimiento. Es como un tercer apellido que con la fuerza de su nombre histórico se antepone a los dos primeros. Así, el cantaor don Antonio Cruz García pasó a conocérsele de «Niño de Rafael», por su padre, a «Niño de Mairena», por su pueblo, para quedarse al fin en Antonio Mairena, acertada y feliz modalidad que, de no equivocarme, comenzó con don José Tejada, «Niño de Marchena».\n\nLuis Caballero\n\nel de ellos. A su naturaleza gitana no hacían mención,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\ne al fin en Antonio Mairena, acertada y feliz modalidad que, de no equivocarme, comenzó con don José Tejada, «Niño de Marchena». Luis Caballero el de ellos. A su naturaleza gitana no hacían mención, la naturaleza que importaba era la de ser hijo de Mairena del Alcor como ellos. Ni por casualidad llegué a conocerle entonces. Cantaores de sus características no era frecuente encontrarles por entre troupés de «creadores y estilistas», así vine a escucharle bastante más tarde y a través de una sola grabación. Me causó tal impresión que decidí apurar mis escasísimas posibilidades hasta dar, al menos, con lo poco que hizo en discos con la guitarra de Esteban Sanlúcar. Unos años más y me llega su primer LP conseguido en Londres. Por fin tengo la oportunidad de conocerle y aplaudirle junto al otro Antonio. ¡Qué maravilla de Antonios! Un segundo LP y su presencia pensante, paseante y mascullando cantes por las calles de Sevilla. Ahora la casualidad sí quiso que por tres veces consecutivas él me oyera por radio enjuiciarle como cantaor, juicio que estimó muy por encima de lo habitual. Antonio podía alguna vez mostrarse convencido de su importancia hasta resultar egolátraico, pero por Antonio podía alguna vez mostrarse convencido de su importancia hasta resultar egolátrico, pero por naturaleza era sinceramente agradecido. naturaleza era sinceramente agradecido, fue por lo que inmediatamente puso en movimiento la mejor manera de agradecerme personalmente mis declaraciones radiofónicas a su favor. Su hermano Curro —al que tanto estimo— se encargó de que nos encontráramos una noche en casa de unos amigos. Fue en Alcalá de Guadaira ya hace muchos años. Antonio era una de esas personas que suelen girar sobre sus sentimientos y razones personales hasta dejarlas como tatuadas en el interlocutor, así me repitió su más hondo agradecimiento la noche de nuestro inicio amistoso. Sin embargo, no fue precisamente óbice este c\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_04 | trigger=\"interior\"]\n\nía que hablar de problemas precisamente sociopolíticos y la verdad es que de este tema sólo hablaba con quienes le merecían plena confianza (tenía sobrados motivos para desconfiar y temer). Antonio Mairena estaba solo. Aseguraría que lo estuvo siempre, que siempre anduvo por la vida defendiéndose más que luchando. Su lucha era interna, íntima, personal, su defensa material, brutal, ambiental (el colectivo flamenco, en cuanto a su funcionamiento interior, puede ser tan canallesco, insolidario y carente de ética como cualquier otro tipo de orden desordenadamente inspirado en la pica-resca). Como buen flamenco en solitario agudizó su mente hasta alcan- Antonio Mairena anduvo por la vida defendiéndose más que luchando zar esos límites en que se empieza a saber lo que se quiere y a conocer los caminos más convenientes para lograrlo y, aunque más bien tarde, llegó a tiempo de hacerse cargo del lid\n\n[ENDING CONTEXT]\n\nconjunto de muchos mundos cantaores? Si Caracol se perdía en el suyo, ¿por qué buscarle en otro?\n\nManolo Caracol era genial para algunos y mucho menos o apenas nada para otros\n\nEl cante tiene una historia de la que sólo conocemos el espíritu. Pululamos en el viento del cante como una veleta ciega de nuestro sur perdido en «el misterio luminoso de lo más oscuro». «Son artes mágicas del vuelo, sin huella o trazo literal que señalen su ruta para repetirse. Artes puramente analfabetas». Así los cantaores cantamos lo que sabemos, pero nunca sabremos lo que cantamos. Lo demás se sabe y se aprende.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantaores conocidos, compañeros y amigos: Don Antonio Mairena, Manolo Caracol",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 3509,
    "article_char_count_full": 20873,
    "article_char_count_review": 4534,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "interior"
      }
    ]
  },
  {
    "article_id": "1986-11-11-right-sobre-el-piyayo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Reyes Nieto «El Piyayo» (Málaga, 1864-1941), tuvo una vida pintoresca y azarosa, desde soldado en la guerra de Cuba a presidiario. Su cante es fiel reflejo de su vida. Un tango irregular con aires de carcelera y guajira, que se acompañaba él mismo a la guitarra en constante improvisación.\n\nJuan Calderón Rengel\n\nace unos años se publicaron en el diario SUR, de Málaga, unos artículos con entrevistas a personas de distintas edades, profesiones, etc., sobre si habían conocido o no al «Piyayo» o, al menos, habían oído hablar de él. No tengo a mano los trabajos, ni siquiera puedo asegurar que los conservo. No se trata de estudios sobre este personaje, sino más bien de una encuesta de la que pudiera deducirse, con garantías de autenticidad, la erosión que el tiempo —ese enemigo del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"memoria\"]\n\ns con entrevistas a personas de distintas edades, profesiones, etc., sobre si habían conocido o no al «Piyayo» o, al menos, habían oído hablar de él. No tengo a mano los trabajos, ni siquiera puedo asegurar que los conservo. No se trata de estudios sobre este personaje, sino más bien de una encuesta de la que pudiera deducirse, con garantías de autenticidad, la erosión que el tiempo —ese enemigo del hombre— va causando inexorablemente en nuestra memoria, en todo nuestro ser. Resultó de aquella prueba que nadie o casi nadie había conocido al «Piyayo» ni había oído hablar de él. Y no porque no gozara de popularidad en su tiempo —que sí la tuvo y grande—. Y es que Málaga la cantaora había empezado a olvidar a una de sus figuras flamencas más representativas y trascendentes. pero aquí se produjo su renacimiento, pues cuando muchos de los profesionales del cante (algunos incluso importantes) eran relegados al más negro olvido y allíse quedaban anquilosados y estan cados, los «jipíos» de este gitano serio, silencioso y desgarbado volvían a sonar y resonar en cafés y tablaos, y sus «ayes» han vuelto a ponerse de moda para alcanzar ese «status» de solera y definitiva consagración, que es como un espaldarazo de vigencia. Y eso, digo yo, será por algo. A mí no me cabe du-da de que la principal labor de mantenimiento, conservación, vigilancia y mejoramiento de la pu-reza del cante la llevan a cabo las «peñas», que agrupan en sus cenáculos a aficionados, o, sencilla-mente, amantes de esta faceta de nuestro «folklore». Ho\n\n[ENDING CONTEXT]\n\nla letra:\n\n«Yo salí de mi cuartel con hambre de tres semanas, y me encontré un pirolillo cargaído de manzanas. Empecé a tirarle piedras y caían avellanas, y al ruido de las nueces salió el amo del peral. \"¿Está usted cogiendo uvas, siendo mío el melonar?\". Me tiró medio ladrillo y me pegó en un tobillo, me hizo sangre en un colmillo y me dolió hasta el morrillo. Yo fui a la venta a curarme. El ventero estaba malo y la mujer no lo sabe. Las cabras están en misa, las mozas en el corral, los platos friegan y barren y la escoba en el vasar. En el cajón está el vino y en la calabaza el pan».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sobre «El Piyayo»",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1530,
    "article_char_count_full": 8924,
    "article_char_count_review": 3152,
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
    "article_id": "1986-11-13-left-ellos-los-protagonistas-dicen-cu",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Martín Martín\n\nCurro Mairena nace en Mairena del Alcor en febrero del año 14, enfrente del Casino, en la calle Mesones, en la fragua de su padre. Allí nacieron también Juan y Aguila.\n\nMairena supone siempre el retorno a la anoranza, a recordar tiempos envueltos en las nostálgicas secuencias que no volverán. A su calidad de buen gita-no y buena persona une una vigilancia extremada en sus respues-tas; no quiere herir a nadie o mo-lestar y muestra una premeditada precaución para entrelazar la pel-lícula de su vida y sus gentes.\n\nQuizá analizando profundamente sus raíces, vivencias y el entorno que le rodea encontremos el por qué. Curro Mairena —nombre artístico que le pusiera su compadre Curro Torres—, legatario de mensaje transmitido sea mal interpretado. Ahí radica el primer\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nuiere herir a nadie o mo-lestar y muestra una premeditada precaución para entrelazar la pel-lícula de su vida y sus gentes. Quizá analizando profundamente sus raíces, vivencias y el entorno que le rodea encontremos el por qué. Curro Mairena —nombre artístico que le pusiera su compadre Curro Torres—, legatario de mensaje transmitido sea mal interpretado. Ahí radica el primer obstáculo a salvar con nuestro entrevistado. Curro Mairena, uno de los mejores siguiriyeros que, recordamos, es un eslabón más de la Casa de los Mairena, siempre al reparo de su hermano y maestro, pero con la fija idea de contornear sus cantes derredor del Majareta. De estas y otras muchas cosas conversamos con Curro Mairena en esta entrevista que hemos querido conservar intacta, que no responde a un guión preconcebido y donde observarán que algunas respuestas quedan dispersas por mor de la avanzada edad de nuestros soleá o una siguiriya nueva? Antonio decía que el cante estaba hecho, que estaba revoloteando en el aire, y que había que cogerlo, pero pocos se atreven. —¿Será por eso por lo que se buscan alternativas a los festivales? —Sí, hoy se hacen muchas pamplinas. Yo no entro en eso, pero sí digo que hay q\n\n[ENDING CONTEXT]\n\nyo le echo.\n\n«Con cantaores como mi hermano Manolo, Menese, Curro Malena y muchos más..., el mairenismo sigue vivo...»\n\n—¿Cuáles son los estilos sigui- riyeros más difíciles de ejecutar?\n\n—Mira, Triana es mu difícil porque antiguamente como se cantaba tan corto, casi sin pronunciar, pues ahí está la cosa. Ya cuando llegó Tomás la hizo más larga porque tenía mucho fuelle. —¿Qué me dices de esa joya que tú guardas de Tío José de Paula según el cante de Paco la Luz?\n\n—Ese cante es mu bueno. Yo lo hice casi por equivocación, porque cuando lo iba haciendo me di cuenta que me faltaba el aire y en-\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen: Curro Mairena",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 2162,
    "article_char_count_full": 11931,
    "article_char_count_review": 2817,
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
    "article_id": "1986-11-18-left-enderezando-entuertos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\ntonces le metí un ¡ay! y de ahí salió un cante.\n\n—¿Qué te dijo Antonio?\n\n—Antonio me dijo que eso no era así, pero que queaba mu gita- no.\n\n—Curro, dicen que Manuel Torre era un majareta.\n\n—Lo dirá el que no lo ha conocido, porque yo cada vez que lo escuché era como un reloj y con más duendes que ninguno.\n\n—¿Desde cuándo no cantas, Curro?\n\n—Desde que me puse malo que tú sabes que me dio la congestion. Y es que ya no es como antes...\n\n—Eso debe ser difícil de super- rar.\n\n—Bueno, más todavía. Mira, yo voy a algún sitio con mi hermano Manué y me enveneno porque quiero y no puedo.\n\n—Dicen que Curro Mairena es un cantaor muy gitano.\n\n—Eso no lo debo decir yo, pero es que Curro Mairena además de cantar mu gitano es que suena más gitano todavía. Porque si se suena gitano se puede cantar gitano,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nra, yo voy a algún sitio con mi hermano Manué y me enveneno porque quiero y no puedo. —Dicen que Curro Mairena es un cantaor muy gitano. —Eso no lo debo decir yo, pero es que Curro Mairena además de cantar mu gitano es que suena más gitano todavía. Porque si se suena gitano se puede cantar gitano, si no, no hay ná que hacer. —Hay buenos artistas, buenos cantaores, pero Antonio ha dejado este valdío, el que se arrima es mi Manué porque tiene la voz parecía y cantando malamente suena gitano, pero no es como Antonio. Con lo que hay yo creo que no sale otro Antonio. Hombre, sí —¿Cómo detectas tú el cante gí- tano? —¿Te llegan hoy esas punzaí- tas? —Mira yo lo sé, cuando me da una punzaía. Mira yo iba por los colmaos de la Alamea y sin ver quienes estaban cantando ya sabía si era o no gitano, porque eso es un sello personal, otra forma de quejarse y de matizar el cante. hay cantaores que tienen afición y luchan por esto, Fosforito, Menese y poco más. darse de Pastora porque mi prima no tenía fin cantando. Pero Antonio era un monstruo. —Curro, Antonio el más completo de la historia. —Esa palabra ya sabes quién la inventó. —Hoy sí se puede decir eso porque ha sío el más completo de tóis los tiempos, aunque hay que acor—Curro, ¿qué es el mairenismo? —Sí, tu compadre Curro Torres que lo definió en La Vanguardia de Barcelona, ¿pero cómo lo definió las tú? —La escuela más completa de tós los tiempos. —¿Es la última revolución del Cante Jondo? —Pues sí, se puede comparar con la revolución de Manuel Torre, pero en este tiempo. —¿Tú te consideras un produc- to del mairenismo? APERITIVOS SELECTOS Especialidad en PLANCHA Mesones, 18 Teléfono 26 35 46 J A E N —Pues sí, porque el mairenismo es la casa de los Mairena en un escaparate, pero que sin Antonio no existiría el mairenismo, estaría solamente la casa.\n\n[ENDING CONTEXT]\n\n(Disco funda doble) Basado en los textos de Federico García Lorca con música de Mario Maya\n\nL / P Cassette PSD - 6000 PSC - 6000 P.V.P. 1.300 pts. P.V.P. 1.300 pts. Si desea recibir algún ejemplar de los indicados le rogamos rellene este cupón, dirigiéndolo por Correo a PASARELA. Lo recibirá contra reembolso al precio antes indicado más gastos de envío.\n\nNOMBRE.....\n\nDIRECCION.....\n\nPOBLACION..... D.P.....\n\nTELEFONO.....\n\nIndique las unidades y a continuación el número de referencia impreso en la línea superior del precio (P. V. P.), para disco o para cassette.\n\nDISCO/S L.P. CASSETTE/S.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Enderezando entuertos",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "16-19",
    "page_number": 16,
    "word_count": 1938,
    "article_char_count_full": 10706,
    "article_char_count_review": 3441,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      }
    ]
  }
]
```
