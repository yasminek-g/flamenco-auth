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
    "article_id": "1982-01-18-right-reflexiones-en-torno-a-un-homena",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEl pasado día dos de enero, en el restaurante Los Arcos de Montilla, con lleno rebosante, Rafael Gómez, «EL LUCERO», recibía el trofeo Venancio Blanco, 1981. Le fue entregado por Luis de Córdoba, trofeo Venancio Blanco 1980.\n\nPocas veces, justo es reconocerlo, una persona se ve rodeada de tanto afecto, de tanto calor humano, de tanta complacencia demostrada..., respirada, diría yo.\n\n¡Sencillamente admirable! Si pensamos que, a la edad de Rafael Gómez, «EL LUCERO», normalmente gran parte de los hombres comienzan a cosechar traiciones, abandonos, desamores, complejas envidias y desengaños, encontrarse con el reverso de la medalla es sumamente grato y esperanzador. Esta reacción afectiva nace del hecho en sí, algo ciertamente valioso, como respuesta humana a una cita con la bondad y el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicitaria\"]\n\ns, complejas envidias y desengaños, encontrarse con el reverso de la medalla es sumamente grato y esperanzador. Esta reacción afectiva nace del hecho en sí, algo ciertamente valioso, como respuesta humana a una cita con la bondad y el cante. No es exagerado decir que vivimos en plena «era» del homenaje a famosos y, en estos momentos de psicosis, es difícil que nadie piense ni fije su atención en valores no profesionales, desprovistos de aureola publicitaria, como algo digno de rendirle reconocimiento público. Afortunadamente las excepciones abundan; un artista plástico, viejo aficionado al flamenco, ha tenido la feliz idea de extraer del silencio y poner de relieve ciertas «piezas» fundamentales del complejo engranaje del mundo flamenco. Venancio Blanco, conocido escultor salmantino, ha querido premiar el arte flamenco cordobés con un trofeo de su creación. El primero le fue otorgado a Luis de Córdoba el pasado año. Con éste, Venancio premiaba un presente lleno de realidades y una fama en ciernes. El correspondiente a 1981 se lo ha concedido al Lucero. La ecuanimidad de Venancio y su conocimiento histórico del flamenco le han llevado a\n\n[ENDING CONTEXT]\n\nGenil, por Juan Hierro, y en Benamejí, con el gran Cayetano en sus años finales.\n\nLa irradiación emanada de estos grandes aficionados mantuvo el interés —escaso interés— que había en este largo momento de desdoro del flamenco, en el que nuestras raíces, nuestras costumbres y nuestro folklore estuvieron en entredicho y, en ocasiones, duramente criticadas.\n\nEste trofeo recaído merecidamente en El Lucero alcanza, en su intencionalidad, a los hombres de su tiempo que, por encima de todo, vivieron, sufrieron y gozaron intensamente la vida del andaluz que no rehúye su destino.\n\nAntonio Povedano\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Reflexiones en torno a un homenaje",
    "periodical": "candil",
    "issue_id": "1982-01",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1198,
    "article_char_count_full": 7206,
    "article_char_count_review": 2778,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicitaria"
      }
    ]
  },
  {
    "article_id": "1982-01-19-right-peteneras",
    "article_text_for_review": "* Por José Márquez Cabello\n\nNo se puede dudar ya que la «petenera», como cante, fue creación de Dolores de Paterna de Ribera, bella mujer dotada de hermosa y penetrante voz. Luego Medina el Viejo (de Medina Sidonia, ¿o de Jerez?), que tuvo que haber conocido bien los cantes de su gadipaisana, basándose en ella hizo su versión dejándonosla su hijo, el Niño Medina, ¿no señor Yerga?; así, como más tárde, El Mochuelo y Pastora, y hasta la actualidad, el de La Matrona, R. Romero, Fosforito, Naranjito de Triana y Perro de Paterna, por nombrar sólo los más cualificados. En cada uno tiene su valor y todos han contribuido, cada cual a su manera, a que tal cante sea hoy admirado cual lo que es: un cante recio, magestuoso y digno de figurar en las mejores programaciones. Y, por supuesto, libre de supersticiones tontas.\n\nIndiscutiblemente debemos este cante a Paterna de Ribera. Aún no faltan quienes lo adjudican a la Paterna de Almería o la de Huelva. Y ¿quién puede asegurar que no fuera el propio Medina el Viejo, fiel seguidor de Dolores, el que tras la muerte de ésta, no clamara cantando la siguiente «letra», que tenemos recogida de la tradición oral: «Petenera, cantaora/de Paterna de Ribera/dile a Dios me dé tu don/pa cantar por peteneras».\n\nCOROMINAS, en su «Breve diccionario etimológico», afirma: «Aire popular de origen incierto, alteración de Paterna, pueblo de Andalucía». Caffarena Such completa en su corto estudio que hay tres Paterna: una en Almería, otra en Huelva y la de Cádiz. Nos inclinamos por la última al ser de allí Dolores, «La Petenera», cuyo apelativo le vendría por hacedora del cante en cuestión.\n\nHay quien opina que las influencias melódicas de la «petenera» datan de los cantos sinagogales hebreos a la vista de la copla «¿Dónde vas bella judía?». Pero ello ofrece dudas después de que Corominas dejara sentado «De origen incierto; aire popular alteración de Paterna». Lo lógico, como testimonió «El Solitario» «...fueron en un principio tonadillas pertenecientes al folclor gaditano». Y entonces —agregamos— la genial Paternera o Petenera las remoldeó e insufló su imprenta o sello personal. Como todo arte ha evolucionado, la «petenera» en el flamenco no podía quedarse atrás hasta llegar a las entrañables y buenas versiones que hoy se escuchan recordándonos siempre a Paterna de Ribera.\n\nPaterna, de Cádiz, merece nuestro aplauso por haberse impuesto la tarea de revalorar su cante, protegerlo y premiarlo a quien sabe «decirlo».",
    "title": "Las Peteneras",
    "periodical": "candil",
    "issue_id": "1982-01",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 408,
    "article_char_count_full": 2471,
    "article_char_count_review": 2471,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-01-20-left-quienes-fueron-los-maestros",
    "article_text_for_review": "Selecciona Rafael Valera\n\nIGNACIO ESPELETA\n\nNació en Cádiz, en 1871. Hijo de Ignacio Espeleta y María Madrugón —de la familia de los Monje, dadora de grandes artistas, como Rosario, «La Mejorana», y Pastora Imperio, fue bautizado en la catedral vieja, que ubicaba la parroquia de Santa Cruz, con el nombre de Ignacio Tomás de Villanueva, teniendo como madrina a la magnífica Juana Vargas. Casó tardíamente —25 de julio de 1927—, a los cincuenta y seis años, con María Patrocinio Delgado. Su hijo José, nacido en 1902, fue autor de unas magníficas grabaciones en Hispavox, «Nochebuena en Cádiz».\n\nIgnacio Espeleta destacó por su majaza y admirable compás en los cantes por bulerías y alegrías, de los que fue intérprete señero; cantes a los que incorporó el entonado y onomatopéyico «tarantantrán», muy adecuado, desde el punto de vista técnico, para «cogerse a la guitarra». Igualmente, sus tientos fueron muy personales y repletos de pureza.\n\nLa figura de este gitano gaditano está ligada a las celebraciones de los famosos Carnavales de Cádiz, durante las primeras tres décadas de este siglo; así como el espectáculo «Las Calles de Cádiz», donde fuera descubierto, ya casi al final de su vida, por García Lorca para el folklore teatral. El propio Lorca diría de él «Hermoso como una tortuga romana» y «Hombre con una singular cultura en la sangre». La labor flamenca de su cante destacó en España y América, junto a figuras de la talla de Encarna La Argentinita, La Macarrona, La Malena y varios artistas más. La vida de este cantaor está jalonada por un sinfín de anécdotas, como la que anota Fernando Quiñones: «En un agasajo ofrecido por el Aeroclub sevillano a Ruiz de Alda, Rada, Durán y Ramón Franco, a poco del famoso vuelo a Buenos Aires del Plus Ultra (1926), una ocasión magnífica donde prodigó su arte, se destapó como creador de un discurso dirigido a la concurrencia y a los aviadores, en la cual abundaron anécdotas disparatadas, palabras incomprensibles y, en suma, tras el enfatizado formulismo oratorio, un reluciente, majestuoso cachondeo». En otra parte de «Cádiz y sus cantes», relata el prestigioso poeta y flamencólogo chiclanero: «Cierta taberna que abrió en Cádiz al gitano cantaor, un admirador suyo, en la intención de ayudarle, hubo de cerrarla a los cuatro días porque se la habían bebido y comido Ignacio y sus amigos». Igualmente es conocida su fama y jactancia de ser poco trabajador; Lorca referiría en una de sus conferencias la confesión del cantaor: «¡Pero cómo iba a trabajar, si soy de Cádiz!». También Fernando Quiñones relatará: «...en sus andanzas por la tienda del “Matadero” donde pasaba horas y horas, Ignacio mostraba sus manos a cuantos quisieran o no quisieran verlas, sobre todo cuando había cerca alguien muy metido en un trabajo pesado, y se jactaba alegremente, como el Tío de la Tiza: —¡Mira estas manos, vírgenes de currelo!».\n\nIgnacio fue el más importante de una larga dinastía gitano-gaditana, que dio numerosos flamencos y toreros. Falleció el cuatro de diciembre de 1938, a los sesenta y siete años. A él han sido dedicados numerosos homenajes, siendo el más destacado, a mi parecer, el celebrado en agosto de 1973 en el Puerto de Santa María, en la III Fiesta del Cante de los Puertos.\n\nResuelva sus asuntos por mediación de:\n\nGESTORIA\n\nCorrea Weglison, 2 Teléfs. 231578 - 233919 - 233064\n\nJ A E N",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1982-01",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 561,
    "article_char_count_full": 3357,
    "article_char_count_review": 3357,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-01-20-right-un-inmenso-catalogo-cantaor",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nQue Alianza, una de las editoriales más serias y de mejor distribución de nuestro país, dé entrada en sus fondos a un libro de temas flamencos, es algo que, sin lugar a dudas, nos reconforta en cuanto es medida objetiva de que los prejuicios editoriales van, de algún modo, desapareciendo. Pero este paso de las ediciones cuasi artesanales a las grandes editoras —anotemos el pionerismo de Revista de Occidente y Espasa Calpe—, junto a la valoración intrínseca que denota, también es muestra, no nos engañemos, de que existe un mercado amplio con inequívocas pruebas de interés por esta importantísima parcela de la cultura popular. ¿Qué más de un aficionado —por fin— se ha dado cuenta de que no es tan malo leer, que aquí hay mucho de snobismo y más de desconocimiento, que «la cosa» está de moda,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"editora\"]\n\npor esta importantísima parcela de la cultura popular. ¿Qué más de un aficionado —por fin— se ha dado cuenta de que no es tan malo leer, que aquí hay mucho de snobismo y más de desconocimiento, que «la cosa» está de moda, que la heterodoxia ahoga? Bueno, el caso es, que los estudios sobre flamenco se prodigan, que bastantes diarios mantienen páginas semanales dedicadas al tema monográficamente, que existen dos revistas especializadas y hasta una editora con catálogo exclusivo... vamos, que el cante, en contra de la rotunda aseveración de Joaquín el de la Paula a Eugenio Noel, va entrando en el papel. Claro, otra cosa sería cuestionarse si merece la pena seguir talando bosques para imprimir «flamenquerías». Personalmente, apuesto por el sí, aunque con muy serias y largas matizaciones; pero de esto no voy a ocuparme ahora, como tampoco de esas palabras apuntadas: mercado, masificación, popularismo, etc. Queden para próxima ocasión. Por Manuel Urbano Centrándonos en el libro de nuestra reseña, lo primero que prende la atención es su título: «Historia del cante flamenco» (1), un rótulo que, hasta ahora, no figuraba en la ya amplia bibliografía jonda, quizá, por pudor, respeto o manifesta impotencia individual ante un tema tan descomunal, difícil y repleto de lagunas. Un título, insisto, que reclama todo y que, como cualquier aficionado sabe, resulta prácticamente imposible de abordar hoy en solitario, por cuanto requiere una legión de estudios científicos —no ya de esa rara avis a la que han dado en llamar flamencólogos— de las más distintas parcelas del saber. Porque del cante, pongámonos la mano en el pecho, no sólo hay que desvelar ingentes nebulosas que determinan sus orígenes y realidad musical, sino revisar montañas de datos, noticias, biografías, etc., que se vienen suc\n\n[ENDING CONTEXT]\n\nbibliografía que cierra el volumen, a mi parecer, no es suficiente.\n\nPero hay algo más a exponer a favor del libro. Este inmenso catálogo, contra lo que pudiera parecer por lo hasta ahora dicho, está lleno de juicios de interés de Mairena, Juan Talegas, Manolo Caracol, etc., que el autor sabe perfectamente dibujar con los suyos propios para ofrecernos lo que a la postre interesa: una viva e interesante historia de los protagonistas del cante, lo que desde aquí agradecemos a Alvarez Caballero, autor de un volumen al que auguramos larga vida en la fundamental bibliografía flamenca. Enhorabuena.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Un inmenso catálogo cantaor",
    "periodical": "candil",
    "issue_id": "1982-01",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1068,
    "article_char_count_full": 6520,
    "article_char_count_review": 3433,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "editora"
      }
    ]
  },
  {
    "article_id": "1982-01-21-right-flamenca",
    "article_text_for_review": "Título: SEVILLA Y TRIANA.\n\nIntépretes: EL RUFO, con la guitarra de PACO CEPERO.\n\nCasa discográfica: Belter - Referencia 2-37-034. Año: 1981.\n\nPensamos que al lector no-habrá pasado desapercibido el recato con que efectuamos el equipo «DOSCAN-DIL» las críticas de la reciente discografía; recato que, nos apresuramos a decir, de modo alguno supone retraimiento de la verdad de nuestro parecer, desde luego, subjetivo, aunque sopesado. Y este cuidado es lo más ajustado posible, cuando se trata de un artista que comienza, por razones obvias: estamos ante una primera grabación y un largo y futuro. Cierto que el tiempo consolida a un joven cantaor; pero no es menos cierto que no pocas primeras salidas le condicionan —y de ello hay múltiples ejemplos para mal— de por vida.\n\nEn nuestras manos una grabación de un joven cantar, El Rufo, del que no poseemos antecedentes algunos, aunque intuimos que su procedencia bien pudiera ser trianera. Estamos, pues, a nuestro parecer, ante un primer disco de alguien que se inicia y con una grabación que no esconde sus aires comerciales; los dos cantes que abren una y otra cara de «Sevilla y Triana» no tienen un sentido distinto. ¿Por intereses de la casa grabadora a fin de intentar una buena realización? No nos cause sorpresa; esto, desgraciadamente, suele pasar incluso con artistas ya consagrados y suficientemente conocidos por el aficionado flamenco. Artistas que, una vez cogido cierto nombre —y esto no es popularidad—, lo incrementa a base de cantes fáciles de consumir y con los lógicos beneficiones económicos para él y para todos los que giran en torno al disco.\n\nAl margen de los dos cantes anteriormente comentados, que en la carpeta rezan como tangos —nada más lejos de la realidad— y que, incluso, llevan orquestación, se advierte en las restantes grabaciones que El Rufo tiene interés en agradar al escuchante a la hora de darse a conocer fuera de su entorno. Incuestionablemente, el disco contiene una buena grabación de fandangos de Huelva —lo más destacado—, concretamente, de Pérez de Guzmán, y en los que El Rufo se deja influenciar muy acertadamente por el último —por ahora— de los buenos intérpretes onubenses: Paco Toronjo. Asimismo, demuestra afición a la hora de realizar los cantes por soleá, alegrías, tientos y malagueñas; aunque, igualmente, la comercialidad es detectable fácilmente en las bamberas.\n\nEn relación con la guitarra que le «acompaña», nos hacemos una pregunta, por cierto, nada nueva: ¿Por qué estos artistas, con inequívocos visos de comercialidad, aparecen asistidos de la guitarra de Paco Cepero? Y a esta pregunta cabe agregarle otra: ¿Por qué todos ellos interpretan las letras escritas por el mentado guitarrista?\n\nNosotros, por ahora, no aventuramos contestación a las interrogantes. Tal vez el lector aficionado tenga alguna más concreta y decidida que la nuestra.\n\nFinalmente, si nos apretaran al resumen, hemos de decir que el disco de nuestro comentario, si no incluyese cantes inequívocamente comerciales, sobre todo los tangos —¿lo son?; preguntamos si son tangos, lo de cosa comercial es, a nuestro juicio, aplastante— que abren las entradas del disco —claramente primera audición—, tendría cierto interés para el aficionado al cante flamenco; porque El Rufo, a pesar de su estilo con teñidos «acamaronados», demuestra —cuando quiere, o cuando puede— que vive y siente el cante e, incluso, sabe escuchar a los grandes maestros de nuestro arte —no se nos oculta que algunos tercios de su soleá recuerdan la mítica figura de Tomás Pavón—; de aquí que no entendamos ciertos recortes a su capacidad artística.\n\nAnte una grabación como la que nos ocupa, sólo nos queda la decisión del cantaor: que concrete el palo al que ha de agarrarse, si al de la orquesta y vano jipío, adornado de facilones aplausos y fáciles pesetas, o a los «palos» durísimos e irrenunciables del único y auténtico cante.\n\nDOSCANDIL\n\nRAFAEL RAMOS ANTUNEZ, «EL GLORIA»",
    "title": "Discofrafía Flamenca",
    "periodical": "candil",
    "issue_id": "1982-01",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 637,
    "article_char_count_full": 3939,
    "article_char_count_review": 3939,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
