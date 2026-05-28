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
    "article_id": "1982-09-7-right-unos-recuerdos-para-antonio-mair",
    "article_text_for_review": "Por Fernando Quiñones\n\nAl que hoy es amigo empecé a conocerlo, muy chaval uno, en el madrileño «Villa Rosa», de la plaza de Santa Ana. Estaba, como tantos otros artistas, esperando trabajo entre aquella última supervivencia de la densa vida flamenca que conoció Madrid. Eran los primeros cincuenta (¿el cincuenta y tres acaso, Antonio?). Bien plantado, con el paso sereno y la apostura que todos le conocemos, la mayor corrección en el cante y en el trato a sus fugaces contratadores en «Villa Rosa», eran entonces cuanto el cantaor tenía —y ya era mucho—, porque su angosto, precario vivir de aquellos tiempos lo traía un tanto achantado y desanimado, como un pájaro volando con una piedra entre las alas. Bien que me acuerdo.\n\nVino luego el Mairena del ballet español de «Antonio», su estirón profesional y económico, el brusco salto al prestigio que le correspondía y que tantos años había tardado en llegar. Pero este era sólo el verdadero comienzo. Antonio Mairena aún no había ultimado su estatura y su significación enteras. La última verdad, el papado flamenco de Mairena, vendrían muy poco después, pausados y seguros como sus andares mismos, para crecer y crecer hasta hoy.\n\nO mucho me equivoco, o eso que llaman «duende» fue lo último en acudirle. Como en un justo reparto de bienes, el duende flamenco no suele acompañar a las voces muy poderosas ni a los saberes demasiado rigurosos y acendrados: las dos prendas fuertes del maestro. Debía sobrevenir la pelea con el cante —sobre todo, con algunos cantes—, la hermosa y dramática batalla en la que, como el protagonista de El viejo y el mar, hay que echarle a la cosa todas las fuerzas que se tienen y también las que no se tienen, para que acabará resplandeciendo, en la voz de Antonio Mairena, el misterio hiriente del duende. Compatible, en el solo caso que uno conoce, con la mayor ortodoxía interpretativa.\n\nUna noche entre las noches, y no en cuarto de cabales ni en amplia reunión de amigos como otras tantas, sino en el atestado Teatro de la Zarzuela, durante el festival de homenaje a Juan Talega, llegó a todo por seguiriyas el cante de Antonio Mairena. Fue entablándose un verdadero diálogo de su voz con la guitarra estimulada y estimulante de Manuel Cano. Palabras, ayes y cuerdas se provocaban y entendían, buscaban y encontraban las zonas más puras, inesperadas y conmovedoras. Eso que los directores de orquesta y de teatro llaman el «tempo», dosificadas síntole y diástole de la emoción cantaora, mezcla sorprendente de inocencia y sabiduría, de esponteneidad y reglamentación, de obligaciones y libertades fluyentes y concertadas, quedaba claro como el agua allí, contagiándose a todos, pero no para partitura «culta» ni para elevado texto literario, sino para un salpicado y grandioso dolor andaluz puesto en pie por geniales analfabetos de Triana, de Cádiz, de Jerez, de Sanlúcar, hace cincuenta, ochenta, cien años...\n\n—Antonio —le dije en Sevilla y en junio del 81—, esa seguiriya grabada que has atribuido al «Fillo», es tuya.\n\n—Bueno —me contestó—: yo recogí un hilo que andaba por ahí, en labios de una gitana vieja. Un hilo limpio y de confianza.\n\n—Pero lo que nos entregaste fue un mantel.\n\nYa en mi libro «El flamenco, vida y muerte» (1) reproché un poco a Antonio Mairena esa atribución indebida, pero no por falsa ni muchísimo menos, ya que su legitimidad, belleza y aire arcaico del mejor cuño quedan fuera de duda, sino porque una aportación declarada del «Pontífice» a un «palo» tan cabal como el de las seguiriyes, hubiera contribuido quizá a animar el poder creativo de otros cantaores y, así, a aumentar el viejo acervo del repertorio musical flamenco, casi congelado hoy, y mucho más en el cante que en el baile o en la guitarra. Es el respeto tabú a los antiguos maestros (en cuya constelación tiene Antonio Mairena ya, desde ahora, su sitio) y es el cambio de tiempos y circunstancias, quienes sostienen esa congelación. Pero los antiguos no eran dioses y también hoy se puede crear.\n\nLa lista de cantaores enciclopédicos, cuantiosos abarcadores de épocas y de estilos, no es muy larga que digamos. Contó ayer, sucesivamente, con un Silverio Franconetti y con un Antonio Chacón. Cuenta hoy con Antonio Mairena. Son tal vez los tres ejemplos más firmes e invocables de vocaciones cantaoras con largo alcance historizador. Iluminan y ensanchan. Crean escuela. Y reparemos en que, de los tres nombres, Mairena es el solo gitano. Otros «enciclopédicos» menores, Pepe de la Matrona, por ejemplo, caen también del lado payo. El temperamento gitano, individualista en sumo grado y replegado en el terreno flamenco, a un instinto y un hacer hereditarios más bien, congénitos, amplía campo en la figura de Mairena, no menos dueña de esas virtudes pero abierta también a una fecunda curiosidad de conocimiento y de recopilación, tan evidentes en su famosa antología discográfica o en su participación en el libro «Mundo y formas del cante flamenco». Cobra así la figura de Antonio Mairena un interés y un carácter históricos realmente especiales. No es sólo el gran cantaor que es, sino además, y como ya he escrito de él en otra ocasión, espina dorsal de la andante flemencología de hoy.\n\nAPERITIVOS SELECTOS Especialidad en\n\nPLANCHA\n\nMesones, 18 Teléf. 23 40 46\n\nJ A E N",
    "title": "Unos recuerdos para Antonio Mairena",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 884,
    "article_char_count_full": 5264,
    "article_char_count_review": 5264,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-8-right-mairena-y-la-guitarra",
    "article_text_for_review": "Aunque la tarea es difícil, porque de Antonio Mairena se ha escrito mucho, se han dado muchas opiniones, quizás, más acertadas y objetivas que las que yo pueda elaborar en este breve comentario, nunca más honda la satisfacción de escribir sobre la persona de Antonio Mairena en esta revista, dentro de este número monográfico, ante cuya petición me atrevo a hacerlo de forma muy particular y llevado de ese cariño que me merece mi gran amigo.\n\nPor Manuel Cano Tamayo\n\nTitularía este comentario: ANTONIO MAIRENA Y LA GUITARRA. De todos es bien conocido, que en el arte del cante y la guitarra, a veces por años de convivencia e indudablemente para una mayor y más perfecta conjunción y expresión artística, nació ese hermanamiento entre el cante y la guitarra que nos ha legado la historia sonora del flamenco. Sirvan como ejemplo: «Antonio Chacón —Ramón Montoya; Manuel Torre—, Miguel Borrull; Pastora Pavón \"Niña de los Peines\", en sus principios, con Luis Molina; Curro el de la Geroma, Manolo el de Badajoz; Manuel Serrapí «Niño Ricardo», hasta su última época con Melchor de Marchena; Manuel Vallejo con el Niño Perez o con Manolo el de Huelva», y así muchas más conjunciones que están en la memoria de todo buen aficionado.\n\nPero de Antonio Mairena, cuya plena incorporación artística nace en una época distinta en que el flamenco toma otro giro, otra visión diferente, porque se proyecta y programa de distinta manera: nacen los Concursos Nacionales en Córdoba —donde obtiene la llave de oro— y organizados no sólo a través de Andalucía, sino de toda la geografía de España los Festivales Flamencos, y es entonces cuando hay que luchar con una participación activa en ellos de forma exhaustiva y continuada, encontrándose en cada momento con una guitarra, con un guitarrista diferente; guitarrista al que Antonio Mairena con su «son», con su gesto, con sus palmas y, a veces, con su baile ha sabido llevar a esa perfecta conjunción para un mayor éxito de su espectáculo, la redondez de su cante. Así lo escuchamos con Manuel Morao conviviendo muchos años de su trabajo, cantando para bailar en el ballet de Antonio. Con Eduardo el de la Malena, en aquella magistral lección por soleá al ilustrar una conferencia de Ricardo Molina en la Primera Semana de Estudios Flamencos en Málaga, o ¿cuántos nombres de guitarristas pasaron junt\n\nto a él por aquella venta de Manolo Manzanilla, o por el tablao «El Duende» de Pastora Imperio en Madrid? Creo imposible poder reseñar en tan breve espacio las vivencias de este gran hombre y artista contadas en tantas reflexiones como hemos vivido juntos.\n\nEste preámbulo quiere llevarme solamente a unas consideraciones que como guitarrista, como aficionado observador y sobre todo como ferviente admirador y amigo de aquél que en muchas ocasiones —aún no siendo mi habitual actividad— me brindó el honor y la responsabilidad de acompañarle; me sugieren en este momento.\n\nLa personalidad de este hombre que, cuando sentado a tu derecha, lanza esa frase cariñosa pero a la vez imperativa, quizás surgida y acopla-da para cada momento... «Vamos, maestro», o «vamos allá...», y pide el concurso de la guitarra, produce ese respeto, ese incipiente temor al ritmo, a la velocidad, a lo que es iniciarse; temor que él acalla, tranquiliza, resuelve con el gesto, con una mirada, son su «son» (sus palmas sordas) y lanza un óle de aceptación y jaleo a una falseta, para luego ser placer, éxtasis y entrega absoluta. Su cante es una continua lección de compás, medida y sabiduría que no hay más que seguir, dejarse llevar, mecer y cerrar, o rematar en sus tiempos.\n\nTraería a este momento el ejemplo que en mi conferencia «Los Toros y sus suertes en la guitarra» señalé hace unos años en la Peña de los de José y Juan en Madrid, al comparar el ilustrar con la «solea» la suerte del toreo de muleta y sus tres tiempos: «Parar, Templar y Mandar», que no es más que ajustarse a la suerte y a su perfecta velocidad. Esto puede ser el cante por soleá de Antonio Mairena, aunque invertiría alguno de los términos, diríase mejor «Pensar, Templar y Cantar»; el resultado, una faena memorable por Alcalá, por Frijones o por la Serneta... ¡por dónde usted quiera maestro!\n\nTodo esto no's lo ha demostrado Antonio Mairena en el transcurso de su dilatada vida artística, basta con sólo recordar sus innumerables grabaciones a través de sus discos, donde ha dejado esa inconmensurable obra de estilos y variantes en todos los palos del flamenco a compás, llámémosle «Cante Gitano Andaluz» o, simplemente, «Cantes de Antonio Mairena», donde supo unirse, entre otras y como más asiduas a las guitarras de Melchor de Marchena y a la de Manuel Serrapí Niño Ricardo en LA GRAN HISTORIA DEL CANTE GITANO ANDALUZ, EL CANTE DE JEREZ, CANTES FESTEROS, LA FRAGUA DE LOS MAIRENA, etc., etc., todos lecciones de sabiduría y bien decir en el cante unido a la guitarra que, en todo momento, le supo dar lo que su cante y espíritu reclamaba, porque así es, a mi entender, la única manera en que está y estará por siempre regido el arte flamenco.\n\nQuiero terminar rindiendo tributo de homenaje y admiración para este artista y gran amigo que, tanto a mí como a todos los que viven en su intimidad y en su espíritu el cante flamenco, sólo ha dejado en el transcurso de su vida y su arte un pozo de sabiduría y de honrada enseñanza.",
    "title": "Mairena y la guitarra",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 914,
    "article_char_count_full": 5333,
    "article_char_count_review": 5333,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-9-right-arte-sentimiento-y-cultura",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Carlos Almendros\n\nSi el flamenco interesa a los espíritus cultivados como fenómeno de Cultura, como manifestación de Arte, o bien como expresión del Sentimiento, a buen seguro que no podríamos encontrar una figura con más cualidad y merecimientos que Antonio Mairena para encarnar las tres características señaladas. Su voz, su talante, su sensibilidad y maestría, su buceo constante en pos de la autenticidad de esta singular manifestación de cultura, hacen de Mairena el Caballero Andante del Flamenco.\n\nEl que esto escribe ha tenido la gozosa oportunidad de poder captar, por encima de discos y de notas biográficas, un poco del «alma» de Mairena, y quiere, como el mejor homenaje a su figura, dar a conocer las vivencias de mis contactos enriquecedores. Como pórtico que enmarque y nos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nen de Mairena el Caballero Andante del Flamenco. El que esto escribe ha tenido la gozosa oportunidad de poder captar, por encima de discos y de notas biográficas, un poco del «alma» de Mairena, y quiere, como el mejor homenaje a su figura, dar a conocer las vivencias de mis contactos enriquecedores. Como pórtico que enmarque y nos introduzca en la singular figura de Antonio Mairena, expondremos los datos biográficos más significativos. Sobre el arte «cantaor» de Mairena se dio una ilustre premonición. El insigne Manuel Torre, monstruo sagrado del Cante en el primer cuarto de siglo, la «voz de trueno» de la Siguiriya, de quien García Lorca dijo que era «el artista que más cultura llevaba en la sangre», cuando las gentes acudían a escucharlo en las postrimerías de su vida, les decía, con voz angustiosa ya próxima a extinguirse: «Yo ya no pueo»... Hay un niño en Mairena que será figura. «Ir» a ver a ese muchacho y «escucharlo de cantar en la tasquita». Hacía referencia con ello Manuel a la pequeña taberna que el padre de Antonio había abierto en Mairena. El muchacho, Antonio Cruz García, nace de familia gitana en Mairena del Alcor (Sevilla), el 5 de septiembre de 1909. La familia poseía en el pueblo una fragua, la célebre «Fragua de los Mairena», que habría de ser la escuela primera, dura y «honda» del chaval. Mientras éste corretea por la fragua, va oyendo y asimilando los sones recios que allí se prodigaban. No tardó en asombrar a la familia y a los paisanos que pudieron comprobar lo bien que se le daban los sones bravos y profundos de los c\n\n[ENDING CONTEXT]\n\nFlamenco auténtico. Su seriedad y concentración son todo un poema, que no necesita comentarios.\n\nEn Mairena tiene el Flamenco el eslabón entre los viejos estilos y el Arte actual; siempre bajo el signo de la autenticidad. En lo que el Cante siempre fue, y puede seguir siendo, se ha de hacer referencia obligada a Antonio Mairena.\n\nPor ello, su pueblo, haciéndose eco del común sentir, le ha dedicado un monumento, el monumento al Flamenco, elevado por nuestro artista a las más altas cumbres.\n\nPolig. Industrial «Los Olivares», C/. Mancha Real, 6 Teléfs. 22 91 00 - 22 91 04. Part. 23 30 29\n\nJAEN\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antonio Mairena: arte, sentimiento y cultura",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 1449,
    "article_char_count_full": 8476,
    "article_char_count_review": 3185,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  },
  {
    "article_id": "1982-09-11-left-mediaocena-de-repentizaciones-pa",
    "article_text_for_review": "A Paco Vallecillo y Manolo Urbano\n\nAntonio Mairena sueña con darle al cante destino y el cante tiene por seña no salirse del camino que la misma vida enseña.\n\nAntonio Mairena siente, abre la boca y escribe: este es mi cante valiente, un gitano que revive cuando lo mata la gente.\n\nAntonio Mairena clama el dolor de su ralea haciendo cierta su fama de morir en la verea hecho tierra y hecho llama.\n\nAntonio Mairena suena a yunque y campo en celo, mete la voz en la trena cuando la pone en el cielo como sangre por la vena. Antonio Mairena reza en cada cante que faja, lo remonta en su grandeza, mide el tercio, lo baraja y no pierde su pureza.\n\nAntonio Mairena lleva una fragua en la garganta para forjar siempre nueva la vieja copla que canta desde el fondo de su cueva.\n\nManuel Ríos Ruiz Madrid y madrugada, 10 de julio de 1982\n\nTEATRO LOLITA\n\n¡Gran acontecimiento artístico! ¡Especialculo monstruo! Hoy domingo 31 de agosto de 1924\n\nBENEFICIO DE\n\nCASIMIRO GONZALEZ\n\nen el que tomarán parte la Rondalla Eilarmónica Mairenense clementos de esta ciudad.\n\nORDEN DEU ESPECTÁCULO\n\n1. Se exhibirán en el Cine, dos películas cómicas en dos partes cada interpretadas por\n\nSANDALIO Y TOMASIN\n\nPEDRO VILARDON\n\nEL NUMERO 100\n\nen cuya interpretación tomarán parte Justo Morillas, José Sánchez, Diego García, Agustín Sánchez, Manuel Acosta, Casiño González y Marcelino Cali (El Núñi), encargado del papel del torero. 4.° Presentación del buen aficionado\n\nque cantará acompañado de la Rondalla Filarmónica los números siguientes: Gupíes cómicos, fox-trot de «La Monterría» y canción veneciana de la zarzuela «El cetro del Sol» 5.° Número sensacional, número especial, número sin igual, número valiente Valiente númerol... Presentación de la despampanante Troupe Anglo-Húngara-Iberica\n\nLos Italianos...\n\nLa componen cinco escalotriantes artistas, que yendo de paso para Ganda, (Méjico), se han ofrecido a tomar parte en esta función, donde darán a conocer sus maravillosos bailes rusos acompañados por la Rondalla fílarmosica. Sastrería parisién — Presentación regia — ¡Valiente número!\n\nANTONIO CRUZ\n\nado el carácter del espectáculo y a ruegos del beneficiado, se ha ofesinteresadamente a cantar lo mejor de su repertorio, acompañado p\n\nPRECIO: UNA PESETA\n\nNOTAS: Dada la duración del espectáculo, comenzará a seccion cinematográfica a las 11 an. punto. Si una vez empezada la función se suspendiera por causas agenas a la Empresa, el público no tendrá derecho a reclamación alguna. ¡Todo Mairena al Teatro! ¡Espectáculo emocionantel IRISA CONTINUA! ¡Todos al beneficio de CASIMIRO GONZALEZ!\n\nFormidable Acontecimiento para el domingo 2 de julio A petición del público reaparecerá el gracioso parodista\n\nel Majzen, Kaides y Moros Notables del Protectorado Español\n\nOTERO. AGRUPACIÓN. (Bailes andaluces)\n\nANITA SEVILLA y RAFAEL ORTEGA (Canciones y Bailes)\n\nMANUEL VALLEJO. (Arte flamenco) acompañado a la guitarra por ANTONIO MORENO y la célebre bailaora LA MACARRONA\n\nZAMBRA GITANA. (Fiesta jerezana)\n\nActuarán elementos de la ORQUESTA BETICA DE CAMARA\n\nREALITO. TRUP. (Bailes andaluces)\n\nNIÑO MAIRENA. (Cante flamenco) acompañado a la guitarra por JOSE GUTIERREZ\n\nA las diez y media de la noche\n\nSevilla 13 de octubre de 1936\n\nM. 992. Imp. Municipal, Sevilla. 10-996\n\n4 únicos días, 4\n\ndel 16 al 19 Diciembre de 1943\n\nPatrocinio Rico - María Luisa de la Vega - Antonia Guijarro Argentina Moral - Sara de Mesa - Antofita Nestares María Ester - Antofita Ramírez - Elenita Giaci Carmen Fuentes - José Pozo - Emilio Guillén Eduardo Ferrer - Narciso Ojeda Niño de Mairena - Paco «Laberinto» Melchor de Marchena Miguel ae los Reyes Andrés Heredia Pepe Ortiz\n\nMaestro director: MANUEL CORONADO\n\nAndrés Martínez\n\nRegidor Maquinista Teofanes Merchán Anselmo Alonso\n\nTerremoto\n\nOrganizador Representante ANTONIO VIVES MIGUEL REINA Vestuario: Talleres Raula, Madrid\n\nFigurines: José Cabaliero\n\nCortinajes: Viuda de López y Muñoz",
    "title": "Mediaocena de repentizaciones para don Antonio Mairena",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "11-14",
    "page_number": 11,
    "word_count": 613,
    "article_char_count_full": 3905,
    "article_char_count_review": 3905,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-14-right-ejemplo-a-seguir",
    "article_text_for_review": "Por Luis de Córdoba\n\nQué edificante y enriquecedor sería para el Flamenco de épocas venideras si todo cantaor, que quiera ser, se fijara como meta llegar a conseguir el prestigio y la trascendencia artística de Antonio Mairena. Creo que han sido muy pocos, a través de la Historia, los que han logrado tan alto nivel y casi podría asegurarse también que en adelante serán muy pocos los que lo consigan. Y es que harto difícil me parece a mí alcanzar tan indiscutible liderazgo artístico, por más que uno se lo proponga, pues en casos tan especiales como este de Antonio Mairena concurren siempre una serie de cualidades personales y circunstancias históricas, todas coincidentes, que no suelen darse con frecuencia ni en las personas ni en el tiempo.\n\nEl prestigio de Antonio Mairena es tal que, puede decirse, ha permitido el fenómeno para mí curioso, en vida de un artista, de que se llegue a valorar, en notable medida, a sus imitadores (observese que no digo recreadores, que en estos siempre cabe la posibilidad de que aporten algo con valor positivo, artísticamente hablando, para el Flamenco), algo imperdonable, diría yo, si la imitación se hiciera a cualquier otro cantar actual; de sobra sabemos lo mal vista que está la imitación (y con razón, dado su escaso valor artístico), por muy buena que sea la copia, pero aún más si el imitado es alguien a quien se puede escuchar personalmente. En el caso que nos ocupa, el prestigio del Maestro y el alto valor de su obra actúan como un gran manto que cubre toda posible censura, al imitador, que no sea, estrictamente, la de su imperfección en la copia.\n\nEs natural, lógica y necesaria, a mi entender, la influencia de los Grandes Maestros en los cantaores jóvenes; pero esta influencia creo que no debe sobrepasar el aprendizaje y conocimiento de los cantes y su técnica; después ha de venir —si se es artista de verdad—, la propia aportación personal a lo aprendido; y no quisiera, naturalmente, que se me interpretara por aportación personal un relajamiento tal que permita «tirar por los cerros de Ubeda» sin ton ni son; no; para mí la aportación personal está, a partir de las formas clásicas, en la propia expresión o la impronta personal que cada uno sea capaz de imprimir a dichas formas; para ello todos sabemos que cada cantar cuenta con sus propias características de voz, inteligencia, gusto estético, etc.\n\nDecía al principio que Antonio Mairena debería ser el gran ejemplo a seguir por los cantaores jóvenes; hay razones, más que de sobra, por todos conocidas, que justifican tal ejemplaridad; no quisiera caer en la repetición manida y fácil del adjetivo adulador hacia el Maestro; sí me es necesario dejar constancia aquí de la gran admiración, el profundo respeto y el afecto que por él siento. Asimismo quisiera destacar en estas líneas algo de lo mucho que, a mi entender, debe el Flamenco a Antonio Mairena: Ahí está, por ejemplo, su gran labor de recopilación, por todos conocida y admirada; ahí está también su gran personalidad artística con tan enorme carga de creatividad; y está, sobre todo, algo que tal vez sea lo que yo más le agradezca como cantaor joven que soy: Su gran labor en la dignificación de un Arte que todos sabemos cómo estaba y todos sabemos cómo, afortunadamente, está.\n\nEstoy completamente seguro de que cuantos formamos parte del Mundo Flamenco, nos sentimos muy orgullosos de Antonio Mairena.\n\nGracias, Maestro.",
    "title": "Ejemplo a seguir",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 579,
    "article_char_count_full": 3414,
    "article_char_count_review": 3414,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
