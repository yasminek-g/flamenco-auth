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
    "article_id": "1987-09-8-right-salvador-rueda-y-el-modernismo-a",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSalvador Rueda consideraría a la siguiriya como la más expresiva estrofa castellana.\n\nSalvador Rueda y el modernismo andaluz\n\nuando el nicara- guíense Rubén Da- río, uno de los más importantes poetas hispánicos de todos los tiempos, despreciaba al malague- ño Salvador Rueda, afirmando: «Yo, que lo he criado poeta», no solamente reflejaba el orgullo y la seguridad de quien se siente triun- fador en el desempeño de cual- quier actividad humana, sino que cometía una de las más notorias injusticias que pueden detectarse en el panorama literario de los cien últimos años. En efecto, no sólo podemos afirmar que se trata de dos personalidades poéticas distin- tas, lo que resulta evidente en el análisis de su obra, sino que ade- más Rueda mantuvo siempre una altiva independencia, aunque, eso sí,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"reconocía\"]\n\na, sino que cometía una de las más notorias injusticias que pueden detectarse en el panorama literario de los cien últimos años. En efecto, no sólo podemos afirmar que se trata de dos personalidades poéticas distin- tas, lo que resulta evidente en el análisis de su obra, sino que ade- más Rueda mantuvo siempre una altiva independencia, aunque, eso sí, plagada de respeto, hacia la egregia figura de Rubén a la que admiraba profundamente y en quien reconocía, como más tarde harían todos, al gran escritor ca- paz de acaudillar al movimiento más renovador de la lírica de fina- José Luis Buendía López les del siglo XIX, que conocemos como Modernismo. La falsedad del aserto de Rubén se pone de manifiesto de igual modo en el hecho de que Rueda, nacido en Málaga en 1857, creó gran parte de su obra con anterioridad a la del nicaraguíense, incluso de la venida de éste a España, que se produjo en 1892, representando tativos (Prosas profanas, es de 1896 y Cantos de vida y esperanza, de 1905), por lo que no es descabellado suponer que el malagueño, que aparte de la obra citada era ya autor de otras como Aires españoles (1890) y Cantos de la vendimia, de 1891, no sólo no fue iniciado poéticamente por el monstruo Rubén, sino que, posi- Cultivador apasionado de la forma en el flamenco, escribiendo gran cantidad de soleares de tres versos o soleariyas, sin olvidar la solemne soleá de cuatro versos. Rubén a su país en el centenario del Descubrimiento del Nuevo Continente. Cuando el vate americano llega a España, Salvador Rueda le presenta un libro ya acabado, En tropel, que anuncia las innovaciones del Modernismo rubeniano mucho antes de que éste escribiera sus títulos más represen- blemente, muchos de los logros definitórios del movimiento, sus numerosas experiencias métricas, sus hallazgos rítmicos y sobre todo sus alardes ornamentales y cromáticos, tuvieron como principal descubridor a nuestro poeta. Debe hablarse, por tanto, y sin ningún tipo de complejos, de influencias mutuas entre los resortes poéticos de uno y otro autor. Si, finalmente, pudo imponerse el nicaragüense fue a causa de su mayor calidad y sobre todo de su potente cultura y cosmopolitismo (Rueda fue prácticamente analfabeto hasta los diec\n\n[ENDING CONTEXT]\n\nno hay espacio ya para cis- nes ni princesas. Ahora esta poesía ha bajado a la calle, se ha teñido con el barro cotidiano. Es otra faceta más de un poeta de diferentes registros que supo y que pudo compaginar la experiencia viva de sus gentes, de su tierra andaluzas, con los más altos vuelos culturales que, como vanguardia imperativa y bajo nombre de Modernismo, planearon sobre el país. A todos nos corresponde la tarea de impedir que tan poderosas actitudes artísticas caigan injustamente en el olvido.\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nTeléfono 25 37 47\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Salvador Rueda y el modernismo andaluz",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "8-10",
    "page_number": 8,
    "word_count": 2259,
    "article_char_count_full": 13505,
    "article_char_count_review": 3850,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "reconocía"
      }
    ]
  },
  {
    "article_id": "1987-09-10-right-flamenco-en-estado-primario",
    "article_text_for_review": "llega a Cantillana todavía el sol se eleva sobre las cumbres de la sierra norte y el ambiente límpido de la tarde permite la contemplación de una feraz y bien labrada vega que proclama la principal riqueza de la antigua Naeva de los romanos, importante puerto fluvial entonces, posterior Cantillana de los árabes, atalaya y fortaleza en el cruce de los caminos entre Córdoba y Sevilla. Tierra en el pasado de pescadores y barqueros —cabe la confluencia del Viar y el Guadalquivir—, el cronista mira hacia el horizonte serrano y evoca con Machado el Grande\n\nQué bien los nombres ponía quien puso Sierra Morena a toda la serranía...\n\nFrancisco Vallecillo\n\nLa entrada en la villa disipa estas evasiones por el campo del lirismo y nos sumerge en una población pulcra y acogedora, tan acogedora como sus gentes, a las que había tratado en ocasión de un festival flamenco años pasados. Ahora el cronista va a asistir a la clausura con fondo de cante flamenco, de una semana cultural que ya tiene el rango que da la tradición y el bien hacer. Un local amplio, un patio habilitado como sala de actos y un escenario improvisado, y público abundante —especialmente infantil— que nos hace temer lo peor. (Pero lo peor no llegó luego, pues los pequeños fueron los primeros en recogerse, en el silencio de siempre, y ausentándose a medida que el espectáculo iba avanzando). Silencio expectan Aquí debían haber seguido los nombres de los participantes, acompañados de una glosa bien merecida por cada uno de ellos. Los nombres no me fueron facili-\n\nte y respetuoso, silencio religioso, dicho sea para emplear un frecuente modismo, y organización perfecta. Hasta que poco a poco va apareciendo el cante. El cante está representado por artistas locales, aficionados sin pretensiones, en estado primario, sin grandes diferencias de capacidad flamenca y, curiosamente, sorprendentemente para el cronista, con un nivel más que aceptable cuando de aficionado se trata; y de aficionados sin pretensiones de divismo, rara a vis en este mundo en el que la modestia no suele brillar precisamente por su ausencia.\n\ntados y ni yo tampoco me llevé una breve nota recordatoria que hubiera dado a cada uno el mérito que le correspondía: mérito unido por un común denominador que puede definirse como gran afición, mucho respeto a los cantes (algo que hoy se echa a faltar) y una intervención que sobresalió sobre las restantes y fue reconocida por todos. Esta intervención, a cargo del aficionado de mayor edad, fue, en efecto, sobresaliente, y así mereció calificarse la flamenquísima Toná que puso aurifero broche a su actuación. Si alguna concesión fue hecha al cante menor, para uno fue —feliz coincidencia— motivo de especial satisfacción: escuchar con toda su enjundia trianera unos fandangos de El Peluso, aquella mezcla de artista bufo-flamenco, el que supo meter airosamente por bulerías la asturianada que canta al «Chalaneru, chanaleru». Mención de honor especial sea hecha al tocaor en rigurosa línea de excelente acompañante sin extravíos al uso.\n\nAquí y ahora estamos evocando este recuerdo como más que mínima y modesta contribución al honor que la Peña Flamenca cantillanera le ha hecho a uno con su invitación a esta celebración flamenca que se va a iniciar.\n\nHace un mes aproximadamente, en ocasión de la ponencia que Manolo Martín y uno mismo presentamos al IV Coloquio del Romancero en El Puerto de Santa María, escuchamos con atención un trabajo que refería la relevante importancia que, a juzgar por testimonios vivos y actuales de gitanas cantillaneras, tuvieron estos cantes de gestas (Romances, Corridos o Cantes de Correntío) en esta hermosa tierra. Tierra de barqueros naevenses, tierra de gitanos y cantaores, tierra hoy de un valiente torero, Manili; en ella nació también un cantaor que fue feliz intérprete de los cantes sanluqueños-gaditanos, las Cantiñas: Paco el Gandul o Paco el Bisté, que de las dos formas fue conocido a finales del siglo pasado. No estamos, pues, en tierra de infieles. Pero ese es otro capítulo que uno espera confiado que expliquen quienes con mucha más autoridad pueden hablar de cante y de toque.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al Mérito del Trabajo)\n\nRecepción diaria de MARISCOS Y PESCADOS ESPECIALIDAD EN ASADOS\n\nROLDAN Y MARIN, 7\n\nJ A E N\n\nTELEFONO 22 97 65",
    "title": "Una experiencia interesante en Cantillana: Flamenco en estado primario",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 710,
    "article_char_count_full": 4307,
    "article_char_count_review": 4307,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-09-11-right-cr-nica-del-xxvii-festival-nacio",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nD urante los días comprendidos en-\n\nDurante los días comprendidos entre el 2 y el 8 de agosto del presente año 1987 ha celebrado la XXVII edición del Festival Nacional del Cante de las Minas de La Unión. Una nueva edición se cierra con la entrega de premios, que ilusiona a unos y desencanta a otros que, a juicio del jurado, no han alcanzado la cota máxima de sus interpretaciones en aquellos cantes con los que llegaron a la gran final. Al final, aunque la procesión vaya por dentro —como reza el dicho popular—, todos contentos y con ánimos de seguir concursando en el festival minero de La Unión con la ilusión puesta en la consecución de la «Lámpara Minera», símbolo incandescente del «Carburico» que ilumina las oscuras galerías de las minas de la Sierra de La Unión.\n\nDOMINGO, 2 DE\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"interpretación\"]\n\nalzó el telón del festival con las actuaciones de las rondallas de Nuestra Señora de los Dolores de El Garbánzal y la rondalla y coro del Hogar del Pensionista de La Unión; una muestra del trovo y del cante, y la lectura del pregón a cargo de doña Génesis García Gómez. La doctora en Filosofía y Letras doña Génesis García Gómez, que en el pasado mes de abril obtuvo, con nota brillante, el doctorado con su tesis «Cante flamenco-Cante minero. Una interpretación sociocultural. (Datos lingüísticos, literarios, sociohistóricos y musicales para la textualidad y su pragmática)», pronunció un bello pregón que, aunque en el fondo constituye un profundo estudio enlazado entre lo literario, lo histórico y lo musical, llegó, en sus variadas escenas —partes del pregón— a conectar con el público asistente, porque aun cuando su «...pregón cantar lo hubiera querido como ayer se pregonaba..., para cumplir esta misión de pregonar el cante, cantando —decía Génesis— debería hacerlo, como era lo propio entre los pregoneros populares, tantos y tan decisivos en el propio origen y evolución del flamenco. Desde Tío Luis, que pregonaba su agua en Jerez, hasta el trágico Macandé, que estrenaba cada día el pregón de sus caramelos; desde las Mirris ha\n\n[ENDING CONTEXT]\n\naños 1985 y 1986. El ganador, el único cantaor que llegó a la final con una letra —copla— de «nuevo cuño», fue Antonio García Gómez «El Califa».\n\nCon la entrega de premios se baja el telón del Festival Nacional del Cante de las Minas de La Unión. Los últimos ecos del cante, del baile y del toque, con el «duende» flamenco morando en las galerías de las minas de La Unión, cierran una nueva edición del cante de las minas. Ahora el silencio embriaga la atmósfera del pasado, esa atmósfera densa del humo del viejo café cantante del Rojo el Alpargatero de principio de siglo en la ciudad de La Unión.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XXVII Festival Nacional del Cante de las Minas de La Unión",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1640,
    "article_char_count_full": 9846,
    "article_char_count_review": 2866,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "interpretación"
      }
    ]
  },
  {
    "article_id": "1987-09-14-right-el-fandango-de-huelva-y-su-provi",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFlamencas. Benalmádena, 1987\n\nDon José Pérez de Guzmán y Ursáiz.\n\nOnofre López González\n\nEn primer lugar quiero mostrar mi gran satisfacción por estar aquí como ponente y tener la oportunidad, tantas veces deseada, para hablar de los cantes de mi tierra. Agradezco a la organización de este Congreso Nacional la invitación amable de acercarme hasta vosotros para tratar de «to-rear», con mi modesto verbo, un toro —no en si bravo o manso— que muchos matadores, de los llamados buenos, se pusieron delante de él, sin que nadie le halla sabido nunca cortarle las orejas de la verdad. En este sentido, y por razones demostrativamente argumentadas, es del todo ineludible dar a esta ponencia un matiz reivindicativo para salir al paso de tantas mentiras y errores que, propios y extraños, dieron en sus\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"serio\"]\n\numentadas, es del todo ineludible dar a esta ponencia un matiz reivindicativo para salir al paso de tantas mentiras y errores que, propios y extraños, dieron en sus respectivas facetas, al tratar los cantes de Huelva. Unos, lo minimizaron tanto que pasaron desapercibidos, otros, los tomaron con total indiferencia y, los más, lo trataron en abundancia pero con el más total y absoluto desconocimiento. Por todo esto, quiero presentarles un trabajo serio y concienzudamente realizado, con ejemplos más que demostrables para que, de una vez por todas, intente servir como una ventana abierta a todos los interesados, sean estudiosos, artistas o simples aficionados, para que se asomen con humildad a esta rica veta musical que conforman los fandangos de Huelva y su Provincia. Para tal efecto, he dividido la ponencia en dos partes, que convergen entre sí. Una será la parte crítica a los estudios realizados y la otra puede ser didáctica, en una explicación de su posible historia. TRATAMIENTO DE LOS FANDANGOS DE HUELVA Y PROVINCIA POR DIVERSOS FLAMENCOLOGOS Y ARTISTAS Es del todo increíble, a la altura en que está la línea investigadora del flamenco, tener que decir que el Fandango de Huelva y su provincia continúa siendo el eterno desconocido para la mayoría de los llamados entendidos. La diversidad de los estilos que los conforman son totalmente confundidos y, con ese desconocimiento, difundidos por toda la geografía, alejándolos, cada vez más, de la realidad de sus raíces musicales. El tema es muy serio porque, entre quienes los confunden, existen plumas de renombrado prestigio en el mundo flamenco que lo han aireado a su libre albedrío, sin tomarse la más ligera molestia de acercarse a sus verdaderas fuentes. Esto, con todos los respetos que personalmente me merecen, no deja de ser más que una gran osadía por sus partes, así como un timo para todos aquellos incondicionales que leen y creen en estas plumas. En el orden de los ejemplos, tratemos el primero: En el año 1968, la firma discográfica VERGARA edita una antología, con el nombre de «ARCHIVO DEL CANTE FLAMENCO», asesorado y dirigido por José Manuel Caballero Bonald. En él notamos una\n\n[ENDING CONTEXT]\n\nlugar donde se cantan.\n\nLos aficionados al flamenco de Huelva reconocemos la gran importancia que tienen nuestros fandangos. Pero no por lo que se ha hecho hasta ahora. Somos conscientes de que hay que llevar a cabo una seria y exhaustiva investigación de los mismos por la gran riqueza de su musicalidad y por la historia que encierra tanta diversidad. Hay que ahondar en el venero de sus raíces donde podemos encontrar causas y cauces de unos melismas primitivos que, con otras similares, fueron determinantes para la formación de una gran parte de lo que hoy conocemos por cante Flamenco-Andaluz.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Fandango de Huelva y su provincia",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "14-17",
    "page_number": 14,
    "word_count": 3845,
    "article_char_count_full": 23037,
    "article_char_count_review": 3793,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "serio"
      }
    ]
  },
  {
    "article_id": "1987-09-17-right-benalm-dena-ha-subido-el-list-n",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCRÓNICA DE URGENCIA DEL XV CONGRESO:\n\n《E 1 mejor Congreso de los habidos hasta ahora》. Con esta elocuente frase, Antonio Alarcón, presidente de la mesa del XV Congreso, cerraba unas hermosas y edificantes jornadas que han marcado un hito histórico por cuanto se han corregido numerosos errores, se han adicionado nuevos temas de debate y, sobre todo, un elemento vital para el desarrollo de los mismos: imaginación organizadora. Por si fuera poco, Benalmádena ha demostrado que con poco más de cuatro meses se puede salir victorioso del envite y reunir el mayor número de asistentes a los mismos: ciento treinta y dos congresistas y ochenta y cuatro acompañantes suponen una cifra a tener en cuenta, según lo visto y vivido en años anteriores. La nota triste, por luctuosa, llegaba con la noticia\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\nreciba nuestro amigo Antonio el más sincero y sentido pésame. Todo comenzaba en la misma sede de aquel pretérito primer Congreso que se celebró allá por 1969. Benalmádena, una edición más, iniciaba nueva andadura el Manuel Martín Martín pasado lunes día cinco con la inauguración de las exposiciones de pinturas y fotografías sobre temática flamenca, en la Casa de la Cultura de Arroyo de la Miel. Una hora después, a las ocho de la tarde, tenía lugar la proyección de la película «Duende y misterio del Flamenco». La Jornada del martes serviría de colofón a los actos preliminares con las proyecciones de los filmes «La niña de la Venta» y «Los Tarantos». A partir de las diez y media del miércoles día 7 se procedía a la retirada de la documentación y credencial de congresistas y acompañantes, donde destacamos un disco interesante sobre la Málaga cantaora (1850-1950), el libro de ponencias y comunicaciones, un método básico de guitarra flamenca a cargo del maestro Comitre y los libros «Enderezando entuertos» de Manuel Yerga y «Cantaores malagueños» de Gonzalo Rojo. Tras una recepción ofrecida por el Ayuntamiento de Benalmádena en el castillo de Bil-Bil, se procedía a la elección de la mesa recayendo por mayoría en Antonio Alarcón Constant, como presidente, José Luque Navajas, Manuel Ríos Ruiz y Agustín Benítez, respectivamente. Acto seguido se iniciaba la lectura de la Comunicación «Letras y algunas dison\n\n[ENDING CONTEXT]\n\nTere, Chari, Sergio, Paco y Santi, hicieron lo imposible para que el evento fuera perfecto, y donde Miguel Martín Anaya —representante del Ayuntamiento de Benalmádena— y nuestro querido compañero Gonzalo Rojo —no hay quien pueda con la experiencia—, perdieron muchos días de sueño para que el XV Congreso quedara impreso en las mentes de más de doscientas personas con un claro y rotundo mensaje: «el mejor congreso de los habidos hasta ahora». Esperamos y confiamos decir lo mismo en Córdoba, porque como dice nuestro refranero popular «a los que bien bailan, poco son le bastan». Así lo deseamos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica de urgencia del XV Congreso: Benalmádena ha subido el listón",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "17-19",
    "page_number": 17,
    "word_count": 2523,
    "article_char_count_full": 15061,
    "article_char_count_review": 3045,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  }
]
```
