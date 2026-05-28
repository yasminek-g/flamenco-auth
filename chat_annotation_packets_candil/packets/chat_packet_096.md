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
    "article_id": "1984-07-4-right-noche-flamenca-en-buenos-aires",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Anselmo González Climent\n\nEn casa del maestro Anselmo González Climent, una madrugada de 1969, Julio Alvarez, abogado, ex-ministro, poeta y dramaturgo; Víctor Massuh, el más importante filósofo argentino, representante de su país en la UNESCO y director de la misma; Julio Mafud, ensayista, sociólogo y catedrático; Carlos Iribarren, folklórico, y como moderador de todos ellos, Anselmo González Climent, un clásico de la investigación del tema «jondo», cuyos rigurosos análisis, en cierto modo, han promovido la aparición de una nueva generación de estudiosos del flamenco.\n\nBuenos Aires, en la entrañable Argentina, reflexiona sobre el cante de Chacón, Niña Los Peines, Aurelio Sellés... Es una reunión de cabales aunque alguno de ellos, por primera vez, hayan sentido el cante, en esa noche,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"público\"]\n\nello, sus cálidos juicios, sus audaces análisis, su neta profundización en el flamenco, aportan significados medulares de lo «jondo», por la vía de una hermenéutica entre socrática y machadiana. Estimamos que nuestros lectores apreciarán, en toda su magnitud, el que las publiquemos en estas páginas, lo que ha sido posible merced a la transcripción que generosamente nos ha remitido Anselmo González Climent, al que desde aquí manifestamos nuestro público reconocimiento. Julio Alvarez.—Víctor, en primer lugar, quiero referirme a una preocupación tuya planteada en el sentido de por qué el flamenco se expresa dificultosamente, por qué las palabras son cortadas o son poco audibles para aquellos que no estamos acostumbrados a la audición de las palabras flamencas o del cante flamenco. Me parece que esa era tu pregunta. Yo creo, con Anselmo, que el cante es el arte del júpio o del grito plástico. Las letras o las coplas suelen ser hermosas, rebosantes de sintético contenido, la mayoría dignas de antologarse. Pero una vez que el cantaor está en «trance», la letra pierde territorio, es materia prima o excusa para que el júpio ascienda y baje, provoque climas muy sutiles y goce de independencia. Julio Mafud.—Es muy posible que esa sea la explicación. Pero yo creo —perdona Víctor que me adelante a tu palabra—\n\n[ENDING CONTEXT]\n\nque no es fácil ni factible la inmediata subida de la carga subjetiva del cantaor con nivel por lo menos mínimamente colectivo de los que lo pueden escuchar. De forma consecuente, el cantaor tiene necesidad, en la mayoría de los casos, de auxilios especialísimos para poder llegar a la esencia del cante y no quedarse en estratos de emergencias. En suma, produce la subida de la subjetividad al nivel de lo visible o, por lo menos, de lo escuchable.\n\nJulio Alvarez.—Yo quiero un poco más de Juanito Mojama. Se acerca la madrugada.\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "NOCHE FLAMENCA EN BUENOS AIRES",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 4007,
    "article_char_count_full": 23993,
    "article_char_count_review": 2943,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "público"
      }
    ]
  },
  {
    "article_id": "1984-07-8-left-razones-de-un-ay",
    "article_text_for_review": "Por Carlos Cruz\n\nA menudo, después de alguna actuación de cante, alguien del público y una vez fuera del escenario, me ha preguntado el porqué de tanto ¡ay!, tan frecuente como expresión dolorosa y medular del cante flamenco. Como intuyo que esa misma pregunta se queda sin respuesta y en gran parte del público recién iniciado en el sentimiento del arte flamenco, vayan estas líneas con la sola intención de arrojar alguna luz sobre el tema.\n\nEse grito (¡ay!) puede estar motivado por diversas causas y localizado en la amplia gama del sentimiento humano (pérdida de un ser querido, ausencia de libertad, etc.) pasando por tonos que van del desgarro total a otro más tenue e íntimo con intermedios de agridulces diferentes.\n\nPor tanto, gritar ¡ay! es trazar un arco de esperanza en el abismo del tiempo. Remontarse al instante preciso del parto, cuando con dolor somos arrojados a la vida y con dolor sentimos el primer aliento.\n\nDecir ¡ay! es invocar el dulce cauterio de labios de la mujer amada, como vino ardiente, sobre la sangrante yaga de los propios.\n\nQuejarse diciendo ¡ay! es descender a las grutas del alma, allí donde la pena se enquista y el bisturí no puede llegar, para emerger de nuevo a la luz gloriosamente sanados.\n\nClamar ¡ay! es arrojar por la boca toda la miseria, hambre e injusticias a que ha sido sometido el pueblo llano andaluz, secularmente víctima de la fría ambición centralista iniciada con la mal llamada Re-\n\nconquista (para reconquistar algo debe de haberte pertenecido con anterioridad el objeto en cuestión).\n\nMi más ferviente deseo es que lo aquí expuesto sirva para añadir luz a la ya deteriorada imagen del cante, propiciada por las prisas de un mundo moderno y caótico, o por la invasión de otros gustos musicales que nada tienen que ver con la esencial naturaleza del alma andaluza.",
    "title": "RAZONES DE UN ;AY!",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 311,
    "article_char_count_full": 1824,
    "article_char_count_review": 1824,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-07-8-right-los-misterios-de-la-siguiriya-gi",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDesde su llegada a principios del siglo XV, los Gitanos se hacen notar como músicos profesionales en casi todos los países europeos. Los instrumentos son generalmente los que se estilan en la comarca donde se verifica la función —laud en los territorios ocupados por los Turcos, salterio o tímpano húngaro (czimbalom) en Hungría, arpa en Francia, sonajas y castañuelas en España—y la música es casi siempre autóctona. En varios países, la música es un acompañamiento de baile, especialmente en España, donde los Gitanos participan con tales espectáculos en diversos festejos populares y, con una frecuencia muy notable, en las fiestas tradicionales del Corpus. El éxito de sus actuaciones se hace patente en la literatura de la época, en los pormenores de La Gitanilla de Cervantes, por ejemplo, y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"interpretación\"]\n\ne patente en la literatura de la época, en los pormenores de La Gitanilla de Cervantes, por ejemplo, y con mayor evidencia todavía en el crecido número de obras teatrales menores —entremeses, bailes, mojigangas, sainetes, tonadillas—cuyo único resorte es un baile de Gitanas. Hay que advertir que muy pronto se ha dado en llamar «de Gitanos» o «de Gitanas» el repertorio del folklore local ejecutado por Gitanos o Gitanas, sea porque a pesar suyo su interpretación dejaba trasparentar un dejo particular, o sea, más sencillamente, porque la indumentaria y el aspecto físico de los intérpretes daba un sabor exótico a los cantos y bailes de la tierra. Es probable que los dos elementos se conjugaron, ya que muchos textos señalan, además de una pronunciación típica —el ceceo—, un tipo de interpretación sui géneris comúnmente designada con la expresión: «a lo gitano». La copla llamada seguidilla, compuesta de un cuarteto y de un terceto alternando heptasílabos y pentasílabos es, con el verso de romance —octosílabos asonantados— la forma métrica más corriente de las canciones gitanas, tanto en la realidad como en el teatro. He aquí un ejemplo sacado de un sainete anónimo del siglo XVIII, La Gitanilla honrada: Por más, cruel Fortuna, que nos persigas, quantos más travajos más alegría. Ande la rueda y ya que no comamos tengamos fiesta. Por Bernard Leblón Existe también una forma sencilla, limitada al cuarteto, como en otro ejemplo recogido en El maulero de Francisco Antonio de Monteser, publicado en 1617: Gitanillo del alma no te alborotes que si no son galeras serán azotes Después de haber visto a tantos Gitanos falsos o verdaderos cantanto y bailando por seguidillas, en el escenario y por la calle, incluso en caló como el caso citado por Larrea Palacín (1), el público va a asociar maquinalmente Gitanos y seguidillas y, por fin, en la segunda mitad del siglo XVIII, aparece la expresión seguidillas gitanas en varias obras de teatro, as\n\n[ENDING CONTEXT]\n\nde la India, es la Andalucía tantesia, romana, mozárabe, judía y mora. Si olvidamos este factor esencial no se pudiera explicar por qué los Andaluces, después de acoger en sus pueblos a los Gitanos perseguidos y de convivir con ellos, adoptaron su rara música oriental, se reconocieron en ella y la hicieron suya, lo que nunca hicieron los húngaros con la lokí dylí. Sin estas circunstancias únicas tampoco se pudieran aclarar los misterios de la siguiriya.\n\n(1) ARCADIO DE LARREA, El flamenco en su raíz, Madrid, 1974 pág. 76.\n\nCapitán Oviedo, 15 - Apartado 76 - Teléfono 22 76 36 J A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LOS MISTERIOS DE LA SIGUIRIYA GITANA",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 3268,
    "article_char_count_full": 19607,
    "article_char_count_review": 3578,
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
    "article_id": "1984-07-11-right-la-novela-andaluza",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor José Luis Buendía\n\nNA vez que hemos analizado las principales líneas por las que ha discurrido la narrativa española de postguerra (ver número anterior de «Candil»), es necesario abordar ahora los perfiles de lo que se ha venido denominando nueva narrativa andaluza, y dentro de ella situaremos la especial consideración que el tema del flamenco haya suscitado en algunos autores representativos.\n\nPero hemos hablado de «nueva» narrativa andaluzas, ¿qué entedemos nosostros cuando incorporamos ese concepto de novedad? Básicamente nos estamos refiriendo al paso de la narrativa del siglo XIX al XX, que ha supuesto una manera radicalmente distinta de entender el fenómeno de la novela, si bien no podemos negar una relación estrecha entre ambas formas de escritura. Para empezar es preciso\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nque ya en el siglo XIX, de forma más o menos velada aparecían en los creadores de aquella centuria. Pero procedamos primero a la caracterización formal de la novela del XIX para después marcar las diferencias con la actual: se suele partir de un error común que es creer que la novela realista arranca sólo del cuadro de costumbres o de los intentos moralizantes de Fernández Caballero, cuando es bien cierto que, sin desdeñar estas influencias, el gran surgimiento de la novela decimonónica viene de la mano de la novela social española del período inmediatamente anterior, ligada al folletín y al movimiento romántico en general. Varios son los rasgos que van a definir este tipo ampuloso de novela: 1) Descripción exhaustiva de tipos literarios y situaciones perfectamente abordadas y resueltas en su totalidad al concluir el desenlace de la obra. 2) Estrecha relación entre marco histórico y tesis política a la hora de centrar en sus coordenadas sociales el relato en cuestión. 3) Una gran preocupación por el presente histórico y los problemas sociales contemporáneos, entre los que no será ocioso recordar los que se derivan de una sociedad en vías de desarrollo capitalista, el poder de la nueva aristocracia del dinero o la casuística de una clase obrera explotada y marginada de los centros de decisión del país. 4) Una gran influencia de la literatura y el pensamiento franceses de la época, hasta el punto de que estar a favor o en contra de dichas influencias, vino a ser sinónimo en nuestra narración de posturas progresistas o reaccionarias, en aquella España caliente, tan propensa a divisiones y banderías. No podemos pensar que en nuestro país esta novela se redujo tan solo a un mero «tipismo» descriptivo y que se quedaba en la superficie de las cosas; ésto, cuando se produjo fue como reacción posterior a los modelos tenidos como abanderados de la progresía foránea (Víctor Hugo, Eugenio Sué); el caso de Fernández Caballero, en este sentido, es suficientemente explicativo. El logro más importante de esta novela decimonónica fue saltar desde este tipismo «ad hoc», con su pesada reiteración costumbrista de tipo (el chulo, el majo o el baturro) hasta la observación pretendidamente fiel del hombre y su contorno, paradigma que se puede entrar en la postura del francés Balzac que sacaba argumentos de la observación continuada de un hombre medio, de la calle, o el español Galdós que reflejó el ambiente de porteros, vendedores ambulantes o mendigos del Madrid de su época. Por todo ello, opinamos nosotros, no se puede hablar de un tipo uniforme de relato del XIX sino señalar algunas de las más importantes características comunes a él, a manera de notas distintivas de esta novelística: A) Contenido marcadamente doctrinario (se habla, en este sentido, de novelas tendenciosas o «de tesis»). B) Limitación de los elementos fantásticos o extraordinarios de la narración en beneficio de los hechos cotidianos de la realidad. C) Ambientación realista y contemporánea (salvo el caso excepcional de algunos relatos imbuidos del exotismo romántico). D) Búsqueda exhaustiva de la verosimiliitud en el relato, acompañada de una coherencia psicológica total del personaje. E) Casi como consecuencia de lo que venimos diciendo, hay que reseñar la utilización d\n\n[ENDING CONTEXT]\n\npor una cuba de vino, ese mismo mosto que alegra las madrugadas en las que el cante flamenco se debate en las contradicciones de su propia tragedia.\n\nVamos a terminar, aunque, en buena ley, no podemos poner punto final, ya que la presencia jonda en la nueva narrativa andaluzas es ininterrumpida. Nuestro arte sirve continuamente de vehículo expresivo a este puñado de novelistas honestos y arriesgados que han decidido poner su técnica y su sensibilidad al servicio exclusivo de este Sur doliente, que clama, desde hace siglos, por una redención que nunca llega. Nuestro Sur...\n\nBIBLIOGRAFIA\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LA NOVELA ANDALUZA",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "11-15",
    "page_number": 11,
    "word_count": 5993,
    "article_char_count_full": 36754,
    "article_char_count_review": 4891,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "1984-07-16-left-las-coplas-de-paco-salgueiro",
    "article_text_for_review": "Dejadme solo un momento; las palabritas del alma me están quemando por dentro.\n\nPor la calle del Limón cantaba Doña Naranja sentaía en su balcón.\n\nla luna que vi en tus ojos y hasta el aire que respiras.\n\nCasita de mi consuelo quiero entrar y no me dejas, quiero salir y no puedo.\n\nYo me asomé a la muralla y ciego me dejó el viento y ciego me dejó el agua.\n\nSi las lágrimas quemaran estarían ya sin luz los ojos de mi cara.\n\nSembré tomillo en el aire y madroños en el agua; aquel que siembra recoge, yo recojo mi esperanza.\n\nSalgo de mi casa me perdí en el viento; ni la muerte con ser compañera me presta su aliento.\n\nFrancisco Salgueiro",
    "title": "LAS COPLAS DE PACO SALGUEIRO",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 123,
    "article_char_count_full": 639,
    "article_char_count_review": 639,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
