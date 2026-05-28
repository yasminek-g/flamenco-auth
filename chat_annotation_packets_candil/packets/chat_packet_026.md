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
    "article_id": "1981-05-13-right-honra-machismo-y-celos-en-el-sen",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor José Luis Buendía López\n\nHemos visto en nuestro análisis sobre el nivel de comunicación en el terreno amoroso, que lo propio de las letras del cante flamenco es un enorme sentimiento de inestabilidad, de pasar de unos máximos resplandecientes a unos mínimos de abatimiento y soledad en los que la desesperación tiene muchas veces un papel decisivo a la hora de marcar los acentos expresivos de ese cante que es, más que nada, un resumen de penas y encade nadas carencias.\n\nDentro de esa inestabilidad, consecuencia sin duda de la de todo un pueblo, el andaluz, sometido y humillado durante siglos, carente (hay que mencionar una vez más el manoseado tópico) de sus más elementales señas de identidad, queremos reflexionar hoy sobre el nivel de esas relaciones amorosas hombre/mujer, que aparecen\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nmás que nada, un resumen de penas y encade nadas carencias. Dentro de esa inestabilidad, consecuencia sin duda de la de todo un pueblo, el andaluz, sometido y humillado durante siglos, carente (hay que mencionar una vez más el manoseado tópico) de sus más elementales señas de identidad, queremos reflexionar hoy sobre el nivel de esas relaciones amorosas hombre/mujer, que aparecen signadas con un marcado acento machista, de dominación total del hombre sobre la mujer, a la que, aunque en ocasiones requiebre hasta límites cercanos a la veneración, como otras veces hemos estudiado sin embargo no puede o no quiere considerarla como verdadera compañera en igualdad de derechos y deberes. Por el contrario, al menos a niveles expresivos, el papel del macho dominador, garañón y jefe indiscutible de la tribu, aparece por todas partes con un orgullo mal disimulado, haciendo alarde y ostentación del poder omnímodo que el rol masculino le otorga en esa sociedad primitivamente organizada, que es la comunidad gitano-andaluza: De que quieras de que no tú entrarás en el camino porque te lo mando yo. Orden brutal, tajante, que revela mejor que muchos tratados teóricos el verdadero carácter ancestral de estas comunidades familiares de ámbito tan primitivo que no vacilaremos en aplicarles, en ocasiones, el sello distintivo de lo tribal. Claro, que inuchas veces, y como ocurre con facilidad en otros ámbitos ajenos a lo flamenco, toda esa sensación de fue\n\n[ENDING CONTEXT]\n\nconfesión de Melibea, que desea ser la única protagonista de su historia amorosa: «¿Por qué no fue también a las hembras concedido el poder descobrir su congoxoso e ardiente amor como a los varones- Que ni Calixto biuiera queixo-so ni yo penada» (La Celestina).\n\nOtra mujer, flamenca y del pueblo, nos va a decir lo mismo, pero con más gracia y apasionado desgarro. Con esta cita procedente del cante flamenco popular ponemos punto final a nuestras íntimas consideraciones sobre el tema:\n\nSi las mujeres tuvieran la libertad de los hombres salieran a los caminos a robar los corazones.\n\nMármoles\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Honra, machismo y celos, en el sentimiento amoroso flamenco",
    "periodical": "candil",
    "issue_id": "1981-05",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "13-16",
    "page_number": 13,
    "word_count": 3699,
    "article_char_count_full": 21299,
    "article_char_count_review": 3081,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombre"
      }
    ]
  },
  {
    "article_id": "1981-05-16-right-las-soleares-de-jose-bergamin",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAunque no quepa en el papel\n\nPor Manuel Urbano\n\nLa recienté aparición de la antología poética de José Bergamín, «Poesías casi completas» (1), nos trae a primer plano la hondura jonda de buena parte de la poesía de este autor del 27, durante tantos años marginada en los «espacios» literarios y muy ocasionalmente conocida, por alguna que otra soleá, en el «mundo» flamenco.\n\nAdelantando que el libro de nuestro comentario es mucho más amplio que el ámbito necesario de esta reseña, y que si bien la deuda del autor para con Lope, los Cancioneros tradicionales, etc., es inmensa, centremos el análisis en las casi trescientas soleares que contiene de confesada raíz y magisterio:\n\n(y con Ferrán) tengo un huerto\n\nque por mi mano he plantado.\n\nLa soledad es la base de la filosofía del espíritu, que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"alma\"]\n\nesta reseña, y que si bien la deuda del autor para con Lope, los Cancioneros tradicionales, etc., es inmensa, centremos el análisis en las casi trescientas soleares que contiene de confesada raíz y magisterio: (y con Ferrán) tengo un huerto que por mi mano he plantado. La soledad es la base de la filosofía del espíritu, que es toda la poéticabergaminiana. La soledad como contemplación, que es la verdad más honda: La soledad no es soledad del alma, es soledad del cuerpo: pero sin ella el alma no tendría nos dirá en uno de sus poemas finales. Así, la soledad no es otra cosa que solidaridad propia; el español está unido consigo mismo, es un solitario: vida ni pensamiento, Cad vez siento más hondo todo lo que me separa de todo, jay! y de todos. Porque me voy separando en que me estaba quedando. Para Bergamín, es necesario estar radicalmente sólo para amarse a sí mismo; de aquí esa obligada búsqueda y encuentro con la soledad. Por ello nos dirá partiendo de los conocidos versos de Lope «A mis soledades voy / de mis soledades vengo»: A soledades del alma no sé si voy o si vengo cuando soledades hallo y soledades encuentro. Pero la soledad no es la saudade, ésta es una pena galaicoportuguesa que nace de la ausencia. Por el contrario, la soledad flamenca, tan diáfana, es la verdad interior, fortaleza, luz y camino: Eres la luz, la verdad y la vida o el camino porque eres la soledad. La soledad de Bergamín, al decir de Carlos Gurméndez (2), «es la soledad de los bosques germánica, que llega a convertirse en cantar de soledad, soleá andaluza, que tiene sabor de senhsucht alemán». Es, también, «La música callada / la soledad sonora» de San Juan de la Cruz: Nunca tanto como ahora que la soledad es sonora. he dejado de sentir ¡Ay! ¡Con cuánta soledad se me va ahondando en el alma el vacío de la verdad! Y aquí el duende, el gusanillo, el laberinto, el birlibirloquesco mundo, tan unamuniano, de la razón y el disparate que son base del pensamiento y sentirbergaminiano, y, a su vez, el alma, la (1) En el libro de bolsillo de Alianza Editorial, número 756; Madrid, 1980. (2) «Bergamín habla de hombres y tortugas», en «El País»; Madrid, 19-IV-1981. realidad vivífica del cante jondo que se abre en el desgarro de la garganta: La soledad de mi vida se está quedando sin alma. Mi corazón ya no tiene sangre para poder dársela. Por ello, si el hombre —al decir de Gurméndez— «aguanta la soledad cantando, es un hombre duro y de temple recio, endurecido por el dolor, duradero, eternizado». Y es\n\n[ENDING CONTEXT]\n\nen la necesidad de un estudio global flamenco sobre Bergamín—, dedicaremos un espacio del próximo número de «Candil» al análisis de lo jondo en su reción aparecido volumen de prosas titulado «Al fin y al cabo» (5).\n\n(3) En el libro «Al volver»; Edit. Seix Barral, Col. Biblioteca Breve; Madrid, 1962. (4) «Don José Bergamín y el Cante Flamenco», en «Diario Córdoba», 29-IV-1962. Hay edición en libro: «Ricardo Molina: obra flamenca», pág. 78-79, Edit. Demófilo; Fernández Núñez, 1977. (5) Edit. Alianza; Col. Tres; Madrid 1981.\n\nTejidos nuevos, para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel.-Las soleares de José Bergamín",
    "periodical": "candil",
    "issue_id": "1981-05",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1568,
    "article_char_count_full": 8912,
    "article_char_count_review": 4128,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "alma"
      }
    ]
  },
  {
    "article_id": "1981-05-18-right-discograf-a-flamenca",
    "article_text_for_review": "Recordamos lo que en 1971 escribiera Fernando Quinones a propósito de la primera aventura discográfica protagonizada por Diego Clavel: «Trae aqui Diego soleá y siguiriya, martinete y tiento Trae eso que se llama cante y — él tan joven— trigo de edades viejas en su alforjilla. Llega aquí al cante y a su discografía, Diego Clavel. Viene volando».\n\nAhora, más años, más experiencia, el cantaor de la Puebla de Cazalla plasma en un disco del sello Columbia su decir actual. Con el apoyo de Juan El Habichuela y Pedro Peña, como guitarristas, Diego Clavel canta Tangos, Fandangos de Lucena, Alegrías, Malagueñas, Siguiriyas, Fandangos de Huelva, Garrotín, Jaberas, Cartageneras y Alboreás. Cuando se acerca uno a este joven, pero veterano cantaor, hay algo que predomina: su humana sencillez. Y es algo que refleja de forma rotunda en sus cantes. Diego Clavel, creemos, acude a cada estilo desprovisto de grandilocuentes empeños, para, sencillamente, pero con entrañable poderío comunicativo, dejar en el aficionado un sentir flamenco preñado de eco-pueblo. En Diego, voz en la raíz del grito, importa la transmisión del mensaje. Voz, clara en transparencia, camina, en ocasiones, no ajustada a ortodoxas matizaciones, pero con el ferviente deseo de llegar, de conectar con el público. Quizás Diego no haya sentido la necesidad de abundar en cada estilo, para, sin embargo, fijar su interés en el poder de comunicación que su personalidad artística tiene. Cantes y Pensamientos no es disco de archivo, sí para escuchar.\n\nSigue cantando la PEÑA FLAMENCA DE HUELVA\n\nDiscograficamente se nos aparece Huelva en un deseo de ajustarnos a los aires cantaores de una tierra. Es un volúmen 2 de «La Peña Flamenca sigue cantando», con un recorrido, en su primer L.P., por distintos estilos de fandangos, para, en el segundo interpretar La Misa Flamenca de Nuestra Señora de la Cinta.\n\nAcometer una realización discográfica es algo positivo en el hacer divulgativo de un Peña. Bien merece nuestro reconocimiento. Pero, permítase-nos alguna puntualización. A la hora de proyectar una labor de investigación y divulgación, en este caso sonora, hubiera sido interesantísimo exponer el documento en su más rica y variada expresividad, acercando al aficionado, el eco peculiar de cada estilo en las voces anónimas de cada lugar. Es enriquecedor, hermoso, distinto. Se huye de la uniformidad en que, sin querer, se cae. Esto no implica que los integrantes de la Peña Flamenca de Huelva, que intervienen en estas experiencias discográficas, sigan en este hacer que, desde la perspectiva no profesional, es loable. Eso sí, para bien de todos, intentando cada día más rigor interpretativo. Si queremos difundir nuestro arte, deberemos hacerlo por los caminos de la pure-",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1981-05",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 438,
    "article_char_count_full": 2747,
    "article_char_count_review": 2747,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-05-19-left-bodas-de-sangre-pel-cula-de-carl",
    "article_text_for_review": "Crear nunca es fácil. El parto de la criatura artística viene siempre precedido de sudores y de sangre. Para Carlos Saura, director de la película «Bodas de Sangre», que hoy comentamos, la creación es un acto de total entrega y voluntad, que le ha costado problemas sensoriales, administrativos e incluso amenazas a su integridad física por los que creen que la razón ha de imponerse a base de petardos y golpes.\n\nEn su dilatada carrera fímica (17 títulos desde 1957) ha sido un testigo apasionado y lúcido de las relaciones humanas, desde las más íntimas y cordiales, que rozan el terreno del subjetivismo a ultranza, los caminos de la incomunicación de la persona («La Madriguera», «Pippermint Frappé»), hasta los valientes alegatos político-sociales («El jardín de las delicias», «La prima Angélica»), pasando por el mundo del subdesarrollo y la delincuencia juvenil («Los golfos», «Deprisa, deprisa»).\n\nLo más sano y positivo de Saura es que no se detiene en absurdas complacencias consigo mismo, no apura las fórmulas del éxito, sino que se abre a nuevos caminos, a exigencias éticas y estéticas de porvenir incierto, pero que responden al espíritu abierto y generoso de este director como al de su colaborador-productor habitual, Elías Quejereta.\n\nEn estas búsquedas nuevas, la última película de Saura, «Bodas de Sangre» de este mismo año 1981, es una feliz conjunción del genio de Lorca, Gades y el propio director de la película. No busquemos en ella elementos aislados de fidelidad a textos y a situaciones porque no las encontraremos: Lorca queda diluido en cuanto al fondo de su discurso dramático, del cual se toma solamente el pretexto anecdótico. El baile de Gades y la coreografía de la que el bailador se hace responsable es la revisión menos tópica que sobre un tema musical andaluz podamos encontrar. El tándem Saura-Gades ha construido una película bella, tersa, geométrica en la belleza plástica de las imágenes, servidas por ese monstruo de la fotografía que es Teo Escamilla. Lo flamenco y lo literario del guión han sido sacrificados en aras de la rigurosidad total, de la matemática perfecta, donde solamente la música, el ritmo, el cuerpo bello y generoso de los danzarines marcará las acciones, las pausas, los silencios. Se trata de una película eminentemente ceremonial, de homenaje a las posibilidades plásticas del cuerpo humano en combinación con los elementos musicales y textuales de los que el film parte. Si se buscan en él otros resultados, otros efectos andalucistas, flamenco-lógicos o de otra procedencia que las que acabamos de describir, los resultados para ese sector del público pueden ser francamente decepcionantes. En esta ocasión Carlos Saura ha optado por los resultados estéticos obtenidos en esa alquimia perfecta del grupo artístico de Gades y el equipo técnico de la película. Que no es poco\n\nJosé Luis BUENDIA LOPEZ\n\nPolígono «Los Olivares» - Calle Alcaudete, 10\n\nJ A E N",
    "title": "Bodas de Sangre, película de Carlos Saura",
    "periodical": "candil",
    "issue_id": "1981-05",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 476,
    "article_char_count_full": 2925,
    "article_char_count_review": 2925,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-05-19-right-quienes-fueron-los-maestros",
    "article_text_for_review": "FRANCISCO LEMA «FOSFORITO»\n\nFrancisco Lema, «Fosforito», fue uno de los más grandes malagueñeros conocidos, quien mantuvo con Chacón una gran rivalidad en este estilo durante casi 30 años.\n\nNació en Cádiz en 1869; el ‘sobrenombre lo motiva su altura y delgadez, existiendo una anécdota sobre ello, relatada por Fernando el de ‘Triana en su libro «Arte y artistas flamencos»:\n\n«Sabido es que por aquel tiempo la luz de más lujo y claridad era la de gas; de éstas había unas cuantas en el tablado del café del Burrero; la instalación de dichas luces estaba hecha a buena altura, pero como para Fosforito no había nada alto, una noche encendió un cigarrillo en una de las más altas, y al notarlo Paco «El Sevillano», le preguntó en tono grave:\n\n—¿Qué edad tienes, niño?\n\n—Dieciocho años —contestó Fosforito. —¿Dieciocho? ¡Cuando tengas veinticinco vas a encendé en er só!».\n\nFosforito fue discípulo de Enrique el Mellizo y, entre otros oficios desempeñó el de ayudante de cochero. Ayudado y orientado por el maestro gaditano, Fosforito debutó en 1883, a los catorce años, en el jerezano Café del Palenque, propiedad del siguiriyero Juan Junquera, valiéndole el éxito obtenido un contrato de cuarenta actuaciones en el local, a 6,25 pesetas por noche.\n\nRealizó bastantes giras por los pueblos de Cádiz y Sevilla, actuando en famosos cafés cantantes como el sevillano de El Burrero, el jerezano Café de la Veracruz, o los madrileños Monedero, Naranjeros o el del Brillante, al que inaugura.\n\nSegún Fernando Quiñones, fosforito se despidió en el Olympia de Sevilla en 1923, aunque se cree que su última actuación pudo haber tenido lugar el 5 de marzo de 1926 en el teatro La Latina de Madrid.\n\nFrancisco Lema contrajo matrimonio con la bailaora Mariquita Malvido, y se mantuvo en figura del cante profesional durante casi toda su vida. Murió en la mayor pobreza, en los primeros años cuarenta y en una pensión de la castiza calle madrileña de Mesón Paredes.\n\nSu decadencia como artista flamenco, sobrevino antes que la de D. Antonio Chacón, existiendo una letra que refleja perfectamente la misma:\n\nFosforito ya no canta.\n\nFosforito se apagó.\n\nPara cantar malagueñas\n\nhay que llamar a Chacón.\n\nSegún Ricardo Molina: «Se conserva (muy olvidado) el cante de Fosforito, una malagueña saturada de profunda melancolía y desarrollada en un plano de dulce musicalidad»:\n\nDesde que te conoci\n\nmi corazón llora sangre,\n\nyo me quisiera morir\n\nporque mi pena es muy grande\n\ny así no puedo vivir.\n\nInterpretando esta malagueña —se cuenta— Fosforito hacía sudar al público, por creer éste que no podrían llegar las facultades del artista a coronar el penúltimo tercio, de modulaciones tan difíciles como ajustadas.\n\nSelecciona RAFAEL VALERA",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1981-05",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 445,
    "article_char_count_full": 2721,
    "article_char_count_review": 2721,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
