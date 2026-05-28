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
    "article_id": "1996-05-43-right-fausto-olivares-en-las-cuevas-ce",
    "article_text_for_review": "Agustín Gómez\n\nCelebramos en Córdoba el Festival Internacional de la Guitarra. Me había prometido esta cita para después. Esta noche tenemos programa doble: Riqueni en el Gran Teatro y Enrique de Melchor e Inmaculada Aguilar en los Jardines del Alcázar. Habremos de decir algo de ello para la prensa provincial, y seguirá mañana el Movimiento perpetuo de la Compañía de Antonio Márquez. Antes fueron El Pele, Paco Peña, El Güito, Niña Pastori, Manolo Sanlúcar... Tendremos después medio mes de julio por delante. Ahora advertimos un cierto cansancio y un trenzado de bordones en las sienes. Pero en mi duermevela matinal, hoy, Fausto amigo, has ido y venido por las verdes praderas del recuerdo. Me ocurre esto con cierta frecuencia, sobre todo cuando amenaza la resaca de intensas vivencias flamencas. Siempre, querido Fausto, te asocio a estos trances de los que tú sales tan airoso tras un largo sueño reparador.\n\n¿Qué me ha de tocar a mí en esta rueda de la amistad sobre el eje de tu carreta? Ha llovido este año; pero de nuevo se secó ya el barro de los caminos, dejando unas crestas duras como espinazos de piedras, esa imagen que José María Moreno Galván dejaba clavada en aquella primera declaración de principios de nuestras exposiciones, El Flamenco en el Arte actual, inducidas por Antonio Povedano. Tú fuiste uno de nuestros puntales, ¿recuerdas tu participación con dos óleos: Se prohíbe el cante y Reunión en la peña? Fue también en julio y agosto, pero del 72, en Montilla. Fue el comienzo de nuestra amistad, a la luz de la estética de lo jondo que el crítico nos definiera así:\n\nComo sabéis, la voz verdadera-mente honda del flamenco no es — no tiene que ser necesariamente— ni la voz melodiosa ni la voz formidable de los grandes tenores: es la voz terrible, ancestral, profunda, que alía raíces nocturnas con las raíces de todos nuestros antepasados. Esa voz, que sale de nuestros huesos, es como el violento trazo de la pintura de nuestros días. También ésta ha renunciado a la estética de la perfección: atiende más al grito que al argumento, más al latido que a la melodía.\n\n¡Cuántas renuncias supiste hacer tú, Fausto Olivares, pintor jondo, en una suerte nueva de pintura negra de profundidades, como la boca del pozo al que te asomabas en tu soledad devoradora de la noche en tu estudio! Cuando todavía, por más que hayamos predicado, queda un desierto que no escucha el grito, que pide argumento melodiado, has dejado tu latido. Algún día vencerá a la aridez acolchonada de suaves dunas y hará vibrar el universo, porque tal es la fuerza de esta pasión desordenada por las aristas de las piedras, Fausto amigo. Tú sabes muy bien que el cante, como arte, es un dolor. Porque tú no viniste a disfrutar de la sopa boba, como nadie que sienta\n\nraíces vivas en las entrañas, sino a luchar, mucho más que por una idea, por un sentimiento.\n\nSi las voces se diluyen en el aire y son efímeras, las hay excepcionales que perduran en la herida que hicieron al rasgar en la misma carne. Tú recuerdas siempre, amigo Fausto, con Fafa tu compañera desde siempre y para siempre, aquellas noches de amigos del cante en Puente Genil, cuando Pedro Lavado daba un tirón del cable que animaba al magnetofón: \"Aquí no graba nadie —nos decía— y mañana volveré yo solo a emborracharme con los rebotes de este cante en las paredes.\" Cuando...\n\nFueron a cobrar, luego, por la taberna, la de Pedro Lavado después, sombras en la nieve, y los sones de soledad cordobesa, huyeron. Entonces, entonces cuando rompió Pedro por Petenera y por la Niña.\n\n—¡Cómo sabe ser Françoise Gérardin (Fafa) tu guitarra acompañante!\n\n¡Cuántas veces los ecos en tu alma de \"la Fernanda, irrompible curva alzada\", \"la Bernarda sombra erguida cortiblanca\", y cuantas Perratas, al arrullo de esa guitarra, han dormido \"al niño calladito que aprendía sin comprender\" han inundado tu estudio de noctívoro! Tus lienzos son noches pobladas de duendes, manes familiares, monstruos de tu cerebro. Tu obra es una frondosa forestación personal de gritos. En Torremolinos vi esa floresta última, abrumadora, fantástica y deslumbrante como un incendio purificador. Como buen flamenco, amigo Fausto, tú sabes —me agarro al presente con uñas y dientes porque tu obra está aquí— echarlo todo a un envite, saborearte y darte generosamente. Sólo así os comunicáis los artistas; pero tú, además —para fraseando los sones de tu guitarra— habitas las cuevas donde los cantes se hacen ecos en una ronda sin fin.\n\nEn Córdoba, a la luz de la Guitarra '96.",
    "title": "Fausto Olivares en las cuevas celestes donde los cantes se hacen ecos sin fin",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "43-44",
    "page_number": 43,
    "word_count": 767,
    "article_char_count_full": 4510,
    "article_char_count_review": 4510,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-05-44-right-fausto-olivares-la-persona",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJosé Fernández García\n\nConfieso que me es difícil, muy difícil, escribir sobre Fausto Oliva-res cuando aún su ausencia no es perceptible con la lejanía suficiente para intentar el desapasionado esfuerzo de acercarse a su persona y arte buscando la objetividad en el juicio. No se puede “explicarlo”, pero se pueden realizar en torno suyo maniobras para un acercamiento. A través de las preguntas, sensaciones, explicaciones, definiciones, aptitudes, afirmaciones, imaginaciones y observaciones, nos podremos aproximar a unas reflexiones más o menos acertadas que, siempre, estarán condicionadas a las distintas interpretaciones o prismas con las que, cada uno de los que le hemos conocido y tratado, seamos capaces de conformar. Cuando todos sus amigos lo hayamos hecho se tendrá de Fausto una\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_01 | trigger=\"fuera\"]\n\nde los que le hemos conocido y tratado, seamos capaces de conformar. Cuando todos sus amigos lo hayamos hecho se tendrá de Fausto una imagen en abanico en la que ninguna de las caras, a la hora del despliegue, semejará a la otra pero cuyo conjunto se acercará -¡tal vez!- algo a la verdad. Yo, de entrada, tomaré, como punto de partida, aspectos de su personalidad en los que creo que será fácil la unanimidad de las opiniones. Quizás, el primero, fuera su tolerancia en el respeto a los criterios ajenos. Tolerancia que él basaba en el diálogo, buscando compartir ideas y fórmulas, referidas a convicciones y experiencias, que predisponían al recíproco enriquecimiento, intentando, como último objetivo, el consenso de verdades exentas de polémica; tolerancia, con ausencia de cualquier atisbo de talante dogmático, apoyada en una inteligencia abierta a la reflexión y a la crítica. Un segundo aspecto, a destacar, sería su clarividencia entendida, no como la facultad de ver cosas ausentes o de adivinar el futuro, sino como la de ver claro aquello que tenemos al alcance de nuestros ojos apoyado en la presunción de una inocencia, no partidista ni infectada, renunciadora de los impulsos emanados del propio corazón que siempre nos dirige y que, a veces, nos equivoca. El tercero, sin que ello suponga la prioridad de los otros\n\n[ENDING CONTEXT]\n\nFafa, la excepcional compañera de Fausto, intuía o\n\nconocía la intención del artista y, tan sólo, un velo de tristeza, momentáneo en sus ojos, anticipó el penoso sentimiento que nos embargó ante la destrucción de la obra de arte.\n\n¡Así era Fausto!, ¡así era el artista! Denso de eternidad y humanidad. Vivo en el recuerdo para los que le conocíamos. Atemporal en el arte. Fausto fue él, y yo, que tuve la inmensa fortuna de conocerlo y tratarlo, quiero rendir en estas líneas homenaje sincero del afecto y de amistad, con las que él me regaló, dejando su huella imborrable que me acompañará siempre.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Fausto Olivares, la persona",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "44-46",
    "page_number": 44,
    "word_count": 1480,
    "article_char_count_full": 9204,
    "article_char_count_review": 2952,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "fuera"
      }
    ]
  },
  {
    "article_id": "1996-05-46-right-el-mundo-en-sus-ojos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPepe Vica\n\nEra entonces un muchacho, casi un chaval, que acabara de descubrir la calle, la amistad y la tertulia. Incluso hubiera dicho que acababa de descubrir la luz de no haber advertiido que la luz nació en él en la misma cuna y vivió siempre prisionera en sus ojos, iluminando su mente y dejándose proyectar limpia y clara por los grandes cañones de su mirada. El Fausto que yo conocí\n\nCasi todo parecía nuevo para él. Escuchaba siempre, con atención, y sonreía. Fausto siempre sonreía. Es cierto que la facultad de sonreír era lo único que, en aquellos tiempos, nos quedó a los jóvenes como herencia. Lo demás, casi todo lo demás, se lo habían llevado los demonios devastadores de la guerra, que aún por entonces aún dejaban asomar unos el rabo y otros los cuernos.\n\nAún estaban de obligada\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nrencia. Lo demás, casi todo lo demás, se lo habían llevado los demonios devastadores de la guerra, que aún por entonces aún dejaban asomar unos el rabo y otros los cuernos. Aún estaban de obligada moda las alpargatas de Antón y los panta-lones bombachos con bastos zurci-dos en las rodillas y la culera. Fausto era afortunado. Su padre, también Fausto de nombre, tenía una pe-queña fábrica de gaseosas y, aunque no podía hablarse de opulencia en un hombre que trabajaba de sol a sol, sí podían Sérvula, su esposa, y su numerosa prole, permitirse pisar la sombra de un suculento jamón, col-gado en la vieja y recia viga de su casa de la calle Arroyo. Cuando visi-taba su casa, salía con tortícolis de tanto mirar al techo. El tendría 15 años, poco menos que nosotros, y no recuerdo cómo se unió a nuestro grupo, un puñado de jóvenes locos por el teatro y algunos, como Pepe Cobo de Guzmán y yo mismo, también por el dibujo. El caso es que se integró pronto. Fausto caía bien; sabía escuchar, sabía ser acompañante, alternar sin estridencias y, sobre todo, sabía sonreír de una forma que daba alegría y hasta envidia. Creo que durante algunos años fui para él como un hermano mayor. Y él lo agradecía quizá porque estando a mi lado se evadía de esa responsabilidad de —siendo tan joven— ser el mayor de seis hermanos. Pero, pese a mi influencia, nunca logré que aceptara tan siquiera un pequeño papel en aquellos sainetes para aficionados que representábamos. Le gustaba todo aquello, pero quizá fue siempre demasiado tímido. Nunca me lo he planteado así, pero quizá fue en aquellos contactos con nuestro teatro, donde había excelentes jóvenes cantaores y cancioneras, donde el duende del flamenco se le coló por sus siempre alertados y expectantes ojos. Y en su alma se quedó para siempre porque el flamenco no podría encontrar mejor acomodo y abrigo. Su alma fue siempre de artista. S\n\n[ENDING CONTEXT]\n\nsus ojos claros y limpios se cerraron para siempre tranquilos, serenos, sabedores de que, a través de ellos, entró un mundo que el talento del artista devolvió cargado de magia, en el que alienta para siempre, inmortal, el espíritu de un hombre ejemplar y de un pintor prodigioso.\n\nY cosa de magia y encantamiento parece también que, cada vez que evoco la memoria de mi fraternal amigo Fausto Olivares, aparezca en mi ánimo una leve sonrisa, de un extraño sabor agridulce, que vence al instante ese amago de pellizco que intenta romperme el corazón. Y es que nadie, todavía, puede matar una sonrisa.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El mundo en sus ojos",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "46-47",
    "page_number": 46,
    "word_count": 1077,
    "article_char_count_full": 6150,
    "article_char_count_review": 3510,
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
    "article_id": "1996-05-49-left-25-de-julio-de-1995",
    "article_text_for_review": "Françoise Gérardin 25 de Julio de 1995\n\nDespués de tú irte, Fausto, me golpeó el brusco bofetón que Tsvetaeva lanzaba a la muerte de Rilke: \"Veamos; ¿cómo fue el viaje?\". Entre el contenerse y la queja, eligió, ella, seguir el paso: No sé, de los tres, el destrozo mayor ahora que nos separan mil interrogantes (minimalistas preguntas de nuestras minutos diarias).\n\nCelebrando un ofertorio te fuiste hombre frente al hombre, tu padre, asido al retrato captador de anhelo y espera, que él, y tú, cabales teníais por limitación de infinitud.\n\nAllí, donde te encuentras, ¿habrá letras nuevas por memorizar? ¿Estás, de Mardoqueos traspasados, averiguando los sones? ¿Cómo encontraste al Beni y a Pericón? ¿Os sorprendió la pronta de Lola?... Ya veo; me estoy pasando... Me concedes, con la cabeza, un \"sí-sí\" de gafas y brazos cruzados..., acostumbrado..., paciente... \"No preguntes por saber que el tiempo te lo dirá\"...\n\nSé que los mundos, el de aquí y el de allá, más que vecinos antónimos, son intrincados semblantes; mejor que nadie, tú supiste darles, a cada uno de ellos, el color de sus sonrojados desgarros, el humor de sus viscerales engaños, el viso de sus fluidas esperanzas, —sin más tragedia, sin más dogma— en unción terrenal, normal, celical. (Simple dependencia al desprendimiento de la luz). Y lo que nos parecía atroz, hermético, se desvela gracias a tu presencia ausente... ¡Otra vez! otra vez el doble, detrás de la cortina, soplándonos: “La verdad no tiene más que un camino”... Haz el favor pues ¡de parar un momento en él!, a esperarnos un rato... porque también me decías: “Dos veritas iguales. ¿Cuál de las dos voy a coger? Si cojo la de mi gusto mi perdición ha de ser”.\n\nAhora, di, ¿quién me traduce a mí el acento quebrajoso de una seguiriya mellada? Y ¿quién hay para contarme, en la Magdalena, melenchones de antaño, en largos paseos, películas de Vidor, de Kubrick, desenmascararme chirigotas y comparsas gaditanas? ¿Que me murmure el soliloquio de Segismundo y me desmenuce las tramas “caracteriales” del soberbio engaño en el coso ibérico?... Dime, ¿quién para dar un papirotazo a estas notas... ¡Que suenen!? ¿Quién para ofrecer un café a la hora del puchero y dar las buenas noches cuando es de día?\n\nEllos, los días, pasaban alumbrando tus sueños... Y, conmoviendo “diversas flores que sólo nacen en la sombra”, la Tierra noctámbula, para ti, abría el portón de sus áuricas entrañas, dejándote cavar, hasta el canto de un ágata que pulir cuando, a la hora de engastar tu obra, el alba con pinceles de metal, orlaba tu descanso...\n\nY la aurora última te arrebató la última labor; se nubló el Arco del Consuelo, se sopló un candil, se legó un lugar al recuerdo... aunque tu alma, benévola en el pedrusco más alto de la verbena de Santa Catalina asentía a la soledad chillona del barrio, sonando un fandango del Cepero; yo, comentándote: eso lo decía el Caracol, ¿no?\n\nSin eco, sin eco nos dejaste... Sólo, tu mudez de camafeos tornasolados y discrepantes gargantas donde mirarnos... donde el tiempo zanjado acerca sus ósculos de iris que nos remite del allá...\n\nFausto Olivares «Chascarrillo 2» Oleo, 65×50, 1985",
    "title": "25 de Julio de 1995",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "49-50",
    "page_number": 49,
    "word_count": 527,
    "article_char_count_full": 3143,
    "article_char_count_review": 3143,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-09-3-left-a-ram-n-porras",
    "article_text_for_review": "N o quiero entrar en valorar ninguna decisión particular del que hasta la fecha ha sido el director de esta Revista, aunque dicha tarea haya sido casi siempre compartida, y desde el año 1990 con mi persona. Mas creo conveniente efectuar algunas valoraciones que son pertinentes ante el abandono de dicha labor, aunque no de los trabajos en esta publicación por parte de Ramón Porras González, ideador, fundador y, hasta hace poco, director de Cándil.\n\nSí quiero creer que se ha perdido parte del poso intelectual y sabio que sobre el flamenco de esta revista él representa, pues sus conocimientos en ambas facetas son sobradamente conocidos, lo cual ha redundado en una línea editorial «alentada por la proporcionalidad en los asuntos debatidos, por el rigor en la exposición y, sobre todo, por el amor en la voluntad de dignificar el flamenco».\n\nAunque hace más de diecisiete años que me incitó a participar en los trabajos flamencos de la publicación con el encargo de la serie ¿Quiénes fueron los maestros?, hasta el día de la fecha, Ramón ha sido un referente sobre el flamenco básico y literario para mi persona. Sus escritos me han servido de guía literaria; sus consejos me han orientado sobre la forma de evaluar el hecho flamenco; sus mensajes e ideas han ampliado mis quehaceres sobre el tema, y su codirección ha propiciado que con humildad no exenta de dificultades ante lógicas carencias, asuma la dirección en solitario (por ahora) de Candil.\n\nQuizá su trabajo no haya sido extenso y continuado, pero su atenta vigilancia de lo publicado, su constante enfoque hacia parámetros ciertamente ortodoxos, sin olvidar los matices heterodoxos de sus escritos flamencos, así como el manteniamiento de una línea literaria brillante, han conformado una serie de artículos muy a tener en cuenta y en los que basar una determinada teoría sobre su visión del flamenco, siempre dirigida hacia el perfeccionismo y perfecta comprensión de nuestro arte.\n\nCon su retirada de las tareas de dirección, posiblemente se pierda un exhaustivo punto de vista riguroso del mundo flamenco; se anule en algo la diversidad de criterios hasta ahora imperante en la publicación, se tarde en ocupar el cierto vacío que siempre le queda a una obra cuando su inspirador se distancia, se manifieste cierto sentimiento de orfandad entre los que continuamos y se refleje la tristeza en los próximos trabajos. Lo que no se pierde es la labor atesorada durante dieciocho años, como bien queda reflejado en los ciento y pico de números publicados. Tampoco el aglutinamiento que supo ejercitar de personas estudiosas de este arte como Juan Antonio Ibáñez, José Cruz García, Fausto Olivares Palacios, Jesús Lechuga Cobo de Guzmán, Manuel Urbano Pérez Ortega, Alfonso Fernández Malo, Pedro Sánchez Ortega o José Luis Buendía López, todos ellos colaboradores iniciales de Candil. Y por ende, una serie de gestiones administrativas para consolidar la economía que hiciera viable la publicación de la Revista, como así ha sido.\n\nSe alejan dieciocho años de historia, la cual queda en los libros (mejor en la Revista), lugar donde se plasman las heroicidades, porque pienso que Candil es eso, una heroicidad desarrollada en su inicio por Ramón Porras González y continuada por un buen número de colaboradores (con los que pensamos continuar contando), que luchan denodadamente por contribuir a un mayor esplendor, perfeccionamiento y difusión de esta cultura musical universal como es el flamenco.\n\nRafael Varela Espinosa",
    "title": "A Ramón Porras",
    "periodical": "candil",
    "issue_id": "1996-09",
    "year": 1996,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 565,
    "article_char_count_full": 3488,
    "article_char_count_review": 3488,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
