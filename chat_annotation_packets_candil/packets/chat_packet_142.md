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
    "article_id": "1986-11-22-right-iscografia-flamenca",
    "article_text_for_review": "FICHA TECNICA\n\nGuitarras: Raimundo Amador, Rafael Riqueni y Miguel «El Roto».\n\nJaleos y palmas: Martín Revuelo, «El Gel», «El Botero» y Paco Jiménez.\n\nFotos: Emilio.\n\nProductor: Diego Pizarro Fernández.\n\nDiseño carpeta: Pedro Castro.\n\nIngenieros de sonido: José Torrano y Jesus Bola. Grabado en los estudios «Alta Frecuencia» de Sevilla.\n\nEdita: PASARELA, S. L. Colaboración especial: Martín Revuelo. Letras: Antonio Murciano, Martín Jiménez y populares.\n\nManuel Martín Martín\n\nual brisa que nos invade y devora\n\ninvade y devora acogemos este primer larga duración de una trianera de oro que justifica, apodicticamente, aquello de que cantar gitano es siempre señal de buen cante. Por ello, el maestro en sabiduría jonda, Paco Vallecillo, sentenció en una noche memorable:\n\n¡Es mucha Juana esta Juana que al revuelo de sus duendes tiene revuelta a Triana!\n\nJuana la del Revuelo, nacida en plena Cava gitana, en la sevillanísima calle Evangelista, de cobriza tez y ojos de azabache, dotada por la divina naturaleza de un gracejo singular, es una sacerdotisa de la liturgia jonda, que le habla a la pena de tú para convertirla, con sus puñaladas lúdicas y festeras bañadas en la ranciedad improvisada, en cante épico y plañidero.\n\nLos tercios morenos, gimientos y sentidos, cortos y secos, se mecen en su garganta para modular los quejíos del cante verdad, anto-jándosenos jocosos y profundos, pero invitando al recogimiento espiritual que supone el desahogo turbado de toda una gitanería oprimida y aún marginada.\n\nJuana la del Revuelo, gitana de viejo bronce, zalamera y cautivadora hasta la lasitud, pletórica de vitalidad y temperamento, conforma la gratificante imagen musical de aquellos que decapitan su misterioso destino con la queja más sentida.\n\nEs reconfortante la presencia de Juana la del Revuelo en el flamenco contemporáneo porque su estampa de postal antigua —canasto a la cintura y pololos de fuste—, y el arrebatador revuelo que marca su cuerpo satisfacen una necesidad emocional y visual aderezada del refinamiento de la gracia a través de la danza. La calidad sonora ha sido cuidada con esmero y las guitarras consiguen el acople vigoroso y acentuado para culminar una obra donde se atisba el buen hacer orientador de su marido Martín Revuelo.\n\nEn otro orden de cosas, si bien la evolución de la guitarra ha contribuído en los últimos tiempos a En definitiva, una primera entrega que, en base al delirio rítmico y a la lujosa flamenquería de Juana, ha conseguido ser el disco más vendido del pasado año, a pesar de alguna concesión mercantil, pero donde se reflejan los éxitos conseguidos por la trianera en la movida festivalera.\n\nque el más negado ose hacer frente a la bulería o a los llamados cantes de compás, no es éste el caso de Juana la del Revuelo quien teoriza armónicamente sobre el compás y el ritmo, domina los entresijos de los tangos, se motiva recordando los matices caracoleros por fandangos, derrama sal por la bahía y muestra bisoñez desacos-tumbrada en los aires de Levante.",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 488,
    "article_char_count_full": 3014,
    "article_char_count_review": 3014,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-11-23-left-noticiario-flamenco",
    "article_text_for_review": "El Servicio de Publicaciones de la Universidad de Cádiz, ha editado cuatro libros sobre temas flamencos, los cuales referenciamos a continuación:\n\nPOESIA FLAMENCA. ANALISIS DE LOS RASGOS POPULARES Y FLAMENCOS EN LA OBRA POETICA DE ANTONIO MURCIANO (M. a C. García Tejera). CANTES GITANO ANDALUCES BASICOS (Alfredo Arrebola).\n\nACROSTICOS DEL ARTE FLAMEN- CO (Félix García Vizcaíno).\n\nCANTAORES ANDALUCES (G. Nú- ñez de Prado).\n\nPara pedidos, los interesados pueden dirigirse al citado Servicio de Publicaciones, calle Ancha, núm. 16, CADIZ.\n\nOrganizado por la Asociación Cultural Hijos de Almachar de Baracaldo (Vizcaya), ha sido convocado el «Primer Concurso de Letras Flamencas».\n\nEl tema es libre. Será necesario un mínimo de 20 versos que encajen en la estructura métrica y rítmica de cualquier tipo de cante flamenco.\n\nLos trabajos deberán enviarse mecanografiados por triplicado, sin que exista en los folios ningún dato sobre el autor. En sobre cerrado aparte deberá figurar La fecha de admisión de los trabajos finaliza el 16 de abril del presente año.\n\nnombre, dirección y teléfono, junto con cualquier otro dato que él pueda considerar de interés.\n\nLos trabajos deberán ser enviados a: «Primer Concurso de Letras Flamencas», Asociación Andaluzay Hijos de Almachar, Castilla la Vieja, núm. 3, Baracaldo (Vizcaya).\n\nPara más información los interesados pueden dirigirse a la citada Asociación.\n\nEl pasado 13 de diciembre de 1986, presentó su dimisión a la Confederación Andaluzas de Peñas Flamencas, el hasta entonces Presidente RAMON PORRAS GONZA-LEZ.\n\nEl presupuesto de la Confederación, hasta esta fecha, ha estado constituido por una subvención de la Consejería de Cultura de la Junta de Andalucía, por cantidad de cinco millones de pesetas. De dicha dotación han recibido las distintas Federaciones Provinciales las cantidades siguientes: El criterio para la distribución de estos fondos, ha estado en función del número de Peñas miembro de cada Federación, excepción hecha de Málaga, que por no estar completamente constituida, sólo ha recibido una parte de la subvención que le podría, en el futuro, corresponder.\n\nEl resto de la aportación de la Junta de Andalucía destinada a gastos de organización y funcionamiento se encuentra depositada en las arcas de la Confederación, para lo que disponga la Junta entrante.\n\nLa Consejería de Cultura de la Junta de Andalucía (Asesoría de Flamenco), nos remite relación de los distintos Consejos que configuran la FUNDACION ANDALUZA DE FLAMENCO, y que a continuación detallamos:\n\nConsejo Rector. Composición:\n\nPresidente Honorífico: Excmo. Sr. Presidente de la Junta de Andalucía, don José Rodríguez de la Borbolla y Camoyán.\n\nConsejeros natos:\n\nPresidente, Excmo. Sr. Consejero de Cultura, don Javier Torres Vela.\n\nVicepresidente 1.º, Excmo. Sr. Alcalde de Jerez de la Frontera (delegable), don Pedro Pacheco Herrera.\n\nVicepresidente 2.º, Excmo. Sr. Presidente de la Diputación Provincial de Cádiz (delegable), don Alfonso Perales Pizarro.\n\nConsejeros natos: Sr. Delegado de la Consejería de Cultura en Cádiz, don Sebastián Saucedo Moreno.\n\nSr. Presidente de la Caja de Ahorros de Je- rez, don Manuel Navarro Palacines.\n\nSr. Presidente de la Confederación Andaluzia de Peñas Flamencas (vacante).\n\nConsejeros electivos:\n\nConsejería de Cultura: D. Pedro Navarro Im- berlón. D. Francisco Vallecillo Pecino.\n\nAyuntamiento de Jerez: D. Juan Taboada Saborido. D. Cristóbal Romero Gandolfo.\n\nDiputación de Cádiz: D. Antonio Fernández García. D. Antonio Marmolejo.\n\nCaja de Ahorros: D. Jesús Mantaza y G Figueras. D. Mariano Ruiz Carretero.\n\nComité ejecutivo:\n\nPresidente de la Fundación (delegable). Junta de Andalucía: D. Francisco Vallecillo Pecino.\n\nDiputación de Cádiz: D. Antonio Fernández García.\n\nAyuntamiento de Jerez de la Frontera: D. Juan Taboada Saborido.\n\nCaja de Ahorros de Jerez de la Frontera: D Mariano Ruiz Carretero.\n\nPresidente de la Confederación Andaluzia de Peñas Flamencas (vacante).\n\nMiembros solamente con voz:\n\nDirector Gerente: D. Joaquín Carreras Mo- reno.\n\nSecretario: D. Salvador Salvago Mora. Composición del CONSEJO ASESOR:\n\nD. Manuel Barrios Gutiérrez, Tomas Pavón, 263 - SEVILLA.\n\nD. José Blas Vega, Espíritu Santo, 42, 28004 - MADRID.\n\nD. José Luis Buendía López, Fermín Palma, 1-B, 6.°-C - JAEN.\n\nD. José Manuel Caballero Bonald, María Auxiliadora, 5 - MADRID.\n\nD. Manuel Cano Tamayo, Colonia Cervantes, 13, 18008 - GRANADA.\n\nD. Juan Ignacio González Merino, Doctor Barraquer, 6 - CORDOBA.\n\nD. Félix Grande Lara, I.C.I. C/. Reyes Cató- licos, 4, 28040 - MADRID.\n\nD. José Luis Ortiz Nuevo, Avda. Miraflores, bloque 62, 2.º-Izda., 41008 - SEVILLA.\n\nD. Fernando Quiñones Chozas, María Auxiliadora, 5 - MADRID.\n\nD. Juan de Dios Ramírez Heredia, Palacio de las Cortes, Carrera de San Jerónimo - MA-DRID.\n\nDr. Antonio Reina Gómez, Palma del Río, 8, 41008 - SEVILLA.\n\nD. Manuel Ríos Ruiz, Hermanos Alvarez Quintero, 2-5.º - MADRID.\n\nD. Alfredo Sánchez Fernández, Granada, 72, 04008 - ALMERIA.\n\nD. Luis Suárez Avila, San Juan, 17, 11500, EL PUERTO DE SANTA MARIA.",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 765,
    "article_char_count_full": 5041,
    "article_char_count_review": 5041,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-11-23-right-ia-flamenca-placas",
    "article_text_for_review": "Por: Manuel Yerga",
    "title": "Discografía flamenca (Placas)",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 3,
    "article_char_count_full": 17,
    "article_char_count_review": 17,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-3-right-flamenco-92",
    "article_text_for_review": "Editorial\n\nDesde muy diversos sectores sociales se está subrayando —con la necesaria lógica y coherencia, entendemos— el compromiso histórico que representa la cita del 92, para todo el Estado español y particularmente para Andalucía. Un reto de tal naturaleza no debe ser ignorado por la cultura autóctona de este país, y por ende, la presencia del Flamenco en tan señalada efemérides debe de realizarse; ello significa a nuestro juicio, abundar aún más, si cabe, en la dignificación. A riesgo de que se nos repute reiterativos, hemos de denunciar los oprobios, las constantes vejaciones de que se hace objeto al Flamenco, desde ciertos instrumentos de comunicación, desde colectivos desinformados y aun desde diversas instituciones del Estado. El pueblo, por lo general, no conoce o conoce mal el Flamenco. Lo que contribuye a que persista, incluso en la sociedad andaluza una como contracultura de lo jondo, aliñada de míticas insensateces, tópicos y lugares comunes, donde los contenidos jondos ceden la vez a montajes esperpénticos, y así los signos que identifican al Flamenco, la liturgia de este arte, es centro de ludibrio y escarnio para innúmeros payasos andaluzados.\n\nSería injusto ignorar los esfuerzos de toda índole realizados por la suprema institución andaluza, motivando, subvencionando, incentivando, promoviendo estructuras de coordinación en orden a una mayor y mejor difusión, conocimiento y contemplación del Flamenco. Tal afirmación suscitará irónica sonrisa en quienes confunden la gratitud y el justo reconocimiento hacia una institución, con el servilismo. Pero ese es otro problema. Ello no obstante, los objetivos están muy lejos de alcanzarse y sólo se comienza en ese camino.\n\nEl reto del 92 es una espléndida ocasión para que instituciones, peñas, federaciones, confederación, otros colectivos culturales y en general la afición, se esfuercen por consolidar esa vía que conduce hacia la dignificación plena del Flamenco. Es necesario que este pueblo someta a la consideración de propios y foráneos su hermoso y único patrimonio cultural.",
    "title": "Flamenco 92",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 316,
    "article_char_count_full": 2069,
    "article_char_count_review": 2069,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-4-right-reflexiones-ante-la-revoluci-n-m",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nGitano: tú te has dejao el corazón en pedazos donde quiera que has cantao. Fosforito\n\nManuel Martín Martín\n\nE stamos a tres años y medio de la des-\n\naparición del primer Hijo Predilecto de Andalucía, el Excmo. Sr. D. Antonio Cruz García, «Antonio Mairena», y, ya que estoy obligado a decir lo que creo, pienso que éste es el marco idóneo para desgranar algunas reflexiones y llamar a las cosas por su nombre. También para convenir que el mairenismo está más vivo que nunca, que continúa siendo más realidad que historia —a pesar de los espúreos mairenistas de ocasión que en vida rindieron pleitesía al Maestro (permitaseme la solemnidad mayúscula) y que hoy se avergüenzan de ello—, así como para establecer un principio diferenciador, abierto y constructivo, que llene de contenido la labor\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"históricos\"]\n\nMaestro (permitaseme la solemnidad mayúscula) y que hoy se avergüenzan de ello—, así como para establecer un principio diferenciador, abierto y constructivo, que llene de contenido la labor didáctica y hermenéutica de quien ganó la plácida nirvana de los elegidos por cuanto gesta la lección más cabal y edificante de los doscientos años de cante gitano-andaluz. Sin necesidad de alardear de excursionismos pretéritos, sin remontarnos a documentos históricos, circunscribiéndonos a los hechos que conocemos (no por dudosas tradiciones orales o por lecturas, sino por haberlos vivido), hemos de aceptar que se ausentó el Excmo. Sr. D. Antonio Cruz García, pero el mairenismo, su magna obra, permanece vivo e inmortal en el indeleble recuerdo de ferencias son más acusadas que nunca, los influjos del mairenismo no están cancelados y múltiples testimonios lo demuestran. Así, la esencia sin definición de la revolución mairenista —entendida como cambio total y radical en la movida jonda, con alteración de gustos, rescate, engrandecimiento Así escribía para CANDIL don Antonio Mairena: «Fue por los años 29 cuando empezó mi carrera..., en un concurso el año 24 en Alcalá de Guadaira; un jurado..., me adjudicó el primer premio» la afición y en la memoria de los cantaores contemporáneos que, entre tercio y tercio, beben de esta inveterada fuente, rebuscando las vetas perdidas del Cante y la queja más sentida en esta cantera del lamento que se nos presenta como el pilar más firme de nuestra genuina manifestación musical. En un tiempo en que las intery reivindicación de matices básicos, y con la creación, desarrollo y recreación de estilos desconocidos, tanto como con la difusión y potenciación del Cante gitano-andaluz—, supone\n\n[ENDING CONTEXT]\n\na los detractores, todo lo que huele a Cante está hoy impregnado del mejor perfume mairenista, ya que el maestro dio la clave de toda la gitanería subsiguiente al mairenismo. Por eso, hoy, todos los cantaores pueden asumir la condición de herederos legítimos de un gitano universal y andaluz, cien por cien, Antonio Mairena, que entrelazó y enhebró con hilos de plata todas las voces de su pueblo y que bien pudo parangonar con León Felipe los versos del poeta.\n\nYo no soy más que una voz, la tuya, la de todos, la más genuina, la general, la más aborigen ahora, la más antigua de esta tierra...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Reflexiones ante la revolución mairenista",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 4625,
    "article_char_count_full": 28119,
    "article_char_count_review": 3355,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "históricos"
      }
    ]
  }
]
```
