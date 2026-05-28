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
    "article_id": "1982-03-22-right-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTítulo: GRANDES DEL FLAMENCO.\n\nReferencia: Philips 6248149 a 6248154 (6 L.P.) 6877131.\n\nIntérpretes: Antonio Mairena, Manolo Caracol, Pericón de Cádiz, Pepe el Culata, Ramón el Português, Rafael Romero, El Chocolate, Los Serranos, Pepe de Lucía, Terremoto de Jerez, La Paquera, Fosforito, Luis de Córdoba, Camarón de la Isla y El Lebrijano.\n\nGuitarras: Paco de Lucía, Melchor de Marchena, Juan Carmona «El Habichuela», Pepe «Habichuela», Ramón de Algeciras, Isidro Muñoz, Manuel Moreno Juanito Serrano, Pedro Peña, Serranito y Enrique de Melchor.\n\nAño: 1981.\n\nDiffícil nos resulta calificar esta nueva y larga entrega sonora: ¿es una antología cantaora, un muestreo o, simplemente, cuatro horas y media de buen flamenco? Dificultad que se nos acrecienta con una nueva pregunta —y no será la última—,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Grande\"]\n\nna antología cantaora, un muestreo o, simplemente, cuatro horas y media de buen flamenco? Dificultad que se nos acrecienta con una nueva pregunta —y no será la última—, ¿quién es el responsable de la selección? Miramos por todos los lados y no encontramos por sitio alguno el nombre del recopilador de cantes y cantaores. Sea, por tanto, «la casa» discográfica, el sello de Philips, el recopilador de esta nueva selección flamenca. Cierto que Félix Grande presenta la muestra con un bellísimo ensayo literario: «El arte flamenco: prodigiosa moral de la memoria», en el que la calidad de su prosa, repleta de íntimas tensiones poéticas, nos trae, una vez más, su equilibrada teoría sobre el cante como testimonio existencial, en el que, sin lugar a dudas, sabe leer entre sus rugosos y lacerados pliegues. ¿Hay que adjudicar al poeta, andaluz de inclinación, el resultado de toda la obra? Aunque así fuese, a nuestro parecer, habría que regatearle buena parte de la responsabilidad total, puesto que la recopilación de cantes y cantaores está efectuada sobre antiguas grabaciones de la casa productora, concretamente, de diversos y muy distantes discos de los años 1967 a 1980. De aquí que, como decíamos al principio, no se nos ofrezca el trabajo con carácter antológico —faltaría más que en el tendencioso mundo de las antologías se llegase a efectuar una sobre la «pertenencia» o no a una determinada casa comercial—, ni siquiera como un ajustado muestreo del flamenco revalorizado a partir del II Concurso de Arte Flamenco de Córdoba, 1956. Estamos ante cuatro horas largas de excelentes grabaciones flamencas; lo que, a nuestro juicio y sin duda alguna, merece un entusiasta aplauso. Pero hay más, esta treintena de artistas nos ofrecen una panorámica flamenca casi total, no ya en los cantes interpretados, también en los estilos y en las personales sensibilida\n\n[ENDING CONTEXT]\n\na nuevos caminos de recreación artística del flamenco. Siempre que los intentos sean realizados desde una responsable actitud, habrá que reconocer el esfuerzo. Pero cualquier comercialidad barata, cuenta, contará, con nuestro total rechazo. Y aquí el hecho nos resulta más doloroso al comprobar que el disco se ha realizado en Sevilla. Siempre hemos lamentado la falta de conocimiento de productores que hacen su trabajo en Madrid; al parecer, los hay también en Sevilla.\n\nDO SCANDIL\n\nAGENCIA DE LA PROPIEDAD\n\nINMOBILIARIA\n\nAvda. del Generalísimo, 8-1.º Teléfonos 22 58 54 - 22 58 58\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1750,
    "article_char_count_full": 11026,
    "article_char_count_review": 3488,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "Grande"
      }
    ]
  },
  {
    "article_id": "1982-03-24-left-un-libro-sobre-la-saeta",
    "article_text_for_review": "Siguiendo el repaso de la bibliografía flamenca más reciente, que ha iniciado para nuestra revista Manuel Urbano, nos toca hoy examinar un libro, breve en contenido, pero rico en sugerencias, que nos ha llegado hace poco tiempo: «La Saeta». En él, su autor (1) realiza un atinado estudio de esta manifestación flamenca tan de primera línea, en unos tonos marcadamente bibliográficos, es decir, que sin que por ello neguemos validez a su honesto trabajo, el autor estudia con detenimiento las fuentes sevillanas sobre el tema, lo que nos parece estupendo, pues ya van sobrando en la historiografía flamenca los alambristas que pontifican desde la cuerda floja de su ignorancia. Así, son manejados, y su pensamiento muy bien desarrollado en el libro, los testimonios saeteros de Luis Montoro, Benito Mas, Agustín Aguilar, Fray Diego de Valencia y Joaquín Guichot entre otros.\n\nCon ayuda de éstas fuentes, López Fernández va elaborando la teoría de cómo la saeta andaluza flamenca procede de las letras litúrgicas de Penitencias, rosarios y campanilleros, así como de los mandatos y pregones de las representaciones populares ligadas a temas religiosos. El autor defiende la geografía de Marchena, Cabra y Puente Genil como los tres puntos fuertes de definición de estilos saeteros, frente a los que prefieren considerar a Sevilla como matriz de este cante. Asimismo divide a las saetas por su cronología y ligazón a los viejos troncos en Narrativas, Explicativas y Afectivas, dedicando un apartado especial al grupo marchenero. El trabajo concluye con unas apretadas, pero clarísimas y sistemáticas conclusiones y una sustanciosa antología de saetas, divididas en grupos según la clasificación antes propuesta.\n\nEn resumen, un trabajo bien hecho, más divulgativo que científico, en una colección irregular en la que se deja entrever cierto tufillo chauvinista al tratarse los temas sevillanos.\n\n(1) Rafael LOPEZ FERNANDEZ. Edit. Grupo Andaluz de Ediciones. Colección: Cosas de Sevilla, número 6. Sevilla, 1981.\n\nJosé Luis Buendía",
    "title": "Un libro sobre la Saeta",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 315,
    "article_char_count_full": 2027,
    "article_char_count_review": 2027,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-03-24-right-dos-libros-en-el-centenario-de-d",
    "article_text_for_review": "AUNQUE NO QUEPA EN EL PAPEL\n\nNo es de extrañar que en un país como el nuestro —aquí donde el olvido, aquí donde la muerte— donde se practica con récord de marca olímpica el regateo y despojo de la auténtica valía intelectual, la necrofilia literaria con ocasión de centenarios y otras efemérides conmemorativas, al menos, nos sirve para refrescar la memoria con algo que, pese a ser patente y notorio, parece condenado a permanecer en las más oscuras buhardillas del desprecio: la existencia de unos hombres y unas ideas que la interesada inclemencia maniquea, garrula, obscena, depredadora, condenó a la más espantosa de las hogueras, la del silencio. Cierto que, a mi parecer, los más de estos centenarios no son otra cosa que capotazos a toro pasado, oropeles a lo juego-floral, revestimientos pontícales, o millares de notas a pie de página; olvidando que las ideas tienen su tiempo histórico y su recuperación no puede significar cosa distinta a la de un enlace vivo con el pasado, a un amarre de la memoria... De una vez, cualquier rescate del pensamiento ocultado —nunca perdido— desde el fijo asiento del hoy, no es un regreso, sino una asunción crítica de la Historia.\n\nManuel Urbano\n\nDos libros nos han arrastrado a estas personales divagaciones y que, por su interés, recomendamos de entrada al lector:\n\nEl primero de ellos es la edición facsímil de «EL FOLK-LORE ANDALUZ» (1), la revista que dirigiera en Sevilla —1882-1883— Antonio Machado y Alvarez, «Demófilo», y que para nosotros constituye todo un monumento literario repleto de historia y actualidad, y, lo que me parece de mayor interés, de auténtico ejemplo vivo. No vamos a entrar ahora en analizar el largo contenido, tan diverso como uno, de la revista, tan sólo nos limitamos a dejar constancia gozosa de su aparición impresa por ser fuente vivífica e inagotable de cualquier sondeo o estudio sobre muy buena parte de la cultura popular andaluza, de la que forma un todo indisoluble el cante flamenco del que, por cierto, contiene «El Folk-Lore» datos de auténtico interés y que merecerán algunas consideraciones en un próximo número.\n\nCAJA RURAL PROVINCIAL DE SEVILLA\n\nPor último, dejar constancia del magnífico prólogo y estudio biobibliográfico, obra de José Blas Vega y Eugenio Cobo, sobre el padre de los Machado. Un estudio sereno, estudioso, científico y casi total, al que, necesariamente, hay que acudir, para conocer la obra y vida del primer folklorista científico andaluz.\n\nCon una óptica distinta se nos viene «LA SEVILLA DE MACHADO Y ALVAREZ» (2) prologada por Concha Cobreros y de la que es autor el infatigable buceador de lo andaluz, Manuel Barrios. Un libro, segundo tomo de una colección recién abierta, que da justa tónica de todas las propuestas editoriales rigurosas y equilibradas, a la vez que ilustrativas. Un libro abierto al gran público lector y que, a su vez, da cumplida satisfacción al lector más avezado.\n\nBarrios acierta plenamente en su forma de enfocar este librito aunado por una gran serie de textos diversos y representativos que dan cabal idea de la Sevilla que le tocó vivir y padecer, sin menoscabo alguno, todo lo contrario, de la labor investigadora del egregio institucionista. Un libro, insisto, que arroja un saldo positivo: la divulgación de un autor, una época y una ciudad. Un libro y una colección que, sin duda alguna, merecen ser seguidos como ejemplo por otras provincias.\n\nEn conclusión, dos libros que continúan el homenaje andaluz a Demófilo por cuanto ellos suponen fuente de conocimiento, rescate histórico y anclaje en las tan menospreciadas investigaciones sobre el folklore. Dos libros radicalmente distintos, pero que, a mi juicio, cumplen plenamente sus objetivos. Ojalá que la pasión investigadora de Antonio Machado y Alvarez continúe con el rigor y jondura que nos traen estas dos entregas sevillanas. Confiamos y esperamos.\n\nPolígono «LOS OLIVARES» - Teléfonos 22 30 00 - 22 30 04 - J A E N\n\nDISTRIBUTOR OFICIAL DE:\n\nVIDRIO LAMINAR DE SEGURIDAD - ACRISTALAMIENTOS EN GENERAL\n\nTRABAJOS DE ALUMINIO PARA OFICINAS Y TERRAZAS",
    "title": "Dos libros en el centenario de «Demófilo»",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 666,
    "article_char_count_full": 4062,
    "article_char_count_review": 4062,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-03-25-right-discograf-a-placas-de-artistas-f",
    "article_text_for_review": "Por Manuel Yerga Lancharro\n\nEL CARBONERILLO",
    "title": "Discografía (placas) de artistas flamencas",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 6,
    "article_char_count_full": 43,
    "article_char_count_review": 43,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-05-3-right-editorial",
    "article_text_for_review": "CON fina sensibilidad para con los estratos humanos que soportan el cante, para con los protagonistas de nuestro arte inconmensurable —cante, toque y baile—, la Consejería de Cultura de la Junta de Andalucía prepara unos encuentros de artistas flamencos en los que se estudien, debatan y adopten conclusiones sobre su propia problemática. Iniciativa esta que «CANDIL» reconoce y aplaude —sin previo conocimiento del programa, ni cuales sean los temas sujetos a diálogo y aceptando como propios los puntos que se adopten como definitivos tras riguroso examen y compromiso—, ya que no es ajena a la realidad existencial y profesional de los hombres y mujeres que hacen el flamenco.\n\nY si hemos calificado de fina la sensibilidad de la Junta de Andalucía al promover estos encuentros, no ha sido por azar, ya que, a nuestro parecer, los problemas del artista flamenco y su larga casuística han transitado por la historia entre el manifiesto desprecio y el olvido, lo que, quiérase o no, significa pura y llana marginación social.\n\nSi preguntamos a la verdad, nos encontraremos que el cantaor, el bailaor o el guitarrista no ha obtenido de la afición, salvo contadísimas excepciones, más que una valoración artística —la apreciación económica es reciente y, por cierto, bien desiquilbrada—y siempre que pudo mantenerla con sus facultades; cuando ellas le faltaron por enfermedad u otra causa, subsistió entre un paternalismo limosnero o en el total desamparo humano y la indigencia, algo de lo que está repleta la historia de nombres que lo certifican y que, desgraciadamente, sigue aconteciendo en nuestros días. Al cantaor le hemos pedido siempre que se nos diera con toda su voz y, por el contrario, en escasas ocasiones le hemos aceptado su más íntima palabra. Nuestra insensibilidad para con su problemática humana y profesional ha sido ostentosamente manifiesta. El señoritismo del «te pago para que me cantes y hemos terminao» continúa desde los orígenes de este arte hasta hoy mismo, aunque las relaciones contractuales entre los protagonistas del flamenco y sus destinatarios sean bien diversas y, sin lugar a dudas, fuente de nuevos problemas humanos y profesionales.\n\nBien es cierto, y es una importante salvedad a cuanto hemos expuesto, que la Institución para los flamencos de la tercera edad se nos alza como algo benemérito; pero, y lo decimos desde hace años, no puede ser aceptado más que como un peldaño de lo que en justicia ha de reconocésele a estos trabajadores: la Seguridad Social. Quede claro, no estamos contra la beneficencia, aunque preferimos la justicia.\n\nY no es este, aunque se nos ofrece como primario, el único tema que pueden abordar los artistas presentes en estos encuentros; no pocos son los que les nacen de las actuales formas de su trabajo, o de las responsabilidades que, como protagonistas que son, tienen contraidas con el arte del que son depositarios. Un arte, parte fundamentalísima de la cultura andaluz, que les exige su purísima conservación y, en la medida de lo posible, su engrandecimiento.\n\nPero no somos nosotros quienes tenemos que efectuar el programa de trabajo, de confección exclusiva por los interesados. «CANDIL» sólo se felicita por estos encuentros, que desea fructíferos para el flamenco y sus protagonistas.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 527,
    "article_char_count_full": 3269,
    "article_char_count_review": 3269,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
