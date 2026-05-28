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
    "article_id": "1989-05-16-right-silverio-franconetti",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSeminario de homenaje y conocimiento a su memoria, organizado por José L. Ortiz Nuevo con el patrocinio del Ayuntamiento de Sevilla\n\nY allá a las tres de la noche, porque tó este tiempo Chacón no estuvo más que en conversación, bebiendo y alternando, pues va y dice:\n\n—Bueno, pues voy a cantar.\n\nY a esa hora, y como era verano, no había un balcón en la calle Abades que no estuviera abierto, y la gente escuchando, como si estuvieran escuchando a los ángeles. Era entonces cuando Chacón estaba en tó su apogeo. Y se le ocurrió al Argabeño decir:\n\nOiga usted, Antonio, ¿por qué no canta uste un cante de Silverio?\n\n—Con mucho gusto.\n\nY me acuerdo que cantó un cante de Silverio que tó el mundo llorando por la cara abajo. Se levantó Fernando el Herrero y le dijo:\n\n—Oiga usté, Antonio, le voy a usté\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nhando, como si estuvieran escuchando a los ángeles. Era entonces cuando Chacón estaba en tó su apogeo. Y se le ocurrió al Argabeño decir: Oiga usted, Antonio, ¿por qué no canta uste un cante de Silverio? —Con mucho gusto. Y me acuerdo que cantó un cante de Silverio que tó el mundo llorando por la cara abajo. Se levantó Fernando el Herrero y le dijo: —Oiga usté, Antonio, le voy a usté a pedir un favor, dígamelo usté con el corazón, es que ese hombre podía cantar eso mejor que usté acaba de hacerlo? Y tenía Chacón el sombrero en una percha, se levantó y cogió el sombrero, se cuadró, se puso los pies firmes y dijo: —Señores: para hablar de ese señor hay que descubrirse, ¡muchísimo mejor que yo! Y claro, tos nos quedamos asombraos del respeto que le tenía Chacón a Silverio, y de quién sería ese hombre en su época. (Del libro: «Pepe el de la Matrona. Recuerdos de un cantaor sevillano») E 1 30 de mayo de 1989 se han cumplido cien años de la muerte de Silverio Franconetti Aguilar, cantaor de flamenco. Nacido en Sevilla (1829), hijo de don Nicolás Franconetti, natural de Roma; y de doña María de la Concepción Aguilar, de Alcalá de Guadaira; el gran Silverio es una de las más destacadas figuras en la historia del género, músico genial, pionero en la tarea de difundir a los públicos el arte al que consagró su vida. Según las escasas noticias que, por ahora, informan de su biografía, Silverio empezó bien pronto a entusiasmarse con la música de los arrabales andaluces. Siendo un niño, la familia Franconetti se traslada a Morón, y, allí, el muchacho aprendiz de sastre, abandona la disciplina laboral para entretener sus horas en las fraguas de los gitanos, donde dicen que pudo ejercitar su predilecto aprendizaje escuchando al\n\n[ENDING CONTEXT]\n\nFernando Turina\n\nDía 2 de junio: Reales Alcázares.\n\n«Las otras músicas que en su tiempo se tocaban» Manuel Cano\n\nDía 3 de junio: Teatro Lope de Vega\n\n«Con sus palabras y sus cantes propios» Calixto Sánchez, Pedro Bacán, Naranjito de Triana, Manolo Franco, Manuel Mairena, José Luis Postigo, Carmen Linares, Paco Cortés, María Soléa, Moraito Chico, La Tomasa, Eduardo de la Malena y Milagros Mengíbar. Antología de los Cubatas. «Chirigota de Cádiz».\n\nPUBLICIDAD\n\nRepresentante\n\nJ. A. PULPON\n\nESPECTACULOS INTERNACIONALES\n\nO'Donnell, 3, 4.° Teléfonos 222058 - 216920 Particular: 228078\n\n41001 SEVILLA\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Silverio Franconetti",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "16-19",
    "page_number": 16,
    "word_count": 1514,
    "article_char_count_full": 9163,
    "article_char_count_review": 3369,
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
    "article_id": "1989-05-19-right-a-ana-m-rquez-bailaora-poema",
    "article_text_for_review": "A Ana Márquez, bailaora\n\nas manos abiertas, el rostro crispado y en las venas cien toros de fuego perseguidos de cerca por galopes de caballo. Como un terremoto el zapateado, con la fuerza de un barco sin quilla, desarbolado, sin más bandera que tu pecho erguido y la inspiración que sube desde un tronco milenario para anidar en el hueco sin voces del taranto. Desde el primer compás sé que miras sin verlo hacia ese punto vacío del espanto en el que tu cuerpo persigue mil duelos de aspavientos desolados. Y tu carne morena, triunfante, bailando, va creando bellezas efímeras sobre la magia del tablado. Hasta que de pronto, como en la tormenta, el zapateado, solemne, pausado, oprime de nuevo al silencio con la agonía del tacón a ritmo de tangos, y concita angustias porque nos lleva implacable, desbocado hasta el abismo insondable del llanto.",
    "title": "A Ana Márquez, bailaora Poema",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 145,
    "article_char_count_full": 848,
    "article_char_count_review": 848,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-20-right-madrid-flamenco",
    "article_text_for_review": "E 1 16 de abril y de nuevo en el Cine Consulado, la Peña Flamenca, Chaquetón, con la colaboración del grupo Cultural «Sintel», celebró en Madrid su VI Festival de Arte Flamenco, abarrotado de un público al que nuevamente tenemos que felicitar por su extraordinario y CABAL comportamiento que permitió su desarrollo con una atención, silencio y afición, dignas del mayor elogio. Dificil papeleta tenía la organización para superarse al poner el nivel tan alto en el festival del año anterior, lográndolo con un cartel más amplio y también atrayente, suponiendo que también de nómina más costosa, obstáculo este que supongo habrá podido superarse con el gran entradón que hubo al agotarse todo el taquillaje. Se inició el festival con la actuación de Enrique de Melchor, en un solo de concierto que enardeció al público con una selección de matices y facetas, que al ofrecernos la ya absoluta madurez en lo artístico de este joven maestro, justifican plenamente el primerísimo lugar que ocupa en su profesión en la que continúa acaparando premios y laureles. De este hombre hemos dicho ya tantas y tan merecidas cosas que a quienes escribimos de esto con tanto atrevimiento como afición y modestia, nos lo pone ya difícil para encontrar palabras con la que continuar haciéndole justicia.\n\nContinuo Carmen Linares, acompañada por Enrique con gusto y primor (a tal señora, tal honor), cantándonos tientos, alegrías y bulerías por soleá, con una maestría y una casta que también justifi-\n\nAntonio Corcobado\n\n(Correspondal)\n\ncan el primerísimo puesto que ocupa entre nuestras actuales cantaoras.\n\nSiguió José Mercé, con una magnífica actuación llena de poderío y de enjundia jerezana, por soleá, siguiriγas y bulerías de la mejor estirpe que nos sitúan ante un cantaor, ya magnífica realidad, y con un futuro esplendoroso dada su envidiable juventud..\n\nCerraron la primera parte de este festival, la bailaora Pepa Montes con Ricardo Miño y su grupo, magnífico conjunto que, con su extraordinario compás, buen cante y mejor toque, prepararon el ambiente para que Pepa Montes realizara una buena exhibición de baile por soleá con reminiscencias de bailaora antigua, ritmo y compás perfectos llenos de armonía y elegancia. Chaquetón, titular de la Peña organizadora, abrió la segunda parte con dos malagueñas de distintos corte y estilo a las que siguieron soleá y tangos, dentro de la mejor escuela gaditana que tan bien conoce desde sus orígenes. Como la ilusión, afición y profesionalidad rinde sus frutos a quien persevera, es este hombre ya también una magnífica realidad que ha calado hondamente en esta afición madrileña que supo agradecerle con entusiasmo sus cariñosos piropos a nuestra capital.\n\nJosé Menese tenía la competición muy cuesta arriba, en principio porque todos los artistas que le precedieron, estuvieron a magnífica altura, y en segundo lugar porque al resultar él, el indiscutible triunfador del festival anterior, tenía la obligación y responsabilidad de cuidar su forma de manera más exigente, para afrontar de manera airosa las rivalidades que en lo artístico le crean sus compañeros.\n\nNo me he olvidado de ese gran artista y mejor profesional que es Juan Habichuela, dejándole intencionadamente para el final de este comentario, para agradecerle doblemente su voluntaria y sacrificada supeditación, que se impone para que en su eficaz y siempre brillantísimo acompañamiento, encuentre el cantaor su mejor lucimiento. Enhorabue-na, maestro.\n\nTuvo el Festival un final brillante en el que todos los artistas nos ofrecieron un divertido cierre con facetas bailaoras que, en algunos, desconocíamos.\n\nOtro reto para el año que viene, amigo Pablo Tortosa, ¿lo superarás? Yo apuesto porque sí, al conocer tu tenacidad, inteligencia y ambición, para que la peña que presides, siga siendo espejo y ejemplo de cómo se crea y hace afición.",
    "title": "Madrid flamenco",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 611,
    "article_char_count_full": 3848,
    "article_char_count_review": 3848,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-21-left-homenaje-en-francia-a-una-ex-bai",
    "article_text_for_review": "E n Toulouse ha tenido lugar el homenaje organizado por el Atelier Flamenco Andalou a la que fue popular bailaora Carmen Gómez, La Joselito, un singular personaje del espectáculo flamenco que inició una larga experiencia profesional al principio de los años veinte. Carmen Gómez vive en Toulouse hace más de diez años y desde fecha anterior se residenció en Francia, conservando su nacionalidad española.\n\nEl acto, organizado por Isabel Soler, directora del Atelier Flamenco, se celebró en el Hall del monumental Teatro de la Ópera tolosano, y estuvo presidido por el vicealcalde de la capital del Alto Garona que impuso a La Joselito la medalla de oro de la ciudad en presencia del cónsul de España y de una nutridísima representación de la cultura y los medios de información franceses y de numerosos aficionados españoles y galos.\n\nEl asesor de flamenco de la Consejería de Cultura, en la representación del director general de Emigración, agradeció en nombre de la Junta de Andalucía el homenaje dedicado a esta popular artista española. El mismo día había participado en una mesa redonda realizada por Radio Francia alrededor de La Joselito, y posteriormente pronunció en el Atelier Flamenco una charla sobre este arte en la cultura.\n\nA continuación publicamos una breve semblanza de esta artista que todavía conserva un amplio saldo de arte intacto, como demostró para asombro de muchos asistentes. Le acompañó generosamente a la guitarra otro gran artista español radicado en Francia, Pedro Soler, hermano de Isabel, constantes evocadores de la tierra andaluza por la que beben los vientos. Isabel desarrolla en Tolosa una actividad didáctica flamenca encomiable a través de su taller. Y la alcaldía de la monumental ciudad que atraviesa el Garona ha tenido un gesto, para con España y Andalucía, de singular exquisitez y delicadeza.\n\nPublicamos a continuación la breve biografía que La Joselito ocupa en el Dic- cionario Enciclopédico de Editorial Cinterco. J. Nervión\n\nJOSELITO, La\n\nNombre artístico de Carmen Gómez, al parecer porque cuando tenía siete años el torero Joselito «El Gallo» le dio su nombre, bautizándola con vino de Jerez. Cartagena (Murcia), 1906. Bailaora. Casada con el guitarrista Juan Relámpago. Discípula de La Macarrona y de Antonio el de Bilbao. Crecida en Barcelona, frecuentó desde muy niña los cafés cantantes de la ciudad, donde se inició artísticamente, sobre todo actuando en el denominado Villa Rosa, hasta los dieciocho años. Durante siete meses lo hizo en el Kursaal Imperial de Madrid, en 1924. Con su marido debutó en el Teatro Romea, en 1925, año que también actúa en el vallecano Teatro Goya, junto al Niño de Tetuán. Con el elenco de La Argentina, se presenta en el Fémina de París, y después en la Ópera Cómica, en 1929, en compañía de Frasquillo, Juan Martínez y Viruta. Se instala en la capital de Francia, donde alterna su dedicación a la enseñanza de su arte con sus recitales e intervenciones en distintas obras, entre ellas la titulada Frasquita, en 1933, en la citada Ópera Cómica. En 1936 realiza varios recitales en la Sala Pleyel parisina con Ramón Montoya, así como en Bélgica y en Gran Bretaña. Realiza giras por Europa, América y Australia. En 1940, toma parte con sus bailes en la versión coreográfica de la obra cervantina, «La ilustre fregona», en la Ópera de París, y en 1942 vuelve a la Sala Pleyel, junto a José Torres. Ha grabado en discos con la guitarra de Pedro Soler y ha intervenido en la grabación discográfica Riches heures du flamenco, con Pepe de La Matrona y Jacinto Almadén, así como en las películas cinematográficas Masinón de danses y La bandera. Sus estilos preferidos son farrucas, alegrías, soleares, siguirias, tientos, tarantos y zapateado. De sus últimas actuaciones en\n\npúblico destacan sus recitales, ilustrando conferencias en unión de Pepa de La Matrona, en la Sala Sarah Bernhardt de París y otros centros culturales franceses. Está considerada una excelente estilista del flamenco más clásico.\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nTeléfono 25 37 47\n\nJ A E N\n\nO,Donnell, núm. 3-4.º Teléfs. 222058 - 216920\n\nPARTICULAR: Teléf. 278078\n\nSEVILLA",
    "title": "Homenaje en Francia a una ex-bailaora española",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 678,
    "article_char_count_full": 4154,
    "article_char_count_review": 4154,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-21-right-razones-de-una-propuesta",
    "article_text_for_review": "E n cualquier época y manifestación artística, ha habido fenómenos extraños que han alterado el normal desarrollo de un arte, y en el Flamenco, como manifestación artística que es, no podía ser menos. Si antes fue la «Ópera Flamenca», ahora, estamos asistiendo a algo parecido, pero con más degeneración aún, puesto que en la actualidad, incluso le están añadiendo instrumentos y voces extraños al cante; cosa que en la «Ópera Flamenca» no ocurrió por acompañarse solamente de guitarra.\n\nParticularmente pienso, que todo este proceso degenerativo del Flamenco no ha llegado a más, por la importante labor que las peñas flamencas han desarrollado y siguen desarrollando. Y en ello debemos estar; debemos seguir trabajando para mantener la pureza en el Flamenco, y, de este modo, legar a las generaciones venideras este hermoso patrimonio cultural lo menos contaminado posible.\n\nNo deseo aparecer como inmovilista, puesto que sería como aceptar que el Flamenco es un arte muerto y, por consecuencia, no suceptible de evolución. No nos oponemos a la evolución, pero ¡ojo! ¿qué evolución?\n\nY lo vamos a conseguir a menos que las peñas nos lo propongamos, aunando criterios y no permitiéndole a los artistas que actúen en nuestros locales, desviaciones —en su afán creador— que a nada conducen y nada aportan. A las pruebas me remito con los engendros de las «Galeras», «La Canastera» y alguno más.\n\nEs evidente el daño que se le está haciendo al Flamenco por parte de algunos «pseudo-flamencólogos» que no distinguen una soleá de un fandango, que los\n\nhay. Por parte de las casas discográficas, que en su desmedido afán por vender —por otro lado razonable— obligan a los artistas a «crear» bodrios. Por parte también de los pseudo-flamencos-político-oportunistas, que los hay; organizando semanas y centenarios, con dinero público, donde mezclan la siguiriya con la charanga; la conferencia ortodoxa con la disertación ramplona que nada aporta. Pero eso sí, bien pagada.\n\nPara solución de muchos de los problemas que el Flamenco tiene planteados en la actualidad, nada mejor sería que escuchar a los flamencos, es decir: a los cantaores, bailadores y guitarristas, que son, en suma, los que tienen la obligación de contribuir, con su estudio y magisterio, a que nuestro arte no se vuelva a prostituir.\n\nAunque parezca hipérbole, hemos conseguido para el Flamenco cotas de dignificación insospechadas —en esto las peñas han tenido mucho que ver—, se han conseguido ventas discográficas impensables, se ha conseguido la mayor difusión de su historia y se ha motivado al gran público.\n\nTodo esto denota, el poder de aceptación que el Flamenco tiene en la actualidad en todas las capas sociales del país. Ahora sería difícil oír eso de: «es una plaga tabernaria y antiestética». Por todo ello, pienso que ha llegado el momento de que callemos los aficionados y los «flamencólogos» para que hablen los profesionales, porque ellos son los que más tienen que decir y, porque ellos, en gran medida, son los responsables del deterioro existente en el Flamenco.\n\nMe consta que la mayoría de los artistas son conscientes de este deterioro artístico progresivo que el Flamenco está sufriendo, por motivos de difícil análisis y que merecerían una profunda reflexión. Para ello, proponemos una serie de puntos que pudieran ser motivo de consideración por parte de todos ellos.\n\nPrimero. Promover el «I Encuentro de Artistas Flamencos», que durante varias jornadas de trabajo, llevaran a cabo el estudio de su problemática artística y laboral.\n\nSegundo. Presentación y debate de ponencias confeccionadas por algunos artistas —que pueden hacerlo—, en las que estarían recogidos los posibles trabajos de investigación, situación actual del Flamenco y sugerencias para un mejor ordenamiento de los Festivales.\n\nTercero. Constituirse en «Asociación de Artistas Flamencos», al igual que lo han hecho otros colectivos de artistas, para una mejor defensa de sus intereses.\n\nCuarto. Revisión periódica, por ellos mismos, del panorama flamenco. Estudio del mercado discográfico, Sociedad de Autores, etc.\n\nQuinto. Cachets, tiempo de actuación y recuperación de cantes en desuso que sólo ellos pueden hacerlo.\n\nDesde este momento, y si esta idea se lleva a feliz término, pueden contar con la incondicional ayuda de nuestra Peña y del Grupo Candil.",
    "title": "Razones de una propuesta",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 684,
    "article_char_count_full": 4319,
    "article_char_count_review": 4319,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
