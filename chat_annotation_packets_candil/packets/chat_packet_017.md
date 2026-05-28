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
    "article_id": "1980-11-13-right-marcelo-barbero",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFugaz evocación de un gran maestro guitarrero, ya casi olvidado\n\nEn el mes de Julio de 1956 moría en su propio domicilio-taller, sito en la castiza calle madrileña de Ministriles, nada menos que Marcelo Barbero, uno de los más prestigiosos guitarreros españoles de todos los tiempos, que fue discípulo y oficial del gran José Ramírez, a principios de siglo y que se hizo cargo del taller del excepcional e inolvidable maestro Santos Hernández, al morir éste y a requerimiento de su viuda, corriendo entonces los años cuarenta.\n\nLa personalidad de Barbero merece, sin embargo, más amplio comentario del que intrínsecamente le corresponde por propio derecho como excepcional «luthier», «Guitarrenbauer», o —en español— artesano guitarrero. Es precisamente esto lo que me anima a evocar algunos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nste y a requerimiento de su viuda, corriendo entonces los años cuarenta. La personalidad de Barbero merece, sin embargo, más amplio comentario del que intrínsecamente le corresponde por propio derecho como excepcional «luthier», «Guitarrenbauer», o —en español— artesano guitarrero. Es precisamente esto lo que me anima a evocar algunos recuerdos de mi adolescencia universitaria, de hechos acaecidos en el lustro 1951-55, período en el que tuve el gran placer de conocer y convivir con este curioso personaje, cuya categoría artística fue solamente superada por su calidad humana. Era Marcelo hombre sencillo y cordial, modesto y digno, amante de la pequeña e íntima tertulia que le entretenía mientras trabajaba sobre el banco, celoso en la selección de sus amistades, consciente de su valía artesanal aunque no engreído, excelente esposo y padre de dos hijos y aficionado donde los haya al cante «jondo», por cuya causa abandonaba fugazmente y con frecuencia su trabajo para escuchar en su humilde receptor «de cinco lámparas» los cantes de los más destacados intérpretes de la época, entre los que se contaban Pepe Pinto, el «Niño de Marchena», Juanito Valderrama, Canalejas de Puerto Real, Juanito Varea, Pericón de Cádiz y tantos más. Constituía un auténtico deleite observar con qué primor y minucioso virtuosismo c\n\n[ENDING CONTEXT]\n\nAntonio Escribano, según indicación del propio autor, habría que rectificarle la ubicación del café de la Marina, punto fuerte de Cayetano Muriel, «Niño de Cabra», pues según ha podido comprobar, el referido café se encontraba en la calle de Jardines y no en la de Aduana; lapsus disculpable, ya que ambas calles nacen en la de Montera y concluyen, paralelas, en la de Peligros.\n\nFinalmente, nuestro colaborador nos indica que su lista no es total, y que muy presumiblemente podría ser incrementada con algunos cafés cantantes más, de los que posee cumplida noticia el flamencólogo José Blas Vega.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "MARCELO BARBERO",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 1521,
    "article_char_count_full": 10904,
    "article_char_count_review": 2939,
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
    "article_id": "1980-11-15-right-el-sentido-de-las-letras-en-el-c",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Francisco Vallecillo\n\nEl mensaje de las letras flamencas, se diría ahora con evidente incongruencia semántica. El Cante fue siempre —y tendrá que seguir siéndolo para no perder su esencialidad— una expresión viva, varia y renovada de la peripecia humana. Por eso el Cante que va de la fragua a la era; desde la casa —el ker de los gitanos—hasta el «ghetto» mísero y lacerante; desde el amor, bajo tantas formas de expresión, hasta el odio; desde el negro pozo jondo de las ducas hasta la alegría de cantar a la luz; desde la conseja al hecho histórico; desde el consuelo a la pena infinita, irremediable; y claro y también y muchas veces sobre todo, especialmente en estos tiempos, cuando es rebelión frente a las injusticias y discriminaciones, como protesta en definitiva de quienes con\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\no, especialmente en estos tiempos, cuando es rebelión frente a las injusticias y discriminaciones, como protesta en definitiva de quienes con sobrados motivos todavía hoy se sienten —o sienten a quienes lo son—siervos empujados a un terrón de hambre y de miseria, a una maleza indesprozable que rodea a la injusticia de una sociedad terriblemente insolidaria. El Cante que no nos vale, el que no pudo valernos nunca porque nadie que lo diga o que lo escuche podrá dejar de sentir sonrojo, es el que, para entendernos, incluso fuera de los confines flamencos, representa al señoritismo trasnochado, a la destreza de jinetes o sus aptitudes para reventar a humildes liebres o su prestancia sobre el brioso corcel (dicho sea para apurar el tópico), aquel que hubieron de crear pobres bufones a cambio de sórdidos mendrugos y encanalladas befas. Y esto, por supuesto, al margen de la belleza de algunos aspectos del entorno y de las costumbres /andaluzas, especialmente cuando la copla no implica ningún servilismo, por ejemplo: Tengo una manola nueva con cuatro mulas castañas y la novia más bonita que cobija el sol de España: sevillana y morenita. El Cante ha de decir la verdad de la vida, de la peripecia humana, fuente inagotable de inspiración. Ya lo hemos explicado en anterior ocasión (revista «Flamenco», diciembre 1976) y tal vez por eso, y por haberlo escrito otras plumas evidentemente más afinadas que la nuestra, cualquier disquisición al efecto puede resultar ociosa. Sí que hay que decir que la letra en el Cante tiene una importantísima misión que cumplir, acaso en el Flamenco más decisiva que en otras formas musicales cantadas. Pero sin perder de vista que la letra en sí —el «mensaje» ad usum no puede ser base única de la copla, ni muchísimo menos, salvar o mejorar siquiera una música, un Cante defectuoso. Es cierto que el Cante llevaba años pidiendo a voces —que es el único modo de pedir las cosas que tiene el cante: a voces, pero a voces afinadas y templadas, hermosas— estaba pidiendo a voces un aire fresco en sus textos. Pedía y afortunadamente lo está consiguiendo, su liberación de aquellas terribles invocaciones a la mare, al cementerio, una desaparición bastante radical de su sentido luctuoso siemp\n\n[ENDING CONTEXT]\n\nhan abordado el tema en profundidad y han dado al mismo una dimensión notoria y muy digna de tener en cuenta, tal los libros «La Copla popular Flamenca», de José Relardo y Francine Belade y «El Cante flamenco, expresión y liberación», de Antonio Carrillo. Ambos admirables. Es indudable que el resurgir de las letras, acercándose cada vez más a una forma poética muy antigua ahora reverdecida, constituye una evidencia indudable. Al extremo de que podamos decir que son los cantaores quienes tienen el deber de servirlas con la mayor perfección musical o cantaora, que es el término que preferimos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El sentido de las letras en el cante",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1354,
    "article_char_count_full": 7805,
    "article_char_count_review": 3852,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1980-11-17-left-una-mirada-atr-s",
    "article_text_for_review": "I complejo resulta el entramado del mundo del libro, éste se acrecienta para mal cuando es de temas flamencos. Cierto, que las dificultades de distribución que encuentra cualquier libro de una editora media en nuestro país, realmente, son terroríficas; pero las que han de vencer los libros que nos ocupan —los más, prácticamente, artesanales—, hacen que sean verdaderamente inexpugnables, siendo por ello y otras causas verdaderamente desolador el desconocimiento que de la letra impresa jonda tiene el mundo flamenco.\n\nAdelantando que, con excelentes fondos, existen tanto una editora como una librería especializadas —desde luego, algo bien escaso en la amplia geografía del cante—, y apuntando que el españolito medio no se caracteriza por su afán en la lectura, se impone reconocer que el aficionado flamenco no se queda a la zaga, ni mucho menos, en su desprecio por la letra impresa. No se me diga, que lo más que se publica son refritos y basuras retóricas, que en cante ya está todo dicho; pues, amén de no ser cierto, tras esa frase se encuentran los cofrades de aquella España machadiana que «desprecia cuanto ignora», del «enterao». No existe justificación posible para que ediciones de trescientos ejemplares —aunque fuesen rematadamente malos— tarden años en venderse, mientras, por el contrario, en escasas fechas se cosumen miles de discos y cintas que degradan, en bastantes ocasiones, hasta el propio material en que están realizadas.\n\nSi tanto nos dolemos por la falta de escritos y documentos para conocer los dos tercios de la historia del cante, cómo entender a estas alturas del siglo veinte semejante rechazo de la escritura de temas flamencos, máxime, cuando la afición supera el número de varios centenares de miles y ello si sólo nos atenemos a considerar como tal a la encuadrada en peñas y otras entidades flamencas... Creo que ya es hora de solicitar, de concienciarnos, que los mismos cauces responsables de estudio y debate por los que discurren las grabaciones sean para los libros.\n\nMas posponiendo este tema, merecedor de un largo análisis, por las dificultades de distribución de nuestros libros, que antes apuntara, nos ocuparemos hoy, al filo del año 1980, de algunas publicaciones que no tuvieron en su día la justa acogida crítica.\n\nta entrega (2) de Rodríguez Cosano, cantaor y aficionado lebrijano.\n\nNo es difícil calificar esta entrega de un modo unitario y global. Su presentador, Manuel Herrera, la estima como un grano de arena más para la historia del cante flamenco; y uno, a decir verdad, concuerda con el prologuista en su calificativo agregando que, este grano de arena es sólido y consistente. Es más, la historia del cante flamenco nunca podrá hacerse sin libros como éste que, si bien no revelan algo fundamental, nos traen pasajes inéditos, historias locales y opiniones sinceras que, armonizadas con otras, de seguro, configuran y ahorman este riquísimo legado que es el cante.\n\nLa entrega se abre con unos juicios sobre la copla flamenca y su función actual, no sólo como «Expresión del sentimiento humano», sino como vehículo de arte y su adecuación a nuestros tiempos. Pero tal vez, a nuestro parecer, lo más novéodo sean las páginas en las que el autor intenta explicar por qué algunas letras perfectamente medidas silábicamente no entran en el cante propuesto. Y digo que intenta, porque personalmente no he encontrado en sus aportaciones razones de aplastante convicción, lo que nos gustaría que desarrollar la plenamente con ejemplos y profundidad como, pongamos por caso, lo hace en la colabora-ción que publica en este mismo número de «Candil». Se lo he «pedio» a «Jesú», que me mande el Cirineo que no «pueo» con mi «cru».\n\nLa parte más amplia del presente libro está compuesta por un buen número de, las más, sabrosísimas letras de hechura y sabor popular, como lo demuestra que ya han sido adoptadas por varios cantaores. Soleares, tientos, tangos, martinetes, fandangos, bamberas, rnalagueñas, tarantos, livianas, serranas, siguiriyas y saetas se suceden en número suficiente y como inequívocas pruebas de que el autor conoce los «sonios» de la copla. Veámoslo en algunos ejemplos: Pasito a pasito lento, tú te «ha» «salío» del tango y yo me he «queao» dentro.\n\nSabiendo que estoy «herío», no «venga» ahora pinchando que «toa» la sangre he «perdío».\n\nNo «enturbia» nunca el agüita «despué» de apagar la «se»; vuelve la carita «atrá» por si otro quiere beber.\n\nMe entra un escalofrío cuando recuerdo aquel día; que el mejor amigo mío en la bamba la mecía a la mujer que he «querío». Finalmente, la entrega se cierra con cumplida noticia de tres cantaores de la provincia de Sevilla y surgidos para el cante en el primer tercio de este siglo: El Gallo, El Chozas y el Chiro, tres protagonistas del cante, reflejo de una época —desde luego que una etapa no ia escriben sólo la sota, el caballo y el rey—, que participan de la grandeza y pesadumbres, biografía incluida, de este lacerado arte andaluz.\n\n(2) Edición de autor. Gráficas Los Palacios; 1978.\n\nMANUEL GARCIA MORENO GENEROS DE PUNTO CONFECCIONES\n\nALMACEN Y OFICINAS: Dr. Civera, 33 - Teléfs. 231390 y 231687 J A E N DECORACION, LAMPARAS Y ARTICULOS DE REGALO\n\nALQUILER DE SILLAS Y MESAS Para toda clase de espectáculos\n\nVirgen de la Capilla, 13 - Teléf. 231333\n\nJ A E N",
    "title": "Una mirada atrás",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 877,
    "article_char_count_full": 5294,
    "article_char_count_review": 5294,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-11-17-right-el-muestrario-flamenco-de-jose-l",
    "article_text_for_review": "José Luis Tejada\n\n(Prólogo de Pilar Paz Pasamar)\n\nJosé Luis Tejada, el poeta y escritor del Puerto de Santa María, nos ofrece en «Del río de mi olvido» (1) una amplísima serie de composiciones poéticas traspasadas de hondura y flamenquismo. Más aún, el libro se nos viene, prácticamente, como una antología mayor de letras de los más diversos cantes.\n\nPero antes de adentrarnos en una sucintísima reseña del libro, se impone recordar que José Luis Tejada pertenece a un cualificado grupo de escritores andaluces exponentes de una época necesitada de urgente revisión críticoliteraria, ya que lo andaluz y lo flamenco, tan vituperados en su línea de salida, constituyen, de algún modo, uno de los nervios modulares de sus obras; no en vano, Tejada es uno de los mejores críticos de Alberti. Y con esta propuesta de revisión no es a m o s defendiendo a ese adornado cadáver denominado poesía «andalucísima»; por el contrario, queremos dejar bien claro que la práctica totalidad de los escritores del Sur de la llamada «Generación del 50»,\n\nelevan a cotas bien altas el alma de Andalucía y el sublime rajo de 'lo jondo. Ah, y si ello no interesa a otras áreas más o menos «amilanesadas», ese es su problema, no el nuestro.\n\nPues bien, regresando al libro de nuestras referencias, no sólo es un muestrario amplísimo de las letras escritas por J. L. Tejada, sino, un amplio catálogo de las de los más diversos cantes, en los que, como es de suponer, resaltan y resuenan más nítidamente los de su comarca. Queden algunas muestras:\n\nTe estabas cortando el pelo. Anillitos de oro claro iban alfombrando el suelo.\n\nBULERIAS\n\nSi señalan que señalen. Más señalaron a Cristo y ahora le encienden altares.\n\nYa sé que te estás hundiendo, pero sálvate tú sola que yo de barcos no entiendo.\n\nNo pases por la botica no te ieche el boticario los polvos de pica-pica.\n\nTú juegas con dos cartones lotería del cariño, dándome pares y nones.\n\nSOLEARES\n\nMe he acarencio a tu cuerpo como un preso se acostumbra a su prisión con el tiempo.\n\nComo su puerta crujía me la dejaba entreabierta hasta las claras del día.\n\nYo estoy durmiendo en el [suelo pa que mi cama no pierda el hoyito de su cuerpo.\n\nMe quité de la bebía, de ti no supe quitarme cuando más falta me hacía.\n\nPero no son solo las soleares y las bulerías, cantes tan próximos y afines al autor, solearillas, cantes de Málaga, polos, cañas, sevillanas, nanas, y un larguísimo etcétera se congregan en esta obra, sin faltar guajiras —¿quién las escribe ahora?— y canciones de murgas gaditanas.\n\nQuede aquí esta reseña, más bien noticia, de un libro desbordado de cantares, algo que, desde Bécquer hasta ahora no dejaron de escribir los más soñalados poetas andaluces, conscientes de que el cante y su carga anímica nunca puede quedarse en un tiempo, en moda.\n\nLETRAS FLAMENCAS Y BIO- GRAFIAS INEDITAS» DE RI- CARDO RODRIGUEZ COSANO\n\nRICARDO RODRIGUEZ COSANO\n\nLETRAS FLAMENCAS Y BIOGRAFIAS INEDITAS\n\nLa letra jonda y su mundo, tres biografías inéditas de otros tantos cantaores y una amplia antología de letras flamencas del propio autor componen es-",
    "title": "EL MUESTRARIO FLAMENCO DE JOSE LUIS TEJADA",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 527,
    "article_char_count_full": 3083,
    "article_char_count_review": 3083,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-11-18-right-dimension-social-del-cante-flame",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Alfredo Arrebola\n\nEn la página del Diario «Sur», dedicada a «Oído al cante», con fecha 19-X-80, aparece un artículo titulado «Flamenco verdad o flamenco estandarizado»?, cuyo contenido, lleno de aberraciones, merece una respuesta por quienes sienten, aman y defienden la «trayectoria social y humana del cante flamenco». Contenido, por otra parte, hoy rechazado plenamente por todo el mundo del flamenco: Cantaores y aficionados. Es un artículo breve, pero cuajado de errores socio-históricos del cante flamenco, denigrando, a su vez, al cantaor como persona social, evocando los tiempos del caciquismo andaluz sobre los cantaores. Y, al mismo tiempo, quitando la afición de los que se acercan al cante flamenco para conocer sus secretos históricos, musicales y humanos.\n\nEsta postura sólo la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\npero cuajado de errores socio-históricos del cante flamenco, denigrando, a su vez, al cantaor como persona social, evocando los tiempos del caciquismo andaluz sobre los cantaores. Y, al mismo tiempo, quitando la afición de los que se acercan al cante flamenco para conocer sus secretos históricos, musicales y humanos. Esta postura sólo la pueden admitir personas que sienten el flamenco como fenómeno de opresión y ridiculez ante la indigencia del hombre-cantaor. Y eso, no. Bajo ningún concepto. Contesto al artículo, aunque se trate simplemente de una opinión subjetiva del autor, porque es imposible admitir tal concepción del cantaor y del flamenco, como manifestación artística y cultural de un pueblo que tanto ha tenido que sufrir en sus propias carnes. Mi respuesta es como cantaor, de cuya profesión me enorgullezco, y como estudioso del mismo. Intérprete-cantaor no es, aquí, sinónimo de hermeneútico. El flamenco es algo más serio que la simple visión —como desgraciadamente ha sucedido— de desprecio del cantaor como persona y ente social. Hoy, afortunadamente, que los cantaores son considerados como unos seres sociales más, y con la misma categoría que cualquiera otra profesión, aparecen a la voz pública conceptos que están más que desechados; borracheras, juergas, libaciones catárticas, etc. Palabra, 'esta última, que no la habrá aprendido el 'autor del artículo, seguramente, en una de esas reuniones de cabales. No puedo —ni quiero— profundizar en el sentido metafísico, social, religioso, antropológico y musical del cante 'flamenco; sin embargo, cuanto diga es fruto de una experiencia cantaora, desde los primeros años de mi vida; cuando parecía\n\n[ENDING CONTEXT]\n\nde Morón, Diego del Gastor...? Como épílogo, le diré que todas las Peñas Flamencas dedican un día a la semana para estudiar y comentar los cantes flamencos. Labor muy aplaudida. Al conocer los cantes, aumentaba el número de socios. Y el flamenco ha tomado el camino que, afortunadamente, alcanzó gracias a la labor de divulgación y revalorización del flamenco. Si Cataluña y el País Vasco tuvieran nuestro folklore —el suyo lo van a enseñar— hace tiempo que estarían reguladas sus enseñanzas en todos los estamentos docentes. En otros países hay cátedras de folklore, ¿por qué en Andalucía, no?\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Dimensión social del cante flamenco",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1290,
    "article_char_count_full": 7966,
    "article_char_count_review": 3292,
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
  }
]
```
