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
    "article_id": "1997-01-25-right-flamenca",
    "article_text_for_review": "Rafael Valera Espinosa\n\n“Luna de Calabozo” Diego de los Santos “Rubichi”\n\nAntonio Jero, guitarra. Domingo Rubi-chi, 2ª guitarra en \"En este cesto llevo\". Gregorio Fernández Junquera, José Rubichi, Chicharito y Rafael Moreno Junquera, palmas y jaleo Música: Antonio Jero Colección: Flamenco vivo Estudio: Alta Frecuencia. Sevilla. Mezclas y grabación por Antonio Algarrada\n\nReferencia: B 6826. Audivis Distribution. 1996. Audivis Ibérica Beltrán, 72. 08023. Barcelona De va consolidando el interés de esta casa francesa por el flamenco —lo cual alabamos y agradecemos en lo que nos concierne— y por las voces de sonido añejo y profundo. Igualmente se acrecienta su iniciativa en la recuperación —mejor plasmación— de unos cantaores, que aunque poseen dosis de calidad, no han alcanzado el “status” de figura flamenca.\n\nEsto último sucede con Diego de los Santos “Rubichi”, un artista de raigambre jerezana que aborda los estilos clásicos del repertorio de los de su tierra. Con una ejecución clásica de los palos y con el desarrollo arrastrao de la casa de los Agujetas, “Rubichi” es un intérprete que mantiene una tesitura cantaora ortodoxa. Así, sus soleares recuerdan, con los matices antes expuestos, el localismo de Alcalá con determinados ecos evocadores de Juan Talega, para continuar por la rememoración que del Mellizo hizo su legendario paisano Manuel Torre y expresar su máximo sentimiento por La Andonda o José Yllanda, según las versiones de Mairena o El Gallina, respectivamente.\n\nEn los cantes mineros muestra influencias de Camarón y Rancapino adobadas de personalidad propia. En las bulerías mantiene las esencias de su casta y su cuna cantaora, y en la granaína la musicalidad que un gitano de Jerez le da al estilo. Los tientos están ejecutados en su línea personal y ligándolos adecuadamente con los tangos, estos últimos con resonancias “pastoreñas”. Con el peculiar eco de Jerez y en una tesitura inapropiada para su voz, Rubichi” intenta en las siguiriyas entremezclar las formas cantaoras de Manuel Torre y Chacón, con los matices personales de Terremoto de Jerez. Con entonaciones que resuenan a Tío Borrico y El Sordera y con determinada monotonia, cumple en las bulerías por soleá. Cierra su trabajo en la más pura tradición de su cuna familiar y jerezana por martinetes.\n\nSe puede considerar este trabajo flamenco vivo como lo encuadra la casa grabadora? ¿Y por qué no? Cierto es que suena más a flamenco de concierto, con lo cual no deja de ser flamenco aunque no esté en la línea ortodoxa a la que estamos acostumbrados. Mas la dinámica actual nos ha metido de lleno de la lógica aceptación de estos trabajos a pesar del hibridismo que se introduce en los mismos. Considero que lo más adecuado es ir escuchando los cortes con detenimiento, para de una manera exhaustiva, analizar cada una de las composiciones de Rafael Riqueni. De esta forma, el que ama la ortodoxia flamenca disfrutará con la soleá y la taranta, y los yanguardistas? lo harán con las sevillanas y bulerías.\n\nEn este “Alcázar de Cristal”, el concertista flamenco sevillano da una auténtica muestra de composición para guitarra, en la que imperan generalmente los toques flamencos. Y es que sus soleá, taranta y alegrías\n\nson claras piezas en las que el virtuosismo, composición y ecos flamencos denotan vida interior, vida íntima y una aproximación a su más intensa emoción personal, a ese gustarse en su acrisolador toque, donde a veces —muy concretamente en la soleá— el sentimiento paternal lo enfoca hacia el estilo con la más sensible composición. Estas matizaciones emocionales las incluye igualmente en su Tema de Amor y Fantasía.\n\nPor contra, el desenfado, la alegría y el ritmo encuadrados a compás —sólo por él y no por el resto de acompañantes— de sus tangos, rumbas, sevillanas y bulerías, mantienen la línea de modernismo habitual en la actualidad, con entremezclas de jazz —en las sevillanas—, arropo de coros y bastante percusión —en las bulerías—. Mas no acaba aquí la dimensión compositora de Rafael Riqueni, pues en su Reflexión, los intimistas sonidos de Tárrega o Rodrigo son evidentes.",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1997-01",
    "year": 1997,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 664,
    "article_char_count_full": 4103,
    "article_char_count_review": 4103,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-01-26-right-el-cante-de-la-petenera",
    "article_text_for_review": "«Ranuro»\n\nEl cante de la petenera, según algunos flamencólogos y eruditos en la materia, lo denominan o lo clasifican como un cante verdaderamente emotivo, melancólico y sentimental dentro de esa diversidad de estilos flamencos envueltos en esa leyenda del misterio del «duende».\n\nLa historia de este cante de la petenera, opinan algunos tratadistas que se presta a grandes confusiones, pues creen algunos y hasta lo afirman (sin aportar pruebas suficientes) que su fuente proviene de los judíos «sefarditas» de los Balcanes, que solían cantar entre sus viellas estas canciones con las mismas estrofas poéticas:\n\nQuien te puso petenera no te supo poner nombre, que debía haber puesto la perdición de los hombres. Hay quien se deja influir por otras copias populares de los judíos: ¿Dónde vas, bella judía, tan compuesta y a deshora?\n\nPoema dramático en tres actos.\n\nVoy en busca de Rebeco, que está en una sinagoga.\n\nArcadio Larrea, en su «Guía de flamenco», habla de la petenera y de su ritmo primitivo que indica raíz americana y que acaso confirma una copla popularísima transcrita ya musicalmente hacia 1880:\n\nHe nacido en La Habana debajo de una palmera; me bautizaron los moros, me pusieron petenera.\n\nCorominas, y en su Breve Diccionario Etimológico de la Lengua Española, dice: «Petenera. Aire popular andaluz parecido a la mala-gueña. 1879 (y Petenera) 1847 origen incierto probablemente, alteración de Paterna, pueblo bello de nuestra Andalucía origen de este cante, y que ya glosara Serafin Estébanez Calderón en su fiesta en Triana y del cual nos habla de ciertas copillas a quienes los aficionados llaman peteneras..., son como seguidillas que van por aire más vivo; pero la voz penetrante de la cantaora dábalos una melancolía inexplicable».\n\nHipólito Rosy, en su «Teoria del cante jondo», se inclina a que la estrofa que antes mencionaba a Rebeco y la sinagoga ayuda a fechar la petenera, ya que las sinagogas desaparecieron en España junto con los judíos a finales del siglo XV, pues cree que pudo fácilmente existir estas letras ya en aquel tiempo, incluso que las peteneras fueran en su origen un canto de judíos sefardías, pues en Oriente Medio entre su pueblo se sigue hablando español y se conserva muchas de nuestras viejas costumbres y figuran canciones muy similares a nuestro cante, incluyendo las peteneras.\n\nComo podrán observar sobre el origen de este cante, hay diversidad de opiniones y de todos los gustos. González de Hervás, el incansable y viajero flamencólogo y poeta, gran amigo de nuestra Andalucía, en su libro «Er cante» dice así en una de sus glosas sobre la petenera:\n\nPetenera de Paterna. Petenera de Almería. ¿Qué puñales de taberna mataron a la alegría?\n\nPor supuesto que no estamos de acuerdo con el señor González de Hervás en legar con carácter de gratitud nuestro cante gaditano a esta población almeriense.\n\nCreemos que solamente es fundamental la ambigüedad de la tonada sobre el año 1942, nacida y vivida en suelo andaluz y aprendida y cultivada por los judíos españoles y que en Paterna de Rivera se erige con la paternidad de este cante representada en esa guapa moza, impulsora de la leyenda, recreando este estilo para cante y baile, aflamencado para sí y dándole toda su expresiva belleza.\n\nSobre esta mujer y de su «mal farrio», los poetas en ocasiones le han dedicado sus estrofas, entre ellos Manuel de Góngora y Tomás Borrás. El primero con una obra en tres actos titulada «La Petenera», que fue estrenada en Madrid la noche del 14 de marzo de 1927, de donde recogemos los siguientes versos: ¡Era mi fario verdá...! Maldita la copla mía. ¡La copla que nunca muere! ¡La que está en mi pecho hundía, y me sigue por la vía con el dolor que me hiere!...\n\nDe Manuel de Góngora es también el «Romance de tu mata de pelo», cuya letra se ha divulgado por el pueblo incluyéndola «como petenera larga» y que varios cantaores la han divulgado. Y de Tomás Borrás el piropo gracejo a tres peteneras en su libro «Palmas flamencas»:\n\nA Cádiz le mira así Paterna de la Rivera; si Cádiz tiene murallas y tiene la salinera, tres mozas echa a renir Paterna de la Rivera...\n\nEl testimonio del cantaor jereza-no «Juanelo», de haber conocido, según versión de Demófilo, padre de los Machado, a esta cantaora de Paterna, y de ser Medina el Viejo, uno de los mejores y máximo intérprete del cante de las peteneras, tiene que ser por derecho propio cante arraigado a nuestro suelo.\n\nEl cante de la petenera, del que los gitanos la respetan, le han dado vida «La Antigua», Juana Ruca, Trinidad «La Parrala», la genial Pastora Pavón, hasta llegar hasta nuestros días, en que encontramos una serie de intérpretes que la cultivan —incluso gitanos— como Rafael Romero «El Gallina», «Naranjito de Triana», «El Perro de Paterna», Rufino, etc., que la florean y la promocionan hacia la juventud flamenca de nuestros días. Ellos han sabido recoger la savia de nuestros poetas andaluces como Julio Mariscal Montes y Antonio Murciano, e incorporarlo a su repertorio para ir pregonando por los caminos de esta bendita tierra la razón de vivir y de ser de un cante. ¡El cante de la petenera!",
    "title": "¡El cante de la petenera!",
    "periodical": "candil",
    "issue_id": "1997-01",
    "year": 1997,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "26-27",
    "page_number": 26,
    "word_count": 862,
    "article_char_count_full": 5113,
    "article_char_count_review": 5113,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-01-27-right-recordando-a-carmen-amaya",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAl fin, después de treinta y tres años, una terracota recordará a los visitantes del cementerio de Begur, que allí fue enterrada una artista sin igual. Fue, junto a Pau Casals, la figura catalana más universalmente conocida. Europa entera, todas las Américas e, incluso, Africa, se rindieron entregadas a la magia de su arte inigualable y revolucionario. Fue mujer y gitana. Es, por los siglos de los siglos, la sin par Carmen Amaya.\n\nBien, pues a pesar de todo ello, hoy es una ilustre semiolvidada institucionalmente. Se olvida sistemáticamente que revolucionó el baile flamenco de mujer, que alcanzó cotas tan altas con su arte que difícilmente podrán ser igualadas, que lo primero que hizo al descender del avión en su regreso a Barcelona, tras su exilio, fue besar el suelo de su tierra natal,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"oficial\"]\n\ne descansar en ella. Era de justicia tal recordatorio incluso sin que sus restos reposasen ahora en ese cementerio. Su marido, J. Antonio Agüero, decidió, a los dos años de su muerte, trasladarlos a Santander. A las 12 de la mañana del sábado 23 de noviembre, con el sonido de fondo del oleaje del mar Mediterráneo y el de la tramontana jugueteando por entre los cipreses, Carlos Arnau, alcalde de la villa, y los Amigos de Carmen Amaya inauguraron oficialmente la hermosa terracota, obra del artista Francisco Polop, que recuerda, ya para siempre, que allí fue enterrada la bailaora del Somorrostro. Participaron en el acto, jun- to a las autoridades locales y los vecinos de la población, la fotógrafa Colita y la bailarina Pastora Martos —ambas la conocieron personalmente y guardan recuerdos imborrables de ella—, la diputada Trinidad Neras; el presidente de la Casa de Ecija de L'Hospitalet, Manuel Reyes; el directivo de la Peña «Los Aficionaos» de Cornellá, José Muñoz, y un significado grupo de aficionados de Cornellá, expresamente desplazados a la villa ampurdanesa para asistir al acto. La\n\n[ENDING CONTEXT]\n\nsu reportaje a Colita para que formara un todo. La obra conjunta de ambos constituye un documento gráfico impresionante y único del último año de vida de la gran bailaora.\n\nResultó, en conclusión, una jornada sencilla, emotiva y cuajada de recuerdos. Un acto de reconocimiento y de homenaje tardío pero de total justicia. Y a lo largo del día entero un casi único tema de conversación, Carmen Amaya, y una petición insistente y unánime: que sea el Mas Pinc, su masía, un museo dedicado en exclusividad a ella. Ojalá que no tarde demasiado tiempo en hacerse realidad porque también es de justicia.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Recordando a Carmen Amaya",
    "periodical": "candil",
    "issue_id": "1997-01",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "27-28",
    "page_number": 27,
    "word_count": 1171,
    "article_char_count_full": 7046,
    "article_char_count_review": 2727,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "oficial"
      }
    ]
  },
  {
    "article_id": "1997-01-28-right-festival-flamenco-homenaje-a-man",
    "article_text_for_review": "E1 pasado día 22 de noviembre, en el hermoso Palacio Municipal de Deportes cordobés «Vista Alegre», una concurrencia aproximada a las tres mil personas, se dio cita para homenajear a este hombre modesto y gran artista, al que Córdoba quiere y respeta, al igual que compañeros del mundo flamenco, que no dudaron en darse cita en la Ciudad de los Califas y dar una gran noche flamenca.\n\nCon todos los artistas en el escenario, me cupo el honor de hacerle el ofrecimiento, y a continuación recibió distintos recuerdos por parte del Ayuntamiento, Diputación Provincial y distintas entidades flamencas.\n\nIntervinieron, abriendo el espectáculo, «El Niño Seve» y «Pepete de Córdoba» (guitarra y percusión) por bulerías; siguió «El Cabrero» con la guitarra de Paco «El del Gastor»; José Mercé, al que acompañó «Moraíto Chico»; «El Pele» con la guitarra de Manolo Silveria; cerrando la primera parte el baile de Inmaculada Aguilar, acompañada de su grupo, con el cante de Manolo Cortés, las guitarras de Manuel Flores y Ramón Rodríguez y las palmas del homenajeado «Finito» y «El Pipa».\n\nDe esta primera parte habría que destacar el buen momento por el que atraviesa José Mercé; al igual que «El Pele», y el baile por soleá de Inmaculada Aguilar, que a pesar del reciente fallecimiento de su padre, quiso participar en este homenaje a su palmero y amigo «Finito».\n\nSe iniciaba la segunda parte con el baile, en esta ocasión de Javier la Torre, que lo hizo extraordinariamente por alegrías. Continuaba la actuación Juana «La del Revuelo» con Martin «Revuelo». El gran espectáculo continuó con unas sevillanas interpretadas por Gloria Fernández, hija de «Finito», para dar paso al «Nano de Jerez», que estuvo acompañado por la guitarra de Manolo Flores.\n\nLa última artista de la noche fue Aurora Vargas, finalizando la misma con un fin de fiesta en la que actuaron alumnos de la gran profesora y bailaora Inmaculada Aguilar.\n\nEn esta segunda parte habría que destacar, además del baile de Javier la Torre, la buena actuación de «Nano de Jerez» y el arte de Aurora Vargas.\n\nEl público supo valorar el rotundo éxito del festival, aplaudiendo a todos los artistas con calor, y la comisión organizadora quedó satisfecha y agradecida tanto por la asistencia de público como por la contribución de sinteresada de cuantos artistas participaron.",
    "title": "Festival Flamenco homenaje a Manuel Fernández Castro «Finito»",
    "periodical": "candil",
    "issue_id": "1997-01",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "28-28",
    "page_number": 28,
    "word_count": 383,
    "article_char_count_full": 2326,
    "article_char_count_review": 2326,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-01-29-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "José Luis Buendía López\n\nResulta alentador, y debiera ser un acicate para todos los que nos dedicamos a la crítica flamenca, la lectura de obras como ésta del profesor Penna, nacido en Turín en 1899 y muerto en Madrid en 1968, después de toda una vida dedicada al estudio de nuestro folclore y literatura. Y apoyo lo que acabo de afirmar, no porque el libro, ahora felizmente traducido, revolucione conceptos, aclare datos históricos o constituya la panacea para enigmas hasta ahora insolubles, sino por su respeto a una cultura ajena y su apuesta decidida a favor de constituirse en cronista de una realidad cultural que, aún en los tiempos que corren, se mira de medio lado por los intelectuales a la violeta que aún piensan que la cultura de transmisión oral es una cosa baladí a la que no se debe prestar la más mínima atención.\n\nConvencido de lo contrario, Mario Penna dedicó largos años de su vida investigadora al estudio del can-\n\nte flamenco y de cuantas vías de acceso conducen a él, deteniéndose en las etapas de su formación, en los ambientes que lo configuran y en los protagonistas que lo condujeron a tan altas cimas. A medio camino entre la ilustración para extranjeros que, como tales, no conocen nuestro arte, y el estudio documentado, apto para cualquier especialista, el libro va desgranando opiniones llenas de sensatez sobre una serie de aspectos que interesan hoy, más que nunca, a los que se aproximan a estos estudios, ya se trate de aspectos filológicos relacionados con las etimologías propuestas, el problema de los orígenes, las interconexiones poesía/cante (interesantes las aportaciones métricas) o la naturaleza de la propia sociedad en la que estos fenómenos tuvieron lugar.\n\nSin embargo, yo me quedo con las observaciones atinadas que realiza el autor acerca de los protagonistas y sus verdaderas intenciones, los mil y un recovecos de su compleja personalidad. En esta línea, resulta esclarecedor su juicio sobre cómo en las llamadas \"juergas\" (capítulo VIII) los gitanos despreciarían a los payos que pagaban las mismas, ofreciéndoles lo más superficial de su arte, mientras que otras gentes no gitanas, pero verdaderamente interesadas en él, ese público al que Penna denomina \"busné\" (ésto es, extranjeros, pero no \"extraños\"), aunque no pudiera pagar, gozarían de la confianza del pueblo gitano, y disfruta El flamenco y los flamencos. (Historia de los gitanos españoles y su música)\n\nDe Mario Penna.\n\nEdición, traducción y notas de Antonio Zoido Naranjo.\n\nEdita: Fundación El Monte y Universidad de Sevilla. Sevilla, 1996 rían por ello de un arte verdadero y no adulterado; circunstancia gracias a la cual determinados observadores aficionados, no gitanos, como el mismísimo Fernando el de Triana, pudieron llevar a cabo sus atinados estudios flamencos sobre aquella raza fundamental en nuestro arte.\n\nEn el capítulo V, al analizar las leyes antigitanas, prescinde de demagogias, y, junto a la crueldad inhumana de pragmáticas y leyes xenófobas, no duda en señalar el camino equivocado de algunos gitanos que se asimilaron por dinero a la clase dominante, la cual los manipuló a su antojo, haciendo más evidente todavía la máxima de que la ley del rey destruyó la ley de los gitanos, ya que, según Penna: “el número de los que saben resistir a tantas presiones se va haciendo cada vez más reducido; muchos sucumben y algunos, que quizá habrían permanecido fieles a la tradición gitana, buscan y encuentran el camino para evadirse”. (Pág. 171).\n\nSon tantos los matices de este tipo, que el libro introduce, que no dudamos en calificarlo tanto de manual para no iniciados, que recoge experiencias y juicios ajenos, como de tratado psicológico y hasta de sociología ambiental, puesto que, manejando ambas disciplinas, consigue iluminar zonas oscuras incluso para muchos españoles que nos dedicamos desde antaño a esta gratísima tarea de la investigación flamenca.",
    "title": "Aunque no quepa en el papel...",
    "periodical": "candil",
    "issue_id": "1997-01",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "29-30",
    "page_number": 29,
    "word_count": 633,
    "article_char_count_full": 3899,
    "article_char_count_review": 3899,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
