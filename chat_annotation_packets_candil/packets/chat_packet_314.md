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
    "article_id": "1995-11-19-right-pre-flamenco-en-barcelona-a-fine",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPresentada al XXIII Congreso de Arte Flamenco Eloy Martin Corrales\n\nAl margen de orígenes más confusos y menos probados, el majismo, fenómeno propio de la segunda mitad del siglo XVIII¹, y uno de cuyos componentes consistió en la defensa de los cantos y bailes tradicionales, \"nacionales\", ante la moda italianizante imperante en España desde el siglo XVII², debe considerarse como el antecedente sociológico del flamenco. La citada pugna explica que, hasta muy entrado el siglo XIX, en los mismos teatros y salas de espectáculos alternasen las dos aludidas modalidades musicales y que no fuese muy infrecuente que los intérpretes dominasen indistintamente los cantes italianos y los españoles.\n\nEn este clima, por tanto en pleno siglo XIX, surgió el flamenco. En una reciente síntesis se argumenta\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradicional\"]\n\nXIX, en los mismos teatros y salas de espectáculos alternasen las dos aludidas modalidades musicales y que no fuese muy infrecuente que los intérpretes dominasen indistintamente los cantes italianos y los españoles. En este clima, por tanto en pleno siglo XIX, surgió el flamenco. En una reciente síntesis se argumenta que sus bases se fundamentaron sobre los siguientes elementos: \"el romanticismo-gitano como sustento ideológico; sobre la cultura tradicional, el majismo gitanizado y la experiencia socializadora de la minería, como sustento sociológico; sobre los cantos y bailes populares españoles y la estilística payo-gitana, como sustento lírico-musical; sobre el consumo, como sustento económico\". De ahí la existencia de tres espacios etnosociológicos: payo-popular, majo-gitano y el laboral minero que se cruzan en el flamenco, y de ahí también que éste surgiera con toda su potencia en tres núcleos andaluces (Jerez, Triana, Cádiz) en los\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nlas diversas regiones españolas. De ahí la entrada en la órbita del flamenco durante el siglo XIX, de numerosos cantes y bailes ajenos a la tradición andaluzay gitana. Esta facilidad fagocitadora del flamenco le permitió satisfacer en buena parte la demanda y consumo de espectáculos musicales en los que sobresalían cantes y bailes tradicionales españoles, a lo largo de toda la geografía peninsular. Lógicamente esta actitud del flamenco se conoce mejor en su entorno natural, Andalucía. Pero no es menos cierto que su actuación fue similar, aunque no tan impactante, en las más diversas regiones españolas, incluso en las colonias y excolonias americanas, con la incorporación de los cantes de ida y vuelta. Páginas opuestas: Obras del puerto de Barcelona. Los nuevos \"morros\", 1883. (Colección particular de D. Angel Martínez Villén.) La Copla. Manuel Cabral Bejarrano. Museo Romántico, Madrid. En esta página: Aspecto de la Rambla de las Flores, al mediodía, 1915. (Colección particular de D. Nicolás Ortiz Bueno.) Para el majismo, CARO BAROJA, J.: \"Ensayos sobre literatura de cordel\", Madrid, 1969. SOPENA IBÁNEZ, F.: \"La Música\", en MENÉNDEZ PIDAL, R. (Fundador); JOVER ZAMORA, J. M. Historia de España. La época de la Ilustración. Vol. I. Els Estado y la Cultura (1759-1808). Madrid, 1888, Vo. XXXI, esp. pp. 601-614. Las iras de \"Don Preciso\" contra la moda italianizante son de sobras conocidas. IZA ZAMACOLA, J. A.: Colección de las mejores coplas de seguidillas, tiranas y polos que se han compuesto para cantar a la guitarra. Madrid, 1799-1800. Utilizo la edición fascímil realizada por la Peña Flamenca de Jaén en 1982. Me baso en el estudio de GARCIA GÓMEZ, G.: Cante flamenco, cante minero. Una interpretación sociocultural. Barcelona, 1993. Sin embargo, la importancia dada a la minería a la hora de explicar el nacimiento y consolidación del flamenco, al tiempo que no tiene en cuenta otros factores explicativos, empobrece sus conclusiones. VILLAR, P.: Catalunya dins l'Espanya moderna. Barcelona, 1962-1964. Especialmente volumen cuart\n\n[ENDING CONTEXT]\n\nla Marica (1820).\n\n136) Seguidillas manchegas (1822).\n\n137) Trípoli trapala (1819-20)\n\n138) En \"El abuelo y la nieta\" (1796), comedia de música cuya acción transcurre en los alrededores de Madrid, introduce seguidillas serias, boleras y varias alusiones a la habilidad para\n\n140) Fandango (1811-1915).\n\n142) Baile con castanuelas (1807).\n\n143) SUERO ROCA, M. T.: El Teatre..., III. El 8 de noviembre de 1817, el músico alemán Mr. Bohrer, de tránsito en Barcelona, ejecutó \"varios caprichos sobre el tema del cachirulo y del\n\n145) Zapateado (1816).\n\n147) Véanse los años citados en las notas 40 y 46.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Pre-Flamenco en Barcelona a fines de siglo XVIII y comienzos del XIX Eloy",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "19-26",
    "page_number": 19,
    "word_count": 10648,
    "article_char_count_full": 64335,
    "article_char_count_review": 4705,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradicional"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  },
  {
    "article_id": "1996-01-3-left-la-reno-vaci-n",
    "article_text_for_review": "Ramón Porras González L1 número 100 de esta Revista, índice de los noventa y nueve precedentes, marca, sin lugar a dudas, un hito en la historia de esta publicación. También el punto de partida para una nueva andadura, impulsada por un renovado rearme de ilusiones de quienes conforman su equipo director. Rearme que tendrá su proyección y sus correspondencias en la busca de enfoques y perspectivas aún no exploradas sobre el estudio y profundización del flamenco. Con seguridad, esta Revista ganará en frescura, en lenguajes más incisivos, más indagadores. Razones de oportunidad determinan, pues, un cambio en parte de la dirección de esta Revista. No se puede permanecer impunemente desde la primavera de 1978 hasta 1996, como director de Candil sin que, pese a los esfuerzos desplegados, uno no incurra en una suerte de delito de cansancio, de opacidades, de costumbres y hábitos encorsetadores. Nuestros lectores no se merecen el perjuicio que podría irrogárseles. Y menos aún se lo merece el arte que durante dos décadas ha centrado nuestro apasio-nado interés.\n\nSin ánimo de hacer constar balances ni tampoco de editorializar los logros conseguidos, cabe evocar el proyecto que un grupo de aficionaños emprendimos, en relación al que ahora queda, con vocación de brillantísimo futuro. Ambas realidades, tan dispares en contenido y presentación, se conectan mediante cierto hilo conductor: la pasión indesmayable por el flamenco. Una pasión que puede y debe evidenciarse no sólo en sesudas exposiciones, en académicas tesis doctorales sobre este o aquel controvertido asunto, sino también y, sobre todo, en la ingenuidad desplegada en el primer tramo de esta publicación, en esa arrogancia tierra que no pone límites a la ilusión y supone, con tremenda candidez, que la eficacia para la dignificación del flamenco está en función exclusiva de la solidez de los argumentos esgrimidos.\n\nLa experiencia que no siempre es madurez pero suele correlacionar positivamente con ella, nos puso al descubierto otros resortes, otras claves, necesarias para que el entorno del flamenco resultara inteligible. Al margen de leves heridas, el saldo de lo gratificante se sitúa muy por encima de lo que, en aquella primavera preconstitucional, cabía esperar.\n\nLa línea editorial de esta Revista ha incurrido, con toda probabilidad, en inexactitudes, en olvidos, en torpezas de enfoque o de planteamiento, pero siempre ha pretendido estar alentada por la proporcionalidad en los asuntos debatidos, por el rigor en la exposición y, sobre todo, por el amor en la voluntad de dignificar el flamenco. Como dijo Rimband, \"Es falso decir: pienso. Se debería decir: me piensan\". Por eso, al juicio de nuestros lectores nos acogemos. Nadie se va. Sólo uno se aparta, como medida de lealtad a un proyecto todavía vivo, como cautela de higiene cultural. Gracias a todos por su comprensión.",
    "title": "La Reno vación",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 454,
    "article_char_count_full": 2868,
    "article_char_count_review": 2868,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-01-11-left-100-n-meros-cien-satisfacciones",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nQue una persona ligada a la crítica flamenca sea a la vez codirector de la Revista de Flamenco \"Candil\", creo que no tiene ninguna incompatibilidad. Es más, considero que es hasta necesariamente obliga-da la primera actividad en parte, para desarrollar la segunda. Que esta misma persona, o sea, el que les escribe, dedique unas líneas a resaltar lo que fue la presentación del número 100 de la Revista codirigida, no está en función de mirarme el ombligo, sino de acometer otra de las tantas tareas que se desprenden al ejercitar el análisis flamenco. Además, no pienso entrar en el contenido del número por ser ampliamente conocido a través de otros trabajos y crónicas publicados en diarios. Por tanto me voy a remitir solamente a lo que fueron los actos desarrollados en la sede de la entidad\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nr otra de las tantas tareas que se desprenden al ejercitar el análisis flamenco. Además, no pienso entrar en el contenido del número por ser ampliamente conocido a través de otros trabajos y crónicas publicados en diarios. Por tanto me voy a remitir solamente a lo que fueron los actos desarrollados en la sede de la entidad editora, con motivo de la presentación del citado número. Así, he de argumentar que la promotora de la Revista \"Candil\", la Peña Flamenca de Jaén, ha conseguido culminar con éxito y no sin fatigas, la publicación de los cien números de la misma, tras una tarea que ha concitado enormes esfuerzos por parte de sus editores y equipo de redacción, culminados principalmente por la amplia aportación recibida de un sinnúmero de colabora-radores que han llenado sus páginas de investigaciones, ensayos, biogra-fias o críticas flamencas, y gracias igualmente a la ayuda económica de instituciones como la Consejería de Cultura de la Junta de Andalucía principalmente, la Diputación Pro-vincial de Jaén y el Ayuntamiento de la capital. Para conmemorar dicho evento, se programaron tres actos que dieran la suficiente cobertura difusora de los logros conseguidos. Un recital Ramón Porras co-director de Candil, en el acto de presentación del número 100. Detrás, de izquierda a\n\n[ENDING CONTEXT]\n\nmás señeros —léase El Gloria— sin llegar a la tesitura adecuada. En la granaína aureliana y posterior malagueña del Mellizo, sus entonaciones carecieron de melodía. Finalizó con una larga serie de bulerías en las que abundaron los tercios jerezanos, ciertos aires acupletaos y algunos ecos rememorativos del Gloria, éstos últimos sin convicción.\n\nMagnífico el acompañamiento efectuado por Paco Cepero, el cual volvió a patentizar su adaptabilidad para desarrollar el compás, la creatividad en las falsetas, el arrope necesario a la cantaora, así como un toque esplendoroso de matiz y flamencura.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "100 números, cien satisfacciones",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1110,
    "article_char_count_full": 6915,
    "article_char_count_review": 2913,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "1996-01-12-right-el-futuro-de-las-revistas-de-fla",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDe ha celebrado en la Peña Flamenca de Jaén la publicación del número 100 de la Revista Candil, acontecimiento que constituye, desde todos los puntos de vista, una auténtica proeza, digna de los mayores elogios, dirigidos, sobre todo, a los responsables de la Revista, que con gran tenacidad y conciencia del enorme servicio prestado al arte flamenco, han salvado todas las dificultades que mantener cualquier publicación supone, máxime si se trata de una publicación de este tipo, con un tema tan específico. Igualmente debe elogiarse el apoyo recibido por las distintas instituciones que hacen posible la existencia de estas publicaciones.\n\nSi sorprendente o casi milagroso es que una manifestación artística, cultural y antropológica de la naturaleza del flamenco, que aparece como tal en el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicación\"]\n\ntan específico. Igualmente debe elogiarse el apoyo recibido por las distintas instituciones que hacen posible la existencia de estas publicaciones. Si sorprendente o casi milagroso es que una manifestación artística, cultural y antropológica de la naturaleza del flamenco, que aparece como tal en el siglo pasado, se mantenga todavía vigente, con la lógica evolución que el tiempo exige, durante ya más de cien años, igualmente milagroso es que una publicación como Candil haya llegado a los cien números, y que tenga todavía vocación de futuro. Pero no se trata de impartir elogios, cuyo merecimiento es evidente, y que ya han sido, por lo demás, impartidos en los lugares y momentos oportunos, sino de realizar una reflexión, aprovechando la oportunidad, para hacer un chequeo al estado de la cuestión, ver en qué momento se encuentra el panorama flamenco en su práctica actual, sus posibilidades de supervivencia en el futuro que se avecina, a punto de terminar el siglo XX, de donde se desprenderá la posibilidad o no de subsistencia de las revistas especializadas que, como Candil, tienen como temática la actividad flamenca en todas sus dimensiones, puesto que sin ésta no puede existir la crítica o reflexión que se practica en la Revista. Ya se habló en la mesa redonda correspondiente, en la que, por cierto, se puso en evidencia cuán escaso es el contingente humano que se interesa por estas cuestiones, como bien resaltó Rafael Valera, de la necesidad de la existencia de publicaciones que, aún careciendo de la inmediatez y frescura de la noticia puntual, función que corresponde al periodismo diario, escrito o hablado, hagan una reflexión\n\n[ENDING CONTEXT]\n\nsegundo milenio; con las características ya enumeradas, el futuro del flamenco creo que está asegurado al menos durante dos generaciones, la surgida a mitad de siglo que está dando sus frutos en estos momentos, y la siguiente que también está produciendo ya, y que es la que lo representará en las primeras décadas del siglo XXI. Lo que ocurra después es imprevisible; se seguirá rememorando y se seguirá haciendo intentos de renovación hasta el límite de lo posible. Cualquiera que sea su evolución los estudios sobre flamenco y las revistas especializadas deberán seguir dando fe de lo que ocurra.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El futuro de las Revistas de Flamenco",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 2120,
    "article_char_count_full": 12678,
    "article_char_count_review": 3281,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicación"
      }
    ]
  },
  {
    "article_id": "1996-01-14-left-muri-antonio-un-rey-sin-sombra",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Martín Martin\n\nD e padres malagueños y nacido en el seno de una humilde familia que tuvo diez hijos, aunque sólo le vivían cinco, Antonio, hijo de Francisco Ruiz Burgos —empedernido conductor borrachín— y de Dolores Soler Barranco —frustrada bailaora—, había venido al mundo el día 4 de diciembre de 1921 en el número 1 de la sevillana calle Alvaro de Bazán, en casa de su tía Ana Ruiz, de ahí que lo bautizaran con el nombre del dueño de la casa.\n\nSus primeros balbuceos los inició al compás de la música de Juan el Organillero, y como se negaba a ir al colegio del Callejón de las Bescas, su madre Lola lo llevó a instancias de una vecina, María la Bruja, a la academia del maestro Realito. Contaba por entonces 6 años de edad y allí conocería a Florentina Pérez, \"Rosario\", con quien\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestro\"]\n\nntina Pérez, \"Rosario\", con quien compartió escenarios por espacio de 22 años. Los niños debutaron al año siguiente junto a Manuel Vallejo en el Teatro del Duque. Luego sería la pianista Pepita Mezquita quien los bautizó como \"Les Petits Sevillanitos\", actuando así en la Exposición Internacional de Lieja (1928) y en la Iberoamericana de Sevilla (1929). Luego, y de la mano del representante artístico Villalvilla, aprendieron otros estilos con el maestro Otero (clásico español), Angel Pericet (escuela bolera) y Frasquillo (flamenco), pasando a llamarse \"Los Chavalillos Sevillanos\", debutando en el madrileño Teatro de Fuencarral en el espectáculo de La Niña de los Peines y junto a Pepe Pinto y La Malena. Coincidiendo con la Guerra Civil, continúan los éxitos de la pareja en Málaga, Barcelona, algunas ciudades francesas y Argentina. De ahí proseguirían por todos los países de Centro y Sudamérica, hasta su presentación en el Waldorf Astoria de Nueva York, donde pasaron a llamarse \"Rosario y Antonio\". Poco después serían contratados como \"los mejores bailarines gitanos del mundo\" para las películas Zigfield girls, Canta otra canción, La cantina de Hollywood y Panamericana. De regreso a España a principios de 1949, debutaron en el Teatro Fontalba de Madrid como \"aristócratas de la danza y triunfadores en América\". Ya con el terreno abonado por la crítica, se presentaron en el Teatro San Fernando de Sevilla, para marchar de nuevo a la capital del Reino a fin de protagonizar dos películas: José María el Tempranillo y El Rey de Sierra Morena. Los triunfos se encadenan y recibe la Cruz de Caballero de Isabel la Católica (1950), para participar a continuación en la película Niebla y sol, hasta que en 1952 hizo la última tournée con Rosario, lo que propiaría el nacimiento del \"Ballet de Antonio\", en\n\n[ENDING CONTEXT]\n\nmales.\n\nYo tengo una espina en mi corazón y no quiero decirle a la gente quién me la clavó.\n\nAntonio Vallejo Muñoz Duros son los golpes que me dio mi suerte y esta serrana si no lo remedio me dará la muerte.\n\nMe dejaron solo y solo me encuentro con las fatigas que yo estoy pasando prefiero estar muerto.\n\nY cuando se lleven a quien yo más quiero antes de verme solito en el mundo se me bunda el cielo.\n\nTú nunca te alegres de ese mal ajeno porque yo digo que \"to\" el que se alegra es porque no es \"güeno\".\n\nQue nunca te pasen ni te lo deseo hay quien le pega Dios mío a su padre y yo me lo creo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Murió Antonio, un rey sin sombra",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "14-16",
    "page_number": 14,
    "word_count": 1966,
    "article_char_count_full": 11398,
    "article_char_count_review": 3437,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestro"
      }
    ]
  }
]
```
