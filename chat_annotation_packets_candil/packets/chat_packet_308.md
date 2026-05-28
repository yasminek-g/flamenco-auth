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
    "article_id": "1995-07-57-right-el-jaleo-fue-madre-de-la-sole",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRevista de Flamenco Peña Flamenca\n\n2151 de Jaén\n\nLa verdad sobre Rita la Cantaora N ☀ 8,21\n\nManuel Vallejo o el ruiseño del flamenco, N ☀ 10,23\n\nAlgo más sobre los cantes de Levante, N ☀ 11,18\n\nUna broma de mal gusto, dada en 1905,\n\npersiste en la actualidad, N ☺ 12,15\n\nAlgo más sobre los Cantes de la \"madrugá\", N ☹ 14,19\n\n¿Forman el árbol flamenco el\n\nverdial y los fandangos de Vélez?, N º 15,7\n\nDiscografia flamenca. Manuel Cen- teno, N º 15,38\n\nDatos para la historia del arte fla-\n\nmenco, N ☺ 15,17\n\nDatos para la historia del arte fla-\n\nDe Cayetano Muriel y una última\n\nmenco, N º 16,18\n\ngrabación, N ☺ 16,33\n\nDiscografia Flamenca: Aurelio Selles, y otros, N ☺ 16,38\n\nAntonio Grau Mora \"Rojo el\n\nAlpargatero\", N ☺ 17,9\n\nDiscografia de Artistas Flamencos:\n\nEscacena, N 17,46\n\nRelación\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"Crítica\"]\n\nViejos letristas flamencos, N ☺ 22,10 Discografia de Artistas Flamencos: José Cepero, N º 22,57 Diálogo abierto sobre los cantes de Málaga y Levante, N º 24,33 Discografia de Artistas Flamencos: Discografia de Artistas Flamencos: Manuel Vallejo, N ☀ 24,40 C. Muriel Reyes “Niño de Cabra”, Arbol del tango gitano, N ☀ 26,31 Discografia de Artistas Flamencos: \"El chato de las Ventas\", N 0 26,50 teraria), N ☺ 27,33 Enderezando entuertos (Crítica Li- Más sobre el Tango Gitano, Discografia de Artistas Flamencos: \"Corruco de Algeciras\", N 0 27,35 Como el gazpacho, el cante flamen- N 0 27,46 co admite cuanto se le eche, Taranta del árbol malagueño, N 0 28,22 Nota aclaratoria para un aficionado, N ☀ 28,25 Discografia de Artistas Flamencos: José Palanca, N ☺ 28,46 ¿De qué enfermedad mueren nuestros Mi adiós al gran maestro (A. Mairena), N 0 29,43 Discografia de Artistas Flamencos: Aurelio Sellés, El Americano, N 0 29,46 Discografia de Artistas Flamencos: Manuel Torre, N ☺ 30,33 De cómo fracasé en mis investigaciones sobre F. Lema \"Fosforito\", N ☺ 30,49 Discografia de Artistas Flamencos: lo\", N ☹ 30,52 Discografia de Artistas Flamencos: Pepe Marchena, N ☀ 31,44 Discografia de Artistas Flamencos: La Niña de los Peines (P. Pavón), N º 32,49 Discografia\n\n[ENDING CONTEXT]\n\nOlivares, por Jaime Olivares, 24-5-1995 El admirado Fausto Olivares no pudo ver este número emblemático de la Revista Candil. Se nos fue —«nadi dabe cómo ha sido»— un día luminoso de Primavera. Ningún pintor, como él, somatizó con tanta hermosura el magnífico universo de lo jondo. Por eso, y por innumerables razones más, Fausto Olivares se constituyó en emblema de esta publicación, en referencia obligada no sólo para quienes le admirábamos, sino para la generalidad de los lectores de Candil que, a través de sus portadas, hallaron una peculiar enseña del flamenco. Descansa en paz, amigo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "¿El \"jaleo\" fue madre de la soleá?",
    "periodical": "candil",
    "issue_id": "1995-07",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "55-75",
    "page_number": 55,
    "word_count": 4067,
    "article_char_count_full": 25622,
    "article_char_count_review": 2874,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "Crítica"
      }
    ]
  },
  {
    "article_id": "1995-07-63-right-indice-de-rese-as-discogr-ficas",
    "article_text_for_review": "Indice de Reseñas discográficas",
    "title": "Indice de Reseñas discográficas",
    "periodical": "candil",
    "issue_id": "1995-07",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "63-63",
    "page_number": 63,
    "word_count": 4,
    "article_char_count_full": 31,
    "article_char_count_review": 31,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-09-3-left-editorial",
    "article_text_for_review": "Editorial\n\nde Carlos Saura\n\nHemos de reconocer, por adelantado, la precariedad de nuestra cultura filmica, pese a nuestra condición de cinéfilos y pese al sincero interés que despierta en nosotros la lectura de un arte que, cuando alcanza plenitud, es hermosamente equi parable al mejor lienzo, a la mejor composición musical o al mejor de los libros. Precariedad, por otro lado, que no es tan literal que no nos permita evaluar el extraordinario trabajo de Saura, en muchas de sus obras, y singularmente en ésta, desde una perspectiva exclusivamente filmica, a la que, en especial, contribuye la aportación, en fotografia, del genial Storaro, poseedor de tres Oscar de Hollywood. Nadie como éste ha creado respecto del flamenco un lenguaje fotográfico más pleno.\n\nPero, como es obvio, nuestra atención en este film se centra en las intervenciones de los artistas flamencos y en sí éstos, conocida y admitida la solven-\n\ncia artística de los mismos, han estado a la altura de las circunstancias. Creemos de verdad que no. Y sospecho que no por causa que les sea imputable. Un director artístico, asesor de flamenco o lo que sea, hubo de advertir que gran parte de las grabaciones flamencas eran noto-\n\nriamente mejorables. A cualquier mediano amante de este arte le consta que Fernanda de Utrera, tal vez la más grande cantaora por soleá de todos los tiempos, merecía que en esta emblemática cinta quedara para la posteridad una intervención más brillante, más acorde a los méritos propios de la cantaora de Utrera. Los menos estudiosos del flamenco, el gran público, que dentro y fuera de España, se acercará, por vez primera, a este arte, merecían una selección más rigurosa de cantes y de artistas, más acorde a lo verdaderamente básico del flamenco. No es de recibo la inclusión de algunos intervinientes —Lole y Manuel, y Manzanita, por citar sólo dos ejemplos— en un film en cuyo frontispicio existe una sola leyenda: Flamenco. No es problema de calidad artística de los citados, sino cuestión de que se identifique como flamenco lo que cualquier neófito en este arte sabe que no lo es.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1995-09",
    "year": 1995,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 350,
    "article_char_count_full": 2092,
    "article_char_count_review": 2092,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-09-3-right-flamenquismo-y-modernismo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPonencia\n\nRafael Núñez Ruiz\n\nIntroducción:\n\nEl objeto de esta ponencia es constatar la presencia del Flamenco en Cataluña a fines del siglo XIX e inicios del presente, tratando de situar su presencia en el contexto de las variadas relaciones de Cataluña -y muy en particular, de Barcelona- con los diversos movimientos económicos y socioculturales del resto de España -y, muy especial, de Andalucía-. Tal constatación evidencia, además, la incardinación del «flamenquismo» y de otras manifestaciones culturales y musicales populares en el proceso de transformaciones urbanas y socioculturales de la Cataluña finisecular, así como la beligerancia que ostentaron hacia el «flamenquismo» los guías ideológicos y culturales del «proyecto catalanizador» de la época, es decir, del primer «catalanismo», y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"título\"]\n\nron diluyéndose en el «catalanismo». Aclaro que el fundamento documental de esta ponencia es la recopilación de numerosas referencias al flamenco, a «lo flamenco» y al «flamenquismo», que he encontrado en mis lecturas de historia social y cultural de la Cataluña del «tombant» (cambio) de siglo. Lo que presentó, aunque basado en una nutrida bibliografía, no es, en absoluto, ni lo he pretendido, un exhaustivo trabajo bibliográfico. Baste saber, a título de ejemplo, que no he tenido en cuenta algunos trabajos de autores citados (como es el caso de los estudios de P. Gabriel sobre el «Espacio urbano y la articulación política en Barcelona» entre 1890 y 1920, la historia de la cultura y ciencias catalanas, dirigida por este historiador y en fase de publicación por Edicions 62, «La cultura del catalanisme» de J. Ll. Marfany, en vísperas de editarse por Empuries, o el del trabajo, creo que inédito, de E. Ucelay de Cal sobre las «formas asociativas y grupales en la sociedad urbana») ni publicaciones de otros autores que, aunque no se citan, pueden aportar alguna luz sobre este asunto (entre otros, por ejemplo, el de M. Ralle sobre la sociabilidad obrera en la sociedad de la Restauración, entre 1875 y 1910). De todos modos, pienso que, a pesar de su modestia, lo que aquí se expone facilita futuras vías de indagación directa sobre fuentes primarias de la época acerca de una reveladora faceta del flamenco en Cataluña, que muestra, a la vez, su polémico y conflictivo encaje en la «cultura nacional» elaborada y acuñada por el catalanismo y, sin embargo, su concomitancia con la cultuta popular de la Cataluña contemporánea. I. Hi pótesis sobre el origen de la presencia del Flamenco en Cataluña y de su relación con las manifestaciones culturales y populares autóctonas La escena, cuando menos, ofrece un enorme parecido incluso en la posición del guitarrista y el supuesto cantaor, con las estampas, grabados o dibujos de los bailes de Triana, de La Perla y El Jerezano, de D. F. Lameyer, en los años treinta del siglo pasado con la \"clásica postura del antiguo fandango andaluz\", de los años cuarenta\n\n[ENDING CONTEXT]\n\nparadigmática son los «cafés cantantes» de Barcelona.\n\n5. Los códigos culturales de «modernistas» y «noucentistas» y la simbología musical y los hábitos lúdicos del catalanismo collisionan con el flamenquismo, en cuanto éste aparece como un elemento distintivo de la nueva cultura popular que no se aviene con el proyecto «nacionalizador» o de hegemonía ideológica de un importante sector de la intelectualidad modernista, catalanista y noucentista.\n\n3. La persistencia de lo flamenco en la cultura catalana contemporánea se ha de explicar, asimismo, por su inserción en la nueva cultura popular,\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Flamenquismo y Modernismo Rafael",
    "periodical": "candil",
    "issue_id": "1995-09",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "3-11",
    "page_number": 3,
    "word_count": 9194,
    "article_char_count_full": 58196,
    "article_char_count_review": 3740,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "título"
      }
    ]
  },
  {
    "article_id": "1995-09-12-left-cr-nica-del-xxiii-congreso-de-ar",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFrancisco Hidalgo\n\nLmicas, las actividades artísticas, turísticas y convivenciales de la más amistosa reunión anual del mundillo flamenco, el Congreso de Arte Flamenco, en su vigésimotercera edición se celebraron del 2 al 9 de septiembre en la villa de Santa Coloma de Gramenet, «ciudad —en palabras de su alcaldesa, Manuela de Madre— mestiza de culturas vivas por cuyas venas fluye con fuerza el arte y la cultura flamenca». Ha sido ésta la segunda ocasión que una localidad catalana acogía al Congreso de Arte Flamenco, en 1986 L'Hospitalet de Llobregat fue la sede de la décimocuarta edición, pero sí es la primera en que la inmensa mayoría de trabajos presentados están elaborados en Cataluña. Hecho, hasta ahora, absolutamente infrecuente.\n\nQuiso la organización, Ayuntamiento de la ciudad y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"recuerda\"]\n\nde vida de la artista, desde su llegada a Barcelona para rodar la película «Los Tarantos» hasta su muerte y entierro. Cuatro exposiciones más pudieron gozar los congresistas y otras dos conferencias, previas a las sesiones de debate del Congreso, programaron los organizadores, así como la presentación del último disco de la colección «Yunque flamenco» que, posteriormente, le sería obsequiado a todos los congresistas. Por cierto, pocos Congresos recuerda este cronista que hayan ofrecido a los parti- cipantes tan repleta «bolsa» de obsequios. Las salas de Can Sisteré acogieron las exposiciones de fotografías de Paco Feria y de Rosa M $ ^{a} $ Panadés. La del primero, auspiciada por la Fundación Gresol Cultural, es testimonio gráfico de algunas, tal vez las más interesantes o de mayor resonancia, de las actividades o de los acontecimientos flamencos celebrados en Cataluña de Septiembre del 94 a Julio del 95. Paco Feria, nacido en Tarrasa en 1960, tiene en la fotografía su particular modo expresivo y en el flamenco ha hallado una fuente riquísima de inspiración y de apasionamiento. La fuerza, la intensidad, la emoción y la magia del flamenco, le han aprisionado y al mismo tiempo por él han sido recogidos en una particular visión. Las fotografías de la barcelonesa Rosa M $ ^{a} $ Panadés componen una impresionante galería de actitudes, de rostros, de gestos, de instantes, sobradamente expresiva de las emociones que el arte de que son portadores puede transmitirnos. Tiene, o así me lo parece a mí, el fotógrafo de lo flamenco algo de felino, de ese felino que agazapado espera el momento preciso para hacerse con la presa codiciada. Sólo con esa tensión, paciencia y precisión puede lograr buenas imágenes flamencas, fotografías hechas, naturalmente, con sentido y sentimiento de lojondo, con conocimiento de un arte que tiene sus propios códigos expresivos Rosa M $ ^{a} $ Panadés está sobrada de todo ello. Lo suyo ha sido un enamoramiento total y sin solución. Ella misma ha dicho: «Descubrir el Flamenco fue como tocar el cielo con las puntas de los dedos, de ahí que una sensación como ésta, tan plena y rotunda, exija ser comunicada y, a ser posible, compartida. Algo en mí se negaba a creer que aquello fuera tan sólo un sentimiento de amor pasajero; sabía que se trataba de algo más profundo, de algo cuyas raíces todavía hoy siguen creciendo». En el mismo marco, Can Sisteré, se celebró la conferencia de José Candado sobre Camarón, en la que hizo un breve y documentado repaso de la vida y de la obra del cantaor desaparecido, y yo mismo ofrecí un breve resumen de la historia del Flamenco en Cataluña, centrándome, especialmente, en el período que abarca de 1850 a 1936, por más desconocido, y ofreciendo algunas informaciones nuevas de locales, artistas y ac\n\n[ENDING CONTEXT]\n\nel bello marco elegido por el alcalde de la ciudad, Celestino Corbacho, para ofrecer a los congresistas y acompañantes un abundante y variado refrigerio, tras el que se procedió al acto de entrega propiamente dicho, mangníficamente conducido por Paco Vargas. Carmen Linares (Cante), Vicente Amigo (Toque) —en su ausencia lo recogió Pepe Arrebola—, Milagros Mengíbar (Baile), Blas Vega (Investigación) y el Grupo Cruzcampo (Labor de Conjunto) han sido los premiados en esta primera edición.\n\nEl vigésimotercer Congreso de Arte Flamenco ya es historia. La próxima cita: en septiembre y en Sevilla.■\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XXIII Congreso de Arte Flamenco",
    "periodical": "candil",
    "issue_id": "1995-09",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 2102,
    "article_char_count_full": 13197,
    "article_char_count_review": 4415,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "recuerda"
      }
    ]
  }
]
```
