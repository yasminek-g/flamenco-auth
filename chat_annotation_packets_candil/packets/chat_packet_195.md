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
    "article_id": "1989-07-15-left-cantaores-de-granada",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJosé Guardia\n\nin la voz no habría cante. No es S posible hacer cante si no se tiene la voz.\n\n«Dijo la lengua al suspiro échate a buscar palabras que digan lo que yo digo»\n\nEn el cante, la palabra es primordial y su mejor o peor modulación entra ya de lleno en el campo de la voz. Una voz, hoy ronca, profunda, seria y dolorida. Mañana alegre y cantarina. Otra voz que es recordada, recreada para que no se olvide. Y otra que se lamenta.\n\nPero si de todas formas alguna quedó en olvido, que la novia eterna, la musa, la fuente con seis pilares, la guitarra, que les brinde el homenaje en esos soníos negros.\n\nPor ese respeto a la voz en el cante y, como sólo pretendemos hablar del cante y de voces en lo que ha sido calificado como: «un inventario apasionado», es por la que al fondo, me acompaña\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nPero si de todas formas alguna quedó en olvido, que la novia eterna, la musa, la fuente con seis pilares, la guitarra, que les brinde el homenaje en esos soníos negros. Por ese respeto a la voz en el cante y, como sólo pretendemos hablar del cante y de voces en lo que ha sido calificado como: «un inventario apasionado», es por la que al fondo, me acompaña una guitarra. ¿De cuántas me habré olvidado?. No es posible saberlo a ciencia cierta. Una voz y otra y otra conforman a la palabra y con palabras se junta el amor por todas las voces que conforman el fenómeno del cante. Quise no olvidar a nadie. Perdón pido al olvidado si el olvido ha sido mío. Sin grandes pretensiones, sino guiado por la inquietud, se ha querido pasar lista a las voces de Granada. Voces que, al igual que las voces del resto de Andalucía cantaron, cantan y cantarán a las cosas más sentidas. Y perdón en nombre de todos si la historia te ha olvidado. Voces de rebeldía, voces de rabia contenida, voces de la pena negra, voces contra la miseria, voces contra la injusticia, voces que transmiten alegría, voces que arrancan la vista, que cantan el sentimiento, que ponen de manifiesto el patetismo de la vida tradicional de las gentes del sur: «El mar presume de hondura, el viento de su poder, la Tierra de su estatura y el hombre... no sé de qué» Y el cante se hace más ancho al juntarse más voces. Y se extiende por los rincones. En cada pueblo, ciudad, aldea, cortijo o tasca, surge una voz que te inquieta. Que te puede agradar o que no entiendes, no compartes, pero una voz que hoy es legión cuando pretende salir del reducido círculo maldito en que los ilustrad\n\n[ENDING CONTEXT]\n\nbarro, iba modelando los distintos momentos del flamenco en unas pequeñas, pero imponentes esculturas, «como otro angel del duende». Son las esculturas de Rafael Marín, joven artista de cuya muerte se cumplen en estos días los diez años.\n\nEn definitiva he intentado este inventario, sólo llevado por la afición pura y altruista, para ello he tenido que apoyarme en amigos sin cuya ayuda quizá no lo hubiera conseguido. Por ello no puedo terminar sin mostrar mi público agradecimiento a Antonio García Larios, a Curro Andrés por su apoyo, a Miguel Angel González, a Antonio Lastra, a Manolo Gómez.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantaores de Granada",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 1524,
    "article_char_count_full": 8566,
    "article_char_count_review": 3265,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      }
    ]
  },
  {
    "article_id": "1989-07-16-left-curiosidades-flamencas",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJ. A. Pérez Bustamante\n\nComo aficionado al Flamenco y coleccionista morboso de antigua discografía flamenca de discos, o placas, de pizarra de 78 r.p.m., cuya audición a través de viejos gramófonos de bocina resulta siempre excitante, además de curiosa, incluso imprevisible, considero que puede resultar de interés para el lector de Candil tener conocimiento de las características de algunas muy curiosas grabaciones de tales tipos de discos, que actualmente escasean casi tanto como los metales preciosos y que han llegado a alcanzar cotizaciones exorbitantes, que dificultan aún más su incorporación a los archivos de los pocos coleccionistas particulares de discos flamencos antiguos que aún quedan por el mundo. En lo que sigue, procederé a enumerar y comentar brevemente algunas de estas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"grandes\"]\n\nprocederé a enumerar y comentar brevemente algunas de estas piezas de museo, cuya audición esporádica depara auténticos placeres y sorpresas. Comenzaré haciendo referencia a los discos Odeón, de la «belle epoque», de 27 cms. de diámetro, cuya particular medida impedía archivarlos en los álbumes standard previstos para los discos pequeños de 25 cms., al tiempo que quedaban archivados con excesiva holgura en los álbumes previstos para los discos grandes de 30 cms. de diámetro. Otro aspecto curioso que presentaban estos discos —que hoy resultaría inconcebible— era la profusión de grabacioens heterogéneas de artistas y estilos que frecuentemente presentaban sus dos caras, haciendo de la placa un auténtico «poutpourri» donde se mezclaban los aires baturros con los flamencos, asturrianadas, o canciones montañesas. Además, la ceremoniosa presentación que solían incluir tales discos: «Disco Odeón..., cantado por...» y que concluía con toda clase de jaleamientos y gritos estentóreos, tan forzados, como «desaboríos», que supuestamente pretendían animar al intérprete, no dejaba de añadir a estas grabaciones un encanto adicional. 1) Zonophone 552147(Garrotín) y 552148 (Jotas) Ambas caras están interpretadas por «el Niño de Medina», sin que proceda realizar especial comentario sobre el garrotín, tan de moda en la época. Sin embargo, la interpretación de las jotas puede calificarse tranquilamente de deleznable, pues resultan carentes de todo aire baturro, monótonas, cansinas y francamente aburridas. Por si todo esto no bastase, la descafeinada jota que se ofrece va soportada por un aflamencado acompañamiento de guitarra, que carece de cualquier reminiscencia, o efluvio, de guitarra aragonesa. 2) Regal K 2942 (Bulerías) y K 2952 (Asturianas) Las bulerías, cantadas por la Niña de los Peines, se comentan por sí mismas, incomparables, mientras que las asturianadas, cantadas por la misma artista, con fondo de alboradas gallegas que no concuerdan con la melodía que se entona («Cuando salí de Cabrales»), resultan execrables, tanto por su deje, como por su estilo. ¡Pobre Asturias! Por su parte, el Niño Ricardo acompaña indistintamente ambas caras, como si de un chico para todo se tratase. ¡Erróneo concepto de la economía! El adefesio esperpéntico que testimonia la audición de estas asturianadas (en toda su más peyorativa dimensión), demuestra bien a las claras el más completo descafeinamiento que se opera cuando incluso los mejores intérpretes de un determinado género se salen de lo suyo, ignorando el sentencioso proverbio de «j'apatero, a tus zapatos!». 3) Gramófono W 263699 (Soleá) y W 263707 (Montañesa) 4) Odeón 13350 (Jotas) y 13354 (Rumba) Ambas grabaciones, a pesar de la explosiva combinación de estilos, resultan excelentes, ya que las jotas están cantadas por el gran jotero que fue Miguel Assó, Ambas caras están interpretadas por Emilia Benito, cuya afiliación artística ignora por completo el que esto escribe. En este caso no se salva ninguna de las caras, incluso a la más benevolente de las críticas. Por un lado, la montañesa «Vega de Pas» apenas recuerda a ningún aire cantábrico conocido, mientras que al lado flamenco le son aplicables análogas consideraciones, a pesar de que la intérprete reproduce una conocida letra del Niño de Cabra. ¡Pobre Cayetano Muriel, si esto oyera volvería a caer fulminado en su tumba! Cabe preguntarse si tal proceder, como implica el dualismo de grabación jotacante jondo buscaba ampliar la comerciabilidad de la grabación intentando hacer clientela matando dos pájaros\n\n[ENDING CONTEXT]\n\nPastora Pavón se aprecian claramente los parentescos y reminiscencias de la genealogía aragonsa sobre las cantiñas gaditanas. Estos estilos de alegrías puede decirse que han desaparecido prácticamente del repertorio actual de los cantaores.\n\nLos ejemplos aducidos (jexisten muchísimos más!) ilustran claramente algunos aspectos muy notables de la evolución de las grabaciones de discos flamencos, que se han producido desde principios del presente siglo hasta la actualidad. Se trata de curiosidades pretéritas, que bien merecen ser conocidas y objeto de comentario por la afición flamenca.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Curiosidades Flamencas",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 1622,
    "article_char_count_full": 10400,
    "article_char_count_review": 5162,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "grandes"
      }
    ]
  },
  {
    "article_id": "1989-07-17-left-el-flamenco-en-la-poes-a-y-en-la",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJosé Luis Buendía López\n\nPonencia presentada por el autor a la Conferencia Internacional «Dos siglos de flamenco». 21-25 de junio. Jerez\n\nEl tema flamenco en la Poesía\n\nComenzaremos en primer lugar por la Poesía: para nosotros está claro que es con los movimientos poéticos románticos cuando se da\n\nR esulta extremadamente difícil sintetizar en el breve espacio de una ponencia un tema tan amplio y al que llevamos dedicados años de esfuerzo, tal cual es la presencia que el tema flamenco ha suscitado desde el momento mismo de sus hipotéticos, y aún insuficientemente estudiados orígenes, hasta nuestros días. Por ello, vaya de entrada la advertencia de que en absoluto va a presentarse aquí de manera exhaustiva un catálogo de nombres y obras que rocen el tema. Tampoco un análisis en profundidad\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicando\"]\n\nflamenco ha suscitado desde el momento mismo de sus hipotéticos, y aún insuficientemente estudiados orígenes, hasta nuestros días. Por ello, vaya de entrada la advertencia de que en absoluto va a presentarse aquí de manera exhaustiva un catálogo de nombres y obras que rocen el tema. Tampoco un análisis en profundidad de tales realizaciones literarias. Para todo ello nos remitimos a la serie de trabajos que, desde hace siete u ocho años, venimos publicando periódicamente en la Revista Candil y que constituyen, creemos, una indagación seria y sistemática entre las conexiones existentes entre ambas actitudes estéticas. plena entrada al tema jondo; en efecto, el flamenco, desarrollado y aclimatado en el siglo XIX, bajo las mismas coordenadas cronológicas y ambientales que el movimiento romántico andaluz, como arte comprometido con su propio tiempo, imbricado en la trama de la más exacta correspondencia histórica, no podía estar, y no estuvo de hecho, al margen de esa circunstancia que le tocó vivir, y de alguna manera sus moldes expresivos estarían en estrecha conexión con la poesía culta de la época. No podemos olvidar que la poesía romántica, al igual que el flamenco, representa, entre otras cosas, un hermoso canto a la libertad (lo que no impide que existan hechos flamencos y románticos profundamente reaccionarios) y que, por tanto, coinciden en cant\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_01 | trigger=\"verdadero\"]\n\npoesía romántica de la década de los cincuenta vaya a estar claramente a favor de lo popular flamenco, lo que la hará desembocar en expresiones en cuanto a la ambientación y también en cuanto a las formas similares a la de nuestros cantes jondos, y motivará que la plana mayor del romanticismo poético adopte teóricamente actitudes flamencas que culminarán en Augusto Ferrán y Gustavo Adolfo Bécquer. Del primero de ellos señalar que se trata de un verdadero creador de coplas flamencas llenas de rajo y hondura sin límites, que se plasman sobre todo en dos libros claves: «La Soledad» de 1860 y «La Pereza» de 1870, para desarrollar un programa jondo, ajustadísimo a los más rigurosos canones del flamenco, en los que igual canta el tema amoroso que la injusticia social que afecta a las clases menesterosas, sin olvidar magníficos registros expresivos que tienen al paisaje andaluz como protagonista. En c\n\n[ENDING CONTEXT]\n\nforma ininterrumpida. No queremos convertir estas páginas en un listín interminable de nula utilidad; día llegará en que se hará precisa una sistematización bibliográfica perfecta de la presencia del flamenco en nuestras letras. Porque es preciso pregonar a los cuatro vientos que nuestro arte sirve continuamente de vehículo expresivo a un puñado de narradores honestos y arriesgados que, liberados de estrechos prejuicios, han decidido poner su técnica y sus sensibilidad al servicio exclusivo de este Sur doliente, que clama, desde hace siglos, por una redención que nunca llega. Nuestro Sur...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El flamenco en la poesía y en la novela",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "17-20",
    "page_number": 17,
    "word_count": 5708,
    "article_char_count_full": 34565,
    "article_char_count_review": 3978,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicando"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_01",
        "family": "AUTH",
        "trigger": "verdadero"
      }
    ]
  },
  {
    "article_id": "1989-07-21-left-poema",
    "article_text_for_review": "D esvirgada y estéril temporada matriarcal caída desde que el mundo robusto cree crear.\n\nLas legislaciones imperialmente ordenadas destrozan el ritmo vital aterrorizado el deslice pacífico de los días que vivían para vivir.\n\nFrancoise Gérardin\n\nse hubo de traducir el amor aproximadamente sinónimo, se hubo de reconocer la palabra correctamente inexpresiva, se hubo de interpretar la sonrisa «propiamente humana»...\n\nY\n\nInquisidores silbatos mandaron golpear la veredilla martilleando su carne plagada de petríficos pezones a millares. Dejó de soplar entre los vibrantes troncos la unión de altísima copa con el pedrusco conversando a sus pies. Se callaron los muchachos la infalible atracción de las hembras. Del bosque tallar huyeron la fresa, la mora, el jilguero, y la rana disecó su cantar sin penca que le guarde frescor. Del riachuelo se arrugó la cama que algún borriquito perdido escala en sueños. El tomillo susurró su agonía.\n\nRemotos recuerdos de fluyente paz y quehaceres amorosos se asoman al límite de trágicos ademanes con la Serrana.\n\n...y sabían distinguir lo nuevoviejo de lo irritantementenuevo\n\nE\n\n1 del Gastor va de parranda con Anzonini y la Fernanda...\n\nAlfonso Fernández Malo razón despierta en dedos silbos virando al norte desconocido...\n\npor las esquinas redondo alivio aprecio largo vino y más vino...\n\nvoz en aquello y la jondura lengua alargada de embocadura...\n\npor la memoria llega un aviso de tabla vieja patada y tino...\n\nun volapié la mano exacta boca de siempre para esta plaza...\n\nestá naciendo lo reinventado: la cantaora a lo utrerano... baila bailando el bailaor bailes medidos en suspensión... dando a compás —dedomesura— el guitarrista su asignatura...\n\ny hubo quien dijo, sin comentarios, hubo quien dijo: lo nuevoviejo con estos tres tres veces bueno.\n\nPara comer Jamón... Jamón\n\nVISITE\n\nTaberna Pepón\n\n参\n\nC/. DOCTOR ARROYO, 12 - TELEF. 21 00 58 JAEN\n\nAPERITIVOS SELECTOS Especialidad en Plancha\n\nC/. MESONES, 18 - TELEF. 26 35 46 JAEN\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al Mérito del Trabajo)\n\nRecepción diaria de MARISCOS Y PESCADOS ESPECIALIDAD EN ASADOS\n\nROLDAN Y MARIN, 7\n\nJ A E N\n\nTELEFONO 22 97 65",
    "title": "Poema",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 342,
    "article_char_count_full": 2161,
    "article_char_count_review": 2161,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-07-23-left-festivales-flamencos",
    "article_text_for_review": "(Esta relación es complemento de la publicada en nuestro número anterior)",
    "title": "Festivales flamencos",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 11,
    "article_char_count_full": 73,
    "article_char_count_review": 73,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
