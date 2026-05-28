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
    "article_id": "1995-01-23-left-som-flamences",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nUna cava de jazz que se convierte en cenáculo flamenco, una vez a la semana, visitado por curiosos y entendidos. En él, programa cada viernes veladas verdaderamente gloriosas.\n\nMayte Martín, además, ha sido noticia constante durante estos meses por participar en casi todos los ciclos reseñados y por, sobre todo, su primer disco. «Muy frágil», lo ha titulado. Un disco largamente esperado, madurado muy lentamente y grabado enteramente a su gusto. El disco, salvo una cancioncilla, es globalmente, flamenco y maduro. Con él irrumpe de forma rotunda en el mundo del microsur-\n\nlas voces de Manuel Mairena, El Pele, Diego Clavel y La Macanita. No desmerecen a su lado en ningún momento el resto del elenco. También fue una agradable sorpresa poder escuchar de nuevo a Pedro Sierra, guitarrista\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\n. Con él irrumpe de forma rotunda en el mundo del microsur- las voces de Manuel Mairena, El Pele, Diego Clavel y La Macanita. No desmerecen a su lado en ningún momento el resto del elenco. También fue una agradable sorpresa poder escuchar de nuevo a Pedro Sierra, guitarrista estrechí-simamente vinculado a esta tierra en la que se formó artísticamente. Por último, el momento en que Juan Moreno Maya «El Pele» evoca, acompañado al piano por David Peña, los espectáculos del gran maestro y genial Manolo Caracol es, a todas luces, uno de los más brillantes de todo el espectáculo. co y satisfará a sus incondicionales, ávidos de él, y a los aficionados en general. Teatro Victoria Una grata sorpresa para los aficionados ha sido el ciclo programado en el Teatro Victoria por la Consejería de Cultura de la Junta de Andalucía entre el ocho de abril y el ventidós de mayo. Cuatro espectáculos integran el ciclo: «Picasso Andaluz», de La\n\n[ENDING CONTEXT]\n\nes que la presencia de artistas flamencos en el Grec sea una novedad. Pero sí lo es que artistas jóvenes y catalanes tomen el relevo y se conviertan en protagonistas. En el ciclo no están todos los que son, pero en cualquier caso, todos podrían decir algo tan sencillo como impensable hace unos pocos\n\nEn definitiva, todo un plantel de artistas flamencos que en los últimos tiempos han seducido a nuevos públicos y han avivado la adormecida afición barcelonesa inyectándole nueva vitalidad. Hoy, la incredulidad inicial ha dado paso a la admiración más rendida. ¡Que así sea por mucho tiempo!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Som flamences",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 1304,
    "article_char_count_full": 7931,
    "article_char_count_review": 2547,
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
    "article_id": "1995-01-24-right-cantes-y-sus-escuelas-o-sus-int-",
    "article_text_for_review": "A ficionados de distintos puntos de España me vienen solicitando el libro que publiqué con el título de «Enderezando entuertos», o bien que les facilite la relación de cantes que figura en el mismo. Ante la imposibilidad de complacerles, porque el libro se agotó en seguida y me resulta muy pesado reproducir la relación de cantes, hoy me permito, con la complacencia de la dirección de la revista Candil remitírsela por si estiman oportuno su publicación, para conocimiento de aficionados y lectores en general. La relación es esta:\n\nCantes de Málaga y Levante\n\nA) Fandango de Málaga, Malagueña, Jabera, Zángano, Jabegotes, Rondeñas, Verdial, Fandango de verdial y Fandangos abandolaos. (La bandolá no existe). B) Fandango de Cartagena, Cartagenera, Taranta clásica (árbol malacitano), Taranta de superficie, Taranta minera, Taranta corta (Taranto), Malagueña atarantada, Fandango de Murcia, Murciana, Granadina y Media Granadina. Ay, la mare mía Haciendo por olvidarte Fuiste la paloma mía Porque andando me desmayo\n\nMi vía por aborrecerte Si de ti pudiera vengarme Desde que te conoci No quiero gastar bromas Pensando en ti desvarío A mi mare por su alma Pensando en ti desvarío Caleta y el Limonar Que las canta en el tablao Tú estás dormía en tu cama Se le han corrió los velos\n\nEl Canario de Alora Juan Reyes Osuna Niño del Huerto La Trini La Trini La Trini (se inspiró en la de Baldomero Pacheco) Fosforito Fosforito Fosforito El Mellizo padre Paca Aguilera Paca Aguilera Chato de las Ventas Niño de Vélez El Perote (Trujillo) El Alpargatero de Málaga Anónima. Fue grabada por la «Niña de los Peines». Anónima. Fue grabada por Garrido de Jerez Niño de Cabra. Niño de la Isla Maestro «Ojena»\n\nHasta el Camposanto me fui Ni mancha ningún linaje Toítas las noches te espero sentaíta en mi balcón Mori de tanta pena Lloro porque tengo pena Le debían enterrá vivo Luto por vía me pondré Ni el viento me respondía El Mellizo hijo (Hermosilla) Pitana La Chilanga Revuelta J. Tabaco Personita\n\nEste fandango abandolao fue cantado y grabado por Manuel «Torre» y Cayetano de Cabra. Tampoco quiero callar la existencia de otro fandango hermano del anterior, el de Lucena, que grabaron e hicieron grande Cayetano, Manuel Escacena, Fernando el Herrero y Rafael «El Gloria»: Cayetano grabó: Pidiendo de puerta en puerta. Escacena grabó: Que tó se tenía que acabá. El Herrero grabó: Ni aquel que inventó los tornentos. El Gloria grabó: Que lo he visto en La Barrera. Este flamenquísimo y precioso fandango tiene su origen en el de Juan Breva, que lo grabó con la siguiente letra:\n\nAmigos lectores: Escuchad este fandango del Breva y observaréis que es igual al de los cantaores reseñados: Cayetano, Escacena, el Herrero y el Gloria, sólo que Antonio Ortega Escalona lo hace a base de influárle un ritmo abandolao muy ligero, y por ello menos «jondo». La única diferencia que encuentro en este cante es en el último tercio, que no tiene parecido alguno con el de Cayetano «Pidiendo de puerta en puerta».\n\n(Ultimo tercio): «por lo que me has hecho sufri». En la gloriosa época de «El Niño Medina», Manuel Escacena, El Herrero, El Niño de la Isla y algunos más, todos ellos grandes maestros en la interpretación de estos cantes, la taranta clásica del árbol malaqueño se distinguía entre las demás, simplemente porque en la ejecución de la misma se tenía muy en cuenta si el cantaor, en la segunda mitad del tercer verso, repetía lo que había cantado en el primero. Así:\n\n1.º) Se inicia con la 2.ª mitad del 3.º. De un soberano.\n\n2. $ ^{\\circ} $ lloraba una cartagenera.\n\n3. $ ^{\\circ} $ a los pies de un soberano (1)\n\n4.°) por Dios y por la Magdalena\n\n5. $ ^{\\circ} $ que no se lleven a mi hermano\n\n6.º) al Peñón de La Gomera.\n\n(1) No forma parte de la estrofa. La copla la forman cinco versos octosílabos, pero hemos de admitirlo porque la práctica en la ejecución de la misma, se lo pide al cantaor.\n\nY para identificar a la Taranta minera, se tenía en cuenta si el cantaor repetía el primer verso en la primera mitad del segundo. Así:\n\nAy... el corazón\n\nel corazón se me parte\n\ncuando pienso en tu partía.\n\nNota: En la relación de cantes, escuelas y cantaores, omito a muchos de ellos por estimar que, al no habernos legado sus cantes en grabaciones, en su voz o en la de algún discípulo, debemos considerar sus escuelas clausuradas.\n\nTales maestros e intérpretes, entre otros, fueron:",
    "title": "Cantes y sus escuelas o sus intérpretes",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 752,
    "article_char_count_full": 4378,
    "article_char_count_review": 4378,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-01-26-left-una-aclaraci-n",
    "article_text_for_review": "Yerga siempre habla con convicción, sin duda porque se siente fuertemente respaldado por la posesión de valiosos documentos fidedignos. (F. Vallecillo Pecino)\n\nEspero que este escrito sea leí- do por el señor Manuel Cerrejón, porque me consta que lee mis artículos para extraer de ellos los datos biográficos de cantaores y llevarlos a la carátula de sus cassettes de Pasarela. También espero que sea leído por los que creen que yo traté de quitar a Francisco José Rosado Clavijo «El Niño León», algo que le pertenece.\n\nSeñor Cerrejón, dice usted en su cassette lanzada recientemente sobre la parcela del arte flamenco, «que el Niño León creó un fandango personal que “algunos” le atribuyen a Juan Varea».\n\nAmigos lectores, ese «algunos» soy yo. Lo que sucede es que a veces no se tiene la suficiente gallardía para personalizar. Yerga Lancharro lo publicó justificando lo que dijo en aquella ocasión, con documento evidente, como suele hacer siempre. Por eso, hasta hoy, nadie ha podido con él.\n\nAsí, pues, ese pronombre, «algunos», queda aclarado que no se trata de varias personas, sino solamente de un servidor de ustedes.\n\nHoy, insisto, para poner de manifiesto, que fue el propio Juan Varea quien me dijo que el fandango era suyo. Primero de forma verbal y después por escrito, accediendo a mis deseos de poseer un documento que poder exhibir en cualquier momento y lugar.\n\nLamento que haya aficionados, a quienes no conozco, que a pesar de mi aclaración, irrefutable por cierto, insistan, además con rabieta, para defender a ultranza lo que, en modo alguno, es defendible. El bollullero «Niño León» (q.g.h.), no necesitó en vida ni la necesita hoy después de muerto, defensores baratos ni jurisconsultos sin títulos, porque él supo y su conciencia también, que el fandango que se cuestiona no le pertenecía y sí de forma inequívoca a su amigo y compañero el de Burriana.\n\nA veces me pregunto: ¿Por qué este pleito no surgió en vida del malogrado Rosado Clavijo, y sí ha salido después de acaecido su óbito? Porque de haberse promovido oportunamente, el propio cantaor hubiera salido en defensa de Varea.\n\nFijaos, amigos lectores, cómo el bueno y honrado a carta cabal, Juan Varea, incapaz de una apropiación indebida, dice que él lo «hiso» (creó) del «material» que recogió del guitarrista «El Rubio Pará».\n\nPersonalmente, en uno de mis viajes a Madrid, en la década de los setenta, le visité en su domicilio ubicado en la Plaza de Roma, 19-3.º Dcha., y hablamos «largo y tendido», saliendo a colación la inexacta paternidad del ya célebre fandango al atribuírsele al de Bollullos.\n\nA todo esto, él me dijo: «El Rubio Pará lo canturreaba con mucha frecuencia, pero con un estilo distinto al que yo utilicé en mi grabación. Por cierto, que fue la primera vez que se hizo del fandango: “En lo alto de una loma/quién tuviera una casita”. A mí se me metió en la cabeza no el estilo, sino la letra porque me resultó muy bonita y campera».\n\nEspero que después de esta publicación queden enterados, de una vez por todas, tanto el señor Cerrejón como aquéllos que me vienen haciendo frente.\n\n¡Cómo me gustaría que mis lectores hubieran podido tratar amplia e íntimamente a Juanito Varea! Hoy podrían decir conmigo que fue muy bueno y humilde. Nunca se atribuyó mérito alguno en sus actuaciones. Todo lo bueno era de sus compañeros. ¡Cómo me hablaba de su compadre Perico el del Lunar, de Manolo el de Badajoz, Calcetines, Fernando el Herrero, Pepita Caballero y Maneli Pavón! Todos eran mejores artistas que él, porque desde bien joven convivió artísticamente con ellos y le querían de verdad. Así se expresó en mi presencia y en la de su esposa.\n\nEl me decía siempre: «Eso disen» y de ahí no le sacaba nadie.\n\nSobre «su» malagueña, la que le regalaran aficionados malacitanos, me dijo: «Eso disen en Málaga que fue creada por mí. Yo, por mi parte, no puedo aceptar dádivas así como así, porque en verdad, lo único que yo hago con mis cantes es ponerles algo mío. Yo no he creado nada, si acaso el fandango en cuestión». He aquí, queridos amigos, plasmada su honradez.\n\nD. E. POHREN\n\nPACO DE LUCIA Y FAMILIA: El Plan Maestro\n\n«En este libro, Don Pohren ha conseguido con acierto dar una visión rigurosa sobre el flamenco contemporáneo a través de la familia Sánchez (Paco de Lucía y Familia)». Manuel Martin Martín, Guía de Sevilla de Diario 16.\n\nPrecio de venta en librerías 2.600 Pts.\n\n«Completo, antológico... D. E. Pohren, gran conocedor de la magia flamenca, ha sabido retratar, sabiamente en su libro, la figura irrepetible de Paco de Lucía. Daniel Pineda Novo.\n\nPedidos a: Sociedad de Estudios Españoles Apartado de Correos, 83. LAS ROZAS (Madrid)\n\nTomatito. Nombre artístico de José Fernández Torres, heredado de su abuelo. Almería, 1958. Guitarrista. Destacó muy joven en su tierra natal, siguiendo las influencias guitarristico-musicales de Paco de Lucía, e incorporándose seguidamente a los festivales andaluces, acompañando especialmente el cante de «El Camarón de la Isla», con quien ha realizado su discografía, como asimismo con Enrique Morente, «La Susi», Vicente Soto, Luis de Córdoba y algunos conjuntos modernos. Entre sus actuaciones más significativas, sobresale su participación en el Certamen El Giraldillo del Toque de la III Bienal de Arte Flamenco Ciudad de Sevilla, en 1984, así como las efectuadas en Madrid durante los Festivales de la Cumbre Flamenca, en 1985, en el Teatro Alcalá Palace. Igualmente es destacable su recital con el grupo Indal Jazz Quartell, en el V Festival de Jazz de Madrid, en 1968. Como concertista, Tomatito ha grabado hasta la fecha dos únicos discos, el titulado «Rosa de amor» y «Barrio negro». En ambos hacía Camarón un cante por deferencia hacia su compañero y amigo.\n\nTOCAORES DE HOY\n\nTomatito",
    "title": "Una aclaración",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "26-27",
    "page_number": 26,
    "word_count": 963,
    "article_char_count_full": 5751,
    "article_char_count_review": 5751,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-03-3-left-enrique-morente-premio-nacional-",
    "article_text_for_review": "Editorial\n\nE 1 Ministerio de Cultura, la propuesta del Ayuntamiento de Granada, ha distinguido a Enrique Morente con el Premio Nacional de Música, 1994. Este galardón que, hasta la presente edición, se ha reservado a la música impropiamente llamada culta, viene a significar, ante todo, el reconocimiento institucional a una singularísima forma de expresión musical y también —cómo no— reconocimiento a la trayectoria artística de un cantar que, entroncado en la más pura tradición jonda, ha buceado incansable en otros ámbitos, tal vez a la busca de un sendero idóneo para la evolución, con éxito siempre discutido pero firme en su convencimiento de que las aportaciones que enriquecen, por muy exóticas que puedan parecer a los puristas, no dañan. Siempre resultará controvertido el concepto de «enriquecimiento musical» que posee el cantaor granadino;\n\nlo que nadie puede discutirle —como en otro momento señalamos— es su arrojo para emprender una vía maldecida por numerosísimos detractores. Pero la concesión de este Premio Nacional de Música, significa más cosas. Durante décadas se alimentó la convicción, aun dentro de la propia casa del flamenco, de que éste era puro folklor. Análisis y reflexiones posteriores han\n\nprecisado, con exactitud, esta cuestión en el sentido de entender que el flamenco, con independencia de la naturaleza de sus orígenes, trasciende lo puramente folklórico y se contextualiza como una de las grandes manifestaciones de expresión musical de todos los tiempos. El Premio Nacional de Música de 1994, no se concede a la jota, a la sardana o a la seguidilla manchega; se otorga al Flamenco, como fenómeno musical que, al margen de sus especificidades, cuenta con validez universal. Este Premio significa, además, la consecución de una cota más en la dignificación de este arte que, hasta hace pocas décadas, era todavía considerado como turbio lenguaje de tabernas y prostíbulos. Hay, pues, sobrados motivos para felicitarnos y, particularmente, felicitar a Enrique Morente, Ayuntamiento de Granada y Ministerio de Cultura por su atinado otorgamiento. Enhorabuena, Enrique.",
    "title": "Enrique Morente, Premio Nacional de Música",
    "periodical": "candil",
    "issue_id": "1995-03",
    "year": 1995,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 325,
    "article_char_count_full": 2107,
    "article_char_count_review": 2107,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-03-3-right-la-poca-dorada-finca-espartero",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE n previos episodios he descrito mi deseo de sumergirme completamente en la vibrante vida flamenca que existía en Andalucía en la década de los sesenta, y por fin la oportunidad de realizar ese sueño con la adquisición de una finca, en 1965, cerca de Morón de la Frontera. Aquel esfuerzo resultó, a mi modesto entender, en el primer centro de la historia del flamenco que ofrecía clases de guitarra, baile y cante, y lo más importante: la oportunidad de participar en esa forma de vida hoy casi desaparecida.\n\nLa verdad es que no teníamos en mente hacer algo grandioso, y desde luego un acercamiento era lo menos pensado —siempre he creído que el flamenco, siendo un arte inspiracional, sufre en las manos de los académicos y los estudiosos, quienes tienden a diluir, con sus análisis y sus\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nclases de guitarra, baile y cante, y lo más importante: la oportunidad de participar en esa forma de vida hoy casi desaparecida. La verdad es que no teníamos en mente hacer algo grandioso, y desde luego un acercamiento era lo menos pensado —siempre he creído que el flamenco, siendo un arte inspiracional, sufre en las manos de los académicos y los estudiosos, quienes tienden a diluir, con sus análisis y sus disecciones, el frágil encanto de ese arte tan creativo. Lo que yo quería hacer era no sólo crear para mí la posibilidad de vivir el flamenco de lleno, sino también fomentar y divulgar el verdadero flamenco en su entorno natural y, por supuesto, ayudar en lo posible a esos merecedores artistas no comerciales que vivían el flamenco diariamente como parte intrínseca de su existencia y no sólo sobre escenarios y en estudios de grabación como la mayoría hace hoy día. Así que cuando la idea de un centro de flamencología se hizo económicamente factible, en el invierno del 1964, pregunté a Diego del Gastor por teléfono si sabía de alguna finca cerca de Morón que sirviera para nuestros propósitos. Esta debía tener la suficiente capacidad como para poder a\n\n[ENDING CONTEXT]\n\npero no estaba tan mal si consideramos que nuestra publicidad era más escasa todavía. No teníamos dinero para una campaña publicitaria adecuada, y además temíamos que dicha publicidad atrayera turistas con sólo un vago interés en el\n\nprincipio contactamos exclusivamente con aquellos que habiendo leído mis libros se habían dirigido a mí por carta. Calculamos que correría la palabra entre aficionados lo suficiente como para mantener la operación, y así ocurrió, aunque a duras penas a pesar de la creencia en el pueblo que está-bamos forrándonos. La finca era verdaderamente el arte por el arte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La época dorada Finca Espartero /",
    "periodical": "candil",
    "issue_id": "1995-03",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "3-6",
    "page_number": 3,
    "word_count": 2525,
    "article_char_count_full": 15047,
    "article_char_count_review": 2784,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  }
]
```
