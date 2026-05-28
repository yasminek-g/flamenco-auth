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
    "article_id": "1995-11-3-right-la-escuela-de-antonio-mairena",
    "article_text_for_review": "Mairena se nos fue en un septiembre de 1983, pero nos dejó su obra, su Cante inmenso. Nos dejó una herencia (\"grandeza testamentaria\", dice Félix Grande) y una escuela. Creemos que Antonio Mairena no fue un genio, como Caracol o como Camarón, sino un maestro, lo que es diferente y, en cualquier caso, igualmente meritorio. El genio es irrepetible, anárquico aun partiendo de lo heredado, es inimitable, es único y no tiene intención alguna de crear escuela, sino de cumplir con su destino muy personal de artista de molde único; el maestro, en cambio, es consciente de su labor didáctica, pretende enseñar, y buscar la metodología y la filosofía apropiada para ello, consiguiendo crear una escuela, con unos alumnos aventajados que toman su dignidad del modelo de referencia y, a su vez, dignifican al maestro con su categoría. Así, la escuela de Antonio Mairena, el mairenismo, dicho sea el término en el sentido más querido y aséptico, sin las connotaciones que últimamente se le están dando de pu- reza u ortodoxia excluyente o simi-\n\nlar, el mairenismo, digo, es un inmenso caudal de cantaores de calidad artística contrastada. Sus más conocidos valedores quizá sean Manuel Mairena, hermano del maestro y su heredero natural; José Menese, el discípulo convertido ya en maestro indiscutible; El Lebrijano, en su primera época sobre todo; Miguel Vargas, cada día con más solera y mairenero confeso; y otros como Curro Malena o Manuel de Paula —con el sello que da Lebrija—, Nano de Jerez, Diego Clavel, José Parrondo, Marcelo Sousa, incluso El Cabrero, José el de la Tomasa o José Mercé, sin agotar aquí la nómina, que sería muy larga, y teniendo siempre en cuenta que cada artista es un mundo personal y en cierto modo único. Incluso podríamos hablar de artistas de baile —Pastora Imperio, Teresa, Luisillo, Carmen Rojas, Laberinto y Rosita Segovia, Matilde Coral y El Negro, Mario Maya, y, sobre todo, Antonio el Bailarín supieron de su grandeza del cante también para el baile— o del toque —Melchor de Marchena, Manuel Cano, El Poeta, Niño Ricardo, Ma\n\nnuel Morao, Pedro Peña, Antonio Carrión, José Luis Postigo, etc.— ligados o identificados con esta escuela, la más significativa de los últimos treinta años. En el terreno de la crítica, de la investigación o de la poesía, son muy devotos de António Mairena hombres como Miguel Acal, Angel Alvarez Caballero, Fernando Quinones, Ricardo Molina, Manuel Barrios, Manuel Herrera Rodas, Ricardo Rodríguez Cosano, Alberto García Ulecia, José María Pérez Orozco, Alberto Fernández Bañuls, Aquilino Duque (autor de la recién publicada \"La era de Mairena\"), etc.\n\nEn esta escuela no se sigue, como muchos ahora dicen, una obediencia ciega, un camino calculado de antemano. Se sigue una estética del cante, y una ética; se sigue un concepto, una filosofía, un menester concreto de cante. Y una fuente, una tradición: Mairena se basa en Manuel Torre, en Juan Talega, en Pastora Pavón, en Joaquín el de la Paula, sus más admirados modelos. Y de su interés salen a la escena discográfica y a la más desgarrada historia del cante nombres como Rosalía de Triana, Manolito de María, Juan Talega, Perrate, Piriñaca, Joaniqui, etc. En esta escuela cabe el campo abierto, porque el mairenismo no se agota en unos estilos y en unas formas, sino que tiene la potencia del desarrollo, sin perder, eso sí, el hilo conductor, la esencia. Así, observamos que, aunque Mairena no grabó ni cantó estilos que en su libro Mundo y formas aparecen prácticamente despreciados, como las marianas, la bambera o la guajira, sus discípulos -Menese, Vargas, Malena, etc.- sí los graban. Pero, ¿cómo los graban? Nos lo decía Manuel Mairena: pensando cómo lo haría Antonio. Tal vez sea así, por lo menos en el caso de Menese. Pero, como Garcilaso respecto a los poetas italianos, no se trata de una imitación ciega, sino creadora y revitalizada, una labor de abeja y no de torpe hormiga.\n\nEsta escuela, de difícil parto, de años de esplendor y conquista durante los setenta y ochenta, en pugna con otras tendencias como el fenómeno musical o el Nuevo Flamenco que empieza por Chiquetete, Loley Manuel o Turronero y acaba en los actuales (flamencos? Ketama, La Barbería, etc., esta escuela, digo, tiene mucha vida por delante, quieran o no quieran los mairenistas más cerrados, quieran o no quieran los antimairenistas más obtusos. El arte de Mairena es universal, eterno, porque está dirigido al corazón, y no sólo al oído, porque está dirigido a todos y cada uno de nosotros, por separado, y no a la masa comercializada. La obra mairenista es ya un clásico, y, por ello, la estudian personas que, como yo mismo, no tuvieron oportunidad de tomarse una copa con su autor. Por ser un clásico, Antonio Mairena cantaor no necesita defensores: su obra se defiende sola. Basta abrir los oídos y las portezuelas del alma.",
    "title": "La Escuela de Antonio Mairena",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 808,
    "article_char_count_full": 4833,
    "article_char_count_review": 4833,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-11-4-right-antonio-bonilla-un-cantaor-en-el",
    "article_text_for_review": "A. B. Martínez\n\nEn todas las épocas del flamenco ha habido grandes cantaores que se resistieron, en un primer momento, a entrar en el campo profesional. Por esta causa u otras circunstancias (falta de padrino, de promoción adecuada o retiro temprano), no tuvieron la posibilidad de acceder a los mercados discográficos. Al no existir esa notable fuente de información sonora, su fama estaba muy limitada a los niveles del gran público, aunque el selecto y entendido sí que tenían referencias notorias de sus dotes artísticas.\n\nPor otra parte, bien es sabido que las casas discográficas, para que un artista les fuese rentable, éste debía de gozar (sin que mediara las circunstancias antes descritas de apoyo) de cierta popularidad que abarcara más allá de su ámbito local o regional, para lo cual era necesario principalmente poseer en su currículum profesional, la realización de numerosas giras por España en los conocidos circuitos-espectáculos que con frecuencia se realizaban por las fechas en que se dieron a conocer como eventos de opereta flamenca. Esa participación artística no alcanzó a bastantes artistas que sí han dejado, en la memoria de buen número de aficionados, su calidad cantora.\n\nSin embargo, sí que existen documentos escritos y la consabida trans-\n\nmisión oral, que no han sido lo suficientemente buceados por investigadores y estudiosos para ampliar, en toda su medida, la historia de este arte. En bastantes ocasiones la omisión en sus trabajos —por esa falta de interés— de la aparición de nuevos valores del flamenco en la época citada, ha propiciado que figuras conocedoras de esta música hayan quedado en el anonimato. ¿Qué hubiera sucedido con Juan Talega o Tía Anica la Piriñaca si no hubiera alcanzado la longevidad?\n\nQué duda cabe que estos estudiosos flamencos se han apoyado, y lo siguen haciendo, en la misma nómina de cantaores —respetables todos— que siempre han aparecido en los libros por mor de sus grabaciones y de sus conocidas giras artísticas, lo cual ha propiciado que el gran público no tenga noticias de cantaores que hubieran podido integrarse en el histórico Catálogo Flamenco.\n\nLamentablemente, al intérprete se le ha medido siempre con la vara del profesionalismo, excepto en casos muy antiguos, sin llegar a reconocer que un artista puede serlo por su calidad cantaora sin haber entrado en el terreno de la profesionalidad. El mismo fruto proporciona idéntico alimento, sea regalado o vendido.\n\nEntre el variable número de excelentes cantaores flamencos que no llegaron a grabar ni a efectuar las famosas tournees, reconocido y elogiado por los profesionales de su época, se encontraba Antonio Boni-lla Sánchez—1906 al 1946—, natural de el Viso del Alcor (Sevilla), con residencia en la capital hispalense, casado en la iglesia parroquial de San Román y con domicilio en la calle El Sol. Su oficio fue el de peluquero —barbero, como así se les llamaba—, montando su establecimiento en la Plaza de los Terceros, aferrándose a la profesión hasta los 35 años de edad, pero cultivando el cante día a día por pura afición.\n\nDespreció interesantes ofertas que le llegaron del mundo del espectáculo y de forma muy directa, por cono- Antonio Bonilla. \"Que en su actuación en este mismo espectáculo, la temporada 1942, dejó confirmado su cartel de cante bueno\".\n\ncer su calidad cantaora: de La Niña de los Peines y Pepe Pinto, las cuales les realizaban cuando asistían a su domicilio particular. Tal fue la insis-tencia en que se profesionalizara, que accedió en 1942 (fecha de la que data el exigido carnet de artista), siendo su trayectoria muy corta, ya que la muerte le sorprendió cuatro años más tarde.\n\nFue un cantaor que dominaba los palos del flamenco, de lo cual la cartelería de los de los espectáculos en que participó dejó constancia de ello con frases como: \"Consagración del cantaor más completo de la actualidad, conocedor de todos los cantes\". Estaba dotado igualmente de una gran sensibilidad para el flamenco. Su salud quebradiza no superó las numerosas noches de alterne de La Europa y las ventas, las cuales frecuentaba necesariamente como el resto de artistas de la época, para procurarse algún recurso económico por la serie de fiestas que en ellas se montaban. Falleció a los 39 años.\n\nEn su corto período de profesional realizó giras artísticas —según la cartelería existente— con La Niña de los Peines, Pepe Pinto, Manolo el Malagueño, Manolo Caracol, Canalejas de Puerto Real y un largo etcétera de artistas que solicitaban su concurso. Fue contratado por Radio Sevilla para intervenir en directo todos los jueves de cada semana. Estas actuaciones eran grabadas por la emisora, pero la dirección de la citada estación emisora, con gran desconocimiento de los valores artísticos andaluces, actuó con cierta ligereza al destruir un buen día —mal día en este caso— todas las grabaciones almacenadas que se realizaron en un determinado período de tiempo.\n\nA pesar de su corta existencia en el campo profesional, también se interesaron por él algunas casas discográficas, y desde Madrid —donde estaban establecidas casi todas ellas— fue llamado por mediación de Canalejas de Puerto Real, con tan mala fortuna que no pudo efectuar el desplazamiento por encontrarse recién operado del estómago, operación de la que no pudo recuperarse.\n\nSirvan estas líneas dedicadas a Antonio Bonilla Sánchez como un pequeño homenaje a todos los artistas que, por cualquier causa, han permanecido en el anonimato para muchos historiadores del flamenco y que, aunque no figuren en libro o texto alguno, su aportación al fomento de las manifestaciones artísticas flamencas fueron riquísimas —en su calidad de profesionales o no— y que sembraron una legión de auténticos aficionados.",
    "title": "ANTONIO BONILLA, UN CANTAOR en el anonimato Martínez",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 926,
    "article_char_count_full": 5728,
    "article_char_count_review": 5728,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-11-6-left-paqui-lara-por-las-vereas-del-ar",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Valera Espinosa\n\nFrancisco Lara González, \"Paqui Lara\", se nos presenta atractiva, con cierto desenfado natural y evidenciando la determinada seguridad profesional de la artista que sabe lo que ha de arriesgar. Su semblante denota frescura salinera y sumirada trasluce dulzurá y fuerza, la dulzura con la que plasma los bajos de sus cantes festeros y esa fuerza determinante para entregarse a la profundidad del quejío siguiriyero. Su edad no es la adecuada para poseer un historial largo de vivencias y compañerismos con las figuras señeras por corta, pero sí ideal para continuar asimilando las esencias flamencas necesarias para redondear su personalidad artística, la cual ha tenido un importante asomo en su disco \"Por las Vereas del Tiempo\", donde patentiza un claro devenir\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"afición\"]\n\nerminante para entregarse a la profundidad del quejío siguiriyero. Su edad no es la adecuada para poseer un historial largo de vivencias y compañerismos con las figuras señeras por corta, pero sí ideal para continuar asimilando las esencias flamencas necesarias para redondear su personalidad artística, la cual ha tenido un importante asomo en su disco \"Por las Vereas del Tiempo\", donde patentiza un claro devenir artístico. -¿Cuándo descubres tu afición? ¿Existen antecedentes artísticos en tu familia? —En mi familia no tengo antecedentes, y por tanto no me inicio a consecuencia de ello. Me inicio ante el ambiente que en mi casa ha habido. Además, mi madre siempre ha estado con el \"acarrero\" de \"tú sabes cantar, ponte\". Luego resulta que había un cantaor en mi pueblo que me escuchó una vez y me dio grabaciones para que escuchara, a la vez que me fue orientando en algunas cosillas. Después me presenté a un concurso y lo gané. Más tarde conocí a Quino Román y éste me centró más en el cante flamenco serio. -¿Qué grabaciones te dio tu paísano? —Me dio cosas que a él le gustaban. Me grabó a Marchena y a cantaores de su escuela. Después, como digo, Quino me ofrece otras grabaciones en las que aprecio otros talantes y otras escuelas cantaoras. —¿Cuál fue el enfoque que le das a tu cante tras dejar los ecos marcheneros? —Yo, tras escuchar a otros muchos cantaores, siempre he pensao que una buena línea es la de Mairena, la de la Niña de los Peines, Fernanda de Utrera... —He observado que también te gusta mucho Chiquetete... —También me gustan algunas de sus cosillas.\n\n[ENDING CONTEXT]\n\npunta con sus cosas primeras, pero es que lo último es bastante bonito. Lo que no me gusta es lo que hacen algunos grupos que hay por ahí, porque eso son cosas aflamencadas.\n\n-¿En qué estilo te sientes mejor?\n\n—Cantando por siguiriγas. Más que por tonás, soleá... No estoy diciendo que sea el que mejor me salga, pero sí en el que me encuentro mejor. A veces, cuando me he subió a un escenario un poco agarrañlla de la voz, me he puesto a cantar por siguiriγas, he echao mis fatiguitas y se me ha quitao.\n\n-¿Cuáles son los personalismos siguirieros que más te llegan?\n\n—Me gustan mucho los de Jerez.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Paqui Lara \"por las vereas\" del Arte",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "6-8",
    "page_number": 6,
    "word_count": 1547,
    "article_char_count_full": 8810,
    "article_char_count_review": 3200,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "afición"
      }
    ]
  },
  {
    "article_id": "1995-11-9-left-rafael-salazar-motos-rafael-fari",
    "article_text_for_review": "Antonio Corcobado\n\nE1 pasado día 21 de noviembre nos despertamos con la desagradable noticia del fallecimiento de este gran artista, que para mi concepto disfrutó del privilegio de contar con la voz más sonora y flamenca de nuestro Arte. Rafael Farina nos ha dejado la referencia sonora ortodoxamente flamenca que de él esperábamos cuantos admirábamos su enorme capacidad, constituyéndose en la figura cantaora más importante de nuestro siglo al haberse prodigado de una manera constante con más dedicación al cultivo de la copla, guiado por un sentido más comercial en épocas en que el cante estuvo un tanto supeditado.\n\nConoci a este gran artista en su Salamanca natal en 1941, aunque no trabé amistad personal con el mismo hasta que aconsejado por Pepe Marchena, según él me manifestó en algunas conversaciones que mantuvimos siempre en presencia y compañía de su hermano \"Calderas de Salamanca\", mi entrañable amigo, decidió trasladarse a Madrid, donde rápidamente se dio a conocer al frecuentar los más importantes centros del flamenco en los que le introdujeron entre otros buenos amigos suyos el importante ganadero D. Manuel Gónzalez \"Machaquito\", así como su primo hermano Emilio Motos, famoso tratante de ganado, en unión de aquel gran hombre que fue \"Vallejillo\", dándose a conocer\n\ncon gran resonancia en los ambientes flamencos de nuestro entonces animado y placentero Madrid. Nuestra amistad tuvo diversos momentos de crisis, salvados éstos alternativa-mente por la buena voluntad que recíprocamente nos guiaba para salvaguardar este sentimiento, especialmente siempre ayudados por la buena voluntad del bondadoso \"Calderas\" que se reanudaba con el deseo de festejarla con alguna fiesta importante que satisfacía nuestra particular manera de \"sentir\". Ultimamente la cosa andaba bastante fría. Sin saber por qué su amorosa compañera\n\nra forjó a su alrededor una barrera infranqueable que hubiera permitido la continuidad de nuestra relación amistosa, dado el empeño que recíprocamente poníamos en superar nuestras discrepancias, en las que siempre teníamos que ceder por su caprichosa manera de interpretar nuestros opuestos criterios en cosas y razones que nada tenían que ver con el Arte Flamenco. Siempre está-bamos fundamentalmente de acuerdo por coincidir en nuestros gustos artísticos en la Escuela Marchenista de cuyo genial creador era, al igual que el que suscribe, un ferviente y apasionado admirador.",
    "title": "Rafael Salazar Motos, \"Rafael Farina\", se nos fue",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "9-9",
    "page_number": 9,
    "word_count": 370,
    "article_char_count_full": 2425,
    "article_char_count_review": 2425,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-11-9-right-el-adios-a-rafael-farina",
    "article_text_for_review": "José Núñez de Castro Gómez\n\nEn plena lucidez artística, a sus 72 años, se nos ha ido, para siempre, un gran profesional del cante flamenco y de la canción española, un gran hombre: Rafael Farina —Rafael Salazar Motos—, gitano de los pies a la cabeza y charro de cuna, nacido en un pueblecito de la Salamanca Monumental, Martinamor. De una numerosísima familia gitana, era el cuarto hijo de los once que tuvo el matrimonio Salazar-Motos. Varios de sus hermanos siguieron el mismo camino del cante, destacando entre ellos \"Calderas de Salamanca\", cantaor de empaque y tronío, que con su voz afilá y ronca, muy gitana, acompañó en el baile, durante muchos años, a la eximia bailaora María Albaicín.\n\nRafael, desde muy joven, ya se hacía sentir su torrente de voz, con un estilo muy personal en el cante por fandango, que dadas sus excepcionales cualidades, sus peculiares fandangos eran y son muy difíciles, no sólo de superar, sino de igualar. Recuerdo que durante mi larga, grata e inolvidable estancia en Salamanca, fui una tarde al Teatro Bretón a escuchar a Farina, que encabezaba un cartel con varias primeras figuras\n\ndel flamenco. El teatro retumbaba con la voz del \"rey gitano\" —como así figuraba en el programa—, cantando ese fandango tan suyo: \"Por Dios que me vuelvo loco/quítala de mi presencia....\", que al igual que otros muchos de su repertorio iba modulando su voz con las inflexiones adecuadas a la expresión de sus sentimientos, pasando de un tono a otro, llevando el cante por donde quería, como meciéndolo. El siempre muy recordado \"Porrina de Badajoz\" también gozaba de esta singularidad e idiosincrasia.\n\nFarina, entra por la puerta grande en el catálogo de los fandangos artísticos de creación personal, o fandangos personales, que a partir de principios del siglo XX crearon algunos grandes cantaores. Así como se popularizaron, con su sello personal, los fandangos de Vallejo, de \"Carbonerillo\", de Pepe Aznácollar, de Antonio \"El Sevillano\", del Niño de Cabra, del Gloria, de Cepero, del Caracol, del Gordito de Triana, de Marchena, del mismo Porrina, y de tantos otros. Rafael Farina ocupa ya un lugar de especial relevancia en\n\nesta selecta lista de creadores de fan- dangos personales.\n\nDe Rafael Farina, no podemos olvidar su otra faceta artística, la de cancionero, el maestro de la copla española, que aflamencaba con su característica manera de interpretarla. En esta parcela ha dejado canciones muy sentidas y populares como ese bolero andaluz \"Las campanas de Linares\", de Ochaita, Valerio y Solano, como homenaje a Manolete, muerto por el toro \"Islero\" en dicha plaza; \"Vino amargo\", un tango-milonga, también con música de Juan Solano; \"Mi perro amigo\", etc., y sobre todo el canto a su tierra, que tanto amaba, con esos dos bellos pasodobles, de los que era autor: \"Mi Salamanca\" y \"Martínamor\", que ha paseado por el mundo entero.\n\nCompartió éxitos clamorosos con grandes figuras de la canción y del cante, primero con la Compañía de Conchita Piquer que le dio la alternativa, a quien le dedicó uno de sus fandangos, al igual que a la gran bailaora gitana Carmen Amaya — \"A la memoria de Carmen Amaya\"—, de la que fue un fiel admirador, siguiendo recorridos artísticos con Lola Flores, Porrina de Badajoz, Antonio \"El Sevillano\", \"La Niña de Antequera\", \"La Paquera de Jerez\", \"El Beni de Cádiz\", \"El Príncipe Gitano\", \"La Niña de la Puebla\", Juanito Valderama y un sinfín de compañeros en el denominador común de lo flamenco, de ese complejo mundo del cante flamenco.\n\nSus restos reposan en Salamanca, en su Salamanca querida. Su arte sigue vivo, sobrevive, y cada vez que escuchemos su privilegiada voz, será como una emotiva evocación al pasado, a nuestras vivencias, porque su cante, sus canciones en algunos momentos de nuestras vidas han tenido un significado especial que nos ha hecho vibrar.",
    "title": "El adiós a Rafael Farina José Núñez de Castro",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 635,
    "article_char_count_full": 3836,
    "article_char_count_review": 3836,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
