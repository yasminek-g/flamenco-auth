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
    "article_id": "1983-03-10-right-antecedente-teresiano-del-mirabr",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nI. INTRODUCCION (1).\n\nA NTE la celebración del Centenario de Santa Teresa de Jesús, deseo que mi aportación sea este intento de enraizar un cante flamenco, el «mirabrás», con manifestaciones musicales gallegas, como ya han hecho varios estudiosos (2), estableciendo como importante nexo de unión un villancico de Teresa de Avila titulado «Ya viene el alba».\n\nPreparando un trabajo de crítica literaria sobre San Juan de la Cruz se me cruzó el citado villancico, «una pequeña joya literaria» para el Padre Angel Custodio Vega (3), poesía «sosa y disparatada» para Vicente de la Fuente (4). Pienso que ni lo uno ni lo otro, que es una excelente muestra de la poesía desenfadada, simpática y popular que hacía la Madre Teresa.\n\nLeer «Mira Brás», escrito así, me hizo asociar esto con el nombre del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\ns, me pareció, tras un pequeño análisis, que se podía tratar de un antecedente interesante, por lo que inicié el estudio del que resultó este trabajo. EL VILLANCICO TERESIANO. Aunque a los efectos de este estudio sólo nos interesan la letrilla y una «cuarteta» o «redondilla», vamos a transcribirlo. Dice: YA VIENE EL ALBA (Villancico) (5) Migallejo, mira quién viene. —Angeles son, que ya viene el alba. Por Guillermo Sena Medina Heme dado un gran zumbido, que parecía Cantillana. Mira, Brás, que ya es de día; vamos a ver la zagala. Migallejo, mira quién viene. —Angeles son, que ya viene el alba. ¿Es parienta del alcalde, u quién es esta doncella? —Ella es hija de Dios Padre, relumbra como una estrella. Migallejo, mira quién viene. —Angeles son, que ya viene el alba. Como hemos dicho, al Padre Angel Custodio le parece «precioso y encantador de veras», «juguete poético con verdadero alarde de gracia, ingenio y devoción», mientras que a Vicente de la Fuente le hace poner esta nota: «Esta poesía es tan sosa y disparatada, que no puedo creer sea de Santa Teresa por más que se pusiera así en el manuscrito de Cuerva, de donde está copiada» (6). El Padre A. C. Vega (7) hace su estudio, por lo que nosotros nos remitimos a él, aunque haremos algunas consideraciones, acercando el villancico a nuestro tema. Tres palabras nos llaman la atención: «Migallejo», «Brás» y «Cantillana», que son las claves para nuestro estudio. Empezando por la segunda, nos parece que está claro que se trata del nombre Blas, con la alteración de la «r» por la «l», cosa bastante frecuente en el habla popular, incluso en nuestros días. Hay que recordar, como afirman los lingüistas, que algunas consonantes no han tenido siempre el mismo valor fonético, y que este villancico está escrito en el siglo XVI y es posible que la letrilla sea anterior. La Santa vuelve a utilizar la fórmula en otro villancico: «Mírale, Gil, que te está llamando» (8). Aquí aparece la primera conexión con el «mirabrás». Me parece que es un antecedente indudable, tal vez el más evidente y más antiguo que se conoce. Ignoro si antes que en mi artículo se ha tratado en este sentido, creo que no. Viene a ratificar la teoría que me comentaba Antonio Murciano de que muchos nombres de cantes son deformaciones de palabras, producidas por la peculiar forma de hablar del andaluz y de los gitanos, en particular, por lo que, en lo referente al cante que aludimos, sostiénen los flamencólogos que «mirabrás» provenía de la unión fonética de «mira, Blas». Desde luego me sumo a esta interpretación. Por lo que se refiere a la palabra «Migallejo», de la que dice el Padre Vega «no sé de qué es contracción» (9), y sobre la que indica que se escribe junto «Migallejo» y separado «Mi gallejo» en las distintas ediciones, creo que es fácil de interpretar, como veremos, pero no en el sentido que él lo hace. Dice: «Empecemos por confesar que no sé si se debe escribir “Migallejo”, como casi todas las ediciones, o bien “Mi gallejo”, que sería abrevia\n\n[ENDING CONTEXT]\n\nantiguas han devenido en «estos estilos —nos dirá Ríos Ruiz en su obra citada— tan similares entre sí —por sus melodías principalmente— (que) son simplemente cantes de fiesta y entretenimiento, que con el paso de los años y la profesionalización de los intérpretes han ganado grandeza y perfiles definidos».\n\n(33) No es necesario referirse a la «saeta», ni a los villancicos flamencos, ni a otros cantes impregnados de letras religiosas. Los estudios son muchos. De los últimos leídos es: «Donde Dios era “Undebé’», de Ramón Porras, en «Candil», núm. 8, Jaén, 1980.\n\nCorrea Weglison, 9\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antecedente Teresiano del «Mirabrás»",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "10-13",
    "page_number": 10,
    "word_count": 3474,
    "article_char_count_full": 20403,
    "article_char_count_review": 4621,
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
    "article_id": "1983-03-13-right-el-cante-jondo-en-antonio-machad",
    "article_text_for_review": "AUNQUE NO QUEPA EN EL PAPEL\n\nPor José Luis Buendía López\n\nA actividad de Manolo Urbano en el terreno de los estudios flamencos es sobradamente conocida por todos los lectores de «CANDIL». Me parece justo, pues, como redactor de la revista y amigo del autor, el asumir la ingrata tarea de ser el que reseñe esta entrega machadiana, lo que constituye algo así como un «criticar al crítico», o lo que es igual, el más difícil todavía de la pirueta literaria.\n\nLo primero que sorprende de este libro es su escasa o nula distribución en las librerías andaluzas, y concretamente jiennenses, lo que nos da pie a pensar si es que se trata de una obra de la que el autor «se arrepiente» y condena al ostracismo, o un caso más, de los muchos existentes, de flagrante delito de ignorancia acerca de los productos culturales de esta tierra del Santo Reino y el Olvido Perpetuo. Ambas hipótesis son verosímiles, pues el libro, plagado de erratas de imprenta, faltas de ortografía y mal quehacer impresor (¡esos versos aislados a comienzo de página!) es un claro ejemplo de lo que pasa cuando los editores confunden un producto intelectual con la propaganda de remedios para la cura de enfermedades venéreas.\n\nAclarado esto, de lo cual es inocente el autor, del cual conozco su escrupulosidad en el envío y revisión de originales propios y ajenos, pasaré a extractar su contenido, el cual no se corresponde, creemos, en su totalidad, con el enunciado título, ya que más que una reflexión absoluta sobre el Cante Jondo en Antonio Machado, lo que se nos ofrece en él es un largo y apretado refrito de citas «ad hoc» sobre el concepto de folklore, cultura popular, e incluso circunstancias biográficas de Machado repetidamente manoseadas en los manuales de bachiller: un ejemplo válido sería cuando en la página 37 se nos explica, en un alarde de originalidad creadora: «Con su llegada a Baeza, 1913, se produce en la poesía de Machado el paso decidido y ascendente de la poesía de tema castellano a la de tema andaluz». Así, avanzado entre un piélago de citas (Bernard Sesé, Tuñón de Lara, Aurora de Albornoz,\n\netc.) y de referencias continuas a los textos del propio don Antonio, que hace que muchas veces las notas a pie de página sean más abundantes que el texto original, el ensayo, dividido en tres partes, nos conduce a la clarificadora conclusión de que a Antonio Machado le interesó el folklore y que de las esencias de la copla jonda está formada una buena parte de su metafísica y su peculiar aliento poético.\n\nEl resto del libro, es decir, el tejido conjuntivo que da armazón al esqueleto de citas, es una personal y discu-\n\ntible manera de interpretar aquellas en las que Manolo Urbano se muestra excesivamente interesado en hacer hablar al texto con las intenciones que más convenga al autor del ensayo, lo que conlleva unos apasionamientos excesivos y unas interpretaciones que no vamos a calificar de inexactas, puesto que todas las especulaciones sobre un texto deben de ser igualmente válidas, por obvio respeto al que las emite, pero sí de discutibles desde otra óptica menos interesada en las dramatizaciones del lenguaje ajeno: es el caso del comentario que hace Urbano a la carta enviada por Machado a Guiomar, en la que se cuenta a propósito del personaje teatral de la Lola que él: «Jamás hubiera pensado en santificar a una cantaora», cosa que nuestro crítico no acierta a explicarse, y, herido en sus más profundas convicciones, califica de «señorito» a don Antonio por no comprender éste que: «Estas mujeres nacidas del hambre y de la explotación andaluzas eran víctimas en las ciudades artificiales del más despreciable y depredador de los señoritismos, el que precisamente las encanallaba y las hacía rodar por los más oscuros lugares» (página 45). Tan hospiciana y cutre defensa de la «mujer caída», lleva a Urbano a olvidar que precisamente era el espíritu moderno de Antonio Machado el que se oponía a ese tipo de discursos decimonónicos, que abundaban, sin embargo, en las llamadas «obras sociales» de Echegaray o López de Ayala, y que lo que el poeta sevillano defendía era la total libertad de creación, rehuyendo tan estúpidos corsés, manejando los tópicos en su justa medida, sin denostarlos ni elevarlos a su formulación definitiva, en una palabra, sin santificarlos.\n\nAsí podríamos continuar, desarrollando todo un catálogo de emociones de Manolo Urbano en este libro, que nos llevaría a plantearnos la eterna cuestión de los límites de crítica, o lo que es igual, el uso y abuso de nuestras propias genialidades. Como este asunto no está en absoluto resuelto, concluiremos diciendo que el trabajo es una no muy cuidada elaboración, más pseudoliteraria que flamenca, en la que se trasluce una evidente habilidad gacetillera del autor a la hora de manejar los conceptos ajenos y sumarlos a las propias ocurrencias.\n\nJosé Cobo Marchal\n\nCapitán Oviedo, 15\n\nTeléfono 22 76 36\n\nApartado n. $ ^{\\circ} $ 76",
    "title": "EL CANTE JONDO EN ANTONIO MACHADO (Manuel Urbano. Ediciones Demófilo. Septiembre, 1982)",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 832,
    "article_char_count_full": 4922,
    "article_char_count_review": 4922,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-03-14-right-ellos-los-protagonistas-dicen-ca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen:\n\nNombre: Calixto Sánchez Marín Nombre artístico: Calixto Sánchez Lugar de nacimiento: Mairena del Alcor Fecha: 27-7-1946 Domicilio: Mairena del Alcor (Sevilla)\n\n—Parece ser que todo cantaor en esta generación debiera tener sus raíces ¿Cuales son las tuyas?\n\n—Yo te puedo decir que en un principio, y teniendo en cuenta el ambiente, el cual puede influir inconscientemente en la persona, te diré que desde los dos años a los doce años, me he criado en un bar. Por tanto en esa etapa, he recibido una influencia cantaora de toda la gente que iba al bar. Antes, los bares no eran lugares de paso como lo son ahora, eran lugares de reunión. El bar de mi padre era lugar de reunión y además era el único restaurante que había en Mairena. Con esto quiero decir que todos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\nen esa etapa, he recibido una influencia cantaora de toda la gente que iba al bar. Antes, los bares no eran lugares de paso como lo son ahora, eran lugares de reunión. El bar de mi padre era lugar de reunión y además era el único restaurante que había en Mairena. Con esto quiero decir que todos los artistas que pasaban por Mairena, todas las compañías que allí actuaban, al final del espectáculo, iban a comer a nuestra casa. Como es lógico, yo he escuchado allí a muchos artistas, aunque no me acuerdo de sus nombres. Y luego, hay una influencia muy directa del pueblo de Mairena, porque Mairena es un pueblo cantaor por excelencia, se ha cantao muchísimo y desgraciadamente, cada vez se canta menos. A consecuencia de esta nueva época flamenca en la que nos estamos metiendo, o nos están metiendo, los pueblos estan perdiendo su identidad y muchos de ellos sus raíces. Entonces, yo he recibido influencia de los cantaores de Mairena, gente de bastante edad que, yo he concido con plenitud de facultades, con 40 años o por ahí. Esta gente cantaban todos divinamente y, no solamente, por cantes duros como siguirias, soleá, saetas, etc., sino por todos los estilos. Los nacidos en Mairena que tengan mi edad, han tenido la influencia a la que yo aludo. Cuando llegaba la feria de Mairena o la Semana Santa, ya estaba el cante a flor de piel; no solamente de los cantaores nativos, sino también de la cantidad de gente que se desplazaban al pueblo durante estas fechas. Me acuerdo que en el centro de Mairena, en la plaza de las Flores que se llamaba, en Semana Santa, se reunían de catorce a quince cantaores cantando por saetas, y desde luego, todos intentando quedar mejor que el compañero. Esto ya se ha perdido, no existe, y sí te puedo decir, que en mis recuerdos de niño están muy presentes estas situaciones cantaoras. Podemos decir por tanto, que esas son mis raíces. Luego hay una segunda etapa que es cuando empiezo en el festival de Mairena. Uno de aquellos festivales grandes que se hacían en el paseo de la feria. Es en estos festivales donde alguna gente joven comienza a tener inquietud por el flamenco. Yo por aquella época me aprendí dos o tres cosas, concretamente una cartagenera y una malagueña y\n\n[ENDING CONTEXT]\n\nabajo. Esa sería una gran labor para mantener el flamenco en candelero.\n\nOtra labor consistiría en organizar por parte de las peñas y con el apoyo del Ministerio de Cultura, la Junta de Andalucía, Ayuntamientos, etc., concursos, tener tocaores que enseñen a los aficionaos, que se difunda el flamenco... Eso creo yo que sería muy positivo.\n\nPor otra parte, también la Universidad tendría un buen papel en esta labor, organizando aulas de cultura flamenca, coloquios, conferencias, recitales, etc., para que los que estudian en estas universidades conozcan en profundidad lo que es el arte flamenco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen: Calixto Sánchez",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 2036,
    "article_char_count_full": 11590,
    "article_char_count_review": 3843,
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
    "article_id": "1983-03-16-left-rememorando-a-rafael-de-leon",
    "article_text_for_review": "Ha muerto en Madrid un poeta: Rafael de León, sevillano, setenta y dos años. Si ya es mucho ser poeta, fue, además, uno de los grandes del cancionero español popular y andaluz.\n\nDesde los años cuarenta —cuando aprendimos sus dramáticos versos en las «matinés» domingueras de los cines— hasta el hoy de la música «tecno», no pasan tan siquiera unas horas de cada día, sin que por cualquier aparato de reproducción —televisión, radio, disco-máquina tragaperras, etc.— oigamos «algo» en lo que haya intervenido Rafael de León.\n\nA este poeta, autor de las más y mejores letras del cancionero popular de su tiempo, lo cantaron todas las tonadilleras desde Concha Piquer hasta Isabel Pantoja. Pero no sólo esto; también fue el autor de hermosas letras de nuestro cante grande de todos los estilos, que llevaron Caracol, Jarrito, Chocolate, Canalejas, Niña de la Puebla, Rocío Jurado, Cepero...\n\nMerecía Rafael de León un recuerdo en nuestra revista y aquí está.\n\nA RAFAEL DE LEON, POETA\n\n«En medio de la fuente se bañaba la rosa...».\n\nCuando ahora la rosa sus rojos pétalos abría alborozada, Rafael de León, poeta, coplero, —Gacela, Romance, Romancillo, Balada, Baladilla, Luto, Centinela...— se nos fue una noche con su Petenera.\n\nLa Lirio lo anda buscando por la arena de una playa sin barcos ni marineros, ni timón ni Rosa de los Vientos.\n\nR. de León\n\nY son cuatro bailadores —los del Café del Burrero— los que bailan una zambra mientras Caracol la canta.\n\nContando está su romance una Reina sevillana a los ángeles flamencos que bailan con la Parrala.\n\nGanaderas salmantinas, con sombrero cordobés, discuten de sus divisas con Belmonte y con José.\n\nNo se miran ya en el río las muchachas de ojos verdes porque tú le pusiste dragones en la orilla una tarde de junio cuando fuiste a bañarte.\n\nA «Pepe Caballero, andaluz de Cervantes», tú le dijiste un día en la voz del poeta: «Ahora escribo mis dramas boca arriba, sobre un negro pupitre de figura alargada y me llega la lluvia con un gusto de tierra que humedece mi verso y la cal de mis ojos».",
    "title": "Rememorando a Rafael de León",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 350,
    "article_char_count_full": 2042,
    "article_char_count_review": 2042,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-03-16-right-arbol-del-tango-gitano",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Manuel Yerga Lancharro\n\nTANGO: Cante atribuido a la raza gitano-andaluza. Su raíz suponemos está en Sevilla, Cádiz o Los Puertos. Sus tercios son lentos. Fueron eminentes intérpretes, entre otros, La Niña de los Peines, Juanito Mojama, Manuel Torre, Fernando el Herrero, Don Antonio Chacón, El Niño de la Isla, Aurelio de Cádiz y El Niño Medina.\n\nTIENTOS: Hijo del Tango gitano. Su creación también se le atribuye a nuestros compatriotas gitanos. De tercios aún más lentos que los de su genitor. Fueron grandes intérpretes los mismos que he reseñado para el Tango.\n\n* * *\n\nEn los primeros años de este siglo, el nombre de TAN-GO GITANO dejó de figurar en las caras de las placas gramofónicas, como consecuencia de una inexplicable absorción por su hijo el TIENTOS.\n\nTANGO LIGERO: De Cádiz. Hijo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nnuestros compatriotas gitanos. De tercios aún más lentos que los de su genitor. Fueron grandes intérpretes los mismos que he reseñado para el Tango. * * * En los primeros años de este siglo, el nombre de TAN-GO GITANO dejó de figurar en las caras de las placas gramofónicas, como consecuencia de una inexplicable absorción por su hijo el TIENTOS. TANGO LIGERO: De Cádiz. Hijo del tango gitano. De tercios más ligeros que los de su padre. Tuvo su mejor intérprete en La Niña de los Peines y Manolo Vargas. Ante este lamentable hecho cabe preguntarse: ¿cuál fue la causa de esta absorción? ¿No será, quizá, que la guitarra impuso a ambos cantes un mismo compás? Pero aunque lleven el mismo compás, ¿es que nadie distingue el TANGO GITANO hecho grande en las voces de La Niña de los Peines y Juanito Mojama, entre otros? ¿Cuál es la causa por la que, hoy, los intérpretes actuales confunden el TANGO GITANO por el TIENTOS y cuando creen estar cantando por TANGO GITANO lo hacen realmente por TANGO LIGERO? ¿Qué pena, señores! No comprendo por qué no se rehabilita el antiguo TANGO GITANO y se suprime la denominación de TIENTOS. TANGUILLO: De Cádiz. Nada tiene que ver con el de Málaga. De tercios muy ligeros y de aire festero, se sale, desde mi punto de vista, fuera de la órbita flamenca para situarse en un plano altamente chuflanero y carnavalesco. El «Chato de las Ventas» nos legó varias grabaciones jocosas, con letras propias exquisitamente interpretadas. También Manolo Vargas grabó tanguillos con mucha sal. Para que puedan hacerse una idea del confusionismo que en la actualidad reina entre algunos cantaores profesionales relataré una anécdota que me sucedió en el mes de octubre de 1980. Con el deseo de\n\n[ENDING CONTEXT]\n\nveinte.\n\nDesde aquí, desde esta revista «CANDIL», formidable vehículo de comunicación entre los que nos interesamos por la parcela flamenca, les insto para que traten de resolver el problema que les dejo planteado, utilizando para ello mis fórmulas, si las consideran interesantes, o bien otras que ustedes, con más autoridad que yo, puedan ofrecer por considerarlas más apropiadas.\n\nTermino ofreciéndoles a continuación una relación de cantes grabados por TANGO GITANO, TIENTOS, TANGO LIGERO y TANGUILLO DE CADIZ, por si les interesa su localización.\n\nBar TOMAS\n\nEspecialidad en\n\nPLANCHA\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Arbol del tango gitano",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1297,
    "article_char_count_full": 7680,
    "article_char_count_review": 3339,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  }
]
```
