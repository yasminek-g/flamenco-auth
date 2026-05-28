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
    "article_id": "1997-03-33-right-carta-a-chano-lobato",
    "article_text_for_review": "$ M_{i} $ querido amigo Chano:\n\nDesde esta tierra mía que es Extremadura, a la que desgajaron hace algo más de dos siglos (el mismo tiempo que tiene el flamenco) de esa bendita Andalucía, porque, como sabrás, todo constituía el territorio de Al Andalus, me llegan voces de que la revista Candil va a editar un número extraordinario y monográfico en homenaje a tu persona y a tu arte. Creo que en nuestra querida España las cosas están cambiando, porque ya era hora de que los homenajes se den en vida y no, como tradicionalmente se ha venido haciendo, después de la muerte con el consabido soniquete de “¡Qué bueno era!”.\n\nHace ya bastantes años un amigo mío de Badajoz, nacido en Arcos de la Frontera (el doctor Antonio Sánchez), me puso sobre tu pista a través de un \"Cacharrito p'acá\" en el cual no me explicaba cómo una rumba (tan denostada) se podía hacer tan flamenca. Yo creo que desde entonces entronizaste la rumba en el Vaticano de lo flamenco.\n\nMás tarde, en el Concurso Nacional de Córdoba del año 1980, te conocí personalmente al contratarte como uno de los cantaores oficiales de dicho Concurso y en el que\n\nlas eliminatorias se celebraban en la Alcazaba de los Reyes Cristianos. Fueron jornadas inolvidables para mí como miembro del jurado, por el alto nivel de los concursantes al baile y sobre todo por su trayectoria profesional a lo largo de aquellas agotadoras jornadas de la que yo no sabía de dónde sacabas las fuerzas de tantas y tantas horas trabajando sin desmayo. Era curioso observar cómo el público estaba deseando que tú acompañases bien a un guitarrista o bien a una bailaora más que por calibrar su grado de maduración, por disfrutar con tu cante. Sin quererlo ni proponértelo te convertiste en el protagonista del escenario. ¿Cómo se puede cantar tan bien, tan a compás y tan en su sitio? Aunabas en tu decir dos cosas que siempre han sido distintas: el cante para bailar y para escuchar, es decir: cantar para atrás y para adelante.\n\nDe tal evento hice un estudio exhaustivo de lo que había sido el citado Concurso, presentándolo como ponencia al Congreso celebrado en aquella ocasión en Fuengirola y en el que clasificaba, a manera de estadística, todos los estilos y escuelas que se habían cantado a lo largo de todas las jornadas y lo mismo en lo relativo al toque y al baile, terminando mi estudio con un canto de exaltación a la organización y a la impecable actuación de Chano Lobato, expresando mi extrañezas de que por aquel entonces sólo era requerido para ser cantaor de atrás.\n\nMeses más tarde te llevé a la Asociación de Arte Flamenco de Badajoz y sin saber que yo era el autor del mencionado escrito me dijiste: \"Hay\n\nFoto: José Pamos\n\nun aficionado de aquí que ha hecho un trabajo sobre mí y quisiera agradecérselo, porque a partir de tal momento no me dejan de llamar para cantar p'alante\". Recuerdo el abrazo que me diste al señalarte que era yo.\n\nPara mí fue una enorme satisfacción y aunque el hecho puede parecer una pedantería por mi parte, lo hago porque tú eres el primero que lo comentas cuando la conversación viene a colación, en tus visitas a Extremadura.\n\nContigo, querido Chano, el cante y muy especialmente el de Cádiz, se engrandece reafirmando su calidad y abriendo nuevos caminos que no están reñidos con la tradición porque tú conviviste con Aurelio, Pericón, Manolo Vargas y tantos otros; y al darle tu impronta nueva los revitalizas en varios sentidos: en el eco de tu voz, en tu compás milimétrico que hace el sueño de cualquiera que alce los brazos para bailar, en tu estética de escenario que yo definiría como el compás que te sobra con la voz y que tienes que expulsar del cuerpo y que produce una mayor comunicación con los públicos y por último una generosidad a la hora de actuar fuera de toda duda, sin distinción de cualquier tipo de escenario.\n\nY si Cádiz fue la puerta principal de los denominados Cantes de Ida y Vuelta, naturalmente que no podían dejar de pasar por tu aduana artística dotándolos de nuevo aire, haciéndolos más a ritmo, oliendo algo más a Caribe en las guajiras y dándole una dosis de más unidades de flamenquismo en la rumba y haciendo un nuevo apartado de estos cantes, al incorporar, al ritmo de bulerías, melodías inolvidables del riquísimo folclore hispanoamericano. En definitiva: haciendo un apostolado flamenco de todos estos palos.\n\n¡Qué lejos quedan aquellas noches en Villa Rosa, esperando la tan anhelada fiesta en un cuarto para poder seguir viviendo y en donde mi paisano Manolo de Badajoz ejercía como hombre de confianza del establecimiento. Ahora el artista es considerado y respetado por un público que llena un teatro como tú bien dijiste en mi tierra con motivo de tu ilustración en sendas conferencias impartidas por Félix Grande y Angel Alvarez Caballero, respectivamente. Y todo eso se debe a artistas tan serios como tú, a pesar de tu repajolera e innata gracia.\n\nBueno, Chano, te voy a dejar porque no te quiero cansar más; pero te voy a decir una última cosa: En el mundo flamenco es más difícil que cantar, tocar y bailar bien, el hecho de que nadie hable mal de un artista. ¡Eso sí que es difícil!; pues amigo Chano, esa condición de que nadie ejerza ninguna crítica sobre tu persona, la tienes tú.\n\nUniéndome a este homenaje de la revista Candil, recibe un fuerte abrazo flamenco.\n\nBadajoz, 5 de marzo de 1997.",
    "title": "Carta a Chano Lobato",
    "periodical": "candil",
    "issue_id": "1997-03",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "33-34",
    "page_number": 33,
    "word_count": 935,
    "article_char_count_full": 5338,
    "article_char_count_review": 5338,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-03-34-right-chano-lobato-un-cantaor-gaditano",
    "article_text_for_review": "Para mí, Chano Lobato es Cádiz. Todo el cante de Cádiz. Todo el sentir y vivir flamenco de Cádiz. Hoy él es lo que ayer fueran Aurelio y Manolo Vargas: el depositario de la más antigua cultura festera y cantaora del mundo. En su voz, típicamente gaditana, hecha de sol y sal, brisas y oleajes marineros, resuenan los ecos de Paquirri el Guanté y de Enrique el Mellizo.\n\nChano encarna el vitalismo, la gracia irresistible y el magisterio del ritmo. Chano es dueño y señor del compás.\n\nSu cante es puro, es decir, sincero, sentido, pero también creativo, nuevo. Pura ortodoxia y descarada heterodoxia. Ha heredado la musicalidad y la creatividad de los grandes maestros de la escuela de Cádiz y, por eso, su cante surge alimentado por la savia que le aportan unas raíces que se hunden en la esencia misma de la gaditanía. Pero Chano es imaginativo y genial. Juguetón y caprichoso. Y, porque así se lo pedía el cuerpo, ha sabido fundir aires tropicales, melodías rioplatenses, con compases jondos y hacer de ello un todo rebosante de flamencura.\n\nChano Lobato es cantaor de alante y cantaor de atrás. Cantaor completo. Tiene la sencillez, la humildad, de quienes aman más el cante que a sí mismos. Por eso, ha sabido durante tantos años dialogar con el baile, inspirar y llevar atado a sus duendes el más universal de nuestros bailadores: Antonio Ruiz Soler, “Antonio”.\n\nChano, además de buen profesional, responsable, respetuoso del público, nervioso siempre por dar de sí todo lo que los aficionados esperan de él, es un aficionado cabal y un estudioso del cante. Despierta, lúcido, está siempre dispuesto a aprender cante y de cante, porque sólo los que mucho saben, valoran los saberes y siguen siempre aprendiendo. Y Chano es sabio.\n\nChano Lobato es, en fin, porque siempre que ha habido ocasión de demostrarlo, lo ha demostrado, un amigo, un hombre que abre su corazón sin reservas ni intereses calculados. Sirvan, pues, estas líneas para rendirle, desde la admiración y la amistad, ese sincero y sentido homenaje que, por tantas razones, tan merecido tiene.",
    "title": "Chano Lobato, un cantaor gaditano",
    "periodical": "candil",
    "issue_id": "1997-03",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "34-34",
    "page_number": 34,
    "word_count": 349,
    "article_char_count_full": 2061,
    "article_char_count_review": 2061,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-03-35-left-mis-diversiones-con-chano",
    "article_text_for_review": "Casi siempre rehusamos la pertinente invitación de que somos objeto para colaborar en este tipo de monográficos por temor a caer e incurrir en la reincidencia y reiteración de datos, pero la verdad, la única verdad, es que en esta ocasión no hemos sido capaces de negarnos al tratarse de Chano, persona a la que admiro como a tal y artista al que profeso gran respeto y afecto entra- ñable por su enorme y reconocida calidad humana. Manuel Ríos Vargas\n\nBañado por la sal milenaria del mar a orillas de La Caleta, seguramente bautizado con manzanilla, como dice la copla, sevillano por querencia y adopción, andaluz universal de rancia estirpe, dotado de gracia sin igual se nos antoja este Chano Lobato al que con buen acierto y mejor criterio vamos a dedicarle, de manera más que merecida por su trayectoria y su hombría de bien, este número de la prestigiosa revista Candil.\n\nMuchos, muchísimos momentos felices nos ha deparado este buen cantaor y nosotros vamos a entresacar algunos, pues sería interminable tratar de recoger y recordar tantos como podríamos efectuar.\n\nNo recordamos la fecha con exactitud, pero creemos que sería recién convaleciente de una intervención quirúrgica cuando nos deleitamos con su cante y con el embrujo guitarrístico de Manuel de Palma, teniendo lugar dicho acontecimiento en la Peña Flamenca La Soleá en la localidad cordobesa de Palma del Río.\n\nOtro de los momentos que recordamos con sumo placer fue en el gaditano Teatro Manuel de Falla, en el año 1985, con motivo de efectuarse la presentación del disco de Los cantes de ida y vuelta, donde a través de nuestra amistad con el siempre recordado Paco Vallecillo tuvimos la oportunidad de acceder a los distintos camerinos, mostrándo-senos el bueno de Chano con una gracia y una repentización inusitada y habitual en él.\n\nLleno de humanidad y ternura y pleno de flamenquería se nos mostró en el Salón Real del sevillano Hotel Alfonso XIII con motivo de la recogida de El Compás del Cante, teniendo lugar dicho acto en el año 1986.\n\nEn el festival Joaquín el de La Paula de mi entrañable y patria chica Alcalá de Guadaira, celebrado en la plaza de toros, nos embelesamos con su bien hacer y su enorme profesionalidad, demostrándonos su valía, sapiencia y conocimientos.\n\nCon sumo placer recordamos su actuación en 1986, en la palaciega Peña Flamenca El Pozo de las Penas, con motivo de la semana cultural en homenaje a la buena bailaora Pepa Montes, donde estuvo magníficamente vehiculado por la rancia y sabía guitarra de Paco del Gastor y donde ambos tuvieron una compenetración perfecta, interpretando una larga serie de tangos argentinos que ya hubiese querido Carlos Gardel haberse asemejado.\n\nAsimismo, le recordamos en una conferencia que tuvimos ocasión de pronunciar en la localidad sevillana de Gines en el año 1988, donde versamos sobre los estilos gaditanos y donde este buen cantaor gaditano estuvo acompañado por la guitarra de su hijo, demostrándonos una vez más su maestría en dichas formas interpretativas.\n\nEn la Peña Flamenca Niño del Mauro, de Alcalá de Guadaira, muy recientemente, concretamente el pasado año, tuvimos también la ocasión de escucharle de forma magistral, donde estuvo acompañado por la guitarra de su hijo y donde comunicó perfectamente con los muchísimos aficionados asistentes.\n\nPodríamos así continuar enumerando un sinfín de momentos vivenciales, pero queremos terminar, amigo Chano, rogándole a un Devel te conserve durante muchos años en tu sevillana casa de la calle Ganso para felicidad propia, de los tuyos y de los que nos consideramos amigos, sobre todo para solaz y deleite de los que bien y tanto te queremos.",
    "title": "Mis diversiones con Chano",
    "periodical": "candil",
    "issue_id": "1997-03",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "35-35",
    "page_number": 35,
    "word_count": 601,
    "article_char_count_full": 3646,
    "article_char_count_review": 3646,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-03-36-left-el-comp-s-brujo",
    "article_text_for_review": "Hace años. Yo admiraba a Chano Lobato no precisamente por su cante por soleá que no acabó de convencerme, sino por su sentido del compás. Fui adentrándome en sus grabaciones y convenciéndome de que nadie, nunca, había manifestado su cante con tanto sentido de la medida, del ritmo; en definitiva: me identifiqué con su salero y compás.\n\nEn Montefrío, donde las noches del estío son frescas, en una plaza abarrotada de gente, hace años lo saludé. Nos presentó el gran Manolo Avila al que la noche flamenca se dedicaba. Chano estaba mal. Venía convaleciente y no lo vi con fuerza. Delicado de salud, pero humano, simpático, amable, artista, con más mundo y más saber estar que todos los que había conocido hasta entonces. El, seguro que ya no se acuerda de mí. Pero en el escenario, a medio gas, acariciando con su cante el compás de la guitarra, hizo, para siempre, la exacta medida de cómo debe ser un cantaor profesional. Confieso que el mundo se me comenzó a mostrar con todas sus contradicciones. Yo estaba ya un poco cansado de oír que los payos no manejan el compás como lo hacen los gitanos. Pero, quizá, de ignorancia. Pero creo que Chano Lobato no es gitano. La escepción que confirma la regla? Quizá. Pero Chano es otrá cosa: Chano es uno de esos cantaores que llegan a legendarios con una larguísima etapa de vivir en los tablaos, de cantaor de atrás, donde está la escuela del compás y de la reiteración. Por eso, esa mágica noche de Montefrío en el verano del 85 (me rectifique alguien si me equivoco, por favor) me quedé “engaçado” a Chano Lobato.\n\nEn Chiclana, estando grabando el disco con los cantaores punteros\n\nJosé Guardia\n\nde Granada, me encontré con una grabación de Chano con Manolo Domínguez. Había producido Rafael Izquierdo y grabado en sus estudios. Manolo Domínguez punteaba y acompañaba con una musicalidad espectacular, respetando el texto y el son de las canciones más populares de un lado y otro del Atlántico: Cacharrito, Noche de ronda, Llorona, Volver, volver, Triana, El arriero (de A. Yupanki) y los Tanguillos de Cádiz. Una grabación aderezada a veces con un organo eléctrico que, a mi entender desvirtuaba el flamenquismo con que Chano Lobato y Manolo Domínguez impregnaban en cada canción cante. Tan presente se me quedaron aquellas \"innovaciones\"?, que quise rematar el curso que daba en la Universidad deseando a todo el alumnado buen provecho de lo estudiado y deseándole paz y felicidad. Para ello no se me ocurrió otra cosa que terminar con un homenaje a las formas cantaoras de Chano Lobato, con sólo la primera estrofa de la célebre canción \"Ansiedad\", que había grabado como final. Y la cosa fue una gozada...\n\nMucho tiempo ya de lo que cuento que no ha hecho más que acrecentar la seguridad, la profesionalidad, la sencillez, la extroversión del arte que lleva dentro Chano Lobato.\n\nPoco se ha prodigado por Granada, pero no hay año que no vuelva varias veces a estas tierras. Chano Lobato es la expresión viva hoy de lo que representa ser un maestro de nuestro arte. Tiene la edad necesaria para eso; tiene toda una vida dedicada al flamenco; tiene el respeto de unos y de los otros. Y no es una voz fresca, ni grata, ni dulce, a veces hasta hiere: Pero es un corazón que brota por la garganta a tiempo, con gracia, con garbo, con medida, con ritmo y, sobre todo: con compás. ¡Qué bien se mide Chano Lobato! ¡Y qué bien canta!\n\nSi no es una reliquia hoy del flamenco de los últimos cincuenta años, que vive y que puede decir mucho cante y mucha historia, si esto no es ser un verdadero maestro del flamenco —bien, Chano—, que venga Dios y lo vea.",
    "title": "Chano Lobato: El compás brujo",
    "periodical": "candil",
    "issue_id": "1997-03",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "36-36",
    "page_number": 36,
    "word_count": 632,
    "article_char_count_full": 3595,
    "article_char_count_review": 3595,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-03-37-left-chano-labato-cantaor-singular",
    "article_text_for_review": "De unos años para acá Chano Lobato despierta un inusitado interés, incluso pasiones, entre los aficionados. Lógico. En él coinciden en hermosa armonía una voz flamenca y gustosa, un compás endiablado y juguetón, una simpatía desbordante y comunicativa, una maestría sabia y sobrada. Chano es artista con saberes e indiscutible personalidad. Pellizca, redondea los tercios y los lleva maestramente a su sitio, con donosura y gracia. Sabe establecer una corriente de comunicación con el público como pocos otros artistas. Sabe contagiar al respetable, mantenerle al sonío del cante y agarrarlo con fuerza para que nadie se escape. Sabe engatusar amablemente al público y distraerlo con sus felices ocurrencias, dominarlo, en suma, para conseguir las atenciones y el silencio, y después, el aplauso entregado, agradecido, cómplice.\n\nLo he podido comprobar con frecuencia en los últimos tiempos. Una de las antepenúltimas ocasiones fue\n\nen la undécima edición del Festival Flamenco de Cornellá, octubre del 94, que se dedicaba en homenaje a la gran maestra del bàile doña Pilar López. Compartían cartel con él Rancapino, Pedro Bacán, José Joaquín y Niño de Pura. En el patio de butacas, además de la homenajeada, otros espectadores de excepción asistían al festival, las bailaoras y profesoras Ana Márquez y Paca García, los periodistas y críticos Ramón Rodó, Diego Anguita, Rafael Mora-\n\nles y Carlos Murias, el bailarín y excepcional concertista de castanuelas José de Udaeta, el bolerista Moncho, el ilustre bailaor y profesor José de la Vega y el genial Antonio, en una de sus últimas salidas públicas.\n\nLa aparición en escena de Chano Lobato fue acogida con cálidos aplausos. Su actuación -lo he dejado dicho en mi obra Aquí, un año de flamenco en Cataluña- estuvo sembrada de sabor y saber. Genuino representante de la mejor escuela del cante gaditano, Chano atesora toda la riqueza de matices que caracteriza a los aires rítmicos de la bahía. Su cante es todo ritmo y sentimiento, contiene toda la gracia y el garbo de la salada claridad gaditana, a la par que una jondura consustancial que nos alborota los adentros. Por soleá nos ofreció un cante de rancio sabor y enduendado, pero por alegrías, tangos y bulerías fue el desiderátum. No se puede cantar más a compás, con mayor gusto y gracia. De ahí que los oles se sucedieran ininte-\n\nrrumpidamente y que el público vi- brara con su magistral forma de de- cir el cante.\n\nEsa actuación, ese éxito, pudieron haber tenido lugar en cualquier otro marco y población; de hecho, menudean. Así aconteció también en mayo del año siguiente en el Festival de Cerdanyola. Y es que Chano está en el momento más álgido de su ya larga y fructífera trayectoria artística. Y es que es sorprendente su dominio de las más variadas cantiñas; de los estilos indianos: guajiras, colombianas, rumbas...; de las bulerías pellizqueras y recogías; de los gadanos tanguillos pícaros; de las añejas soleares... Y es que en el cante de Chano hay mucho de sol de patio andaluz, mucho de luminosidad de mar en día claro, mucho de brillo de cielo al mediodía, mucho de gusto de pan candeal.\n\nChano, como el torero en el ruedo, cita el cante, acude a él y le da lances de repajolera gracia, con sabor y saber. En su voz desgarrada, fosca, quebrada y refulgente, que todos los adjetivos le son aplicables, suena siempre personal, eterno y renovado. Juega, juguetea y requejuguetea con el compás. Rompe el molde del cante para modelarlo de nuevo. Tanto da que la letra sea venerable y clásica como nueva. Cual-quier copla él la puede decir a compás, la doma, la ciña y la cuadra. No le sobra una sílaba, no le falta una nota. Sin perder nunca las lindes del cante, sabe traspasarlas y seguir siendo genuino y verdadero por los atributos de su sapiencia, de su maestría.\n\nSabe, en fin, Chano dar un recio sabor flamenco, jonda vibración, emotividad, galanura y compás magistral a los cantes, tanto atrás como alante. Ni más ni menos. Es Chano Lobato, no cabe duda, un cantaor singular.",
    "title": "Chano Lobato, cantaor singular",
    "periodical": "candil",
    "issue_id": "1997-03",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "37-38",
    "page_number": 37,
    "word_count": 673,
    "article_char_count_full": 4005,
    "article_char_count_review": 4005,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
