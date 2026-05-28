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
    "article_id": "1982-05-4-right-las-malague-as-de-juan-breva",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPARECE natural y lógico que uno de los empeños más obligados de la flamencología sea clasificar y poner nombre a las formas flamencas; pero con frecuencia sucede que flamenco y flamencología no se entienden porque no sólo son de natura-leza distinta sus sujetos sino que ésta quiere ejercer su dictadura sobre aquel, olvidando que la verdadera ciencia ha de partir de la propia conciencia de humildad y aceptación del fenómeno tal y como es; llegando a sus conclusiones después de la observación del fenómeno natural y nunca tratar de arreglar la naturaleza de las cosas a las elucubraciones teóricas del científico. Otra cosa bien distinta, aunque vicio igualmente de la flamencología, es que demasiado frecuentemente demuestre o explique sus conclusiones por el consabido método de «esto es así\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"flamencólogos\"]\n\ncomo es; llegando a sus conclusiones después de la observación del fenómeno natural y nunca tratar de arreglar la naturaleza de las cosas a las elucubraciones teóricas del científico. Otra cosa bien distinta, aunque vicio igualmente de la flamencología, es que demasiado frecuentemente demuestre o explique sus conclusiones por el consabido método de «esto es así porque lo digo yo». Esto es lo que hace que muchos sientan vergüenza de ser llamados flamencólogos (que hasta ahora sólo se había considerado la vanidad que para otros supone que le llamen así), independientemente de lo gruesa, pretenciosa, remilgada, empa honada y farolera que pueda resultar la palabra. Pero, bueno, ya viene por ahí la polémica. Ahí es nada la que me organizaron cuando en 1967 rechacé del cartel la palabra «flamencólogo» con la que me obsequiaba al anunciarme la organización como conferenciante ilustrado por Fosforito y Paco\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\ntificado por CANDIL. Me preocupa que siempre se haya hablado de las «malagueñas de Juan Breva» y que ahora digan teóricos que Juan Breva nunca cantó malageñas; sin molestarse siquiera («esto es así porque lo digo yo») en argumentar que el nombre de «malagueñas» ha cambiado, flamencamente, de significado, como es cosa normal que su ceda con muchos vocablos del Castellano. (Ya se sabe: la palabra «retrete», por ejemplo, significaba antigua-mente «lugar apartado para las damas». A ese significado antiguo le sobrevino un envilecimiento hasta llegar al significado de hoy). En el proceso normal de evolución de un idioma —mucho más en las palabras de argot— se producen estos cambios de significación; pero un verdadero lingüista, como un verdadero flamencólogo, antes de negar el significado primero, debe de explicar las razones de su cambio. Jamás se han preocupado esos teóricos de explicar por qué los cantes de Juan Breva son llamados «malagueñas de Juan Breva». (Y no ya en las etiquetas, sino en el decir común de las gentes flamencas de su tiempo). Ellos saben que sobrevino después una estilística de artistas personales a cuyas formas se les llamó malagueñas, graninas, cartageneras, murcianas, en contraste con las formas que anteriormente obstentaban tales clasificaciones —«las malagueñas nuevas»...— y para evitar confusiones niegan el viejo significado. ¿No caen en la cuenta de que crean una confusión mayor? ¿Cuantos hay que se forman un lío o se escandalizan, porque no está en sus manos advertir que se trata de conceptos distintos con la misma palabra «malagueñas», antiguo y viejo, y que, efectivamente, Jua\n\n[ENDING CONTEXT]\n\ny yo ni siquiera soy académico.\n\nAGUSTIN GOMEZ\n\nMedalla de Plata en el X Salón Internacional de Bruselas\n\nFabricación de toda clase de plantillas ortopédicas en conglomerado de caucho y corcho, con extensa gama de piezas accesorias para confeccionar y adaptar a las mismas. (Arcos internos o longitudinales. Arcos transversos. Cuñas pronadoras y supinadoras. Herraduras, etc.)\n\nLas plantillas y piezas accesorias, se hacen en tres consistencias: BLANDAS, DURAS Y SEMIDURAS. También fabricamos según diseño Técnico.\n\nFábrica y oficinas: Arrastradero, 6 y 8 - Teléfonos 22 33 92 y 22 51 12 - J A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«Las Malagueñas de Juan Breva»",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 995,
    "article_char_count_full": 6121,
    "article_char_count_review": 4240,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "flamencólogos"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  },
  {
    "article_id": "1982-05-5-right-el-flamenco-como-medio-de-promoc",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE N las dos o tres últimas décadas se ha producido un profundo cambio en la apreciación del flamenco por parte de la sociedad o, al menos, por un importante sector de ésta. En efecto, aunque todavía persiste entre las personas más alejadas de los movimientos culturales el estereotipo del flamenco como cosa de gentes poco recomendables o como un producto mitificado y mixtificado para turistas, estas concepciones están ya muy lejos de ser generales y hoy, en los medios culturales tanto oficiales como privados, progresistas o castizos, se siente un interés muy grande hacia el flamenco, interés que quizá sea desmesurado pues posiblemente se ha pasado, sin solución de continuidad, del desprecio más absoluto a una exagerada sobreestima olvidando los años de olvido y desidia. Resultado de ese\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"serio\"]\n\nenco, interés que quizá sea desmesurado pues posiblemente se ha pasado, sin solución de continuidad, del desprecio más absoluto a una exagerada sobreestima olvidando los años de olvido y desidia. Resultado de ese abandono es el hecho de que junto con la avidez por todo lo flamenco camine un desconocimiento bastante generalizado, adobado con la profunda carencia de documentos en los que basar un conocimiento o estudio que pretenda ser mínimamente serio. Y sobre esta falta de base científica se quiere cimentar una pretendida ciencia a la que González Climent puso nombre: nada menos que Flamencología, que por no tener, ni siquiera dispone de una taxonomía elementalmente sistemática y más o menos generalmente admitida. Al revés de lo que ocurre con cualquier tipo de conocimiento, aquí es la «ciencia» la que crea al «científico»: el flamencólogo. Así, en este delicuescente caldo de cultivo, florece un nuevo modo de promoción social. Ante la falta de auténticas autoridades en la materia, cualquier aficionado de anteayer, pontifica ante un grupo de amigos de su peña. Con un poco de suerte y osadía, trasciende hasta los medios de comunicación sociales de su localidad y hasta quieren incluirlo entre sus actividades y recurren a lo que tienen a mano con tanta urgencia como poca selectividad. ESBOZO DEL PROMOCIONADO TIPO El sedicente flamencólogo se encuentra descubriendo el camino que conduce a la cultura del ocio. Superadas las necesidades vitales —y\n\n[ENDING CONTEXT]\n\nde nadie del desconocimiento, es posible medrar con poco esfuerzo.\n\nEn tercer lugar, el flamenco aparece como un filón no demasiado explotado donde la competencia es escasa y por tanto la probabilidad de encumbrarse es mayor que, digamos, en el fútbol o en otras actividades que, aun no requiriendo excesivos conocimientos, resultan más azarosas. Por último, el flamenco ofrece posibilidades de actuación dentro del medio cultural que, afortunadamente, goza de una atención y un interés crecientes en nuestros días.\n\nEQUIPO ALFREDO\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Flamenco como medio de promoción social",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 1124,
    "article_char_count_full": 6995,
    "article_char_count_review": 3087,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "serio"
      }
    ]
  },
  {
    "article_id": "1982-05-6-right-el-popularisimo-gramofono-flamen",
    "article_text_for_review": "Por Manuel Urbano\n\nJ UAN Ramón Jiménez, en un breve y hondísimo re- lato, «Trascádiz», nos de- jaría aduendada noticia de él:\n\n«Un gramófono flamenco que desgañita su ay en no sé qué puerta hacia adelante, se sale por detrás y llena el mar desierto, que se va intensando, de sus palabras aullantes, mayores» (1).\n\nEl inenarrable universo del ayeo inundado de la palabra más intensa, el quejo que anuncia espantado la salida del cante, el derrame lacerado y trágico que se eleva dolorido de presencias cuando mueren todas las músicas del mundo, a pesar de estar oprimido, «en lata», conserva sus desgarrados remolinos de música y, como viera el poeta de Moguer, llena el mar desgañitando su ay.\n\nDel primer tercio de este siglo la estampa; la pobreza, también en ella, es el soporte del cante.\n\nLa inversión total en este tablao ambulante, mecánico y proletario, eran las cuatro pesetas diarias que había que adelantar por el alquiler de tan extraño artefacto, que arrancaba sonios negros de las negras estrías de la pizarra. El trabajo fácil y orillándo la mendicidad: «¿Hay algo pá la máquina?». De esquina en esquina encalada por los barrios flamencos, a las puertas de las barberías —el mejor lugar—, de las tabernas, pidiendo la voluntad: «¿No hay ná pá la máquina?»... y las enardecidas sevillanas corraleras de Pepe el Limpio, la ya aterciope-\n\nlada voz de falsete de Chacón, el poderío de Manuel Torre o el fandango de moda, se desgranaban suplicantes con extraños matices por las placas.\n\n¿Cómo llamarían a estos costaleros de la copla? Catorce, dicen, habían entre Sevilla y Triana y, otros tantos, en Cádiz. ¿Cómo sentirían estos hombres el cante? El perrito blanco del tecnificado pseudo organillero flamenco, escucha, asombrado lazarillo, el sombrío destrozo andaluz de «la voz de su amo». ¿Por qué aristas, por qué surcos, discurre desangrándose el auténtico compás de las penas?\n\nAlguna moneda de cobre y con la música, con el grito, con la queja, con las siguirias y las sevillanas a otra parte: la bocina, como una lámpara votiva, en la mano y la caja, en improvisado trono sobre la miseria del hombro, haciendo padecer con todo el peso inconmensurable de la jondura inmisericorde del cante.\n\nHoy, medio siglo largo después, un gramófono flamenco sigue desgañitándose, rozado, con el ¡ay!.",
    "title": "El popularismo gramófono Flamenco",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "6-6",
    "page_number": 6,
    "word_count": 384,
    "article_char_count_full": 2305,
    "article_char_count_review": 2305,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-05-7-left-una-sensible-perdida-para-el-can",
    "article_text_for_review": "E N Murcia, donde residía ya jubilado de sus quehaceres en el campo de la pedagogía, ha fallecido a principios de abril el gran aficionado y eminente investigador del flamenco don Augusto Butler y Genis, que en numerosas publicaciones, conferencias y actos culturales de muy variada naturaleza, popularizó el seudónimo de Máximo Andaluz.\n\nAugusto Butler debió nacer a principios de siglo. Gaditano, de la propia Gades, tuvo la fortuna de formarse como aficionado «de los de entonces» en la meca jerezana. Excelente escritor, vivió el Cante y sus aledafios (con la intensidad con que entonces podía vivirse y empaparse en él) desde los años veinte hasta alrededor de la década de los setenta en la que desde su Cádiz natal, como profesor de la Escuela de Comercio, fue trasladado a Las Palmas, para acabar allí su carrera profesional. Decimos que vivió intensamente el Cante porque estuvo inmerso en el genuino ambiente de las reuniones de aquella época, fue amigo de viejos profesionales de los que supo recoger datos, referencias y, en suma, vivencias del mundo variopinto del flamenco en su pura e incluso prístima entidad. Una prueba evidente y luminosa de cómo captó el cúmulo de la historia y las historias del flamenco está en su libro «Javier Molina, jerezano y tocaor», en el que tantos hemos bebido un aspecto tan fundamental de la vida del legendario maestro jerezano, de su relación con Chacón y de una época tan significante en el flamenco de Jerez. De Chacón fue Máximo Andaluz expresivo y fiel vasallo como tantos jerezanos lo fueron y otro tanto puede decirse de Manuel. También dejó una muy interesante bibliografía, como «Romancero del Cante», «Jerez en la canción popular andaluz», «La canción Andaluz», por citar algunas de las que conocemos.\n\nHace unos años tuve la dicha de reunirme con don Augusto en Jerez. Vivimos, solos él y yo, una jornada inolvidable, nos empapamos de jerecismo y tras almorzar en una Venta a la sazón muy conocida, nos paseamos por muchos lugares que a nuestro admirado amigo le trajeron la emoción y la nostalgia del recuerdo que, para tantos aficionados, es una forma medular de sentir y de existir. Paramos para tomar café en una plaza cerca de donde el señor Juan Junquera tuvo su famoso Café Vera Cruz. Pasamos revista a una serie de conceptos, compartimos aficiones y evocamos no pocos pasajes de nuestras propias experiencias, de la suya sobre todo. Antes y después de este encuentro, pero sobre todo antes, sostuve con don Augusto una continuada correspondencia que en tantas ocasiones fue para mí fuente abundante de conocimientos, sin duda no suficientemente aprovechados y no precisamente por culpa suya. Obtuve de él varias colaboraciones para mi desaparecido «Flamenco». Poseía un envidiable archivo del que a mis manos llegó una reducida parte, si bien tuve la satisfacción de contribuir para que lo fundamental fuese a parar a las manos, cuidadosas y pulcras, de otro eminente investigador, mi estimado amigo Carlos Almendros.\n\nPocos flamencos van quedando —si es que queda alguno todavía— de la talla de don Augusto Butler y Genis: inexorable ley de vida, cuyo acatamiento tanta congoja produce. Aquí queda el testimonio de nuestro pesar que estamos seguros que compartirán quienes se vieron honrados con la amistad del desaparecido maestro gaditano y de quienes conocen su obra y su vida flamenca. Testimonio que renovamos a sus familiares y especialmente a sus hijos.\n\nFRANCISCO VALLECILLO\n\n«CANDIL» se complace en anunciar a sus lectores y amigos, a la vez que se felicita, la reedición de «LA SOLERA FINA», el libro de cantes de don ANTONIO ALCALA VENCESLADA, que tuvo su primera aparición impresa en 1925, y que hoy es el número dos de la colección de libros «CANDIL».\n\nPara pedidos, dirigirse a la administración de esta revista: Apartado de Correos, número 510, Jaén.",
    "title": "Una sensible pérdida para el Cante Flamenco",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 634,
    "article_char_count_full": 3835,
    "article_char_count_review": 3835,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-05-7-right-tambien-ellos-fueron-importantes",
    "article_text_for_review": "L OS guitarristas que figuran en nuestra nómina desde primeros de siglo, merecen todo respeto y, desde luego, más alta consideración que la que se le viene dando en escritos, libros y conferencias. La guitarra flamenca reclama todo un serio análisis, imposible realizar sin una atención rigurosa a estos artistas que rellenaron una página importante e insosla-yable del arte flamenco. Quede de ellos una relación nominal de quienes grabaron en placas antiguas, significando el cantaor que acompañaron con más frecuencia. Puede que esta nómina no sea total, faltando en ella algún guitarrista. Los guitarristas más antiguos de los que se conservan grabaciones son: Angel de Baeza, Román García, Enrique Molina y Javier Molina Cundi. Los que más grabaron: Ramón Montoya, Manolo de Badajoz, Niño Ricardo y Miguel Borrull, hijo; entre estos, como comprobará el lector, resulta difícil concretar el cantaor a que más acompañaron.\n\nQuede, pues, esta nómina que ofrezco a los más jóvenes aficionados.",
    "title": "También ellos fueron importantes",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 155,
    "article_char_count_full": 993,
    "article_char_count_review": 993,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
