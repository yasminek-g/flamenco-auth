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
    "article_id": "1982-07-29-left-quienes-fueron-los-maestros",
    "article_text_for_review": "DIEGO «EL LEBRIJANO»\n\nDiego Sebastián Fernández Flores, «El Lebrijano», nació —según Antonio Murciano, sobre lo que mantienen dudas Antonio Mairena y Ricardo Molina— en Lebrija, el miércoles 21 de abril de 1847, falleciendo en Sevilla en la primera década de este siglo. Gitano, al que se le describe como corpulento, se dedicó durante su juventud a faenas agrícolas hasta su temprana consagración como artista flamenco en los cafés cantantes del último tercio de siglo de mayor prestigio, como el sevillano de la calle del Rosario, El de Silverio.\n\nDe voz afillá, se le considera como una de las figuras más tempranas y grandes del flamenco de Lebrija. Uno de los grandes intérpretes históricos de siguiriγas, dentro del grupo que Mairena y Molina estiman como primeros seguidores de El Fillo, y de la escuela de Silverio y El Nítri. Junto a las siguiriγas, destacaron sus cantes por tonás, debla, cañas y soleares; brillando con luz propia en lo que se denomina como «martinete redoblao».\n\nDe él se cuenta que afirmara antes que Manuel Torre: «Señores, el día que yo canto con duende no hay quien pueda conmigo».\n\nBERNARDO «EL DE LOS LOBITOS»\n\nBernardo Alvarez Pérez nació en la cantaorísima Alcalá de Guadaira en 1886, trasladándose a Sevilla junto a su familia en 1891, debutando en el café cantante de Novedades con el nombre artístico de Niño de Alcalá, y alcanzando un éxito tal, que fue contratado en el mismo café para actuar en Madrid, donde le apodan con el de los lobitos, ya que cantaba por bulerías cierta copla que hubiera oído a un montañés:\n\nAnoche soñaba yo\n\nque los lobitos me comían...\n\nFalleció en Madrid el 30 de noviembre de 1969.\n\nBernardo fue durante muchos años una de las primeras figuras de los cafés de cante madrileños y de los principales escenarios de España. Ejemplo vivo en su especialidad para muchos artistas —malagueñas de El Canario y Gayarrito—, y gran intérprete de los cantes de Levante, de trilla —primer cantaor que los grabara— y nanas; sin que por ello puedan olvidarse sus siguiriyes, bulerías y soleares, de incuestionable categoría, hasta el punto de que de él ha dicho la crítica: «Era la ternura del cante, el Azorín de la copla flamenca. Cantaba con la delicadeza de un pájaro y con el sentimiento de una alma en pena. Ni más ni menos: un maestro».\n\nSu carrera artística estuvo jalonada de premios y galardones, destacando el de la Academia del disco de Francia, por la colectiva Antología de Hispavox de 1950.\n\nAPERITIVOS SELECTOS Especialidad en\n\nPLANCHA\n\nMesones, 18 Teléf. 23 40 46\n\nJ A E N",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1982-07",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "29-29",
    "page_number": 29,
    "word_count": 434,
    "article_char_count_full": 2545,
    "article_char_count_review": 2545,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-3-right-editorial",
    "article_text_for_review": "Editorial\n\nEl rigor en pro de la defensa y pureza del flamenco, que nos impusiéramos desde la aparición de «CANDIL», reclamaba con evidente justicia un número monográfico dedicado a uno de los artistas flamencos más grandes de la historia cantaora, a la vez que egregio cuidador de las limpias esencias jondas, Antonio Mairena. Hoy con motivo de la celebración en Jaén del X Congreso Nacional de Actividades flamencas, lo que era evidente laguna queda plasmado en hermosa realidad.\n\nEstas páginas de «CANDIL» pretenden ser un expreso reconocimiento al redondo cantaor hispalense, reconocimiento repleto de gratitud por su inmaculada dedicación a este arte, reconocimiento a su grandeza cantaora. Un reconocimiento que no nos atrevemos a llamar homenaje porque Antonio Cruz se merece más, ciertamente mucho más, que lo que este número de «CANDIL» pueda contener.\n\nPero no estaba en nuestro ánimo realizar un selector florilegio en torno al jondísimo arte de Mairena, maestro y entrañable amigo, nuestra intención radicaba en ofrecer un trabajo estudioso y que redondease, en la medida de lo posible, su copiosísima bibliografía, sin lugar a dudas la más abundante de la historia del cante; un número estudioso y crítico en sus más nobles acepciones intelectuales y que estuviese repleto de hondura y jondura, cualidades estas sin las que es imposible entender el cante, ni nada con él relacionado. Sinceramente, creemos que nuestras aspiraciones se han visto cumplidas en buena parte, aunque somos conscientes de que la figura del maestro de los Alcores no queda agotada, ni mucho menos, con este número de «CANDIL», tan repleto de investigaciones como de las íntimas, nobles e indestructibles vivencias flamencas, ese terremoto de «razones incorpóreas», esa catarata humana indefinible que inunda, alimentándola, la más bella y lacerada de las músicas andaluzas y que aquí tuvieron su origen en la garganta mágica, en el corazón inmenso, en el más puro quejó y queja gitana, en el alma andalucísima y en la memoria morena de uno de los más altos hombres del sur: Antonio Mairena.\n\nFinalmente y una vez más, «CANDIL» tiene obligación de dejar nítida constancia de gratitud hacia sus colaboradores, muchos de ellos anónimos y sin cuya generosidad no hubiese sido posible este número. Su mención sería amplia y prolija; sabemos por su amistad y desinterés nos perdonarán que omitamos sus nombres; no obstante queden los de Francisco Vallecillo y el del propio Mairena, que pusieron totalmente abiertos y a nuestra disposición sus excelentes archivos.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 404,
    "article_char_count_full": 2547,
    "article_char_count_review": 2547,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-4-right-antonio-mairena",
    "article_text_for_review": "Padre Pedro Ayala, 12 Teléfono 25 85 28 SEVILLA - 5\n\nSevilla, julio de 1982\n\nCARTA ABIERTA A DON MANUEL URBANO\n\nSR. D. MANUEL URBANO. Director de la Revista Flamenca CANDIL, de la Peña Flamenca de la Ciudad de JAEN.\n\nEstimado amigo:\n\nEstando en mi conocimiento una muy agradable sorpresa de que la Revista Flamenca de Jaén CANDIL, a través de su digna dirección y de su gran consejo de redacción, han llegado a culminar, con sus grandes esfuerzos, al lanzamiento de un gran número monográfico dedicado y en homenaje a mi persona artística y humana, coincidiendo con la celebración en la bellísima y hermosa Ciudad de Jaén del Congreso de Actividades Flamencas y que, en su contenido, deseo de todo corazón lleve implícito mi profundo agradecimiento para Usted, para todos los que con Usted colaboran de una forma o de otra, así como para toda la hospitalaria y muy flamenca Ciudad de Jaén y su gran Peña Flamenca, y deciros que con vuestras normas trazadas y ejecutadas, como ésta de CANDIL, es como se dignifica y se hace cultura dentro de un mundo que tan precisado está de ese elemento.\n\nEste inconmensurable número de la Revista Flamenca CANDIL no ha de caer en el vacío, llegará a todos los rincones del flamenco, hasta donde está lo más difícil de éste mundo, pero cumpirá con su cometido inexorablemente, su cometido de la sustanciosa siembra de la Cultura Flamenca.\n\nLos puntos oscuros, que el mundo del Flamenco viene soportando desde hace mucho tiempo, es preciso combatirlos con las mejores armas y yo pienso que las mejores son ir sembrando cada día más Cultura Flamenca, decir cada día más la Verdad en donde está en éste Arte Flamenco y tan andaluz y que, cada día, esa Cultura esté más al alcance de este mundo que, por fin y afortunadamente, va saliendo del vasto campo de la miseria en donde se crió.\n\nYo deseo, con mis setenta y tres años que cumplié el día 7 de setiembre y con mi simple y humilde autoridad, seguir ayudando a ésta gran obra de reivindicar al máximo al mundo del Cante Flamenco y Gitano Andaluz.\n\nA Don Manuel Urbano y a todos los que con él colaboran, éste viejo Artista nunca olvidará vuestro esfuerzo y vuestro cariñoso homenaje con éste CANDIL que ya estamos viendo que alumbra más que los mismos rayos del sol. Que esos rayos de luz sirvan para que el Mundo del Flamenco encuentre el futuro que merece.\n\nMiles de gracias, Culturía Mañana",
    "title": "ANTONIO MAIRENA",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "4-4",
    "page_number": 4,
    "word_count": 420,
    "article_char_count_full": 2378,
    "article_char_count_review": 2378,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-5-right-salia-y-remate-para-antonio-mair",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Antonio Fernández «Fosforito»\n\n...que existe una masa desatenta, incomprensiva, ignorante, ruda, el artista no lo ha ignorado nunca.\n\nAntonio Machado\n\nGitano tú te has deja el corazón en pedazos donde quiera que has cantao.\n\nQuizá por eso, querido Antonio, a veces tu corazón se duele y te pide que atemperes tus desgarros y emociones. Que nunca nos prives de tu grito, pero que dulcifiques tu queja.\n\nNo se nos olvida, amigo mío, que siempre has sido un pionero en abrir caminos por la rosa de los vientos, de nuestro particular mundo, y que has sido y eres caminante infatigable, pregonero de jondura, creador, re-\n\ncreador y rehabilitador, de gemios, siempre presentes en tu garganta. Tú mereces con creces la indiscutible dignidad que representas, y creo, que todos somos un poco tus\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\ns tu queja. No se nos olvida, amigo mío, que siempre has sido un pionero en abrir caminos por la rosa de los vientos, de nuestro particular mundo, y que has sido y eres caminante infatigable, pregonero de jondura, creador, re- creador y rehabilitador, de gemios, siempre presentes en tu garganta. Tú mereces con creces la indiscutible dignidad que representas, y creo, que todos somos un poco tus deudores, por el precioso e inapreciable legado de arte, que nos has ido dando, y que es, documento de inmenso valor para presentes y futuras generaciones de aficionaos y estudiosos, donde has puesto tu latido, tu talento, tu sensibilidad y sabiduría, y las vivencias, alegrías y sinsabores de tu amplia y prolifera vida de investigación flamenca. Yo que tuve la gran fortuna de estar junto a ti en tantos momentos de responsabilidad, de cara a la gente, y que he sufrido contigo ese tiempo mágico pre-actuación inmerso en los miedos de la responsabilidad de preocupación y, a veces, de angustia, ante el ¿cómo estaré? ¿cómo serán\n\n[ENDING CONTEXT]\n\nglorificar generosamente a tus antecesores y maestros.\n\nEsto te honra y dignifica aún más tu calidad humana; pero y a pesar de tu limpia modestia, los aficionados de verdad, los que te seguimos de cerca, sabemos de tu constante inquietud, de tu conciencia, de tu gran amor y compromiso con el cante, y de tu continuo recrear, dando a la luz nuevos matices, presintiendo estilos y recatándolos cuando ya estaban en clara fase de extinción y que, en tu voz, son cantes nuevos por desconocidos, que tú has tratado con exquisita y jonda esencia y con la más pura y fiel ortodoxia.\n\nLo tuyo sí, Maestro.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«Salía y remate para Antonio Mairena»",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 1081,
    "article_char_count_full": 6178,
    "article_char_count_review": 2645,
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
  },
  {
    "article_id": "1982-09-6-right-antonio-mairena-y-el-duende",
    "article_text_for_review": "Por Angel Alvarez Caballero\n\nNo voy a generalizar, aquí y ahora, sobre el duende en el flamenco, tema que quizás algún otro día trate con mayor detenimiento. Sí quiero hablar del duende en Antonio Mairena, un cantaor que valora extraordinariamente esta enigmática facultad de algunos artistas para alcanzar una mayor riqueza expresiva, un más profundo entrañamiento con lo jondo en esa especie de trance que todavía nadie ha sabido explicar suficientemente.\n\n«Hay días que viene y días que no viene» —declaraba Mairena en el diario «Informaciones» de Madrid el 25 de octubre de 1968—. «Días en que quiere uno cantar y no puede hacerlo. Días en que, al contrario, parece que va a ser una noche de chufla y termina siendo una noche grandiosa... ¡Qué quiere usted que le diga!».\n\nConmigo fue más explícito, en una ocasión en que le entrevisté para el mismo periódico.\n\n—Yo por duende entiendo todo aquel artista que transmite —me dijo—. Claro que no es lo mismo transmitir a un señor que no está preparado para digerir el cante flamenco o el cante gitano, como le queramos llamar, o a un señor que está preparado. Para transmitirle esos duendes a un señor que está preparado, hay que contar con el artista o el intérprete que en su forma de sonar lleve consigo ese duende, en su manera de expresar, de deletrear, en la técnica también es un factor importantísimo para todos aquellos que sepan digerir el cante tal y como es el cante, porque sin esa técnica...\n\nPorque los que creen que solamente con sonar gitano ya se cuenta con el duende, eso no es suficiente. La forma de sonar tiene que ir unida con la técnica exacta de cómo son los cantes, o mejorarlos, por ejemplo, como hizo Manuel Torre, como hizo Pastora Pavón, como hizo Tomás Pavón, como hizo Joaquín el de la Paula, como hizo Rafael el Gloria, en los que yo he conocido. Esto es lo que yo creo que sea el duende: hacerle a usted sentir una cosa que usted no sabe lo que es, pero que sí que en un momento dao a usted se le eriza el cabello, usted no sabe lo que le pasa, a usted le hace beberse tres whiskis, o tres copas de vino, o tres copas de aguardiente, o lo que sea, pero que usted no se explica cuál es el motivo. Esto creo que es el duende.\n\nLe pregunté más. Le pregunté si el cantaor que habitualmente puede transmitir ese duende a los oyentes, cuando canta sin duende, ¿está privando al oyente de algo esencial en su cante?\n\n—El cantaor nunca sabe cuándo el duende está con él —me respondió—. O sea, que yo en este momento que estoy con vosotros tomando una copa, si se encartara de poder cantar yo no sé si el duende estaría conmigo o no estaría. Podré estar mejor de facultades, o peor, pero lo que no sé es, hasta que no empiezo a cantar y voy desarrollando el cante, no sé si el duende está conmigo o no lo está.\n\n—Pero en el supuesto este —insistí— de que usted cantara ahora y viera que, según se va desarrollan-\n\ndo el cante, no cuenta con el duende, ¿usted nos privaría de algo de su cante? ¿Su cante seguiría siendo bueno esencialmente?\n\n—Yo desarrollaría el cante... le doraría, como se suele decir, le doraría la pildora: ejecutaría el cante tal y como es el cante, desarrollaría su técnica, pero entonces ya estaría falto de ese elemento tan esencial y tan imprescindible, porque entonces es como si al cuerpo le falta el alma. Si al cante le falta el duende, es como si al cuerpo le falta el alma, le falta la vida.\n\n—¿Usted siente el duende, percibe el duende, en otras circunstancias vitales al margen del cante?\n\n—Sí, efectivamente. Yo, que no sé nada de música, he asistido a grandes conciertos musicales con amigos que yo tengo directores de orquesta, en Nueva York, en Londres, en París, donde se han ejecutado obras de gran envergadura, y yo, que no entiendo de música, aunque me gusta muchísimo, en cierto momento de alta música clásica, me ha llegado el duende... También he visto el duende en el toreo, como también lo he visto en la pintura, y en todo lo que es arte creo que existe el duende. En su libro «Las Confesiones de Antonio Mairena», el cantaor ha formulado la teoría de la «razón incorpórea», algo impalpable e indefinible que hay que sentir y respetar para ser un buen gitano. «La Razón Incorpórea» —escribe así, con mayúsculas— «es el honor nuestro, la base de la cultura gitana, el conjunto de nuestras tradiciones y de nuestros ritos antiguos: una cosa que sólo entiende un gitano como Dios manda y que sólo los gitanos la viven. La Razón Incorpórea es intransmible e ininteligible fuera de nosotros, porque no se puede conocer de verdad lo que no se puede sentir. Sólo se nos permite expresarla por medio de metáforas. La Razón Incorpórea es la fuente de inspiración inagotable del cante gitano y del cantautor, y éste la expresa de forma intuitiva por medio del duende...» (1).\n\nEn una primera lectura creí que Mairena identificaba la Razón Incorpórea con el duende, pro veo que no, que en realidad confiere al duende sólo el papel de transmisor de esas esencias gitanas sin las cuales —según él— no se puede ser un buen cantaor. En definitiva, Mairena vuelve a poner en el ruedo de la polémica el elemento racial, y quienes dan al elemento andaluz un valor por lo menos igual al elemento gitano, pienso que tendrán algo que decir al respecto. Personalmente creo que sí, que el duende es más consustancial a la forma de «decir» gitana, a sus melismas, a ese rajo sin traslación posible al mundo payo. Pero no pretendo sentar dogma ni mucho menos, porque en definitiva me sigo preguntando como cualquier aficionado a este arte: ¿qué será eso del duende?",
    "title": "Antonio Mairena y el duende",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 999,
    "article_char_count_full": 5563,
    "article_char_count_review": 5563,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
