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
    "article_id": "1982-03-11-left-las-saetas-de-miguel-calvo",
    "article_text_for_review": "A Miguel Calvo le vino la pasión cantaora unida a su nacencia en el martenio café La Peña, por cierto, decorado con los espejos del famoso café cantante de Pepe el Costeño. Poeta unido al pueblo y lo popular, personalísimo presentador de múltiples festivales de cante, Miguel Calvo nos da hoy cumplida muestra de su hacer flamenco con esta gavilla de saetas, íntimas, naturales y sentidas que, como otras muchas suyas que se alzaran con premio en diversos concursos, merecen la altísima condecoración de ser cantadas y correr hacia el costado de un Cristo o a enjugar las lágrimas de una Dolorosa. Pero no adelantemos juicios, queden ellos para el lector.\n\nLas lágrimas de tu cara bajan pidiendo consuelo, para el hijo de tu alma que agoniza en el madero.\n\nDestrozao va Jesús por el amargo sendero bajo el peso de la cruz ha caído por el suelo. Qué triste es la ingratitud.\n\nVirgen del Mayor Dolor con lágrimas dolorías viendo cómo el Redentor tropezaba y se caía. Cristo de la Expiración siempre mirando a los cielos pidiendo la bendición del que busca tu consuelo.\n\nQué pena será tu pena y qué Angustias tu dolor, que tu carita morena se ha quedado sin color lo mismo que una azucena. Ay, Cristo de la Humildad con el rostro ensangrentao y los ojos sin mirá que a toítos has perdonao a toítos por igual.\n\nAy, Virgen de los Dolores, una cara tan divina no la pintan los pintores, que los pinceles no atinan.\n\nAmarrá lleva las manos con muy fuertes ligaduras, por decir a los humanos que no hay diferencia alguna, que toítos semos hermanos. Divino clavel del Sur tu cuerpo va floreciendo, Cristo de la Vera Cruz, y en tu costado naciendo, como un venero de luz.\n\nSus ojos miran al suelo para aguantar el dolor por el pesado madero. Que no te aflijas, Señor que aquí estamos los jaeneros\n\nMadre mía de los Dolores mira si te pido bien; que no le falten los sudores a los hombres de Jaén.\n\nVirgen de la Soledad, qué solitaria caminas, porque nadie puede ya arrancarte las espinas en tu corazón clavás.\n\nCONSTRUCCIONES\n\nCALLE ALCAUDETE, 10\n\nJ A E N\n\nInstituto de Estudios Giennenses. Candil : boletín de la Peña Flamenca de Jaén. N.º 20, 3/1982. Página 11",
    "title": "Las Saetas de Miguel Calvo",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 384,
    "article_char_count_full": 2152,
    "article_char_count_review": 2152,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-03-11-right-una-saeta-para-manolo-zapata",
    "article_text_for_review": "Cuando en Arcos, llegado el barrunto de la poesía, me dio también por el cante, ponía la voz entoldada como intentando copiar la de un hombre grande y moreno, tocado de cañero ladeado, con pinta de aperador de cortijo y risa de mina que cantaba seguiriyas en los tabancos, fandangos en el Casino y saetas en las calles sobrecogidas de la Semana Santa. Era la voz montaña de Monolo Zapata, un hombre bueno que, en vez de escribir versos como los demás habitantes de mi Arcos, los dibujaba en el aire, pendolista de caminos y trochas. De tal caballero popular me queda una imagen entre humana y mítica, entre campesina y urbana, entre aineja y moderna, porque tenía un caballo, pero la vida lo había hecho conducir un taxi. En aquellos años de Bachiller y novias movedizas, Zapata ya era una institución para el pueblo liso y llano. No sólo se hacía presencia con su voz sonámbula, sino que brillaba en la feria como un centauro chispeante, era el eje de su Hermandad de la Vera Cruz en el Jueves Santo y el abanderado de la Romería. Unicamente en\n\nPor Antonio Hernández\n\nla procesión de la Virgen de las Nieves, que por entonces era una manifestación para el lucimiento de los señoritos, no descollaba su presencia en medio del cortejo, aunque sí en las bocas de sus paisanos que decían: «...pero hoy no canta Zapata». Le faltaba, pues, una campana a la fiesta. Le faltaba un poco de sonrisa a la alegría.\n\nManolo Zapata no se llama Zapata, sino Gallardo. Su apellido copia su prestancia y es como un reflejo que a su condición popular le añade finura popular. Se dice que el sobrenombre de revolucionario mejicano le viene del empleo de su familia, en cuanto a que los Gallardos fueron siempre los responsables de las caballerizas de los Zapatas, una de las tribus distinguidas que, desde Zamora, hicieron lujo en Andalucía. Sin duda, ahora llevan, por fin, una nobleza. Y donde quiera que se hallen, sus descendientes serán reconocidos por una voz cantaora, más\n\nque por su escudo de armas. Así se escribe el futuro, hasta que se escriba con letras verdaderas. Manolo quizás no sepa nada de esto. Y si lo sabe, no debe importarle mucho. Lo suyo es cantar, conceder su riqueza de pájaro asombrado, cascablear el aire. Pero ya va siendo hora de que Arcos, su pueblo y el mío, le restituya cuanto le han quitado, le rinda un homenaje, que será un homenaje al cante y, sobre todo, a su saeta, esa que lleva clavada Manolo para que en las noches del Viernes Santo nos acurruquemos en el misterio cálido de su voz, para que nos salvemos momentáneamente de la sordidez; le ponga Arcos una calle cercana a la de Julio Mariscal a fin de que los niños lean sus nombres de campo y digan: «Los dos cantaron el pueblo». Porque cuando muera Manolo irá a buscar a su amigo Julio para que le haga las letras con que cantar en la muerte. Y es bueno que no tenga que recorrer muchas calles quien, cansado ya, ha recorrido tantos caminos.",
    "title": "Una Saeta para Manolo Zapata",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 525,
    "article_char_count_full": 2920,
    "article_char_count_review": 2920,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-03-12-left-una-manera-de-ser-y-entender-el-",
    "article_text_for_review": "Por Federico Vázquez\n\nEs curioso observar cómo se han apropiado del flamenco para llevarlo como bandera de nuestra imagen fuera de España, y no han dudado en presentarlo como una pan-dereta abominable. Valiéndose de él lo han vilipendiado de una manera tan procz que lo han querido convertir en la bata de cola y la peineta, amén del grito estentóreo y el jaleo. Cuando se ha intentado desechar esa imagen, muy pocos han estado. Díganlo, también, las nulas subvenciones —salvo honrosísimas excepciones— que ha recibido un arte que tiene atributos tan importantes para estar en nuestro Ballet Nacional, en muy larga representación. Quizás ahora, cuando lo vean, dirán que el flamenco es algo interesante, porque lucirá decírselo a los convecinos. Ahora es posible que los advenedizos de turno comiencen a retirar sus prejuicios anteriores y crean llegado el momento de hacerse defensores leales. Como siempre, algo es bueno si una cierta clase lo acepta y los imitadores y propagandistas lo atestiguan, sin saber que eso ha estado ahí mucho antes de que su sapiencia repentina lo descubriera. Antes sería la gente de mal vivir y de estratos sociales no concordes con la sociedad maximalista en que han sido educados y, por supuesto, sin el desarrollo del yo, porque eso sería renunciar a ciertas situaciones que les son beneficiosas para su relación social, pero en la mayoría de los casos anuladoras de su personalidad. Véase la regresión total que han sufrido las músicas populares, en favor de una música multinacional que lo único que busca es el apropiamiento de ingentes masas manejadas por unos pocos, y en la que no se toma parte para nada si no es para pagar. En ese hacinamiento, la personalidad individual —como expresión de lo que uno es capaz de hacer— queda abolida, y se convierte en voluntad solícita al mandato del consumismo musical que la moda crea oportuno imponer.\n\nEsta perorata, que a cualquier inquieto se le puede escuchar, no tiene plasmación en la vida flamenca, a pesar de ciertos intentos masificadores, que han tratado de mercantilizarlo. El flamenco, como expresión en la copla de los sentimientos y vivencias, asume el carácter personal como algo consustancial, y en sus contiguos devenires siempre ha salido en busca de su razón de ser: el hombre, diciendo lo que siente, vive y padece, por encima de cualquier circunstancia tergiversadora. Y es en esta línea donde las Peñas Flamencas tienen su razón de ser y manera de entender el flamenco. Porque una Peña debe estar creada para escuchar, no para oír; para hacer aficionados, no simples espectadores; para enseñar, no para confundir; en fin, para eso: ser flamencos. Todo lo demás le sobra e hiere al flamenco. Quienes no sean conscientes de esto, no deberían estar en una Peña Flamenca, más bien en un club de diversión. Por eso, la Peña Flamenca no puede ser numerosa, debe pecar de pequeñez, porque la comunicación artista-aficionado tiene su sitio entre pocos. Lo que sí se debería fomentar es la creación de un mayor número, que fueran aglutinantes —en tiendas, diría Paco Vallecillo— de lo bueno que deben conservar: la intimidad de quien dice y escucha, imposible en una masificación.\n\nEsto y alguna cosa más deberían buscar las Peñas, pero el tiempo ha de ser el que ponga a cada uno en su sitio y juzgue la labor hecha.\n\nY viene a la memoria la letra de un fandanguillo de Huelva, que bien pudiera ser un lema para las Peñas:\n\nYo no digo que mi barca sea la mejor del puerto pero sí digo que tiene los mejores movimientos que ninguna barca tiene.\n\nAvda. Generalísimo, 25\n\nTeléfono 21 10 01",
    "title": "Las Peñas Flamencas: Una manera de ser y entender el Flamenco",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 608,
    "article_char_count_full": 3583,
    "article_char_count_review": 3583,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-03-12-right-noticias-de-algo-mas-que-de-una-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA PROPOSITO DEL BICENTENARIO DE JOSE CADALSO:\n\nPocos españoles alcanzan con tanta justicia el reconocimiento histórico de adelantados de su época, como el gaditano José Cadalso: romántico antes de que el Romanticismo fructificase en movimiento, liberal con anterioridad a los tiempos en los que la libertad fuese noble bandera política. Patriota ejemplar, muerto en el sitio de Gibraltar —febrero de 1782—, el coronel Cadalso amó apasionadamente a España de tanto padecerla. Consciente de que la verdad está «atada al carro de la mentira triunfante», acomete la redacción de «Cartas Marruecas», su más conocida obra, escrita en 1770 y que no pudo ver su aparición impresa, ya que fue publicada póstumamente en felletón del «Correo de Madrid», en 1789.\n\nNo vamos a entrar ahora en la presumible\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"crítica\"]\n\nla. Consciente de que la verdad está «atada al carro de la mentira triunfante», acomete la redacción de «Cartas Marruecas», su más conocida obra, escrita en 1770 y que no pudo ver su aparición impresa, ya que fue publicada póstumamente en felletón del «Correo de Madrid», en 1789. No vamos a entrar ahora en la presumible influencia de Montesquieu sobre el autor andaluz y sus «Cartas», pero sí nos interesa recalcar que no son cosa distinta de una crítica a la España de su época. Denuncia y lacerado sentir que las hace permanecer olvidadas hasta que son redescubiertas por los hombres de una generación marcada por las misma preocupaciones: la del noventa y ocho. Por Manuel Urbano Y dentro de estas cartas, la VII —presumiblemente redactada en 1774—, se nos viene con el alto interés de un monumento literario sobre uno de nuestros cantes, el polo. Es la primera vez, como ha sido señalado por Arcadio Larrea, que nos sirve la literatura española noticias «sobre un cante considerado flamenco, interpretado precisamente por gitanos» y, muy probablemente, como puede deducirse del texto, de la comarca jerezana. Pero, a mi entender, no estr\n\n[ENDING CONTEXT]\n\ndiciéndome a mí mismo en voz baja: —¿Así se cría una juventud que pudiera ser tan útil si fuera la educación igual al talento? —Y un hombre serio, que al parecer estaba de mal humor con aquel género de vida, oyéndome, me dijo con lágrimas en los ojos: —Sí, señor; así se cría».\n\nSi me es permitido, por último, admitaseme una interrogante. ¿Por qué se continúan escondiendo o, lo que es peor, se manotea asombradamente ante la crítica seria y responsable de determinados personajes, más o menos actualizados, que encajan perfectamente en los descritos por José Cadalso?\n\nJ A E N\n\nCorrea Weglison, 9\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A propósito del bicentenario de José Cadalso: Noticias de algo más que de una fiesta del siglo XVIII",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1334,
    "article_char_count_full": 7805,
    "article_char_count_review": 2766,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "crítica"
      }
    ]
  },
  {
    "article_id": "1982-03-13-right-x-congreso-de-actividades-flamen",
    "article_text_for_review": "En la reunión celebrada por la Ejecutiva para el X Congreso de Actividades Flamencas, el pasado 7 de marzo en Almería, se adoptaron importantes acuerdos: entre ellos cabe destacar la determinación de las fechas en que aquél tendrá lugar, días 16, 17 y 18 de septiembre. Las comisiones de trabajo, creadas al efecto, trabajan ya a ple\n\nCONGRESO NACIONAL DE ACTIVIDADES FLAMENCAS≈ JAEN 1982 na máquina, bajo la supervisión y coordinación de miembros de la Ejecutiva. Señalemos algunas precisiones de la comisión de Ponencias. Se han considerado los criterios de valoración que han de primar en el sistema de selección de Ponencias. Entendemos que es esta una labor fundamental para que el Congreso tenga el mínimo de mensaje, operatividad e ilustración que todos deseamos, y no se convierta en una mera fiesta social. El pasado Congreso de Almería creemos que cumplió con dignidad este cometido, pese a las pocas afortunadas disquisiciones de Kayros. Si el aspecto sustantivo del Congreso, que por necesidad ha de referirse a ponencias y comunicaciones, se cumple con rigor, todos los demás éxitos deben ser estimados colaterales. Desde esta perspectiva, el criterio amplio de selección que hemos adoptado enriquecerá, ciertamente, el debate sobre aportaciones no especificamente jondas. Hay planos que alumbrar desde una óptica antropológica, musical y cultural, etc. El plazo máximo para presentar Ponencias y Comunicaciones se ha establecido el 31 de mayo.\n\nEn otro orden de cosas, se preparan importantes publicaciones de obras inéditas que enriquecerán la valija de los Congresistas. Ya desde ahora agradecemos cuantas sugerencias nos lleguen sobre aspectos sustantivos o procedimentales del Congreso. El éxito, evidentemente, será de todos.\n\nRestaurante\n\nProlongación Antonio Herrera, s/n. Teléfono 22 79 54 - JAEN",
    "title": "X Congreso de Actividades Flamencas",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 279,
    "article_char_count_full": 1818,
    "article_char_count_review": 1818,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
