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
    "article_id": "1989-03-10-left-ni-o-de-cabra",
    "article_text_for_review": "Cuando en Puente Genil, tierra de cantes y cantaores, los \"Mediaolla\" teníamos (antes de \"la bulla\") el café-bar en la Matallana frente al Paseo o El Romeral, ya ha llovido lo suyo. Entraron un dediodía José \"El Seco\" y otro señor algo más bajo que él cuyo talante denotaba una persona nada corriente o, al menos, alguien relacionado con artistas. Preguntando por Antonio \"Media\". José Bedmar \"El Seco\" pidió una manzanilla y su acompañante un doble con leche y dos cortas de aquéllas que parecían un perol sin asas bocabajo. Servidos que fueron en el mostrador y cerciorados de la ausencia del cantaor de la casa, pasaron al cuarto que \"El Seco\" conocía como \"el de los jipíos\" y que servía unas veces para cante y otras para juegos de cartas y dominó.\n\nAl volver mi hermano de la calle de sus habituales salidas concernientes al negocio, le dije: ahí está tu amigo \"El Seco\" y otro señor que desean hablarte. Acto seguido se fue para el consabido cuarto y yo detrás por aquello de la curiosidad. Mi hermano me llamó exclamando: Vuelve a invitar a estos señores a \"tó\" lo que quieran sin cobrar ini una chica! José tomó otra manzanilla, y Cayetano, que ya sabía yo el nombre por ofrselos a labios de \"El Seco\", pidió otro doble con leche y otras dos tortas; el señor José le había instado: \"Anda, Caetano, come más tortas, tú que tragas lo tuyo, que en La Puente parace son mejores que en tu Cabra\". Acaso mi hermano \"Media\" supiera lo comilón que era el señor Muriel, cuando me ordenó: \"Nene, trae un cesto de tortas y que ellos cojan las que quieran\". Cayetanito exclamó rápido: \"No, hombre, el cesto no; yo con una docena tengo bastante, porque de aquí a poco, sabréis, tenemos que estar \"tajalán-donos\" los tajos de guarro a que mos ha conviaó ese tal Marta el cortijero\". (El dicho Marta era don Eusebio, gran aficionado y mecenas incansable del cotarro flamenco).\n\nJ. Márquez Cabello\n\nMe enteré en tanto los atendía que mi \"Media\", con su fresca y potente voz iría con ellos como \"telonero\", para abrir plaza, cual siempre ocurre con los más jóvenes y menos duchos cantaores entre varios de algún nombre y muchas leguas a las espaldas.\n\nBien entrada la noche volvió a casa contento de haber alternado con el artista Cayetano Muriel \"Niño de Cabra\", figura puntera del cante en toda Andalucía y fuera de ella, no parando de tararear la copla favorita del egabrense:\n\nQuisiera tener de lomo la barriga prevenía, y de longaniza el colmo diciendo con alegria: ivenga vino que majogo! En nuestro bar-café teníamos un primitivo tocadiscos de bocina \"La Voz de su Amo\" y en todo el mostrador de seis metros, cuatro anaqueles a todo lo largo repleto de discos de pizarra, algunos del grueso de un \"Candil\" extraordinario y grabados sólo por una cara que Dios sabe dónde irfan a parar cuando \"la bulla\". A uno le cogió camino de Ifni a reclutar indígenas (que ironía) para ir contra los que estaban en contra. Pero a lo esencial vayamos. Yo a más de camarero y barman, era el \"disyoqui\" que se dice ahora o rayaplacas que se decía antes. Había ocasiones que tenía que pasar el mismo disco siete u ocho veces seguidas a instancias de mi hermano o algún otro aficionado. Un día ya cansado, recuerdo que le respondí: Este, como no quieras que lo ponga de canto, lo veremos hecho papilla. Me replicó enérgico: Tú haz lo que te digo, sin rechar ni apurarte por desgastar alguno, que ya viene de camino otra remesa.\n\n\"Mediaolla\" mi hermano, dicho sea de paso porque apenas es sabido, fue requerido varias veces por promotores o agentes, para engrosar elencos flamencos. Pero él, acaso intuyendo la vida del artista por dentro, de sacrificios y preocupaciones, aunque por fuera (para muchos) divertida, no quiso nunca perder su libertad de acción y su desenvolvimiento de bar y clientela.\n\nDespués supe repasando las investigaciones del ex-alcalde de Benamejí: Don José Arias, Pepe para los amigos, lo que le aconteció a Cayetano estando de \"fiesta\" con el poeta Manuel Machado a quien no conocía aún y con quien terció hablar de la poesía en las \"letras\". Y fue que el de Cabra le dijo: \"Hoy no oye usted más que versitos de coplas y coplas de un cursi que pa qué. Descúbrase pa oir esto: Crece el fuego con el viento/ con la noche el padecer/ con el recuerdo el tormento/ con los celos, el querer/\". La gracia estaba en que dicha copla era del acervo de don Manuel. Desde que lo supo fueron cordobés y sevillano, entrañables amigos.\n\nC/. Doctor Arroyo, 12\n\nTeléfono 250058\n\nJAEN\n\nAPERITIVOS SELECTOS Especialidad en Plancha\n\nC/ Mesones, 18\n\nTeléfono 263546\n\nJAEN",
    "title": "Niño de Cabra",
    "periodical": "candil",
    "issue_id": "1989-03",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 802,
    "article_char_count_full": 4555,
    "article_char_count_review": 4555,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-03-11-left-aurelio-de-c-diz-y-el-mal-gusto",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFrancisco Vallecillo remite a la Dirección de CANDIL esta carta abierta, respecto al trabajo de Anselmo González Climent publicado en el número anterior de esta Revista.\n\nLa leyenda que precede a esta carta, \"AURELIO DE CADIZ Y EL MALGUSTO\", es bien significativa de la repudiable opinión que le merece el Asesor Flamenco de la Junta, y siempre respetado amigo y maestro, la divulgación del pensamiento del cantaor gaditano. Hemos de hacer constar que, sustancialmente, compartimos los pronunciamientos de nuestro comunicante en lo que se refiere a la talla inconmensurable de Juan Talega, y al poco crédito que tiene absurdos insultos y denuestos dirigidos no sólo contra el cantaor de Dos Hermanas sino contra el maestro desaparecido Antonio Mairena. Aún más: rechina la sensibilidad, por muy\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nnstar que, sustancialmente, compartimos los pronunciamientos de nuestro comunicante en lo que se refiere a la talla inconmensurable de Juan Talega, y al poco crédito que tiene absurdos insultos y denuestos dirigidos no sólo contra el cantaor de Dos Hermanas sino contra el maestro desaparecido Antonio Mairena. Aún más: rechina la sensibilidad, por muy endurecida que uno la tenga, cuando de manera arbitraria, absurda, Aurelio imputa ignorancias al mejor de los Mairenas y establece prioridades entre éste y su hermano Manuel, que al más lego de estos foros, hace simplemente reír. Pero, realizada esta precisión, creemos necesario reflexionar sobre la inconveniencia de publicar íntegro este trabajo, motivo de la ponderada pero enérgica protesta de Francisco Vallecillo. A tal efecto, caber preguntarse: ¿Hizo Aurelio de Cádiz estos comentarios o no los hizo? La honestidad y rigor profesional de Anselmo González Climent no ofrecen la más mínima duda: los hizo. Y si ello es así, tales manifestaciones contribuirán sin duda a un conocimiento más amplio de la, tal vez, sicótica personalidad de Aurelio. Es decir, eso es historia que supondría torpeza el silenciar. Por otro lado qué perjuicios pueden ocasionar las diatribas del Tuerto de Cádiz a un Juan Talega, viejo \"león gaditano\" y a un Antonio Mairena maestro indiscutido de maestros. Evidentemente ningunos. Otra cosa sería, poner en entredicho el buen gusto y la sensibilidad de nuestros lectores. Equivocados o no, nuestro criterio, por elemental, es bien sencillo: la verdad a la postre, se defiende por sí misma, y nunca es razonable desvirtuar el mensaje, matando al mensajero. Pensamos. Francisco Vallecillo E xtraños como hemos sido a la publicación en el número anterior de Candil de nueve páginas\n\n[ENDING CONTEXT]\n\ny la rotundez vital... Muchas vallas temperamentales no ha podido sobrepasar Juan Talega para alcanzar el vértigo de tope que distingue a los cantaores de época. Carece de esa decisión —fecundación?— interna que lleva al libre vuelo de la expresión. Su caso no es tan curioso como ejemplar: un cantaor que reúne casi definitivamente las condiciones más difíciles del flamenco, que entra en sabrosísi-ma madurez, pero que —y aquí no importan los años— no recibe los aguijones del duende. Es incapaz de saltar al vacío. Talega no sabe perderse porque sencilla-mente no quiere perderse a sí mismo».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aurelio de Cádiz y el mal gusto",
    "periodical": "candil",
    "issue_id": "1989-03",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 1983,
    "article_char_count_full": 12274,
    "article_char_count_review": 3385,
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
  },
  {
    "article_id": "1989-03-12-left-reflexiones-y-conclusiones-sobre",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFrancisco Zambrano Vázquez\n\nC onocidas, leídas y escuchadas casi todas las opiniones de críticos y aficionados, sobre el celebrado XVI Congreso Nacional de Actividades Flamencas, con el ánimo totalmente sosegado y sin apasionamiento ni visceralidad, quiero desde estas líneas hacer unas reflexiones sobre el citado Congreso que tuve el honor de presidir, por si la visión desde la mesa del mismo, puede aportar algo nuevo que abra luz a sucesivas celebraciones. No se hace pues este escrito como contestación, ni para entrar en polémica, con determinadas críticas recibidas y que acepto, en la parte que me toque (sin duda la mayor por ser el presidente), con el ánimo espiritual en que acepté dirigir el Congreso y por eso empiezo diciendo que en éste y de este Congreso me sentí orgulloso y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"origen\"]\n\ntestación, ni para entrar en polémica, con determinadas críticas recibidas y que acepto, en la parte que me toque (sin duda la mayor por ser el presidente), con el ánimo espiritual en que acepté dirigir el Congreso y por eso empiezo diciendo que en éste y de este Congreso me sentí orgulloso y contento por dos razones: La primera porque siendo Presidente de la FEFEX y ponente, en este Congreso hemos conseguido que se acepten las denominaciones de origen de nuestros cantes extremenos, y no quiero aquí entrar a revatir opiniones tan sesgadas como \"El presidente de la mesa, en su ponencia hizo un análisis de los cantes extremenos subdividiéndolos en \"Los cantes que no necesitan comparación ni estudio y los tangos y jaleos extremenos\". Creo que la idea de los organizadores del Congreso, de dar a conocer las ponencias con antelación suficiente a todos los congresistas (para que se lean), contesta y desmiente esta gratuita aseveración, quizás hecha por desconocer, lo que ocurrió en el anterior Congreso, celebrado en Benalmádena. Y aquí quiero significar lo que para mí fue un acierto de los organizadores, adelantando las ponencias, que debieron hacer innecesaria la lectura tediosa y total en el momento de la exposición y debate, así lo entendimos en la mesa, y por eso llamamos la atención de los ponentes pidiendo brevedad y resumen de lo escrito, ya que todos debíamos conocerlo, pero sin duda así no debieron de entenderlo algunos congresistas y críticos. A pesar de todo, vuelvo a reiterar que la fórmula es la ideal, porque deja el mayor tiempo posible para el debate y da opción a los que se encuentren en desacuerdo con el ponente, a poder llegar al Congreso debidamente documentados y no con simples opiniones. Creo que una ponencia es distinta a una comunicación que no necesita debate; y además redundamos en que la Comisión Intercongreso, debe realizar una criba de las ponencias y otorgar en or\n\n[ENDING CONTEXT]\n\ndebatirse, fijar a cada uno el tiempo que estimen necesario para su debate atendiendo a su contenido. 4. Las votaciones de las propuestas y su aprobación deberían ser vinculantes y sin posibilidad de rebocación posterior en las conclusiones, haciendo una llamada a los congresistas para que asistan a los debates de las ponencias (al menos a las que les interesen).\n\n5. En el Orden del Día, de la última sesión, debería figurar pues Redacción y Lectura de las Conclusiones y no Redacción y Aprobación de las Conclusiones.\n\nY como última conclusión, decir que: \"a veces de la oscuridad surge la luz\".\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Reflexiones y conclusiones sobre el último Congreso Nacional de Actividades Flamencas Francisco",
    "periodical": "candil",
    "issue_id": "1989-03",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1812,
    "article_char_count_full": 10849,
    "article_char_count_review": 3540,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "origen"
      }
    ]
  },
  {
    "article_id": "1989-03-12-right-sem-foro",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSemáforo\n\nLa imprevisión social de los flamencos Paco Vallecillo\n\nLa nueva ley de pensiones no contributivas que prepara el Ministerio de Trabajo y Seguridad Social beneficiará a 700.000 ancianos y minusválidos, de los que unos 200.000 no reciben actualmente ninguna clase de ayuda. Es de suponer también que estas pensiones resolverán paralelamente el problema de la asistencia médico-farmacéutica o que, en su defecto, la proyectada Ley del Medicamente asumirá este sustancial aspecto asistencial, mucho más importante aún que el económico, desafortunadamente e inevitablemente harto menguado. Así podría creerse que los viejos artistas flamencos que todavía quedan y que no cuentan con ningún remedio oficial a su absoluto desvalimiento, formando parte como forman del colectivo que ahora va a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"afición\"]\n\niejos artistas flamencos que todavía quedan y que no cuentan con ningún remedio oficial a su absoluto desvalimiento, formando parte como forman del colectivo que ahora va a ser atendido, podrán ya prescindir del magro apoyo que a trancas y barrancas puede prestarles la sociedad por la intermediación de la Institución de la Tercera Edad de los Artistas Flamencos (ITEAF). No será así, porque no sería justo que la ITEAF, que canaliza la ayuda de la afición hacia los mayores, desvalidos, por medio de aportaciones que mayoritariamente se deben a los propios artistas en activo a través de festivales en Congresos y a algunas colaboraciones saltuarias de organismos oficiales, no sería justo que la ITEAF considerase innecesaria su dedicación a esta noble causa. Los veteranos artistas desamparados, que por implacable ley de vida (o de muerte) cada vez son menos, necesitan imperativamente de un suplemento que alivie la escasez de lo que van a recibir del Estado. Con mayor motivo cuando este pequeño óbolo les llegará siempre impregnado de algo que la ancianidad aprecia sobre todos los bienes materiales: el saberse rec\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\namáticos antecedentes evoca. Los nombres de unos flamencos que perdieron la vida en el viaje hacia o desde el tajó cada día distinto de su actividad laboral debieran representar acicate para que los propios interesados movieran una solución específica y concreta para la máxima garantía y cobertura de sus propios intereses. Uno se atreve a pensar que cuando en España se restablecieron las libertades ciudadanos, los artistas flamencos perdieron la gran ocasión de formar sus filas dentro de un sindicato solvente: acaso éste fue un tremendo fallo, sin duda por una explicable falta de sentido de clase. Siempre es tiempo de rectificar y nadie mejor que los propios interesados para decidir algo tan importante como el futuro de su seguridad sociolaboral. Ahí quedan, por si hiciera falta recordarlos, los nombres de quienes no pudieron dejar tras su desaparición el consuelo de un Estado protector mediante pensiones especiales concomita\n\n[ENDING CONTEXT]\n\ny aquí encontrará, a pesar de tantas agresiones con sedicentes propósitos de renovación, a los sucesores de María la Burra, de Fernanda de Utrera, de Fosforito, de Menese, de Chocolate y hasta de cualquier ignoto campesino o artesano que en la besana o en el alfar seguirá templándose a compás para entonar una soleá de la Serneta o de El Fillo. Y Japón, como tantos países del mundo, seguirá asistiendo al flamenco, con respetuoso silencio, embebido y absorto en el disfrute de un arte incopiable y ajeno a su práctica que constituye un patrimonio único e intransferible de la buena tierra andaluz.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Semáforo",
    "periodical": "candil",
    "issue_id": "1989-03",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1299,
    "article_char_count_full": 8244,
    "article_char_count_review": 3754,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "afición"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "1989-03-13-right-podio-y-picota-ventolera",
    "article_text_for_review": "Hoy ocupa también este podio la muy flamenca Villa de Alhaurin de la Torre, en la serrana Hoya malagueña, que acaba de rendir homenaje a Fosforito, declarándolo alhaurino de adopción. Antonio que ya es hijo predilecto de su natal Puente Genil y adoptivo de la capital cordobesa, escogió este rincón malagueño para vivir y en él ha consolidado a través de muchos años sus raíces. Enborabuena por este nuevo reconocimiento de sus virtudes tanto al Maestro como al municipio y vecindario que se han honrado a sí mismos ofreciéndole este honor.\n\n1rremediable castigo a Manuel Ríos Ruiz, excelente poeta y erudito flamencólogo, cuya reciente obra está izada hoy mismo en el Podio, por su inexplicable omisión, que parece tenida de sectarismo sino existiera en ella una evidente contradicción con otras afirmaciones suyas, cuando en la Enciclopedia del Flamenco escribe textualmente:\n\nA la hora de situar a Manolo Caracol en los anales del flamenco, habría que ponerlo junto a Silverio, Don Antonio Chacón, Manuel Torre y Pepe Marchena, entre los maestros y los genios.\n\nGravísimo pecado de omisión -volis nolis-, para que este tribunal no encuentre ninguna circunstancia atenuante.\n\nCondena sin atenuantes a los responsables del flamenco en el Canal Sur -soñado espejo de Andalucía- en sus primeros pasos. La presentación, lastimosa. Ni Andalucía se merece una presentación tan paupérrima (¿dónde el flamenco de las ocho provincias?) ni la serie de lamentables defectos acumulados: 1.° El flamenco en la cola de todos los programas, incluido el tostón peliculero. 2.° La falta de representatividad, agravada con una mujer que pega gritos micrófono en mano (ivaya actitud flamenca!) y un caricato que maldita la gracia que tuvo. 3.° El desconcierta guitarrístico que echó a pique a cinco estupendos tocaores abogados en la más absoluta desconexión. (El sexto mejor ignorarlo). 4.° El recorte del programa (inevitablemente a cargo del flamenco, lo último siempre en todos los medios informativos) dejando sin actuación, para rabia de los aficionados, a tres cantaoras de fundamento: María la Burra, Fernanda y Bernarda. Nada menos.\n\nTras la inauguración, un primer programa (a la hora de esta censura) que hace augurar todo lo peor: lo peor puede ser que Canal Sur prosiga el maltrato que la TV andaluza ha dedicado casi siempre —sobre todo en sus últimos tiempos— al flamenco. Venir ahora a proyectar pasajes de la última Bienal de Flamenco resulta algo así como el invento del ungüento curalotodo. Porque ya hubo bastante Bienal para traerla ahora a modo de refrito y como prueba palpable de ignorancia y de falta de imaginación. En un programa desilusionante que se emite el peor día de la semana a la peor hora de ese día para una afición tradicionalmente dedicada los sábados a la vida de Peñas y Tertulias, a recitales, a concursos, a festivales. La ignorancia del flamenco en este medio informativo andaluz no exime de responsabilidades. Todavía es tiempo de rectificar y siempre es tiempo de aprender.",
    "title": "Podio y Picota/Ventolera",
    "periodical": "candil",
    "issue_id": "1989-03",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 486,
    "article_char_count_full": 3002,
    "article_char_count_review": 3002,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
