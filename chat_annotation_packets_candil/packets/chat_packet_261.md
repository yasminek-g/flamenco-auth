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
    "article_id": "1992-09-17-left-sobre-el-gratuito-invento-del-ca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Barrios\n\nAl estar sumido ese difícil mundo del flamenco en el radicalismo de las pasiones más encontradas, estoy seguro de la polémica que va a suscitar este artículo. No la rehúyo. Por el contrario, la deseo si a través de ella podemos acercarnos un poco más a la verdad. Lo que demando —creo que con cierta legitimidad— es que los argumentos en contra de mi teoría se documenten con textos que puedan contrastarse, único método que merece credibilidad y respeto, frente a esas «verdades absolutas» que se van repitiendo al cabo de los años sin un aporte documental que las justifique.\n\nEl cante no pudo ser creación genuina de los gitanos\n\nSin ánimo de herir susceptibilidades, creo que bastaría un conocimiento elemental de la Historia para entenderlo así. A este conocimiento añadiríamos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\ntextos que puedan contrastarse, único método que merece credibilidad y respeto, frente a esas «verdades absolutas» que se van repitiendo al cabo de los años sin un aporte documental que las justifique. El cante no pudo ser creación genuina de los gitanos Sin ánimo de herir susceptibilidades, creo que bastaría un conocimiento elemental de la Historia para entenderlo así. A este conocimiento añadiríamos las siguientes razones: a) Por la propia limitación creadora del gitano. Nadie debe sentirse ofendido por esta aseveración, universalmente reconocida, incluso por un gitanó-filo tan amante de la gente more-na como Rafael Lafuente: «Lo curioso es que los gitanos no han creado absolutamente nada de lo que se les atribuye. No es obra suya el cante flamenco, ni consustancial a su naturaleza el «ángel» que les reconocemos por rutina, ni las mismas galas femeninas con que se adornan las andaluzas cuando quieren acentuar su personalidad regional» (1). «El gitano no crea, por regla ge- neral. Se sirve de coplas hechas por otros» (2). «El gitano no inventa; simplemente se instala, acepta y, en el mejor de los casos, reelabora la herencia andaluzia» (3). Nadie ignora que el paladin de la teoría gitanista del cante fue Antonio Mairena, quien no pudo eludir la contradicción por cuanto «él ha sabido, como nadie, reproducir—con las inevitables adulteraciones itinerarias— la mayor parte de los cantes de los más afamados maestros del pasado. Y lo ha hecho utilizando, según él, la transmisión oral que le ha merecido más crédito de autenticidad» (4). b) Porque, de ser gitan\n\n[ENDING CONTEXT]\n\nuna forma equivocada, porque los encargos no se piden, sino que se dan, y porque al sustituir la frase «mira que...» por «te pido», se está prescindiendo de un peculiar modismo andaluz.\n\n(12) Tomás Andrade de Silva, en el fascículo de «Antología del cante flamenco» (Madrid, 1959).\n\n(13) El delincuente español, de Rafael Salillas (Madrid, 1898).\n\n(14) Bosquejo histórico del cante flemanco, de Manuel García Matos (Barcelona, 1950).\n\n(15) ¿Somos o no somos andaluces?, de Luis Caballero (Sevilla, 1973).\n\n(16) Algo más sobre lo andaluz, lo gitano y lo flamenco, de José María Osuna (Madrid, 1952).\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sobre el gratuito invento del cante gitano",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1565,
    "article_char_count_full": 9617,
    "article_char_count_review": 3203,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      }
    ]
  },
  {
    "article_id": "1992-09-18-right-cr-nica-del-xx-congreso-de-arte-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nH abían pasado siete años y la ría del Odiel ni lo notaba. Nos recibió el lunes, 7 de septiembre de este año de eventos, como si hubiera sido ayer cuando, entre las ondas de este mar bravío, Gómez Hiraldo, Paco Vallecillo, Manolo Cano y tantos otros que ya no están, no nos hubiésemos empeñado en detener la historia antes de que eclosionara con la avidez de los grandes acontecimientos. Entonces, hace siete años, amigos de Huelva, muchos de los que este año nos hemos reunido bajo las plemares de vuestra efemérides, ya intuíamos la hermosura de vuestra aportación al Nuevo Mundo, la desazón agridulce de vuestros fandangos marineros incomparables que, aunque carentes del dejillo que sólo vosotros sabéis imprimirles, nos apropiamos en aquel final de verano de no importa cuántos años, ya que lo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"nuevo\"]\n\namigos de Huelva, muchos de los que este año nos hemos reunido bajo las plemares de vuestra efemérides, ya intuíamos la hermosura de vuestra aportación al Nuevo Mundo, la desazón agridulce de vuestros fandangos marineros incomparables que, aunque carentes del dejillo que sólo vosotros sabéis imprimirles, nos apropiamos en aquel final de verano de no importa cuántos años, ya que lo que realmente interesa es que otra vez nos habéis convocado y de nuevo hemos acudido junto a vosotros a estrenar un corazón, a rezar un eterno minuto de silencio por los que ya no pueden acompañarnos. El viaje Fue el lunes cuando las naves esperanzadas embarcaron en las naos de esa preciosa sede de la Casa de Colón, el abuelo lejano y marinero que a todos nos convocaba para iniciar la andadura marinera en ese espacio mágico que, hasta el sábado 12, supo ser para nosotros la mejor de las tres Carabelas para llevarnos a buen puerto en el proceso mar de la flamenquería. Al timón, Agustín Gómez, quien, acompañado por Luis Córdoba y José Luis Rodríguez, pusieron rumbo a la tierra prometida, alumbrando unos continentes para la verdad del cante, tal y como al día siguiente, bajo el cielo protector de la Peña «La Orden», realizaron Onofre López y Eduardo Fernández Jurado, acompañados por una marinería de entusiastas fandangueros que, al contrario que sucediera con el arriesgado genovés, no dieron entrada en sus corazones al motín ni al desaliento. Al fondo, sobre la blancura de una vela inmaculada, Juan Gómez Hiraldo, retirado por la muerte de tan especial singladura, fue testigo de la importante gesta que comenzaba en el instante preciso en que, desde diversos puntos de España y del mundo, los congresistas nos fuimos agrupando bajo la enseña de esta nave capitana que, por segunda vez en la historia de estos encuentros, es Huelva y su provincia. Problemas de la navegación No le fue fácil al Almirante señalar como felizmente concluso el periplo marinero, allá por los terrores góticos de fines del siglo XV. Tampoco para nosotros resultó sencilla la maniobra, pese a contar, desde muchos meses antes de nuestra salida a la mar, con un sólido equipo, léase organización del Congreso, que\n\n[ENDING CONTEXT]\n\npero también con el aliento de nuestra amistad sostenida a lo largo de los años y muy por encima de nuestras puntuales diferencias. Fijábajos la vista en las olas azules, bañadas por la brisa, y, en su horizonte imposible, algunos intuíamos la presencia inexacta de París, con la disculpa, a nuestra impreciación geográfica de que todo lo amado se agiganta en el recuerdo. Deberíamos aprender algo de francés para poder, el año que viene, agradecer a los próximos anfitriones, su entrega, como ahora mismo lo hacemos con Huelva. Muchas gracias, amigos. Merci, France...\n\nJuan de la Malena\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XX Congreso de Arte Flamenco de Huelva José Luis Buendía López (Enviado especial de CANDIL)",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "18-22",
    "page_number": 18,
    "word_count": 2919,
    "article_char_count_full": 17853,
    "article_char_count_review": 3803,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "nuevo"
      }
    ]
  },
  {
    "article_id": "1992-09-23-left-el-arte-de-mario-maya-y-el-miste",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nS in temor a equivocarme, pienso que los congresistas que asistimos al XX Congreso de Arte Flamenco de Huelva, íbamos predispuestos a escuchar con estoicismo todos los fandangos onubenses que nos cantaran, así como toda la sensiblera dulzura de los estilos de «ida y vuelta», a los cuales estaba dedicado al evento por aquello del Quinto Centenario. Y en honor a la verdad, no sé si por la predisposición, éste que les escribe no tuvo en ningún momento sensación de hartura, aunque si el espectáculo «Y... después América» hubiera durado cinco minutos más, esta sensación sí se hubiera producido, pues, a pesar de realizar un extenso y completo recorrido por los localismos y personalismos —en el que imperó el tratamiento «atoron-jao»— de los fandangos de la tierra, la pobreza de la coreografía y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"representación\"]\n\nningún momento sensación de hartura, aunque si el espectáculo «Y... después América» hubiera durado cinco minutos más, esta sensación sí se hubiera producido, pues, a pesar de realizar un extenso y completo recorrido por los localismos y personalismos —en el que imperó el tratamiento «atoron-jao»— de los fandangos de la tierra, la pobreza de la coreografía y el visualizar siempre el mismo escenario y atalajes, otorgaba determinada monotonia a la representación. Otro ambiente bien distinto fue el vivido en la Peña Flamenca Femenina de Huelva, donde un ramillete de las componentes de su cuadro flamenco, con una mesura digna de la sensibilidad que las distinguen, nos cantaron el adecuado número de fandangos para satisfacernos en una justa medida. Circunstancia ésta que también se produjo en la Peña del Cante Jondo de Moguer, en la que tanto Manuel Ollero como Juan Pérez «Vicentico», pusieron su granito de arena para solaz y diversión de los asistentes. Y aunque el ambiente lo propiciaba, el desarrollo de las extensas jornadas de debate y la cansinería de algunos, produjo la desbandada en la Peña Cultural Flamenca de Punta Umbría, una vez que Paco Toronjo, con su habitual quejío y sus filosóficas letras, evocó personalismos como el de Rebollo y localismos como el de su tierra, aunque con determinadas carencias en los remates. Y es que una progr\n\n[ENDING CONTEXT]\n\nlos duendes flamencos en «Islantilla». Para ello, un cante por bulerías de Juan, que supo ser la antesala al magnífico baile del de Los Palacios. Con semblante serio y sereno —como corresponde a la pres- tancia del baile masculino—, con preciosa y sobria colocación en el escenario, «El Mistela» se arrancó por cantiñas con el compás necesario para ir desarrollando composición de figura, perspectiva del escenario, seguridad en los giros y una preciosa derivación a las bulerías que fueron muestra de la maestria actual del sevillano y que estuvieron en consonancia con su espléndido arte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El arte de Mario Maya y «El Mistela» en Huelva Rafael",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1051,
    "article_char_count_full": 6372,
    "article_char_count_review": 2990,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "representación"
      }
    ]
  },
  {
    "article_id": "1992-09-24-left-presentado-el-libro-antonio-mair",
    "article_text_for_review": "D entro de los actos programa mados por la Junta de Andalucía, al hilo de las celebraciones universales de Sevilla, en homenaje a don Antonio Mairena como Andaluz Universal, y bajo el título: «Honores al Señor Antonio Mairena», el consejero de Cultura y Medio Ambiente de la Junta de Andalucía, Juan Manuel Suárez Japón, presidió la presentación del libro: «Antonio Mairena en el mundo de la Siguiriya y la Soléa», original de Luis Soler Guevara y Ramón Soler Díaz; ambos corresponsales de CANDIL en Cádiz y Málaga. Presentación que corrió a cargo de Manuel Martín Martín, director cultural de la Fundación Antonio Mairena y crítico de flamenco de D16.\n\nAl acto, que tuvo lugar en los salones del hotel Al-Andalus Palace de Sevilla, el día 1 de agosto pasado, asistieron gran número de perso-\n\nnalidades de la cultura, aficionados y artistas flamencos.\n\nEn cuanto a la obra presentada, es producto de la desmedida afición de los autores y del trabajo de investigación rigurosa que durante seis años han llevado a cabo, donde se recogen y analizan casi 1.300 soleares y más de 700 siguiriyes de artistas nacidos antes de 1920, co-tejándolas con las que dejó grabadas el maestro de los Alcores; poniendo de manifiesto la recreación y evolución que estos cantes han tenido en Antonio Mairena.\n\nCumple decir, que los autores objetivan con honestidad sus criterios sin dejarse llevar por preferencias cantaoras personales.\n\nE $ _{s} $ de admirar la seriedad y rigurosidad de la investigación que, tío y sobrino, han llevado a cabo durante esos seis años, así como la metodología empleada en el análi-\n\nCarlos Cruz\n\nTeléfono (953) 441028 sis de datos y las escuchas del material fonográfico; consiguiendo hacer de este libro —casi 600 páginas— un instrumento de consulta imprescindible para todo buen aficionado, y que a buen seguro se convertirá en un clásico del flamenco.\n\nEn el mismo acto se hizo entrega por José Luis Cuberta Graña, presidente de la Fundación Antonio Mairena —entidad patrocinadora— del III Premio de Periodismo y Ensayo Antonio Mairena, dotado con un millón de pesetas, a los autores Luis Soler Guevara y Ramón Soler Díaz, por su trabajo «Origen y evolución de la Siguiriya y la Soleá en Antonio Mairena», trabajo que motivó la obra presentada, y que ha sido coeditada por la Fundación Antonio Mairena y la Consejería de Cultura de la Junta de Andalucía.\n\nFinalmente, se presentó la discografía completa de Antonio Mairena, editada por la Consejería de Cultura, recogida en una colección de dieciséis compact-disc que antologiza todos los registros sonoros que impresionara el maestro, en un período de más de cuarenta años (1941-1983).\n\nSólo resta desear que el libro tenga la distribución nacional que la obra se merece, por parte de la Fundación Antonio Mairena, y no se quede en el mero objetivo de recuperar la inversión. Creemos que el trabajo realizado y la insigne figura de Antonio Mairena demanda el esfuerzo de la Fundación como uno de sus objetivos prioritarios.\n\nNota: Todos los aficionados que estén interesados en la adquisición del libro, pueden dirigirse a Francisco Celaya, Plaza Santa Cruz, 2, 2.°-E, Sevilla - 41004, y a C.E.F.Y.C., S. A. Decano Félix Navarrete, 2, Málaga-29002. O bien a los autores Luis Soler Guevara, Avda. de Holanda, bloque 1-C, 4.°-D, Algeciras (Cádiz), teléfono (956) 653734, y Ramón Soler Díaz, Pasaje San Fernando, 3, 9.°, Málaga - 29002, teléfono (95) 2323072.\n\nRosario López",
    "title": "Presentado el libro «Antonio Mairena en el mundo de la siguiriya y la soleá» de Luis Soler Guevara y Ramón Soler Díaz Pedro",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 565,
    "article_char_count_full": 3439,
    "article_char_count_review": 3439,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-09-24-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "Discutir con Félix Grande acerca del contenido de este libro, constituyó la más hermosa disputa sobre la intangibilidad de la poesía que me ocurriera durante el verano de 1991, fecha en la que, bajo su dirección, un grupo de aficionados debatíamos en El Escorial sobre el papel de los intelectuales en el flamenco. Y he dicho lo anterior porque difícilmente se puede bajar a un poeta de la talla de Félix de la seguridad de sus convicciones. Cuando uno se enfrenta al debate científico, resulta fácil desmontar las opiniones del contrario si uno tiene la suficiente tenacidad como para ir tirando por tierra las teorías opuestas a las nuestras, a base de una constatada y sería argumentación de conclusiones bien probadas y que, al mismo tiempo, contengan datos suficientes que invaliden los de nuestro antagonista. Ya saben ustedes que, como decía Antonio Machado: «la verdad es la verdad, dígala Agamenón o su porquero», y hasta el mismo Juan Pablo II ha tenido que admitir, algo tarde, es verdad, que Galileo llevaba razón al afirmar que la tierra giraba en torno al sol.\n\nPero eso no vale con un poeta, porque su verdad no es unívoca ni demostrable, sino que un día se le agarra al alma y lo zarandea con la misma fuerza con que los atardeceres del otoño pueden hacernos cambiar las más arraigadas convicciones, transformando el frío en brisa benefactora o la decadencia del esplendor natural en sublima «García Lorca y el Flamenco» Félix Grande Editorial Mondadori. Madrid, 1992.\n\ndos estados psicológicos de lo in- animado.\n\nY es que un poeta es siempre un poeta, y si, por añadidura, está criticando la labor de otro, que además se apellida García Lorca, la argumentación se torna imposible, ya que le perdoma cosas tales como considerar al cante flamenco: «creación del pueblo español», o que afirmara sin rubor que la taranta y la romera son cantes gitanos por excelencia. Para Félix, nada de esto tiene importancia, puesto que: «A pesar de sus ignorancias ocasionales, volando sobre ellas, corrigiéndose a sí mismo y, en una palabra, conduciendo el conocimiento poético hasta mucho más allá de su propio saber intelectual interesado por un fenómeno expresivo, García Lorca nos dejó algunas iluminaciones inéditas e irrepetibles» (página 35).\n\nEstamos de acuerdo, Félix. Quizá en El Escorial, el año pasado, sobró pasión y faltaron ventanas poéticas abiertas para respirar vuestro aire. Ahora tu libro lo ha dejado bien claro. De los juicios de Federico sobre el flamenco hay que aspirar el olor de la planta, nunca abrazarse al tallo. El granadino hablaba, como en su preciosa definición de «El Duende», «desde la planta de los pies», y ahí se equivoca, puesto que prima, sobre la razón, el conocimiento.\n\nAdemás, el autor del libro que criticamos se reboza en una hermosa transcripción de la vida y la muerte de Federico en términos flamencos, y nos transmite la sensación de que el autor de el «Romancero gitano» vivió alientos jondos y transpiró el último suspiro dentro de la estética de nuestro arte. Desde luego, si todo ello no es cierto, cosa que yo no sé, aunque mantengo mis dudas, está magistralmente contado. De corazón a corazón. Como dijimos en su día de las «Memorias del Flamenco» y de esa «Agenda Flamenca», tan arrebatadora, cuando hablan los poetas y nos transmiten sus emociones, es mejor callarse. Poner en entredicho sus teorías sería como discutir el dictado de la lluvia en otoño.",
    "title": "Aunque no quepa en el papel José Luis",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 573,
    "article_char_count_full": 3414,
    "article_char_count_review": 3414,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
