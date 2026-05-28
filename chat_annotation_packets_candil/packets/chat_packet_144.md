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
    "article_id": "1987-01-11-right-lucas-l-pez",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Martín Martín\n\nrables las ocasiones en que hemos defendido y aplaudido la ingrata labor de quienes sirven al flamenco bajo las siglas de la I.T.E.A.F. Esta misma tribuna ha servido para denunciar la precaria situación de algunos artistas de la tercera edad y, a su vez, para animar a estos hombres que, con su apoyo desinteresado e incondicional, introducen un rayo de felicidad y un goteo de esperanza en muchos hogares flamencos.\n\nEn la cresta de esta menguada ola humana se encuentra el bueno de Lucas López, flamenco «juncal» de nacimiento, sensible al dolor ajeno por naturaleza y lugar-teniente de los tercios jondos des-de su atalaya almeriense por más que insista en «Yo ya me voy a retirar de las apariciones públicas en el flamenco. Esto es espantoso, no te aclaras».\n\nLucas López\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionados\"]\n\nducen un rayo de felicidad y un goteo de esperanza en muchos hogares flamencos. En la cresta de esta menguada ola humana se encuentra el bueno de Lucas López, flamenco «juncal» de nacimiento, sensible al dolor ajeno por naturaleza y lugar-teniente de los tercios jondos des-de su atalaya almeriense por más que insista en «Yo ya me voy a retirar de las apariciones públicas en el flamenco. Esto es espantoso, no te aclaras». Lucas López es de esos aficionados de lujo, imprescindibles en la movida jonda, que lo dan todo Almeriense de nacimiento, sensible al dolor ajeno y con una tarea hermosa, pero desconocida, la ITEAF. a cambio de nada y que gusta llamar a las cosas por su nombre. Suele ir con la verdad por delante, no rehuye al entrevistador y se agiganta ante las dificultades sin convocar a la muchedumbre por descalificaciones. Nuestra redacción quiere mostrar una opinión formada sobre la I.T.E.A.F. y nadie mejor que este apóstol almeríense, sístole y diástole de una tarea hermosa pero desconocida, para situarnos a las puertas de la institución. Además, son muchas las dudas de carácter burocrático que nos plantean los artistas flamencos, referidas a la Seguridad Social, y que esperamos encuentren la respuesta adecuada en esta entrevista. —Lucas, ¿cuándo nace la ITEAF? —La idea se alumbró en una propuesta de Francisco Vallecillo presentada en el VIII Congreso de Actividades Flamencas celebrado en Fuengirola. Se gestó en el siguiente celebrado en Almería, y a lo largo de estos dos sutilísimos puentes recibió su reconocimiento oficial. Fue una tarea laboriosa que tuvo con la aprobación de los estatutos los primeros reflejos de efectividad. lítica Interior— tras la culminación de un expediente laborioso y extenso en el que rindieron informes los ministerios de Cultura, Trabajo y Seguridad Social y el propio Gobierno civil de Almería. —¿Con qué finalidad nace? —El fin esencial de la institución es la ayuda social, económica y médico-farmacéutica a los artistas flamencos de la tercera edad. —¿Esta atención se traduce principalmente en cantidades económicas? La ITEAF nace de una propuesta de Fran\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_02 | trigger=\"falso\"]\n\npor mucho que blasone de ser entendida, aficionada y demás monsergas. —Eso es predicar con el ejemplo. Lucas, ¿podemos hacer balance de estos cuatro años largos de presidencia? —No hay mejor balance que el estadillo económico que te puede facilitar el tesorero. En él encontrarás el destino que ha tenido el dinero recibido, que a todas luces ha sido insuficiente. Yo creo que en estos años habremos repartido unos diez millones de pesetas. —Sin falsos alardes de modestia te puedo asegurar que está lleno de congratulaciones porque tengo la evidencia de que poco a poco se han conseguido las metas que nos propusimos. Para mí no hay nada en el flamenco como haber sido presidente de la ITEAF. —?El balance moral? -Ha habido ingratitudes? —De todo género. De disgustos, ingratitudes e incomprensiones no hemos estado huérfanos, desgraciadamente. Los hemos recibido tanto de gente ajenas al flamenco\n\n[ENDING CONTEXT]\n\nmejor ni más largo que Antonio Mairena, el maestro por antonomasia a cuyo talento creador tanto le debe el cante. El nombre de Mairena se conserva siempre en el recuerdo por su trascendental y original aportación al cante y al arte que le dio gloria. Yo a Antonio se lo debo todo, todo lo que sé.\n\n—Pues sí, que la ITEAF reciba ayuda de todos, desde los medios oficiales a los aficionados, y que la tirantez entre la afición se arregle con buena hermandad, camaradería, unión y colaboración, pero sobre todo que salgan buenos artistas flamencos.\n\n—¿Te gusta el término flamen-cólogo?\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Lucas López",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "11-14",
    "page_number": 11,
    "word_count": 3959,
    "article_char_count_full": 23257,
    "article_char_count_review": 4705,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionados"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "falso"
      }
    ]
  },
  {
    "article_id": "1987-01-15-left-precisiones-a-un-comentario-escr",
    "article_text_for_review": "Aunque no quepa en el papel\n\nde Félix García Vizcaíno «Félix de Utrera»\n\nEdita: Servicio de Publicaciones de la Universidad de Cádiz, 1987\n\nRamón Porras\n\na escueta y desnuda reseña biblio-\n\ngráfica del librito que nos presenta el Servicio de Publicaciones de la Universidad de Cádiz, sin un comentario a la popular y entrañable figura de Félix de Utrera, podría no reflejar el valor humano de estos «Acrósticos del Arte Flamenco».\n\nEvidentemente, valor humano más que valor poético, aunque no sea desdeñable y merezca toda nuestra atención la rigurosa disciplina a que se somete el autor en su ejercicio poético.\n\nHay otros valores que con oportunidad destaca el prólogo de esta obra: el histórico, particularmente. Desde la óptica del guitarrista de Utrera se perfila la semblanza, sucinta y en ocasiones redonda, de numerosos artistas, muchos de ellos ya desaparecidos. Para cualquier aficionado presenta interés la captación que realiza un artista de tan amplia y apreciable trayectoria como Félix de Utrera, respecto a personajes ya míticos, como Joaquín el de la Paula, Manuel Torre o Enrique el Mellizo.\n\nMerece destacarse, en otros aspectos, el ejercicio de solidaridad y reconocimiento que Félix de Utrera practica en relación a numerosos artistas vivos, reconocimiento que es tanto más meritorio, cuanto más abunda el navajeo dialéctico en ese, cada día, más esperpéntico mundo.\n\nEl contenido del Editorial correspondiente al número 47 de nuestra revista, ha merecido el sarcástico comentario de Agustín Gómez en su habitual página de la prensa cordobesa. «El editorialista —dice— o se hace el tonto o no se entera». Todo ello porque en el vilipendiado editorial cuestionamos el concepto de profesionalidad atribuido al flamenco, o dicho de otra forma, porque estimamos equívoca la expresión «artista flamenco profesional».\n\nAgustín Gómez echa en falta nuestra descalificación sin paliativos a los intentos de la Junta por confeccionar un censo de artistas profesionales flamencos; para el crítico cordobés que con harta frecuencia, se erige en profeta iracundo de lo jondo, era nuestra obligación proferir denuestos contra quienes, con las dificultades y falta de precisiones que señalábamos osan emprender la formalización de un censo, entre otras razones porque es absolutamente necesaria la identificación de aquéllos que han de ser beneficiarios de una acción de gobierno determinada.\n\nPor lo visto, para el prístino Agustín, hasta tanto no dilucidemos la noción «artista profesional», debe de aparcarse la política tuteladora de la Junta hacia los artistas flamencos. Frívola conclusión para un frívolo comentario. Es evidente que discrepamos de este punto de vista, pero al querido colega no le placen o mejor no tolera las discrepancias, ya dijimos profeta. Por ello, entra en la diatriba personal, en el infundio, en injuriosas presunciones, al atribuir al editorialista el servilismo propio de quien sólo escribe a los dictados del jefe. Jefe: polvorienta palabra que, sin duda, tiene alta significación en el pedigrí de nuestro, pese a todo, respetado amigo.\n\nPara cualquier lector inteligente, las objeciones alegadas, en nuestro editorial, al criterio adoptado para la confección de un censo, constituyen una crítica contundente del mismo. Pero ello no significa que no estimemos absolutamente necesario el hacerlo, porque lo contrario determinaría un mal mucho mayor: la dejación del deber de tutela hacia los artistas flamencos que con tanta insistencia venimos reclamando de las instituciones. El admirado Agustín Gómez no lo entiende así. Y no hay por qué lamentar este contraste de ideas y posicionamientos. Lo que de verdad lamentamos es ese estilo que genera vejaciones más que reflexión, más insultos que mesura; ese estilo de increpar al discrepante cuando se carecen de argumentos con virtualidad para crear en los lectores convicción. Lo sentimos.",
    "title": "Aunque no quepa en el papel: Acrósticos del Arte Flamenco y precisiones a un comentario escrito de Agustín Gómez",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 598,
    "article_char_count_full": 3880,
    "article_char_count_review": 3880,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-15-right-forman-el-gigantesco-rbol-flamen",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(Extracto de la conferencia pronunciada con motivo del homenaje a José Salazar Molina, «Porrinas»).\n\nE l fandango, desde su origen, es un\n\nbaile andaluz, localizado en la Comarca de Verdial, de la provincia de Málaga, el cual va acompañado, por una parte, de diversos instrumentos de percusión, y de otra, de un cante cuyo origen se desconoce.\n\nUnos lo creen de procedencia oriental y otros lo suponen de nacimiento malacitano. De este cante «prototipo» que iba asociado al baile fandango, se derivó o extra-jo una hijuela que más tarde sería aflamencada, asignándosele el nombre de la región donde se produjo tan feliz acontecimiento: verdial.\n\nDespués sería padre de larga vida y de numerosa prole, entre la que destaca el que consideramos su primogénito y al que bautizaron con el mismo nombre\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"segunda\"]\n\nn con el mismo nombre del baile, esto es: fandango. Manuel Yerga Dicen doctores en la materia, que el proceso de su aflamencamiento fue obra de árabes y otros aseguran que lo fue de judíos. De aquí que resulten dos etimologías, una para el fandango como cante flamenco, y otra como cante jondo. Para los sustentadores de la primera opinión procedería de los vocablos árabes «Felahme-ihum» o bien «felah-mengu», en cambio para los que mantienen la segunda de las opiniones procedería del vocablo judío «Jon-Tod». Que me perdonen esos doctores porque rechace con estoicismo sus opiniones. La verdad es que del sufrido y siempre vejado cante flamenco, cada individuo, español o extranjero, puede «despacharse a su gusto». Puede decir de él cuanto se le antoje, porque nadie saldrá a su encuentro para rebatir sus teorías. El arte flamenco es tan misterioso y se halla tan oculto por la nebulosa acumulada en torno a él, en el transcurso de los tiempos, que nadie honradamente puede decir que lo conoce y sabe de dónde procede. ¡Cuánto se ha escrito y cuántos puntos de vista tan dispares se han suscitado sobre su origen! Para el estudioso y diplomático indopakistaní, Aziz Balouch, agregado cultural de la embajada de su país en España y cantaor profesional en la década de los treinta, formando compañía con Pepe Marchena, Niño de Almadén, Pepe Palanca y Ramón Montoya, el cante jondo fue introducido en Andalucía por el gran Ziryab, cantor y músico de naturaleza persa. Este genial musicólogo, que pasó a la historia con letras de oro, se formó culturalmente en el Sindh, cuya región gozaba ya de preemi- nencia cultural y folklórica 2.500 años antes de Cristo. Nos dice, además, el señor Aziz, que Ziryab, para poder llegar al califato de Córdoba, a cuyo palacio fue enviado por el Califa de Bagdad, Harum-el-Baschid, «para enseñar a los músicos andaluces su música y el cante jondo, tuvo que recorrer toda la frontera entre Persia y Arabia, atravesando los califatos de los Abasidas y el reino de los Algarávides». Prosigue diciendo el señor Aziz que «la guitarra fue introducida en España por Ziryab. Que en principio este instrumento musical tenía tan sólo cuatro cuerdas, que la quinta fue añadida por el propio Ziryab y que la sexta ha sido aplicada, relativamente, en época moderna». Que en el Sindh «las gentes recitan versos religiosos y amatorios con melodías de siguiriγas, solea-res y malagueñas y que los marroquíes, pese a su proximidad con España, no han creado, virtualmente, ningún cante flamenco». Para mí, que he investigado durante años y años, el cante flamenco o jondo, nació de las entrañas de Andalucía, madre de la gracia y de los poetas de lujo. Y como el resultado de mis investigaciones ha sido infructuoso, para poder afirmar lo que creo y siento, tengo que apoyarme en lo que considero lógico: en que el alma de las gentes de Al-Andalus es toda ella pura poesía. Me baso en fundamentos poéticos, porque es sin duda lo que mejor define la cualidad creativa y la sensibilidad de los pueblos. De aquí que diga sin ambages que donde haya un andaluz, habrá un poeta. Siendo, para mí, incuestionablemente así, ¿cómo no fueron ellos los creadores del cante flamenco, siendo esta manifestación artística la que mejor les define y caracteriza? Antonio Ortega «Juan Breva», verdedor ruiseñor de Vélez Málaga; arti\n\n[ENDING CONTEXT]\n\nNiña de los Peines...\n\nHe aquí algunas letras de carta- generas clásicas de la escuela cha- coniana:\n\nLos pícaros tartaneros un lunes por la mañana le robaron las manzanas a los pobres jarrieros que venían de Totana. Porque tiro la barrena me llaman el barrenero\n\nverdial, el fandango de verdial y los de ritmo abandolao.\n\nPara mí esta prodigiosa familia de cantes forman el tronco del árbol malagueño, por ello les aplicó, honradamente, las leyes biológicas de la reproducción, porque de ellos nacieron todos los cantes enumerados, así como el resto de los injustamente titulados cantes payos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "¿Forman el gigantesco árbol flamenco malagueño el verdial y los fandangos de Vélez?",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "15-19",
    "page_number": 15,
    "word_count": 5133,
    "article_char_count_full": 29790,
    "article_char_count_review": 4955,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segunda"
      }
    ]
  },
  {
    "article_id": "1987-01-20-left-buzon-flamenco-respuesta-a-enriq",
    "article_text_for_review": "BUZON-FLAMENCO\n\nNos lo cuenta Enrique Orozco Fajardo como anécdota; o él lo cuenta como tal, al referirse a la malagueña (según él, de su creación).\n\nLos que se dedican a escribir de Flamenco a veces cometen errores; otros escriben muy bien, pero no tienen ni idea de la diferencia que hay entre la soleá de Alcalá y la de Cádiz. En eso estamos de acuerdo con Enrique Orozco, pero en lo que no podemos estar de acuerdo es con lo de atribuirse un cante por malagueña que no le pertenece.\n\nCuando estuvo aquí, en Cornellá (Barcelona), cantó esa malaqueña y dijo que era de su creación; la mayoría de los que le escuchábamos, por respeto, nos sonreímos simplemente; nos acordamos de Cayetano Muriel. Pero al leerlo en la revista CANDIL no puedo dejar de intentar, como dice el buen aficionado Manuel Yerga Lancharro, «enderezar este entuerto».\n\nLa malagueña, ESTUVE LLO-RANDO, es sin lugar a dudas del Mellizo, pero en versión de Cayetano Muriel (Niño de Cabra), un cantaor cordobés injustamente olvidado al que su pueblo algo le debe.\n\nEsta malagueña, de la que se quiere apropiar Enrique Orozco, hace mucho que la escuché en una placa de Cayetano. Posteriormente, en los cassettes que selecciona Pepe Arias, de Cayetano, en el volumen primero, la tenemos para el que quiera comprobarlo.\n\nSegún como se mire el farolillo de Enrique, no tendría demasiada importancia, pero lo que pasa es que después vienen las confusiones. En la historia del Flamenco hay miles de errores; unos por ignorancia y otros por falta de rigor del responsable.\n\nCuando escuché a Enrique, aquí en Cornellá, me gustó mucho, porque cantó esta malagueña divinamente, y unos fandangos por granaínas de Chacón muy bonito, pero al decir que eran creaciones suyas me causó disgusto, me sentí engañado; que tenga en cuenta todo el que esté en este mundillo del Flamenco, en la vertiente que sea: escritor, cantaor, etc..., que hoy no se puede engañar al público, por lo menos a lo que al Cante se refiere, no nos van a dar gatos por liebres.\n\nSeñor mío: He leído la revista número 47 y me decido a escribirle para que sepa lo que ciertamente ignora: que desde hace más de treinta años sé cómo se llamó Bernardo el de los Lobitos.\n\nQue es cierto que en la revista número 38 apareció el artista con «los zapatos cambiados». ¿Por culpa imputable a quién? Posiblemente a mí. Posiblemente a la revista. Me da igual. ¿No se imaginó que pudiera tratarse de un error? Cierto es que no es lo mismo decir José Pérez Alvarez que Alvarez Pérez. En aquella fecha no me pareció oportuno comunicar al amigo don Pedro Sánchez que rectificarán el error en la revista número 39, porque para mí la cosa carecía de importancia, pero ahora veo que sí la tiene. Por eso hoy quiero destruir el error en la forma que yo tengo por costumbre. ¡Así!:\n\nFíjese, sênor Martin:\n\nLa revista número 38 es de fecha marzo-abril de 1985. La certificación en mi poder tiene fecha 24 de abril de 1970. Y mi amistad con Bernardo viene de más antiguo aún.\n\n¿Queda, pues, suficientemente aclarado que nunca ignoré el nombre de pila del cantaor alcalaíno? Creo que sí.\n\nHace ya más de 17 años que publiqué una pequeña biografía de él, creo que en «Flamenco» de Ceuta. En ella, como es mi norma, hice constar su verdadero nombre y el por qué fue conocido como Bernardo.\n\nMi correspondencia y entrevistas con él y con el de la Matrona (ambos nacieron el mismo año), durante mis siete años de vida política, fue bastante frecuente.\n\nPrecisamente la última carta que recibí del viejo cantaor traía fecha 29 de noviembre, y cuando la recibí el día 3 de diciembre ya llevaba cuatro días en el otro mundo. Al contestar a su carta recibí la triste noticia, desde Amparo, 92, del fallecimiento de mi estimado amigo.\n\nCordialmente, Manuel Yerga Lancharro",
    "title": "BUZON-FLAMENCO Respuesta a Enrique Orozco",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 658,
    "article_char_count_full": 3767,
    "article_char_count_review": 3767,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-20-right-la-universidad-de-c-diz-inaugura",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJ. A. Pérez-Bustamante de Monasterio\n\nE 1 día 30 del pasado mes de enero, bajo la presidencia del Excmo. Sr. Rector de la Universidad de Cádiz, tuvo lugar la inauguración de una nueva línea editorial del Servicio de Publicaciones de dicha Universidad, titulada «Folclore andaluz y cante flamenco».\n\nEl acto fue organizado conjuntamente por el Vicerrectorado de Extensión Universitaria y el Servicio de Publicaciones, y en él fueron presentados, en una primera parte, los siguientes libros: «Poesía flamenca: Análisis de los rasgos populares flamencos en la obra poética de Antonio Murciano», por M. a C. García Tejera; «Cantaores andaluces», por G. Núñez de Prado; «Cantes gitano-andaluces básicos», por Alfredo Arrebola, y «Acrósticos del Arte Flamenco», por Félix Vizcaíno («Félix de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"Publicaciones\"]\n\n, por Félix Vizcaíno («Félix de Utrera»). Inició el acto el Rector de la Universidad con unas palabras de salutación a los presentes, cediendo la palabra, a continuación, al Vicerrector de Extensión Universitaria, que puso de manifiesto la feliz circunstancia que supone or- ganizar actos de este tipo a través de la colaboración de los servicios de la Universidad con su Vicerrectorado, pasando la palabra seguidamente al director del Servicio de Publicaciones, quien explicó y justificó las razones que han impulsado a la editorial de esta Universidad para iniciar esta nueva línea de publicaciones, a través de la cual la Universidad desea mantener un contacto directo con temas populares, especificamente andaluces, sin perjuicio de seguir manteniendo su programa de publicaciones estrictamente académico, más especializado y asequible para un círculo mucho más restringido de lectores, dentro del ámbito de la docencia y de la investigación universitarios. Se espera, a través de este enfoque publicista adicional, conseguir una mayor comunicación entre la Universidad y la sociedad, interesando\n\n[ENDING CONTEXT]\n\nes la instauración de un Aula Flamenca que, indudablemente, hará mucho en pro de una mayor comprensión y acercamiento de la Universidad a su pueblo, que desde este momento sentirá a la Universidad más cerca, más suya, más entrañable y más comunicativa, borrando así de su mente la imagen de una Universidad exclusivamente profesionalizada, académica, especializada y distante para la gran mayoría de su gaditana población. Se trata, en definitiva, de una forma más de prestar un servicio a la comunidad, de servir y complacer a la ciudadanía, de extrovertirse hacia los más, en fin, de hacer patria.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La universidad de Cádiz inaugura una nueva línea de publicaciones sobre folclore andaluz y cante flamenco",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1437,
    "article_char_count_full": 9011,
    "article_char_count_review": 2722,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "Publicaciones"
      }
    ]
  }
]
```
