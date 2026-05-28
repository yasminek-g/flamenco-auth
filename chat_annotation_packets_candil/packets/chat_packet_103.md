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
    "article_id": "1984-11-13-left-las-letras-flamencas-de-paco-sal",
    "article_text_for_review": "A quel peine de plata lo tiré al río, y te peino y despeino con mis suspiros.\n\nNi los cielos ni la tierra te podrían perdonar, que yo te pido justicia y tú me des-caridad.\n\nTú lo tienes que saber: el que gana por la fuerza: qué fuerza puede tener?\n\nMi mare me lo decía: el campo es un libro abierto que enseña sabiduría.\n\nAgujas de mi reloj que yo las iba arrancando y el tiempo no se paró.\n\nPlacita del Perejil, el ramiño que me dabas el beso que yo te di.\n\nFrancisco Salgueiro",
    "title": "Las letras flamencas de Paco Salguero",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 95,
    "article_char_count_full": 478,
    "article_char_count_review": 478,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-11-13-right-la-bienal-ha-terminado-viva-la-b",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEste año como ya es sabido, se celebró en Sevilla la III Bienal de Arte Flamenco, como exaltación al toque de guitarra; comenzó ésta el día 12 de septiembre y finalizó el 12 de octubre, todo un mes que no está nada mal. Dicha Bienal se desarrolló sobre distintos marcos, tales como el Claustro del Monasterio de San Jerónimo, el Hotel Triana, la Plaza del Lucero, los Jardines de la Torre de don Fadrique, Reales Alcázares y en el Teatro Nacional Lope de Vega, donde se dio por terminada.\n\nResaltar cada una de las múltiples actuaciones sería una labor ardua, por lo cual nos vamos a concretar en resaltar las mejores noches cantaoras, así como los momentos de más claros relieves artísticos.\n\nLa apertura se dio con el «Giraldillo» del baile de la pasada Bienal a Mario Maya, quien en unión de su\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"famili\"]\n\nompañía puso en escena «El Amargo», de Federico García Lorca. Mario bailó extraordinariamente bien, demostrando la obtención y ostentación de dicho galardón; junto a él nombres harto significativos y elocuentes, como Juana Amaya, Rafael de Alcalá, Isidoro Carmona y Manuel de Paula entre otros. Otra actuación notoria y digna de los más altos elogios fue la que tuvieron en una misma función José Menese y José el de la Tomasa, conjuntamente con la familia «Habichuela». artístico, encargándose de ello Fernanda y Bernarda «las eternas niñas de Utrera», así como Joselero y Paco Valdepeñas, todos ellos acompañados por las guitarras de Dieguito, Juan y Paco del Gastor. Noche flamenca donde las haya la que nos brindaron estos grandes artistas. «Soleras» nos mostraron y demostraron Naranjito de Triana, Beni de Cádiz que estuvo en artista toda la noche, Luis Caballero y Miguel Vargas, así como solera, nostalgia, dolor y emoción nos llegarían en el arte incomensurable y en los pies de Antonio «El Farruco». ¡Qué decir de Manuela Carrasco y Pepa Montes, ambas acompañadas a la guitarra por sus respectivos esposos! Qué gran éxito el conquistado en la misma noche por Antonio Suárez, Pansequito y Romerito, todos ellos acompañados por las guitarras de sus hijos. Bajo el título de «Recordando a Die- go del Gastor» subió bastante el tono La «Nueva Triana» nos deleitó con Pata Negra, la soberbia actuación de Juana Revuelo, el buen hacer de Boquerón y la garra, fuerza y compás del baile de Angelita Vargas. El Pele que hacía su presentación oficial en Sevilla, aun cantando bien no demostró ser poseedor de los dos premios obtenidos en el último Concurso Nacional de Córdoba. Esa misma noche quien sí estuvo soberanamente fu\n\n[ENDING CONTEXT]\n\nde Manuel Franco Gutiérrez y Josefina Barón Chinchilla, nació en la sevillaná calle Luis Montoto el día 14 de julio de 1960. Aprendió a tocar o a hacer los primeros acordes con su tío Manolo Barón, y con sólo 14 años ingresa en la Academia de Matilde Corral para continuar su aprendizaje, sobre todo en cuanto a acompañar para bailar se refiere.\n\nQuiero dejar constancia —a la vez que le felicito—, de la recordatoria que hiciera al compañero enfermo José Ca-la el Poeta, al recibir el Giraldillo, hermosas palabras las tuyas, Manolo, donde pones de manifiesto tu humild-dad y tu hombría de bien.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La Bienal ha terminado, ¡Viva La Bienal!",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 1237,
    "article_char_count_full": 7329,
    "article_char_count_review": 3350,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "famili"
      }
    ]
  },
  {
    "article_id": "1984-11-15-left-carta-abierta-y-demasiado-imprec",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nQuerido maestro y amigos:\n\nH e recibido la noticia, gratificante por muchos conceptos, de que está en trance de publicarse su nuevo libro sobre flamenco. Para mí, ello no ha constituido novedad. Sé que la amorosa reflexión sobre lo jondo ha pervivido en usted, y que incluso, en Mar de Plata, ha trascendido a otros amigos que, arrebatados por los sonidos negros, practican audiciones, dialogan sobre contenidos medulares del flamenco, realizan audaces exégesis y, en definitiva, profundizan en esa expresión estremecedora, la más hermosa, tal vez, que pueblo alguno halla creado.\n\nUn silencio, demasiado largo el suyo, maestro. El sólo rumor de que pronto recuperaremos sus análisis, ha generado sinceras expectativas, porque algo está cambiando o se ha transformado ya, desde que se celebrara el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"mentor\"]\n\ns medulares del flamenco, realizan audaces exégesis y, en definitiva, profundizan en esa expresión estremecedora, la más hermosa, tal vez, que pueblo alguno halla creado. Un silencio, demasiado largo el suyo, maestro. El sólo rumor de que pronto recuperaremos sus análisis, ha generado sinceras expectativas, porque algo está cambiando o se ha transformado ya, desde que se celebrara el primer concurso de Córdoba, del que usted fue cualificadísimo mentor. Temo cualquier tipo de sistematización, y mucho más si ésta se refiere a la historiografía flamenca tan plagada de torpes presunciones, tan carente de instrumentos fidedignos para la investigación. Ello, no obstante, tras la década de los sesenta, se producen relevantes concurrencias respecto al fenómeno jondo, hechos significativos, valoraciones, que inducen a pensar en el advenimiento de una nueva época, la época de los Festivales Flamencos. De ella y de otras atropelladas reflexiones quisiera hablarle, maestro, esperando que este arrebato se atempere con la segura precisión de su juicio. Reitero mis reservas hacia cualquier tipo de nomenclatura momificadora. Pero la denominación puede valer, si con ella aprehendo una realidad; o, más exactamente, constato en la misma un listado de despropósitos que cercenan hoy la verdad flamenca. Procuro no incurrir en el viejo dogmatismo de Demófilo. Entre otras razones, porque ya tengo asumido que lo jondo, en est\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_02 | trigger=\"expresión\"]\n\ndirán. En cualquiera de los casos, es lo cierto que lo que fundamentó denominaciones comúnmente admitidas —época de los cafés cantantes, época de la ópera flamenca, etc.— fue el medio o el entorno en el que el flamenco se manifestó. En tal sentido, nadie cuestiona que este medio, hoy, es el Festival Flamenco, y, si ello es así, nuestra propuesta de denominación no está carente de una cierta lógica. Aborrezco el término «Festival», referido a la expresión jonda. Lo festivo es sólo su comparte, que puede resultar hasta grotesto si no se contempla como el vértice de su contrario: el dolor. Y en mitad, hermoso silencio, entre notas discordantes, una tensión humanísima, profunda, incentivadora de la sorpresa que nos sumerge en un hermoso flujo heraclitiano. Durante las dos últimas décadas, todos hemos sido testigos de una espantosa proliferación de estos expectáculos, dentro y fuera de la geografía\n\n[ENDING CONTEXT]\n\nde espectacular explendor que vive hoy el flamenco. En algún sentido el flamenco vuelve a españolizarse, a convertirse en una suerte de folklore oficial andaluz, es decir, vuelve a diluirse ante un público copioso pero desatento, y sobre todo poco receptivo a la comunicación flamenca.\n\nDemasiado arrebato, demasiada im- precisión en esta ya tediosa carta. Pe- ro era una necesidad —al menos la mía íntima— hacerle confidencia de estas inquietudes, a usted que, a través de la lectura de sus libros, generó en mí una hermosa pasión por el flamenco.\n\nSu amigo y admirador.\n\nRAMON PORRAS GONZALEZ\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Carta abierta y demasiado imprecisa a A. González Climent",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 966,
    "article_char_count_full": 6172,
    "article_char_count_review": 4021,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "mentor"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "expresión"
      }
    ]
  },
  {
    "article_id": "1984-11-15-right-ellos-los-protagonistas-dicen",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEn aquella época, rara vez nos acompañábamos de guitarra, casi siempre era haciendo el compás con los nudillos, y ocurría una cosa, que todo lo marcábamos al compás de soleá\n\n—Tendría que empezar diciendo que pertenezco a una familia de una larga tradición cantaora, por lo tanto mis vivencias flamencas me vienen a través de mi familia.\n\n-¿De ahí lo de Fosforito?\n\n—Nosotros siempre solemos empezar lo mismo; ¿cómo fueron tus comienzos?\n\n—Bueno, lo de Fosforito es a través de mi padre, pero esto fue bastante después, porque en principio yo era ANTONIO DE PUENTE GENIL y así aparezco en un cartel en el año 47 en Ronda. Antes de ponerme ese nombre, a mí me conocían como el hijo de Fosforito; un trotamundos que iba cantando de feria en feria de ganao y por las tabernas poniendo la gorra, cantaba\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"compás\"]\n\nndangos y verdiales. Lo mismo que yo, había otra serie de gente y nos juntábamos en Collera, yo iba de compañero de CARLOS EL DE SAUCEJO y de ROSAFINA DE CASARES, que si vive tendrá ya cien años, porque en aquella época ya era mayor. Lugo había otros que no ha trascendió su nombre, porque ROSAFINA sí, hasta grabó, cantó con VALLEJO y era creador de un fandango. En aquella época, rara vez nos acompañábamos de guitarra, casi siempre eran haciendo compás con los nudillos, y ocurría una cosa, que todo lo marcábamos en compás de soleá. Con estas andanzas y esta forma de vida, se cimentó mis conocimientos y mi sentido del compás se acentuó. Luego cuando ya fui mayorcito, tuve la suerte de conocer a un guitarrista en Antequera, que era barbero, porque antiguamente casi todos los barberos tocaban la guitarra, entonces hicimos Collera y nos fuimos por ahí y nos enrolábamos en cualquier troupe, que unas veces nos pagaban y otras no, recuerdo que algunas veces no teníamos ni para pagar la fonda y teníamos que dormir en los portales de las casas, que entonces los dejaban abiertos. Pero donde quiera que hubiera una casa de niñas valientes, allí entrábamos nosotros y por lo menos el plato de comida lo teníamos seguro, luego siempre había alguien que nos llamaba para cantar. Otras veces, hablábamos con el empresario de los cines de verano y le hacíamos la propuesta de cantar después de la película, muchas veces aceptaba, subiendo la entrada una perra gorda, y a nosotros nos daba dos o tres pesetas. Ya en el año 47, teniendo yo 15 años, hice lo mismo, salirme de mi casa y hacer la ruta de la serranía cantando hasta Ronda. En Ronda estuve un año porque encontré a un empresario qué hacía bolos y me contrató, entonces yo iba con EL GITANO DE BRONCE: aún conservo un cartel de Ronda de ese año en el que aparezco como ANTO- NIO DE PUENTE GENIL. —Nosotros creemos que Puente Genil ha sido un pueblo muy cantaor. —Efectivamente, mi pueblo ha tenido siempre una gran tradición cantaora, yo siempre he estado con la oreja puesta en todos los viejos que cantaban. Recuerdo que escuché a «EL SECO», a «MALOS PELOS», a «PINTURAS», por cierto, que éste hacía la malagueña del «CANARIO» perfecta. Es decir, que Puente Genil ha dao un gran manojo de cantaores, aunque, profesionalmente pocos, salvo JUAN HIERRO que incluso grabó y actuó con VALLEJO y CEPERO. Bueno, esto más o menos fueron mis comienzos. —Si lo deseas, puedes conti\n\n[ENDING CONTEXT]\n\ntenía, porque eso lo daban a partir de los dieciséis años. Entonces le dije que era de Puente Genil y cantar, que había ido a buscarme la vida; me preguntó qué edad tenía, le dije que catorce y entonces me pegó la guantá más grande del mundo. Nuevamente me pidió la documentación, le volví a contestar que no tenía, otro guantazo. Me cogió y me llevó a la cárcel, donde me tuvieron quince días preso, pero no en el penal, sino en la cárcel.\n\nNOMBRE: NOMBRE ARTISTICO: FECHA DE NACIMIENTO: DOMICILIO:\n\nAntonio Fernández Díaz «Fosforito» 3-8-1932 Finca «El Zanganillo»\n\nALHAURIN DE LA TORRE (Málaga)\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen. Fosforito",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "15-17",
    "page_number": 15,
    "word_count": 3594,
    "article_char_count_full": 19975,
    "article_char_count_review": 4058,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "compás"
      }
    ]
  },
  {
    "article_id": "1984-11-18-left-buz-n-flamenco-a-juan-p-rez-s-nc",
    "article_text_for_review": "Por JUANERO\n\nA Juan\n\nQuerido Juán; como amigo y como aficionado agradecido, hoy, como todos los años, te recuerdo con verdadera emoción.\n\n¿Y cómo olvidar también a Santos Simarro, cuando compareció ante don Ramón Carmona, en Jaén, para comunicarle la triste noticia de tu fallecimiento, hace hoy dieciocho años? ¡Dieciocho años, cómo pasa el tiempo! ¡Y parece que fue ayer cuando nos vi-mos por última vez!\n\n¿Cómo no recordar con alegría a Manuel Díaz Macías, de San Fernando, cuando compareció ante el señor Juez, don Cayo Puga, para comunicarle la buena nueva de tu nacimiento en la casa de tus padres ubicada en la calle de San Roque, número 95, de la ciudad de Puerto Real?\n\nCreo que los verdaderos aficionados tienen contraída una gran deuda contigo, como cantaor profesional de los buenos, y no pierdo la esperanza de que llegará el día en que todos, de común acuerdo, te la hagan efectiva con creces. Sólo es preciso una llamada de alerta; una llamada de atención a través de un medio de comunicación: re-\n\nvista flamenca, periódico, radio, etc., para que tus amigos y seguidores, que no son pocos, despierten de su «letargo» y se dispongan a trabajar sin desmayo para la consecución de tal propósito.\n\nDe igual forma lo pedí para Manuel Vallejo y llegó el día que tanto anhelé. Gracias a sus seguidores y grandes aficionados, se le hizo el homenaje que se merecía y hoy, Manolo, tiene su calle en la ciudad hispalense, para perpetuar su preclaro nombre artístico.\n\nHe vivido, como tú sabes, la década de los treinta; recuerdo perfectamente la de los veinte y puedo decir que durante ese período, la mayoría de los cantaores gitanos, de forma incomprensible, quedaron «mudos», quizá porque sus cantes no gustaban a los aficionados y sí el cante menos seco, menos recio o «afillao» y más dulce y melódico. Hoy, por el contrario, son los cantaores gitanos quienes con el gran apoyo de los no gitanos, han ocupado una buena superficie de la triangular parcela flamenca. ¿Por qué? Por ese apoyo que continúa todavía. Creo que tal actitud, que no rechazo, ha contribuido en gran medida a que muchos cantaores de valía pasen por esta vida sin pena ni gloria y hayan sido relegados a un segundo plano artístico.\n\nSi algunos escriben o hablan de Vallejo y de ti, es para no perdonaros el que hayáis nacido de padres «castella-\n\nnos» (no gitanos) y para atacaros escogiendo lo que consideran, erróneamente, vuestros flancos más débiles, a saber: que Vallejo tuvo voz de niña y que tú no cantaste a compás. Yo, ante tales versiones, no tengo por menos que rebelarme y decir con énfasis que Vallejo, con su voz, con la que Dios puso en preciosa garganta, FUE EL MEJOR CANTAOR QUE NOS HA DADO LA CIUDAD DE LA GIRALDA. Y en cuanto a ti, que ese mismo Dios te concedió EL MEJOR ECO FLAMENCO DEL SIGLO ACTUAL y que con él cantaste muy requetebién para todos los gustos: con dulzura, a la vez que con gran flamenquismo. ¡Y por bulerías no digamos! Cuando cantabas por este palo, en tus comienzos, los gitanos se disputaban tu «nacencia».\n\nSé que Dios, el casi olvidado en estos tiempos que corren, te tendrá a SU lado escuchando tu voz única; esa voz que EL depositó en tu ser, en el mismo instante de tu concepción, allá por el mes de octubre de 1905.\n\nAl llegar este infausto día doce de diciembre, cumpleaños de tu muerte, llevado por el ímpetu, alzo mi voz contra los desagradecidos y olvidadizos, que son muchos, con la intención de forzarlos a que te recuerden y después, sosegados, agradezcan públicamente tu gran aportación personal a la nómina de estilos flamencos.\n\nYo no dudo, querido amigo, que descansas rodeado de la única y verdadera PAZ. Y tú has de saber que nosotros vivimos sobre una inmensa superficie minada de artefactos maléficos, que nos vienen proporcionando una vida llena de miedo y de inseguridad.\n\nAquella noche cantaste por todos los palos. Cantaste preferentemente lo que el numeroso público te pidió, lo que se cantaba entonces y que te hizo muy popular: «Mari-Cruz», «Con sombrero negro» y tus fandangos personales rebozados de flamenquismo e impregnados de la sal de tu Cai: «Y no vienen mis amigos», «Porque volar no podía», etc., etc.\n\nRecordarás, Juan, aquella noche de agosto de 1934, en el Teatro Domínguez, cuando se te presentó, inesperadamente, en el escenario, Antonio Saavedra, gitano con elegancia y señorío, abrazándote y diciendo en alta voz: «He aquí, señoras y señores, al mejor cantaor gitano por bulerías!».\n\nRestaurante\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Espécialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65",
    "title": "Buzón Flamenco: A. Juan Pérez Sánchez «Canalejas»",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 796,
    "article_char_count_full": 4643,
    "article_char_count_review": 4643,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
