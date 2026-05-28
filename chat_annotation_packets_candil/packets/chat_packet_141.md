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
    "article_id": "1986-11-17-right-letras-flamencas",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJosé Núñez de Castro Gómez\n\nAlosno Las cosillas de Alosno Las llevo en el corazón A donde quiera que voy Es para mí un galardón Decir que del Alosno soy\n\nTengo una novia en Alosno Que se parece a una estrella Tengo una novia en Alosno No la hay más guapa que ella Solamente se parece A la Virgen de la Bella\n\nSan Benito es mi patrón Yo soy del Cerro cerreño San Benito es mi patrón Y en el monte tengo un campo Con un jaral que está en flor P'a darle flores ar Santo\n\nAguanta los temporales Como barco en altamar Ya estoy de aguantá Las rarezas de tu mare Que es una enferma mental (F. de Rebollo)\n\nBonito, alegre y campero El fandango de Valverde El Gatillo lo cantaba A la luz de los luceros La luna lo acompañaba\n\nCuando de Alosno sali Catorce años tenía Cuando de Alosno sali Y yo vivo con la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mujer\"]\n\nn la serranía Que Paymogo va cantando Tienen los pastos comunes Cabezas Rubias y el Cerro Tienen los pastos comunes Y yo los tengo contigo Sábados, domingo y lunes, Cabezas Rubias y el Cerro Pasar una noche sin luna Qué triste será en la mar Pasar una noche sin luna Pero es más triste vivir Sin esperanza ninguna Acordándome de ti (F. de Juan M. a Blanco) Niebla Y la sierra con el pico Me gusta la niebla espesa Y el silencio de la noche Y una mujer que me espera En su ventana en Aroche Tiene una fuente escondía Santa Eulalia la minera Donde las niñas solteras Beben el agüita fría Las tardes de primavera Comienzo felicitando a Manolo Herrera por sus afortunadas entrevistas, a esas reliquias flamencas vivientes que tanto prestigian a su revista. Para mí es, con mucho, lo más enjundioso de su habitual contenido. Aunque alejado de aquello que se relaciona con el difícil entrama-do flamenco, cuando se da a la publicidad algún dato biográfico o una simple manifestación artística que no sean correctos, velando siempre por la buena formación de los iniciados en nuestros asuntos flamencos, enseguida me dispongo a salir de mi letargo para EN-DEREZAR ENTUERTOS, por el bien de ellos. Así lo hago hoy para, con todos mis respetos, anular la afirmación de Oliver de Triana en cuanto a que Miguel Borrull era catalán. ¿De qué Miguel Borrull se trata? No lo sé, aunque me imagino que de Miguelito, que era de su época (1899-197...). Me referiré, en primer lugar, a Miguel padre y a continuación, y en síntesis, a sus hijos. Miguel Borrull nació en Valencia el año de 1878, en el seno de una familia gitana adinerada. Sintiéndose capaz de enfrentarse, en solitario, con la vida, salió hacia... ¿Madrid, en primer lugar?, hacia... ¿Barcelona, en segundo? No lo sé. Este detalle tan importante, desde el punto de vista biográfico, lo dejé en la nebulosa por olvido involuntario y ya no me atrevo a volver a bucear para dejarlo despejado. ¿Por qué? Sencillamente porque ya no tengo treinta años y porque, además, no me resulta rentable. Y existe esa duda por un detalle de nacimiento que después leerán para que puedan sacar las deducciones lógicas. En Barcelona contrajo matrimonio con una gitana baila\n\n[ENDING CONTEXT]\n\nno he vuelto a rastrear en la vida de esta prodigiosa familia, por lo que ignoro qué habrá sido de ella. ¡Cuánto me hubiera gustado escribir una amplia biografía de los Borrull!\n\n* * *\n\nNOTA: Agradezco las cartas que me envían algunos aficionados pidiéndome les informe sobre el curso de mis investigaciones en la vida del gran Silverio. Aún no la he terminado, porque me cuesta mucho trabajo salir hacia Málaga y Linares. Los años pesan y además resulta muy costoso. Cual-quier investigación de alguna im-portancia puede suponer un des-embolso superior a las cien mil pesetas. Esto es así.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Letras flamencas",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1471,
    "article_char_count_full": 8449,
    "article_char_count_review": 3817,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mujer"
      }
    ]
  },
  {
    "article_id": "1986-11-19-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "Aunque no quepa en el papel\n\nMANUEL CANO\n\nLa guitarra\n\nHISTORIA, ESTUDIOS Y APORTACIONES AL ARTE FLAMENCO\n\nServicio de publicaciones. Universidad de Córdoba. Monte de Piedad y Caja de Ahorros de Córdoba.\n\nGranada, 1986\n\nse a uno de los profesionales más completos que en el orden de la guitarra (flamenca y de concierto) hayan existido, a lo largo de la historia de dicho instrumento. Hace unas semanas, con motivo de compartir con él la tarea de miembro del jurado que habría de otorgar la III distinción «Compás del Cante», oí de su boca el legítimo orgullo que le proporcionaba el hablarme de este libro que acababa de salir y yo aún no conocía. Ahora, después de haberlo paladeado, creo una obligación difundir las razones que a mi juicio asistían al maestro Cano para mostrar su contento ante el trabajo realizado. Fundamentalmente opino que la obra consiste en la culminación por el momento de una carrera emprendida desde los albores de la vocación guitarrista de este granadino universal; él, que ha dado conciertos en todas las partes del mundo, acompañando a multitud de cantaores con sus flamenquísi-\n\nmos sones y, en fin, ejercido un amplio magisterio desde su cátedra en el conservatorio cordobés, tiene ahora la oportunidad con este\n\nGranadino de nacimiento y cordobés de corazón, Manuel Cano recopila, através de este libro, todas las enseñanzas acumuladas libro de incidir en todos y cada uno de los aspectos por los que ha discurrido tan meritoria labor, y realiza con él ese sueño, largamente acariciado, de compendiar en un trabajo teórico, todas las enseñanzas acumuladas, todas las experiencias recopiladas a través de esa doble vía fundamental para todo trabajo de investigación artística: de un lado el esfuerzo cotidiano del estudio, el estar sobre el instrumento elegido con la paciencia y la dedicación que sólo los grandes son capaces de realizar, y por otra parte, una suma de aportaciones que el artista ha recibido del contacto mantenido durante años con esa dura asignatura del arte popular, ya recopilando falsetas de otros artistas, ya ligando el misterio de su sonanta a ese otro dolor estremecido de las letras de cantaores diversos que han templado su arte en el crisol de la guitarra de Manolo Cano.\n\nEl resultado final de todo ello es este libro, que como la propia actividad del autor, contiene una\n\nlección teórica y otra práctica consistente en dos cintas de casette que compendian cuarenta y tres cantes diferentes de cantaores y guitarristas míticos (Juan Breva/Ramón Montoya, Centeno/Manolo el de Huelva, Manuel Torre/Javier Molina, etc.), que ilustran el capítulo V del libro, dedicado precisamente a recorrer la historia flamenca desde el momento en que las grabaciones primitivas nos permiten aproximarnos a ese bellísimo maridaje entre el cante y la guitarra.\n\nPero hasta llegar hasta aquí, el libro ha desarrollado todo un programa teórico de aproximación a la guitarra flamenca, cuyo contenido resulta imprescindible para todo buen aficionado al arte jondo: comienza con una exhaustiva y a la vez amena descripción de los instrumentos de cuerda, que en su evolución han ido conformando la actual guitarra flamenca; al mismo tiempo se nos habla de los autores que realizaron menciones de los citados instrumentos: Cantigas de Alfonso X «El Sabio», Arcipreste de Hita, Vicente Espinel, etc., a la vez que se nos traza, paralelo a la atinada selección de los textos, una muy cuidada reproducción en esquema gráfico del perfil de dichos instrumentos, la disposición de sus trastes, etc., con lo cual, nuestra visión se amplía con la inexcusable presencia de las imágenes mencionadas.\n\nEl capítulo segundo lo forma un estudio de la canción popular española, y cómo la guitarra ha ido incorporándose paulatinamente a sus contenidos rítmicos y expresivos, para pasar en el capítulo siguiente a la incorporación de la guitarra al arte flamenco, con testimonios que van desde las descripciones del costumbrista Estébanez Calderón y sus tópicas menciones del Fillo o del Planeta, hasta aquellos que el autor considera los últimos bastiones de la guitarra jonda: Melchor de Marchena, Diego el de Gastor, Sabicas, etc., a la vez que se traza una apretada, pero bién, pergueñada biografía de cada uno de ellos y se señala en qué ha consistido su peculiar aportación personal. La segunda parte de este estudio está dedicada al análisis de los diferentes estilos de cante a los que la guitarra presta su apoyo, incluyendo el perfil de los mismos, sus peculiaridades melódicas de compás a la guitarra y una amplia muestra de pentagramas que ilustran a los afortunados capaces de manejar el instrumento a cerca de sus posibilidades sonoras en los palos jondos que son objeto de estudio.\n\nFinaliza el trabajo con el ya reseñado capítulo V en el que se explican uno a uno los cantes grabados en las cintas y un capítulo posterior dedicado a los aspectos materiales de la guitarra, encarnados aquí por los llamados luthiers o guitarreros (Ramírez de Galarreta, Esteso, Barbero, etc.) esto es, a los constructores de aquéllas, que con su grandeza artesana, aportan la base recipendiaria del arte de los posteriores ejecutantes. Por nuestra parte, sólo nos queda aconsejar este libro verdaderamente imprescindible, señalando que su autor, Manuel Cano, ha tenido la grandeza de unir el rigor científico más irreprochable con un carácter ameno, de iniciación verdadera para los que tenemos la desgracia de no ser especialistas ni siquiera conocedores del tema, pero que gracias a este estudio, podremos en lo sucesivo, hablar con mayor propiedad del mismo, y ¿como no?, amar más profundamente lo que ya permanece más cerca de nosotros a través de las sabias palabras de un amigo.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados\n\nEspecialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65",
    "title": "Aunque no quepa en el papel: Manuel Cano. La guitarra",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 960,
    "article_char_count_full": 5887,
    "article_char_count_review": 5887,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-11-20-right-e-hacian-pensar",
    "article_text_for_review": "Siempre que veraneo en Chiclana de la Frontera hago una visita especial a mi amiga (vieja amistad) Encarnación Marín Sallago, «La Sallago», pero este año la he visitado tanto por deseo como por imperativo, pues me llamó a Madrid a principios de verano, diciéndome que era muy importante que la viese. Una tarde de este agosto pasado, acompañado por mi mujer, tuvimos el placer de abrazar y besar a esa veterana cantaora de Sanlúcar de Barrameda; y la dejé que contase: «Recuerdo perfectamente una reunión flamenca con Diego el del Gastor, que siempre me trató con mucho cariño; se estaba cantando y cuando lo hice yo, pues resultó muy brillante —según los reunidos—, una persona me preguntó que si yo era gitana. Le dije que no, como siempre, porque yo siempre he creído ser paya “por los cuatro costaos”. Entonces, “El del Gastor” me cojió por la muñeca y, con una mirada que no puedo olvidar, me dijo bajito: “Niña, tú eres de nosotros, que lo sé yo”. Lo tomé a guasa, pero no lo he olvidado.\n\nCosas como ésa me han pasado con Joselero el de Morón, con “El Perrate”, con La Fernanda, que me reprocha y me dice: “Te va’castigar, por renegar de los tuyos...”. Estos recuerdos y muchos más que resultarían largos de contar me hacían pensar, y mucho más desde que mis primas mayores, las de Algeciras, hace poco me dijeron, que el padre de mi padre fue gitano, y por eso te llamé a Madrid, para que vinieses y te contase cosas que nunca te quise contar, porque mi madre era una mujer de gran rectitud moral heredada de mi abuela, ésa que ya te conté que siendo mujer era celadora del puerto de Sanlúcar. Te estoy hablando por vía de los Sallago, pues mi madre se llevó malísimamente con su suegra. Recuerdo que una vez —ya era yo zagali-ya—, después de una gran bronca, mi madre me dijo que mi abuela había sido una “alegre” en su pueblo, Jerez, y que mi padre era gitano. “—Mamá, tú dices eso por despecho...” Y entonces su formalidad de Sallago hizo que mi madre se callara. Yo de esto ahora he venido a darme cuenta, porque voy atando cabos.\n\nSegún mis primas, mi abuelo paterno fue gitano, de Arcos de la Frontera; que había sido tan guapo como mi padre; que era hijo de un cantaor de otros tiempos, un tal Pedro Marín, que le decían de apodo “El Ciego de la Peña”, y que por coincidencia el apellido Marín lo tuvo mi abuelo, el que yo creía serlo, el padre de mis tíos. A lo mejor por eso se llevaba tan malamente mi madre con su suegra. Ya ves, mi bisabuelo cantaor, mi abuelo y mi padre, cantaores, y por último yo».\n\nLe conté a «La Sallago» que, defendiendo su razón, contradije públicamente a mi buen amigo Francisco Vallecillo, en el Congreso de Jaén, pues afirmaba que a ti te acogía esa rama paterna calé.\n\nComo sé que es tu deseo, escribiré a CANDIL para que ellos publiquen este reconocimiento, y que sepan tus primos que tú decías lo que creías verdad, que no has querido desagraviarlos, y que si tu honradez decía que eras paya, ahora también serás tan honrada como para no negar que tienes sangre gitana. Yo me uno a tu disculpa, para decirle al señor Vallecillo que del mismo modo que él comprende mi defensa, yo comprendo su acierto, pero que «La Sallago» quisiera conocer de dónde él supo su rama gitana; ¿quién se lo contó? Pues lo de Marín y Marín no la dejan muy satisfecha y parece cosa muy cogida por los pelos.",
    "title": "Me hacían pensar",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 613,
    "article_char_count_full": 3334,
    "article_char_count_review": 3334,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-11-21-left-ablan-las",
    "article_text_for_review": "Nueva peña flamenca en Francia\n\nA la ya larga lista de peñas flamencas que proliferan por distintos países, se une ahora una de nueva creación con el nombre de CENTRO CULTURAL DE ARTE FLAMENCO «PACO CEPERO», con domicilio en Mairie-38420 DOMENE (Francia). La junta directiva de este colectivo flamenco es la siguiente: presidente, Juan Dueñas Jiménez; vicepresidente, Antonio Aguilera Burgos; secretario, José Martín Torres; vicesecretario, Cristóbal Ruen, y tesorero, Manuel Ruiz.\n\nDeseamos toda clase de aciertos a estos entusiastas aficionados.\n\nNoche flamenca en Biarritz (Francia)\n\nJosé el de la Tomasa, jefe de lidia consumado, había llegado enfermo, y se le vió atormentado toda la no-\n\nEl gran triunfador de la velada organizada por la Peña Flamenca «La Debla» fue, sin lugar a dudas, un duende algo particular que encarnaron la valentía y el pundonor. Todos los artistas sabían que un público compuesto en su menor parte por aficionados, y el resto (más o menos 400 personas) por curiosos, había acudido en cuanto tuvo noticia de este evento. Ninguna concesión a la facilidad comercial, sino un cante, un toque y un baile «por derecho». Y este público, sorprendido e impresionado, que no se atrevía demasiado a expresar su sentimiento durante la velada, recompensó a los artistas, obligados a reaparecer repetidas veces al final del espectáculo. ¡Bravo! che por no podernos ofrecer una actuación perfecta. Los lectores de esta revista conocen el profesionalismo del sobrino-nieto de Manuel Torre, y lo que pudo arrancarle a su cuerpo cansado sigue siendo impresionante, sobre todo en la segunda parte, en la que después de una mala-gueña notable seguida de los tradicionales verdiales, cantó por siguiri-yas de Jerez y Triana con el tan poé-tico y ligado, pero lleno de dificultades, cambio del Planea. A continuación, José, sentado, cantó por martinete. Luego, avanzando hacia el público, sin micrófono, nos ofreció debla y toná grande, brindando con el corazón su cante jondo y auténtico a este público subyugado. Guitarra llena de emoción retenida la de Quique Paredes.\n\nEn la primera parte, acompañado de Manolo Domínguez, había empe-\n\nLos dos guitarristas mostraron de lo que eran capaces en el acompañamiento del baile de Anunciación Rueda Torres, «La Toná». La artista más completa dentro de su disciplina en el último Concurso Nacional de Córdoba, estuvo a la altura de la ilusión de los organizadores. Tuvo un bonito y sincero éxito en su segunda aparición, bailando por tangos de Málaga con mucho salero, soberbiamente acompañada por el cante de El Moli y Antonio Saavedra, que apoyaban las inspiradas guitarras de Quique Paredes y Manolo Domínguez.\n\nzado su actuación cantando por alegrías, seguidas por la granaína y por sus personalísimos fandangos del Niño de la Carzá.\n\nAntonio Saavedra, cantaor con casta, demostrando una vez más su arte en el cante p'alante, asumió el difícil compromiso de abrir la segunda parte. Ayudado por la acompasada guitarra de Quique Paredes, puso al público en pie con sus entrañables bulerías a las que habían precedido una soleá apolá y unos tarantos muy sentidos.\n\nLos organizadores habían elegido las guitarras de Quique Paredes y de Manolo Domínguez. Estos artistas pudieron expresar su arte que, como dijo tan justamente José el de la Tomasa, «no es de partitura». Quique tocó a gusto unas bulerías que le salieron redondas. Aunque no estuvo en su mejor momento, Manolo, con sus delicadas falsetas, a menudo llenas de melancolía (alegrías y farruca), supo llegar al alma de un público entusiasta.\n\nJosé Mercé ganador del trofeo «Lucas López» de la Peña «El Taranto»\n\nLa Peña Flamenca «El Taranto», de Almería, organiza anualmente el Trofeo «Lucas López», consistente en premiar la mejor actuación de un artista flamenco en cualquiera de los actos organizados por la citada Peña durante el año.\n\nA tal efecto, se reunió el jurado para decidir el ganador del IV Tro-feo, correspondiente al año 1986.\n\nEste jurado, después de larga deliberación, eligió como finalistas a los artistas: José Soto «José Mercé», Antonio Núñez «Chocolate», y José Fernández «Tomatito», y previa votación final proclamó ganador a José Mercé por su actuación en la Peña el día 10 de mayo de 1986.\n\nEl jurado estaba compuesto por las siguientes personas: presidente, don Alfredo Sánchez Fernández; don Antonio Zapata García, don Antonio Verdejo López, don Diego López López, don José Lorenzo Figuero, don Antonio Zapata Roldán, don Enrique Arriola Arriola; secretario, con voz y voto, don Rafael González Jiménez.\n\nRamón Porras reelegido presidente de la Peña Flamenca de Jaén\n\nEn asamblea general ordinaria celebrada el día 29 de diciembre por la Peña Flamenca de Jaén, resultó reelegido como presidente Ramón Porras González, al aprobar la asamblea su gestión y presentarse a la reelección, siendo votado por unanimidad.\n\nRamón Porras hizo una amplia exposición del programa que la Peña Flamenca de Jaén llevará a cabo en el año 1987, entre los que destacan: recitales mensuales, mesas redondas sobre determinados cantes, conferencias y un «Encuentro de Críticos Flamencos», a celebrar en el mes de mayo, en el que estarían representados todos los críticos de las zonas más cantaoras. Este encuentro sería el primero que se celebra en España sobre el Flamenco.",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 846,
    "article_char_count_full": 5294,
    "article_char_count_review": 5294,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-11-22-left-podio",
    "article_text_for_review": "Columna de honor entre las de este podio para el maestro Fosforito, que tras su accidente automovilístico está prácticamente recuperado. Su fuerte naturaleza y auto-disciplina han contribuido decisivamente a esta rehabilitación que nos permitirá seguir contando tras la interrupción invernal con la permanente lección de dignidad y sabiduría flamenca del Maestro.\n\nReconocimiento público a la afición de la histórica villa de Guillena por la constitución de la Peña Flamenca «La Rivera», un nombre más en las entidades que rinden culto a nuestro arte en la ancha geografía andaluza: ahora aquí, al lado de Sevilla misma.\n\nLugar de honor en el podio a una firma comercial andaluza, generoso mecenas que ha puesto los medios para la creación de un trofeo que el día 30 de enero se entrega por tercer año consecutivo. Ahora a Chano Lobato, como antes fue a Fosforito y todavía antes a Manuel Mairena. La Cruz del Campo es la patrocinadora —sponsor dicen los amantes de extranjerismos innecesarios— de esta valiosa aportación que ya ha adquirido carta de naturaleza en el mundo serio del Cante. Esto no es publicidad; ojalá que cada día y en cada manifestación popular importante pudiera divulgarse el nombre de particulares que motu propio y con verdadero desprendimiento coadyuvan a las tareas sociales y culturales de la Administración pública.\n\nha semana de exposición a la víndica pública que juzgara la actitud de un joven cantaor cordobés, punto de mira de la afición califal que, en muy buena parte, ha creído en él; actitud largamente recriminable cuando ha tomado el Cante a puro pitorreo, dedicándose a cantar Siguiriyas con acompañamiento de guitarra, violín y no sabemos si algún extraño y absurdo instrumento más. Por lo visto, el hombre, emulando a otro genio del modernismo flamenco que ya hiciera la misma faena con idéntico cante y música de órgano eclesial, está inventando la pólvora flamenca.\n\nA la picota, igualmente por derecho propio, Nano de Jerez, quien en ocasión que debiera haber sido solemne y a través de un bodrio televisivo, la noche de fin de año, se ha presentado ante millones de espectadores haciendo la vieja, ridícula y antiflamenca escenita del bombero. Gracia que tiene el hombre y que ha querido sacar del estrecho círculo de sus amistades que tantas veces le han reprochado la estulticia del bailecito de caseta de feria con tiro al muñeco...\n\nVENTOLERA",
    "title": "Podio",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 391,
    "article_char_count_full": 2392,
    "article_char_count_review": 2392,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
