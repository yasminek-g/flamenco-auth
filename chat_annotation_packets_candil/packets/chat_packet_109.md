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
    "article_id": "1985-03-13-left-algo-sobre-los-cantes-de-estilos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSi queremos entrar, seriamente, en el origen y nacimiento de los diferentes cantes y estilos que abarca el extenso campo o expectro musical del arte flamenco, es necesario recorrer y conocer la historia del solar de donde nacen expontáneamente cada uno de ellos. Por esta razón nosotros, en esto del cante, queremos partir de un axioma y no sólo de la vulgar hipótesis que tanto, desgraciadamente, ha proliferado y prolifera en la bibliografía flamenca y en las diferentes versiones que unos y otros, con perdón, mantenemos, a veces, erróneamente.\n\nEs evidente que nosotros como oriundos de la Región Murciana, y más concretamente de la marinera y siempre bella Cartagena, hablaremos, por lo que nos toca, de los cantes de estilos mineros de Cartagena y La Unión, aunque a algunos tal denominación\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestro\"]\n\nmantenemos, a veces, erróneamente. Es evidente que nosotros como oriundos de la Región Murciana, y más concretamente de la marinera y siempre bella Cartagena, hablaremos, por lo que nos toca, de los cantes de estilos mineros de Cartagena y La Unión, aunque a algunos tal denominación le parezca un tanto atrevida. Y para llevar a efecto el presente artículo hemos extraído algunas notas de la biografía que escribimos, en estos momentos, sobre el maestro Antonio Piñana padre, por considerar que tal figura señera es el máximo velador de los cantes de Cartagena y la Unión. Además, en el presente, incluiremos otras observaciones que van íntimamente ligadas a la crítica o distintas opiniones que emanan, claro, de conocedores —así lo creemos— del llamado fenómeno flamenco. Comenzaremos diciendo que toda la extensión del campo de Cartagena, en donde la ciudad de La Unión está comprendida, y su sierra minera ha albergado desde largos años atrás, en lo más álgido de su ambiente, los sones folklóricos de donde surgirán los irrepetibles e inigualables cantes de estilos mineros que años más tarde y de boca del maestr\n\n[ENDING CONTEXT]\n\ny bigarros ornatos del Oriente primario; es nacimiemto, como la mañana del monte, sin parto. Sin aviso, multiplica en sueños oceánicos la velocidad de vértigo entre piedras y dispersos canalillos ruisueños en transparentes carcajadas que se juntan para formar un surco generoso inundando las gargantas de aquáticos frutos dulces; es vida, como la madura creación, sabiduría. Se personaliza en búsqueda roncosa y cristalina ahondándose, ensanchándose, quietándose... Por lentos cauces maestros atina el vientre de la mar que abre su espera inmóvil a la llegada Malagueña.\n\nFrancoise Gerardin\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Algo sobre los cantes de estilos mineros de Cartagena y La Unión",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 1910,
    "article_char_count_full": 11738,
    "article_char_count_review": 2737,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestro"
      }
    ]
  },
  {
    "article_id": "1985-03-15-right-manolo-de-huelva",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMANOLO DE HUELVA\n\n(Imagen y anécdota) (I)\n\nGuitarristas conocidos, compañeros y amigos\n\nManuel, Manolo. Aparece sentado en una buena foto de estudio elegantemente vestido con uno de aquellos trajes cruzados que él mismo —como sastre— se confeccionaba. Al pie de la fotografía dice Fernando el de Triana: «Niño de Huelva» «El amo de los modernos guitarristas», para más adelante ampliar estas afirmaciones elogiosas considerándolo como: «Artista supremo de la guitarra; compositor del más delicado y caprichoso paladar; acompañante limitado a lo que esto debe ser, pues dice, y tiene razón, que entre copla y copla, el que quiera puede demostrar su arte, pero en saliendo el cantaor, se acabaron las flores. Y como ésta es la máxima del Niño de Huelva, esto es lo que lo tiene colocado en primera\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"compás\"]\n\niene razón, que entre copla y copla, el que quiera puede demostrar su arte, pero en saliendo el cantaor, se acabaron las flores. Y como ésta es la máxima del Niño de Huelva, esto es lo que lo tiene colocado en primera línea como acompañante. Como solista, es sencillamente maravillosa su labor. ¡Qué soleares! ¡Qué rosas! ¡Qué seguirias! ¡Qué tarantas! ¡Qué malagueñas y todo lo que toca! Y sin rozar una nota ni separarse un átomo del más estricto compás». Eso lo dice quien supo de eso. Por mi parte yo sabía de Manolo de Huelva y su guitarra pero sin haberlo tratado personalmente. Mas quiso Dios que una noche de Por: Luis Caballero «El amo de los modernos guitarristas» Manuel, «El Niño de Huelva», solista, acompañante excepcional, un supremo artista de la guitarra. san Juan de 1950 viniera a facilitarme la afor- tunada oportunidad de saludarle y cantar con él. Habiéndonos avisado a mi cuñado Pepe Aznalcóllar y a mí para una fiesta sin apenas el tiempo preciso de encontrar «tocaor», tratamos, logicamente, de hallarlo dándonos una vuelta urgente por la inevitable —entonces— y legendaria «Europa» flamenca. De pronto vi-mos al de Huelva fumando y charlando parsi-moniosamente con alguien bajo el quicio de una puerta. Ninguno de los dos nos atrevimos, de momento, a abordarlo. Aznalcóllar lo conocía bien y dudaba pudiera negarse, pero el tiempo apremiaba y sin más remedio y tratándolo de usted, como era habitual en mi cuñado, decidió proponerle su participación en la fiesta. Después de enterarse donde era, quie- nes eran los festejadores y contaores a quienes tendría que acompañar aceptó de la manera más amable y correcta. Recuerdo que la reunión se celebró en una alegre y ámplia azotea, circunstancia que dio lugar, ya bien entrada la noche, a que cierto airecillo le enfriara las manos. Me lo dijo a mí precisamente: «Ya no toco más. Dile a esta gente que abajo sí, pero aquí ni un minuto más». Y guardó la guitarra. Manolo de Huelva, un amante del cante y de la guitarra, de la que Antonio Mairena ha escuchado salir toques, cantes y conversaciones de verdadera técnica artíst\n\n[ENDING CONTEXT]\n\nde su caudal artístico— una intachable profesionalidad.\n\nCreo que la última vez que estuvimos juntos fue en Huelva. Sus paisanos le rindieron un importante, cariñoso y merecido homenaje al que me adherí de los primeros. Se encontraba muy mal; sosteniéndose con dificultad y ayuda. Entre Paco de Lucía y yo lo sentamos en la presidencia de la cena que nos dieron ter-\n\nminado el largo recital. Allí lo dejé, melancólicamente digno, apagándose, sin guitarra ya sobre su andalucísimo corazón flamenco, pero junto a otra figura de la mejor guitarra. ¡Quien sabe si rumiando consejos y rectificaciones!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Manolo de Huelva",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1201,
    "article_char_count_full": 7329,
    "article_char_count_review": 3720,
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
    "article_id": "1985-03-17-left-aportaci-n-del-flamenco-a-los-ac",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDe la Asesoría de Actividades Flamencas de la Consejería de Cultura de la Junta de Andalucía, hemos recibido el programa que a continuación publicamos, por el cual el Flamenco se suma a los actos del V centenario del Descubrimiento. Y lo hace con los cantes de ida y vuelta. Los cantes que se embarcaron, frente al ancho mar de Berberías, en las alforjas de conquistadores y aventureros, hacia las Indias. Los cantes que partieron de Cádiz o del río que ciñe a Triana o de la luminosa Huelva, con el estremecido acento que es, a veces, el término de las lágrimas. Los cantes que anduvieron por atónitos paisajes, míticos ríos, infinitas miserias, y un día tornaron más dulces, más tiernos y entrañables, más atemperados por eco del trópico aún no contaminado.\n\nEl quinto centenario del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"histórico\"]\n\nciñe a Triana o de la luminosa Huelva, con el estremecido acento que es, a veces, el término de las lágrimas. Los cantes que anduvieron por atónitos paisajes, míticos ríos, infinitas miserias, y un día tornaron más dulces, más tiernos y entrañables, más atemperados por eco del trópico aún no contaminado. El quinto centenario del Descubrimiento de América merecía con estricta justicia, esta referencia. Aunque, al parecer, sin suficiente soporte histórico que pueda tenerse por cierto, existe la creencia de que una parte de la música popular española tuvo influencia en algunas músicas folklóricas hispanoamericanas. Más cierto aparece, sin embargo, el hecho de que andaluces que cruzaron el océano generalmente desde el puerto de Cádiz, encontraron en aquellas repúblicas unas formas musicales ya populares que trajeron a España —Andalucía más concretamente— adornándolas con un indiscutible aflamencamiento que las hizo más atractivas en nuestro ámbito. Los toreros, tan afines al flamenco y toda su cohorte variopinta, los gallegos que fueron portadores de la mejor raza de gallos de pelea, buhoneros y truhanes y aquella tropa infinita de postdescubridores, trajeron —que no llevaron— los aires de la Guajira y la Rumba cubanas, la Milonga y la triste Vidalita argentina, la Colombiana, en fin, que en un inevitable fenómeno de aflamencamiento llegaron a convertirse en cantes flamencos, cantes no ciertamente jondos, pero sí ricos en melismas traducidos en un felicísimo mestizaje. Por: Francisco Vallecillo Que Cádiz fue la aduana por la que entraron estas músicas está confirma-do por el hecho de que fueron dos eminentes artistas gaditanos —Pepa de Oro, hija del popular mataor de toros, y Diego Antúnez—, los que dieron forma y rango a las variedades citadas o al menos a la mayor parte de ellas; seguidos ya en la segunda década del siglo por José Centeno, Niño de Marchena, Manuel Escacena, Pepe de la Matrona y varios más que popularizaron unos estilos desafortunadamente casi en desuso en nuestros días. (Populares fueron también Angelillo y El Americano, más ya en la línea decadente y muy poco flamenca). La preponderante y fundamental presencia de Andalucía en las celebraciones del V Centenario del descubriamiento aconseja que en estas vísperas que se avecinan, la Consejería de Cultura realice una campaña de difusión y potenciación de los cantes de ida y vuelta, como aportación del flamenco a los trascendentes eventos que se acercan. LA GUAJIRA Guajiro se llama al campesino blanco en Cuba. Es sin duda la Guajira el cante más representativo de los hispanoamericanos flamencos o aflamencados, tiene obviamente su origen en la isla caribeña y parece ser que llegó a España en la segunda mitad del siglo XIX. La Juan Breva Guajira constituye a su vez una preciosa pieza para el concierto de guitarra. La Guajira es calificada de copla híbrida, influida directamente por el compá\n\n[ENDING CONTEXT]\n\ndurante la semana del 16 al 21 de septiembre con una serie de actos diversos paralelos.\n\nEl concurso requiere de unas fases previas de selección que serían organizadas por la Federación Onubense de Peñas Flamencas en toda la provincia (y en otras si así lo estimara conveniente y el número de incritos lo demandase), cuyos gastos correrán a cargo, en todo caso, de los organizadores, que podrían compensarlos con taquilla.\n\nAl habla con la Federación provincial de Peñas Flamencas, van a estudiar con el mayor interés la mejor fórmula para participar en esta difusión de los CANTES DE IDA Y VUELTA.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aportación del flamenco a los actos conmemorativos del V Centenario del Descubrimiento (Los cantes de Ida y Vuelta)",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1752,
    "article_char_count_full": 10604,
    "article_char_count_review": 4518,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "histórico"
      }
    ]
  },
  {
    "article_id": "1985-03-18-right-del-aciago-marzo-al-funesto-abri",
    "article_text_for_review": "JAIME\n\nA batido y melancólico queda el cronista, Aque siente y ama en esta extraña familia del flamenco, cuando ha de contar a sus hermanos lectores la pérdida irreparable de quienes entregaron su cuerpo y su alma de artistas a este apasionado patrimonio cultural andaluz.\n\nEn un aciago mes de marzo, y en la memoria del buen aficionado, quedaron impactados los nombres de Juan Antonio Montoya Manzano, «Farruquito», (Sevilla, 24-1974, accidente de moto), o Melchor de Marchena (Marchena, 12-1980, insuficiencia cardíaca), a los que hay que añadir el fallecimiento del maestro extremeño Enrique Jiménez Mendoza, «Enrique el Cojo».\n\nEnrique el Cojo estaba a punto de cumplir los setenta y tres años, cuando una maligna trombosis cerebral acababa el pasado día 29 con la vitalidad de unos brazos primorosos que hermosearon el encanto plástico de una Sevilla que supo amar y comprender como nadie a este bailaor de embrujo. Desde la calle Espíritu Santo o desde el palacio ducal de Las Dueñas, el maestro supo desparramar sus enseñanzas impulsado siempre por su increíble capacidad de superación, y sobre todo, descubriendo su misa en el aire espiritual y seductor del crisol sevillano donde el incienso, el perfumado azahar y el duende inexplicable que la circundan, sirvieron de bálsamo, muleta y sostén para una tara física que, en su movilidad, pasó desapercibida. Eran las vísperas de la Semana Grande sevillana cuando los corazones se vistieron de luto porque unos modos danzantes y un estilo discutible partían para un viaje sin retorno.\n\nEnrique «el Cojo», académico de un baile singular y maestro de brazos zalameros y cintura vibrante, había nacido en Cáceres un 31 de marzo de 1912\n\nY del aciago marzo al primaveral y funesto abril. A las ya conocidas pérdidas de Manuel Vega García, «Carbonerillo», (Sevilla 6-1937, tuberculosis pulmonar), y de Manuel Serrapí Sánchez, «Niño Ricardo», (Sevilla, 14-1972, cirrosis hepática), hemos de sumar la del exquisito poeta y gran aficionado José Carrasco Domínguez, la de la soberbia saetera Encarnación Fernández Sol, «La Finito de Triana», y la de Joselero de Morón.\n\nPor amistad, por afinidad y por admiración, hemos sentido muy profundamente en nuestras\n\nentrañas el fallecimiento de Luis Torres Cádiz, «Joselero», ocurrido el pasado día 16 en Morón de la Frontera. Luis había nacido en La Puebla de Cazalla el 23 de enero de 1910. A temprana edad marchó a Morón de la Frontera a casa de su hermano «Joselero» de quien adoptó el sobrenombre artístico. Cuando decidió cambiar los encajes y las «tiras bordás» por los tercios añejos de su suegro Juan Ama-ya y los sones soleareros y gitanísimos de una Triana que se ocultó en la garganta de esta reliquia de cante gitano-andaluz.\n\nQuiero recordar que sería en la II Reunión de La Puebla de Cazalla, allá por el año 1968, cuando lo escuché por primera vez. Aún permanece imborrable en el recuerdo un cante por siguiriyas donde Joselero condensó en una sola copla todo lo que de patético y embriagador palpitaba en este aljibe de esencias incorruptas:\n\nEn el hospitalico mare\n\na mano derecha\n\nque s'ha dejao\n\nla mare de mi arma\n\nla camita hecha.\n\nCon Joselero, los flamencos hemos perdido el latigazo crujiente de unos ecos que nos han herido y dolido hasta remover nuestro sistema nervioso. En definitiva, se nos va un compendio de actitudes vitales, una reliquia intemporal, una peculiar filosofía de vida, preñada del amargo espiritual del pueblo gitano, sólo apta para una minoría de cabales ortodoxos y puros, ante la adulteración engañosa y aberrante de quienes confunden la cultura flamenca con un envilecido comercio de persas.\n\nA buen seguro que Enrique, Pepe, Encarnación y Luis, en el azul estrellado de la Sevilla celestial, colmarán el delirio de los ángeles flamencos. Descansen en paz. Luis Torres Cádiz «Joselero», nace en La Puebla de Cazalla el 23 de enero de 1910 y su fallecimiento se produce el pasado día 16 en Morón de la Frontera.\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N",
    "title": "Del aciago marzo al funesto abril sevillanos",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 662,
    "article_char_count_full": 4017,
    "article_char_count_review": 4017,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-03-19-right-verg-enza-ajena",
    "article_text_for_review": "Dibujo de: ANGEL LOPEZ\n\nPor: Antonio Corcobado\n\nEsto es lo que yo he sentido como español y ferviente aldalucista al ver reflejada en el B.O. del Estado de 29 de noviembre de 1984, la orden n.° 26.421 del Ministerio de Educación y Ciencia que a continuación transcribo para justificar los motivos que me inducen a la crítica que sobre dicha orden, desarrollaré.\n\n26.421, Orden de 2 de octubre de 1984 por la que se autoriza la adscripción a la Universidad de Cádiz de la cátedra de «Flamencología y Estudios folklóricos andaluces» de Jerez de la Frontera.\n\nIltmo. Sr: Vista la propuesta de la universidad de Cádiz de que se autorice la adscripción a dicha Universidad de la cátedra de «Flamencología y Estudios folklóricos andaluces» de Jerez de la Frontera, inscrita en el Registro Provincial de Asociaciones con el n.° 108 con la que se ha suscrito un Convenio de adscripción y teniendo en cuenta lo dispuesto en el artículo 10.3 y disposición transitoria de la Ley 11/1983 de 25 de agosto. Este Ministerio de acuerdo con la Consejería de Educación de la Junta de Andalucía y con el informe favorable de la Junta Nacional de Universidades, ha dispuesto:\n\nPrimero.—Autorizar la adscripción a la Universidad de Cádiz de la cátedra de «Flamencología y Estudios folklóricos andaluces» de Jerez de la Frontera, aprobando el convenio suscrito al efecto entre dicha Universidad y la cátedra, que no implicará incremento alguno del gasto público.\n\nSegundo.—Autorizar a la Dirección General de Enseñanza Universitaria para dictar cuantas resoluciones sean precisas para el desarrollo de la presente Orden. Lo digo a V.\n\nI. para su conocimiento y efectos.\n\nMadrid, 2 de octubre de 1984.— P.D. (orden de 27 de Marzo de 1982) La secretaria de Estado de Universidades e Investigación, Carmen Virgili Rodón.\n\nIltmo. Sr. Director General de Enseñanza Universitaria.\n\nY he sentido vergüenza ajena por considerar que órdenes de esta naturaleza tienen mucho de descalificadoras y negativas, y no debieran tener acogida en el B.O. donde a mi modesto entender tan sólo debiera tener reflejo cuanto de positivo y constructivo fuera capaz de crear el Ministerio de Educación y Ciencia, en este caso.\n\nDesconozco las condiciones en que haya podido suscribirse este Convenio o compromiso entre la Universidad y la Cátedra, pero entiendo que no se hace imprescindible su conocimiento, por cuanto que implícitamente este acuerdo nace con la tara de la imposición de que no ha de implicar incremento alguno del gasto público y cabe preguntarse ante tanta generosidad en la tacañería, cómo piensa el legislador que pueda dedicarse atención y estudio, al desarrollo de algo tan vital por representativo para el Pueblo Andaluz, cuando se le niegan toda clase de medios para la investigación de una de las facetas que más se acusan en su extraordinaria personalidad.\n\nCreo que esto, más que un error, es todo un insulto, a ese Pueblo, sufrido y postergado durante tanto tiempo que no se hubieran atrevido a publicarlo los Gobiernos más reaccionarios que hemos padecido, y éste, no es el cambio lo que le ha ofrecido a esta región, a la que en pago a su acendrado españolismo se la obsequia con una nueva marginación y humillación que posiblemente no olvide. ¿Hasta cuándo van a perpetuar, señores del gobierno, tanta injusticia y desconsideración?\n\nMayor extensión podría dar a esta crítica pero entendiendo que la insensibilidad del autor de este disparate no va a variar por ello, será mejor cerrar esta colaboración con los versos del insigne poeta de esta bendita tierra anda-luza, don Manuel Machado, que ya en sus «dejos fatales» dejaba intuir su desilusión y desesperanza:\n\nEnseñanza del vivir... yo ya no sé qué pensar ni siquiera qué sentir.\n\nCamino que no es camino demás está que se emprenda porque más nos descarria cuanto más lejos nos lleva.\n\nY esta es, señor legislador, la grandeza de ese Pueblo, que al dolor de una más de tantas injusticias a que se le somete, tiene la elegancia de responder con la poesía de uno de sus más preclaros hijos.\n\n¿Cabe acaso, mayor señorío?\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65",
    "title": "Vergüenza ajena",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 704,
    "article_char_count_full": 4240,
    "article_char_count_review": 4240,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
