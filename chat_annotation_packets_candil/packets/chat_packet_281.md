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
    "article_id": "1993-11-13-right-cante-en-la-bodega-poes-a-manuel",
    "article_text_for_review": "A Agustín Gómez\n\n1\n\nU na crujía emplazada entre andanas en tercera. Un aire pleno de aroma y entrañado de solera. La venencia venenciando\n\nun oloroso de yema. Los dueños y sus escribas celebrando buenas ventas: los ingleses compran bien\n\ny pagan con deligencia. Por tal motivo el buen vino —espléndido en su excelencia—\n\nes brindis exaltativo de su marca y de su enseña.\n\nBlasones y banderolas engalanan la bodega como a palacio de reyes, al festejar la cosecha con la solemne alegría de una gran fiesta flamenca.\n\nPatricio Garvey Gómez, señor de casas y tierras, bodeguero jerezano de finas formas señeras, ha levantado un convite para perpetuar sus señas. Erase el año veintiuno del siglo de la belleza, romántico diecinueve uevo en rumbos y estrellas\n\nPatricio Garvey Gómez, campechano a su manera, irlandés y jerezano por la sangre de sus venas, puso la gloria bendita encima de cada mesa. La nobleza jerezana con su enjoyada majeza, en landós y limusinas fue llegando a la bodega. Y en la calle Guadalete no cabía una pimienta. El crepúsculo encendía los ojos de las doncellas, los rizos de los gitanos, los pechos de las flamencas, las cuerdas de las guitarras, el amor de las marquesas, los encelos de los dandys con lechuguinos y horteras...\n\nPatricio Garvey Gómez, amo y señor de la fiesta, con su diamante más caro adornando su chorrera\n\ny cinco sortijas de oro en la izquierda y la derecha, le dijo a su mayordomo: Es hora de abrir la puerta.\n\nEntraron pobres y ricos, gente joven, gente vieja, y viajantes y franchutes, ediles, jueces, albaceas, malletos y capataces, comerciantes, costureras,\n\ntoreros y matarifes, corredores y alcahuetas, profesores de números y profesores de letras, y pintores y banqueros en muy extrañas parejas, generales con fajines\n\ny cruces en la guerrera, la doña desconocida que nadie sabía si era si princesa o periquita, si se coló por las buenas o es la novia del señor que de Sanlúcar se acerca... Y hasta aquel tonto del barrio, del Mamelón y Alameda, también entra a divertirse a través de la gatera. Sirven las primeras cañas los sirvientes con libreas y se enciende en el ambiente una flama que es la greca del arranque de la copla en la voz siguiriera.\n\nPatricio Garvey Gómez, cabal donde los hubiera, mandó hacerse el silencio y señaló a la morena Tía Salvaora del Muro, ancha de cara y caderas, con un clavel en el pelo y un lunar sobre la ceja. la guitarra le hizo un guiño\n\ny remató la falseta. Ella se buscó la voz y fue sacándola afuera\n\ndesgarrada y condolida como piedra de cantera. Su cante por siguiriya trajo a la fiesta la pena. El vino lució de nuevo como brilla una patena. Patricio Garvey Gómez, se sirvió de la botella y dio su caña a beber a una mujer de bandera,\n\na Mariquita la Jaca, gitanita veinteañera, hermosa como una mata de nardos en su maceta. Y ella le correspondió\n\ncon la toná de su abuela. La concurrencia se anima cuando la noche penetra por el ras de las ventanas atravesadas de rejas. La luna en el alto cielo se queda de pronto quieta y con su luzbrujería más clara que nunca fuera ilumina enteramente a Jerez de la Frontera.\n\n2\n\nLa madrugada se ciñé luceros en su escarola. Canta Luis El Cautivo, el de la cara mazorca\n\ny vocecita laína para ser tan dolorosa. El Puli y Manuel José se desafían por rosas. Tío Corro y Manuel Jesús, que llevan la misma ropa —la blusas de rayadillo y verdes las medias botas—, cantan los cantes del campo mentando las amapolas. Y Tío Diego El Picaor de repente se emociona con la propia tragirrabia que le fue echando a la copla.\n\ny 3\n\nPatricio Garvey Gómez que siente el cante de veras, manda servir más jamones, chicharrones y mollejas, mientras el vino recorre gargantas como piqueras.\n\nY al sonar las cinco en punto retumbando por las tejas, Vicente y Juan Macarrón recuerdan viejas consejas, historias de amores puros y de tragedias toreras. Los presentes sintieron latir el alma y su estela: era el pellizco del cante, el misterio que lo alienta. Patricio Garvey Gómez rompió su blusa de seda, tiró por alto el sombrero convertido en revolora\n\ny sobre el barril de gasto apoyó su corpulencia porque una lágrima llaga le lloraba en la conciencia.\n\nEl Cuadrillero y sus hijas La Custodia y La Vicenta se arrancaron por jaleos y las guitarras veredas de los tocaores castizos, cual soles contra tinieblas,\n\nformaron un alboroto con tanta enjundia flamenca, que cuando al rayar el día se abrió de nuevo la puerta, la bodega, en su silencio hecha catedral desierta, era ámbito y recuerdo del milagro de la fiesta.\n\nEl cante, el baile y el toque dejaron allí la esencia de un pueblo de antigua casta con el arte por vivencia.\n\nPatricio Garvey Gómez, desbrochadas las prendas y la bolsa de los reales vacía de consistencia, recompuso la figura y buscó a su sanluqueña.\n\nAmanecía en Santiago, el barrio entero despierta y en toda casa gitana se encendía la candela: sobre cada anafe, al fuego, una gran olla de berza.\n\nManuel Ríos Ruiz",
    "title": "Cante en la bodega Poesía Manuel",
    "periodical": "candil",
    "issue_id": "1993-11",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 868,
    "article_char_count_full": 4949,
    "article_char_count_review": 4949,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-11-14-left-ayer-y-hoy-del-flamenco-en-catal",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFrancisco Hidalgo\n\nHora es de conocer la historia, de desvelar el pasado, de defender la verdad y de erradicar la ignorancia, los tópicos y las falsedades con que algunos gustan de enmascarar el flamenco, sus manifestaciones y su presencia en Cataluña.\n\nSe equivocan quienes sitúan la presencia y la divulgación del flamenco en Cataluña a partir de los grandes movimientos migratorios de los años sesenta de este siglo, cuando miles de andaluces llegaron a ella en busca de un puesto de trabajo. Mienten quienes afirman que la manifestación de este arte en Cataluña es una imposición franquisita. Quienes así se manifiestan y defienden una u otra teoría públicamente están demostrando, nada más, pero también nada menos, una ignorancia supina, un total desconocimiento de la historia de Cataluña\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nuienes así se manifiestan y defienden una u otra teoría públicamente están demostrando, nada más, pero también nada menos, una ignorancia supina, un total desconocimiento de la historia de Cataluña (del flamenco ni es necesario señalarlo) o que manipulan la verdad por intereses particulares y partidistas. Cataluña, y más concretamente Barcelona, ha sido desde hace más de un siglo, por diversas razones que enumeraré, uno de los enclaves donde el arte andaluz, el flamenco, ha tenido una difusión notable fuera de su ámbito natural. Hasta tal punto ha sido así, que en Barcelona nació una de las máximas figuras del baile flamenco de todos los tiempos por personalidad y universalidad, la inmortal Carmen Amaya. Pero no sólo ella. Que si así hubiera sido tan sólo tendría categoría de anécdota, a pesar de su inmensidad artística. Hay que reconocer que si pocas, contadas veces, el genio flamenco, el artista cabal, se ha dado fuera del Sur, sea gitano o payo, Cataluña ha sido la tierra donde han brotado figuras con categoría y calidad para ser consideradas relevantes dentro del mundo flamenco. Si hiciéramos una relación de artistas flamencos nacidos en Cataluña, el resultado sería superior al de muchas provincias andaluzas. Tan sólo Cádiz, Sevilla y, tal vez, Córdoba, la superarían. En todo caso la nómina es superior a la que hayan podido dar Jaén, Almería o Granada. Como muy superior es a la de otras zonas tenidas tradicionalmente por más flamencas, Murcia y Extremadura. Asimismo, en un hipotético «ranking», Barcelona ocuparía el tercer puesto, tras Sevilla y Madrid, por número de «Cafés Cantantes». Todo ello que pudiera parecer insólito a primera vista, tiene su explicación y justificación. Agentes introductores del flamenco en Cataluña Dos son los agentes básicos de la introducción, y posterior desarrollo, del flamenco en Cataluña. De una parte, la inmigración andalu\n\n[ENDING CONTEXT]\n\nMartínez, a Chicuelo, a Manuel Castilla, a Juan Antonio España, compartan con Paco de Lucía la honda perplejidad cuando Cañizares puntea. Y también el baile ahí. Las jovencísimas y prometedoras Mónica Fernández, realidad plena ya, casi; Susana Escoda, Rosana Romero... Toda una constelación abierta, espesa y feliz, improbable en otros lugares, real y cierta. Criados en las duras periferias de Barcelona o en la hosca y dura gran ciudad. Incansables al desaliento, caminantes decididos por la dura ruta del sinvivir diario. Presente impagable y esperanza de futuro cierto que, ojalá, todos gocemos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ayer y hoy del Flamenco en Cataluña",
    "periodical": "candil",
    "issue_id": "1993-11",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "14-16",
    "page_number": 14,
    "word_count": 2813,
    "article_char_count_full": 17546,
    "article_char_count_review": 3514,
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
    "article_id": "1993-11-17-left-aunque-no-quepa-en-el-papel-jos-",
    "article_text_for_review": "Durante los días 24 y 25 del mes de octubre de 1992, y organizado por la Peña Flamenca de Jaén, tuvo lugar en esta capital el Primer Congreso de Críticos de Arte Flamenco, una reunión importante debido precisamente a su carácter constituyente y de la que ya hemos dado cuenta desde las páginas de «Candil» en los números siguientes a la celebración de tal evento. Ahora, la Diputación Provincial ha tenido a bien publicar en un cuidado volumen, las seis ponencias que se desarrollaron como base fundamental en la que sustentar la polémica acerca de la creación que se vislumbraba en este primer Congreso de los responsables de la Crítica Flamenca. Como, debido a su interés, en esta misma revista hemos publicado tales intervenciones, nos limitaremos aquí a reseñar sus contenidos.\n\n«Criticica y Flamencología», de Aurelio Gurre Chalé, señala la frontera entre ambas actividades, aún consciente de la dificultad que ello entraña, pero seguro de las diferencias de sus competencias, pues si bien el flamencólogo mantiene un carácter más teórico y general, el crítico deberá saber explicar, clasificar y juzgar lo que está viendo y escuchando en cada momento, con unos contenidos más inmediatos y capaces de dilucidar sobre la marcha la barrera entre lo auténtico y lo falso.\n\nMiguel Acal Jiménez, en «La Unión como fuerza necesaria», defendía la necesidad de la profesio-\n\nVarios autores.\n\nDiputación Provincial. Jaén, 1993\n\nIndice\n\nCritic a y Flamencología Aurelio Gurrea Chalé\n\nLa unión como fuerza necesaria Miguel Acal Jiménez\n\nEl lenguaje de la crítica flamenca en los medios de comunicación Agustín Gómez\n\nDel ayer al futuro: Características del Periodismo Flamenco Manuel Ríos Ruiz\n\nDeontologia y funciones de la crítica flamenca Manuel Martín Martín\n\nLa pureza en el Flamenco Paco del Río\n\nnalidad en la función de la crítica flamenca, que deberá de ser continuada, retribuida y con criterios claros y perceptibles por parte de quienes deben de acogerse a sus opiniones. Para ello es preciso la unión de estos profesionales, sin rencillas ni malas artes, sino con la meta común de la defensa del mundo flamenco y la consolidación laboral y humana de quienes a tal ejercicio crítico se dedican.\n\n«El lenguaje de la crítica flamenca en los medios de comunicación», de Agustín Gómez, ponencia presentada en su momento con un gran despliegue pedagógico, analiza los diferentes lenguajes que el ejercicio crítico debe apurar a la hora de llevar a cabo sus planteamientos; métodos distintos según que el soporte de tal actividad sea la prensa escrita, la radio o la televisión. El ponente lleva a cabo en su trabajo un supuesto crítico de cada uno de los casos aludidos, señalando en todos ellos cómo el que analizare la función jonda deberá hacer traslúcido e inteligible el mensaje que desea hacer llegar al receptor de dicha crítica. La amplia experiencia de Agustín Gómez, hace que sus reflexiones, por ejemplo acerca de la polémica soleá de Charamusco, constituyan un modelo evidente de cómo se debe de ejercer la crítica flamenca en radio, anteponiendo el documento riguroso a la especulación apasionada.\n\n«De ayer al futuro: características del periodismo flamenco», de Miguel Ríos Ruiz, más que una ponencia al Congreso, constituye una interesante aportación teórica del flamencólogo jerezano, a manera de elaborada conferencia en la que se nos desmenuzan los orígenes y posterior desarrollo que el periodismo flamenco experimentó\n\nRosario López\n\ndesde los viejos papeles del siglo XIX a nuestros días.\n\nTeléfonos (953) 253139 Mucho más a propósito para el desarrollo de las sesiones fue la presentación, por parte de Manuel Martín Martín, de su «Deontología y funciones de la crítica flamenca», sin duda la que más polémica desató en el momento de sus públicas exposición y defensa. Como código deontológico básico, defiende Martin la insobornable defensa que el crítico ha de hacer de los intereses flamencos, anteponiéndolos a los suyos propios, incluso, como llega a afirmar en un párrafo: «ha de ser capaz de todos los sacrificios en holocausto del flamenco». Más tarde, y entre las funciones que ha de asumir la crítica, señala las de: informativa, denotativa, didáctica, protectora, orientadora y lo que él denomina «función respuesta», mediante la cual el destinatario de la crítica ha de participar, en cierto modo, del placer de descubrir las sensaciones que observó en el crítico; todas estas funciones han de estar obligatoriamente embridadas en la anteriormente citada ética profesional, sin la cual dichas funciones resultarían postizas o manipuladoras.\n\nFinalmente, Paco del Río, en «La pureza en el flamenco», desgrana una serie de consideraciones sobre cómo este requisito debe ser inalienable en las reivindicaciones de toda función crítica, eso sí, sin fanatismos ni exclusiones de ningún tipo.\n\nEn resumen, creemos que este libro ayudará a comprender mejor a todos aquéllos que no tuvieron ocasión de acudir al Congreso, las bases teóricas sobre las que se van a sustentar en el futuro las diferentes actividades de quienes cargan sobre sus espaldas con la grave responsabilidad de enjuiciar los hechos flamencos.\n\nNiño Jorge\n\nTeléfono (953) 275687 ALREO de la FIESTA GITANA",
    "title": "Aunque no quepa en el papel... José Luis",
    "periodical": "candil",
    "issue_id": "1993-11",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 830,
    "article_char_count_full": 5203,
    "article_char_count_review": 5203,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-11-17-right-alre-de-la-fiesta-gitana-miguel-",
    "article_text_for_review": "Dibujos de Miguel Alcalá del libro «Le Flamenco et les gitans», Editorial Filipacchi, París, Francia, reproducidos bajo licencia del autor.\n\nTextos de Manuel Martín Martín\n\nMaría la Burra.—María Fernández Flores (Jerez de la Frontera, 1931). Hija de Manuela Flores, gitana de Las Cabezas de San Juan, y del Tío Borrico, de quien adopta el sobrenombre y hereda sus cantes. Es, por tanto, el último rescoldo de una dinastía verdaderamente gloriosa donde el clan familiar resume siglo y medio del mejor cante gitano de Jerez. Un mal casamiento la llevó a Sevilla para sufrir las mil y una ducas y sacar adelante a sus hijos. Soleares al golpe y bulería son sus altas credenciales exhibidas tanto en festivales como formando en los espectáculos «Los hijos del hambre», «Cantando la pena... la pena se olvida y «Casta». E n mi trabajo anterior, publi- cado en esta revista, omití al- gunos datos biográficos del buen cantaor «El Chato de las Ventas». Y lo hice porque en el momento de disponerme a escribir no los ten- nía a la vista. Los había trabucado depositándolos en el fondo de un mar de «papeles flamencos». Hoy, una vez localizados, quiero, y así lo hago, ponerlos a disposición de los lectores de «Candil».",
    "title": "Alreó de la fiesta gitana Miguel Alcalá-Manuel",
    "periodical": "candil",
    "issue_id": "1993-11",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "17-20",
    "page_number": 17,
    "word_count": 206,
    "article_char_count_full": 1211,
    "article_char_count_review": 1211,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-11-21-left-m-s-sobre-pedro-mart-n-alonso-el",
    "article_text_for_review": "Manuel Yerga Lancharro\n\nSiempre creí que el cantaor se llamó Tomás, porque así lo dijo Sabicas cuando le acompañó en la ejecución de una Soleá: «¡Qué bien cantas Tomás!». ¿Que por qué lo dijo así? Lo ignoro.\n\n«El Chato de las Ventas» vino al mundo dentro del seno de una familia muy humilde, formada por el matrimonio de Alejandro Martin con Juliana Alonso. Vivieron en la calle Mayor, número quince, del pueblo de Illescas, de la provincia de Toledo. Alejandro, deseoso de poder prosperar, para bien de su familia, se trasladó a la villa de Madrid, fijando su residencia y la de los suyos en el barrio de las Ventas.\n\nPedro, siendo aún muy joven, se aficionó al cante flamenco, y con inusitadas ganas de aprender se dedicó, en sus horas de asueto, a visitar cuantos cafés cantantes y colmaos existían en la capital, dedicados, especialmente, al especáculo flamenco. En uno de esos colmaos tuvo la suerte de conocer a la genial cantaora Paca Aguilera, que llegaría a ser su maestra en los cantes por soleá y malagueñas.\n\nGracias a su bien decir el cante, los empresarios pusieron su vista y oídos en el illescano para contratarlo. Así pues, pronto actuó como profesional por toda España, enrolado en aquellas célebres compañías.\n\nInteresado por los cantes de Triana, aprovechando esas fechas de poco trabajo de la estación invernal, se desplazó a Sevilla en busca de sus amigos y compañeros, Felipe el de Triana, Paco el Boina y Antonio el de la Calzá. Una vez localizados se pusieron a su disposición y le cantaron lo que Pedro quería aprender (aún conservo la\n\n¿Qué Perote? Perotes son todos los naturales de Alora (Málaga).\n\nSeñor Bellido Soler.\n\nCon los pocos datos que usted me proporciona no podré complacerle de forma total; no obstante, sí le diré algo sobre el yerno del guitarrista, «El Maestro Pérez».\n\nEl Perote de quien usted de- sea conocer esos datos se lla- mó Juan de la Cruz-Ramón Trujillo García. Nació hace 163 años.\n\nFueron sus padres Francisco Trujillo y María García. Sus abuelos paternos, Francisco Trujillo y María Sarmiento.\n\nSus abuelos maternos, Juan García y Antonia Gil.\n\nTodos fueron naturales de la ciudad de Alora.\n\nSobre sus cantes muy poco se sabe en la ciudad donde nació, sin duda por residir desde bien joven en Sevilla. Pero podemos asegurar que fue un buen cantaor por malagueñas.\n\nLe recomiendo que para escuchar su estilo por malague-ña pueda localizar el disco del señor Diego «El Perote» (Diego Beigbeder Morilla). Creo que fue quien mejor cantó por su homónimo Juan. ¡Ah!, se me olvidaba, otro que cantó muy bien esa malagueña fue Sebastián Muñoz Beigbeder, «El Pena».\n\nEsperando haberle podido complacer, le saluda.\n\nManuel Yerga Lancharro fotografia que se hicieron). Este, que sin duda debió tener un buen oído musical, regresó en pocos días a Madrid con los cantes aprendidos.\n\nComo en su plenitud artística le cogió la avalancha de los fandangos y los «Cantes de venida»: Milongas, Guajiras, Colombianas y Vidalitas, no tuvo otra alternativa que procurar aprenderlos para poder sobrevivir dentro de esa nueva corriente de interpretación tan al gusto de los aficionados. Dedicado a los cantes hispanoamericanos, logró ser uno de los mejores intérpretes por esos palos. Y nos lo demostró con sus numerosas grabaciones con sus propias letras bufas o jocosas que todos hemos escuchado alguna vez:\n\n«Cataluña pide a gritos que le den la autonomía».\n\nMea culpa\n\nNota: Para los que no leyeran mi trabajo anterior, diré que «El Chato de las Ventas», junto con otros que formaban una «compañía», se encontraban en Cáceres actuando cuando sobrevino el torbellino de la guerra civil y quedaron atrapados y aislados, teniendo que esperar al final de la contienda. Junto con otro cantaor, no llegaron a conocer ese final, porque a él se le paró el corazón y al otro porque... murió sin querer morir.\n\n«El que viva el año dos mil», etc. Nuestro hombre, según me dijo Antonio Tovar Río «El Niño de la Calzada», q.e.p.d., fue la esencia pura de la gracia y de la simpatía.\n\nPor estas cualidades humanas, no fue merecedor de la muerte que tuvo, sólo porque cantara letras republicanas. Su óbito provocado se produjo en la ciudad de Cáceres, el año de 1936, a sus cuarenta y nueve años de edad. ¡Descanse en paz!\n\nUna vez más diré: Que nadie cante letras con significación política. Un aficionado de Sevilla, llamado Manuel Cerrejón, ha lanzado, a través de «Pasare-la» una cassette con cantes de Paco Mazaco. En la carátula de la cassette figura una mini biografía del artista y yo, al leerla, observé que existía error en cuanto al motivo del óbito de Paco Mazaco. En ella se dice que falleció de cirrosis y la verdad es que falleció de tuberculosis pulmonar. El día 21 de enero de 1994 me llama por teléfono el señor Cerrejón para decirme que si él lo había hecho constar así es porque en mi trabajo «De qué enfermedad mueren nuestros artistas», consta que Mazaco falleció de cirrosis.\n\n¿Cómo se produjo el error? No lo sé. Pido disculpas a mis lectores.\n\nInmediatamente después de llamarme, he localizado la certificación de defunción y, efectivamente, falleció de tuberculosis.\n\nCordialmente os saluda,\n\nManuel Yerga Lancharro",
    "title": "Más sobre Pedro Martín Alonso «El Chato de las Ventas» Manuel",
    "periodical": "candil",
    "issue_id": "1993-11",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 872,
    "article_char_count_full": 5163,
    "article_char_count_review": 5163,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
