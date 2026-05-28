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
    "article_id": "1988-01-16-right-las-cabales-de-plata",
    "article_text_for_review": "Antonio Corcobado / Corresponsal\n\nLíbo y el envidiable lirismo con que Pepe Verdú, en colaboración con el magnífico poeta y extraordinario conocedor del Cante que es Manuel Ríos Ruiz, lleva a cabo la presentación del programa «El Cuarto de los Cabales», en la noche de los domingos en Radio Nacional de España, 1, nos ha permitido conocer de su interés por dotar al programa de alicientes que inciten a su audiencia y afición flamenca, a participar de manera tan activa que bien pudieran ofrecernos el índice estimable de muchas cosas, y entre ellas muy especialmente la de estimar el grado de perfección en que se encuentra la afición al flamenco en su conjunto.\n\nEstá dentro de lo posible el que la modestia de este «loco» que es Pepe Verdú, ya de vuelta de tantas cosas, me acarree alguna reprensión por los elogios que necesariamente he de hacer al informar de que con su envidiable tesón, ha hecho posible la institucionalización del premio «Los Cabales de Plata» para el Cante, Baile y Toque, con una secuela de actos para su entrega en la próxima primavera, programados con un buen gusto y detalles tan flamencos que son un verdadero excitante para que el premio eche raíz y permanezca en el futuro. pación activa, pues si bien el valor intrínseco del premio no es grande, sí lo es en grado superlativo para colmar emocionalmente a sus ganadores.\n\nEl premio que con su clara inteligencia ha despersonalizado este paladín de nuestro querido Arte, simbolizándolo en una silla de anea en plata sobre peana de madera noble, será adjudicado entre los participantes, suponiéndolos aficionados que dirijan al programa sus cartas manifestando su predilección por los artistas de cada una de las facetas, garantizando la seriedad de los fallos, un jurado, compuesto por personalidades de reconocida solvencia, además de la vigilancia y presencia de Pepe Verdú, más la actuación notarial que se tiene prevista.\n\nConsiderando que este acontecimiento requería toda nuestra atención, obligándonos a concederle la mayor extensión para conocimiento de la afición, igualmente nos obliga a requerir de ésta su partici- Si pudiéramos llegar a conocer el grado de audiencia del programa y dentro de él, el de participación de los aficionados, podríamos haber ido, querido Pepe, mucho más lejos de cuanto se hubiera previsto, al poder contar con datos muy interesantes para valoraciones posteriores que nos permitieran enjuiciar con acierto, aspectos y facetas muy importantes en relación con este arte, sobre los que tanto se viene divagando.\n\nAl felicitarnos por su magnífica aportación para el encumbramiento del Flamenco, hacemos pública, querido amigo, nuestra enhorabuenas con el deseo de que su inagotable fecundidad siga nutriendo el caudal de su ilusión, poniendo a su disposición las páginas de este CANDIL, con el que tantas y tan buenas cosas vamos encontrando a veces en nuestra atenta búsqueda en pro del flamenco.\n\nEl pasado día 29 de enero tuvo lugar en el sevillano Hotel Alfonso XIII la entrega de la 4. a Distinción «Compás del Cante» que, otorgada este año por el jurado a la modalidad del toque flamenco, recayó en la personalidad universal de Francisco Sánchez Gómez, conocido en el mundo artístico como «Paco de Lucía».\n\nEl acto congregó a numerosos invitados, autoridades, prensa especializada y amigos del galardonado, y fue presentado por don Enrique Osborne Isasi, al que siguieron en el uso de la palabra, tras la lectura del acta por parte del secretario del jurado, señor Núñez de Castro, don Francisco Vallecillo Pecino, presidente de aquél, quien resaltó la personalidad de «Paco de Lucía» y agradeció la labor llevada a cabo por la entidad patrocinadora «La Cruz del Campo, S. A.», en beneficio del flamenco. A continuación cerró el acto el director general de dicha entidad, don José Ruiz de Castroviejo y Serrano, que hizo entrega del galardón y pronunció un bello parlamento-semblanza en torno al homenajeado.\n\nEn suma, un acto importante en el reconocimiento del arte flamenco y una personalidad indiscutible, la de «Paco de Lucía» realizada una vez más y al que, desde estas páginas, la redacción de CANDIL desea felicitar efusivamente.\n\nCANDIL",
    "title": "Las Cabales de Plata",
    "periodical": "candil",
    "issue_id": "1988-01",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 680,
    "article_char_count_full": 4169,
    "article_char_count_review": 4169,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-01-17-left-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "EDITA: XV CONGRESO NACIONAL DE ACTIVIDADES FLAMENCAS DE BENALMÁDENA. OCTUBRE DE 1987 CANTAÕRES MALAGUEÑOS Pinceladas Flamencas (1850 - 1950)\n\nJosé Luis Buendía López\n\nGonzalo Rojo Guerrero\n\nUna docena de «pinceladas flamencas». Doce proyectos para futuras biografías de cantaoras y cantaores flamencos malagueños cuya actividad humana y artística abarca desde 1850 hasta 1950. ¡Ahí es nada! Un siglo de la historia cantaora de Málaga, con nombres de tal resonancia que a cualquier flamenco le entran por la sangre apenas pronunciados: Juan Breva, el Piyayo, el Cojo de Málaga, el Canario, Paca Aguilera o la Pirula.\n\nNombres todos con vitola propia y de los que Gonzalo Rojo nos amplía lo que para muchos es un enigma, apenas una denominación impresa en una vieja placa de pizarra. Naturalmente, en las pequeñas dimensiones de un tan breve libro no puede caber completo un retrato de cada uno de ellos. El autor no lo pretende, y de ahí lo de «pinceladas» o «proyectos». Los artistas presentados son retratados sobre el fondo claroscuro de sus modestísi-\n\nmos avatares personales. Son siluetas esbozadas contra el muro de la nostalgia; antiguos daguerotipos literarios, similares a esos tan bellos de color sepia que ilustran la obrita. En ellos, el breve fogonazo nos habla de sus humildes orígenes, de su pasión por el arte flamenco, de ese ir remando cuesta arriba y contra corriente en las aguas de una vida que no parecía hecha a su medida. Algunos daban la sensación de sacar la cabeza a flote: Juan Breva, que mereció el aprecio y la deferencia de ese buen aficionado que fue don Alfonso XII, el cual le regalaba sus propios alfileres de corbata en pago a sus sabias actuaciones. Pero, ¡ay!, otro aguijón menos augusto signaba la vida de estos primerísimos protagonistas de nuestro arte. Nos cuenta Gonzalo que Juan Breva fue enterrado gracias a las limosnas recaudadas en un pañuelo por varios amigos en los cafés malagueños. Los reales\n\nobsequios no bastaron para frenar ese sino maléfico que pendía sobre casi todos ellos.\n\nAsí, desgranando pequeños detalles humanos, el autor desarrolla este libro bellísimo que ha sido otro regalo más para no olvidar nunca este increíble XV Congreso de Actividades Flamencas, que ha tenido su sede en Benalmádena.\n\nAlguien, no obstante, pensará que la biografía, el apunte vital del artista, no es nada al lado de lo incommensurable de su arte.\n\nCraso error. Sería suponer el cante o al baile una envoltura etérea, inmaterial y situada fuera del discurso implacable de la historia. Sería caer en el imperdonable olvido que dimana de creer que estos primitivos aedos de nuestra antigua raza están creando arte desde fuera de ellos mismos. Algo así como dejar a un lado de la senda jonda la más preciosa y vital encarnadura de sus protagonistas.\n\nAPERITIVOS SELECTOS Especialidad en PLANCHA\n\nMESONES, 18 TELF. 26 35 46 J A E N\n\nPágina 32 CANDIL",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1988-01",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 479,
    "article_char_count_full": 2888,
    "article_char_count_review": 2888,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-01-17-right-el-ballet-pre-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEl trozo de terra sigillata pertenece a un vaso construido, posiblemente, bajo la dominación romana de Asta Regia; sus ruinas se encuentran a once kilómetros de Jerez de la Frontera, en el camino que conduce a Trebujena, provincia de Cádiz\n\nTerra sigillata (Siglo I o II, d. de J. C.) Museo Arqueológico de Jerez\n\nJuan de la Plata\n\nHace algún tiempo que tenía conocimiento de la existencia, en el Museo Arqueológico de Jerez, de una rara y curiosa pieza, en cuya decoración aparecía grabada una figura en actitud de baile, que podría tener un imitarse mi discurso de ingreso en la Academia Jerezana de San Dionisio, de Ciencias, Artes y Letras, sobre «La tradición flamenca de Jerez», logré ponerme en contacto con la directora del citado museo, doña Rosalía González Rodríguez,\n\nportante parecido\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"público\"]\n\nría tener un imitarse mi discurso de ingreso en la Academia Jerezana de San Dionisio, de Ciencias, Artes y Letras, sobre «La tradición flamenca de Jerez», logré ponerme en contacto con la directora del citado museo, doña Rosalía González Rodríguez, portante parecido con las actuales formas del baile flamenco andaluz. Por más que intenté ver dicha pieza, todo fue inútil, ya que el Museo Arqueológico jerezano hace años que se encuentra cerrado al público debido a interminables obras de ubicación en un nuevo y más amplio local. No obstante, con motivo de edi- Fue en el mes de noviembre de 1987 cuando, en el despacho de la directora del museo, ésta puso amablemente en mis manos el tro- zo de terra sigillata que tanto ha- la cual aceptó localizar en sus archivos arqueológicos la pieza mencionada, para que la fotografíase y la reprodujese en la portada de mi obra. bía ansiado conocer. Al momento quedé deslumbrado. Efectivamente, se trataba de un hallazgo de vaso romano, encontrado hace más de cuarenta años en las excavaciones de las ruinas de las mesas de Asta Regia por el profesor don Manuel Esteve Guerrero, quien lo cita en sus obras, pero sin mencionar, en ningún caso, el enorme parecido de la figura grabada con un bailaor actual de flamenco. Es más, ni siquiera alude a esta figura. Esteve lo relaciona, entre los hallazgos más destacados, extraídos por él mismo de dicha excavación, como «la conocida por terra sigillata, que constituía la vajilla romana», sin entrar en ningún tipo de detalles descriptivos de la figura q\n\n[ENDING CONTEXT]\n\nde Asta Regia?—, grabada en un trozo de vaso romano de terra sigillata, cree-mos que puede arrojar bastante luz sobre los desconocidos orígenes de los cantes y bailes andaluces, que actualmente conocemos como flamencos. La prueba, pequeña, pero esclarecedora, se conserva como el mejor tesoro de la historia anti-quísima de nuestro folklore en el Museo Arqueológico de Jerez, gracias a cuya directora podemos hoy dar a conocer tan trascendental hallazgo arqueológico, inadvertido hasta ahora para todos los investigadores flamencos.\n\nDoctor Arroyo, 12\n\n- Teléfono 210058\n\nJ A E N\n\nPágina 34 CANDIL\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El ballet pre-flamenco",
    "periodical": "candil",
    "issue_id": "1988-01",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1081,
    "article_char_count_full": 6597,
    "article_char_count_review": 3167,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "público"
      }
    ]
  },
  {
    "article_id": "1988-01-18-right-el-romance-en-el-cancionero-infa",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«Yo me encuentro así que soy el español de todos los tiempos, que haya oído y leído más romances».\n\nRamón Menéndez Pidal\n\nCantos de ida y vuelta\n\nDelia Elena Santana de Kiguel\n\nBUENOS AIRES\n\nMuchos de los cantos y juegos infantiles son eco de aquellos romances entroncados con las gestas medievales y florecidos en esa vertiente épicolírica que en España se nutrió de los diversos géneros literarios. Ramón Menéndez Pidal les dedicó su vida. rough o Malbrough, duque y general inglés de actuación en la guerra de Sucesión española) que pervive en una antigua melodia francesa, penetra en América, se conserva como juego infantil y continúa una evolución que permite recogerla en Venezuela como romance en el\n\nY es en razón de aquellos nobles antepasados que advertimos en los juegos una cantidad de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"oficial\"]\n\ncon el siguiente texto (1): «Una dama tuvo un hijo/ lo fueron a bautizar/ y los padrinos pidieron/ Mambruno se ha de llamar./ Mambruno se fue a la guerra/ no se sa(be) cuándo vendra,/ si pa'la pascua de reyes/ o si para navidad./ Y la dama subió el morro/ y se puso a divisar/ a ver si venía venir/ a su amante militar./ Las noticias que te traigo/ no te las quisiera dar/ dicen que Mambruno ha muerto,/ debajo de la tierra está./ En medio e'cuatro oficiales/ ya lo llevan a enterrar,/ cuatro trompetillas roncas/ y un clarín a su compás./ Con esto y no canto más/ al pie de un verde aceituno/ que aquí se acaban los versos/ del romance de Mambruno». Este «romance noticioso» según la clasificación de Menéndez Pidal, conservado como canción infantil en Europa y en América, quizá retorne al viejo continente como romance en estos ininterrumpidos diálogos ultramarinos. Pero sigamos jugando. Hallamos a veces que los juegos son simples dramatizaciones sin mucha acción, y entonces el romance tiene primacía. Recitados o cantados alternan en el relato las voces de los diferentes personajes. Entre los de tema religioso es bien conocido el romance «El martirio de Santa Catalin\n\n[ENDING CONTEXT]\n\nRaquel, y DANNEMANN, Manuel: ob. cit., pág. 102. (5) COTARELO Y MORI, Emilio: «Colección de Entremeses, Loas, Bailes, Jácaras y Mojigangas desde fines del siglo XVI a mediados del XVIII», Nueva Biblioteca de Autores Españoles, 17, vol. I, Madrid, 1911, pág. 289. (6) GONZÁLEZ CLIMENT, Anselmo: Pepe Marchena y la ópera flamenca y otros ensayos, Madrid, Demófilo, 1975, pág. 190. (7) ARETZ, Isabel: «El Polo, Historia-Música-Poesía», en Boletín del Instituto de Folklore, vol. III, número 6, Caracas, Venezuela, diciembre de 1959. (8) MOLINA, Ricardo: Obra flamenca, Madrid, Demófilo, 1977, pág. 112.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El romance en el cancionero infantil",
    "periodical": "candil",
    "issue_id": "1988-01",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "18-20",
    "page_number": 18,
    "word_count": 3240,
    "article_char_count_full": 19347,
    "article_char_count_review": 2806,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "oficial"
      }
    ]
  },
  {
    "article_id": "1988-01-21-left-discograf-a-flamenca",
    "article_text_for_review": "Manuel Martin Martin\n\nHoy, en el mundo del disco, nos situamos ante uno de los trabajos más redondos, por antológico y flamenco, que conocemos sobre los villancicos. Nos llega por mor del Area de Cultura del Excmo. Ayuntamiento de Jaén y gracias al encanto de la música que brota de una mujer maravillosa, Rosario López, cantaora rigurosa por la gracia de Dios que ha tenido la bendita virtud de vaticinar el porvenir de lo popular para así colocar la voz anónima de la Navidad en la frondosa copa del incorrupto árbol flamenco. Y ahí radica uno de sus valiosos méritos porque, como declarase Andrés Bretón, una obra de arte sólo tiene valor si en ella vibra el futuro.\n\nAsí las cosas, para lograr que su obra complazca al crítico y entusiasme al aficionado, ha conseguido reunir a unos palmeros de lujo (Miguel Funi, Cristobalina de Funi y Pepa la del tío Benito) una guitarra de superlujo, la del lebrijano Pedro Bacán. Estos elementos favorecen sobremanera el estallido en mil pedazos de un corazón cuajado de flamenquismo moreno —el de Rosario López— y desempolvan la pasión volcánica de esta gran cantaora que hoy nos ofrece una impresión discográfica que al principio sorprende, luego engancha y finalmente convence.\n\nEste L.D. fue presentado en la ensolerada P.F. de Jaén coincidiendo con el I Encuentro de Críticos de Flamenco, ha sido editado por Pasarela y queda conformado por villancico de Andújar, villancico de Torredelcampo (recogido hace unos catorce años a Juan el Niño), villancico por petenera, toná y martinente, bulería de Jerez, bulería de Cádiz y villancico por nanas, algunos de ellos recreados sobre los viejos testimonios que nos legaron la Niña de los Peines y Vallejo, entre otros.\n\nSería a finales de septiembre cuando, una vez finalizadas las mezclas, recibí el cautivador mensaje de su cante. Ello posibilitó aporrear la olivetti y arrancarle la siguiente reflexión:\n\nJaén, un alto cauce que el agua de lo jondo inunda poco a poco, saca a relucir la memoria colectiva de una tradición esencialmente andaluza. El alma sonora de la Navidad, después de transitar durante muchos años entre filas anónimas, revela su vuelo en unos cantes teñidos con hojas de violeta, del grave color del invierno enfurecido.\n\nY avanzando por el aire, tejiendo sus guirnaldas de esperanza, amor y felicidad, una voz con sabor a sangre rocía de perlas estrelladas la sencillez de ingenuas melodías. Es Rosario López, a quien se ha confiado la más noble misión que pudiera ejercerse: poner en suerte las experiencias vitales del ciclo navideño jaenero; porque no hay que cantarle a la tierra que te vio nacer, hay que hacerla florecer a través del cante.\n\nLa finísima intuición flamenca de Rosario López ha sabido caminar sin descanso para arrancar la semilla dulce del hogar en que se forjan las cosechas. A la postre, no hace otra cosa que retornar a la luz misteriosa de la tierra con la pura unidad de un olivar; pero, en este caso, el fruto mostrado alumbra el rescordo de un fuego remoto que se fundió en su vientre, donde ahora compone y agranda todo lo vivido con la insurgencia de una mujer con jondura. Y es que el cante, cuando no es un soplo de vida, es fingido. Con gran dominio y conocimiento de los recursos técnicos, el profundo vibrar de su relampagueante magnetismo, imanta un mensaje de paz y alegría escanciado con el chorro de emoción acerado que se derrite en sus venas. Su lenguaje, personal y entrañablemente humano, no es más que un modo de llorar con hermosos metales el testimonio de largos años de sabiduría popular. En definitiva, da a su obra un perfil de gesta elaborada donde la tradición queda enriquecida con una expresión flamenca que despoja el cante de melífluos folklorismos.\n\nSu valor como cantaora es alto, aunque, quizá, su nombre no ha alcanzado la fama que reclamaría una obra de rara perfección como la suya. Pero ella sigue refugiada en la música, y hoy nos brinda el milagro sonoro de lo vivido. Escuchemos, pues, el golpeteo de su sangre. Dejemos que su música, por sí sola, hable a lo largo del aire. Descubriremos a Rosario López. Entonces comprederemos su mensaje.",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1988-01",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 699,
    "article_char_count_full": 4121,
    "article_char_count_review": 4121,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
