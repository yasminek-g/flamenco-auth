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
    "article_id": "1981-11-8-left-flamencolog-a-y-flamenc-logos",
    "article_text_for_review": "En un trabajo que publiqué hace años en la desaparecida revista «FLAMENCO» hube de referirme a la propensión por parte del excelente escritor argentino Anselmo González Climent a inventar —literalmente— voces que, por el hecho mismo de ser inventadas, no existen. El idioma, todos los sabemos, no se inventa, lo hace el pueblo y cuando alguna vez los sesudos varones de la Academia han querido crear un vocablo nos han salido con ideas disparatadas: tal aquella inolvidable del jeriñac. Si algún lector tuviera curiosidad por conocer algunos de tales vocablos, con mucho gusto pondría a su disposición una serie de ellos, que no se cuentan con los dedos de ambas manos y que, obviamente, no son ni arcaísmos (tan feizmente conservados en América) ni siquiera americanismos no admitidos por la Academia o tan en desuso que nadie recuerde ya. Pero de esta labor de González Climent una voz hizo fortuna: flamencología. Con ella, sus aérivados de flamencólogo y flamencológico. Y bien es cierto que para este caso (y no para otros muchos que cité en su día) se usó de una técnica perfecta, puesto que se trató lisa y llanamente de unir a la voz flamenco el sufijo griego Logos. Pero no van por ahí mis tiros ahora. Ya hemos tratado muchas veces el tema como para no cansar al lector, repitiendo argumentos que tenemos por válidos... y a lo mejor no lo son. Allá cada uno admitiendo o rechazando si, además de musicolólogo, no se incurre en excesivo empacho hablando de zarzuelógolo, operólogo, jotólogo... y flamencólogo y el ciento y la madre.\n\nNo íbamos por ahí, decíamos. Hoy queremos referirnos a una de las intervenciones —la última en el tiempo— que en el Congreso de Almería tuvo nuestro amigo Antonio Mata, a quien entrañablemente nos unen tantos desacuerdos y afectos a un tiempo. Antonio dijo, o nos pareció oír, que existía la figura y título del flamencólogo y apostilló su afirmación dando de memoria una breve relación de los principales, entre los que modestamente se incluyó. Pero dijo más o al menos dejó sentado de un modo más o menos explícito que el título o el nombramiento de flamencólogo lo da la Cátedra de Flamencología de Jerez, aunque no explicó a virtud de qué procedimiento. Y si efectivamente lo dijo así, esto es, si a nuestros duros oídos llegó la verdad de lo que recordamos, por ésta ya no pasamos. A cualquier persona se le ocurriría preguntar en primer término quién da a su vez a la Cátedra de derecho a otorgar la titulación. Pero no es necesario, porque la Cátedra no ha otorgado jamás ese nombramiento, atributo, dignidad o lo que sea. Cuando hemos recibido tantos aficionados (y profesionales) el honor de ser designados Miembros numerarios de la Cátedra de Flamencología y Estudios Folklóricos Andaluces no hemos creído nunca que se nos estaba invistiendo de una cuaiidad docente, de un cargo que incuso ha sido usado, y mi amigo Antonio lo sabe bien, con el aditamento de Nacional. La Cátedra jerezana, muy generosa en este reparto de dignidades —hasta el extremo de haberle tocado incluso a uno mismo— nombra Miembros numerarios y ad honorem y Caballeros Cabales. Y nos parece que ya está bien, cuando lo de Cabal no nos suena nada bien. ¿Cabal en el flamenco? ¿Quién? (Aquí habría que hablar también un poco, por venir al pelo, de aquella famosa devolución de la onza carnicera al general Sánchez Mira que quién sabe si aparte la falta de peso, fue, con Estébanez Calderón, uno de los primeros flamencólogos).\n\nPara nuestro gusto particular, hay un término que colma todas las satisfacciones de una dilatada vida en el flamenco: el de aficionado. Aceptaríamos el de estudioso, modesto en nuestro caso, y cederíamos a quien lo merezca el de especialista. Y llegaríamos a generalizar para quienes no gustan oír de la existencia de clases por mor de las luchas, el de flamencófilos. De ahí no pasamos. De la pura adjetivación, sin la absurda invención de fantasmagóricos diplomas e investiduras camelísticas.\n\nBueno, y si mi amigo Antonio Mata llega a convencerme algún día, aceptaré en mala hora la palabreja flamencólogo: con tal que sea aplicada sola, exclusiva y excluyentemente a muy determinados y concretos cantaores que sobre do-minar todos los cantes, tengan, además de la máxima intuición, la cultura suficiente para explicar y hacer entendible sus propios conomimien-tos. De la demás fauna flamenca, incluídos, con perdón y con todos los respetos y reconocimiento a su labor parcial, incluídos los musicólogos es-pecializados en la materia.\n\nFrancisco VALLECILLO",
    "title": "Flamencología y flamencólogos",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 755,
    "article_char_count_full": 4512,
    "article_char_count_review": 4512,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-11-8-right-las-letras-flamencas-de-jose-mar",
    "article_text_for_review": "Pontonés del qatorce, Pepe Márquez es, tal vez, el más prolífico creador de letras flamencas en nuestros días y con más de mil títulos registrados en la Sociedad de Autores. Su vocación cantaora le viene de lejos, en la taberna que su padre poseía en Punte Genil oyo cantar al Tenazzas y Cayetano Muriel en alternancia con Malospelos, El Seco y su difunto hermano Madiaolla; una taberna que parecía una casa de cuna por los «niños» que a ella acudían: Niño del Arahal, Niño del Seco, Niño de la Jara, Niño del Chicuelo, Niño de Casares, Niño de Alameda, Niño del Genil, Niño de Fuentes, Niño del Lucero, Niño de Pinturas... Márquez Cabello sabe que la buena copla nace cantándose, no sólo teniendo en cuenta su medida y enfoque, sino lo más esencial, su engarce prosódico, dada la variedad de estilo y golpes personales.\n\nCoautor con Fosforito de agunas letras, ha escrito malagueñas, alegrías, fandangos, soleares, etc., etc., muchas de las cuales pasan por anónimas —y es el mejor caificativo que puede hacer.\n\nSOLEARES\n\nTanto como yo te quiero no debería quererte, que hasta del vaso en que bebes tengo unos celos de muerte. Qué veneno tú me has dao que paso un día sin verte y vivo desesperao. Querer tan grande como este mío no ves en nadie.\n\nCARTAGENERAS\n\n...ay, que a la mina por más que truene un barreno yo no dejaré a la mina; que pa ruín y rastrero el que a espaldas te calcina a consensia y traicionero. Desde Murcia a Cartagena voy andando tos los días sólo por ver la morena que me va a enterrar en vía de no venirse a las buenas. sele— o grabadas por Fosforito, Jarrito, Antonio de Canillas, Pepe de la Isla, Moreno de Córdoba, Cándido de Málaga, El Bonela, Mari Carmen Reyes, Juan el Minino, José Mercé, Turronero, Talegón, Camborio, Chiquito de Osuna, La Cañeta, El Chaqueta, Luis de Córdoba, Moreno de Madrid, Chiquito de Málaga, Manuel de Luisa, Barquerito de Fuengirola, Chiquito de la Calzá, Montoyita, Antonio el Malagueño, José Salazar, Agustín de las Flores, Naranjito de Triana, Gitanillo de Vélez, Pepe de Vélez, Chiquilín, Pepito Vargas, El Charro, Pedro Lavado, El Ronquillo, Tiriri, Angel de Alora, Pepe Campillos, Niño de la Alegría, Pepe Albaicín, La Repompa, Antonio Suarez, Pepe Sanlúcar, El Floro, La Faraona, Antonio el Granaino, Pacorro, Carbonera de Cartama, Arriero de Colmenar y Carmen Córdoba, junto a no pocos más.\n\nPepe Márquez, ganador, entre otros, del premio de la revista Flamenco para letras jondas, es, sin lugar a dudas, uno de los más aceptados y reconocidos letristas populares. De su hacer que-den algunas muestras inéditas:\n\nTARANTA\n\nVengo de la mina, mare, donde me vuelvo enseguía, que una niña de Linares me va a mí a costar la vía como el Señó no me ampare.\n\nGRANAINAS: CORTA Y LARGA\n\nGraná joya incomparable, suspiró un rey por tí y estuvo con Lorca y Falla en que eras como jardín ¡lo más hermoso de España!\n\nMe gustaría el estar en la Vega junto a tí fruta viene y fruta va, y en el agua del Genil sambullirte y algo más...\n\nLIVIANA Y SERRANA\n\nSi la noche nos coge no me importará, que a luz de tus ojos yo me guiaba.\n\nMi serrana me jase mojinerías en ca vez que le güelo a barbería. A jara y tierra, a suó y tabaco, prefiere ella.\n\nMARTINETE\n\n¿Por qué, Dió, tovía hay gente viviéndola al sarto mata, durmiendo de puente en puente, pidiendo de casa en casa?\n\nEn pelea o discusión no es cobarde ceder, porque con tu buena acción bien claro das a entender tener un gran corazón.\n\nEn esta vía de ná a qué tanto discutir; si siglos tendrás Allá, so peaso de infelí, por toa la Eternía.\n\nCuándo llegará la hora que a nadie le falte el cocio; porque yo tengo sabio que en el mundo hay pan de sobra pero ¡muy mal! repartió.\n\nPETENERAS: CHICA Y GRANDE Son tan grandes los pesares taladrando mis sentios que no vivo en mis cabales (bis) desde que topé contigo.\n\nSon tan jondos mis pesares taladrándome el sentido...\n\nLoco del mundo maldigo (bis) porque no quieren mirarme los ojos en que me miro, ojos que van a matarme ojos que van a enterrarme si domarlos no consigo.\n\nSEGUIRIYAS\n\nEn cama me veo con dudas de muerte y pa más pena, tormento y calvario, no vienes a verme. Tus malas arsiones me van a matar, que yo me paso la noche llorando y el diita igual.\n\nBAMBERAS\n\nNo me hables, por favor, del columpio de Los Pinos (bis) que de celos muero yo viendo al vaina de tu primo embobao en tu alreó. Que ningún chabó te meza mientras yo no esté delante (bis) que me vuela la cabeza de pensar que algún tunante con tu aire se embelesa. Tu familia con mi gente tiene dura gresca armá (bis) pa que deje de mecerte cosa que no lograrán porque así nació el quererte.\n\nProlongación Antonio Herrera, s/n.\n\nTeléfono 227954 - JAEN",
    "title": "Las letras flamencas de José Márquez Cabello",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 839,
    "article_char_count_full": 4670,
    "article_char_count_review": 4670,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-11-10-right-el-centenario-del-primer-cancion",
    "article_text_for_review": "Si los estudiosos de nuestra lírica tuviesen clara conciencia del lugar que ocupa el raigón nutricio del auténtico aristocráticismo poético, no estarían, los más, exclusiva y continuamente hurgando el dorado ombligo de una literatura que se literatura, des-huesada, etérea. Sépase de una vez, que la hermosísima e inalienable poesía popular española no concluye en los Cancioneros de los siglos XV y XVI, ni en el Romancero; por tanto, cualquier análisis o intento de historiar nuestra literatura que prescinda —como es crónico— del estudio de la que se subestima por popular, no dejará de ser una coja, miope y elitista visión descafeinada. Mas aún, a mi parecer, será imposible comprender la literatura y, muy especialmente, la poesía hispánica desde el Romanticismo a nuestros días, si no existe un paritario y concienzudo examen de la poesía popular y, fundamentalmente, de la copla flamenca, la que tiene en su haber una serie de canciones — anótenlo los chamarileros literarios - de igual o mayor belleza y, sin lugar a dudas, infinita más profundidad existencial, pongamos por caso, que las recogidas por Gil Vicente. Y si esta atención a la poesía flamenca fuese una realidad, aunque sólo para los aficionados al cante, el monumento literario e histórico que es el «Primer cancionero de coplas flamencas populares según el estilo de Andalucía», de Manuel Balmaseda González, no hubiese pasado, al menos en este primer centenario de su aparición impresa (1), en el más doloroso de los olvidos. Doloroso, porque sólo dolor produce tanto desconocimiento, como el que nos arrastra esta cita también centenaria, 1881, de uno de los intelectuales más lúcidos del pasado siglo, Leopoldo Alas, «Clarín»: «El cante es el único género artístico que prospera; cante en la Comedia; cante, en Variedades; cante, en Lara; cante, en la Zarzuela; cante, en Arderius; cante, en Martin. El cante ha derrotado a Echegaray... el teatro flamen-co nos lleva a la estupidez por una rapidísima pendiente» (2).\n\nAnotando, una vez más, que por poesía flamenca únicamente entiendo a la copla, mientras la que parece tener patente de corso para utilizar ese nombre a lo más que podría considerársele es como poesía de inspiración flamenca - por cierto, su inmensa mayoría está repleta de ripios y lugares comunes, de una calidad literaria verdaderamente deplorable -, el calificativo que he dado a este libro de monumento, precisa algunas justificaciones históricas y literarias.\n\nSi bien es verdad que existen algunos cancioneros populares andaluces y flamencos contemporáneos e, incluso, anteriores al de Balmaseda, este es el primero que llega a editarse de poeta popular conocido, un jornalero ecijano muerto de hambre a los veinticuatro años en Málaga. Un cancionero que Pitre dio a conocer, a su salida, por toda Europa y que arrancó bellísimas páginas de entusiasmo a Demófilo y Luis Montoto en «El folk - lore andaluz» (3), muy certeramente reproducidas en la segunda edición prologada con verdadero tino por José Luis Ortiz Nuevo (4), y a la que remitimos al lector en bien de la brevedad de esta reseña conmemorativa, en la que, sin embargo, queremos dejar algunas pruebas palpables, no sólo de la hondura y jondura de su lacerado sentir:\n\nTodo el añito me llevo Diciéndole a mis penitas: Que me dejen descansá Siquiera por una horita.\n\nAl río yo me tiré, Y el agua me sostenía; ¡Como me veía tan pobre Ni el agüita me quería! sino de su belleza intrínseca, La ví enterraita Con la mano fuera; Que como era tan desgraciaita Le fartó la tierra.\n\n¿No está en esta siguiriya toda la realidad de la cultura andaluza en su drama existencial por la posesión de la tierra? ¿No puede ser este el más desolado canto de la tragedia del jornalero andaluz? No conozco ningún texto literario que presente unas manos tan vacías y clamorosas. Pero quede alguna otra muestra:\n\nHasta el carrerito Pasaba llorando; Y la conocí por el pañolito Que la iba tapando.\n\n¿No es esta siguiriya - como muchas otras de Balmaseda -, una recreación bellísima de otras populares y, en este caso concreto, una reelaboración de la que Bécquer anotara en «La venta de los gatos» con tanto entusiasmo?:\n\n«En el carro de los muertos ha pasado por aquí: llevaba una mano fuera; por ella la conocí».\n\nEn el carro de los muertos Ayer pasó por aquí: Llevaba la mano fuera; Por ella la conocí.\n\nPor cierto, en 1881, los anónimos prologuistas de Balmaseda, transcriben esta copla con unas ligeras variantes de como fuera tomada por el autor de «Rimas», que la hacen mucho más flamenca, por lo que, a mi parecer, es muy probable que el poeta sevillano la anotase defectuosamente, o que la misma, como la buena moneda popular, en su anónima andadura cantaora, adquiriese la pátina de su hondura:\n\nManuel URBANO\n\nALMACEN Y OFICINAS: Dr. Civera, 33 - Teléf. 231390 y 231687 JAEN Pero dejemos esta coda becqueriana - que brindo a Rafael Montesinos, de quien «CANDIL» espera su trabajo sobre el flamenco en Becquer-, y admitamos la grandeza inmarchitable del «Cancionero» de Balmaseda, en el que sus 461 coplas - polos, peteneras, soleares y siguirias -, al decir de sus editores, amargas como la mirra y purísimas como gota de rocío, constituyen una justísima sintesis histórica de un pueblo aquejado por una catarata de injusticias que, como la muerte, tienen una presencia real que trasciende a este monumento literario.\n\nGENEROS DE PUNTO CONFECCIONES\n\nMesones, 18 - Teléf. 23 40 46 J A EN",
    "title": "El Centenario del «Primer Cancionero Flamenco» de Manuel Balmaseda, y una reflexión becqueriana",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 905,
    "article_char_count_full": 5434,
    "article_char_count_review": 5434,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-11-11-right-el-festival-nacional-del-cante-d",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAnte todo queremos indicar que nuestra única y exclusiva intención es salvar a nuestro Festival Nacional del Cante de las Minas de un naufragio que se presagia a corto plazo, ya que, el timón de esta nave «cantaora», creemos, necesita una renovación, total y absoluta, de sus pilotos y dotación general.\n\nA través de lo acontecido durante veintiún años y como consecuencia de la deficiente labor de la Organización (de la que se podría dispensar alguna) y miembros del jurado calificador del concurso de cante, pensamos que nada positivo se ha conseguido, toda vez que el cante, cuya claridad y realidad debería estar en vías de superación, a estas alturas se encuentra estancado y en la más deprimente falsedad de como se debe interpretar los estilos mineros y de como debieran ser respetadas sus\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestra\"]\n\ncomo consecuencia de la deficiente labor de la Organización (de la que se podría dispensar alguna) y miembros del jurado calificador del concurso de cante, pensamos que nada positivo se ha conseguido, toda vez que el cante, cuya claridad y realidad debería estar en vías de superación, a estas alturas se encuentra estancado y en la más deprimente falsedad de como se debe interpretar los estilos mineros y de como debieran ser respetadas sus líneas maestras. Pensamos que todavía es tiempo, si ponemos todos de nuestra parte, de ir a esa reunificación necesaria para que el origen, la tradición y la pureza de nuestros cantes brillen en su máximo explendor. Hablar así nos produce una profunda tristeza; y es, precisamente, nuestro cariño, el motivo que nos empuja a exponer —siempre guiados y apoyados por opiniones solventes— los errores y defectos de nuestro Festival. He aquí un ejemplo: ABC de Madrid, en su página de espectáculos del martes 18 de agosto, del presente año, y en su reseña del resultado del Festival Nacional\n\n[ENDING CONTEXT]\n\nsu grandeza real.\n\nAntonio Piñana (padre)\n\nMedalla de Plata en el X Salón Internacional de Bruselas\n\nFabricación de toda clase de plantillas, ortopédicas en conglomerado de caucho y corcho, con extensa gama de piezas accesorias para confeccionar y adaptar a las mismas. (Arcos internos o longitudinales. Arcos transversos. Cuñas pronadoras y supinadoras. Herraduras, etc.)\n\nLas plantillas y piezas accesorias, se hacen en tres consistencias: BLANDAS, DURAS Y SEMIDURAS. También fabricamos según diseño Técnico.\n\nFábrica y oficinas: Arrastradero, 6 y 8 - Teléfonos 22 33 92 y 22 51 12 - J A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Festival Nacional del Cante de las Minas",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 978,
    "article_char_count_full": 6040,
    "article_char_count_review": 2651,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestra"
      }
    ]
  },
  {
    "article_id": "1981-11-12-right-guitarra",
    "article_text_for_review": "\"Encantado matraz...\"\n\nJ. L. Núñez\n\nArbol sonoro al que la mano deja un pájaro asustado y malherido. Sextillizo calambre contenido en una copa de memoria añeja.\n\nDolor encarcelado. Intacta queja que espera un golpe para ser gemido. Nudo de contrición frente al olvido, donde se incuba una nostalgia vieja.\n\nSicomoro real. Caoba encinta, que pare un jílgueral de voz distinta según el labrador que la entretiene.\n\nNiña de miel y savia. Extraña planta a la que hay que agarrar por la garganta y ararle el corazón para que suene.\n\nJoaquín Márquez",
    "title": "Guitarra",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 91,
    "article_char_count_full": 543,
    "article_char_count_review": 543,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
