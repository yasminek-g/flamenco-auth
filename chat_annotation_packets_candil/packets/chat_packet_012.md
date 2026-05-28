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
    "article_id": "1980-07-14-right-carta-a-pastora-pav-n-i-ina-de-l",
    "article_text_for_review": "A dmirada PASTORA:\n\n¿Cómo era él y cómo tú? Niño de Jerez y Niña de los peines. El con su tórrea estatura y el tamaño de su voz. Dicen que se enfundaba siempre en pantalones de pana y que en cierta ocasión tú le preguntaste el «por qué». Te respondió el Torre con ese gesto suyo taciturno, anticipadamente triste: —porque cuando canto siguirias no sé de otra tela que aguante sobre mis muslos el pellizco de mis manos. Tú eras también así..., pero por otro caminar. Porque tu voz era de metal salino y la inundaban ecos marineros. Y tú vehemencia más dulce y ese equipaje de gracia irrenunciable en tus maneras. Entonces se cantaba la vida y una solea podía ser la crónica de un dolor reciente o acaso el restatar de una antepasada cicatriz que mágicamente se apoderaba de tu garganta. Entonces cantar era una forma más de vivir, cuando cada palabra tuya la mecían nervios siempre calientes, sonido de existencia:\n\nYa no son las mismas flores\n\nlas flores de tu ventana;\n\nMaría de los Dolores.\n\nestán perdiendo colores\n\ncomo tu cara gitana\n\nHoy es otro el tema, Pastora. El cante, en demasiados casos, es impresentable. No tiene encarnadura. Suena perfectamente bien, pero carece de gemidos que no sean aprendidos. Cantar ya no es vivir sino, al menos, recordar. Por eso, quien recuerda, quien estruja su memoria, quien amontona dolores nuevos, puede todavía cantar; quien recuerda por efecto de una transmisión de pálbitos y desgarramientos que saltan por arriba del tiempo y del espacio; quien recuerda su historia y su dolor, quien hace vida su cante y no música de entretenimiento, cante enlatado para un L.P. Es difícil en estos tiempos recordar, Pastora. ¿En un multitudinario festival? ¿En una reunión de técnicos señores? ¿En un espectáculo teatral o televisivo y como música de fondo de tantos y tantos payasos andaluzados? Se han perdido los cenáculos, la sinagoga y la taberna... Cantar es un oficio y el flamenco —tantas veces— un disfraz. Por eso las encopetadas señoras —prestigian entre la burguesía la afición al cante jondo—, cuando me son presentadas, me escudriñan, afilan su juicio sobre la «artista» y evocan el tablao, la que profesa cantar y debe divertir y estar alegre. Alegría, Pastora. Como si el cante fuera un chiste que se canta, un malabarismo, el puro mimetismo. Alegría. Como si el cante no naciera también de la grama y del alarido; como si el cante no creara ovas de amargura y músculos fundidos y gemidos oceánicos y desesperación:\n\nA la sierra de Armenia\n\nme quiero ir\n\ndonde moros y cristianos\n\nsepan más de mi.\n\nComo si el cante, Pastora, no fuera recordar una historia de simáticas marginaciones que inopinadamente asumo yo, padezco yo...\n\nYo quiero recordar, Pastora, reconstruir tu mundo y que me llenen muchedumbre de voces y ser fiel a gritos y susurros ancestrales y cantar lo más cerca de la vida.\n\nSiempre tuya,\n\nRosario López",
    "title": "Carta a Pastora Pavón (Iñina de los Peines)",
    "periodical": "candil",
    "issue_id": "1980-07",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 490,
    "article_char_count_full": 2872,
    "article_char_count_review": 2872,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-07-15-left-quienes-fueron-los-maestros",
    "article_text_for_review": "«SÊNOR» MANUEL MOLINA\n\nVamos a plasmar en esta sección, en primer lugar, la figura de Manuel Molina, conocido maestro de Jerez de la Frontera, por sus cantes por siguiriyes y tonás.\n\nNacido en Jerez, de raza gitana, a mediados del siglo XIX, fue uno de los grandes maestros jerezanos del cante. Se sabe poco de su vida, pero, afortunadamente, quedan tres muestras de su cante por siguiriyes, que dan cabal fe de sus asombrosas facultades y creatividad. Ha sido otro de los cantaores que merecieron el calificativo de «Señó» por su porte senorial y carácter serio y respetable.\n\nManuel Molina o «Curro Molina», como le llamaban sus amigos, mantuvo relaciones muy amistosas con Enrique «El Mellizo» - figura ya reseñada en esta sección - y Frasco «El Colorao» (de este último, de raza gitana, de finales del siglo XVIII, puede decirse que fue el gran maestro de la siguiriya de Triana, donde nació). Manuel Molina tuvo que abandonar prematuramente el cante a causa de una sordera contraída por su gran dedicación a nuestro arte.\n\nComo Enrique el Mellizo, estuvo dotado de poderosa inventiva musical y de consumado arte para la siguiriya y los martinetes, así como la toná, cantes que esencialmente distinguen a su raza. Según Ricardo Molina, una de las seguirillas que se atribuyen a Manuel Molina es:\n\nDicen que duermes sola mienten como hay Dios porque de noche en el pensamiento dormimos los dos. Volviendo otra vez al poeta cordobés, éste afirma: «Esta siguiriya la cantaba también Enrique el Mellizo, pero su énfasis, la manera de desarrollar los tercios, incluso ciertas analogías melódicas con otros cantes del señor Manuel Molina, proclaman claramente su parentesco».\n\nE sta siguiriya es cantada en la actualidad por Antonio Mairena, Jarrito y La Paquera, anteriormente también lo fue por Manuel Torre, quien aprendió algunos cantes de la figura que nos ocupa. Otra de las siguiriyes del señor Manuel Molina, es la corta, que comienza: •Y no me desmás penas», posteriormente difundida por Manuel Vallejo. Pero, posiblemente, la más famosa y la que más entroncaba con su personalidad es la siguiente siguiriya amartinetada:\n\nEstán tocando a misa en San Agustín como no tengo velo ni mantilla yo no puedo ir.\n\nE $ ^{1} $ senor Manuel Molina ha sido un ejemplo singular de cantaor eminente, creador y pundonoroso.\n\nCURRO FRIJONES\n\nNatural de Jerez de la Frontera, de raza gitana, nació hacia mediados del siglo XIX, más concretamente entre los años 1860 al 70. Murió por el primer cuarto de nuestro siglo.\n\nDe nombre Antonio Vargas, fue un gitano que se dedicó durante mucho tiempo al negocio de carnicería, decayendo posteriormente sus actividades, debido a su carácter un tanto desordenado y extravagante.\n\nPastora Pavón era perfecta conocedorea d la persona y los cantes de Frijones, de ahí que hayan podido llegar hasta nuestros días. Sobresalió Frijones por los cantes de soleares, creando un tipo de éstos muy recortado y ligado en los tercios. También Antonio Vargas fue en contadas ocasiones el creador de sus propias letras, girándo estas siempre sobre la temática del matrimonio, ya que tardó mucho en casarse con «La Farota», gitana que le sobrevivió y la que, según declaraciones de Pastora Pavón, «cantaba lo suyo». En relación con esta boda, Frijones tenía una letra donde expresaba su amor por el celíbato:\n\nMe llamo Curro Frijones\n\ny no me caso con La Farota\n\npá no echarme obligaciones.\n\nIgualmente, Antonio Vargas era un buen cantar de tangos, y se cuenta que D. Antonio Chacón fue un gran admirador de estos cantes, prefiriendo, sobre todo, el que comienza con «Me gusta verte llorar».\n\nE $ ^{n} $ cuanto al cante de Frijones por siguiriγas, se tienen noticias de que el hermano de Manuel Torre, Pepe Torre, cantaba unas muy simila- res a las de Frijones.\n\nPara terminar, el cante de Antonio Vargas o «Curro Frijones», tiene aún hoy, una gran vigencia, sobre todo entre los artistas de su tierra, y en Sevilla.\n\nTRINIDAD NAVARRO «LA TRINI»\n\nCantaora de conocida belleza y elegancia, nacida en Málaga hacia 1875, fue una gran cultivadora de la «Malagueña», tal vez como ninguna otra cantaora, popularizó una propia que ha llegado hasta nuestros días y que tiene una ejecución bastante difícil; alternó con famosas figuras de su época, como Juan Breva, Fosforito o «El Canario». E sta mujer era tuerta y, según cuentan, este suceso ocurrió de la siguiente forma:\n\n«Había terminado la brillante actuación de «La Trini» aquella noche en el tablao, y en unión de su amante y acompañada de otros artistas, dispusiéronse celebrar una de las pintorescas «juergas» de carácter popular y especialmente andaluz tan frecuente en los cantaores... Agustín el novio de la Trini, alargó a ésta una navaja en cuya afilada punta le brindaba una aceituna; quiso la cantaora cogerla con los labios como demostración de cariño y precipitándose sobre el arma con excesiva rapidez y absoluta falta de cálculo en la distancia, se hundió ella misma la acerada hoja en una de sus hermosas pupilas... La cantuora perdió el ojo para siempre...»\n\nLos cantes de Trinidad Navarro se han olvidando con el tiempo (excepto su malagueña) —como siempre —Pastora Pavón era perfecta conocedora de los mismos. Igualmente los Pena, padre e hijo, también los conocían, grabándolos posteriormente en disco.\n\nEl cante de «La Trini» estaba matizado de ternura, pasión y espiritualidad. Una de sus coplas más famosas es la que dice:\n\nYo canto la pena mía mi cante a nadie conmueve; yo soy como el ave fría que canta como la nieve al amanecer el día.\n\nSelecciona: Rafael Valera",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1980-07",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 925,
    "article_char_count_full": 5553,
    "article_char_count_review": 5553,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-07-16-left-flamenca",
    "article_text_for_review": "EN VEZ DE...\n\n¿EL DISCO FLAMENCO EN CRISIS?\n\nEn esta página de CANDIL debieran tener cabida las novedades discográficas del mundo flamenco, pero hay tan poco interés en las casas grabadoras por nuestro arte que, al no existir nuevas apariciones, tendríamos que dejarla en blanco. Y, ante la tentación de dejarla en blanco, preferimos hacer algunas consideraciones sobre el tema, para conocimiento del aficionado y reflexión de quien corresponda. Lógicamente, una pregunta nos asalta, ¿por qué el desinterés de las empresas del disco actualmente? Encontramos varias motivaciones. La primera y principal es su no rentabilidad. Y esta afirmación, oída a ejecutivos del ramo nos lleva a formularnos un ¿por qué? Otra pregunta se nos viene a la mente, ¿no hay aficionados compradores? Varias respuestas pueden configurar la situación por la que atravesamos. Una, para nosotros primordial, es la poca aportación artística grande, tiene abundante discografía en su haber que, al no estar condicionada por la novedad —el disco flamenco de ayer y de hoy tiene y tendrá siempre plena vigencia—, hace que la industria fonográfica no se meta en nuevos gastos. Unir también, el que las nuevas generaciones, salvo contadas excepciones, no tienen demasiada capacidad creativa con posibilidades de llegar al público. Su comunicabilidad, en líneas generales, es pobre. Su dimensión artística que hoy se da. Por otra parte, el cantaor ya hecho, el artista grande, tiene abundante discografía en su haber que, al no estar condicionada por la novedad —el disco flamenco de ayer y de hoy tiene y tendrá siempre plena vigencia—, hace que la industria fonográfica no se meta en nuevos gastos. Unir también, el que las nuevas generaciones, salvo contadas excepciones, no tienen demasiada capacidad creativa con posibilidades de llegar al público. Su comunicabilidad, en líneas generales, es pobre. Su dimensión artística, lo mismo. En esta situación es difícil grabar porque la rentabilidad, no olvidemos que una empresa vive de las ganancias, es nula; Y luego, no hablemos de la falta de profesionales productores que conozcan el tema. En casi todas las ocasiones cuando se escucha el rasgueo de una guitarra, palmas y una voz que trata de dar a su decir cierto aire flamenco, resulta que, en opinión de estos «técnicos», ya tenemos cante en su máxima pu- reza. Con estos conocimientos ya me dirá el aficionado, las posibilidades de realizar grabaciones que respondan a un mínimo de conoci- miento y calidad.\n\nAsí las cosas, habría que plantearse la necesidad de, por una parte las peñas flamencas, tratar de iniciar una búsqueda de nuevos valores (capaces de dinamizar el arte de lo jondo. Por otra, interesar a las casas discográficas en un hacer artístico, con extraordinarios y ricos valores, que merece atención y esfuerzo en pro de su fijeza y limpieza. Nuestro Arte necesita apoyo firme y decidido. CANDIL intentará abrir nuevos cauces en sinceros afanes por un flamenco mejor, enraizado en sus más puras e incontaminadas esencias.\n\nDOSCANDIL",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1980-07",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 484,
    "article_char_count_full": 3026,
    "article_char_count_review": 3026,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-09-3-right-editorial",
    "article_text_for_review": "La Cátedra de Flamencología de Jerez, ha distinguido a la Revista «Candil» por su labor en «pro» de la difusión del Flamenco. La noticia, ciertamente, ha incentivado nuestro propósito de mantener esta publicación que estimamos cumple con dignidad un importante objetivo: conocer y difundir el arte «jondo» desde una contemplación histórica y cultural del mismo. Intentamos, así, contribuir al estudio de un área frecuentemente maltratada de nuestra cultura.\n\nPero la distinción de la Cátedra de Flamencología de la que sinceramente nos honramos, tiene, a nuestro juicio, otra lectura que no sentimos recato en puntualizar: la indiferencia con que las instancias oficiales, a las que les viene atribuida por el ordenamiento jurídico la custodia y defensa de la cultura, asisten a nuestro humilde empeño. Y es que se parte de un concepto unívoco de cultura, oficialista, encorsetada, de claras connotaciones burguesas, sin que se acepte la otra cultura «viva», las otras formas de expresión, el cante, pálpito secular de la sangre, hondones del dolor de un pueblo, cuando tales maneras de cultura no han encontrado -ni falta que les hace - formulaciones académicas.\n\nHe ahí un reto que no vamos a eludir. He ahí una tarea que, por el momento, no motiva suficientemente la sensibilidad de los que podrían hacer más prontamente eficaz nuestro intento.\n\nPor eso, la mención de la Cátedra de Flamencología de Jerez, tiene para nosotros enorme relevancia porque evidencia que detrás de un casi inapreciable esfuerzo, bulle la tensión y el interés de gentes que, acaso, ya hayan captado el mensaje, o, lo que es lo mismo, se perfile ya su, en otro tiempo desdibujada, identidad como pueblo. Sinceramente, gracias.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 273,
    "article_char_count_full": 1705,
    "article_char_count_review": 1705,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-09-4-right-seguridad-social-para-los-artist",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(Propuesta para un Régimen Especial de los artistas flamencos)\n\nIN duda alguna, de los trabajos presentados en el VIII Congreso de Actividades Flamencas, uno de los que obtuvo más cálida acogida fue el de Francisco Vallecillo, «Ante la situación de los Flamencos de la Tercera Edad». La dimensión social de esta ponencia caló en los asistentes entre los que se encontraban algunos profesionales muy sensibilizados por este problema. Y sin embargo, al final todo se redujo a un puro fuego de artificio. Se hizo realidad una vez más la incoherencia y falta de seguimiento tan meridionales de la que fundadamente nos tildan, de aquellos asuntos que iniciamos y no sabemos concluir. Se habló en el citado Congreso de constituir una Comisión que estudiara la posible ejecución de los acuerdos adoptados.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"cuerpo\"]\n\nprofesionales del flamenco, atendidas las especiﬁdades de la actividad laboral que desarrollan. El trabajo del admirado Francisco Vallecillo rezuma bondad, beneficiencia, sin que queramos por ello decir que ésta es incompatible con los objetivos que vamos a ofertar. Primera cuestión: en qué situación legal se encuentran nuestros profesionales del cante, del toque y del baile. ¿Cuáles son sus posibilidades de acceder a una jubilación digna? ¿Qué cuerpo legal les otorga una cobertura de riesgos en caso de accidente, invalidez, fallecimiento, etc? En definitiva, de qué prestaciones básicas pueden, en la actualidad, beneficiarse los artistas flamencos. Ante situaciones heterogéneas, como hemos podido constatar, la respuesta exige matizaciones. Veamos. Parece lógico que nuestros profesionales debieran estar acogidos a la normativa que establece el Decreto 2133/75, de 24 de Julio y demás disposiciones concordantes, regulador del Régimen Especial de la Seguridad Social de los Artistas. Así el artículo 2 del citado Decreto, al fijar el campo de aplicación de este Régimen Especial relaciona las actividades de teatro, circo, variedades y folklore. Primera dificultad: la poca homogeneidad de la labor que cumplen los artistas flamencos. No afecta la misma problemática a los, por ejemplo, asalariados de un tablao flamenco, que a aquellos artistas, que son los más, que se limitan a realizar sus festivales veraniegos y algún recital que otro en el invierno. Mientras, por los primeros se cotiza en este Régimen Especial, con cierta asiduidad, por los segundos, no se suele cotizar ya que en la inmensa mayoría de estos contratos que se formalizan por una sola actuación, es fácilmente eludible esta obligación. Y aun en el supuesto de que se cotizase, topamos con la dificultad casi insalvable de que la media anual de días cotizados no rebasa los ochenta. Es cierto, que algunos artistas en candelero, excepcionalmente, pueden hacer noventa o cien festivales; la media normal de actuaciones de los artistas f\n\n[ENDING CONTEXT]\n\ny juristas, sobre todo los mecanismos legales exigidos para aspirar a la constitución de esa Mutualidad Especial de la Seguridad Social de los Artistas Flamencos.\n\nOjalá, en esta ocasión, el celo y la tenacidad venzan a la apatía. Por el momento, nos limitaremos a lanzar la idea en la seguridad de que encontraremos lectores sensabilizados. En cualquier caso, quede patente nuestra promesa formal de que en lo que respecta al Grupo Candil no pararemos en la sola especulación, si se nos otorga por las personas interesadas el aval necesario para coordinar tan importante tarea.\n\nRamón Porras.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "SEGURIDAD SOCIAL PARA LOS ARTISTAS FLAMENCOS",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 1484,
    "article_char_count_full": 9454,
    "article_char_count_review": 3639,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "cuerpo"
      }
    ]
  }
]
```
