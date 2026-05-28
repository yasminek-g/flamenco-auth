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
    "article_id": "1995-11-14-right-noche-flamenca-en-sanl-car",
    "article_text_for_review": "Paco Arana\n\nEsta noche que voy de retirada hacia mi casa en bajo Guía, meditando los problemas y agobios de mañana, me desvío de la calle Bolsa a la Trasbolsa, para encontrarme otra vez con la alegre compañía de las gentes de la Peña Flamenca, la que ríe, llora y canta. La Peña Puerto Lucero, en la que a nadie se le extraña. Esta noche están todos, como otras noches, también ellos han traído su bagaje cargado de agobios y problemas de un día como tantos, de una jornada dedicada a su quehacer diario.\n\nMientras Curro y Manuel afinan sus guitarras, El Nono descorcha una botella y sirve el vino, El Chipí ha puesto sobre la mesa su tabaco... El ciprés de las guitarras se ha fundido en un lamento profundo y lánguido.\n\nJuan \"El Plazoleta\" se apunta por cantiñas, justo al tono, cabal y acompasado; con qué mimo de niño ciego acaricia cada tercio de su cante emocionado. El Nono le jalea y se ensimisma, para hacer un cante improvisado, gitanea con los ayes a compás en un trabalenguas alargado.\n\nHa empezado el ritual de los flamencos, Antonio pide tono —al dos por medio— para cantar unos tangos. Un torrente de ritmo se desboca con cortes y recortes perfectamente unisonados, donde palmas y guitarra se acompañan para dar entrada al cantaor que hace una letra que Paco le ha pasado:\n\nNo bebas de la fuente\n\nde los milagros.\n\nBebe tus lagrimitas\n\ntus desengaños.\n\nTodo vibra en la noche y poco a poco se empaña el ambiente pleno de ecos andaluces y sabor a manzani-lla sanluqueña, la que en la mar y el sol se baña, y como dijera Don Manuel, da color a la bandera de España.\n\nAgonizan las horas velozmente entre quejíos de misterio y ritmos sincopados, gran parte de los cantes más genuinos, se han evocado esta noche en este cuarto: mirabrás, peteneras, alegrías, marianas y fandangos; se han teñido falsetas legadas por Montoya, El Habichuela y El Niño Ricardo.\n\nHa juntado el reloj sus manecillas. Es la hora de la verdad. Aquí en este cuarto, se ha puesto un halo de misterio que flota entre botellas vacías y pitillos apagados. Gime rasgueos la guitarra con trémolos brillantes y bordones que-brados, brotando así la siguiriya, la que más sabe de cante, de nostalgia y de presagios, y Antonio, el que lleva a Sanlúcar de apellido, dice una letra compungida que le quema las entra-ñas y casi nunca ha cantado:\n\nLas fatigas dobles,\n\ntengo que pasar.\n\nQue se ha echao a la calle la mi compañera\n\ny yo en el penal.\n\nHace tiempo que la luna golpea el tejado de la casa, a estas horas, la noche es ya una mujer madura por más de cien jipíos preñada, que ha vivido la alegría de los cantes que festejan y divierten, y ahora se entrega al desconsuelo de lo jondo con profundas lágrimas de plata.\n\nYa vienen las claritas de día y una aurora nueva ha posado su embrujo en la ventana de la estancia; de nuevo la voz del cantaor se hace aguardiente y se entrega y se destroza por soleá gitana:\n\nHice un trato con la muerte:\n\n-A mí que me llame tarde\n\nque acudiré de repente.\n\nAún están todos en esta noche prolongada, y los problemas y agobios de un día como tantos, se ha hecho madrugada.",
    "title": "Noche Flamenca en Sanlúcar",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 561,
    "article_char_count_full": 3086,
    "article_char_count_review": 3086,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-11-15-left-carta-abierta-a-yerga-lancharro",
    "article_text_for_review": "Manuel Cerrejón\n\nEstimado amigo: Le puedo asegurar que me juré no contestarle nunca, pero por primera y última vez, lo voy a hacer. Recibido el núm. 97 de la Revista Candil, en la última página leo, y me quedo sorprendido cuando veo mi nombre; me dice usted \"espero que este escrito sea leído por el señor Cerrejón\".\n\nMi querido amigo: yo leo todo lo que llega a mis manos del mundo del Flamenco. Dice que yo me aprovecho de sus artículos para extraer de ellos los datos biográficos de cantaores y llevarlos a la carátula de sus cassettes; le hago saber que también lo hacemos en compact disc; lo que no sé es si este nuevo formato lo conocen en su bonito pueblo de Fuente de Cantos. Por su escrito creo que es el último disco que llegó a sus manos del Niño León. Sepa usted que es la primera vez que sale en discos el nombre, apellidos, fecha y lugar de nacimiento, sin equivocaciones. Los datos aportados son de mi archivo personal, como podrá comprobar. Por cierto, para terminar con el Niño León, parece ser que en su extenso archivo no contaba el cantaor nacido en la provincia de Huelva. Se lo digo por la carta recibida por Vd. el 25-11-89, lo podrán comprobar los aficionados, escrita por Vd., a este modesto estudio.\n\nSr. Yerga, ¿recuerda que le llamé por teléfono, y después de veinte minutos hablando, se convenció de los muchísimos entuertos escritos\n\nEspaña. Ministerio de Justicia. Registros Civiles. Serie A N.: 098756\n\nRegistro Civil de BOLLULLOS DEL CONDADO... Provincia ___HUELVA___\n\nCERTIFICACION EN EXTRACTO DE INSCRIPCION DE NACIMIENTO\n\npor Vd. mismo? Sabemos de su gran archivo. Tuvo la gran suerte de tener muchas personas que le regalaron discos de pizarra, en el tiempo que estuvo de alcalde; otros los compraba por kilos... Pero Vd. sabe que tenerlo todo es casi imposible; eso lo puede comprobar con algunos cantes que Vd. me pidió y que mando para que vean que el Sr. no lo tiene todo, ni lo sabe todo.\n\nSr. Yerga: sabido es que el Niño de la Loma nació en 1912. Creo que ni el bueno de Juan, ni Vd., sabían que el genial cantaor sevillano, Manuel Vallejo, grabó su primera versión en disco Pathe, año 1922, por una cara malagueñas, estilo Enrique el Melli-\n\nD. Manuel, le diré que por razones de espacio, no le puedo enderezar sus entuertos. Me va a permitir contarle uno, que Vd. no sabía (como decía mi querido y añorado Manolo Oliver, el saber está muy repartido), el 5 de abril de 1989, me mandó Vd. su libro \"Enderezando entuertos\", por cierto que le doy las gracias por la dedicatoria hacia mi persona, recordando que me pudo, al serio y gran estudioso Manuel Cerrejón. Seguimos con su libro, páginas 13 y 14, nos habla de la malagueña del Niño del Huerto, dice Vd., conservo una carta escrita por Juan de la Loma. A su pregunta le contesto que su creador fue Antonio González Marfil \"Niño del Huerto\", cuando yo aprendí su malagueña, en el año 1927, ya estaba enfermo de garganta; cuando Vallejo aprendió y grabó ese cante ya llevaba yo bastante tiempo cantándolo. Sr. Yerga, esta es la información que le puedo facilitar. Un saludo, Juan Gamberto Martín.\n\nzo \"Ay, la mare mía\"; por la otra cara soleares, la sonanta de Ramón Montoya. Le digo que Vd. no lo sabía, primero, que se lo hubiera dicho a Juan de la Loma; segundo, mi querido amigo Yerga, en la discografía de Vallejo, escrita por Vd. en la Revista Candil, dice bien \"Ay, la mare mía\", grabada en 1930, disco Odeón, guitarra Niño Pérez (por cierto, le falta también la grabada en los años 30, \"A la vera mía\", con Manolo de Huelva. Resumiendo, con lo que le gusta enderezar un entuerto, éste no\n\nlo pudo enderezar. El saber está muy repartido y el Flamenco nos morimos, y somos aprendices, malo el que vaya de catedrático.\n\nD. Manuel, en el principio de este artículo le dije que no iba a contestar nunca. No lo cumplí, pero sepa que voy a seguir sus estupendos artículos a través de esta Revista, eso sí, no le contestaré jamás. Se despide este cabal, que cree seguir siendo amigo suyo,\n\nManuel Cerrejón.\n\nPaco",
    "title": "Carta abierta a Yerga Lancharro",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 716,
    "article_char_count_full": 4007,
    "article_char_count_review": 4007,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-11-16-left-enderezando-entuertos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHace ya algún tiempo que publiqué, en esta Revista, un trabajo sobre la calidad artística de Juan Valderrama Blanca, con el título \"Juan Valderrama, cantaor por derecho\". De ese trabajo he extraído la esencia de lo que hoy escribo sobre él: que creo sin lugar a dudas que puedo decir en alta voz y con la autoridad que me confieren mis numerosos y asiduos lectores, que en Juanito concurren las esencias y virtudes necesarias para ser un buen cantaor por todos los palos flamencos.\n\nPor si alguien me sale otra vez diciendo que si su voz, que si su excesivo garganteo..., le diré que no es precisamente la voz, sea cual sea ésta, la que realmente hace al artista. El arte no se aprende, se nace con él y después, poco a poco, con el transcurso del tiempo, se va depurando hasta llegar a la plenitud\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nlamencos. Por si alguien me sale otra vez diciendo que si su voz, que si su excesivo garganteo..., le diré que no es precisamente la voz, sea cual sea ésta, la que realmente hace al artista. El arte no se aprende, se nace con él y después, poco a poco, con el transcurso del tiempo, se va depurando hasta llegar a la plenitud del mismo. Por eso estoy y estaré siempre en abierta oposición con aquellos que argumentan, para mí sin fundamento, que la voz más apropiada es la afillá. Y no es así. La voz de El Fillo fue ronca y grave, por lo que sólo es apropiada para la interpretación de un reducido número de cantes. Por el contrario, la voz no afillá es generalmente apta para todas las escuelas. Por eso tienen muchísimos seguidores los cantes emitidos con ese tipo de voz limpia y dulce, más propia de la raza no gitana. Y lo prueba el hecho evidente de la existencia de muchísimas grabaciones en pizarra realizadas en los años 10 al 30. (Ver discografía, por ejemplo, de la \"Niña de los Peines\", \"El Mochuelo\", \"Pepe Marchena\", Manuel Vallejo y del propio Juanito Valderrama.) Pregunto: ¿Cuántos cantaores ha habido con la voz afiliá? Que yo sepa, muy pocos. ¿Y cuántos seguidores tuvieron? Es posible que ninguno excepto, como es natural, algún aficionado de raza gitana. ¿Y cuántos seguidores tuvo durante su dilatada vida artística, con su voz de plata, el gran \"Pepe Marchena\"? ¿Y de Manuel Vallejo, a pesar de su voz anifiá como se manifestaban en Sevilla, qué podemos decir? ¡Pobre Manuel! ¡Cuántos detractores y enviadosos tuvo siempre! A Manuel le sucedió como a todo artista, sea de la faceta que sea: que tuvo que pasar a mejor vida para que sus maldicentes cambiasen de forma radical para que dijeran, incluso con énfasis, que tanto la discutida Llave como los demás títulos y honores conquistados, lo f\n\n[ENDING CONTEXT]\n\nla Malagueña. (Sus tercios son horizontales.)\n\nDicen: Que Merced Fernández Vargas \"La Serneta\", tía abuela de \"El Borrico de Jerez\" fue de Utrera; y yo digo que nació en Jerez y falleció en Utrera, donde vivió muchos años dando clases de guitarra. (Así lo corroboran mis documentos.)\n\nNOTA: El Pequeño recuadro que apareció en la Revista solicitando de los lectores las grabaciones de los cuatro discos de la Antología de Valderrama dio sus frutos. Las llamadas por teléfono fueron muy numerosas y procedían de diversos puntos de España. Gracias a todos, pero de forma especial al japonés Seiju Ota.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Enderezando entuertos Manuel",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1096,
    "article_char_count_full": 6521,
    "article_char_count_review": 3443,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      }
    ]
  },
  {
    "article_id": "1995-11-17-right-nostalgias-guitarr-sticas-madril",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJ.A. Pérez Bustamante En los primeros años de la década de los cincuenta, la gran afición de quien esto escribe por todo lo relacionado con el mundo flamenco asociada con algunas coyunturas felices, le permitieron entablar conocimiento y amistad con diversas personalidades destacadas de dicho ámbito artístico, muy pujante a la sazón en la Villa del Oso y el Madroño entre las que se contaron guitarreros, guitarristas, cantaores y aficionados de todo tipo a tan sublime Arte. Algunas de tales personas ya han sido objeto de breve tratamiento en páginas de Candil, tales como el guitarre Marcelo Barbero y el guitarrista Paquito de la Isla. El presente artículo pretende rememorar sucintamente, a través de unos pocos recuerdos y anécdotas residuales, los perfiles y más bien ignoradas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\ns ya han sido objeto de breve tratamiento en páginas de Candil, tales como el guitarre Marcelo Barbero y el guitarrista Paquito de la Isla. El presente artículo pretende rememorar sucintamente, a través de unos pocos recuerdos y anécdotas residuales, los perfiles y más bien ignoradas personalidades de la guitarra flamenca en la etapa final de sus vidas, lo cual me lleva a hablar de mis vivencias personales surgidas de mi relacion personal con un gran aficionado, Luis García Nieto y con un veterano profesional, Manuel Bonet, excelentes amigos, que hicieron todo lo posible por transmitir me —con más entusiasmo por su parte que éxito por la mía— arte tan sublime y difícil cual es el buen tañer flamenco, el primero en la más ortodoxa línea de Ramón Montoya y de Luis Molina el segundo, fieles ambos a sus respectivos maestros, en tres sesiones semanales de clase, que en casa de García Nieto eran tres \"matinés\" completas, inolvidables, sin echarle cuentas al reloj, auténticas tertulia flamencas entre dos amigos con fondo de recital de guitarra permanente, mientras que en \"casa Bonet\", tales clases —más formales y menos retóricas— eran de una hora exacta de duración. Muchos fueron los temas flamen-cos comentados con ambos maestros entr\n\n[ENDING CONTEXT]\n\nreseña incluida resulta, en mi modesta opinión, excesivamente breve e incompleta, dada su dilatada actividad profesional y diversificación de actuaciones. Por lo que a García Nieto respecta, no aparece ninguna mención biográfica específica en la \"Enciclopedia\" en cuestión. Sirvan pues, estas líneas de merecido y agradecido recuerdo reivindicativo de la memoria de una excelente persona, gran amigo y excelente artista de la guitarra flamenca, como fue Luis García Nieto, cuyo magistral virtuosismo tocaor y sus excelencias pedagógicas eran tan ampliamente conocidos como unánimemente ensalzados.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Nostalgias guitarristicas madrileñas",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1246,
    "article_char_count_full": 7927,
    "article_char_count_review": 2859,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "1995-11-19-left-homenaje-a-antonio-murciano",
    "article_text_for_review": "Más de ochocientos aficionados al mundo flamenco se dieron cita en la noche del 15 de Diciembre pasado en el bello escenario del Imperial Cinema de Arcos, cedido gentilmente por su propietario a la Federación de Peñas Flamencas Gaditanas, que rindió un hermosísimo homenaje al gran escritor, poeta y flamencólogo andaluz, don Antonio Murciano González.\n\nArtistas flamencos de la talla de Manuel Soto Sordera, Juan Villar, Encarnación Marín \"La Sallago\", Paco Cepero, Tina Pavón, El Perro de Paterna, El Cachorro, Diego de los Santos \"Rubichi\", Paqui Lara, Canela de San Roque, Ana Peña, Carbonero, Pedro Carrasco, Antonio Jero, Titi Flores, Miguel Chamizo, Laura Vital y la presencia del escritor y periodista sanluqueño Eduardo Domínguez Lobato, de quien corrió a su cargo la introducción del acto, junto a los presentadores del mismo, la popular cantaora y señora de la copla María José Santiago, junto al veterano presentador de la SER, Antonio Núñez Romero, hicieron pasar una noche muy agradable con el merecido homenaje que se le rindió al ilustre arcense antes citado.\n\nTestigos de excepción fueron la presencia de muchísimos peñistas y amigos del conocido escritor, hasta llenar el amplísimo salón del Imperial Cinema, y que contó con la presencia de los famosos artistas Juanito Valderrama y Dolores Abril.\n\nDon Antonio Murciano recibió en el transcurso del festival flamenco-homenaje varias distinciones de las diferentes Peñas gaditanas y la I Medalla de Oro de la Federación de Peñas Flamencas de Cádiz, que impuso el presidente de la misma, Antonio Núñez Romero en presencia de la totalidad de su Junta Directiva. Las primeras autoridades de la ciudad acudieron a la cita, y el público se volcó a tan importante evento flamenco en honor al maestro de las letras gaditanas, que emocionadamente agradeció tal distinción por parte del mundo flamenco gaditano con vivas palabras de afecto y admiración, y elogió sobremanera el gran espectáculo ofrecido en la tierra que le vio nacer: su monumental ciudad de Arcos de la Frontera, ofrecido en su honor.\n\nTelegramas de adhesión recibidos por parte de la popular cantante Rocío Jurado, Cátedra de Flamencología, etc., fueron leídos en el escenario por María José Santiago y Antonio Núñez, que el público acogió con muestra de simpatía y con una atronadora ovación. Un festival-homenaje para don Antonio Murciano que quedará para el recuerdo entre los buenos cabales de este rin-cón gaditano. ¡Enhorabuena!",
    "title": "Homenaje a Antonio Murciano",
    "periodical": "candil",
    "issue_id": "1995-11",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 391,
    "article_char_count_full": 2461,
    "article_char_count_review": 2461,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
