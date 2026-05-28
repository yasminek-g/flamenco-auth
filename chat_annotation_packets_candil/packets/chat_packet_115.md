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
    "article_id": "1985-07-20-right-cantes-de-ida-y-vuelta",
    "article_text_for_review": "Por: Yerga Lancharro\n\nHace veinticinco años, poco más o menos, con motivo de un trabajo sobre «Manuel Torre» dije que este cantaor fue un excelente intérprete de los cantes de Levante y estuve a punto de «ser pasado por las armas» dentro del mundillo flamenco. La verdad es que nadie me lo perdonó. Y ello fue debido a lo poco que se sabía del artista jerezano. Ante tal actitud me vi forzado a demostrar a algunos «enteraos» que se habían sentido «heridos» por mis manifestaciones, que yo no había ultrajado la memoria del extinto cantaor. Y lo hice de la única forma posible: Publicando sus cantes de Levante que yo poseía. Luego, el tiempo y mis «detractores» vinieron a darme la razón en demasía. Y fue así porque dijeron sin fundamento que Manuel fue creador de un estilo por malagueña. ¡De crear, nada! Manuel cantó bien por este palo, por tarantas del árbol malacitano y por cartageneras, pero nada más.\n\nY viene todo esto a colación, porque de regreso de mis vacaciones en tierras catalanas, me encuentro entre la correspondencia atrasada, varias cartas en las que sus remitentes me piden que proporcione a profesionales y aspirantes a serlo, grabaciones de las guajiras, milongas, colombianas y vidalistas que aflamencaran los aficionados, Sebastián Fernández Flores, Rafael Flores Nieto y algunos andaluces más, tras su permanencia, durante varios años, como soldados en tierras americanas.\n\nLa verdad, amigos lectores, es que he quedado gratamente sorprendido al recibir las peticiones y no puedo por menos que extrañarme, ya que cuantas veces publiqué mis deseos de que se ejecutaran TODOS LOS CANTES, incluidos, por supuesto, los «americanos» la Chufla de Garrido y las Asturianas y Montañesas que aflamencaron aquellos genios: «Niño Medina», «Niño de la Isla» y otros, para\n\nque ninguno de ellos cayese en el abismo del olvido, era criticado coléricamente y catalogado como un verdadero ignorante, desconocedor de los que parecen ser los únicos cantes, como son las soleares y las siguiriyes.\n\nDe nuevo el tiempo viene a darme la razón. Así pues, no digamos nunca «de esta agua no beberé» porque es muy probable que tengamos que hacerlo.\n\nAhora resulta que, incluso, se trata de can-tes dignos de ser propagados; dignos de ser escuchados como las soleares y las siguiriγas.\n\nQue hay que cantarlos. Urge que sean estu-\n\ndiados por los profesionales CON VISTAS A LA CONMEMORACION DEL QUINTO CENTENARIO DEL DESCUBRIMIENTO DE AMERICA. ¡HA! ¡Pobres cantes hispanoamericanos! Demos gracias a dicha conmemoración, porque de no ser por ella estos cantes hubieran desaparecido.\n\n¿Cuántos «gitanos» grabaron cantes mal titulados de ida y vuelta? Que yo sepa ninguno.\n\n— No hay que olvidar que estos cantes no son tan fáciles de interpretar como a primera vista puede parecer.\n\nPues bien, en vista de ese descomunal interés surgido de repente, yo voy a aportar mi pequeño granito de arena, permitiéndome hacer las siguientes recomendaciones:\n\n— Como se trata de cantes «dulces» hay que localizar y preparar a cantaores «preciosistas» y por supuesto hay que tener presente que, por lo general, no son cantes aptos para ser ejecutados por nuestros entrañables cantaores «gitanos». Al menos históricamente podemos decir que no lo fue. Y como renovadores que efectivamente los hicieron más taquilleros por más preciosistas, aunque menos flamencos, podemos reseñar, sin lugar a equivocarnos, al «Chato de las Ventas», «El Americano», «Pepe Marchena» y algún otro.\n\n— Hay que estar en posesión de un buen oído musical quizá por los muchos versos que los constituyen, su alto grado de preciosismo y sus excesivas modulaciones.\n\n— No hay que precipitarse en la búsqueda de los posibles futuros intérpretes porque aún queda largo tiempo por delante. — Hay que estudiarlos con perseverancia y total arrobo cuando se estén escuchando, y por supuesto hay que escucharlos en las voces de aquellos primeros especialistas que nos legaron sus grabaciones, como «El Mochuelo», «El Breva», Teresa España, «Niño de Cabra», «Niño de la Isla», «Pena padre» y Manolo Escacena.\n\nAngelillo (X) Chacón Bernardo el de los L. El Corruco Chaconcito El Americano (X) El Canario El Chato de las Ventas (X) El Mochuelo (X) Valderrama (X) La Andalucita La Rubia (X) Luquitas de Mna (X) El Malagueño (X) Vallejo Niño de la Huerta (X) Miguel Herrero Niña de la Puebla (X) Niño de Cazalla Niño Fanega Niño de la Flor Niño de la Isla (X) Niño León Niño de Marchena (X) Niño del Museo (X) Pena hijo Pena padre (X) Pepe Aznalcóllar Manolo Escacena (X) Garrido de Jerez Guerrita (X) Cepero Juan Breva (X) El Molinero (X) El Pinto Teresa España (X)",
    "title": "¿Cantes de ida y vuelta?",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 760,
    "article_char_count_full": 4611,
    "article_char_count_review": 4611,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-07-21-right-d-iscografia-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nD ISCOGRAFIA FLAMENCA\n\nUna vez más, hemos de agradecer a la casa Hispavox la buena idea de su colección MAESTROS DEL CANTE FLAMENCO, la cual está sirviendo para que jóvenes aficionados puedan escuchar las excelencias cantaoras de los grandes maestros desaparecidos.\n\nEste es el caso, del disco que nos ocupa. Mucho se ha dicho de la figura de Bernardo el de los Lobitos, e incluso de algunas de las grabaciones seleccionadas para este disco, que figuraron anteriormente en la famosa antología que a mediados de los cincuenta consiguiera el premio de la Academia Nacional del Disco de Francia.\n\nA lo largo del mismo vamos escuchando la maestría de un añejo cantar, con su voz fina y sensible, plena de matices flamencos y adobada de una sencillez sin igual, que hacen de estas grabaciones un\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\n, e incluso de algunas de las grabaciones seleccionadas para este disco, que figuraron anteriormente en la famosa antología que a mediados de los cincuenta consiguiera el premio de la Academia Nacional del Disco de Francia. A lo largo del mismo vamos escuchando la maestría de un añejo cantar, con su voz fina y sensible, plena de matices flamencos y adobada de una sencillez sin igual, que hacen de estas grabaciones un auténtico tesoro de nuestro arte. Qué bien conocía el cantaor los estilos. Cómo iba desarrollando a través de ellos, los diferentes personalismos que recogen estas grabaciones adaptándolos perfectamente a su personalidad sin desfigurar la del intérprete que rememora. Abre la cara A unos tientos llenos de matización flamenca, medidos hasta el máximo con una estructura casi perfecta. En la siguiente grabación, a través de la malagueña, nos recuerda a «La Trini», con una interpretación fina y sensible. Nos introduce perfectamente en las «temporeras» a través de los cantes de trilla que a tantos cantaores han servido como modelo. Recupera seguidamente las Nanas con suave modulación plena de ternura. Vuelve a dejar constancia de su compás y conocimiento en las Soleares de Utrera y cierra la cara A con los ecos mineros de las cartageneras. La cara B comienza con el medido compás de las bulerías que le dieron el nombre artístico, para continuar con gran melodiosidad por granaínas. Al igual que comentábamos para las Nanas, Bernardo vuelve a recuperar un estilo flamenco que\n\n[ENDING CONTEXT]\n\nel duende y al que ya bautizamos como el esperado fecundo manantial de un acervo que se nos va.\n\nSentimos expresarlo así, pero quienes lo definíamos como un oasis en el menguado desierto flamenco, en esta ocasión hemos de decir que ha sido incapaz de imponer la fidelidad y la pureza gitana ante las motivaciones comerciales. De todas formas no perdemos las esperanzas ante este amasijo de pureza y heterodoxia que flora sin rumbo por las inmensas aguas de una dinastía cantaora, a la que no se le van a descomponer las raíces por muy verderón que haya salido este junco. Así lo deseamos.\n\nDoscandil\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1391,
    "article_char_count_full": 8359,
    "article_char_count_review": 3120,
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
    "article_id": "1985-07-23-left-buz-n-flamenco",
    "article_text_for_review": "Con sumo agradecimiento a Yerga Lancharro\n\nEstimado Manolo:\n\nTe tengo por tan servicial como excelente aficionado y persona (y no es coba). Te vengo observando desde hace mucho tiempo que nos escribiste a la «Peña Juan Breva» recabando datos de arte y artistas malagueños para aumentar tus saberes al par que ofrecer los propios si es que falta hiciesen. Luego en diversas publicaciones, boletines o revistas, fuiste exponiendo detalles y curiosidades de tus manos y grandes archivos: el discográfico y el mental, con entusiasmo y desinterés admirables.\n\n¡Claro que tienes derecho —como cada quisque— a equivocarte!, y más por la barahunda de papeleo y añejas placas que tienes que manejar para ordenar y traer algo de luz. Pero yo no te reprocho los leves equívocos observados última-mente, sino ponértelos en conocimiento para la debida corrección en bien de todos los «aficionados».\n\nNo te decía que no fueras capaz de cantar tal malagueña, sino si te era posible amoldar unos canones (los de Reyes) a los (de Lema), tonal, prosódica y melódicamente diferentes. Sé que te apuntas, como uno, todo lo que encarte y sea menester.\n\nCómo no vamos a conocer aquí en esta Entidad los estilos de «La Trini» que eran cuatro: «No se borra de mi mente...», «Fuiste tú, paloma mía...», «Maldeciste a mi mare...» y «Que yo siga con mi pena...». También eran cuatro, creemos, los de Francisco Lema: «Si de ti pudiera vengarme...», «Desde que te conoci...», «Mi vía por aborrecerte...» y «Al campo me voy a llorá...». Como Chacón y La Trini, tenía más «letras» de sus varios estilos.\n\nDe don Antonio creemos que eran 15 los modelos que usaba, algunos con pequeña diferencia: «A qué niegas el delirio...», «A dar gritos me ponía...», «Viva Madrid que es la Corte...», «Del convento las campañas...», «En mi vía negaré...», «En un cementerio entré...», «A qué tanto me consientes...», «Se me presentó la muerte...», «A la derecha te inclinas...», «En San Antón me prendieron...», «Buscando la flor que amaba...», «Serrana no has compredió...», «Allí fueron mis quebrantos...», «Dando en el reloj la una» y «Cómo quieres que las olas...» (esta era llamada malagueña-totanera).\n\nMas, haber llegado a reunir siete u ocho viejas placas del jerezano, ya es un mérito y grande, y el tenerlas ahí en tu casa en pro y esclarecimiento del Arte y disponibles para la afición.\n\nGracias te repito, amigo Yerga, por tu larga labor.\n\nAbrázate, J. Márquez Cabello",
    "title": "Buzón flamenco",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 404,
    "article_char_count_full": 2434,
    "article_char_count_review": 2434,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-07-23-right-hablan-las-penas",
    "article_text_for_review": "Organizado por el Ayuntamiento de Santa Coloma de Gramanet (Barcelona) y las Peñas Flamencas, ha sido convocado el III Concurso de Cante Flamenco, al que podrán presentarse cuantos aficionados y profesionales no consagrados lo deseen respaldados por una Peña.\n\nHan sido establecidos dos grupos de cantes:\n\n1.º Obligatorio: Siguiriyas. Voluntario: Toná, Martinete, Debla, Carcelera, Tangos, Tientos y una mención especial a la Liviana, otorgada por la Peña Niña de la Puebla.\n\nEste grupo estará dotado con cuatro premios, el primero de 80.000 pesetas.\n\n2.º Grupo: Obligatorio: Soleares. Voluntario: Bulerías, Cantiñas, Alegrias, Tarantas, Mirabrá, Cartagenera, Malagueña, Granaína, Rondeña y una mención especial a la Caña otorgada por la colonia Egabrense. Para este grupo se han establecido cuatro premios, siendo el primero de 60.000 pesetas.\n\nLa final tendrá lugar el día 19 de octubre. Los interesados deberán dirigirse al Area de Educación Cultural, calle Lavaderos, 1-3, Santa Coloma (Barcelona).\n\nII Concurso Abanico Flamenco, en Badalona\n\nOrganizado por la Peña Flamenca «Los Hijos de Córdoba» de Badalona (Barcelona), se ha convocado el II Concurso «Abanico Flamenco», en él podrán participar cuantos aficionados y profesionales lo deseen. El plazo de inscripción finalizará el día 30 de octubre.\n\nSe han establecido dos grupos de cantes, así como cuatro premios siendo el primero de 80.000 pesetas. Se ha establecido, además, un premio de 35.000 pesetas a la mejor interpretación de la Soleá de Córdoba.\n\nPara más información, los interesados deberán dirigirse a la citada Peña, calle Balmes, 37 o bien llamando al teléfono (93) 3836203.\n\nNueva Junta Directiva en la Peña Flamenca de Huelva\n\nEn Asamblea General Ordinaria celebrada por la Peña Flamenca de Huelva, resultó elegida nueva Junta directiva, la cual quedó compuesta de la siguiente manera: Presidente, Juan M. Lorenzo Márquez. Vicepresidente, José Ruciero Martell. Secretario, Antonio Márquez Muñoz. Tesorero, Manuel Walkelin Calero. Director academia, José Sollo García. Vocales, Rafael Jurado García, Leonardo Pásaro Días, Mario Garrido Cabezas. Domingo Martín Ramos, José A. Romero Rodríguez, Antonio Vázquez Crespo, Eusebio Domínguez del Molino, Aurelio Gutiérrez Hernández, Andrés Lineros Domínguez, José Sánchez Suárez y Diego Ortiz Jiménez.\n\nDeseamos toda clase de éxitos a los amigos de Huelva.\n\nPORTADA DE NUESTRO NUMERO ANTERIOR\n\nPor un error ajeno a nuestra voluntad, el autor de la portada del número anterior, no es el que figuraba en el sumario. Hemos de hacer justicia diciendo que el óleo titulado «Tierra sobre tierra» es obra del pintor jiennense Miguel Ayala. Pedimos disculpas a nuestro buen amigo y también a nuestros lectores.",
    "title": "Hablan las peñas",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 409,
    "article_char_count_full": 2719,
    "article_char_count_review": 2719,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-07-24-left-relaci-n-de-placas",
    "article_text_for_review": "Por: M. Yerga Lancharro",
    "title": "Relación de placas",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 4,
    "article_char_count_full": 23,
    "article_char_count_review": 23,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
