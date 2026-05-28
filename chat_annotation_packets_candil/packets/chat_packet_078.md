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
    "article_id": "1983-07-21-right-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFESTIVAL INTERNACIONAL DE LA GUITARRA. 3.er ENCUENTRO FLAMENCO\n\nRGANIZADO por el Centro Flamenco «Paco Peña» y patrocinado por la Delegación de Cultura del Excmo. Ayuntamiento de Córdoba, se ha celebrado en la ciudad hermana el Festival Internacional de la Guitarra. 3.er Encuentro Flamenco, durante los días del 11 de julio al 6 de agosto, con el siguiente programa:\n\nCursos Internacionales de Guitarra Flamenca dirigidos por Paco Peña y Mario Escudero; de guitarra clásica dirigido por John Williams y de danza por Inmaculada Aguilar. En el ciclo de conciertos, ha habido un programa excelente, con recitales flamencos a cargo de Pepe Lora, verterano cantaor muy querido de la afición cordobesa. Manuel Soto «El Sordera», «El Chaparro» y el ganador del X Concurso Nacional de Cante de Córdoba,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nl ciclo de conciertos, ha habido un programa excelente, con recitales flamencos a cargo de Pepe Lora, verterano cantaor muy querido de la afición cordobesa. Manuel Soto «El Sordera», «El Chaparro» y el ganador del X Concurso Nacional de Cante de Córdoba, Juan Moreno Maya «El Pele», con las guitarras de Moraito Chico y Manuel de Palma. En guitarra flamenca, también ha intervenido figuras de la talla de Sabicas, Manuel Cano, Mario Escudero y Paco Peña. En guitarra clásica la participación ha sido de lujo, con John Williams, Benjamin Verdery y David Russell. En el baile, actuó el gran Mario Maya, que lo hizo en la gran fiesta final. En definitiva, un programa ambicioso y sugerente, que ha tenido muy buena acogida en la afición cordobesa. Los conciertos en su mayoría se han celebrado en la Posada del Potro, sede actual de la Delegación de Cultura el Avuntamiento de Córdoba. Nuestra enhorabuena a la Delegación de Cultura y al Centro Flamenco Paco Peña. XI FESTIVAL FLAMENCO. PEÑA ENRIQUE EL MELLIZO L sábado 20 de agosto se celebró en Cádiz, organizado por la Peña Flamenca «Enrique El Mellizo» el XI Festival de Arte Flamenco, que como ya es tradicional, tuvo una asistencia masiva de aficionados; calculamos que se dieron cita más de dos mil personas en el Teatro Pemán. En esta ocasión, el festival estuvo dedicado a homenajear al cantaor gaditano Juanito Villar, al que las peñas gaditanas entregaron placas en recuerdo del homenaje. En esta reunión flamenca sobresalió el cante de Antonio Núñez «Chocolate», con ese metal de voz tan puro y tan gitano. Una actua\n\n[ENDING CONTEXT]\n\nmás al norte de toda España, ya que las provincias limítrofes, situadas todavía más al norte que nuestra capital (León, Santander, Asturias o Galicia), no poseen peña semejante, pudiendo decir con toda justicia que es la más septentrional de cuantas acuden a los Congresos de Actividades Flamencas, que vienen celebrándose desde hace años, primero en Sevilla y sucesivamente en Málaga, Almería y Jaén.\n\nRestaurante\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "— HABLAN LAS PEÑAS —",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1115,
    "article_char_count_full": 7125,
    "article_char_count_review": 3194,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "1983-07-22-right-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTítulo: HOMENAJE A TERREMOTO DE JEREZ.\n\nCanta: Terremoto de Jerez.\n\nToca: Manuel Morao y Paco de Antequera.\n\nReferencia: Hispavox, S. A. 157001 (2 LP's).\n\nNA expresiva y abierta fotografía de Fernando Fernández Monje, realizada en la Peña Flamenca de Jaén con el objetivo invisible de la emoción y la amistad, nos señala el camino de un doble LP que Hispavox ha editado bajo el entrañable título de «Homenaje a Terremoto». Cuidada la presentación, acertado el planteamiento, esta realización de la prestigiosa firma discográfica es algo a tener en cuenta por la afición flamenca.\n\nEl primer disco recoge la voz del desaparecido cantaor interpretando soleares, bulerías por soleá, tientos, siguiriyas, bulerías; en la cara B, fandangos, soleares, siguiriyas y bulerías. El segundo LP reúne un racimo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombres\"]\n\neditado bajo el entrañable título de «Homenaje a Terremoto». Cuidada la presentación, acertado el planteamiento, esta realización de la prestigiosa firma discográfica es algo a tener en cuenta por la afición flamenca. El primer disco recoge la voz del desaparecido cantaor interpretando soleares, bulerías por soleá, tientos, siguiriyas, bulerías; en la cara B, fandangos, soleares, siguiriyas y bulerías. El segundo LP reúne un racimo de nombres y hombres que acuden presurosos a este homenaje a Terremoto. Realmente, todo lo que aparece en el álbum se ha escuchado ya en otros discos. Es pues, creemos, una recopilación que, al calor de Terremoto, se pretende permanezca en la actualidad. Y en unas páginas especiales que «CANDIL» dedicara al genial y jondo jerezano, sintetizábamos nuestra opinión sobre los discos grabados por el cantaor. Así, entonces decíamos y hoy repetimos: «A la hora de hacer un somero repaso a lo grabado, cabe señalar que es un reflejo de sus actuaciones. Su profesionalidad, en ocasiones irregular, producto que duda cabe, de un raro-rebelde-peculiar ser, también aparece en los discos. Dentro de una verdad indiscutible e indiscutida: la raíz gitana, Terremoto ha dejado, discográficamente, firmes muestras de su arte. No nos gustaría especificar, porque en el cantaor de Jerez hay que hablar de un todo. No de partes. Había, hay, una forma de sentir y expresar el cante. Ciertamente, existen unos cantes inscritos con más fuerza en su geografía cantaora: bulerías, soleares, siguiriyas, fandangos, dando a todos ellos los específicos acentos de su Jerez natal. En su voz —el mundo se le quedaba chico para su grito dionisiaco, escribió Juan de la Plata—los estilos t\n\n[ENDING CONTEXT]\n\nconfigure su personalidad cantaora.\n\nDOSCANDIL\n\nMedalla de Plata en el X Salón Internacional de Bruselas\n\nFabricación de toda clase de plantillas ortopédicas en conglomerado de caucho y corcho, con extensa gama de piezas accesorias para confeccionar y adaptar a las mismas.\n\n(Arcos internos o longitudinales. Arcos transversos. Cuñas pronadoras y supinadoras. Herraduras, etc.)\n\nLas plantillas y piezas accesorias, se hacen en tres consistencias: BLANDAS, DURAS Y SEMIDURAS. También fabricamos según diseño Técnico.\n\nFábrica y oficinas: Arrastradero, 6 y 8 - Teléfonos 22 33 92 y 22 51 12 - J A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1629,
    "article_char_count_full": 9968,
    "article_char_count_review": 3326,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombres"
      }
    ]
  },
  {
    "article_id": "1983-07-24-left-placas-de-artistas-flamencos",
    "article_text_for_review": "JOSE PALANCA",
    "title": "Discografía (placas) de artistas flamencos José Palanca",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 2,
    "article_char_count_full": 12,
    "article_char_count_review": 12,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-07-24-right-jos-palanca",
    "article_text_for_review": "IN MEMORIAM\n\nCAJA GENERAL DE AHORROS Y MONTE DE PIEDAD DE GRANADA\n\nLA GENERAL",
    "title": "José Palanca",
    "periodical": "candil",
    "issue_id": "1983-07",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 14,
    "article_char_count_full": 77,
    "article_char_count_review": 77,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-09-2-right-editorial",
    "article_text_for_review": "DIRIGEN: Ramón Porras Pedro Sánchez\n\nONSEJO DE REDACCION: José L. Buendía, Juan Antonio Ibáñez, Fausto Olivares, Rafael Valera, Miguel Calvo, Alfonso Fernández y Antonio Trujillo.\n\nCRETARIA, ADMINISTRACION Y GERENCIA: Joaquín Sánchez, Juan J. Carras- cosa y José Cruz García. LABORADORES:\n\nManuel Andujar, Alfredo Arrebola, Manuel Barrios, José Blas Vega, J. M. Caballero, Donald Luis Caballero, Antonio Escribano, Alejandro Fernández, Cotta, Agustín Gómez, Félix Grande, José Heredia Maya, Antonio Hernández, Arcadio Larrea, Pepe Marín, Antonio Mata Gómez, Sofia Noel, Antonio Núñez, José L. Ortiz Nuevo, J. A. Pérez Bustamente, Antonio Piñana, Juan de la Plata, Antonio Povedano, Fernando Quinones, Manuel Ríos Ruiz, Manuel Ríos Vargas, R. Rodríguez Cosano, Guillermo Sena, Francisco Vallecillo y Manuel Yerga.\n\nPORTADA:\n\n«Lo osceno de la soleá». Oleo de Miguel Ayala.\n\nCONTRAPORTADA: Juanito Valderrama.\n\nFOTOGRAFIAS: Jaime Luque, Francisco Olivares, Fausto Olivares. Archivo «Candil». ANAGRAMAS: «Vica». REDACCION Y ADMINISTRACION: Maestra, 16 - Jaén (España). Teléfono (953) 23 29 36. E D I T A : Peña Flamenca de Jaén. MARCA N.º 911.293. IMPRIME:\n\nArtes Gráficas Sociedad Provincial, S. A. Ortega Nieto, 3 - Jaén Depósito Legal: J. 133 - 1978\n\nNada tiene de extraño que esta, en cierta medida precursora, interpretación de la historia andaluza, muestra tan evidentes concomitancias con quienes, sensibilizados por la necesidad de una busca de nuestra identidad cultural, encaran el flamenco, en la actualidad, como un signo más de una civilización brillante que a este Sur atormentado se le ha expropiado.\n\nN mil novecientos ochenta y tres se cumplen cien años del nacimiento de un ilustre andaluz: Rafael Cansinos Assens. Cien años que no han sido suficientes para recuperar en toda su dimensión literaria y jonda la tan ignorada como inconmensurable personalidad de este autor sevillano.\n\nLa importancia como instrumento de exégesis de «La Copla Andaluzia» estriba, a nuestro juicio, en el enfoque original con que se plantean integralmente las formas de expresión andaluzas. En las antípodas del empalagoso entorno quinteriano, Cansinos estructura una teoría dulce y tremenda sobre la copla que vehicula, por otra parte, la historia de las reivindicaciones andaluzas; es decir, se produce el primer intento riguroso, aunque parcial, por retomar el hilo conductor de una historia especificamente andaluza. Cansinos Assens es uno de los personajes más eruditos de este siglo y su sensibilidad para el estudio de temas orientales y referentes a nuestra tradición árabe y judía, está patente en su obra. Pocos autores mejor preparados que él para abordar una misión tan compleja y arriesgada. El mundo real de la copla se conecta sustancialmente con el flamenco, y en cuyas raíces rastrea —Cansinos— las huellas de la injusticia, del hambre, de la persecución, el grito del hombre apaleado y sin derechos en una tierra que sólo le sirvió de tajó y de osarios.\n\nA la editorial Demófilo se debe la reedición, en mil novecientos setenta y seis, de «La Copla Andaluza», obra de significada trascendencia para la comprensión del fenómeno flamenco, que ha pasado desapercibida como tantas otras de Cansinos Assens.\n\n«CANDIL» quiere rendir en el primer centenario de su nacimiento un emocionado recuerdo a Rafael Cansinos Assens, autor prolífico como pocos, y de una perspicacia extraordinaria para profundizar en el arcano mundo de lo jondo. Con ello queremos hacer patente nuestro intento de contribuir, en la medida de nuestras humildes posibilidades, a rescatar del olvido y de la indiferencia a valores como Rafael Cansinos Assens que, desde casi el anonimato, tanto han aportado al esclarecimiento del flamenco.\n\nImposición de la Medalla de Trabajo a Juanito Valderrama Literatura romántica y cante flamenco ¿Responsabilidad de figura? «Con ternura y con gran valor» Colección de guitarras de Manuel Cano Algo sobre las tarantillas de estilos mineras Ellos, los protagonistas, Carlos Cruz Victoria de Miguel Chequeo grafológico Las letras flamencas de José Sánchez del Moral. ¿De qué enfermedad mueren nuestros artistas? Cayetano Muriel «Niño de Cabra» Hablan las peñas Buzón flamenco Discografía flamenca Discografía (placas) de artistas flamencos\n\nNOTA.—«CANDIL» no se hace necesariamente solidario de los puntos de vista contenidos en los artículos firmados. Es, incluso, consciente de que muchos de ellos versan sobre materia controvertida, y por ello invita a los estudiosos de estos temas al debate de los mismos.\n\n«CANDIL» agradece a la Junta de Andalucía y a la Excma. Diputación Provincial su colaboración en este número.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1983-09",
    "year": 1983,
    "language": "es",
    "article_type": "editorial",
    "pages": "2-3",
    "page_number": 2,
    "word_count": 707,
    "article_char_count_full": 4634,
    "article_char_count_review": 4634,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
