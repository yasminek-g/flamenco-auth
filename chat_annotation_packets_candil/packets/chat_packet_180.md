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
    "article_id": "1988-09-23-left-hablan-las-pe-as",
    "article_text_for_review": "Nueva Peña Flamenca\n\nEn la ciudad de Albacete ha sido fundada una nueva peña flamenca con el nombre de «El Altozano», cuya sede social ha sido fijada en la calle Concepción, Cafetería Bonsay, y cuya primera junta directiva ha quedado constituida de los nombres siguientes: presidente, Pedro Fernández Fernández; secretario, José Jorquera Manzanares; tesorero, Jesús Bernal Lorenzo; vocales, Antonio Moreno García, Juan Ramón Bravo Muñoz y Luis García Pardo.\n\nNuestra cordial enhorabuena a todos, con nuestros mejores deseos de una larga andadura flamenca.\n\nLa Peña Flamenca «Rincón del Cante» nombró nueva junta directiva\n\nEn asamblea general de socios celebrada el pasado día 15 de septiembre, la Peña Flamenca «Rinción del Cante» de Córdoba, eligió nuevo presidente, habiendo quedado, por tanto, la nueva junta directiva de la forma siguiente: presidente, Antonio Ruz Espinoza; vicepresidente, Jorge Gil Arjona; secretario, José Urbaneja Diéguez; vicesecretario, Jesús Perula Luna; tesorero, José Luis Otero Nieto; vocales, Bernardo Mesa García, Rafael Delgado Centella, Francisco Peñas Aranda y José Salinas Martín.\n\nDeseamos muchos éxitos a los amigos de Córdoba.\n\nV Concurso de Cante «Rincón Flamenco»\n\nOrganizado por la Peña «El Rincón Flamenco» de Córdoba, con el patrocinio de la Diputación Provincial, Ayuntamiento de Córdoba, la Federación Provincial de Peñas Flamencas y firmas comerciales, ha sido convocado el V Concurso de Cante «Rincón Flamenco».\n\nSe han establecido tres grupos de cantes y cada cantaor deberá interpretar un cante de cada grupo, más uno libre a su elección.\n\nLas fases selectivas se celebrarán a partir del 18 de noviembre.\n\nPara esta edición se han establecido cinco premios y dos especiales, siendo el primero de 125.000 pesetas.\n\nPara más información, los interesados pueden dirigirse a la Peña «El Rincón Flamenco», calle Rejas de Don Gome, 4, Córdoba.\n\nLa Federación de Peñas Flamencas de la provincia de Jaén, informa:\n\nEl viernes día 7-10-88, en la sede de la Peña Flamenca de Jódar (Jaén), se celebró asamblea general extraordinaria de la Federación Provincial de Peñas Flamencas de Jaén, para analizar la precaria situación por la que atraviesa la Federación, situación motivada por la inoperancia del hasta entonces presidente de la misma.\n\nDespués de una breve exposición de la situación de cara a la Confederación Andaluza de Peñas Flamencas y a los Organismos Oficiales de la provincia, y al poco apoyo de estos últimos a nuestra Federación, debidos principalmente a la dejadez y a la falta de espíritu del presidente, la asamblea decide:\n\n1.º Conforme a los actuales estatutos, artículo 12, apartado d), se acuerda por 10 votos a favor y ninguno en contra, el separar a don Vicente Alises Campos del cargo de presidente de la Federación Provincial de Peñas Flamencas de Jaén.\n\n2.º Se acuerda nombrar una junta gestora que se encargue de convocar, en el tiempo y forma establecido, elección de nuevo presidente, y que mientras tanto se haga cargo de la Federación, con todos los derechos y deberes reconocidos en los actuales estatutos para la nueva gestora, formada por: presidente, don Fernando Medina de la Rosa; secretario, don Antonio Vázquez Lozano; tesorero, don Francisco Moreno Galán.\n\nDespués de ser nombrada esta junta gestora, agradeció a todos los asistentes la confianza depositada en ellos y pidió la colabora-ción de todas las Peñas para poder sacar adelante la Federación.\n\nEn esta misma asamblea se presentó la nueva Peña Flamenca «Calixto Sánchez» de Espeluy, que con anterioridad había solicitado su adhesión a la Federación de Peñas Flamencas de Jaén.\n\nEn reciente asamblea de socios celebrada por la Peña Flamenca «La Bulería» de Jerez de la Frontera, resultó elegida nueva junta directiva, habiendo quedado la misma compuesta de la siguiente manera: presidente, Rafael Banderas Rubiales; vicepresidente, Carlos Domínguez Caravaca; secretario, Fermín Galán Egea; tesorero, José Antonio Gámez Marín; vocales, Antonio Núñez Romero, Juan Macías Ruiz, Juan Rosado Cabezas y Francisco Bermejo Solís.\n\nNueva junta directiva de la Peña «La Bulería» de Jerez\n\nDeseamos toda clase de éxitos a la nueva ejecutiva.",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 647,
    "article_char_count_full": 4171,
    "article_char_count_review": 4171,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-09-23-right-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n1.ª.—Podrán tomar parte en este Concurso los cantaores, tocaores y bailadores, de uno y de otro sexo, profesionales o aficionados, que hayan cumplido 16 años el día en que finalice el plazo de inscripción y se ajusten a lo dispuesto en las presentes bases.\n\n—Fecha y lugar de nacimiento.\n\n—Lugar de residencia. —Domicilio.\n\n2.ª.—Los interesados en participar en el Concurso deberán solicitar su inscripción mediante escrito dirigido a la Comisión Organizadora del XII Concurso Nacional de Arte Flamenco, domiciliada en Oficina Municipal de Turismo, Plaza de Judá Leví, s/n. -14003-Cordoba, hasta el 15 de abril de 1989, indicando en su escrito:\n\n—Nombre y dos apellidos. —Nombre artístico.\n\n—Teléfono.\n\n—Sección (cante, baile o toque) en que concursa y premios a los que se presenta.\n\n3. a.—Los\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_01 | trigger=\"dentro\"]\n\nnteriores del Concurso, no podrán aspirar a aquellos premios en que están incluidos los cantes o bailes por los que fueron premiados, independientemente de la denominación actual de los premios, aunque sí podrán optar a los restantes. Los premios obtenidos en anteriores ediciones del Concurso podrán ser valorados por el Jurado a efectos de una posible concesión de los especiales Silverio o Antonio. 4.ª.—Los gastos de viaje de los concursantes, dentro del territorio español, serán abonados por la Organización a razón de 17 pesetas el kilómetro. Los no residentes en el término municipal de Córdoba, devenga-rán una dieta diaria de 5.000 pesetas durante los días en que, a requerimiento del Jurado, inter-vengan oficialmente en el Con-curso. 5.ª.—La Comisión Organizadora tendrá, a disposición de los concursantes, profesionales cualificados para acompañamiento en cualquiera de las secciones del Concurso. No obstante lo anterior, los concursantes podrán presentarse a participar con su propio acompañamiento, siendo los gastos de su exclusiva cuenta. 6.ª.—Los profesionales contra-tados por la Organización para acompañamiento no podrán participar en calidad de concursantes. 7.ª.—El Concurso constará de dos fases: una de clasificación (eliminatoria) y otra de opción a premio. 8.ª.—Podrán ser dispensados de la fase de clasificación, los concursantes que, a juicio del Jurado, reúnan la calidad necesaria para optar a premios. 9.ª.—El Jurado se reserva el de-recho a no admitir a ninguna de las fases del Concurso, a aquellos solicitantes que, a su juicio, no reú-nan la calidad mínima que la categoría del Certamen exige. 10.ª.—La fase de clasificación se celebrará durante los días 2 al 11 de mayo de 1989. El lugar, día y hora será comunicado previamente a los concursantes admitidos. 11.ª.—Cuando el Jurado considere que tiene elementos de juicio suficientes, podrá suspender la actuación del Concursante. 12. a.—Las pruebas de la fase de opción a premios serán públicas y se celebrarán durante los días 12 al 18 de mayo de 1989. 13.ª.—Los concursantes premiados quedan formalmente obligados a actuar en un solemne acto público que se celebrará el día 20 de ma\n\n[ENDING CONTEXT]\n\nDE TOQUE DE GUITARRA\n\nPremio «Ramón Montoya». Diploma y 150.000 pesetas.\n\nSólo flamenco (concierto).\n\nPremio «Manolo de Huelva». Diploma y 150.000 pesetas.\n\nAcompañamiento a cante y baile.\n\n21.ª.—El Jurado podrá declarar desiertos los premios que considere oportunos a la vista de la actuación de los concursantes.\n\n22. a.—Se entiende que por el hecho de concurrir a este Certamen, los concursantes aceptan incondicionalmente estas Bases, así como cualquier resolución que se adopte por incidencias no previstas, tanto por la Comisión Organizadora como por el Jurado del Concurso.\n\nCórdoba, 1988\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario Flamenco",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 1044,
    "article_char_count_full": 6919,
    "article_char_count_review": 3792,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "dentro"
      }
    ]
  },
  {
    "article_id": "1988-09-24-right-discograf-a-flamenca-placas",
    "article_text_for_review": "Por: Manuel Yerga",
    "title": "Discografía Flamenca (Placas)",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 3,
    "article_char_count_full": 17,
    "article_char_count_review": 17,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-11-4-left-a-juan-breva-una-voz-con-seis-cu",
    "article_text_for_review": "D el cante a oro y fuego en los almanayes una esfinge humana florea con su voz de taracea la madera de ciprés que se figura voz y diapasón cuando la caja de resonancia echa el marfil de sus vientos al pentagrama diáfano de la malagueña.\n\nLa Serranía se hace eco y abanico de sal cuando Juan Breva se echa la guitarra al cuerpo y memoriza su vida en un cante alante vibrando en la pena de un Polifemo sin ojo. Desde el café cantante hasta las setentayocho revoluciones, la fotografia de un hombre fundido entre el contrapunto de un apéndice de madera mueve el alma y rasguea bandolás, verdiales, rondeñas o jaberas como almáciga veleña para tocarle a la historia falsetas de olas y ramas. Un hombreguitarra como un centauro galopa ciego por el recuerdo Juan Breva, una voz con seis cuerdas caña de azúcar y limón palosanto en la mirada se deja retratar eterno, callado, como los dioses siempre a punto de arrancar.\n\nJesús Cuesta Arana",
    "title": "A Juan Breva (Una voz con seis cuerdas)",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "4-4",
    "page_number": 4,
    "word_count": 168,
    "article_char_count_full": 933,
    "article_char_count_review": 933,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-11-4-right-el-flamenco-en-la-prensa",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJean Paul Tarby\n\nEn la abundante bibliografía que desde casi un siglo trata del flamenco a partir de enfoques cada vez más diversificados, parece que no existe ningún estudio sistemático de la importancia ocupada por el arte flamenco en la prensa de los dos últimos siglos. Es como si, a los estudiosos del arte gitano andaluz, no les hubiera interesado tal campo de investigación (1).\n\nUn solo ejemplo bastará para poner de manifiesto esta situación. Ningún autor, entre todos los que hemos leído, menciona la existencia de antiguas revistas que hacia finales del siglo XIX o principios del siglo XX, ya se dedicaban exclusiva o parcialmente, a la defensa y divulgación de la cultura flamenca.\n\nAdemás de tales publicaciones más o menos especializadas, es seguro que existirían otras más, trátese\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"crítica\"]\n\nque hacia finales del siglo XIX o principios del siglo XX, ya se dedicaban exclusiva o parcialmente, a la defensa y divulgación de la cultura flamenca. Además de tales publicaciones más o menos especializadas, es seguro que existirían otras más, trátese de diarios, revistas científicoliterarias, enciclopédicas, teatrales, musicales, o revistas de diversiones y espectáculos, en los cuales no es imposible encontrar un artículo, una entrevista, la crítica de una actuación, un poema o un dibujo que tenga como tema el arte flamenco. El flamenco, como cualquier producto artístico y cultural fue y sigue siendo juzgado y comentado en la prensa española. Sería, pues, una pena que dejáramos «dormir» en los anaqueles de los archivos hemerográficos tantos documentos que sin duda alguna proporcionarán numerosísimos datos sobre la historia del flamenco, cuanto que son escasos estos datos como lo subraya Elías Teres Sádaba en la conferencia que presentó en la reunión internacional de estudio de los orígenes del cante flamenco en 1969: «Cuando se habla de los primeros tiempos de la historia del cante flamenco, se suele evocar la información que Juanero cantaor gitano de Jerez facilitó a don Antonio Machado y Álvarez hacia 1880. Según ésta, el cante hubiera empezado a aparecer más o menos a finales del siglo XVIII, con el famoso Tío Luis el de la Juliana quien encabeza una lista de cantaores que va extendiéndose, y en la cual aparecen luego los apellidos de los contemporáneos de Demófilo. Pero se trata de una tradición oral relativamente tardía, y todos sabemos que las tradiciones orales, incluso si se les concede un fondo de verdad, han de ser consideradas con mucha prudencia al momento de redactar una historia rigurosamente documentada. Es necesario recurrir a otras fuentes si existen» (2). ¡Sí que existen!, y la prensa española es una de ellas, por lo menos lo pudimos comprobar a lo largo de nuestras investigaciones. En el editorial de uno de los últimos números de CANDIL, el periodista subraya el papel cada vez más significativo que siguen desempeñando desde años, los\n\n[ENDING CONTEXT]\n\ncante flamenco», Actes de la réunion internationale sur les origines du chant flamenco, publicación del centro de estudios de la música andaluzay del flamenco, Madrid, 1969, pág. 11 (traducción francesa).\n\n(3) Candil, editorial, núm. 52, 1987, pág. 5.\n\n(4) El Folk-Lore Andaluz (1882-1883), edición de José Blas Vega, edición del Ayuntamiento de Sevilla, Col. Alatar, núm. 5, Sevilla, 1981, pág. XXXI.\n\n(5) Ver MOLINA FAJARDO, Eduardo: Manuel de Falla y el Cante Jondo, publicación de la Universidad de Granada, 1962. (6) CRUZ CONDE, Antonio: «El concurso de 1956», Candil, núm. 44, 1986, pág. 35.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Flamenco en la Prensa",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 3252,
    "article_char_count_full": 19875,
    "article_char_count_review": 3717,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "crítica"
      }
    ]
  }
]
```
