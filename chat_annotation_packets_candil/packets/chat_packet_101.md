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
    "article_id": "1984-09-22-left-la-guitarra-flamenca",
    "article_text_for_review": "CUANDO se escribe de memoria y especialmente cuando deben manejarse datos de cronología y homónimia, el pliego de olvidos y omisiones nos asalta a cada paso y así nos ha sucedido en nuestra anterior crónica en estas mismas columnas. Aunque en ella no intentábamos formular, para cada época de la interpretación guitarrística, unos censos completos ni mucho menos exhaustivos, al releer el original publicado nos hemos advertido de una más sensible omisión, la de Víctor Monje, SERRANITO, uno de los grandes adalides de la guitarra flamenca de concierto en la época actual. Víctor, que es resumen de modestia, generosidad y buen sentido, sabrá disculparnos y aceptará sin reservas el perdón que le solicitamos ahora.\n\nSi estrujamos un poco nuestra reseca memoria, todavía nos quedan en nuestra menguada retentiva algunos nombres trascendentes, algunos con los que honramos en el pasado nuestra amistad —José Capinetti, en Cádiz, por ejemplo—, otros que prestigian en la actualidad el toque de acompañamiento al nivel de la restante familia Carmona Carmona, los Habichuela verdaderamente granaños (Juan nació en Málaga). Y tantos otros como irían conformando un amplio elenco, entre ellos el Niño Pérez (de quien sólo nombramos su dinastía), Luis Molina, Luis Yance, Manuel Naranjo... Y tantos más de antes y de aho-\n\nCorrea Weglison, 9\n\nJ A E N",
    "title": "La guitarra flamenca",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 214,
    "article_char_count_full": 1343,
    "article_char_count_review": 1343,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-09-22-right-buz-n-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMadrid, 19-de octubre de 1984\n\nDon Ramón Porras\n\nDirector de «Candil»\n\nMi querido amigo:\n\nJaén\n\nPor mis múltiples ocupaciones, que no se constrñen exclusivamente al estudio, crítica y divulgación del arte flamenco, llevo bastante retraso en la confección de fichas y ordenación de materiales sobre este tema, tarea en la que actualmente me hallo enfrascado con la pretensión, siempre un tanto quimérica, de ponerme al día. Quienes se dedican a parecidos afanes me comprenderán, pues supongo que a todos nos pasa un poco lo mismo.\n\nPues bien, esta carta y estas explicaciones vienen a cuento porque, enfrascado en esa labor, le llegó el turno al número 21 de esa revista, de fecha mayo-junio 1982, que diriges junto a Pedro Sánchez, y en cuya página 30 hay una «carta» de A. Gómez Alfaro en la que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"Editorial\"]\n\n, no había reparado. La «carta» en cuestión es réplica a un artículo anterior de Manuel Yerga Lancharro, quien pone en tela de juicio la veracidad de algunas afirmaciones de Gómez Alfaro sobre los últimos días de Chacón vertidas en otro artículo anterior que reprodujera la revista «Sevilla Flamenca». Como quiera que Gómez Alfaro en su réplica alega haber tomado esos datos presuntamente erróneos de mi libro «Historia del Cante Flamenco» (Alianza Editorial, Colección de Bolsillo núm. 836, págs. 159-160, Madrid, 1981), y lamenta como lo hiciera Manuel Urbano en su crítica del mismo aparecida en el número 19 de CANDIL la ausencia de las notas «que indicasen el origen exacto de la documentación manejada», quedando en definitiva la cuestión debatida sin resolver salvo lo que yo tuviera que decir al respecto sobre mis fuentes, es por lo que —excesivamente tarde, lo reconozco— me siento obligado a dirigirte estas aclaraciones que no hago con el propósito de entrar en polémica alguna, sino únicamente con el fin de dilucidar esa cuestión a los lectores que puedan estar interesados en la misma y en primer lugar a Gómez Alfaro. Copio textualmente de la «carta» de réplica de Gómez Alfaro: «Desgraciadamente, Angel Alvarez Caballero desconocía la biografía de Chacón que tan amorosamente es- cribió el autor de “Enderezando entuertos”, pues, en las páginas 159-160, dice así sobre los últimos años del gran artista flamenco: “Pero entonces se puso enferma Anita, la compañera de tantos años de Chacón, y la situación se le puso tan negra en el aspecto económic\n\n[ENDING CONTEXT]\n\nadmitirlo así. Quienes pretendemos historiar el arte flamenco, si lo hacemos con honradez, debemos alegrarnos cada vez que un error, una leyenda o una hipótesis dudosa son reemplazados por un dato fehaciente, sobre todo en materia tan huérfana de fuentes fiables y rigurosas como la nuestra. Este es todo cuanto puedo aclarar por mi parte en relación a ese tema. Recibe un fuerte abrazo.\n\nFdo.: Angel Alvarez Caballero\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nJ A E N\n\nRoldán y Marín, 7\n\nTeléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Buzón Flamenco",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1263,
    "article_char_count_full": 7502,
    "article_char_count_review": 3194,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "Editorial"
      }
    ]
  },
  {
    "article_id": "1984-09-23-right-hablan-las-pe-as",
    "article_text_for_review": "FESTIVAL FLAMENCO 1984 DE LA FEDERACION DE PEÑAS FLAMENCAS DE MADRID\n\nRGANIZADO por la Federación Provincial de Peñas Flamencas de Madrid, y en su domicilio social, tendrá lugar el día 9 de noviembre el Festival Flamenco 1984, en el que actuarán al cante: MARIANO MORILLA, JUAN CASILLAS y JULIA MENESES, acompañados a la guitarra por: ANTONIO AMAYA, JOSE MANUEL MONTOYA y EL MAMI. Al baile: Aire flamenco «LOS TRUCO» y Cuadro Flamenco «LOS CABALES». Contando, además, con la colaboración extraordinaria de AGUSTIN FERNANDEZ y RICARDO «EL YUNQUE».\n\nVI CONCURSO DE MODELOS PARA EL CARTEL ANUNCIADOR DEL VIII CONCURSO DE CANTE JONDO\n\nAN sido hechas públicas las bases para el concurso del cartel anunciador del VIII Concurso de Cante Jondo que organiza la Peña Flamenca «La Platería» de Granada.\n\nEl tema del cartel será de libre elección, inspirado, lógicamente, en motivos de Cante Jondo, con algún símbolo granadino.\n\nPara este concurso se ha establecido un único premio indivisible de CIEN MIL PESETAS y un accésit de VEINTICINCO MIL PESETAS.\n\nTodas las personas que deseen optar al concurso deberán presentar la obra montada sobre tablero o bastidor, en la secretaría de la Peña «La Platería», Placeta de Toqueros, 7. 18010-GRANADA, antes del día 10 de enero de 1985.\n\nLa rotulación que habrá de llevar es la siguiente: VIII CONCURSO DE CANTE JONDO «PEÑA PLATERIA». PATIO DE LOS ALJIBES DE LA ALHAMBRA, 5 JUNIO. GRANADA, 1985.\n\nNUEVA JUNTA DIRECTIVA EN LA PEÑA FLAMENCA «FOSFORITO» DE MADRID\n\nN Asamblea General celebrada el pasado día 5 de octubre por la Peña Flamenca «Fosforito» de Madrid, resultó elegida nueva Junta Directiva, quedando constituída de la forma siguiente: Presidente: JUAN FERNANDEZ VALENZUELA. Vicepresidente: SANTIAGO CABALLERO CUBERO. Secretario: LUCIANO ALTAMIRANO REDONDO. Tesorero: RAFAEL SANCHEZ MENDIETA. Relaciones Públicas: JOSE ANTONIO JAREÑO MOLERA. Vocales: FELIPE PAEZ ALJAMA y FRANCISCO CRIADO BAENA. Deseamos toda clase de acierfos a la nueva junta.\n\nNUEVA JUNTA DIRECTIVA DE LA PEÑA FLAMENCA «RINCON DEL CANTE» DE CORDOBA\n\nN Asamblea General celebrada el día 4 de septiembre por la Peña Flamenca «Rin- E con del Cante», resultó elegida nueva Junta Directiva, siendo reelegido presi- dente nuestro buen amigo PACO RUIZ, quedando el resto de la Directiva de la forma siguiente: Vicepresidente: JOSE MARIA SALOR SOLIS. Secretario: JOSE UR- BANEJA DIEGUEZ. Tesorero: JOSE OTERO NIETO. Relaciones Públicas: ANTO- NIO RUIZ ESPINOSA. Vocal material: JORGE GIL ARJONA. Vocales: MANUEL MORENO MAYA, JOSE CASTELLANO ASENSIO, FRUCTUOSO RODRIGUEZ JUSTO y JESUS PREULA LUNA.\n\nCAMBIO DE DIRECTIVA EN LA ASOCIACION CULTURAL FLAMENCA «LA UNION DEL CANTE» DE MIJAS COSTA\n\nA sido elegida nueva Junta Directiva de la Asociación Cultural Peña Flamenca «LA UNION DEL CANTE» de Mijas Costa (Málaga) que ha quedado configurada de la forma siguiente: Presidente: RAMON MORENO VAZQUEZ. Vicepresidente: SEBASTIAN FUENTES GALVAN. Secretario: ANTONIO LOPEZ ARAUJO. Tesorero: ANTONIO MERINO VILLAR. Contador: FRANCISCO LAVADO SANCHEZ. Relaciones Públicas: ANTONIO IGLESIAS MEJIAS. Vocales: JOSE RUEDA PEREA, JUAN HORMIGO HARO, JOSE JIMENEZ BLANCO y MANUEL GARCIA ROMERO.\n\nII CONCURSO DE CANTE JONDO, PEÑA FLAMENCA «EL MORATO»\n\nRGANIZADO por la Peña Flamenca «El Morato», tendrá lugar en la ciudad almeriense el II Concurso de Cante Jondo, que estará dotado con 6 premios; el primero de ellos de 75.000 pesetas. Podrán tomar parte cuantos cantaores de ambos sexos lo deseen, profesionales o aficionados.\n\nLa gran final se celebrará en el mes de febrero. Para todas cuantas personas estén interesadas en inscribirse, deberán dirigirse a la Peña Flamenca «El Morato», Cuevas de Pozo, s./n. Almería-04008.",
    "title": "Hablan las peñas",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 570,
    "article_char_count_full": 3711,
    "article_char_count_review": 3711,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-09-24-right-discografia-de-artistas-flamenco",
    "article_text_for_review": "Discografia Flamenca\n\nTítulo: CARMEN LINARES. SU CANTE Canta: CARMEN LINARES Tocan: PEPE y JUAN HABICHUELA. Referencia: (30) 130211 HISPAVOX\n\nEn la redonda encrucijada, seis doncellas bailan.\n\nN ECESARIAMENTE el poeta, definidor en el tiempo, se nos aparece al iniciar la escucha de esta entrega discográfica que nos hace el sello Hispavox, y que tiene como protagonista a Carmen Linares en la voz y a los hermanos Habichuela, Juan y Pepe, en las guitarras. Es el poeta:\n\nTres de carne y tres de plata.\n\nLos sueños de ayer las buscan, pero las tiene abrazadas un Polifemo de Oro, ¡La guitarra!\n\nSon las guitarras de una dinastía. Hermosas guitarras para la voz, compás preciso y precioso, toque —toques de música flamenca.\n\nEl disco, ya en principio, nos inclina a una apreciación: la flamencura de Carmen Linares. Esta mujer cantaora, a la que seguimos desde hace unos años, se mete de lleno en los estilos que dice con frescura de voz, pero apoyada en unos esenciales y añejos ecos para hacer que se respiren —respiremos— aires jondísimos y verdaderos. Ajustada a cada ritmo, y desde el estudio realizado con rigor profesional, Carmen nos dice sus cantes por tangos, bulerías por soleá, siguiriyas, fandangos, malagueñas, cantiñas, tarantas —que dedica a su pueblo— y bulerías.\n\nY hay, en cada cante, un destello de recreación artística a valorar muy positivamente.\n\nEl verso, popular siempre, hace a la cantaora, pregonera de sentencias, hacedora de requiebros, mientras su voz pellizca al tiempo en un grito de amor al arte que nos une.\n\nSí. Carmen Linares es cultura musical andaluza. El poeta, nuevamente, para abrir y cerrar una visión temporal del arte, con la estética de lo jondo.\n\nVestida con mantos negros piensa que el mundo es chiquito y el corazón es inmenso.\n\nVestida con mantos negros piensa que el suspiro tierno, el grito, desaparece en la corriente del viento.\n\nVestida con mantos negros se dejó el balcón abierto y al alba por el balcón desembocó todo el cielo.\n\n¡Ay, yayayayay, que vestida con mantos negros!\n\nTítulo: JUVENTUD Y PUREZA Canta: JUAN CASILLAS Tocan: MANUEL SANTIAGO Y ENRIQUE CAMPOS Referencia: FONODIS, S. A., DIS 46/117\n\nE N ocasiones el paisaje define a un artista. Al menos a nosotros como aficionados así nos lo parece. Y al escuchar el disco «Juventud y Pureza», de Juan Casillas, reafirmamos nuestro pensamiento.\n\nDesde su entorno malagueño, desde su serranía pura y valiente, Juan Hatero Cabello —de flamenco Juan Casillas— traslada el eco de su voz a la anchura discográfica, que es como si se asomara al mar y en barca de desafíos, llegara al puerto de la profesionalidad con vocación e interés.\n\nEl admirado Gonzalo Rojo, desde el fondo de su corazón de amigo, destaca: «Tras el estupendo regalo que Juan Casillas nos hiciera hace tres años con su primer fruto discográfico, ahora, en esta primavera de 1984, el joven cantaor malagueño de Cuevas de San Marcos y residente en Villanueva de Algaidas, nos vuelve a regalar el oído con la aparición de un nuevo larga duración que de forma rotunda viene a confirmarnos su madurez y sus conocimientos de las sedas y los percales de su oficio. Entre los muchos aciertos a destacar en la práctica flamenca de Juan Hatero Cabello, hay que señalar su cabal y admirable dominio de los cantes básicos y de sus más genuinas formas derivadas, que ha venido elaborando en solitario, con una admirable afición a todo aquello que concierne al flamenco. A esto hay que añadir su voz propicia al rajo y su profundo conocimiento del compás, que hacen de él, sin ningún tipo de exageración, un artista que conmueve al más exigente aficionado a este arte». Es el amigo que escribe desde el silencio de la audiencia entrañable y escucha a escucha, no podemos olvidar los numerosos premios que el artista ha conseguido y que avalan un compromiso con la pureza.\n\nEl disco, con variedad en su contenido, se abre por tangos, alegrías, debla y toná y tangos del Piyayo. En la segunda cara podemos oír y sentir serranas, fandangos, bandolá y soleares. Como guitarristas acompanían a Juan Casillas, Manuel Santiago y Enrique Campos.\n\nDesde CANDIL, al significar el disco, queremos ser empujón de entusiasmos, ante las enormes dificultades que todo artista, que no esté inscrito en ciertos y determinados núcleos y circuitos comerciales, tiene que padecer y sufrir.\n\nHay muchos Juan Casillas —lo elegimos a él como adelantado—, que necesitan apoyo decidido para ir enriqueciendo sus conocimientos en busca de un camino que les defina y los sitúe en el campo profesional.\n\nDOSCANDIL",
    "title": "DISCOGRAFIA DE ARTISTAS FLAMENCOS",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 759,
    "article_char_count_full": 4547,
    "article_char_count_review": 4547,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-09-25-right-jos-l-pez-alvarez-bernardo-el-de",
    "article_text_for_review": "Discografia (placas)\n\nPor Manuel Yerga Lancharro\n\nFernando «El Herrero»\n\nJosé Cobo Marchal\n\nCapitán Oviedo, 15 Apartado n.º 76\n\nTeléfono 22 76 36 JAEN",
    "title": "José López Alvarez («Bernardo el de los Lobitos»), Fernando «El Herrero» y",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 23,
    "article_char_count_full": 150,
    "article_char_count_review": 150,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
