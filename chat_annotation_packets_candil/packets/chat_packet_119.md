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
    "article_id": "1985-09-21-right-buzon-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor M. Yerga Lancharro\n\nEstimados lectores de CANDIL. Les pido un favor: localicen la revista número 38/1985, y a la vista de la misma descifren conmigo el contenido del escrito de don Rafael Rodríguez Villegas, que aparece a continuación del que le dedico al aficinado Salvador Castro Morón, andaluz residente en Badalona.\n\nSi tienen la revista a mano, podrán leer: «A Manuel Yerga Lancharro», y a continuación treinta y seis líneas que literalmente dicen:\n\n«Por supuesto que no voy a poner en tela de juicio todo cuanto dice en su libro «Apuntes y datos para las bibliografías de “Rojo el Alpargatero”, La Trini, Chacón y el Torre, a los que describe como cuatro puntales del flamenco, y a cada uno de ellos usted los sitúa con certificados a cada uno en su sitio, EXCEPTO A CHACON, QUE NO ESTA\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\niteralmente dicen: «Por supuesto que no voy a poner en tela de juicio todo cuanto dice en su libro «Apuntes y datos para las bibliografías de “Rojo el Alpargatero”, La Trini, Chacón y el Torre, a los que describe como cuatro puntales del flamenco, y a cada uno de ellos usted los sitúa con certificados a cada uno en su sitio, EXCEPTO A CHACON, QUE NO ESTA MUY SEGURO ENTRE OTRAS COSAS. Pero lo que a mí me llama poderosamente la atención es con la gran propiedad e interés que describe la vida de Antonio Grau Mora, “Rojo el Alpargatero”, en todos sus términos familiares. Ya me gusta todo esto como simple aficionado, pero no lo suficientemente simple para no darme cuenta del interés que se pone en aclarar... los problemas de los cantes de Levante o más concretamente de Cartagena y La Unión. Me quiero recordar, de varias cosas que bajo mi punto de vista de aficionado contradicen, de alguna forma, a otras que se dicen ahora. En 1972, el maestro don Antonio Mairena, q.e.d., dijo en plena Unión y que figura en la revista CANDIL: «¡Qué grandes son los cantes de esta tierra!». En 1968, en Jerez de la Frontera, se le impuso el nombramiento de Caballero Cabal de la Orden Jonda, por la Cátedra de Flamencología al maestro Piñana Segado (Piñana padre), donde fueron miembros y testigos de dicha confirmación el Tío Borrico de Jerez, Juan de la Plata, Terremoto, Antonio Murciano y Antonio Fernánde\n\n[ENDING CONTEXT]\n\nincuestionable, no debe molestarse nadie. Lo que tenemos que hacer todos los buenos aficionados es asentir y felicitarnos por sinigual nacimiento.\n\nLa suerte de los levantinos es que se descubriesen las minas en sus tierras, porque de haberse descubierto en las de Extremadura, hoy esos cantes no se llamarían de Levantes sino de la tierra de los conquistadores de América: Extremadura. ¿O no?\n\nNOTA PARA UNOS AFICIONADOS: «Rojo el Alpargatero» no grabó. Sí su hijo Antonio, que aprovechó el nombre artístico de su progenitor para poder vender unos discos cuyos cantes dejan bastante que desear.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Buzón Flamenco",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1328,
    "article_char_count_full": 7721,
    "article_char_count_review": 3021,
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
    "article_id": "1985-09-22-right-hablan-las-penas-ii-concurso-de-",
    "article_text_for_review": "HABLAN LAS PENAS\n\nII CONCURSO DE CANTE RINCON FLAMENCO\n\nOrganizado por la Peña Rincón Flamenco de Córdoba, con el patrocinio de la Consejería de Cultura de la Junta de Andalucía, Diputación Provincial y Ayuntamiento de Córdoba, Ayuntamiento de Linares (Jaén), Ayuntamiento de La Unión (Murcia) y varias firmas comerciales, ha sido convocado el II Concurso de Cante «Rincón Flamenco». Para esta edición se han establecido los siguientes premios: primero, 85.000 pesetas; segundo, 60.000 pesetas, y tercero, 40.000 pesetas. Además de estos premios, se conceden dos especiales a la mejor Taranta, de 30.000 y 10.000 pesetas. Otros dos de igual cuantía a la mejor Minera, y otros dos de 50.000 y 35.000 pesetas, a los mejores intérpretes de los cantes de Córdoba.\n\nIII CONCURSO DE CANTES DE MALAGA\n\nOrganizado por la Peña «El Sombrero» y patrocinado por distintas firmas comerciales, se convoca el III Concurso de Cantes de Málaga, dotado con seis premios, el primero de ellos de 125.000 pesetas y Sombrero de Oro. Las fases previas se celebrarán hasta el día 15 de noviembre y la final el día 22 del mismo mes.\n\nPara más información, los interesados deberán dirigirse a la citada Peña, calle Antonio Jiménez Ruiz, 29, Málaga.\n\nNUEVA DIRECTIVA EN LA PEÑA «RINCON DEL CANTE» DE CORDOBA\n\nEn asamblea general celebrada por la Peña Flamenca cordobesa «Rincón del Cante», ha sido elegida nueva junta directiva, la cual ha quedado compuesta de la forma siguiente: presidente, Miguel López Fernández; vicepresidente, José M.³ Soler Solís; adjunto al presidente, José Urbaneja Diéguez; secretario, José L. Otero Nieto; vicesecretario, Bernardo Mesa García; tesorero, Manuel Nogueras Barrientos; relaciones públicas, José García Rodríguez; vocales, José Castellanos Asensio, José Salinas Martín y Rafael Delgado Centella.",
    "title": "HABLAN LAS PENAS II CONCURSO DE CANTE RINCON FLAMENCO",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 281,
    "article_char_count_full": 1808,
    "article_char_count_review": 1808,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-09-23-left-discograf-a-flamenca",
    "article_text_for_review": "TITULO: El Yunque CANTA: Ricardo Losada Maya REFERENCIA: Hispavox, S. A. (60) 160310\n\nEl Yunque\n\nNuevamente llega a nosotros la voz de Ricardo Losada, que anteriormente escuchamos en su L.P. titulado «Sentimiento». No veíamos entonces, donde quería llegar el «Yunque», de sobra sabemos que una primera grabación tiene muchos inconvenientes y que a veces hay que buscar lo comercial, en detrimento del buen cante.\n\nCuando vi este disco con su reclamo comercial «premio nacional de cante flamenco», piqué y lo adquirí; vagamente recordaba haber escuchado al cantaor y en efecto así era; fue en el concurso que se celebró en Córdoba, vídeo que conservo, y he de decir que no me explico cómo un jurado puede cometer tamaña barbaridad, otorgando un galardón de esta categoría a per- Este flamenco orquestado o con sus variantes, al que algunos llaman melódico no tiene cabida en lo que yo al menos entiendo por flamenco; no me im-\n\nEl actual es un caso más de los que el cantaor novel o el consagrado, a veces, cambia su trayectoria por motivos que yo no entro en juzgar.\n\nsona o personas no merecedoras de él. Con ello no hacen nada más que equivocar al que se inicia en el cante, tanto para escucharlo como para decirlo.\n\nNo voy a hacer la crítica de «Sentimiento» que ya se hizo en esta revista; pero sí quiero informar que en aquél, al menos, bueno, malo o regular, había algo; en éste no existe nada que se parezca a lo que el aficionado entiende por flamenco.\n\nGerardo Fuentes\n\nporta decir que lo desconozco y de ahí que no pueda informar más sobre este L.P.\n\nSentimos que en esta ocasión la casa Hispavox, que tantas y tantas veces nos ha deleitado con sus grabaciones, no lo haya conseguido una vez más. Esperamos continuará lanzando nuevos valores y al mismo tiempo le rogamos siga grabando sus máster de profesionales antiguos que calaron hondo en el aficionado.\n\nEl consejo que damos a la afición respecto a este disco creo se ve bien claro en la crítica, pero por si alguien lo quiere más claro digo: «El aficionado, de verdad, debe desestimar este tipo de grabaciones que nada, absolutamente nada aportan al flamenco».\n\nPOR TONAS, DEBLAS Y MARTINETES ANTONIO MAIRENA • ENRIQUE MOREnte • RAFAEL ROMERO • RAMON MEDRANO PEPE DE LA MATRONA • JOSE DE LOS REYES \"EL NEGRO\" • PEPE ALGECIRAS EL BORRICO DE JEREZ • GARBANZO DE JEREZ • GABRIEL MOREno • JUAN VAREA MANUEL VALENCIA \"DIAMANTE NEGRO\" • EL CHOCOLATE\n\nPor Tonás, Deblas y Martinetes\n\nS e oye el disco de un tirón y deja un saber de siglos en el aire. Aquí las voces acompanían a las voces. El eco se pierde en la historia y el cronista se siente embriagado de incontaminadas muestras cantaoras. Aquí está la raíz del grito. Aquí, el cantar se magnifica. No hay escenario. No hay público. El cante está solo. Porque siempre la tragedia o la pena es una: Lamento terrible y ciego que junto al sol de la fragua espera sediento el agua donde se apague su fuego. Tremendo y soberbio juego entre la angustia y la pena, ardiente queja morena, donde el yunque es un grillete al que triste se encadena el dolor del martinete.\n\n¿Hacer historia de las tonás mientras las voces de Antonio Mairena, Enrique Morente, Pepe el de la Matrona, José de los Reyes «El Negro», Rafael Romero, Pepe Algeciras, Ramón Medrano, El Borrico de Jerez, Garbanzo de Jerez, Manuel Valencia «Diamante Negro», Gabriel Moreno, Juan Varea y el Chocolate, recorren los claroscuros de la más absoluta entrega? Creo que no es necesario. Sólo, hacer referencia a la pluralidad de estilos que el L.P. contiene. En esta producción de HISPAVOX, todo un arco iris de artistas flamencos ponen luz a la noche de las músicas. Y el viejo cantaor da paso, midiendo al joven profesional, limpio en resonancias o al contrario. Es significativo reseñar cómo las facultades, en un momento determinado, sólo ayudan a la voz, mientras el regusto se queda prendado en el esfuerzo, titánico esfuerzo por no romper el esquema melódico que el grito quiere imponer.\n\nEs disco para escuchar, como más, en reunión cabal. La grabación, recopilación de otras anteriores, es plaza necesaria en cualquier colección para aprender, estudiar y asimilar.\n\nJuan Antonio Ibáñez",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 719,
    "article_char_count_full": 4164,
    "article_char_count_review": 4164,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-09-24-left-placas",
    "article_text_for_review": "Por: MANUEL YERGA LANCHARRO\n\nDe don ANTONIO CHACON",
    "title": "PLACAS",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 8,
    "article_char_count_full": 50,
    "article_char_count_review": 50,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-11-3-right-editorial",
    "article_text_for_review": "Juan Varea\n\nJuan Varea ha muerto. Juan Varea no nació en Triana, ni en la Plaza de Santiago de Jerez, ni en Alcalá, ni en Utrera, ni en los puertos. Ni aún en otro lugar de menos renombre de Andalucía. Pero Juan Varea era un maestro; una voz de estremecedora jondura, un hermoso renglón de la más reciente historia del flamenco. Para quienes, con enorme estulticia, valoran más el pedigrí de un cantar que su capacidad de comunicación jonda, para quienes examinan antes la partida de nacimiento de un artista que su trayectoria profesional, Juan Varea es la excepción, un fenómeno inexplicable, por que con él quiebra, en algún sentido, el principio de la territorialidad del cante. No es andaluz, en la magia del flamenco, quien nace en Andalucía sino quién, por su natural sensibilidad, conecta con los hondones de la cultura jonda, quien, asumiendo espontáneamente el misterio de arcanas y soterradas voces, es capaz de testimoniarlo, y aún más, de transmitirlo y ofrecer del cante cumplida fehaciencia.\n\nJuan Varea, trajo de su luminoso levante un metal de voz limpio y de hermosos registros, un metal de voz, acrisolado en Jerez pero de singulares cadencias, inspiradas en la más venerable tradición flamenca que le permitían entrar por derecho, como el mejor, en los cantes por soleá o dulcificar un increíble eco en los varios estilos de malagueñas que dominaba.\n\nÁ Juan Varea no se le ha estimado por el gran público, en toda la medida que merecía su aportación personalísima al cante. Sólo un pequeño sector de la afición conocía la jondura que albergaba este pequeño-grande maestro levantino, lleno de sabiduría y de entrega, atenazado, últimamente, por los agobios del asma pero transfigurado cuando cantaba y derramando en cada tercio, gusto exquisito, paladar.\n\nJuan Varea, viejo amigo y maestro, descansa en paz.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 302,
    "article_char_count_full": 1826,
    "article_char_count_review": 1826,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
