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
    "article_id": "1985-11-19-right-a-don-jos-m-rquez-cabello-endere",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\neo en la revista de ju- lio/agosto últimos, la co- municación que gentil-\n\nPor M. Yerga Lancharro mente me dedica esa excelente persona, buen letrista y mejor aficionado que es, José Márquez Cabello, a quien le agradezco su exposición, porque con ella no ha hecho otra cosa que ofrecerme «material» suficiente para poder hilvanar este trabajo que, por supuesto, no va a ser una obra literaria, porque sé que nuestro humilde campo la rechazaría. A él y a los lectores de CANDIL se lo ofrezco, en la seguridad de que les agradará conocer el porqué del título que lo encabeza.\n\nDespués de largos años de investigación en la vida artística y privada de don Antonio Chacón, puedo decir, sin equivocar a nadie, que creó siete estilos por malagueñas. Veamos:\n\n1)... Qué tienes por mi persona a qué niegas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"cuerpo\"]\n\nnuestro humilde campo la rechazaría. A él y a los lectores de CANDIL se lo ofrezco, en la seguridad de que les agradará conocer el porqué del título que lo encabeza. Después de largos años de investigación en la vida artística y privada de don Antonio Chacón, puedo decir, sin equivocar a nadie, que creó siete estilos por malagueñas. Veamos: 1)... Qué tienes por mi persona a qué niegas el delirio, qué tienes por mi persona; le das martirio a tu cuerpo tú te estás matando sola, y yo pasando tormentos. (sic) 2)... A dar gritos me ponía en la tumba de mi PARE (1) a dar gritos me ponía; escuché una voz DER VIENTO (1) no la llames, me decía que no responden los muertos. (sic) 3)... Allí fueron mis quebrantos en un hospital la vi, allí fueron mis quebrantos; quién me iba a decir a mí que mujer que quise tanto, iba a tener tan mal fin. (sic) 4)... Del convento las campanas si preguntas por quién doblan, del convento las campanas; dile que doblando están por mis muertas esperanzas. (sic) 5)... De aquella campana triste dando en el reloj la una, de aquella campana triste; hasta las dos estoy pensando el querer que me fingiste y me dan las tres llorando. (sic) 6)... Que te quise con locura en mi vía negaré, que te quise con locura; mira qué cariño fue que en contra del mundo entero vuelvo·a quererte otra vez. (sic) 7)... À qué tanto me consientes si tú no me has de querer, a qué tanto me consientes; mátame ya de una vez que yo prefiero mi muerte a sufrir y padecer. (sic) Una vez reseñadas las siete malagueñas, arguyo sin paliativos que está dentro de lo posible que a Márquez Cabello le hayan informado mal; o bien que, en esos momentos en que me escribió se encontraba desmemoriado. Por si así fuese, yo, con todos mis respetos hacia él, voy a intentar aclararle algunos puntos equivocados de los que conforman su escrito. Como dejo expuesto, tras costosas y pesadas diligencias en Madrid, he conseguido acarrear para mi archivo, con l\n\n[ENDING CONTEXT]\n\ncomo «material» válido, para la elaboración de conferencias? Me consta, asimismo, que en ellas se omite, intencionadamente, la fuente en que se ha bebido. Me da igual. Ya estoy acostumbrado. Yo, a ciento dieciocho kilómetros de distancia de la tierra de mis antepasados: AN-DALUCIA, me congratulo «por lo bajini» de que se utilice, porque así, al menos, me cabe la satisfacción de poder decir: por algo será.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A don José Marques Cabello, enderezándole algunos entuer- tos",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 1701,
    "article_char_count_full": 9859,
    "article_char_count_review": 3567,
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
  },
  {
    "article_id": "1985-11-22-left-noticiario-flamenco",
    "article_text_for_review": "HOMENAJE AL POETA DE ALCALA\n\nE 1 pasado viernes día 20 de diciembre, tuvo lugar en Alcalá de Guadalra un caluroso homenaje a Manuel García Fernández, «El Poeta de Alcalá», un moronés de nacimiento pero afincado en el pueblo cantaor de Joaquín el de la Paula, gitano sembrao de gracia, que ha preñado de gitanería flamenca con su pañuelo aleteante y su verso acompasado a media humanidad.\n\nOrganizado por el Excmo. Ayuntamiento de Alcalá de Guadaira y con la presentación a cargo de Emilio Jiménez Díaz, Manuel Martín Martín y Manuel Bohórquez, estuvieron en el cante Fernanda y Bernarda de Utrera, Rancapino, Nano de Jerez, Aurora Vargas, Naranjito de Triana, Beni de Cádiz, el Cabrero, Curro Malena y Jo\n\nNOTA\n\nEn nuestro número anterior publicamos el poema titulado VERSOS DE OTOÑO PARA MACANDE, que por un error de imprenta apareció sin firma. Hemos de decir que dicho poema es obra de nuestro colaborador y amigo JESUS CUESTARA-NA.\n\nPedimos disculpas a nuestros lectores y al autor.\n\nS ensibilizada con todo lo referente al flamenco, Radiocadena Andalucía ha puesto en marcha un nuevo programa de información y difusión de las actividades flamencas en la Comunidad Andaluzay.\n\nLa intención del mismo es llenar el vacío informativo que existe en toda la geografía andaluza sobre las diferentes y múltiples actividades que se realizan diariamente vinculadas al arte jondo, desde la actividad de los artistas hasta el sentir de los aficionados, pasando por los actos y tareas que realizan las peñas flamencas y las diferentes instituciones públicas y privadas.\n\nEl nuevo programa, que recibe el nombre de «Puente Flamenco», se emite diariamente de lunes a viernes de 9,30 a 10 de la noche por las emisoras de O.M. y se realiza desde el centro regional de Sevilla. En él participan las once emisoras que componen la red andaluza. La labor de todo el equipo será coordinada por Carlos Arbelos.\n\n«Puente Flamenco» nace así, para unir los diferentes puntos de la geografía andaluza, donde vive y se desarrolla el cante, el toque y el baile flamencos; para unir pasado, presente y la proyección de futuro del arte jondo y para confrontar las diferentes opiniones que sobre él existen, en una concepción abierta a todos los que en él quieran participar.\n\nSEGUNDA DISTINCIÓN «COMPAS DEL CANTE»\n\nE l pasado día 13 de diciembre, tuvo lugar en Sevilla un hecho cultural que la redacción de CANDIL ya anunciaba en su número anterior; nos referimos al fallo de la segunda edición de la distinción «Compás del Cante», creado por «La Cruz del Campo, S. A.» y que en esta ocasión ha sido otorgada al cantaor Antonio Fernández Díaz, «Fosforito», quedando como finalistas los cantaores José de la Tomasa y Chano Lobato.\n\nEl preciado galardón, una preciosa alegoría en bronce del escultor Sevilla-no Jesús Gavira Alba, fue otorgada al maestro de Puente Genil por un prestigioso y serio jurado, presidido por don Francisco Vallecillo Pecino, y en la decisión del mismo pesaron, aplicando las bases de la presente convocatoria, criterios como la pureza, profesionalidad y constancia del cantaor, así como las labores investigadoras y de resurrección de estilos en desuso, puesta al día de matices perdidos, así como la labor desarrollada en recitales, conferencias, ciclos culturales, etc.\n\nLa redacción desea felicitar al gana-dor, nuestro entrañable «Fosforito», así como a la empresa «La Cruz del Campo, S. A.», por tan extraordinaria labor en pro de nuestro arte, sobre todo, una vez que su presidente, don Enrique Osborne, haciéndose eco de la unánime petición del jurado, prometió plantear al Consejo de Administración de dicha entidad, la posibilidad de abarcar en la próxima convocatoria a los aspectos del toque y del baile flamencos. Con ayudas tan estimables nuestro arte ganará, y Andalucía se pondrá en marcha hacia la definitiva dignificación y recuperación de sus raíces culturales. CANDIL\n\nJ. A. PULPON\n\nESPECTACULOS INTERNACIONALES\n\nO'Donnell, núm. 3-4.º Teléfs. 22 20 58 - 21 69 20\n\nPARTICULAR: Teléfono 27 80 78\n\nSEVILLA",
    "title": "Noticiario Flamenco",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 656,
    "article_char_count_full": 4020,
    "article_char_count_review": 4020,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-11-23-left-hablan-las-pe-as",
    "article_text_for_review": "II EDICION PREMIO DE ENSAYO GONZALEZ CLIMENT-1986\n\nPatrocinado por «Luis de Córdoba» y organizado por la Peña Flamenca de Córdoba y Virgilio Márquez, editor, ha sido convocado el II Premio de Ensayo González Climent sobre el Arte Flamenco en cualquiera de sus facetas (cante, toque y baile) y enfoques (histórico, geográfico, musical, literario, lingüístico, social, etc.).\n\nLos trabajos deberán ser originales e inéditos.\n\nLa obra premiada será publicada por Virgilio Márquez, editor.\n\nHabrá un solo premio de 100.000 pesetas.\n\nLos envíos de trabajo se harán a la citada peña, calle Romero Barros, 4, Córdoba - 14003, antes del 10 de marzo de 1986.\n\nNUEVA JUNTA DIRECTIVA EN LA PEÑA FLAMENCA «LA SOLEA» DE Torreperogil\n\nEn Asamblea General de socios de la Peña Flamenca «La Soleá» de Torreperogil (Jaén), resultó elegida nueva Junta Directiva, la cual quedó compuesta de la siguiente manera: Presidente: Juan José Martínez Chaves. Vicepresidente: José Sánchez Valera. Secretario: Luis Hurtado Torralba. Tesorero: Manuel Sánchez Gallego. Vocales: Luis Martínez Martínez, Isidoro Jumillas Aranda, Juan Fernández Gallego, Antonio Ruiz Torres y Miguel Lara Lara.\n\nNuestra cordial enhorabuena a la nueva junta. En Asamblea General celebrada por la Peña Flamenca «Sanroqueña» de San Roque (Cádiz), resultó elegida nueva Junta Directiva, quedando compuesta de la forma siguiente: Presidente: Francisco Orellana Pérez. Vicepresidente: Manuel Ruiz Caballero. Secretario: Antonio Muñoz Barba. Tesorero: Juan Martín Mateos. Vicesecretario: Antonio Calvo Rodríguez. Vocales: Joaquín García Baeza, José Rojas Gutiérrez, José Yetor Beltrán, Rafael Toledo Pedresa, Francisco Escañuela Alvarez y Francisco Casas Galdeano.\n\nNUEVA DIRECTIVA EN LA PEÑA FLAMENÇA «SANROQUEÑA»\n\nNuestra felicitación a la nueva junta.\n\nNUEVA JUNTA DIRECTIVA DE LA TERTULIA FLAMENCA DE ALCALA LA REAL (Jaén)\n\nEn Asamblea General Extraordina- ria celebrada el día 25 de noviembre por la Tertulia Flamenca de Alcalá la Real (Jaén), resultó elegida nueva Junta Directiva, quedando compuesta de la siguiente manera:\n\nJunta Rectora: Francisco Vera Castillo, Valeriano Montañés Escobar, Manuel Martínez Sánchez, Daniel Muñoz Ríos, Ramón Pérez Cano. Secretario: Rafael Arjona Serrano. Tesorero: Rafael Jiménez Marañón. Bibliotecario: Ramón Piñas Piñas. Vocales: Octavio\n\nXIV CERTAMEN NACIONAL DE GUITARRA FLAMENÇA EN JEREZ\n\nOrganizado por la Peña Flamenca «Los Cernícalos» y dedicado a la memoria de MANOLO DE BADAJOZ, ha tenido lugar en el Teatro Villamarta de Jerez (Cádiz), el XIV CERTAMEN NACIONAL DE GUITARRA FLAMENCA, con el patrocinio de la Excma. Diputación de Cádiz y del Excmo. Ayuntamiento de Jerez y la colaboración especial de la firma GONZA-LEZ BYASS, S. A.\n\nEl Certamen ha constituido un rotundo éxito de participación, dándose la circunstancia de haberse inscrito concursantes de distintas provincias españolas así como de Bélgica, Japón y Francia.\n\nEl resultado de la votación del jurado es el siguiente: Ganador del 1 $ ^{er} $ Premio dotado con 250.000 pesetas y trofeo del Ayuntamiento de Jerez, RA-FAEL FERNANDEZ ANDUJAR de Madrid.\n\n2.º Premio dotado con 100.000 pesetas y trofeo de la Diputación de Cádiz, para MANUEL MORENO JUNQUE-RA de Jerez.\n\nEspinosa García y Rafael Molina Cortés.\n\nIgualmente, se concedieron cinco ac- césit de 15.000 pesetas cada uno.\n\n3 $ ^{er} $ Premio dotado con 25.000 pesetas y una guitarra construida por Valeriano Bernal y trofeo de la Peña «Los Cernícalos», para JOSE MORENO JUSTICIA de Jaén.\n\nDeseamos toda clase de aciertos a los amigos de Alcalá.",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 534,
    "article_char_count_full": 3556,
    "article_char_count_review": 3556,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-11-23-right-palmas-a-comp-s-para-pasarela-po",
    "article_text_for_review": "N o todo en este extraño, pero fascinante, mundo flamenco va a ser ingratitudes, intereses creados, atentados a la propiedad intelectual, «creadores» oportunistas o mangantes de vía estrecha. Amen de estos aspectos negativos que reseñamos, también existen profesionales con una afición desmedida por engrandecer todo lo que huele a flamenco, aficionados de a pie que se estremecen sobrecogidos por esta manifestación musical y, cómo no, hombres que trabajan con mentalidad empresarial aderezada del romanticismo que conlleva el ser andaluz.\n\nVisto así el panorama de lo jondo, siempre es gratificante difundir y reconocer la labor encomiable de los que apoyan con su esfuerzo, trabajo y dedicación a esta seña de identidad genuinamente andaluz.\n\nJesús del Gran Poder, 7 - planta 2.ª, letra J - Teléfono 375898 - 41002 SEVILLA\n\nPor Manuel Martín Martín\n\nY este, queridos amigos, es el caso de Pasarela, una firma sevillana, comercial y discográfica, que viene brindando en corto espacio de tiempo un apoyo fundamental, decisivo y sonoro a los profesionales de este arte, consagrados y no consagrados.\n\nAsí lo ha entendido, el pasado mes de diciembre, el Ministerio de Cultura, galardonando a Pasarela con el «Premio Nacional a la Empresa Discográfica, dentro de la creatividad y aportación cultural, por la obra más destacada de música popular española. Desde Andalucía con el Flamenco de José Galán».\n\nDesde estas flamenquísimas páginas de CANDIL, nuestra más cordial enhorabuena, el brindis más sonoro y sincero por la labor realizada, y nuestras palmas a compás para Pasarela, ejemplo a seguir tras las palabras de su director, Luis M.ª de la Cueva: «Mientras Diego y yo estemos aquí, jamás entrarán músicas extrañas en nuestras grabaciones flamencas». Así lo deseamos y por ello os admiramos.\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N",
    "title": "Palmas a compás para Pasarela Por Manu",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 294,
    "article_char_count_full": 1860,
    "article_char_count_review": 1860,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-11-24-left-placas",
    "article_text_for_review": "Por: MANUEL\n\nYERGA LANCHARRO\n\nANTONIO GRAU\n\n«ROJO EL ALPARGATERO» (hijo)",
    "title": "PLACAS",
    "periodical": "candil",
    "issue_id": "1985-11",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 10,
    "article_char_count_full": 72,
    "article_char_count_review": 72,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
