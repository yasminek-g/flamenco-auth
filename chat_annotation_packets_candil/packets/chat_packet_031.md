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
    "article_id": "1981-09-5-right-antonio-grau-mora-rojo-el-alparg",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[1]\n\nNacimiento: Libro 15, Folio 261. Defunción: Libro 31, Folio 88.\n\nACTA LITERAL DE SU BAUTISMO\n\n«En la Parroquia Iglesia de San Martín, de la villa de CALLOSA DE SEGURA, de la provincia de Alicante, Obispado de Orihuela, día 8 de diciembre de 1847. Yo, don Antonio Gálvez, Vicario Ecónomo de la misma, BAUTICE solemnemente a un niño, hijo de Manuel Grau y de Manuela Mora, consortes de ésta. Abuelos maternos José y Carmela Canales; paternos Manuel y Manuela Grau. Le puse por nombre ANTONIO. NACIO a las cuatro de la tarde del día de ayer, según relación de los padrinos que lo fueron, Antonio Macías y Antonia Marcos, a quienes advertí el parentesco espiritual y demás de que certifico.»\n\nACTA LITERAL DEL BAUTISMO DE SU ESPOSA\n\n«En la ciudad de Almería y en la Iglesia Parroquial de San\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"Segundo\"]\n\niudad. Abuelos paternos, Nicolás Dauce y Rafaela Ortas. Maternos, José Moreno y Ana Borrás, ésta natural de Institución, los demás, de esta ciudad. Fue madrina María del Mar Ruiz, a quien advertí su obligación y parentesco espiritual. Testigos, Antonio Alvarez y José Díaz, sirvientes de esta Parroquia. Doy fe.» Antonio convivió con sus padres hasta el año de 1868, en que tuvo que incorporarse al Ejército, como soldado, a la Primera Compañía del Segundo Batalión del Regimiento de Infantería España, número 48, de guarnición en Málaga y a las órdenes de don Juan del Rosal. En el año de 1870 le fue concedida licencia provisional y con ella en el bolsillo volvió al hogar de sus padres. Pero no tardó mucho tiempo en darse cuenta de que el trabajo de obrero agrícola no se había hecho para él. En el año de 1872 preparó las maletas y se marchó a la ciudad de Cartagena, donde permaneció año y medio, poco más o menos, debiéndose dedicar a alguna actividad comercial, porque se justifica de forma fehaciente que de Cartagena pasó a Almería, donde estuvo ejerciendo su profesión de comercio. En esta capital tuvo la suerte de conocer a dos personas que en breve tiempo llegarían a ser los pilares fundamentales sobre los que construiría su nueva vida de hombre libre e independiente. Tales personas fueron: 1) BARTOLOME GONZALEZ TORRES, natural de Almería, de profesión INDUSTRIAL ALPARGATE-RO, con quien Antonio debió trabajar en la confección de alpargatas. (¿De ahí su apodo?). Así debió ser. No encuentro otra explicación. Y debió ser así porque Antonio figuró allí dedicado al comercio. ¿No es posible que incluso llegase a ser socio de Bartolomé? Está dentro de lo posible. Como también lo está que entr\n\n[ENDING CONTEXT]\n\naficionado para decir: «Oiga usted, amigo, eso no es una granaína, es una media». Ni que decir tiene que aquella tarde pasé unos minutos desagradables, pero imaginense cómo los pasaría el cantaor.\n\nEn fin, amigos lectores, que los que tengan autoridad en nuestro mundo del arte flamenco, levanten su voz, y pidan con severidad a las casas grabadoras que, antes de lanzar al mercado unos cantes, procuren que su nominación sea correcta. Que no contribuyan a enredar nuestra madeja más de lo que ya está.\n\nConstrucciones CRUZ GARCIA OBRAS EN GENERAL POLIGONO «LOS OLIVARES» CALLE ALCAUDETE, 10 JAEN\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antonio Grau Mora, «Rojo El Alpargatero»",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 2021,
    "article_char_count_full": 11974,
    "article_char_count_review": 3330,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "Segundo"
      }
    ]
  },
  {
    "article_id": "1981-09-7-left-en-torno-al-ix-congreso-de-activ",
    "article_text_for_review": "A primera reflexión en torno al Congreso reciente celebrado, debe referirse, dicho sea en lenguaje de estricta justicia, a la meritoria y rigurosa labor de los organizadores que acertaron en primar aspectos sustantivos del flamenco sobre otros aspectos accidentales, aunque ello haya supuesto limitación en los esparcimientos.\n\nEl IX Congreso de Actividades Flamencas ha mostrado más exigencia, más rigor en la selección de los trabajos allí expuestos. Algunas de las ponencias seleccionadas como la titulada por Manuel Cano, «Aportación de la Guitarra al Flamenco» y la realizada por el equipo Alfredo, «El Folklore Andaluz y el Flamenco en el A. L. E. A.», por solo citar dos de las más representativas, son un claro exponente del rigor que debe imperar en un Congreso. En general, ha existido más altura que en ediciones anteriores, por lo que respecta a Ponencias y Comunicaciones. Tal vez el tiempo concedido a cada una de ellas y el posterior debate no se ha instrumentado adecuadamente. El Congreso sólo trabaja en Pleno, lo que dificulta que las ponencias puedan enriquecerse con variadas aportaciones, fruto de un detenido debate, que la premura de tiempo que impone el Pleno no propicia. Acaso, sería interesante introducir la modalidad del trabajo en comisiones o ponencias, antes de las exposiciones del Pleno. Aspectos negativos también han existido en este Congreso. Tal vez, los mismos que en ediciones anteriores. Dos sectores muy concretos de congresistas se empeñan en polemizar, bizantinamente, sobre la primacía de payos o gitanos en el cante flamenco. Y ocurre que el Congreso resulta arrastrado hacia una dinámica de debate superflua, inoperante y hasta frívola. Este absurdo maniqueismo bipolariza la atención de unos pocos, atraídos por una ya malsana busca de argumentos en apoyo de su tesis, con menosprecio del tiempo debido a cuestiones, sin duda, más importantes. Creemos llegado el tiempo de que se moderen con energía estas estériles contiendas. Existen otros aspectos negativos que nosotros consideramos estructurales y que hacen referencia a la línea exclusivamente sugerencial con que se plantea el Congreso, desdeñándose, por ahora, toda proyección imperativa o ejecutiva. Pero esta es una objección que no es atribuible a los organizadores, que en el IX Congreso han cumplido con toda dignidad.\n\nRamón Porras",
    "title": "En torno al IX Congreso de Actividades Flamencas",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 367,
    "article_char_count_full": 2344,
    "article_char_count_review": 2344,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-09-7-right-viejas-paginas-flamencas-tres-te",
    "article_text_for_review": "Y A, casi al filo del centenario en que se conmemora el nacimiento del poeta de Moguer, a fuer de sinceros, hemos de reconocer nuestra cicatería, el regateo de una lectura sosegada de su obra, la mejor forma de homenajear a un escritor. Y esto, lamentablemente, ocurre en tiempos coincidentes con la efervescencia autonómica andaluza, olvidando que, entre otras cosas, Juan Ramón fue un adelantado.\n\nC L A S E\n\n¡Sevillanas en claustro mudéjar! ¡Qué piano Pleyel... de Barcelona! ¡Debussy! En tres semanas, solfeo (¡gracia inútil de la cansada mano!), clave de fa, Armonía, y luego... ¡Sevillanas! (¡Monjas en Sevillana! ¡O cercana Sevilla! ¿Holbein os presintió en sus letras de la muerte? ¡Sensualidad cargada, lijera pantorrilla, con zapatón serrano, y media azul y fuerte!\n\nSin lugar a dudas, Juan Ramón fue lo fino 'andaluz y su obra el reencuentro de una Andalucía total desvelada, universal y trascendida, nacida «de observación aguda y sentimiento justo, sin alardes andalucistas tampoco, sin exaltación\n\ninnecesaria de Andalucía». Pocos autores como él han calado tan hondo en lo que viene aceptándose como el alma del Sur, sin caer, todo lo contrario, en el epidémico y garrulo tópico panderetero. Andalucía como necesidad deseada y deseante. Andalucía como compromiso de política poética, como búsqueda y realidad posible e imposible de una idea universal creada y recobrada en el íntimo compromiso de su escritura. Conciencia y responsabilidad de ser andaluz, en incansable indagación melancólica de una Andalucía espiritual expresada en inapresables cadencias musicales: a veces, elegiacas; otras impresionistas. Amor a la tierra de nacencia y de asunción, en la que permaneciera ininterrumpidamente hasta los treinta años, que le inundará de presencias, tristes y esbeltas, su ausencia madrileña o americana. La idea de regreso al Sur, su reencuentro con todo lo perdido, sería para él la culminación de su existencia; por ello en el destierro americano buscaría lugares que le recordasen Andalucía, sus gentes, arquitectura, costumbres y, sobre todo, su habla.\n\nY en el poeta («me puse por nombre el andaluz universal a ver si podía llenar de contenido mi continente») no pudo faltar nuestra copla, la que escribe (soleares, siguiriyas, sevillanas) y la que, sobre todo, inunda y es sustento y alma de su propia poesía.\n\nDíficil nos resulta resumir en unas páginas el sentimiento flamenco de Juan Ramón, a ello le hemos dedicado un apretadísimo trabajo de más de setenta páginas y merecería un ensayo de mayor espacio (1), más si alguna copla sobresale en su aprecio esta es, sin lugar a dudas, las sevillanas. Queden ellas en tres textos distintos (un poema sobre las mismas, una prosa en la quedan bellísimamente descritas de modo impresionista y algunas sevillanas del propio J.R.) como sincerísimo homenaje de «Candil» a uno de los poetas andaluces verdaderamente hondos. U.—\n\nLa tarde unje, divina, el claustro. El sol rosado endulza el mar, el río, las viñas, los pinares. En el aire sereno, grato de sol salado, yerra un olor suave y triste de azahares).\n\n¡Sevillanas!... Se estingue entre las azucenas... Y vuelan, libro al brazo, en loca algarabía, un grupo alegre de señoritas morenas, que esconden, sin saberlo, tesoros de armonía.\n\nSEVILLANAS\n\nLas sevillanas, este baile único, son como un vuelo. Se adelanta la pareja y se abre de alas y ensaya un poquito de aquí y allá. Luego, el aleteo se fija, se enreda, se complica, hasta que le entra el goce de sí mismo, y entonces, copla a copla, se yergue, se ladea roza el suelo con el ala, se tiende, se embriaga, enloquece su oleaje... ¡Ya está loca la pareja! El cuerpo humano femenino es, por la sevillana, eterno manantial de gracia diferente, resorte maravilloso del alma rítmica, flor depurada de siglos de baile volador.\n\n¡Sevillanas! Fuera, La Giralda sueña vagos tá. nos malvas, vibrando en la luz completa de la tarde. No hoy un rincón por leve que sea —comisura de labio, cáliz de flor— que no encante y transparente la luz. Esta luz tan alta que es toda, y en todo, música. Por un recodo de carmín y verde se va un rumor de campanillas de coche. Sobre la plaza de toros arde en oro la alegría. Y en la azotea, entre macetas azules, ∂las sevillanas!\n\nCae la tarde. Las casas se ponen rojas. Todavía la pareja se viene al centro, abre las alas y vuelca en la última luz divina de la hora exacta.\n\nLA PRIMAVERA VIENE (2)\n\nEn mis álamos blancos ponen las nubes lijeras copas rosas por los azules.\n\nPor los azules la primavera viene pintando luces.\n\nME DICE «DEJAME»\n\nLa mogueriza cárdena que nadie quiere, si me paro a mirarla, me dice «Déjame».\n\nMe dice «Déjame perderme por el monte, mi monte verde».\n\nMAR MIO\n\nLas olas y las alas del mar bravío saben corresponderse con igual ritmo.\n\nCon igual ritmo, mi ala con tu ola suban, mar mío, bajen, mar mío.\n\nTARTESIA ALTIVA\n\nComo soy de Moguer y de Sevilla, canto mis ilusiones por seguidillas.\n\nPor seguidillas canto mis ilusiones, Tartesia altiva.\n\nASTORGA Refrigeración\n\nInstalaciones Frigoríficas AIRE ACONDICIONADO Instalaciones reparaciones y mantenimiento\n\nMancha Real, 7 (Polígono Los Olivares) Teléfono 21 18 87\n\nJ A E N",
    "title": "VIEJAS PAGINAS FLAMENCAS TRES TEXTOS DE JUAN RAMON JIMENEZ",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "7-9",
    "page_number": 7,
    "word_count": 855,
    "article_char_count_full": 5156,
    "article_char_count_review": 5156,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-09-10-right-breve-perfil-biogr-fico",
    "article_text_for_review": "Breve perfil biográfico\n\nGitano y de Jerez por los cuatro costados, Fernando Fernández Monje, Terremoto, nació el 17 de marzo de 1934 en el número 30 de la calle Sor Eulalia del flamenquísimo barrio y parroquia de Santiago, donde fuera bautizado.\n\nDesde muy joven dejó constancia de su esencia jonda. Iniciado en plena adolescencia como bailaor - aún se recuerdan sus casi infantiles actuaciones en el tabanco de Canalejas, la Pandereta, etc. -, pronto actuaría en los principales cuadros flamencos, que dejaría al filo de los años sesenta para dedicarse por entero al cante; no obstante, el baile sería en él algo más que un recuerdo, de aquí que sus actuaciones por bulerías las rematase al compás del baile.\n\nMonstruo del cante desde sus inicios, atestiguó con su flamenquísima voz, purísima, sin artificios, la gran cantidad de sentimientos que anidan y se transmiten por un auténtico corazón cantaor, de lo que ha dejado escasa pero elocuente prueba en su discografía: 1963.—«Terremoto», Philips 433862 PE\n\n1969.—«El genio del Terremoto», Fontana 701953 WPY LP.\n\n1969 — «Canta Jerez», Hispavox HH 16.638 EP.\n\n1970-—«Terremoto de Jerez», Hispavox HH 16-735 EP.\n\n1969.—«Genio y duende del cante gitano», Hispavox HH (S) 10-361 LP\n\n1978—•Sonidos negros», Ariola 25.644 H LP acompañado siempre a la guitarra por su cuñado -Terremoto casó con Isabel Pantoja Carpio-, Manuel Morao: siguiriyas, bulerías, soleares, martinetes, taranto, malagueña, fandangos, etc.\n\nTestimonio vivo del cante, figura artística y humana repleta de personalidad, Terremoto fue verdadero profeta en su tierra, como lo demuestran los premios Nacional de Flamenco (1965), Copa Jerez y Caballero de la Orden Jonda (1968) y Premio El Gloria (1972), junto a otros galardones nacionales.\n\nAquejado por una enfermedad hepática, desde hace años, falleció en Jerez el domingo, 6 de septiembre de 1981, en su casa de la calle los Dolores de la barriada gitana de la Asunción. El viernes\n\nanterior actuó en Jerez, y el sábado en Ronda, desde donde regresó ya herido de muerte. Su entierro constituyó un auténtico testimonio de pesar colectivo y al que acudieron más de tres mil personas.",
    "title": "Breve perfil biográfico",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 343,
    "article_char_count_full": 2152,
    "article_char_count_review": 2152,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-09-11-left-fernando-terremoto-en-siguriyas",
    "article_text_for_review": "DOLOR del arte ya no música, víscera ya, cruenta mostración, toro vivo abierto en canal, agua clara vendada, muerta a voz en cuello dónde?: ¿en Cádiz mil ochocientos treinta, Jerez de la Frontera mil novecientos doce, Triana, Magdalena de Jaén -¿cuándo?-, Potro de Córdoba, Granada, Málaga, Chanca en llaga de Almería, Huelva, Sanlúcar, dónde?\n\nIn memoriam\n\nLos ojos perdidos para fuera, lúcidos para dentro, constelados de vino y apariencia febril, las manos parteras, los labios buscadores, la garganta henchida, despeñándose dolor abajo, tierna y espantosa al sumidero de los años caidos, de los rostros borrados (¿de quién y dónde, cuándo?), gestan, elaboran el lloro verdadero turbio de tiempos y pestañas y palabras confusas, lo alevantan a rempujones tan casuales como seguros, deshilachan el grito ciego y suficiente, tiran encima de la mesa las señales de nacido a un sino vivísimo, andaluz, inerme, inmortal.\n\nFERNANDO QUÍÑONES",
    "title": "Fernando Terremoto en «siguiríyas»",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 145,
    "article_char_count_full": 937,
    "article_char_count_review": 937,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
