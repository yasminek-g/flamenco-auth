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
    "article_id": "1989-01-7-right-el-carbonero-un-fandanguero-exce",
    "article_text_for_review": "Opinión:\n\nEl fandango, ese cante tan superficial para los que se pasan de puristas, como único y definitivo para los que de ahí no pasan, imperó —como imprescindible en el repertorio de casi todos los que más o menos comían del cante— desde mediados los años 20 hasta la guerra civil. En cuanto al capítulo irremediable de las modas flamencias creo que al fandango lo sustituyó la zambra caracolera de los 40.\n\nCon el Carbonero de la Macarena, como fandanguero excepcional, hemos tenido lo que se llama una gran suerte, pues grabó mucho más que\n\nEl cante por fandangos llegó a escalar cotas que no han vuelto a repetirse. En ese palo sí que podemos hablar de una verdadera proliferación de auténticos «creadores». Todos los fandangueros tenían su propio fandango y todos los aficionados adoptaban el del cantaor que mejor les iba a sus condiciones. Todos cantábamos el fandango que alguno de aquellos profesionales acababa de poner de moda. Se perdieron muchos, buenos, regulares y malos, pero también, afortunadamente, por el conducto oral y discográfico se han salvado buena parte de los que hoy podemos escuchar tanto grabados como en la voz de alguna que otra figura del momento. Luis Caballero\n\notros. Yo diría que dejó impresionada en aquellas placas de entonces toda su múltiple variedad de fandangos completa.\n\nQuiero recordar cómo al principio de los años 30 comenzaron a sonar con unos estilos «nuevos», más o menos paralelos, él, Pepe Pinto y Manolito Caracol. Como fandanguero, el Carbonero se impuso e impuso su aire de una manera total. No había casino o taberna de cualquier pueblo con máquina cantaora que no tuviera el último disco del Carbonero por fandangos. Entonces, la gente sencilla escuchaba a los cantaores que iban en las «troupes» y algunos discos. Se cantaban y se aprendían los cantes en la taberna y reuniones y era frecuente en el campo oír a lo lejos alguien cantando alguno de los fandangos popularísimos de Manuel Vega el Carbonerillo.\n\nYo debía tener 10 ó 12 años cuando lo escuché en persona.\n\nSin dejar de sonar con esa amarga alegría característica de los macarenos, construyó unos cuantos fandangos realmente personalísimos, aunque no destacó en los demás cantes.\n\nY resulta curioso que andemos recordando en este sentido al Gloria, Cepero, la Calzá, Aznalcóllar, etc., y nadie quiera o pueda o no se atreva a montar los fandangos del Carbonero.",
    "title": "El Carbonero, un fandanguero excepcional",
    "periodical": "candil",
    "issue_id": "1989-01",
    "year": 1989,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 398,
    "article_char_count_full": 2384,
    "article_char_count_review": 2384,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-01-8-right-aurelio-sell-nondedeu",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n1 Aurelio fue equivocadamente invitado al Festival de Jerez de la Frontera. Don Tomás García Figueras le envió una curta (tuve ocasión de leerla) en la que le adjuntaba la proclama y las bases para participar jcomo concursante! (Es intransferible la inefable narración de Aurelio).\n\nLe respondió al alcalde que ni cuando tenía veinticinco o treinta años pensó concursar en parte alguna, y que toda su vida fue un profesional para minorías. Jamás le oí hablar tan desmesuradamente bien de don Antonio Cruz Conde.\n\nAvanzando los años, Aurelio atiza su sinceridad y en muchas apreciaciones se ha puesto sórdidamente inapelable. Su guerra íntima declarada a Talega —jahora acompañante de jurado!— ha tomado estado público. «Juan sólo conoce el cante monótono de su tío Joaquín. El resto de los cantes no\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"grandes\"]\n\narte alguna, y que toda su vida fue un profesional para minorías. Jamás le oí hablar tan desmesuradamente bien de don Antonio Cruz Conde. Avanzando los años, Aurelio atiza su sinceridad y en muchas apreciaciones se ha puesto sórdidamente inapelable. Su guerra íntima declarada a Talega —jahora acompañante de jurado!— ha tomado estado público. «Juan sólo conoce el cante monótono de su tío Joaquín. El resto de los cantes no ha podido oírlos de los grandes maestros de la época de oro. Talega tiene poca vergüenza, es un sucio, un cantaor aburrido y corto. Es un hindú que no puedo camelar. Ya me estoy hartando de la genialidad de los gitanos. Que si Torre para aquí, que si Torre para allá, y jala que te jala. Torre solamente fue bueno como siguirieyero y eso cuando podía hacerlo. En los demás cantes no hacía más que dar vueltas alrededor de algo que desconocía elementalmente. Talega, y eso no me lo han cont\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"memorias\"]\n\nto. J Aurelio no cantó en el homenaje a Pastora Pavón. Se limitó a mandarle un telegrama porque «estoy harto de concurrir a festivales donde Pastora y Pinto se anuncian y no concurren». 4 Ricardo vuelve a admitir —cada vez con menos atenuaciones dialécticas—que fue negativa su primera impresión del cante de Aurelio (concurso de 1956). Ahora afirma caricruz: «Aurelio no es frío, sino que fríos eran mis oídos». Está entusiasmado escribiendo sus memorias flamencas. Será un libro aproximado de trescientas páginas y cree que saldrá a la venta a fines del corriente año. Al parecer lo está haciendo con auxilio de un escritor o periodista amigo y me pide disculpas por no haberme elegido a mí para esta tarea, ya que mi residencia en Buenos Aires impedía una labor al alimón. Por esa razón me explico su actitud acaparante: regatea los datos de su vida y me pide mil excusas. Me lo dice precisamente cuand\n\n[EVIDENCE WINDOW 3 | retrieval_hint=CRIT_02 | trigger=\"profundidad\"]\n\ndo, lince y veloz, contraatacó para vaticinarme que si yo mantenía esa fidelidad paya, me perdería muchos conocimientos de primera mano del clan gitanó, se abondarían nuestras incipientes diferencias «étnicas» y, lo peor, no podríamos intercambiar planes comunes para el futuro. _sta predicción de Ricardo fue malagorera, ya que a pesar de reunirmos en vísperas del cuarto concurso, nunca más —excepto por vía epistolar— volvimos a tratar con paz y profundidad los temas que tan entrañablemente nos unían. Pero no hay pasos perdidos en la vida. El bloqueo gaditano me sirvió para ahondar en esa dirección. La publicación periódica de una de las obras inéditas de Anselmo González Climent, en esta Revista, va a proseguir, pese al imprevisto fallecimiento del maestro y amigo. «Viejo Carné Flamenco» es una colección de entrevistas con relevantes protagonistas del Cante y de la Fiesta, realizadas en las décad\n\n[ENDING CONTEXT]\n\nfandangos personales y poniendo en serias dudas el arcaísmo de jóvenes concursantes empachados de vieja discografía. Aurelio y yo disfrutábamos de la cabezonada operística que Jorque empleaba para escandalizar a una clase media impelida a ranciedad. Como mi amistad con él y Aurelio era pública y notoria, una delegación de aquella gente pidió a otros jurados (¡y autoridades!) establecer algo así como una zona de restricción para que Jorge y su minúscula comitiva no pudieran ingresar a las sesiones semiprivadas. Ni por esas. Sólo consiguieron que mi compatriota zumbara ya con abierto cachondeo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aurelio Sellé Nondedeu de Cádiz",
    "periodical": "candil",
    "issue_id": "1989-01",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "8-12",
    "page_number": 8,
    "word_count": 6491,
    "article_char_count_full": 37887,
    "article_char_count_review": 4500,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "grandes"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "memorias"
      },
      {
        "window": 3,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "profundidad"
      }
    ]
  },
  {
    "article_id": "1989-01-13-left-por-entre-los-cantes-de-levante-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nson los caminos que nos llevan a él. Así, la música es amor porque surge y nos llega a través de los caminos más puros y hondos del sentimienz. La música distingue al hombre y la huerta porque es cultura, y la cultura es el único ideal que puede redimir o remediar el drama del ser humano sobre la tierra. Aunque sea otra cosa más de las muchas que ignoramos, la verdad es que la música forma parte integral de nuestro propio acontecer diario, de nosotros como seres humanos y de la propia naturaleza que nos circunda: canta, hace música el viento en los árboles como el agua que corre. Y no estoy haciendo poesía, me refiero a los registros sonoros que vienen a originar toda esa inadvertida sinfonía que se llama música concreta por irremediablemente natural. Son notas musicales los sonidos que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"autoridad\"]\n\nue música la diversidad de acentos que al hablar nos distinguen entre sí, que marcan la distinción fonoparlante de los pueblos, comarcas, regiones, países y hasta continentes. Cada latitud geográfica se expresa sobre la musicalidad de su acento verbal característico y paralelo a sus aires folklóricos. Es la música de los pueblos, del pueblo, de la tierra... No sé si estaremos de acuerdo, pero yo estimo en mucho, y creo que en su justo valor, la autoridad que le conceden al musicólogo sus largos conocimientos de la música y sus orígenes en general. Considerando noblemente que errar es de humanos y rectificar de sabios, podemos, de la mano del técnico en musicología, hallar muchos vestigios «arqueológicos» por entre el silencio histórico de los principios musicoflamencos. Y es así cómo en nuestro caso el prestigioso y andalucísimo sevillano colega y contemporáneo de don Manuel de Falla, Hipólito Rossy, nos dice que «El cante jondo, la música y el baile flamenco constituyen un arte exclusivamente español de las provincias de Andalucía y Murcia, con salpicaduras en las de Badajoz, Albacete, Alicante y Valencia». Que «El flamenco o cante jondo se formó a base de la cultura musical\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_03 | trigger=\"interpretativo\"]\n\na investigadora. Sin embargo, parece como si Rossy reparara en el escenario geográfico donde se gesta esta expresión musicoespiritual sin añondar en él, sin detenerse a sopesar, si no minuciosamente, sí como atención a la base fundamental del fenómeno en cuestión, pues en otros puntos geográficos de España estuvieron alguna que otra de esas civilizaciones y no por ello dejaron claramente definido el aire estructural que caracteriza el desarrollo interpretativo de la música jondo-flamenca. Ya sabemos que no es igual generalizar que concretar, pero ahora lo creo necesario si hemos de hablar concretamente del cante de Levante, del cante de una tierra determinada, de esta tierra, una tierra donde a través de la identidad del Cante seguimos encontrando a Andalucía. Porque es la tierra la que hace al hombre como hace al árbol, la que da su vegetación al aire como el acento a la palabra de su gente. La tierr\n\n[ENDING CONTEXT]\n\norales, medios tantas veces desorbitados por el apasionado compadreo de unos o distorsionado por la incomprensión o antipatías de otros. Su obra, gracias a las más fieles técnicas de la conservación grabada, responde viva y vibrante al estudio y crítica del más riguroso e imparcial tribunal del Cante Flamenco. La mina y la huerta que Murcia canta en su cante se hace bandera en el conocimiento y hondura de un hijo cantar o más de esta tierra que hoy acuerda rendirle el más justo homenaje que esta personalidad cantaora merece y al que toda la afición consciente se adhiere en buena hora.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Por entre los cantes de Levante y Antonio Piñana",
    "periodical": "candil",
    "issue_id": "1989-01",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 1753,
    "article_char_count_full": 10532,
    "article_char_count_review": 3805,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "autoridad"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "interpretativo"
      }
    ]
  },
  {
    "article_id": "1989-01-14-left-una-voz-con-alma-daniela-pineda",
    "article_text_for_review": "Malagueñas, soleares y seguirillas gitanas... Historias de mis pesares y de tus horitas malas. Manuel Machado\n\nsa voz — voz total— voz verdadera: Ansia, fuego, pasión, rabia y desplante, fragua viva y cabal del puro cante que sabe a siguiriya y petenera... Esa voz con raíz de enredadera, que es grito de dolor y un ¡ay! amante, es taranta y toná —pecho sangrante— que hace vibrar a Andalucía entera... Esa voz que suspira y que desgarra cuando la cita a fondo una guitarra es de Antonio Mairena —luz de albura—... Que trae de los Alcores sus cantares y arrancándose al son de soleares hace del Cante Jondo esencia pura.\n\nDaniela Pineda Novo (De las Reales Academias de Sevilla, Córdoba y Málaga)",
    "title": "Una voz con alma",
    "periodical": "candil",
    "issue_id": "1989-01",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 121,
    "article_char_count_full": 696,
    "article_char_count_review": 696,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-01-14-right-cinco-a-os-de-la-desaparici-n-de",
    "article_text_for_review": "Paco Vallecillo\n\nNo es hoy ocasión para realizar, una vez más, el panegírico del maestro Antonio Mairena ni mucho menos un análisis incluso esquemático de la grandeza de su obra y de su arte. Maestro, decimos, tenido por muchos —uno entre ellos— como el más grande cantaor de todas las épocas. Reciente están aún los ecos del XXVII Fesival de Cante Jondo que con el nombre de su hijo predilecto se celebra en Mairena del Alcor al cierre de la temporada festivalera. Homenaje dedicado este año al también desaparecido pintor vallisoletano José Manuel Capuletti Lillo. Alrededor de estas celebraciones, las primeras cadenas radiofónicas de Andalucía hicieron una rememoración el día 5 de octubre, fecha del nacimiento y también de la muerte de Antonio Mairena y con espacios de cante suyo (algunos celosamente guardados y prácticamente inéditos para el gran público); y de este modo fue puesto de relieve una vez más la importancia que el artista desaparecido pudo dar a la Llave de Oro que él fundió nuevamente en el crisol de su irrepetible capacidad cantaora, poniéndola en un listón que hoy por hoy aparece como inalcanzable.\n\nJamás en el mundo entero pudo un gitano alcanzar niveles de reconocimiento tan altos: Andaluz Universal, Hijo Predilecto de Andalucía, cuando recibió de manos de un alcalde sevillano el título de hijo adoptivo de la capital de la Bética, uno recuerda con un punto de nostalgia y tristeza que apenas finalizado el acto y fundidos los dos en un fraternal abrazo, aún sudoroso tras haber cantado soberbiamente por soleares, nos dijo en voz baja, guiñando en un gesto entre emocionado y pícaro, tras haber alzado la vista hacia el artesonado de la sala capitular hispalense, adornada con las efigies de prepotentes reyes castellanos: —Dica, dica bien. Ahí están unos cuantos de los jundós que maraban a los calorres. La misma impresión que debió sentir cuando, como refleja una espléndida fotografía de Corte, recibió del rey Juan Carlos la Medalla de las Bellas Artes, presente la reina y un ministro socialista —¡quién pudiera haber imaginado el hecho diez años antes!—rubricando la solemnidad.\n\nJerez fue siempre plaza fuerte y almenada que tardó mucho tiempo en entregarse a Mairena. Es más, no estamos seguros de que el cantar de los Alcores llegara a conquistarla definitivamente. Tal vez no fuera sino una ocupación temporal, más o menos transitoria. De esto podría uno aportar testimonios que pueden conducir a la perplejidad. Es posible que la Fundación Andaluzas de Flamenco tenga ocasión —no es esto una afirmación, sino una presunción meramente subjetiva, no es ni siquiera una sugerencia— que pueda tener ocasión, decíamos, la Fundación Andaluzas, de recoger la demanda de la gran afición jerezana si, efectivamente, esa demanda tomara cuerpo, para rectificar algo que no se compadece suficientemente con la generosidad y el chanelamiento —sabiduría— de las buenas gentes flamencas de la tierra del Marco.",
    "title": "Cinco años de la desaparición del Maestro",
    "periodical": "candil",
    "issue_id": "1989-01",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 477,
    "article_char_count_full": 2943,
    "article_char_count_review": 2943,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
