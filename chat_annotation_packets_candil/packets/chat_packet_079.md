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
    "article_id": "1983-09-4-left-imposici-n-de-la-medalla-de-trab",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEL DIA 5 DE NOVIEMBRE, EN JAEN\n\nLa revista CANDIL ha querido recabar de Juanito Valderrama, desde la perspectiva de su larga permanencia en los escenarios, su opinión sobre asuntos que conciernen directamente al flamenco o a sus aledafios. He aquí sus respuestas.\n\nON motivo de la imposición de la medalla al mérito en el trabajo a Juanito Valderrama, se han organizado en su provincia natal, Jaén, toda una serie de actos de homenaje al cantaor de Torredelcampo. Un personaje tan admirado por unos como denostado por otros, seguidor, en un principio, de la inspiración de «El Niño Marchena» y, con posterioridad, labrador de un estilo propio que ha alternado especies típicamente flamencas con cuplés.\n\n—Juan, sabemos que Torredelcampo siempre ha tenido una gran tradición cantaora, ¿nos puedes\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionados\"]\n\nnaje al cantaor de Torredelcampo. Un personaje tan admirado por unos como denostado por otros, seguidor, en un principio, de la inspiración de «El Niño Marchena» y, con posterioridad, labrador de un estilo propio que ha alternado especies típicamente flamencas con cuplés. —Juan, sabemos que Torredelcampo siempre ha tenido una gran tradición cantaora, ¿nos puedes hablar de tus comienzos? —Sí, efectivamente, en mi pueblo siempre ha habido muchos aficionados que han cantado muy bien, siempre ha tenido mucha afición flamenca. En cuanto a mis comienzos, fueron muy prematuros, o sea, que empecé muy joven. En mi casa había una gran afición flamenca, porque casi toda mi familia cantaba; mi hermano Paco, que es mayor que yo, era el mejor aficionado que había en el pueblo, con poquita voz, pero muy buen aficionado; mi hermano Angel y mi hermano Manolo también cantan bien; mi hermana también cantaba un poquito, sobre todo por saetas; la pobre murió muy joven. Pero el que cantaba en mi familia era mi padre. Ahora, artistas antes que yo no ha habido nadie. Un día vino al pueblo, coincidiendo con la feria, un cantaor que se llamaba el Niño de San Lucas, y organizó un concurso para los aficionados, y nos presentamos varios amigos aconsejados por mi hermano Paco; amigos que desde muy pequeños nos juntábamos en las tabernas a cantar. A este concurso se presentaron varios aficionados de otros pueblos; uno de Torredonjimeno, que cantaba muy bien, que le decían Rompegalas, otro de Martos, otro de Jaén. El concurso se celebró en un cine que se llamaba el Cine del Sillero. Ese día conseguí mi primer premio, que consistía en cinco duros en plata. Yo tenía ganas de hablar con vosotros porque en CANDIL se me ha ignorado, y si se ha hablado de mí ha sido para mal. Se ha publicado algún trabajo en el que se hablaba de los cantes de trilla, adjudicándoselos a otro cantaor, concretamente la Sie- □ «Yo tenía ganas de hablar con vosotros porque en CANDIL se me ha ignorado y si se ha hablado de mí ha sido para mal». ga, la Trilla y la Temporera, y no es cierto, porque esos cantes los saqué yo. Porque yo segaba y trillaba, y estos cantes los aprendí de pequeño de los aficionados viejos que trabajaban en el campo. La Temporera donde mejor se ha cantao de España ha sido en Torredelcampo y en Arjona, este cante se conocía por Arjoneras; el primero que los cantó y los grabó fui yo. Son cantes de aquí, de nuestra tierra. De pequeño cantaba esta letra: —He conocido a bastantes artistas de la provincia, por ejemplo: al Cabrerillo de Linares, que cantaba dos o tres tipos de tarantas suyos muy bien. El Cabrerillo cantaba muy bonito, recuerdo que era un hombre bajito, muy nervioso y fumaba mucho. Luego estaba Luis «El Pavo», que tocaba lá guitarra muy puro, muy A mí me gusta la siega que tenga tres golpes buenos, el almuerzo y la merienda y a la noche el dinero. —¿Conocías tú algún otro ambiente flamenco en la provincia de Jaén? bien. Había otro que le llamaban «El Gordito», que murió, que tocaba la guitarra, y en aquella época estaba a la altura de Montoya. Había otro que se llam\n\n[ENDING CONTEXT]\n\ny claro, con idea de que no me llevaran a coger el pico y la pala, pues estuve cantando a los jefes, y por donde llega el capitán Guzmán y me dice: «De pico estás muy bien, veremos a ver mañana de pala cómo andas».\n\n—Creo que Dolores Abril lo que hace muy bien no es lo que hace conmigo.y tiene un sitio en España. Ella canta la canción española muy bien. Pero ha tenido que adaptarse a las cosas que hace conmigo, que para no ser lo suyo, hay que ser muy buen artista.\n\n—Es lo más bonito que se puede decir.\n\n—¿Y Dolores, qué clase de artista es? Aunque ella esté presente háblanos con sinceridad.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Imposición de la Medalla de Trabajo a Juanito Valderrama",
    "periodical": "candil",
    "issue_id": "1983-09",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 3620,
    "article_char_count_full": 19887,
    "article_char_count_review": 4723,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionados"
      }
    ]
  },
  {
    "article_id": "1983-09-6-right-literatura-rom-ntica-y-cante-fla",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor José Luis Buendía López\n\nHORA quisiéramos analizar cómo el flamenco responde con sus letras a esas inquietudes temáticas que todo el mundo reconoce como románticas, esto es, y muy a vuelta pluma: 1) Afan de libertad frente al invasor. 2) Oposición política al régimen absolutista de Fernando VII. 3) Exaltación de los héroes populares. 4) Mal del siglo. 5) Choque entre realidad y mundo soñado. 6) Afan individualista, exaltación del «yo». 7) La mujer como algo inaccesible y causa de desengaño. 8) Gusto por lo exótico, los viajes, fusión con la naturaleza. 9) Sentimentalismo e irracionalismo frente al cerebralismo de la época precedente. 10) Gusto por lo macabro y sepulcral, masoquismo innato. 11) Fatalismo existencial, potencial, impotencia ante la dialéctica del tiempo destructor y el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"históricos\"]\n\ne desengaño. 8) Gusto por lo exótico, los viajes, fusión con la naturaleza. 9) Sentimentalismo e irracionalismo frente al cerebralismo de la época precedente. 10) Gusto por lo macabro y sepulcral, masoquismo innato. 11) Fatalismo existencial, potencial, impotencia ante la dialéctica del tiempo destructor y el desengaño. Analicémoslos con cierto deteni- miento y por separado. POR LA LIBERTAD Y LA DIGNIDAD DE LOS SERES HUMANOS. Curiosos tiempos históricos los que corrían para España a comienzos del siglo XIX: Fernando VII, ese «Correctísimo miserable» como le denominara Fernando Quiñones, al que su propia madre, la reina María Luisa, no dudará en calificar de «fal-to de carácter, falso, cruel, ambicio-so» y otros epítetos por el estilo, entrega el trono de España a Napoleón y con él las libertades públicas y privadas de los españoles. Un grupo de patriotas, indignados por ese agravio a la integridad de la nación y a su dignidad personal de hombres libres, inician un levantamiento contra el dominador extranjero que pronto se hará general. Las tropas francesas serán detenidas en su avance. El 26 de mayo de 1808, sobreviene en Sevilla un alzamiento popular, se organiza una Junta, que con el nombre de Suprema declara la guerra a Francia, en gesto similar al producido en Asturias con algunos días de antelación. A estas Juntas se sumarían en seguida Córdoba y Cádiz. Se están fraguando los cimientos de las libertades que un gran sector del Romanticismo, el llamado liberal, asumiría como propio. Cádiz permaneció pa- ra siempre como un bastión de la libertad, a pesar de sus constantes asedios. Así lo proclama la copla flamenca: Napoleón Bonaparte con toas sus t\n\n[ENDING CONTEXT]\n\nsobre tan atractiva materia:\n\nALBORG, J. L., «El Romanticismo», vol. IV de su Historia de la literatura española. Gredos, Madrid, 1980.\n\nBECQUER, G. A., Obras completas. Aguilar, Madrid, 1960.\n\nDIAZ-PLAJA, G., Introducción al estudio del romanticismo español. Espasa-Calpe, Madrid, 1967, 3.ª edición.\n\nFERRAN, A., Obras completas. Espasa-Calpe, Madrid, 1969.\n\nPEYRE, H., Qué es verdaderamente el romanticismo. Doncel, Madrid, 1972.\n\nLLORENS, V., Liberales y Románticos. Castalia, Madrid, 1979, 3.ª edición.\n\nLLORENS, V., El romanticismo español, Fundación Juan March. Edit. Castalia, Madrid, 1979.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Literatura romántica y cante flamenco",
    "periodical": "candil",
    "issue_id": "1983-09",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "6-10",
    "page_number": 6,
    "word_count": 5495,
    "article_char_count_full": 32681,
    "article_char_count_review": 3307,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "históricos"
      }
    ]
  },
  {
    "article_id": "1983-09-11-left-responsabilidad-de-figura",
    "article_text_for_review": "Por Antonio Corcobado\n\nOTUNDAMENTE sí. ¿Por qué?\n\nPorque a quienes hemos venido asistiendo, en el lento discurrir del tiempo, al afianzamiento y consolidación de ese gran artista que hay en ti, Antonio Mairena, nunca se ha escapado a nuestras modestas do-tes de observación, la gran lucha que como hombre modesto, discreto y callado y por encima de estas nobles y grandes virtudes, amable, educado y correcto, has tenido que mantener para que de una manera rotunda llegara a imponerse el mérito y reconocimiento a tus excepcionales condiciones de intérprete e investigador de esta ciencia tan difusa que es el CANTE JONDO, en su línea de pureza.\n\nNacido al ARTE, en una época en la que abundaron grandes figuras, tuviste que abrirte paso con la verdad de tu técnica y de tu cante con la única razón de tu responsabilidad que a veces, irónico contrapunto, tenía que luchar con tu propia timidez y modestia que retrasaron la justa expansión a que tus méritos y nombre tenían innegable derecho.\n\nLa gran publicidad desplegada por muchos empresarios en una época floreciente y, por tanto, propicia para la explotación de espectáculos que poco o nada tenían que ver con el ARTE, que con tanto tesón y cariño tú defendías, fue quizá una de las causas que retardaron el pleno conocimiento de lo que tu inmensa figura llevaba dentro de sí, como auténtico catedrático de un ARTE, que si fue y, aún hoy, sigue siendo de minorías, se debió a la voracidad de quienes generalmente no atendían a otra razón que a la de su exclusivo beneficio, explotando unos espectáculos «pseudo-flamencos» que lejos de orientar a esa inmensa afición que se había despertado, la desorientaban al dar paso preferente a cancionistas y cupleteros que si indudablemente distraían, bien poco era lo que enseñaban al ignorar la raíz y el origen de lo que decían y pretendían falsamente representar.\n\nEn mi ya largo peregrinar como aficionado por los senderos de este ARTE, siempre me impuse la obligación de ser exigente conmigo mismo, eligiendo para cada reunión a aquellos artistas de quienes (vana ilusión) trataba de aprender lo que de pureza encierra en sí, cada cante, huyendo de estilismos a los que cuando ha llegado la ocasión, he reconocido su mérito y ello carecería de importancia si no se debiera a la impronta que dejaron en mi vocación de pretendido buen aficionado, las enseñanzas derivadas de las directrices que tanto tú, como algún otro artista excepcional, marcábais con vuestra pedagogía como consecuencia de ese afán investigador al que con tanta ilusión te has venido entregando, siendo esta una face-ta que creo necesario airear para que llegue a toda la afición el auténtico completo que forma tu personalidad de artista excepcional, al que si algún día, porque el tiempo es inexorable, mermara tus facultades de gran intérprete, podrías continuar tu enseñanza en conferencias en las que volcando tu sabiduría siguieras rindiendo el gran servicio que por afición, gusto y delectación, te has impuesto responsablemente. Si importancia tiene y, para mí la hay en gran medida, en la fidelidad de tus interpretaciones ajustadas a lo que mandan los cánones, muchas más le concedo a la que ha tenido, y estoy seguro seguirá teniendo, tu labor investigadora, razón que preferentemente ha inspirado la dedicación de este homenaje.\n\nTorpe deseo mío el pretender tener un hueco en el homenaje que el Excmo. Ayuntamiento de tu pueblo va a rendirte próximamente, al que quiero y deseo adherirme brindándote el tributo de esta modesta colaboración en la que van encerrados juntamente con mi admiración y respeto el cariño a que te has hecho acreedor, querido Maestro, para agradecerte cuanto nos has enseñado, ampliando el modesto bagaje de nuestros conocimientos como aficionados y al desearte desde aquí una larga vida, te envía un fuerte abrazo tu admirador y amigo.\n\nPorque por causas que desconozco el homenaje del libro que se pensó editar no se llevó a cabo, el mío como el de otros muchos aficionados, supongo no pudieron ver la luz, quedando en el anonimato del que hoy pretende salir buscando su posible acogida en nuestras páginas de «CANDIL», como tributo póstumo a su memoria, ya que falta de entre nosotros, aun cuando de por vida perdurará su recuerdo, ya que al no existir colisión entre su vocación y trabajo, nos legó una obra tan completa en la que se acredita como dijo el poeta griego Pindaro que LLEGO A SER EL QUE ERA, nada más y nada menos que ANTONIO MAIRENA.\n\nMadrid, mayo 1982",
    "title": "¿Responsabilidad de figura?",
    "periodical": "candil",
    "issue_id": "1983-09",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 751,
    "article_char_count_full": 4479,
    "article_char_count_review": 4479,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-09-11-right-con-ternura-y-con-gran-valor",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAnte la vida y la muerte de Antonio Mirena\n\nPor Félix Grande\n\nTUVO una vida muy difícil y una hermosa vejez y ha merecido un clamoroso entierro. Sus familiares (después de sus hermanos y toda la comunidad gitana, sus familiares somos todos aquellos que amamos el flamenco) recibieron muchos telegramas de pésame. Uno de ellos, el de la Casa Real. Otro, del presidente del Gobierno. Otro, del vicepresidente del Gobierno. Otro del ministro de Cultura. Hace algún tiempo, a petición de la Asociación Nacional Presencia Gitana, el Ministerio de Trabajo le otorgó la medalla al Mérito en el Trabajo. Pocas veces una medalla ha sido tan bien puesta como cuando quedó en el pecho de este viejo gitano. Hace aún escasos meses, la Corona le entregaba la medalla de Oro de Bellas Artes. Pocas artes tan\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nl Gobierno. Otro, del vicepresidente del Gobierno. Otro del ministro de Cultura. Hace algún tiempo, a petición de la Asociación Nacional Presencia Gitana, el Ministerio de Trabajo le otorgó la medalla al Mérito en el Trabajo. Pocas veces una medalla ha sido tan bien puesta como cuando quedó en el pecho de este viejo gitano. Hace aún escasos meses, la Corona le entregaba la medalla de Oro de Bellas Artes. Pocas artes tan fieramente bellas como el arte flamenco, y ningún cantaor, tal vez, más digno de esa distinción que el gran don Antonio Mairena. El anciano maestro recibió esa medalla junto a Plácido Domingo, Ernesto Halffter, Cristino Mayo, Nuria Espert, Athur Lundkvist, Mario Camus, Antonio Gades, Jean Cassou, Teatro Lliure... Es lo más natural. Dijeron los informadores que la ovación más numerosa y más vehemente fue la que saludó al nombre del cantaor de Mairena del Alcor. Es hermoso que sucediera así. Hace pocas semanas cayó enfermo. Su corazón no resistía. Combatió con la muerte y logró arrebatarle al\n\n[ENDING CONTEXT]\n\nfacultades y muriéndose. No tienen Seguridad Social. Y no tienen jubilación. Por cada don Antonio Mairena que puede atender a sus enfermedades y morir clamorosamente, hay muchos artistas flamencos, a quienes la vejez los convierte en mendigos. Le pido a mi Gobierno que legisle contra esa mendicidad, contra esa humillación. Don Antonio Mairena nos lo agradecería. Algo como un suspiro sonaría por entre la tierra que lo cubre. De «El Socialista», n.° 327 - 14-IX-83.\n\nJ. A. PULPON\n\nESPECTACULOS INTERNACIONALES\n\nO'Donnell, núm. 3 - 4.° Tlf. 22 20 58 - 21 69 20 SEVILLA PARTICULAR: Teléfono 27 80 78\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«Con ternura y con gran valor»",
    "periodical": "candil",
    "issue_id": "1983-09",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1839,
    "article_char_count_full": 11203,
    "article_char_count_review": 2642,
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
    "article_id": "1983-09-13-left-colecci-n-de-guitarras-de-manuel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor J. A. Pérez-Bustamante\n\nM ANUEL CANO TAMAYO, excelente concertista y profesor de guitarra flamenca, es sobradamente conocido como para necesitar a estas alturas de especial presentación. Todo buen aficionado al flamenco es consciente de su brillante virtuosismo, así como de la gran originalidad de su repertorio guitarristico, que le confiere una singular personalidad, avalada, además, por su abierta simpatía, sentido del humor y elevado concepto de la amistad.\n\nExiste un aspecto aspecto relevante dentro de la proyección guitarristica de Manolo Cano, bastante conocido, auque no en detalle, cual es su vocación de coleccionista de guitarras, aspecto este al que va dedicada la atención preferente de este breve artículo.\n\nCon ocasión de una no muy lejana visita que tuve el placer de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"granadino\"]\n\nrta simpatía, sentido del humor y elevado concepto de la amistad. Existe un aspecto aspecto relevante dentro de la proyección guitarristica de Manolo Cano, bastante conocido, auque no en detalle, cual es su vocación de coleccionista de guitarras, aspecto este al que va dedicada la atención preferente de este breve artículo. Con ocasión de una no muy lejana visita que tuve el placer de efectuar a la casa del artista, sita en un tranquilo paraje granadino, concretamente en la Colonia de Cervantes, 13, visita organizada a través de los buenos oficios de nuestro común y apreciado amigo Rafael García Villanova, gran aficionado, entendido y ejecutante de exquisito gusto de la guitarra en su proyección de música clásica, tuve el gran placer de examinar en directo y con cierto detenimiento la valiosa colección de instrumentos que posee Manuel Cano, en una grata e inolvidable «soirée» en la que el artista nos iba mostrando —con evidente fruición— los diversos ejemplares de guitarras que posee, acompañado todo ello por amenas explicaciones y anécdotas referentes a la historia y vicisitudes relacionados con la adquisición de cada ejemplar. Quede claro, ante todo, que el interés primordial de Cano en relación con las docenas de instrumentos que posee —algunos de gran valor— no se centra de modo especial en su número, ni en su aspecto de inversión patrimonial; por el contrario, el interés principal del artista por esta colección radica en disponer de ejemplares o muestras representativas de las obras de arte que han ido saliendo de la mano de los más destacados maestros guitarreros españoles. Sin ánimo de ser exhaustivo en la consideración de las características constructivas y datos relevantes, o comentarios anecdóticos, relacionados con esta excelente e inédita colección de guitarras, me limitaré en lo que sigue a relacionar y comentar brevemente lo más llamativo de algunos de los instrumentos más destacables: 1) Guitarra de diez clavijas (para cinco cuerdas dobles), que han sido posteriormente transformad\n\n[ENDING CONTEXT]\n\ncuyos antiquísimos precedentes han de buscarse en el arco de caza del hombre de la época neolítica. Confio, a través de lo hasta aquí expuesto, haber podido despertar de algún modo la curiosidad del lector aficionado a la guitarra del payo, o a la «sonanta» flamenca, a la «bajañí» del «calé», así como haber podido contribuir un poco al mejor conocimiento de la personalidad, de las aficiones y de las inquietudes de ese gran artista y amigo que es Manolo Cano. Cádiz, septiembre, 1983\n\nSERAFIN ALCALA\n\nAvenida Muñoz Grandes, 14 y 16 - J A E N\n\nDistribuidor:\n\nSucursal en BAILEN: Zarco del Valle, 8\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Colección de guitarras de Manuel Cano",
    "periodical": "candil",
    "issue_id": "1983-09",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 1875,
    "article_char_count_full": 12139,
    "article_char_count_review": 3655,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "granadino"
      }
    ]
  }
]
```
