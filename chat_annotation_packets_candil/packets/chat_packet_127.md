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
    "article_id": "1986-03-4-right-proceso-al-antigitanismo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«... Caballo que se desboca\n\nal fin encuentra la mar\n\ny se lo tragan las olas».\n\nE so dijo el apasionado Federico y no puedo asgurar que no me\n\ndejo llevar a veces por ese maldito caballo de la pasión. Sin embargo, la polémica no me gusta, la discusión serena, sí. Confieso que al enterarme de la propuesta de Manuel Barrios en su Contraréplica sobre el caló y los gitanos, y el ofrecimiento del equipo CAN-DIL que desea someter el debate al arbitraje de sus lectores, mi primera reacción fue la de negarme a entrar en un conflicto que pudiera ser mucho más largo que la conversación prevista inicialmente. Por otra parte, no me ha de-iado insensible la alabanza de Manuel Barrios respecto al tono de mi última intervención y quiero agradecerle y felicitarle por la corrección, elegancia y serenidad\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\nrcambio de datos y referencias sin ninguna animadversión y con el mismo deseo sincero, por ambas partes, de defender la verdad, junto con la posibilidad de aportar al lector algunos elementos documentales que le ayudarán a formar su propio juicio, éstos son los motivos que me han movido finalmente a aceptar el reto. Por Bernard Leblón Para mayor comodidad, adopto la forma dialogada y respeto el orden y la numeración de mi interlocutor. Quiero limitarme, siempre que sea posible, a citar los documentos o a resumir los datos, dejando los comentarios a cargo de los lectores. 1.º.—Manuel Barrios: No estoy de acuerdo con Bernard Leblón respecto a las fechas de publicación de los diccionarios de caló y a la influencia del de Borrow sobre los demás. —Bernard Leblón: Las fechas de publicación de los primeros diccionarios de caló son las siguientes: 1. BORROW, George H., The Zincali; or An account of the Gypsies of Spain. With an original collection of their songs and poetry, and a copious dictionary of their language. London, John Murray, 1841. 2 vols., 12. $ ^{\\circ} $ (2.130 palabras). 2. TRUJILLO, Enrique, Vocabulario del dialecto gitano. Madrid, 1844, 104 pág., 12.º. 3. JIMÉNEZ, Augusto, Vocabulario del dialecto gitano..., Sevilla, Impr. de J. M. Gutiérrez de Alba, 1846. 111 págs., 12.º. 4. CAMPUZANO, Ramón, Origen, usos y costumbres de los Jitanos y diccionario de su dialecto..., Madrid, 1848. 16.°. 5. D.A. de C., Diccionario del dialecto gitano, Barcelona, 1851, etc. Son trece en total sin contar el de Vega Suert que Manuel Barrios cita en su apartado 3.º y que no conozco. Le agradecería mucho si me pudiera dar las referencias precisas de este último. Quiero precisar que no me es nada grato pensar que un inglés haya podido ser el primer investigador en\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"lugares\"]\n\notundamente todavía, por no llevar fecha dicho documento. 2.º. —M. B.: Mientras que Bernard Leblón sostiene la tesis de un pueblo cíngaro único —incluyendo a los gitanos españoles—, mi teoría es que el pueblo gitano, de origen semita —entroncando con la etnia judía— atravesó los territorios semitas de Africa antes de llegar a España muchos años antes de que los cíngaros lo hagan por el nordeste español. —B. L.: Esta teoría se halla en diversos lugares y particularmente en Gitanos de la Bética, de José Carlos de Luna, que era un poeta, no un historiador, y que no aporta ninguna prueba documental al apoyo de su fantástica hipótesis acerca de los supuestos «sumero-semitas» (sic). Aprovecho la ocasión para rendir homenaje a una investigadora española demasiado ignorada en su propia tierra, doña Amada López de Meneses, quien dedicó gran parte de su vida a la búsqueda de los documentos de los archi\n\n[ENDING CONTEXT]\n\npido disculpas a Manuel Barrios por haber resumido de manera a veces muy brutal los puntos principales de su demostración, pero no era posible proceder de otra forma y espero haber respetado, a pesar de ello, el sentido de sus argumentos.\n\nEspero también que los lectores de CANDIL y el mismo Manuel Barrios quedarán convencidos, por lo menos, de que mi intención no es otra que la de buscar, apasionadamente —pero sin embargo con serenidad y objetividad—, la verdad.\n\nESPECTACULOS INTERNACIONALES\n\nO'Donnell, núm. 3-4.º\n\nTeléfís. 22 20 58 - 21 69 20\n\nSEVILLA\n\nPARTICULAR:\n\nTeléfono 27 80 78\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Proceso al antigitanismo",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 3209,
    "article_char_count_full": 18930,
    "article_char_count_review": 4371,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugares"
      }
    ]
  },
  {
    "article_id": "1986-03-7-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDe Bernard Leblon (Press Universitaires de France, Paris, 1985)\n\nPor:\n\nJosé Luis Buendía López\n\nLas cosas suceden casualmente y sin embargo marcan a las personas para siempre: Un día ya lejano el profesor francés Bernard Leblón siente el tirón de Andalucía, se deja enamorar por Sevilla y comienza a frecuentar el conocimiento de los gitanos españoles. Esta circunstancia se va a convertir en una feliz realidad vivencial para el propio autor y en un apreciable interés para la investigación española, que va a encontrar en Leblón uno de los más serios estudiosos sobre el tema de los gitanos dentro del panorama de escasa bibliografía existente, repleta además de tópicos incontables, cuando no de prejuicios que llenan de pavor.\n\nSu primer libro, ya reseñado en estas páginas, constituía un\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Granada\"]\n\na encontrar en Leblón uno de los más serios estudiosos sobre el tema de los gitanos dentro del panorama de escasa bibliografía existente, repleta además de tópicos incontables, cuando no de prejuicios que llenan de pavor. Su primer libro, ya reseñado en estas páginas, constituía un recorrido apasionante a través de la presencia gitana en la literatura española. Fue subautismo de fuego. Más tarde vendrían las ponencias en los Congresos de Jaén, Granada y Huelva, sus colaboraciones en CANDIL, donde por estas fechas se halla enzarzado en una hermosa polémica con el escritor Manuel Barrios, constituyendo ambas opiniones diferentes puntos de vista sobre los gitanos, que engrandecen nuestra panorámica general sobre algo tan plaga-do de lagunas, olvidos o insidias. La lectura de este último libro de Leblón completa el cuadro de sus estudios; libro profundo y ameno al mismo tiempo, está dividido en dos partes: La primera se ocupa de la, llamémosla, cuestión gitana, desde la Edad Media, fecha de la aparición en España de los representantes de esta raza, hasta el Siglo de las Luces, pasando por el estudio de todas las marginaciones y vejaciones sufridas por estas gentes, para desembocar en la pragmática de 1783, que pese a que algunos investigadores la consideran panacea de la solución gitana, para Leblón es indudable que sigue siendo dura e inhumana, y que concluye el propio autor: «El garrote es bastante más grueso que la zanahoria» (en todos los ejemplos citados la traducción es nuestra) como lo demuestra el que se siga discriminando a los gitanos en sus oficios, separando a padres e hijos so pretexto de educación, etc. Esta primera parte termina con la situación de los gitanos en la actualidad, y abarca desde el período Ilustrado hasta el siglo XX, concluyendo Bernard que nada ha cambiado y que se sigue adelante con un genocidio cultural que como él mismo afirma: «No aporta nada a la sociedad dominante si no es algunas escorias suplementarias; por el contrario, constituye una mutilación del patrimonio común de la humanidad en su conjunto». En la segunda parte de la obra, subtitulada con el nombre genérico de «La Justicia en acción», se hace un repaso a las instituciones de este secular brazo perseguidor, desde la Santa Hernandad hasta la Inquisición, desmenuándose las peculiaridades de cada código regresivo y llegándose a citar casos concretos como el del aparentemente filantrópico juez Francisco de Zamora, que en el fondo no representó más\n\n[ENDING CONTEXT]\n\na nuestro entender esenciales: a) Sin lo sitano lo flamenco no a) Sin lo gitano, lo flamenco no hubiera llegado a alcanzar la más alta calidad de lo andaluz.\n\nb) Sin lo andaluz, no hubiera sido posible el flamenco.\n\nEl meritorio trabajo de Manuel Barrios, con el que habrá que contar desde ahora para no incurrir en osadías que a veces desembocan en dislates, nos sabe a poco. Y nos sabe a poco, porque estamos seguros de que nuestro admirado amigo nos hará mérito de esa ampliación que esperamos y para satisfacción del estudioso de éste nuestro Arte habrá de depararnos algún día no lejano.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 1311,
    "article_char_count_full": 8189,
    "article_char_count_review": 4094,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "Granada"
      }
    ]
  },
  {
    "article_id": "1986-03-8-right-la-visi-n-de-andaluc-a-en-rub-n-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFue en este primero viaje cuando se aficionó a España en su do-\n\nble faceta: la cotidiana y la literaria. En el primer aspecto, sabemos que recorrió sus calles, se entusiasmó con sus costumbres ancestrales, en las cuales percibió la paternidad de numerosos rasgos de conducta americana, eso que hoy tan pomposamente denominamos en ocasiones las «raíces» de un pueblo. Literariamente, Rubén era un lector infatigable de nuestra literatura, hasta el punto de que Cervantes será, junto con el francés Víctor Hugo, la primera y más importante de sus fuentes literarias confesadas, y no sólo en lo que concierne al gran invento cervantino del Quijote, sino que también se sorprende con su Viaje al Parnaso, que sabemos le deslumbró por su capacidad de compaginar poesía y crítica literaria. También\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"pasión\"]\n\ns» de un pueblo. Literariamente, Rubén era un lector infatigable de nuestra literatura, hasta el punto de que Cervantes será, junto con el francés Víctor Hugo, la primera y más importante de sus fuentes literarias confesadas, y no sólo en lo que concierne al gran invento cervantino del Quijote, sino que también se sorprende con su Viaje al Parnaso, que sabemos le deslumbró por su capacidad de compaginar poesía y crítica literaria. También siente pasión por escritores medievales como Gonzalo de Berceo, al que dedicara un soneto en Prosas profanas, Juan Ruiz «Arcipreste de Hita», e incluso otros menos conocidos incluso dentro de nuestro propio ámbito literario, como lo fueron Johan de Duenyas, Johan de Torres, Valtierra o Santa Fe, sin que falten alusiones de respeto y reconocimiento hacia Alfonso X el Sabio o Raimundo Lulio, el inabarcable amoroso del amor, al que sabemos leyó con frucción. Tampoco se puede olvidar su lectura de autores españoles de otras épocas literarias, ya sea el Siglo de Oro u otros períodos: Góngora, con el que siente una innegable afinidad en materia poética y a quien dedicara, a la par de Velázquez, tres sonetos en Cantos de vida y esperanza, pero también están presentes Santa Teresa, Quevedo o Gracián. La huella de Bécquer Más tarde, la severidad crítica de Saavedra Fajardo o Larra promueven en él decididas admiraciones, y dentro de su siglo, aunque conoció y valoró de diversa manera la poesía de Zorrilla, Campoamor o Núñez de Arce, quien verdaderamente abrió en él una huella indeleble fue Gustavo Adolfo Bécquer, el cual llegó a influenciar en una parte considerable de su propia producción artística. Las influencias españolas de Rubén se complementan con su admiración por la pintura de nuestro país, siendo sus autores favoritos Velázquez, Murillo, Ribera, Pantoja, Zurbarán, Valdés Leal y Francisco de Goya. En fin, no queremos insistir más en este españolismo de la obra de Rubén Darío, qu\n\n[ENDING CONTEXT]\n\nde, Cincuenta años de poesía española (1850-1900). Madrid, 1960. DÍAZ PLAJA, G., Modernismo frente a 98. Madrid, 1966. GULLÓN, Ricardo, Direcciones del Modernismo. Madrid, 1971. LÓPEZ ESTRADA, F., Rubén Darío y la Edad Media. Barcelona, 1971. OLIVER BELMÁS, A., Poesía escogida de R. Darío. Edic. Madrid, 1951. ONÍS, Federico de, Sobre la caracterización del Modernismo. Universidad de Puerto Rico, 1955. PAZ, Octavio, Los signos en rotación y otros ensayos. Madrid, 1961. SALINAS, Pedro, La poesía de Rubén Darío. Buenos Aires, 1948. SILVA CASTRO, R., Rubén Darío a los veinte años. Madrid, 1968.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La visión de Andalucía en Rubén Darío",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "8-12",
    "page_number": 8,
    "word_count": 5059,
    "article_char_count_full": 30334,
    "article_char_count_review": 3566,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "pasión"
      }
    ]
  },
  {
    "article_id": "1986-03-12-right-respuesta-a-un-apasionado-racist",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Manuel Martín Martín\n\nD esconsola- do ciudadano Rincón: Aca- bo de leer su in-\n\ncoherente misiva —disculpe el tratamiento, el tú se lo reservo a los flamencos—, y, de repente, he dicho: otro racista que quiere recabar importancia saltando al ruedo de la discordia. Otro cualquiera en mi caso ya puede imaginar el uso que podría hacer de ella. Yo, cruzándome de brazos, y en base a mis principios educacionales, pensé: detesto a la gente importante y me producen náuseas los racistas y los «flamencólogos» de despacho.\n\nDe entrada, desconsolado ciudadano Rincón, ¿cómo se permite enjuiciar una labor informativa y crítica de unos hechos acaecidos donde su presencia ha brillado por su ausencia? Me consta que tan sólo ha asistido a los festivales de Los Palacios, Alcalá de Guadalra, Mairena del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nalra, Mairena del Alcor y el celebrado en Huelva por la ITEAF. Para más inri, visiona el cante bajo un prisma óptico excesivamente arcaico y, lo que es más grave, clasificando a los cantaores por el color de su piel. En este sentido, desconsolado ciudadano Rincón, hay que tener la cara muy dura para intentar tomarle el pelo, demagógicamente y sin argumentos, a unos lectores que merecen el respeto de todos los colaboradores de CANDIL y que saben mejor que nadie la trayectoria festivalera de cuantos cantaores aludo en mi artículo. Además, desconsolado ciudadano Rincón, no se puede partir de una premisa falsa. El artista que más festivales hace no tiene porqué ser el más destacado de los mismos. A mayor abundamiento, le recuerdo, ya que parece ignorarlo, que la mayoría de los festivales se organizan sobre criterios puramente comerciales y, en contadas ocasiones —«rara avis»— se forjan sobre la difusión y pureza de lo jondo. De ahí que cada aficionado pueda confeccionar su cartel —en base a su concepción del cante—, y de que yo, que soy otro aficionado de a pie, vaya al que me dé la gana. Usted sabe bien que ni en El Correo de Andalucía (del que conserva todos mis artículos), ni en las revistas donde colaboro, ni siquiera en el programa radiofónico que realizo a diario, cobro u\n\n[ENDING CONTEXT]\n\npolémica, a la vez que lamentamos que la acritud presida a veces las opiniones de nuestros remitentes, actitud ésta que enturbia la objetividad de aquéllas, y que, de presidir en posteriores comunicantes nos obligaría a prescindir de la publicación en esta revista de los juicios demasiado apasionados en los que se mezclen posturas personales, a las que, en ningún momento, CANDIL desea prestarse. Porque voy con la verdad yo no engañaré a mi gente, aunque tenga que luchar en contra de la corriente.\n\nAPERITIVOS SELECTOS\n\nBar TOMAS\n\nEspecialidad en\n\nPLANCHA\n\nMesones, 18 Teléf. 23 40 46\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Respuesta a un apasionado racista",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "12-14",
    "page_number": 12,
    "word_count": 2239,
    "article_char_count_full": 13364,
    "article_char_count_review": 2919,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  },
  {
    "article_id": "1986-03-14-right-cantes-de-ida-y-vuelta",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Antonio Hita Maldonado\n\nT erminó de leer leer el artículo así denominado en cuestión, del\n\nque es autor don Francisco Vallecillo, publicado éste en el número 42 del pasado mes de diciembre. Todo ello con motivo de la preparación, por parte de la Consejería de Cultura, de un disco de larga duración. «La Colombiana debe ser bastante moderna, pues dejan-do atrás a Escacena, sólo se citan cantaores más o menos actuales como intérpretes que divulgaron este cante: Valderama, Angelillo, El Americano y la eximia bailaora Carmen Amaya, que grabó un disco en el que cantaba, con voz ligera-\n\nEn dicho artículo se detallan los diferentes estilos que componen los denominados cantes de ida y vuelta, pormenorización que, en líneas generales, estoy de acuerdo.\n\nPero llegado al apartado CANTE E\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"HISTORICOS\"]\n\nan-do atrás a Escacena, sólo se citan cantaores más o menos actuales como intérpretes que divulgaron este cante: Valderama, Angelillo, El Americano y la eximia bailaora Carmen Amaya, que grabó un disco en el que cantaba, con voz ligera- En dicho artículo se detallan los diferentes estilos que componen los denominados cantes de ida y vuelta, pormenorización que, en líneas generales, estoy de acuerdo. Pero llegado al apartado CANTE E INTERPRETES HISTORICOS, y en su primera parte, al tratar específica-mente sobre la Colombiana, deduzco por su lectura la carencia de una información más objetiva, profunda y a la vez más real sobre su nacimiento y divulgación, ya que literalmente el señor Vallecillo nos escribe: mente cascada y derroches de gitanería, aquella popular —entonces— Colombiana que empezaba con el verso: Quisiera, cariño mío, que tú nunca m'orviaras.. Ignoro dónde ha podido obtener el señor Vallecillo una información tan primaria de este cante y, como consecuencia de ello, quisiera hacerle unas aclaraciones, y a la vez a los lectores de CANDIL, en la seguridad de que les agradará conocer realmente el nacimiento del cante que nos ocupa en cuestión: «La Colombiana». 1.º. —Este estilo no fue nunca cante de importación, pues jamás existió en nuestro país hermano (en este punto ambos coincidimos). La Colombiana fue creada, y cantada sólo dentro de nuestras fronteras, por lo cual no debería incorporarse nunca, junto con los denominados cantes de ida y vuelta, como el señor Vallecillo agrupa. 2.º.—Efectivamente, la Colombiana es relativamente moderna, siempre que consideremos moderno a un estilo que tiene de vida poco más del medio siglo (seguidamente les aclaro). 3.°.—Su nacimiento y posterior divulgación nace en los años treinta, cuando el tan vituperado por determinados artistas y críticos de esta generación don José Tejada Martín «Niño de Marchena», junto con don Hilario Montes, y tomando como base de su creación, entre otras formas musicales, la Rumba española, realizan una composición aflamencada a la que bautizan con el nombre de Colombianas y que en su segunda parte era interpretada a dos voces (esta segunda a modo de acompañamiento). Para este menester fue requerido el «Niño de la Flor». Tal creación, y a la vez novedad, fue presentada al públ\n\n[ENDING CONTEXT]\n\n«La Andalucita», etc., etc.\n\nCreo que con estas breves líneas he podido contribuir de forma aclaratoria a algunos puntos acerca de un estilo, aunque muy oído en su tiempo, al parecer nadaba desde hace algunos años un tanto en el desconocimiento general (olvidemos el porqué).\n\nIncluso el célebre Manuel Vallejo, que realizó de ella una adaptación féstera, mezcla de Colombiana y Tango, y cuya grabación fue publicada en 1934.\n\nEn otra ocasión (?), pues no soy colaborador de CANDIL, y siempre que me lo permitiesen les hablaría sobre las Vidalitas, Milongas y Guajiras, su historia y su divulgación.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantes de ida y vuelta. La Colombiana",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1165,
    "article_char_count_full": 7145,
    "article_char_count_review": 3915,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "HISTORICOS"
      }
    ]
  }
]
```
