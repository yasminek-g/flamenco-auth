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
    "article_id": "1982-05-16-left-cartas-a-candil",
    "article_text_for_review": "Señor director de la revista CANDIL\n\nMi estimado amigo:\n\nHace unos meses, sensibilizado por ese tema de la tercera edad de los artistas flamencos, que CAN-DIL, ha tocado con indudable acierto en varias ocasiones, escribí un breve artículo bajo el título «Flamencos de la tercera edad» (PUEBLO, 23/XI/81). Al parecer, este artículo ha sido reproducido por la revista SEVILLA FLAMENCA, ns. 19/20, a la que mereció determinados elogios, según he sabido ahora gracias a un artículo anónimo aparecido en CANDIL, número 19, bajo el título «Enderezando entuertos: ¿Chacón murió en la indigencia?».\n\nSu autor, a quien resultan incomprensibles los elogios a mi artículo, asegura que no desea buscar polémicas; aunque ello también está en mi ánimo, quiero sin embargo consumir un turno de alusiones a la vista de la directa pregunta que me hace en su texto: «¿Quiere decir el señor Gómez Alfaro que don Antonio (Chacón) pasó necesidades económicas? ¿Que acabó sus días en una modesta pensión madrileña, solo, como una rata?... ¿De dónde viene esa desinformación que asevera que don Antonio vivió en la indigencia los últimos años de su vida?».\n\nDado el interés mostrado por mi enderezador hacia mi modestísimo artículo, habrá podido apreciar que intentaba solamente apoyar, en la medida de mis fuerzas y en lo que tiene de justo y razonable, un proyecto que favoreciera a los viejos artistas flamencos, merecedores de mejor trato cuando llegan a la ancianidad. El hecho de que figurase Chacón entre otros casos que se citaban también, era puramente circunstancial y, sin duda por comprenderlo así, SEVILLA FLAMENCA se identificó con la tesis central del artículo, sin detenerse en su examen anecdótico.\n\nPuede creer mi enderezador, que me alegra encontrar en él un biógrafo apasionado por cuantas cuestiones afectan a su personaje, del que mi artículo decía: «El final de don Antonio Chacón también estuvo unido a la penuria económica, en el solitario cuarto de una modesta pensión madrileña». Si ello no ocurrió así, es cosa que me alegra, pero que no empece la tesis de mi artículo, del que puede prescindirse del caso de Chacón, sin que sus argumentaciones pierdan validez. En cualquier caso, lamento que los derechos de las últimas grabaciones y las limosnas del señor duque no fueran suficientes como para impedir la necesidad de alquilar habitaciones a estudiantes, actividad que no deja de ser un signo de modestia económica —yo no utilicé nunca la palabra indigencia—.\n\nAhora bien, como el colaborador de CANDIL número 19 parece tan deseoso de ilustrarme, hasta el punto de haber roto la promesa de no éscribir que hizo un día a Manuel Urbano, según confiesa, quiero decirle «de donde viene esa desinformación». En el mismo número de CANDIL habrá podido ver dos páginas, firmadas precisamente por su entrañable M. Urbano, dedicadas, bajo el título «Aunque no quepa en el papel: Un inmenso catálogo cantaor», al reciente libro «Historia del cante flamenco», de Angel Alvarez Caballero. Según M. Urbano, de este libro «interesa resaltar, sobre todo, su recopilación de ingente cantidad de fichas bibliográficas y de hemeroteca, con excelente criterio y orillando controversias, para ofrecer de forma\n\nordenada la historia de más de un cuarto de millar de cantaores flamencos desde el legendario Planeta hasta las más recientes levas cantaoras, y todo ello muy bien adobado y repleto de multitud de datos, noticias, apreciaciones y juicios de calidad, estilos, etc. etc.».\n\nDesgraciadamente, Angel Alvarez Caballero desconocía la biografía de Chacón que tan amorosamente escribió el autor de «Enderezando entuertos» pues, en las páginas 159/160, dice así sobre los últimos años del gran artista flamenco: «Pero entonces se puso enferma Anita, la compañera de tantos años de Chacón, y la situación se le puso tan negra en el aspecto económico, que él mismo buscó grabar aquellos discos. José Ortega y Enrique el Granaíno le subieron sujetándolo por los brazos a un estudio de la calle Peligros. Poco después, el 21 de énero de 1929, moría en una modesta pensión madrileña donde ocupaba un cuarto».\n\nEn honor a la verdad, debo decir que de aquí salió la frase de mi artículo, pues mi conocimiento de la bibliografía flamenca no es tan amplio como yo desearía. Pienso ahora que M. Urbano está asistido de toda la razón al decir que faltan en «Historia del cante flamenco», aun al precio de su monotonía, las notas que indicasen el origen exacto de la documentación manejada. De esta forma sería posible, mediante una inteligente labor detectivesca, llegar hasta quien lanzó por vez primera las falsas afirmaciones sobre los últimos días de Chacón. Entre todos los entuertados conseguiríamos sin duda enderezarlo para siempre.\n\nRogándole acepte mis disculpas por las molestias ocasionadas, reciba el cordial saludo de su affmo.\n\nA. GOMEZ ALFARO\n\nNota de la Redacción.—Contra nuestra voluntad y por un error de imprenta apareció como anónimo el artículo «Enderezando entuertos...», cuando su autor era nuestro colaborador Manuel Yerga Lancharro. Por cierto, A. Alvarez Caballero dá en la bibliografía de su libro la biografía que de Chacón publicara Yerga en «Flamenco»; cosa que, naturalmente desconocía A. Gómez Alfaro, al no saber quien fuera el autor del artículo que, en contra de las normas de CANDIL figuró como anónimo.\n\nInstituto de Estudios Giennenses. Candil : boletín de la Peña Flamenca de Jaén. N.º 21, 5/1982. Página 16",
    "title": "Cartas a Candil",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 876,
    "article_char_count_full": 5424,
    "article_char_count_review": 5424,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-05-16-right-el-romanticismo-flamenco-de-augu",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nVIDENTEMENTE el flamenco no se ajusta a los límites geográficos de Andalucía, aunque en ella nacieron, crecieron y se desarrollaron sus expresiones de queja y desgarro, de amor y muerte, de vida en su total compl ejidad. Pero indudablemente esos límites andaluces abren sus brazos mutricios y abrazan gran parte del territorio español, impregnando con su recio olor a pueblo activo el folklore de otras regiones, y atrayendo a su esfera a figuras nacidas más allá de Despeñaperros (¿Verdad, Carmen Amaya, Gades, Vicente Escudero?). Precisamente en Madrid, en el año 1835, vino al mundo Augusto Ferrán, el poeta romántico amigo de Bécquer. Ambos fueron artífices del cambio de la poesía de su época en las décadas del 50 y del 60, que tomaría la dirección inequívoca de lo popular como meta artística\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"declarado\"]\n\ns Flamencas celebrado en Almería, y a ese trabajo remito el interesado en profundizar en este tipo de relaciones. Sólo diremos, para enmarcar la renovación poética llevada a cabo por Ferrán, bajo el signo de lo popular, que, hacia mediados del siglo XIX se percibe en una serie de investigadores y creadores la aureola de lo flamenco, tanto en la adopción de temas provinientes de la sensibilidad andaluza, como en el gusto cada vez más abiertamente declarado por la copla popular de extracción flamenca. Sería el caso de A. de Trueba, que aludía en 1852 a la necesidad de acudir al pueblo llano como base de toda inspiración poética, o a los testimonios de Ventura Ruiz Aguilera o Angel María Dacarrete (poeta éste en el que, según la reciente crítica, encontramos el más claro antecedente de Bécquer), que giosan la belleza del cante flamenco, para desembocar en el discurso de entrada en la Real Academia Española del gaditano Antonio García Gutiérrez, en el cual disertaría sobre «Refranes, cantares y romances», constituyendo su estudio un entusiasta documento sobre nuestro arte. Es en esta esbozada línea populista de la nueva lírica romántica en el que hay que enmarcar la aparición de los dos libros claves de Augusto Ferrán: «La Soledad», de 1860, y «La Pereza», de 1870, que ya fueron saludados en el momento de su aparición pública por el mencionado escritor Ventura Ruiz Aguilera, en carta a Manuel Murguía, como los primeros versos de un escritor culto con sabor de cantares verdaderamente populares. Pasaremos sin detenernos por el bellísimo prólogo de Gustavo Adolfo Bécquer a «La Soledad», en el que realiza un sentidísimo elogio del populismo del libro, y reproduciremos tan sólo un pequeño párrafo en el que, al glosar el título que le puso su autor atina con el tono domin\n\n[ENDING CONTEXT]\n\nandalucísima que el amor es el trabajo que más le satisface:\n\nMe llama holgazán tu madre, ¡como si el querer no fuera una ocupación muy grande!\n\nY es que, en fin, el poeta sabe que todos los sentimientos e inquietudes del hombre se pueden cantar por lo jondo, y no merece la pena decirlo de otra manera. A ello dedicó su vida y su obra, por eso lo recordamos aquí, por saber descubrir flamencamente las verdades «tan gordas» que se dicen en el cante de nuestra tierra:\n\nLos cantares de mi tierra dicen verdades muy gordas que se cantan en voz alta para que todos las oigan.\n\nJosé Luis Buendía López\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El romanticismo flamenco de Augusto Ferrán",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "16-19",
    "page_number": 16,
    "word_count": 4475,
    "article_char_count_full": 25985,
    "article_char_count_review": 3424,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "declarado"
      }
    ]
  },
  {
    "article_id": "1982-05-20-right-el-flamenco-en-el-arte-actual",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Francisco Zueras\n\nHaciendo historia, hay que comenzar por destacar que la primera de estas exposiciones tuvo lugar en agosto de 1972, en la localidad cordobesa de Montilla, proporcionando la doble lección de que si el flamenco y las artes plás ticas podían formar una simbiosis perfecta, también podía ser una total realidad la conjunción de ambas cosas con la poesía.\n\nCuando con el gran pintor Antonio Povedano a la cabeza, un grupo de artistas plásticos, críticos de flamenco y de arte --Agustín Gómez, Fausto Olivares, Venancio Blanco, Francisco Moreno Galván y el autor de este comentario—decidimos poner en marcha, hace diez años, las exposiciones monográficas «El Flamenco en el Arte Actual», no sospechábamos la proyección, brillantez y continuidad que estas muestras iban a tener. Y es\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"segunda\"]\n\nActual», no sospechábamos la proyección, brillantez y continuidad que estas muestras iban a tener. Y es este esplendor conseguido el que me mueve a escribir, a manera de recapitulación de una década, un comentario para CANDIL, esta magnífica revista tan propicia a comentar todo lo que se hace por y para el flamenco. Aquel éxito nos estimularía —siempre con Povedano como insuflador de renovados entusiasmos— para organizar otras exposiciones. La segunda monográfica tendría lugar en la Sala Municipal de Arte, de Córdoba, en mayo de 1974, haciendo de telón de fondo del «VIII Concurso Nacional de Flamenco». No hubo representación poética pero sí se incrementaría la nómina de artistas plásticos con los nombres de Antonio Campillo, Giuseppe Gambino, García Donaire, Gutiérrez Montiel y Angel López-Obrero. Y así, junto a un reducido grupo de pintores y escultores —Venancio Blanco, Antonio Bujalance, Eduardo Carretero, Miguel García de Veas, Francisco Hernández, Miguel del Moral, Francisco Moreno Galván, Fausto Olivares, Antonio Povedano, Rafael Rodríguez Portero y Francisco Zueras— se exhibieron textos de otros tantos poetas, igualmente aficionados al flamenco: Juan Bernier, Caballero Bonald, Luis Jiménez Martos, Mario López, Antonio Murciano, Manuel Ríos Ruiz, Mariano Roldán y Fernando Quinones. El malogrado crítico de arte José María Moreno Galván nos escribiría un texto para el catálogo, que fue un profundo estudio sobre la «estética de lo jondo». Este creciente interés nos movería a estudiar a fondo este fenómeno de las relaciones entre la pintura, la escultura y el flamenco, utilizando como tribuna el Seminario de Estudios de la «Peña Flamenca de Córdoba», en la semana cultural celebrada en el mes de hablaría sobre «El Flamenco en las Artes Gráficas del siglo XIX». Las decisivas gestiones de Povedano harían posible la tercera monográfica. Esta tendría lugar en Madrid, en el «Club Urbis», entre mayo y junio de 1976, y sería un gran acontecimiento por la altura artística de la muestra y por ir arropada de otros acontecimientos —conferencias sobre el fla\n\n[ENDING CONTEXT]\n\ndeducirse de este comentario, un acontecimiento de excepción han llegado a ser aquellas monográficas que nacieron silenciosamente ahora hace diez años. Ni más ni menos que una rotunda lección de arte contemporáneo a través del flamenco. Elogiable intento el de estos veintidós artistas de conjugar una cosa y otra, porque como dijo António Gala en el catálogo: «El flamenco y el arte son dos pesos muy grandes. Contados hombres hay que puedan cargar con semejante cruz y arrastrar tanta herencia». Pues bien, algunos de esos contados hombres se han dado cita en esta magnífica exposición itinerante.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Flamenco en el arte actual",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1940,
    "article_char_count_full": 12523,
    "article_char_count_review": 3712,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segunda"
      }
    ]
  },
  {
    "article_id": "1982-05-22-left-la-guitarra-y-su-aprendizaje",
    "article_text_for_review": "Por Antonio Piñana (padre) y Juan Ruipérez Vera\n\nD E SDE la época medieval en que la guitarra poseía tres cuerdas, teniendo como misión el acompañamiento de canciones populares, hasta hoy, que está constituida por seis, las virtudes musicales que atesora alcanzan tal magnitud que, unidas a sus recursos de expresión sumamente difíciles, ha sido objeto de estudio por grandes maestros y musicólogos, entre los que podríamos citar al profesor Manuel García Matos.\n\nLa guitarra desde su evolución va adquiriendo, por la sucesiva adición de la quinta y sexta cuerda, el perfeccionamiento que dio lugar a la introducción de distintos procedimientos y mecanismos que le imprimen un gran efecto musical, destacando entre ellos: el «vibrato», el «portamento» y el «trémolo» que, unidos al «rasgueado», a los distintos «arpegios» y al «picado» en las falsetas, así como a las ejecuciones del dedo pulgar, hacen que se transmita, por medio de los dedos del ejecutante, toda una amplísima gama de matices y de sentimientos humanos.\n\nNo queremos dejar de resaltar que en la guitarra se da una dualidad muy importante y significativa, por lo que ha sido estudiada bajo dos versiones. La primera es aquella en que las obras han sido transcritas en partituras y su ejecución corre a cargo de intérpretes dotados de estudios musicales, dándose esta circunstancia, en la mayoría de los casos, en aquellos ejecutantes encuadrados en la música clásica. En segundo lugar, las obras para guitarra flamenca, observada desde el punto de vista como acompañante del cante, debido a la dificultad que encierran algunos de sus mecanismos, no ha sido anotada en partituras. La dificultad ha hecho que en la actualidad no existan métodos eficaces para el estudio y aprendizaje del acompañamiento de las distintas versiones del arte flamenco, prueba de ello puede ser Rafael Marín (citado por Ricardo Molina y Antonio Mairena en su libro «Mundo y formas del cante flamenco»), quien en 1902, publicó un método do nde no acertó a vencer las dificultades que el «rasgueado» representaba en la anotación. Por cierto, los citados flamencólogos indican: «Que los tocaores no saben música, por lo general. Aprenden de oído y varían a su «gusto» las falsetas. (La experiencia ha demostrado que el conocimiento científico de la música no ha beneficiado a los pocos tocaores que se preocuparon de adquirirlo)».\n\nEs cierta la afirmación y la apreciación del poco beneficio que ha representado a los tocaores que se preocuparon por adquirir el conocimiento científico de la música; pero es necesario, una vez más, introducirnos en el tema técnico-musical procurando no olvidar que todo aquello que se interpreta, bien sea el toque de concierto o de acompañamiento al baile y al cante, en el arte flamenco es música y, por tanto, susceptible de ser anotado. ¿No será que el arte flamenco al ser un impulso intuitivo, momentáneo y emocional que brota, al estar rodeado de un ambiente propicio, del interior de un intérprete que lo siente y vive, y a su vez carece de unos conocimientos musicales pero no desprovisto de un arte, la causa por lo que esta aforación musical unida al sentimiento no pueda ser transcrita en un pentagrama? Creemos que el flamenco sí puede ser anotado sin necesidad de que en la anotación se pierda toda la belleza, toda el alma y todo el sentimiento que el artista imprima a su creación; la música, al ser un arte formado por la combinación del tiempo y el sonido es, en definitiva, commensurable y, por tanto, susceptible de anotar. Ahora bien, analizados estos conceptos, observamos que esta carencia de anotación es debida a la poca atención que a este tema se le ha prestado durante largos años y, por supuesto, a la falta de una programación objetiva que nos lleve a un perfecto aprendizaje.",
    "title": "La guitarra y su aprendizaje",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 627,
    "article_char_count_full": 3781,
    "article_char_count_review": 3781,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-05-22-right-las-alturas-de-manuel-torre-y-va",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nN el número 18 de CANDIL aparece un breve y sustancioso trabajo firmado por usted, «¡Qué jartura de Manué!», que me confirma su gran calidad de aficionado y mejor escritor; un trabajo, que es delicada y sutilísima réplica a uno de los capítulos de mi libro «Apuntes y datos para las biografías de Rojo el Alpargatero, La Trini, Chacón y Manuel Torre» (1), donde discrepa con fineza de mis razonamientos sobre la altura física de Manuel Torre. Delicadeza, por cierto, poco ejercitada en el mundo flamenco y menos aún por su autoerigida gendarmería. Si bien usted, señor Barrios, hace un examen riguroso y técnico de la fotografía que se publica, y en la que aparece Manuel Torre haciéndole entrega a Vallejo de la llave del cante en el madrileño teatro Pavón, 1926, quisiera señalar que los artistas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nle entrega a Vallejo de la llave del cante en el madrileño teatro Pavón, 1926, quisiera señalar que los artistas marcados con los números 3, 11 y 18 son más altos que «Manué», a pesar de estar situados en un plano posterior al ocupado por el gitano de Jerez; fíjese como el niño Luis Maravilla, con pantalón corto, le llega casi al hombro... Pero dejemos las discrepancias y apreciaciones fotograficas. Por mi parte sigo creyendo que el «Majara», el hombre de las rarezas (palabras textuales de Pepe Pinto), era más alto de lo normal; pero no como se le ha tallado sin fundamento: casi un gigante. Algo que, a mi parecer, no es más que una burda mitificación, cuando su altura real la tiene en su grandeza cantaora. De todos modos, usted puede demostrar fehacientemente y para siempre la altura física de Manuel localizando su expediente de quintas en el archivo del Ayuntamiento de Jerez de la Frontera, reemplazo de 1899. En cuanto a José Cepero, repito lo que ya he dicho en varias ocasiones, sólo fue un poco más bajo que «Manué», no sobrepasando el metro sesenta y cinco. A él lo conocí en mi ciudad, patria de Zurbarán, y en su casa madrileña, número 42 de la calle Mesón de Paredes. Y ya que vamos de «jarturas» y Manueles, cojo a otro que, según su sobrino, midió un metro cincuenta y cinco y no uno cuarenta y cinco como usted afirma. Por cierto, me parece que usted, amigo Barrios, no siente simpatía alguna hacia Manuel Vallejo, quien para mí es, sin duda alguna, el mejor cantaor que ha dado la ciudad de la Giralda. Un artista que supo cantar a compás y que, incluso, utilizó en la simple ejecución de un fan- dango por soleá, todos los tonos musicales (cante bien distinto del arrastrao); por algo fue bautizado por Fernando Rodríguez Gómez, «El de Triana», como «ruiseño» de moderno». He dicho que, a mi parecer, usted no siente simpatía alguna por Vallejo, si bien puede interrogarme «¿A quién le fue simpático?». Y yo, bien a pesar mío, tendría que decirle que a muy pocos. Esta es la verdad ama\n\n[ENDING CONTEXT]\n\nde semana en el sótano del «Pinto», con Pastora, Pepe y Tomás, donde acudía para aprender fervientemente... Sepa, también y por último, que estos, igualmente, son mis cinco.\n\n(1) Libro número 1 de la COLECCION CANDIL; Jaén, 1981.\n\nManuel Yerga Lancharro\n\n(2) «El artista flamenco y la Seguridad Social», en ABC de Sevilla, jueves, 30 de enero de 1969.\n\nRECTIFICACION:\n\nEn el número 18 de «CANDIL» y dentro de la sección de Manuel Yerga, «Discografía (placas) de artistas flamencos», se consignaba como mariana «Con la Virgen del Pilar», cuando en realidad es una granaina lo grabado por Tomás Pabón.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Las alturas de Manuel Torre y Vallejo",
    "periodical": "candil",
    "issue_id": "1982-05",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1072,
    "article_char_count_full": 6245,
    "article_char_count_review": 3640,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombre"
      }
    ]
  }
]
```
