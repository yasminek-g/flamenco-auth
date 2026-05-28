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
    "article_id": "1986-05-6-right-artistas-flamencos-en-ceuta",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor: Francisco Vallecillo\n\nC euta goza de una rancia tradición flamen-\n\nca, pues aunque no haya sido tierra de artistas famosos, por ella han pasado e incluso han estado avecindadas figuras muy importantes que convivieron alrededor de un selecto grupo de aficionados, civiles y militares, que en cierto modo ejercieron un mecenzago meritorio que trascendió hasta los medios profesionales de Andalucía. En el siglo pasado y según testimonio del afamado costumbrista mala-gueño Serafín Estébanez Calderón, «El Solitario», ya existió un bailaor de flamenco apodado Parete de Ceuta, creador de un baile llamado El Pasuré. El desconocimiento del nombre de pila y apellidos de este artista nos ha impedido bucear en los registros civil o parroquiales en averiguación de algunos datos que hubieran podido\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"famili\"]\n\nya existió un bailaor de flamenco apodado Parete de Ceuta, creador de un baile llamado El Pasuré. El desconocimiento del nombre de pila y apellidos de este artista nos ha impedido bucear en los registros civil o parroquiales en averiguación de algunos datos que hubieran podido dar inicio a una investigación altamente apetecible y necesaria tanto en relación con Ceuta como con el baile —hoy desconocido— y obviamente con el personaje. De conocida familia ceutí nació el tocaor de guitarra Antonio Arenas, que en la década de los años 60/70 fue importante profesional que acompañó a las primeras figuras cantaoras (Mai-rena, Vallejo, etc.) y de quien existen numerosas grabaciones en acompañamiento de cantes. Antonio vive en Madrid, todavía activo. También en Ceuta, en el Pasaje del Recreo, nació Manuel Molina, El Encajero, gitano de padre y de madre, vendedora de telas y encajes, de dónde el apodo\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nabsolutamente perdida, Macandé, nacido en Cádiz, 1897-1947 adquirió popularidad por la manera personal, sensible y con garra, de cantar el fandango. Murió en el manicomio de Cádiz. tarrista que formó parte del grupo «Los Gaditanos» en el que cantaban El Chiquetete (Juan Pantoja, padre de la popular Isabel) y otros más. Manuel El Encajero, que murió prematuramente, fue el progrenitor del actual Manuel Molina, también nacido en Ceuta, compañero de arte y marido de la popular Lole. En Málaga, ya octogenario, todavía rueda por ventas y puntos de reunión flamenca, Juan El Africano, que aunque nacido en La Línea, vivió muchos años en Ceuta, de donde le proviene el apodo artístico. El siempre se declara ceutí y para hacerle confesar su lugar verídico de nacencia hay que forzarlo o poco menos. Compañera del bailarín Antonio y cabeza de su elenco de bailaora durante varios años fue la ceutí Carmen Rojas (Carmen Cárceles Escacena), coincidiendo con la época en la que Antonio Mairena dictó su inolvidable lección permanente de cante para bailar; ahora Carmen, en un plano más modesto que impuso el tiempo, prosigue sus actividades, ya más alejada del baile flamenco en espectáculos que se\n\n[ENDING CONTEXT]\n\nAntonio Mairena, también pasó temporadas en Ceuta desde una lejanísima Semana Santa que cantara saetas en un balcón de la calle Camoens hasta su último recital (en la Tertulia cuya fundación él inspirara) el año 1983, meses antes de su desaparición. El Maestro fue un admirador ferviente de Ceuta y la utilizó con frecuencia como refugio para el cultivo de sus inquietudes, para gozar a veces de sus alegrías y contentos y resignarse y consolarse en otras ocasiones de las amarguras y desilusiones que tampoco faltaron en su trayectoria humana y artística, plena de exquisita hiperestesia. tesia.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Artistas flamencos en Ceuta",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 1270,
    "article_char_count_full": 7830,
    "article_char_count_review": 3784,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "famili"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  },
  {
    "article_id": "1986-05-8-left-el-flamenco-afluente-del-r-o-de-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE n el número 3.280 del diario «El País», corres-\n\n3.280 del diario «El País», corres- pondiente al domingo 30 de marzo de 1986, el escritor Francisco Umbral, comete uno de los muchos desafueros e injusticias que, con demasiada frecuencia, se suele inflingir a nuestro amado mundo flamenco. Al referirse a Carios Saura y a su versión cinematográfica de «El amor brujo», Umbral afirma que el citado director, como otros de su generación: «Huyen de la historia, refugiándose en la estética del flamenco y sus tragedias pueriles». Realmente parece imposible condensar en tan pocas palabras tanta ignorancia y mala fe acerca de un tema sobre el cual el frívolo escritor de la «jet» madrileña parece desconocerlo todo. Queda por tanto claro que estas reflexiones no son una réplica a Umbral, que ya le\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nn serias dudas de que tan ocupado escritor haga un sitio entre sus múltiples y productivas actividades, y, saliendo de sus aristocráticos salones, de un paseo por las calles siempre frescas de este pueblo andaluz, para intentar comprender la hermosura sin tacha del flamenco. Más bien el presente trabajo intenta ser un toque de atención hacia todos aquellos que, sin llegar al cerril oscurantismo de Umbral, desconocen la viva presencia de nuestro arte en el contexto de la historia. Porque, en efecto, son dos las graves acusaciones contenidas en la desafortunada frase que comentábamos, que encierra además una importante omisión. Las acusaciones se refieren al alejamiento del fla- Vamos a proceder paso a paso, comenzando por lo más grave de todo, el falso supuesto de que refugiarse en el flamenco significa rehuir la historia, o lo que es igual, si sabemos leer bien, que nuestro arte no participa de las convulsiones e inquietudes de aquella. Ante tamaño disparate, creo que debemos reflexionar sobre cual debe ser el objetivo de la historia, ya que no parece aconsejable seguir considerando a ésta como un núcleo cerrado, en el que se narran las genealogías o las batallias de los reyes y los cuatro protagonistas más destacados, que provienen de las clases dominentes y que, a la postre, son los que pagan la redacción de las crónicas históricas; antes bien habría que reflexionar, a la manera de Bertolt Brecht, sobre quienes son los verdaderos protagonistas de la historia: «¿Quién construyó las siete puertas de Tebas? Los libros están llenos de los nombres de los reyes. ¿Fueron los reyes quienes levantaron los pesados bloques de piedra?» Evidentemente la respuesta sólo, puede ser negativa; ante ello, insistimos en que el objeto de la historia ha de ser otro que el de magnificar la conducta de la casi siempre deleznable nómina de figurones oficiales; de ahí que coincidamos con el\n\n[ENDING CONTEXT]\n\nla angustiosa situación existencial de un pueblo y unos individuos, a los que la historia precisamente parece negarles todo aquello que es imprescindible para una vida digna. Estética, pues, a través de una ética, como el profesor José María Valverde reclamó en una situación crítica en la historia de la España contemporánea. Lo contrario serían vanos adornos retóricos, y nada más lejos de eso que el flamenco, que, siglo a siglo, golpe a golpe, ha demostrado haber entregado su sangre en el diario holocausto que supone la fidelidad sin quiebras a la entraña del pueblo andaluz del que procede.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El flamenco, afluente del río de la historia",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 2191,
    "article_char_count_full": 13014,
    "article_char_count_review": 3521,
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
    "article_id": "1986-05-9-left-por-fin-flamencolog-a-y-flamenc-",
    "article_text_for_review": "Por: Francisco Vallecillo\n\nVoz del pueblo, voz del cielo. Y también, como es sabido, voz de la Real Academia Española. De ahí que no habíamos arriesgado nada cuando avanzamos nuestra creencia de que la bendita RAE terminaría por consagrar el vocablo flamencólogo y todos los derivados de flamencología. La última edición del Diccionario (1983/1984) consagra, de momento:\n\n—Flamencología: Conjunto de conocimientos, técnicas, etc., sobre el cante y baile flamencos.\n\n—Flamencólogo/ga: Dícese de la persona experta en las variantes del cante y baile flamencos.\n\nY al margen de esta salvedad que hacemos en el enunciado relativa a la posible transitoriedad y temporalidad en cuanto a recoger solamente estas dos voces, justo es reconocer que la Academia no ha pecado de manga ancha en esta ocasión y la carta de legitimidad concedida lo ha sido mesurada y ajustadamente. Veamos, para seguir el hilo de nuestro pensamiento, las definiciones que siguen:\n\n—logía: Elemento compositivo que entra pospuesto en la formación de algunas voces españolas con el significado de «discurso, doctrina, ciencia». (*)\n\n—logo/ga: Elemento compositivo que entra en la formación de algunas palabras españolas con el significado de «persona versada, conocedora, especialista» en lo que el primer elemento significa. Debe diferenciarse de aquellos cultismos que son griegos o latinos: CATAlogo, MONOlogo, DIALogo.\n\n(*) discurso: Ciencia o sabiduría (2. $ ^{a} $ aceptación).\n\nciencia: Conocimiento cierto de las cosas por sus principios y causas. Cuerpo de doctrina metódicamente formado y ordenado que constituye un ramo particular del humano saber. (El subrayado en la 1.ª aceptación es nuestro). A mi qué me importa que un rey me culpe si el pueblo es grande y me abona, voz del pueblo, voz del cielo...\n\nLetra Clásica de Mirabrás\n\nLa prudencia, pues, de la Real queda evidenciada cuando al difinir flamencología se limita al conocimiento, técnica, etc., huyendo (o no queriendo entrar) de/en discurso, doctrina y ciencia. Mientras que para explicar el concepto de flamencólogo, se atiene escrupulosamente al único significado del compositivo logo/ga, de tal modo que utiliza distintas pesas y medidas en el manejo de uno y otro elemento compositivo o sufijo. No quedará ya ninguna duda de que flamencología y famencólogo carecen —gracias a la semántica oficial— y definitivamente, de ese pretendido ropaje de sabiduría a nivel de doctrina y ciencia con el que se han pretendido arropar quiénes de estos modestos conocimientos (técnicas, variantes, etc.) pretendían establecer un linaje doctoral al que se accedía por la puerta falsa de la propia egolatría.\n\nAhora y para terminar, si queremos jugar un poco al divertido juego del significado de la linguística y sus combinaciones, veamos las siguientes definiciones oficiales:\n\n—Flamenco: tiene también como acepción la de achulado.\n\n—Flamenquería; equivale a calidad de flamenco, chulería.\n\n—Flamenquismo: afición a las costumbres flamencas o achuladas.\n\nDe donde se infiere que hemos de tener mucho tacto con los flamencos y las actitudes flamenquistas conducentes a la flamenquería, según por el sitio por el que se mire el Diccionario de la Real Academia Española o el vocablo del que se arranque. Que si está bien claro que flamenco no es solamente achulado, ni siquiera andaluz agitanado, resulta inadmisible que flamenquería y flamenquismo hayan de permanecer unidas al concepto de chulerías y cosas achuladas simplemente.\n\nY otra rareza que cae en el terreno de lo absurdo: flamenco tiene también esa isólita acepción, la de «profesional del baile flamenco». Del baile solamente. Envío: a Agustín Gómez, flamencó- a divinis.\n\nBar TOMAS APERITIVOS SELECTOS Especialidad en PLANCHA\n\nMesones, 18 Teléfono 23 40 46\n\nJ A E N",
    "title": "Por fin, flamencología y flamencólogo/ga",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "9-9",
    "page_number": 9,
    "word_count": 583,
    "article_char_count_full": 3757,
    "article_char_count_review": 3757,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-05-9-right-paco-de-luc-a-un-guitarrista-de-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas dicen:\n\nPor: Rafael Valera Espinosa\n\nM e encuentro a un Paco de Lucía con\n\nsemblante de preocupación. Quizás porque es reacio a las entrevistas. Quizás por la responsabilidad que conlleva el presentarse delante de un público amante del flamenco y además con el handicap de cierto periodo de relajamiento producido tras su gira por Europa. Compruebo y él matiza en la conversación que a continuación sigue, que el sentimiento de preocupación está reflejado en su cara por lo segundo. Un artista de la calidad de Paco de Lucía, sea el lugar que sea, ha de mantener las cotas de popularidad y buen hacer con su guitarra, como lo demostró en Jaén recientemente.\n\nMe pregunta si la entrevista requería mucho tiempo. Le dije que sí. Se volvió a mostrar preocupado porque con mi\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"afición\"]\n\na si la entrevista requería mucho tiempo. Le dije que sí. Se volvió a mostrar preocupado porque con mi requerimiento le hacía perder tiempo. Tiempo necesitado por él para llevar a cabo la puesta a punto de su mente, manos y grupo, así como instrumento y todo el montaje de la puesta en escena de su espectáculo. Accede con sencillez y me vuelve a rogar que sea breve. Y he aquí el diálogo mantenido: —¿Tu dedicación a la guitarra flamenca vino por afición propia o fue imposición de la afición paterna? —No, fue una imposición natural. Fue algo que se hacía en mi casa cuando yo nací. Mi padre, mi hermano... Yo soy el más joven de la familia. En- tonces, cuando tuve conciencia de que era un ser humano, ya tenía una guitarra en las manos. Ya sabía hacer compás, ya sabía tocar. —¿Era exigente tu padre contigo a la hora de trabajar con la guitarra? —¡Hombre, claro! A mi padre le debo yo lo que soy ahora mismo. Si no hubiera sido por mi padre que me obligó cuando era un niño... Uno cuando es un niño no tiene más afición que jugar, irte a la calle a darle a la pelota y al cachondeo. Mi padre fue el que me obligó a tocar la guitarra de chiquitito. —¿Qué recuerdos mantienes de los Chiquitos de Algeciras? —¡Uff!, han pasado muchas cosas ya. Pero bueno, ahí estaba empezando todo. Estaba en Algeciras co\n\n[ENDING CONTEXT]\n\ncuando Camarón estaba... Bueno sigue estando, porque Camarón es un fenómeno hasta que se muera. Pero cuando estaba más fuerte, estaba más jovencito y lleno de vida y de ilusión. Hemos pasado noches enteras de fiesta, pero muchas noches. Con Caracol, me acuerdo también una noche que estuvimos muy a gusto. Muchas veces, he pasao media vida de fiesta.\n\n—Se dice que en la actualidad Camarón es el Manuel Torre de antes, ¿qué opinas?\n\n—A mí que me perdonen Manuel Torre y los aficionados. No quiero perderle el respeto a nadie, para mí Camarón es el mejor que ha salido en toas las épocas.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen. Paco de Lucía, un guitarrista de concierto",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 1740,
    "article_char_count_full": 9518,
    "article_char_count_review": 2922,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "afición"
      }
    ]
  },
  {
    "article_id": "1986-05-11-left-flamenco-y-tradiciones-musicales",
    "article_text_for_review": "T enemos hoy una noticia que para nosotros\n\nes alegre, muy alegre, porque ya saben ustedes que con frecuencia, no demasiado, porque pocas veces se publican cosas de flamenco en periódicos diarios, como no sean los que tienen ya sección fija dedicada a ello y naturalmente quiénes escriben en esas secciones fijas suelen estar lo que se llama enteraos.\n\nSaben ustedes que con frecuencia, con relativa frecuencia, nos hemos quejado de las cosas raras que sobre flamenco se escribían y se publicaban en periódicos diarios y se escriben y se publican a veces.\n\nEn esta ocasión hemos de alegrarnos, porque en «El País» concretamente se ha publicado un artículo muy interesante de Francisco Vallecillo. Un artículo dedicado a Antonio Mairena con motivo del homenaje que se le ha rendido en el Círculo de Bellas Artes de Madrid.\n\nSaben ustedes que A. Mairena fue cantaor extraordinario en todos los sentidos: yo lo estimaba como cantaor, le estimaba como persona y le estimaba como amigo, y he tenido la suerte de estar en su casa varias veces, de hablar con él mucho, no todo lo que hubiera querido, y de recibir muchas lecciones de él, porque sabía mucho, y de estimarle realmente como merecía: no creo que como merecía, pero por lo menos como yo era capaz de estimarle.\n\nEn este artículo, Francisco Vallecillo abre un camino que hasta ahora no seguía nadie tratando de flamenco; y el camino es intentar buscar en el alma del intérprete como tal intérprete, pero más que como intérprete como recreador, como creador constante de los cantes.\n\nSaben ustedes, y es una cosa conocidísima, que el intérprete quiera o no quiera, muda el cante en cada ocasión que lo ejecuta. Eso depende de muchas circunstancias. Ya más difícil es que un intérprete conscientemente intente mejorar, cambiar, modificar esos cantes, según una línea que se ha trazado, una línea estética.\n\nEso es lo que hacía A. Mairena. Por eso creo que sería interesante y la idea ha venido precisamente leyendo ese artículo de Paco Vallecillo, sería interesantísimo comparar las distintas grabaciones del mismo cante, que ha\n\nPor: Arcadio Larrea (*)\n\nido haciendo en el tiempo A. Mairena, y ver qué es lo que había mudado en cada uno de ellos, porque así tendríamos una idea de su línea estética que es importantísima.\n\nY ese artículo nos hace notar todavía otra peculiaridad de A. Mairena que es su humildad. Quién lea sus memorias o lo que ha escrito como las memorias, o se ha presentado como tales, de A. Mairena, quizá saque la impresión de que es un hombre o que era un hombre vanidoso, aquí en cambio se investiga su terrible, su tremenda humildad.\n\nDense ustedes cuenta que todos sabemos cuáles eran las malagueñas de Chacón, que todos conocemos los Cantes de Marruno, unos cantes de quien fuera, de éste, de otro, el de más allá.\n\nSin embargo, no creo que nadie conozca unos que pueda llamar tientos de Mairena o soleares de Mairena o demás, porque Mairena que precisamente elabora esos cantes, se callaba y mantenía su humildad, su —digamos—anonimato, en esa elaboración constante y acrecía el acervo estético, el acervo musical de los cantes, sin que figurara él como tal. Eso demuestra a mi entender una humildad extraordinaria, es decir demuestra su total entrega al cante, que para él era lo más, es decir lo importante era el cante, no era Mairena con todo lo importante que era.\n\nYo quisiera dar la enhorabuena al amigo Vallecillo por este artículo, que creo fundamental si se quiere realmente ir entendiendo poco a poco lo que es el flamenco y como se ha ido produciendo.\n\nHubo un momento que creí que eso se haría en un libro que lleva varios años publicado que me engañó, creo que a muchos otros también, por su título —y no diré quién es el autor—, porque parecía que en él se iba a bucear precisamente en el alma del artista, pero no ha sido así, quedó una cosa superficial totalmente, aunque el título era muy ampuloso, y en cambio este artículo de Vallecillo, sí es fundamental, sí es básico, sí señala un nuevo camino que espero haya quien siga.\n\nYa digo que sería interesantísimo a raíz de eso, coger y, por ejemplo, comparar las distintas grabaciones de soleares que ha ido haciendo Antonio al paso de los años, las distintas grabaciones de las seguiriyes, las distintas grabaciones de los tientos y ¿por qué no? también las distintas grabaciones de las bulerías, porque Antonio era un real, enorme creador de cantes festeros. Lo sé, porque quizá eran los que salían en el momento en que afloraba toda su enorme humanidad, y al acabar los fines de fiesta, siempre era Antonio quién ponía en realidad la guinda con sus maravillosas bulerías.\n\n(*) A modo de homenaje al desaparecido gran investigador y musicólogo D. Arcadio de Larrea Palacín, me permitó reproducir el texto de una charla suya en enero de 1984, por las antenas de Radio Nacional de España Radio 2 (F.M.) en el programa que da título a estas columnas.\n\nPor la transcripción: Fernando Molina Arroyo (Córdoba).",
    "title": "Flamenco y tradiciones musicales",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 853,
    "article_char_count_full": 4954,
    "article_char_count_review": 4954,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
