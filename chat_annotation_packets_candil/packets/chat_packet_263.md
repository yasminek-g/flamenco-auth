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
    "article_id": "1992-11-3-right-anselmo-gonz-lez-climent-o-el-si",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE 1 mundo del arte y de la cultura está sujeto a vaivenes. Hay siempre en él una especie de oleaje, de flujo y reflujo; movimientos pendulares que hacen posible que elementos ocultos, olvidados o ignorados afloren a la superficie. Los hombres hacen la cultura y el arte, y el concepto de inmortalidad aplicado al hombre y su obra es siempre relativo; fácil es de comprender por la propia naturaleza humana. A la larga, el hombre y su obra están condenados al olvido, que no otra cosa es la muerte. Pero, antes de llegar a ella tras de la desaparición física, queda una luz que se convierte paulatinamente en penumbra o, si es candela, en rescoldo que alumbra o calienta por mucho tiempo, el tiempo que pudo cobrar aquella dinámica generada por el impulso vital. En todo ese tiempo hay destellos,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nA la larga, el hombre y su obra están condenados al olvido, que no otra cosa es la muerte. Pero, antes de llegar a ella tras de la desaparición física, queda una luz que se convierte paulatinamente en penumbra o, si es candela, en rescoldo que alumbra o calienta por mucho tiempo, el tiempo que pudo cobrar aquella dinámica generada por el impulso vital. En todo ese tiempo hay destellos, incluso etapas encendidas y otras apagadas. La cultura y el arte, en su evolución, revalorizan o deprecian sucesivamente a los hombres; producen modas y, con ellas, a sus respectivos personajes. La estrella de Ricardo Molina brilla en plenitud, fue rutilante y casi en solitario desde su aparición. Aparecen ahora algunas otras estrellas en el firmamento de la flamencología. No debemos de entender las en competencia, ni mucho menos en lucha por conseguir una parcela mayor a costa de otras. Esto hay que advertirlo cuando no sabemos otra cosa que convocar concursos y echarnos a pelear con muy mal humor y peor estilo. Hasta ahora hemos sido demasiado primarios en la apreciación de la obra de un hombre; la hemos apreciado en la medida que hemos despreciado las demás o la de su oponente. Parece como si no fuéramos capaces de sumar esfuerzos, de apreciar la variedad humana, la complejidad del mundo; al contrario, parece que nos exigimos la simplicidad que sólo es posible en los espíritus puros. En otro orden, pero al socaire de la misma idea, a Fulanito se le acusa, o se le refiere con reticencia, por el hecho de compartir aficiones —pongamos por caso— al flamenco y al género lírico españolamente entendido como zarzuela. Parece que es suficiente rampa para deslizarle por ella al descrédito. Creo que se salva del peligro por su afán de separar ambos campos con jerarquías de valores diferentes; aunque también entiendo, como fuente de conocimiento, que pueden ser complementarios y enriquecedores. Y es que exigimos al flamenco vocación exclusiva o matrimonio fiel e indisoluble.\n\n[ENDING CONTEXT]\n\na Sevilla, la seriedad que no conoce Málaga, la honra que no resiste Cádiz, la plasticidad que no florece en Jaén, la tragedia integral que no vive el Levante andaluz, la majestuosidad que se fragiliza en Huelva, el realismo que no cuadra en Granada». Y luego añade esto tan terrible: «Pero Córdoba no puede unir a Andalucía. Sin narcisismo, vive para sí misma». Es curioso: El idea-lismo y el sentido de la estética mueve la flamencología de Anselmo González Climent para hacernos pensar; mientras que el realismo y el sentido práctico mueve la flamencología de Ricardo Molina para hacernos vivir.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Anselmo González Climent o el sino de un intelectual",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-8",
    "page_number": 3,
    "word_count": 6231,
    "article_char_count_full": 37636,
    "article_char_count_review": 3603,
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
    "article_id": "1992-11-9-right-percepciones-epistolares-sobre-l",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nT rasciende a lo que es nuestro actual propósito el ofrecer, en este trabajo, transcripción completa de la correspondencia que numerosísimos intelectuales y representantes conspicuos de las artes y las letras mantuvieron con Anselmo González Climent, la mayoría de los cuales testimoniaban así la admiración sentida por tal o cual obra del maestro argentino. La primera observación en la que coincidimos mi compañero Pedro Sánchez y yo, cuando en Mar del Plata tuvimos constancia de esta correspondencia, se refería a la heterogeneidad de los correspondales y, consecuentemente, a la desigualdad de los contenidos: Ricardo Molina, Edgar Neville, Gerardo Diego, Aurelio de Cádiz, Guillermo Díaz Plaja, Alvaro Domecq, Ramón Gómez de la Serna y un largo etcétera.\n\nEs evidente que el apunte\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nde interés. Como más adelante podrá constatarse, en las cartas que transcribimos, se producen apostillas, relatos anecódicos de la máxima importancia no tanto porque los mismos satisfagan una comprensible curiosidad por indagar en los aledaños del flamenco, sino por la valiosa información que aportan respecto del grado de valoración, investigación y receptividad del fenómeno flamenco en sectores intelectuales que, en algún sentido, lideraban el arte y el pensamiento de aquella sociedad. T al ha sido el primordial objetivo que ha alcanzado la correspondencia entre Ricardo Molina y Anselmo González Climent que el Ayuntamiento de Córdoba ha publicado en un libro de primorosa presentación sobre documentación aportada por esta revista («Cartas de Ricardo Molina a Anselmo González Climent». Demófilo. Córdoba, 1962). No es posible entender el entresijo de esta importantísima década, 1955-1965, así como el alcance y proyección de los Concursos Nacionales de Arte Flamenco de Córdoba, sin haber examinado tal correspondencia. Es cierto que sólo contamos con un único eje en ese intercambio epistolar,\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_03 | trigger=\"ejemplar\"]\n\na, hemos de distinguir los siguientes epígrafes: 1. Epistolario informativo, y 2. Antología epistolar tauro-flamenca. La denominación no es nuestra sino del propio González Climent que, tras mecanografiarlas, tenía así ordenadas las cartas recibidas. En el primer epígrafe el autor de Flamencología recoge las informaciones epistolares acerca de cantes y cantaores que se le remiten, a sus instancias y con ocasión de enviar a sus corresponsales un ejemplar de alguno de sus libros publicados. Creemos que resultará ilustrativa, dentro de este epígrafe la carta que Edgar Neville dirige a González Climent y que pese a su extensión no nos resistimos el transcribirla: Madrid, 7 de diciembre de 1961. Señor D. Anselmo González Climent. Buenos Aires. Mi querido amigo: Muchas gracias por su libro antológico. Creo que está usted contribuyendo de una manera muy eficaz a lo que pudiéramos llamar el pedestal sobre el cual levantar el monumento a todo lo que es flamenco. Todos los que nos hemos ocupado de este arte, y yo llevo ya cuarenta años ocupándome de él, hemos sentido el vacío de una base como la que está usted construyendo. Apenas hay libros del siglo pasado que se ocupen de ello, y los que hay son malos o se limitan a recopilar unas cuantas coplas que estaban de moda en su tiempo. Los mismos poetas que usted cita titulan de pronto «siguiriya» o «soleá» o «bulería» a un poema y luego lo escriben en romance. Yo todavía no he podido encontrar, ni en el mismo Manuel Machado, una «siguiriya» o una «soleá» que quepa en la rigurosa medida del cante. Ha habido también libros con biografías de cantaores célebres, pero han sido tan superficiales y tan flojos que n\n\n[ENDING CONTEXT]\n\ndistinción no es sino un producto de la fantasía de unos pocos, pues la realidad, históricamente comprobada, la niega: flamenco se llamó al gitano primeramente; el gitano se dio a cantar por precio, y lo que cantó se llamó flamenco porque el flamenco lo cantaba. Y, ¿qué era lo que cantaba? Precisamente y únicamente canto hondo (cante jondo): «segui-riyas», «soleares», «cañas» y «polos», que sólo se denominó entonces «cante flamenco». ¿Qué queda de la distinción absurda?\n\nPero me alargo demasiado y quiero concluir. Felicítole de nuevo y le estrecho cordialmente sus manos.\n\nManuel García Matos\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Percepciones epistolares sobre la obra de González Climent Ramón",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "9-13",
    "page_number": 9,
    "word_count": 4658,
    "article_char_count_full": 27903,
    "article_char_count_review": 4461,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "ejemplar"
      }
    ]
  },
  {
    "article_id": "1992-11-13-right-en-torno-a-anselmo",
    "article_text_for_review": "No creo que nadie hasta ahora haya escrito sobre el arte flamenco con la concienzuda profundidad, el minucioso detenimiento, la exigencia de matices y la claridad de ideas, libre de prejuicios, que aplicaba Anselmo González Climent a sus mejores estudios y críticas.\n\nEn la valoración escrita del flamenco que, paralela a la repristinación de este arte, se operó en España desde la década de los años cincuenta, ningún escritor ha llegado tan lejos como Anselmo en puntos a buenos rigores y sutilezas, tan distantes del estéril purismo a palo seco como de las concesiones a granel. Era y es cosa excelente leer unas páginas sobre flamenco tratadas como si de música sinfónica o alta literatura se estuviese hablando, y en este aspecto no sólo fue adelantado el escritor gaditano-argentino, sino que aún no ha sido superado en cuanto a afinamiento crítico y cuidados de juicio, no exentos, sin embargo de una salpicada y oportuna amenidad.\n\nLa andante flamencología le debe a Anselmo González Climent una nueva dimensión para la literatura flamenca, y este homenaje monográfico de «Can- dil» no puede ser más justo y merecido.\n\nFernando Quinones",
    "title": "En torno a Anselmo",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 187,
    "article_char_count_full": 1144,
    "article_char_count_review": 1144,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-11-14-left-para-anselmo",
    "article_text_for_review": "Fui a tu casa, a llenarme de fotos, libros, hojas, y hablar con tu mujer y con tu hija.\n\nFrente a la ventana que mira al mar la pena no olvidó la alegría de tu andaluza sangre, tu extraño miedo de salir más allá de las puertas, porque adentro hay un universo y afuera, la calle, es ruido demorado.\n\nHablé y hablé sin pausas, con vos, argentino de La Línea de Gibraltar, que sentía amor por las palabras, la valentía de los capotes, el morirse por algo que importara, la pesquisa de Dios.\n\nHermano mío, no me quejo por saber muy poco, pero sé quien sabe, únicamente la verdad.\n\nCuéntame algo despabilando orejas, con tu noble vozarrón de cante y sal.\n\nJulio Alvarez (Buenos Aires, 19 enero 1989)",
    "title": "Para Anselmo",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 128,
    "article_char_count_full": 694,
    "article_char_count_review": 694,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-11-14-right-homenaje-al-amigo",
    "article_text_for_review": "Anselmo, nacido en Buenos Aires, fue y es un gran argenti- no y un gran español, tímido, envuelto, inmerso en una An- dalucía que viajaba dentro de él como por casa propia.\n\nTrabajaba (como vándalo) con su máquina de escribir, diariamente, de 12 a 14 horas.\n\nSabía un rato muy largo, de música, historia, teología (que profundizara en los últimos años), literatura en general y española y latinoamericana en particular, arte y ensayo y de no sé cuánto más.\n\nHabía sido un buen deportista en barras y anillas y un apasionado «hincha» de fútbol (fue muy feliz con los dos campeonatos mundiales que ganara la Argentina).\n\nLe encantaba su hogar, como árabe antiguo; no salía mucho. Alguna vez lograron que comprara otra casa gracias a que, con un simple agujero en la pared medianera, pudieran pasar muebles, papeles y él mismo.\n\nSin embargo fue el gran caminador de Andalucía; con su oído alerta y con su grabador a cinta, logró hallar cantaores viejos y jóvenes, y con ellos toda la esencia de los cantes, decires, alegrías y penas (saja descarrarias) del Cante Jondo. Por él los argentinos, y me atrevo a decir que muchos españoles, de aquí y de allá, volvimos a comprender que detrás del grito está un alma que tiene cuerpo, deseos, fuerzas y debilidades, amor y odio, en una palabra: personas, humanidades que palpitan en medio de este siglo tantas veces soso, indiferente, distraído o suicida.\n\nAnselmo es mi hermano, pero también es mi puente ancho y firme con el sur de España.\n\nJulio Alvarez Mar del Plata, 4 de febrero de 1990",
    "title": "Homenaje al amigo",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 266,
    "article_char_count_full": 1532,
    "article_char_count_review": 1532,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
