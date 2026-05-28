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
    "article_id": "1987-11-4-right-importancia-de-las-letras-en-el-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPONENCIA PRESENTADA EN EL XV CONGRESO NACIONAL DE ACTIVIDADES FLAMENCAS, BENALMÁDENA, 1987\n\nJosé Luis Buendía López\n\nResiento que la rutina tiene la\n\nculpa de todo. Y ¿por qué no decirlo?, la decadencia en que este arte nuestro aparece sumido desde hace una buena porción de años. Alguien se preguntará a qué tipo de decadencia me refiero, cuando hoy en día los profesionales del flamenco viven mejor que nunca, han superado afortunadamente la etapa de las estrecheces económicas, y el arte jondo como fenómeno cultural, ha entrado en un estadio de normalización de todos sus cauces de difusión, esto es, se editan más libros que nunca, las grabaciones de los artistas tienen una mayor calidad y, por resumir a grandes rasgos, los contactos del profesional jondo con su público, jamás se han visto\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"experiencia\"]\n\nitan más libros que nunca, las grabaciones de los artistas tienen una mayor calidad y, por resumir a grandes rasgos, los contactos del profesional jondo con su público, jamás se han visto acompañados de mayor éxito económico. Y, sin embargo, yo me atrevo a hablar de decadencia y rutina. Para aclarar estos duros términos debo de desarrollar no sólo una explicación que los justifique, sino además una especie de personal confesión que explique una experiencia personal, que, aunque tenga este carácter, creo que responde a un sentimiento colectivo bas- La historia en cuestión es bien fácil de resumir: Un buen número de aficionados, entre los que se encuentra el que esto escribe, se vieron un día sorprendidos, no importan las circunstancias concretas, por un tipo de música y letra, Todos coinciden en señalar que el flamenco, a medida que rompía la intimidad familiar, iba dejando a un lado lo más puro de su autenticidad... tante arraigado en un determinado sector del consumidor de arte flamenco y, lo que es más importante, y hablo ahora en términos mercantiles, o si queremos de promoción de un producto, potencialmente susceptible de crear aficionados, esto es, hipotéticos clientes, futuros adictos, o como queramos llamarles de nuestro arte. que bellamente conjuntadas le conmocionaron hasta la raíz más profunda; en los tres o cuatro versos de\n\n[ENDING CONTEXT]\n\nsu venero al pie de las hogueras inquisitoriales y en la linde de las rutas del éxodo».\n\nNo, no consintamos que se pierda tan rico patrimonio. En ninguna otra expresión musical se toleraría a un buen profesional que cantara siempre la misma letra para acompañar su melodía. Mucho menos en este arte nuestro que por proceder de una cultura oral, transhumante y castigada por la acción implacable de la historia, no tiene otro sistema de transmisión que lanzar al aire los resortes más profundos de esa memoria colectiva. Ayudemos entre todos a recuperarla cuidando como un tesoro nuestras letras.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Importancia de las letras en el cante flamenco. Ponencia",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 1740,
    "article_char_count_full": 10417,
    "article_char_count_review": 2985,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "experiencia"
      }
    ]
  },
  {
    "article_id": "1987-11-6-left-poema-para-t-a-anica",
    "article_text_for_review": "Aunque el cante\n\nAunque el cante le haya trazado las venas en una inquietante leyenda es su voz un reguero de amapolas entre el ahogo ciclópeo que es a veces la biografía alargada. En un barrio de soles verticales una voz se hace bodega allá en donde la pena se sale. Y en cada torre se encarama una guitarra y en cada astro el rajo gitano de Tía Anica «La Piriñaca» en cuya cabeza nimba un ejército de faraones. La soleá, la seguiriya, la bulería... El Flamenco le abre y le cierra los ojos en una permanente vigilia por donde el duende baja a sentarse en el trono de su memoria. Y ahí está sentadita con la incontinencia del cante entre el claroscuro de su semblante cayéndole encima un rompimiento de gloria. En su numen la terca enredadera de pinchos y de aromas que le sube por el obelisco de su plateado cuerpo hasta sembrar sus entrañas de barruntos y amaneceres. ¡Tía Anica la Piriñaca! La imagino entre el susurro de los almanayques cantándole nanas al aire con su boca a sabor de lubricán. Hoy la tempestad de su cante sigue con la vigencia de las estrellas por más que su esfinge cuarteada quiera despintar el otoño florecido que lleva por dentro. Es su vida un altarito ambulante en donde se adora la Historia del Canti\n\nJesús Cuesta Arana",
    "title": "Poema para Tía Anica",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "6-6",
    "page_number": 6,
    "word_count": 229,
    "article_char_count_full": 1251,
    "article_char_count_review": 1251,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-11-6-right-el-cante-extreme-o-ponencia",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPONENCIA PRESENTADA EN EL XV CONGRESO NACIONAL DE ACTIVIDADES FLAMENCAS, BENALMÁDENA - 1987\n\na Federación de Entidades flamen-\n\ncas de Extremadura (F.E.F.E.X.) ha dedicado parte de los años 1986 y 87 a la organización y realización del I Concurso Nacional Cante Extremeño, en el que consiguieron los primeros premios.\n\nM. a de los Ángeles Salazar «La Kaita» y Alejandro Vega en Jaleos Extremeños; Domingo Rodríguez y «La Kaita» en Tangos Extremeños; Francisco Dávila en tres modalidades: Taranta de «Pepe El Molinero», Fandangos de Pérez de Guzmán y Fandangos de Manolo Fregenal; José Guerrero en Fandangos al estilo de «Porrina de Badajoz», y Prim Barquero en la modalidad de cantes libres.\n\nTodos ellos, junto a las guitarras extremeñas de David Silva y José A. Conde, y a la guitarra amiga de J.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"título\"]\n\návila en tres modalidades: Taranta de «Pepe El Molinero», Fandangos de Pérez de Guzmán y Fandangos de Manolo Fregenal; José Guerrero en Fandangos al estilo de «Porrina de Badajoz», y Prim Barquero en la modalidad de cantes libres. Todos ellos, junto a las guitarras extremeñas de David Silva y José A. Conde, y a la guitarra amiga de J. L. Postigo, han grabado un disco, bajo el patrocinio de la Excma. Diputación de Badajoz, que presentamos con el título de «Cantes Extremeños» y que supone el hasta ahora trabajo más importante de recuperación y difu- Francisco Zambrano Vázquez sión de nuestros cantes y de nuestros jóvenes valores que ha realizado la F.E.F.E.X. Por ello, nos parece muy oportuno adelantar en este XV Congreso Nacional de Actividades Fla- Pero, por otra parte, da fuerza ponerse a pensar la dificultad que entraña decir y también saber, dónde empieza y dónde termina Extremadura, o dónde termina y empieza Andalucía y desde cuán- Dificultad entraña decir y también saber, dónde empieza y termina Extremadura o dónde termina y empieza Andalucía y desde cuándo mencas, un breve ensayo del estudio de investigación que en torno a nuestros cantes extremenos estamos realizando. A veces, quizás pudiera parecernos un poco arriesgado, en un momento en el que, sin discutir el evidente origen andaluz de este arte, causa de nuestras pasiones, encuentros y estudios, se mezclan de una forma casi excluyente las palabras flamenco y andaluz, venir a plantar a Andalucía la paternidad para Extremadura de una serie de modalidades y estilo de nuestro cante flamenco. do; y también continúan reflexionando hasta qué punto guarda más relación en el flamenco y en las costumbres y en definitiva en la cultura: un andaluz de Sevilla o Huelva, con un extremeño de Badajoz que un andaluz de Almería con otro de Cádiz, o bien un murciano de Cartagena o La Unión con un andaluz de Jaén y ese mismo andaluz de Jaén, Granada o Almería con otro andaluz de Sevilla. Y todo esto sin olvidar puntos de encuentro y permanencia de maestros de nuestro arte, como ha sido y es Madrid y, por otra parte, la afición que día a día espontáneamente aflora en sitios alejados de la geografía sur e incluso fuera de nuestras fronteras. Por eso se me antoja que si es importante discutir en un congreso, por ejemplo, una supuesta Soleá del Charamusco, gitano anónimo en el flamenco, porque la «oportunidad» del gran maestro Antonio Mairena la encontrará en creados en torno a la PLAZA ALTA de Bad\n\n[ENDING CONTEXT]\n\nde RCA (nueva) por Manolo Cantarrana con la denominación de Aires de Fregenal en el apartado denominado «Cantes flamencos de la periferia andaluz», que pueden llevar a la confusión a quien no los conociere.\n\nTerminamos diciendo que todos estos cantes, como dije anteriormente, han sido recogidos en la interpretación de los jóvenes valores extremenos, que consiguieron los primeros premios del I CONCURSO NACIONAL DE CANTE EXTREMEÑO que recientemente hemos celebrado, en un disco-álbum que titulamos CANTES EXTREMEÑOS, y que ponemos a disposición de todos los aficionados a través de este Congreso.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El cante extremeño. Ponencia",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "6-9",
    "page_number": 6,
    "word_count": 2971,
    "article_char_count_full": 17658,
    "article_char_count_review": 4104,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "título"
      }
    ]
  },
  {
    "article_id": "1987-11-9-right-la-fundaci-n-andaluza-de-flamenc",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJ. A. Pérez Bustamante\n\nE ha llegado a mi poder un estético folleto ilustrado, editado por la Consejería de Cultura de la Junta de Andalucía, en colaboración con la Excma. Diputación Provincial de Cádiz y los Excmos. Ayuntamientos de Cádiz y Jerez de la Frontera, que suministra muy valiosa información acerca de la estructuración, fines y propósitos de la recién creada FUNDACION ANDALUZA DE FLAMENCO, cuya sede está ubicada en la histórica villa de Jerez de la Frontera, de rancia tradición cultural, folclórica y vitivínicola, que siempre ostentó categoría de primer rango en la génesis, evolución y recreación de los palos del cante jondo más bellos y más profundos. Vaya por delante la primera y más sincera felicitación a todos los miembros patrocinadores de tan acertada iniciativa, así como\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradición\"]\n\nor delante la primera y más sincera felicitación a todos los miembros patrocinadores de tan acertada iniciativa, así como por la elección de una ciudad para ella, que a lo largo de dos siglos largos ha sido prolífica cantera de las más ricas menas que tiene el mineral flamenco, sin que se advierta en la actualidad signo al gunó de que la potencia, o riqueza, de tales yacimientos corra el menor peligro de agotamiento, o extinción. La insuperable tradición de artistas de la talla de Juanelo de Jerez, de «El Loco Mateo», de Rosario «La Mejorana», de Antonia «La Gamba», de Curro Frijones, «Carito», la «Materna», la «Macarrona», la «Jeroma», Javier Molina, de don Antonio Chacón, de Manuel Torres (no Torre, según el erudito comentario de J. Blas Vega), del «Niño de Gloria», de los clanes de los «Agujeta», «El Borrico», Anita «La Piriñaca», los «Parrilla», «La Paquera», Fernando «Terremoto» y un larguísimo etcétera de artistas de excepción, cuyos «jipíos» y «ayes» han hecho crujir —y siguen haciéndolo— tantos esqueletos de buenos aficionados, han hecho llorar a tantos «cabales» en inolvidables madrugadas cárdenas jerezanas y han ocasionado tantas histéricas y emocionadas rasgaduras de camisas a muchos oyentes electrizados y poseídos por las atávicas garras de tanto duende fragüero, rezumando «sonios negros», justifica por sí sola la indiscutible excelencia de la elección de esta ciudad, a la vez industriosa y típica, que es Jerez, como sede y capital de tantos proyectos e iniciativ\n\n[ENDING CONTEXT]\n\nmodestas líneas no podemos hacer sino expresar la más cálida felicitación y mostrar nuestro más sincero agradecimiento por tal iniciativa, al tiempo que deseamos todo género de éxitos, aciertos, venturas y satisfacciones a todos sus promotores, en la seguridad de que el tiempo —juez único e insobornable de toda actividad humana— justificará la procedencia y justificación de su empeño, que, indudablemente, cristalizará en un revitalizador resurgimiento de todo tipo de actividades, que sean portadoras de un adjetivo tan común y tan controvertido, al mismo tiempo, como es todo lo «flamenco».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La fundación andaluzas de flamenco",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 1334,
    "article_char_count_full": 8796,
    "article_char_count_review": 3126,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradición"
      }
    ]
  },
  {
    "article_id": "1987-11-10-right-madrid-flamenco-corresponsal",
    "article_text_for_review": "Antonio Corcobado\n\nCORRESPONSAL\n\n■ n el suntuoso e incomparable marco comparable marco de los jardines Cecilio Rodríguez en el Retiro, y dentro del programa Veranos de la Villa, organizados por el Excmo. Ayuntamiento, entre los días 17 de julio al 15 de agosto, se ofrecieron a nuestro público y afición diez recitales de guitarra, baile y cante, cuidadosamente seleccionados, como puede verse en el programa que se acompaña que merecen nuestro aplauso y felicitación a la organización por su acierto. Si la hora para su celebración fue un acierto más al ser la adecuada para estos festivales mucho más en la época en que se han dado, no le fue a la zaga el suntuoso escenario escogido en el que el aroma y los efluvios de sus cuidadas plantas y fuentes aliviaron y aun estimularon nuestros sentidos para mejor predisponernos a disfrutar de tan buena selección y representación artística en la mínima parte en que pudimos presenciarla porque obligaciones profesionales y familiares, limitaron nuestra asistencia.\n\nOtra nota de mal gusto que se hace necesario resaltar es la comunicación del artista con el público en un tono y ambiente confianzudos que nada dice en favor de ninguna de las partes, especialmente cuando en el diálogo falta la más elemental de las cortesías. Al artista hay muchas maneras de agradecerle una buena actuación y éste, igualmente, puede corresponder de manera mucho más correcta y agradable de como viene siendo usual.\n\nLa guitarra ha tenido una magnífica representación por la indudable calidad de los artistas contratados, mostrándose, como reiteradamente vengo afirmando, en una progresión muy considerable sobre el momento actual del cante, posiblemente porque tenga más campo para la creación y la genialidad.\n\nEl baile ha tenido también una extensa y muy cuidada representación y ha debido resultar extraordinaria esa mezcla de juventud y solera que se intuye en el programa, lamentando el que no me fuera posible presenciar ninguna actuación.\n\nEn el cante, Miguel Vargas llegó al público con facilidad en su larga actuación y éste supo corresponderle con su calor y aceptación. El Chato de la Isla bajó muchos enteros en relación con su buena actuación en la Cumbre y sobre los demás artistas no puedo emitir juicio al no haberlos escuchado, aunque sigo manteniendo afirmaciones anteriores de que el cante se encuentra en un momento muy bajo a pesar de la gran y creciente afición que existe y en la que también se aprecia una muy deficiente orientación y formación.\n\nNuestro agradecimiento a la organización, es grande, porque grandes han sido las atenciones dispensadas a mi persona en representación de CANDIL, agradecimiento que como aficionado he de prolongar por su acierto en la elección de tan buen plantel de artistas en sus diferentes manifestaciones.\n\nLlama poderosamente la atención el hecho de que con el apoyo que nuestro Excmo. Ayuntamiento viene dispensando a las manifestaciones flamencas en nuestra capital, patrocinando recitales y festivales en sus diferentes distritos durante todo el año, haya regateado su aportación a la financiación del Congreso de Actividades Flamencas que este año debería haberse celebrado, haciéndolo fracasar.",
    "title": "Madrid flamenco CORRESPONSAL",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 510,
    "article_char_count_full": 3191,
    "article_char_count_review": 3191,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
