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
    "article_id": "1988-07-11-left-viejo-carn-flamenco-aurelio-de-c",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nos excelentes amigos de CANDIL han querido correr el riesgo de acogerme en sus columnas para iniciar una suerte de serial conteniendo apuntes flamencos al paso, tomados a partir de la década de los años 50. Se trata, sin mayor ambición, reflejar una intrahistoria de festivales y concursos varios que van desde 1949 a 1962 aproximadamente. En suma, datos reflotados de anotaciones casi taquigráficas. En todos los casos, datos indemnes (por lo mismo, con errores propios y ajenos), ofrecidos sin relación correctiva o comparativa con hechos posteriores. Esto explicará, entre otras cosas, la eclosión de muchas ingenuidades que la época misma inspiraba, y, sobre todo, al no alterar este material digamos literal, el dejar muchas veces en crudo a personajes que el tiempo posteriormente ha\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestro\"]\n\no posteriormente ha dignificado o empequeñecido. Debuto con la figura de Aurelio de Cádiz que desde 1978 me reclama el para mi joven amigo y colega José Blas Vega desde el prólogo de sus Conversaciones flamencas con Aurelio de Cádiz («...y para redondear este ciclo quedamos a la espera del ensayo sobre la personalidad y el cante de Aurelio, puede y debe hacer Anselmo González Climent»). En esta ocasión, repito, agavillo impresiones sueltas del maestro de Cádiz durante nuestra intensa y compañera misión de jurados del histórico e inolvidable Primer Concurso de Córdoba. Nació el 4 de noviembre de 1887 en donde debía ser: Cádiz. Mi documento de identidad reza: Aurelio Sellés Nomdedeu. He tenido 22 hermanos, la mayoría de ellos aficionados al cante, especialmente uno de los mayores. Sin embargo, mi primera ambiación fue la de ser torero a cualquier precio. Todavía, con mis venerables años, me sigue corroyendo el venenillo de la fiesta y todo lo que tiene que ver con ella, que no es poco ni aburrido. Llegué a capear por provincias y fui novillero, con el apodo de “El Gaditano”. Por fin, crucé el charco y lidié en casi todos los países hispanoamericanos con tradición taurina. Mis an\n\n[ENDING CONTEXT]\n\nremedio que seguir los vaivenes de la moda. Su caso es el que más lamento.\n\n—Uno de los aspectos que más estimo en el cante es la capacidad de producir “silencios”, dar cabida oportuna a “silencios oportunos”». (Se sobreentiende que el maestro alude a ciertos pasajes del cante donde aparece la situación-éxtasis, acaso el tárab.). «Primero siento la necesidad de dar un tirón; y llegado a la cúspide del abismo, hago una especie de “parón” torero, naciendo así el tope del silencio. Esto solía hacerlo muy bien un cantar ya prácticamente olvidado: el Corruco de Algeciras.\n\n(Mar del Plata, 1988)\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Viejo carné flamenco. Aurelio de Cádiz",
    "periodical": "candil",
    "issue_id": "1988-07",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1533,
    "article_char_count_full": 9043,
    "article_char_count_review": 2811,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestro"
      }
    ]
  },
  {
    "article_id": "1988-07-13-left-poemas",
    "article_text_for_review": "«A mí me está consumiendo como veintidós carbones veintidós años que tengo». Aquilino Duque. Soleares.\n\nEsos bucles pudieran ser la medida de todas las cosas, cuando ya no esperamos sino el desarme sideral o la caída de una estrella en el tablao donde Manuela no cesa de imponer su precario reino de gestos, enmascarados muchachos algunos, que no ignoran el paso del río por Sevilla o noches de duermevela en sus adelfas; cuerpos de sal tornan como girasoles en un enfurecido espacio de lánguidas miradas que rehúyen el sol de los lunares por miedo a abrasarse en sus mejillas, rotas como el tacón de un zapato.\n\nFrancisco Chica\n\nA Manuela Arana, mi madre, con el corazón roto en Cádiz.\n\nEn este mar de Cádiz entre la piedra flamenca las manos tectiformes de Enrique el Mellizo me trae la amargura de la muerte. Una presentida muerte que poco a poco se va bebiendo la brisa de los amaneceres. Y el fuego de la fragua de Donday se revuelca en la vaporosa memoria que va dejando espitas de desconsuelo porque mi madre se va al ladito de las estrellas en donde su pelo blanco en cortinajes de viento se mece entre las olas como un barquito de papel que solo, va navegando entre la nieve de la espuma. La tarde cae como una siguiriya como una flecha mordiente sobre la Caleta hasta repintar de rojo el aura romántica de esta pena que a la grupa llevo por estas orillas en donde los corceles son troncos de faraones y las sombras sonidos negros en este paisaje desolado, la bailarina Telethusa borracha de cante toma a mi madre de la mano y me deja un pellizco en la garganta, solo, cuando la tarde cae como una siguiriya. Jesús Cuesta Arana",
    "title": "Poemas",
    "periodical": "candil",
    "issue_id": "1988-07",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 296,
    "article_char_count_full": 1636,
    "article_char_count_review": 1636,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-07-13-right-ellos-los-protagonistas-dicen-la",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Martín Martín\n\nE $ ^{1} $I pasado cinco de agosto, en el XX Festival de Arte Flamenco de Pegalajar, se homenajeaba a la mujer cantaora en la persona de Dolores Jiménez Alcántara, «La Niña de la Puebla». Allí estuvo CANDIL, en La Charca, para em-paparnos de las cristalinas aguas de\n\nvuelta triunfal en la Maestranza de Sevilla, alcanzó sus mayores éxitos tras la incivil guerra y, desde su atalaya malagueña, supo ofrecer dulzura y luz a unos cantes que secundaron Montoya, Manolo de Badajoz, Sabicas, Niño Ricardo, Luis Yance, Antonio Delgado o Manolo Sanlúcar.\n\nTras unas gafas oscuras se esconde el secreto de los «campos de mi Andalucía». Cultura, quietud vivificadora y clarividencia ante las viejas cosas de una tierra añeja. Paradójicamente, enfrente todo sigue confuso.\n\nP.: Dolores,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nis Yance, Antonio Delgado o Manolo Sanlúcar. Tras unas gafas oscuras se esconde el secreto de los «campos de mi Andalucía». Cultura, quietud vivificadora y clarividencia ante las viejas cosas de una tierra añeja. Paradójicamente, enfrente todo sigue confuso. P.: Dolores, usted nace en una tierra donde existe devoción plena por el Flamenco. ¿Cuándo ocurría su nacimiento? R.: ¡Ojú! ¿Que cuándo fue? Tú lo que quieres es saber mi edad, ¿no? Bien, hombre, en 1908. P.: ¿Qué recuerda de aquellos años? R.: Hombre, yo he jugado mucho, he cantado mucho. Cantar siempre, siempre cantando, y jugar he jugado como las demás, sin vista ni ná, he correteado... Y después esta morisca a la que se rendía tri- buto. La que popularizara los cam- panilleros creados por Manuel Torre, debutó hace más de once lustros en el sevillano Salón Olim- pia, dio la alternativa a Juanito Valderrama, el de «El emigrante», impulso a Sabicas a que diera la otros recuerdos más buenos de cuando ya empecé a aprender, a leer, a escribir, estudié música, pero ya fue en Madrid. P.: ¿Recuerda el nombre de algunos de los cantaores de la Puebla de Cazalla? R.: Es que yo entonces no conocía a los cantaores. Cuando yo viene de Madrid entonces ya empecé a cantar, con 15 ó 16 años, y había uno que le decían «El Niño de Jerez», que era un buen aficionao, pero no era Manuel Torre, no, no, uno que en La Puebla le llamaban así. Había otro que también canta-ba muy bien que le decían «La Niña de los Peines», que era un fans de «La Niña de los Peines»... Había unos cuantos. P.: Con veintitrés años debuta en Sevilla. R.: Sí que me suena. Lo que pasa es que yo en La Puebla he estado poco. Una vez que empecé a cantar y empecé a ganar los concursos de por allí, a ganar premios y copas, entonces ya me f\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\nes para salir a cantar. P.: ☐ Y un tal Gallardo? R.: Eso fue porque ya las cosas suenan. Empezaron a decir que había una que llenaba el Salón Olimpia todas las noches... También hubo un espectáculo en el Teatro Duque y, en fin, llegaron a Madrid los rumores y me llamaron para grabar, que yo en Madrid grabé en un solo día en todas las casas de discos. P.: Y de Sevilla a Madrid. P.: ¿Qué cantes solía hacer en su etapa sevillana? R.: Yo empecé imitando a Marchena, cantaba mucho por Marchena. P.: Luego llegarían los campani- lleros. R.: Sí, mi padre fue quien me hizo la letra de los campanilleros. Cuando yo fui a grabar yo no los cantaba ante el público, y los canté allí ensayando en el estudio y entonces gustó mucho y me dijeron que los tenía que grabar. Ya tenía yo toda la grabación hecha, pero el director de la Casa Regal decía, ¡eso hay que grabarlo!, ¡eso hay que grabarlo!. P.: Uste\n\n[ENDING CONTEXT]\n\ngusta leer?\n\nR.: Sí, leo mucho, muchísimo. El artista flamenco tiene hoy que tener cultura para presentarse en los medios de comunicación, pero para cantar lo que hace falta es llevarlo dentro, la prueba está en que hay quien no sabe ni leer ni escribir y te levantan para arriba y te pegan un pellizco que dices ¡ojú!\n\nP.: ¿Hasta cuándo va a estar cantando la Niña de la Puebla?\n\nR.: Yo que sé. Yo ya no quiero, pero luego el gusanillo se rebela.\n\nP.: Pues que siga el gusanillo otros ochenta años.\n\nR.: Muchas gracias.\n\nAPERITIVOS SELECTOS Especialidad en PLANCHA\n\nMESONES, 18 TELF. 26 35 46 JAEN\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen: La niña de la Puebla",
    "periodical": "candil",
    "issue_id": "1988-07",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 2035,
    "article_char_count_full": 11108,
    "article_char_count_review": 4357,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombre"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      }
    ]
  },
  {
    "article_id": "1988-07-15-right-madrid-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAntonio Corcobado\n\nCORRESPONSAL\n\nUn verano más el Excmo. Ayuntamiento de Madrid, a través de su Área de Cultura y dentro del programa Veranos de la Villa, ha mostrado su interés por complacer a la aparente y creciente afición flamenca organizando, del 15 de julio al 20 de agosto, una serie de recitales de cante, baile y guitarra de indudable interés que, como todo, han podido ser mejorados, teniendo presente que por circunstancias que desconocemos y que nadie desde el Ministerio de Cultura ha tenido interés en explicar, se nos privó de la Cumbre Flamenca que durante las fiestas patronales de San Isidro se nos venía ofreciendo.\n\nUna vez más, también hemos de agradecer la atención que a CAN-DIL se le ha prestado por la organización invitando a su correspondal a presenciar estos recitales,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"crítica\"]\n\ndo, han podido ser mejorados, teniendo presente que por circunstancias que desconocemos y que nadie desde el Ministerio de Cultura ha tenido interés en explicar, se nos privó de la Cumbre Flamenca que durante las fiestas patronales de San Isidro se nos venía ofreciendo. Una vez más, también hemos de agradecer la atención que a CAN-DIL se le ha prestado por la organización invitando a su correspondal a presenciar estos recitales, cuya referencia crítica ha de ser forzosamente corta al limitar su opinión a las funciones presenciadas. Sin afán alguno de menoscabar los méritos artísticos de los concertistas de guitarra, que los tienen y en abundancia, hay que dejar constancia de que el público los tolera como paso obligado a las segundas partes. Chano Lobato, que conoce cuánto le quiere el público de esta capital, se presentó con una gran preocupación de quedar bien que mostró a lo largo de su actuación; él sabrá por qué, no estuvo ni mucho menos a la altura a que su saber y veteranía le obligaban. Mal de voz y asfixiado en algún final, no tuvo a lo largo de su actuación la equívoca brillantez que le haya podido suponer la tolerancia\n\n[ENDING CONTEXT]\n\ncostoso vestuario, nos encontramos con un bello espectáculo que gustó grandemente al numeroso público asistente que con sus insistentes aplausos requirió repetidamente la presencia de esta gran bailaora, a la que aconsejamos, si ello es posible, el reforzamiento de la parte cantaora, a nuestro juicio, la más débil de su magnífico espectáculo del que ella es eje y motor.\n\nA nuestra felicitación y aplauso unimos el deseo de que la fecundidad creadora de la ilusión que acumula en su sentir y maneras de interpretar el baile, nos siga deleitando con sus nuevas y siempre valiosísimas aportaciones.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Madrid Flamenco",
    "periodical": "candil",
    "issue_id": "1988-07",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1369,
    "article_char_count_full": 8350,
    "article_char_count_review": 2774,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "crítica"
      }
    ]
  },
  {
    "article_id": "1988-07-17-left-voces-del-futuro",
    "article_text_for_review": "R esulta ya reiterativo el afirmar que el flamenco constituye un legado inestimable de nuestro patrimonio cultural. Si bien, acaso, no se ha insistido demasiado en el carácter dinámico de este patrimonio, que no está muerto, mientras existen voces que por vía de la rememoración lo traigan a la realidad vivida, no sólo contemplada, y en algún sentido lo recreen. Quienes nos comprometimos en la profundización del fenómeno flamenco y en la salvaguarda de sus hermosas esencias, haríamos un flaco favor al mismo, si nuestros desvelos no mirasen al futuro. Y el futuro está en la voz, en el arte de los buenos aficionados que traeremos a esta página esperanzadora del mañana. En la voz de José Heredia «Joselete», gitano, nacido y criado en Linares, confluyen, como en las entrañas de su tierra, múltiples metales. Experto en determinados cantes de Levante —consiguió el primer premio de tarantas en La Unión—, se encuentra bien, según\n\nsus propias declaraciones, en palos tan básicos y gitanos como la sigui-riya y la soleá. De hecho, las fuentes de las que ha bebido, Manuel Torre, Antonio Mairena, Terremoto, Camarón, inducen a pensar en un cantaor serio y riguroso, lo que es cierto. A ello hemos de añadir su estricto sentido del compás que, ciertamente, se ha aquilatado por una estimable experiencia como cantaor para bailar. Todo lo cual no contradice su buena andadura por los cantes de su tierra que merced al esfuerzo de la Peña Cabrerillo de Linares y al Ayuntamiento se encuentran en fase de plena recuperación.\n\nSu mayor interés, en la actualidad, se centra en conseguir ayuda institucional para la grabación de su primer disco. Ojalá lo consiga pronto.\n\nEn cualquiera de los casos, la voz de «Joselete» comienza a figurar ya en carteles importantes y no nos cabe duda de que la suya es una de las auténticas voces del futuro.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nRoldán y Marín, 7\n\nTeléfono 22 97 65\n\nPágina 32 CANDIL\n\nInstituto de Estudios Giennenses. Candil : boletín de la Peña Flamenca de Jaén. N.º 58, 7/1988. Página 17",
    "title": "Voces del futuro",
    "periodical": "candil",
    "issue_id": "1988-07",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 357,
    "article_char_count_full": 2136,
    "article_char_count_review": 2136,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
