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
    "article_id": "1997-07-15-right-y-por-fin-el-t-tulo-universitari",
    "article_text_for_review": "José Guardia Rodréguez\n\nHace unos meses, en el número 87 de Candil, hice un largo análisis y crítica sobre los estudios de Flamenco en la Universidad de Granada. En aquella ocasión terminaba diciendo que la orla de la primera promoción ya estaba en la calle y vista. Y que estábamos a la espera de ver el tipo de título que se entregaba a esta primera promoción. Y concluía diciendo que cuando lo tengamos le haría al amigo lector una segunda entrega de esta historia y a ello, me dispongo:\n\nDenunciadas ya las vicisitudes por las que atravesó la denominada a lo largo de cinco años y, tras la espera de muchos meses, a sólo veinte alumnas se les ha hecho entrega de un título otorgado por el rector de la Universidad, en el que se da cuenta del contenido de los estudios realizados, las horas empleadas en ellos y el carácter del título que lo es de Grado Medio en baile flamenco y propio de la Universidad de Granada.\n\nEn la anterior ocasión, con motivo de la orla decía que, en ella, figuraban tanto el alumnado como los profesores que han llegado plagados de dificultades hasta el final. He tenido el orgullo y la suerte de conocer a las alumnas y, en verdad, son verdaderas flamencas y verdaderas universitarias. El primer paso está dado. Si la Universidad de Granada se siente incapaz de continuar tan digna tarea, puede que haya otras universidades que quieran intentarlo. Eso sería lo deseable con la idea de que, dentro de un tiempo, esa titulación media pueda ser homologada por varias universidades y reconocida como tal por parte de todos.\n\n“Sería un paso de gigante en el camino de la dignificación y desarrollo del arte flamenco...”. Pues bién, el título, recoge algunos datos importantes como las 900 horas lectivas de-sarrolladas; como que es título de grado medio en baile flamenco y que se atiene a las disposiciones legales vigentes. Gran importancia reviste éste reconocimiento oficial por parte de la Universidad de Granada del esfuerzo realizado por todos, pero fundamentalmente, por las alumnas. Ojalá cunda el ejemplo. Y ojalá las alumnas hagan valer su título con la dignidad que confiere ser una primera promoción. Primera y única por el momento que se da en una Universidad española y, quizá, del mundo. Lástima que hasta ahora sea eso y sólo eso, la primera y única promoción. Pero como digo, es a esas alumnas a quines corresponde hacer honor a sus esfuerzos y a la Universidad de Granada. Porque hoy se reconoce,\n\nen este título la materia universitaria que debe ser y que es el flamenco como fenómeno único entre los pueblos; lleno de música, de historia, de arte, de poesía popular genuína; digno de entrar por la puerta grande en ese gran universo de la antropología social y en el pleno ámbito cultural. Por ahora, así lo entienden bastantes de las alumnas. Varias de ellas tienen sus propias academias de baile; otras dirigen escuelas municipales en sus respectivos pueblos. Las hay que, como Mónica Bellido, alumna destacadísima que alternaba sus trabajos cada noche en el tablao de Mariquilla —la sala Neptuno de Granada— con otras tareas bien distintas como las de sacar adelante sus estudios de Filología española en la Facultad de Filosofía y Letras y, en la actualidad, estudiante de periodismo en Madrid. O como Paloma Clavero, profesora de árabe por las aulas de Málaga, o como Marina Sola, presidiendo la peña \"El Tacón\" de Motril. Las hay, en fin (perdonadme las no mencionadas) dignas acreedoras del primer esfuerzo. El flamenco hecho estudio universitario... y eso tenía que ser —una vez más— en Granada y en su Universidad. Demos la enhorabuena a estas cultas flamencas y que sepan que se espera mucho de ellas.",
    "title": "Y por fin el Título Universitario para el estudio del Flamenco",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 628,
    "article_char_count_full": 3659,
    "article_char_count_review": 3659,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-07-18-right-la-fascinaci-n-por-el-flamenco-e",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAmelina Correa Ramón 1 odas las tendencias culturales que desde el primer tercio del siglo XIX se obsesionaron por buscar aquello que constituía el \"alma del pueblo\", condujeron en Andalucía, de forma natural, al hallazgo literario de un gran filón espiritual: el flamenco, como expresión quinta-esenciada de las alegrías y las penas de todo un pueblo desde tiempos inmemoriales.\n\nEspecialmente, a los autores modernistas les atrajo la faceta de pasión dolorida, de tristeza, de desgarro que se contiene en el cante hondo y que es uno de sus elementos constitutivos. Así pues, fascinados por su carácter puro y visceral, numerosos escritores de fin de siglo fijaron su atención en aquél, convirtiéndolo en motivo de inspiración.\n\nFrancisco Villaespesa, Manuel Machado y sus “seguidillas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicaron\"]\n\ncarácter puro y visceral, numerosos escritores de fin de siglo fijaron su atención en aquél, convirtiéndolo en motivo de inspiración. Francisco Villaespesa, Manuel Machado y sus “seguidillas gitanas” Nos vamos a detener ahora en dos casos concretos de poetas andaluces que representan buenos ejemplos de la corriente modernista en España. Se trata del almeriense Francisco Villaespesa (1877-1936) y del sevillano Manuel Machado (1874-1947). Ambos publicaron, con una diferencia de dos años, una colección de “Seguidillas gitanas” con las que trataron de captar la esencia popular que una sabiduría de siglos encerraba en esa estrofa flamenca. Según el Diccionario de la Real Academia de la Lengua, la seguidilla gitana es una “copla andaluza, plañidera y sombría, que se compone por lo general de cuatro versos, los dos primeros y el último de seis sílabas y el tercero de once, dividido en dos hemistiquios de cinco y seis”. Así, Francisco Villaespesa recoge el esquema métrico básico de cuatro versos y lo reproduce sin excepciones a lo largo de treinta y una estrofas en la colección de “Seguid\n\n[ENDING CONTEXT]\n\nes el malo; el peor es aquel que sentimos sin poder llorarlo. (Francisco Villaespesa, pág. 865). Las que se publican no son grandes penas. Las que se callan y se llevan dentro son las verdaderas.\n\n(Manuel Machado, pág. 220).\n\nPorque así es, y así lo manifestó el genial poeta Antonio Machado, hermano menor de Manuel, cuando argumentaba que “las coplas no se escriben ; se cantan y se sienten, nacen del corazón, no de la inteligencia, y están más hechas de gritos que de palabras... Sólo la costumbre de llorar cantando, propia de nuestro pueblo, es capaz de encerrar tanta pena y tantos amores”.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La fascinación por el flamenco en los poetas modernistas andaluces",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1133,
    "article_char_count_full": 6830,
    "article_char_count_review": 2715,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicaron"
      }
    ]
  },
  {
    "article_id": "1997-07-20-left-ziryaby-mugaddan-al-qabri",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEn los primeros siglos de dominación musulmana en El-Andalus existía una composición lírica, la qasida, arábigo-hispana, poesía de origen arábigo de evidente conexión con Africa, que por su ascendencia se convirtió en culta y clásica.\n\nEvoca la gasida temas de la vida nómada en el desierto; comienza generalmente con la mención de los campamentos abandonados que se encuentran durante el viaje y ruega a sus compañeros para que se detengan a fin de cantar la vida errante de los que se han marchado.\n\nHasan Ali ben Nafi, $ ^{1} $ nació en 789 en Mesopotamia y era un liberto del califa Abbasi “Al Mahdir”.\n\nLa gasida empezó siendo breve, generalmente, rígida, monorrítmica, y se estaba manteniendo en El-Andalus más por su perfección formal que por la sinceridad de su temática, excepto cuando ésta\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"expresión\"]\n\ndurante el viaje y ruega a sus compañeros para que se detengan a fin de cantar la vida errante de los que se han marchado. Hasan Ali ben Nafi, $ ^{1} $ nació en 789 en Mesopotamia y era un liberto del califa Abbasi “Al Mahdir”. La gasida empezó siendo breve, generalmente, rígida, monorrítmica, y se estaba manteniendo en El-Andalus más por su perfección formal que por la sinceridad de su temática, excepto cuando ésta servía metafóricamente a la expresión exacerbada, que como tradición agarena, tenía mucho que ver con los “efebos”, fuera aquella pasión o no platónico. A continuación el poeta relata sus propios amores, viajes, afanes y fatigas para, finalmente, hacer un elogio del príncipe a quien está dedicado el poema. Pero estas primitivas gasidas fueron influidas por Ziryab desde su llegada a Fl-Andalus en 822. Abu-el- Fue discípulo del famoso músico y cantor, director del conservatorio o escuela de formación de artistas que existía en Bagdad, Isaq al Mawsili; su talento adquirió pronto notoriedad, por lo que el califa Harunn al Rashid, pidió al maestro que se lo llevara para conocerlo. Su éxito ante el soberano provocó la envidia y celos de Al-Mawsili; hasta el punto que t\n\n[ENDING CONTEXT]\n\nme faras bon, besa ma bokella eo se que non teiras.\n\nGracias amigo mío no me dejes sola guapo besa mi boquita ya sé que no te irás.\n\nDesmitificando a Ziryab, y magníficando como nos parece de justicia a aquel músico popular, el Ciego de Cabra, al que se le llamó en su tiempo Mugadan-al Qabri, cuyo recuerdo sí debemos conservar, pues él sí puede tener a nuestro juicio alguna participación en la creación del arte jondo-flamenco, por lo que podemos tal vez considerarlo como uno más de sus progenitores, o por lo menos impregnará en su genio los fandangos enraizados de la herencia de la mozarabía.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ziryab y Mugaddan-al Qabri",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "20-22",
    "page_number": 20,
    "word_count": 1855,
    "article_char_count_full": 11044,
    "article_char_count_review": 2825,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "expresión"
      }
    ]
  },
  {
    "article_id": "1997-07-22-right-flamenco-en-el-xlvi-festival-int",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDel 20 de junio al 6 de julio del presente año se celebró la cuadragésimosexta edición del Festival Internacional de Música y Danza de Granada, que una vez más contó con su, ya desde hace años, imprescindible programación flamenca, crecientemente solicitada por el heterogéneo público que el evento convoca. A guisa de acotación: no hemos de olvidar que, casi sistemáticamente, los espectáculos que tienen como protagonista al arte andaluz por excelencia consiguen llenos absolutos, sobre todo en los escenarios estrella del Festival, por ejemplo el anfiteatro del Generalife, bajo la luna y cercado de cipreses. En consonancia con la muy reciente y loable determinación de “sacar el Festival a la calle” —para escalofrío de incontaminados diletantes—, junto a esos escenarios de excepción a que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"nuevos\"]\n\ne tienen como protagonista al arte andaluz por excelencia consiguen llenos absolutos, sobre todo en los escenarios estrella del Festival, por ejemplo el anfiteatro del Generalife, bajo la luna y cercado de cipreses. En consonancia con la muy reciente y loable determinación de “sacar el Festival a la calle” —para escalofrío de incontaminados diletantes—, junto a esos escenarios de excepción a que acabamos de aludir, el flamenco también visitó los nuevos espacios habilitados con motivo del acontecimiento, verbigracia la azotea del Palacio de Exposiciones y Congresos; volvió a entornos más reducidos, pero ensolerados, singularmente cautivadores y de extraordinario valor cultural, caso del Corral del Carbón, monumento nazarí del si- glo XIV; y rehabilitó, en fin, ambientes que siempre han sido suyos: cuevas del Sacromonte, y carmen de la Peña Platería, en pleno Albayzin y dominado por la Alhambra, foros sagrados para auditorios de todo el mundo, donde los bien titulados trasnoches flamencos —continuaciones nocturnas de la fiesta, al finalizar las actuaciones estelares del Festival— tuvieron lugar de manera gratuita a lo largo de siete veladas. Por ese orden, pues, abordaremos los referidos momentos flamencos del encuentro musical granadino, en una rápida panorámica general. De entrada, y para enfocar el tema desde una perspectiva equilibrada, tal vez no sea ocioso advertir que una gran producción flamenca del Festival Internacional de Música y Danza es asignatura pendiente, que los distintos sectores administrativo y artístico harían bien en estudiar cuanto antes. Por otra parte, salta a la vista que los problemas que aquejan al conjunto de la muestra inciden proporcionalmente —\n\n[ENDING CONTEXT]\n\nescuela mairenera en estilos de la Serneta y Triana; su serie tarantera se compuso de dos cantes de Linares y el almeriense atribuido a Pedro el Morato; concluyó su primera salida por tangos. Ya en la segunda, por seguiri-yas, se acordó de Loco Mateo, el Torre y Cagancho, aunque muy escaso de facultades; más deficitario aún, se quedó sin fuelle al remate de una soleá por bulerías; guitarra y voz estuvieron muy desconcertadas por bulerías; y acabó la velada por tangos extremeños, como no podía ser menos, y también del Tate de Jerez y Sevilla-nos de la Niña de los Peines en versión lebrijanera.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Flamenco en el XLIVI Festival Internacional de Música y Danza de Granada",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "22-24",
    "page_number": 22,
    "word_count": 2954,
    "article_char_count_full": 18363,
    "article_char_count_review": 3328,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "nuevos"
      }
    ]
  },
  {
    "article_id": "1997-07-25-left-xxxiii-concurso-nacional-de-tara",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nUna ensombrecida cita con la raíz flamenca linarense\n\nComo viene siendo habitual, la tercera de agosto se está conformando como la semana preferida patronal de San Agustín, y todo gracias a la celebración del Concurso Nacional de Tarantas de Linares. Ante este acontecimiento siempre me viene a la mente la misma pregunta: ¿Por qué el certamen ha de configurarse sólo como un acto de enmarcación de la feria? No quiero restarle importancia a lo que de identificativo tiene para la tierra minera la Feria de San Agustín, mas sí deseo exponer —una vez más— cuál es mi teoría sobre lo desacertado de convocar para dichas fechas el desarrollo del evento flamenco.\n\nLa comarca de Linares, El Centenillo y La Carolina como tal minera, es igualmente la auténtica zona flamenca de la provincia de Jaén, al\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"reconocimiento\"]\n\na dichas fechas el desarrollo del evento flamenco. La comarca de Linares, El Centenillo y La Carolina como tal minera, es igualmente la auténtica zona flamenca de la provincia de Jaén, al margen de que determinados personalismos flamencos como los de José Yllanda, Rafael Romero (ambos de Andújar), Carmelo y José Revuelta (sendos hermanos de Quesada), El Fleta y El Calaco (de Jódar), Juan Valderrama (de Torredelcampo), etc., tengan igualmente su reconocimiento e importancia en la historia de este arte. Por tanto, como núcleo identificativo del flamenco jiennense con su cante por tarantas como buque insignia, habría que valorar la importancia que su promoción —del concurso— ha de tener para el enriquecimiento de nuestra historia en general, y de Linares en particular. El programar el acontecimiento tarantero como un acto más de una feria que comienza, es no concienciarse en resaltar nuestro patrimonio musical: Sólo hay que recordar un poco por encima, la cantidad de artistas flamencos que en Linares han nacido y podremos tener una de las nóminas más amplias de la historia del flamenco. Que músicos creativos como Luis el Pavo, Basilio, Frutos, El Pescaero, La Rubia de las Perlas, La Niña de Linares, El Cabrerillo, José la Luz, Personita, los Pucheretes, El Jorobao, Luisa Romero, Gabriel Moreno, Carmen Linares o Joselete, por citar los más populares, tengan anualmente un leve recordatorio, no es justo. Que un estilo propio, identificativo de una forma de ser y cantar, con todas las influencias que ha acarreado y acarrea, como es la taranta linarensé\n\n[ENDING CONTEXT]\n\npesar de sus años, desarrolló un ortodoxo recital flamenco que comenzó por polo rematado por soleá apolá, para seguidamente demostrar su conocimiento de los aires de la tierra y muy especialmente del “Tonto de Linares”. Sus fandangos pusieron el Ecuador de su participación, para seguidamente con carencia de facultades, mas con conocimiento, abordar las siguiriyas de El Nitri, El Marruro y Manuel Molina. Nuevamente los fandangos para evocar a Pepe Pinto, Caracol, El Peluso, Vallejo y su propia personalidad. Cerró su noche flamenca y la XXXIII edición del certamen tarantero con guajiras y caña.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "XXXIII Concurso Nacional de Tarantas de Linares",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 1616,
    "article_char_count_full": 9996,
    "article_char_count_review": 3207,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "reconocimiento"
      }
    ]
  }
]
```
