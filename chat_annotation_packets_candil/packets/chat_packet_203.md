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
    "article_id": "1989-11-19-left-entrevista-con-juan-casillas",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n-¿Dónde naciste?\n\n—Nació en el campo, entre Cuevas de San Marcos y Villanueva de Algaidas. Lo que pasaba es que los medios de comunicación eran mejores para Cuevas de San Marcos y por esto fui inscrito y así figura en mi carnet de identidad, en Cuevas de San Marcos. Pero yo siempre digo que soy de Villanueva de Algaidas.\n\n-¿Cómo surge la afición flamenca en ti?\n\ne como surge la afición flamenca en ti? —Surge de una manera muy natural porque la familia de mi padre, sin haber sido ninguno profesional, han sió todos muy buenos aficionados al flamenco. Mi abuelo cantaba; mi bisabuelo, según mis noticias creo que tocaba la guitarra y cantaba. Además, por aquella época poseía los discos de Chacón, Manuel Torre... De todos los cantaores que imperaban al comienzo de los años treinta. Sin embargo,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"públicamente\"]\n\namenca en ti? —Surge de una manera muy natural porque la familia de mi padre, sin haber sido ninguno profesional, han sió todos muy buenos aficionados al flamenco. Mi abuelo cantaba; mi bisabuelo, según mis noticias creo que tocaba la guitarra y cantaba. Además, por aquella época poseía los discos de Chacón, Manuel Torre... De todos los cantaores que imperaban al comienzo de los años treinta. Sin embargo, como antes decía, ninguno llegó a cantar públicamente. —¿Qué recuerdos mantienes de la forma de cantar de ellos? —Lo que más recuerdo con nitidez eran las reuniones familiares que se formaban cuando realizábamos las matanzas. Al finalizar de matar los cerdos, siempre salía una botellita de vino y a las dos de la tarde estaba toda la familia cantando. —¿Normalmente qué cantes desarrollaban? —En aquella época los cantes que más se escuchaban entonces. Recuerdo a un tío mío que hacía muy bien los cantes que realizaba el Niño de la Huerta. También hacía muy bien los cantes de Pastora, los cantes de Tomás... Así fue como comencé a oír el flamenco. Luego, con diez u once años, recuerdo cómo mi padre me llevaba a los festivales flamencos y en ellos escuchaba también a Chocolate, Terremoto, Fosforito, Menese y Antonio Mairena. A partir de ahí fue cuando yo comencé a formarme, primeram\n\n[ENDING CONTEXT]\n\nde los cantaores más importantes que ha tenió la historia flamenca. Yo pienso que Manuel Torre tuvo que cantar muy bien. También pienso que tuvo que ser un genio. Un hombre que en un momento dao te pegaba un pellizco y te hervía la sangre. Sin embargo, yo me quedo con Chacón, porque escuchando sus grabaciones lo veo más musical, más completo. Escuchas las grabaciones por siguiriyas de Manuel Torre y son para quitarse el sombrero, pero creo que Chacón fue un cantaor mejor que él.\n\n-¿Y la Niña de los Peines...?\n\n—¿Y la Nina de los Peines...? —¡Hombre! ¿Pastora? ¡Un mostruo! ¡Y su hermano Tomás!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Entrevista con Juan Casillas",
    "periodical": "candil",
    "issue_id": "1989-11",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1605,
    "article_char_count_full": 9058,
    "article_char_count_review": 2933,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "públicamente"
      }
    ]
  },
  {
    "article_id": "1989-11-21-right-una-revoluci-n-en-el-an-lisis-de",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPhilippe Donnier\n\n¿S e puede transcribir el cante? ¡Sí! Contesté de un modo algo atrevido a una pregunta que se me hizo en el último Congreso de Córdoba. Presentaba entonces un sistema de escritura musical inspirado del cante gregoriano; este sistema carecía de objetividad en la medida que dependía del oído del transcriptor.\n\nEste año puedo contestar ¡si! a la misma pregunta, pero con mucha más seguridad, gracias a las nuevas posibilidades de transcripción gráfica de la voz humana, ofrecidas por el analizador de sonidos llamado «Sonágrafo». La superioridad de los gráficos obtenidos reside en su total objetividad.\n\nEl sonido de una cuerda de guitarra está producido por el vaivén de la cuerda entre dos posiciones extremas, removiendo el aire periódicamente y produciendo así un sonido (fig.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"Según\"]\n\noz humana, ofrecidas por el analizador de sonidos llamado «Sonágrafo». La superioridad de los gráficos obtenidos reside en su total objetividad. El sonido de una cuerda de guitarra está producido por el vaivén de la cuerda entre dos posiciones extremas, removiendo el aire periódicamente y produciendo así un sonido (fig. 1). Antes de presentar ejemplos de transcripciones, tendré que empezar introduciendo algunos principios básicos de acústica. Según el grosor, la longitud y la tensión de la cuerda se producirá una brivación más o menos rápida. Se llama frecuencia (F), al número de idas y vueltas que puede dar la cuerda en un segundo. Si la frecuencia es grande (alta), el sonido percibido será agudo o alto (voz de mujer, violín, etc...), si la frecuencia es chica (baja) el sonido percibido será grave o bajo (voz de hombre, contrabajo, etc...). a) Relación entre altura de un sonido y frecuencia de vi-bración $ ^{(1)} $. I. Análisis de una melodía Un oído humano «normal» puede percibir frecuencias de 50 Hz. (sonidos muy graves), hasta 15.000 ó 20.000 Hz. (casi ultrasonidos). La frecuencia se mide en Hertzios (Hz): número de vibraciones por segundo. A cada no\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_01 | trigger=\"verdadera\"]\n\nun gráfico del segundo tipo (fig. 3), pues representa la variación continua de la frecuencia del sonido en Hertzios (su «altura»), en función del tiempo en segundos y eso a cada instante (fig. 5) $ ^{(2)} $. Podemos seguir en este gráfico muchos detalles de una melodía cantada A. subida, B: bajada, C: vibrato, D: quebrado de la voz, etc..., y esto con la altura objetiva y la duración exacta de cada acontecimiento melódico. El «Sonagrama» es una verdadera «fotografía» de la voz humana. Damos aquí un gráfico simplificado del primer tercio de una interpretación de la malaqueña de Baldomero Pacheco (fig. 6) $ ^{(3)} $. Las posibilidades analíticas ofrecidas por este sistema de «fotografía» de la voz son realmente muy grandes pero exponerlas rebasaría los límites de esta corta exposición introductiva. II. Análisis del timbre de la voz a) Estructura físico-acústica de un sonido complejo Hasta aho\n\n[ENDING CONTEXT]\n\nlaboratorio ER 165 del Museo del Hombre (CNRS), por Philippe DONNIER y Jean SCHWARTZ en febrero y junio de 1989, en el aparato KAY ELEMENTRICS CORP. Model 5500 signal analysis workstation (fig. 12).\n\nA modo de conclusión dejaré unos puntos suspensivos invitándoles a imaginar las posibilidades inmensas que ofrecen los potentísimos medios de análisis de la musicología moderna aplicada al flamenco...\n\n(1) A lo largo de esta exposición, se asimilará la voz humana al sonido producido por un instrumento de cuerda.\n\n(3) «Magna Antologia del Cante Flamenco», vol. XIII, cara B. 7, Niño de las Moras.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Una revolución en el análisis del cante flamenco: Las computadoras analizadoras de sonidos",
    "periodical": "candil",
    "issue_id": "1989-11",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1216,
    "article_char_count_full": 7318,
    "article_char_count_review": 3770,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "Según"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_01",
        "family": "AUTH",
        "trigger": "verdadera"
      }
    ]
  },
  {
    "article_id": "1989-11-22-right-el-flamenco-ha-muerto-viva-el-fl",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nComunicación a la Ponencia n.º 4: «¿Hacia dónde camina el Flamenco?», XVII Congreso de Actividades Flamencas, Jerez de la Frontera\n\nFotos de Antonio Mairena y «Camarón de la Isla» de José Eduardo Lamarca\n\nPido perdón a los señores Congresistas por el desenfadado título de mi comunicación, y también a los ingleses por apropiarme paródica, aunque respetuosamente, de su fórmula parlamentaria más trascendente. Con ello no pretendo nada más que hacer notar que el flamenco, surgido y aclimatado hace aproximadamente dos siglos, en unos ambientes distintos de los actuales, y sometido por tanto a módulos culturales que nada tienen que ver con los presentes, es un ilustre cadáver disecado por una actitud que tiene aún la suficiente sensibilidad para saber apreciar sus exquisitas esencias. Para ello\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"nuevos\"]\n\nente, de su fórmula parlamentaria más trascendente. Con ello no pretendo nada más que hacer notar que el flamenco, surgido y aclimatado hace aproximadamente dos siglos, en unos ambientes distintos de los actuales, y sometido por tanto a módulos culturales que nada tienen que ver con los presentes, es un ilustre cadáver disecado por una actitud que tiene aún la suficiente sensibilidad para saber apreciar sus exquisitas esencias. Para ello inventa nuevos formatos tecnológicos que rescaten aquellos tesoros a punto de extinguirse, edita libros en los que la nostalgia reviste a veces forma de lacrimógeno sepelio, y hasta organiza Congresos de Actividades Flamencas como el que hoy nos acoge. Sin embargo, considero que el flamenco hoy, poco o nada tiene que ver con el núcleo originario que lo motivara y, por lo tanto, la respuesta a la pregunta base de esta ponencia que se cuestiona: «¿Hacia dónde camina?» debe quedar en suspenso sin crispaciones ni grandes sobresaltos. Verán por qué. En unos cuantos puntos quiero resumir mi idea acerca de la diferencia entre aquel flamenco glorioso, aunque ya histórico, en relación con el que se nos avecina. 1.º) Nuestro arte, nacido en sociedades marginales, sobre todo (aunque no exclusivamente) en su vertiente gitana, y de extracción singular —por lo diferente— y rural, ha pasado a ser urbano, sin específicas señas de identidad que lo aparten de otros espectáculos masivos, y plenamente integrado en los engranajes de los circuitos comerciales, escaparán público, promoción personal de los intérpretes, etc. 2.º) Como consecuencia de todo ello, el sentido ritual, sacralizado, y hasta religioso que era inseparable de su esencia misma, en ocasiones se ha transformado en espectáculo, grandiosidad (en cantidad más que en calidad), perfeccionamiento artístico o simple derroche de facultades. Contemplar hoy en día a un gitano cantando alboreás en festivales y hasta en televisión creemos que ilustra suficientem\n\n[ENDING CONTEXT]\n\nUnos y otros se mirarán por encima del hombro, ya pasa en nuestros días, y esbozarán una media sonrisa de desprecio, pero ambos se asomarán de vez en cuando a echar una ojeada al portal del vecino, aunque no sea más que con la intención de enterarse qué pasa allí dentro; por eso los planos de evolución no serán nunca totalmente paralelos; alguna chispa saltará que incendiará de algún modo los materiales cercanos. Repasen la historia y verán cómo el fenómeno no es nuevo. Por todo ello, como en la vieja Inglaterra debemos reflexionar: El flamenco ha muerto, jviva el flamenco! Muchas gracias.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El flamenco ha muerto, ¡viva el flamenco!",
    "periodical": "candil",
    "issue_id": "1989-11",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1610,
    "article_char_count_full": 9887,
    "article_char_count_review": 3589,
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
    "article_id": "1989-11-23-right-noticiario-flamenco",
    "article_text_for_review": "Informe de los acuerdos tomados en la reunión de Junta Directiva de la Confederación Andaluzia de Peñas Flamencas, celebrada el pasado 4 y 5 de noviembre de 1989, en la ciudad de Huelva.\n\nReparto de subvenciones:\n\nEfectuada la distribución de la subvención a Federaciones año 89, las cantidades entregadas fueron las siguientes:\n\n2.º Fecha de inscripción de los cantaores, hasta el 31 de diciembre de 1989, a través de las peñas federadas de cada província.\n\n3.° Presentación a nivel provincial de las fases provinciales, cuando las respectivas federaciones lo estimen oportuno. 4.° Sábado, 24 de febrero de 1990. Se celebrará el mismo día en cada provincia,\n\n1.º Presentación en Sevilla, el viernes, 24 del presente mes de noviembre, de las bases y cartel anunciador, a las primeras autoridades de la Junta de Andalucía y medios de comunicación nacionales, especialistas en Flamenco.\n\nSubvención recibida de la Consejería de Cultura de la Junta de Andalucía: 16.000.000 de pesetas, proyectos año 1989, distribuidos a Confederación y Federaciones: 6.000.000 de pesetas y 10.000.000 de pesetas para la organización del I Concurso de Cante Flamenco de la Comunidad Autónoma Andaluzía.\n\nI Concurso de Cante Flamenco\n\nla final provincial, de ellas saldrá el can- taor que representará a cada una de las 8 federaciones.\n\n5.º Sábado, 24 de marzo de 1990. Gran final. La misma se celebrará en la ciudad de Córdoba por acuerdo unánime de la Junta Directiva de la Confederación. 6.º Las peñas quedan desde este momento autorizadas a recoger inscripciones de cantaores de su localidad o zonas limítrofes.\n\nBases\n\nPrimera: Podrán participar todos los cantaores de ambos sexos y mayores de 16 años que lo soliciten.\n\nSegunda: La inscripción la realizará el cantaor a través de una peña flamenca federada de su provincia, hasta el 31 de diciembre de 1989.\n\nTercera: El Concurso constará de dos fases: provincial y regional. La fase provincial comprenderá una fase selectiva y una final provincial, a celebrar el 24 de febrero de 1990. La fase regional consistirá en una gran final a celebrar en la ciudad de Córdoba, el 24 de marzo de 1990.\n\nCuarta: La selección de la fase provincial será efectuada por un jurado designado por las Peñas federadas y del que formará parte, al menos, un miembro de la Junta Directiva de la Federación Provincial. En la gran final, el jurado será designado por la Junta Directiva de la Confederación Andaluzia de Peñas Flamencas.\n\nQuinta: A los concursantes se les comunicará con la debida antelación, fecha y lugar de actuación.\n\nSexta: La organización pondrá, a disposición de los concursantes, guitarristas para su acompañamiento. No obstante, podrán presentarse con su propio acompañamiento, siendo los gastos de su exclusiva cuenta.\n\nSéptima: El fallo de los jurados de cada una de las fases será inapelable. Octava: Por el solo hecho de participar, los concursantes se comprometen a aceptar las presentes bases.\n\nNovena: La Organización se reserva el derecho de modificar o alterar cualquier punto de los expuestos, así como grabar o filmar la actuación de los concursantes para la mayor difusión del cante flamenco.\n\nDécima: Se establecen tres grupos de cante:\n\nGrupo 1.º: Soleares, siguiriγas, tonás, polos, cañas y serranas.\n\nGrupo 2.º: Bulerías, tangos, tientos y cantiñas.\n\nGrupo 3.º: Malagueñas, granaínas, fandangos y cantes de Levante.\n\nCada cantaor deberá interpretar dos cantes de cada grupo en la Fase Provincial y un cante de cada grupo en la Gran Final.\n\nDecimoprimera: Se establecen los siguientes premios:\n\nFase Provincial:\n\nGran final:\n\nConcurso patrocinado por la Consejería de Cultura de la Junta de Andalucía y organizado por la Confederación Andaluzía de Peñas Flamencas.",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1989-11",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 593,
    "article_char_count_full": 3715,
    "article_char_count_review": 3715,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-11-24-left-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLa Peña Cultural Flamenca «La Trilla», convoca su VIII Concurso de Cante Flamenco\n\nBases del Concurso:\n\n1.ª.—Podrán participar cuantos cantaores lo deseen, sin limitación de edad ni sexo.\n\n2.ª.—Las inscripciones habrán de hacerse en el domicilio social de la Peña, Avda. de Sevilla, número 6, o llamando al telefónó 395281, de 8 a 11 de la noche, o bien por correo, indicando nombre y apellidos del concursante, domicilio, nombre artístico si lo tuviere o teléfono donde pueda ser llamado.\n\n3.ª.—La selección previa para la final se celebrará los sábados que señalen oportunamente, siendo los concursantes avisados con la debida antelación y subvencionados con 1.000 pesetas de dieta.\n\n4.ª.—La Peña pone un guitarrista a disposición del concursante, si bien éste puede ser acompañado si lo desea por\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"interpretar\"]\n\ne pueda ser llamado. 3.ª.—La selección previa para la final se celebrará los sábados que señalen oportunamente, siendo los concursantes avisados con la debida antelación y subvencionados con 1.000 pesetas de dieta. 4.ª.—La Peña pone un guitarrista a disposición del concursante, si bien éste puede ser acompañado si lo desea por su guitarrista, siendo los gastos por cuenta del mismo. 5.ª.—Todos los concursantes, en la fase de selección, deberán interpretar un cante de cada uno de los grupos siguientes: A) Siguiriyas, serranas, livianas, tonás, martinetes, soleá, caña y polo. B) Tientos, tangos, alegrías, cantiñas, mirabrás, caracoles, romances, soleá por bulerías y bulerías. C) Malagueñas, granaínas, tarantos, peteneras, cartageneras, tarantas, mineras y fandangos. D) Un cante libre. 6. $ ^{a} $.—El concurso comenzará el sábado 6 de enero de 1990. 7.ª.—El plazo de inscripción finalizará el día 8 de febrero de 1990. 8.ª.—Los concursantes clasificados para la final, que se celebrará en el mes de MARZO, serán avisados con la debida antelación. 9. $ ^{a} $.—Premios: (Son otorgados por el Excmo. Ayuntamiento de Trebujena.) Para fomentar el cante de Trilla (no obligatorio) se establece un premio especial de 25.000 pesetas. Se establecerán tres accésits de 5.000 pesetas para los tres primeros aficionados locales, no finalistas. 10.ª.—Los premios no podrán ser declarados desiertos, excepto el cante de Trilla, y se hará entrega de ellos el mismo día de la final del concurso. 11.ª.—Si por alguna causa hubiera de ser alterado el orden del programa del Concurso, los participantes lo aceptarán a todos los efectos, ya que la inscripción en el mismo presupone la aceptación de las bases. 12. $ ^{a} $.—El fallo del jurado será inapelable. La Peña Flam\n\n[ENDING CONTEXT]\n\nextraordinaria, celebrada por esta Peña el día 1 de diciembre del presente año, fue elegida la nueva Junta Directiva.\n\nPeña Flamenca «El Quejío»\n\nEn reciente asamblea general de socios, la Peña Flamenca «El Quejío», de Ubeda (Jaén), eligió nuevo Presidente y éste a su vez nueva Junta Directiva, quedando la misma compuesta de los siguientes nombres:\n\nPeña Flamenca «Rincón del Cante»\n\nEn asamblea general de socios celebrada por la Peña Flamenca «Rincón del Cante», de Córdoba, el día 28 de octubre pasado, resultó nueva Junta Directiva que relacionamos para conocimiento de los aficionados:\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1989-11",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 1251,
    "article_char_count_full": 7942,
    "article_char_count_review": 3388,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "interpretar"
      }
    ]
  }
]
```
