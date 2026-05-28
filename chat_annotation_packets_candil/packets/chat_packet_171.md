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
    "article_id": "1988-03-23-left-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNueva junta directiva de la Peña Flamenca «La Soleá» de Palma del Río\n\nEn asamblea general ordinaria cele-breada por la Peña Flamenca «La Soleá» de Palma del Río (Córdoba), el día 26 de febrero pasado, resultó elegida nueva junta directiva, quedando la misma compuesta de la siguiente forma: presidente, Gregorio González Rodríguez; vicepresidente, Francisco Morales Fuentes; secretario, Manuel Morales Pérez; tesorero, Juan González Cabrera; vocales, Antonio Carmona Losada, José Antonio Carrillo Carrillo, Manuel Caro Reyes, Manuel Ruiz Barrientos y Félix Lara López.\n\nDeseamos toda clase de aciertos a la nueva junta.\n\nNueva junta directiva de la Peña Flamenca de Córdoba\n\nEl día 29 de enero pasado celebró asamblea general la Peña Flamenca de Córdoba, en la que resultó elegida nueva junta\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nente: presidente, Francisco del Cid García; vicepresidente, Gonzalo Gongora Navarro; secretario, Antonio Gómez Palacios; vicesecretario, Fernando Saco Mellado; tesorero, Francisco Serrano Navarro; vocales, Francisco Dios Canalejo, José Cañete García, Marcelino Ferrero Márquez, Juan R. Martínez García, Juan L. Oliver Palomo, Juan Oporto Ruiz y José Rides Herrera. Deseamos toda clase de éxitos a los amigos de Córdoba. Nueva junta directiva en la Peña Flamenca «Los Cabales» de Madrid Nos comunica la Peña Flamenca «Los Cabales» de Madrid, la composición de la nueva junta directiva, la cual reproducimos a continuación: presidente, Juan Díaz Cañero; vicepresidente, Francisco Robledo Horgado; secretario, Sixto Herrera Gómez; vicesecretaria, Pilar López López; tesorero, Francisco Jiménez Matamoros; vocales, Francisco López, Lorenzo López López y Federico Cano García. Deseamos muchos éxitos a la nueva junta. La Tertulia Flamenca «El Viejo Agujetas» nombró nueva junta directiva El día 15 de febrero pasado tuvo lugar la reglamentaria asamblea general que anualmente celebra la Tertulia Flamenca «El Viejo Agujetas», en la que resultó elegida nueva junta directiva, quedando compuesta como sigue: presidente, Antonio Bergalo Castellano; vice-presidente, Arturo Miranda Gata; secretario, Juan Rizo Bernal; vicesecretario, José A. Bernal Bergalo; tesorero, Antonio Mateos Martin; vicetesorero, José A. Niño Peña; relaciones públicas, Rafael Bergalo Castellano; vocal de\n\n[ENDING CONTEXT]\n\nde Ministros de 21 de abril de 1932 y cuantas disposiciones se opongan a lo establecido en los artículos anteriores.\n\nCuanto antecede se divulga para conocimiento de los interesados en la organización de actos públicos en el sector de las actividades flamencas, dando a conocer la normativa copiada tanto a fines de solicitar el uso del apelativo «nacional» cuando así se desee, como para respetar la prohibición legal establecida para tal uso sin atenerse al precepto legal en vigor, de cuya vigilancia, con independencia de la que compete a las Autoridades Superiores, cuidará este Departamento.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1988-03",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 1762,
    "article_char_count_full": 11958,
    "article_char_count_review": 3090,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "1988-03-24-left-discograf-a-flamenca-placas",
    "article_text_for_review": "Por: Manuel Yerga",
    "title": "Discografía flamenca (Placas)",
    "periodical": "candil",
    "issue_id": "1988-03",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 3,
    "article_char_count_full": 17,
    "article_char_count_review": 17,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-05-3-right-editorial",
    "article_text_for_review": "Del 21 al 25 de junio pasado se ha celebrado, en Jerez de la Frontera, la Conferencia Internacional «Dos siglos de Flamenco», bajo los auspicios del Centro Español del Instituto Internacional de Teatro-Unesco y el Instituto Nacional de las Artes Escénicas y la Música, con la coordinación de la Fundación Andaluzía de Flamenco. Dicho acontecimiento merecerá la atención pormenorizada de CANDIL en la próxima entrega. No obstante, estimamos del máximo interés el realizar una valoración de urgencia de lo que, a nuestro juicio, ha significado esta Conferencia. Intuimos que este sucinto análisis no será, por suerte, pacífico entre los lectores de CANDIL que hayan asistido a estas jornadas de estudio. El tiempo quita o da la razón a las personas y a sus proyectos. Y a una perspectiva de tiempo más amplia nos remitimos, para que se enjuicien el acierto o desacierto de estas apresuradas apreciaciones. Pero vayamos al grano.\n\nDesde la óptica de la profundización analítica del fenómeno Flamenco, esta Conferencia ha significado el acontecimiento más relevante de la última década. La calidad de algunas de las ponencias presentadas, el rigor con que se han abordado aspectos del jondismo, hasta ahora ignotos, y, en general, la visión multidisciplinar que se ha ofrecido, justifican, a nuestro entender, tan comprometida afirmación, la cual, por otro lado, en términos de pura objetivización, requiere algunas precisiones.\n\nPrimera: la ausencia de posterior debate a la lectura de las ponencias, ha desvirtuado, por completo, el sentido, comúnmente admitido en otras áreas de la investigación, de esta Conferencia, cual es el de contrastar criterios, aquilatar análisis y, en cualquiera de los casos, enriquecer las tesis del ponente, ya sea por la vía de la reflexión colectiva o por la vía de admitir o inadmitir las objeciones que se le hubiese formulado. Así planteadas estas jornadas de estudio, podrían cumplir parecido cometido, con la sola publicación de los trabajos encomendados.\n\nSegunda: ausencia, por otro lado, de estudios musicológicos en sentido estricto. Sin esta aportación, no puede captarse integralmente el fenómeno flamenco.\n\nTercera: ausencia de material bibliográfico de apoyo, con la sola excepción de la fotocopia de las conferencias.\n\nCuarta y última, por ahora: poca brillantez de las comunicaciones artísticas ofrecidas en forma de festival, con la salvedad del espectáculo «Esa forma de vivir». Lo que no aludimos como dato accesorio, sino en el sentido de que toda indagación sobre flamenco debe nutrirse de la dimensión vivencial de este arte.\n\nEstas y otras precisiones que más adelante analizaremos, no han oscurecido la importancia de un acontecimiento, cuya vocación, por fortuna, es institucionalizarse, cada dos o tres años, según manifestaciones del propio consejero de Cultura. No nos cabe duda de que esta Conferencia, en ediciones sucesivas, puede convertirse en el foro más importante para la profundización y mejor defensa del arte flamenco.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1988-05",
    "year": 1988,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 463,
    "article_char_count_full": 2987,
    "article_char_count_review": 2987,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-05-4-right-la-uni-n-catedral-del-cante-mine",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Martín Martín\n\nA quisiera dejarme embaucar por la custodia de la Minera, por la queja desgarradora del Taranto, por las galerías y socavones que encierra la Taranta, por la melancolía de la Cartagenera y, en definitiva, como dijera Manuel Machado, por «Todo el Cante de las Minas»... Pero también siento la necesidad de rendir homenaje de gratitud a todos aquellos hombres y mujeres que lo hicieron posible, a los que dignificaron las fuentes de aguas torrenciales de este arte singular, de este venero de intemporales esencias como acercamiento popular a largos años de vida minera.\n\nEs por ello que convierto a CANDIL en el castillete minero desde donde pueda otear el mejor símbolo de La Nueva California, la fragua viva y protectora del cante donde se forjaron ecos misteriosos en unas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nde intemporales esencias como acercamiento popular a largos años de vida minera. Es por ello que convierto a CANDIL en el castillete minero desde donde pueda otear el mejor símbolo de La Nueva California, la fragua viva y protectora del cante donde se forjaron ecos misteriosos en unas gargantas que han servido de yunque para modelar el hostigamiento y la trayectoria del pueblo unionense. Lancemos, pues, las campanas al vuelo y convoquemos a la gran familia flamenca a Fiesta de Cante Grande, porque La Unión amamantada en la liturgia del cante, esa Unión aromada y apresada por el orgullo de una tradición histórica y cultural, se transforma del 7 al 14 de agosto en el lienzo donde puedan plasmarse el grito, la queja, el rasguear de la prima al bordón y la belleza plástica de nuestra danza más genuina. Por nuestra parte nos sentiríamos satisfechos con que nuestros cabales lectores supieran comprender que el Arte Flamenco, como sentimiento definidor del ser andaluz, tiene en La Unión un valedor insobornable, un protector que ha sabido durante veintisiete años hacer bueno aquello de que «de la buena siembra, más que de los propios cultivos, depende el resultado de su cosecha». Así canta Andalucía las penicas del cantar. Lo mismo que en Herrerías, igual que en el Garbanzal. Francisco de la Brecha Y en la memoria de los Cantes Mineros la ciudad murciana p\n\n[ENDING CONTEXT]\n\ncomo de idiotas, con perdón. Estos cantes no son propiedad de nadie, a lo sumo corresponden a toda esta zona y lo único cierto es que el Festival nació en La Unión y nada más.\n\n—Salvador Alcaraz, ponemos punto y final. Para concluir, ¿qué se va a encontrar el visitante del 7 al 14 de agosto en La Unión?\n\n—Se va a encontrar un pueblo en fiesta que vive toda una semana cultural de forma sorprendente. Un público muy generoso, excesivamente educado para el flamenco, que jamás adopta una posición contraria al artista y con una predisposición innata para acoger a todos los que vienen de fuera.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La Unión, Catedral del Cante Minero",
    "periodical": "candil",
    "issue_id": "1988-05",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 3268,
    "article_char_count_full": 19685,
    "article_char_count_review": 2988,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "1988-05-7-right-g-nesis-garc-a",
    "article_text_for_review": "Manuel Martín Martín\n\nConversar con Ginés Jorquera y/o Génesis García es todo un regalo para el oído. De ahí que en nuestra visita fugaz a La Unión fuera del todo necesario hacer una parada obligada en Cartagena. Allí encontramos la exquisitez por lo jondo, la tertulia amena y edificante, en definitiva, ese ambiente especial y distendido que, unido a una desmesurada educación, es indispensable para saborear hasta altas horas de la madrugada la espiritualidad que subyace en este arte.\n\nSentir por sus adentros la chorreda gravedad de una taranta, blasonar con altivez la profundidad de una minera, hablar acompasadamente a ritmo de taranto y pronunciar el nombre de Génesis García es una misma cosa. Génesis, docente, escritora, conferenciante y pregonera, parsimoniosa y aljibe de viejos saberes es, pues, una tratadista de lujo para desentrañar el siempre resbaladizo origen de los Cantes Mineros. Su claridad de ideas, su capacidad de síntesis y sus vastos conocimientos la hacen ser la persona idónea para desvelar los interrogantes que a diario se formula el lector de CANDIL.\n\n—Génesis, ¿Cantes Mineros, Cantes de Levante o Cantes de las Minas?\n\n—La denominación Cantes de Levante, en sentido geográfico, es más amplia y depende de dónde te sitúes. A la Sierra Minera ya se le llamó en Cartagena la Sierra de Levante, pero, hoy por hoy, consideramos Cantes de las Minas a aquéllos que guardan una relación directa con la temática minera. Esto no quiere decir que los cantes nazcan mineros, los cantes son mucho más antiguos que las minas.\n\n—¿Estamos de acuerdo en que el Taranto de Almería es el punto de arranque de estos cantes?\n\n—Sí. El Taranto es un cante básico y la Taranta es también un cante primitivo, pero más extrovertido y profesionalizado. Sí, yo estoy en la creencia de que el Taranto de Almería es el origen y la madre de la Minera.\n\n—La problemática social del minero queda reflejada de forma contundente en los Cantes de las Minas.\n\n—Ten en cuenta que el cante minero es el más socializado. El minero es el único cantaor que al mismo tiempo es sujeto del cuerpo social como trabajador, aunque la problemática de la minería incide en el cante, pero más ligada al trovo que a la parte profesional.\n\n—¿Podemos estimar como determinante la intervención del cantaor profesional andaluz y levantino?\n\n—Absolutamente. Lo que pasa es que, además, hay otra parte paralela y popular de expresión, pero son los profesionales los que han determinado el cante y lo han hecho obra de arte. No creo que haya flamenco sin profesionalización, sin que el término implique vivir exclusivamente de eso.\n\n—Sin embargo, hay nombres con cantes propios.\n\n—Sí, porque tenemos que distinguir entre lo que yo llamo el cante de los ancestros, Taranto y Taranta, y el cante de los maestros que serían las Cartageneras.\n\n—Aquí diferenciáis tres estilos de cartageneras.\n\n—Bueno, la cartagenera grande o de Chacón, la del Rojo el Alpargatero y lo que solemos llamar la cartagenera chica.\n\n—Por último, ¿qué épocas encontramos en la gestación de los Cantes Mineros?\n\n—Yo creo que hay una etapa popular de acarreo de materiales que situaríamos de memoria entre 1840 y 1880; otra de profesionalización con la llegada de El Rojo el Alpargatero y otra en la que coincide la crisis minera con la decadencia del cante, es decir, con la Opera Flamenca donde los Cantes de Levante se ponen muy de moda, porque son los que más se prestan a las veleidades operísticas. Posteriormente volveríamos a caer en desgracia o en el descrédito cuando se reivindican los cantes más primitivos hasta que de una forma anecódica, con aquello que ya sabes de Juan Valderrama, surge el Festival de La Unión gracias a las instituciones.",
    "title": "Génesis García",
    "periodical": "candil",
    "issue_id": "1988-05",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 620,
    "article_char_count_full": 3705,
    "article_char_count_review": 3705,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
