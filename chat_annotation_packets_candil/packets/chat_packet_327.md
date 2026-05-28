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
    "article_id": "1996-11-23-right-ix-semana-cultural-la-gaviota",
    "article_text_for_review": "«La guitarra, tan sobria como rica, que áspera o dulcemente se adueña del espíritu y en la que se concentran los valores esenciales de otros nobles instrumentos cuya herencia recoge, sin perder su propio carácter, aquel que debe al pueblo por su origen...».\n\nPaco del Río\n\nPaco Cepero presentó el primer tema de su próximo disco\n\nA sí elogiaba Manuel de Falla la guitarra. Y ese elogio cobraba valor en la Peña «La Gaviota» escuchando a Paco Cepero. Era noche de homenaje y de distinciones, dentro de la IX Semana Cultural, dedicada al recuerdo de Manuel de Falla con motivo de cumplirse cincuenta años de su muerte, y que esa jornada estaba siendo protagonizada por el flamenco.\n\nEl jerezano Paco Cepero recibía la «Gaviota de Oro», que se la impuso el presidente de la entidad, Francisco Quintanilla. Se personificaba en la figura del guitarrista y compositor del «Mundo Flamenco» al que Falla dedicó lo mejor de su inspiración y creatividad. Pero esa\n\nnoche no estaba prevista la intervención del artista. Sí estaba programada la actuación de Juan Romero, que ofreció sus cantes acompañado a la guitarra por Manolo de Ceuta.\n\nLuego fue Manoli la de Gertrudis la que de manera singular dedicaba su actuación a Paco Cepero, quien sentado junto a su esposa en primera fila escuchaba sonriente y complacido la noche de cantes. Y entonces llegó el momento de la sorpresa.\n\nLa guitarra lloró... de alegría\n\nPaco Cepero se levantó de su asiento y nadie sabe de dónde salió su guitarra. Sin decir palabra se situó en el escenario y comenzó a acariciarla. La templaba con mimo, mientras en la sala se escuchaba un murmullo de sorpresa y expectación. Una vez la guitarra a gusto del artista, comenzó a sonar por derecho. Ya en la Peña reinaba el silencio más profundo. Se apagaron las luces del fondo. El acorde inicial sonó a piano de cola. Le siguió un trémolo sostenido haciendo sonar los bajos a compás, cerrando los ojos el artista, como queriendo buscar en la oscuridad de sus sentimientos las notas doradas que acompañadas por los duendes revolotearon la sala... Nadie decía nada. Era una música embrujada de flamenco y misterio. Los disonantes, los semitonos y los arpeggios alternaban con los apuntes por soleá, seguiriya o granaína. Era como una acumulación de arte en una composición sin nombre. De pronto, un fuerte rasgueado y el punto final. Aplauso espontáneo de todos los asistentes puestos en pie.\n\nEl artista, con su modestia habitual, cabeza humillada y respetuosa, dando las gracias. De nuevo sentado y otra vez al paraíso musical, en esta ocasión a la caleta gaditana. ¡Qué bien sonaron esos tangos! Era una música descriptiva a la que no hacía falta imágenes. Llena de compás, de arte. «Tangos caleteros». Lo vivió el público y lo agradeció con otro cálido aplauso y algunos piropos flamencos.\n\nY el cante de Juan Villar recordó viejos tiempos\n\nEn ese momento, Paco Cepero pidió a Juan Villar que subiera al escenario. Dijo que quería recordar aquellos viejos tiempos en los que ambos actuaban juntos. Se abrazaron y Juan se sentó a su lado y comenzaron por soleá. Luego por tangos y el final por bulerías. Era la apoteosis de la noche.\n\nLos espectadores, en su mayoría buenos aficionados —algunos de Jerez— no creían lo que escuchaban. Luis Guerrero, el padre de la profesora de baile, Carmen Guerrero, no paraba de decir «¡No se puede cantar mejor!». Lo repitió muchas veces. El guitarrista Manolo de Ceuta: «Hace años que no escuchaba a Juan Villar cantar así, como esta noche». El comentario por lo bajini era generalizado. Juan Villar dio la noche. Se le vio serio, entregado, adivinándose en él las ganas de recuperar tiempo. Por su parte, Paco Cépero le hizo un acompañamiento de gala, que fue interrumpido en varias ocasiones con aplausos del público. En resumen, una velada como la que hacía tiempo que no se disfrutaba en Cádiz. Ya era hora. Luego, antes de marcharnos, pasada ya la medianoche, saludando a los artistas, nos pudimos enterar que la primera interpretación de Paco Cépero, aún no tiene nombre y que será incluida en su próximo disco en el que ya está trabajando, por lo que se trató de un verdadero regalo de estreno.\n\nAmbos artistas salían esa madru- gada para varios lugares de Europa.",
    "title": "IX Semana Cultural “La Gaviota”",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 720,
    "article_char_count_full": 4226,
    "article_char_count_review": 4226,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-11-24-right-escuela-de-arte-flamenco-de-este",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA dmirados, ilustres miembros de este XXIV Congreso de Arte Flamenco, señoras y señores:\n\nEs de justicia comenzar agradeciendo esta oportunidad que nos brindáis, pues somos sabedores del carácter extraordinario o excepcional que la misma tiene; oportunidad para poneros en cumplido conocimiento —con toda la humildad pero también con todo el orgullo e irreductible voluntad— de un proyecto alumbrado desde Andalucía para Andalucía y el resto de España y el mundo.\n\nEste alumbramiento al que me refiero tiene lugar en nuestra Estepona, ciudad que, como ya habréis tenido noticia, experimenta una evolución importante en los últimos tiempos, de la mano de un grupo de profesionales no comprometidos politicamente, pero unidos en el empeño común de tender puentes hacia el progreso y el futuro de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"Escuela\"]\n\nto al que me refiero tiene lugar en nuestra Estepona, ciudad que, como ya habréis tenido noticia, experimenta una evolución importante en los últimos tiempos, de la mano de un grupo de profesionales no comprometidos politicamente, pero unidos en el empeño común de tender puentes hacia el progreso y el futuro de nuestra tierra. Como bien sabéis, Estepona es una ciudad de casi cuarenta mil ha- José Ignacio Crespo de Lucas expone el proyecto de la Escuela bitantes, ubicada en pleno corazón de la Costa del Sol, con veinticinco kilómetros de costa, una fuerte tradición agrícola y pesquera y un pujante y cada vez más relevante sector servicios —comercio y turismo— y un sector secundario o de la construcción, tradicionalmente intermitente y ahora de apogeo progresivo y continuo. Esos puentes tendidos hacia el progreso, en los albores del siglo XXI, deben propiciar un desarrollo sostenido, unas infraestructuras sólidas, no meramente coyunturales o de naturaleza especulativa, sentar las bases para un crecimiento rápido pero natural, innovador y audaz pero consistancial con nuestras raíces más profundas, sustantivo pero respetuoso con la calidad de vida y el medio ambiente; se trata en suma, de ir hacia el punto de inflexión equidistante entre un desarrollo, de controlado sufrido por otras poblaciones de nuestro entorno y la más agónica atonía económica, cultural y social, propia de los pueblos que pretenden vivir de espaldas al progreso. En este marco y desde esta óptica contemplamos el proyecto que hoy traemos ante vosotros, ilustres congresistas: la Escuela de Arte Flamenco de Estepona, que nace como puente hacia el progreso, pero también como mirada retrospectiva y mano tendida hacia nuestros más remotos orí\n\n[ENDING CONTEXT]\n\nel evento lo permite—, que el espectador pueda cenar antes, al tiempo o después del espectáculo.\n\nEste es, agrandes rasgos, el proyecto que queremos para nuestra ciudad y que aquí traemos para su conocimiento y consideración. Estamos seguros de que el apoyo de todos ustedes allanará el camino, no siempre fácil, hacia la sensibilización de las administraciones competentes, de modo que nuestro proyecto mañana sea una realidad. No quiero abusar de su amabilidad, concluyo esta intervención de la misma manera que la comenzaba, dándoles las gracias en nombre del pueblo de Estepona. Muchas gracias.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Escuela de Arte Flamenco de Estepona",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 1216,
    "article_char_count_full": 7656,
    "article_char_count_review": 3353,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "Escuela"
      }
    ]
  },
  {
    "article_id": "1996-11-26-left-noticiario-flamenco",
    "article_text_for_review": "“Nuestro Flamenco”, de RNE, cumple 12 años\n\nE1 programa «Nuestro Flamenco», que se emite por Radio Clásica, de Radio Nacional de España, con motivo de cumplir su décimosegundo aniversario, ofrecerá a sus oyentes el día 9 de noviembre, sábado, una edición especial en la que participarán como invitados José Menese, Pepe «Habichuela», «Chaquetón», José Mercé y Vicente Soto, junto a Adolfo Gross, director de Radio Clásica.\n\nNuestro Flamenco, que salió a las ondas en el otoño del 84, ha contado desde el primer momento con el respaldo de los oyentes y los profesionales: cantaores, guitarristas, bailaores, escritores y personas especializadas. La colaboración generosa e incondicional de todos ellos ha supuesto, a lo largo de estos doce años, una mano tendida, imprescindible, que ha arropado cálidamente al programa. Además, la propia emisora, la cadena de música clásica de RNE, ha avalado y sostenido la presencia del flamenco.\n\nPor el programa han pasado desde las máximas figuras hasta aquellos que comienzan, convirtiéndose en una referencia viva para el aficionado y en una tribuna abierta, donde se han expuesto las más diversas opiniones. Desde Sordera a Paco de Lucía, desde El Chato de la Isla a Enrique Morente, Manolo Sanlúcar, Vicente Amigo, Carmen Linares, Gerardo Núñez, Miguel Vargas, Paco Cepero, El Güito o Cristina Hoyos, sin olvidar a los críticos, investigadores y periodistas, y con un recuerdo entrañable a los ya desaparecidos Rafael Romero y Juan Varea, todos han alentado la intención de «Nuestro Flamenco», que siempre ha sido la de servir de puente entre los artistas y el público, difundiendo una música y explicándola, a través de los comentarios efectuados por sus autores. En el programa se incluyen las siguientes secciones: Entrevista», «Flamenco en papel de cartas» (buzón del oyente), «Grabaciones históricas», «De inspiración flamenca» (músicas inspiradas o surgidas por el flamenco), «Los poetas y el flamenco», además de presentación de discos, libros, publicaciones, noticias, etc.\n\nEscrito, dirigido y presentado por José Velázquez Gaztelu, «Nuestro Flamenco» se pone en antena por Radio Clásica, de RNE, los sábados, de 11 de la noche a 1 de la madrugada.",
    "title": "Noticiario Flamenco",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 345,
    "article_char_count_full": 2200,
    "article_char_count_review": 2200,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-01-3-left-pepe-marchena",
    "article_text_for_review": "En diciembre de 1976, concretamente en su día cuarto, fallecía en Sevilla José Tejada Martín «Pepe Marchena». Desaparecía uno de los artistas más famosos y polémicos de la historia del arte flamenco; mas no así una forma de entender, vivir y cantar la universal música andaluza, pues aún hoy la personalidad artística del sevillano se mantiene en candelero y arropada por una auténtica legión de seguidores.\n\nCierto es que sus detractores son también bastante numerosos y que siguen reiterando los rígidos criterios sobre su arte. Sin embargo, conforme transcurren los años y su obra se va conociendo en profundidad y muy especialmente sus primeras placas, el respeto por sus conocimientos y su singular manera de cantar, va creciendo entre los mismos.\n\nSi nuestros progenitores, a veces desconocedores pero entusiastas aficionados del pueblo llano, nos inducían a escuchar —como auténtico maestro— a Pepe Marchena, los que rozamos el medio siglo, sufrimos el impacto que supuso el lanzamiento de la antología de Hispavox y el desarrollo del I Concurso Nacional de Arte Flamenco de Córdoba —ambos acontecimientos precursores de otra forma de entender o escuchar el arte flamenco— y creemos que por tal motivo nos inclinamos a observar una determinada ortodoxia y al estudio de otras voces flamencas, en las que generalmente imperaron las de rajos gitanos, las cuales creemos que en diferentes ocasiones han estado encubiertas por la popularidad del cantaor sevillano y sus imitadores y seguidores. Nos podrían servir los ejemplos de Juan Talega, Tía Anica la Piriñaca o hasta el propio Tomás Pavón.\n\nAhora, la Diputación de Sevilla, para rememorar la figura de Pepe Marchena, ha editado una serie de grabaciones suyas que se realizaron en el decenio que va del año 24 al 34. ¡Cuántos matices sonoros flamencos conocidos se aprecian en las mismas! ¡Qué cantidad de evocaciones y escuelas de aquella época! ¡Con qué sabiduría y conocimientos cantaba el artista! Por todo ello, pensamos que Marchena consideró que si se\n\nguía por esos derroteros, llegaría a ser un preciado cantaor como los primeros de entonces, que no era ni es poca cosa. Pero él quería más: sobresalir como artista flamenco y como figura creadora. Su ímpetu, sus inquietudes y vena cantaora así se lo reclamaban. Y ciertamente que lo consiguió, estemos más o menos de acuerdo, pues su trayectoria ha estado sembrada de creatividad —aceptada o no—, al imponer otra visión de nuestro arte, al sumar otras voces y otra musicalidad, así como otros matices para desarrollar los estilos flamencos. Su escuela ha servido para incorporar a numerosos aficionados a este arte, los cuales han ido posteriormente descubriendo otras formas o maneras de cantar. Su figura ha sido emblemática en una época para gran número de los mismos, y sobre todo, su obra está impregnada de una inmensa creatividad, nos guste más o nos guste menos, o seamos partidarios o detractores de su escuela.",
    "title": "Pepe Marchena",
    "periodical": "candil",
    "issue_id": "1997-01",
    "year": 1997,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 477,
    "article_char_count_full": 2938,
    "article_char_count_review": 2938,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-01-3-right-el-cante-ante-la-melancol-a-y-el",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«Grandes eran mis duquelas...»\n\nGerhard Steingress\n\nHoy nos encontramos con un fenómeno paradójico en lo que a la interpretación del cante flamenco como fenómeno sociocultural andaluz se refiere: Por un lado existe una minoría de aquellos que, hace aproximadamente quince años, han reconstruído poco a poco el origen y carácter del flamenco en y para la vida cultural andaluz, apoyándose en los conocimientos y métodos científicos adecuados, mientras, por otro lado, nos vemos enfrentados a una mayoría absoluta de aquellos, llamémoslos, «los aficionados en general», que siguen creyendo y propagando el mito o la leyenda flamenca, ignorando o incluso rechazando los resultados más evidentes de la investigación científica. Mas esta postura de afición «ciega» también se refleja en la ideología de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"creativo\"]\n\nados a una mayoría absoluta de aquellos, llamémoslos, «los aficionados en general», que siguen creyendo y propagando el mito o la leyenda flamenca, ignorando o incluso rechazando los resultados más evidentes de la investigación científica. Mas esta postura de afición «ciega» también se refleja en la ideología de «pureza», que confunde el criterio artístico con la nostalgia o una visión de estética completamente aislada del desarrollo del proceso creativo como manifestación de los inevitables cambios culturales. Probablemente esta situación va a cambiar con el tiempo, pero hay que suponer que también tal fervoroso mantenimiento de la leyenda flamenca tendrá un significado y un papel dentro de lo que podríamos llamar la idiosincrasia andaluz. Y es este aspecto el que nos interesa especialmente en las presentes páginas. Observamos, por ejemplo, que el cante significa mucho más que arte para todos aquellos que lo consideran como expresión «natural» de un omino-so «ser andaluz». En el cante se busca y en el cante se encuentra, pues, una identidad étnicocultural de toda una región; o vamos a formularlo algo más cautelosamente: por lo menos esta manifestación artística se ofrece y se define de acuerdo como el llamado «ideal andaluz», concepción teórica e ideológica formulada por toda una generación de intelectuales con pensamientos profundamente enraizados en la idea del «ser andaluz» como valor étnico-regional, que tanta repercusión tenía en las polémicas acerca de una España integral a partir de finales del siglo XIX hasta el presente.¹ Como se advierte, el tema del flamenco abre todo un abanico de dimensiones culturales, artísticas, ideológicas e incluso políticas. Yo voy a reducir la complejidad del tema para precisar tres dimensiones\n\n[ENDING CONTEXT]\n\nla identidad étnico-cultural de toda una región.⁵³ 50) Sobre la etimología de este término, véanse STEINGRESS 1993, págs. 333-338.\n\n51) Véanse ibídem, págs. 318-332.\n\n52) El cante flamenco pues, no solamente «nació» como «bastardo cultural» sino gracias a su calidad asimiladora es capaz de ser asimilado por otras corrientes musicales y de influir de este modo en su desarrollo.\n\n53) Véanse GERHARD STEINGRESS: «¿Hacia dónde camina el flamenco?». En: XVII Congreso Nacional de Actividades Flamencas. Flamenco y Futuro. Ponencias y comunicaciones. Jerez de la Frontera (s.a.), págs. 105-116.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El cante ante la melancolía y el mito nacional",
    "periodical": "candil",
    "issue_id": "1997-01",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "3-11",
    "page_number": 3,
    "word_count": 9019,
    "article_char_count_full": 57340,
    "article_char_count_review": 3381,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "creativo"
      }
    ]
  }
]
```
