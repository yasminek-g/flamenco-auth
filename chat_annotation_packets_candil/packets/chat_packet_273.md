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
    "article_id": "1993-05-11-left-emilio-gonz-lez-de-herv-s",
    "article_text_for_review": "La Soleá\n\nSólo tres volantes solos, con puntilla negra, tiene la bata blanca de Soledad.\n\nCabe en el mundo mayor equidad?\n\nSólo tres volantes solos; como los tres versos de la Soleá.\n\n¿Y pa qué mas?\n\nSi con tres versos volantes puede airearse la gran verdad.\n\n«Mira qué bonita era; como la espiga del trigo que trilla el trillo en la era.»\n\nSólo tres volantes solos marcan elegantes ritmos y desplantes de la Soleá.\n\n¡Y qué gran verdad! Sólo tres volantes solos, con puntilla negra, tiene la bata blanca de Soledad ¡Sólo tres volantes solos!... ...¡Como los tres versos de la Soleá!\n\nEmilio González de Hervás",
    "title": "Emilio González de Hervás",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 106,
    "article_char_count_full": 609,
    "article_char_count_review": 609,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-05-12-left-jos-menese-scott-rafael",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nY a la tercera fue la vencida. Gracias a los diplomáticos y cariñosos menesteres de dos enormes aficionados, como son Fernando Montoro y José Solís, el encuentro con José Menese se produjo al fin. Anteriormente, en mil novecientos ochenta y cuatro, durante el desarrollo del decimosegundo Congreso de Actividades Flamencas de Cáceres, los dos intentos fueron fallidos. El trajín congresual, las improvisadas reuniones, los imprevisibles compromisos y\n\n—Aunque te lo haya preguntado mil veces, háblanos de tus comienzos. demás aconteceres del Congreso, propiciaron el despiste horario de unos y otro para entablar el diálogo. El interés de Candil por dialogar con un artista de prestigiosa, dilatada y reconocida trayectoria artística, era manifiesto. La singularidad del arte de José, sus\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"famili\"]\n\n62 y grabando mi primer disco en el año 63, que fue un auténtico éxito. De ahí hasta aquí pienso que es de sobra conocida mi vida. Quiero decirte que lo que sí tengo claro es mi forma de cantar, lo que quiero en la vida, lo que le deseo a la gente, lo que le deseo al mundo... Esa es otra, es una utopía muy personal que yo tengo. —Hubo una tía de mi padre que se llamaba «Lola Crujera» y no «Lola de Lucena», como dice Fernando el de Triana, de la familia Crujera, que son una gente en la que cantan todos; es curioso, y cantan además muy bien, suenan muy gitano. Después aparezco yo, porque mi padre cantaba fatal, mi madre no conocía nada; pienso que he sido como algo caído del cielo. —Con Moreno Galván estuviste en el Concurso de Jerez del 72. —¡Sí, pero sólo a escuchar! En aquel concurso se presentaba un íntimo amigo nuestro, Alvarito, que cantaba como las verdaderas maravillas. Era en la época en que yo me iniciaba en todo este proceso del flamenco. Alvaro Trigueros era el nombre del amigo. —¿Qué recuerdos mantienes de tu estancia en Zambra, y por qué el pequeño homenaje que en tu último disco rindes a Rafael Romero? —Para mí, el homenaje es muy grande. En este puñetero país nadie hace nada por nadie y yo a Rafael le tengo un agradecimiento tremendamente enorme. Rafael conmigo se portó sinceramente bien, me ayudó mucho y a la hora de enjaretar los cantes, era un tío cabal, era un tío que sabía lo que quería y lo que debía ser. Recuerdo que una noche, en una reunión de amigos en el estudio de Moreno Galván, yo canté. Seguidamente Francisco le preguntó qué pensaba de mí, y Rafael contestó: «Figu\n\n[ENDING CONTEXT]\n\nfuturo tienen los festivales flamencos?\n\n—No lo sé. Yo, desde luego, haría una reestructuración de los festivales flamencos porque de ellos se nutren un montón de familias. Ahora, esos festivales multitudinarios que duran hasta las cuatro, cinco o seis de la mañana, me parecen horrorosos. Pienso que un espectáculo, sea de lo que sea, y de flamenco aún más, pues que dure tres o cuatro horas. Deberían reestructurarse, aunque yo no soy el más indicado; si pudiera lo haría.\n\n-¿Qué proyectos tienes para el futuro?\n\n—¿Proyectos, yo? Cantar bien esta noche en la pe- ña de Jaén. ¿Te parece bien?\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "José Menese Scott Rafael",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "11-13",
    "page_number": 11,
    "word_count": 2545,
    "article_char_count_full": 14112,
    "article_char_count_review": 3231,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "famili"
      }
    ]
  },
  {
    "article_id": "1993-05-14-left-el-lenguaje-de-la-cr-tica-flamen",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAgustín Gómez\n\nE1 gran empeño, por ser la gran dificultad, de la crítica flamenca, como de la flamencología, es traducir en palabras las vivencias y emociones producidas por el fenómeno flamenco. Claro que es la traducción en palabras de todas las vivencias que en el mundo son, la gran dificultad de la comunicación de sentimientos y conocimientos, pero si son de flamenco aún más.\n\nTodos los artistas tienen algo de monstruosos, porque los imaginamos hipersensibles para «sus cosas», los imaginamos unidimensionales y, al tiempo, incompletos de bagaje armónico para soportar el mundo en el que su arte los sitúa. Los artistas nos suelen parecer desequilibrados, con una moral distinta, disparatados o disparados emocionalmente... Puede que nada tengan de ello, que sean personas normales y que la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nonales y, al tiempo, incompletos de bagaje armónico para soportar el mundo en el que su arte los sitúa. Los artistas nos suelen parecer desequilibrados, con una moral distinta, disparatados o disparados emocionalmente... Puede que nada tengan de ello, que sean personas normales y que la cultura natural o adquirida corresponda a su grado de sociabilidad y cosmopolitismo. Pero si son artistas del flamenco, todas estas dudas se nos multiplican. La gran dificultad de la crítica flamenca o flamencología —decía— es hallar correspondencia u homologación entre la sensibilidad y la vida práctica, entre el arte y la cultura de andar por casa; tal vez, discernir entre cultura y subcultura... No sé si los estudiosos del arte tienen la misma dificultad: encontrar palabras suficientes (insisto en que la dificultad es para cualquier cosa, pero aquí se extrema). Hallar un lenguaje que abarque este arte humano del flamenco, para entenderse precisamente por los flamencos, debe ser nuestra máxima preocupación. En el ambiente cultural del Ateneo de Córdoba conseguimos hacer un par de temporadas una tertulia que resistió unas cuantas sesiones. Los profesores de música nos pedían precisión de lenguaje. Se da lo que se puede —les replicaba yo—, porque los flamencos tenemos nuestras claves de entendimiento, que en la mayoría de las ocasiones y conceptos va poco más allá de las manos y de los gestos. Lo que me molesta un poco es que a veces los profesores resuelven con un guiño de suficiencia o con el comentario breve que elude la refriega, por no rebajar la explicación a tonos menores, sin pensar que estamos los aficionados al flamenco haciendo un pino porque nos entiendan ellos que son absolutamente legos en nuestra materia. Y es que hay términos que no tienen el mismo significado para nuestro uso que para el lenguaje común de la música. Es lo que nos hace parecer unos pardillos. En el fondo es aquí donde radica la enorme dificultad para acercarse al flamenco, sobre todo cuando se quiere aprender en un cursillo intensivo. Mi máxima aspiración en estas tertuliaes e\n\n[ENDING CONTEXT]\n\népoca.\n\nEl lenguaje de la radio\n\nHe querido ofrecer un programa radiofónico de los que he acostumbrado a hacer durante treinta años en la COPE de Córdoba, por dar senci-\n\nllamente lo que tengo. No sé si he acertado a deciros que el lenguaje de la radio ha de apoyarse en el propio cante y en la propia guitarra, grabados o en directo. Pienso que es el medio más acertado para la divulgación de estas dos facetas flamencas, por su agilidad y poco coste, porque su vehículo es el propio sonido. Pienso que es el medio más idóneo para hacer crítica y no mostrador o mesa capilla de dimes y diretes.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El lenguaje de la crítica flamenca en los medios de comunicación",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "14-17",
    "page_number": 14,
    "word_count": 5328,
    "article_char_count_full": 31486,
    "article_char_count_review": 3699,
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
    "article_id": "1993-05-18-left-sinopsis-hist-rica-de-los-cantes",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJuan Ruipérez Vera\n\nDurante muchos años, y siempre al lado del Maestro Piñana, fuimos exponiendo a través de la palabra y en cuantos medios de difusión nos lo han permitido, el fundamento y el principio de los Cantes de Cartagena, porque sobre ellos la historia, afortunadamente, está escrita. Y está escrita así: a) Cartagena, desde su prehistoria, fue un emporio minero. Diódoro de Sículo (Libro V, cap. XXXII) dijo, refiriéndose a Cartagena, «que en tiempos muy antiguos unos pastores de la Iberia encendieron fuego en los montes y habiéndose propagado aquél a las espesas y opacas selvas que los cubrían, se extendió el incendio a casi toda la región montuosa. Perseveró el fuego largo tiempo, llegando a arder también la tierra, hasta fundirse y liquidarse el mineral argentífero que encerraba,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"pura\"]\n\nXII) dijo, refiriéndose a Cartagena, «que en tiempos muy antiguos unos pastores de la Iberia encendieron fuego en los montes y habiéndose propagado aquél a las espesas y opacas selvas que los cubrían, se extendió el incendio a casi toda la región montuosa. Perseveró el fuego largo tiempo, llegando a arder también la tierra, hasta fundirse y liquidarse el mineral argentífero que encerraba, el cual brotó a la superficie en forma de arroyo de plata pura...; los naturales del país desconocían de este metal... Los fenicios descubrieron aquella riqueza, adquiriendo la plata —a los indígenas— a cambio de objetos de escaso valor... En un segundo viaje, para llevarse la plata que aún quedaba, arrojaron el plomo que usaban para anclas y lo sustituyeron con plata...» (Aristóteles, Strabón, Polibio, Silio Itálico y Posidonio, sobre este tema, se refieren en términos parecidos). b) Tito Libio, en la «Historia de Roma», Libro III, cap. IV, dice «que solamente en una parte de la zona minera de Carthago-Espartaria, trabajaban para el Emperador 40.000 esclavos (aquí está cortado) del Llano del Beal, con sólo 100 esclavos producía diariamente 25.000 dracmas». c) Cuando la minería en Cartagena comenzó a tener importancia fue en plena época romana —así nos lo describe Eduardo Cañavate—. «Los iberos, conociendo la naturaleza del metal, comenzaron a explotar sus célebres minas... Los romanos se hicieron dueños de las minas, y a quienes se encargaban de dirigirlas les entregaban gran número de esclavos...; los esclavos, mientras proporcionaban ganancias increíbles a sus amos, agobiados ellos noche y día en las profundidades de las minas, sucumbrían con frecuencia al peso del excesivo trabajo. No existía para ellos remisión ni descanso; los capataces los obligaban con el látigo a sufrir las penalidades más terribles, y muchos morían miserablemente». d) En el prólogo del tomo I de «Memorias del Instituto Geológico» se describe lo siguiente: «En la de la costa merece figurar, en primer término, la de la sierra de Carta-gena, famosa desde los más remotos tiempos de la historia humana por su extraordinaria riqueza minera, aun cuando en realidad su renombre se deba, no precisamente a los minerales de hierro allí existentes, sino a la de plomo argentí-fero, que ha venido explotándose hasta nuestros días por los diferentes pueblos que oc\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_03 | trigger=\"tradición\"]\n\nineros. Herrerías, Portmán, el Garbanzal y Roche, solicitan la segregación del término municipal de Cartagena. Se accede a la petición. En el Ayuntamiento se conserva el documento en que se señala el Garbanzal como cabeza del nuevo municipio. Corre el día de San Silvestre de 1859. La primera sesión del nuevo Ayuntamiento se celebra al día siguiente bajo la presidencia del alcalde, don Antonio Sáez». h) Casal, como cronista, dejó constancia de la tradición de Cartagena a las manifestaciones folclóricas, así nos dice que «no fueron pocos en Cartagena los guitarristas y cantaores que florecieron y gozaron de merecida popularidad en el último tercio de la pasada centuria en que estaba, en todo su apogeo, el «cante jondo»... Los guitarristas más populares: Antonio Avila, Antonio Fuentes, Donato Miralles, Bartolo el de Oria (cantador de Marín), Juan Recules, Juan González, El Polizón, Castillo, el Alfr\n\n[ENDING CONTEXT]\n\nde Cartagena. El primer premio se lo llevará Curro Piñana (u otro cantaor que esté en la órbita del maestro, y que, además, será socio de la Peña Flamenca Trovera Antonio Piñana de Cartagena). Por último, entendemos como un grave error y una gran negligencia de la organización el di-luir a «La minera» (cante representativo de La Unión) con las «tarantillas» de Cartagena: La organización, con esta exigencia, acaba de extender el certificado de defunción de la «minera».\n\nSi esto no se cumple así es que el intento ha sido fallido, y, por supuesto nada habrá cambiado: La «Minera» vivirá. Amén.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sinopsis histórica de los cantes de Carta gena",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "18-20",
    "page_number": 18,
    "word_count": 3116,
    "article_char_count_full": 19163,
    "article_char_count_review": 4951,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "pura"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradición"
      }
    ]
  },
  {
    "article_id": "1993-05-21-right-alre-de-la-fiesta-gitana",
    "article_text_for_review": "Dibujos de Miguel Alcalá del libro «Le Flamenco et les gitans», Editorial Filipacchi, París, Francia, reproducidos bajo licencia del autor. Textos de Manuel Martin Martin 1.401\n\nTía Juana del Pipa.—Juana de los Reyes Valencia (Jerez de la Frontera, 1905 - Jerez de la Frontera, 1987). Hija del cantaor Luis el de la Mahora y apodada así por su primer marido Antonio el Pipa. Se hizo profesional a instancias de Antonio Mairena, quien la hizo debutar en Córdoba. Formando parte del elenco de «Las Viejas de Jerez», en unión de La Chicharrona, Luisa Torrás y La Tati, parecía sacada de una postal antigua. Moviendo con genialidad incomparable su más de cien kilos de peso, sus brazos dejaron en el lienzo de noches memorables todas las vicisitudes del alma gitana de mujer. Tía Juana armonizó en la esfera de su cuerpo todas las embestidas que la vida le reservó.\n\nLa Chicharrona.—Juana Jiménez Carpio (Jerez de la Frontera, 1908 - Jerez de la Frontera, 1989). Había nacido en la calle de la Sangre, en el mismísimo barrio gitano de Santiago, y contrajo matrimonio con Joselito Charamusquito, bailaor éste no profesional. Compartió escenarios y triunfos con La Pompi, Tía Anica la Piriñaca y Tía Juana la del Pipa, formando parte con esta última del grupo «Las Viejas de Jerez». Adquirió nombradía en los tablaos madrileños y destacó, principalmente, en el cante por saetas, Ha dejado herencia cantaora en su hijo Luis Loreto Jiménez, que despuntó en los «Jueves Flamencos», de Manuel Morao. Juana Fernández.—Juana Fernández de los Reyes (Jerez de la Frontera, 1955). Hija de Tía Juana y de su segundo marido, Juan Fernández o El Bizco Guzi. Es prima de La Mina y como ésta cantaora y bailaora del más rancio sabor. Intervino en la obra «Cantando la pena... la pena se olvida» (1986), con textos de Manuel Machado, y en el espectáculo «Flamenco, esa forma de vivir» (1980), de Manuel Morao. La actitud artística de Juana de Jerez constituye un escape espiritual que se concreta en un escape físico en el escenario, reaccionando ante los arrebatos academicistas con la sinceridad de su verdad y con un lenguaje desgarrado y directo de destreza singular.",
    "title": "Alreó de la fiesta gitana",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "20-23",
    "page_number": 20,
    "word_count": 361,
    "article_char_count_full": 2150,
    "article_char_count_review": 2150,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
