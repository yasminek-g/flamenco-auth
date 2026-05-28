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
    "article_id": "1985-07-14-right-ram-n-montoya-y-manolo-de-huelva",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor: J. A. Pérez-Bustamante\n\nL a Delegación de Cultura del Excmo. Ayuntamiento de Sevilla ha tenido el rotundo acierto de editar un álbum de dos discos, dedicado exclusivamente a la guitarra flamenca de concierto, de los cuales tres caras corresponden a la recopilación integral de todo lo mejor que grabó Ramón Montoya y una cara corresponde a grabaciones inéditas de Manolo de Huelva.\n\nEl presente álbum, grabado por la Corporación más arriba indicada para la «III Bienal de Arte Flamenco», constituye una inapreciable joya discográfica para todo buen aficionado a la guitarra flamenca y, además, incluye una excelente introducción y comentarios, a cargo de don Rodrigo de Zayas, en un folle-to de veinte páginas, ilustrado con numerosas fotografías, dibujos y caricaturas de ambos artistas, así\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"Voz\"]\n\nsepa, no existe grabación alguna previa de guitarra solista a cargo de Manuel Gómez Vélez («Manolo el de Huelva») y la cara que en este álbum aparece grabada tiene una curiosa historia que Rodrigo de Zayas relata minuciosamente en el folle-to que acompaña al álbum. Son, incluso escasos, los discos de pizarra en los que aparece grabado el toque de «Manolo de Huelva», acompañando a cantaores, de ellos quizás el último haya sido uno grabado en la «Voz de su Amo» acompañando a Canalejas de Puerto Real unas bulerías y unos fandangos, gra- Argentina y Ramón Montoya. Portada del programa presentado en París, sala Pleyel, el 28 de febrero de 1937. bado a principios de la década de los años cin- cuenta. Distinto es el caso de lo que este álbum recoge sobre Ramón Montoya, correspondiente a siete discos de 30 cm. de 78 r.p.m., de los que hay que diferenciar un bloque de seis (grabado en París en 1936 por la «Boíte à Musique», tras innúmeras vicisitudes, que relata en detalle Rodrigo de Zayas) y un séptimo, que contiene unas soleares a dos guitarras (con Amalio Cuenca) y unas siguiriyas gitanas, grabado por «Columbia», con gran calidad de grabación, y que en opinión de Arcadio Larrea constituye un auténtico y valioso incunable, del que no se tiraron más que medio centenar de copias, que no fueron puestas a la venta al público (ver «Guía del Flamenco», A. Larrea, Editora Nacional, 1975). El que suscribe tuvo la gran fortuna de poder adquirir una de estas copias en el año 1953, en la calle Cabeza, en Madrid, directamente de la mano de la viuda de Ramón Montoya, por el precio de 60 pesetas de las de entonces. Que yo sepa, no se ha realizado anteriormente ninguna grabación de este disco (que carece de registro numérico en su etiqueta) en microsurco hasta el momento de la\n\n[ENDING CONTEXT]\n\nrevista.\n\nEn resumen, el buen aficionado a la guitarra flamenca que desee escuchar cómo tocaban y lo que tocaban los más destacados tocaores hace medio siglo, dispone ahora de una opor-\n\ntunidad única de escuchar unas grabaciones auténticamente históricas por espacio de una hora, gracias a esta encomiable iniciativa del Ayuntamiento de Sevilla, editada por la empresa «Dial Discos, S. A.», con el número de registro 54 9317-18. Muy acertadamente, el título de este álbum insustituible, reza: «CONCIERTO DE ARTE CLASICO FLAMENCO», porque lo que en él se contiene, además de flamenco, ya es clásico.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ramón Montoya y Manolo de Huelva, mano a mano",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1392,
    "article_char_count_full": 8618,
    "article_char_count_review": 3409,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "Voz"
      }
    ]
  },
  {
    "article_id": "1985-07-16-left-guitarristas-compa-eros-y-amigos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(Imagen y anécdota)\n\nⅢ\n\nQue Ricardo se llamara Manuel —y algu- na vez Manolo el Carbonero— es algo que ignoraba la inmensa mayoría de la afición antes de que el flamenco comenzara a ser mot- ivo de interés literario. A pesar de que Pasto-\n\nSiendo muy joven, empezó como segundo guitarrista en el Salón Variedades, de Sevilla, y de acompañante para el cuadro flamenco que dirigía el notabilísimo estilista Antonio Moreno.\n\nra lo jaleara apasionadamente por su nombre de pila, en algunos discos, a Manuel Serrapi sólo se le conocía por Ricardo. Después, ya quedó biográficamente aclarado que Ricardo era su padre (es por lo que siempre será bueno escribir de las cosas buenas).\n\nPara recordar, aunque superficial y lacónicamente, a este completo y largo maestro de la guitarra flamenca, yo al menos,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"alma\"]\n\na el notabilísimo estilista Antonio Moreno. ra lo jaleara apasionadamente por su nombre de pila, en algunos discos, a Manuel Serrapi sólo se le conocía por Ricardo. Después, ya quedó biográficamente aclarado que Ricardo era su padre (es por lo que siempre será bueno escribir de las cosas buenas). Para recordar, aunque superficial y lacónicamente, a este completo y largo maestro de la guitarra flamenca, yo al menos, he sometido, una vez más, mi alma y mis conocimientos a la delicada y deliciosa disciplina de escucharlo con la más despierta minuciosidad. Ricardo sigue vivo a través de su guitarra en innumerables grabaciones, y yo, sin sonrojarme por mi mala calidad poética, expresaría el sentimiento que me sugiere su música como una brisa absolutamente sevillana desgranada en jazmines. Absolutamente digo por qué Ricardo era Sevilla tocando. Ningún guitarrista sevillano más sevillano que Ricardo. Su exaltado barroquismo más que hondo es precio- sista porque el dardo de Sevilla no hiere desde un teorema de lágrimas sino más bien desde un revuelo de gracia. Ricardo era el Chicuelo de la guitarra. El mismo le dijo un día a Manolo Barrios que había llorado viendo torear a Chicuelo. «Siendo muy joven —leemos en «Arte y artistas flamencos»— empezó como segundo guitarra en el Salón Variedades de Sevilla, y de acompañante para el cuadro flamenco que dirigía el notabilísimo estilista Antonio Moreno. Al terminar la segunda temporada del Va- APERITIVOS SELECTOS Mesones, 18 Teléf. 23 40 46 Especialidad en PLANCHA J A E N riedades, le avisaron para echar unos días en el café Novedades mientras Javier Molina se curaba de una enfermedad que padecía. Aceptó el Niño Ricardo el compromiso y estuvo tocando sólo hasta que de nuevo reapareció Javier. Mas como el muchacho había cumplido bien su cometido, le contrataron para seguir actuando fijo en la casa, y continuó de segundo con el gran maestro, y como su escuela fue de la más excelente calidad desde el principio, de ahí el original estilo que adquirió, con el cual sigue, cada vez más acentuado con su facilísima ejecución. Como su fama está consagrada por todos los públicos ¿para qué decir más de este indiscutible valor artístico? Y por otra parte ¿qué más garantías que ser discípulo de Javier Molina y Antonio Moreno?». ¡Cuánta nostalgia repartida en nombres: El Salón Variedades, que\n\n[ENDING CONTEXT]\n\ndos tercios de la primera malagueña me paró. «A ver la otra. Bien. Cuando quieras». ¡Qué bien se cantaba con Ricardo!\n\nManuel Serrapí, Niño de Ricardo y de Sevilla. Sin duda llevó la guitarra a unos extremos técnicos arrolladores. El mismo se arrollaba de tanto como quería decir. Parece como si le faltasen dedos y le sobraran argumentos, risas, amores y suspiros. Era un manantial, una fuente, un surtidor resuelto en un ángel musical profundamente sevillano. Sevillano él y sevillana su guitarra: su pena alegre como una bandada de pájaros revoloteando por entre las enredaderas de los patios.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Guitarristas, compañeros y amigos. Niño Ricardo",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1123,
    "article_char_count_full": 6597,
    "article_char_count_review": 3975,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "alma"
      }
    ]
  },
  {
    "article_id": "1985-07-17-right-les-gitans-dans",
    "article_text_for_review": "Por: José L. Buendía López\n\nPero permitasen antes dos palabras sobre la personalidad de Monsieur Leblón, este romántico rezagado, conocido de sobra por los lectores de «Candil», que en plena juventud hizo la campaña de Argelia, se enredó en los ojos oscuros de una mujer sevillana y acabó enrolándose en una tribu nómada de gitanos. ¿No les suena un poco a Lord Byron que rompiendo los moldes temporales hubiera recalado en nuestros días?... Algo de eso hay. Pero Bernard no se pierde en vagos suspiros líricos, sino que trabaja duro en lo que, como antes decíamos, constituye su tema fundamental de investigación: las relaciones del mundo gitano con la literatura española y el flamenco: estudio que ya diera a conocer a los lectores de habla hispana a través de las ponencias de los Congresos de Actividades Flamencas de Jaén, Granada, Cáceres y más recientemente Huelva, y, naturalmente, en las páginas de nuestra revista. Vaya pues por delante nuestro respeto y agradecimiento para esta actitud estudiosa hacia nuestro folklore, y que su ejemplo sirva para eliminar perjuicios en aquellos que aún piensan que dicha disciplina no tiene altura universitaria y debe quedar relegada a las páginas feriales del diario local.\n\nEl que este ensayo no esté traducido a nues- tra lengua no es sino una de las muchas lagunas con las que, a veces, debemos enfrentarnos los que, de manera seria, y no simplemente anecdótica, deseamos aproximarnos a aquellas áreas de conocimientos que despiertan nuestro interés y que han sido estudiadas en profundidad por intelectuales de más allá de los Pirineos. Esto no debe ser obstáculo ni cortapisa que nos impida adentrarnos en este riquísimo estudio sobre la presencia de los gitanos en la literatura española, que forma parte de una tesis doctoral consagrada a la citada minoría étnica por este autor, experto en temas flamencos, que la defendió brillantemente en la Universidad Paul Valéry de Montpellier en el año 1980.\n\nY volviendo al libro que nos ocupa, señalaremos que su autor centra su estudio en la aparición del tema gitano en diversos períodos de nuestra cronología literaria, siendo Cervantes, y más concretamente su famosa Gitanilla la que actúa como eje divisorio en las mencionadas fechas, señalando los diferentes aspectos temáticos que, en torno a la raza calé, tienen su reflejo literario en autores como Gil Vicente o Lope de Rueda. Posteriormente, y con Cervantes como punto de referencia, el profesor Leblon nos muestra con una finura crítica digna de todo encomio, cómo pueden analizarse algunas de las facetas materiales y espirituales más importantes de los gitanos en los siglos XVI y XVII a través de la literatura española del Siglo de Oro, tales como modos de vida y organización social, lengua, medios de subsistencia, religión y costumbres, etc. No por ello deja el investigador de estudiar en perspectiva la evolución del tema gitano en la literatura española hasta bien avanzado el mismísimo siglo XX, analizando sus relaciones expresivas con la poesía de algunos autores contemporáneos, entre los que tal vez merezca destacarse por su popularidad a Federico García Lorca.\n\nPara finalizar, y antes de entregarnos una cuidadosa bibliografía, el autor nos previene de que no ha querido crear un nuevo mito en torno al tema; analiza el sentido ritual de la fiesta cañí y llama la atención, a propósito del gitano, sobre: «Un sistema de valores radicalmente diferente del nuestro, mantenido contra viento y marea por estos anacrónicos saltimbanquis» (la traducción es nuestra).\n\nEn resumen, libro imprescindible, que desearíamos ver pronto traducido por su mismo autor, que, nos consta, maneja el castellano con aire y garbo suficiente para llenar de sal el hueco de una cantiña gaditana.",
    "title": "«Les gitans dans…»",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 607,
    "article_char_count_full": 3751,
    "article_char_count_review": 3751,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-07-18-left-solearillas",
    "article_text_for_review": "«Solearillas»\n\nNo creas que es por halagarte; pero te juro, bien mío, que no me cambio por nadie.\n\nSilencio en la noche quieta. Silencio en el campanario. Silencio en la plaza yerta.\n\n¡Qué pena me da, Consuelo, recordar tus años mozos y verte como te veo;\n\n¿Cómo sería mi vivir sin esta recia esperanza que tanto me acerca a tí?\n\n¡Ay, castillo de mi pueblo! Ayer, ajetreo de vida; hoy, inmenso pudridero.\n\nLo quieras o no lo quieras, trece celemines son algo más de una fanega.\n\n¿Cómo voy a estar tranquilo si sé que sales y entras con mi mayor enemigo?\n\nHoy me voy a emborrachar; pero Dios sabe que lo hago por pura necesidad.\n\nJuan Calderón Rengel",
    "title": "«Solearillas»",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 118,
    "article_char_count_full": 649,
    "article_char_count_review": 649,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-07-18-right-donde-hay-yeguas-potros-nacen",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJuan Trujillo, además de perote por naturaleza o nacimiento, lo era por antonomasia. Perote por apodo. Como dijeramos café café. Como si dijeramos por duplicado, a marchamartillo, lo que ya le confiere cierta categoría de autencidad y confianza entre sus paisanos.\n\nEl cante flamenco, como sus más profundas raíces filosóficas y líricas, tiene una buena do-sis de enigmático. Y esa característica se revela en muchas de sus letras.\n\nEn el año 1982, la Peña Flamenca de Alora, organizó un festival en homenaje a Juan Trujillo «El Perote», conocido también por «Trujilejo». Hablar de cante flamenco en Alora es como hacerlo de la miel en casa del colmenero. Entre otras cosas, porque Alora ha sido y es uno de los más potentes focos con luz propia en esa exquisita faceta del cante andaluz. No\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"inscripción\"]\n\nEntre otras cosas, porque Alora ha sido y es uno de los más potentes focos con luz propia en esa exquisita faceta del cante andaluz. No confundamos a este extraordinario cantar con el estupendo Diego «El Perote», asimismo aloreño, conocido también por el «Piín», primo de Sebastián Muñoz Beigveder, «El Pena». Como homenaje al «Pijín», se colocó el 18-3-1966 sobre la puerta de la casa núm. 18 de calle Erillas, de Alora, una placa con la siguiente inscripción: «En esta casa nació el día 15-2-1886 en cantaor Diego Beigveder Morilla, Diego «El Perote». Voy a relatar brevísimamente una anécdota relacionada con él, de la que los dos fuimos protagonistas. Yo había publicado un artículo en SUR de Málaga, titulado «La Perosía» con fecha 2-12-1966, en el que, entre otras cosas, decía que «no había tenido la oportunidad de escuchar sin premuras, con tiempo, como hay que hacer esas cosas, su repertorio de sabiduría flamenca». La respuesta no se hizo esperar. A los pocos días se presentó en mi despacho (en Alora) el propio Diego, dispuesto a cubrir ese vacío flamenco que yo venía padeciendo, como así ocurrió. Pero no recuerdo en mi vida una reunión más sosa, anodina, inquietante y triste, pues sólo éramos dos los que la integrábamos (lo mínimo en reuniones), no había vino\n\n[ENDING CONTEXT]\n\ncon otro aire flamenco—, porque también:\n\n«Donde candelita hubo\n\nsiempre rescoldo quedó».\n\n¿Qué hacéis todos aquí, en el inmenso redondel donde gime un único toro desatado? ¿Vinisteis a dejaros centrar en el oscuro rincón de la siguiriya, o fuístéis arrebatados por el giro eterno de la perfecta geometría de un cuejo. Yo sólo puedo responder, reclinado absorto sobre una mesa vacía, que la tarde acaba alargando su excesiva amargura de azúcar sintética, mientras el bulto negro concentra su sombra en mi vaso, haciéndome tragar todos los vestigios de una desorbitada leyenda.\n\nFrancisco A. Chica\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Donde hay yeguas, potros nacen",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "18-20",
    "page_number": 18,
    "word_count": 1727,
    "article_char_count_full": 10396,
    "article_char_count_review": 2902,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "inscripción"
      }
    ]
  }
]
```
