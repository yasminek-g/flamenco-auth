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
    "article_id": "1982-09-46-right-se-acuerda",
    "article_text_for_review": "La pequeña anécdota\n\nFue en Córdoba, sede de la Peña Flamenca, cercana a la placita del Potro, horas antes al descubrimiento de la lápida a Ricardo Molina en la casa donde vivió y escribió casi todos sus años de existencia el gran poeta y tratadista flamenco.\n\nTrípodes, cables y flashes de la «tele» movíanse preparando para hacer unas secuencias sobre Antonio Mairena. Al verlo allí en la de anea, tranquilo, patriarcal, como ajeno y en tó, quise —tras muchas veces de aparentes desapercibos ambos— estrechar la diestra del grande entre los grandes de mi apreciado Arte. Alargándole mi mano, le dije: —¿Se acuerda usted de mí, maestro? —¡Cómo no me viá cordá, hombre... Usted es inconfundible! El eco dado a las palabras —no ellas en sí— me supo a gloria. Un su adicto incondicional, y de mi afecto, me exclamó: ¡Anda!... ¡Eso no se pilla a ca instante!\n\nNo obstante mi gozo, me obstinaba yo en dar con el caso a que referiría «lo de inconfundible». Acaso fuera cuando en cierta ocasión se disponía a cantar y comenzado el guitarrista su cante (que la guitarra también canta) no paraba el murmullo, respecto a si era o no era mejor su habitual acompañante... Entonces se me antojó gritar: ¡Pongamos las orejas como pitas!... ¡Por favor! Otro buen aficionado, a mi vera, hizo un gesto a tenor con la frase y el silencio se produjo eclesial, imponente...\n\n¿Se acuerda usted, maestro?\n\nPor J. Márquez Cabello\n\nSaluda a todas las peñas flamencas\n\nVIRGEN DE LA CAPILLA, 13 TELEFONO 253008 ___ JAEN",
    "title": "¿Se acuerda?",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "46-46",
    "page_number": 46,
    "word_count": 258,
    "article_char_count_full": 1494,
    "article_char_count_review": 1494,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-47-left-chac-n-y-mairena-en-mi-concepto-",
    "article_text_for_review": "Por Luis Caballero\n\nNacemos en el lugar del Cante y el Cante en nosotros, y nosotros ya siempre en el Cante como drogados o heridos por la fuerza de su hondura. Es el veneno del Cante. Se dice, decimos. Ese veneno... Saturados, decepcionados, cansados o deseándolo como un remedio sin más remedio, necesitamos cantar y escuchar cantar. Más aún que deseo y necesidad es puro vivir viviéndolo por vivir en él, porque quien canta y siente el Cante es el Cante mismo.\n\nYo no soy ni fui jamás persona asida y uncida al Cante por otra causa y razón que no sea la de ser parte de su propia naturaleza. Por supuesto, yo y todo el que canta y todo el que siente el Cante sin cantarlo, tantas veces más y mejor que los que lo cantamos. Así es porque así nacemos. ¿Envenenados? Digamos marcados por una aptitud singularmente sensible a esta raíz musical. Aun en el despertar a este fenómeno, y hasta careciendo del más mínimo entorno ambiental, el predestinado se iniciará, intuitivamente, obedeciendo la llamada interior de esta parcela sentimental. Es la tierra, la latitud, el clima, el tiempo, el hombre y su cultura.\n\nY después la realización, la consumación, el encauce de ese caudal centrándolo por entre las dos orillas que delimitan, orientan y conducen el Cante en su pureza.\n\nPero ¿qué es la puréza? ¿Cuándo es puro el Cante? ¿Quién y quién subraya esa verdad o verdades? Porque es que el concepto de pureza que tratamos de localizar en el Cante reside más en el fondo de cada criterio personal que en un acuerdo o coincidencia unánimes.\n\nNo es imprescindible deambular por entre los cuatro puntos cardinales del Cante en general para lograr aproximarnos a una cierta y positiva formación que pueda responder a la verdad que buscamos y a un juicio ecuánime de la escala de valores.\n\nIndirectamente mi cante rodaba y rondaba por la delicada escuela chaconiana cuando comencé a comprender, no lo que faltaba en el mundo del cante de Chacón (a Chacón le sobraban conocimientos), sino lo que no le «iba» a Chacón, que es distinto. Las demás escuelas también me llegaban fraccionadas y distantes. Fue entonces cuando la suerte quiso que Antonio de Mairena apareciera en mi vida flamenca y yo en la suya bajo el sello de una amistad que día a día había de ir aumentando en afecto, consideración y entendimiento.\n\nPara no faltar a la verdad yo lo andaba buscando desde mucho antes. Al primer Mairena que conocí y escuché fue a Juan. Bastante después a Curro e inmediatamente a Manolo. De Antonio conocía sus primeras grabaciones con Esteban de Sanlúcar, sus intervenciones en el ballet del otro Antonio y un L.P. grabado en Londres. Lo suficiente para que la llamada interior de mi parcela sentimental me aconsejara intuitivamente la necesidad de penetrar en ese otro mundo donde, al igual que en el de Chacón, iba a hallar la perfección, pero con otro sonido, y, naturalmente, no lo que falta en ese otro mundo, sino lo que no le «va».\n\nSin duda la pureza no puede ser otra cosa que la perfección: Cantar los cantes perfectamente. A la perfección puede faltarle ángel, gracia, duende, pero nunca en las voces de don Antonio Chacón y don Antonio Mairena, más que dos mundos, dos extremos, dos mitades que al tocarse, precisamente por los extremos, constituyen y construyen la unidad del Cante en su totalidad.\n\nIndependientemente de estas mal hilvanadas conclusiones de un chaconiano-mairenista en cuanto a perfección-pureza como equilibrio del Cante en la verdad buscada, Mairena, con toda su profundidad andaluza a cuestas, vino a modificar y clarificar la estructura e imagen que hasta entonces yo tenía del Cante. Sin embargo, Mairena no dicta lecciones. No concuerda con su carácter entrar o salir en consejos. De Mairena se aprenderá escuchándole cantar y muchas veces hablando con él de las mil cosas del Cante. En el curso de nuestra larga amistad, sin duda hemos ido aprendiendo mucho de la vida del Cante y de otras cosas de la vida.\n\nNunca me entusiasmaron los premios. Será por la imposibilidad evidente de no poderlos ganar. Mi modestísima condición de cantaor no da para mucho. Pero, curiosamente, conservo uno que, para mí, sobrepasa los límites de lo importante en premios.\n\nEncerrados en el cuarto clásico celebrábamos el regreso de Manolo de Huelva a Sevilla unos quince aficionados. Sin borrachera ni delirio, Antonio cantó como no recuerdo haberlo oído jamás. De vez en cuando, Manolo, según su filosofía flamenca, dejaba la guitarra con suma delicadeza, y, con elegancia y sin prisa, liaba un cigarro, tomaba un sorbito de conagc y hablaba del cante.\n\nHubo un momento en que alguién me pidió que cantara por jaberas. Debió ocurrirsele considerándome a mí y al estilo que me pedía antípodas de lo que allí se estaba cantando. En un momento de inspiración hice la jabera y dos malagueñas de Chacón. Fue entonces cuando de pie, don Antonio de Mairena y don Manuel de Huelva me premiaron con su aplauso.\n\n(Tan insólito acontecimiento, sobre todo por parte de Mairena, me animó a publicarlo en la prensa).\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N",
    "title": "Chacón y Mairena en mi concepto del cante",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "47-47",
    "page_number": 47,
    "word_count": 869,
    "article_char_count_full": 5079,
    "article_char_count_review": 5079,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-48-left-corona-po-tica-para-el-cantor-an",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSelección de Antonio Murciano\n\n(La noche del 12 de mayo de 1972, en el acto de clausura de la VI SEMANA DE ESTUDIOS FLAMENCOS de Málaga, el entonces alcalde de la ciudad, señor Utrera Ravassa, impuso al cantaor ANTONIO MAIRENA la Medalla de Oro de la Semana. Fueron invitados de honor al acto, Pastora Imperio, Manolo Caracol, Agustín Castellón «Sabicas» y Pilar López, medallas de oro en las anteriores ediciones. Aquella noche, cantó Mairena, larga y magistralmente. En el fin de fiesta intervinieron Diego Clavel, Curro Mairena y Pepe Meneses con las guitarras de El Poeta y de Melchor de Marchena, y al baile Los Bolecos. Previamente, el poeta Antonio Murciano, pronunció una conferencia sobre la personalidad humana y artística del cantaor homenajeado, a la que puso broche con la corona\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"reproduce\"]\n\ns de oro en las anteriores ediciones. Aquella noche, cantó Mairena, larga y magistralmente. En el fin de fiesta intervinieron Diego Clavel, Curro Mairena y Pepe Meneses con las guitarras de El Poeta y de Melchor de Marchena, y al baile Los Bolecos. Previamente, el poeta Antonio Murciano, pronunció una conferencia sobre la personalidad humana y artística del cantaor homenajeado, a la que puso broche con la corona poética que en esta grata ocasión reproduce «CANDIL». Los poemas de Alcántara, García Ulecia y José María Arévalo se incorporan a esta selección como colaboraciones inéditas). EL CANTAOR Y SONETO-HOMENAJE A ANTONIO MAIRENA Ⅰ Mairena está parado. Es imposible averiguar su astro. Ni la noche serena ni el furor del invierno destemplado. Pero él está quieto porque lo ha encontrado. ¿Sirio? ¿Lucifer? Sin nombre refulge en lo alto. Sólo envía su música al melancólico gitano, al sevillano triste, hierático. II En las fuentes del alba silenciosa, tejiendo sombra y luz, nació tu cante, transido aún de luna y ya radiante de claro ruiseño y roja rosa. Yo el alma reverencio poderosa y el subterráneo sol que, suspirante, la voz incendia de tu raza errante, la queja de tu raza misteriosa. India andaluz, tu laurel más puro floreció en los plateados olivares y los verdes naranjos de Sevilla. Allí te aclama el martinete oscuro su rey, allí su reino soleares te rinden, y su imperio: seguiriya. Ⅲ PARA ANTONIO MAIRENA Se abre la copla en tu garganta como un pavo real, el último pavo real que viera el padre Adán un poco antes de aquel ciego quedarse con ojos descartados de la zambra final del paraíso. Se funden bronce y noche en el fuego sereno de tu voz y parece qu\n\n[ENDING CONTEXT]\n\nojal del señorío. Porque el cante es tu vida y tu mensaje hoy quiero levantar en tu homenaje, la voz flamenca del soneto mío.\n\n(SOLEA DEL MAESTRO)\n\n«Una cogí del romero y otra el pueblo me la dio: con ellas abrí a la vida mi cante y mi corazón».\n\nAntonio Murciano\n\nToldos Hernández\n\nAvd. Granada, 39 - 221639 - 221587\n\nJ A E N\n\n……\n\nTOLDOS - LONAS ARTICULOS DE TERRAZA Y JARDIN\n\nPERSIANAS\n\nPUERTAS PLEGABLES\n\nMONTEMAR\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Corona poética para el cantor Antonio Mairena",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "48-50",
    "page_number": 48,
    "word_count": 1679,
    "article_char_count_full": 9525,
    "article_char_count_review": 3303,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "reproduce"
      }
    ]
  },
  {
    "article_id": "1982-09-51-left-mairena-en-la-uni-n",
    "article_text_for_review": "Aquello acabó con los claros del día, de modo que el mismísimo lucero del alba pudo llegar a tiempo para dar testimonio del suceso, y lo dará, sin duda, casi tan puntualmente como Paco Vallecillo, que allí estaba, venido al frente de la embajada.\n\nCuentan las crónicas que nunca, desde los tiempos del Rojo, los manes flamencos de la sierra se conmovieron de igual manera. Aseguran los cronistas que nunca, tampoco, desde la época áurea, vibraron así, en las galerías, los ecos de la copla.\n\n¿Te juegas tu soleá\n\n(Le dijo El Rojo a Mairena:\n\n—¡Qué hermoso cante, el de esta tierra!, sentenció Antonio.\n\ncon mi cante por minerás?).\n\n—¡Qué hermoso cualquier cante, de cualquier tierra, cantado por tu voz, maestro!, replicaron los de acá, sin sentenciar nada, porque ya estaba sentenciado.\n\n(No apuestes, Alpargatero: cada cante, en su lugar, en su punto y a su tiempo).\n\nFue por febrero del setenta y dos, si no me acuerdo mal. Mairena había llegado a La Unión para recibir de la afición levantina el homenaje debido, que organizaba un periódico de la región. Y, como Paco el de Lucía había arribado para otro tanto, el encuentro genial era inevitable.\n\nHelos ya, pues, improvisadamente juntos, en el ritual flamenco: éste, al cante; aqueste, al toque. ¡Ahí es nada!\n\nLos que se las saben todas proclaman que la estampa, inédita hasta entonces, no se ha repetido después. No sé si esto es verdad. Lo que sí sé —y ello me excusa de descripción más detallada— es que, retratos aparte, aquella conjunción increíble quedó registrada para memoria de las generaciones. Los hay que poseen el documento sonoro y lo guardan como reliquia.\n\nY una reliquia es, ni más ni menos.\n\nRompió Mairena a cantar, y allí se acabó el mundo. Entró en trance y en trance puso a la concurrencia en-\n\nYa era la noche en la mitad del universo, y noche avanzada. Ya estába el cónclave reducido a lo justo. Ya estaban levantados los espíritus, merced, por partes alícuotas, a la experiencia de la tarde y a cierto vinillo del país.\n\nAusente estaba ya Paco, más presente, y bien presente, Antonio. Y Antonio, aquella madrugada, no necesitaba guitarras de portento. Le bastaba, escuetamente, una buena guitarra, y para eso estaba allí, atenta, como siempre, a la que salta, la sonanta gitana de Fernández. Allí estaban, también, Pencho y Alfredo, para terciar en el cante, llegado el caso. Cuando se arrancó por bulerías y se marcó unos pasos, las manos en los picos del pañuelo rojo, ora ceñido al cuello, ora trazando dibujos en el aire, se alcanzó el paroxismo.\n\ntera. En unos minutos, se pasó sucesivamente de la discreta exaltación al éxtasis pleno, y del éxtasis, al delirio. Juran y perjuran los mairenistas de siempre —en la circunstancia lo fuimos todos— que jamás habían escuchado a Antonio Cruz cantar de forma semejante.\n\nMás de uno y más de dos lloraban a lágrima suelta. Recuerdo a Diego Granados y a Joaquín Alfonso, borrachos perdidos, no de vino —que ya el vino había perdido la virtud de emborrachar—, revolcándose por el santo suelo, en una especie de flamenquísimo «delirium tremens».\n\nYo lo vi, y no se me olvida.",
    "title": "Mairena, en la Unión",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "51-51",
    "page_number": 51,
    "word_count": 532,
    "article_char_count_full": 3102,
    "article_char_count_review": 3102,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-09-51-right-evocaci-n-de-antonio-mairena",
    "article_text_for_review": "$ \\underline{\\text{A su $ \\underline{\\text{paso $ \\underline{\\text{por el $ \\underline{\\text{paisaje}} $}} $ minero de La Unión}} $\n\nPor Asensio Sáez\n\n—He venido como invitado de honor al Festival minero.\n\n—¿Que hace en La Unión António Mairena?\n\n¿Ocho, diez años ya? El tiempo pasa, corren veloces las manecillas del reloj, ¡digo sin correr! Valga como ejemplo —al menos para uno que lo vio nacer— el propio Festival Nacional del Cante de las Minas, adulto hoy, crecido en edad, gracia y sabiduría ante Dios y ante los hombres. Veintidós ediciones y vamos andando. «La vita fugge», que certificó Petrarca. Eso. El corazón con canas, como quien dice.\n\nDescubro su perfil de moneda romana, alcanzado por las luces del «tablao». Bajo la luna gorda del agosto minero, embarazada de coplas, se celebra, una vez más, la fiesta litúrgica del cante de La Unión. En el escenario alguien canta una «minera» impecablemente, sólo que en frío. ¿Una pieza de orfebrería, una vidriera de la catedral? Sin embargo, nadie ha conectado con el «cantaor».\n\n—¿Qué, Antonio?\n\n—?Ve usted?\n\n—Ha fallado el duende.\n\nEl duende. Todos sabemos que el duende le dio tema a Federico para una conferencia magistral, modelo en la historia de la literatura. «El duende hay días que viene y días que no viene» —palabras de Antonio Mairena—. Vale la pena rememorar aquella entrevista que Tico Medina, precisamente pregonero de una versión del Festival de La Unión —¡Dios, qué hermoso pregón el suyo!—, le hiciera un día a Antonio Mairena. Tampoco recordamos en estos momentos el año; sí que Antonio Mairena se definía en ella como gitano por los cuatro costados. Gitano cabal desde la calva al tacón. Exo importaba, importa. Lo demás, el cromo de pandereta, ya es harina de otro costal. «¿Por qué voy a ir por la calle con una camisa de lunares y un sombrero de ala ancha, como los gitanos de las postales que compran los turistas?».\n\nLe digo que una noche fui testigo de la ceremonia sagrada, secreta, dolorosa y gozosa al mismo tiempo, de su cante. Y que aquella noche él sí que fue asistido por el duende. Bien lo recuerdo. ¡Qué cante el suyo, palabra! Su voz, campana de gloria y duelo, a la vez; trueno y suspiro, desierto y oasis, torreón de la pena y la alegría; su garganta, en fin, el estuche de una joya: la de su voz precisamente.\n\n—Muchas gracias.\n\nMe explico que, oyéndolo cantar así, Antonio Murciano pudiera escribir, sin el menor esfuerzo, su «Brindis» para el «cantaor»:\n\nAntonio, cantaor, andaluz puro, llave de qué gitanas catedrales, yunque y toná, cabal entré cabales, voz de fragua, minero de lo oscuro...\n\n—Minero, Antonio, minero...\n\n—Sí, sêõr.\n\n—Vamos a ver, Antonio, ¿qué habría que hacer para cantar como Dios manda el cante de los mineros?\n\n—Sentir aquí, corazón adentro, el drama de la mina.\n\n¿Sabe Antonio Mairena que ya la mina no es lo que fue, que la vida venció a la literatura del hombre-topo de las galerías y los pozos y que el pliego de cordel del minero «cavando su sepultura» ha sido sustituida —afortunadamente, por supuesto— por el trabajo «a roza abierta», avalado por la última tecnología, bajo un sol exultante que alumbra la alegre escenografía del molino y la palmera, el árbol y la mar: Mediterráneo y Menor, poniéndole un zócalo de azules al paisaje?\n\n—Si quiere usted, Antonio, mañana le llevo a visitar una mina.\n\n—¡Pero si mañana andaré de viaje! Compromisos, ¿sabe usted? En otra ocasión, acaso...\n\nCuando se despide, terminada la jornada del Festival minero, ya es tarde, muy tarde, o muy temprano, según se mire. En el cielo se ha apagado el farolón de la luna y de la sierra, después de una noche tercamente calurosa, bajan los primeros manotazos del viento niño y aliviador de la aurora. Mañana empieza a ser hoy.\n\nTELEVISORES «PAL» COLOR Placas compactas Hornos en 3 versiones Campanas extractoras de humos Frigoríficos - Congeladores - Arcones Lavavajillas - Lavadoras",
    "title": "Evocación de Antonio Mairena",
    "periodical": "candil",
    "issue_id": "1982-09",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "51-52",
    "page_number": 51,
    "word_count": 659,
    "article_char_count_full": 3893,
    "article_char_count_review": 3893,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
