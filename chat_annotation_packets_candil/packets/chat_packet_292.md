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
    "article_id": "1994-09-4-left-la-poca-dorada-mis-encuentros-co",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMis encuentros con DIEGO DEL GASTOR (y 2)\n\nTérminé la primera parte de Diego del Gastor (Candil n.º 94) mencionando la considerable atracción que Diego tenía para las mujeres. Utilicé la frase «Diego no corría detrás de las mujeres, ellas corrían detrás de él». Más adelante abundaremos sobre este punto, ya que el romance fue parte importante en los últimos años de su vida. Pero primero vamos a investigar algo más su interesantísima forma de ser, y recordar algunas de las experiencias flamencias que tuve la buena fortuna de compartir con Diego.\n\nA Diego le encantaba un intercambio de ideas, pues tenía muchas y bien definidas. Como dije antes, él era bastante irreverente en cuanto a los valores establecidos, pero no de la manera irreflexiva de la mayoría de los objetores políticos. Mientras\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"guia\"]\n\nél era bastante irreverente en cuanto a los valores establecidos, pero no de la manera irreflexiva de la mayoría de los objetores políticos. Mientras no estaba demasiado bebido, que es cuando la razón tendía a eludirle, él procuraba entender todos los puntos de vista en una discusión. Por ejemplo, no le gustaba el régimen de Franco, pero también compartía la creencia que los tipos mediterráneos necesitaban una mano más duera que lo normal para guiarles. «Los españoles», solía decir, «tienden a ser indisciplinados, y dada la oportunidad crean caos y anarquía. Mira la República de los años treinta. Había libertad para todos y abusamos de ella descaradamente. Había casi tantos partidos políticos como votantes, y el crimen y la corrupción eran incontrolables. Entonces, por lo menos, España no estaba preparada para la democracia». Aunque Diego consideraba a casi toda la gente alrededor de Franco, incluida su familia, oportunistas enriquecidos a expensas del pueblo, respetaba al mismo Franco como un hombre íntegro que intentaba verdaderamente ayudar a su patria. «Mira su historial», decía, «despidió a éste y al otro y hasta a su propio hermano por corrupción, pero no puede controlarlo todo. Es verdad que al principio tuvo la mano demasiado dura, pero se ablandó bastante cuando lo consideró posible. Yo le veo como un hombre dedicado a procurar salvar a España de sí misma». Sobre todo, Diego consideraba a Franco un genio de la política. Pensaba que haber mantenido a España apartada de las guerras tanto tiempo era de admirar (por entonces 25 años, desde la guerra civil española), y que su manera de tratar a Hitler había sido par\n\n[ENDING CONTEXT]\n\nla calle San Miguel hasta el Ayuntamiento, donde el Gallo de Oro fue puesto en su ataúd, y de allí al cementerio. Algún tiempo después se organizó un homenaje en su honor para financiar el busto que fue levantado en los Jardines de la Alameda de un Diego mirando con ojos vacíos a los transeúntes, los niños jugando y, como en vida, a los amantes de turno. Adicionalmente, nombraron la calle peatonal que sube desde la Plaza San Miguel hasta el Gallo de Morón «Diego del Gastor».\n\nAsí que Morón recordó bien a su hijo adoptivo gitano, que había proporcionado a Morón fama y hasta cierta fortuna (3).\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La época dorada: Mis encuentros con Diego del Gastor D",
    "periodical": "candil",
    "issue_id": "1994-09",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "3-10",
    "page_number": 3,
    "word_count": 6199,
    "article_char_count_full": 36632,
    "article_char_count_review": 3273,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "guia"
      }
    ]
  },
  {
    "article_id": "1994-09-11-left-nano-de-jerez-rafael",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMenudo, simpático y vitalista; claro de ideas y con un intenso bagaje cantaor que le circula por la sangre, Nano de Jerez es consciente de cuál ha de ser su papel en el mundo flamenco. Primero, se identifica como intérprete de Jerez de la Frontera y miembro del «clan» de «Tío Juane», con todas las connotaciones que dicha filiación lleva consigo. Posteriormente, reconoce el magisterio que sobre su persona artística ha ejercido Antonio Mairena, asumiendo con orgullo el seguimiento que, alternativamente con Jerez, desarrolla de la personalidad del de «Los Alcores». Finalmente, saca la cara por la ortodoxia flamenca y aboga por una recreación basada en las claras raíces del flamenco.\n\nOmo surge lo de Nano de Jerez?\n\n—Pues eso surge porque me llamo Cayetano y por mi abuelo. Trabajando de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"famili\"]\n\ndo empiezo a cantar en los «Jueves Flamencos» que organizaba El Morao y a los que iba con pantalones cortos, todos me decían «Nano» y me quedé con «Nano de Jerez». Lo cierto es que nadie me conoce por Cayetano; tú preguntas en Sevilla o Jerez por Cayetano y nadie sabe darte razón; si preguntas por «Nano de Jerez», entonces sí. —¿Cuándo te apercibes de que tú podías ser artista flamenco? —Fue con catorce o quince años, pues como miembro de una familia gitana y fragüera yo siempre escuché el cante alrededor de la fragua. Oía a mi padre, a la familia de mi padre y a los amigos de mi padre. Entre los amigos de Tío Juane —mi padre— se encontraban El Chozas, El Borrico, Terremoto... De ahí se me vino el cante al oído y al corazón, porque el cante se lleva dentro, y de tanto escuchar a estos grandes artistas me di cuenta de que yo también podía entonar los cantes, así que lo hice en los bautizos, bodas y fiestas. A partir de aquí le dije a mi padre que yo iba a tirar el martillo y que me iba a dedicar al cante. Por aquella época Manuel Moreno «El Morao» organizaba en la plaza de toros de Jerez un espectáculo que se llamaba «Los Jueves Flamencos», el primero donde cobré por cantar. Posteriormente me contrató La Cañeta de Málaga, con la que estuve un año trabajando, más tarde me fui a Japón y al volver y durante una estancia en Marbella, conocí a una bailaora-cantaora con la que posteriormente me casé en Sevilla, ciudad de la que hice mi cuartel general, aunque todas las semanas me voy a Jerez para no perder su agua, pues sigo bebiendo en mi tierra. —¿Qué recuerdos mantienes en tu memoria de las reuniones de tu padre con El Borrico, El Chozas y Terremoto? —Sinceramente, lo que me queda en la memoria es un sentimiento de admiración hacia esos artistas. Y tengo que decirte que también tuve la suerte de trabajar con algunos de ellos como El Borrico, Terremoto o El Sordera, en las fiestas que se organizaban en las bodegas de Jerez. Recuerdo que había una viña que le decían «El Majuelo», donde se organizaban fi\n\n[ENDING CONTEXT]\n\nsin respirar, cómo podía cantar Santiago y Santa Ana y no respirar. ¿Quién sería este monstruo? Yo no lo comparo con nadie.\n\n—¿Don Antonio Chacón?\n\n—Un cantaor de una majestuosidad y un señorío inigualable. Era un canario entre las flores y la miel.\n\n—¿Juanito Mojama?\n\n—Juanito Mojama era el eco, los soníos negros en sus tientos y sus siguiriγas.\n\n—¿El Gloria?\n\n—El cantaor de los pulmones más grandes del mundo. Un cantaor al que le tengo mucha admiración como saetero y por ese aroma de Jerez que tenían sus cantes por soleá-bulería y por bulerías.\n\n—¿Terremoto?\n\n—Eso, un terremoto cantaor.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Nano de Jerez Rafael",
    "periodical": "candil",
    "issue_id": "1994-09",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "10-12",
    "page_number": 10,
    "word_count": 2579,
    "article_char_count_full": 14547,
    "article_char_count_review": 3651,
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
    "article_id": "1994-09-13-left-poes-a-emilio-gonz-lez-de-herv-s",
    "article_text_for_review": "La Siguiriya¡La Siriguiya\n\n«Cantaba Silverio en aquel colmao, y crujía hasta el yunque de tablas: que me lo ha contao...».\n\nEsa que canta y llora es la Siguiriya.\n\n¡Esa que está llorando es la Siguiriya!\n\n¡Calva del corazón! Pena, penilla.\n\nCrótalo, falda y tacón, rayando el alba.\n\n¡La Siguiriya! ¡Mira el desmeleneo de su copilla!\n\nBaila llorando, mientras está cantando, la Siguiriya.\n\n¡Ansia, rabia y desplante!\n\n¡Fragua viva del Cante!\n\nPor eso brilla y humilla, como piedra de toque, la Siguiriya. «Cantaba Silverio en aquel colma...».\n\nEmilio González ¡Ay, madre de mi alma! de Hervás ¡La Siguiriya!\n\nLa Soleá\n\nSólo tres volantes solos, con puntilla negra, tiene la bata blanca de Soledad.\n\nCabe en el mundo mayor equidad?\n\nSólo tres volantes solos; como los tres versos de la Soleá.\n\n¿Y pa qué más?\n\nSi con tres versos volantes puede airearse la gran verdad.\n\n«Mira qué bonita era; como la espiga del trigo que trilla el trillo en la era.»\n\nSólo tres volantes solos marcan elegantes ritmos y desplantes de la Soleá.\n\n¡Y qué gran verdad!\n\nSólo tres volantes solos, con puntilla negra, tiene la bata blanca de Soledad.\n\nSólo tres volantes solos!...\n\n...¡Como los tres versos de la Soleá!\n\nEmilio González de Hervás",
    "title": "Poesía Emilio González de Hervás",
    "periodical": "candil",
    "issue_id": "1994-09",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 203,
    "article_char_count_full": 1220,
    "article_char_count_review": 1220,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-09-14-right-bienal-x27-94-la-desproporci-n-p",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE xisten una serie de datos objetivos en la octava celebración de la Bienal de Arte Flamenco «Ciudad de Sevilla», que me hacen pensar en lo desproporcionado. Y utilizó el término siguiendo la primera acepción que le da el Diccionario de la Real Academia: falta de la correspondencia debida de las partes de una cosa con el todo o entre cosas relacionadas entre sí.\n\nEl primer dato parece premonitorio de todos los demás: el «Ciudad de Sevilla» no luce por ningún lado. La Bienal podía haberse situado en cualquier otro lugar de España, preferentemente Madrid, y su discurrir hubiera sido el mismo, si no mejor. Existe alguna razón para olvidar o evitar la relación entre la Bienal y Sevilla?\n\nSe puede aducir que el flamenco es un arte universal que no necesita, obligatoriamente, estar ubicado en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"privado\"]\n\nngún lado. La Bienal podía haberse situado en cualquier otro lugar de España, preferentemente Madrid, y su discurrir hubiera sido el mismo, si no mejor. Existe alguna razón para olvidar o evitar la relación entre la Bienal y Sevilla? Se puede aducir que el flamenco es un arte universal que no necesita, obligatoriamente, estar ubicado en un sitio concreto para lucir y lucirse. Pero, independientemente de otras consideraciones, a Sevilla se le ha privado de un protagonismo que, por sí fuera poco, le cuesta dinero. Alguno considerará que la Bienal está tan suficientemente asentada como para hacer innecesaria la indicación precisa de la ciudad que la acoge. En el mismo sentido, pero al revés, esta precisión no le iba a quitar ni un ápice de valor a lo consolidado. ¿O quizás sí? Otro de los datos objetivos a tener en cuenta es el del criterio seguido en las contrataciones. Sin entrar, por supuesto en la forma de las mismas porque administradores hay para pedir las cuentas necesarias. Es lícito dudar de esta petición de cuentas pero, en cualquier caso, no soy yo quien tiene autoridad o competencia para hacerlo. Me refiero al criterio artístico, al seguimiento de una línea que, si bien no rompe totalmente con la mostrada en anteriores ediciones, sí la potencia grandemente. Cada cual tiene, con respecto al arte que llamamos flamenco, sus particulares ideas. Aunque es evidente que deben existir coincidencias entre las ideas para llegar a conclusiones que se adecúen a lo conocido como tal. Existen unas coordenadas, si lo prefi\n\n[ENDING CONTEXT]\n\nexcepciones, no está conectada con su esencia y responde a juicios foráneos, normas particulares o gustos personales.\n\nJosé Luis Ortiz Nuevo se ha apuntado, esta vez hasta como organizador, un nuevo tanto. Un tanto importante de cara a la galería —dicho sea en el mejor sentido— y puede presumir de su triunfo y exigir mayores atenciones para con el cante. En este sentido, su trabajo es encomiable sin retintines. Pero es conveniente, desde mi punto de vista, no perder el norte de la realidad, si lo que queremos es conseguir, cada día, un mejor conocimiento y un mayor respeto para nuestro arte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Bienal&#x27;94 La desproporción por bandera",
    "periodical": "candil",
    "issue_id": "1994-09",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1730,
    "article_char_count_full": 10338,
    "article_char_count_review": 3169,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "privado"
      }
    ]
  },
  {
    "article_id": "1994-09-15-right-cr-nica-del-xxii-congreso-de-art",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n¿Qué necesidad tenía usted, Don Aurelio Gurrea, de interrumpir su sopor mediterráneo, ese dulce «far niente» a la vera del mar primigenio de todas las culturas, y meterse en este berenjenal de los flamencos para que todos quedásemos encantados, detenidos en la magia azul de la ribera más antigua del mundo, al menos de lo que yo alcanzo a mirar desde la insignificancia de mi visión?\n\nNinguna, y, sin embargo, usted, ú, querido Aurelio, y un sensa-\n\nMiembros del Comité Ejecutivo del XXII Congreso. De izquierda a derecha: Rafael Morales Montes Francisco Hidalgo Gómez Paco Valero Vargas José Arrebola Rivera Aurelio Gurrea Chalé Jaime López Krahe Pablo Franco Cejas\n\ncional equipo de gentes capaces, habéis sido lo suficientemente sensibles para eternos a todos en el saco común de nuestras cosas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"artesano\"]\n\nbien hecho, aunque algunos, pensando en la belleza de esas playas y en el blanco discurso de los pueblos serranos que nos han acogido, la verdad, hemos mirado de reojo al dios golfo coronado de pámpanos. Prólogo para afortunados Los días 5 y 6 de septiembre, la iniciativa congresual tejió el tapiz de los sueños por el que, más tarde, discurriría una interesante actividad opcional, a manera de prólogo de feria. Cuatro artistas distintos (lo de artesano quede para los cúrsiles e ignorantes) ofrecieron en la Casa de la Cultura de Estepona el resultado de sus indagaciones artísticas flamencas. La pintura, de la mano de José Hidalgo y José Olivares, tuvo que competir en la brecha jonda, inaugurada en la plaza de las Flores, con la magia fotográfica de Carlos Albelos, ese andaluz trasplantado de ida y vuelta, y la maestría guitarrera y coleccionista de Ángel Luis Cañete. Los cuatro demostraron que, con técnicas distintas, se puede acceder a ese balbuceo flamenco que tanto nos inquieta. Un solo reproche a la organización, por qué no exponer estas espléndidas muestras en los salones desocupados del complejo Atalaya, para solaz cotidiano de los congresistas? Todos lo hubiésemos agradecido, en especial los artistas, que habrían encontrado así el necesario calor humano para su obra. Fue Casares, el pueblo que vio nacer a Blas Infante, aquel hermano mayor de nuestras inquietudes andaluzas (perdón por no hablar de paternidades que luego son discutidas por hijos más o menos díscolos) el que acogió con los brazos abiertos el acontecimiento de la reedición del libro del notario ilustre, «Orígenes de lo Flamenco y Secreto del Cante Jondo». A tal señor, tal honor, puesto que fue la voz, sabia y documentada, de Manuel López, la que devolvió al tiempo histórico lo que, a veces, las memorias febles se encargan de borrar, la gran aportación teórica a los orígenes andaluces que dicho libro representa. El bello crepúsculo del impoluto pueblo serrano se tiñó de matices blanquiverdes. No se veía más que cielo y agua Era la mañana del día 7 de septiembre del día consagrado a Mercurio, cuando los congresistas nos reuníamos en el paradisíaco rincón del conjunto Atalaya Park, elegido por la Organización para celebrar este XXII Congreso. Al\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_02 | trigger=\"entendido\"]\n\nSanta Coloma de Gramanet. Realizado el oportuno referéndum, la presidencia recayó en Gonzalo Rojo, auxiliado, en la labor de vocales, por Francisco Hidalgo y Francisco Navarro. La Ponencia: «La Saeta en Mála- ga; las Saetas malagueñas», defendi- da por su autor, Eusebio Rioja, inauguró la parte científica de nuestra reunión. En ella, el estudioso malacitano realizó un recorrido por tan apasionante tema flamenco y semanasantero, no siempre bien entendido y aceptado en los ambientes jondos. A través de documentos de archivos y prensa local, Eusebio recogió y nos trasladó fielmente las noticias existentes sobre la saeta malagueña, desde las primeras noticias de su constancia callejera, hasta los años setenta, para, en una segunda parte, pasar a definir sus características y tipología, distinguiendo tres estilos diferentes, desde el primitivo, caracterizado por el tercio de unión qué enlaza la siguiriya y el martinete, hasta el segundo, nacido a comienzos de los años cincuenta gracias al genio de Antonio de Canillas, y un t\n\n[ENDING CONTEXT]\n\ntodos, muy pronto, antes de que nos demos cuenta. El símbolo de Santa Coloma es la paloma y su gracilidad rápida y elegante no sabe de distancias. Volando junto a ella nos dirigiremos a Barcelona con el símbolo de la paz como nuestro propio símbolo; en el pico, unas palabras flamencas y en el corazón un abrazo para los hermanos catalanes que habrán de acogernos bajo su cielo protector. Que así sea. ALREO de la FIESTA GITANA Dibujos de Miguel Alcalá del libro «Le Flamenco et les gitans», Editorial Filipacchi, París, Francia, reproducidos bajo licencia del autor. Textos de Manuel Martín Martín\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XXII Congreso de Arte Flamenco José Luis",
    "periodical": "candil",
    "issue_id": "1994-09",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "15-19",
    "page_number": 15,
    "word_count": 3472,
    "article_char_count_full": 21672,
    "article_char_count_review": 4983,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "artesano"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "entendido"
      }
    ]
  }
]
```
