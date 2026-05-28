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
    "article_id": "1980-01-6-right-martos-en-la-geografia-de-los-ca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nProloguillo inicial\n\nPor MIGUEL CALVO MORILLO\n\nDesde los años cuarenta buscar testimonios escritos de la historia de Martos, tanto de los siglos pasados como del presente, es una labor difícil y desconsoladora. Archivos, bibliotecas y colecciones particulares ardieron o desaparecieron en la guerra de los tres años —que diría Antonio Galla—. En y después del treinta y seis, en y después del treinta y nueve, por ese miedo atávico —miedo razonado por múltiples causas— al papel escrito o impreso, por ignorancia de unos y de otros, fue destruido y con ello cuanto podía ser fuente de información veraz y fidedigna del pasado.\n\nPor esta causa, las noticias del Martos de principios de siglo, salvo raras excepciones, las recibimos a través de la tradición oral, deliciosa forma por la cual llegaron\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"leyendas\"]\n\nel treinta y nueve, por ese miedo atávico —miedo razonado por múltiples causas— al papel escrito o impreso, por ignorancia de unos y de otros, fue destruido y con ello cuanto podía ser fuente de información veraz y fidedigna del pasado. Por esta causa, las noticias del Martos de principios de siglo, salvo raras excepciones, las recibimos a través de la tradición oral, deliciosa forma por la cual llegaron hasta nuestros días romances, cantares y leyendas con toda su verdad intacta, de esta guisa, supe, siendo yo mozo, la existencia de los Cafés Cantantes en la ciudad de la Peña. Estamos a principios del siglo XX. La histórica Villa, hoy ciudad, se ha transformado enormemente. La desamortización de Mendizábal, la pérdida del poder de las Ordenes de caballería y militares, y otras razones, causan un gran cambio en la sociedad marteña. Los desmontes efectuados en el primer tercio del XIX, tales como los de Monte Nuevo (Monte Lope Alvarez), la Sierra de la Graná, Los Villares Alto y Bajo, la Encomienda de Víboras, las Pedrizas, etc., han convertido, en medio siglo, cotos y monte bajo en olivares en plena y a\n\n[ENDING CONTEXT]\n\ncalidad de sus conocimientos. Mi recuerdo a mis amigos, ya muertos, Pepito Pulido más de cincuenta años de camarero, y Pepe Márquez Villar, que fue crupier con Pepe Pinto, en Sevilla, en la casa de juego que tenía el comprovinciano Simeón Escabias, de Valdepeñas de Jaén, éstos y mi padre, junto con Juan Yeguías «El Rubio Paneras», me relataron la historia de los cafés cantantes de Martos, historia que ni por asomo yo pensara que viera la luz impresa en una revista dedicada a fomentar el conocimiento y grandeza de los cantes de nuestra bendita Andalucía, y que ampliaremos en próximos trabajos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Martos en la geografía de los Cafés Cantantes",
    "periodical": "candil",
    "issue_id": "1980-01",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "6-8",
    "page_number": 6,
    "word_count": 1627,
    "article_char_count_full": 9409,
    "article_char_count_review": 2750,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "leyendas"
      }
    ]
  },
  {
    "article_id": "1980-01-8-right-quejio",
    "article_text_for_review": "¿Quién ata ese desgarro? ¿Quién recoge el grito por los amplios pasillos del silencio y el desamparo? ¿Quién enjuga el sudor a tanto ahogo? ¿Por qué una hoz conduce los negros borbotones de la garganta? ¿Por qué su casa se puebla de cuchillos? ¿Por qué angustia al candil y cruje en las paredes la cal morada, loca en la libertad de la pena y el sollozo: la razón que ensancha el grito y desborda el ojo inmenso de las guitarras? ¿Quién muere en este rito de la desolación amarrado al recuerdo en la piel de la sangre? ¿Quién le presta el hombro hermano a este aullido para que encuentre la rama en que posar su herida? ¿Por qué no hay más puerta ni otra afirmación que la del dolor para agarrarse? ¿Por qué enturbia el vino y anuda las aneas de las sillas a los espejos? ¿¡Ay!! ¡Cómo duele en los pulsos el relincho de ese golpe de azada en roca viva! Quién explica el lamento de tijeras que anuncia espantado la salida del cante?",
    "title": "Quejio",
    "periodical": "candil",
    "issue_id": "1980-01",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 175,
    "article_char_count_full": 931,
    "article_char_count_review": 931,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-01-9-right-a-antonio-cuevas-el-piki-devorad",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEra por siguiriyas. Sí, era por siguiriyas. El tormento y la comezón que me turbaron aquella noche de invierno era un infierno por siguiriyas. La calle San Juan de los Reyes es como una cinta transportadora de silencio. Cuantos subíamos por ella camino del Albaicín, en noches de sábado alegres, con la primavera verde de los veinte años sobre la espalda, callábamos sin ponernos de acuerdo, como por un pacto tácito, en cuanto llegábamos a la plazuela del Aljibetrillo y la Alhambra empezaba a hacernos guiños desde su perspectiva plana, de mala pintura de aficionado. Aquel silencio estaba lleno de plenitudes, sabíamos que arriba aguardaba el reino de las sombras transparentes, de la cal emocionada hecha poema, de las plazuelas de San Miguel o San Nicolás, que cada uno pensaba habían sido\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"pobres\"]\n\nnosotros sólo nos callaba la calle San Juan de los Reyes, cuando en la noche buscona y picaresca de los sábados subíamos solos, o acompañados por el primer amor fugaz de nuestras vidas, en busca del Albaicín: Callejuelas sin salía buenas para enamorar pero muy comprometías Y de repente, aquella noche de invierno de 1968, el mágico silencio se quebró con un aullido irreconocible. Era junto a la puerta falsa del colegio del Ave María, colegio de pobres, de los de cultura menguada, sin subvenciones estatales, bachilleres del hambre andaluzas con los que nos hermanábamos en nuestro deambular sabático. En aquel sitio la tortuosa callejuela se abre en abanico como el estuario de un río que fuese a estrellarse contra el muro de Sierra Nevada que se adivina a lo lejos transparente. El aullido lejano, de garganta ancestral y transhumana, que parecía venir volando por encima de todos los tiempos muertos de la historia, nos atrajo como un fuego irremediable. Al acercarnos vimos un pequeño espacio protegido por una pueblerina puertecilla de cristales, que, empañados por el frío de la noche, no nos permitía ver el interior. Pero no importaba, el gemido sobrehumano continuaba brotando, haciéndose cada vez más articulado, más fácilmente reconocible, poblando de esquinas aquellos silencios anteriores, hasta convertirse en una protesta viva, un rebelarse contra la mala suerte, la marginación, el fario, de la gente humilde a la que no le sale ni una a derechas porque (undebé lo quiere) o (me cago en la leche que mamé) hasta las leyes de la naturaleza son contrarias al que nada tiene, sino su voz para quejarse. Qué desgracia tengo mare hasta en el andar que los pasitos que palante doy se me\n\n[ENDING CONTEXT]\n\nde la incertidumbre, del miedo, y te clavó para siempre el asta de la soledad. Recorriste medio mundo, preguntaste en todas partes por tu memoria perdida y recibiste sólo la quemazón, el odio de la gente a la que intentabas abrazarte. Persecuciones, o Dios le ampare hermano, la noche por compañera. Ahora cantas, definitivamente solo, pateando con rabia la tierra, como si quisieras matar para siempre la esperanza: que los pasitos que palante doy se me van patrás mientras los dioses solares entristecen como si escucharan tu protesta muda, tu viejo cante por siguiriyas.\n\nJOSE LUIS BUENDIA LOPEZ\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A Antonio Cuevas, «El Piki», devorado por el tiempo de repente",
    "periodical": "candil",
    "issue_id": "1980-01",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 1141,
    "article_char_count_full": 6799,
    "article_char_count_review": 3323,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "pobres"
      }
    ]
  },
  {
    "article_id": "1980-01-12-right-la-saeta-de-ja-n",
    "article_text_for_review": "Por Antonio Almendros Soto\n\nEl primer recuerdo que tengo de la saeta es el escucharla a los aceituneros en Valcrespo. Era yo muy pequeño.\n\nAlgunas noches, sin duda cuando la lluvia hacía presentir la holganza al día siguiente, organizaban juegos y representaban «pasillos» llenos de ingenuidad e imaginación. Recuerdo la representación que hacían de un paso de Semana Santa que salía de San Ildefonso y reproducía el momento en que Jesús era azotado por dos sayones. Aún siento escalofríos cuando rememoro al aceitunero que hacía de Redentor, desnudo de cintura para arriba en la noche decembrina.\n\nLas saetas eran las típicas de Jaén. Unas saetas hondas, tristes, sin barroquismos y por eso precisamente más conmovedoras. Tenían deje de canto gregoriano, no había quiebros en la voz y recordaban el cantar de los yunteros en la besana: pocas inflexiones, recia la voz, con el desgarro del que ha de permanecer inclinado sobre la esteva horas y horas, dando su sudor, sus energías y su canto, para conseguir el pan y hacer que no fuera demasiado amargo.\n\nEn aquella Semana Santa humilde y sencilla de nuestro Jaén de principios de siglo, sin oropeles ni excesivo terciopelo, con la austeridad ascética de los nazarenos de las «Siete Escuadras», que enseñaban el borde del pantalón bajo la parda túnica, los «luceros de dos en dos» so-naban empañados, opalescidos por las lágrimas.\n\nPara más realismo, alguno o alguna entonaba una saeta, mientras alguien redoblaba en la gruesa puerta de la enorme cocina-escenario, lo que con buena voluntad sonaba a tambor. Mis sentimientos de chiquillo me identificaban plenamente con el drama de Jesús y vivía íntimamente cada momento de su Pasión. La saeta escuchada a los presos desde la Ropa Vieja, era bálsamo que ungía mis heridas sentimentales.\n\nLlegaba muy profundo y uno al escucharla se sublimaba como si sobre las sienes de Nuestro Padre Jesús rociaran jazmines hechos cante.\n\nAl final sonaba estruendosa una ovación que rompía el hechizo de las notas aún vibrantes al mezclarse con los clarines de los «romanos».\n\nPasó el tiempo... Y con el paso se modernizó la copla. Una chica sevillana, en la plaza de Santiago, al comienzo de los años veinte, desgrana-ba las sofisticadas notas de la saeta de Andalucía baja.\n\nLas lágrimas, al paso de la Virgen, eran rocío, envueltas en el gorgeo de la seguidilla, en la noche vivificante de la primavera giennense.\n\n|Qué bonita cantaba la saeta!\n\nPero añoro aquella humilde saeta de Jaén, en labios de aceituneros; al rescoldo de los troncos que se quemaban en el fogón, del que subía el olorcillo a las rosetas recién hechas, depositadas en enorme tinajón. y alguien, con los nudillos, remedaba el redoble del tambor, en los maderos de una vieja puerta.",
    "title": "La «Saeta» de Jaén",
    "periodical": "candil",
    "issue_id": "1980-01",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 454,
    "article_char_count_full": 2740,
    "article_char_count_review": 2740,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-01-13-left-alfredo-arrebola-embajador-flame",
    "article_text_for_review": "En fechas muy recientes hemos tenido la gran suerte y mayor placer de poder recrear nuestro oído, fantasía y gusto flamenco, escuchando unas sobrias, medidas y valientes interpretaciones —verdaderas oraciones— de diversos cantes de la mejor solera y prosapía flamenca a cargo de una figura tan insólita, polifacética y recia, cual es la de Alfredo Arrebola, que actuó en el amplio salón de actos de la Delegación de Cultura de nuestra ciudad «a palo seco» (es decir, sin socorrida «electrónica» amplificadora), dentro del marco de actividades de «Extensión Universitaria», organizadas por la Facultad de Ciencias de Cádiz.\n\nAntes de seguir con el comentario de su arte y actuación concreta en Cádiz, no estará de más bosquejar en gruesos trazos un breve apunte biográfico de la curiosa trayectoria humanística y artística de Arrebola, que configuran una personalidad única en su género.\n\nHijo de padres también «cantaores», Alfredo Arrebola nació en Villanueva Mesía, pueblecito situado en la vega de Loja (Granada) donde pasó sus años de adolescencia dedicado a las faenas del campo. De su temprana vocación flamenca da idea el hecho de que a los 10 años debutase en un teatro junto con José Palanca, Niño de la Isla, Niña de Antequera y otros artistas. A los 18 años ingresó en el aspirantado de los padres Salesianos de Antequera y posteriormente pasó cuatro años en el convento de los Padres Capuchinos, vocación que, sin embargo, no cristalizó definitivamente. Posteriormente se hace maestro nacional y más adelante se licencia en Filosofía y Letras, Sección de Lenguas Clásicas, alternando sus estudios con recitales de cante flamenco. Recientemente y ello constituye un antecedente tan original como único, se ha doctorado en la Universidad de Granada sobre temática socio-flamenca, ilustrando su disertación ante el tribunal calificador con escogidos ejemplos de diversos cantes, acompañado a la guitarra por el excelente guitarrista Manuel Cano, para mejor ilustrar las hipótesis y argumentos sobre los que basa sus conclusiones de Tesis.\n\nAparte de otros muchos méritos, cuenta de modo muy destacable en el haber profesional de Arrebola el hecho de dedicar especial atención a la introducción del cante flamenco y de sus motivaciones en los medios universitarios de muchos distritos españoles, conjugando su erudición humanística con sus excepcionales facultades y depurado estilo como «cantaor» flamenco, que le permite actuar simultaneamente en la doble vertiente de conferenciante-cantaor, cual corresponde a su formación profesional y a su sensibilidad artística y vocacional como intérprete. Es autor de numerosos artículos periodísticos y trabajos de investigación sobre temática que ahonda en motivos, orígenes e interaciones de la idiosincrasia del hombre andaluz y de su ambiente regional sobre esta manifestación única, verdadero tesoro universal y patrimonio artístico genuinamente andaluz, como es el cante flamenco; simbiosis lograda de ecos extinguidos de múltiples razas, civilizaciones y culturas pasadas, de las que Andalucía fue anfitriona de excepción. Arrebola dirige con acierto y con pasión el «Aula de Flamencología», encuadrada dentro del Vicerrectorado de Extensión Universitaria de la Universidad de Málaga, que mantiene una estrecha conexión con la célebre y activa peña flamenca local de «Juan Breva».\n\nNos deleitó Arrebola con muy cuidadas, sobrias, entonadas y valiosas interpretaciones de cantes por malagueñas, «siguirías» livianas y cabales, cañas y «tonás», viéndose obligado a interrumpir su recital «a media faena» debido a un lamentable y trivial percance laboral que se cebó en las uñas de su joven y prometedor guitarrista acompañante Enrique Campos.\n\nLos universitarios gaditanos hemos tenido, al fin, una excelente oportunidad de escuchar y poder apreciar en toda su dimensión a un gran cantaor flamenco y a un erudito conocedor de la amplia, sugestiva, intrigante y arcana temática flamenca, que le distingue netamente de tanto «flamencólogo» estereotipado, diletante y sabihondo como hoy anda suelto por el mundo, sumido en peregrinas teorias, falaces invenciones y fantasías elucubrantes de lo indemostrable.\n\nJ. A. Pérez-Bustamante de Monasterio\n\nCuenta Arrebola en su haber profesional con muchos y valiosos premios, distinciones y menciones honoríficas que acreditan su valía personal y el alcance de su interpretación, otorgados en Madrid, Córdoba, Jerez y Málaga. Y este es, señores, en resumen, nuestro hombre, Alfredo Arrebola nada más y nada menos.",
    "title": "Alfredo Arrebola, embajador flamenco en los medios universitarios de Cádiz",
    "periodical": "candil",
    "issue_id": "1980-01",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 683,
    "article_char_count_full": 4510,
    "article_char_count_review": 4510,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
