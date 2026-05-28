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
    "article_id": "1991-05-21-left-rojo-el-alpargatero-en-la-uni-n-",
    "article_text_for_review": "Francisco J. Ródenas\n\nE n 1890 —y no después, como supone Manuel Yerga (*)—Antonio Grau, Rojo «El Alpargatero», se instala en La Unión con su esposa y sus tres hijos. Las ciudades natales de los mismos nos informan del periplo familiar previo por tierras andaluzas y murcianas: Antonio (Málaga, 1884), José (Sevilla, 1886) y Pedro (Cartagena, 1889).\n\nMuy probablemente, en esta última ciudad, la errante familia Grau valorase las posibilidades de la vecina y pujante villa minera de La Unión como destino de su futuro profesional, buscando, al mismo tiempo, un domicilio definitivo. Así fue: Antonio Grau Mora residirá en La Unión los últimos 17 años de su vida.\n\nRecién llegado, Antonio Grau regenta —a partir de 1890— un céntrico café localizado en el número 107 de la calle Mayor, donde se situaba, asimismo, su propio domicilio. Se trataba de un local alquilado, no muy espacioso, en torno a los 150 metros cuadrados de superficie, que el propio Rojo acondicionaría como café.\n\nEl elevado valor de la contribución abonada por el mismo —superior a las 300 pesetas anuales, sólo equiparable a 3 establecimientos similares en el conjunto del municipio, nos indica que se trataba de un local de cierta categoría (¿café cantante?).\n\nEl padrón de habitantes de 1895, efectivamente, recoge a Antonio Grau Mora, de profesión «cafetinero».\n\nCircunstancias probablemente adversas para la economía familiar le obligan al traslado («reconversión») de su negocio y de su domicilio. En 1896, el supuesto café cantante —rebajado su condición— pasó a ser establecimiento de venta de vinos y aguardientes en calle Bailén, cerca de su nueva residencia en calle Méndez Núñez.\n\nSerán sus vecinos los prebostes locales Pociano Maestre Pérez y Pío Wandosell Gil.\n\nAdvertimos entonces que la familia Grau comparte casa —supuestamente realquilado en su propio domicilio, no «posada» precisamente— con Juan Fernández Ruiz, jornalero almeriense, siéndonos desconocida cualquier otra vinculación de éste con el Rojo.\n\nLa nueva aventura comercial apenas perduró un año. A partir de 1897, Antonio Grau desaparece para siempre de la nómina de comerciantes locales. Este hecho resulta —cuanto menos— significativo si tenemos en cuenta que los supuestos $ \\tilde{\\text{reveses}} $ de fortuna? de El Rojo coinciden con el momento algido de la economía minera en La Unión (no deberían faltarle clientes).\n\nNada sabemos de su pericia personal y familiar sino hasta el momento de su muerte. Antonio Grau Mora, «Rojo el Alpargatero» para las musas, falleció el 21-4-1907 (no pudo entrar en el monumental Mercado Público abierto meses después) víctima de neumonía.\n\nAmortajado en caja de pino, dispuso de un urgente y modesto entierro en una fosa de alquiler (la número 72) del cementerio municipal de La Unión. Diez duros costó enterrar al patriarca de los cantes levantinos. ■\n\n(*) «Apuntes y datos para las biografías de Rojo el Alpargatero, La Trini, Chacón y Manuel Torre» / Manuel Yerga Lancharro. Col. Candil: Peña Flamenca de Jaén, 1981.\n\nFuentes documentales: Ultima investiga- ción realizada por el propio archivero Francisco J. Ródenas. Archivo Municipal de La Unión.",
    "title": "Rojo «El Alpargatero», en La Unión (1890-1907) Apuntes inéditos",
    "periodical": "candil",
    "issue_id": "1991-05",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 497,
    "article_char_count_full": 3144,
    "article_char_count_review": 3144,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-05-21-right-una-historia-en-coplas-la-traged",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOpinión\n\nE xiste un romance relativo a los gitanos de El Puerto de Santa María sobre el que ha recaído repetidamente la atención del investigador Luis Suárez Avila desde que, según cuenta, pudo recogerlo el año 1966 de un gitano octogenario, Juan Vargas Ortega, en la misma ciudad a que se refiere $ ^{(1)} $: Los gitanitos del Puerto fueron los más desgraciados, que a las minas del azogue se los llevan sentenciados. Y al otro día siguiente les pusieron una gorra y unas babuchas de esparto que el sentimiento me aboga. Y para darles más martirio, les pusieron un maestro que a todo el que no andaba listo a palitos lo dejaban muerto. Los gitanitos del Puerto fueron los más desgraciados, que se pueden comparar con los que están enterrados. Las sucesivas estrofas de este corrido figuraban ya,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"Publicado\"]\n\nque a todo el que no andaba listo a palitos lo dejaban muerto. Los gitanitos del Puerto fueron los más desgraciados, que se pueden comparar con los que están enterrados. Las sucesivas estrofas de este corrido figuraban ya, numeradas diferenciadamente, entre los martinetes recolectados por Demófilo en 1881 $ ^{(2)} $ y, ciertamente, con una variante léxica, no por pequeña menos interesante. En efecto, Demófilo aludía a «alpargatas de espar- (1) Publicado en «Gitanos de Cádiz y los Puertos», Diario de Cádiz, 27 febrero 1974, de todo esto escribe largamente en «El romancero de los gitanos bajo andaluces, germen del cante flamenco», págs. 563/607, de El Romancero. Tradición y pervivencia a fines del siglo XX. Actas del IV Coloquio Internacional del Romancero (23/26 junio 1987), Fundación Machado-Universidad de Cádiz, 1989. También, en «El romancero de los gitanos bajoandaluces. Del romancero a las tonás», págs. 29/94, de Dos siglos de flamenco. Actas de la Conferencia Internacional (21/25 junio 1988), Fundación Andaluzas de Flamenco, Jerez de la Frontera, 1989. (2) Colección de cantes flamencos recogidos y anotados por A. Machado y Alvarez (Demófilo), Ediciones Demófilo, Madrid, 1974, págs. 149/159. Divide Demófilo los textos en dos partes, la primera de las cuales incluye «corridos de tres, cuatro y cinco coplas, que vienen a formar, por decirlo así, pequeños trovos en los que se conservan hechos particulares que aún viven algunos en la memoria de los cantadores», con el núm. 5, pág. 151, figuran las tres primeras cuartetas del romance ofrecido por Suárez Avila. En la segunda parte, incluye «coplas sueltas, algunas de las cuales formaron parte de un trovo»; con el núm. 25, pág. 155, figura la última de las cuartetas del romance. to», y no a «babuchas de esparto»; dado que la documentación de este último tipo de calzado en nuestro idioma arranca de la segunda mitad del siglo XIX $ ^{(3)} $, ello parece decirnos que Suárez Avila encontró una versión moderna de un romance cuyo original, por otra parte, pudiera no ser t\n\n[ENDING CONTEXT]\n\nbajo las caldeadas tierras de Almadén.\n\nfermos tienen donde escupir, y al mismo tiempo las inmundicias, e insectos, piojos, chinches y pulgas de unos se comunican a todos», siendo muy difícil cualquier curación (AHN, Alma-dén, legajo 735).\n\n(23) Por falta de enfermería, se había habilitado una capilla anejía a la cárcel, pese a ser un lugar «reducido y falto de ventilación», según un informe de 16 agosto 1755; las camas estaban tan próximas que era imposible moverse entre ellas, «ni los en\n\nO'Donnell, 3, 4. $ ^{\\circ} $ Piso\n\nTeléfonos (954) 22 20 58 y 21 69 20\n\nParticular: Teléfono 27 80 78\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "¿Una historia en coplas? La tragedia de «Los gitanitos del Puerto»",
    "periodical": "candil",
    "issue_id": "1991-05",
    "year": 1991,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "21-25",
    "page_number": 21,
    "word_count": 3866,
    "article_char_count_full": 23940,
    "article_char_count_review": 3676,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "Publicado"
      }
    ]
  },
  {
    "article_id": "1991-05-25-right-ense-ar-a-quien-no-sabe",
    "article_text_for_review": "Al aficionado don Luis Gómez Coronado, con afecto:\n\nAlguien podrá decir de mí que soy excesivamente exigente aun en cosas fútiles, como en este caso.\n\nSoy, sencillamente, un hombre que lleva dentro de la órbita flamenca más de cuarenta años y sabe muchas cosas, unas nimias y otras majestuosas que hoy son harto difíciles de aprender por falta de maestros de altura.\n\nLa consulta que me hace don Luis es interesante, y aunque a «prima facie» pudiera parecer irrelevante, no lo es. Y no lo es porque siempre conocí a un gran número de «entendidos» y algunos cantaores profesionales que no sabían distinguir una granaína de una media granaína. Media granaína\n\nAsí, pues, esté tranquilo y no desespere, que no es usted el único que desearía saber qué es un cante por granaína.\n\nLa granaína es lo más parecido a cierto estilo por malagueña (por algo pertenecen al mismo árbol). Ejemplo: «A buscar la flor que amaba».\n\nSin embargo, en la media granaí-na sus tercios tienden a irse hacia arriba, a saber: «La que vive en la Carrera».\n\nEscuche ambos cantes y se dará cuenta de la enorme variación estilística que existe entre ambos.\n\nYo representaría esquemática-mente a ambas granaínas de estas dos formas:\n\nGranaína\n\nA provecho esta oportunidad para pedir a los directivos de la Comisión Organizadora del próximo Congreso a celebrar en Linares, que editen para los aficionados dos cassettes, una con cantes por granaína y otra por media granaína. Porque estoy seguro que de esta forma conseguiríamos terminar, de una vez y para siempre, con el confusionismo imperante desde que estos cantes fueron alumbrados.\n\nA continuación ofrezco, con mucho gusto, una relación de cantes muy conocidos en la voz del «Rey de la Granaína», Manuel Vallejo:\n\nGranaína:\n\nÉngarzá en oro y marfil Fue porque no me dio gana Si Dios me llamara a cuenta Era en el mundo envidiable A buscar la flor que amaba (1) Y no me la dejaron ver\n\nMedia granaína:\n\nLa que vive en la Carrera Viva el puente del Genil Si yo te quiero de veras Donde yo te conocí Adonde yo me pueda ir Nació la serrana mía Con un suspiro le pago Serrana que te olviara\n\n(1) Esta granaína la hemos oído muchas veces grabada o interpretada en directo, como malagueña. Así, precisamente, y por ejemplo, la tiene grabada Terremoto de Jerez. Representante:\n\nEspectáculos Internacionales\n\nO'Donell, 3.°-A, piso Teléfs. 222058 - 216920 Particular: 228078 41001 SEVILLA",
    "title": "Enseñar a quien no sabe Manuel",
    "periodical": "candil",
    "issue_id": "1991-05",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 406,
    "article_char_count_full": 2401,
    "article_char_count_review": 2401,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-05-26-left-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nC on la despedida del año llegaba a las librerías el libro que hoy reseñamos. Simultáneamente, recibíamos la noticia de que el responsable de esta bellísima edición, Andrés Raya Saro, se despedía con este título de una actividad empresarial que le ha llevado a ser el celoso cuidador de la edición de unos cincuenta títulos, la mayoría de ellos sobre flamenco, de muchos de los cuales les hemos ido dando cuenta a través de estas páginas. Para nosotros es un motivo de alarma y preocupación que gente de este talante humano y profesional deban abandonar la tarea de editar libros sobre nuestro arte por la falta de clientela sobre los mismos. ¿Dónde están los flamencos que lo saben todo?, ¿dónde se forman las nuevas generaciones que se aproximan a lo jondo?... Dejemos las respuestas, por ahora,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"reconocimiento\"]\n\nste talante humano y profesional deban abandonar la tarea de editar libros sobre nuestro arte por la falta de clientela sobre los mismos. ¿Dónde están los flamencos que lo saben todo?, ¿dónde se forman las nuevas generaciones que se aproximan a lo jondo?... Dejemos las respuestas, por ahora, en el aire; las casas de discos se quejan, igualmente, de la falta de rentabilidad de los productos flamencos. Algo extraño ocurre. Pero quede claro nuestro reconocimiento al señor Raya por su entrega editorial, por perder dinero para que nosotros se pamos un poco más sobre este mundo tan confuso. El libro de Eugenio Cobo, prologado por unas hermosas palabras de José Blas Vega, en las que éste profesa marchenismo por los cuatro costados, es también una reivindicación del cantaor sevillano. Los que conocemos a Eugenio sabemos de su apasionada, rabiosa a veces, defensa de Pepe Marchena, que Cobo realiza en artículos, congresos, conferencias de todo tipo. En esta ocasión, y como reza el título, no sólo se aproxima a la creación artística, cantaora en este caso, de Marchena, sino a su perfil humano, señalando su talante abierto a la amistad, su desprendimiento material, que lo llevaba a gastar con «Vida y cante del Niño de Marchena» los amigos todo lo que ganaba, sus aficiones (mujeres, lotería). En todos los aspectos de esta rica y compleja personalidad artística y humana, Eugenio se detiene con el entusiasmo del convencido que intenta, al mismo tiempo, convencer. Para ello se vale también de testimonios de otros periodistas (interesantísimas las reseñas sobre sus actuaciones en vivo de hace cuarenta o cincuenta años), de cantaores diversos y hasta supuestamente adversarios, como en el caso de Antonio Mairena, y, en\n\n[ENDING CONTEXT]\n\nque hace ya años que disfrutamos de sus enseñanzas. Sentémonos bajo esta parra andaluza de sabiduría y que su fresca sombra refresque los excesivos e inútiles ardores actuales. Amén.\n\nNOTA: Ante las numerosas preguntas de amigos que se interesan por el fondo editorial Virgilio Márquez, les comunicamos que pueden solicitar información, y efectuar igualmente pedidos de libros, dirigiéndose a: Andrés Raya Saro, Apartado 3.098. 14080. Córdoba.\n\nCarlos Cruz\n\nRepresentante:\n\nJ. A. Pulpón Espectáculos Internacionales\n\nO'Donell, 3.°-A, piso Teléfs. 222058 - 216920 Particular: 228078 41001 SEVILLA\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1991-05",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 990,
    "article_char_count_full": 6139,
    "article_char_count_review": 3361,
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
  },
  {
    "article_id": "1991-05-27-left-enderezando-entuertos",
    "article_text_for_review": "Léo la revista «Sevilla Flamenca», número 68 y, como hago con todas, no suelo leer más que los trabajos cuyos títulos me atraen. ¿Por qué? Pues sencillamente, porque de esta forma me evito tener que enderezar todos los «torcidos» que puedan aparecer en las mismas.\n\nRecordará, señor Ríos Vargas, que no hace mucho tiempo tuve que librarle de «los cuernos» de un torito andaluz muy flamenco, residente en la ciudad de Badalona, quien, por cierto, está bastante bien enterado y con toda la razón del mundo le decía cosas que él sabía y usted ignoraba. Yo me entrometí entre ambos, a través de la revista, para quitar todo el «hierro» posible al tema, explicando al aficionado que, tanto usted como yo, y como todo aquél que escribe lo que le han contado, estábamos expuestos a publicar algo inexacto. Que si alguna vez hemos equivocado a los lectores, es porque nuestros informantes nos han equivocado por mala intención o por ignorancia.\n\nHoy me veo obligado a salir a su encuentro con este trabajo porque he leído en la revista, «Sevilla Flamenca», algo incorrecto relacionado con mi inolvidable y llorada «Niña de los Peines».\n\nSobre Pastora me va a permitir que por el momento no revele el por qué la Niña convivió dos o tres años con La Trini en Málaga. Ni tampoco cómo organizó «La Trini» su vida inmediatamente después de que- darse viuda y de perder a su única hija. Usted afirma que Pastora llevó en tercero o cuarto lugar el apellido Carrillo y que por eso fue prima de Trinidad. Quisiera saber cuál es su fuente de información sobre esa relación de consanguinidad entre ambas cantaoras.\n\nNo es así. No es cierto que fuesen primas por cuanto Pastora fue gitanísima y «La Trini» no.\n\nSu versión no se ajusta en nada a la realidad. Yo puedo ofrecerle el árbol genealógico completo de Pastora para que pueda ver que el apellido comentado no consta en el mismo.\n\nRecomiendo a nuestros lectores que se trasladen a Málaga. Puedo asegurarle que no le será difícil localizar a algún sobrino-nieto de «La Trini». Ella tuvo siete hermanos y la proliferación familiar forzosamente será extensa. Yo tuve la suerte de localizar la persona que me habló de su tía abuela. Se trata, como digo, de una extensa familia no gitana.\n\nY termino, señor Ríos Vargas, ofreciéndole algunos datos relativos al árbol genealógico de Pastora Pabón Cruz «Niña de los Peines»:\n\nPadres: Francisco Pabón, de Viso del Alcor Pastora Cruz, de Arahal\n\nAbuelos (maternos): Tomás Cruz, de Arahal Gracia Vargas, de Arahal\n\nAsí, pues, los cuatro apellidos de Pastora son: Pabón, Cruz, Cruz, Vargas.",
    "title": "Enderezando entuertos",
    "periodical": "candil",
    "issue_id": "1991-05",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "27-27",
    "page_number": 27,
    "word_count": 439,
    "article_char_count_full": 2564,
    "article_char_count_review": 2564,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
