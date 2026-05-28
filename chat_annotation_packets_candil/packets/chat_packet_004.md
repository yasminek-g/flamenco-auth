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
    "article_id": "1979-11-13-right-canalejas-de-puerto-real",
    "article_text_for_review": "Yo me asomé a la muralla y me respondió el viento, de qué te sirven tantos suspiros si ya no tiene remedio.\n\nEn ocasiones se siente la necesidad de ir al pasado para hacerlo, nuevamente, vivir. Y es posible, gracias a ese trozo de recuerdo hecho fotografía, disco antiguo o un viejo y amarillento recorte de periódico. Hoy recuerdo viendo el programa, arrugado y maltrecho por el paso del tiempo, que anunciaba una Noche Flamenca, —hace ya años—, en la Plaza de San Nicolás de Granada. Como recordar es vivir, un nombre se me agranda ahora. Canalejas de Puerto Real figuraba en aquel cartel. Entonces, juntos aquella noche, compartimos el aplauso de la afición. Ese aplauso que el viento se llevaba desde la Torre de la Vela para bajar hasta la vega; rica y fértil tierra, hoy vendida y pisada por el progreso y el cemento. Pero, no nos perdamos en el recuerdo. Hoy, desde esta atalaya verde-esperanza, quisiera, en la sencillez de unas palabras, hablar con la brevedad de lo justo, de su arte. Canalejas de Puerto Real, aunque nacido en Puerto Real, yo diría que estuvo y está —nunca muere lo que no queremos que muera—, inserto en la tierra del olivo y la mina. Por ello a la natural vivacidad rítmica que su nacencia otorgó a su voz, salada claridad de un sonoro duende, Jaén enriqueció su decir impregnándolo de sugerentes acentos. Canalejas de Puerto Real, marinero en tierra, echó el ancla familiar aquí en Jaén, dejando libre el cabo de su arte para recorrer todos los puertos flamencos de la geografía cantaora.\n\nFue su momento, época de cante con diferentes versiones y muy diversa aceptación. Canalejas de Puerto Real, guardó siempre el fiel equilibrio de un hondo sentido profesional, junto al amplio conocimiento de un arte que dominaba. Canalejas de Puerto Real, ¡sonora expresión su nombre!; Juan Pérez Sánchez en su carnet de identidad vivió y sintió Jaén. En Jaén dejó el fruto de una amistad que llegaba a todas partes. En Jaén dejó el fruto de una familia y Jaén supo darle a él y a su cante toda la nobleza que encierran sus piedras, la sabiduría de muchos siglos de historia, la natural elegancia del olivo. Cómo no recordar el eco de su lamento en una oración-cante al Abuelo, a Nuestro Padre Jesús,\n\nDe oro son las potencias\n\ny la corona de espinas.\n\nY Tú la llevas con paciencia\n\nsobre tus espaldas divinas\n\nla cruz de la penitencia.\n\nPero..., dejo de pensar escribiendo. Voy a escuchar su voz; la técnica lo hace posible,\n\nEl otro día fue Ramona\n\npor cisco a la fundición,\n\nlos picaros de las minas\n\nquerían robarle el honor.\n\nEs el mejor homenaje y reconocimiento que puedo hacer a Canalejas de Puerto Real, a su mujer y a sus hijos.\n\nJuan Antonio Ibáñez",
    "title": "Jaén en el Cante. Canalejas de Puerto Real",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 472,
    "article_char_count_full": 2679,
    "article_char_count_review": 2679,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1979-11-14-right-quienes-fueron-los-maestros-fran",
    "article_text_for_review": "Quienes fueron los maestros...\n\n«El Fillo» es uno de los cantaores que más fama han cosechado dentro de la historia del arte flamenco. Se cree que nació en Puerto Real, aunque el cantaor gaditano Aurelio Sellé opina que era natural de la Isla de San Fernando. Existen igualmente dudas sobre su nombre de pila verdadero, si Francisco, Antonio o Diego; pero siguiendo las noticias de «Demófilo» y Rodríguez Marín, su nombre era el de Francisco. Nacido a principios del siglo XIX debió, de pasar su niñez en su pueblo natal, trasladándose en su adolescencia a Sevilla y más concretamente a Triana. De raza gitana y voz ronca y quebrada, voz que desde él se conoce como «afillá», fue uno de los primeros maestros del cante flamenco, teniendo discípulos famosos como su sobrino Tomás «El Nitri». Francisco Ortega Vargas «EL FILLO»\n\nEn Triana, «El Fillo» se enamoró de «La Andonda», gitana mucho más joven que él, nacida en Morón y genial intérprete de Soleares.\n\nSegún Ricardo Molina, este cantaor, aunque residente en Sevilla, pasó temporadas en cor-tijadas y ventas de los alrededores, desplazándose también a localidades como Lebrija, Alcalá, Morón, Utrera y Jerez. Siguiendo los datos de Ricardo Molina en Morón, un niño, hijo de militar italiano y de madre española, cada vez que sentía cantar a Francisco, acudía tímidamente y acababa por acercarse tanto al cantaor, que éste lo tomaba en sus brazos y lo sentaba sobre sus piernas. Aquel niño se llamaba Silverio Franconetti. Aquella actitud de Francisco Ortega para con los niños mientras cantaba, fue reflejada en una letra por soleá que dice:\n\n«La Andonda le dijo al Fillo: ¡Anda y vente, pollo ronco, a cantarle a los chiquillos!».\n\n«El Fillo» vivió y murió pobre y como en aquella época los cafés cantantes estaban empezando, se cree que este cantaor hizo sus cantes en el más antiguo del que se tienen noticias y que se llamaba «Café de los Cagajones», pero se cuenta que el que quería escuchar a Francisco Ortega por derecho y en su ambiente, tenía que cruzar el puente de barcas que llevaba al gitano barrio de Triana.\n\nDentro del cante gitano, Francisco Ortega fue un cantaor general que en todas las modalidades sobresalió y dejó la huella de su\n\npoderosa personalidad. Su nombre tiene valor magistral en la caña, el polo, la toná, la soleá, el romance y la siguiriya, principalmente en esta última y cómo no, en las cabales, si éstas se consideran clasificadas aparte de las siguirivas. Respecto a las cabales, se cree que se han transmitido hasta nuestros días, tres creaciones de Francisco Ortega y que fue éste el que realizó la separación de la rama materna de las tonás trianeras. Además de las tres cabales, existe una siguiriya que comienza «Matastes a mi hermano…», y en la que se observa un cierto aire de los cantes de Tomás «El Nitri», que seguramente fue su principal transmisor.\n\nDe otros cantes de Francisco Ortega, uno de los que con más pureza ha llegado a nuestra época, ha sido la caña, transmitida por dos grandes maestros como son Silverio Franconetti y Diego Bermúdez Cala «El Tenazas de Morón», éste último, igualmente discípulo suyo.\n\nDe la fecha de su muerte se tienen pocos datos y se cree que murió probablemente en Sevilla hacia 1850.\n\nLA ANDONDA\n\nCitando a Ríos Ruiz, «La Andonda fue una de las más legendarias mujeres flamencas y es tenida por gitana de rompe y rasga. Nacida en Morón, en la primera mitad del siglo XIX, vivió en Triana, donde se hizo famosa como genial solearera».\n\nComo observamos en los datos biográficos de Francisco Ortega «El Fillo», estuvo amancebada con él y parece ser que los cantes por soleá de Tomás Pavón, son transmisiones de los cantes de «La Andonda».\n\nSelecciona: Rafael Valera",
    "title": "Quienes fueron los maestros. Francisco Ortega Vargas «El Fillo»",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 632,
    "article_char_count_full": 3702,
    "article_char_count_review": 3702,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1979-11-15-right-discograf-a-flamenca",
    "article_text_for_review": "DISCOGRAFIA FLAMENÇA\n\nCiertamente es escasa la producción discográfica que en este tiempo ve la luz —el premio nacional al mejor disco flamenco de 1979 que otorga la cátedra de Flamencología de Jerez, ha quedado desierto, por estimar el jurado que lo grabado este año no tenía los suficientes méritos para conceder el galardón—, y las motivaciones bien pudieran ser varias y, sin lugar a dudas, objeto de un estudio que analizara desde el momento artístico del cante hasta el interés de la industria fonográfica por el flamenco, sin olvidar el conocimiento que los productores puedan tener de nuestro arte. De una colección de viejas grabaciones en reconstrucción técnica y de lo último realizado por dos jóvenes artistas, nos ocuparemos en este número de «CANDIL».\n\nCOLECCION: LOS ASES DEL FLAMENCO. EMI-ODEON, S. A.\n\nDe singular acierto podemos calificar la idea del sello EMI de ofrecer al aficionado de hoy, una serie de discos de especial categoría, algunos y que bien pueden servir para llegar a un mejor conocimiento de viejos estilos del cante, que ahora cobran especial resonancia al vivir momentos de incertidumbres y titubeantes variaciones artísticas, en ocasiones poco afortunadas.\n\nEnjuiciar uno por uno cada disco, supondría un extenso comentario que, por motivos de\n\nespacio es imposible. No obstante, recoger en síntesis y de forma generalizada la jondura, el decir de cantaores de la talla de un Manuel Torre, don Antonio Chacón, Tomás Pavón, El Gloria, etc. que desfilan por la colección discográfica, exponiendo cada uno su personal estilo de sentir, vivir y comunicar el cante, y que representan toda una página de la historia del flamenco. Notamos, eso sí, la ausencia de grabaciones de Juan Mojama. En aquellos viejos discos de pizarra hay muy interesantes muestras de su hacer jondo. Espremos que aparezcan.\n\nNuestra enhorabuena a la casa discográfica EMI por el sonoro regalo que está dando a la afición.\n\nDISCO: MARIA DEL AMOR. CANTA: LUIS DE CORDOBA. TOCAN: ENRIQUE DE MELCHOR E ISIDRO SANLUCAR. PHILIPS. Referencia 64 29 898.\n\nCasi desde sus inicios profesionales hemos venido siguiendo el hacer artístico de este joven cantaor cordobés, ya con una muy apreciable producción discográfica en su haber. En el disco que nos ocupa, Luis de Córdoba, con las guitarras de Enrique de Melchor e Isidro Sanlúcar, dice tangos, graninas, bulerías, siguirias, guajiras, fandangos del gloria, soleares, tarantos, para terminar con un romance. Diriamos que el trabajo es coherente. Hecho con sentido profesional, con intentos serios de configurar cada estilo según su conocimiento, asimilado —creemos—, en duras sesiones de aprendizaje, con conocedores profundos de lo que es el cante, a la vez que imprime un tono actual a sus interpretaciones, conjugando tradición cantaora y nuevos aires flamencos. El camino a recorrer es amplio y Luis de Córdoba lo está haciendo con decidido y firme paso, aunque el peligro de desviarse siempre está presente en el mundo artístico. Nos gustaría que en próximas grabaciones se desprendiera de su rigidez interpretativa para que, siendo fiel a lo aprendido, abriera su decir a unos cauces de mayor expresividad y comunicación.\n\nDISCO: A MI SEVILLA. CANTA: EL CHOZAS. TOCAN: ISIDRO CARMONA Y PEDRO BACAN. BELTER. Ref. 2.27 080.\n\nAnteriormente decíamos que vivimos tiempos de indefiniciones artísticas. En los jóvenes cantaores, generalmente predomina un sentir innovador cimentado no sabemos sobre qué bases de conocimiento y experiencia artística. El hecho es evidente y se puede comprobar en festivales, recitales y, lógicamente, en el mundo del disco. No se nos tache de puristas, pero es nuestra opinión, todo hacer debe tener unas coordenadas que conlleven a una obra digna. Y la introducción puede servir para analizar el último disco de «El Chozas», cantaor inmerso en esa corriente innovadora a que hacíamos referencia. Con las guitarras de Isidro Carmona y Pedro Bacán, el disco contiene Bulerías, soleá, tangos, malagueñas, bulerías por soleá, taranta, siguirias y cantiñas. Con aciertos en la grabación, hay sin embargo —no queremos decir desconocimiento—, sí un querer hacer el cante en una personal concepción y el resultado no es muy halagüeño. Desde nuestra visión le sugerimos a «El Chozas», sugerencia que puede servir para otros artistas, que arrope su capacidad con un claro conocimiento de lo que es y significa el cante, como hecho, que va más allá de decir unas letras con más o menos vivacidad rítmica y melódica. Confiamos en que «El Chozas» ponga su buena voz flamenca —que la tiene—, al servicio de un mayor saber. Sin lugar a dudas, supondría una dimensión nueva para la trayectoria del joven artista.\n\nDOSCANDIL",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 749,
    "article_char_count_full": 4689,
    "article_char_count_review": 4689,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-01-3-right-editorial",
    "article_text_for_review": "U n Candil alumbraba el cante\". La memoria está henchida de sangrantes onerosidades sobre este pueblo. Memoria para dignificar las viejas cicatrices y recuperar la historia nunca escrita del cante. La memoria que esconde como un relato bíblico, tanto vigor errante, tantas voces amordazadas, tanto equipamiento de ternura y de desesperación, en un CANDIL La diminuta llama fulgurece en la cueva, se estremece; una lengua divina que corona toda la estatura de la soleá, como a un após-tol. Allí se mentan el clamor de las persecuciones, la postración de una cultura, el alijo de alegría que nuestro pueblo guarda, irrenunciablemente.\n\n\"Las sombras del Candil... cubren los largos llanos del Sur con un silencio metálico y racial, en donde la pequeña llamita que inmortalmente atraviesa la región y los siglos, siembra en los campos un poco de luz, apenas nada, una señal, la compasión. Todo el Sur ha quedado ahora untado por esa despaciosa y casi clandestina humareda de sombras de Candil... Sopla al viento de unos siglos de herrumbre y de persecución y se mueve la llama, y se mueven las sombras con una bárbara lentitud, hacia atrás, hacia el penoso siglo XVI... Retroceden las sombras hacia las sombras, y el candil comprensivo; testamentario alumbra el abismo de unos centenares de años que, como monstruos, fueron famosos y temidos, provocaron leyendas y odio, y a veces cierta misericordia. Las sombras de candil, la llama pobre del candil, ponen un lento luto de música sobre el luto brutal que quema a dos comunidades abandonadas.\n\n\"Un Candil alumbraba el cante\". La memoria de Félix Grande, tan extremecedora y entrañable, es también nuestra memoria; una memoria a la que este pueblo, tremendamente desconocido todavía, no puede, no quiere renunciar, \"porque, como deciamos en la Editorial del primer número de nuestra revista, lo nuestro es el Candil: la luz diminuta sobre esta manifestación del misterio, en cierto modo, inexcrutable. Eso sí, luz con la autenticidad de la llama que es viva y la unción que a ésta le viene concedida por los óleos\".",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1980-01",
    "year": 1980,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 339,
    "article_char_count_full": 2061,
    "article_char_count_review": 2061,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-01-4-right-a-nadie-se-le-puede-sustraer-lo-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nContestando a un amigo de Levante\n\nEstimado amigo: Me dispongo a contestar su atenta carta, tras agradecerle la felicitación que con motivo de mi onomástica, me envía. Y lo hago con el tema principal que usted aborda en su carta: Las ofensas que cierto señor hizo a mi persona, por discrepar conmigo en un tema de nuestro oscuro mundo del arte flamenco.\n\nEfectivamente callé entonces, cuando lo lógico hubiera sido denunciarle ante el juzgado competente. Pero no, hoy mantengo la misma postura, pues considero que la indiferencia es lo único que tal señor merece. Ni siquiera mi indiferencia ha merecido, porque yo le perdono y así se lo comuniqué a través del director de la revista «F» de Murcia. Por lo visto tal ofensa fue motivada por el hecho -según él- de intentar quitar a Levante un cante\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"clásica\"]\n\ntente. Pero no, hoy mantengo la misma postura, pues considero que la indiferencia es lo único que tal señor merece. Ni siquiera mi indiferencia ha merecido, porque yo le perdono y así se lo comuniqué a través del director de la revista «F» de Murcia. Por lo visto tal ofensa fue motivada por el hecho -según él- de intentar quitar a Levante un cante que le pertenece. ¿Es que se puede despojar a alguien de algo que no posee? Me referí a la taranta clásica del árbol malacitano; cante de la misma familia que la granaina. ¿Es que los granadinos pueden tomar a mal si digo que la granaína y el fandango «un Sereno se dormía», pertenecen por entero a Málaga? ¿Es que la granaína al estilo de un Chacón o un Sellé, por poner a dos grandes como ejemplo, no es casi una malagueña? En varias ocasiones he tenido que decir a cantaores y aficionados que están equivocados; que no se trata de una malagueña, sino de una granaína. Recuerdo que una noche en Las Cabezas de San Juan, un cantaor dijo al público que iba a cantar por malagueña y granaína al estilo de Sellé, y lo que interpretó no fue lo que dijo. Esto nos prueba la ignorancia incluso de algunos profesionales. Cantó por granaína y terminó con una media granaína tal y como LO DISPUSO don Antonio Chacón y por supuesto, tal y como le siguieron casi todos los cantaores de su época. En cuanto al fandango que he citado, ¿no se trata de un fandango de verdial? Pues claro que sí. Con esto no quie\n\n[ENDING CONTEXT]\n\nsingular peña flamenca se enseña todos los días a cantar y bailar como mandan los cánones. ¡Qué bonita y fructifera labor!\n\nHoy que es festivo, querido amigo, aprovecho para hacer una recopilación de los cantes levantinos, con la intención de ponerlos a disposición de cualquier peña o entidad flamenca, por si entrara en sus cálculos grabarlos en cintas cassettes y ponerlos a la venta entre los aficionados de España.\n\nMe gustaría que estos cantes fueran ampliamente difundidos, de forma que no quedase un sólo artista con la duda actual de distinguir unos cantes de otros.\n\nMANUEL YERGA LANCHARRO\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A nadie se le puede sustraer lo que no posee",
    "periodical": "candil",
    "issue_id": "1980-01",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 1091,
    "article_char_count_full": 6254,
    "article_char_count_review": 3076,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "clásica"
      }
    ]
  }
]
```
