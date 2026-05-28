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
    "article_id": "1984-01-18-right-la-verdad-sobre-el-viejo-medina",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Francisco Vallecillo\n\nACIA tiempo que teníamos concertada esta entrevista con Manolo Yerga. Pudo haber tenido lugar en ocasión del homenaje a Manolo el de Huelva, pero una pasajera indisposición de nuestro interlocutor dio al traste con estos deseos que, al fin ahora, se ven satisfechos. Manolo Yerga es un apasionado de la investigación en el campo del flamenco. Tal pasión supera —este es nuestro parecer— su propia afición al cante, que no es poca, y para el que está particularmente dotado. Sobre sus amplios saberes de cantes, formas, estilos y orígenes, adquiridos en contacto con infinidad de artistas pertenecientes a tres generaciones y contrastados con un caudal discográfico que se cifra en millares de ejemplares. Yerga ha dedicado mucho de su tiempo y de su dinero a juzgados y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Hombre\"]\n\noca, y para el que está particularmente dotado. Sobre sus amplios saberes de cantes, formas, estilos y orígenes, adquiridos en contacto con infinidad de artistas pertenecientes a tres generaciones y contrastados con un caudal discográfico que se cifra en millares de ejemplares. Yerga ha dedicado mucho de su tiempo y de su dinero a juzgados y parroquias, tras la adquisición de documentos y de informes con los que ha formado un fichero envidiable. Hombre de convicciones muy arraigadas, su labor en este aspecto de la investigación no es suficientemente conocida. Nosotros, que sabemos de ella y por esto le admiramos, le hemos reprochado en más de una ocasión el uso demasiado personal y restringido que hace de sus resultados. De ahí nuestro interés en reunirnos con Manolo Yerga y sacarle algo de lo que sabíamos que estaba rastreando. Hay una inevitable actitud defensiva en el hombre que lo fía todo a los papeles y a los discos cuando sabe que ahora está apoyado —y huelga decir que el adverbio ahora juega aquí un puro papel transitivo— sólo en la referencia oral, si bien en este caso directísima. Hoy la entrevista va a versar sobre la Petenera, mejor dicho, sobre su supuesto creador El Viejo Medina. Sacamos a colación este tema porque siempre lo hemos visto en estado de nebulosa y dado que los escasos tratadistas del tema son poco claros y convincentes. —¿Puede considerar al Viejo Medina como creador de la Petenera? Preguntamos, sin ambages, a Manolo Yerga: —No quiero hacer afirmaciones arriesgadas, como tantas se hacen sobre la historia del arte flamenco. No existen datos suficientes que\n\n[ENDING CONTEXT]\n\ndel Niño, esto es, de «Medina el Barbero», al que visité en una ocasión. De sus labios sé que Medina no es apodo; que es apellido que llevaba su padre en cuarto lugar. Me dijo, además, que su abuela y demás antepasados fueron muy populares en el barrio de Santiago, de Jerez, en cuya parroquia fueron bautizados.\n\nAsí pues, que los Medina no tienen nada que ver con la ciudad asidoniense, antigua capital del Cádiz árabe. Yerga Lancharro habla siempre con convicción. La información que acredita este caso es de primerísima mano y así hay que tomarla como incuestionable. QUIENES FUERON LOS MAESTROS\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La verdad sobre el viejo Medina",
    "periodical": "candil",
    "issue_id": "1984-01",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1109,
    "article_char_count_full": 6393,
    "article_char_count_review": 3234,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Hombre"
      }
    ]
  },
  {
    "article_id": "1984-01-19-right-rosario-la-mejorana",
    "article_text_for_review": "Selecciona: Rafael Valera\n\nA UNQUE en esta sección solamente han aparecido auténticos maestros cantaores, la figura que traemos en este número a pesar de haber figurado en la historia de nuestro arte como una de las mejores bailaoras, no por eso abandonó el cante, dejando muestra de calidad interpretativa además de creatividad. Rosario Monje «La Mejorana» nació en la «Tacita de Plata» en 1862, concretamente en el gaditano barrio de La Viña y dentro de la dinastía de las Cachucheras. Comenzó su carrera artística a la temprana edad de 16 años, dando auténtica muestra de su valía como bailaora en los tablaos flamencos —principalmente en los del Burrero y de Silverio—. El arte y la hermosura de Rosario «La Mejorana», cautivaron a media Andalucía en su tiempo. Se le debe, fundamentalmente, la innovación en los bailes flamencos para mujer. «La Mejorana» es la primera bailaora que comienza a levantar muy alto los brazos, dando así más realce a la figura femenina. Como sucede con todas las innovaciones, al principio causó sorpresa y tuvo sus detractores, pero, en un corto período de tiempo, esta forma de bailar fue aceptada por la afición y los profesionales, creándose así una nueva estética del baile flamenco femenino. Como cantaora —faceta que queremos resaltar— fue también Rosario «La Mejorana» buena dominadora de los cantes por soleares, destacando principalmente en las bulerías y cantinas, y una auténtica maestra por alegrías, a las cuales supo apuntarle un «juguetillo» —como dice Quiñones— de su creación.\n\nSu vida artística fue muy breve pero intensa, solamente duró tres años, en los cuales Rosario Monje demostró su gran calidad como cantaora y bailaora. Casada con el sastre de toreros Víctor Rojas, dio al arte flamenco dos grandes pilares, uno en el baile, la inigualable Pastora Imperio, y otro en el toque, el sevillano guitarrista Víctor Rojas.\n\n«La Mejorana» —de nuevo según Quiñones— «...atraviesa, con la celeridad y también con el esplendor de un cometa, que lo deslumbra todo a su paso, por el Cádiz y la Sevilla del más difícil y exigente momento del arte jondo». Murió en Madrid en 1922.\n\nSin embargo, quizás la descripción más exacta de la figura de esta bailaora-cantaora, la realiza Fernando el de Triana, en su libro «Arte y artistas flamencos». Dice así: «No era mejor que las mejores, pero no había ninguna mejor que ella...» (...). «Su cara era blanca como el jazmín; de su boca los labios eran corales: (¡Qué bonito principio para una siguiriya, verdad!), y cuando reía dejaba ver, para martirio de los hombres, un estuche de perlas finas, que eran sus dientes; su cabello, castaño claro, casi rubio, sus ojos, no eran ni más ni menos que dos luminosos focos verdes; y, como detalle divino, para coronar su encantadora belleza, era remendada, pues sus arqueadas\n\ny preciosas cejas y sus rizadas y abundantes pestañas, eran negras, como negras eran las “ducas” que pasaban los pocos hombres que tenían la desgracia, ¿he dicho la desgracia?, ¡sí!, ¡la desgracia!, de hablar con ella siquiera cinco minutos.\n\nSu figura era escultural y cuidaba siempre de vestir los colores que más la hermoseaban, pero siempre su bata de cola, de percal, y su gran mantón de Manila» (...).\n\n«Cuando salía bailando y terminaba la falseta, hacía una parada en firme, y al compás de la fiesta (de palmas sordas), se cantaba ella misma estos juguetillos que a la vez bailaba y, mientras tanto, había cristiano que se limpiaba la baba cuatro o cinco veces, pues sin darse cuenta se quedaban embobados.\n\nYo soy blanca, y te diré\n\nla causa de estar morena:\n\nque estoy adorando a un sol,\n\ny con sus rayos me quema.\n\nDormía un jardinero\n\na pierna suelta:\n\ndormía y se dejaba\n\nla puerta abierta.\n\nHasta que un día,\n\nle robaron la rosa\n\nque más quería.\n\nComo final de este detalle, otra vez la fiesta animada, segunda cantiña del cantaor de turno, ovación delirante, y las mayores atenciones para la sublime bailaora».",
    "title": "Rosario «La Mejorana»",
    "periodical": "candil",
    "issue_id": "1984-01",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 660,
    "article_char_count_full": 3935,
    "article_char_count_review": 3935,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-01-20-left-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHablan las Peñas\n\nNUEVA DIRECTIVA DE LA ASOCIACION CULTURAL FLAMENCA «LA UNION DEL CANTE»\n\nA Asociación Cultural Flamenca «La Unión del Cante», de Las Lagunas, Mijas Costa (Málaga), eligió nueva Junta Directiva, la cual quedó compuesta de la siguiente forma:\n\nPresidente: Salvador Menéndez Sánchez; Vicepresidente: Sebastián Fuentes Galván; Secretario: Rafael Boler García; Tesorero: Juan Ramón Navarro Quirós; Relaciones Públicas: Juan Hormigo Haro; Vocales: José Rueda Perea, Francisco Lavado Sánchez y José Moreno Redondo.\n\nDeseamos toda clase de éxitos a la nueva Junta.\n\nCAMBIO DE DIRECTIVA EN LA PEÑA FLAMENÇA DE ALORA\n\nN el pasado mes de diciembre celebró Asamblea General de Socios la Peña Flamenca de Alora, eligiendo nueva Junta Directiva, que quedó compuesta como sigue: Presidente:\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"Públicas\"]\n\nosé Rueda Perea, Francisco Lavado Sánchez y José Moreno Redondo. Deseamos toda clase de éxitos a la nueva Junta. CAMBIO DE DIRECTIVA EN LA PEÑA FLAMENÇA DE ALORA N el pasado mes de diciembre celebró Asamblea General de Socios la Peña Flamenca de Alora, eligiendo nueva Junta Directiva, que quedó compuesta como sigue: Presidente: Andrés Borrego Escudero; Vicepresidente: Francisco Casermeiro Fernández; Secretario: Pedro Aranda Cuenca; Relaciones Públicas: José María Lopera Rodríguez; Vocales: Jesús Lorenzo, Francisco Cuenca, Francisco Luque, Benito Moreno, José Vergara, Joaquín García, José María Martín, José Alcántara, Francisco Estrada y Juan Hidalgo. Nuestra enhorabuena a la nueva Junta. HOMENAJE A JOAQUIN EL DE LA PAULA ORGANIZADO por la Delegación de Cultura del Ayuntamiento de Alcalá de Guadaira, ha tenido lugar un homenaje a Joaquín el de la Paula, el día 10 de febrero, con motivo del cincuentenario de su muerte. Con tal motivo, se han celebrado distintos actos entre los que destacan el nombramiento de Hijo Predilecto de Alcalá de Guadaira por la Corporación Municipal. También se celebró una exposición de fotografía flamenca, la presentación de un busto de bronce de Joaquín el de la Paula, obra de Jesús Gavira, un acto de exaltación de la figura de Joaquín a cargo del poeta Manuel Alvarez López, con la actuación del pianista Manuel García Matos. A continuación, fue presentado el libro «Joaquín el de la Paula, gran artífice del cante por soleá de Alcalá». Presentación que corrió a cargo de don Francisco Vallecillo (Director del Departamento de Flamenco de la Junta de Andalucía). Finalizando este homenaje con un recital de cante, de Fernanda de Utrera y Manolo de Mairena, con la guitarra de José Cala «El Poeta». FESTIVALES FLAMENCOS EN BURDEOS (Francia) RECITAL FLAMENCO EN SAN JUAN DE LUZ ORGANIZADOS por la Tertulia Flamenca de Burdeos (Francia), han ten\n\n[ENDING CONTEXT]\n\ny Ramón Montoro Campos, vocales de régimen interno.\n\nEl tiempo transcurrido desde la elección de presidente, en la persona de Alfonso Fernández Malo, hasta su toma de posesión, ha sido un «período puente», en el que se ha desarrollado una gestión coordinada entre ambas presidencias. Por otro lado, la nueva Junta Directiva de la «Peña Flamenca» de Jaén, designó como sus representantes en la asamblea de la Federación Provincial de «Peñas Flamencas» de Jaén, a Joaquín Sánchez Martínez y Luis de la Rosa Galán.\n\nAPERITIVOS SELECTOS\n\nEspecialidad en PLANCHA\n\nMesones, 18 – Teléf. 23 40 46\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "HABLAN LAS PENAS",
    "periodical": "candil",
    "issue_id": "1984-01",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1005,
    "article_char_count_full": 6459,
    "article_char_count_review": 3511,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "Públicas"
      }
    ]
  },
  {
    "article_id": "1984-01-21-right-buz-n-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSeñor director de CANDIL:\n\nSin ánimo de entrar en polémica, desearía contestar, a través de la Revista que usted tan dignamente dirige, a don Salvador Castro Morón, que tanto interés tiene que la suya obre en mi poder, cosa que agradezco.\n\nVaya por delante, señor Castro, que todo cuanto dije en mi anterior misiva nunca, de verdad, iba dirigido a herir la figura de don Manuel Avila, ni tan siquiera tratar de emborronar el buen hacer (?) del jurado o la comisión organizadora del Festival Nacional del Cante de las Minas de La Unión y, ni mucho menos, herir a quienes al leer mi anterior carta a CANDIL han traducido un ataque, por mi parte, a personas, artistas, entidades, etc.\n\nPara empezar, señor Castro, todo el que participa, realiza, dirige u organi-za cualquier acto público (teatro, cine,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"crítica\"]\n\ntan siquiera tratar de emborronar el buen hacer (?) del jurado o la comisión organizadora del Festival Nacional del Cante de las Minas de La Unión y, ni mucho menos, herir a quienes al leer mi anterior carta a CANDIL han traducido un ataque, por mi parte, a personas, artistas, entidades, etc. Para empezar, señor Castro, todo el que participa, realiza, dirige u organi-za cualquier acto público (teatro, cine, cante, baile, etc.) está sujeto a una crítica positiva o negativa..., y aquí no voy a entrar en la disyuntiva de si en un momento determinado lo que critica mi car-ta conlleva un signo u otro. ¿Usted, ante sus aseveraciones, está capacitado para —subjetivamente— dilucidar, el criticar mi carta, si es así o no? Yo, afortunadamente —por aquello de ser cartagenero de la provincia de Murcia y vecino de la ciudad hermana de La Unión—, por haber seguido durante muchos años la trayectoria del Festival del Cante de las Minas, haber visto ganar a muchos cantaores y haber oído la diversidad de versiones que de un mismo cante se ha premiado (la «Minera»), tengo que defender, por el bien de nuestros estilos mineros de Cartagena y La Unión, la PUREZA genuina del cante de esta tierra murciana, cartagenera o unionense. Le aseguro, señor Castro, que el señor Avila, en la final del XXIII Festival Nacional del Cante de las Minas de La Unión, no llegó a cantar el famoso estilo de MINERA propio de estos lares (y, a pesar de ello, no ponemos en tela de juicio los conocimientos y los estudios realizados —por él— sobre el cante en general). Al mismo tiempo le agradezco de corazón, señor Castro, que a estas alturas nos recuerde a los aficionados de esta tierra —y en especial a mí— que gracias al festival conocemos —conozco— la Minera, Taranta, Cartagenera, Taranto, Murciana, Levantica, etc. ¿Pero, en este punto, sabe usted quién resurgió todos estos cantes que estuvieron aletargados desde el año diez de este siglo?, y ¿sabe usted cómo surgió el Festival de La Unión? Pues bien, a tenor de cómo se expresa en este punto entiendo que está usted falto de información. Y aún más, estos estilos\n\n[ENDING CONTEXT]\n\nAb°. Pat°. José y Carmela Canales; Mat°. Manuel y Manuela Grau; Le puse por nombre Antonio. Nació a las cuatro de la tarde del día de ayer, segun relacion de los Pad°. que lo fueron Antonio Macia y Antonio Marco, a quienes adverti el parentesco espiritual y demas de que Certifico = Antonio Galvez, V°. E°.». CERTIFICO: Que esta partida es copia literal de la que obra en el Expediente Matrimonial de Antonio GRAU MORA con M. a del Mar Doucet Moreno. Y para que conste, a instancia de Manuel Yerga Lancharro, firtuy sello esta copia en Málaga, a veinte de noviembre de mil novecientos ochenta.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "BUZON FLAMENCO",
    "periodical": "candil",
    "issue_id": "1984-01",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 2166,
    "article_char_count_full": 12936,
    "article_char_count_review": 3726,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "crítica"
      }
    ]
  },
  {
    "article_id": "1984-01-22-right-discografia-flamenca-discografia",
    "article_text_for_review": "Discografia Flamenca\n\nOMENTABAMOS en nuestro último número de CANDIL, en referencia a un disco de Antonio Mairena, que como sucede con los grandes artistas después de acaecer su muerte, las casas grabadoras escudriñan sus archivos y lanzan las antiguas e inéditas grabaciones del mismo. Si anteriormente era la casa Columbia la que reeditaba unas grabaciones del año 59, es en esta ocasión otra casa discográfica, concretamente la Philips, la que lanza al mercado dos discos de larga duración editados anteriormente en el año 1973, los cuales también le sirvieron a Félix Grande para dejar patente la personalidad y calidad de Antonio Mairena en su antología flamenca —editada por esta misma casa en 1981— «Grandes del Flamenco».\n\nEn el disco que se indica arriba, o sea, el primer volumen, Antonio Mairena vuelve de nuevo a demostrar su gran apasionamiento por los cantes primitivos y la gran labor de investigación realizada por él en el campo de las tonás. Sigue profundizando en lo añejo de las siguiriyas con incidencia de ecos jerezanos y una magnífica rememoración de la figura de Paco la Luz. Soleares con justo compás, conocimiento y calidad interpretativa. En cuanto a los estilos festeros, una vez más Antonio Mairena se acerca a la Tacita de Plata para meternos en el bullicio del carnaval gaditano con unos graciosos tanguillos. Después sigue ubicado en la comarca con cantes por tangos muy festeros y salineras cantinas. Vuelve de nuevo a Jerez de la Frontera con unas extensas bulerías y con el marcado compás de la localidad gaditana.\n\nNo queremos cerrar este comentario sin advertir al aficionado que el cante por cantiñas que figura en el disco está marcado como cante por cañas, error de impresión en la etiqueta del mismo, que tiene mucha importancia para los legos en nuestro arte por cuanto un disco de Antonio Mairena sienta auténtica cátedra en el mundo flamenco.\n\nLas guitarras de Melchor de Marchena y su hijo Enrique dan, una vez más, lección de acompañamiento y calidad interpretativa, sobre todo en los toques por siguiriyas del maestro desaparecido, conjuntándose perfectamente padre e hijo en los estilos festeros.\n\nC OMENTAMOS a continuación el segundo volumen de Antonio Mairena «UN CANTAOR PARA LA HISTORIA» que, como queda anteriormente reflejado, ha editado la casa Philips de unas grabaciones originales de 1973.\n\nAl igual que en el anterior comentario, Antonio Mirena continúa con su pasión por los cantes de fragua, los que realiza en este volumen con auténtica entrega y sabor añejo. Sigue con la profundidad de las siguirias y si en el anterior volumen se acercaba a Jerez de la Frontera, en este segundo, Antonio Mirena se queda en Triana con una rememoración llena de solera del maestro trianero señor Manuel Cagancho en su cara A, para seguir con la misma solera y profundidad en la segunda cara. Vuelve a incluir cantes por soleares con la calidad interpretativa y el conocimiento a que nos tenía acostumbrados en el mencionado estilo. Bulerías llenas de compás, que junto a los tangos nos vuelve a dejar en Sevilla y en este último estilo con ecos evocadores de la personalidad artística de Pastora Pavón «Niña de los Peines». Por último, otra vez Antonio Mirena revaloriza ese estilo por él adaptado al compás de «bulerías por solea» como son los romances antiguos o corridos gitanos, en esta ocasión el de la princesa Celinda —El Chozas de Jerez lo tiene grabado como Romance de Zaide— con un magnífico acompañamiento de guitarras y coro y cierra este disco con la famosa «Giliana» —cante, por otra parte, entonado casi con las mismas características que el mencionado romance— y donde, una vez más, Antonio Mirena vuelve a sacar su faceta investigadora a la vez que difusora de nuestro arte flamenco.\n\nEn relación a las guitarras, sirva el mismo comentario realizado para el primer volumen, donde padre e hijo —este último en los inicios de su carrera artística— dan la auténtica medida de su calidad.\n\nDOSCANDIL",
    "title": "DISCOGRAFIA FLAMENCA DISCOGRAFIA DE ARTISTAS FLAMENCOS",
    "periodical": "candil",
    "issue_id": "1984-01",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 657,
    "article_char_count_full": 3961,
    "article_char_count_review": 3961,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
