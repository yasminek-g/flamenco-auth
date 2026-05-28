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
    "article_id": "1997-07-3-left-a-miguel-vargas",
    "article_text_for_review": "Qué doloroso es tener que seguir abordando, en la primera página —una vez más—, el fallecimiento de un artista flamenco. Sabemos que es ley de vida encontrarse con la muerte, mas pensamos que no debiera ser tan tempranamente en ocasiones. Sin embargo, no tenemos más remedio que aceptar el óbito, aunque no sin sentir mucha más rabia que resignación.\n\nSe nos ha ido Miguel Rubio Vargas en su plenitud artística, en el momento en que su arte se consideraba pleno de matices personales tras haber acrisolado los ecos y las enseñanzas de Rafael Romero, Juan Varea, Pericón, Pepe de la Matrona, etcétera, después de haber pasado por Zambra, aunque su base cantaora —como la de la mayoría de los nacidos en La Puebla— estaba influenciada por Antonio Mairena.\n\nSe nos marchó “El Cateto”, apelativo que tan cariñosamente le puso “El Gallina” por su procedencia agraria, por su sencillo comportamiento y por el enorme tamaño de sus manos, expresión corporal única de su sentimiento flamenco cuando cantaba. Porque Miguel Vargas era un cantaor serio, profundo y con amplio conocimiento de los estilos. Su quejío por siguirias aún resuena en la Peña Flamenca de Jaén —fue una noche del 10 de enero pasado—tras una armoniosa y anterior entonación por rondeñas, malagueñas, marianas y soleares, evidenciando un acercamiento a Manuel Torre.\n\nY es que Miguel se ha destacado siempre por imponer seriedad a sus interpretaciones, por cumplir como artista flamenco, por traspasar con su arte el cuerpo del aficionado hasta llegar a lo más profundo de su ser flamenco. \"El cante festero se queda para quienes lo han vivió\", decía, en un alarde de honestidad consigo mismo y con sus seguidores, dejando patente, una vez más, su formalismo cantaor. No llegó a ser considerado primera figura de este arte, pero sí consiguió el respeto y la admiración del aficionado serio, del crítico, del componente del jurado o de sus propios compañeros. Igualmente supo hacerse acreedor de un puesto en el corazón de cada una de las peñas flamencas y en la nómina de los sobresalientes de este arte.\n\nVuelve el flamenco a vestirse de luto con la muerte de Miguel Vargas y, posiblemente, con más motivo que en otras ocasiones, pues al igual que sucedió con Terremoto y algunos cantaores de fama, pensamos que sus grabaciones no hacen honor al inmenso caudal de arte que el morisco poseía. Sin embargo, la cordura y el amor por el flamenco seguirá perdurando y seguros estamos que grabaciones particulares han de aportarse por alguien o algunos para que se edite una justa y buena antología de su cante.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-17",
    "page_number": 3,
    "word_count": 432,
    "article_char_count_full": 2567,
    "article_char_count_review": 2567,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-07-3-right-fin-de-la-epoca-dorada",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nD. E. Pohren\n\nF recuentemente me hacen dos preguntas básicas sobre el flamenco: 1ª) ¿Qué fue de la vida flamenca que descrito en mis libros y en los artículos que he escrito para Candil?; y 2ª ¿Por qué cesamos nuestras actividades flamencas en la Finca Espartero? Las respuestas están estrechamente relacionadas. Vamos a empezar con la primera pregunta, ¿qué diablos pasó con esa pintoresca vida que existía hace tan pocos años? Diría yo que la llegada de la prosperidad al flamenco durante la década de los 70 causó la desintegración casi inmediata de esa vida tal como lo habíamos conocido. ¿Y la causa de tal prosperidad dentro del mundo flamenco?: El creciente turismo en masa, que empezó en los años 50 y generó una gran demanda del flamenco de tablao, con las correspondientes oportunidades de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nos pasó con esa pintoresca vida que existía hace tan pocos años? Diría yo que la llegada de la prosperidad al flamenco durante la década de los 70 causó la desintegración casi inmediata de esa vida tal como lo habíamos conocido. ¿Y la causa de tal prosperidad dentro del mundo flamenco?: El creciente turismo en masa, que empezó en los años 50 y generó una gran demanda del flamenco de tablao, con las correspondientes oportunidades de trabajo, tuvo gran importancia. Esa influencia fue principalmente extranjera, pero los españoles no se quedaron atrás, pues al mismo tiempo se despertó su interés por los concursos flamencos en Córdoba en 1956 y 1959, y por el evento de la Llave de Oro del Cante, en la misma ciudad, en 1962. Esos acontecimientos prepararon el terreno para una multitud de estudios flamencos en varias localidades (Madrid, Barcelona, Cáceres y algunos lugares más “al norte”, pero sobre todo de Despeñaperros para abajo), y para los festivales flamencos, que brotaron como setas dentro y fuera de Andalucía. Tanta actividad, digna y educativa, lavó la cara del flamenco: de repente éste asumió una respetabilidad nunca conocida y fue aceptado socialmente hasta en Andalucía, donde anteriormente era tolerado apenas como fondo para juerguistas. Como es natural, toda esta actividad bien remunerada apartó a muchos flamencos de su vida anterior, y pronto el énfasis cambió del flamenco del reservado o cuartito —en el que muchos de los a\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_04 | trigger=\"comerciales\"]\n\ns normalmente no había más que un solo guitarrista para aportar el elemento musical durante toda la noche. Al contrario, el baile de reservado, cuando había, solía exigir menos esfuerzo que el de escenario, pues los bailes del primer grupo eran más bien de inspiración y mucho más cortos que los larguísimos bailes coreografiados de teatro y tablao. ² Hubo excepciones, artistas satisfechos con lo que tenían, que rechazaron numerosas oportunidades comerciales (Diego del Gastor, Manolo de Huelva, Juan Talega y Manolito de María, para nombrar sólo cuatro de los que cono-cí personalmente. Los artistas abrazan este tipo de flamenco por varias razones: la compensación económica es mucho mayor, y en general implica un menor esfuerzo¹; son más respetados como artistas de escenario, y este trabajo tiene un tinte de “normalidad” con horas más bien fijas y una vida ordenada que evita, si quieren, el consumo ex\n\n[ENDING CONTEXT]\n\nen la inmensa mayoría de los flamencos). En cambio, hoy día un joven aspirante flamenco escucha cualquier música que le gusta a través de los modernos métodos de comunicación, la pone a media velocidad y la copia, y enseguida la íntegra en su flamenco, por inconexa que sea y sin importarle de qué parte del mundo procede. A lo mejor la semana siguiente graba ésa y otras fusiones, y casi en el acto mucho del resto de los aspirantes le copian. El resultado: un seudo flamenco, en el mejor de los casos, o un no-flamenco en el resto, que los modernos aceptan rápidamente como flamenco auténtico.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Fin de la Época Dorada",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "3-8",
    "page_number": 3,
    "word_count": 4816,
    "article_char_count_full": 28875,
    "article_char_count_review": 4060,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "comerciales"
      }
    ]
  },
  {
    "article_id": "1997-07-8-right-manuel-p-rez-narv-ez-la-serneta",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNo quiero copiarla íntegramente porque ocuparía mucho espacio y porque resulta harto reconocida. Tengo ante mí el acta de defunción de La Serneta, escrita de puño y letra por Daniel Pineda Novo -ningún utrerano, ni tan siquiera el que suscribe, nos ocupamos de sacarla del archivo de Santiago- y compruebo que María de las Mercedes Fernández Vargas, nacida en Jerez de la Frontera el día 19 de marzo de 1840, hija de Salvador Fernández, herrero, de Gilena y de María del Rosario Vargas, de Jerez, falleció en Utrera a las 10 de la mañana del día 18 de junio de 1912, en la Plaza de la Constitución número 9, de gastroenteritis. Es decir, que el pasado día 18 de junio, cuando la Utrera flamenca ultimaba los preparativos del XLI Potaje Gitano, se cumplieron ochenta y cinco años de la muerte de esta\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mujer\"]\n\nnacida en Jerez de la Frontera el día 19 de marzo de 1840, hija de Salvador Fernández, herrero, de Gilena y de María del Rosario Vargas, de Jerez, falleció en Utrera a las 10 de la mañana del día 18 de junio de 1912, en la Plaza de la Constitución número 9, de gastroenteritis. Es decir, que el pasado día 18 de junio, cuando la Utrera flamenca ultimaba los preparativos del XLI Potaje Gitano, se cumplieron ochenta y cinco años de la muerte de esta mujer, jerezana, como queda dicho, pero utrerana ya de por vida, que nos llegó en 1863 y que aquí en Utrera creó su soleá \"apasionada y dulce\" al decir de Fernando el de Triana, soleá que, evidentemente, se registra en la historia como soleá de Utrera, cantaora que llegó a ser reconocida como la mejor soleaera de la historia y acaso la mejor cantaora del siglo XIX. Con anterioridad a La Sernetá y junto a los nombres de el Planeta, el Fillo, Frasco el Colorao, Juan Encueros y otros, aparece el nombre de Juan Pelao, citado por Demófilo en su Colección de Cantes Flamencos, como el Pelao de Utrera, creador de tonás, pero es un nombre vinculado a Triana de donde no resulta fácil sacar su ascendencia utrerana que se sostiene por distintos investigadores del flamenco. La Sernetá —leemos otra vez a Fernando el de Triana, un hombre que llegó a adorarla—, “gitana de sin par belleza sobre la que volcó la divina naturaleza el tarro de la salsa y el grado máximo del faraónico estilo del cante por soleá”, según desprende Daniel Pineda en su biografía leída en la conferencia internacional “Dos Siglos de Cante Flamenco”, celebrada en Jerez en 1988, “debió comenzar interpretando los cantes básicos antiguos de sus paisanos Curro Pabla, Tío Luis el de la Juliana, Tía Sarvadora y el señó Manuel Molina, que aprendería de niña en la fragua donde trabajaba su padre”. Repasando la historia de los teatros y cafés cantantes jerezanos que nos describe Manuel Ríos R\n\n[ENDING CONTEXT]\n\nJuan Montoya, se le rindió homenaje a La Serneta, en glosas que hicieran Juan de la Plata, Antonio Gallardo y el que esto escribe.\n\nPero es necesario —esto ya lo he hablado en varias ocasiones con el delegado de Cultura de nuestro Ayuntamiento y me consta que está en cartera— que todos los que sentimos el flamenco y mostramos preocupación por la cultura y la historia del pueblo, pongamos de nuestra parte para que el recuerdo de La Serreta se perpetúe aquí de una forma digna, como corresponde al nombre y a la categoría de esta mujer cantaora que inmoralizó en el flamenco el nombre de Utrera.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La Serneta",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1490,
    "article_char_count_full": 8498,
    "article_char_count_review": 3538,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mujer"
      }
    ]
  },
  {
    "article_id": "1997-07-10-left-a-prop-sito-de-unas-quejas-de-ga",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHacia 1740, Jerónimo de Alba y Diéguez se entretenía en escribir en un cuadernillo algo de lo que entonces le llamaba la atención de cuanto sucedía en Triana, en particular lo relacionado con las actividades de los gitanos. Tuvo la ocurrencia de titularlo \"Libro de la gitanería de Triana de los años 1740 a 1750 que escribió el Bachiller Revoltoso para que no se imprimiera\". Afortunadamente, aunque con cierto retraso —casi doscientos cincuenta años—, estos apuntes han terminado saliendo a la luz pública en forma de libro.\n\n“Para la danza son las gitanas muy dispuestas y en las casas de Landín, el pandero de cascabeles suena en fiestas por cualquier pretexto, que en esto no admiten ruegos.\n\n1) Editado por el Ayuntamiento de Sevilla en 1995 y difundido en el XXIV Congreso de Arte Flamenco,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"histórico\"]\n\nlica en forma de libro. “Para la danza son las gitanas muy dispuestas y en las casas de Landín, el pandero de cascabeles suena en fiestas por cualquier pretexto, que en esto no admiten ruegos. 1) Editado por el Ayuntamiento de Sevilla en 1995 y difundido en el XXIV Congreso de Arte Flamenco, celebrado en esta ciudad en 1996. Este texto constituye un documento precioso, el más antiguo de cuantos hoy podemos manejar para asomarnos a ese momento histórico en el que lo que después conoceríamos como cante flamenco estaba dando sus primeros balbuceos. En él podemos leer lo siguiente: Sevilla a representar sus bailes y la acompañan con guitarra y tamboril dos hombres y otro le canta cuando baila y se inicia el dicho canto con un largo aliento a lo que llaman queja de galera, porque un forzado Una nieta de Balthasar Montes, el gitano más viejo de Triana, va obsequiada a las casas principales de gitano las daba cuando iba al remo y de éste pasó a otros bancos y de éstos a otras galeras. Es tal la fama de la nieta de Balthasar Montes que el año pasado del 46 fue invitada a bailar en una fiesta que dio el Regente de la Real Audiencia, Don Jacinto Márquez al que no impidió su cargo tan principal tener de invitados a los gitanos y las Señoras quisieron verla bailar el Manguindoi por lo atrevida que es la danza y autorizada por el Regente a súplicas de las Señoras, la bailó, recibiendo obsequios de los presentes. Estas líneas nos corroboran algo de dominio público: la afición de las gitanas por los bailes, así como su profesionalidad. Efectivamente, casi desde su llegada a nuestro país, el baile, como la buenaventura, había sido una de las formas de vida características de muchas gitanas españolas. Un arte en el que han demostrado siempre su personalidad y singulares dotes interpretativas. Gracias a ellas, han sabido dar una impronta personal a cuantas danzas aprendían y después interpretaban, las mismas que se bailaban en toda nuestra geografía y especialmente en la andaluza. En este caso, la afamada nieta de Balthasar Montes ejecuta, posiblemente ante las miradas lascivas de algunos señores principales y los atónitos y quizás avergonzados ojos\n\n[ENDING CONTEXT]\n\ncuadrillas de gitanos que vivían del espectáculo y se conservaron durante siglos en sus repertorios de danzas. Si efectivamente pudo tener algo que ver este Baile de Galeras con el origen de los \"largos alientos\" que da-ban los gitanos trianeros es, por supuesto, una hipótesis de imposible verificación, a menos que aparecie-se algún otro escrito que, como el del Bachiller Revoltoso, terminase saliendo a la luz. No obstante, aun-que sólo a título de curiosidad, nos parecía oportuno dar a conocer esta pieza y dejar abierta la interrogante que planteamos. Ellos, los viejos aficionados, dicen...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A propósito de unas “quejas de galeras”",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 1235,
    "article_char_count_full": 7308,
    "article_char_count_review": 3798,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "histórico"
      }
    ]
  },
  {
    "article_id": "1997-07-13-right-t-o-evaristo-entrevista",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEstamos asistiendo a un proceso de manipulación y trivialización del Arte Flamenco, y por ello, se hace preciso retomar el hilo conductor de la denuncia de tal situación, a través de entrevistas con viejos aficionados, para que nos hablen del flamenco que fue y del que es en la actualidad. Hoy traemos a nuestras páginas a un gitano cabal de los pies a la cabeza: Evaristo Heredia Maya. Su personalidad cautiva al primer golpe de vista; su porte elegante y correcto \"aliño indumentario\" acuna un alma esencialmente flamenca y le presenta la estampa de un torero de los de antes. Por su carácter y agradable trato le definiríamos como el arquetipo de flamenco andaluz, que dijera Alselmo González Climent: Es el señor andaluz, cordial sin miríñaques, rumboso sin exhibición, entendido sutil del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Granada\"]\n\nn miríñaques, rumboso sin exhibición, entendido sutil del cante, del baile y del toreo como de las exquisiteces de la vida. Es delicado en el sentir y recio al jurar; sentencioso y cordial en justa proporción. Le sobra tronío que es la calidad rutilante de lo viril no entorpecida por lo exquisito; es el cabal por excelencia. —Evaristo, ¿cuántos años tienes? —Pues da la casualidad de que hoy cumplo 71 años. Nació en el año 1926. —Tú naciste en Granada, verdad? —Sí. Yo nací en la provincia de Granada, concretamente en Gabia la Grande, un pueblecito que está a 7 kilómetros de Graná. -¿Cuántos años llevas viviendo en Algeciras? —En Algeciras hace cincuenta años que vivo; me vine aquí con veintiún años. Pero voy con frecuencia a Graná, por aquello de que a uno no le gusta perder sus raíces. Además de que allí tengo a mis hermanos y gran parte de la familia. -¿Gabía ha sido y es flamenca...? —Siempre hubo muy buenos flamencos... Había uno que era un fenómeno, el Tío Carlicos \"Pecho Lebrillo\". —:Siempre estuviste vinculado a la Sociedad del Cante Grande de Algeciras? —Siempre. Desde su fundación en el año 71 con mi compadre Antonio Rubio, que es un fenómeno. Son muchos años llenos de una gran actividad flamenca, porque por esta casa han pasado además de los grandes artistas de la Comarca, como Antonio Medreles, Canela de San Roque, Juan Delgado, Paqui Lara, Rocío Alcalá, todos los mejores de Andalucía; porque en Algeciras hay mucha y muy buena afición. —Por lo que sabemos, tu afición viene de antiguo... —De to la vida de Dios, porque en mi casa siempre había flamencos... Siempre había flamenco porque mi padre, con frecuencia, llegaba por la madrugá con “Pecho Lebrillo”—que llevaba una foto de Tomás “El Nítri” colgá como si fuera la Virgen de las Angustias—, con el “Niño Pinichi”, con “El Habichuela” (el padre de Juan Carmona), con “Tía Marina”, Cobitos, otro que le llamaban Miguel “El Saetas”, “El Tranca”, “Joselillo Medio-Pan”, Juanillo “El Gitano”..., en fin, muchos. Ya os podéis imaginar lo que allí se armaba y lo bien que se cantaba. Cuando se ponían a cantar por soleá o por siguirias, nadie se movía y no se oía ni una mosca... Recuerdo que “Pecho Lebrillo” me decía: “Niño, me gusta tanto el cante que, cuando estoy a gusto, los pelos de las piernas me atraviesan los calzones”. —En alguna ocasión te hemos oído hablar de otro que le decían “El Boega”... —Sí, era tío político mío, casao con una hermana de mi padre, mi tía Justica. Era mu buen aficionao. Yo recuerdo que, cuando íbamos por ahí a comprar ganoa, montaos en las bestias por esos caminos, mi padre siempre iba delante,\n\n[ENDING CONTEXT]\n\nlas de Manuel Martín, Luis Soler, Miguel Acal y poco más; el resto ni es de calidad ni es objetiva.\n\nPa ser crítico de teatro hay que saber de teatro, pa ser crítico de toros hay que saber de toros, y pa ser un buen crítico de flamenco hay que saber de flamenco, y no que algunos, por el hecho de tener una pluma fácil creen que tienen licencia para hablar de tó.\n\nAhora, la poca crítica que hay de calidad es muy buena.\n\nEstas fueron, en suma, las palabras de Evaristo Heredia Maya, mojadas de afición y cuyo contenido compartimos plenamente; recogidas en la Sociedad del Cante Grande de Algeciras.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Tío Evaristo (entrevista)",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "11-15",
    "page_number": 11,
    "word_count": 4178,
    "article_char_count_full": 23982,
    "article_char_count_review": 4243,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "Granada"
      }
    ]
  }
]
```
