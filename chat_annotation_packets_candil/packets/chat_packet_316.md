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
    "article_id": "1996-03-3-right-la-epoca-dorada",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nD. E. Pohren \"Fernandillo de Morón\" (Tercera parte de Finca Espartero)\n\n1 al como se ha visto en mis artículos de Candil (números 92 al 99), la vida flamenca en Andalucía —que vivimos entre los años 1955 y 1975— fue una mezcla deliciosa de profundidad y diversión, dominada la inmensa mayoría del tiempo por la fiesta. Hacia falta, desde luego, un excelente sentido del humor para entender y disfrutar plenamente del pitorreo y la guasa subyacente en casi todas las actividades. Pocas cosas se tomaban en serio, y cuando lo eran, se disimulaban con una capa de cachondeo. Durante nuestros primeros años en la finca, esta manera de vivir se vio generosamente enriquecida por el artista y buen amigo Fernandillo de Morón: la vida nunca era aburrida con él, y a veces tenía momentos de verdadera\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nse tomaban en serio, y cuando lo eran, se disimulaban con una capa de cachondeo. Durante nuestros primeros años en la finca, esta manera de vivir se vio generosamente enriquecida por el artista y buen amigo Fernandillo de Morón: la vida nunca era aburrida con él, y a veces tenía momentos de verdadera inspiración, algunos de los cuales describiré en estas páginas después de presentar una breve historia de su vida y entorno. Con ello, entenderemos mejor aquellos tiempos. Fernandillo de Morón: arte y simpatía. Fernandillo había pasado una existencia bastante más dura que la mayoría de los gitanos de Morón, la que le proporcionó una sabiduría y unos conocimientos poco usuales en varios campos. Era hijo de una de las hermanas de Diego del Gastor, y de un tratante de ganado gitano, festero flamenco de alguna fama. Su familia había vivido bastante bien hasta que su padre cayó en una de las primeras batallas de la guerra civil, no mucho después del nacimiento de Fernandillo. Así que a una edad muy tierra Fernandillo asumía el papel de hombre de familia, que significaba que tan pronto pudo andar él estaba en la calle pidiendo o, más frecuentemente, intentando robar el pan diario para su familia. En aquellos tiempos, conocidos como los años de hambre, conseguir comida para la familia era posible sólo para los ricos, los fuertes y los muy listos. Fernandillo (y todos los demás) hablaban sobre las colas sin fin de lante de las panaderías oficiales, donde sólo había pan suficiente para mantener viva a la población. Pero aún después de esperar largas horas en cola, no se podía estar seguro de que no se acabaría el pan antes de tu turno y que tu familia comería aquel día. El recordaba cómo el hambre afectaba a la gente, cómo los vecinos solían pelear por un poco de pan, y cómo los niños más valientes solían intentar quitárselo de las manos de los viejos y los débiles y correr como el vient\n\n[ENDING CONTEXT]\n\nMaría, Diego del Gastor y Fernandillo (y más tarde con la de varios otros de esa misma filosofía, como Juan Talega, Joselero, Anzonini, Perrate, Mano-lo el Poeta de Alcalá, etc., etc.). Desde el punto de vista total, el flamenco de verdad se vio seriamente disminuido por la muerte de cada uno de ellos. No quedan muchos de los auténticos flamencos, lo que significa que pronto este arte se quedará completamente en manos de los profesionales astutos, un espectáculo teatral sin alma ni significado...\n\nUn beso al autor después de recibir Fernandoillo un adelanto contra sus ganancias juerguísticas.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La Epoca Dorada",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "3-6",
    "page_number": 3,
    "word_count": 3463,
    "article_char_count_full": 20557,
    "article_char_count_review": 3528,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  },
  {
    "article_id": "1996-03-7-left-qu-pasa-con-el-flamenco-de-hoy",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLuis Soler Guevara\n\nEn primer lugar quiero exponer para aclarar posiciones en el tablero de juego, que ni soy flamencólogo ni pretenderé nunca serlo. Es un título que me viene ancho y además no me hace ningún favor; huyó de este concepto que entiendo mal calificado por su praxis en la historia de nuestro arte. Sólo me siento un aficionado que llega al mundo de la literatura y de la crítica desde lo más jondo de nuestros cantes y sus vivencias y no al contrario.\n\nPara quien esto suscribe, un flamencólogo es un señor que escribe relativamente bien, tiene poder en el mundo del flamenco, ha escrito algún que otro libro, pero que adolece de un desconocimiento enorme de nuestros cantes. En cuanto se le pellizca un poquito salen las Lagunas de Ruidera por todos los lugares patrios.\n\nPero vayamos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"título\"]\n\ndo de la literatura y de la crítica desde lo más jondo de nuestros cantes y sus vivencias y no al contrario. Para quien esto suscribe, un flamencólogo es un señor que escribe relativamente bien, tiene poder en el mundo del flamenco, ha escrito algún que otro libro, pero que adolece de un desconocimiento enorme de nuestros cantes. En cuanto se le pellizca un poquito salen las Lagunas de Ruidera por todos los lugares patrios. Pero vayamos con el título de este artículo. El mismo encierra una gran pregunta: ¿Qué pasa con el flamenco, hoy? Para muchos nuestros cantes gozán de buena salud. Esta afirmación cobra su verdad en cuanto que hace referencia a: * El flamenco cada vez más está ganando prestigio. * En muchos países como Francia, Japón, Estados Unidos, etc., se le presta una mayor atención. Don Antonio Chacón. Jerez de la Frontera. * La guitarra flamenca está en su cenit. * Nuestros bailes y cantes han atrasados todas las fronteras. La crítica flamenca, tanto en revistas como en prensa y otros medios, le dedican más espacios. El número de libros editados sobrepasa con creces las exigencias del más optimista. * Las reediciones de muchos y valiosos materiales en compac-dis es una realidad. * En nuestros responsables políticos se ha incrementado la sensibilidad por el fenómeno flamenco, quizá por aquello de la presión del voto. Todo ello con ser cierto no avala que nuestros cantes gi\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_01 | trigger=\"dentro\"]\n\n. Esta tendencia, cuanto es mayor, mayor es su identificación. Cantar no es el acto de mostrar las facultades canoras de los individuos, sino la cualidad de rebuscarse para penetrar en los demás. En ello radica no sólo la esencia de nuestro arte sino también su más consumada substancia. Lo sublime es engrandecer, exaltar lo sencillo y lo simple a categoría de admirable. Entregarse a lo sublime es despojarse de su yo, para dejarse penetrar en su adentro para el placer, el deleite o la agonía que genera el estado anímico del goce de quien deviene las vivencias. Disculpen los lectores el matiz que estamos introduciendo. Dife- renciamos el decir el cante con el saber cantar. Saber cantar es relativamente fácil. Es más, con el tiempo y mucha afición y una buena herramienta, se consigue aprender. Pero la dificultad donde se genera, es en el decir. Al igual que no es lo mismo oír, que escuchar, que sentir, tampoco es igual hablar que decir. ¿Se puede saber decir el cante con quince años? Es muy difícil, sólo algunos genios lo han conseguido: Chacón, Pastora, Camarón, La Repompa, por citar algunos. Y por supuesto que ninguno de éstos se estancaron. ¿Por qué? Porque las vivencias acumuladas ib\n\n[ENDING CONTEXT]\n\nY esto es muy importante, porque no es lo mismo saber cantar que conocer el cante. Es más, son muchos los que conociendo el cante no saben cantar y viceversa. La acción de cantar no implica el conocimiento del cante. Igual ocurre con el flamencólogo e intelectual, si no se es un gran aficionao.\n\nLa diferencia entre un cantaor ramplón que conoce el cante pero que no tiene amistad con él, es la que se establece entre el hombre o la mujer que sabe escribir, pero que no siente lo que escribe. A fuerza de sinceridad no sólo creo en lo que digo, sino que además así lo siento en mis entrañas.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "¿Qué pasa con el flamenco de hoy?",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "7-11",
    "page_number": 7,
    "word_count": 5542,
    "article_char_count_full": 32301,
    "article_char_count_review": 4295,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "título"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "dentro"
      }
    ]
  },
  {
    "article_id": "1996-03-12-left-matilde-coral-sabidur-a-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen...\n\nFotografías: José Pamos\n\nRafael Valera Espinosa\n\nComo en su baile, la alegría preside su semblante, ademanes y conversación, porque Matilde Coral es todo simpatía y gracia. Mas no deja pasar la oportunidad para mostrar su sabiduría flamenca y bailaora, una erudición artística adquirida por sus más de cuarenta años de figura flamenca. No escatima respuestas ni regatea verdades; sus verdades, las de una bailaora que sabe del amargor de los sufrimientos y de las dulzuras de los triunfos. Se lamenta de la posible falta de reconocimiento oficial a su impartición de clases, como si hubiera alguien que pudiera autorizar o aprobar su arte, un arte que ha sido doctorado por todos los públicos del mundo en el examen diario del escenario. Refiere con alegría y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"convivenc\"]\n\nenca. No escatima respuestas ni regatea verdades; sus verdades, las de una bailaora que sabe del amargor de los sufrimientos y de las dulzuras de los triunfos. Se lamenta de la posible falta de reconocimiento oficial a su impartición de clases, como si hubiera alguien que pudiera autorizar o aprobar su arte, un arte que ha sido doctorado por todos los públicos del mundo en el examen diario del escenario. Refiere con alegría y cierta nostalgia su convivencia con otros artistas y presume de haber tenido el valor de rebelarse contra todos por amor. -¿Cómo comienza tu andar flamenco? hizo la mili con él—y muchos más. Esto, desde muy niña, me influenció bastante. Luego, en la parte de atrás de mi casa, que era una fábrica, los gitanos hacían sus \"gatos\", como ellos suelen decir; conviviendo con personas y con familias de una estirpe fuera de serie, como la de los \"Pelúos\", los \"Amador\" —que antes eran Tío Raimundo—, los \"Currilis\"... Todo eso lo he vivido y me ha llenado de arte, y para colmo me casé con un gitano maravilloso que es mi marido Rafael, lo que supuso rizar el —Cuando comienzo a enamorarme de la gente que veía por la tarde, en la puerta de mi casa, pues tengo 61 años y por tanto he vivió los últimos años de los barrios que se alumbraban con luz de gas, concretamente en el de Chapina, el Zurraque que es donde yo me he criao, y porque me ha gustao muchísimo la música. Mi padre cantaba muy bonito la soleá de Triana y tenía amigos como \"El Arenero\", Manolito Olivé —que rizo. Sé también muchas cosas por mi marido de la fragua de los \"Caganchos\"; él le sonaba a Curro Puya, padre de Gitanillo de Triana y su primo Titi le sonaba a la familia Canales. —Entonces, tu baile se forma de lo aprendido de las familias. —¡Claro, como se bailaba antes! En aquellos tiempos no había escuelas de baile flamenco; esto ha venido después. Lo que sí había en Madrid era una serie de escuelas de gitanas mayores que sí se dedicaban a la enseñanza del flamenco. Después, a partir de Enrique el Cojo y Realito, sí que hubo, pero éste era otro baile, un baile más estudioso dentro de la raíz, porque estábamos más cerca de la base. Hoy hay más estudio que raíz, por desgracia. -¿Hubo antecedentes en tu familia en el baile? —Mi abuela Pepa bailaba muy bien por tangos; era alta, morena, con ojos verdes... Según ellos era \"gachi\", cosa que yo no pongo en duda. Yo me parezco a mi bisabuela, qu\n\n[ENDING CONTEXT]\n\nen la siguiriya, zarpazo en la soleá, garra que es filo y cuchilla de carcelera y toná. Gitano de viejos ritos que en la copla se desgarra y se fue muriendo a gritos con falsetas de guitarra. Abuelo del cante jondo, emperador del compás; venero de limpio fondo que no se agota jamás. Voz que rompe la tiniebla y en las entrañas se mete. Rey flamenco de la debla y señor del martinete. Calor de fragua gitana, sabor de vino y de mar... ¡Jerez, los Puertos y Triana bicieron su voz vibrar! Su cante fue la cadena que estrujó su corazón... ¡Ay, que entre Alcalá y Mairena se está muriendo un león!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Matilde Coral, sabiduría flamenca y bailaora",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "12-15",
    "page_number": 12,
    "word_count": 3618,
    "article_char_count_full": 20064,
    "article_char_count_review": 4026,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "convivenc"
      }
    ]
  },
  {
    "article_id": "1996-03-16-left-manuel-yerga-lancharro-en-las-d-",
    "article_text_for_review": "Me dice el Sr. Cabrera Bonilla, de la Puebla de Cazalla, afincado en Martorell (Barcelona), que por aquellos lugares de Cataluña se viene diciendo, entre aficionados, que hoy se canta mejor que se cantó en los años diez al treinta. Que el cante valía menos. Pues bien, yo tengo que decirles que son unos nescientes y que me perdonen. Que para expresarse así, generalizando, hay que estar en posesión de unos conocimientos muy amplios, sabiendo comparar los cantes de ayer con los de hoy. Tienen que saber que lo mismo que antes hubo cantaores excelentes y no tan excelentes, hoy acontece lo mismo. Con la ventaja de antes, porque existieron cantaores-creadores que por desgracia para nosotros han desaparecido para siempre.\n\nY en cuanto al toque de la guitarra, insisto diciendo una y mil veces que entonces se acompañaba más pausadamente y con más \"honradez\" hacia el cantaor. Por el contrario,\n\nhoy lo hacen más que por otra cosa, por exponer públicamente su virtuosismo de auténticos concertistas, imprimiendo a sus ejecuciones la velocidad de un bólido.\n\nEl cante, desde su aparición en público, ha tenido unos intérpretes y aficionados que han actuado de correa de transmisión en el tiempo, haciendo posible que el cante, con sus altibajos normales, continúe llegando hasta nosostros y así será por los siglos de los siglos.\n\nYo recomendaría, que para hablar con cierta autoridad sobre el tema que nos ocupa, se tiene que estudiar con atención los cantes y de forma primaria las siguiriyas, soleares, malagueñas, tarantas y cartageneras de aquel \"catedrático del cante\" que se llamó artísticamente Fernando el Herrero. Para mi gusto el mejor intérprete de los cantes de poder. ¡Fernando fue sencillamente genial! ¡Lástima que los hijos de Las Cabezas de San Juan no quieran saber nada de su memorantísimo paisano!\n\nTambién recomiendo que se escuchen, agudizando el sentido, los siguientes cantes:\n\nPor Lucena al \"Niño de Cabra\". Por tarantas al \"Cojo de Málaga\", Manuel Escacena, \"Pepe Marchena\", \"Niño de la Puerta del Angel\" y a \"Pepe el Molinero\". Por malagueñas al \"Pena Hijo\" y a \"Manolita de Jerez\". Por peteneras al \"Niño Medina\", excelente continuador que fue de la escuela de su padre, a quien le debemos la creación de este cante. También, y por supuesto, a la discípula del Viejo, la \"Niña de los Peines\" que, acompañada a la guitarra por el malogrado Luis Molina, nos legaron \"un dulce\" de cante y toque. Por siguirias a \"Fernando el Herrero\" y a \"Paco Mazaco\". Y por fandangos a \"Antonio el de la Calzá\", \"El Carbonerillo\", \"El Corruco\", \"El Niño León\", \"El Sevillano\" y \"Pepe el Molinero\".\n\n¡Ah!, se me olvidaba decir a esos aficionados radicados en Cataluña: ¿Existen en la actualidad cantaores que sean capaces de superar o de igualar los cantes de Fernando el Herrero? Creo que ni los hay ni los habrá. Por la misma razón por la que no habrá otra \"Niña de los Peines\". Estos son fenómenos de la madre naturaleza que se producen una sola vez en la vida. ¿Y, qué me tienen que decir del último tercio de los fandangos de \"El Carbonerillo\", \"Niño de la Calzá\" y de José Rebollo Piosa, de Huelva? ¿Hay quien sea capaz de interpretarlos? Creo que no, porque se trata de un fenómeno de interpretación sin posible explicación. Me consta que algunos artistas-copistas tuvieron el atrevimiento de cantarlos, pero fracasaron en su intento. El último tercio, o lo que es lo mismo, la llave que cierra el cante por fandangos de esos genios, imposibilita a todo aquel que se atreva con él. ¿No es verdad que hasta ahora nadie se ha dado cuenta de esta imposibilidad?\n\nY ya, para finalizar, ofrezco a esos aficionados este ejemplo que es, sin duda alguna, muy a tener en cuenta: En Huelva existe un buen cantaor, no profesional, que interpreta todos los estilos de fandanguillos de su tierra y los fandangos que a través de Dolores Parrales llegaron procedentes del árbol malacitano, de forma muy excelente, incluso por el de Pepe Rebollo que ya es decir, pero al llegar al último tercio, se acabó lo que se daba: no consiguió interpretarlo con fideidad.\n\nNota: Yerga Lancharro pide a quienes corresponda, que se organizen con frecuencia en pueblos y ciudades de Andalucía, madre única del arte flamenco en sus tres facetas, concursos de cantes y de guitarra, para que de esta forma, conseguir nuevos valores que den vida perdurable a nuestro arte singular.",
    "title": "¿En las décadas de los diez, veinte y treinta, se cantó peor que en la actualidad?",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 738,
    "article_char_count_full": 4365,
    "article_char_count_review": 4365,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-03-17-left-se-nos-fue-am-s-rodr-quez-rey",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFrancisco del Río Moreno Sus cenizas reposan en la Caleta de Cádiz, cuyas aguas recogieron también las de su hermano Beni de los que el artista cantaba en sus coplas. Para Amós, esto no fue posible, ya que un fuerte temporal de levante lo impedía. Se tuvo que arrojar las cenizas al mar desde la muralla del propio castillo de San Sebastián, a la vista de La Punta del Nao, hacia donde las olas llevaron las cenizas y las flores a reunirlas imaginariamente con las de su hermano.\n\nSebastián, faro de navegantes, y que tiene forma de un gigantesco barco, de ahí su nombre.\n\nlas de su hermano Beni de Cádiz. Eso ocurría el 24 de diciembre de 1992, en el lugar conocido por La Punta del Nao, una piedra grande, sumergida frente al castillo de San\n\nEl día 6 del pasado mes de marzo, las cenizas de Amós\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\nflores a reunirlas imaginariamente con las de su hermano. Sebastián, faro de navegantes, y que tiene forma de un gigantesco barco, de ahí su nombre. las de su hermano Beni de Cádiz. Eso ocurría el 24 de diciembre de 1992, en el lugar conocido por La Punta del Nao, una piedra grande, sumergida frente al castillo de San El día 6 del pasado mes de marzo, las cenizas de Amós Rodríguez Rey eran esparcidas en las aguas de La Caleta gaditana, en un lugar cercano al que tres años antes se depositaran Aquel día, las cenizas de Beni se esparcieron desde un barco velero, Habían asistido a este último viaje su viuda, Rosario Vaca Cortés, la que fuera esposa de Beni, cariñosamente conocida por \"Perla\", representantes de peñas y entidades gaditanas encae El día, como decíamos, era gris y desapacible, soplando fuerte el levante. La comitiva había llegado a Cádiz procedente de Sevilla sobre las 2 de la tarde en varios coches, siendo recibidos en la sede de la Peña Flamenca Enrique el Mellizo, desde donde partieron hacia el castillo de San Sebastián, donde se consumó el último acto. El sepelio en Sevilla En Sevilla los actos del sepelio se iniciaban a mediodía del día\n\n[ENDING CONTEXT]\n\nGrandullón, buena gente. Cantaor completo. Tus amigos del mundo te recordaremos siempre.\n\nAmós en Cádiz, que fue una conferencia en el Aula Militar de Cultura, sobre los cantes de Cádiz y Los Puertos, ilustrada por Rancapino y Carmen Jara con la guitarra de Manolo de Ceuta.\n\nDurante la conferencia, Amós hizo gala de sus dotes de cantaor, entonando el prefacio de la misa gregoriana y después, su aplicación a la malagueña del Mellizo, malagueña que a continuación terminó Rancapino magistralmente.\n\nEste programa tuvo que ser repetido a petición de numerosos aficionados de la Bahía gaditana.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Se nos fue Amós Rodríguez Rey",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1123,
    "article_char_count_full": 6666,
    "article_char_count_review": 2792,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  }
]
```
