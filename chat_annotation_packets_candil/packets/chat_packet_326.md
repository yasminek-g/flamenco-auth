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
    "article_id": "1996-11-17-right-sobre-la-poes-a-del-cante",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n“En el océano de la poesía española de los últimos siglos pocas veces, ni en la poesía popular ni en la poesía culta, han sido aventajadas en precisión expresiva, en emoción y en dramatismo las mejores coplas gitano-andaluzas”. (Félix Grande) Francisco Cruz Pérez\n\nCon mi participación en este Congreso no pretendo descubrir nada a los aficionados cabales del cante, porque no soy un estudioso de esta forma de expresión. Si me apuro, quizá no sea inútil aclarar que mi acercamiento al mundo flamenco no se debe a mi interés por el cante jondo. Admiro sin tapujos hasta donde alcanzo a entenderlo y a sentirlo, su belleza escalofriante, pero la plenitud de su dimensión artística afecta escasamente a mis necesidades espirituales. Sin embargo, hay en el arte flamenco algunas letras —pocas, a mi\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"comparad\"]\n\ns del cante, porque no soy un estudioso de esta forma de expresión. Si me apuro, quizá no sea inútil aclarar que mi acercamiento al mundo flamenco no se debe a mi interés por el cante jondo. Admiro sin tapujos hasta donde alcanzo a entenderlo y a sentirlo, su belleza escalofriante, pero la plenitud de su dimensión artística afecta escasamente a mis necesidades espirituales. Sin embargo, hay en el arte flamenco algunas letras —pocas, a mi juicio, comparadas con la ingente cantidad de ellas que ya han surgido del cante mismo— que sí entran de lleno en las urgencias nutritivas de mi sensibilidad y que tienen que ver de modo directo con la creación poética. Así pues, me sitúo ante este corpus de letras con el único criterio de un lector de poesía y no de un curioso por las creaciones de tipo popular. La poesía, cuando se despega del ripio, del tópico consabido, es sólo una. De la misma manera que la llamada poesía culta, en rigor, nos ofrece menos poemas de incuestionable calidad creativa de los que a simple vista suelen admitirse, las letras flamencas que pueden ser consideradas poemas sin paliativos, una vez escindidas de su contexto social y ámbito musical, son, como ya he escrito, muy pocas. Pero las que son, poseen el don más propio de la mejor poesía: ser testimonio de una realidad determinada y, a la vez, abrirla a una nueva dimensión in sospechada de la experiencia humana. Estas letras, además de referir lo vivido, le añaden realidad. Por eso, se erigen en verdaderos poemas, a pesar de estas observaciones de Luis Rosales: «El cante no se escribe, (...)...el cante jondo se apoya, mucho menos sobre la letra que cualquier otro cante. (...) Yo diría que el cante, aislado de la letra, se desvalora, pero la letra aislada del cante, no sólo pierde su valor sino su sentido». $ ^{1} $ Es esto, justamente, lo que les ocurre a aquellas letras que no superan su estricta condición de letras y que sólo en la interpretaci\n\n[ENDING CONTEXT]\n\nestructuras verbales simples de la tradición, llenas de sabiduría compositiva.\n\nCon lo dicho, de ninguna manera estoy sugiriendo que los poetas de mi generación hagan letras al modo flamenco para compensar las dificultables carencias que yo he señalado. Esto supondría un error, no sólo histórico, sino estético. Sólo me atrevo a indicar un posible camino de renovación poética partiendo de esta fuente popular, aprovechando sus genuinos reacomodos estróficos y ciertos resortes de la intuición que activan la intensidad expresiva, dignos de ser incorporados al irrenunciable espíritu contemporáneo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sobre la poesía del cante",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1227,
    "article_char_count_full": 7465,
    "article_char_count_review": 3571,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "comparad"
      }
    ]
  },
  {
    "article_id": "1996-11-18-right-la-est-tica-plural-del-baile-fla",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJuan M. Rodríguez “El Mistela”\n\nPara empezar, quisiera decir que, a mi humilde entender, no se puede hablar de la «estética» en singular sino de las «estéticas». O tal vez habría que decir, centrándonos en la propuesta del Congreso, que hay una estética plural en el baile flamenco. Y ello en función de la óptica con que lo miremos, el punto de vista en que lo enfoquemos. Porque no es lo mismo el baile pasado de técnica, que el baile racial, intuitivo y personal. No es lo mismo el baile del hombre que el baile de mujer. Ni el baile que se ejecuta en una fiesta intimista y festera, que el que se ofrece en un escenario. Y ni tan siquiera es igual el baile de ayer que el baile de hoy y posiblemente tampoco sea igual el baile de las diversas zonas flamencas...\n\nA mi modesto entender —insisto\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"Arte\"]\n\nesta intimista y festera, que el que se ofrece en un escenario. Y ni tan siquiera es igual el baile de ayer que el baile de hoy y posiblemente tampoco sea igual el baile de las diversas zonas flamencas... A mi modesto entender —insisto en que no pretendo sentar cátedra ni fijar una teoría inamovible, sino simplemente dejar sentada mi postura ante esta cuestión para, después, afrontar el debate—, podríamos establecer los siguientes enfoques: I. Arte y técnica Si se entiende el arte, en resumen, como la capacidad de crear, de improvisar, de hacer una obra con los materiales que tienes a tu alrededor, la técnica será, por contra, la capacidad de asimilar estudios, de utilizar recursos y materiales, de controlar movimientos, ritmos, espacios, de una manera matemática, y consecuentemente, predeterminada. Es evidente que un baile, en abstracto, entendido con una carga improvisada y libre o, por contra, encorsetado y sujeto a leyes predeterminadas, no será nunca una expresión del baile con valor artístico. A mi entender hay que saber llegar a un justo equilibrio entre lo que entendemos como baile de «arte» y lo que entendemos por «técnica». Creo sinceram\n\n[ENDING CONTEXT]\n\nenriquecedora, podemos situar los saltos que, como nadie, relampagueantes e inverosímiles, arte puro, ejecuta Farruco, y que, sin embargo, los cogió de Fred Astaire.\n\nY lo mismo podríamos hablar de zonas bailaoras. Y lo mismo de esa controvertida y eterna disputa de bailes payos y bailes gitanos cuando yo creo que lo que existe, por encima de todo, es el BAILE con mayúsculas. Y en nuestro mundo, el baile flamenco, con sus características, con sus muchos enfoques, con sus variantes, pero arte andaluz, universal y eterno. Así, al menos, yo lo siento. Y con él comprometo toda mi vida artística.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La estética plural del baile flamenco",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1222,
    "article_char_count_full": 7176,
    "article_char_count_review": 2789,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "Arte"
      }
    ]
  },
  {
    "article_id": "1996-11-20-left-la-est-tica-y-el-braceo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDado el interés que me merece la entrevista efectuada a la bailaora Matilde Coral por D. Rafael Valera, que aparece en la página 2.352 y sucesivas del número 104 de la revista Candil, a juicio muy personal, la considero importante como lección y experiencia para aquellos no entendidos, entendidos y «enteraos», como se dice dentro de la jerga flamenca con rito del «jondo».\n\nApreciando la sabiduría de la bailaora Coral, me llama la atención, entre otras preguntas que le formula el Sr. Valera, esta en particular: «¿Llevaba razón Vicente Escudero cuando se refería a que un hombre perdía masculinidad cuando levantaba los brazos por encima de la cabeza?». Dicha pregunta me ha dejado perplejo. Nada más inaudita la duda que deja entrever el Sr. Valera, no así la correcta contestación de la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"flamencólogo\"]\n\nlar: «¿Llevaba razón Vicente Escudero cuando se refería a que un hombre perdía masculinidad cuando levantaba los brazos por encima de la cabeza?». Dicha pregunta me ha dejado perplejo. Nada más inaudita la duda que deja entrever el Sr. Valera, no así la correcta contestación de la bailaora, demostrando estar en lo cierto y «enterada». Me pregunto, ¿en qué ha podido fundamentar el Sr. Valera tal pregunta, dado que le consideró documentado y buen flamencólogo? Yo no tengo más remedio que enmendarle y lo haré de manera acorde y consecuente, con juicio crítico basado en uno de los diez puntos del DECÁLOGO DE LA DANZA que Escudero estableció, después de meditado estudio, para ser aplicado por todos aquellos bailarines y bailadores que quisieran seguir la verdadera tradición del baile flamenco puro (jondo) y masculino, con el respeto indispensable a la estética y plástica sin mixtificaciones, virtudes estas que deben acompañar siempre a un buen bailaor. Punto 6°: Armonía de pies, brazos y cabeza P rueba evidente es la acertada contestación de Matilde Coral, llena de dignidad y poderosa razón: «Vicente Escudero fue el primero que los levantó. De hecho, el éxito de Antonio Gades ha sido sus brazos y los mueve exact\n\n[ENDING CONTEXT]\n\nmasculinidad cuando levantaba los brazos para bailar. Por tanto, queda así aclarada la ci-tada pregunta del Sr. Valera, la cual puede dar lugar a lamentables y ten-denciosos equívocos. Sí estoy con Matilde Coral en el sentido de que ciertas frases extrapoladas de los comentarios de los artistas, pueden ser aprovechadas por «estudiosos» y transcribirlas, con intereses comerciales, cambiando su sentido original.\n\nSi los datos aportados han servido para esclarecer uno de los muchos entuertos existentes en la abundante literatura despiadada sobre el flamenco, me sentiré plenamente satisfecho.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La estética y el braceo",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1446,
    "article_char_count_full": 8855,
    "article_char_count_review": 2850,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "flamencólogo"
      }
    ]
  },
  {
    "article_id": "1996-11-21-right-y-se-despertaron-los-duendes-en-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nN\n\no siendo muy propio el escenario, sin embargo así ocurrió. Aquello nada tuvo que ver con el entreguismo de un público facilón al que la ramplonería imperante machaca a golpe de voces y gorgoritos. No, lo que presenciamos sin duda se incardina en las noches jondas e inolvidables de las primeras movidas festivaleras de los años sesenta.\n\nTras una suculenta olla flamenca los apenas doscientos comensales que nos dimos cita en el Restaurante «Las Pedrizas» de Casabermeja, fuimos obsequiados con un modesto cartel flamenco que después resultó bastante menos modesto. Sin desmerecer a ninguno de los que en él figuraban, la atención por lo novedoso fue rodeando la figura del manilveño Andrés Lozano, quien tras cantar por tientos-tangos y alegrías en los que nos dejó los recuerdos de Curro Valero\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"ritmo\"]\n\no novedoso fue rodeando la figura del manilveño Andrés Lozano, quien tras cantar por tientos-tangos y alegrías en los que nos dejó los recuerdos de Curro Valero con los cantes de Gaspar de Utrera, Cojo de Málaga, Pastora, Antonio Mairena y Beni de Cádiz, logró tres formidables malagueñas que harán historia, sobre todo las atribuidas al jerezano Antonio Chacón y a La Trini. La primera parte la cerró con una tanda de soleares algo aceleradas en su ritmo que nos trajo los recuerdos de Enrique el Mellizo, Manuel Torre, Juaniquí, Frijones, La Andonda y Joselero. Tras el cantaor de Manilva subió al escenario Pepe Cañete con su hija María José, y los hermanos Requena, uno de ellos al cante y el otro con la guitarra. Se bailó por soleá y por alegrías con cierre a la bulería con gracia y temple, pero algo deshilvanado en su ritmo y acompañamiento. En la segunda parte sí logró brillo Pepe Cañete, sobre todo en soleares que se rebuscó hasta conseguirlo en los cantes de Juan Villar. Luis Soler Guevara Por fandangos los recuerdos fueron para Joaquín el Canastero, Chocolate y Pepe Aznalcóllar entre otros. Por tarantos su impronta nos la trajo desde el hacer del maestro Fosforito con su «Porque ya no aguanto más». Por malagueñas nos trajo un cante de la Trini que cerró con el fandango de la calle Rute. En su actuación por bulerías nos repitió el disco del inolvidable Manolito de María. Su diálogo con la guitarra quizá no alcanzara los frutos apetecidos, y es que el cantaor de Cañete la Real a veces se distrae en demasía con el público en claro intento de agradar, más que de rebuscarse asimismo. Con todo nos agr\n\n[ENDING CONTEXT]\n\nun riñón hacer cultura, pero que tie- nen clara sus raíces como pueblo. De nombre también Andrés Lozano, como el cantaor, su coincidencia pro- vocó en el curso de la noche no po- cas anécdotas. Le vimos un hombre preocupado por su pueblo y por la cultura de sus gentes.\n\nQue tomen nota los promotores de festivales y las peñas flamencas. Todavía quedan artistas con talento, sabor y gusto en esto del cante, y que el cantaor de Manilva afincado en San Pedro de Alcántara, Andrés Lozano, es uno de los pocos que presenta, con toda justicia, el reclamo de un lugar y un sitio en el mundo del cante.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "...Y se despertaron los duendes en Casabermeja",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1403,
    "article_char_count_full": 8222,
    "article_char_count_review": 3248,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "ritmo"
      }
    ]
  },
  {
    "article_id": "1996-11-22-right-el-flamenco-en-asturias",
    "article_text_for_review": "R. S. García de Cuarto\n\ní, la llamada Villa de Jovellanos dio al flamenco un cantaor cuya profesión la refrendaron los documentos oficiales que son de exigencia ineludible en estos casos. Era aquél, José González, a quien, por consecuencia de haber sido presidente de un modesto club de fútbol, se le conocía por «El Presi», que si bien llegó a alcanzar lauros indiscutibles en la interpretación de los cantos de su tierra, sus inicios artísticos —aunque raro pudiese parecer— discurrieron por la senda del flamenco.\n\nJosé González Cristóbal (Gi-jón, 1908-1983), por ser hijo de militar, vivió parte de su infancia en diversos puntos de la Península, correspondiendo sus estancias últimas a Africa y Granada. Allí fue sin duda donde los ecos del flamenco se le incrustaron en el sentir.\n\nRetornado a su lugar natal acude a colegios y academias, forma parte de coros que interpretan canciones asturianas y otras de aires suramericanos, pero él no deja de prodigar el flamenco en tertulias y círculos de recreo.\n\nAsí las cosas, llega el año 1929, y «El Presi» decide ya presentarse como cantaor de flamenco. Le acompañan a la guitarra otros gijo-nenses que, tal vez aleccionados por\n\nbarbero castizo —en Gijón también los hubo— se despachan a modo en el manejo de la sonanta. A partir de allí, en todas las representaciones públicas que hicieron compañías de teatro asturiano y otras agrupaciones corales de Gijón, no dejó de incluirse la actuación flamenca del cantaor\n\nnacido en la gijonesa barriada del Carmen.\n\nEn 1935 viene a Gijón «Angéllo», que entonces era uno de los famosos de la Opera flamenca, Séllase la amistad, y el 22 de enero, en el Dindurra, teatro principal, cañtan al alimón con éxito señalado Ángel y «El Presi» quien, aquella noche recibe de manos del otro la documentación que le acreditará como profesional del cante flamenco.\n\nLas actuaciones de José González, instituido ya como profesional del flamenco, dieron en prolongarse hasta casi los finales de la década de 1940. Tras de algunas pasadas por diversos puntos de la región asturiana, incluso Oviedo, sale con compañías, que hacen representaciones de teatro astur, a Santander, Bilbao, San Sebastián, León, Palencia, Zaragoza y Madrid (teatro cómico) y cierra los espectáculos de aquéllas con cante flamenco.\n\nAsí fue el desarrollo que su biógrafo* calificó como primera etapa artística de «El Presi». La flamenca, decimos nosotros. Después, alentado por un compositor regional con fama, adoptó la decisión de encauzar sus inquietudes artísticas por el derrotero de los cantos de su tierra, y aquí se consagró como intérprete singularísimo en la faceta cantaora astur. Puntal señero.\n\nLa amplitud interpretativa de este asturiano en cuanto a palos flamencos ha de centrarse en lo que va del fandango a las soleares, como bien se refleja en el folleto que circuló por teatros y tablados en los que hacía comparecencia. Allí, bajo el título «Repertorio de canciones flamencas de «El Presi», aparecen, con sus letras respectivas: tarantas, campanilleros, cartageneras, fandanguillos, soleares. Es decir, a lo que podía llegar un cantar venido al arte en época de la Opera Flamenca era la moneda de cuño más alto. Esos cantes los expresaba con buenas maneras y gusto, ya un no acusando su voz ecos de resonancia purísima, sonaban bien. Lo que sí atesoraba era unas facultades envidiables, que en los cantes que permiten remate a plena voz —tan del gusto de aquel tiempo— espoleaban a la parroquia. Con todo, cantaor no desdeñable, ni mucho menos, este Pepe que —como tantos otros, por razones que son obvias— tienen en blanco su página en la historia flamenca.\n\nComo añadidura cabe señalar que «El Presi», por la influencia que en él tuvo el eco de lo flamenco, interpretó los aires asturianos con peculiar estilo. Modalidad que hubo de acarrearle más de un disgusto por críticas adversas. Decíase, y es cierto, que los sones de «por allá abajo», con los que se había identificado, hacíanle aflamencar los autóctonos de Asturias. Reprochándole, además, que para acompañamiento de ellos prefiriese la guitarra. Caso insólito. En fin de cuentas, influencias del sentir que son irrenunciables. Tan a lo claro está la circunstancia aludida, que este gijonés —intérprete egregio del cantar astur— no pudo, o no quiso, olvidarse de la maravilla que enseñoreaba su inquietud artística, y en cuantas oportunidades estimó propicias puso apéndice de cante flamenco a sus grabaciones de asturiano, que era su regla de oro, llevadas a cabo así en España como en tierras americanas en las que permaneció por temporadas luengas.",
    "title": "El Flamenco en Asturias",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 747,
    "article_char_count_full": 4589,
    "article_char_count_review": 4589,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
