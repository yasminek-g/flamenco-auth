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
    "article_id": "1987-01-8-left-de-mi-memoria-una-estampa",
    "article_text_for_review": "Por más que los números rojos y negros se descuelguen de los almanayes siempre te recuerdo con tu vino y con tu cante. Eras como una estatua a medio acabar con la nebulosa impronta de tus manos faraónicas. En tu espúreo destierro de tabernas y amaneceres se dibujaba una ascendente enredadera de penas y sortilegios. De tu coro celestial te iban cayendo sombras volátiles y en tu garganta la rémora del cante se solapaba hacia un universo de carbón. Ya no hay eco, ni hay calle\n\nque soporte la miel agriada de tu estampa de cantaor oculto por donde más de una vez la flama testimonial de tus alados conciertos se hacía desconchón parietal lli en donde los ratones furibundos de la humedad creaban paisajes desolados. Y la vida tocó silencio y te quedastes traspuesto en una vereda que no iba a ninguna parte al pie de una historiada maleta en donde no había más que un quejido errante. ¡Ay! Estampa que de mi memoria sale en donde un flamenco anónimo ubicuamente, como el cantar de los grillos abre de par en par el álbum de la infancia y deja escuchar su cante.",
    "title": "Poema: De mi memoria una estampa",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 193,
    "article_char_count_full": 1062,
    "article_char_count_review": 1062,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-8-right-cantaores-conocidos-compa-eros-y",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLuis Caballero\n\nuando yo me cria- ba —como se si- gue diciendo en muchos pueblos— me gustaba el cante y lo cantaba. Cantaba, como es natural, lógico y normal en estos menesteres, lo que oía, sin saber lo que cantaba ni el valor que podían tener los cantes. Fue por entonces cuando el Niño de Marchena comenzó a prodigarse discográficamente. Había aparecido en los umbrales del cante comercial con una fuerza espectacular de éxitos verdaderamente arrolladora. No había máquina cantaora desde Madrid a Cádiz en la que no sonaran los fandangos de aquel joven ruiseñor nacido nada menos que en la sevillana y flamenca Marchena. Aquellas placas, de las que conservo alguna copia, están impresionadas con la prisa que exigían los límites técnicos de entonces, y este factor, unido a la velocidad\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionados\"]\n\ntacular de éxitos verdaderamente arrolladora. No había máquina cantaora desde Madrid a Cádiz en la que no sonaran los fandangos de aquel joven ruiseñor nacido nada menos que en la sevillana y flamenca Marchena. Aquellas placas, de las que conservo alguna copia, están impresionadas con la prisa que exigían los límites técnicos de entonces, y este factor, unido a la velocidad melismática de Marchena —máxima en aquellos principios— originaba en los aficionados una especie de esfuerzo disparado no exento de cierta gracia caricaturesca. Todos queríamos cantar como Marchena o lo de Marchena, o lo del Carbonero, o —después— lo de Canalejas, etc. Queríamos cantar lo del último aparecido como novedoso, ya fuera bueno o regular, eso no importaba, lo que importaba era la novedad, la última «creación» del cantaor más de moda. Mientras tanto rechazábamos despecti- el cante es arte y el arte vive, crece y busca horizontes evolutivos en la juventud, naturalmente mirando hacia atrás para seguir adelante. Que en esa búsqueda el mal de fondo haga naufragar barcos que debieron y debían seguir navegando con cualquier tiempo ya es otra historia. «Fue creador e impulsor de la ópera flamenca. Su musicalidad y el preciosismo de su voz fueron armas suficientes para colocarse como una primera figura del género» vamente el «cante viejo», el cante de nuestros antepasados, el cante que también podía escucharse en algún disco y, por qué no, en alguna de nuestras propias casas, en las voces de alguno de nuestros propios padres. Tal vez haya, raramente, quien pueda extrañarse de que así fuera, será porque no ha reparado en que así sigue siendo porque así es. Y es así porque La nuest\n\n[ENDING CONTEXT]\n\ncomo jirones históricos-sentimentales del cante andaluz. Ya se le ha hecho bastante daño al cante, al cantaor y a la misma Andalucía con tanto divisionismo irrespetuoso, ignorante y personal.\n\nJosé Tejada Martín, el archipopular Niño de Marchena en sus primeros tiempos, para en los últimos llamarse definitivamente Pepe Marchena, murió con resignado valor y serenidad. Había apurado la vida, su vida, hasta la última gota. Días antes de su muerte mi mujer y yo fuimos a verlo. Mi paisana Isabelita, su mujer, nos contó que le había rogado lo amortajara de smoking. Original hasta después de muerto.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantaores conocidos, compañeros y amigos: Pepe Marchena",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1132,
    "article_char_count_full": 6932,
    "article_char_count_review": 3303,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionados"
      }
    ]
  },
  {
    "article_id": "1987-01-9-right-cantaores-conocidos-compa-eros-y",
    "article_text_for_review": "CANTAORES CONOCIDOS, COMPAÑEROS Y AMIGOS\n\nLuis Caballero\n\nuántas veces me dijo Antonio Mairena esa indiscutible y experimentada verdad de que nunca se acaba de aprender a cantar y a saber de cante por muchos años que se viva! ¡Cuánto tenemos que rectificar los que cantamos para ir aproximándonos a un mejor hacer, y cuánto, al mismo tiempo, los que también buceamos en esas profundidades abismales tergiversadas tantas veces por espejismos! Nuestros propios discos, por ejemplo, vienen a corroborar esta inmovible afirmación: ¿Qué cantaor está conforme, sobre todo, con lo que grabó hace diez años? Fosforito me decía que a todos aquellos que le trajeran un disco de los primeros que hizo se lo cambiaría con mucho gusto por uno de los últimos. La vida es un continuo aprender rectificando, y la verdad es que mucho he debido yo rectificar hasta llegar a comprender, sentir y catalogar como maestro Pepe Pinto, sobre todo después de tanto tiempo asegurando lo contrario. Naturalmente que nos estamos refiriendo al Pinto cantaor de días que esos arreglos los componía él —«a ver si se entera la gente»— con cachos de cante puro.\n\nPepe Pinto sabía cantar y lo que cantaba, sin dejar de tener en\n\nSabía cantar y lo hacía bien. Su afición no tenía fronteras, pero sabía que cantaría siempre sobre unas cualidades instrumentales de voz negadas por la ciencia especializada, pero el cante era su vida\n\ncante grande y nunca al intérprete de fragmentaciones amalgamadas con argumentos seudopoéticos. Ese, queramos o no, y lo digo porque de algún modo subsiste, es otro cantar, aunque mi querido amigo Pepe dijera por aquellos cuenta que lo que cantaba lo cantaba bien. Era consciente de su condición como cantaor profesional y no perdía la menor ocasión de aumentar sus extensos conocimientos. Su afición no tenía fronteras. Lo dejó todo por el cante a\n\npesar de saber que cantaría siempre sobre unas cualidades instrumentales de voz negadas por la ciencia especializada. Pero el cante era su vida, soñaba con el cante.\n\nLe oí decir repetidas veces, porque repetía mucho las cosas, aquello de: «Señores, de verdad, por mi santa madre, yo he despertao más de una noche a Pastora pa que me dijera si un tercio de una soleá o una seguiriya o lo que fuera era así o no». Tenía a la casa de los pavones en el sentido sin dejar de admirar a don Antonio Chacón hasta la idolatría. «Yo ganaba, nos decía, mucho dinero de “grupier” allá por los años veinte y le pagaba a Chacón pa escucharlo. Era un monstruo y yo aprendí mucho de él. Todavía no se ha dao otro igual». Y Chacón moría con Pastora. Pastora, ¿qué te hacía Chacón escuchándote? «Me besaba las manos, decía Pastora. De rodillas, me besaba las manos. Chacón era un caballero, un señor y un artista fenomenal».\n\nA mí, hasta Pepe y Pastora, como amigos, me llevó Mairena. El Pinto desconfiaba de nosotros los «redentores» del cante, los empeñados en hacer volver las aguas a su cauce natural. El sospechaba (más bien negaba) pudiera saber de cante el universitario, el intelectual y el oficinista, por ejemplo;\n\nesa gente que no sabe hacer compás ni decir olé a tiempo. Sin embargo, alguna de esa gente le erigieron un monumento a su mujer en la puerta de su casa. ¿Cabe más? Entonces su agradecimiento no tuvo límites, de lo que todos nos alegramos, pues, evidentemente, murió con la plena satisfacción de ni saberse olvidado él ni la cantaora de su corazón, ya para siempre en bronce encabezando la legendaria catedral del cante: La Alameda de Hércules.\n\nNo es que dejaran de tener fundamento sus dudas y precauciones antes de convencerse de que el movimiento renacentista del cante constituía, por fin, una realidad avalada por la fuerza de la fe, la perseverancia y la autoridad. El decía, y con razón, que su mujer no volvería, y no volvió, a cantar más en ningún escenario, pues hasta en el mismo teatro San Fernando de su propia y amadísima Sevilla no la habían comprendido ni valorado. Tuvo que ser Antonio Mairena —y éste sí que captó de inmediato el futuro renacentista— quien consiguiera que Pastora entrara por la puerta grande de la Universidad y aceptara los propósitos que luego se convertirían en hechos contundentes. Pepe Pinto lo vio con lágrimas en los ojos: En el bello redondel central del Casino de la Exposición su mujer volvió a ser aclamada cuando bailó autoacompañándose por bulerías. El público que ahora aplaudía era de lo más prometedor: eran estudiantes y éramos nosotros, los desfacedores de entuertos flamencos. Aquella noche y otra más yo estuve con Mairena al lado de Pepe y Pastora. «Qué buen cantaor se ha hecho este muchacho», decía Pepe refiriéndose a Antonio, y Pastora decía: «Pepe, dime las letras pa yo cantarlas. No me acuerdo más que de una».\n\nAlgunas mañanas de invierno tomábamos el sol con ellos en la puerta de su célebre bar de la Campana. Pastora sentada, contándole a mi mujer cosas de su hermano Tomás, Pepe de pie, hablándonos de él, de su mujer, de Chacón..., regalándonos discos suyos dedicados, retratos de su Pastora, invitándonos, explicándonos el cante, su cante, los cantes, descubriéndonos su alma de hombre bueno, cariñoso, trabajador y artista.\n\nPero... la perturbación mental de su compañera lo desquició hasta la autodestrucción.\n\nY quiso la casualidad que también mi mujer y yo nos enteráramos de su muerte al mismo tiempo. Nos lo dijeron dos amigos en el bar del teatro Cervantes. Uno de ellos era Pepe Marchena que volvía de verlo por última vez. En otra habitación de la casa, Pastora, ausente de esta pena, se debatía en la suya incoherente y alucinante, mientras la Almeda iba quedándose sin esas otras dos columnas de Hércules del Cante.",
    "title": "Cantaores conocidos, compañeros y amigos: Pepe Marchena. 15 y 16 Cantaores conocidos, compañeros y amigos: Pepe Pinto",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "9-10",
    "page_number": 9,
    "word_count": 964,
    "article_char_count_full": 5652,
    "article_char_count_review": 5652,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-10-right-cantaores-andaluces",
    "article_text_for_review": "Guillermo Núñez de Prado\n\n(Selección). Biblioteca de la cultura andaluza. Barcelona, 1986\n\nJosé Luis Buendía López\n\n» L os pueblos que más cantan\n\nfolclore 公 BIBLIOTECA DE LA CULTURA ANDALUZA\n\nEn este sentido el libro de Núñez de Prado, por cuya reedición felicitamos a los responsables de la Biblioteca de la Cultura Andaluzia, constituye una rareza bibliográfica inusitada y una fuente cambiante de apreciaciones por nuestra parte. El libro es precioso porque es único en su género; resulta apasionante, aunque a veces su retórica sea un ejemplo rayano en la ridiculez; debe consultarse porque demuestra cumpliamente cómo por vez primera un\n\nson los que más sufren». Así comienza el autor este recorrido sentimental a través de los protagonistas del arte jondo; porque no otra cosa constituye el presente trabajo que unas patéticas aproximaciones a la atmósfera social y emocional que sirviera de soporte al cante a principios de este siglo y finales del anterior. La obra fue publicada en 1904, cuando ya nombres como los de Demófilo, Balmaseda, Rodríguez Marín, etc., habían realizado el esfuerzo supremo de bucear en la expresión poética popular, de sentar las bases del folklore como vía de aproximación a la esencia misma de las gentes más sencillas de nuestra Andalucía. Unos años más tarde (en torno a la década de los treinta) José Carlos de Luna, los hermanos Caba o Fernando de Triana, desde diferentes puntos de vista estéticos e ideológicos, comenzarían a sentar de manera pretendidamente científica las bases de una verdadera flamencología.\n\nautor de ideología absolutamente progresista es capaz de romper una lanza por la categoría humana y profesional de los creadores e intérpretes del cante jondo, relegados hasta entonces a meras comparsas de una sociedad injusta que les daba la misma importancia que a las realas de perros para la caza o al caballo preferido del señorito, sólo que no tenían ni la cuidada alimentación ni los decorosos habitáculos de éstos.\n\nEn efecto, y comenzando por la parte negativa del trabajo, hay que señalar de qué engolada manera se traza un esquemático y encorsetado cuadro de los artistas flamencos en el cual caben todos los tópicos que se han vertido sobre los mismos: así, Anita la de Ronda: «Ha vivido esclava del amor como una perra de su dueño»; la Parrala era: «El arcángel rebelde de la belleza desplumando vivo al trovador de los bosques, para distraer con las notas de la melancolía tristísima del martirio, su espantable aburrimiento».\n\nTal iracundia verbal que hacen al libro verdaderamente penoso en gran parte tiene como disculpa el que así solía construirse una gran parte del discurso decimonónico, del que Núñez de Prado es heredero: ampuloso, exagerado en adjetivos, truculento. Frente a ello no podemos olvidar la otra cara de la moneda que antes señalábamos, el carácter progresista del autor que se vierte en el texto en todo momento al analizar multitud de facetas de la vida y personalidad de los artistas: es por ello que no duda en proclamar el derecho que asiste a Antonia la de San Roque a exhibir legítimamente su lesbianismo, que sólo puede molestar a una moral: «Hipócrita y carnavalesca que rige a una sociedad compuesta de máscaras y farsantes en todas las esferas y en todos los órdenes». En el terreno de la justicia humana considera a ésta ejecutada por hombres poco honestos y escasamente legitimada por tanto para condenar a sus semejantes, y no duda en equiparar los derechos del presidiario a los de la mismísima familia real, porque hasta el mismísimo criminal, afirma Núñez: «Es de cualquier modo y mírese como se mire, un organismo tan bien o quizás mejor acabado que el de nuestros muy amados monarcas».\n\nNo creemos necesario multiplicar los ejemplos. Todo el libro abunda en sentimientos de solidaridad, de igualdad con los desheredados de la fortuna; es de una bondad filantrópica hacia sus semejantes que lo hacen emocionante y distinto a los demás. Así debe ser mirado y admirado, no para aprender en él, pues los errores son de bulto: claro está que ni Silverio era argentino ni Rojo el Alpargatero de Cartagena, sino para reflexionar en voz baja con este autor de principios de siglo y poner nuestro corazón en hora con una de las voces más apasionadamente defensoras de la limpia honradez y la grandeza de nuestro arte.",
    "title": "Aunque no quepa en el papel: Cantaores andaluces",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 717,
    "article_char_count_full": 4320,
    "article_char_count_review": 4320,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-11-left-podio",
    "article_text_for_review": "En espacio de honor en el Podio para esa institución del nombre largo y la eficacia patente en favor de los viejos artistas flamencos. El festival de Cádiz, con la valiosa cooperación de las entusiastas huestes mellizeras de Antonio Benítez ha dejado un montoncito más para aliviar tantas duquelas y poner también encima del podio a los artistas en activo solidarios de sus compañeros desvalidos.\n\nY en el mismo sitio y en olor de gratitud, el Ayuntamiento de L'Hospitalet de Llobregat que de su propio presupuesto de gastos —no menguados— ocasionados en la organización del XIV Congreso de Actividades Flamencas ha detraído, también a favor de la ITEAF, una sustanciosa cantidad que tal vez constituya el más sobresaliente éxito de cuantos han tenido los munícipes hospitalenses. (Algunos impacientes, habrán podido comprobar que lo que funciona, funciona... y punto). A los amigos Pujana, Loli y Ruiz, artífices del Congreso y valedores del flamenco en la novena provincia andaluza. Enhorabuena.\n\ninconmiserativamente a la Picota, expuesto a la censura de la afición, un librito recientemente editado que comprende montones de disparates más o menos informativos e históricos. Pasemos por alto que en caracteres notorios (negritas) se señalen como cantaores sobresalientes los nombres de El Espeleta, Diego el Morrullo, Pedro el Morrao, El Ojana, Serenita de Jerez, Francisco Delgado El Tato, Paco Laberinto (como cantaor); Niño de los Rizos, Carmen Amaya (cantaora), la Andona, Micaela la Chunga (cantaora), Maestro Patiño (cantaor), Manolo Manzanilla, Joaquín la Cherma, S. Valladares (?) y Manuel Adorno, en negritas,\n\nrepetimos, o mayúsculas totales; mientras que Antonio Mairena, por ejemplo, figura en la caja menor (ínfimo relieve).\n\nPasemos también por alto que, aparte de los nombrados y bajo un mismo epígrafe de «Cantaores más conocidos», se cite a Paco de Lucía, Manolito Parrilla, Manolo el de Huelva, Ramón Montoya, Paco de Lucena, Manolo Cano, Miguel Borrull... (puestos a poner, echamos de menos el nombre del señor Pulpón). Pero la globalidad de la desinformación y el desaliño convierten en inapelable esta sentencia condenatoria.\n\nPor este descuidado tratamiento sostenido por quien se tiene por flamencólogo; por la presencia en el texto de disparates tan solemnes como el de atribuir a Merced la Sernetá el haber dado nombre (?) y sello a las soleares de Alcalá, dicho sea a título de mínimo ejemplo de un largo repertorio de dislates; por ser introducida la publicación a través de un prólogo en el que se la califica de texto de estudio, «como la básica del cante, fuente donde ha de beber el profano...»; por todo lo que se dice y por lo mucho que se calla, la condena a lamentable exposición pública se hace inevitable.\n\nAun conscientes de que el castigo no ha de resultar ejemplarizador en este caso, no dudamos en mandar a la Picota a Televisión Española en su programa «Los Flamencos», el 25 de enero último dedicado a Antonio Mairena. Del abundante material grabado en Madrid y Sevilla que la Gran Casa tiene en su fonoteca de Antonio Mairena, se eligió el número menos adecuado para una solemnización. Pero mucho peor fue la segunda parte. Tras una discreta actuación de Manuel Mairena y José de la Tomasa, capaces ambos de logros infinitamente superiores, vino un final distorsionante, aberrante casi. Ni el baile elegido —la Petenera, ¡vaya por Dios!— tras la escenita de la echada de cartas, ni el monótono baile, ni la impropiedad del cante con un intérprete sin apenas voz (¡vaya fusilamiento de los tangos trianeros tan maltratados por un descendiente de una gran casa cantaora!), ni la escasa capacidad de la guitarra sirvieron, todos a la vez, al fin propuesto, y por ello nadie se salvó de la quema. Y, mucho menos, nadie se salvó de la Picota y por encima de todos la defectuosa programación, fruto muy posible de la precipitación. Ventolera",
    "title": "Podio",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 633,
    "article_char_count_full": 3883,
    "article_char_count_review": 3883,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
