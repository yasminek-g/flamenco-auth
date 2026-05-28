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
    "article_id": "1994-07-12-left-un-manifiesto-sobre-los-cantes-d",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDado el cariz puramente anecdótico, vamos a comentar un «Manifiesto» aparecido en el año 1976 en la revista Flamenco, editada por la Tertulía Flamenca de Ceuta, revista fundada por ese gran hombre y caballero que fue don Francisco Vallecillo Pecino, sabedor, como nadie, de los íntimos secretos del cante y del flamenco, en general.\n\nVaya por delante mi más «hondo» respeto hacia «Francisco de la Brecha», Paco para los amigos; hombre de gran humanidad y «respetador» de tantos y tantos «disparates» (perdón) y criterios contradictorios que, a lo largo de muchos años, se han ido vertiendo en este azotado mundo flamenqueril (sus siempre sabios consejos no los hemos olvidado; a pesar de que don Francisco, Paco para los amigos, no está con nosotros). El señor Vallecillo fue, durante tantos años\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\neto hacia «Francisco de la Brecha», Paco para los amigos; hombre de gran humanidad y «respetador» de tantos y tantos «disparates» (perdón) y criterios contradictorios que, a lo largo de muchos años, se han ido vertiendo en este azotado mundo flamenqueril (sus siempre sabios consejos no los hemos olvidado; a pesar de que don Francisco, Paco para los amigos, no está con nosotros). El señor Vallecillo fue, durante tantos años como se conocieron, un gran amigo del Maestro Piñana. Su trato hacia él siempre fue exquisito y llano; baste decir que ambos en sus contactos coloquiales y epistolares se trataban de «hermanos», y, para ser más exactos, se llamaban, el uno al otro, «hermanico». Posiblemente, este preámbulo ni siquiera tendría que hacer falta, pero, de verdad, como quiera que el «Manifiesto» que vamos a comentar «tiene guasa» (como bien habría dicho el señor Vallecillo), antes de dar nuestra opinión sería un tanto esclarecedor conocer su contenido. Sobre el mismo, y como simple comentario, don Francisco (que firma con una B, de Brecha) dice lo siguiente: De la Peña del Rojo el Alpargatero, de La Unión, recibimos el trabajo que publicamos íntegramente a continuación. No ocultamos cierto escepticismo\n\n[ENDING CONTEXT]\n\ncorresponde dentro del solar en donde nacieron, y entre otras cosas porque ellos nunca perdieron su vigencia (aunque hubieran estado olvidados durante tantos años); el Flamenco —una vez que se produjeron los diversos estilos de cante— está vivo, y los Cantes de Cartagena también los están, ocupando por su calidad un lugar preferente en este arte.\n\nRecordar la historia permite reconocer y valorar lo que es tradicional. Al mismo tiempo, y una vez dejada constancia de lo bueno que la propia historia nos ofrece, se ha de entender que es mejor olvidar aquello que poco engrandece a nuestro entorno.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Un manifiesto sobre los cantes de Levante Juan",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "11-13",
    "page_number": 11,
    "word_count": 2035,
    "article_char_count_full": 11907,
    "article_char_count_review": 2842,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "1994-07-14-left-modernidad-y-flamenco-rafael",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nN o es mi intención poner en tela de juicio la documentada aportación de Francisco Hidalgo, apreciado amigo y compañero en el devenir flamenco en tierras catalanas, en su «Ayer y hoy del flamenco en Cataluña» (Candil, núm. 90, noviembre-diciembre de 1993). Todo lo contrario. Considero que su artículo constituye una apretada y certera referencia para cuantos quieran asomarse al conocimiento de esta alargada punta norte-levantina del cuarto creciente de la geografía flamenca.\n\nHay, no obstante, un par de puntos en el referido artículo que desearía situar, de manera informal, y sin pretensiones eruditas, en el contexto específico de la cultura catalana, de la considerada oficialmente, y considerada como tal por los medios de comunicación, como «alta cultura (catalana)». Uno de estos dos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradiciones\"]\n\nido artículo que desearía situar, de manera informal, y sin pretensiones eruditas, en el contexto específico de la cultura catalana, de la considerada oficialmente, y considerada como tal por los medios de comunicación, como «alta cultura (catalana)». Uno de estos dos puntos es, por su ausencia, el de la beligerancia del «modernismo» a caballo de los siglos XIX y XX y del «neomodernismo» de estos tiempos de «posmodernidad» finisecular contra las tradiciones y expresiones musicales flamenca, aflamencada y afines («género chico»...) tan arraigadas en los ambientes populares de entonces y de ahora, por diferentes en el tiempo y naturaleza que parezcan; y otro por su rotunda presencia, el tocante a la encrucijada actual de las actividades peñísticas, caracterizadas, según Francisco Hidalgo, por una reiterativa rutina que puede resultar estirilizante. Planeando por encima de ambos, prima, en mi opinión, el intento persistente y deliberado de secuestrar la paternidad del arte flamenco a quienes lo recrean y reproducen en el ambiente sociocultural que le es propio con vistas, entre otros fines, a patentar en exclusiva aquel flamenco almacenado y expuesto «in vitro», lo que se antoja indispensable para su mercantilización aséptica (digerible por amplio público urbano de los circuitos convencionales) y su comercialización discográfica (como se decía en una reciente noticia periodística: «la industria del disco busca talentos en la calle»... y, muy particular, en la calle del flamenco) y, sobre todo, para su neutralización cultural e ideológica. Comenzaré, en sentido inverso al acostumbrado, por el segundo de los dos puntos para hilvanarlo enseguida con el primero. Refiriéndonos, pues, a las actividade\n\n[ENDING CONTEXT]\n\ncrítica en los periódicos «nacionales» de Cataluña. Por el contrario, algunos de los diarios más rigurosos dedican una crónica semanal a lo que ocurre en las salas de espectáculos estables barceloneses (A.M.D.G., se decía antaño en los colegios de jesuitas). En este sentido, dista mucho este periódismo de la información penística habitual en periódicos andaluces (por más que G. Rojo se queje de que la información flamenca no resulta primordial en los planes de ajuste de los medios regionales), tanto como dista el entorno de aquellos cronistas del de la mayoría de críticos flamencos andaluces.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Modernidad y flamenco Rafael",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 2571,
    "article_char_count_full": 16207,
    "article_char_count_review": 3350,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradiciones"
      }
    ]
  },
  {
    "article_id": "1994-07-15-right-a-don-fernando-ca-as-merch-n-man",
    "article_text_for_review": "A migo en el flamenco. He recibido su comunicación donde me dice que ha leído un trabajo mío sobre Francisco Lema «Fosforito» y no está muy conforme conmigo cuando le trato de galleguño. Lo siento, pero me reitero en lo publicado por mí. Sepa, amigo mío, que ese sobrenombre se lo puso don Antonio Chacón porque sabía, como buen amigo y compañero, que era de ascendencia gallega.\n\nMe dice, además, que «nadie conoce el segundo apellido de “Fosforito” porque ninguno de los que han escrito sobre el artista lo han dado a conocer y yo creo que como hijo de madre tendrá cuatro apellidos como todos los nacidos dentro de un matrimonio auténtico».\n\nAsí es ciertamente. Yo investigué lo que nadie sabe en Galicia y en Cádiz, para llegar a conocerlos. ¿Por qué usted y otros como usted, que les resultaría más fácil que me resultó a mí, por el hecho de residir en Cádiz, no se lanzan a realizar una investigación profunda y así conseguirían saber lo que tanto desean?\n\nVoy a hacer con usted, como paisano del galleguiño, lo que no he hecho con nadie:\n\nTronco del árbol genealógico\n\nTranscripción literal del acta de nacimiento del padre del cantar.\n\n«Elíseo López Varela, Cura Párroco de Santa María de Leiloio, Ayuntamiento de Malpica, Provincia de la Coruña, Archidiócesis de Santiago de Compostela, certifica: que en el libro tercero de Bautizados de este Archivo Parroquial, en el folio noventa y cuatro vuelto hay una partida que literalmente dice: En la parroquial Iglesia de Santa María de Leiloio a veinte días del mes de maio, año de mil ochocientos veintisiete. Yo don Francisco Borrás, Pro. Cura Ecónomo de ésta Bauticé solennemente y puse los Santos óleos a un niño que nació la noche de antecedente, hijo del matrimonio de José de Lema, natural de Cerqueda y de su mujer María Alvarez, natural de Cances y vecinos del lugar de Brión, términos de ésta sobre otras. Abuelos paternos, Jacobo de Lema, natural de Cerqueda y su\n\nmujer Rosa Fernández, difunta, natural de ésta de Leiloio. Abuelos paternos, Pedro Alvarez y su mujer Josefa Sánchez, naturales de San Martín de Cances. Púsele por nombre Francisco. Fueron sus padrinos Francisco Patiño y su mujer Doña Joaquina Sanjurjo, del mismo lugar de Brión, a quienes les advertí lo que prevé el Ritual Romano. Y para que conste...».\n\nEste niño a quien se refiere el documento que precede, es la rama de un árbol familiar de barqueros. Al hacerse hombre, como buen gallego emigró, posiblemente con su barca, hacia Cádiz en busca de un mejor vivir en su profesión. Esto acaeció el año de 1850 y en esa Tacita de Plata conoció a la chicanera Josefa, con quien contrajo matrimonio el año de 1855 en la Iglesia de San Antonio. Algun tiempo después nació nuestro cantaor, siendo bautizado en la misma Iglesia.\n\nLo demás, hasta su fallecimiento, ya lo di a conocer de forma exhaustiva creo que en esta misma revista. De su cuerpo sin vida jamás se supo.\n\nAmigo mío: ¿Queda debida- mente justificado que don An- tonio Chacón, hombre recio y muy responsable le dijese galle- guiño? ■",
    "title": "A don Fernando Cañas Merchán Manuel",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 524,
    "article_char_count_full": 3028,
    "article_char_count_review": 3028,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-07-16-left-poemas-y-coplas",
    "article_text_for_review": "Cantaor\n\nHoy has vuelto de nuevo con tu cante al tablao de tu pena sin consuelo, a vender tu amargura y tu desvelo a beberte la vida en cada instante.\n\nTragedia en torno a ti es la constante que se enjuga en quejos sin pañuelo, que se embriaga en jipíos, muerte y cielo, y que queja en un ¡ay! seco y vibrante.\n\nHoy has vuelto de nuevo con tu cante al tablao de tu pena sin consuelo, a poner en los tercios duende y celo a cantar con tu voz jonda y brillante.\n\nTu mensaje es un verso resonante que sale de tu pecho y alza el vuelo, y el temblor de tu boca clama al cielo con la fuerza de un río desbordante.\n\nHoy has vuelto de nuevo con tu cante al tablao de tu \"pena sin consuelo, buscando el eco antiguo que es tu anhelo al compás de tu huella delirante.\n\nDescubre tu camino siempre herrante detrás de algún añejó y negro velo, con melismas venidos de otro cielo a este mundo que te hizo caminante.\n\nHoy has vuelto de nuevo con tu cante al tablao de tu pena sin consuelo, a poner el calor que rompe el hielo, de tu acento cabal y deslumbrante.\n\nHaz que toda tu estirpe se levante y que el rancio conjunto que hacia el cielo elevas hecho un árbol desde el suelo, continúe el camino hacia adelante.\n\nMírate en tu memoria antepasada, rebusca en tus lamentos de jondura que hallarás una copla que nos hiera.\n\nSerá tu voz más ronca y más sincera al eco de tu grito y desventura y al clamor de tu pena consolada.\n\nCaracoles\n\n¡Esta gitana! ¡Esta gitana! Recorre (to) Madrid, esta gitana, va pregonando flores cada mañana.\n\n¡Quieremé quieremé! Como te quiero yo; Que te voy siguiendo prima, desde Cibeles hasta Mayor.\n\n¡Ay gitanita! Si tú no me quieres, pa que me incitas. Te quiero yo, con alma y vida, celo y pasión.\n\nVendo flores, vendo flores! Un ramito... compremé usté! de rosas para la novia, pa la solapa un clavel.\n\nTu boquita es la flor de azahar y tu cuerpo me huele a canela; con la carita recién perfumá y los canastos en las caeras.\n\nTengo rosas, señorito, llevo claveles, también jacintos. Este ramito, compremé usté.\n\n¡Vendo flores, vendo flores! Vas por las calles, de mi Madrid, y vas repartiendo amores (pa) tos flamencos, menos pa mí.\n\nVendo flores, vendo flores. Un ramiño... compremé usté, de rosas para la novia, pa la solapa un clavel.",
    "title": "Poemas y coplas",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 419,
    "article_char_count_full": 2253,
    "article_char_count_review": 2253,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-07-17-left-la-poderosa-figura-de",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nC armen Amaya, la más genial bailaora de todos los tiempos, la más personal e inimitable, la de la poderosa figura sobre los escenarios, la inolvidable e irrepetible gitana del Somorrostro, ha sido recordada y homenajeada con ocho días de diferencia en Barcelona y en Cornellá de Llobregat.\n\nEl 23 de abril se descubría en la Ciudad Condal, la placa que da su nombre a una calle de la Villa Olímpica, en lo que antiguamente era el Somorrostro, barrio en el que nació y vivió una parte de su vida, barrio que ya no existe y del que no queda prácticamente ningún vestigio. El 1 de mayo se inauguraba en Cornellá de Llobregat, en el barrio de San Ildefonso, el de mayor tradición flamenca de la ciudad, la plaza que lleva su nombre.\n\nLa dedicatoria de la calle ha sido una iniciativa del propio\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"según\"]\n\na calle de la Villa Olímpica, en lo que antiguamente era el Somorrostro, barrio en el que nació y vivió una parte de su vida, barrio que ya no existe y del que no queda prácticamente ningún vestigio. El 1 de mayo se inauguraba en Cornellá de Llobregat, en el barrio de San Ildefonso, el de mayor tradición flamenca de la ciudad, la plaza que lleva su nombre. La dedicatoria de la calle ha sido una iniciativa del propio Ayuntamiento de Barcelona, y según Paco Narváez, concejal de vía pública, «Inaugurando esta calle, el Ayuntamiento de Barcelona ha querido pagar en parte la deuda que la ciudad tenía contraída con la genial artista barcelonesa. Una vez decidida la ubicación de la calle, se aprobó por unanimidad que fuese Antonio Ruiz quien ayudara al alcalde de Barcelona a descubrir la placa conmemorativa. Antonio aceptó encantado y pudimos contar con él, afortunadamente, para tan entrañable acto». Antonio, emocionado, desde un sobrio escenario adornado con varias de las magníficas fotografías que Colita le hizo a Carmen Amaya, afirmaba que el acto «me ha parecido maravilloso. Y que lo estaba pidiendo a todo el mundo este homenaje a la sin par, única y jamás repetible Carmen Amaya. Genios como ella no se dan con facilidad, es irrepetible. A pesar de tener muchas imitadoras, siempre se imita a los genios, cayendo a menudo en lo caricaturesco, ella es lo genial porque solamente un cuerpo como el suyo puede aguantar un pantalón puesto que le caiga mejor que a todos los bailarines». La maestra Pilar López, que se había desplazado desde Madrid acompañando a Antonio para asistir al acto, también tomó la palabra: «Tanto Antonio como yo fuimos muy amigos de Carmen. La conocimos muchísimo como persona, y como bailao\n\n[ENDING CONTEXT]\n\nnerviosa, mimbreña y violenta. Carmen Amaya, incluso en su expresión corporal, es un caso aparte.\n\nGozó en vida de la admiración general y entusiasta de cuantos la vieron bailar por medio mundo. Y tras su muerte entró a formar parte de la leyenda, no sólo por los hechos extraordinarios que había protagonizado, sino porque los seres fuera de lo común, como lo era Carmen Amaya, hacen que los hechos, reales o no, sean creíbles».\n\nComo ha dicho Pilar López, «con tales “alegrías” derechita al Cielo, mi inolvidable Carmen”. Y desde allí seguro que bajó para estar con nosotros un ratito en su plaza.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La poderosa figura de Carmen Amaya",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1594,
    "article_char_count_full": 9289,
    "article_char_count_review": 3351,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "según"
      }
    ]
  }
]
```
