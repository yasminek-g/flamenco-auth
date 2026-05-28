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
    "article_id": "1979-11-4-right-el-cante-jondo-en-la-recuperacio",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEL CANTE JONDO EN LA RECUPERACION DE LA CULTURA ANDALUZA\n\ne ha dicho que este pueblo no tiene historia. Andalucía como apéndice de Castilla, como los glúteos de España, como el patio so- leado de Europa donde se hacinan chismes para de vez en cuando hacer reir, hambreada risa, a los otros pue- blos — más serios de España—. Todavía insisten muchos y hasta nos califican de «snobistas» a quienes desde distintos ángulos, hemos aceptado el reto de redescubrir nuestras raíces, retomar el hilo conductor de nuestra historia. Porque es lo cierto, como señalara con toda lucidez Blas Infante, que Andalucía «ya no sabe lo que es...» Todo se reconduce, entonces, a un problema de cultura. Recons- truir la cultura del pueblo más viejo de Occidente. Allí donde se operaron las grandes síntesis doctrinales\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"cuerpo\"]\n\nnificado y carácter existen- cial del cante. «El jornalero, sin embargo, ni ríe cuando ríe, ni llora cuando llora. El hambre lo ha venido a diluir. Sin embargo, no pasa día sin que aún venga a ser o a recordar lo que fue o a contar su historia. Es cuando dice, sin saber lo que dice, sin que nadie entienda lo que dice, pero saliendo de la hondura de su ser, una terrible, una lúgubre melodía que tiembla en sus labios exangües, que contorsiona su cuerpo y que descompone en gesto trágico las líneas de su semblante. Es lo felah-menco. ¡Cante Jondo!, Ya veréis si vive o no Andalucía». (La verdad sobre el Complot de Tablada y el Estado Libre de Andalucía.- Ed. Aljibe) Blas Infante plantea ya en las primeras décadas de este siglo la especifidad de la alienación cultural de nuestro pueblo. Y lo que para el objeto de este trabajo es más interesante, el carácter sintomatológico del flamenco en el proceso de recuperación de nuestra cultura. Porque si el jornalero andaluz ya no sabe lo que es, ya ha perdido la memoria de su pasada grandeza, aún «sin saber lo que dice, sin que nadie entienda lo que dice», con el cante jondo expresa recónditamente lo que fue. Es un lenguaje que habrá que descifrar porque llega envuelto en impurezas folklóricas, en tópicos y lugares comunes que las culturas alienantes han incentivado. Pero el manantial es pristino, fidedigno. El flamenco es una forma de rememorar lo que muchos y durante mucho tiempo han intentado que olvidemos. El cante jondo, en cuanto cultura, se ha visto sometido también al punto de degradación a que ha llegado nuestra historia. Andalucía ha sufrido muchas rupturas, la última de ellas se produce bajo el modo de producc\n\n[ENDING CONTEXT]\n\nInfante.-La verdad sobre el complot de Tablada y el Estado Libre de Andalucía).\n\nPara Andalucía ha comenzado, desde hace varias décadas, un nuevo ciclo, un nuevo proceso de recupera-ción de su cultura y de su identidad. No ha sido coincidencia que este nuevo ciclo se simultanee con el llamado último renacimiento del cante. En nuestras manos está, tremenda responsabilidad, el que esta vez la recuperación de la cultura andaluza no se malogre.\n\nRamón Porras\n\n(Servicio J. ALAMEDA)\n\nBODAS - BANQUETES Especialidad en CARNE A LA BRASA\n\nSU SITIO IDEAL\n\nDr. Juan Nogales, 11\n\nJ A E N\n\nTeléfono 22 99 18\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Cante Jondo en la recuperación de la cultura andaluzia",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 1590,
    "article_char_count_full": 9778,
    "article_char_count_review": 3312,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "cuerpo"
      }
    ]
  },
  {
    "article_id": "1979-11-6-right-la-inmensa-memoria-flamenca-de-f",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAunque no quepa en el papel\n\nPor Manuel Urbano\n\nNo obstante, mi alta estima por su espléndida obra literaria, confieso por adelantado, que la sola presencia física de este libro de Félix Grande me estremeció y eso que ya han pasado por mis ojos —confío que por mis entenderas—, algunos miles de volúmenes de las más dispares materias. Dos tomos con cerca de ochocientas páginas «en letra chiquitilla», monográficamente dedicados al cante, me traían las que creo justas prevenciones, junto a no pocas interrogantes. Para qué tanto papel si el cante se esconde en las últimas habitaciones de la sangre? Estamos ante un barroco intento de dar cobijo (el diccionario dice: hospedaje sin manutención), a la inmensidad inenarrable del largo, dolido y dolorido quejío de ese bajo pueblo andaluz, abajo de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"cercan\"]\n\nigramas, términos, biografías, la lista de todas las actuaciones en los cafés de cante, etc., etc., sin faltar las acostumbradas fotos de refrito del libro de Fernando de Triana? Lo cierto es que adquirió el libro para dejarle en el anaquel de los que esperan su lectura en noches de insomnio. Para cuando tenga tiempo, pensé, caray con Félix, qué mamotreto: setecientos gramos de fino papel impreso. Pero una frase de Caballero Bonald, de quien tan cercano me siento al enjuiciar el cante, escrita en el prólogo y leída al azar, me empujó a no demorar su lectura, mejor, me arrojó a una lectura apasionada: «Félix Grande no ha querido evitar la plausible tentación de que sea su propia memoria quien protagonice de continuo esa otra memoria del flamenco que va más allá de toda posible escritura». Mientras se deshacían a patadas mis infundados prejuicios, se me vino al recuerdo la tan manoseada como insustituible frase de Walt Whitman: «esto no es un libro, quien vuelve sus páginas toca a un hombre». Y es que el libro empieza con el tirón de enganche de un hombre a los presupuestos inalienables del cante, desde una vieja y sobrecojedora madrugada y su recuerdo es permanencia, norma y propuesta: fidelidad. De aquí los propósitos, de aquí el fin conseguido plenamente en la obra: «Un libro sobre esa honda moral de la memoria que se transmite en el flamenco, un libro sobre la sangre múltiple, su pena, su bravura, su delicadeza espantosa, su f\n\n[ENDING CONTEXT]\n\ngitanas».\n\nQue no perdamos la memoria, sólo desde ella podrá reconstruirse la única verdad del cante.\n\nQuede esta apretada reseña desde una de las perspectivas —la más interesante para mí—, que el libro encierra. De los otros muchos libros que contiene, me apropiaré de sus muchos y utilísimos datos. De lo que contiene de subjetivo, de hipótesis de trabajo, las discutiré con los amigos.\n\nFinalmente, si se me permite, recomiendo la lectura de este libro. Sus cerca de ochocientas páginas contienen otros tantos latidos.\n\ncalzados\n\nzādy\n\nartesanía en piel\n\nplaza de la constitución, 9\n\njaén\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La inmensa memoria flamenca de Félix Grande)",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "6-8",
    "page_number": 6,
    "word_count": 1921,
    "article_char_count_full": 11219,
    "article_char_count_review": 3071,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "cercan"
      }
    ]
  },
  {
    "article_id": "1979-11-8-right-cantar-y-pensar",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMesa redonda sobre temas flamencos\n\nMás que por indagar en enigmas y curiosidades flamencas, por el puro deleite de conversar con los amigos. Aunque también por aquello. Lo poco y lo mucho que del flamenco se sabe, lo hemos aprendido, fundamentalmente, por transmisión oral. Fernando Quinones, nuestro invitado de hoy es un conversador nato. Escucharle es como leer un libro en el que se nos sugieren valoraciones nuevas del flamenco, enfoques distintos; y, además, el anecdotario..., tan lleno de chispa y de frescura. Nuestra idea era la de fijar un tema para cada mesa redonda y seguirlo rigurosamente. Pero en el caso de Fernando, cuya conversación está siempre sembrada de sugerentes sorpresas, de puntillas entre el interrogante y el testimonio de archivo, era preferible no poner mojones a la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nque se nos sugieren valoraciones nuevas del flamenco, enfoques distintos; y, además, el anecdotario..., tan lleno de chispa y de frescura. Nuestra idea era la de fijar un tema para cada mesa redonda y seguirlo rigurosamente. Pero en el caso de Fernando, cuya conversación está siempre sembrada de sugerentes sorpresas, de puntillas entre el interrogante y el testimonio de archivo, era preferible no poner mojones a la tertulia. En los locales de la Peña Flamenca de Jaén, a diálogo abierto, sin moderadores ni sistemas, ésta fue la crónica apresurada de nuestra Mesa Redonda: FERNANDO QUÍNONES: Pensaba qué ocurre en 1980 en el flamenco. ¿Cual es la situación de este arte milenario? El libro mío titulado «El Flamenco, vida y muerte», que salió hace diez años y que se va a reeditar ahora, terminaba con un párrafo muy pesimista; escribía que el flamenco corresponde a otra época. El flamenco importante nació y creció cuando este país era otra cosa, es decir, cuando prevalecía la cultura tradicional, la economía agrícola. Entonces merced a unas circunstancias socio-económicas y políticas el flamenco surge de la necesidad, de las cárceles, de la miseria; también el flamenco alegre, vivo, que está ahí, porque Andalucía no sólo es lágrimas, tenemos además, una sonrisa muy verdadera, muy nuestra. Bueno, cuando llegan las computadoras, las chimeneas, los semáforos y sobrevienen los horarios de trabajo, los medios de comunicación, el cine, la televisión. Yo pensaba que el flamenco era una cosa como arqueológica, sen- Fernando Quifones: «Y sin embargo esos que parecieron locos, hoy día están considerados como creadores clásicos sumidos de lleno en la línea tradicional de la historia del cante». tida por el pueblo. La evolució\n\n[ENDING CONTEXT]\n\ndel flamenco.\n\nFERNANDO QUÍNONES: Sí Pepe, pero quizás estés hablando un tanto idealista. Yo lo que hago es tirar «palante» y hacerme cargo de lo que se me viene encima. Lo mismo que el cambio que está experimentando Jaén, que están tirando media ciudad antigua. Esto es un hecho inexorable que ocurre a nivel andaluz. Las supervivencias tienen que encontrar asiento en esa nueva cosa que está ahí. Si el pueblo mantiene la suficiente vitalidad interior y no se extranjeriza, entonces no importa de qué medios se sirva para llevar a cabo el mantenimiento de las costumbres y cultura andaluza.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantar y pensar",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 2849,
    "article_char_count_full": 16687,
    "article_char_count_review": 3357,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "1979-11-11-right-elegia-para-el-descomp-s-y-otros",
    "article_text_for_review": "Elegia para el descompás y otros estremecimientos...\n\nbullicio y vocerio en remolinos acompasan la muerte flamenca.\n\nipedid silencio! que el cante difunto no calla a nadie.\n\ndetrás del público ávido y resignado detrás de luces rufas y fulgurantes detrás de micrófonos baladrones los orfeos mitológicos piden silencio cegadas voces que no disciernen el reflejo quemado del cante escondido en la profunda mirada de ojos tabicados por el polvoriento día desvanecido en el sueño grisáceo de preguntas y ansias arrinconadas adormecido en los labios usados por demasiadas pocas palabras.\n\nalegrias, colores y alcoholes se esfuman en el aire. afición valiente, cansancio viril que por un cante... pero no... se irán... se van las siluetas encorvadas los oídos y corazones flamencos heridos hasta la próxima promesa.\n\ndetrás de oscuros tabiques detrás de añosos troncos vibran a la espera «los sonidos negros».",
    "title": "Elegía para el descompás y otros estremecimientos",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 139,
    "article_char_count_full": 902,
    "article_char_count_review": 902,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1979-11-12-right-unas-viejas-habaneras-sevillanas",
    "article_text_for_review": "La noticia nos viene de un viejo recorte de periódico de los primeros años veinte, publicado en el diario «Andalucía» y rotulado «Observaciones sevillanas: Canciones y Cantares» de Rafael Porlán y Merlo, uno de los poetas fundadores de «Mediodía», de una altura jonda personalísima e inigualable y como apuntara Ricardo Molina (Diario Córdoba, 21 de octubre de 1951), «sentenciosa y breve como una solea de las nuestras y por las que canta:\n\n«Fue el ruido de un portazo: temblaron todos los quicios, crujieron todos los arcos».\n\nY si nos acompañamos del testimonio del flamencólogo cordobés, no es por otra razón distinta a la de prevenir alguna reacción que acuse de antiflamenquismo a nuestro autor, quien, continuando con el juicio de R. Molina «había bebido el filtro mágico del cante jondo».\n\nTranscribimos, sin más, el artículo de Por- lán, que nos trae, al menos para mí, una no- ticia de cante desconocida.\n\n«Del rincón más lleno de humo de las latas (1), sale a veces una canción de borrachos altamente sugestiva. No es el flamenco inherente y fatal que desgarra la boca con alaridos formidables, sino una canción, una verdadera canción de compás lánguido y sentimental muy para poner los ojos como de cordero, húmedos y mansos. El grupo de andrajosos que la entona lo hace de un modo que hay que llamar con amore, en italiano y todo para que resulte más mimoso y paternal.\n\nA pesar de los vasos que hay en la mesa, imposible pensar en la borrachera estrepitosa y estúpidamente jovial que necesita el cante hondo como vestíbulo de un sueño pesado. Esta borrachera es honrada y tranquila y va a cargo de viejos maestros de un oficio típico y antiguo que ya desapareció, suplantado por alguna maquinaria, como desapareció aquella afición de su mocedad de salir en comparsas el Carnaval, que es de donde les vienen esas canciones en las que no hay heridas, ni mal-diciones, ni te están criando pa mí, sino melodías amorosas, acaramelados trinos, suspiros de cuerpos que se apoyan en balaustradas de parques, cabrilleos de luna en las huellas de la barca, bóvedas de verdura, quejas de surtidor... Una romántica ternura anima esas barcarolas, esas romanzas, aunque genuinamente tabernarias, aunque extrañas en las bocas desdentadas y malolientes con un ritmo de habanera. Esas promesas de amor, esas protestas de eternidad no se hacen por ninguna saluita ni por la gloria de nadie —los rehenes briosos y rotundos—; se basan en la sublime garantía de la voz que tiembla al jurar.\n\n(1) Lata.—Taberna de vino de la hoja.\n\nEn una grata mezcla de voces altas y bajas, como cuando se reunían en grupos vocales rudimentariamente distribuidos en torno de la bandera de su comparsa, resucita el grupo de viejos borrachos las canciones de entonces.\n\nInteresan esos cantores, viejos andaluces de los pocos que quedan, ocurrentes y socarrones, que viven su vejez ociosa y haragana a costo de las sobonerías con gracia que saben emplear para meter la cabeza en casa de alguna entená (cuando ellos mueran se llevarán también la palabra entená con otras muchas cosas). Y sus canciones de cadencia vieja, evocan cantos de boesiak de Gorki, o de guen de Richepun. Dolientes, lánguidas, apasionadas, perfumadas de juventud antigua; son más del alma del pueblo, ahora que lo flamenco ha de-generado en marchoso».\n\n¿Qué grupo humano andalucísimo sería éste? ¿Cual el oficio de estos andrajosos cantores ya desaparecidos, como tantas otras cosas, para siempre? ¿No estamos ante el mismo grupo humano en el que uno de los alter ego de Antonio Machado quisiera experimentar su máquina de trovar?\n\nEl excesivo y brillante lenguaje literario de esta estampa sevillana del primer cuarto de nuestro siglo nos quita la fijeza de la noticia; pero, por el contrario, nos la trae repleta de expresividad y hondura. No sé por qué esta canción me suena a agua, me trae rumores de río, de guadalquivires.\n\nSugerencias aparte, quede la noticia de un andalucísimo cante coral, romántico, específicamente tabernario y con ritmo de habaneras. Un cante ya perdido; también y como profetizara Porlán, esos viejos cantores se llevaron la palabra entená —no la he encontrado recogida por D. Antonio Alcalá Wenceslá en su «Vocabulario andaluz»—, que se me viene traducida con alguna picaresca acepción.\n\nManuel Urbano",
    "title": "Unas viejas habaneras sevillanas",
    "periodical": "candil",
    "issue_id": "1979-11",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 708,
    "article_char_count_full": 4279,
    "article_char_count_review": 4279,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
