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
    "article_id": "1996-05-17-left-manuel-balmaseda-gonz-lez",
    "article_text_for_review": "Este andaluz, nacido aproximadamente a mitad del siglo XIX y muerto en Málaga sobre el año 1881, está hoy totalmente olvidado, posiblemente incluso por sus propios conciudadanos de Ecija; las noticias que damos de él están tomadas de los artículos de “El Folklore Andaluz”.\n\nConcretamente en el órgano de la Sociedad “El Folklore Andaluz” (dirigido por D. Antonio Machado Alvarez Demófilo¹), se pública una carta de D. Luis Montoto y Rautens-traus que, entre otras cosas, dice:\n\n“A un tiempo mismo recibí tu “Colección de Cantes Flamencos” y un librito titulado “Primer cancionero de Coplas Flamencas” $ ^{2} $ cuyo autor es un trabajador en las líneas férreas, limpiador de los coches de los trenes de viajeros.\n\nDesprovisto de toda educación literaria, siente hondo y tiene de poeta más que muchos de los que escriben versos muy pulidos y aderezados. Manuel Balmaseda, que así es nombrado, canta porque sí, por la misma razón que canta el pájaro, porque Dios ha querido que cante.\n\nY si es o no poeta, tú lo dirás después de haber leído sus coplas.\n\nCanta nuestro pobre trabajador: I. Si el “queré” era bueno o malo a un sabio le pregunté. y el sabio no había “querío” y no supo “respondé”. 1) Corresponde a los años 1882-83, editado por el Excmo. Ayuntamiento de Sevilla, en conmemoración del Centenario, Colección Alatar (1981), páginas 89 a la 92.\n\n2) Fue publicado este librito por la librería e imprenta de Hidalgo y Compañía de la calle Génova, al precio de 1 peseta.\n\nII. Todos los sabios del mundo. Vienen a “aprendé” de mí. Y aprovechan la ocasión cuando me sienten dormir”.\n\nPor este estilo continúa la carta, transcribiendo también algunas de sus coplas que poseen un gran sentimiento y profundidad como esta que pongo por ejemplo:\n\nLo mismo que aquel perro que anda siempre por las calles. Buscando “güesos” que tiran has de “andá” tú por buscarme.\n\nPero lo que de verdad me impresionó fue el grupo de “seguiriya”, de las que Montoto dice: “Muy sentidas son sus seguidillas gitanas; de ellas puede decirse, repitiendo palabras tuyas, que son “delicados poemas de dolor”. Entre éstas, quiero destacar las siguientes: Límpiate los ojos que “llorá” no vale que la manchita, que a ti te ha “caído” se lava con sangre.\n\nPor aquí pasó \"pa'agranda\" mis males el mismo carrito, yo lo conocí que llevó a mi \"mare\".\n\nLa vi “enterraíta” con la mano fuera, que como era tan desgraciaíta le “fartó” la tierra.\n\n¿Puede pintarse a una mujer tan desgraciada con colores más vivos, que los que el poeta emplea en la última seguiriya?\".\n\nDice de esta “seguiriya” Monto: “Un Bécquer encontraría en esa seguidilla motivo para preciosa leyenda; y no digo nada si la relacio-3) Esta copla se la escuché por los años 39-40 en el estilo de Bambera a la Niña de la Alfalfa.\n\n4) Sr. Pitre. Folklorista italiano, vinculado como socio de honor del Folklore Andaluz. nara con aquella otra copla, de to- dos conocida, que dice así:\n\nEn el carro de los muertos ayer pasó por aquí, llevaba la mano fuera por eso la conocí. $ ^{3} $\n\nApuesto todas mis coplas a que la mano que se quedó sin tierra fue la misma que salía del carro de los muertos, como para dar al mundo el adiós postrero.”\n\nCuando Montoto escribió esta carta, ya había muerto Balmaseda, a la manera, como dice el propio D. Luis, “como mueren los hijos del trabajo; sumido en la miseria; dejando en el mayor de los desamparos a una viuda modelo de madres, y a una niña que apenas balbucea el nombre de su padre desventurado”.\n\nManuel Balmaseda se quedó en paro en Sevilla, y poco antes de su muerte partió con destino a Málaga en busca de trabajo.\n\nAllí, el autor del \"Primer cancionero de coplas flamencas\" murió de hambre, enfermedad muy común de los pobres de entonces y de todos los tiempos.\n\nTermina la carta Montoto diciendo “que la edición del “Primer cancionero de coplas flamencas” es la herencia que Balmaseda ha dejado. Procuremos que no sea ilusoria.\n\nRecomienda, recomienda el cancionero como yo lo haré; porque bien vale cuatro reales y porque practicaremos así una verdadera obra de caridad\".\n\nContesta Demófilo acusando el recibo de esta carta diciendo:\n\n“Querido Luis: La mejor recomendación que pudiera hacer del “Primer cancionero de coplas flamencas”, del honrado jornalero y malogrado poeta M. Balmaseda, cuya obra ha sido dada a conocer en toda Europa por nuestro ilustre consocio honorario Sr. Pitre $ ^{4} $, es insertar tu carta, que tan perfectamente retrata los nobles y delicados sentimientos de tu corazón...; de este modo las luchas interiores, las horas de mortales angustias y las aspiraciones del noble hijo del pueblo, muy por encima del círculo de hierro en que estuvo aprisionado y sus horas de mortales angustias que le inspiraron esta copla:\n\nMi pecho lo están partiendo yo no lo puedo aguantá son muchos los asesinos ¡y grandes golpes le dan!\n\nhabría logrado establecer un vínculo constante entre él y los señores de su familia que le han sobrevivido.\n\nEl trabajo para fomentar esa herencia, sería a mi entender, la mejor oración que la esposa podría elevar a la memoria del honrado poeta y el mejor tributo que pudiéramos rendirle los que somos como él obreros de la inteligencia. Demófilo.”",
    "title": "Manuel Balmaseda González",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 888,
    "article_char_count_full": 5177,
    "article_char_count_review": 5177,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-05-22-right-homenaje-a-fausto-olivares",
    "article_text_for_review": "Manuel Urbano Salgan por esas ventanas los trallazos de las siguiriyas\n\nFernando Quinones Que ya no estaba pero está\n\nAntonio Povedano Fausto Olivares\n\nVenancio Blanco ;Hombres que cantan!\n\nMiguel Viribay Abad Acotaciones al mundo de Fausto Olivares\n\nPedro A. Galera Andreu El arte de Fausto Olivares\n\nRamón Porras González Poema a Fausto Olivares\n\nSérvula Palacios Poema\n\nJosé Sánchez del Moral Fausto, eterno candil\n\nLuis Quesada Fausto Olivares en las raíces del Flamenco\n\nDaniel Grandidier Fausto y su búsqueda pictórica\n\nRoger Decaux Fausto en su duración\n\nRamón Porras González Los colores jondos de Fausto Olivares\n\nJosé Luis Buendía López Descrédito de los cánones\n\nAgustín Gómez Fausto Olivares en las cuevas celestes donde los cantes se hacen ecos sin fin\n\nJosé Fernández García Fausto Olivares, la persona\n\nPepe Vica El mundo en sus ojos (El Fausto que yo conocí)\n\nJuan Antonio Ibáñez Sombras tiene la llama, de un fuego que permanece\n\nFrançaise Gérardin 25 de Julio de 1995",
    "title": "Homenaje a Fausto Olivares",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 156,
    "article_char_count_full": 985,
    "article_char_count_review": 985,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-05-23-right-salgan-por-esas-ventanas-los-tra",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Urbano\n\nTras una larga y densa noche de fiesta, Fausto se encierra junto con un amigo, también pintor, en su estudio de artista. El magnetofón, siempre a mano, cargado de cintas con antiguas grabaciones recogidas in situ a los más puros cantaores, desgrana su queja sucesiva por la cerrada estancia. Se acumulan las horas una a una, como una a una, en un rosario interminable de quejas y lamentos, sin reposo alguno van arañándose soleares y siguiriyes. A la noche cerrada le sigue el alba y, luego, los primeros rayos de sol de la mañana; pero el magnetofón gira incansable con los más serios y ortodoxos cantes, no hay descanso para lo que, a la postre, no es más que la belleza de la pena sucesiva.\n\nAmañanó el día. La ciudad, ocho pisos abajo, debía estar hormigueando en su quehacer\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"compás\"]\n\ny siguiriyes. A la noche cerrada le sigue el alba y, luego, los primeros rayos de sol de la mañana; pero el magnetofón gira incansable con los más serios y ortodoxos cantes, no hay descanso para lo que, a la postre, no es más que la belleza de la pena sucesiva. Amañanó el día. La ciudad, ocho pisos abajo, debía estar hormigueando en su quehacer cotidiano. Fausto, tercio a tercio, asoma su emoción por el brillo de los ojos; se estremece a cada compás del grito. El tiempo, para él, quedó detenido y en el ángulo más lejano y obscuro del techo cuando brotó del magnetófono con todo su desgarro vital la primera “salía” de siguiriγas. El amigo de Fausto no pudo más. Toda la noche silente y solita- A compás con la pintura jonda de Fausto Olivares rio, aguantando el ahogo de un mundo antiguo que le mordía los sentidos hasta el desespero: −\";Fausto, abre ya la ventana, que no puedo más, que quiero oír el ruido de los camiones!\". Al margen de la anécdota, que nos abre la sonrisa, es preciso recoger que la noche siempre fue de cálido abrigo para Fausto Olivares, aunque la partiese el frío cuchillo del cante, siempre para él manantial irresistible de todo arte. Fue Olivares un caballero andaluz total y un giennense que no ocultaba su identidad, la que nos marca el \"ronquío\". De Jaén, como el mítico lagarto; de un pueblo con el que siempre era y del que respiraba sin esfuerzo cuanto de jondo y de hondo tenía su cultura. Lo esencial andaluz fue para él masa viva, forma definitiva, no un añadido. Y si el hombre tenía los pies anclados en el Sur, como artista se elevaba desde él en tronco firme hasta derramarse en florida rama de frutos de sabor universal. Desde un rincón del barrio de San Ildefonso hasta el fin del mundo llevando en su paleta un tétrico golpe de bordón convertido en pinturapintura; el erótico quiebro de tarjeta postal de una bailaora, pon- Fausto Olivares «Tiene unos dientes...» Cera al oleo, 32×22, 1983 gamos por ejemplo, supo trans\n\n[ENDING CONTEXT]\n\nde \"Tres pintores jondos giennenses\". Puestos a redactar definiciones para concluir, bien pudiera decirse que en la pintura de Fausto Olivares anida toda la fuerza vital del cante, cuando ya el cante se ha ido. Son el poso de un pueblo, cuanto en el momento más solemne del rito de la sangre pronunciaron las palabras que ya fueron oídas una y mil veces, huellas de vida desgarrada por la queja más antigua e implacable; por ello yo también necesito abrir la ventana a la mañana para que por ella salgan todas las amargas confesiones que se suman en una noche repleta de soleares o siguiriyas.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Salgan por esas ventanas los trallazos de las siguiriyas",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "23-25",
    "page_number": 23,
    "word_count": 1823,
    "article_char_count_full": 10778,
    "article_char_count_review": 3585,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "compás"
      }
    ]
  },
  {
    "article_id": "1996-05-25-right-que-ya-no-estaba-pero-est",
    "article_text_for_review": "Fernando Quinones\n\nEn pleno centro de Jaén, un establecimiento cuelga en la calle a diario y junto a su puerta una cabeza de caballo. —La recuerdo de cuando era así de chico. Y es uno de los recuerdos más fuertes que tengo de mi infancia. Eso me dijo Fausto Olivares hace diez o quince años, y no sé por qué se me quedó, como tampoco sé por qué es lo primero que se me vino a las mientes cuando, la noche de este último 27 de mayo, me espetó Manuel Urbano en su casa que Fausto ya no estaba\n\nque no, que ya no estaba en Francia ni en su calle Millán de Priego, ni tras de sus astutas gafas de ejecutivo, ni viendo nunca nunca esa cabeza de caballo, ni en pie como un torero delante de sus telas, ni escuchando mejor que nadie unas cantiñas, ni diciéndole a Fafa sentados a la mesa: Ponle a Nadia y Fernando un poco más. Que ya no estaba con nosotros, que ya no estaba ni consigo aunque sí duradero en su Candil y en su pintura encandilada última, de masas cálidas, rosáceas, de vísceras eróticas bestiales, deleitables, llamando ciegas como una siguiriya ciega, el puro, oscuro amor biológico, imaginado y visto por las manos mayores no del mayor sino del solo pintor de un Eros anatómico, de la embriagadora, la embriagada, la monstruosa carne que nos rige, destazada en sus cuadros por Fausto Olivares Palacios y expuesta sobre trémulos fondos, paisajes del deseo que la nutre y enciende como a la voz una guitarra en las noches del alma y las del cuerpo.",
    "title": "Que ya no estaba pero está",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 272,
    "article_char_count_full": 1457,
    "article_char_count_review": 1457,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-05-26-right-fausto-olivares",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nConoci a Fausto hace bastantes años con ocasión de un viaje a Segura de la Sierra, acompañando a mi buen amigo Rufino Martos. El pintor y amigo, Paco Cerezo, nos había invitado a pasar unos días en la casa que tiene en aquella atalaya de La Sierra de Segura.\n\nFue una presentación breve, de obligada cortesía. Después, mi desmemoria, no me ha dejado claridad de fechas, ocasión, ni sitio, de nuestro primer encuentro: sólo recuerdo que la motivación fue el flamenco; su gran afición coincidía en apasionamiento analítico con la mía. Tras aquella primera toma de contacto vinieron frecuentes entrevistas en las que, a nuestra manera, “arreglábamos” el mundillo flamenco. Cuando nos reuníamos oíamos mucho flamenco, poníamos nuestros reparos a las deficiencias, “condenábamos” los cruces indebidos,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficiones\"]\n\ndeficiencias, “condenábamos” los cruces indebidos, los desafinamientos, etc. Naturalmente, teníamos nuestras pre-ferencias en las que solíamos coincidir: los dos estábamos influidos por la parte caliente, por el desgarro, por lo hondamente expresado. Curiosamente, siempre que nos veíamos, nuestra conversación giraba alrededor del flamenco; sólo en muy raras ocasiones lo hacíamos sobre pintura, cosa natural, pues normalmente, se comparten las aficiones, pero no las convicciones vocacionales; un respeto a la intimidad, al que —entre amigos— rara vez se falta. Claro que, por supuesto, hablábamos de otras muchas materias. Fausto y yo teníamos muchas cosas que nos preocupaban de manera semejante. Pictóricamente hablando Fausto tenía ideas muy claras y convicciones muy profundas. Así, con el mismo interés y preocupación, veíamos ambos las situación decadente en la que se encontraba el tema flamenco en la pintura, en la década de los setenta. Eran los años del resurgimiento, de atención especial al cante; un momento realmente crucial para el flamenco, en el que, a excepción de las artes plásticas, todo lo demás, vivió sus mejores momentos de recuperación y asentamiento. La pintura sobre flamenco estaba en manos de pseudoartistas, desprestigiada y vulgarizada hasta el descrédito. Una situación que, naturalmente, provocó entre nosotros —pintores y aficionados—, un auténtico malestar: nos preocupaba seriamente esta desatención a una faceta tan importante en lo cultural, plásticamente hablando. Así, en nuestro interés por el \"saneamiento\" de este aspecto incrustado en nues\n\n[ENDING CONTEXT]\n\npintura vivimos siempre en perfecto acuerdo; teníamos afinidades comunes, admirábamos las mismas voces y nos acompañaban los mismos fantasmas.\n\nHa sido realmente duro que, cuando las puertas del éxito se le habían abierto, cuando la perspectiva que se le ofrecía podía colmar sus aspiraciones, haya sido el momento elegido por el destino para, de manera impecable, señalarle el final de trayecto.\n\nCon su muerte, Jaén ha perdido un pintor en el más amplio sentido de la palabra; el flamenco ha perdido un defensor de sus valores fundamentales, y los amigos hemos sufrido una pérdida insustituible.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Fausto Olivares",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "26-28",
    "page_number": 26,
    "word_count": 1434,
    "article_char_count_full": 9017,
    "article_char_count_review": 3215,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficiones"
      }
    ]
  }
]
```
