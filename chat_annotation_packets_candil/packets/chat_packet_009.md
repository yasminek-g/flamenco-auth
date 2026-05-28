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
    "article_id": "1980-05-6-right-viejas-p-ginas-flamencas",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSin ánimo de inducir a conclusiones de la lectura, quisiéramos señalar algunos juicios aquí contenidos los que, contra el parecer de algunos, no son tan recientes, ni tan nuevos, también, esta visión del arte y, en especial, baile flamenco, tan propios de su época.\n\nPor último \"CANDIL\" solicita de sus lectores el envío de viejos textos, documentos, etc. prácticamente desconocidos de la afición flamenca, a fin de darles la justa y necesaria difusión en estas páginas.\n\nANDIL inicia hoy una sección de viejas páginas flamencas poco conocidas y casi olvidadas. Estas que reproducimos a continuación pertenecen a Ricardo León, escritor malagueño y miembro de la Real Academia de la Lengua, quien, como tantos otros escritores de su época -Jacinto Benavente, Antonio y Manuel Machado, y hasta el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"fieles\"]\n\nsección de viejas páginas flamencas poco conocidas y casi olvidadas. Estas que reproducimos a continuación pertenecen a Ricardo León, escritor malagueño y miembro de la Real Academia de la Lengua, quien, como tantos otros escritores de su época -Jacinto Benavente, Antonio y Manuel Machado, y hasta el propio Eugenio Noel- se sentiría arras-trado apasionadamente por el baile de la Imperio. Por Ricardo León Manes augustos de Ovidio y de Marcial, fieles un tiempo al garbo y a los crótalos de las saltrices de Gades; Príncipe de los ingenios españoles, que en los quiebros y vueltas de las famosas Seguidi- llas viste y alabaste el brincar de las almas, el retozar de la risa y el azogue de todos los sentidos; docto Espinel, que añadiste una voz a la vihuela y un arrogante corcel, de ojos de lumbre, al carro de oro y de cristal de las Musas; Rey poeta, Felipe IV, más propenso a las lecciones de los Almendas y Quintanas que a las lecciones de la Historia; Señor de la Torre de Juan Abad, españolísimo Quevedo, flor de la ciencia erudita y espuma de las sales plebeyas, amigo de jácaras y chaconas, de zarabandas y folías; asistidme vosotros, desde la cumbre del Parnaso, ingenios peregrinos, para que yo acierte a cantar y a describir el rumbo y primor y donosura de los insignes bailes de mi tierra. Y vosotros lectores míos, valores prudentes, damas de calidad, pulcras doncellas, amadores del arte aristocrático, no me toméis a mal que fuer de español y andaluz, entre con mi pluma en el corro de estos donaires, y me siente, a la oriental, sobre la alcatifa de una manta jerezana, al lado de unas mozas de rumbo y de unos mocitos jaquetones, y pierda el seso apenas oiga el tañir de la vihuela y mire los primeros giros y suertes del bolero. ¡Válgame don Ramón de la Cruz, don Francisco de Goya y aún la gentilísima Caramba, reina del bolero, en\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\nméticos; es un pastorcillo de la Campania que vevió el dulce mosto del Falermo insigne y oyó las eglogas de los ruiseños virgilianos. Corriendo los siglos, este rapazuelo inmortal, eternamente joven y alegre, mezcló a sus danzas ritmos nuevos, peregrinas canciones de pueblos bárbaros y niños, y viniendo a tierras españolas, aprendió también coplas de gesta y juglaría, romances heroicos, gacelas orientales, vidtióse de almalafas y caireles, y, al escuchar los gemidos de las razas nómadas y errabundas, sintió un impulso de suavísima tristeza. Por fin, cuando las naves castellanas echaron sus áncoras en las Riberas del Nuevo Mundo, el hispano pastor, oriundo de la vieja Roma, orón sus pasos y canciones con las danzas guerreras del gran Chaco y los mimosos aires de las Antillas. Que es el baile andaluz un hilo de oro del espléndido collar de nuestra historia... Venid, pues conmigo, lectores de mi\n\n[ENDING CONTEXT]\n\nespañoletas que en los alcázares de Madrid y Aranjuez hicieron las delicias de príncipes y reyes.\n\n¡Baile andaluz, de clásico abolengo, flor y nata de las alegrías españolas! Nadie le mire con desdén, ni desprecie a la guitarra por plebeya, que en esta noble rival de la lira y en estas danzas, prez y orgullo del Genil y del Betis, se pintan, como en espejos gloriosos, la historia, las costumbres y el carácter de la raza.\n\nBusquen otros con afán, las exóticas elegancias de la Duncan; yo echaré siempre mi capa española al paso arrogante y gentil de la Musa de mi tierra, de la Pastora Imperio...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Viejas páginas flamencas",
    "periodical": "candil",
    "issue_id": "1980-05",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 1566,
    "article_char_count_full": 9069,
    "article_char_count_review": 4447,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "fieles"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1980-05-8-right-los-cafes-cantantes-de-madrid",
    "article_text_for_review": "En el número siete de «CAN-DIL», Antonio Escribano publicaba la nómina de los cafés cantantes madrileños, en un trabajo que no dudo en calificar de exhaustivo y en el que aparecen, con su concreta ubicación, treinta y cinco de estos establecimientos, sin contar aquellos análogos en los que esporádicamente se ofrecían conciertos de cante. Meritoria recopilación ésta que, entre otras conclusiones, arroja suficientes luces para conocer la difusión flamenca en una época esencial y a la que considero digna de muy serias revisiones.\n\nEn la referida colaboración, un duende de imprenta —pío y devoto en esta ocasión—, dio olor y corona de santidad al café de Telesforo de la calle Arlabán, apareciendo como Café de San Telesforo. Quede, pues, subsanado el error —como solicitara el Sr. Escribano—, ya que nunca el establecimiento estuvo bajo la advocación de tan santo patrono.\n\nPor cierto, de esa larga lista que se aporta de cafés cantantes no recogidos, según su autor en anteriores escritos de flamencólogos, algunos ya eran conocidos por haberse dado noticia de ellos en otras publicaciones literarias, como es el caso del Café del Pez, uno de los visitados en serio por los hermanos Manuel y Antonio Machado en los años finales del pasado siglo, de lo que dejara clara constancia Miguel Pérez Ferrero (1): «También han empezado a ir a los cafés flamencos, entre los que siempre se puede elegir. Allí van en serio, enamorados del cante y del baile, que merecen honores de gran arte popular. Mas su café preferido es el de La Marina, en la calle de la Reina. No se cansan de ensalzar su cuadro flamenco, que actúa y que está formado por la célebre Matilde Prada, bailaora de lo fino; el cantaor Revuelta, las Coquineras, Medina, La Camisona, la Macarrona... A veces cambian y acuden al Café del Pez y al Naranjero, en el que buscan asimismo, el tipismo del lugar, pues se halla en la plaza de la Cebada».\n\nMas si hemos reproducido el párrafo anterior, no sólo ha sido para dejar constancia de noticias precedentes sobre la existencia del café del Pez, ni por señalar, una vez más, un documento connocer cual sea la más certera, ya que, apresurémonos en decirlo, no tengo criterio personal para optar por una u otra.\n\nDos son, en conclusión, las diferencias advertidas. Una, Escribano, como antes lo hicieran Ricardo Molina y Antonio Mairena, así como Manuel Ríos Ruiz, habla del «Café de Naranjeros» —en plural—; mientras Pérez Ferrero lo cita en singular, «al Naranjero».\n\nLa otra consiste en una des-\n\ntundente que prueba las ciertas relaciones de Antonio Machado con el arte flamenco —algo en lo que vengo empeñado desde hace años—; sino fundamentalmente por haber advertido en el texto transcrito de Pérez Ferrero algunas notables diferencias con la interesante y pormenorizada catalogación de Escribano. Diferencias que indico lejano de cualquier polémica y más aún, del innoble deseo de «apostillar»; únicamente por el interés de co-conformidad sobre la ubicación del café de La Marina, sito para nuestro colaborador en la calle Aduana, y para el ya desaparecido escritor y crítico en la madri-leña calle de la Reina.\n\nQueden, por mi parte y como coda final, abiertas las interrogantes en la confianza de que serán borradas por el propio Escribano, o por cualquier otro estudioso o aficionado. Lo que agradeceríamos.",
    "title": "LOS CAFES CANTANTES DE MADRID",
    "periodical": "candil",
    "issue_id": "1980-05",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 552,
    "article_char_count_full": 3325,
    "article_char_count_review": 3325,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-05-9-left-tres-instantes-flamencos",
    "article_text_for_review": "LO QUE ENALTECE A ESTE GITANO\n\n(Antonio Mairena)\n\nNo es el bramido errante de la seguiriya, ni el estallido aéreo de la soleá virreina, lo que enaltece a este gitano mítico y salobre su perdurable voz de madera encarada. Ni es la luz que plisó su irrestañable herida llena de potestades y caliente hermosura lo que responde a que oígamos su relámpago y nos quedemos solos, acompañados, graves. Ni aún su antecedente de fragua y de martillo donde domó el metal con voluntad diamante, sino el ciclón sonoro de su sangre y su alma repitiendo a la vida que descartó la muerte.\n\nRAFAGA INMORTAL\n\n(Manuel Soto)\n\nComo el mar que se funde todo en su última ola que es la que llega hasta la playa y rompe entre la arena y movedizo ofrece un abanico a la mirada, Manuel Sordera, porque es Noche Vieja, porque es noche de amor, junta en su casa a sus amigos y se da fecundo en su último cante. En él se explaya como si no tuviera otra ocasión, como si se muriera o se tratara de su día vertebral: como la ola que es todo el mar, su cante es toda su alma. Y de su voz, que es como una tersura que de buena a primera se agrietara, brota el temblor del mundo en sus inicios y es el mar otra vez, allí entre tantas voces añejas de camino y mimbre, Juan Junquera, Juan Hambre, La Serrana, presentes por estirpe en Terremoto, Joselito Sordera y el Juanata. Quien no entiende que el mundo es un gitano, dos gitanos, tres gitanos que cantan en un momento sólo.\n\nla vida se detiene y se hace tanta, tan rica y tan completa que comprendo que la inmortalidad existe a ráfagas.\n\nEL CARIPEN\n\n(A Luis Moreno, Juan Peña y Fernando Terremoto)\n\nEn Caripén estamos todos, sólo los que sabemos ir acompañados. Si falta alguien llegará a la cita cuando el cante incorpore su milagro. Hay como una dulce frente y declina en el humo y se agiganta como un mago. Está Luis de las largas palabras y está Juan, sin palabras, lebrijano. Luis que tiembla dentro de una cueva porque su voz de cinz viene de abajo, de las profundidades de sus dudas, de sus niños temores de ángel malo. Y acurruca la noche un son espeso que flota en todo sin ser iniciado, como un son que tirita y no se esboza, como esperando que se limpie algo. Juan naquera su cante, lo acaricia para Luis, con miedo de mostrarlo, igual que si supiera que hay demonios, que hay espías, que hay gente de otro carro. Y me dicen palabras que yo entiendo desconociendo su significado, palabras que crecieron junto al trigo y su don dan entre remoto y claro. Dulce Fernando de su cante de hocino, corazón gladiador, voz de candado, triste de Santiago sin su risa de viña, sin sus ojos, triste el barrio. ¿Qué es para él el mundo? ¿Dónde habita en él la forma de los otros? ¿Cuándo perdió, para su bien, toda medida de aspirar a lo estéril? ¿Cómo es sabio? ¿Es que acaso ser sabio es otra cosa? El vive de sus centros, crece en su árbol, se acurruca en el bien porque ya sabe que existe el mal y que tiene mal fario. Pobre de mí entre sus cosas, pobre, como si fuera acaso el que ha inventado la pobreza, quien la ha sentido antes que nadie nunca y quien lega el pecado. Y estamos juntos como quien ofrece la vida por saber. Y nos lloramos historias que a Luis lo hicieron fuerte y débil, puro: cordero y leopardo; y que sacaron de su pueblo un día a Juan para en su cante rociarnos con el alivio de su paz de olivo; y Terremoto hicieron a Fernando. Yo que no sé hacer bien más que el silencio, a mí que no se más que ir asediando, me entra en los ojos un temblor de niebla, pero de niebla a quien le nace un rayo de tanto ver la luz. Los miro y sigo. Doy gracias. Sonrío. Y me voy perdonando.",
    "title": "Tres instantes flamencos",
    "periodical": "candil",
    "issue_id": "1980-05",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "9-9",
    "page_number": 9,
    "word_count": 691,
    "article_char_count_full": 3615,
    "article_char_count_review": 3615,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-05-10-right-joselero",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen:\n\n\"JOSELERO\"\n\n-¿Qué supone para ti el cante?\n\n—El cante es cosa muy delicada, no se aprende, no es aprendido, el cante es nacio, y el que nace con el cante tiene que tener algo propio; en cambio, el que se hace aficionao por la garganta u otra cosa, tiene que aprender, lo que le hace carecer de mérito. El mérito radica en nacer con el cante y hacer cosas con él.\n\n—Yo he cantao con Joaquín el de la Paula y Manuel Torre, del que hago una siguiriya; tenía yo medio conocimiento; también en esa edad, teníamos 17 años, empecé a actuar con «El Quino», bailaor, y mi cuñao, Diego el del Gastor; pero cuando Diego se ha hecho un mostruo, una figura y ha creado, ha sido del Movimiento pa cá, hace cuarenta años.\n\nEn mi familia todos han salido artistas. Uno,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\ncon él. —Yo he cantao con Joaquín el de la Paula y Manuel Torre, del que hago una siguiriya; tenía yo medio conocimiento; también en esa edad, teníamos 17 años, empecé a actuar con «El Quino», bailaor, y mi cuñao, Diego el del Gastor; pero cuando Diego se ha hecho un mostruo, una figura y ha creado, ha sido del Movimiento pa cá, hace cuarenta años. En mi familia todos han salido artistas. Uno, «Andorrano», que es completo, canta por derecho pá escuchar, por siguiriyas, soleá, alegrías, bulerías... y baila por bulerías y rumbas, ha estado en Alemania con «La Cingla», una bailaora. Lo que pasa es que es un hombre raro, como Diego el del Gastor, no le gusta estremecerse, pero es un artista. También una hija, «La Niña Amparo de Morón», que actuó en el Guajiro en los años sesenta, y esa no tuvo academia para cantar y bailar por bulerías. —¿Cuándo comenzastes a cantar? Pero antes los artistas pasaban muchas fatigas y había que aguantá. Pero quizás las fatigas le dieran mayor grandeza al cante. Las fatigas hacen que el cante se ponga más vivificao y más bueno. Antes, lo que he dicho, que miraban a un artista como a un cualquiera, como a un perro. ¡Anda, ya están ahí los artistas! Estaban de fiesta comiendo toos y ni les servían, ni que vais a tomá, ni ná. ¿Cuándo van a cantar? ¡Ya cantarán! A última hora se acordaban de ellos. ¡Anda, llévale una botella de vino! ¡Toma estas papillas! Ahora tienen que ir con el coche a por el\n\n[ENDING CONTEXT]\n\n‡!, a fuerza de pie, o lo que fuera, de pierna, pero sin poner posturas femeninas, vamos, que eran más masculinos. El Quino era un fenómeno, tenía unos deos, sin exagerar, así de largos; era un gitano que cuando hacía así con las manos y las abriía y aquellos deos tocaban el palillo, y empezaba a tocar por soleá el palillo al compás de la guitarra de Diego... y haciendo desplantes, pero ¡macho!\n\n—Con esto terminamos. ¿Tú has bailao? Te lo pregunto porque te veo hacer muchas figuras.\n\n—No, pero me gusta el baile. ¡Yo no he bailao en mi vía.\n\n(Y aquí, naturalmente, concluyó la entrevista).\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Joselero",
    "periodical": "candil",
    "issue_id": "1980-05",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 1909,
    "article_char_count_full": 10314,
    "article_char_count_review": 3052,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1980-05-12-right-aunque-no-quepa-en-el-papel-a-pr",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPocos libros, como éste de George Borrow (1), han sido objeto de tantas citas en la bibliografía flamenca y, a su vez, creo, han padecido la desgracia de no ser leídos por buena parte de quienes lo mencionaron en sus trabajos. Apreciación, esta última, a la que es fácil de llegar tras la lectura del texto, y que nos empuja, una vez más, a manifestar que el refrito es plato común en los escritos sobre el cante, sobre esta parcela fundamental de la cultura andaluza en la que se ensancha la iletralidad de muchos de sus exégetas con el atrevimiento de ascua y propia sardina; modo este de escribir a la brasa, que viene churrascando desde hace no pocos años la pulpa real del arte flamenco. Felicitémonos, pues, por la reedición de este libro, publicado por vez primera en 1841, hasta ahora\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"práctica\"]\n\nacercarse con sumo cuidado y no poca desconfianza historiográfica, y al que, a la hora de clasificarlo, habríamos de situar, necesariamente, entre los típicos, y en ocasiones tópicos, de los viajeros románticos. Apresurándonos a anotar que nos encontramos frente a un amenísimo libro de impecable factura literaria, hemos de dejar constancia de que no son muchas las notas que contiene referidas al cante y bailes flamencos; al primero lo despacha, prácticamente, con una rotunda expresión: «tal es su manera de cantar, en el más bárbaro estilo de su pueblo», nos dirá refiriéndose a una gitana de Sevilla (2). Por lo que hace al baile, ofrece un dibujo demasiado pinto-resco, a lo viajero anglosajón, de poca credibilidad y con más pinta de una sesión de brujería que de baile, producto de auténtica elucubración: «da una patada en el suelo, y colocándose las manos en las caderas, se mueve rápidamente de derecha a izquierda, avanzando y\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_04 | trigger=\"mujeres\"]\n\ndistintas medidas, sin que ninguna de ellas entre en el cante por siguiriillas, algo a retener. De tres y cinco versos, respectivamente, Borrow sólo da entrada a una letra. Mas donde el libro ha llamado poderosamente nuestra atención ha sido en uno de sus capítulos finales, y que hasta ahora no he encontrado citado y analizado en la abundosa bibliografía que retoma algunas de las anotaciones (4) «Ahora —dijo Antonio a la más joven de las tres mujeres—, tráeme el pajandi (guitarra), que voy a cantar una gachapla (copla). La muchacha trajo la guitarra, y el gitano, después de templarla con cierto trabajo, rascó vigorosamente las cuerdas y se puso a cantar: —Gitano, ¿por qué vas preso? Caminito de Antequera Preso llevan a un gitano, Antes de perderla el amo. El canto y la música duraron mucho tiempo. Las dos mujeres jóvenes no se cansaban de bailar, mientras la vieja hacía a veces restallar sus dedos o medía el compás golpeando en el suelo con un palo». G. B. «La Bíblia en España»; pág. 123; Edit. Alianza Editorial, Libros de Bolsillo, n.º 254; Madrid, 1970. Traducción de Manuel Azaña. de Don Jorgito el Inglés, no obstante el interés que, a mi juicio, encierra. Lo acotaremos para el lector, a fin de que extraiga sus propias conclusiones, antes de apuntar las nuestras personales: «Los gitanos, abyectos y viles como han sido siempre, han encontrado, no obstante, admiradores en España, individuos a quien agrada su fraseología, pronunciación y modo de vivir, y sobre todo los cantos y bailes de los gitanos. El de\n\n[ENDING CONTEXT]\n\nmás conocidos flamencólogos, según me confesara hace unos días. Terminemos, pues, como iniciáramos la reseña, felicitándonos por la reedición de este libro, que nos ha dado la oportunidad de acercarnos a uno de los primeros testimonios escritos sobre el cante flamenco.\n\nAh, y ya que hablamos de reediciones y de libros no leídos por quienes los citan, vivimos en el desespero y en la esperanza de que alguien lance al mercado editorial el libro de Carlos y Pedro Caba, del que sólo sé que existen dos ejemplares en España y una escasa decena de personas afortunadas en su lectura.\n\nMANUEL URBANO\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel. A propósito de la reedición de Los Zincali",
    "periodical": "candil",
    "issue_id": "1980-05",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "12-14",
    "page_number": 12,
    "word_count": 3175,
    "article_char_count_full": 18532,
    "article_char_count_review": 4157,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "práctica"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mujeres"
      }
    ]
  }
]
```
