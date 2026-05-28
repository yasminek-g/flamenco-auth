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
    "article_id": "1989-05-22-left-podio-y-picota-ventolera",
    "article_text_for_review": "A todo lo alto, para recibir plácemes de la afición flamenca, una casa de discos andaluzas que prepara la edición de DOS COMPACTOS FLAMENCOS. Que nosotros sepamos, el flamenco en esta novísima técnica de reproducción musical solamente había sido afrontado, en una hermosa selección, por una multinacional instalada extramuros de Andalucía. El ejemplo de Philips (*) ha sido continuado ahora por Pasarela. Queden aquí consignados estos dos nombres como pioneros (acaso exista alguno más que nuestra ignorancia impida lamentablemente traer aquí: perdón si así fuera) del interés y la decisión realizada de ir acomodando al flamenco en un medio de difusión que hasta ahora le parecía vedado. Romper de este modo la indiferencia y la modesta acogida que nuestro arte merece en los amplios sectores melómanos, constituye un acto ejemplar y laudatorio.\n\n(*) PHILIPS, 1987. Polygran. El Cante Flamenco. Antología histórica. Un sitio de honor para la Fundación Municipal de Cultura del Ayuntamiento de Cádiz. Ahí es nada: la repetición del programa iniciado en 1988 regido por el hermoso título El Cante en los Cafés que este año tiene su segunda edición con una serie de actos en el Café del Tinte, sito en el viejo Callejón del Tinte de la tierra madre de la gracia y el ángel. Y ya mismo el ciclo Cante y Baile en los Patios con un programa de seis fiestas con viejos y nuevos nombres de la tierra, algunos tan notables como Adela la Chaqueta, Curro La Gamba, Alfonso de Gaspar, Chano Lobato, Moraíto Chico, Mariana Cornejo, Gineto, Juanito Villar, Niño Jero..., y así hasta un total de más de cuarenta artistas (populares y familiares, ojo al parche) repartidos por toda la geografía urbana y vecinal garantana. ¡Que cunda el ejemplo y que vayan copiando los renovadores del flamenco tradicional! A una peña flamenca sevillana que goza de nuestra mayor estima por la celebración de un concurso de cante para jóvenes menores de 17 años en el que —según nos cuentan— ha intevenido algún niño-niño. De la misma falta acusamos también a la Federación Provincial que ya va por su cuarta edición de un concurso flamenco para menores de 17 años. Aunque el pecado no es excesivamente grave (por eso silenciamos nombres), pensamos que con menos de diecisiete años (y no digamos con menos de diez) el ser humano permanece aún dentro de su ciclo de crecimiento y éste afecta en muchas ocasiones a buena parte de los órganos y las capacidades físicas, inclusive la de la voz. Muchos posibles y hasta probables buenos cantaores se han malogrado por su prematura dedicación. El cante requiere exigencias distintas al cultivo del baile o de la guitarra. Un cantaor difícilmente adquiere su capacidad antes de los cuarenta años y su granazón antes de los cincuenta.",
    "title": "Podio y Picota/Ventolera",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 459,
    "article_char_count_full": 2745,
    "article_char_count_review": 2745,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-22-right-discografia-flamenca",
    "article_text_for_review": "Francisco de la Brecha Discografía Flamenca\n\n«Suenan las campanas», de Pansequito Guitarra: Tomatito L.D. de Fonográfica del Sur, Utrera\n\nbatalla por las nuevas formas del flamenco, ese gran sofismay que predica la necesidad de atemperar —nunca mejor dicho— el cante para salvarlo de este incurable mal que le contaminan los endemoniados puristas, hablar del acierto que tuvo aquel jurado cordobés que otorgó el premio a la creatividad parecería en uno mismo algo así como una imperdonable apostasía. Y no es así, ciertamente. Allí y entonces, Córdoba supo estar a su altura. Un joven cantaor nacido en ese Sur de sures que es el Campo de Gibraltar —en La Línea de la Concepción— José Cortés Jiménez, Pansequito, demostró palmariamente hasta dónde es lícito recrear un cante hones-ta y dignamente. Las formas, ni viejas ni nuevas, simplemente intactas, porque si cambian, el cante se deforma. El compás —habrá que repetirlo más veces?—, el mismo, porque se trata de una medida y, por tanto, no existe ni compás grande ni chico; ni corto ni largo; ni alto ni bajo; ni pesado ni ligero. Mera y propiamente como es.\n\n¿Y en qué consiste entonces la recreación?, Se podrá preguntar. Pues escuche usted el disco. Escúchelo y verá cómo se alarga en un tercio, cómo se ligan unos\n\nmelismas, cómo se embebe una exquisita voz afillá en esa difícil cuadratura de una soleá-soleá o de una soleá-cantiñera; oiga cómo se producen fracciones de tonos incontables... y luego escuche ese matute de las nuevas formas, compare... y juzgue. Y si prefiere usted las nuevas formas, con su pan se lo coma, y yo que lo vea. (Que no lo veré).\n\nEl disco\n\nMuy hermosos los fandangos, que cuando se cantan bien hasta los fandangos gustan. Muy flamencos los tangos en los que se pueden disculpar las voces que hacen coro ocasional al cantaor con mucha más fortuna que en las bulerías que dan título a la grabación.\n\nLa recreación en las alegrías (que con gran donosura tiran lo suyo para San-lúcar) son de una admirable perfección dentro de esa línea que caracteriza a Pan-sequito. El sonido en los tanguillos no pa-rece perfecto, puede que sea del ejemplar que nosotros hemos adquirido. Todo el disco se constituye en una magnífica lec-ción de compás, salvado sea el fandango. Armonioso compás que subraya la mano maestra del almeriense en un amadrinamiento admirable de la guitarra con el cante.\n\nC/. Mesones, 18\n\nTeléfono 26 35 46\n\nC/. Doctor Arroyo, 12\n\nTeléfono 210058\n\nJAEN\n\nManuel Yerga: Discografia flamenca (placas)",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "22-26",
    "page_number": 22,
    "word_count": 418,
    "article_char_count_full": 2495,
    "article_char_count_review": 2495,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-23-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "«Mi patria es la lengua francesa», afirmaba Marcel Camus en un momento en el que dicha lengua se veía amenazada por extrañas incursiones foráneas y degradada por el mal uso, a la vez que el gran novelista y dramaturgo apostaba con firmeza por el único vehículo al que permanecemos ligados toda nuestra vida, el que nos acompaña hasta la muerte y en el cual vertemos el gran caudal de nuestros afectos, nuestros miedos e inseguridades, en el que toman forma, una a una, como un rosario inexcusable, las palabras de amor que definen nuestra pertenencia al grupo de los privilegiados que se sienten acariciados por la suave brisa de un verbo amigo.\n\nSi tanto respeto por la lengua manifestaba el intelectual francés, pletórico de éxitos y reconocimientos públicos, yo quiero figurarme lo que debe de significar esa enorme posibilidad expresiva para el desposeído, para el marginado, para aquél que debe de pedir socorro, proteger a un hermano, o inventar un mundo en medio de otro que le es hostil y que se expresa tan solo con el lenguaje brutal de los vencedores. Estoy hablando del Caló, la lengua de los gitanos, ese hermoso cofre de signos, de grafemas misteriosos y sonoras articulaciones que ellos trajeron desde tiempos inmemoriales a tantas partes del mundo. Aunque no quepa en el papel... José Luis Buendía López\n\n«Diccionario Gitano-Español y Español-Gitano\n\nTineo Rebolledo\n\nEdición fascimilar de la de 1909. Servicio de Publicaciones de la Universidad de Cádiz, 1988. Ellos, que tan ligeros de equipaje viajaron siempre, no pudieron prescindir del ropaje fecundo que los configuraba como seres inteligentes, que los hermanaba ante la adversidad pero que también los apretaba con más fuerza en el fondo de la mísera carreta y los calentaba más que el fuego de la hoguera que los congregaba.\n\nTambién se lo quisieron quitar. Al poder constituido le parecería excesivo que los don nadie tuvieran también un vehículo propio en el que remansar sus afectos o gritar a los cuatro vientos la iniquidad que se adentraba por sus vidas. Ya lo habían hecho antes con otras razas pleótricas de vida y sensibilidad: minorías judías, moriscas o mozárabes habían sido planchadas por el rodillo de los vencedores, sus lenguas sojuzgadas; pero, mire usted por donde, la fuerza de lo verdadero perdura por entre los resquicios de los instrumentos de tortura que maneja el inquisidor: las jarchas mozárabes alumbraron para siempre el despertar de la lírica europea.\n\nSí, quisieron callar para siempre a los gitanos, la cantinela: «Que no hablen su lengua» se repite machaconamente a lo largo de los siglos en boca de todos los intolerantes, y desgraciadamente casi lo han conseguido. El Caló es hoy una reserva, casi un lecho ecológico a proteger, y eso\n\nes lo que hacen autores modernos como Pablo Moreno Castro y Juan Carrillo Reyes, autores de un Diccionario Gitano del que ya nos hemos ocupado en otro lugar, o lo que hacía Tineo Rebolledo con una honradez a prueba de bomba con este diccionario de 1909, del que hoy, ochenta años después, la Universidad de Cádiz nos ofrece una esmerada reedición facsimilar que, no dudamos, ha llegado en el momento justo, ya que el gran vacío sobre este tipo de estudios lingüísticos hace necesario el acopio de todo lo preexistente.\n\nEl autor, que lo es así mismo de una excelente «Historia y costumbres de los gitanos», publicada en 1914, comienza por ofrecernos el doble diccionario gitanoespañol y español-gitano, para después mostrarnos la verdadera conjunción de los verbos en caló, mal asimilada, en forma de pastiche, a la castellana a causa de la sedentarización progresiva del pueblo gitano, y con la que no tiene nada que ver, con lo cual la obra gana en interés, no sólo lexicográfico sino también morfosintáctico. Por último Rebolledo, en la parte final del libro, nos da una preciosa muestra, que podemos calificar de costumbris\n\nta, de historias y cuentos gitanos, narrados por ellos mismos y que rezuman frescura y espontaneidad por todas partes, constituyendo a la vez una especie de tratado sociológico que muestra las mañas, los derroches de talento e inteligencia que este pueblo tuvo que asumir frente a la intolerancia de las gentes que no admitían su talante.\n\nLibro, pues, precioso y necesario, y no sólo para los estudiosos que nos acercamos con respeto a este tipo de investigaciones, sino sobre todo para el mismo pueblo gitano que, ahora, cuando tanto se habla de cultura para todos y de normalización lingüística, tiene la obligación de recuperar su lengua, de transmitírsela a sus hijos y, en fin, de hacer posible sin ningún tipo de espasmos viscerales ni acres enfrentamientos la convivencia entre gentes que hablen de distinta manera, pero que sientan la condición de personas no discriminadas por encima de cualquier otro planteamiento. Que aprendan, que aprendamos todos, a amar la lengua en la que un día ya remoto, resonaron por el mundo tantas voces hermosas.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al Mérito del Trabajo)\n\nRecepción diaria de MARISCOS Y PESCADOS ESPECIALIDAD EN ASADOS\n\nROLDAN Y MARIN, 7\n\nJ A E N\n\nTELEFONO 22 97 65",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 846,
    "article_char_count_full": 5108,
    "article_char_count_review": 5108,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-24-right-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEl Ayuntamiento de Jerez organizará a través de su Delegación de Educación y Cultura y en colaboración con la Confederación Andaluzas de Peñas Flamencas el XVII CONGRESO NACIONAL DE ACTIVIDADES FLAMENCAS, dedicado a Silverio Franconetti.\n\nEl Alcalde de la ciudad solicitó en 1987 en Benalmádena, ser sede del 17 Congreso Nacional, candidatura que fue ratificada posteriormente en el Congreso de Córdoba el pasado mes de octubre, por la importancia que la ciudad ha tenido y tiene en el origen y desarrollo del flamenco, lo que se aceptó por unanimidad por los Congresistas.\n\nLos dos últimos Congresos se celebraron en Benalmádena (1987) y Córdoba (1988), y alcanzaron un alto nivel de organización y apoyo, asistiendo entre Congresistas y acompañantes alrededor de 400 personas, lo que supone un\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"según\"]\n\noctubre, por la importancia que la ciudad ha tenido y tiene en el origen y desarrollo del flamenco, lo que se aceptó por unanimidad por los Congresistas. Los dos últimos Congresos se celebraron en Benalmádena (1987) y Córdoba (1988), y alcanzaron un alto nivel de organización y apoyo, asistiendo entre Congresistas y acompañantes alrededor de 400 personas, lo que supone un gran esfuerzo de organización y promoción de la ciudad, aspecto éste que, según los organizadores de ambos congresos, tuvo una gran proyección, tanto en ámbito local como nacional. Todo ello lleva consigo fijar unos objetivos que, sin descuidar la atención a los visitantes (congresistas y acompañantes) y dando la importancia que tiene a unos días de encuentro en conviviencia anual de la gente del flamenco, supongan un avance en el estudio de los problemas que afectan al fenómeno del flamenco en todos sus aspectos y a la búsqueda de soluciones para los mismos. Fines Los fines del Congreso que ya vienen marcados en el Reglamento del Congreso de Actividades Flamencas, son los siguientes: Fomentar la práctica, el estudio y la investigación del arte flamenco en sus tres vertientes de cante, toque y baile, velando por su pureza como bien irrenunciable, así como también conectar a las organizaciones y promotores de actividades flamencas para procurar un mejor entendimiento entre ellos y dar solución a los problemas que su dedicación plantee. Congresistas Podrán ser miembros del Congreso: 1. Las peñas y asociaciones flamencias específicas legalmente constituidas. 2. Las organizaciones de concursos y festivales. 4. Los profesionales del flamenco. 5. Los estudiosos, tr\n\n[ENDING CONTEXT]\n\nque acaba de obtener el premio especial del jurado del Festival de la Rosa de Oro de Montreux.\n\nEl filme, que lleva por título «El Cante de la Sierra», tiene como base una entrevista con el cantaor y ofrece una perspectiva amplia del personaje. La cámara lo muestra en concierto, en sus tareas de cabrero y en familia. Ha sido rodado el pasado verano en Aznalcollar, Marinaleda, La Carbonería y durante un recital en Bayonne (Francia).\n\nEs una realización de Martine Voyeux, Béatrice Soulé y Amar Arhab y ha sido producida por las cadenas 1. $ ^{a} $ y 7. $ ^{a} $ de la televisión francesa.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 1564,
    "article_char_count_full": 9723,
    "article_char_count_review": 3270,
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
  },
  {
    "article_id": "1989-05-25-right-hablan-las-pe-as",
    "article_text_for_review": "II TORREÓN FLAMENCO DE LAS GABIAS (Granada)\n\nDeseamos toda clase de aciertos a la numerosa Junta.\n\nHan sido establecidos tres grupos de cante, más uno especial de cantes por granaínas. Igualmente se han establecido cuatro premios, siendo el primero de 135.000 pesetas. La final tendrá lugar el día 15 de julio.\n\nOrganizado por la Peña Cultural de Arte Flamenco de Las Gabías (Granada) y patrocinado por el Ayuntamiento, ha sido convocado el II Torreón Flamenco de Las Gabías (Concurso Regional de Cante).\n\nPara más información los interesados pueden dirigirse a la citada Peña, Plaza del Fuerte, n.° 7.\n\nEn asamblea general de socios celebrada por la Peña Flamenca de Lepe (Huelva), resultó elegida nueva Junta Directiva, quedando la misma compuesta de los siguientes nombres: presidente, José Aguaded Botello; vicepresidente, Isidoro Ruiz Muriel; secretario, Antonio Díaz Gómez; vicesecretario, Manuel Santana Gómez; tesorero, Carmelo García Ávila; vicetesorero, Manuel del Valle Martín; vocales, Manuel Oria Villegas, Juan M. Prieto Povea, Sebastián Farauste Pérez, Francisco Fernández Zamorano, Juan Pérez Martín, José Santana Gómez, José A. Rodríguez Moreno, José Forque Bejarano, Ernesto Castañeda Oria, Fernando Romero Gómez y Antonio Mendoza.\n\nNUEVA JUNTA DIRECTIVA EN LA PEÑA FLAMENCA DE LEPE\n\nNUEVA JUNTA DIRECTIVA DE LA PEÑA FLAMENCA «FRASQUITO DE PUENTE GENIL»\n\nEn Asamblea General Ordinaria celebrada por la Peña Flamenca «Frasquito de Puente Genil», fue elegida nueva Junta Directiva, quedando la misma compuesta de la forma siguiente: presidente, Antonio Ramírez Soria; vicepresidente, Antonio Gutiérrez Rodríguez; secretario, Antonio Sojo Sojo; vicesecretario, José Amador Trigos; tesorero, Andrés Herrera Gaona; vocales, José Navas Carmona, Antonio Moreno Alijo, José L. Mendoza González, Antonio Carmona Roldán, Rafael García Lorenzo y Pablo Cabello Jiménez.\n\nDeseamos una feliz andadura flamenca.\n\nXVIII VOLAERA FLAMENCA DE LOJA\n\nLa Peña Flamenca Alcazaba, bajo el patrocinio del Ayuntamiento de Loja (Granada), Delegación de Cultura y Diputación Provincial, convoca la XVIII Volaera Flamenca (Concurso de Cante Jondo), cuya final se celebrará el día 26 de agosto.\n\nLos cantes a interpretar por los concursantes han sido divididos en tres puntos.\n\nLos premios establecidos para los finalistas son cinco, siendo el primero de 150.000 pesetas y la grabación de un disco.\n\nLos interesados pueden dirigirse a la citada Peña, Cerrillo del Fraile, n.º 1, o llamando a los teléfonos: 320438, 320251 y 321327.\n\nNUEVA JUNTA DIRECTIVA DE LA FEDERACIÓN PROVINCIAL DE PEÑAS DE ALMERÍA\n\nEl pasado mes de marzo y con motivo de la visita que efectuaron algunos miembros directivos de la Confederación Andaluzia de Peñas Flamencas, quedó nuevamente constituida la Federación Provincial de Almería, según relacionamos: presidente, Constantino Díaz Benete, de la Peña «El Morato»; vicepresidente, José Antonio López Alemán, de la Peña «El Taranto»; secretario, Juan Martínez Sánchez, de la Peña «El Yunque»; tesorero, José Manrubia Medina, de la Peña «El Sabinal»; vocales, Gabriel García López, de la Peña «El Yunque»; Manuel Villegas Barrionuevo, de la Peña «El Sabinal»; y José M.ª Zapata Company, de la Peña «El Morato».\n\nDeseamos que esta nueva junta consolide, definitivamente la Federación. Mucho éxito.\n\nVII CONCURSO DE CANTE JONDO «Niño de Vélez»\n\nCon el patrocinio de la Delegación Municipal de Cultura del Ayuntamiento de Vélez-Málaga, ha sido convocado el VII Concurso de Cante Jondo «Niño de Vélez», en el que podrán participar cuantos cantaores de ambos sexos lo deseen sin limitación de edad.\n\nEl concurso constará de dos grupos de cantes, habiéndose establecido seis premios —tres para cada grupo—, siendo el primero de 125.000 pesetas para cada grupo.\n\nLos interesados podrán inscribirse hasta el 28 de julio, dirigiéndose a la Peña Flamenca «Niño de Vélez», calle Tejada, Edif. Granada, o llamando al teléfono 501469.\n\nMario Maya Fajardo nació en Córdoba en 1937. De él dice Enrique Llovet: «Mario Maya es un bailaor excepcional. Su sistema coreográfico —deliberadamente frío y nada sensual— está pulido como el cobre. Su enervante y, si puede decirse así, pausada forma de zapatear, recuerda el eterno compromiso andaluz entre los bailes profanos y los religiosos. Sobrio y casi trágico de gesto, Mario Maya zapatea purificadoramente. Su giro de muñecas, de dentro a fuera y con los dedos juntos, la quietud de sus caderas y la armonía de sus pies y sus manos, también parecían una defensa de la dignidad».\n\nBAILAORES DE HOY Mario Maya\n\nLA GENERAL\n\nCaja de Ahorros y Monte de Piedad de Granada Para lo que haga falta",
    "title": "Hablan las peñas",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "25-27",
    "page_number": 25,
    "word_count": 712,
    "article_char_count_full": 4635,
    "article_char_count_review": 4635,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
