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
    "article_id": "1992-07-16-right-al-camar-n-de-la-isla",
    "article_text_for_review": "Al Camarón de la Isla\n\nS in tiempo ni espacio en mi memoria para aparar la vida que viviste, te recuerdo, aún difuso, si es que fuiste, cangilón aguaespuma de la noria.\n\nCon la semilla de ave migratoria en médula de todo te creciste —ahora que pareces ido o que te huiste— en dibujo o desdibujo de tu historia.\n\nPor la bocana del mundo: un trasmundo de aire nuevo nacido de la entraña de tu voz negrinegra en ¡ay! profundo.\n\nQueda, al fin, cabal rechazo y no maña: un enfrentado y abierto ;no! rotundo ante el curvo perfil de la guadaña. Al Camarón, dormido Camarón, lo llevó la corriente: puso pie y muchos pies hasta el último andén y se instaló en el último vagón.\n\nApañó el hatillo viejo del son y subió en hora de partida al tren seguido de la reata del «amén», del «tenía que pasar lo que pasó».\n\nEl Tomatito miraba al de Lucía, como mira el sol el agua de rocío, con los sacais de gotas consumidas.\n\nAlfonso Fernández Malo\n\nLos tres tan mínimos, los dos contritos: simple toque rasgado de la vida al cante tan jondo de uno mismo.",
    "title": "Al Camarón de la Isla",
    "periodical": "candil",
    "issue_id": "1992-07",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 195,
    "article_char_count_full": 1036,
    "article_char_count_review": 1036,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-07-17-right-camar-n-la-misma-verdad-distinta",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMiguel Acal\n\nA pareció casi como un niño malo. Venía a romper esquemas, a horadar el edificio monolítico que se estaba construyendo. No ocultó jamás sus preferencias por unas formas que se salían de la ortodoxia afincada. Logró el delirio de un sector y la revolución de las maneras. Era joven, vitalista, desordenado y apasionante.\n\nTenía todos los ingredientes necesarios para ser un ídolo. Ninguno para que la culta y seria élite del momento pudiera reconocer sus valores. Pero, lentamente, fue abriendo las telas del corazón y perforando las secas meninges de los santones.\n\nSu vida era sencilla: actuaciones en directo, grabaciones, contactos con músicos eminentes, su familia, sus amigos... No hacía declaraciones que conmovieran los cimientos del sistema. El iba a lo suyo y se preocupaba de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nes de los santones. Su vida era sencilla: actuaciones en directo, grabaciones, contactos con músicos eminentes, su familia, sus amigos... No hacía declaraciones que conmovieran los cimientos del sistema. El iba a lo suyo y se preocupaba de poco más. No hablaba de los demás y, muy poco, de sí mismo. Era así y vivía feliz. Y se fue lo mismo que vino. Había ganado, y gastado, mucho dinero; tenía a la masa —cada vez más a la élite— pendiente de su voz; era una leyenda viva para muchos y un triunfador inexplicable para algunos. Ahora se ha puesto de moda el análisis, la disección metódica —es curioso la fuerza que, ahora, tiene el método en algo tan anárquico e impalpable como el flamenco— de todas y cada una de sus facetas. Es natural que así sea porque, en vida, pocos se le dedicaron. Claro que fue rápido su paso, como el de todos, con el agravante absoluto de una juventud que insultaba. Aunque fuese una ju\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_03 | trigger=\"creativa\"]\n\nmiento. No, porque ¿dónde empieza la verdad del uno y termina la del otro?, si las dos conmueven. Una rápida mirada al fenómeno flamenco nos sitúa ante dos grandes elementos: los creadores y los restauradores. La historia del cante está llena de ellos. (Perdonen que olvide la erudición por innecesaria y pedante). Modernamente, junto a claras demostraciones vinculadas de tal forma con otra anterior, está teniendo más fuerza la actitud vitalista, creativa. El cantaor ha sido un restaurador, incluso un arqueólogo. La restauración pretende devolver la lozania, fijar la belleza, alargar la vida. La arqueología busca el pasado para dar con sus claves, para encontrar sus razones vitales. Ambas, si distintas, tienen muchos puntos en común. Y las dos nos descubren el embrión de la modernidad, que está presente en todo intérprete. El hombre siempre es fiel a su tiempo. Cuando «Camarón de la Isla» aparece, los flamencólogos no se vuelcan en elogios. Incluso lo rechazan o lo arrinconan. Justamente lo mismo que ocurre, en su momento, con Manuel Torre ante Chacón, Pastora ante Marchena o Mairena ante Caracol. Cuando Camarón desaparece todo son elogios, frases altisonantes, rebuscados intentos poéticos para definir la maravilla. Hay como un intento de borrar el silencio anterior con un\n\n[ENDING CONTEXT]\n\nun estudio más amplio, más pormenorizado, con muchos datos que evidencien la sabiduría. Pero ante lo que está claro, no son necesarias muchas páginas.\n\nJosé Monje Cruz —hay coincidencia hasta en los apellidos, aunque esto sea anécdota— ha sido el heredero directo, no de una forma de cantar, sino de concepción gitana del cante. Y ha sabido llevarlo en volandas de la admiración, más allá de las fronteras anteriores.\n\nCamarón, como gitano, cumplió con su obligación histórica. Aunque muchos, cuando pase la fiebre, recorten —eterna lucha entre dos formas— los veinticuatro quilates de su gitanería.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Camarón: La misma verdad, distinta forma",
    "periodical": "candil",
    "issue_id": "1992-07",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1241,
    "article_char_count_full": 7501,
    "article_char_count_review": 3901,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "creativa"
      }
    ]
  },
  {
    "article_id": "1992-07-18-right-camar-n-de-la-isla-algo-m-s-que-",
    "article_text_for_review": "E l vino de su cante se ha detenido en nuestro paladar para toda la eternidad. Permanecerá en nuestro recuerdo y aprisionado en las estрыas del microsurco. Camarón de la Isla ya es historia y leyenda.\n\nQuiso ser torero y fue cantaor. Hubiera dado igual de ser al contrario, un único y último anhelo estético le impulsaba y guiaba, lanzar el grito hacia lo alto, la espiritualidad y esencia del arte, porque como muy bien ha puesto de má- nifiesto Agustín Gómez, «el grito tiene materia plástica en sí mismo, sólo hay que verla y darle forma». Pero fue cantaor y como tal, un fen- nómeno sociológico.\n\nEra intuitivo, creador, imagina-tivo pero tuvo que ceñirse a los férreos cánones imperantes. Sin embargo, cuando fue ortodoxo, resultó innovador. Camarón de la Isla fue un cantaor idealista. Su cante fue exaltación y apasionamiento, espiritualidad, y también materia orgánica, vida. Una vida que fue generosa con él y que le dio a manos llenas todas las cosas, buenas y malas. Y él las vivió, las malas y las buenas, con ansia, con avaricia, glotonamente. Nos lo dijo en el estremecido lamento de una seguiriya o en el compás juguetón y ligero de una bulería.\n\nSus gestos, sus arbitrariedades, incluso sus desigualdades y, por supuesto, sus genialidades forman\n\nparte, hoy ya, de la leyenda. En vida fue espejo para las generaciones flamencas jóvenes y mito-insignia de un flamenco transformado en hecho sociológico.\n\nCamarón, con ser el gran cantar que fue, pasará a la historia del Flamenco, seguramente, como un hecho social de muy determinadas características. Nunca nadie ha logrado, en el mundo del Flamenco, las multitudinarias manifestaciones de fervor que obtuvo él. Su público, y hay que contarlo por miles, sólo iba a un espectáculo flamenco cuando él actuaba, el resto del cartel le era indiferente.\n\nSi en la historia del Flamenco hubiéramos de buscar otro fenó-meno sociológico de parecida magnitud, fervor y apasionamiento, tan sólo en la genial bailaora Carmen Amaya lo hallaríamos. Ambos son figuras indiscutiblemente universales. Nunca ningún gitano había alcanzado tales cotas de reconocimiento mundial. Sus gentes eran plenamente conscientes de ello y se lo agradecerían con lealtad y admiración inquebrantables.\n\nExisten, no obstante, algunas diferencias evidentes entre los dos. Carmen Amaya fue bailaora genial para sus fieles y para el aficionado clásico. Camarón, sin embargo, no lo es tanto para este último. Bien es verdad que le estima por su excelente y personalísima forma de interpretar el cante, pero rechaza el apasionamiento extraflamenco que provocaba en una parte de sus incondicionales, como también a un sector de ellos. Un sector integrado, básicamente, por gentes afectas a la rumba y a la salsa, a los ritmos calientes y ligeros. Un sector en el que también se incluyen, muy a menudo, gentes que rozan la marginalidad.\n\nUnamos a todo ello el claro predicamento de que gozaba entre los modernos y postmodernos y tendremos las principales razones del porqué Camarón era un hecho social tan especialísimo. Con su muerte los aficionados han perdido un gran cantaor, y sus seguidores, su espejo, guía y mito.",
    "title": "Camarón de la Isla, algo más que un cantaor",
    "periodical": "candil",
    "issue_id": "1992-07",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 512,
    "article_char_count_full": 3145,
    "article_char_count_review": 3145,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-07-19-right-camar-n-para-siempre-recuerdos-y",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«¿Has visto cómo cantar José en el último disco?» —me preguntó atónito un gitano entrado en años, en medio de la Plaza Alta, refiriéndose al trabajo «Flamenco vivo»—, recopilación de actuaciones en directo de Camarón en 1987. «¡Qué forma de cantá! Canta tan bien en el disco que yo l'escuchao una vez y l'a guardao».\n\n¿Qué querría retener aquel gita- no en su memoria tras escuchar una sola vez aquellos cantes del Camarón, y decidir después guardar el disco en un arcón de su casa sin volverlo a oír? ¿Quizá quiso retener el regusto de aquellos cantes escuetos, estrictos, vivos —guitarra y voz solitarias solamente—, después de otros trabajos algo más «sofisticados» de José grabados en estudio? ¿Quizá los quejíos y «bocaos» tremendamente acompasados de Camarón y Tomatito? (¡Qué guitarra más\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\ndel Camarón, y decidir después guardar el disco en un arcón de su casa sin volverlo a oír? ¿Quizá quiso retener el regusto de aquellos cantes escuetos, estrictos, vivos —guitarra y voz solitarias solamente—, después de otros trabajos algo más «sofisticados» de José grabados en estudio? ¿Quizá los quejíos y «bocaos» tremendamente acompasados de Camarón y Tomatito? (¡Qué guitarra más gitana la suya sonando por bulerías, tangos o fandangos!, y ¡qué voz tan auténtica la de José por alegrías!). ¿O quizá pensaba el gitano que aquélla era la única forma de conservar, cual tesoro escondido, la huella de emoción, de sentimientos irrepetibles que aquellos cantes habían dejado en su corazón al oírlos por primera vez? ¿O no sería,...en fin..., que aquel buen gitano tendría miedo en el fondo de su al- ma a dejarse arrastrar por esos cantes fuera de sus propios dominios, de los dominios de su propia voluntad conducida de repente a los terrenos de la fatalidad, de lo más hondo de sí mismo; donde las lágrimas son la única ventana para nuestro dolor o nuestro júbilo? Me he hecho estas preguntas cientos de veces durante estos días atrás, ahora sabiendo que Camarón no vive ya —al menos entre nosotros, de carne y hueso—, para herirnos complacidos con su voz, o para deslumbrarnos con sus ojos saltones de niño desconfiado y desamparado, y con su palabra corta y tímida pero esencial: ¿Ha muerto Camarón? ¿Se ha ido? Nos queda el infinito consuelo de haber compartido con José y Chispa —una semana antes de que él se nos marchara de aquí— unas horas de ensueño, que aún alentaron más si cabe nuestro deseo, nuestro anhelo ya lejano en el tiempo, de\n\n[ENDING CONTEXT]\n\nlos emisarios del pasado: habita también el amor. Preguntar y recordar significa comenzar a saber: significa también dar fe de una vocación amorosa». He querido —utilizando la frase de Félix—,\n\nEpílogo\n\ndar fe de mi vocación amorosa por la figura de Camarón, recordando aquellos momentos anteriores a su marcha que disfruté con él, con Dolores y con su familia. A ellos y a los lectores les pido perdón, si mi memoria ha transgredido algún terreno a ella vedado o ha cometido pecado de subjetividad, de apasionamiento. En tal caso, os reclamo a que no olvidéis, que nuestro corazón lo necesitaba.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Camarón para siempre (Recuerdos y retazos de nuestro último? encuentro con José Monje)",
    "periodical": "candil",
    "issue_id": "1992-07",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 2840,
    "article_char_count_full": 16289,
    "article_char_count_review": 3266,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      }
    ]
  },
  {
    "article_id": "1992-07-22-right-el-caro-precio-de-la-gloria-la-f",
    "article_text_for_review": "a conmoción nacional que dentro del mundo flamenco supuso el fallecimiento de este genio del cante, no nos sorprendió por cuanto que teniendo antecedentes muy concretos sobre el mal que venía padeciendo minando su salud con fuertes progresos al concitarse en un cuerpo débil y escaso de defensas dos males imparables médicamente, esperábamos este desenlace con la pena de saber la certeza de una muerte anunciada.\n\nLa obra que deja grabada como testimonio de su genial creación, va a ser con toda seguridad como lo han sido y lo serán las de todos los genios que en el arte han crea- do alguna cosa, discutida por la circunstancia natural y afortunada de moverse este arte entre el dogma y la herejía y ante esos dos términos tan antagónicos van a producirse multitud de opiniones que, como siempre ha sucedido, crearán un ambiente propio para la polémica.\n\nQue José Monje Cruz «Camarón de la Isla», ha sido para mi gusto personal el segundo genio que ha dado el Arte Flamenco, después de Pepe Marchena en este siglo, está fuera de toda duda, como también lo está el hecho de que con sus creaciones ha roto todos los moldes establecidos hasta su aparición como estrella del cante, cotizándose su caché a una altura económica jamás conocida, teniendo la virtud de haber conectado con el gusto de una juventud amplia y numerosa deseosa de conectar con el ambiente flamenco, dando a sus espectáculos una viveza y animación extraordinarias, al llenar con el anuncio de su nombre estadios y campos de fútbol, como plazas de toros, circunstancia que garantizaba a los empresarios la seguridad de pingües beneficios, razón por la cual tenían que pasar por sus exigencias y si ello se producía en sus actuaciones personales, otro tanto ocurría con sus grabaciones discográficas que, sin ser muy numerosas por lo cor-\n\nNo deja de producirnos una gran pena el hecho de que habiendo ganado en plena juventud, fama, gloria y dinero, no haya tenido a su lado amigos y personas que le hubieran aconsejado debidamente para evitar el triste final a que se ha visto arrastrado, sin duda, por falta de consejeros leales que le hubieran permitido a tiempo establecer rectificaciones en su rumbo de vida para que hubiera podido disfrutar de los grandes honores conseguidos, y sin duda, tan mal digeridos por una falta de experiencia que nos ha privado en plena juventud de esperar la superación de todo cuanto había realizado hasta el momento de haber disfrutado de la salud necesaria, teniendo que lamentar su muerte como una circunstancia natural y desgraciadamente muy corriente en nuestro ambiente flamenco, en el que parecen estar predestinadas todas sus glorias a morir como cigarras despilfarradoras de salud y fortuna.\n\nto de su existencia, no son menos interesantes artísticamente.\n\nEl espectacular acontecimiento que supuso en su pueblo natal su enterramiento no debe constituir en modo alguno el último homenaje que deje a toda la afición satisfecha, si es que como se rumorea y con bastantes visos de certidumbre queda la triste realidad de una familia deshecha y sin recursos económicos que aseguren la educación y supervivencia de unos hijos por cuya total educación debemos poner todo nuestro empeño para conseguir su estabilidad, hasta que llegada su mayoría de edad puedan encontrar el camino que les permita orientarse de manera adecuada, de acuerdo con sus gustos y criterios y de pensar seriamente en realizar lo que sea preciso en beneficio de esta familia, será conveniente realizarlo con la mayor proximidad en el tiempo a tan luctuoso suceso, porque con el paso del tiempo, como ya tenemos comprobado en casos similares, las ideas y los proyectos se van difuminando en el tiempo hasta perderse en el olvido.\n\nCreo que de esta lamentable circunstancia podrán sacarse enseñanzas provechosas, para que muchos de los «enganchados» que hay en el ambiente flamenco extraigan las consecuencias que les eviten el caer en estos males, de los que tan difícil resulta el reponerse cuando ha han hecho sus estragos.\n\nResulta confortador el que al conocer Camarón la importancia de su mal y su difícil recuperación, haya buscado en los últimos momentos de su vida refugio en su convencimiento religioso y por ello pedimos a Dios que le colme de su gloria para compensarle de lo que como artista le quedó por recoger, facilitando a toda su descendencia su protección y su amparo, incitándonos a ser solidarios para situar a su familia a buen recaudo de carencia y necesidades que pudieran producirse, rindiéndole de esta manera el mejor de los homenajes que pudieramos brindarle.\n\n¡Descansa en paz, genio!\n\nCarlos Lencero",
    "title": "El caro precio de la gloria, la fama y el dinero Antonio",
    "periodical": "candil",
    "issue_id": "1992-07",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 766,
    "article_char_count_full": 4611,
    "article_char_count_review": 4611,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
