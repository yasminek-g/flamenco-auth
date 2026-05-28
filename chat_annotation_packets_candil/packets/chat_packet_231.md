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
    "article_id": "1991-03-21-left-in-memoriam-rafael-romero-el-gal",
    "article_text_for_review": "In memoriam\n\nCuando aún estaban en plena vigencia las fiestas navideñas a las que él en tantas ocasiones alegró con sus cantes, quiso la fatalidad del destino que fuera su nombre la primera baja producida al comenzar el año, en la ya reducida nómina de maestros cantaores con auténtica solera, al acentuarse la dolencia de sus padecimientos que de manera rápida determinó su fallecimiento, produciéndose su enterramiento en el panteón familiar en el cementerio de La Almudena de Madrid, y en el más estricto ambiente familiar, al no haberse facilitado ningún despacho de prensa ni comunicado de radio hasta pasados unos días después del óbito.\n\nAun cuando ya estábamos en conocimiento de alguna parte de sus dolencias, que a veces trastornaban su normal comportamiento por la falta de control que las determinaban, nada hacía presagiar un final tan rápido que al ser conocido por toda la afición ha producido una gran conmoción y sentimiento al estar altamente considerado como uno de los grandes maestros del cante flamenco, que ejerció como enlace testimonial entre dos épocas de lo más apasionadas e interesantes que produjo este arte.\n\n1 rasladado a Madrid desde su Andújar natal allá por el año 1941, afrontó una verdadera aventura por las dificultades que habría de ir venciendo, tanto en lo económico, por la penuria de la época, y también por lo escasamente y mal pagado que estaba este arte, por la mala consideración de que disfrutaba, mucho más también por la dureza de la oposición que suponía tener que abrirse paso ante figuras tradicionales y firmemente encumbradas, lo que no fue obstáculo para que al calor de ellas y muy especialmente acogido por Perico el del Lunar padre, completara en poco tiempo una extraordinaria formación flamenca que rápidamente le permitió abrirse paso para que su nombre primeramente alcanzara una buena estimación y cotización en Villa-rosa, desde donde accedió a formaciones y espectáculos hasta ingresar en el Tablao Zambra inaugurado en 1954, donde estuvo actuando ininterrumpidamente durante 18 años hasta que, en 1972, en que por el fallecimiento de su fundador, propietario y director cerró sus puertas.\n\nEl gran prestigio que adquirió su nombre fue el premio a que le hicieron acreedor su indesviable vocación y formación profesional flamenca, ya que sin estar adscrito a ninguna de las escuelas tradicionalmente influyentes en los cantaores de su época, supo crearse un estilo muy personal sin salirse de la pureza y ortodoxia que reinaba en la época, imponiendo a sus cantes un sello tan personal que bien pudieron determinar la creación de un estilo en el que no hizo ningún tipo de concesiones, circunstancia que agrandó su figura hasta convertirlo en uno de los más altos e indiscutibles valores de nuestra última época.\n\nLa personalidad que irradiaba de su gitanísima figura, trascendió de una manera absoluta a su cante, haciendo de él un rito que realizaba como auténtico ceremonial todas sus prodigiosas actuaciones.\n\nVivió con gran dignidad su carrera profesional. Fue hombre antento y amable para quienes dentro de sus especiales características racionales, supimos entenderle y comprenderle, tolerando como cosa natural su apasionado gitanismo en el que vivió inmerso hasta el final de sus días.\n\nValor auténtico de nuestro arte flamenco nos deja una interesante y extensa discografía como fuente testimonial e inagotable para cuantos quieran aprender la auténtica belleza que encierra en sus diferentes palos y estilos todo el cante\n\ngrande del que este genial Rafael Romero «El Gallina» ha sido uno de sus más acreditados intérpretes.\n\nHa sido abundantísimo el tratamiento que a este triste acontecimiento se le ha dado en Madrid y Sevilla a través de los medios de comunicación Radio Nacional 1, Radio Nacional 2, Radio Nacional 5, Onda Madrid, Radio Intercontinental, donde se le han dedicado programas completos ensalzando cuanto fue y representó esta gran figura que no ha tenido el mismo eco en la prensa diaria, en alguno de cuyos diarios se le ha dado un trato denigrante que en nada se ha correspondido con el interés y el amor de la esposa y de los hijos del artista fallecido.\n\nDesde estas columnas y como homenaje de amistad, respeto y admiración enviamos a sus familiares nuestra condolencia.\n\nMadrid, 21 de enero de 1991",
    "title": "In memoriam. Rafael Romero «El Gallina»",
    "periodical": "candil",
    "issue_id": "1991-03",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 699,
    "article_char_count_full": 4300,
    "article_char_count_review": 4300,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-03-22-left-el-ltimo-de-la-escuela-carta-a-r",
    "article_text_for_review": "Sí, amigo Rafael, contigo se ha ido el último de la escuela, pero no porque tú fueras el último de la clase, sino porque contigo ha desaparecido el último de los cantaores que han creado escuela.\n\nAunque nunca estuviste en la cresta de la ola, tu personalísima ejecución de los cantes sí que ha sido puntualmente valorada por los auténticos aficionaos.\n\nTus resueltas modulaciones en los cantes por caña, petenera, serrana, rondeña, soleares y cantes de la madrugá, han hecho de ti el último de esa pléyade de maestros que perfilaron y enriquecieron este hermoso Flamenco que nos ha sido legado.\n\nContigo, Rafael, se ha ido uno de los gitanos cantaores más completos y carismáticos.\n\nTu consuetudinaria figura juncal rezumaba arte hasta en el caminar. El color de tu rostro estriado mostraba cuál era tu tribu. Esa que tantos y tan buenos artistas ha aportado al Flamenco hasta hacerlo un arte inconmensurable.\n\nPero tu arte no ha muerto contigo, porque aquí en tu tierra, en tu Jaén, has dejado unos fieles discípulos que continuarán haciendo tus cantes con la finalidad que tú les enseñaste. Sabes que me refiero a Rosario López, Carlos Cruz y Paco «El Pecas».\n\nMe gustaría vivir lo estipulado para pregonar tu magisterio y hablar de tu específica personalidad; pero para morir es sólo necesario que la hoja del otoño ceda al viento y los hombres al sueño.\n\nAl menos me queda la seguridad de que aquí, en tu tierra, no te hemos sido ingratos.\n\nEl tiempo te ha requerido, como a todos nos lo demandará, con ese apacible sueño que espero hayas encontrado.\n\nDescansa en paz.\n\nPedro Sánchez Ortega",
    "title": "El último de la escuela Carta a Rafael Romero «El Gallina»",
    "periodical": "candil",
    "issue_id": "1991-03",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 273,
    "article_char_count_full": 1595,
    "article_char_count_review": 1595,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-03-25-left-entrega-de-la-vii-distinci-n-com",
    "article_text_for_review": "Rafael Valera Espinosa\n\nE n un mínimo «roá», con la esencia añeja de su arte pleno de matiz, simple y resoluto, nos encandiló la magistral Matilde Coral con el enorme granito de arena que aportó en el homenaje que se le rendía a Manuel Muñoz Alarcón con la entrega de la VII Distinción «Compás del Cante», instituido por La Cruz del Campo, S. A. Y es que Manolo Sanlúcar ha sido un claro merecedor de este trofeo. Pienso que pocas veces, un jurado con la solvencia de este, ha mantenido un criterio tan unánime, tan firme y tan tajante. Como bien escribe mi compañero José Luis Buendía en el anterior número de «Candil», creo que no estuvieron inspirados, sino acertadamente inspirados.\n\nEl acto tuvo lugar, como decimos, en un hermoso y artístico «roá», El Salón Real del Hotel Alfonso XIII. Y bajo su preciosista artesonado, la representación del arte flamenco a través de artistas, intelectuales, autoridades, aficionados y directivos de la empresa patrocinadora. Todos, como una auténtica familia, reconociendo la incuestionable figura de Manolo Sanlúcar, su quehacer flamenco y una trayectoria artística que es viva muestra de tesón y calidad singulares.\n\nEn los postres, tras la entrega de «er Oscar der flamenco» —como bien lo definió Fernanda de Utrera el pasado año—, las palabras serenas, simples, humildes, llenas de calor y emoción de Manolo Sanlúcar agradeciendo el homenaje que se le estaba rindiendo. Tras él, como en representación de todos los asistentes, la contestación primorosa, ocurrente, ilustrada y versátil de Antonio Gala.\n\nY, sin más, el arte flamenco. Con su inigualable gracia y sencillez, Juan Miguel Rodríguez Sarabia, «Chano Lobato», se aposenta en el central corro que a tal fin han establecido los presentes. A su lado, serio, concentrado en la labor a realizar con su guitarra, José Luis Postigo. Da comienzo la fiesta con bulerías en las cuales «Chano Lobato» muestra sus características peculiares: compás, gracia, raíz gadi-tana, algunos ecos caracoleros y matizaciones del cante para bailar, en el estilo. vidable. ¡Qué frescura de movimientos! Armoniosos, acompasados, suaves de ejecución y plenos de gracia, testimoniando así su personal arte, el cual perdurará durante largo tiempo en la memoria de los que allí estábamos.\n\nSurgen seguidamente las cantiñas. En su mesa, atraída por el duende que iba surgiendo del cante de «Chano», Matilde Coral luchando consigo misma y con su marido, Rafael «El Negro», para una vez superar las reticencias, levantarse y con un andar «a compás», realizar un sinuoso recorrido hasta desembocar en su mínimo «roá», el que necesitó para componer una artística figura y comenzar un baile por cantiñas inol- Y como de una fiesta-homenaje se trataba, otra vez las bulerías en la voz de «Chano Lobato». Más ecos caracoleros, algunos de La Niña de los Peines, los del propio artista, y ante el arranque nuevamente de Matilde Coral, sumándose aquí Rafael «El Negro», el recuerdo de Antonio «El Chaqueta». Sigue personalizando la fiesta «Chano Lobato» con sus graciosos tanguillos, tras los que surgen de nuevo los aires «por bulerías» con unos matices que nos recuerdan sus años mozos cuando en el citado compás solía meter letras y entonaciones de tangos argentinos.\n\nCon solidario reconocimiento a Manolo Sanlúcar, también ocupa su «roá» la «honda» Fernanda de Utrera. Su rancia voz llena la sala «por soleá». Los que referenciamos eventos flamencos, por lógica vamos tomando apuntes de lo que está aconteciendo para posteriormente ofrecer el máximo detalle. Y cosa curiosa, cada vez que escucho a Fernanda de Utrera, cuando más tarde me propongo relatar el matiz de su arte, sólo encuentro que he anotado: «Fernanda de Utrera: Por soleá». Y es que el arte inigualable de esta gitana me produce tal abstracción, que aunque cante por Alcalá, La Serneta o Juaniquín, siempre queda en mi memoria su personal forma de acrisolar estos matices, que no es necesaria ninguna anotación aclaratoria complementaria. Así sucedió también el pasado día 7 de marzo en Sevilla.\n\nEl punto final lo puso «El Nano de Jerez» con aires de su tierra «por bulerías», acompañadas de unos pasos de baile que fueron clara muestra de su desenfadado arte.\n\nHasta aquí la sucinta crónica de una noche flamenca en la que se reconoció el arte y la creatividad de un artista flamenco universal como es Manuel Muñoz Alarcón, «Manolo Sanlúcar».",
    "title": "Entrega de la VII Distinción «Compás del Cante»",
    "periodical": "candil",
    "issue_id": "1991-03",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 714,
    "article_char_count_full": 4380,
    "article_char_count_review": 4380,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-03-26-right-el-flamenco-y-la-cultura-popular",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCristina J. Cruces Roldán\n\n1. Introducción\n\nAsistimos en los últimos tiempos en Andalucía al resurgir de una manifestación fundamental de su folklore: el flamenco. La proliferación de todo tipo de espectáculos flamencos, la masiva asistencia a convocatorias de este carácter, los programas de la televisión andaluza dedicados al flamenco... son algunos ejemplos de la afirmación anterior.\n\nPero también asistimos a este interés sobre todo en las zonas centrales del Estado español. Y, más allá de las fronteras estatales, un fenómeno sociológico de interés: «España está de moda». España como generalización de lo particular, totalización de la parte (Andalucía) que se presenta al mercado como un producto de consumo más, en que el flamenco y su amplia simbología asociada juegan un papel\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradición\"]\n\ngan un papel destacado. Este conjunto de fenómenos requieren una revisión desde las Ciencias Sociales que clarifique, con casos concretos, de una parte, el sentido que en nuestra cultura ha tenido y tiene el flamenco, más allá de sus aspectos formales que están incorporándose, sin más, a los circuitos de compra y venta propios del sistema económico capitalista. De otra, que ponga en entredicho esta apropiación de los resultados culturales de la tradición (funcional) de un pueblo y abogue por la re-apropiación por los anda- luces del flamenco como bien de uso, frente a su progresiva definición como bien de cambio que anula la identidad diferencial de nuestro pueblo. Intentaremos ofrecer, desde una perspectiva que sigue a la escuela marxista del estudio de la cultura popular, una interpretación desde la Antropología Social del significado de flamenco en Andalucía y de su progresiva mistificación. De hecho, los contenidos del flamenco se han decantado en muchas ocasiones hacia la plasmación de las condiciones materiales y sociales de existencia de grupos marginados y la relativa denuncia/activación política resultante. El flamenco como producción de las clases subalternas, con una peligrosidad potencial evidente, ha sido históricamente y está siendo hoy en gran medida, bien destruido, bien reconducido, desviado de sus funciones iniciales para ser incorporado a la «cultura oficial» que permita su neutralización. Frente a ello, defenderemos la necesidad de cargar aún de contenido la expresión flamenca en Andalucía, retomar la posesión del «flamenco vivido» frente al «flamenco comprado» en dos niveles de identidad básicos y no excluyentes. De una parte, la identidad andaluza global, para la que el flamenco sigue siendo un hecho definidor. De otra, la de los segmentos de clase (popular como subalterna), incluyendo en ella s\n\n[ENDING CONTEXT]\n\nvez globalmente la obra de los primeros folkloristas andaluces.\n\n(2) Durante 1988-89 realicé una investigación para la Fundación Andaluza de Flamenco, inédita: «Un estudio antropológico de las peñas flamencas sevillanas», en que se trata la cuestión. Sus conclusiones están actualmente en prensa en la revista Sevilla Flamenca y en las Actas del XVII Congreso Nacional de Actividades Flamencas (Jerez, septiembre de 1989).\n\n(3) Entendido en su «valor de uso», no «flamenco-mercancía».\n\n(4) «Uno se asomaba a la boca de una mina y la vio tan profunda que se encomendó a Dios».\n\nBibliografía citada\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El flamenco y la cultura popular andaluza Cristina J",
    "periodical": "candil",
    "issue_id": "1991-03",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "26-33",
    "page_number": 26,
    "word_count": 9473,
    "article_char_count_full": 60077,
    "article_char_count_review": 3467,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradición"
      }
    ]
  },
  {
    "article_id": "1991-03-34-left-discografia-flamenca",
    "article_text_for_review": "Manuel Yerga: Discografia flamenca (placas)\n\n\"Quédate con el Cante\"\n\nPrograma Flamenco\n\nSintonícenos de lunes a viernes, de 20,30 a 22,00 horas; viernes, sábados y domingos de 0,30 a 3,00 horas, FLAMENCO\n\nVargas, Manuela. Nombre artístico de Manuela Hermoso Vargas. Sevilla. 1941. Bailaora. Profesional desde niña. Trabajó junto a otros artistas con su maestro Enrique el Cojo, El Güito, El Mimbre, Matilde Coral, Chano Lobato, Fosforito, Chocolate y Enrique Morente. Ha sido glosada su personalidad artística, con profusión, por escritores, c.t.ticos y poetas, de cuyas opiniones seleccionamos las siguientes: Salvador López de la Torre: «El baile de Manuela Vargas está cargado de ese terrible dramatismo, de esa fatalidad que pesa sobre toda Andalucía. Manuela Vargas no inventa nada; se limita a transmitir, y su testimonio nos dice que así es Andalucía, por la sencilla razón de que así es». Angel del Campo: «Ella es una vara de nardo. Está consumida por la danza, sumida por la danza. Es un mimbre que se retuerce y se blandea al son de la sonata. Es uno de esos remolinos de polvo que el viento hace danzar por las veredas cuando nadie lo ve. Baila, baila mucho Manuela. Y lo que baila es todo genuino, sin añadidos ni modernidades». Claude Saraute: «Para mí esta mujer ha sido una revelación. Esbelta, nerviosa, flamea como un sarmiento ardiendo al ritmo de la petenera y de la siguiriya gitana. La tragedia es su reino, y la belleza de su línea, la pureza de cada una de sus actitudes, la dolorosa gravedad de su frente representan la más alta expresión del arte. Cuando sacude la cola de su vestido en un estremecimiento de sedas, la gitana se encabrita y retuerce y se ve en ella la heredera de las caravanas milenarias de los nómadas rindiendo sacrificio al culto ancestral de que Sevilla se ha hecho la guardiana».",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1991-03",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "34-35",
    "page_number": 34,
    "word_count": 308,
    "article_char_count_full": 1828,
    "article_char_count_review": 1828,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
