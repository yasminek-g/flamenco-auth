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
    "article_id": "1979-09-3-right-editorial",
    "article_text_for_review": "Editorial\n\nEn su obra «Teoría de Andalucía», Ortega y Gasset -se conmemora en estos días el veinticinco aniversario de su muerte - opina que España ha vivido sometida a la influencia hegemónica de Andalucía. La afirmación de nuestro filósofo que se refiere al siglo XIX, requiere algunas matizaciones. Sí, es cierto que a través del siglo XIX y parte del XX existe una hegemonía política de la burguesía andaluza a nivel de todo el estado español. La cifra de políticos andaluzes que ostenta el poder en este período es reveladora: cuatro presidentes de la I República son andaluzes. Todo el siglo XIX español está netamente marcado por dos figuras andaluzas: Mendizábal, artífice de la desamortización y Cánovas, protagonista de la Restauración. Es preciso, sin embargo, puntualizar que la hegemonía española no está en manos del pueblo andaluz, sino en manos de la clase dominante andaluza que controla, hasta en las más altas esferas, el aparato del Estado. Es entonces, cuando España se representa a través de lo andaluz y su cultura se españoliza. También el cante jondo, sobre todo el cante jondo. Se produce el primer «boom» del arte flamenco que coincide, lógicamente, con la aparición de los cafés cantantes y su difusión por toda la geografía hispana. Dicho en otros términos, el cante jondo se le expropia al pueblo andaluz y queda trivializado como folklore oficial del estado. Esta sencilla reflexión explica a nuestro juicio, la pérdida progresiva de la identidad cultural andaluza y el origen del deterioro y descomposición del cante. Porque éste, mientras fue expresión de la etnia andaluza, mantuvo su riqueza de comunicación, su profundidad, su belleza intrínseca; pero al difuminarse, como folklore oficial del estado, el cante tiene que ensanchar sus contenidos, adecuarse a una base sociológica que le era extraña. En definitiva, falsearse. Por eso sobreviene el reinado del fandango y del cuplé, la popularidad de una forma de cante ridícula que se prodiga junto a la imagen del torero, cantaor y la gitana en literatura y filmografía folletinesca, de una espantosa vulgaridad. Es por eso, también, por lo que aquellos palos de neta personalidad y de difícil adaptación como la solea y la siguiente queda relegados y desconocidos del gran público, a Dios gracias.\n\n<CANDIL> en su humilde intento, pretende retomar la vieja situación del cante; cuando éste era sólo cántico o aullido, luz o desesperación de un hombre y de un pueblo: Andalucía.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1979-09",
    "year": 1979,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 401,
    "article_char_count_full": 2465,
    "article_char_count_review": 2465,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1979-09-4-right-de-los-caf-s-cantantes-al-festiv",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA pocos estudiosos y «aficionaos» del cante se les oculta que la decadencia del arte flamenco comienza, cuando en la segunda mitad del siglo pasado se inicia su popularización. Los cafés-cantantes plantean el flamenco como espectáculo, como oficio; cante que ha de ser dicho, por primera vez, para otros, antes que para uno mismo; cante sometido a la mecánica de puntuales realizaciones, sin inspiración. Esta es la razón por la que los más geniales cantaores —tal es el caso de Manuel Torre—, fracasan estrepitosamente en los cafés cantantes o, al menos, no dan la medida de sus reales posibilidades. El carácter intimista del cante, su necesaria espontaneidad, la radical vivencia que ha de basar tan estremecedoras expresiones, han de supeditarse a la disciplina horaria que marca el espectáculo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\nrealizaciones, sin inspiración. Esta es la razón por la que los más geniales cantaores —tal es el caso de Manuel Torre—, fracasan estrepitosamente en los cafés cantantes o, al menos, no dan la medida de sus reales posibilidades. El carácter intimista del cante, su necesaria espontaneidad, la radical vivencia que ha de basar tan estremecedoras expresiones, han de supeditarse a la disciplina horaria que marca el espectáculo entre otras muchas más limitaciones. Antonio Machado Alvarez lo entendió así y sus observaciones en el prólogo a su colección de coplas nos resulta hoy premonitorio. Disentimos del comentario que en sentido contrario, realiza Ricardo Molina en su conocida obra «Mundo y Formas del Cante Flamenco». Entiendo que el poeta cordobés confunde dos conceptos perfectamente distinguibles: cante y cantaor. Es cierto que hacia 1880 —época de florecimiento de los cafés cantantes—, están en activo cantaores de la talla de Silverio, Enrique el Mellizo, Loco Mateo, Manuel Cagancho «La Serneta», Tomás Nitri, Paco la Luz, entre muchos más. Tal afirmación no contradice, primero que épocas anteriores no existieran cantaores de la talla de los anteriormente mencionados; y segundo, que no es el café cantante el que propicia semejante florecimiento de cantaores; en todo caso, lo que hacen los cafés cantantes es utilizar ese florecimiento en base a una afición ya existente que hace pensar a los más listos que el flamenco puede ser negocio. Por otra parte, hay que pensar que no eran los cafés cantantes el «habitat» idóneo del «duende», ni el ambiente adecuado para que tan geniales cantaores dieran toda la medida de su arte. Por el contrario, era de esperar que las exigencias de un público heterogéneo condicionara la autenticidad del cante, con la consiguiente pérdida de frescura y profundidad. Otra de las observaciones de Ricardo Molina en torno a los cafés cantantes se refiere a la influencia benéfica que éstos proyectan sobre el flocklo-re andaluz. Tal afirmación, pensamo\n\n[ENDING CONTEXT]\n\ncasi aullido de la siguiriya o la templanza plural y cadenciosa de la soleá.\n\nHe ahí una labor importante que las Peñas Flamencas pueden desarrollar si en lugar de prodigarse en aparatosos festivales flamencos, —necesarios, por otra parte, por virtud de las necesidades económicas de los cantaores ya profesionalizados—, fomentan la reunión íntima, cabal de esos mismos profesionales que entre tanto y tanto espectáculo flamenco, rozan el peligro de no recordar, qué fue realmente el cante Jondo.\n\nRamón Porras\n\nConstrucciones\n\nOBRAS EN GENERAL\n\nPolígono «Los Olivares»\n\nCalle Alcaudete, 10\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "De los Cafés-Cantantes al festival flamenco",
    "periodical": "candil",
    "issue_id": "1979-09",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 1098,
    "article_char_count_full": 6910,
    "article_char_count_review": 3625,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      }
    ]
  },
  {
    "article_id": "1979-09-5-right-para-una-sociolog-a-de-las-taran",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEn puridad, como apuntara Tomás Borrás, el flamenco como expresión artística no se configura hasta bien afianzado el pasado siglo. Desde mediados del siglo XIX hasta el primer cuarto del XX, se desarrolla la etapa que denominamos histórica del cante jondo: época larga y distinta en la que, con toda su grandeza y servidumbre, aflora públicamente el flamenco que conocemos. Más de uno ha considerado a estos años como «Epoca de oro del cante», para otros, con ella y muy pronto, se iniciaría su decadencia al perder el protagonista del cante su interioridad expresiva. Sin entrar en su grandeza o agonía, a mi juicio es esta una etapa en la que el flamenco en sus diversas manifestaciones externas (ya es producto de consumo, también nace como desgarro y expresión de una situación socio-laboral por\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\noca de oro del cante», para otros, con ella y muy pronto, se iniciaría su decadencia al perder el protagonista del cante su interioridad expresiva. Sin entrar en su grandeza o agonía, a mi juicio es esta una etapa en la que el flamenco en sus diversas manifestaciones externas (ya es producto de consumo, también nace como desgarro y expresión de una situación socio-laboral por primera vez en la historia), corre muy parejo a la Historia de España; mejor, de las dos Españas. Así, pongamos por caso, conviven letras reaccionarias, obscenamente compuestas para halagar a un público, a unos hombres, que las pagan; junto a otras nacidas en la calle, en el trabajo e incluso con vocación de decidida llamada política si bien estas últimas, hemos de reconocerlo, de decidido mal gusto. (Recordemos que a partir de 1854 el proletariado andaluz irá tomando conciencia de clase, y no por sus triunfos, sino por sus fracasos. También estamos en los años de las intentonas revolucionarias burguesas, tan ligadas a la oligarquía, que bien poco calaran en las masas populares. Una época esta, que se inicia con un cierto esplendor industrial regional y que concluye con una grave acentuación de su decadencia y postrac\n\n[ENDING CONTEXT]\n\ndesde niños —paseantes por las galerías de la mina con el agua a las rodillas—, porque el vino es barato, pero el tocino es caro —el kilo a veces vale tanto como el jornal de un hombre—, porque la carne y la leche son artículos de lujo reservados a los pudientes, para el pobre bacalao, garbanzos y alubias; porque los accidentes son muchos —el minero temerario, y esa cuerda de polea que hay que apurar al máximo porque tiene que comprarla de su bolsillo; porque las viviendas son pocas y pequeñas —el hacinamiento es campo abonado para los gérmenes infecciosos—. El plomo, metal de vida y me\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Para una sociología de las Tarantas y los cafés de cante de Linares",
    "periodical": "candil",
    "issue_id": "1979-09",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 1337,
    "article_char_count_full": 7955,
    "article_char_count_review": 2830,
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
    "article_id": "1979-09-7-right-la-taranta",
    "article_text_for_review": "Donde la tierra no es surco ni cultivada piel, ni paisajes heróicos; donde ni el trigo, ni el pájaro u otras energías coronan predios embriagadores... Donde la tierra es entraña, huracán de silencio, vientre perforado, de innúmeras varices de metal, estremecidamente, hiendes tú, rompes tú, desvencijada voz, acorralada y dulce, ávida, delirante... Me pregunto: ¿Qué fervores enciende la tiniebla? ¿Qué condición del hombre puede engendrar, en una mina, este rito soleado del cante¿\n\nRamón Pozzas",
    "title": "La Taranta",
    "periodical": "candil",
    "issue_id": "1979-09",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 75,
    "article_char_count_full": 496,
    "article_char_count_review": 496,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1979-09-8-right-alusiones-gitanas-en-la-literatu",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Jesús Lechuga y Cobo de Guzmán\n\nLa recopilación de citas y alusiones a los gitanos que ofrecemos a continuación, comienza a partir del siglo XVII con el Padre Martín del Río (libro IV de las disquisiciones acerca de la Magia), quien al hablar de la adivinación por medio de las rayas de las manos, es decir, de la Quiromancia, considera acertado hablar de los gitanos, ya que son sobre todo las mujeres de aquella casta, las especialistas en esta técnica adivinatoria. Escribía el jesuita a fines del siglo XVI, (concretamente en 1616 se publicó su obra citada en Venecia), «que el echar la buenaventura es profesión gitanesca», aprendidas «sus recetas generación tras generación, transmitidas por vía oral».\n\nSegún el citado autor, «los gitanos son muy dados a toda clase de maleficios. Son, en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombres\"]\n\nmujeres de aquella casta, las especialistas en esta técnica adivinatoria. Escribía el jesuita a fines del siglo XVI, (concretamente en 1616 se publicó su obra citada en Venecia), «que el echar la buenaventura es profesión gitanesca», aprendidas «sus recetas generación tras generación, transmitidas por vía oral». Según el citado autor, «los gitanos son muy dados a toda clase de maleficios. Son, en esencia, ladrones y hechiceros o hechiceras: los hombres más entregados al latrocinio, si cabe; las mujeres a la magia». Sigue el autor diciendo cómo los príncipes los toleraban en sus Estados, «con gran escándalo de los rústicos, más expuestos que nadie a sus latrocinios, más dados también a creer en los prodigios que se narraban acerca de ellos». «Si a los demás se les piden responsabilidades si hurtan, a ellos se les abre la mano; si a los demás se les persigue por maleficios o adivinaciones, a ellos se les deja hacer». «No se bautizan, y si se bautizan lo mismo daría que no lo hicieran; no tienen patria ni señor conocido». Este autor creía firmemente que, «según la experiencia, cuando alguien daba una moneda a un gita- no, sacándola de una bolsa o caja en donde había más, automáticamente aquellas que quedaban se iban a buscar a la bolsa del gitano; lo cual, claro es que no puede hacerse sino por vía de maleficio». Con respecto a la capacidad de hablar distintos idiomas, el buen padre jesuita se asombra, estando en León en 1584, de que el «conde» de una horda de gitanos que apareció en dicha ciudad aquel año, «hablara el castellano como si fuera de Toledo», lo que le lleva a la conclusión de que aquello se debía a alguna maldición o condena religiosa. Recordando la creencia de aquellos tiempos de que los gitanos que «venían de Egipto Menor», se convirtieron al cristianismo, después tornaron a los errores de la gentilidad, y que una vez vueltos a la fe, en penitencia, todos los años algunas de sus familias, por expiación del crimen, debían salir a recorrer el mundo, el padre Del Río aña\n\n[ENDING CONTEXT]\n\ntumba abierta, pensando, queriendo al amigo, al amigo que esa noche está en Sevilla, sentado con José y con el otro José Romero, en el vino y la paz de los amigos, José dirá tan limpiamente y al invento: «Fernández Malo, escucha ésto ya que te vas pa Jaén»:\n\nHay cosas que dan la muerte,\n\ny no probar el aceite.\n\nAquí, ya aquí, en este Jaén de nuestras ducas, por agradeció que soy, amigo José, aquí tienes tu casa.\n\nAlfonso Fernández\n\nALMACENES\n\nMANUEL GARCIA MORENO\n\nGENEROS DE PUNTO CONFECCIONES VENTAS MAYOR Y DETALL\n\nALMACEN Y OFICINAS: Dr. Civera, 33 - Teléfonos 23 13 90 y 23 16 87\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Alusiones gitanas en la literatura española",
    "periodical": "candil",
    "issue_id": "1979-09",
    "year": 1979,
    "language": "es",
    "article_type": "article",
    "pages": "8-11",
    "page_number": 8,
    "word_count": 3065,
    "article_char_count_full": 17863,
    "article_char_count_review": 3643,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombres"
      }
    ]
  }
]
```
