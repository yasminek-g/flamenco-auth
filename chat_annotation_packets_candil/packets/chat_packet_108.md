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
    "article_id": "1985-03-3-right-editorial",
    "article_text_for_review": "Editorial\n\nLa Confederación\n\ncaba de constituirse la Confederación Andaluza de Peñas Flamencas. Esta entidad, alentada reiteradamente por la Consejería de Cultura de la Junta de Andalucía, ostenta la representación de cerca de trescientos colectivos flamencos, dentro de nuestra Comunidad autónoma, y nace como respuesta a una demanda planteada por amplios sectores de «aficionaoas». La idea, cuya cristalización ahora se estrena, se había debatido en los tres o cuatro últimos Congresos de Actividades Flamencas y, desde luego, no gozó de unánime aceptación. Hubo quienes subrayaron los peligros de una sórdida burocratización del flamenco; quienes estimaron era inadecuado «organizar» lo jondo, ya que organizar significa, en cierta medida acotar, distribuir, limitar, funciones éstas que no propician la buena salud del arte que es, ante todo, libertad. Otros, sin ignorar las antedichas amenazas pero desde una óptica más operativa, captaron la necesidad urgente de constituir un instrumento de coordinación que uniera esfuerzos y concertara voluntades. a esta última postura se adhirió, desde un principio CANDIL, y dicho posicionamiento exige ahora, aunque sólo sea sumariamente una explicación. En cualquier manifestación artística, y el flamenco lo es en grado sumo, todo tipo de intervención resulta negativo, entendiendo por tal la actividad que se encamina a señalar cauces, a fijar alguna suerte de control, a manipular en definitiva. Ello no es óbice, y en eso debe consistir una política cultural que no incurra en torpezas intervencionistas, para que se creen, se incentiven o se recuperen las condiciones objetivas óptimas de manera que el flamenco pueda desarrollar todas sus virtualidades artísticas, desde la puramente creativa hasta la simplemente contemplativa. Existen demasiados vértices extraños al flamenco, demasiadas bolsas espúreas, demasiadas degradaciones que sin pertenecer a la esencia de lo jondo y ni aun a sus aledanos, convergen en su mismo centro, lo atenazan y producen una forma de nefasto enmascaramiento. A la eliminación de estos y otros perniciosos agentes, debe tender la Confederación recien constituida. Y aún más: a generar un entorno propicio, un ambiente que contenga aquellas condiciones objetivas necesarias para que el flamenco sea todo lo que fue, todo vida, todo arte, todo libertad.\n\nRamón Porras González, abogado y presidente de la Federación de Peñas Flamencas de Jaén, ha sido elegido para el cargo similar a nivel de Andalucía. En la elección han participado las federaciones de entidades flamencas andaluzas, siendo la primera vez que se constituye una organización flamenca a nivel de la Comunidad Andaluza.\n\nRamón Porras, además de los cargos citados, es director de la revista «CANDIL» y tiene varias publicaciones, no sólo como flamencólogo, sino también como escritor y poeta. Pionero en la promoción y difusión del arte flamenco en Jaén, fue fundador de la Peña Flamenca y su presidente durante varios años.\n\nLa creación de la Federación Andaluzía de Peñas Flamencas era una necesidad que no solamente se venía demandando por los aficionados al flamenco, sino también por las propias instituciones culturales andaluzas que requerían un interlocutor para la planificación y organización de las actividades flamencas en Andalucía.\n\nEn este sentido se manifestó en Jaén el consejero de Cultura de la Junta de Andalucía, Javier Torres Vela, señalando la necesidad como un punto de partida para poder intensificar la acción política y cultural en materia flamenca, dando cauce a las reivindicaciones que este patrimonio histórico y cultural tiene planteado actualmente.\n\nPor su parte, en la estancia en Jaén la pasada semana, el director general de Música, Teatro y Cinematografía, Jesús Cantero, confirmó la desaparición del departamento de Flamenco de la Consejería de Cultura, adscribiéndose como sección al departamento de Música de la citada Dirección General. Asimismo, ratificó que al frente de la citada sección continuará el hasta ahora jefe del Departamento de Flamenco, Francisco Vallecillo.\n\nLa elección de Ramón Porras como presidente de la Confederación Andaluzia de Peñas Flamencas se ha producido en la asamblea constituyente celebrada en Antequera. Como vicepresidente fue elegido el cordobés José Arrebola Rivera, y secretario el onubense Manuel Cabezas García. También resultaron elegidos José Cantón Martínez, de Almería, tesorero; y los vocales, Ricardo Rodríguez Cosano, Francisco Prieto Tejada, José Delgado Olmos y Antonio Núñez Romero. La Confederación la integran 250 peñas flamencas andaluzas.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 686,
    "article_char_count_full": 4582,
    "article_char_count_review": 4582,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-03-5-right-la-poes-a-espa-ola-de-la-postgue",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor: José Luis Buendía López\n\nEstamos convencidos, y esperamos que lo mismo ocurra a nuestros efectos, de que no se puede hablar seriamente de los movimientos poéticos españoles que acogieron en España el tema flamenco como una de sus inquietudes básicas, sin antes examinar, siquiera sea someramente, el marco en el que aquellos se desarrollaron, y las líneas diferentes de poesía que se produjeron en nuestro país, y que hoy, habida cuenta del paso de los años, y, por consiguiente, de la necesaria perspectiva histórica, resultan fáciles de definir. Necesariamente hemos de advertir las limitaciones que, en un trabajo como éste, se nos presentan, toda vez que nuestra meta más inmediata es la percepción del tema flamenco en los versos de esa poesía postbélica, y de que, los mismos críticos,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"escuela\"]\n\nriamente hemos de advertir las limitaciones que, en un trabajo como éste, se nos presentan, toda vez que nuestra meta más inmediata es la percepción del tema flamenco en los versos de esa poesía postbélica, y de que, los mismos críticos, especializados en dicha materia, presentan frecuentes controversias sobre un mismo fenómeno artístico e ideológico, e incluso no se ponen de acuerdo a la hora de encasillar a uno o a otro autor, a esta o aquella escuela poética, en una determinada tendencia. Nosotros, por tanto, solamente presentaremos un marco general, una suscinta panorámica de la poesía española de postguerra en primer lugar, y posteriormente, de las directrices andaluzas de esos mismos temas poéticos, advirtiendo que no se trata más que de un marco general y nunca de un estudio en profundidad, ya realizado en repetidas ocasiones, con gran rigor, por especialistas que incluimos en la bibliografía de urgencia con que acompañamos el presente trabajo. Una vez que el entorno poético de postguerra esté definido en sus principales líneas maestras, procederemos a realizar algunas calas sobre cómo alguno de estos poetas han dado cabida al flamenco en sus versos, acogiéndolo como vivencia fundamental de su obra, dentro de un marco culto y con recursos poéticos lejanos a la copla; en este primer tipo de poemas el flamenco será, por tanto, solo un tema, una cala en la expresión de indudable interés. Por último, para finalizar el trabajo, incluiremos un pequeño muestreo de cómo los poetas con\n\n[ENDING CONTEXT]\n\na evolucionar, desde una poesía nostálgica de la belleza perdida, ligeramente decadente, hacia formas, más o menos novedosas, que responden a un común deseo de investigar la materialidad poética.\n\nTerminaremos, antes de ocuparnos en concreto de la realidad poética andaluza, reseñando tan sólo un hecho de difícil discusión: hoy se produce tanta y tan buena poesía en la totalidad del territorio español, que cabe hablar cá-si de renacimiento del género en las tres o cuatro últimas décadas. Hay, por tanto, parcelas suficientes de estudio para todos los investigadores inquietos por el tema.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La Poesía Española de la Postguerra: El tema flamenco (1)",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "5-8",
    "page_number": 5,
    "word_count": 3466,
    "article_char_count_full": 21536,
    "article_char_count_review": 3127,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "escuela"
      }
    ]
  },
  {
    "article_id": "1985-03-8-right-sobre-la-palabra-payo-y-otros-ex",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nS in ánimo de entablar una batalla dialéctica —siempre inútil en este difícil mundo del flamenco, donde cada uno cree tan sólo aquello que quiere creer—, me permito terciar en la réplica que Manuel Yerga Lancharro ha dado al artículo «¿por qué payo?», de Francisco Vallecillo. Al fin y al cabo fui yo quien levantó la liebre en el Congreso de Actividades Flamencas celebrado en Cáceres, a través de una conferencia, resumen de varios años de trabajo, que, por cierto, no mereció en las páginas de CANDIL más que un juicio de dos palabras: extensísima y densísima.\n\nDije en aquella ocasión: «El vocablo PAYO —por sorprendente que a algunos pueda parecer— no existe en el lenguaje caló. Prescindiendo de su sentido peyorativo, que lo tiene, es germanesco, y sirve para designar, no al gítano, sino al\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"autoridad\"]\n\nceres, a través de una conferencia, resumen de varios años de trabajo, que, por cierto, no mereció en las páginas de CANDIL más que un juicio de dos palabras: extensísima y densísima. Dije en aquella ocasión: «El vocablo PAYO —por sorprendente que a algunos pueda parecer— no existe en el lenguaje caló. Prescindiendo de su sentido peyorativo, que lo tiene, es germanesco, y sirve para designar, no al gítano, sino al pastor. Por supuesto, no tengo autoridad alguna para orientar criterios, ni tal empeño anda entre mis vocaciones, pero ya saben los que lo emplean que, cuando hablan de payos, no se están refiriendo a los «no gitanos», sino a los pastores, utilizando, además, un término despectivo que no pertenece al caló, sino al hampa carcelaria». Naturalmente, ni el uso —el·mal uso— impuesto por la costumbre, ni el hecho de que mi querido y admirado Juan de Dios Ramírez Heredia lo emplee como sinónimo de «no gitano», desvirtúan la evidencia de esta clarificación; pero está visto que, en estos temas, hay quienes prefieren persistir en el error, antes que rectificar humildemente sus esquemas de siempre, aún cuando el error —como en este caso— contribuya a desfigurar un lenguaje que, legitimado por la extraordinaria personalidad de la etnia, deberíamos conservar en toda su pureza, sin las perjudiciales adherencias de la jerga bergante. Desde luego estoy de acuerdo con Yerga Lancharro cuando afirma que la inmensa mayoría utiliza el vocablo «PAYO» como equivalente a «NO GITANO», pero esto no debe ser obstáculo para que, una vez desvelado el error, no insistamos en él, sobre todo, si al insistir, atribuimos al acervo de un grupo étnico específico perfectamente respetable (los gitanos), aquello que pertenec\n\n[ENDING CONTEXT]\n\nno como apelativo caprichoso, sino como adjetivación peyorativa con la que el individuo antisocial —obligado a la trashumancia por eludir la justicia— designa al sedentario.\n\nFinalmente, pienso que la propuesta de llamar «gachó» o «garrochí» al no gitano carece de sentido, como carecería de él un vocablo extraño al de nuestra lengua común para designar al «no agote», «no chueta», «no maragato» o «no pasiego». Nombre: José Georgio Soto Nombre artístico: José el de la Tomasa Fecha de nacimiento: 19 de agosto de 1951 en Sevilla Domicilio actual: C/. Macarena, 3, Huertas-Blq. 19-7.º, Sevilla\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sobre la palabra «Payo» y otros excesos",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1140,
    "article_char_count_full": 6917,
    "article_char_count_review": 3352,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "autoridad"
      }
    ]
  },
  {
    "article_id": "1985-03-9-right-ellos-los-protagonistas-dicen-jo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nYo de jovencito hacía los cantes de mi familia, de mi abuelo, de mi padre. Un día mis compañeros de trabajo —porque mi profesión es tapicero— me animaron para que me presentara a un concurso, así lo hice y, desde entonces aquí me tenéis, metido de lleno en el flamenco\n\n—¿Conociste a tu abuelo? ¿Cómo cantaba?\n\n—Para mí, PEPE TORRE fue un gran cantar, lo que yo le he escuchao a mí me encanta, lo que pasa es que tenía su hermano que era MANUEL TORRE, que era un genio, y claro, mi abuelo no pudo salir a la luz por tener el hermano que tenía. No obstante, tiene cosas grabadas que ahí están, como son los cantes de «El Planeta» y de «Frijones», creo que los hizo con mucha dignidad.\n\n—¿Y por la familia de tu padre, qué artistas ha habido?\n\n—Pues, francamente, no lo sé. Porque mi padre es de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"recuerdas\"]\n\nque era un genio, y claro, mi abuelo no pudo salir a la luz por tener el hermano que tenía. No obstante, tiene cosas grabadas que ahí están, como son los cantes de «El Planeta» y de «Frijones», creo que los hizo con mucha dignidad. —¿Y por la familia de tu padre, qué artistas ha habido? —Pues, francamente, no lo sé. Porque mi padre es de origen italiano, desde luego mi padre canta flamenco; me gustaría saber quienes eran sus antepasados. —¿Tú recuerdas haber oído hablar a tu abuelo de su hermano Manuel —Sí, mi familia toda hablaba de MANUEL TORRE, principalmente mi abuelo, para ellos MANUEL era como un dios, siempre que conversaban sobre él lo hacían con verdadera devoción. —Entonces, ¿tú te habrás criado en un continuo ambiente flamenco? —Sí, pero no en un ambiente de cantar to los días. Siempre era cuando había una reunión familiar, con motivo de una boda, un bautizo, etc., entonces se cantaba, y yo he ido asimilando los cantes de mi familia; hasta que, como ya he dicho antes, un día salí cantando en un concurso y hasta hoy. —¿Qué concurso fue este? —Bueno, en realidad el primer concurso que me pres\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_04 | trigger=\"comercialidad\"]\n\nque salir de tu tierra, te encuentras con un técnico que es de Zaragoza —con todo el respeto para ese señor de Zaragoza— estás bebiendo un vino que no es de tu tierra, y estas pensando en el avión pa volverte a Sevilla otra vez. Esto es una cosa mu fría, por eso, una grabación es muy difícil que refleje la verdadera talla de un artista. —Nosotros nos referimos a otra clase de muralla, por ejemplo, si la casa grabadora te exige que metas algo de comercialidad en tu cante. —No, a mí nunca me ha exigido que meta nada comercial, al revés. Después de haber grabado todos mis discos me lo propusieron una vez y yo dije que no. Querían que cantara siguiriya y soleá y ellos le ponían una música pop de fondo; lógicamente yo me segué, diciendo que la soleá y la siguiriya era una cosa más seria que todo eso. —Hay una grabación tuya que nos gustó mucho, que si no recuerdo mal se titula «Véreas Negras». En este\n\n[ENDING CONTEXT]\n\nEn el cante de Manuel hay una transmisión entre lo que hace y lo que yo percibo; con mi abuelo esto no ocurre.\n\n—José, para terminar, suponemos que tendrás algunas anécdotas que contarnos.\n\nBueno, sí que me ha ocurrido varias. Una de ellas, a vosotros os puede hacer gracia, pero a mí no me hizo ninguna. Resulta que una vez fui a cantar a Almería y después de catorce horas de coche Rafael Mendiola y yo llegamos y cuando nos presentamos me dijeron: ipero si tú cuando tienes que venir a cantar es el más que viene! total, que nos fuimos a la Peña y me puse a cantar y salió mejor que cobrando.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas dicen: José de la Tomasa",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "9-11",
    "page_number": 9,
    "word_count": 2943,
    "article_char_count_full": 16035,
    "article_char_count_review": 3724,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "recuerdas"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "comercialidad"
      }
    ]
  },
  {
    "article_id": "1985-03-12-left-letras-flamencas-de-j-m-rquez-ca",
    "article_text_for_review": "Soleás y Seguiriγas\n\nDe tóas tus esas cosillas, por la que vivo amargao, es que vayas y que vengas del uno al otro colmao.\n\nPor qué no ve el ser humano que con lo que emplea en armas nadaría el mundo en grano, regüeldos, sonrisa y calma?\n\nPa soleá jonda y grande de las que parten el alma, la que por tí me desvive corriendo mis entrañas.\n\nSi más estoy sin tu habla más secos veo mis ojos, sin ver, de tanto llorá. Te quiera o no quiera, que venga o que voy qué sacará mala lengua mintiendo los pasos que doy. Ni al sueño le cuento que sueño contigo; que toito el tiempo lo paso llorando de muerte en martirio.",
    "title": "Letras flamencas de J. Márquez Cabello",
    "periodical": "candil",
    "issue_id": "1985-03",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 121,
    "article_char_count_full": 610,
    "article_char_count_review": 610,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
