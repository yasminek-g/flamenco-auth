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
    "article_id": "1990-01-19-right-el-solitario-int-rprete-sui-g-ne",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOpinión\n\nS. A. García de Cuarto\n\nE 1 título de este pergeño bien aclara, creemos, que nuestra intención no ha de deslizarse por la encrucijada que lleva a tratar, otra vez más, del inefable Estébanez Calderón como un oportuno aperturista de la historiografía flamenca.\n\nAparte de relator de las andanzas que tan propicias resultaron a la puesta en marcha de la historia del flamenco, fue Don Serafín aficionado práctico y, por ello, fidedignas muestras de cantar exhibió a presencia de amistades íntimas.\n\nLa faceta cantaora del inquieto costumbrista —malacitano él— si no desconocida al absoluto, pese a aparecer en algunos libros, está bastante olvidada.\n\nSi no trascendente respecto a contenido histórico, por supuesto; la cosa no llevará la historia del flamenco más allá de la fecha de la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"título\"]\n\ntá bastante olvidada. Si no trascendente respecto a contenido histórico, por supuesto; la cosa no llevará la historia del flamenco más allá de la fecha de la publicación de las Cartas marruecas de Cadalso ni de la existencia de Tío Luis el de la Juliana sí, al menos, curioso nos parece todo eso; y ya que de cante va el aserto y porque la noticia, si con amplitud menor, se menciona en otro libro que, específicamente ataña a la tal materia, aún a título de eutrapelia flamenca, nos decidimos a airearla con el detalle fiel que la documenta. Arcadio Larrea Palacín (El flamenco en su raíz, página 148, epígrafe: ¿Cómo hemos de entender al Solitario?) dice textualmente: «...Luis González Gallardo, en una carta de 1842, decía de Estébanez Calderón que estaba siempre dispuesto a hacer “el chiste de cantar la caña” con aquella sandungay aquel escupir de majo que le entraron en el cuerpo, con la crisma y la sal del bautismo» «y Cánovas del Castillo (sobrino del autor de las “requeteestrujadas” “Escenas Andaluzas”, decimos nosotros) daba esta imagen: “y ejercitadísimo en los donaires, bizarrias, bailes, cantos y chanzas de su tierra”. (En todo y por todo, andaluz castizo)». Larrea no miente al hacer señalamiento de la actitud cantaora de «El Solitario», mas da en error al referirse a la persona que firma la carta de 1842. No era ella el don Luis González Gallardo que menciona, sino otra con idéntico nombre, pero apellidado González Bravo, es decir, el fogoso político español (gaditano por más señas), liberal en principio, y en periclitación al moderantismo en las postrimerías de su carrera. Que Larrea Palacín había de conocer el contenido de la epístola aquélla, cosa es de aceptación factible; ahora bien, a lo qué parece, en su manejo como material propicio a las intenciones propuestas, extremó la cicatería; pues, situando al margen el error que antes se hace notar —en concinción estamos de que el duendecillo que lanza a voleo las erratas ronda en pareja con el subconsciente— con la tan sucinta parte que de ella transcribe, pone en menoscabo una revelación que —aún sin rango de transcendencia histórica fundamental— sí aporta curiosidad anecdótica ligada a la personalidad de quien quería ilustrar prácticamente sus apuntaciones sobre el tema. Don Serafín, al par que la literatura y otras cosas importantes, cultivaba la política y era beneficiario de la amistad de don Luis González Bravo. Amigo en situación no exenta de intimidad, según pone de manifiesto la misiva a la que muy pronto daremos espacio aquí. En la Bibliografía Flamenca acopiada p\n\n[ENDING CONTEXT]\n\nno fuese capaz de haber copiado del propio Fillo los sones de la caña; y hasta que los expresara con voz «afillá», en pro de la mejor exposición de la estampa. ¡Cosas vercedes, mío Cid!\n\nBien podría cerrarse así esta solazosa cata flamenca, pero, sensibilizada nuestra curiosidad al máximo, tratemos de complacerla agregando a los que hizo Mitjana otro interrogante.\n\n¿Despacharía don Serafín la caña a palo seco, o acompañándose él mismo con la guitarra, según años después (en las jaberas, verdiales y demás) lo hacía su contemporáneo Juan «el Breva»?\n\nDoctor Arroyo, 12 / Teléfono 21 00 58 / JAEN\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«El Solitario», intérprete sui generis de la caña",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1147,
    "article_char_count_full": 6899,
    "article_char_count_review": 4196,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "título"
      }
    ]
  },
  {
    "article_id": "1990-01-20-right-bibliograf-a",
    "article_text_for_review": "S inceramente, sentimos no poder compartir el entusiasmo de José Antonio Hernández Guerrero, prologuista de la presente edición, acerca de la personalidad literaria de José Carlos de Luna y del valor de la obra que hoy nos ocupa. En efecto, la obra poética y crítica del autor nos parece frívola y carente de importancia, aquejada en todo caso del peor de los costumbrismos, el que intenta describir lo que no se conoce en profundidad, el que se aproxima a la superficie colorista de las cosas, llevado por sus peculiaridades más o menos atrayentes, pero que en absoluto se detiene a penetrar esas apariencias a la búsqueda de la necesaria profundidad que justifique plenamente esos sondeos que se intentan realizar sobre determinadas parcelas del ser andaluz o de las señas de identidad que lo constituye.\n\nEn cuanto al libro «Gitanos de la Bética», que el prologuista considera: «un libro que aún está vivo», e incluso: «Capítulo importante de la antropología marginal y fronteriza», no deja de ser un confuso y amalgamado centón que su autor, nacido en Málaga en 1890 y muerto en Madrid en 1964, construye, no desde la aplicación de métodos etnológicos ni la utilización de una seria y rigurosa bibliografía sobre el tema, sino desde el refrito de unas apreciaciones más o menos discutibles de visitantes ocasionales que se entusiasmaron con el tema gitano, sobre todo el popular George Borrow, e incluso utilizando los escritos de los tradicionales enemigos y perseguidores implacables de los gitanos a lo largo de la historia, como lo fuera Sancho de Moncada, del que José\n\nAunque no quepa en el papel... José Luis Buendía López\n\nCarlos de Luna intenta distanciarse con una supuesta mentalidad abierta y comprensiva, para después, a lo largo del libro, retomar, de forma más civilizada, pero igualmente intolerable, muchos de los tópicos e infundios que aquel burdo doctor orquestara en contra de esta raza; así, el autor de «Gitanos de la Bética» califica de «tozudez» el deseo gitano de mantener su independencia (página 87) o en el capítulo IV, al describir el aspecto de éstos desde una supuesta postura científica, desgrana perlas como las que citamos a continuación: «Como en todos los seres de inteligencia poco cultivada, la suspicacia y el egoísmo frenaron los sentimientos nobles y generosos...». «La fisonomía de los gitanos trasluce el orgullo, la astucia y el servilismo, de manera desagradable que no consiguen disimular...». «La alegría es para ellos un sentimiento forzado, y no saben aparentarla con la sonrisa, siempre dura y despectiva; la buscan en el aturdimiento, pero nunca la sienten en el alma».\n\nNo creo preciso continuar; nos gustaría conocer el lugar desde el que don José Carlos (orondo gobernador civil de Sevilla y Badajoz, por cierto ciudades abundantes en gitanos) conoció el mundo de éstos, en qué momento aprendió a ver los tan tristes, traicioneros, egoístas y serviles. Nosotros, por nuestra parte, pensamos que solamente presenció tan de-solador espectáculo desde el atácico sentimiento racista que lo acompañó siempre, por más que él intentara disfra-zarlo de deportivo interés hacia las señas particulares de los pueblos. Señores investigadores, está bien que se resuciten tex-tos y autores antiguos u olvidados, pero no para magnificarlos en exceso ni ver-ter sobre ellos el incensario de inútiles reivindicaciones. El presente libro, que no dudamos pueda ser oportuno, es una muestra de cómo ciertos intelectuales se aproximan a temas que no conocen y, en ocasiones, ni siquiera les importan; sus observaciones sobre las costumbres, canciones, músicas, atuendos o rasgos físicos y psíquicos de los gitanos, no son más que el reflejo de lo que una parte importante de la sociedad dominante ha pensado, piensa aún, sobre ellos. Ese es el valor del libro, por cierto tan espléndidamente editado como todos los de este fondo editorial de la Universidad Gitana: el darnos una muestra significativa de la futilidad de algunos juicios, y, ¿cómo no?, de lo es-peso y espúreo de algunas ideologías.",
    "title": "Bibliografía",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 652,
    "article_char_count_full": 4036,
    "article_char_count_review": 4036,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-01-21-left-poemas-paco-arana",
    "article_text_for_review": "Pastora, Llave del Cante\n\n¡Pastora! ¡Cómo me suena ese nombre a flamenca y a señora! Decir Pastora es decir: La flor de las cantaoras. Ni la Andonda ni la Trini, ni la Rubia ni la Lola, ni Antonia la de San Roque ni Anilla la de Ronda... No hubo cante por derecho ni mujer encantadora, ni gitana tan juncal, ni aurora como su aurora. Ni corazón generoso que se fue quemando a solas en la alegría del cante y en la pena de la copla. Sus tangos peines de azúcar, granaínas... Pena mora, lorqueñas de Federico, alboreás (pa) las bodas, bulerías (pa) las fiestas y siguiriyas que lloran. Bamberas lucen enaguas, cantiñas bañan las olas, saeta de los balcones Semana Santa te añora. Soleares, peteneras, cantes de duende y alcoba; cada cual en su momento, en su sitio y a su hora; no hubo ni habrá en la vida quien iguale a esta señora, en los cantes de la tierra ni en los cantes de la gloria. Y así que pasen cien años la afición la rememora, que no ha nacido en el mundo, juncal, gitana y señora que diga tan bien el cante como lo dijo Pastora.\n\nPaco Arana\n\nCruz de la Saeta\n\nLa voz de la saeta cruz de aire que suena sola entre dos luces como una queja bravía de una memoria que canta. La noche de madera anima de guitarra alumbrada por la boca. Suenan a martillo los clavos igual que un cante de adentro unos tercios de olas nazarenas elevan la madrugada de la fragua al martinete. La ceremonia de la cera lagrimea las pisadas lentas y las ojeras abiertas retratan una vía de amargura en un cielo penoso que no quiere a sus estrellas. ¡Qué descalza va la penitencia por las calles de la vida! Entre una riada de flores en balcón de ninguna parte una mujer de sombras con son de orfebrería cierra los ojos y canta con los tambores del alma. La saeta de la voz cruz de sal que llora sola sola.\n\nJesús Cuesta Arana",
    "title": "Poemas Paco Arana",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 344,
    "article_char_count_full": 1811,
    "article_char_count_review": 1811,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-01-22-left-noticiario",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\na Fundación Andaluzas de Flamenco celebró, el pasado 1 de febrero, una reunión de su Consejo Rector. Como se recordará, la Fundación tiene cuatro patronos fundadores: La Consejería de Cultura de la Junta de Andalucía, la Diputación Provincial de Cádiz, el Ayuntamiento de Jerez y la Caja de Ahorros de Jerez.\n\nEn dicha reunión, que estuvo presidida por el consejero de Cultura y presidente de la Fundación, don Javier Torres Vela, se aprobó el Presupuesto Ordinaryo para 1990, que ascienda a 60.000.000 de pesetas, representando una subida del 40% sobre el Presupuesto Ordinaryo de 1989, que fue de 43.500.000 pesetas. El actual presupuesto servirá para consolidar por una parte el Centro de Documentación que esta Fundación mantiene abierto al público, con servicios de videoteca, fonoteca y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"acceso\"]\n\nr Torres Vela, se aprobó el Presupuesto Ordinaryo para 1990, que ascienda a 60.000.000 de pesetas, representando una subida del 40% sobre el Presupuesto Ordinaryo de 1989, que fue de 43.500.000 pesetas. El actual presupuesto servirá para consolidar por una parte el Centro de Documentación que esta Fundación mantiene abierto al público, con servicios de videoteca, fonoteca y biblioteca especializados en flamenco, que constituyen el único punto de acceso al público en nuestro país para cualquier interesado en el Arte Flamenco. De otra parte, el presupuesto aprobado permitirá el desarrollo de múltiples actividades de promoción del flamenco, entre las que podemos destacar las siguientes: * Continuación de su «Biblioteca de Estudios Flamencos», colección de libros de estudio sobre diversos aspectos del flamenco, del que saldrán 3 títulos durante 1990, correspondientes a los títulos premiados en nuestro I Premio de Investigación. * Patrocinio del Congreso Naci\n\n[ENDING CONTEXT]\n\nde concurso. Asimismo, 50.000 pesetas como gratificación para los cantaores que les acompañen.\n\n9. Se establece un único premio indivisible, que no podrá ser declarado desierto, dotado con 500.000 pesetas y diploma. 10. Todos los finalistas recibirán un diploma que les acredite como tales.\n\n11. Las deliberaciones de los jurados serán secretas, tanto en la fase de selección como en la final. No se harán públicos los nombres de los inscritos ni seleccionados.\n\n12. El fallo y entrega del premio se efectuará la última noche de concurso, en el mismo lugar y tras la actuación del artista invitado.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 1563,
    "article_char_count_full": 9659,
    "article_char_count_review": 2588,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "acceso"
      }
    ]
  },
  {
    "article_id": "1990-01-23-left-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOrganizado por la Hermandad de la Vera-Cruz, y con el patrocinio de la Fundación de Cultura del Ayuntamiento de Osuna (Sevilla), ha sido convocado el Cuarto Concurso de Saetas «Carmen Torres», cuya final tendrá lugar el día 31 de marzo de 1990, a las 20 horas en la Iglesia Colegiata de Santa María de la Asunción.\n\nXI Concurso de Cante y Baile Flamenco de San Pedro de Alcántara\n\nCuarto Concurso de Saetas «Carmen Torres», en Osuna\n\nPara esta edición han sido establecidos cinco premios, siendo el primero de 200.000 pesetas, así como tres premios de 25.000 pesetas para concursantes locales que no puedan optar a premio superior.\n\nOrganizado por la Peña Flamenca de San Pedro de Alcántara (Málaga), con la colaboración de la Comisión de Fiestas del Ayuntamiento, ha sido convocado el XI Concurso\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nuedan optar a premio superior. Organizado por la Peña Flamenca de San Pedro de Alcántara (Málaga), con la colaboración de la Comisión de Fiestas del Ayuntamiento, ha sido convocado el XI Concurso de Cante y Baile Flamenco. Los interesados podrán inscribirse en el Ayuntamiento de Osuna, o llamando al teléfono 4810050. Los interesados podrán inscribirse, hasta el día 23 de marzo de 1990, llamando a los teléfonos 783117, 784783 y 780187, o en la Peña Flamenca, calle Palangre, núm. 16, San Pedro de Alcántara (Má-laga). Para el cante han sido establecidos cuatro premios, siendo el primero de 175.000 pesetas y para el baile tres, siendo el primero de 200.000 pesetas. En asamblea general ordinaria, celebrada por la Peña Flamenca de Jaén, el día 28 de diciembre pasado, fue reelegido presidente Vicente Morales Güeto. Morales Güeto había agotado su mandato estatutario de dos años, y, al no presentarse otro candidato, fue reelegido por unanimidad como reconocimiento a la labor desarrollada en el bienio anterior. El presidente, a su vez, nombró nueva junta directiva, la cual quedó compuesta de la forma siguiente: Vicente Morales Güeto, reelegido presidente de la Peña Flamenca de Jaén Presidente: Vicente Morales Güeto. Vicepresidente: Pedro Sánchez Ortega. Secretario: Leovigildo Francisco Aguilar Burgos. Vicesecretario: Juan Cruz Barranco. Relaciones públicas: Tomás Ortiz Ibáñez. Vocal de prensa: Rafael\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_04 | trigger=\"Segunda\"]\n\núblicas: Mariano Martínez Elías. Vocal mantenimiento: Leandro García Regaña. Concurso de Cante Flamenco «Mirando a la Torre» Peña Flamenca «Torre del Cante», de Alhaurín de la Torre (Málaga) Vocal delegado de barra: Teodoro de los Santos Zafra. Bases: Primera: Podrán tomar parte en este concurso cuantos cantaores profesionales o aficionados, de uno u otro sexo, lo deseen y realicen la inscripción en las condiciones que aquí se establecen. Segunda: La inscripción puede hacerse personalmente, llamando a los teléfonos 410332, 411044 y 410829, o por escrito dirigido a la Peña Flamenca «Torre del Cante», calle Viñas, 11, de Alhaurín de la Torre. En todo caso harán constar su nombre y apellidos, nombre artístico, dirección y teléfono. El plazo de inscripción quedará cerrado el día 31 de marzo de 1990. A los concursantes se les comunicará, con la debida antelación, la fecha de actuación para la fase clasificatoria. Si el participante manifiesta con tiempo suficiente la imposibilidad de presentarse en la fecha fijada, la organización determinará una fecha alternativa que será inalterable. Cuarta: La organización pondrá a disposición del cantaor un guitarrista profesional. No obstante podrá venir acompañado de su propio guitarrista. Quinta: Tercera: Para optar a cualquiera de los premios, el concursante deberá realizar un cante de cada uno de los siguientes grupos: $ \\tilde{S} $ Soleares, Cantiñas (Alegrías, Romeras, Mirabrás, Caracolés), Tientos, Tangos y Builerías. C: Malagueñas, Granaínas, Cantes de Levante, Peteneras y Fandangos. Grupo D: Un cante de libre elección. Sexta: El jurado, designado por la organización, estará formado por conocedores del cante y su fallo será inapelable. Séptima: Para mayor difusión de este concurso, la organización se reserva el derecho de grabar las interpretaciones de los cantaores en las distintas fases del mismo. En su momento se comunicará a los concursantes su pase o no a la final. Octava\n\n[ENDING CONTEXT]\n\ny de propuesta o propuestas a debatir y aprobar.\n\nAunque seguiremos en contacto y próximamente recibirás nueva información sobre: hoteles, precios, inscripciones, etc., quiero desde hoy animarte a la realización y envío de esa ponencia que seguro ya tienes pensada y que te recuerdo deberá estar en la Secretaría de la Organización del XVIII Congreso en: IFEBA, Apartado 253 - 06080 BADAJOZ, a ser posible como ideal el 15 de mayo de 1990 y como fecha límite el 31 de mayo de 1990, mientras tanto recibe un cordial y flamenco saludo.—El Director del Congreso, Francisco Zambrano Vázquez.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 1976,
    "article_char_count_full": 12879,
    "article_char_count_review": 5045,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "Segunda"
      }
    ]
  }
]
```
