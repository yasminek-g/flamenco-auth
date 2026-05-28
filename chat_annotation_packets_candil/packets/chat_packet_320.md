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
    "article_id": "1996-05-29-right-acotaciones-al-mundo-de-fausto-o",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMiguel Viribay Abad\n\nEl Consejo de Redacción de la Revista Candil me pide una colaboración para el número monográfico que la prestigiada publicación dedica a Fausto Olivares (Jaén, 1940-1995). Sólido artista que contribuyó, de manera notable, en el desarrollo del flamenco jaenés. Con el desaparecido Angel García Cruz y los flamencólogos José Solís Rostáing, Angel Fernández Cos y Fernando Perez Mesa, fue persona destacada en la creación de la Peña Flamenca; hoy, de referencia obligada para quienes desean conocer la andadura sociológica del flamenco en Andalucía. Sus orígenes están ligados a la reunión que el grupo de amigos mantenía en un conocido bar de Jaén. Ello justifica la publicación de este número al que me sumo con admiración al compañero desaparecido y el reconocimiento para\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"logrado\"]\n\nng, Angel Fernández Cos y Fernando Perez Mesa, fue persona destacada en la creación de la Peña Flamenca; hoy, de referencia obligada para quienes desean conocer la andadura sociológica del flamenco en Andalucía. Sus orígenes están ligados a la reunión que el grupo de amigos mantenía en un conocido bar de Jaén. Ello justifica la publicación de este número al que me sumo con admiración al compañero desaparecido y el reconocimiento para quienes han logrado que la minoritaria y cerrada tertulia de entonces, sea hoy paraíso abierto a cuantos desean participar, en una u otra medida, de la grandeza que habita el flamenco. Fausto descubrió su interés por este aspecto de la cultura española a través de su condiscípulo Darío Villalva —afamado pintor y flamencólogo— durante sus estudios en la madrileña Escuela de Bellas Artes de San Fernando. Conviene señalar también que, además de la significada vertiente flamenca, la obra de Fausto Olivares abarca un cosmos popular que pertenece, por derecho de sangre, a la periferia de lo español; en cuyo núcleo central reside el mágico universo flamenco. Contemplada así la figura del artista jaenés puede ser analizada en el marco que le corresponde. Otra cosa limita su horizonte de manera sensible, y, probablement\n\n[ENDING CONTEXT]\n\nleve iconicidad.\n\nSu último viraje temático corresponde al mundo del carnaval, expresado en superficies de reducidos formatos (cartulinas de 22 x 32 cm.) y rutilantes de color. ¿Se había apartado de sus temas flamencos? La respuesta adecuada se la llevó el pintor. De cualquier manera, este mundo expresado con el lenguaje habitual del artista, que tema y saturación de colores hacen diferente, también corresponde a una concepción de raíces festivas y populares; en este caso, impregnada de un venecianismo que, a mis ojos, incluye el guiño y evita lo doliente de ancestrales conceptos de carnaval.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Acotaciones al mundo de Fausto Olivares",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "29-31",
    "page_number": 29,
    "word_count": 1493,
    "article_char_count_full": 9272,
    "article_char_count_review": 2884,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "logrado"
      }
    ]
  },
  {
    "article_id": "1996-05-31-right-el-arte-de-fausto-olivares",
    "article_text_for_review": "Hace ya más de veinte años que conocí a Fausto y a su obra, recién llegado entonces a Jaén. Me sorprendió desde el primer momento aquella pintura que rezumaba modernidad, en sintonía con corrientes y movimientos artísticos europeos que, a comienzos de los 70, conocíamos más por revistas especializadas que por la difusión de los audiovisuales y no digamos por la directa contemplación en exposiciones. Lo “Moderno”, y mucho más por estas latitudes sureñas, era un extraño gusto que tenía —tampoco voy a negarlo— un placer de iniciado, elitista, aunque desde la perspectiva del gusto oficial, o tal vez oficioso, era sinónimo de rebeldía; de heterodoxia, en el más amplio sentido.\n\nVeía, y aún me sigue pareciendo, en el arte de Fausto Olivares, algo de aquella “babosa huella” a los Francis Bacon, con que solía auto-contemplarse el pintor inglés. Los rostros y las figuras grotescas que se de-forman en su propio movimiento al\n\n“Beni de Cádiz” por Fausto Olivares\n\nFausto Olivares «Zapateado» Oleo, 65×50, 1986\n\nroce con el aire, como si de una masa amorfa se tratara; esa inconsistencia del ser humano sometido a las constantes e imprevisibles modificaciones del ambiente, me evocaban la \"náusea\" existencialista en que se desenvolvió L'art autre parisino de los años 50, si bien de tendencia más informalista y antifigurativa que el arte de Olivares. Pero todo eso lo percibía sabiendo, bien es cierto, de su conocimiento y vivencia del mundo artístico francés, mas también percatándome de una peculiar manera de afrontar los temas, que no podía entenderse sin separarla de una sólida y personal forma de ver y en consecuencia de pintar. Sus extrañas figuras humanas; sus preocupaciones por los problemas de la materia o los misteriosos e irreales tonos pictóricos podían verse, sí, como un compartir ideas de la modernidad imperante, sin embargo el acento era propio.\n\nDecir que esa originalidad venía de una tradición hispana proclive a expresionismos no sería sino caer en el tópico de la veta brava española, que tanto placer causara a los críticos extranjeros de este siglo siempre que afrontan al pintor español en relación con el arte de vanguardia. Existe, evidentemente, ese rasgo peninsular en Fausto, pero no desde un punto de vista del tremendismo, no por sentido menos válido, sino por la asunción del drama del vivir en clave de silencioso sufrir. Algo que pocas manifestaciones como el flamenco han sabido expresar en íntima unión con lo que\n\nes la visión de la existencia en Andalucía. Silencio y concentración de impulsos que ha dado un timbre de elocuente elegancia a estas tierras de la Alta Andalucía, pero que se desborda en un momento en la más fulgurante explosión de sentimiento. Es también la grandeza de nuestro barroco. A esa tradición pertenece Fausto Olivares.",
    "title": "El Arte de Fausto Olivares",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "31-33",
    "page_number": 31,
    "word_count": 457,
    "article_char_count_full": 2793,
    "article_char_count_review": 2793,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-05-33-right-poema-a-fausto-olivares",
    "article_text_for_review": "Fausto Olivares\n\nRamón Porras González La color de lo jondo, la color, como un sueño relámpago que estalla, como algo desangrarse que no acalla, bajo la herida, un lúdico clamor.\n\nEs cante manuscrito, en el temblor de los rojos que corta la cizalla del tercio siguiriya, sobre el que hallarazón para tan mágico estertor.\n\nUn delirio violeta, exuberantes senos, besos y muslos palpitantes conforman este Rorscharch de jonduras,\n\neste lienzo Terpsícore, color que desentraña el grito y lo depura, la color de lo jondo, la color...",
    "title": "Poema a Fausto Olivares",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "33-34",
    "page_number": 33,
    "word_count": 86,
    "article_char_count_full": 528,
    "article_char_count_review": 528,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-05-34-right-poema",
    "article_text_for_review": "Poema\n\nYo me apoyo en el silencio de las cosas que recuerdo, fragmentos de las ternuras que me quitaron el sueño.\n\nLos recuerdos que recuerdo son de llanto y amargura, de este dolor tan dolido he perdido mi figura.\n\nUna mañana de mayo fui a coger rosas al huerto, y vi un pájaro sin alas que al no volar había muerto.\n\nTú no sabes la agonía que mi corazón ha pasado, con suspiros angustiosos mi alma se ha desgarrado.\n\nAquellas verdes esperanzas cómo se desvanecieron, cayeron las altas torres como si fueran un sueño.\n\nPara qué quieres vivir, para qué quieres soñar, si la vida desaparece como las olas del mar.",
    "title": "Poema",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "34-34",
    "page_number": 34,
    "word_count": 111,
    "article_char_count_full": 612,
    "article_char_count_review": 612,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-05-35-left-fausto-eterno-candil",
    "article_text_for_review": "Fausto, eterno candil\n\n¡DULCE.! como verdial tramontano que unirse quisiera con jazmines, luceros y rosáceos amaneceres.\n\n¡PROFUNDO! como siguiriya llorosa que va rumiando soledades o bebiendo cálices de insondables amarguras.\n\n¡SONRIENTE..! como noble abanico de polícromas bulerías que hacen bailar a la noche vestida de lunares. ¡Que no se apaguen los candiles!\n\n¡Más aceite de Jaén para ese candil!\n\n¡Antes que al hombre y la mujer hizo Dios el candil para vencer a la tristeza!\n\ncomo petenera que va pisándose los harapos, lamentos y quejas por las esquinas desafortunadas. ¡Oídme, mis candiles: Seguid con reverberos, que la oscuridad son los negros bajíos pellizcándome los tejidos!\n\n¡TIERNO.! como nana hecha cante de natos vagidos, dándole gracias a la Vida que acaba de iniciarse. ¡Y la luz candilera se hizo ascua para calentar a los inicuos sentimientos!\n\n¡SENTIMENTAL..! como Tango del Piyayo, que no hay mayores duquelas que la feroz hambruna de una prole con carencias y su deber es alimentarlas.\n\n¡COSMOPOLITA.! como canto de ida y vuelta, que se fue mú honrao y ha vuelto, orgulloso, con diadema floreada y ritmos de cañaduz. ¡Alumbra, candilito, que semos mú probes y no tenemos lú artifisiá!\n\n¡FAUSTO!: del Flamenco,\n\nun sitio preferencial en el cielo. ¡Candil: presta tu luz a todos los mares!\n\n¡FAUSTO!: allí ¡ya! donde alumbran los eternos candiles, y quién sabe si un Divé se arranca por deblas.\n\n¡FAUSTO!\n\nTú eres,\n\n¡FAUSTO!\n\nla Serrana bebiéndose\n\nlos vientos de Sierra Morena. ¡Sí, FAUSTO!:\n\nTú eres el CANDIL\n\nque nos sigue alumbrando\n\nen esta vida de ausencias!\n\n¡Tu ausencia!",
    "title": "Fausto, eterno candil",
    "periodical": "candil",
    "issue_id": "1996-05",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "35-35",
    "page_number": 35,
    "word_count": 257,
    "article_char_count_full": 1604,
    "article_char_count_review": 1604,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
