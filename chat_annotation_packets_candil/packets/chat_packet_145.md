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
    "article_id": "1987-01-21-right-iscografia-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFICHA TECNICA\n\nTítulo:...de azabache Canta: Manuel de Paula Tocan: Manolo Franco y Manuel de Palma Palmas y coros: Familia Fernández. Referencia: PSD-5020-Pasarela\n\nEn corto espacio de tiempo se viene minusvalorando el compás con el cante como si lo uno no estuviese entrañablemente ligado a lo otro. El crítico, al que se le ve el plumero en las fiestas, le otorga un valor secundario. El cantaor que carece de él, cuando acomete cantes básicos, se «atraviesa» ostensiblemente y procura no «meter las manos» porque queda al descubierto. Así se explica la poca importancia que ambos (no doy la lista porque agotaría el papel) conceden al compás. Manuel Martín Martín\n\nDicen que «eso», como todo en la vida, se aprende. Imagino que lo aprenderán en los discos. Yo sé de muchos, bastantes, que se\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"Según\"]\n\nMartín Dicen que «eso», como todo en la vida, se aprende. Imagino que lo aprenderán en los discos. Yo sé de muchos, bastantes, que se despidieron con más se setenta años y no pudieron lograrlo, o de otros, en activo, que se morirán con esa pena. Ahora, con el estudio afanoso y la avanzada tecnología, es muy posible que el milagro ocurra y aprendan a redoblar las palmas por sevillanas, pero a buen seguro que continuarán «descuadrando» el cante. Según he podido constatar, buena parte de los guitarristas consagrados, la inmensa mayoría de los cantaores dotados de tal privilegio y de los aficionados viejos, opinan que «eso» se aprende en el hogar familiar, desde pequeñito, y escuchando cantar por soleá, «ar gorpe», tangos, bulerías, romances, alboreás, etc., y no acunado al aire de las canciones populares. En fin, lo cierto y verdad es que son opiniones contrapuestas y algunas bastantes divertidas, que se producen a diario en este huerto sin guarda, donde los más «listillos» muestran su poder de convicción hablando de Manuel Torre o de Chacón como si hubiesen estado toda su vida juntos en una juerga. Pero mejor será ir al grano, volver a deleitarme con las notas morenas y acompasadas de este «...de azabache», e intentaré no caer en la tentación de los típicos tópicos que tanto disgustan a Antonio Zapata, eminente jurado del II Giraldillo del Cante. Hablar de compás, en el caso que nos ocupa, y pronunciar el nombre de Manuel de Paula es una misma cosa. «Eso», que despectivamente llaman los que no lo tienen, no guarda secretos para él. Y buena prueba de\n\n[ENDING CONTEXT]\n\ncircunscrito a grabar los estilos como son, sin arabescos, recogiendo bien el ritmo e imprimiéndoles la alegría propia de los mismos.\n\n¡Que su voz es la adecuada para estos estilos! A veces deberíamos obviar las matizaciones sobre las voces, porque la historia del Flamenco está repleta de ejemplos que han plasmado la calidad de los intérpretes con voces adecuadas o menos adecuadas.\n\nBuen acompañamiento en el toque desarrollado por Enrique de Melchor, que acerca con frescura al cantaor al desarrollo de los cantes. Falsetas donde son necesarias y sobriedad y simpleza en los momentos adecuados.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1391,
    "article_char_count_full": 8181,
    "article_char_count_review": 3195,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "Según"
      }
    ]
  },
  {
    "article_id": "1987-01-22-right-ablan-las-pe-as",
    "article_text_for_review": "Nueva junta directiva de la Peña Flamenca «El Lucero»\n\nEn asamblea general celebrada por la Peña Cultural Flamenca «El Lucero», de Montilla (Córdoba), resultó elegida nueva junta directiva, quedando la misma compuesta de la forma siguiente: presidente, Rafael Ortiz Laguna; vicepresidente, Luis López Vela; secretario, Francisco Gómez Soriano; tesorero, José Panadero García; vocales, Antonio Ortiz Laguna, Juan Muñoz García, Ruperto Herrador Márquez, Francisco Arroyo Pérez y Bienvenido Carrasco Martínez.\n\nNueva junta directiva de la Peña Flamenca de Alora\n\nDeseamos toda clase de aciertos a la nueva junta.\n\nEn asamblea general celebrada el día 23 de enero de 1987 por la Peña Flamenca de Alora (Málaga), resultó elegida nueva junta directiva, quedando compuesta de la siguiente forma: presidente, José Vergara Moreno; vicepresidente, Francisco Casermeiro Fernández; secretario, Francisco Cuenca García; tesorero, Juan González Hidalgo; relaciones públicas, Andrés Borrego Escudero; vocales, Andrés Vera Jiménez, Francisco Bravo Acedo, Benito Moreno López, Francisco Aranda Camuña y José Alcántara Rosas.\n\nNuestra cordial enhora- buena a la nueva junta.\n\nRamón Porras, de nuevo elegido presidente de la Federación de Peñas de Jaén\n\nEn reciente reunión celebrada por la Federación Provincial de Peñas Flamencas de Jaén en la localidad de Arjona, en la que estaban representadas todas las peñas federadas, resultó elegido nuevo presidente Ramón Porras González, una vez cumplido el mandato del anterior presidente, Vicente Alises. Pa de fundación y consolidación de la Federación, fue elegido por unanimidad, al recordar los asistentes la positiva labor realizada en su anterior mandato.\n\nRamón Porras, que ya fuera presidente en la eta-Felicitamos muy sinceramente a nuestro compa-ñero y demás miembros de la junta, deseándole una acertada andadura flamenca.\n\nOrganizado por la Peña Flamenca «Torre del Cante», de Alhaurín de la Torre (Málaga), con el patrocinio del Excmo. Ayuntamiento de la ciudad, ha sido convocado el VII Concurso de Flamenco «Mirando a la Torre», dotado con 290.000 pesetas en premios, siendo el primero de 100.000 pesetas y La fase de preselección tendrá lugar hasta el día 2 de mayo. La final se celebrará el día 16 de mayo.\n\ncontrato para el XIV Fes- tival de Cante.\n\nPara más información, los interesados podrán dirigirse al domicilio de la Peña, calle Viñas, 11, o al teléfono 410332.\n\nLas señas siguen siendo las mismas, es decir, Peña Cultural Flamenca «Curro Malena», calle Fernando Cámara, número 2, teléfono 879009, El Cuervo (Sevilla).\n\nTras el trágico y triste fallecimiento de Pepe Pozo, presidente fundador de dicha entidad, la nueva composición de la junta directiva queda como sigue: presidente, Agustín Benítez García; vicepresidente, Braulio González Zambrano; secretario, Jorge Luis Molina; tesorero, Andrés Cruz Barea; vocal 1°, Luis de Jesús Rodríguez; vocal 2°, José González Perele; vocal 3°, Francisco Rodríguez; vocal 4°, Diego Muñoz Ruiz, y vocal 5°, Rafael Doblas Romero.\n\nA todos ellos les desea- mos feliz andadura fla- menca en esta nueva eta- pa.\n\nEnrique de Melchor, ganador de la Guitarra Flamenca Maestro Patiño\n\nLa Peña Flamenca de Elche, con el patrocinio de Emilio Bustos, ha instituido el trofeo Guitarra Flamenca Maestro Patiño, que en esta edición ha recaído en el gran tocaor Enrique de Melchor. El ya tradicional premio «Zapato de Oro», que distingue la mejor actuación al cante, en lo sucesivo también se otorgará éste de nueva creación, para premiar el mejor acompañamiento a la guitarra.",
    "title": "Hablan las peñas",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 542,
    "article_char_count_full": 3556,
    "article_char_count_review": 3556,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-01-23-left-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDías pasados, en Mai-rena del Alcor, se reunió el Patronato de la Fundación Cultural Privada «Antonio Mairena», bajo la presidencia de don Rafael Alvarez Colunga, adoptándose, entre otros, los siguientes acuerdos:\n\n1.º Reelegir los cargos de los miembros del Consejo del Patronato.\n\n2.º a) Organización en colaboración con la Cruz Roja de Andalucía, de un acto de exaltación y difusión de la saeta flamenca.\n\nb) Convocatoria de 3.º Admitir como nuevos miembros del Patronato, a los siguientes señores: Antonio Fernández Díaz «Fosforito», Enrique Ferrer Romero, Enrique Jiménez Ramírez «Enrique de Melchor», José Menese Scott y Manuel Martín Martín.\n\nun premio de trabajo perriodístico 1987, que será anunciado seguidamente. c) Establecimiento de un premio, bajo el título «Casa de los Mairenas», que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"mantener\"]\n\ndel Patronato, a los siguientes señores: Antonio Fernández Díaz «Fosforito», Enrique Ferrer Romero, Enrique Jiménez Ramírez «Enrique de Melchor», José Menese Scott y Manuel Martín Martín. un premio de trabajo perriodístico 1987, que será anunciado seguidamente. c) Establecimiento de un premio, bajo el título «Casa de los Mairenas», que será otorgado con ocasión del XXVI Festival de Cante Jondo «Antonio Mairena», 1987. Al objeto de rememorar y mantener vivo el recuerdo de la obra del insigne cantaor Antonio Mairena, la Fundación Cultural Privada, que lleva su nombre, convoca un concurso periodístico, que se regirá por las siguientes BASES: 1.ª Podrán concurrir los autores de artículos de prensa, publicados en castellano, en el presente año 1987, relacionados con la obra de Antonio Mairena, en su doble vertiente de cantaor e investigador del flamenco y su legado discográfico. 2.ª Se establece un único premio de doscientas cincuenta mil pesetas. 3.ª La fundación «Antonio Mairena», podrá reproducir o hacer reproducir, editar o difundir por el procedimiento que desee, el trabajo premiado, con indicación del nombre de su autor; y no estará obligada a\n\n[ENDING CONTEXT]\n\nEl Cabrero. Chano Lobato. Aurora Vargas. Angelita y El Biencasao. José Luis Postigo. Tomatito. Organiza: Iltmo. Ayunta- miento.\n\nBAEZA:\n\nVALDEPEÑAS:\n\nAgosto, 8. Manuel Mairena. Luis de Córdoba. Ana Reverte. Luis El Polaco. Pepe Galán. Manuel Simón. Enrique de Melchor. Manuel Franco. Asociación Cultural «Vir- gen de la Cabeza».\n\nCANILLAS DEL ACEI- TUNO (Málaga):\n\nAgosto, 13. Calixto Sánchez. Pepe Galán. Antonio de Patrocinio. Pedro Bacán. Junta de Festejos.\n\nJIMENA DE LA FRON- TERA (Cádiz):\n\n—Estación FC. Calixto Sánchez. Juan Villar. Pepe Galán. Tina Pavón. Pedro Bacán. Iltmo. Ayuntamiento.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 1078,
    "article_char_count_full": 6921,
    "article_char_count_review": 2791,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "mantener"
      }
    ]
  },
  {
    "article_id": "1987-01-24-left-flamenca-placas",
    "article_text_for_review": "Discografia Flamenca (Placas)\n\nPor: Manuel Yerga",
    "title": "Discografía flamenca (placas)",
    "periodical": "candil",
    "issue_id": "1987-01",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 6,
    "article_char_count_full": 48,
    "article_char_count_review": 48,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-3-right-editorial",
    "article_text_for_review": "E l colectivo CANDIL ha querido que el número cincuenta de esta publicación se dedique, integramente, a un cantaor ilustre: Antonio Fernández Díaz, «Fosforito». Toda monografía, cuando se refiere a un cantaor vivo, entraña un riesgo que pretendemos soslayar: intentar la exhaustividad, cuando la premisa debe ser la limitación y las conclusiones un epílogo en blanco. Imposible determinar lo que Fosforito, abierto y cambiante en su arte como el río de Heráclito, será mañana. Imposible circunscribir en un centenar de páginas la rica, la plural trayectoria de un artista que ha dejado su piel, cantando, en los más insólitos lugares del mundo, durante los últimos cuarenta años. Por eso, más que monografía, esta edición cincuenta de CANDIL es una aproximación. Sincerísimo intento de recabar juicios, imágenes, anécdotas del cantaor de Puente Genil. Todo ello, si se quiere, dentro de estructura asistemática donde la opinión libre de muchos preclaros colaboradores, sin condicionante alguno, ha primado sobre cualquier otro criterio. Es la propia personalidad de Fosforito la que vertebra este trabajo, soporte documental y punto de partida, dicho sea con toda modestia, para la elaboración de una biografía, que muchos ya demandamos.\n\nLa personalidad de Antonio Fernández Díaz ha impregnado las últimas tres décadas de este siglo. Ya dijimos en otra ocasión que el concurso cordobés de 1956, foro en que se revelan y eclosionan todas las virtualidades artísticas de Fosforito y, en cierta medida, surje un nuevo concepto de cantaor, constituyó un hito para «nuestra» sistemática de la historiografía del Cante: la época del Festival Flamenco. Aproximarse a la vida y al arte de este cantaor significa comprender la historia más reciente del Flamenco, y en tal sentido, esta entrega de CANDIL supone una humilde contribución, como en otro tiempo fue el número dedicado al fallecido maestro Antonio Mairena, a la historiografía jonda de este siglo. Pero, con independencia de todo ello, el grupo CANDIL ha creído que el maestro de Puente Genil merecía sobradamente este pequeño homenaje. Cerca de cuarenta años manteniéndose en una línea de cante de rigurosa pureza, sin concesiones ni mediocridades, debe suscitar la admiración no sólo de cualquier aficionado, sino de todo estudioso de la cultura vivía autóctona de esta Andalucía doliente.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 366,
    "article_char_count_full": 2344,
    "article_char_count_review": 2344,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
