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
    "article_id": "1996-03-18-right-manuel-yerga-lancharro-enderezan",
    "article_text_for_review": "Amós Rodríguez Rey\n\nEn el diario \"ABC\", de Madrid, de fecha 6 de marzo de 1996, aparece una nota de nuestro amigo Ríos Ruiz, dándonos la triste noticia del fallecimiento de Amós Rodríguez, hermano del también fallecido Beni de Cádiz. Lo he sentido porque tuvimos una buena relación de amistad y muchas controversias sobre el arte flamenco.\n\nNuestro amigo Ríos Ruiz nos dice: \"Descendiente del legendario Viejo de la Isla...\". Yo no tengo más remedio que enmendarle la plana, porque el fallecido no tuvo que ver, desde el punto de vista de la consanguinidad, con Pedro Fernández Piña, \"Viejo de la Isla\", y sí fue nieto de José López Domínguez, \"Niño de la Isla\".\n\nNi Amós ni su hermano, llevaron los apellidos de su abuelo. Por qué? No lo puedo decir. Yo escribí algo de la biografía del legendario cantaor y precisamente fue Amós quien me pidió que no lo publicase. Sólo diré que la abuela fue una guapísima gitana y su abuelo un payo procedente de una familia de barqueros de Galicia. Hacabo de adquirir de la firma \"Fonobrón\", de Sevilla, el compacto que contiene los cantes de Don Antonio Chacón, acompañado a la guitarra por su paisano Juan \"Habichuela\". Una vez que he leído el fantástico álbum que contiene una relación completa de los cantes, he observado los siguientes errores, producto de un total y absoluto desconocimiento de los cantes:\n\nCartagenera de Chacón. \"Ay, de noche y día\". Deben decir: Taranta.\n\nCartagenera de Chacón. \"Ay, del Soberano\". Deben decir: Taranta.\n\nCartagenera de Chacón: \"Son desabrios\". Deben decir: Taranta.\n\nCartagenera de Chacón: \"Ay, la vía\". Deben decir: Taranta.\n\nCartagenera de Chacón: \"Ay, con San Antonio\". Deben decir: Taranta.\n\nCartagenera de Chacón: \"Ay, mi alma\". Deben decir: Taranta.\n\nEste tipo de taranta (ver mi libro donde trato de los cantes de Málaga y Levante) procede del árbol malacitano y es cante hermano de la malagueña.\n\nUna vez más tengo que decir: ¿Pero siempre tiene que ser Yerga Lancharro quien advierta a la afición de estos errores? ¿Qué creéis que gano con dedicarme a \"enderezar entuertos\"? ¡Nada! Sólo que las firmas que lanzan los cantes, me odien en lugar de agradecerme que les esté enseñando lo que no saben. Ya tengo dos firmas dedicadas a la comercialización de nuestros cantes que se han negado a venderme sus cassettes directamente, como lo venían haciendo, cuando, como digo, tendrían que agradecerme mis enseñanzas. Así es la vida.\n\nDije, digo y diré que mientras mi corazón y mi mente me respondan a plenitud, no dejaré de \"enderezar entuertos\". Esto es, siempre y cuando existan revistas que admitan mis escritos.\n\nTermino: Señores de \"Fonobrón\", no digan ustedes seguidillas, digan seguiriya o bien siguiriya. Sepan que las seguidillas son chambergas, boleras, manchegas, etc., que nada tienen que ver con las siguirias flamencas.",
    "title": "Enderezando entuertos",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 466,
    "article_char_count_full": 2819,
    "article_char_count_review": 2819,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-03-19-left-la-fascinaci-n-literaria-por-el-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAmelina Correa Ramón\n\nE $ _{1} $ movimiento romántico, que tiene su apogeo en el primer tercio del siglo XIX, trajo consigo una profunda reivindicación de \"lô popular\". En realidad, el único rasgo indiscutible del Romanticismo es el de su extraordinaria complejidad, construida sobre una difícil síntesis de contrarios. De ahí que el romántico pueda ser, a un tiempo, tradicional y progresista, católico y ateo, intimista y extrovertido, popular y aristocrático.\n\nPero, a pesar de todo, el Romanticismo creyó de manera firme en la existencia de un espíritu del pueblo, lo que los teóricos alemanes denominaron volkgeist. De este modo, en oposición al internacionalismo del siglo XVIII, los románticos exaltan los rasgos diferenciales de cada país. Se revalorizan así los antiguos poemas épicos o\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradiciones\"]\n\nl y progresista, católico y ateo, intimista y extrovertido, popular y aristocrático. Pero, a pesar de todo, el Romanticismo creyó de manera firme en la existencia de un espíritu del pueblo, lo que los teóricos alemanes denominaron volkgeist. De este modo, en oposición al internacionalismo del siglo XVIII, los románticos exaltan los rasgos diferenciales de cada país. Se revalorizan así los antiguos poemas épicos o legendarios, las canciones, las tradiciones locales, se favorece el cultivo de las lenguas vernáculas, y se da predominio a lugares o a pueblos que se consideran especialmente pintorescos o tradicionales. Así, el Romanticismo europeo descubrió España, país romántico por excelencia, y dentro de ésta, se sintió fascinado de manera especial por Andalucía, donde la herencia árabe parece permanecer inmutable al tiempo. Además, Andalucía cuenta con una forma de arte distinta y que refleja netamente la expresión de todo un pueblo: el flamenco. El flamenco se ofrece a la mirada fascinada del escritor como una forma pura de la pasión, como un arte virgen, incontaminado aún por la uniformidad de la sociedad burguesa occidental. El tema perduró hasta bien entrado el siglo XIX, y con fluctuaciones y cambios, se recuperó desde el extremo modernista de comienzos del siglo XX, a la búsqueda siempre de un ensueño co\n\n[ENDING CONTEXT]\n\nel lenguaje popular andaluz: \"Le he encargaito a mi mare, que el día que yo me muera, con tu retrato me entierren/para tenerte a mi vera\". (Poesías completas, edición de Antonio Fernández Ferrer, Sevilla, Renacimiento, 1993, p. 236).\n\n(4) CANSINOS-ASSÉNS, RAFAEL: La copla andaluz (1936), Sevilla, Biblioteca de la Cultura Andaluz, 1985, p. 39.\n\n(5) No se olvide que, según el Diccionario Etimológico de Joan Corominas, la palabra \"gitano\", documentada en español desde 1570, procede probablemente de \"egiptano, derivado de Egipto, por haber afirmado los gitanos que procedían de este país\".\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La fascinación literaria por el flamenco a principios de siglo",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 2559,
    "article_char_count_full": 15454,
    "article_char_count_review": 2952,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradiciones"
      }
    ]
  },
  {
    "article_id": "1996-03-22-right-la-comunicaci-n-catalu-a-pionera",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n1) En cambio sí que las hay escritas por plumas extranjeras y publicadas en países extranjeros. La más completa y reciente es la segunda edición del \"Manuale di Storia della Chitarra\" excelentemente editada en 1992 por la prestigiosa firma Bèrben en Ancona (Italia) cuyo primer volumen (\"La Chitarra Antica, Classica e Romantica\") se debe al cualificado autor Mario Dell'Ara y el segundo (\"La Chitarra Moderna e Contemporanea\") al no menos cualificado profesor, guitarrista y compositor Angelo Gilardino. Recomendamos la lectura de esta monumental obra italiana a todos cuantos se interesen por la historia del instrumento musical más español.\n\n2) Algunos de los tratadistas que vienen dedicando sus esfuerzos a desvelarnos esa historia son: Cristina Bordás, María Isabel Osuna, José Luis\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"reconociese\"]\n\nos se interesen por la historia del instrumento musical más español. 2) Algunos de los tratadistas que vienen dedicando sus esfuerzos a desvelarnos esa historia son: Cristina Bordás, María Isabel Osuna, José Luis Romanillos, Juan José Rey, Carlos Paniagua, José Miguel Moreno, Rodrigo de Zayas, Javier Suárez-Pajares, José Blas Vega, Norberto Torres y el mejicano afincado en España Gerardo Arriaga, entre otros. Sería falsear la realidad que no me reconociese a mí mismo formando parte de este grupo. Por otro lado, es de justicia mencionar la labor que desde 1890 vienen desarrollando las Ediciones de La Posada (Ayuntamiento de Córdoba), publicando anualmente la colección \"La guitarra en la historia\" que ya cuenta con seis volúmenes en los que se recoge veinticuatro trabajos redactados por veinticuatro especialistas internacionales en la investigación de diversos y distintos aspectos de esta historia —organología, literatura, composición, técnicas, interpretación, etc.—, colección que me honro en coordinar y que forma hoy día el Es cierto que la historia de la guitarra española está aún por escribirse en España. Es cierto que aún no podemos encontrar en librerías una historia de la guitarra española escrita por autores españoles¹. Pero también es verdad que desde no hace mucho, vienen apareciendo frecuentes trabajos acerca de distintos aspectos de la historia de la guitarra española, dado\n\n[ENDING CONTEXT]\n\nde Ramquedo aguel huevo que hay desde la Cela a la inmense Siata Logaúxio, pain laderas Debigue xamente ve infiece que en διάínds la Brut s en la Sextura 2 en la Quinta 3 en la Huma la en la Tercera 5 en la Segunda 6 en la T mas 65 se ha de contar, parol, principio de d.Kastil.\n\nTocose que las cinco hayas que com- prehende el papel son Figursoy Repa- cion selas cinco órdenes Primeroas que tiene el Instrumento, suplendo con una Hayla- la que falta para la Seзона con consen- ma que devia contenen la Pautes paras- ejecto: Vueso demonstrado lo dito en la tavas que se estampa para mayor clu-\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La comunicación «Cataluña, pionera en la didáctica de la guitarra»: algunas precisiones",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "22-24",
    "page_number": 22,
    "word_count": 2536,
    "article_char_count_full": 15547,
    "article_char_count_review": 3024,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "reconociese"
      }
    ]
  },
  {
    "article_id": "1996-03-24-right-el-flamenco-ante-la-juventud",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDavid Pino\n\nEl principal objetivo de este trabajo atiende, principalmente, a enfocar el fenómeno flamenco en su panorámica de la juventud actual. Todo ello se basará en mi propia experiencia de aficionado y modesto cantaor joven, que vive inmerso en la actitud generalizada de la juventud hacia el flamenco y también en la problemática que aquélla encuentra para sintonizar con dicho arte. Dificultades de la juventud para acceder al Flamenco Comunicación al XXIII Congreso de Arte Flamenco. Santa Coloma de Gramanet, Septiembre, 1995.\n\nno de los prejuicios que la juventud siempre ha sostenido y sostiene a la hora de sintonizar con el flamenco es que éste es un arte hecho para un tipo de público con una cierta edad, más avanzada que la juvenil y lo cierto es que hay algunos aspectos en el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"participantes\"]\n\nd para acceder al Flamenco Comunicación al XXIII Congreso de Arte Flamenco. Santa Coloma de Gramanet, Septiembre, 1995. no de los prejuicios que la juventud siempre ha sostenido y sostiene a la hora de sintonizar con el flamenco es que éste es un arte hecho para un tipo de público con una cierta edad, más avanzada que la juvenil y lo cierto es que hay algunos aspectos en el terreno artístico y más concretamente en el musical que requiere de sus participantes, no vamos a decir ya que una cultura, puesto que para disfrutar de flamenco no hay que entenderlo sino más bien sentirlo. Pero sí es necesario percibirlo quizás un momento en el cual el receptor del mensaje flamenco y más aún del emisor, estén en una etapa vital la cual le permita pararse, sosegarse, tranquilizarse..., en definitiva, tener una necesidad imperiosa de llenar su espíritu de algo tan transcedente y sublime como es el flamenco. Pero todas estas circunstancias parece que se dan con mayor facilidad en las personas que ya tienen una cierta madurez, puesto que los jóvenes de hoy día, parecen estar más declinables hacia otro tipo de música más bullanguera y ruidosa. De cualquier forma, este mismo caso se da también en la misma música clásica o en cualquier otra celebridad humana que requiera de un ensimismamiento. Siempre que analizamos la actitud de la juventud actual hacia el flamenco, llegamos a la drástica conclusión de que esta juventud está desatendiendo y haciendo caso omiso a sus raíces, tradición, cultura... Y casi nunca pensamos en que ésta puede que no sea una posición que los jóvenes hayan elegido por iniciativa propia, sino que la cultura dominante nos ha \"obligado\" a adoptarla con el constante bombardeo anglosajón, porque entiendo el flamenco como algo tan humano, verdadero, espiritual e identificado con el ser, que supongo prácticamente imposible que a alguien con la más mínima sensibilidad le pase desapercibido, independientemente de la edad e, incluso, la nacionalidad. Luego entonces, la primera interrogante surge por sí sola: ¿Por qué la juventud en su mayoría argumenta que no le gusta el flamenco? Sencillamente creo que se debe a que no lo ha escuchado. ¿Por qué? Supongo que por todo lo dicho anteriormente y por otra razón que considero algo decisiva para el caso: Pa\n\n[ENDING CONTEXT]\n\nque este ejemplo ya al final de esta comunicación, tan sólo me resta hacer constancia de mi gran interés porque el tema que nos ha ocupado cuente con una mayor preeminencia dentro de los temas propuestos a debatir en próximos congresos.\n\nTambién quisiera añadir que el presente trabajo conlleva algunas sigerencias y propuestas que se han lanzado al aire y que, al no quedar concluidas ni aseveradas por nuestra parte, dejamos la puerta abierta para establecer futuros debates, ya que de ser tenidas en cuenta, su viabilidad estaría sujeta a los organismos competentes al efecto.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El flamenco ante la juventud",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 2099,
    "article_char_count_full": 12548,
    "article_char_count_review": 3896,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "participantes"
      }
    ]
  },
  {
    "article_id": "1996-03-26-left-noticiario-flamenco",
    "article_text_for_review": "ACTIVIDADES\n\nPreCONGRESUALES\n\nDía 8 de septiembre, domingo:\n\n12'00 horas: Inauguración en el Parque del Alamillo del Paseo de los Cantes Flamencos. Pregón flamenco de las flores, y a continuación concierto por la Banda de Música de Sevilla.\n\nDías 9 y 10 de septiembre, lunes y martes:\n\nConferencias, mesas redondas, exposiciones, etc., etc. SESIONES CONGRESUALES\n\nDía 11 de septiembre, miércoles:\n\nPrograma del XXIV Congreso de Arte Flamenco.\n\nDel 12 al 14 de Septiembre, Sevilla, 1996\n\nA partir de las 10 de la mañana: Recepción congresistas en el Hotel Alcora.\n\n19'30 horas: Paraninfo de la Universidad de Sevilla. Conferencia inaugural del Congreso y conmemorativa del Centenario de D. Manuel de Falla, por don Ramón Serrera, catedrático de historia de América de la Universidad de Sevilla. Tema de la conferencia: \"D. Manuel de Falla y el cante jondo.\n\n20'30 horas: Reales Alcázares de Sevilla. Recepción a los congresistas por parte de la alcaldesa de Sevilla.\n\n22'00 horas: Concierto por la Real Orquesta Sinfónica de Sevilla.\n\nDía 12 de septiembre, jueves:\n\n10'00 horas: Primera sesión del Congreso. Tema: \"La estética del cante\". Comunicaciones y mesa redonda en torno al tema con la intervención de prestigiosos artistas del cante. Coloquio abierto a los congresistas. Proyección del vídeo \"La estética flamenca en la óptica de Alfredo Kraus\".\n\n14'30 horas: Almuerzo en el hotel.\n\n17'30 horas: Segunda sesión del Congreso. Tema: \"La estética del baile\".\n\n18'30 horas: Comunicaciones a la ponencia y mesa redonda en torno al tema con la intervención de destacados artistas del baile flamenco y coloquio abierto a los congresistas.\n\n21'00 horas: En el Teatro Lope de Vega, concierto de Carmen Linares.\n\n23'00 horas: Recepción y presentación del disco del Congreso.\n\nDía 13 de septiembre, viernes:\n\n10'30 horas: Tercera sesión del Congreso. Tema: \"La estética del toque\".\n\n11'30 horas: Comunicaciones a la ponencia y mesa redonda en torno al tema con destacados artistas del toque y coloquio abierto a los congresistas.\n\n14'30 horas: Almuerzo buffet o servido en el hotel.\n\n17'30 horas: Cuarta sesión del Congreso y sesiones paralelas dedicadas a programaciones flamencas, didáctica, tratamiento interdisciplinar, etc., etc. Un compromiso para con los Ayuntamientos, los profesores, C.E.P. (s), comisiones de congresistas, etc.\n\n19'00 horas: Puesta en común, comunicaciones, mesas redondas y coloquio abierto a congresistas.\n\n21'00 horas: En el Teatro Maestranza; \"Carmen\", montaje de Salvador Távora.\n\n23'00 horas: Recepción y presentación de una producción del Congreso. Día 14 de septiembre, sábado:\n\n10'00 horas: Quinta sesión del Congreso y revisión de los Estatutos. Conclusiones. Elección de sede para el XXV Congreso, elección del Comité Intercongresos, etc.\n\n14'30 horas: Almuerzo.\n\n21'00 horas: Cena de clausura en el Hotel Alcora.\n\n24'00 horas: Monasterio de San Jerónimo. \"Cumple años feliz\". Un homenaje de la Bienal a los artistas que andan alrededor de \"los 50\": Chaquetón, Calixto Sánchez, La Chana, Diego Clavel, Manolo Domínguez, etc., etc.\n\nPRODUCCIONES DEL CONGRESO\n\n* Libro: \"El flamenco y los flamencos\", de Mario Penna. Traducción de Antonio Zoido.\n\nLibro: \"Historia del flamenco a su paso por Sevilla en el siglo XIX\", de José Luis Ortiz Nuevo.\n\nLibro: \"Itinerario botánico por el campo de la copla\", selección de la Fundación Machado.\n\nDisco compacto: \"De la lírica al cante\". Un acercamiento a los poetas andaluces a través del cante flamenco. Cante: Calixto Sánchez.\n\nDisco compacto: \"Cantaores sevillanos a comienzos del siglo XX\".",
    "title": "Noticiario Flamenco. Programa del XXIV Congreso de Arte",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 546,
    "article_char_count_full": 3565,
    "article_char_count_review": 3565,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
