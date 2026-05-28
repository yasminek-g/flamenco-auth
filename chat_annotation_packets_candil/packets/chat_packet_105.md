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
    "article_id": "1985-01-7-left-gazhira-bailaora",
    "article_text_for_review": "Suena en la noche el furor de tu negra arboleda y los luceros se dispersan simétricos en el universo candente de tu bata infinita.\n\nGazhira, sedente, en retrato vivo y anónimo en tu pose, va y viene un vendaval. Aunque tus brazos conjuren la hoquedad celeste de la memoria\n\nLa danza. La mirada. El misterio.\n\nToda una ágil y certera cinematografía de luces y de sombras en donde tu suspiro apaga el tintineo huraño de los sueños y tu bendita locura se yergue triunfante en la romántica falseta de una soleá.\n\nLos brazos. Los ojos. La boca.\n\nSuena en la noche, el taconeo incesante de sus místicos sentimientos pero hay un candil pobre y tenebrista —en el camino—, que alumbra sin sosiego, la boca abierta de tus besos.\n\n(Gazhira lleva en sus pies una legión de niños interminables que le levantan el traje y le roban los amaneceres)\n\nLos relojes y los galgos persisten en el horizonte, y tú, Gazhira, bailas, bailas...\n\nJesús Cuestarana",
    "title": "Gazhira, Bailaora",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 164,
    "article_char_count_full": 936,
    "article_char_count_review": 936,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-01-7-right-el-cante-inventado",
    "article_text_for_review": "N o son pocos los tratadistas del flamenco que estiman y aceptan el hecho de que el cante vaya por cauces de renovación y ven con cierta complacencia los intentos que artistas influidos por esa tendencia van poniendo en práctica, alguno con arraigo suficiente como para erizar el escaso y cano cabello que conservan las mal pensantes mentes de viejos aficionados: vetustos inmovilistas de la tradición cantaora.\n\nEn la edición andaluza de ABC, pronto hará catorce años, publicamos un trabajo sobre este aspecto de la modernización del cante, aspecto que lejos de declinar se mantiene plenamente válido. Hemos dicho siempre que el cante está inventado y que si ciertamente pueden aceptarse las florituras, añadidos y variantes en los que en nuestro lenguaje (en nuestra galaxia, diría un modernista) solemos llamar cantes chicos —valga también el título de cantes de compás libre (tan libre como que está ausente de ellos)— de ahí no pasamos. El número de las malagueñas, por ejemplo, puede ser ilimitado. Alargar o acortar tercios, situarse en tesituras variadas y variantes en una petenera es, además de lógico, admisible y puede llegar a ser incluso plausible. Pero inventar un cante de los entendidos básicos, deviene tarea imposible. A menos que queramos referirnos a los estilos, cuya variedad conocida no hace sino reafirmar que tales cantes, como básicos, son fundamentales y como fundamentales inmutables. (Que alguien cree —literalmente— un estilo nuevo de soleares, por ejemplo, las que dio Por:\n\nPaco Vallecillo\n\nEscribo, tacho, rompo, lloro, ¡esta maldita voz desafinada!\n\n(Luis González)\n\nen llamar de Charamusco, sosteniendo sin enmendarla una actitud que no es ahora el caso de afrontar, es posible y ahí está el ejemplo por si nace alguien capaz de seguirlo en el futuro). Pero el cante medular ya está inventado y por mucho que tratemos de exprimir nuestra ca-\n\npacidad imaginativa, no podemos concebir có-mo y de qué forma se va a crear un compás nuevo. Porque sin compás no habrá cante medular, esto es una pura obviedad.\n\nEn esa evolución está el desarrollo de influencias vocales, tonales y melismáticas que marcan diferencias visibles y tangibles. La evolución se compadece perfectamente con la in\n\nSon los estilos personales (la aportación del artista) más o menos influidos por los factores ambientales (tan determinantes siempre) los que han marcado la evolución permanente del Cante Jondo\n\nnovación, ahí está el meollo de la creatividad admisible en los cantes esenciales: pero con un sometimiento riguroso a estructuras formales que son inamovibles. Porque lo que no puede\n\nser, no puede ser; y, además, es imposible, como dijo un insigne paisano de Séneca. Evolucion, continuamos repitiéndonos, traída como aportación de todo cuanto el individuo puede prestar de su propia naturaleza y personalidad, en tanto que éstas sean proyectables y tenga capacidad de engendrar; siempre con sujeción a esos cánones y soleras en los que se contiene y custodia la infinita armonía de unas formas que en su propia rebeldía a las leyes de la música encuentran su indiscutida genuidad. Desaparecidos Pastora y Juan Talega, desaparecido Antonio Mairena, el escueto censo de cantaores capaces de movilizar esas aportaciones que constituyen el proceso evolutivo por el que se puede legítimamente llegar a la recreación a través de una decisiva personalidad, se muestra cada día más reducido. La escasez se hace más visible y más lamentable cuando trata de cubrirse en engendros sedicentemente flamencos, en cantes inventados, en hueras canciones folklóricas aflamencadas que se guardan en el bolsillo de la americana como formulaciones científicas en las que la falaz ignorancia y la censurable petulancia se acumulan en bodrios que hay que calificar —descalificar— con la mayor dureza. El cante —nuestro cante— es magmático y como substancia y basamento no admite artificios que en vez de culminar su grandeza contribuyen a degradarla y prostituirla. Frente a las modas de los tiempos y frente al desarraigo de quienes unen el desconocimiento a la incapacidad, digamos como el filósofo: Que inventen ellos.",
    "title": "El Cante inventado",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 653,
    "article_char_count_full": 4117,
    "article_char_count_review": 4117,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-01-8-right-el-flamenco-oficial",
    "article_text_for_review": "Hubo un tiempo en que los artistas flamencos recibían su estipendio —cuando cobraban, que eran las menos veces — en calderilla tras una noche de cante, baile o toque agotadora. Eran los tiempos heroicos del cante. Así empezaría a escribirse la historia del flamenco. Una historia en que si ellos —los artistas— fueron los auténticos protagonistas de situaciones creaciones y tragedias, también serían los extras y tramoyistas de la acción. Aquel primitivo flamenco claustral, esotérico, tabú y desconocido de ghetos, corrales, fraguas y tabancos, fue lenta y progresivamente asomando fuera de los muros de su propio hábitat gracias a hombres como Silverio Franconetti que lo llevarían a los tablaos de los cafés cantantes. Hay quien, como Demófilo, piensa que éste fue un hecho nefasto y negativo por cuanto el cante perdería su carácter primigenio y prístino mixtificándose al nutrirse de ma\n\nSin duda, en un principio, la anarquía fue la norma y la indefensión ante la sociedad del drama del flamenco cuando aún éste no conocía la existencia de la palabreja. De entonces acá ha llovido mucho.\n\nPor: Antonio Rincón\n\ntices extraños que imperaban en el exterior de su reducto natural. Sin embargo, podemos argumentar en contra, que de no haber sido así no hubiera sido posible su conocimiento por las generaciones actuales, como tampoco hubiera tenido lugar su desarrollo ni la aparición de muchos de los grandes ídolos de nuestro Arte Flamenco como Chacón, Manuel Torre, Pastora y Tomás Pavón, Caracol, Vallejo, Juan Talegas o el mismísimo don Antonio Mairena.\n\nAsí pues, hemos de convenir en que si\n\nEl cante jondo perdió pureza con su divulgación y ecumenismo, también es cierto que se enriqueció al apropiarse de otras normas igualmente válidas llegando a conformar lo que hoy es un espectro amplio y rico de estilos.\n\nPor otra parte, cabe pensar que al incorporarse a su estudio una élite intelectual cualificada cuando el cante sufría el olvido y el desinterés de la sociedad, ajena en su mayoría a los auténticos valores del flamenco, ésta trajo consigo una mayor atención sobre este arte si no incipiente, sí al menos nuevo para muchos. Los nombres de Falla, García Lorca, Alberti, Blas Infante, entros otros son ya eslabones compactos que van ineludiblemente unidos a la cadena de acontecimientos que estructuraron el mundo flamenco contemporáneo.\n\nHoy, ahora, el arte flamenco ha tomado tales dimensiones en Andalucía sobre todo, pero no sólo en ella, que ha merecido capítulo aparte en el organigrama de Cultura de la Junta de Andalucía que ha creado para este fin un departamento exclusivo para su estudio y difusión.\n\nDel ghetto, de la cava, de la taberna, el cante jondo —pasando por las reuniones (aquéllas del hueso aceitunero y guitarrazo), tablaos y cafés cantantes— se sienta por derecho propio en la poltrona de la administración que pregona a los cuatro vientos sus excelencias y su inefable carisma de arte andaluz por antonomasia.\n\nSin embargo, esto, con ser mucho, no debe bastar porque es mucha la responsabilidad de este departamento que cuenta de antemano con la anuencia de los jerarcas y la esperanza ilusionada de los aficionados. Los errores, partidismos u omisiones en que pudieran caer los responsables de esta nueva etapa del arte flamenco tendrán, sin duda, una proyección infinitamente mayor que en épocas anteriores y, por lo tanto, causarían un daño irreparable al flamenco del que difícilmente podría resarcirse.\n\nAsí las cosas este flamenco oficial y burocrático, para merecer el respeto y acatamiento del aficionado de a pie, debe desprenderse de todo matiz sectarista y espíritu de infalibilidad que pudiera arrogarse atendiendo a su función rectora.\n\nDebería, el departamento, por la misma razón, dogmatizar menos y hacer más. Su carácter ejecutivo tiene que prevalecer por encima de otras consideraciones. Que no es la panacea que viene a curar los males inveterados que afectan al cante desde tiempo inmemorial lo sabemos todos; pero también sabemos que el aficionado necesita creer en su pragmatismo y en su poder resolutivo y de ahí que en su fuero interno piense que se puede hacer más.\n\nEl departamento debe ser omnipresente, aunque exento de soberbia y excesivo paternalismo; conciliador entre las distintas corrientes que nutren el gran cauce flamenco, pero al mismo tiempo imparcial a carta cabal; claro y límpido en sus actuaciones si quiere granjearse la confianza de sus acólitos; enérgico en su lucha contra manipulaciones y ventajistas, pero dialogante con los demás; ecuánime en sus apreciaciones y generoso hasta donde le permitan sus fuerzas y, sobre todo, debe tener siempre presente que el arte flamenco es el «leit-motiv» de su función y cargo por lo que está obligado a luchar por su defensa.\n\nConfiemos que sea así por el bien del flamenco y los flamencos. Cuenta para ello con el aval sin precedentes de una afición esperanzada y crédula. De lo contrario, el departamento acabaría siendo mera comparsa en el juego del poder, sin personalidad ni identidad y a merced de oscuras maquinaciones.",
    "title": "El Flamenco Oficial",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 820,
    "article_char_count_full": 5056,
    "article_char_count_review": 5056,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-01-10-left-las-letras-flamencas-de-jos-luis",
    "article_text_for_review": "A CARMEN LINARES\n\nToa la leche que mamaste debía haberse vuelto veneno el día que me dejaste.\n\nQue por dormir yo en tu cama me tengo que levantar con los luceros del alba.\n\nTe pedí que me lo dieras y no me quisistes dar el fuego de tu candela.\n\nVas a vivir condenao por no romper las cadenas, los grilletes y el candao.\n\nEntre misas y rosarios chiquilla te vas a quedar más amarilla que el cirio que hay delante del altar.\n\nEl subir la cuesta cansa, pero más agobio dan las fatiguitas del alma.\n\nParece que no te acuerdas cuando con nuestros cuerpos destrozábamos las siembras.\n\nMe da rabia porque sabes lo bonita que te pones con la bata de lunares.\n\nEl resplandor de tus carnes es la estrellita polar que guía a los caminantes.\n\nYo me alumbro con el sol, me baño con el rocío, busco calor en tu cuerpo y siempre estoy arrecio.",
    "title": "Las letras flamencas de José Luis Buendía",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 156,
    "article_char_count_full": 828,
    "article_char_count_review": 828,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-01-10-right-ellos-los-protagonistas-dicen-ma",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMi padre era un gran aficionado al cante; era un amante de los cantes gitanos como gitanos que somos —Manolo, ¿nos puedes explicar cómo fueron tus comienzos? Suponemos que sería de la mano del entrañable Maestro.\n\n—No. Fue de la misma mano que llevó a ANTONIO y de la misma manera. Como todos sabéis, mi padre era un gran aficionao al cante; era un amante de los cantes gitanos, como gitanos que somos, aunque los cantes no gitanos nunca los despreciaba porque le tenía un gran respeto a CHACON, JUAN BREVA, etc., a todo el que cantara bien. Pero claro, nosotros estamos hechos de una forma... de la misma forma que se forja el hierro en una herrería, con la misma forja, con la misma dureza.\n\n—¿Tu padre se dio cuenta del pedazo de ar-\n\nEntonces, de la misma forma que mi padre llevaba a mi hermano\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\nque somos, aunque los cantes no gitanos nunca los despreciaba porque le tenía un gran respeto a CHACON, JUAN BREVA, etc., a todo el que cantara bien. Pero claro, nosotros estamos hechos de una forma... de la misma forma que se forja el hierro en una herrería, con la misma forja, con la misma dureza. —¿Tu padre se dio cuenta del pedazo de ar- Entonces, de la misma forma que mi padre llevaba a mi hermano a Alcalá o a cualquier otro sitio, pa que escuchara a MANUEL TORRE a JOAQUIN, etc., esto mismo lo hacía conmigo, me hacía que escuchara a mi hermano. Mi padre estaba que no veas, porque tener en una familia gitana un primogénito con lo guapo y lo buen mozo que era ANTONIO... claro, mi padre estaba loco con su hijo. Tista que tenía en la casa? ¿Qué decía el gitano RAFAEL de sus hijos, de los tres? —Mi padre era un padrazo. Era, como ya he dicho, un gran aficionao al cante y sabía que su hijo era un maestro; mi padre era una persona que exigía mucho, no se conformaba con cualquier cosa, por ejemplo: en su trabajo de herrería cuando estaba haciendo una reja, no decía ya está, no hasta que la obra estaba perfeccionada. En estas cosas ANTONIO era como él. —Manolo, ¿pesa mucho, en la actualidad, llamarse Mairena? Mi padre en la herrería fue un fuera de serie, igual que Antonio en el cante, tenían unos sentimientos artísticos inigualables. —Yo creo q\n\n[ENDING CONTEXT]\n\ndecir cuándo y cómo se debe de entregar, y aun así, os vais a equivocar. Porque los artistas somos los menos indicados para decidir esto. Creo que se debiera de formar una comisión de personas entendidas, suficientemente cualificadas, para que algún día se entregue la llave; porque lo que no debemos consentir es que la llave se entierre con ANTONIO y él no quería eso. El quería que el flamenco siguiera adelante.\n\nRecepción diaria de Mariscos y Pescados Especialidad en Asados\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas dicen: Manuel Mairena",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "10-12",
    "page_number": 10,
    "word_count": 1962,
    "article_char_count_full": 10941,
    "article_char_count_review": 2988,
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
  }
]
```
