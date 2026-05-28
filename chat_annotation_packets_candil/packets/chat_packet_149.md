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
    "article_id": "1987-03-31-left-sangre-entera-volcada-o-el-cante",
    "article_text_for_review": "on ese empuje trémulo que nace en los principios del ser [y la garganta,\n\ncon esa consistencia de frío y enteresa que tiene un río largo o los doce sonidos de una noche en diciembre, así tu cante surte de extremidades firmes al sudor, la fieresa, y las horas se alargan por esos derroteros donde crece intangible la flor de la memoria.\n\nDiluidos que son, y arrejuntados, por los vidrios oscuros los qué de los que han sido para la historia nombre, el cante —nuestro signo de ser pueblo-raíz o cumbre de una expresión antigua, y en ti más que expresión: sangre entera volcada— rompe estructura y crea vida para el deleite de los sueños del hombre que busca sus orígenes.\n\nHierática hermosura de lo pobre, de lo triste que encierra nuestro sino limpio de escarcha y vivencias hasta la melodía fragante que enerva el corazón entre pañuelos y auroras [sin medida,\n\nancho de espacio, hondo de grito y de humano, tu cante sin consuelo nos redime de días pasados bruscamente.\n\nPRUDENCIO SALCES Noviembre de 1984",
    "title": "Sangre entera volcada o el cante de Fosforito",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "31-31",
    "page_number": 31,
    "word_count": 176,
    "article_char_count_full": 1004,
    "article_char_count_review": 1004,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-31-right-la-2-distinci-n-compas-del-cante",
    "article_text_for_review": "omo redactor-jefe de esta revista, debo confesar una cosa previa: sea cual sea el resultado de este número-homenaje al maestro de Puente Genil, yo no voy a estar por completo satisfecho del mismo. Y ello porque el mejor homenaje, el único, que puede rendirsele a Fosforito, afortunadamente ya ha tenido lugar hace tiempo, y consiste en reflexionar sobre la ósmosis esencial que existe entre el cantaor y el cante jondo al que da vida, esto es, afirmar que Fosforito es al cante lo que la piel al resto de la encarnadura que forman los diferentes tejidos del cuerpo humano, algo que no es una funda ni un añadido, tampoco la forma que envuelve el fondo de la creación artística, sino una unidad indisoluble: Fosforito rehace un cante que siempre existió en los hondones soterrados del pueblo andaluz, y, a la vez, esa energía poética y musical, terriblemente humana, cobra identidad, nombre y apellidos en la garganta de Antonio, en su extraordinaria sensibilidad, acorde y acompasada como pocas, y entonces, cante y artista afloran a la\n\nsuperficie de la tierra como esas incontenibles corrientes fluviales que van a dar a la mar, que aquí, como en la estrofa manriqueña, también es el morir; morir de angustia ante la visión inefable de algo que se intuye más que se presencia, morir ante la desazón que nos acogota cuando Fosforito templa la voz y crispa el rostro, aún mucho antes de lanzarnos su gemido exacto, como un arco de viento tendido entre dos palmeras.\n\nPor todo ello, cuando en diciembre de 1985, el jurado que otorga la distinción del «Compás del cante», modélico galardón creado por la Cruz del Campo, nos reuníamos bajo el aroma espiritual de la Giralda, para la difícil tarea de concederlo en su segunda edición, puedo asegurar que la discusión, planeada hasta los límites de la madrugada, se acabó apenas iniciado el aperitivo de entrada: Fosforito era su ganador por práctica unanimidad de los aficionados que allí barajábamos los términos de las bases: pureza, profesionalidad, constancia, nuevos matices cantaores, labor de difusión del cante y un sinfín más de consideraciones evaluables. Todos estos conceptos eran las perlas de una corona que recayó, en apenas cinco minutos, sobre las sienes de un hombre que las ve enrojecer a diario cuando las venas le señalan lo abultado de su pasión cantaora, y sus manos realizan la elocuencia del mensaje más verdadero jamás explicado mediante el lenguaje artístico.\n\nNunca un galardón con tales criterios para ser concedido estuvo tan hecho a la medida del que habría de recibirlo. Con su otorgamiento ganamos todos: el jurado, que acertó plenamente; la entidad organizadora, que se prestigió más si cabe en su noble empeño a favor de nuestro arte y, en fin, toda la familia flamenca. Fosforito, en cambio, no ganó nada, o, al menos, nada nuevo. Como tampoco gana nada con el homenaje entrañable que hoy le rinde CANDIL. Su premio lo tiene adherido a las propias entrañas cantaoras, junto al desgarro de su voz, rota en miles de madrugadas. Un día bajarán a entregárselo dos ángeles morenos.",
    "title": "La segunda distinción «Compás del Cante» para Fosforito",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "31-31",
    "page_number": 31,
    "word_count": 514,
    "article_char_count_full": 3058,
    "article_char_count_review": 3058,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-32-left-de-el-tambien-se-ha-dicho",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«Este Fosforito de hoy, el que ganó en 1956 el concurso de Córdoba, se ha convertido andando el tiempo en el maestro flamenco, en el nuevo maestro del cante andaluz. Creo que esta opinión puede ser compartida por muchos, muchísimos aficionados. Sulargura estilística, su sabiduría flamenca, su profesionalidad máxima hacen de Fosforito la figura actual».\n\n(MANUEL RIOS RUIZ)\n\n«S»i volvemos al viejo tema lorquiano de la musa, el ángel y el duende, la voz de Fosforito pelea broncamente —como Jacob— con el ángel del frío, esquiva en gracia el plegado armonioso de la musa y se entrega tronchada, balbuciente, enfebrecida al deseo, negro del duende».\n\n(PABLO GARCIA BAENA)\n\n«La discografía de Fosforito es una auténtica antología del cante. Además como es un cantaor muy completo, tiene también sus\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"puro\"]\n\n, esquiva en gracia el plegado armonioso de la musa y se entrega tronchada, balbuciente, enfebrecida al deseo, negro del duende». (PABLO GARCIA BAENA) «La discografía de Fosforito es una auténtica antología del cante. Además como es un cantaor muy completo, tiene también sus características muy especiales». RAFAEL GOMEN MONTERO 《Fosforito, hoy por hoy, es manantial donde brota el saber, y fuente donde tiene que beber el que quiera permanecer puro》. (JULIO M. DIEZ) «P or su permanencia en cabeza de los mejores, por la amplitud de su saber, a Fosforito nadie le hace sombra hasta ahora. Con Fosforito, un consumidor vital del flamenco, el cante alcanza alturas inusuales». (JUAN BUSTOS) «Fosforito responde a lo que esencialmente debe ser el cante jondo. Lo que ante todo, presta su sello a su arte es la patente personalidad que trasciende por cualquier tipo o estilo de cante. Cuanto asimila lo convierte, espontáneamente y por virtud propia, en cante personalísimo. Dotado de un prodigioso sentido musical, jamás comete un fallo». (ANTONIO GIL) osforito, aparte de demostrar que es el más enciclopédico de todos los cantaores presentes se destapa como un consumado orador que sabe usar la frase exacta y el calificativo correcto en el momento preciso». (LUIS MELGAR) on Fosforito se ha reencontrado la estética jonda del cante por la que clamaba García Lorca desde Granada. Haría falta un Fosforito para cada manifestación artística de Andalucía y que lo pudieramos ver, aunque fuera al cabo de una verdadera batalla por explicarlo; más aún, un Fosforito para la política, otro de las letras, otro de la fe, fe en nosotros mismos... y Andalucía sería entonces una inmensa luminaria». (AGUSTIN GOMEZ) (MANOLO SANCHEZ) «Antonio es un hombre que lleva cantando cuarenta años en candelero, dando la buena imagen del flamenco por todas partes, de España y a nivel mundial, como artista de sapiencia, cumplidor, y sabiendo estar en el s\n\n[ENDING CONTEXT]\n\ncante. Este difícil raro entremezclamiento es el que nos aproxima al secreto de Fosforito. En una palabra: lo entrañable en él no perjudica a lo cordial, la lágrima al ojo, la verdad a la belleza».\n\n(ANSELMO GONZALEZ CLIMENT)\n\n«H an pasado los años y Fosforito —miembro de la familia de los Piconeros, que está en el flamenco desde hace varias generaciones—, hoy sigue siendo un maestro, un cantaor largo, conocedor prácticamente de todos los cantes, que interpreta con justeza, casi siempre con ortodoxia y con unas formidables facultades de voz y reserva de recursos».\n\n(ANGEL ALVAREZ CABALLERO)\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "De él también se ha dicho",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "32-32",
    "page_number": 32,
    "word_count": 1105,
    "article_char_count_full": 6738,
    "article_char_count_review": 3560,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "puro"
      }
    ]
  },
  {
    "article_id": "1987-03-33-left-poemas-a-fosforito",
    "article_text_for_review": "FOSFORITO\n\nHay un hondo temblor de madrugada —avanzada de olivos contra el río— que en el galope del escalofrío va esculpiendo una sombra tatuada.\n\nHay una voz en el silencio alzada que quiere provocar un desafío, mas todo lo domina el griterío de la sangre en el tumulto rebelada. Calla la voz porque lo impone el rito que es juego y es pasión y jerarqu*la oráculo de Oriente, llama y grito. Dejad que cante y llore en su agonia que se extingue en sí misma. Fosforito se ofrece en holocausto el nuevo día.\n\nCafé de Manolo Santos, calle Santa Catalina, taberna de Antonio Cantos, ¡las vueltas que da la vida!\n\nCuestecita de Jesús, qué difícil de subirla si no me acompañas tú.\n\nJoaquín González Estrada\n\nA LA VOZ DE FOSFORITO\n\n(Sonetillo con estrambote)\n\nToda la pena o la suerte o el amor, cabe en un cante, ese cante, vida y muerte de tu voz, noche adelante. Voz de pueblo, voz de cielo, tu voz es música herida, voz de luto, voz de duelo, pura voz rota, transida. Tu voz se acerca y se aleja tu voz me duele en la entraña como un puñal o una queja. Voz de sombra, eco de espanto, sangre a son, grito de España, tierra y fuego —¡oh Dios!— tu cante. (Honda voz, caverna y pozo. Voz con la que sufro tanto. Tu voz de llanto es mi gozo).\n\nMedia voz o grito abierto el cante es queja. La copla, vida herida por tres versos. Hasta el ruedo de la tarde, de la noche de los tiempos suben hondos tercios, lances. Como un toro del chiquero, el buen cante salta al aire. Quién me lo fija en los medios? Citar de frente es la clave. Y la llave del misterio el cante por naturales. Y la estocada en el centro. Porque el cante es arte y parte la almendra del sentimiento. De la sima de la carne o de la cima del alma de un ciego amor, nace el cante. Cuando su arranque es de dentro, sube por dentro y araña los entresijos del cuerpo. Tiene voz de tribu errante, raíces, sonidos negros, duendes sueltos por la sangre. Cante-pellizco en el pecho, cante-nudo en la garganta cante que levanta el vello. Ese cante es cante grande, puro y jondo por derecho, recio y rancio, llanto al aire. Cante de verdad, concierto de ecos de terribles madres para guitarra y silencio.\n\nENVIO: A Antonio Fernández Díaz «Fosforito», pontanés. ¡Así canta Andalucía! Ahí va el cañero a tus pies.\n\nSONETO AL ENTE CORDOBÉS\n\nLlegar a ser profeta en otra tierra, cualquiera con su arte ha conseguido; pero en la tierra donde se ha nacido, pocos profetas nuestra historia encierra. Cuando el hombre es ingrato siempre entierra valores de otros hombres distinguidos y esta ingratitud, a veces ha sido el hecho por qué un pueblo se soterra. Un pueblo, cuando es agradecido escribe en su blasón con letras de oro, que todo su linaje es bien nacido. Tú, Córdoba, has marcado un nuevo hito porque has reconocido con decoro el arte de tu hijo, «Fosforito».\n\nFrancisco Barrionuevo",
    "title": "Poemas a Fosforito",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "33-33",
    "page_number": 33,
    "word_count": 519,
    "article_char_count_full": 2833,
    "article_char_count_review": 2833,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-34-right-los-escritos-de-fosforito",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEl gran maestro en todos los cantes Antonio Mairena, a estas alturas no es un artista criticable ni discutible.\n\nAntonio, desde hace mucho tiempo en el mundo del flamenco por su gran labor demostrada, es punto y aparte.\n\nEl tiene todo esto más que supera- do y está muy por encima de estos pro- blemas tan a ras de tierra.\n\nDespués del maestro está el gran vacío (en cierto modo lógico), después, los demás.\n\nAntonio Mairena a pesar de ello, o quizá por ello, aunque lleva tiempo retirado del gran público, jamás estuvo al margen de este tinglado y aconteceres flamencos, por eso, es tan agradable ver que el Maestro Mairena está cada día más presente en la mente de todos los buenos aficionados, tanto es así que ahora más que nunca son constantes los homenajes que está recibiendo aunque presiento\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"Maestro\"]\n\neceres flamencos, por eso, es tan agradable ver que el Maestro Mairena está cada día más presente en la mente de todos los buenos aficionados, tanto es así que ahora más que nunca son constantes los homenajes que está recibiendo aunque presiento que no todo el trigo está tan limpio como pretende aparentar. Estamos seguros que muchos de estos homenajes son de corazón, de esa buena gente llevadas por su admiración, el respeto y el afecto hacia el Maestro. Sin embargo a muchos «se les ve el plumero» de la vanidad, el afán de protagonismo, el deseo de honrarse así mismo apoyados en la indiscutible y gran categoría artística y humana de don Antonio. Creemos que es demasiada osadía llamar homenaje, al simple hecho de Ya está bien señores. Ya es hora de que nos unamos todos como un solo hombre con la mejor disposición y buena voluntad, y le tributemos ese gran homenaje nacional que nuestro Antonio merece con creces, y que todos le debemos. poner el nombre de don Antonio Mai-rena en el cartel anunciador, a sabiendas de que no va a estar presente, y cumplir con el envío de la consabida «plaquita» conmemorativa de la efemérides. Quede bien claro que este homenaje, debe de ser con todas las consecuencias a favor del homenajeado y que ahí es donde podrían las peñas y demás entidades flamencas volcarse y demostrar su flamenquísima generosidad en la fila cero del festival que con este motivo se organizaría en el lugar que encontrásemos más adecuado. Antonio Fernández «Fosforito» Cuando alguien me sugirió escribir sobre Antonio Mairena, con motivo de la celebración de sus oficiales cincuenta años dando la cara de frente y por derecho a todos cantes, me llenó de satisfacción por que me es muy gra- to hablar\n\n[ENDING CONTEXT]\n\npor verme rodeado de tantos y tantos amigos entrañables que saben de mi vivir sacrificando todo, viviendo de, por y por el cante, siempre con la obsesión de estar bien, dando lo mejor de mí en cada momento.\n\nPor eso creo que todo esto queridos amigos, desborda mi gratitud, avasalla mis propios merecimientos y entorpece mi palabra, sólo os puedo decir, gracias, muchas gracias con el corazón en la boca.\n\nAntonio Fernández «Fosforito» Mayo, 1981\n\nCANDIL\n\nJ. A. PULPON\n\nESPECTACULOS INTERNACIONALES\n\nO'Donnell, núm. 3-4.º\n\nTeléfs. 22 20 58 - 21 69 20\n\nPARTICULAR:\n\nSEVILLA\n\nTeléfono 27 80 78\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Los escritos de Fosforito",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "34-36",
    "page_number": 34,
    "word_count": 2813,
    "article_char_count_full": 16269,
    "article_char_count_review": 3341,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "Maestro"
      }
    ]
  }
]
```
