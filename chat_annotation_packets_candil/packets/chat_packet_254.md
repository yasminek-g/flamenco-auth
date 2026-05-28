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
    "article_id": "1992-05-23-left-prodigio-del-cante",
    "article_text_for_review": "(Una noche con El Niño Martos)\n\nAlfonso Yuste\n\nE1 Cante, no el «Canto», homónimos muy aproximados pero con aplicación distinta; el tema, el escenario, la audiencia y el íntimo sentir del medio los separa, les hace seguir sendas musicales y expresiones psíquicas no paralelas. Digo el «Cante» y no el «Canto», porque trataré del «Cante» si prosigo escribiendo como es mi intención. Cualesquiera de los dos producen, penetran agudos estremeciendo médula abajo, desviando al corazón sus radiaciones para extenderlas por el área sin metro de las almas.\n\nCantar. El pueblo que canta no muere. Axiomático. (No descubro el axioma). El hombre lo sabe. Y el pájaro. Machado lo avala. Lo signa la matemática del poema. Y así se manifiesta hasta en lo más crudo.\n\nVaya para «Candil» la anécdota y el resultado en ella contenido. Y por «El Niño Martos».\n\nA sí fue: Octubre de 1938. Noche negra hecha blanca; una «cuchillada» de luz toda «Cante», quitó lastre a las sombras; y a las vísceras españolas enfermas de odio. Terapeuta El Niño Martos. Ignoro si podrás responder; ¡está! desde la fila de los vips. Así sea. Te recuerdo. Recuérdame. Formábamos parte de uno de los ejércitos de la España dividida. Incivil la lucha. Lugar: a unos doce kilómetros de Belalcázar; zigzagueaba el frente a ambas márgenes del río Zújar. Paradójicamente, nosotros estábamos en la orilla derecha. Nuestros oponentes a la izquierda. No lejos Sierra Trapera. No hacen falta más detalles; tu memoria ha de situarte en el escenario con la breve orografía reseñada. Tú y yo. Tu voz galvanizada en aires y acentos andaluces; yo megáfono en mano. El coloquio con nuestros hermanos adversarios, a punto. Intercambio de balas, borrasca no muy intensa; los cuerpos, por trincheras guarnecidos. Sin más novedad. Sí. Aquella noche parecía faltarle espacio a las estrellas.\n\n—Niño, preparate. —Lo estoy.\n\nUn teniente puso un tocadiscos sobre una ametralladora. Resbaló un chotis del disco. Fue subrayado con una andanada de tiros. No sorprendió el aplauso. Hablé yo y me escucharon, sí, pero con remilgos y alguno que otro aviso, bronce ardiendo imponiendo punto en cualquier período, importándole poco la sintaxis de mi parrafada.\n\n—¿Dónde está el Niño Martos? Estaba agazapado. Le tocó el turno a él. El dio en el clavo. Cantó. Gloria bendita y después, paz. A falta de guitarra una ametralladora inició el rasgueo. Al segundo júpio hasta Dios se puso a escuchar.\n\nTres coplas, cuatro. Acabaron con los fogonazos aureolas del odio. Como respuesta brotaron los olés, intermitentes, coreados, ecos del verdadero sentimiento albergado en todos los corazones. A izquierda y derecha del Zújar el cante hizo el prodigio y el resto de la noche transcurrió sin disparar. Al amanecer la diana corrió a cargo de las alondras; —alguna quedaba viva—. ¡Qué bendición!\n\nMás de ciento veinte olés se repitieron a la amanecida. Procedían de una compañía de enfrente sustituida por el relevo.\n\nDonde estés, Niño Martos, recuerda: Cantaste una saeta. Al otro lado con asombro recibida. Recibe ahora un saetazo con el timbre de tu voz y el fuego de mi sentimiento. Compartelo con todos los que cantan.",
    "title": "Prodigio del cante",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 515,
    "article_char_count_full": 3142,
    "article_char_count_review": 3142,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-05-23-right-aportaci-n-a-la-figura-de-luis-m",
    "article_text_for_review": "Dedicado a mi peña flamenca de «Juan Breva», de Málaga\n\nDesde siempre vengo observando cuán enormes son las dudas que existen en cuanto a la raza y a la naturaleza del que fue famoso guitarrista, en la década de los años diez, Luis Molina. El que acompañó, como nadie, a la sin par Pastora Pavón «Niña de los Peines» y a Manuel Pavón Varela «Maneli» en su cante por la petenera del jerezano José Rodríguez de la Concepción «Viejo Medina». Luis vivió soltero y en solitario en la calle de Avemaria, número dieciocho, de la villa de Madrid. Ganó un buen dinero que supo administrar, como ningún otro artista lo hizo en aquella época. Fue, sin duda, el guitarrista mejor cotizado del momento. Contaba tan sólo treinta y cinco años de edad, cuando se compró un vehículo automóvil, que él mismo conducía. Un mal día se le ocurrió hacer, en solitario, un viaje a San Sebastián, viaje que sería, fatalmente, el último, porque a su regreso, en el kilómetro siete de la carretera de Alsasua a Guipúzcoa, a las seis menos cuarto de la mañana del día veintinueve de agosto de 1919, chocó frontalmente contra un árbol, empotrándosele el volante en el pecho, circunstancia que le causó la muerte en el acto.\n\nEl doctor don Francisco Iraola López de Gonochea, junto a su compañero, el forense don Jacinto Aguinaga Munárriz, practicaron la autopsia a Luis, y su cuerpo sin vida fue sepultado en el cementerio cristiano de Alsasua.\n\n¡Pobre Luis! ¡Quién le iba a decir que en ese triste momento se vería sin ningún amigo a su lado! Ni siquiera su hermano Antonio supo del nefasto accidente y ello es así porque lo justifica el hecho evidente de que tuvieron que testificar ante el juez, varios hijos de Alsasua. ¡Así es esta perra vida! Rita Giménez García «La Cantaora» fue enterrada en el minúsculo pueblo de Zorita del Maestrazgo (Castellón), «El Corruco» en Balaguer (Teruel) y «Fosforito», ¿dónde?\n\nNo dudo que el finado, en las alturas, junto a Javier Molina, Ramón Montoya, Manolo de Badajoz, su hermano Antonio y tantos otros guitarristas, estarán situados a la derecha del Padre, en un lugar preferente. ¡Descansen en paz!\n\nFicha biográfica\n\nLuis-Casimiro Molina Jiménez. Especialidad, guitarrista. De raza no gitana. Natural de Madrid. Hijo de Asunción Molina Jiménez, natural de Antequera (Málaga). Nieto, por línea materna, de Antequera. Nació el día cuatro de marzo de 1883 en la calle Barquillo, número 34-4.º, principal izquierda. Luis tuvo un hermano más joven que él, guitarrista, se llamó artísticamente Antonio «El Jerezano». Residió y falleció en Sevilla. Rosario López\n\nTeléfono (953) 253139",
    "title": "Aportación a la figura de Luis Molina Manuel",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 436,
    "article_char_count_full": 2595,
    "article_char_count_review": 2595,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-05-24-left-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«El folk-lore andaluz», n.º 7 (Homenaje a Paco Vallecillo) Fundación Machado, Sevilla, 1991\n\nDecir que los estudios folklóricos «están de moda», como no hace mucho léíamos en una publicación de difusión nacional, me parece como mínimo una frivolidad; es como asegurar que lo están las matemáticas, la física o la historia, disciplinas en las que no cabe otra cosa sino continuar las líneas de investigación ya abiertas en el pasado y que, a base de esfuerzos y sacrificios, han de repercutir, de manera decisiva, en la planificación futura de los programas y en la vinculación de éstos con la sociedad. No de otra forma sucede con el folklore, disciplina que, desde su aparición, de la mano de los postulados populares del último romanticismo, con su secuela de exaltación de lo local, no ha dejado\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicación\"]\n\nad. No de otra forma sucede con el folklore, disciplina que, desde su aparición, de la mano de los postulados populares del último romanticismo, con su secuela de exaltación de lo local, no ha dejado de progresar, y en cuya redefinición española tuvieron un papel destacadísimo figuras egregias como la de don Antonio Machado y Alvarez, «Demófilo», cofundador precisamente de la primera etapa de «El Folk-lore andaluz», de la cual es continuadora la publicación que hoy reseñamos. Y aunque, en principio, Flamenco y Folklore son dos disciplinas no necesariamente unidas, y hasta la inclusión de la primera en el ámbito de la segunda es objeto de fuertes polémicas, hoy, con motivo de la publicación del número siete la revista, no tenemos más remedio que hacernos eco de su contenido, puesto que se trata de un emotivo y respetuoso homenaje a nuestro querido Paco Vallecillo, figura incommensurable para el flamenco, a la que, c\n\n[ENDING CONTEXT]\n\nAguilar de la Frontera, o que apreciamos uncidos a la estética más pura de nuestro ancestral cante jondo, van ocupando con gran dignidad los huecos humanos de este nuevo cancionero flamenco que acaba de nacer, pero que no dudamos ocupará en poco tiempo el lugar que le corresponde, que no es otro sino el servir como base letrística para desembocar en la explosión de un cante, a pesar del riesgo, ya anticipado por Manuel Machado, de olvidarse del nombre de quien lo compusiera, algo que nosotros intentamos desde aquí evitar, recomendando previamente la lectura de este hermoso joyel de emociones.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 1229,
    "article_char_count_full": 7516,
    "article_char_count_review": 2561,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicación"
      }
    ]
  },
  {
    "article_id": "1992-05-25-left-flamenca",
    "article_text_for_review": "Curiosidad y expectación sentía por escuchar el nuevo trabajo de Camarón con Paco de Lucía tras bastantes años de disociación flamenca. Nuevamente la modernidad (flamenca? se apoderara del criterio artístico de los intérpretes, ¿o mejor de Paco de Lucía?, para presentar un trabajo que está en una línea continuadora de lo efectuado entre las dos figuras en los últimos años de su colaboración discográfica. Y aludo al criterio del guitarrista de Algeciras, porque tras escuchar los trabajos del de la Isla con Tomatito, se aprecia determinada diferencia que me predispone a pensar que la dirección artística del primero es rotundamente efectiva.\n\n¿Se transforma Camarón cuando entra en un estudio de grabación? o ¿lo transforman? Me hago estas dos preguntas porque una vez escuchado el trabajo «Flamenco vivo», grabado en directo por los festivales de Bornos, San Fernando, Jerez, Puebla de Cazalla, Tomares y el barrio de Triana, durante el verano de 1987, se denota la inclinación del cantaor por una determinada ortodoxia que nada tiene que ver con lo efectuado en los estudios de grabación. ¿Cuál es el auténtico arte de Camarón? Escribe Ricardo Pachón en la carpeta de «Flamenco vivo» en referencia a lo contenido en el disco: «Estamos ante la verdad del cante actual. La soledad del cantaor. El cuerpo a cuerpo con su público. El flamenco de los ochenta».\n\nP or otro lado, escuchando igualmente lo más novedoso del cantaor antes de este «Potro de rabia y miel», su «Soy gitano», dentro de la modernidad que supone el estar arropado por una orquesta sinfónica, si es que se puede llamar arropo a un acompañamiento, presumo, efectuado tras haber realizado\n\nlos cantes el artista, encuentro que en la grabación que da título al trabajo, Camarón, creo que en un intento de no perder la línea clásica, canta con su acostumbrado melisma por tarantos. O como en «Dicen de mí», bulerías por siguiriyas según la carpeta, el de San Fernando de sarrolla un compás de los más ortodoxo. Similar tónica utiliza en los fandangos de Huelva titulados «El pez más viejo del mundo». La raíz permanece y se identifica.\n\n¿Qué pasa en este «Potro de rabia y miel»? Pues que Camarón se deja llevar, insisto, por los criterios artísticos de «Los Lucía» en un afán de configurar una modernidad que abra nuevos caminos hacia una comprensión más amplia del flamenco. Mas ¿es éste el adecuado y verdadero fin? Tiempo al tiempo. Particularmente escucho más flamenco en los dos trabajos antes citados que en éste. Aún y a pesar de todo, en las grabaciones por bulerías y tangos existen determinados tercios en los que Camarón, gracias a su subconsciente, muestra la raíz flamenca. No sucede lo mismo en la rumba, donde a veces existe un matiz salsero. Por último, no comprendo cómo a estas alturas a una grabación en la que impera más el matiz por taranto y cartagenera se le denomina tarantas, a no ser que se quiera reivindicar a la última como el cante matriz del primero y la segunda. Destacar también el virtuosismo de Paco de Lucía y de Tomatito.",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 513,
    "article_char_count_full": 3029,
    "article_char_count_review": 3029,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-05-25-right-de-la-teor-a-a-la-pr-ctica",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHacia una renovación pedagógica del Flamenco\n\nPonencia presentada en el XIX Congreso Nacional de Actividades Flamencas (Linares)\n\nPhilippe Donnier\n\nE n los Congresos de Córdoba, Jerez y Badajoz, tuve la oportunidad de exponer elementos para una musicología del Flamenco fundamentados tanto en una reflexión teórica sobre una semiología específica como sobre una ayuda tecnológica. Dentro de las polémicas diversas levantadas por mis trabajos una pregunta (implícita o explícita) se ha planteado constantemente:\n\n—¿Para qué sirve aquello?\n\nUna primera contestación, propia del investigador encerrado en su torre de marfil, sería:\n\n—No tiene por qué servir, la investigación fundamental tiene su meta en su propio desarrollo y en la lógica o verosimilitud de sus proposiciones... hasta que se\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"práctica\"]\n\nestre lo contrario... Otra postura consiste en buscar aplicaciones en el mundo diario que vayan más allá de las simples confirmaciones de «laboratorio». Las ciencias «duras» (Matemáticas, Física) son muy exigentes en cuanto a estas confirmaciones, mientras más se desplaza la investigación hacia las ciencias «blandas» (Sociología, Psicología, Etnología, etc.) más difíciles se hacen las confirmaciones de «laboratorio» y por tanto las aplicaciones prácticas. La «clientela» (lectores de artículos, auditores de conferencias, críticos...) se acostumbran también a rebajar su nivel de exigencia, dejando pa- so al intuicionismo y al impresionismo emocional que le sirven de medida para enjuiciar a las teorías propuestas. Cuantas teorías económicas, psicológicas, políticas, sobreviven con vigor y hasta con soberbia a los más estrepitosos fracasos prácticos... A primera vista la música, y más todavía el Flamenco, aparece como un terreno de estudio eminentemente «blando» y, como pasa en la mayoría de las ciencias humanas, puede dar lugar a lucimientos retóricos intuicionistas capaces de desencadenar discusiones sin fin y enfrentamientos apasionados. No obstante, más que cualquier otro arte, la música es susceptible de ser representada casi en su totalidad por medios gráficos propios de las matemáticas (ver ponencias anteriores). Esta «representabilidad» permite esquematizar las producciones Flamencas con fines analíticos. Una vez realizado un juego bastante completo de esquemas\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_03 | trigger=\"guía\"]\n\ne en sus pretensiones. La ciencia es voluntariamente y metodológicamente reducciónista y simplificadora en sus teorías y experimentos e incurre sistemáticamente en «falsedades» si se quiere enjuiciar sus resultados con la vida real cotidiana. Es por tanto falsa e inoperante? Galileo ha tenido que prescindir del aire para «imaginar» su teoría de la gravedad y hemos tenido que esperar a Newton para confirmar experimentalmente las leyes físicas que guían la caída de los cuerpos en el vacío con un experimento de «laboratorio». Ahora, es una perogrullada reconocer que nadie domina las leyes que guían una maceta real que cae de un balcón real en la real cabeza del pobre transeúnte que paseaba por real casualidad en ese «dichoso» momento. —¿A quién se le ocurriría pedirle cuenta a Galileo o a Newton por no haber previsto este acontecimiento real? —¡No le pidamos peras al olmo! La investigación d\n\n[ENDING CONTEXT]\n\nde las dos cartas de alumnos, la segunda siendo de carácter más personal carente de crítica formal.\n\nDe la teoría a la práctica, gracias a múltiples colaboraciones (no puedo olvidar al joven crítico de Flamenco Francisco Martínez, que me ha proporcionado cientos de grabaciones que han servido de base para todas mis transcripciones, y a otros tantos que me animan en este trabajo a veces tan árido...), una nueva etapa se abre en la pedagogía del Flamenco, aquí en Andalucía..., hace tiempo que, bajo otros cielos, se usa de una forma u otra el asesoramiento musical para la enseñanza del Flamenco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "De la teoría a la práctica",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "25-27",
    "page_number": 25,
    "word_count": 2980,
    "article_char_count_full": 19409,
    "article_char_count_review": 4075,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "práctica"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "guía"
      }
    ]
  }
]
```
