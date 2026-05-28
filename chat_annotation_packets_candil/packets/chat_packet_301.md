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
    "article_id": "1995-01-16-left-tangos-de-c-diz-campanilleros-co",
    "article_text_for_review": "Tangos de Cádiz No me hagas más sufrir diciéndome que me vaya. Diciéndome que me vaya no me hagas más sufrir, que de tu vera serrana yo ya no me puedo ir.\n\nCuando me fui de tu vera, me mandaste a llamá, diciéndome que volviera y te echaste a llorar.\n\nVoy a perdé la razón, tus labios me dicen ;vete!, tus ojos dicen que no.\n\nPonte primero de acuerdo con tu corazón, serrana, que, si me voy, ya no vuelvo, aunque me llames mañana.\n\nNo me lo hagas más te pido por Dios, que es peligroso jugá con las cosas del amor.\n\ny a nardos en flor. Allí encontrarás las mujeres más bellas y hermosas que son como rosas de un mismo rosal.\n\nForastero... Forastero que llega a mi tierra, olvida la guerra oyendo cantar. Y su alma se vuelve gitana y, por Sevillanas, se pone a bailar, con gran frenesí. Y fragante de vino y de flores, borracho de amores, se siente feliz.\n\nUn milagro... Un milagro de Sol y Olivares, Cortijos, Parrales\n\ny Almendros en flor. Pajarillos que llenan el campo\n\ncon su alegre canto\n\nde paz y de amor.\n\nY el río, al pasar, Embrujao de amores, soñando,",
    "title": "Tangos de Cádiz-Campanilleros. Coplas",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 201,
    "article_char_count_full": 1060,
    "article_char_count_review": 1060,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-01-17-left-al-mundillo-del-arte-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nExordio del autor al decálogo ideado por el aficionado Manuel Centeno Fernández, miembro de la Peña Cultural Flamenca «Torres Macarena», de Sevilla.\n\nE studiado detenidamente el contenido del decálogo flamenco, fruto de la fina sensibilidad de mi amigo Manuel Centeno, saco la agradable conclusión de que sus diez puntos están impregnados de puros sentimientos y de celo sobre lo que él, tan acertadamente, considera tesoro artístico de su pueblo. En ellos hace responsables solidarios a todos los que dicen sentir el arte flamenco en lo más profundo de su ser, conminándoles severamente para que velen por ese tesoro de incalculable valor que Dios, tan generoso, depositó en el alma del pueblo andaluz: El Arte Flamenco.\n\nEl contenido de los diez puntos felizmente nacidos de la pluma de su autor,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_01 | trigger=\"de verdad\"]\n\nnte, considera tesoro artístico de su pueblo. En ellos hace responsables solidarios a todos los que dicen sentir el arte flamenco en lo más profundo de su ser, conminándoles severamente para que velen por ese tesoro de incalculable valor que Dios, tan generoso, depositó en el alma del pueblo andaluz: El Arte Flamenco. El contenido de los diez puntos felizmente nacidos de la pluma de su autor, está predestinado a ser estudiado por todo aquel que de verdad sienta nuestro arte como cosa de su pertenencia, y estoy convencido de que en un plazo no muy lejano, obrará el milagro que presiento: que el arte flamenco, en sus tres facetas, como símbolo auténtico del alma andaluza, llegue a las Universidades como una asignatura de obligado estudio, al menos en las de Andalucía. Y o, con antecedentes genealógicos andaluces, siento el arte y lo llevo cada día de mi vida a flor de labios, mejor diría a flor de garganta, interpretándolo en los momentos de soledad y en ocasiones en que me siento deprimido. Por ello, el primer día que leí los diez «mandamientos flamencos» me dije: Yerga, este documento de valioso cont\n\n[ENDING CONTEXT]\n\nhacer más extenso este exordio, sólo diré que idéntico espectáculo al de Morón me ofreció Utrera ese mismo año, por lo que me dije: Yerga, se acabaron tus veladas flamencas. Quédate sólo con las de Mairena, Los Palacios y Badajoz, donde, al menos, los asistentes saben estar y decir jolé! en el momento exacto; en ese momento que Manuel Centeno nos pide en su decálogo.\n\nTermino pidiendo a Dios que los «diez mandamientos flamencos» obren el milagro que espero y que tanto necesitamos para que nuestros espectáculos lleguen a situarse al mismo nivel social que la ópera y la zarzuela.\n\nRosario López\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Al mundillo del Arte Flamenco",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1563,
    "article_char_count_full": 9436,
    "article_char_count_review": 2748,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_01",
        "family": "AUTH",
        "trigger": "de verdad"
      }
    ]
  },
  {
    "article_id": "1995-01-18-right-por-malague-as-paco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSiento que mi artículo «Por Malagueñas» (Candil, número 83) no le haya gustado al cantar? aficionado Luis Moreno Pérez, ahora metido —espero que transitoriamente— a crítico para, al dictado de la voz de su amo, enmendarle la plana a todo aquel que ose criticar a su «genial» amigo, el cantaor Diego Clavel. Claro que con amigos como usted, Diego no necesita enemigos. Sus medias verdades y sus mentiras enteras hacen un flaco favor a Diego Clavel que es —así lo dije y lo mantengo—cantaor honrado y profesional, pero que, como artista y hombre público que es, debe aceptar la crítica cuando se equivoque. Y esta vez se ha equivocado. Rectificar es de sabios y es lo que debe hacer. Como yo estoy dispuesto a hacerlo si usted o cualquier otro me demuestra que no llevo razón, pero no con\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"puro\"]\n\nantengo—cantaor honrado y profesional, pero que, como artista y hombre público que es, debe aceptar la crítica cuando se equivoque. Y esta vez se ha equivocado. Rectificar es de sabios y es lo que debe hacer. Como yo estoy dispuesto a hacerlo si usted o cualquier otro me demuestra que no llevo razón, pero no con descalificaciones de taberna, sino con argumentos sólidamente construidos en base a documentos y pruebas contrastadas. Todo lo demás es puro libelo. Mi vanidad está satisfecha y no persigo, por tanto, publicidad alguna con mi desinteresada labor en pro del Arte Flamenco. Sobre mi trabajo al frente de la Federación Provincial de Peñas Flamencas de Málaga, siga indagando y pregunte a los aficionados (artistas, críticos, socios de peñas, etc.) de mi tierra (yo soy andaluz), de esta manera podrá enterarse de cuál ha sido mi trayectoria durante cuatro años al frente de dicha institución flamenca. Afortunadamente «todos», como «pueblo», podemos participar del Flamenco, pese a que personajes como Luis Moreno\n\n[ENDING CONTEXT]\n\nme invita en serio —¡como si esto fuera cosa de broma!— a que estudie y escuche más los cantes de Málaga. Yo, por mi parte, le reto donde, cuando y como quiera a que, usted con sus pruebas y yo con las mías, analicemos, ante los testigos más cualificados que usted y yo consideremos, la obra discográfica «Diego Clavel. 31 Malagueñas». Así y sólo así, se podrá demostrar si mi crítica es una cuestión personal —que no lo es— con el cantaor de Puebla de Cazalla, o si por el contrario estamos ante una obra mal concebida que, sin embargo, no resta méritos al cantaor en otras facetas de su arte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Por Malagueñas Paco",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 2832,
    "article_char_count_full": 17877,
    "article_char_count_review": 2633,
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
    "article_id": "1995-01-20-left-el-an-lisis-contrastivo-en-la-in",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nU no de los temas más apasionantes para cualquier aficionado o estudioso del Flamenco es la recuperación de las raíces históricas, de los patrones musicales y letras originales de cada uno de sus palos, así como de los estilos que los han enriquecido.\n\nPara ello, quienes se han acercado a explorar el pasado, la historia —la prehistoria, si consideramos la ausencia de fuentes documentales escritas—, de estos cantes han dependido generalmente de la memoria de los cantaores que, de generación en generación, han ido conservando los modelos que en cada época consiguieron ser reconocidos como creaciones individualizadas, lo que hoy entendemos por estilos diferenciados: la siguiriya de cambio de Manuel Molina, la de María Borrico, etc., etc., por citar sólo un par de las variantes o estilos que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"transmisión\"]\n\ne los cantaores que, de generación en generación, han ido conservando los modelos que en cada época consiguieron ser reconocidos como creaciones individualizadas, lo que hoy entendemos por estilos diferenciados: la siguiriya de cambio de Manuel Molina, la de María Borrico, etc., etc., por citar sólo un par de las variantes o estilos que por este camino han podido llegar hasta nosotros. Este método de investigación es el que hoy solemos denominar transmisión oral de los cantes. Advertiremos que antes de admitir como legítima una modalidad de seguirya nos hemos preocupado por comprobar su autenticidad, valorando y analizando la fidelidad de la transmisión desde su creador hasta nuestros días. Para ello nos hemos remontado, eslabón tras eslabón, desde la más fidedigna fuente actual (para cada caso) hasta llegar a la fuente materna. Ricardo Molina y Antonio Mairena, aunque conscientes de los riesgos que este método implicaba $ ^{1} $, intentaron aplicarlo con el máximo rigor. Así, a propósito de las seguiriγas, nos dicen $ ^{2} $: Sin embargo, los resultados han sido, en muchos casos, engañosos o inexactos. Muchas de las versiones que se nos proponían como ejemplos de cantes con nombres y apellidos tenían más del mismo Mai-rena que de sus supuestos creadores. Las llamadas de atención sobre la fragilidad de este método de recuperación de formas musicales no tardarían en llegar. En 1970, Elías Terés escribía $ ^{3} $: Todo sabemos que las tradiciones orales, aun concediéndoles un fondo de verdad, deben acogerse con mucha cautela a la hora de redactar una historia rigurosamente documentada. Es necesario acudir a otras fuentes, si las hay. Los riesgos e inexactitudes implícitos en este acercamiento al pasado creemos que se deben, fundamentalmente, a los siguientes factores: 3. Inexactitud en la atribución de los mismos. 1. Dificultades en el aprendizaje de los estilos. 2. Dificultades en su reproducción. 3. Inexactitud en la atribución de los mismos. Con respecto a las primeras, no podemos olvidar que, salvo en casos de convivencia familiar o de una prodigiosa memoria musical, la simple posibilidad de memorizar unas líneas melódicas era mucho más limitada que en la actualidad. Los cantes, los matices personales que aquí nos interesan, sólo podían aprenderse mediante la audición directa cuan\n\n[ENDING CONTEXT]\n\nsencillez con que desarrolla los tercios, gracias, sin duda, a sus envidiables facultades y técnica cantaora. Una imponente conjunción de esencialidad y solemnidad.\n\nEso es todo cuanto, desde el acercamiento científico y riguroso que nos facilita el método que proponemos, se puede decir de dicha seguiriya. Si su creador fue Manuel Cagancho o su padre Antonio, son cuestiones que escapan a cualquier metodología. Y lo mismo puede decir, por mucho que nos duela reconocerlo, sobre las posibles aportaciones que Manuel pudiese hacerle, caso de que su creador fuese, como hoy se dice, su padre.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El análisis contrastivo en la investigación del Flamenco",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "20-22",
    "page_number": 20,
    "word_count": 2561,
    "article_char_count_full": 15725,
    "article_char_count_review": 3948,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "transmisión"
      }
    ]
  },
  {
    "article_id": "1995-01-22-right-som-flamencs-o-el-flamenco-triun",
    "article_text_for_review": "E 1 Flamenco vive en Barcelona una época casi dorada, una época que casi emula en brillantez la de los años veinte y primeros treinta. La Ciudad Condal parece decidida a recuperar la capitalidad del Flamenco en Cataluña. Desde hacía mucho tiempo no contaba —salvo las excepciones de los tablaos y del desaparecido festival de «La Caixa»— con una programación tan rica y variada. Los aficionados han podido disfrutar, de abril a julio, de una sabrosa y atractiva oferta flamenca: la heterodoxa programación del tablao Los Tarantos, la del Teatro Victoria, la de —y van tres años ya— los viernes de L'Eixample, la del Jazz Sí Club: «Músiques del mon», y la del Grec-94. «Flamencos de Barcelona».\n\nAhí es nada. Y olvidamos adrede la variadísima y constante programación de las peñas flamencias. ¿Quién dijo que Cataluña vive de espaldas al Flamenco? Probablemente sería alguien que quiso hacer verdad la famosa frase de Valle Inclán: «Sólo es verdad aquello que se recuerda», esforzándose por olvidar. Un inútil e interesado esfuerzo que para nada sirve ya que la realidad es una verdad irrefutable.\n\nTarantos\n\nLos Tarantos, histórico tablao de la Plaza Real, ha iniciado tras su reapertura una nueva etapa que combina la actuación diaria del cuadro de la casa, dirigido por Yunko Watanabe, con recitales de artistas con prestigio y tirón promocional en los fines de semana. Así, en abril, ofreció las actuaciones de Loli Carmona que vino acompañada de José Soto «Sorde-\n\nrita» y Pepe Habichuela, del gran José Menese, acompañado a la guitarra por Juan Carmona «Habichuela», de Dieguito con la guitarra de Antón Giménez. En mayo, Yolanda González nos permitió disfrutar con el cante de Talegón de Córdoba, desgraciadamente poco conocido y valorado por la afición de aquí. También la acompañaron Morenito de Illoray Juan José Amador; Faraón volvió a demostrar su versatilidad de bailaor y cantaor acompañado por Montse Cortés, Manuel Castilla y Salva de María. En junio tuvo cabida, dentro de esta ecléctica programación, el baile de la joven Mónica Fernández. La bailora de Cornellá está empeñada en ser figura y seguro que lo logrará. Cuida y prepara con mimo cada una de sus apariciones en público. Para la ocasión se hizo acompañar en el cante por Chiquí de la Línea y Román, por Julián Navarro «El Califa» y Manuel Castilla a la guitarra, Domingo Patricio a la flauta y Quirós, hijo a las palmas. Su reciente premio en La Unión ha venido a confirmar su decidida progresión.\n\nMayte Martin\n\nEs, sin duda, la cantaora más emblemática de Cataluña. Con su irresistible personalidad ha recogido toda la tradición del cante y ha sabido situarlo en sus recursos expresivos, su precisión y su ductilidad expresa. Mayte es una cantaora larga, completa y siempre inconforme y perfeccionista. Su aprendizaje ha sido duro y fértil. Tiene un profundo y estricto conocimiento de la arquitectura del cante. Lo ha ganado en horas innumerables de aprendizaje y con coraje. Está llena de un enorme sentido común y de sinceridad artística. Sabe lo que quiere y es tozuda para lograrlo.\n\nComo además es una trabajadora incansable, llena de vigor y coraje, le debemos que en Barcelona se haya alzado un espacio estable para el Flamenco; L'Eixample.",
    "title": "«Som Flamencs» o el Flamenco triunfa en Barcelona de nuevo",
    "periodical": "candil",
    "issue_id": "1995-01",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 534,
    "article_char_count_full": 3227,
    "article_char_count_review": 3227,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
