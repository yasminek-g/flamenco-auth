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
    "article_id": "1998-03-24-left-carmen-linares-como-siempre-sobe",
    "article_text_for_review": "Francisco Hidalgo Gómez\n\nEn 1994, Carmen Linares grabó un disco espléndido: «Canciones populares antiguas recopiladas por Federico García Lorca». Ahora, cuatro años después, forman parte del espectáculo, que durante la segunda quincena de marzo nos ha ofrecido el teatro Tívoli de Barcelona, «Un rato, un minuto, un siglo... con Federico García Lorca». José Sámano, responsable del guión y de la dirección, ha completado el espectáculo con algunos versos del poeta granadino y opiniones de poetas, artistas e intelectuales (Neruda, Alberti, Dalí, Cossío, Buñuel, Aleixandre, Moreno Villa, Dámaso Alonso, Cernuda, Machado...) sobre su persona, su música o su poesía que dice entregadamente la espléndida actriz Lola Herrera.\n\nEl material elegido configura una visión de García Lorca en su más estrecha relación con lo popular; una visión, también, del enorme potencial creativo de este gran poeta y dramaturgo, de su capacidad de seducción, del entusiasmo que provocaba cada vez que se sentaba al piano e interpretaba alguna de las canciones que grabaría con su comadre, la gran Encarnación López, «La Argentinita», y que son parte esencial del espectáculo.\n\nLola Herrera recita los textos con una exquisita devoción y con eficacia. La actriz de «Cinco horas con Mario», magnífica por momentos, se mete en la piel del poeta, desentraña sus versos con variedad de registros y salva con pundonor y entrega la lectura de los textos. Su decir es perfecto, su voz acaricia las palabras, su gesto contenido, justo, preciso; y sus manos, cuando las mueve, hablan. Llena ella sola la escena, y en el montaje la han dejado realmente sola, nada arropada. En los momentos más dramáticos nos estremeció literalmente, sobre todo al recitar «Gacela de la muerte oscura»:\n\nQuiero dormir un rato,\n\npero que todos sepan que no he muerto. Pero cuando el espectáculo alcanza cotas incendiarias es cuando canta Carmen Linares. Está en todo momento soberbia, algo a lo que, por otra parte, nos tiene sobradamente acostumbrados. Se basta y se sobra ella sola para emocionarnos. Derrocha arte, buen gusto, saber y su andalucísima voz provoca verdaderos estremecimientos de emoción, trasmina esencias. ¡Qué variedad de matices y colores tiene su voz! Hace todo un derroche de flamenquería en «Anda Jaleo» y alcanza cuotas sublimes con la segunda parte de «Nana de Sevilla», que canta «a capella». Carmen Linares es todo un lujo, siempre. Traspasa las candilejas y te calienta dulcemente el corazón.\n\nComo valor añadido, que no es menor, anotemos el decidido aire flamenco que le ha imprimido a las canciones de Lorca, cantadas por ella tienen un sabor nuevo, sin que por ello pierdan nada de su esencia popular. A cada canción le aporta el tono justo, cada una la interpreta con el aire preciso, muy sabrosamente, muy flamencamente. Los arreglos musicales son valientes, gustosos y muy actuales, de una mediterraneidad total. Es Carmen Linares mucha cantaora y mucha artista. Y como he dicho en alguna otra ocasión, cualquier cosa cantada por ella me gusta más.\n\nEl espectáculo, por fin, hace de su propia sobriedad una baza a su favor y del trabajo de los intérpretes una fuerza escénica. Discurre sin provocar el mínimo cansancio. Contiene algunos aciertos incuestionables. Así el juego de luces que crea una atmósfera perfecta o el ofrecer alternadamente «Llanto por Ignacio Sánchez Mejías» con la canción «Café de Chinitas». Justo es también remarcar el alto nivel de los seis músicos que participan. Decididamente, un espectáculo que uno recomienda abiertamente.",
    "title": "Carmen Linares, como siempre, soberbia",
    "periodical": "candil",
    "issue_id": "1998-03",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 565,
    "article_char_count_full": 3542,
    "article_char_count_review": 3542,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-03-24-right-fernanda-y-bernarda-giraldillas-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Peña Narváez\n\nFernanda y Bernarda Jiménez Peña, nacidas en Utrera, hijas de José Jiménez Fernández y de Inés Peña Vargas, naturales de Utrera, nietas de Andrés Jiménez Torres, de Utrera y de Aurora Fernández Camacho, de Alcalá, en línea paterna, y de Fernando Peña Soto, de Lebrija y de Josefa Vargas Torres, de Utrera en línea materna, son además descendientes de un largo etcétera de una larga dinastía de familias gitanas que saben y conocen las veredas y los secretos de lo que es y significa el mejor arte.\n\nUtrera, pues, ciudad alegre, sana, generosa, limpia y clara como las aguas de sus viejas fuentes, pueblo de gracia, de arte y de duende, es el crisol en el que se fundió el arte incommensurable de estas dos artistas, es el cofre dorado que guarda y conserva el valor incalculable\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"intim\"]\n\nfica el mejor arte. Utrera, pues, ciudad alegre, sana, generosa, limpia y clara como las aguas de sus viejas fuentes, pueblo de gracia, de arte y de duende, es el crisol en el que se fundió el arte incommensurable de estas dos artistas, es el cofre dorado que guarda y conserva el valor incalculable del poderío de esta riqueza. ¡Resulta difícil hablarles de Fernanda y Bernarda!... Sus biografías las conocemos bien todos. Nacieron al cante en la intimidad de una familia gitana, bajo la mirada celosa de un padre que no admitía, de ninguna de las maneras, que sus hijas fueran artistas. En vida sólo accedió por aquel respeto y admiración que le causaba Antonio Mairena, a que, de su mano y confianza, grabaran un disco, y así en aquel «Sevilla, cuna del cante», en el que Bernarda hace una antológica versión del aire inconfundible e inigualable de la bulería de Utrera y Fernanda escribe el cante de la Serneta como hasta entonces no lo había hecho nadie, incluyendo las más prestigiosas voces que tuvieron la dicha de recibir la transmisión directa, así repetimos, el mundo del flamenco supo, en estampación sonora, que Utrera tenía el secreto, que guardaba el tesoro y que en Utrera dormía el misterio de la criaturización del cante. Por supuesto, la verdad de esta fuente ya la conocían con anterioridad Antonio Mairena y el genial Manolo Caracol y así ambos, en interminables ocasiones, venían hasta el pueblo para saborear y beber en el néctar sabroso y dulce del compás y de la pureza\n\n[ENDING CONTEXT]\n\nme dejen de pamplina No me gustan las rutinas de los peces de colores, de tantos sabios doctores apostados en esquinas. El cante es oración canora y no hay voces más senoras que las que salen de «aquí». Poco se me importa a mí la línea corredentora, si el corazón no te llora, si no te hace sufri. Fernanda y Bernarda son el más puro sentimiento que te marca el diapasón del quejío y del lamento donde vibra el corazón. Por eso, mi devoción hacia ellas, es permanente, por ser una cuestión de fé. Ya lo dice la Serneta: «Yo nunca a mi ley falté, pues las tengo tan presentes, como la primera vez».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Fernanda y Bernarda, «Giraldillas Flamencas»",
    "periodical": "candil",
    "issue_id": "1998-03",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "24-26",
    "page_number": 24,
    "word_count": 1399,
    "article_char_count_full": 8143,
    "article_char_count_review": 3120,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "intim"
      }
    ]
  },
  {
    "article_id": "1998-03-26-right-el-cante-por-verdiales",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJosé Núñez de Castro Gómez\n\nMálaga cantaora, tal como la definió Manuel Machado, hace honor a su más profunda esencia de pura raigambre flamenca en su más popular manifestación, como es el fandango —de origen árabe—, que encierra en sí una pluralidad de cantes diferenciados, según la región o comarca, que ciñéndonos al tema que nos ocupa, es el incomparable «fandango mala-gueño» por excelencia, dentro del cual se establecen unos estilos concretos y medidos, de gran riqueza artística, tanto en su melodía como al acompañamiento: la jabera, la bandolá, la malagueña —elevada a la más alta categoría del cante—, la rondeña y los verdiales.\n\nComo malagueño de nacimiento, e interesado por todo lo cultural y musical, desde mis primeros años de juventud, sentía la curiosidad por conocer y vivir en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"ejemplar\"]\n\nalsa una «fiesta por verdiales», y satisfice mis deseos con una inolvidable vivencia que tuve en un 28 de diciembre (festividad de los Santos Inocentes, en plenas Navidades); también se celebran estas fiestas en la noche de San Juan, en la explanada de la antigua y típica «Venta del Túnel», en el pantano del Agujero, a la entrada de la capital por la zona norte —ahora en la «Venta San Cayetano»—, con motivo de su fiesta mayor, organizada por la ejemplar y edificante Peña Juan Breva, lugar de «encuentro de choque», rito ancestral de las distintas «pandas» procedentes de Almogía, Comares, Cómpeta, Lagares de Alora —el fandango de Lagares—, Cártama, Coín, etcétera, y en definitiva de los Montes de Málaga, en el Partido de Verdiales, de donde este cante toma su nombre, y lugares cercanos a él, como Santa Catalina, Venta Larga, Tres Choperas y otros que, como bien dice Pepe Luque Navajas investigador y experto en estos cantes, «donde\n\n[ENDING CONTEXT]\n\ncon las mismas medidas y los mismos compases. Un cante al servicio del baile, al igual que en los verdiales, el baile es el protagonista. Ello no quita reconocer la universalidad del baile sevillano, extendido por el mundo entero y altamente cotizado a nivel internacional.\n\nEs de desear, para todo amante de esta riqueza artística, comiencen a demostrarse, a expresarse con los niños de corta edad de la Málaga cantaora y bailaora, para llegado su momento, el baile por verdiales que como sabemos, exige la participación de una pareja como mínimo, sea el más bello exponente de su música popular.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El cante por verdiales",
    "periodical": "candil",
    "issue_id": "1998-03",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "26-27",
    "page_number": 26,
    "word_count": 1147,
    "article_char_count_full": 6937,
    "article_char_count_review": 2570,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "ejemplar"
      }
    ]
  },
  {
    "article_id": "1998-03-27-right-flamenca",
    "article_text_for_review": "José Luis Buendía López\n\nE l auge que han alcanzado los estudios flamencos, ha hecho necesaria una recapitulación que afrontara los casi doscientos años (casi los mismos que nuestra Historia flamenca) en los que, autores muy diversos, de dentro y fuera de España, imbuidos por concepciones distintas de lo jondo y hasta por ideologías diferentes, pero atrincherados todos en un común amor hacia este arte, nos hemos dedicado a los estudios sobre el mismo, a explorar con nuestra palabra, los mil entresijos que se abren a la hora de dilucidar teorías y aproximar posturas.\n\nAsí, convencidos de que prestamos un servicio al Flamenco y a la Cultura en general, somos varias generaciones las que llevamos escritos miles de trabajos acerca de los hechos flamencos, la biografía de los intérpretes, la periodización que podemos apreciar a lo largo de sus dos siglos de existencia, etc.\n\nESCENAS ANDALUZAS,\n\nRIZARRIAS DE LA TIERRA.\n\nALANDES DE TOBOL, BASCO POPULARES, CAUSOS DE CONTEURAS Y ARTICULOS VARÍON, QUE DE TAL Y CUAL MATERIA, ALIBA Y ENFONCES, ADIET A CERLÁL T PO DEVERHO SON T CONFÁS, ALIQUE SEMPRE POR LO ESPAÑOL Y CASTERI BA DADO A LA ESTANZA EL SOLITARIO, somete aos relatos i no corpo, cumplanz, enquêchos ao mario de o se o de milho por al calado y enero de algués almóadas. EDICIÓN DE LULO ADOREBA CON DISETO Y VILLA T CIGO DISEJO POR D. F. LAMEYER.\n\nFlamencología\n\nLo que, en principio, no era sino una afición humanística y social, antropológica y folklórica, tenida por extravagante por los que no nos entendían, hoy constituye una ciencia más, que valientemente fuera bautizada por uno de nosotros, Anselmo González Climent, con la audacia de los pioneros, con el arriesgado nombre de “Flamencología”.\n\nPues bien, para poner al día los estudios bibliográficos sobre el Flamenco, fuimos convocados ocho profesionales, estudiosos del mismo, por el Centro Andaluz de Flamenco, a unas jornadas que analizaron el pasado, el presente y el porvenir que aguarda a nuestras inquietudes sobre tan difusa y complicada materia.\n\nLas primeras jornadas sobre bibliografía flamenca, han tenido lugar en Sevilla, en las postrimerías del pasado año, magnificamente coordinadas por nuestra compañera, la doctora Cristina Cruces Roldán.\n\nEn ellas, y durante un gratísimo fin de semana, en el que apenas nos dimos un respiro, los investigadores\n\nconvocados hemos vertido nuestras experiencias hablando cada uno de un libro emblemático dentro de la historia jonda (obvio es advertir lo limitado de la selección), que previamente había sido nominado en reñida lid, dimanada de una amplia consulta evacuada a destacadas personalidades expertas en la materia.\n\nEl análisis que cada uno de nosotros realizamos acerca del libro encomendado, y que cubre, desde las primeras indagaciones costumbristas de Estébanez Calderón a nuestros días, junto con los debates que cada uno de los análisis particulares motivaban al resto de los compañeros, será objeto de un libro de próxima aparición, editado por el Centro Andaluz de Estudios Flamencos, que sin duda ha de resultar de enorme interés y del que tendremos informados, en el momento de su pública aparición, en el número correspondiente de nuestra revista Candil.\n\nFERNANDO EL DE TRIANA ARTE Y ARTISTAS FLAMENCOS",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1998-03",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "27-29",
    "page_number": 27,
    "word_count": 521,
    "article_char_count_full": 3249,
    "article_char_count_review": 3249,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-03-28-left-discograf-a-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Valera Espinosa\n\nTítulo: «Agujetas en la soleá»\n\nCante: Manuel de los Santos Pastor «Agujetas»\n\nToca: «Curro de Jerez»\n\nProducción: Francisco Alcolea y Julián Sanz\n\nTécnico grabación: Manuel Gu- tiérrez\n\nReferencia: ALIA Discos para Turismo Andaluz\n\nNúclima vez que Manuel grabó, ya que hace demasiado tiempo que sus grabaciones no me llegan por ningún conducto. Y eso que siempre he sentido emoción con su arte, desde que le escuché su disco «Cantes Gitanos», grabado allá por el 1972. Más tarde vendría a Jaén y disfrutamos de su cante y de su comportamiento anárquico.\n\nQuizás esta introducción esté de más, pero quiero resaltar con ella que, particularmente para mí, Agujetas, dentro de los especiales parámetros que circundan su personalidad, ha sido y es, un cantaor de rajo profundo y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionados\"]\n\ntarde vendría a Jaén y disfrutamos de su cante y de su comportamiento anárquico. Quizás esta introducción esté de más, pero quiero resaltar con ella que, particularmente para mí, Agujetas, dentro de los especiales parámetros que circundan su personalidad, ha sido y es, un cantaor de rajo profundo y añejo sentimiento jondo. Y ahora, de pronto, aparece un compacto de esos considerados independientes; de los que se efectúan gracias al esfuerzo de aficionados que piensan que este arte también se puede difundir y producir sin nada de marketing, ni de presiones al artista para que grabe comercialidad. Que piensan con acertado criterio que el flamenco es algo natural, simple y enormemente bello. Y lo más loable, que las cualidades de naturalidad, simpleza y belleza se pueden plasmar sin más. El repertorio de este trabajo es el clásico de Manuel. No se sale un ápice de sus soleares, bulerías —«pá'cuchar», como él suele decir—, siguiriγas, fandangos, cantes de fragua y tarantos; quizá le falte sus tientos-tangos. Y como corresponde a su forma\n\n[ENDING CONTEXT]\n\nen un cantaor jerezano, los tonos caracoleros afloran con los matices de «Agujetas» en los fandangos. Los tarantos mantienen la impronta camaronera adobada de matices jerezanos aunque sin acertada resolución.\n\nAfina más en los cantes a compás y es por soleá donde muestra sus mejores armas flamencas: queja, rajo en la voz y entrega. Otro tanto sucede en las siguiriyas, donde las formas de su casta cantaora asoman con rotundidad y la influencia del genio Manuel Torre toma carta de naturaleza en la estructura del estilo. Finaliza el trabajo con martinetes en la más pura línea de su tierra.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1998-03",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "28-29",
    "page_number": 28,
    "word_count": 1273,
    "article_char_count_full": 7798,
    "article_char_count_review": 2676,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionados"
      }
    ]
  }
]
```
