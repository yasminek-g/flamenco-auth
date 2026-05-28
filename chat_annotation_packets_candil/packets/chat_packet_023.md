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
    "article_id": "1981-03-13-right-el-sentimiento-amoroso-en-el-pri",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor José Luis Buendía\n\nRAZONES PARA UN ESTUDIO\n\nProbablemente sea el amor entre la pareja el sentimiento más universal de todos los tiempos. Miles y miles de páginas se han dedicado a estudiarlo desde numerosos y, a veces, complementarios puntos de vista. Pero hay una constante en casi todos esos estudiosos: los comportamientos amorosos objetos de atención han sido totalmente atípicos y poco representativos del sentir de una mayoría. Podemos poner infinidad de ejemplos de nuestro aserto: así para definir el amor en el periodo renacentista, uno de los que más han profundizado en la moderna caracterización del sentimiento erótico, se ha acudido a la noción de «amor sublime», acuñado por Dante y Petrarca entre otros, desde sus innegables posiciones históricas y de clase que no cabe analizar\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\ne «amor sublime», acuñado por Dante y Petrarca entre otros, desde sus innegables posiciones históricas y de clase que no cabe analizar en este momento, y continuado en toda Europa por una legión de escritores que han ido modelando esa noción de sublimación amorosa hasta el punto de que el sentimiento irrumpió en la forma de entender el amor de numerosas parejas de la época, haciendo buena la afirmación de Oscar Wilde de que a veces la naturaleza imita al arte, y dando lugar a que existan, aún hoy, numerosas personas que creen que ese era el sentimiento común durante los siglos XV y XVI, olvidando que junto a Dante y Beatriz, Petrarca y Laura, existían numerosas formas populares de sentir y vivir el amor y el sexo, como tan magistralmente ha demostrado Mijail Bajtin (La cultura popular en la Edad Media y en el Renacimiento.—Barral Editores, Barcelona 1971), que llega a afirmar que hay un: «Segundo mundo, el de la cultura popular, que se construye en cierto modo como parodia de la vida ordinaria, como un mundo al revés». Fuera, pues, de nuestro ánimo el etiquetar el sentimiento amoroso de cualquier época o de cualquier comunidad étnica como algo similar, constante e invariable. Por el contrario, es preciso darse cuenta de que con la visión del mundo de cada sociedad y de cada época, como por ejemplo la cultura popular gitano andaluza, se construye una episteme particular que genera su peculiar estimación de los contenidos amorosos. La historia, en suma, juega un papel\n\n[ENDING CONTEXT]\n\nsimple, elemental, pero inevitable, con una certidumbre de claro talante fatalista:\n\n¿Qué quieres que yo le haga? una pena sin alivio sólo la muerte la acaba.\n\nY es que, en última instancia, el problema es, y ha sido siempre en materia amorosa (y el flamenco confirma la regla) el apostar demasiado fuerte en la empresa y empeñarse en una baza definitiva, en un juego dulce y trágico a la vez, o a lo peor ¿quién sabe?, ni siquiera se trata de un juego, como afirma sentenciosa la copla desgarrada:\n\nYo creía que el amor era cosita de juguete y ahora veo que se pasan las fatigas de la muerte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El sentimiento amoroso, en el primitivo cante gitano andaluz",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "13-16",
    "page_number": 13,
    "word_count": 5154,
    "article_char_count_full": 30279,
    "article_char_count_review": 3109,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      }
    ]
  },
  {
    "article_id": "1981-03-17-right-los-cafes-cantantes-en-una-notic",
    "article_text_for_review": "Buscando datos para un trabajo opuesto al tema que nos ocupa, en el número 661, de 20 de Marzo de 1900, del semanario giennense «EL PUEBLO CATOLICO», encontré una noticia sobre los cafés y tabernas del cante, encabezada con el título: CONTRA LOS CAFES CANTANTES, y que literalmente transcribo:\n\n«Se ha publicado una importante Real Or- den por el Ministerio de la Gobernación, refer- rente a las tabernas y Cafés Cantantes.\n\nLas disposiciones que en dicha Orden se dan serían muy dignas de aplauso si fueran cumplimentadas con celo y energía, pues todo cuanto se haga con esos centros de corrupción será poco y merecerá el aplauso y el apoyo de la gente sensata. Mucho nos alegra-remos que esta Real Orden no sea agua de cerrajas como tantas otras, y termine con esos malditos Cafés, causa de muchos crímenes y foco de enfermedades vergonzosas; azote mortal de los pueblos corrompidos y origen poderosísimo de tantas calamidades como nos agobian y entristecen».\n\nComo vemos, el reaccionario «Pueblo Católico», del cordel de «El Siglo Futuro», no se anda por las ramas para arremeter contra estos establecimientos a los que considera, más o menos, como mancebías, casas de lenocinio o Patios de Monipodio, en pueblos y ciudades que nada tienen que envidiar a Sodoma y Gomorra. Lástima que, a pesar de intentarlo, no haya podido encontrar la referida Real Orden, labor que dejo a los investigadores, y así poder conocer con detalle las disposiciones que se daban en la misma.\n\nUna vez más nuestro querido Jaén, desprecia cuanto ignora, y no nos habla, en sus noticias, de cómo eran esos lugares donde el grito desgarrado de la pena encontraba eco. Sin preguntar tan siquiera, sin informarse, se le niega el pan y la sal a esta forma de expresión que ya es común en casi toda Andalucía. Se formula el anatema, porque en Jaén, durante muchos años se le ha negado el ser a la cultura viva, y cuando el artista se sale de\n\nTranscripción y comentario Miguel Calvo Morillo",
    "title": "Los cafés cantantes en una noticia de 1900",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 337,
    "article_char_count_full": 1964,
    "article_char_count_review": 1964,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-03-18-right-honor-y-lecturas-para-un-libro-c",
    "article_text_for_review": "Por Manuel Urbano\n\nEn «Sevilla y abril de 1881», firmaba don Antonio Machado y Alvarez, Demófilo, su «Co-lección de cantes flamencos», primera pieza investigadora en el tiempo y fundamental aún, a cien años de distancia, para conocer y adentrarse en parcela tan angular de la esencia de Andalucía como es el cante flamenco.\n\nNo voy a incidir ahora en la importancia que, como folklorista y pionero de los estudios etnológicos españoles, tiene Machado y Alvarez; tampoco recordaré la incompatibilidad e incomprensión de los hombres y estructuras de su época para con él —hasta los tribunales eclesiásticos giennenses andaron a vueltas con su persona e ideas—, simplemente, deseo que esta sección de «Candil» sea un sincerísimo, aunque modesto, homenaje al padre de los estudios flamencos, sin cuya aportación nuestro recio y limpio arte, de seguro, hubiese llegado a nuestros días de un modo bastante trunco.\n\nSi viva e incuestionable importancia poseen las casi setecientas cincuenta letras de la «Colección», agrupadas por cantes —tal vez la recopilación sistemática más completa de coplas flamencas realizada hasta nuestros días—, tanta o más tiene el prólogo que las antecede, al que, por cierto, en numerosísimas ocasiones se acude para citarle como magisterio, y sin que, lamentablemente, se haya estudiado con el rigor, atención y desapasionamiento que recla-ma. En conclusión, estamos ante un libro al que, sin duda, en el mejor sentido de la palabra, se le debe clasificar de clásico. Todo el honor para él y que sobre el mismo recaigan lectura a millares.\n\nY si de insustituibles, a un siglo de su primera aparición impresa, hemos calificado tanto al prólogo como a la recopilación, no es menor el valor de los cerca de tres centenares de anotaciones —cifra que ya de por sí da cumplida referencia de la amplia y concienzuda labor investigadora de Demófilo— con las que no sólo el autor documenta numerosas coplas sino que aportan cumplidas noticias históricas, folklóricas, lingüísticas, etc., etc.\n\nMas si de excelente e imprescindible calificamos la «Colección de cantes flamencos», no se opone a esta adjetivación, todo lo contrario, la imperiosa necesidad de una justa y amplia edición crítica serena y desapasionada, estudiosa e investigadora, como el propio libros. Porque, digámoslo de una vez, el único homenaje que puede rendírsele a un libro es el de su lectura y estudio. Aunque, y de ello soy consciente, esta propuesta de una edición crítica arrancará las iras de más de un autoerigido inquisidor flamenco que utiliza como insulto las palabras «doctoral», «académico» y «universitario».",
    "title": "Aunque no quepa en el papel. - Honor y lecturas para un libro centenario",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 415,
    "article_char_count_full": 2609,
    "article_char_count_review": 2609,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-03-19-right-discograf-a-flamenca-no-es-el-ca",
    "article_text_for_review": "NO ES EL CAMINO\n\nEl New Hondo de El Turronero\n\nSeñores responsables de su realización inuestro idioma no es lo suficientemente rico, expresivo, abundante, como para reseñar y anunciar una nueva experiencia musical? Y conste que nada tenemos en contra de la lengua inglesa. Pero, sinceramente, en este caso no tiene razón de ser, unir dos palabras, New-nuevo y Hondo-jondo, esencia, raíz, cultura del Sur, para, además, ofrecernos un producto señalado como «un auténtico nuevo cauce para el cante flamenco», que queda reducido a un montaje de grabación. Montaje que se deja entrever en todo el L. P. Fijemos nuestra atención, por ejemplo, en la caña. ¿Se puede decir con un acompañamiento\n\nQuizás el silencio sería el mejor argumento a la hora de ocuparnos del último disco de «El Turronero». Pero, el que calla otorga, y desde luego, con todos los respetos para quien no compartía nuestra opinión, no estamos dispuestos a otorgar ninguna gratuita complacencia, ni siquiera indiferencia, a este hecho discográfico. Acerquémonos pues, al «New Hondo de El Turronero». Y comenzamos en la portada. SI YO VOLVIERA A NACER Aires de Huelva ritmico desprovisto de todo el sentido que ese cante conlleva? Pierde, pensamos, toda su razón de ser como estilo flamenco, rompe su esquema musical, su profundidad, su emoción, su contenido, su comunicación. Turronero, ¿es posible cantar con esos arreglos orquestales, o se graba la voz por un canal, acompañamiento por otro y luego la técnica hace el resto en una buena mezcla de sonidos? Esto no es arte. Es trabajo de laboratorio.\n\nDOSCANDIL\n\nSe dice en la presentación del disco, que «con el New Hondo ha nacido un nuevo camino para el flamenco». No. Esperemos que el camino quede cortado, bruscamente. Este camino, creemos, no llevaría al flamenco a ninguna parte. Lo llevaría a su despersonalización, a su muerte. Y no somos catastrofistas. Nos gusta que el cante se llene de nuevos aires enriquecedores de su razón de ser, pero no estamos de acuerdo con actitudes, hechos que atentan contra su fundamento, su esencialidad. Así de fácil.\n\nTejidos nuevos, para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N",
    "title": "Discografía flamenca.- No es el camino.- El New Hon- do de El Turronero",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 353,
    "article_char_count_full": 2142,
    "article_char_count_review": 2142,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-03-20-left-quienes-fueron-los-maestros",
    "article_text_for_review": "PAQUIRRI «EL GUANTE»\n\nCantaor de raza gitana nacido en Cádiz a principios del siglo XIX. Estimado como uno de los más creativos en los cantes por soleares y al que algunos flamencólogos consideran anterior a Enrique Jiménez «El Mellizo». De profesión guantero —que ejerció en la gaditana calle de Juan de Andas— desarrolló toda su actividad artística en su Cádiz natal. Se cree que su muerte fue por envenenamiento motivado por una oscura razón de celos.\n\nEl nombre de Paquirri ha quedado en los anales flamencos como figura eminente. Creador de varias soleares, de las que sólo se conservan cuatro, que responden al tipo patético y ligado de la soleá corta, la que, según Ricardo Molina, era con la que solían rematar sus cantes La Sernet a y El Mellizo. Por otra parte, hay que señalar su soleá apolá, que ha llegado fielmente hasta nuestros días en la versión grabada de Diego Bermúdez Cala «El Tenazas de Morón».\n\nA Paquirri, además de excelente cantaor, bailaor de clase y tocaor, hay que reconocerle su fuerte personalidad y, sobre todo, sus grandes facultades creadoras origen de la anotada soleá apolá con la que remataba cañas polos y antiguas malagueñas. Igualmente, es considerado como el padre de las alegrías de Córdoba, como señala Fernando Quiñones: «sus cantes natales dícese instauró aquel gaditano en la ciudad califal con un dulce y burlón compás, imitador del pausado acento cordobés».\n\nPosiblemente, el estilo más conocido —aparte de la soleá apolá— sea la soleá que los gadita-nos aún cantan recordando al maestro:\n\nMetio en cañaverales\n\nlos pájaros son clarines\n\nal divino sol que sale\n\nAparte de la grabación de la soleá apolá de Paquirri, ya reseñada, que hiciera El Tenazas, el estilo de este gaditano ha quedado bien patente en una grabación más reciente del malogrado Pepe el de la Matrona.\n\nTIO JOSE «EL GRANAINO»\n\nCantaor al que se le han adjudicado varias localidades para ubicar su cuna. Unos, creen que fue Cádiz; otros, Sanlúcar; también, aunque para los menos, Granada, tomando como gentilicio su sobrenombre artístico. La creencia comúnmente aceptada es la que atribuye el origen del apodo al oficio que, de vendedor de granadas, desarrollaba Tío José por las calles de su ciudad natal, Cádiz. También se ha considerado que el sobrenombre provenga de su larga estancia, desde pequeño, en Granada hasta su regreso a Cádiz, donde falleciera. Por último, Blas Vega alude a él como banderillero de Cúchares, tras dejar el oficio de vendedor ambulante.\n\nTampoco la fecha de su nacimiento se conoce con certeza, especulándose entre varias de finales del XVIII y otras de comienzos del XIX. Por otra parte, se cree que fue de raza gitana y tío de Romero «El Tito» —posible creador del cante por romeras—. Lo que sí es cierto, es que está considerado como el creador de los cantes por caracoles y un estilo muy personal de la caña, de la que nos queda el testimonio grabado de Pepe el de la Matrona en «Tesoros del flamenco antiguo».\n\nQuiero cerrar esta sucinta biografía de este cantaor enciclopédico de estilo netamente gaditano, maestro de muchos y dueño de una gran vocación, con una cita de Fernando Quinones: «todas las noticias que de él tenemos, y su época misma, coinciden en definir a Tío José el Granaíno como a un verdadero clásico del cante».\n\nMARIA BORRICO\n\nFamosísima cantaora nacida, probablemente, en San Fernando (Cádiz), en la primera mitad del siglo XIX; y a la que también le atribuyen cuna je-rezana por los giros de su cante siguiriyero. Según la tradición oral renovó los cantes de El Fillo, y su especialidad debió ser el cante por siguiriyas, de la que una sigue siendo popularí-sima entre los buenos aficionados:\n\nDice mi compañera\n\nque no la quiero;\n\ncuando la miro, la miro a la cara,\n\nyo er sentido pierdo.\n\nEsta siguiriya, probablemente, se cantaba para rematar todo un recital de ellas; igualmente, con la misma se rematan algunos cantes, tales como los de livianas o serranas. Por cierto, se cree que la costumbre de realizar estos remates fue iniciada por Silverio Franconetti.\n\nMaría Borrico está considerada como la creadora de una siguiriya esforzada y valiente, a veces, comparada en sus cualidades con la de El Ciego de la Peña; caracterizada, al decir de Ricardo Molina, por su «exaltación y apasionamiento», a la que «unta de pasión», a juicio de Félix Grande. Finalmente, y a mi juicio, la siguiriya de María Borrico tiene una personalidad inigualable, pudiendo considerarse su estilo como eslabón entre el viejo y moderno, en el que revela aires de cambio.\n\nSelecciona: RAFAEL VALERA",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 763,
    "article_char_count_full": 4554,
    "article_char_count_review": 4554,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
