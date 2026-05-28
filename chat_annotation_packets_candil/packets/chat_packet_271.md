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
    "article_id": "1993-03-17-right-siguiriyas-gitanas",
    "article_text_for_review": "Siguiriyas gitanas\n\nNo me pás, gitana, que guarde silencio si ninguna faca ha cortao mi lengua ni ha llegao mi entierro.\n\nDéjame que hable de tus negras trenzas y de tus ojazos, tan llenos de asombro, en la noche aquélla.\n\nDeja que recuerde, sin morir, muriendo, tu temblor de vida al pasar mis manos por tus duros pechos.\n\nDeja que, en mis sueños, corra tu camino y que se remuevan el vino y las facas al oír tu grito.\n\nY que, bajo el surco carnal de tu vientre, germine tu nana que riega esta noche mi orgasmo de nieve.\n\nDiego Granados",
    "title": "Siguiriyas gitanas",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 101,
    "article_char_count_full": 537,
    "article_char_count_review": 537,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-03-18-left-ritmos-de-ida-de-vuelta-y-de-vai",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(A propósito del tango, de la guajira y de rumba)\n\nPonencia presentada al XX Congreso de Arte Flamenco. Huelva, 1992\n\nBernard Leblon\n\nLos cantes llamados «indianos» o de «ida y vuelta» ocupan un lugar aparte en el repertorio flamenco. Los puristas absolutos y algunos partidarios incondicionales de los cantes básicos suelen despreciarlos rotundamente a pesar de sus interpretaciones por ciertos maestros del cante, como Chacón, la Niña de los Peines, el Niño Gloria, el Niño de Cabra, etc. En cambio, muchos aficionados y algunos flamencólogos famosos —Manfredi Cano, Fernando Quiñones y otros— confiesan haberse rendido a sus encantos. Hay que reconocer, en cualquier caso, que esos cantes corresponden con una etapa decisiva (aunque también criticada) de la evolución del flamenco —la de los\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"público\"]\n\no, muchos aficionados y algunos flamencólogos famosos —Manfredi Cano, Fernando Quiñones y otros— confiesan haberse rendido a sus encantos. Hay que reconocer, en cualquier caso, que esos cantes corresponden con una etapa decisiva (aunque también criticada) de la evolución del flamenco —la de los cafés de cante de finales del siglo XIX y principio del siguiente—, con un episodio importante de su historia, en lo que se refiere al ensanchamiento del público y del repertorio, y con una tentativa de fusión con las culturas musicales hispanoamericanas en el momento en que las antiguas colonias han logrado su independencia. Sabemos que uno de los términos utilizados para designar esta música, el de «ida y vuelta» ha sido rechazado por quienes afirman que el viaje no tuvo tanta vuelta y prefieren por lo tanto el adjetivo «indiano». Sin embargo, esta última palabra no carece de ambigüedad porque se aplicó al emigrante español que regresaba a su tierra natal después de hacerse rico en América, lo que también podía pasar con alguna música. Aquí hay que detenerse un poco para considerar que las músicas importadas en España desde América no eran nunca puramente americanas. Todas eran el producto de mezclas o mestizajes diversos en los cuales el elemento autóctono, indio, se hallaba muy reducido y muchas veces ausente del todo. En Cuba, por ejemplo, la música oscilaba entre dos polos representados respectivamente por la guajira, aportación blanca, andaluza, y la rumba, mestiza pero de muy predominante ascendencia africana. Si entramos un poco en la cuestión de los ritmos, notaremos que la guajira, igua\n\n[ENDING CONTEXT]\n\nde Las Santas Marías del Mary llega a ser hoy la música étnica de los gitanos catalanes, quienes se identifican totalmente con ella. Gracias a la popularidad de los Gypsy Kings y al éxito creciente de los grupos gitanos de Perpiñán, la rumba sigue evolucionando, influida en la actualidad por las formas más modernas de la salsa. Así, vemos que el diálogo musical entre Europa y América sigue vigente y podemos sospechar que los años venideros nos reservarán nuevas sorpresas, aunque es previsible también que nos iremos alejando cada vez más de lo que consideramos hoy como el flamenco auténtico.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ritmos de ida, de vuelta y de vaivén",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "18-20",
    "page_number": 18,
    "word_count": 2604,
    "article_char_count_full": 15846,
    "article_char_count_review": 3237,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "público"
      }
    ]
  },
  {
    "article_id": "1993-03-20-right-alre-de-la-fiesta-gitana",
    "article_text_for_review": "Dibujos de Miguel Alcalá del libro «Le Flamenco et les gitans», Editorial Filipacchi, París, Francia, reproducidos bajo licencia del autor. Textos de Manuel Martin Martin 1.353\n\nTío Bacán.—Sebastián Peña Peña (Lebrija, 1911). Bastián Bacán responde a otro concepto del cante, más hierático y ritual, que vivifica y responde a una situación dramática ya olvidada y a su devenir. Con él se ha inflamado el abanico solearero, fundiendo el acento íntimo y afligido de un nostálgico, Juaniquín, con el tono más intenso y cargado de acción del Tío Benito el de Pinini. Trátese, pues, de un reencuentro con la auténtica dimensión humana del cante gitano. Nos encontramos frente a unas soleares de embarazosa estructura, muy elaboradas y pulidas hasta un grado máximo, y que no admiten sobreañadidos ni adulteración. «Chache» Bastián da una medida exacta de cuanta soledad comporta el cante gitano.",
    "title": "Alreó de la fiesta gitana",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "20-23",
    "page_number": 20,
    "word_count": 141,
    "article_char_count_full": 890,
    "article_char_count_review": 890,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-03-24-left-aunque-no-quepa-en-el-papel-jos-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nTrazar una biografía sobre un personaje que hace tiempo entrara, por derecho propio, en la categoría de mito, es siempre arriesgado, porque significa traspasar las fronteras del templo y se corre el riesto de, o bien abusar del incensario, con lo cual el tufillo ramplón aumenta, o desacralizar lo que, a la mayoría de los mortales, se nos antoja intangible.\n\nGonzalo Rojo, en el presente estudio, ha sabido soslayar los escollos y penetrar en la figura del cantaor de Vélez Málaga con el rigor que debe de tener todo intento biográfico y a la vez con la delectación, nunca empalagosa, que le produce la figura del biografiado. El resultado es un libro espléndido, abundante en datos e inexcusable para el que quiera aproximarse a la egregia figura de Antonio Ortega Escalona.\n\nI oda la obra\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"histórico\"]\n\nbe de tener todo intento biográfico y a la vez con la delectación, nunca empalagosa, que le produce la figura del biografiado. El resultado es un libro espléndido, abundante en datos e inexcusable para el que quiera aproximarse a la egregia figura de Antonio Ortega Escalona. I oda la obra discurre en un difícil paralelo, solamente posible con el grado de madurez humana y flamenca del autor, entre los avatares vitales de Juan Breva y el contexto histórico que España, y más concretamente la región malagueña atravesaban, con lo que el relato gana en rigor y precisión, hecho al que no es ajeno la incorporación de textos de época imprescindibles, ya sean testimonios periodísticos o pequeñas crónicas históricas que redondean el magnífico trabajo investigador del periodista malagueño, que incorpora también una cuidada selección fotográfica y la bibliografía más completa que existe sobre el tema, sin olvidar el adorno de ese florilegio poético que, a manera de broche final, realza la alta estima de que el mismo gozó entre los intelectuales y artistas de la época. La casette que acompaña al libro, con grabaciones efectuadas en el año 1910, con la guitarra maestra de Ramón Montoya, un auténtico tesoro. El autor del presente trabajo está inmerso en ese apasionamiento que despierta, más allá de nuestras fronteras, el tema flamenco, y que lo ha llevado a multitud de reflexiones sobre este arte, que más tarde se han vertido en correctas comunicaciones a congresos y en artículos y libros imprescindibles para determinar aspectos de la estética jonda, no ya desde el apasionamiento, más o menos elemental, que preside muchos de nuestros estudios, atacados por el mal de la superficialidad, sino utilizando métodos científicos de trabajo como la semiología o el estructuralismo, mediante los cuales, el profesor Tarby realiza una aproximación a\n\n[ENDING CONTEXT]\n\nlibro, bellamente impreso por la cuidada edición, puede y debe ser una guía de cómo los estudios generales van quedando desfasados en beneficio de estos análisis parciales, porque sólo acotando el objeto de nuestra indagación, podremos obtener, como hace Génesis, resultados definitivos que pongan luz en el marasmo, desgraciadamente tan abundante en las aproximaciones flamencas, de los comentarios generales o las afirmaciones rotundas sobre hechos que requerirían una atención pormenorizada y aparte, realizada inexcusablemente por buenos especialistas.\n\nJuan de la Malena\n\nTeléfono (953) 271448\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel... José Luis",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 1660,
    "article_char_count_full": 10471,
    "article_char_count_review": 3475,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "histórico"
      }
    ]
  },
  {
    "article_id": "1993-03-25-left-noticiario-flamenco-congreso-de-",
    "article_text_for_review": "Entre los días 20 y 26 de septiembre próximo, se llevará a cabo en la capital francesa, el XXI Congreso de Arte Flamenco.\n\nAl publicarse este número de Candil, la recepción de ponencias y comunicaciones al Congreso se halla cerrada, no así la inscripción de congresistas, que tiene dos plazos: el primero, haste el 31 de julio, con tarifas preferenciales; el segundo prácticamente hasta las vísperas de celebrarse el evento.\n\nLos organizadores han hecho ya reservas de alojamientos en la Ciudad Internacional Universitaria de París, sede del Congreso, y ha diseñado un conjunto de actividades y visitas turísticas paralelas al encuentro.\n\nAsimismo se ha confeccionado el programa provisional de este XXI Congreso, y de los recitales y festivales flamencos asociados.\n\nPara una más amplia información, los interesados deben dirigirse a:\n\nSecretaría del XXI Congreso de Arte Flamenco,\n\nFlamenco en France, 33 rue de Vignoles,\n\n75020 Paris\n\nTel. (331) 43 48 99 92 Fax: (331) 43 48 05 81\n\nEl comité de honor está integra- do por los señores don Gabriel Ferrán de Alfaro, embajador de España en Francia; don Jack Lang, ex-ministro de Cultura y Educación de Francia; don Jordi Solé Tura, ministro de Cultura del Estado Español; don Jacques Chirac, alcalde de París; don Fe- derico Mayor Zaragoza, director general de la UNESCO; y don Manuel Chaves González, presi- dente de la Junta de Andalucía.\n\nLeo en vuestra bien llevada revista Candil un inteligente artículo firmado por don Francisco V. Vargas, que en parte viene a darme toda la razón; y digo en parte porque este buen aficionado sólo se refiere a uno de los puntos en que una vez más queda maltratado ese cante tan bello como es la malagueña.\n\nDiego Clavel no ha demostrado estar en posesión de los conocimientos que exige la antológica grabación de nada menos que —no me lo puedo creer— 31 malaqueñas. Esa hazaña supondría, de serle posible a un cantaor hecho de la cumbre mairenista a la fecha, el más concienzudo, largo y sacrificado estudio que de la malaqueña pueda concebirse, y ése no es el caso de ningún cantaor post-mairenista de las zonas soleareras de Sevilla y Cádiz.\n\nNo es que para cantar la mala-gueña el cantaor tenga que ser necesariamente de la zona mala-gueñera de Málaga. El Mellizo, Chacón, Pastora, Marchena y otros no lo eran y la hacían «de bien pa arriba». Lo que ocurre es que, a partir de la implantación total de la más acérrima y fanática empresa gitanofílica flamenca, aflamencada y flamencoide, los cantes que no les cuadran a los gitanos de hoy que cantan y escuchan, pasaron sin más explicación al desván de lo inservible. Así la preponderancia radical del «cante gitano puro» dejó sin posibilidad de aprendizaje a jóvenes como Diego Clavel que, por la cuenta que le tenía, se ciñó a la pauta marcada por el verdadero, indiscutible e inconmensurable maestro de los alcores, si quería que lo tuvieran en cuenta los organizadores festivaleros siempre más papistas que el Papa.\n\nEn aquellos inicios recuerdo que, por alguna débil razón de ética personal, llegaron a tocar el campo de la malagueña cantaores de la talla y veteranía de Juan Varea y Luis Caballero. Su justo cometido duró el tiempo que tardaron los intelectuales y críticos descubridores de Tío Luis el de la Juliana, el Fillo y la Andonda, en considerar la malagueña como un fandango folklórico. Son los mismos que enloquecen con el «cante» sublime de Camarón, acompañado por la Filarmónica de Londres, y Morente como «transfigurador» en la acción del análisis adverso de los tiempos-cauces» del llamado cante flamenco.\n\nPedantería, más o menos así se las he oído decir a los que saben de esas cosas tan profundas. Yo, pobre de mí, de los tiempos de Chacón y ya próximo a los 90, me quedo con lo que entiendo. Por lo mismo le doy la razón a don Paco V. Vargas y se la quito a los que no reparan en que una obra que ha de pasar a la posteridad, debe estudiarse hasta el fondo de lo que merece.\n\nPosiblemente Málaga, cantaora a su aire, pueda encontrar cantaores que, además de conocer cierta diversidad de malagueñas, le añadirían la natural propiedad de su acento; y la verdad es que como señal de identidad didáctica hubiese resultado oportunamente encomiable.\n\nSalvador Santana",
    "title": "Noticiario Flamenco Congreso de Arte Flamenco en París",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 718,
    "article_char_count_full": 4228,
    "article_char_count_review": 4228,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
