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
    "article_id": "1985-05-19-left-la-vieja-del-cante",
    "article_text_for_review": "Fulgente hasta en las sombras, medio adormilada y con los senos al frío la vieja del cante, se consume y se va lentamente jubilando de los falsos fastos que da el oropel. En las estрыas de sus plateados ojos viene y va; una solea. En su oscuro sueño, se presiente una oleada de sortilegios y en su quieta sonrisa de azufre, los zarpazos certeros de algún extraño felino le va abriendo los repintados labios hasta infectar de cauces su ripiosa boca. Boca que ya tiene los arrumacos de una flor de metal. En el batallar infame de sus ridos trajes aún deja al descubierto algunas perlas brillantes. A la vieja del cante, le combate ya la inconclusa enfermedad que es la fiebre de no estar en ningún sitio, porque sus tetas le pesan cada día más. Y en el negro orificio que es la duda de su mirada parece salirle, una cabra espantada. De pronto, el trompetazo sórdido del tiempo la hace rodar por el vetusto escenario, animada sólo por la infernal cantinela del presentador que con su chabacana voz, le hace aborrecer su adorada inmortalidad. - la vieja del cante, y en el suelo hay un esqueleto de páy más allá; una guitarra y un tambor.\n\nJesús Cuestarana,",
    "title": "«La Vieja del Cante», de Jesús Cuestarana",
    "periodical": "candil",
    "issue_id": "1985-05",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 209,
    "article_char_count_full": 1153,
    "article_char_count_review": 1153,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-05-21-left-d-iscografia-flamenca",
    "article_text_for_review": "Título: PREMIO ANTONIO MAIRENA Canta: Jesús Sánchez Trigueros «ITOLI DE LOS PALACIOS» Guitarra: José Luis Postigo Referencia: Hispavox. (30) 130.274\n\nNos encontramos con la primera grabación de un artista que es bastante conocido en la comarca que lo ha visto nacer. Un cantaor que se suma a esa serie de figuras que en su día ganaron el premio del concurso de Mairena del Alcor, instituido por el maestro, Antonio Mairena y que tras la consecución del mismo han ido progresando en su arte hasta convertirse en figuras actuales.\n\n¿Podría ser éste el mismo resultado para Itoli de los Palacios? Desde luego, tras la escucha de este disco, de José Sánchez, se comprueba cierto dominio de los estilos y con el pasar del tiempo esperamos que se vaya incrementado la profesionalidad y, por tanto, el perfeccionamiento en el de- sarrollo de los mismos.\n\nVarios son los cantes que ha incluido en este disco, quizás con inclinación hacia los estilos libres, pero no obstante deja constancia de saber andar por los cantes a compás. Itoli realiza un cante un tanto pausado, como si con esta circunstancia entreviera una mayor seguridad en sí mismo. Así, en las granaínas y media granaína se le aprecia este matiz, igual sucede en las siguiriyas que remata con el conocido y hoy tan popular cambio de Manuel Molina. En los fandangos —de El Gloria— deja patente una gran entrega con matización en su voz, así como en las malagueñas. Sensibilidad demuestra en las peteneras y mineras y cierta lentitud en las soleares. En varias grabaciones José Sánchez evidencia cierto seguimiento de los ecos maireneros con más abundancia en los tientos.\n\nLa guitarra de José Luis Postigo, sobria y sensible, vuelve a replegarse a su justo papel llevando al cantaor por los caminos adecuados para la mejor realización de los cantes. Una guitarra que no busca el lucimiento propio supeditándose a su auténtica labor.\n\nTítulo: MI VERDAD Canta: Antonio de Patrocinio Guitarra: Merengue de Córdoba y Paco Serrano Referencia: DOBLON. 50 1771\n\nAntonio de Patrocinio es un joven cantar cordobés de los nuevos valores surgidos en la capital de la Mezquita, pleno de ilusiones y de voz. Aunque un poco tarde, no queríamos que estas grabaciones pasaran desapercibidas hacia una afición flamenca que quiere, poco a poco, ir valorando los nuevos nombres que se están incorporando al mundo flamenco.\n\nAl igual que figuras consagradas, Antonio de Patrocinio abre su disco con un estilo de ida y vuelta, quizás el más popular: colombianas. En esta grabación el cantaor cordobés muestra sus dotes de intérprete que mantiene en sus interpretaciones algo de comercialidad, porque no es solamente en las colombianas donde expone esta fa- Buena ejecución de las livianas y mala- gueñas, así como buena evocación de Pastora Pavón en las peteneras.\n\nceta, si no también en los tarantos y e.i. ese nuevo estilo —el tiempo juzgará— que él denomina «aceituneras», una especie de copla flamenca que viene a sumarse en los distintos intentos que por este camino están realizando figuras ya consagradas.\n\nComo cantaor joven, Antonio de Patrocinio posee grandes facultades y como suele suceder a veces, el desarrollo de algunos estilos tienen falta de matización por esta circunstancia, así sucede en los fandangos y en las siguiriyas, éstas últimas rematadas con el cambio, igualmente, de Manuel Molina. Esperamos que con el transcurso del tiempo, el afianzamiento como cantaor y la adquisición de profesionalidad, Antonio de Patrocinio pueda ir introduciendo la matización necesaria en su cante.\n\nAcompañamiento adecuado a la personalidad del artista el realizado por Merengue de Córdoba y Paco Serrano, dándoles tonos y compás para el buen desarrollo de los cantes.",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1985-05",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 604,
    "article_char_count_full": 3712,
    "article_char_count_review": 3712,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-05-21-right-pepe-de-la-matrona",
    "article_text_for_review": "(Selecciona Candil)\n\nJ osé Núñez Meléndez, el trianero que sin darle importancia a riesgos ni supersticiones de una época tan problemática como era la de primeros de siglo, se embarcaba para ir a tomar café a La Habana, nació en Sevilla el 4 de junio de 1887.\n\nJose, uma ei apodo que lo ha inmor- talizado del oficio materno, y, des- de muy joven, consta que fue una ins- titución viviente en su barrio y en toda la ciudad; sus bromas y donaires, sus sentencias cargadas de filosofía sin fin y gracia popular a puñados, lo hicier- on famoso antes de que el cante en- grandeciera para siempre su apellido y el alias por el que era conocido.\n\nPero enseguida su arte pasa a primer plano, primero en Sevilla y en las ferias de sus pueblos, más tarde en aquella Córdoba romántica y atractiva de los tablaos flamencos, y, por último (en aquella época resultaba inevitable) en Madrid, donde se reunía la flor y nata de la flamenquería de toda España; aquella villa y corte que rendía a diario homenaje a Chacón, muy pronto va a encontrar en Matrona un amigo y un extraordinario cantaor. José admiró y fue a su vez respetado por el monstruo jerezano, alternando juntos en numerosas ocasiones.\n\nPero muy pronto Matrona se deja llevar por la vocación americana de esa su Sevilla natal, que persigue la mar desde Sanlúcar y busca aquellas tierras, en las que aprenderá diferentes estilos de los llamados de ida y vuelta y que tan famoso lo harían en los espectáculos teatrales aflamencados en los que con tanto éxito actuó: Rumbas de 1914, milonga de Pepa de Oro, etc.\n\nSin embargo al hijo de Manolita la Matrona, había que oírlo en privado, en el rincón de cabales, con su copa y su «colilla» (así llamaba a monstruosos cigarros puros), desgranando con maestría lo mejor del cante antiguo, que él ha conservado como nadie en una discografía que mereció el premio de la Cátedra de Flamencología y multitud de homenajes. Gracias a Pepe conservamos cantes tan añejos como la caña de José el Granaíno, la cabal de Silverio o la Siguiriya de cambio de María Borrico; aunque donde Matrona no se podía aguantar era en esos viejos tangos trianeros («serranita me publicastes») que ponía a bailar a todo el mundo alrededor de la hoguera encendida de su gracia.\n\nEn Jaén tuvimos la suerte de acompañar respetuosamente una de sus últimas geniales intuiciones artísticas; aún mermado de facultades lo suyo fue una lección magistral de compás y de medida, de gracia y sabiduría de siglos. Quede para el recuerdo de los flamencos su duende estremecido.",
    "title": "Quiénes fueron los maestros: Pepe de la Matrona",
    "periodical": "candil",
    "issue_id": "1985-05",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "21-25",
    "page_number": 21,
    "word_count": 442,
    "article_char_count_full": 2528,
    "article_char_count_review": 2528,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-05-25-right-a-placas",
    "article_text_for_review": "DE «PACO MAZACO» Por: Manuel Yerga\n\n¿El por qué de sú apodo «Mazaco»?\n\nVeamos:\n\nPACO «MAS SACOS»\n\nPaco nació en Sevilla, cerca de la Puerta Osario. Quedó huérfano siendo muy niño. En Coria (Sevilla) donde vivía con unos familiares, había un panadero que sintió compasión de él. Un buen día lo llamó a su casa para decirle: «Mira niño, ¿tú quieres aprender, poco a poco, el oficio de panadero?». A lo que Paquito asintió sin titubeo. El contrato verbal fue aprender el oficio, darle las comidas, pero tenía que dormir en la panadería. ¿Pero, cómo dormir? Muy sencillo: poniendo unos sacos en el suelo y otros para cubrir su cuerpo.\n\nEn la panadería había un «Maestro Pala» con mucha guasa. Al día siguiente, de madrugada, el maestro despertó al niño diciéndole: «Venga arriba, que ya es tarde, ¿has dormió bien?». A lo que Paquito le contestó: «No ceñó, no he dormió bien, porque he pazao mucho frío. Esta noche me tié uzté que da má zaco». Nuevo día y el maestro vuelve a ponerle el mismo disco y el niño volvió a contestar diciéndole que aún necesitaba más sacos, porque «eza» noche también había pasado mucho frío. El maestro le contestó: «Mira, niño, cómo te voy a dar má zacos si ya te he dao to los que teníamos vacíos?». «¡Vaya con la criatura, eres más frío que un témpano!».\n\nEl «Maestro Pala» lo bautizó con el sobrenombre de «MA-ZACO» y así lo llamaba en la panadería y en la calle. De esta forma el alias se extendió por el pueblo como una mala epidemia y así era conocido por sus convecinos.\n\nSiendo aún muy joven aprendió los cantes de Luis «El Quijá» y sobre el año 1919 debutó como cantaor junto al Gloria, Vallejo y Pastora, llegando a ser uno de los mejores cantaores por siguirias, medias granaínas y cantes sin acompañamiento.\n\nAlgunos señores han publicado que Paco era de Coria (Cáceres); que siendo un niño llegó a Sevilla, donde su padre fue guardia civil. Esto no es así. Paco nació en Sevilla. Tuvo esa suerte.\n\nManuel Yerga",
    "title": "Discografía Placas",
    "periodical": "candil",
    "issue_id": "1985-05",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 349,
    "article_char_count_full": 1949,
    "article_char_count_review": 1949,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-07-3-right-editorial",
    "article_text_for_review": "Editorial\n\nFestivales Flamencos\n\no ha reiterado la afición, lo han analizado críticos y profesionales. Se ha l'debatido en colóquios, conferencias, congresos... y lo que antes pudiera reputarse derrotismo, hoy es ya una realidad incuestionable: El Festival Flamenco ha entrado en una fase de irreversible recesión. Sencillamente, se acaba. Esta forma multitudinaria de divulgación jonda ha vehiculado durante más de tres décadas importantes comunicaciones flamencas, cuyo destinatario fue, por lo general, un público sin iniciación y desinformado, frívolo, y en cualquier caso, desatento. Los profesionales encontraron, por otro lado en el Festival Flamenco una forma legítima de dignificar sus ingresos y salir así de esa penuria secular que los recluía en ventas y prostíbulos. Acaso sea esa la mejor aportación que los Festivales han hecho a la Historia del Flamenco. Como ocurre en todo fenómeno de masas —hacemos esta acotación con reservas— el arte queda desdibujado, sobre todo si éste tiene un carácter tan intimista y tan poco objetivable como el Flamenco. La comunicación no se produce y el público termina por agotarse ante una expresión «standalizada», sin garra y sin dolor, tediosamente repetida. En la temporada que está a punto de transcurrir, los recintos festivaleros, salvo contadas excepciones, estuvieron semivacíos, sin ese calor multitudinario de otros años; y es que el respetable se cansa de pagar 1.000 o más pesetas por asistir a un espectáculo devaluado, donde el Flamenco alcanza su más baja cota de expresión, y donde los profesionales, ya sea por déficit del propio entorno, o porque nadie puede desgarrarse, cuando tiene contratados 40 recitales en los 40 días siguientes, se limitan a cumplir con el ofrecimiento de dos o tres cantes que como único repertorio, repiten en todas sus actuaciones. El Festival Flamenco se acaba, y sin duda comienza otra época de la que no sabemos qué ventura o desventura, nos deparará.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1985-07",
    "year": 1985,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 305,
    "article_char_count_full": 1950,
    "article_char_count_review": 1950,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
