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
    "article_id": "1981-11-13-left-presencia-jonda-en-pablo-gargall",
    "article_text_for_review": "A mi parecer, no ha tenido la significación que debiera la conmemoración del centenario de otro Pablo universal, el aragonés Gargallo. Un artista que supo modelar el hierro con las yemas de los dedos. De algo de carne tan dura, tan viril, tan añeja, supo extraer la voluptuosidad, la flexibilidad, el aire que flota y define, la lírica de lo épico. Gargallo fue el primer escultor que abandonó lo grávido para modelar lo que palpita en el vano y la oquedad con una poesía de lenguaje nuevo extraido, al decir de Gaya Nuño, de «una lección viejísima que se conocen todos los herreros de pueblo, todos los que han recortado un galio de hierro para la veleta de la iglesia. El aire colabora con el herrero y con el gallo...»\n\nY en la obra de Gargallo, como en la práctica totalidad de esa hermosa generación de artistas españoles de la Escuela de París, no pudo faltar la representación jonda en su humanísima, estética y, sobre todo, sensual expresión. Ahí quedaron para siempre y nosotros sus torsos de gitanos, las múltiples cabezas de picaores, La Farruca —lamentablemente perdida—, las mujeres con mantilla, etc. etc. y, sobre todo, la Pequeña bailarina española. Una bailaora de metal a la que el arte convirtió en junco; en la que el sabor arañado del óxido de la materia se torna en cálido regusto sensual: el frío del hierro se puebla de ardores, de modulación musical, de íntimo calor. Gargallo supo que en toda materia anida un corazón, que el aire no es refugio de silencios. Del hierro, nacido para herramienta, símbolo del sudor, el artista ha extraido todo el noble refinamiento suntuoso de nuestro tiempo. Aquí, en esta bailaora, en esa inconfundible contorsión, está todo el arrebato de la sangre y la gracia embrujada en gesto jondísimo: el reposo, la firmeza, la pasión y el desplante del auténtico señorío —Pierre Courthion diría de ella: «La Española se abre y despliega como una flor»—; pero aquí, como en toda su obra, la anécdota es trascendida —algo que interesa resaltar— para convertirse en el más hondo pálpito de las raíces, en una vibración espiritual que se eleva sobre el propio ritmo escultórico; algo así como la detención de una imagen incorpórea que huye o, tal vez, la representación artística de la ligereza de densidad.\n\nManuel Urbano",
    "title": "Presencia jonda de Pablo Gargallo",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 389,
    "article_char_count_full": 2270,
    "article_char_count_review": 2270,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-11-13-right-diego-clavel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen...\n\n—Diego, ¿de dónde viene tu afición tremenda?\n\nTe puedo decir que me inicié formando parte de un grupo de campanilleros, en el que yo hacía los solos. Oye, ¿sabes?, en La Puebla siempre hubo grupos de campanilleros que cantaban mu requetebién. Bueno, pues en ese grupo se vió que yo destacaba un poquito porque, la verdad, se nace con el cante, que este no se aprende; se aprenden los estilos y los palos, pero el cante no se aprende se nace con él dentro.\n\nNo, yo no tengo ningún antecedente cantaor en mi familia. Siempre digo que la raiz cantaora de mi casa la he iniciado yo. Mi madre, eso sí, cantaba; pero cantaba como cualquier persona. Si quieres te digo más: en mi pueblo, en La Puebla, no ha habío cantaores de renombre. Se habla de un tal Gallardo que,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"alma\"]\n\n; por eso nunca he querido dedicarme al cante, eso te lo puedes creer. Mira, si yo hubiese sido un buen albañil... ¡Cuánto me hubiera gustao y cuánto daría por ser un buen albañil! Lo que pasa es que era un albañil mu malo, mu torpe, vamos, que aquello no me entra-ba a mí en la cabeza; si hubiese sío al revés, yo sería hoy un buen albañil en vez de un cantaor. Nunca he querío dedicarme a esto; pero hoy me entrego al cante con toa la fuerza de mi alma y mis entrañas. El caso es que mis hermanos se compraron un R-β pa llevarme y traerme... Volviendo a lo de Cabra, me hicieron una prueba y me seleccionaron para la final; yo me ilusioné un poquito porque me dieron el tercer premio, aunque, la verdad, hubo protestas favorables a mí porque consideraban que debía haber quedado en mejor lugar. Total, que aquello me gustó ya un poquito, aunque todavía tenían mis hermanos que seguir arrastrándome Volví otro año a Cabra, pero no me seleccionaron para la final. Fíjate lo que son las cosas, ese mismo año me presenté en Mairena al grupo de siguiriyas y martinetes y me llevé el premio. En aquellos años ganar el concurso de Mairena era lo máximo para un aficionado; prueba de ello es que de allí salió Camarón, Meneses... vamos, que ha salído mu buena gente de allí. El año que yo lo gané, Calixto ganó el de malagueñas. .—Después vinieron tus grabaciones con las letras éticas y de testimonio político de Caballero Bonald... Bueno, a raíz de que yo ganara el premio de Mairena, Moreno Galván me llevó a Madrid; si Paco no me hubiera llevado a Madrid, quizá, yo estaría todavía en La Puebla. Con él —tu sabes que\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\no a cantar mis letras. Concretamente, en el disco «Encuentros» hay cinco cantes con letras mías; también en el último todos los cantes tienen mis letras. .—Generalmente, y porque te lo piden, realizas el cambio de Manuel Molina. Creo que se puede decir sin riesgo de equivocarse que, tras Manuel Vallejo, has sido quien más ha revalorizado este cambio. ¿Cómo surgió esto en tí? A este cante yo no lo conocía como de Manuel Molina. Este cante se lo escuché a Antonio Mairena, y Antonio lo hacía sin ligarlo tanto como Vallejo. Entonces yo le dije a Francisco: «Francisco, yo puedo hacer este cante too seguío», y él me dijo que sí, que lo hiciera si podía. Y todo esto sin habérselo yo escuchao a Vallejo, y resulta que lo ligué por mi cuenta, y parece que esto lo he reinventao yo. No el cante, sino la forma, que yo, como te he dicho, sólo había escuchao de esa manera en Mairena; luego fue cuando escuc\n\n[ENDING CONTEXT]\n\nlos viejos maestros, creo que siempre seguiremos adelante y nunca pasará nuestro cante. Mira, que hay cantaores que se comen el mundo en dos días y luego pasan. Yo, desde que salí —ahora llevo diez años— siempre he seguió la misma línea y, más o menos, siempre hago las mismas actuaciones, y cada año se eleva un poquito ese número. Está claro que los que estamos apegaos a las raíces no pegamos el bombazo para luego caer. Si el cante está basao en las raíces, creo que no se perderá.\n\nInstalaciones, reparaciones y mantenimiento\n\nMancha Real, 7 (Polígono Los Olivares) - Teléfono 2118 77\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen... Diego Clavel",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 2229,
    "article_char_count_full": 12235,
    "article_char_count_review": 4209,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "alma"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1981-11-15-right-el-testimonio-jondo-de-j-m-cabal",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\npor José Luis Buendía «Un palo detrás de otro le fueron dando a mi cuerpo, pero no van a lograr que diga lo que no siento».\n\nEl mundo del flamenco no sólo se logra con unas bases musicales más o menos fijadas por la tradición. Ya hemos visto cómo este arte es portador de la esencia de un pueblo humillado, perseguido, que echa con rabia sus coplas afue-ra para que no se le pudran dentro.\n\n(Martinete de J. M. C. B.)\n\nLas letras son, por lo tanto, algo fundamental en el flamenco. Ya hemos analizado algunas de las clásicas, de las ligadas a la tradición del primitivo cante gitano. Ahora vamos a analizar brevemente el compromiso de un intelectual con el mundo temático del flamenco, que ha sabido poner su arte culto al servicio de este arte popular. Vamos a repasar brevemente, más que como\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_01 | trigger=\"verdadero\"]\n\nintelectual con el mundo temático del flamenco, que ha sabido poner su arte culto al servicio de este arte popular. Vamos a repasar brevemente, más que como estudio riguroso a manera de breve antología, este puñado de letras que nuestro autor, Caballero Bonald, ha compuesto no hace mucho para ser cantadas por magníficos profesionales del cante como Turronero, Diego Clavel, Manuel Soto «Sordera», etcétera. Pretendemos tan sólo dar a conocer este verdadero ramillete lírico en su versión escrita, ya que, como es lógico, el sitio de estas letras está en la versión que el cantaor haga de ellas en el mensaje humano y artístico que transmitan a un auditorio compuesto por gentes diversas, de gustos populares y sencillos, como es siempre, o debiera serlo al menos, el verdadero sujeto receptor del cante flamenco. Queda, pues, lejos de nuestra intención una profundización crítica en estas letras, ni mucho menos una revisión poética de Caballero, labor que creemos haber realizado ya al ocuparnos de su lírica. El camino de coplero popular que acaba de emprender nuestro autor es algo simplemente iniciado, habrá que esperar a que fructifique en el difícil arte oral para el que está muy bien dotado. Sólo así sabremos, a la vista de los resultados humanos que los distintos cantaores consigan con estas letras, si éstas han cumplido el cometido para el que fueron escritas y podamos identificarlas con ese «Cantar» del que nos habla otro poeta culto, Manuel Machado, cantar que va alejándose de quien lo compone para hacerse patrimonio del pueblo: «Cuando la gente ignore que ha estado en el papel, y el que lo cante llore como si fuera de él... copla de mis amores, cantar de mis dolores, entonces tú serás la copla verdadera, la alondra mañanera que lejos volarás... y en labios de cualquiera, de mí te olvidarás...» (Manuel Machado: «El Cantar») Mientras que esto sucede contentémonos con ambientar estas copillas de Caballero en sus circustancias populares, puesto que según mi punto de vista este popularismo es la principal de sus características. Al igual que las primitivas co- plas flamencas, éstas han nacido ancladas en lo más sencillo y cotidiano de la realidad gitano-andaluza, pero también en sus aspectos más esenciales; son coplas de amor, de pasión familiar y sobre todo de marginación, de pena por una situación injusta que va a ser confiada a ese reduc-to íntimo del cante. Al igual también que las primitiv\n\n[ENDING CONTEXT]\n\nvale que atravesara otra cosita algunos días»\n\nCon esto hemos llegado al final del trayecto que nos habíamos propuesto. Es posible que alguna de estas letras, que hemos recogido de oído, puesto que no están publicadas en ningún sitio, contengan algún pequeño matiz o variante respecto al original por error de nuestra transcripción al papel. En todo caso, aunque pidamos disculpas por ello a su autor, no habríamos hecho más que repetir lo que ha sido y es una constante de toda poesía oral: la posibilidad de sufrir alteraciones a bordo de ese bello vehículo que es la transmisión de boca a boca.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El testimonio jondo de J. M. Caballero Bonald",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "15-19",
    "page_number": 15,
    "word_count": 4886,
    "article_char_count_full": 27887,
    "article_char_count_review": 4054,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_01",
        "family": "AUTH",
        "trigger": "verdadero"
      }
    ]
  },
  {
    "article_id": "1981-11-20-left-del-congreso-de-almeria-y-una-gr",
    "article_text_for_review": "por Manuel Yerga Lancharro\n\nE s público y notorio entre los asistentes al IX Congreso en Almería, que sucedieron cosas ciertamente desagradables e imputables a personas concretas. ¿Creen los organizadores, pongamos por caso, que se puede considerar como error el que no nominasen como de mi archivo los cantes de las dos cassettes? Sinceramente, pienso que todo respondió a un plan preconcebido para resaltar el mairenismo, como si Antonio Mairena —y ese es un error de los mairenistas—, dada su incuestionable categoría de gran cantaor, necesitase de tales arquestamientos. ¿Fue, acaso, expontánea la entrega de la petición a favor de Mairena? Me permito dudarlo [Cuánto mairenismo] Y si a todo esto unimos la evidente lucha por el poder, por conquistar una silla en el ejecutivo, los no disimulados deseos de figurar y ciertos tejemanejes, de verdad, decido apartarme de un ambiente tan poco grato.\n\nComo los asistentes conocen, renuncié publicamente a ocupar un puesto en la Mesa, argumentando que tanto yo como Mairena, como cualquier otro cantaor por eminente que sea, no debe ocupar un puesto entre los ejecutivos. La razón es clara. Como hay muchos Yergas y Mairenas, admitiéndolos a todos, con el tiempo no habría lugar para e propio ejecutivo. Y aquí quiero insistir que admiro a Mairena reconociendo su aportación a la nómina de cantes; pero que también admiro, junto a no pocos aficionados, a los grandes payos y gitanos: Fosforito, Rafael Romero... Por cierto, si se acordaron de Mairena, ¿por qué, pongamos por caso, se olvidaron de Don Antonio Fosforito? ¿Es que no se considera uno de los mejores cantaores desde el ya lejano Concurso de Córdoba? IAhI, alguien me dice al oído que no es gitano. Yo tampoco lo soy, ni siquiera andaluz, quizá por ello en treinta y cinco años de entrega al cante no he recibido más que sinsabores. IA lo peor es que no he hecho nada por el flamenco y en favor del conocimiento de sus intérpretes! Y esto no es inmodestia. Quede\n\nun ejemplo de lo que, por no emplear un calificativo más duro, supone falta, digamos de rigor. Si lo aduzco es como prueba de realidad vivida y no como vano personalismo:\n\nEn cierta ocasión se le interrogó a un señor los motivos por los que quedara desierto el premio de investigación de Jerez, cuando era notorio que yo, desde hacía muchos años, me dedicaba a esas diligencias. La contestación fue terminante: «Ese señor lo que consigue en sus investigaciones se lo queda para él». A esto más vale no replicar, los lectores de «CANDIL» pueden apreciar las reservas que efectúo en mis investigaciones, o las muchísimas personas a las que generosamente he facilitado cuanto solicitarán de mi archivo o, mejor todavía, ese señor -¿el mismo? - que reclamara mi cabeza por la biografía que publiqué de Chacón. ¿Tengo la culpa de que el artista sea hijo de padres desconocidos, de que el Espasa tenga que rectificar la fecha de su nacimiento, de que se imponga por seriedad retirar de cierta casa de Jerez el letrero que dice «aquí nació Antonio Chacón»?\n\nPero, regresando al Congreso de Almería, hay algo que me interesa aclarar por el bien exclusivo del cante. En una de las cassettes distribuidas, «Cantes de Levante», la taranta minera «Hay que madugar», cantada por el Cojo de Málaga, no es un taranto, como se dice. Algo que tengo que denunciar, como la desastrosa calidad de la grabación que, de ninguna forma acepto como provinente de mi archivo y que yo grabara de discos seminuevos, como es el caso de las tarantas de superficie. Y el origen de estas pésimas grabaciones no puede ser otro que, de las cintas grabadas por mí, utilizasen copias distintas para la grabación; algo que ya ocurrió con la casette de Manuel Torre. Confío en que, más que por la responsabilidad de mi archivo - que no existe -, por el bien del cante, los encargados de las grabaciones aclaren lo sucedido.",
    "title": "Del Congreso de Almería y una grabación desafortunada",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 660,
    "article_char_count_full": 3860,
    "article_char_count_review": 3860,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-11-20-right-los-gitanos-y-el-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\npor Antonio Mata Gómez\n\nQuiero advertir que el pueblo gitano, así lo llamaré en el trabajo, merece un crédito y un cariño muy contrario a la actitud de algunas poblaciones españolas, donde están intentando marginarlo, incluso, de sus territorios. En nigún momento voy a emplear las palabras «raza gitana» para evitar que alguien, que quiera «cojer el rábano por las hojas», pueda insinuar siquiera que hablo de racismo. Contra la extendida teoría de que los «psíganos, egipcianos o gitanos» llegaron a España sobre el año 1490, existe la fundadísima y profunda de que, procedentes de su país de origen, el Indostán, atraviesan parte de Asia y Europa hasta llegar a España algo antes de 1425; es decir, 65 años antes. Y una prueba fehaciente del hecho nos la da el estudioso Carlos Almendros, quien,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"conserva\"]\n\ngen, el Indostán, atraviesan parte de Asia y Europa hasta llegar a España algo antes de 1425; es decir, 65 años antes. Y una prueba fehaciente del hecho nos la da el estudioso Carlos Almendros, quien, aportando un testimonio de Amanda López de Meneses, da cuenta de un documento salvaconducto que portaba Juan de Epipto Menor, por el que se autorizaba a él y a su gente el paso por los Reinos de España, otorgado por Alfonso V el Magnánimo, y que se conserva en el archivo de la Corona de Aragón de Barcelona, en el que se ordena se diera toda clase de facilidades a los gitanos para su paso por todas las regiones españolas. Posteriormente, tanto Alfonso V como Juan II, otorgaron salvaconductos a nuevos grupos de gitanos que entraron en España, por Cataluña primero y Andalucía por mar, después. En general el pueblo los recibió bien, hasta que por sus extrañas costumbres, supersticiones, nomadismo, etc., hicieron que, al cometer pequeñas tropelías, hubieran de dictarse algunas prag máticas a partir de los Reyes Católicos, para de una parte, tranquilizar a la población y de otra, obligarlos a asentarse en el lugar que eligieran para su vida y trabajo, y tenerlos controlados bajo vigilancia. Los gitanos que se instalaron en España poseen una serie de valores, como son, el profundo respeto de los hijos a los padres; la alta estima en que tienen fijada la valoración de la familia; la profunda religiosidad a su Dios, reflejada en la gran aceptación de los misterios de la religión; la fidelidad de la palabra entre ellos y la incapacidad para mentirse; el máximo respeto que suelen tener con el gitano de más edad de la comarca, siempre que sea reconocido por ellos; la gran confianza en que «mañana, Dios proveerá» y, por último, el valor que dan las jóvenes gitanas a su virginidad. Frente a estas virtudes y, quizá por los am\n\n[ENDING CONTEXT]\n\nparecen en absoluto al cante flamenco.\n\n¿Que su arte es excepcional? Por supuesto.\n\n¿Que son unos grandes intérpretes? Seguro.\n\n¿Que en la actualidad la llave del cante la tiene, con todo su mérito, un gitano? Seguro.\n\nTodo esto es completamente cierto, pero ello no es óbice, para que, reconociendo su extraordinaria calidad de interpretación y su difícil habilidad para la recreación no los considere como creadores de ningún estilo de cantes.\n\nAsí que demos a Dios lo que es de Dios, a un Devel lo que es de un Devel, al flamenco lo que es del flamenco y a los gitanos lo que es de los gitanos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Los gitanos y el flamenco",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1218,
    "article_char_count_full": 7244,
    "article_char_count_review": 3468,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "conserva"
      }
    ]
  }
]
```
