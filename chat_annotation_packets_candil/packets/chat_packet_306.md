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
    "article_id": "1995-05-17-left-camar-n-el-genio-perdido-domingo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE1 cante de Camarón es realmente patético. Porque así y no de otra forma hay que calificar la pelea conmovedora entre el hombre y el fuego, donde no hay otro camino que huir para escapar o someterse y perecer.\n\nSu voz menuda casi «laina», con poca fuerza, no es el mejor instrumento para comunicar todo lo que de «jondura» lleva dentro el «cantaor».\n\nSu vida se resume en la busca anhelante de una nueva manera de llenar su voz con los modos y matices tonales que para su inspiración y gusto, eran necesarios darles de contenido.\n\nAl principio fue un «cantaor» ortodoxo, ajustándose su voz a los cantes, llenando de insinuantes sonidos nuevos los cantes viejos y derramando su genio por los vientos, acompasando en su cante lo viejo y lo nuevo. Por este sendero hubiera llegado sin dilación a ocupar\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nontenido. Al principio fue un «cantaor» ortodoxo, ajustándose su voz a los cantes, llenando de insinuantes sonidos nuevos los cantes viejos y derramando su genio por los vientos, acompasando en su cante lo viejo y lo nuevo. Por este sendero hubiera llegado sin dilación a ocupar el trono del reino. Pero el manantial de su inspiración con el transcurrir de los días se convierte cada vez más en río caudaloso que no cabe en el cauce estrecho de su voz menuda y esto fue la causa del duelo con su arte. Entonces, si no se puede influir sobre el sonido y su eco, sí cabe la posibilidad de hacerlo sobre el caudal de «jondura» que rebosa de su profundo pozo. Y empezó la lucha. En muchos de sus cantes, y en especial en aquellos donde se inició, los auténticamente propios, los de su bahía, la curva melódica vamos encontrándola con el transcurso del tiempo, cada vez, más asimétrica, más incompleta, reiterando alargamientos postizos, en melismas elementales, que intentan anudar la ruptura, para una y otra vez, volver a necesitar de glosolalias o de los gritos enharmónicos, para incluso cuando la voz ya no tiene timbre en su registro, utilizar los chillidos destimbrados buscando que remienden el roto, lo que a veces lleva el cante a una confrontación entre lo melódico musical en el modo tonal «jondo» y aquello que lo repele por su falta de armonía. En alguna ocasión todo este esfuerzo de intento creativo, nos quiere en apariencia dar la impresión, de que su manera de hacer los cantes no se acomoda a las estructuras con que los compases amalgamados y simples, encorsetan a las líneas melódicas, para arquitecturar distintas clases de cantes, pero realmente esta apariencia nos parece deliberadamente engañosa, porque la base del cante que busca crear Camarón, el fundamento de su lucha es que intenta adaptarse a la ortodoxia del compás, tanto como el registro de su menuda voz pueda permitírselo. Alguien puede creer que hablamos sin datos fidedignos para aventurar este comentario,\n\n[ENDING CONTEXT]\n\nde dolor y analgésicos, de realidad virtual, y de vida que se escapa de prisa en cada cante, de íntima sensación de seguridad de que en todo momento el «cantaor» sabe que éste es su último dolorido trabajo, y que poco después también llegará para él en soledad el conocimiento supremo, el encontrar la respuesta de todas las respuestas, la última, la que también había de aclarar su postrera pregunta, cuando solicitando ayuda antes de dejarse llevar por la muerte solloza: «oamaita» ¿qué es lo que me pasa a mí?... ...Ya no hay más remedio que conformarse a la voluntarita de un Debel del cielo...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Camarón, el genio perdido Domingo",
    "periodical": "candil",
    "issue_id": "1995-05",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1225,
    "article_char_count_full": 7286,
    "article_char_count_review": 3617,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      }
    ]
  },
  {
    "article_id": "1995-05-18-left-jerez-su-cuna-lo-acogi-con-arte-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPresentación en Jerez del segundo volumen de la serie «Tríptico Flamenco», de Vicente Soto «Sordera»\n\nY Jerez, su cuna, lo acogió con arte. No podía ser de otra manera, pues un nativo de esta tierra y miembro de uno de los clanes cantaores de mayor prestigio merecía este trato. Vino a su Jerez a presentarnos el trabajo con que continuaba su «Triptico Flamenco», un compacto que muestra su conocimiento sobre el flamenco jerezano. Si antes lo hizo con Cádiz, su pueblo, su arte y el de sus mayores, tenía que plasmarse en el posiblemente más amplio de sus trabajos. Y no es porque haya querido volcarse más con el de su tierra, es simplemente que sus vivencias, sus memorias, sus maestros, sus amigos, sus rincones favoritos y sus familiares son de aquí, y por tanto su conocimiento de todos es\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_01 | trigger=\"dentro\"]\n\nhizo con Cádiz, su pueblo, su arte y el de sus mayores, tenía que plasmarse en el posiblemente más amplio de sus trabajos. Y no es porque haya querido volcarse más con el de su tierra, es simplemente que sus vivencias, sus memorias, sus maestros, sus amigos, sus rincones favoritos y sus familiares son de aquí, y por tanto su conocimiento de todos es mucho más amplio. Y lo dijo a boca llena: «Este trabajo creo que es el más añorado por mí, porque dentro de la trilogía, el de Jerez es añorado enormemente por mí, porque es mi raíz, donde yo he nació y por considerar a Jerez un pilar fundamental del cante. Este arte, sin el pilar de Jerez, se vendría al suelo. Y mi intención —dentro de mis posibilidades— ha sido la de efectuar un trabajo honesto y conseguir la meta que me había marcado. Creo que es un trabajo importante en mi carrera». La expresión satisfecha de su cara denotaba esa añoranza por él matizada, así como el orgullo de sentirse jerezano, constatando a la vez que nunca va a dejar de sentirse jerezano, constatando a la vez que nunca va a dejar de reivindicar los maestros históricos del flamenco nacidos en Jerez: «Mira, hablo con gentes mayores de mi familia que han escuchao cantar a bastantes, y algunos dicen que el cante del \"Marruro\" lo hago muy bonito; otros me dicen que les gusta la cabal del \"Sernita\"; algunos que las bulerías de \"La Plazuela\" —que las hago con un aire muy personal—. A todos los cantes que interpreto intento darles un toque propio, por tanto, cuando a unos les gusta una cosa y a otros les\n\n[ENDING CONTEXT]\n\ndeben de preocuparse por el flamenco. Tenemos como ejemplo mi caso, pues el trabajo que estoy realizando con el “Triptico Flamenco”, para cualquier casa discográfica independiente supone un gasto económico bastante importante, ya que son muchas horas de estudio de grabación y tú sabes el precio que tienen; así como el montante de profesionales técnicos y artísticos que participan en estos trabajos. Esta preocupación que Radio Nacional de España tiene sobre el flamenco, debe ser compartida por el resto de las Instituciones. Desde luego, ellos lo han demostrao con esta labor».\n\nManolo Canalejas\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Jerez, su cuna, lo acogió con arte Antonio",
    "periodical": "candil",
    "issue_id": "1995-05",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1096,
    "article_char_count_full": 6340,
    "article_char_count_review": 3166,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "dentro"
      }
    ]
  },
  {
    "article_id": "1995-05-19-left-discografia-flamenca-rafael",
    "article_text_for_review": "¿E stamos equivocados los que reivindicamos el escalofrío «jondo» que nos produce el emocional que jío flamenco? ¿Son los artistas conscientes de esta reclamación, o por contra la soslayan con el convencimiento de que otros complementos musicales introducen tal riqueza de matices en nuestro arte, que no es necesario confluir en la transmisión del sentimiento «jondo»?\n\nYo me mantengo en mis trece, pues lo que se produce dentro de mí cuando escucho el quejumbroso lamento siguiriyero de un cantar, pesa más en la balanza de la emoción flamenca, que toda la serie de arreglos, entonaciones, arropos musicales o divertimentos artísticos a los que nos están conduciendo los nuevos valores. Y no por ello dejo de reconocer que los trabajos efectuados con comedimiento y calidad carezcan de méritos y quizá se configuren en el tiempo como el flamenco que ha de imperar.\n\nYa he referido en otras ocasiones que «el matiz ortodoxo imprimeme al flamenco sentimiento, profundidad, quejío, ansias de libertad, grandeza de pueblo... El enfoque heterodoxo —en la mayoría de los casos— aporta donosura, desenfado, cierto abandono de los esquemas, ¿búsqueda de nuevos catrocinan: Junta de Andalucía. Consejería de Cultura y Medio Ambiente. Dirección General de Fomento y Promoción Cultural. minos?, tratamiento festero... y sobre todo, la aportación de otros instrumentos que nada antes han tenido que ver con este arte. Con el primero al aficionado se le eriza el vello. Con el segundo matiz se siente receptivo, alegre y expectante.\n\nPues bien, estos conceptos o matices del cante antes reseñados se ubican en las interpretaciones que José Heredia ha incluido en sus dos últimos compactos, los cuales llevan los títulos de «Joselete de Linares» y «El cante de Joselete»; el primero de ellos grabado como premio a la consecución del máximo galardón del II Concurso Flamenco de la Comunidad Autónoma de Andalucía, y el segundo por expresa deferencia de la Diputación Provincial de Jaén, pienso que al considerarlo como el cantar de mayor proyección de futuro de la provincia. Aunque habría que aclarar que el primero es el segundo y viceversa, en cuanto al enclaustramiento en el estudio de grabación se refiere.\n\nEn «Joselete de Linares» se aprecia la línea ¿moderna? y cierta tónica a entremezclar la ortodoxia y la heterodoxia. Y es que en muchas ocasiones el artista no puede sustraerse a lo que su subconsciente le ordena por ser su formación can-\n\ntaora netamente clásica. Así, en sus iniciales bulerías se denota con toda nitidez lo referido, amén de la inclusión de otros instrumentos que solapan a la guitarra y de la inclusión de un cansino estribillo. Mucho más flamencas son sus siguientes soleares, con una acompasada entrada por Alcalá, bellezas melismáticas y ciertas entonaciones caracoleras seguidamente, para continuar con algo de apresuramiento por Triana, antes de rematar por Cádiz. Nuevamente «agitana» la taranta de su tierra con más fuerza y convicción en la segunda interpretación. Sin embargo, en los tangos, para qué sirven los cajones y otras lindezas innovadoras instrumentales?, pues sus calidades de voz y metal flamenco se pierden en favor de la modernidad y de las entonaciones rebuscas. En los fandangos abunda en el criterio y entonación moderna. Buena afinación hacia Manuel Torre y entonación personal de la creatividad del jerezano, para en la continuación seguir matizando el personalismo. ¡Qué bien suena sola la guitarra! En la cabal del Loco Mateo aborda algunos ecos modernos, mas su resolución resulta flamenca. Y tras la ortodoxia de las siguiriyas, la heterodoxia de las bulerías por soleá, una grabación que parece hecha en el neoyorkino «Harlem» en plan de Jam-Sessión y con el heterodoxo espíritu de Caracol sobrevolando el oscuro local cantaor. Por el sonido jazzístico que se escucha y tras la comproba-ción de los acompañantes que figuran en el compacto, se aprecia un montaje musical. Tomás Pavón no se merece esto. ¡Ah!, Curro Frijones, tampoco. En las malagueñas aborda con acierto y melisma la creatividad del Canario, para rematar con adecuados aires abandolaos. Como parte final del comentario de este compacto, resaltar el adecuado tratamiento métrico, por el\n\nbuen desarrollo cantaor que efec- túa el artista, de las composiciones del maestro Francisco Almagro.\n\nY como el segundo es el primero, en «El cante de Joselete», el de Linares ha actuado como si hubiera querido dejar sentadas las bases de su formación flamenca. Es un compacto más clásico, ortodoxo y formal, si por formal entendemos la norma natural de expresar el flamenco; o sea, el cantaor y el guitarrista complementándose y aunando esfuerzos por transmitir sus esencias «jondas».\n\nEl trabajo lo abre con unos tangos en una tesitura festera y ciertas influencias modernas, con algunas fases estentóreas. Las soleares las enfoca con una salida fresca y caracolera y una ubicación permanente en Alcalá con reposamiento y compás. En las malagueñas matiza el personalismo de La Trini con acertado melisma y lo enlaza con aires abandolaos. Continúa en el mismo terreno a través de unas rondeñas en las que evoca la creatividad de Rafael Romero, rematándolas con fandangos de Lucena. Sus siguientes bulerías están enfocadas con el tratamiento que Caracol efectuaba las de Jerez, para seguidamente abordar el cuplé por bulerías, ciertos enfoques de romance flamenco y estribillo con determinada comercialidad. Las tarantas de su tierra vuelven a tener ciertos melismas marcheneros, aunque su personal metal de voz gitana aflamenca los ecos dulzones. Son las siguiriyas las que una vez más patentiza su calidad cantaora y su acercamiento a Manuel Torre y muy concretamente en su creativo cambio de Santiago y Santa Ana. Acaba la obra con fandangos y salida caracolera, para posteriormente evocar la tesitura de «Chocolate» y rematar con un enfoque del Gloria a través del eco caracolero de Beni de Cádiz.",
    "title": "Discografia flamenca Rafael",
    "periodical": "candil",
    "issue_id": "1995-05",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 946,
    "article_char_count_full": 5909,
    "article_char_count_review": 5909,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1995-05-20-left-presencia-de-c-ntico-en-el-flame",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAutor: Agustín Gómez\n\nCol. Arca del Ateneo. Córdoba, 1995\n\nManuel Gahete\n\nE stablecer puentes de conexión entre poesía y música es llover sobre mojado, porque —y de esto no me cabe la menor duda—la poesía sin música no existe. Esta tesis, avalada por escritores tan deslumbrantes e innovadores como Poe, Carlyle o Verlaine, supone la ruptura esencial de esta manifestación literaria con la prosa, por mucho que los adjetivos y los esnobismos pretendan argumentar, bajo el capcioso título de vanguardia, razones que condenan a los émulos de Tántalo a una persecución frustrante, irracional y deletérea.\n\nQue la poesía se concibe, en sus orígenes, para ser cantada es un aserto incuestionable. Y esta conjunción connatural a su nacimiento ha venido conformando la ciencia de la palabra poética como\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"artejos\"]\n\naria con la prosa, por mucho que los adjetivos y los esnobismos pretendan argumentar, bajo el capcioso título de vanguardia, razones que condenan a los émulos de Tántalo a una persecución frustrante, irracional y deletérea. Que la poesía se concibe, en sus orígenes, para ser cantada es un aserto incuestionable. Y esta conjunción connatural a su nacimiento ha venido conformando la ciencia de la palabra poética como ley inmanente que trasmuda los artejos invisibles de tan sutil entramado. Penetrar en el complejo universo de estas relaciones supondría desviar nuestro objetivo prímicial hacia espacios casi ignotos para una «inmensa minoría» de poetas; y no se trata de descubrir los vacíos que provocarán la caída de tantos dioses de barro, sino de hacer diáfano lo que la naturaleza jamás ha creído necesario dilucidar. Presencia de CÁNTICO Por otra parte, la poesía muestra las brasas y el humo de un incendio en la sangre, por ser ésta referente inequívoco de la vida; expone sentimientos desbordados, pasiones ocultas, tensas fibras de dolor y gozo, realidades ajenas a la realidad de la retina. Sentimiento y música alcanzan el cenit de las líneas en la parábola del flamenco. Y cualquier otra relación que pretenda establecerse, aun necesaria y por supuesto posible, queda empañada por la suprema verdad que aúna la voz desgarrada y el verbo lírico. Podremos sentir con intensidad gradativa el quejido humano en liza corporal con la guitarra vibrando. Para unos, esta confrontación supone el resuelto más íntimo y auténtico de la naturaleza anímica; para otros, la manifestación diáfana de un arte, la expresión profunda de un carácter autóctono y mágico. Agustín Gómez devana toda la luz de su pasión y su ciencia en las páginas afables, de ágil comprensión y lectura, llamadas a la elegía y al panegírico, tocadas por la bondad y sabiduría de un hombre consagrado a la investigación del flamenco, en esta reflexión trascendida que el Ateneo de Córdoba, pendiente siempre de ponderar los valores de nuestra identidad y cultura, deja impresa a la presente y las futuras generaciones. Su estructura se organiza en torno a la connivencia innata existente entre poesía y música, de la que ya hemos esbozado difusas pinceladas; teniendo como eje el interés del impulsor del grupo cordobés Cántico, Ricardo Molina, por el universo mítico del flamenco, del que era un entusiasta redomado y amador ferviente. A él se presta especial atención en este ensayo, descubriendo peculiaridades difuminadas por el allegamiento y el rumor; señal\n\n[ENDING CONTEXT]\n\nDemasiada responsabilidad y honor para estos seres, trasverberados, no sé si a su pesar, por un venablo de fonemas y aliento; agridulce aliento que enlaza espacios distantes, adivinándose próximos en el sentimiento y en la música, puentes inequívocos alzándose entre la poesía y el flamenco, veneros germinados desde una misma luz, regresando en eterno retorno a la búsqueda de las comunes y originales raíces, incapaces de subsistir sin el mutuo alimento, sin la recíproca entrega, sin el contacto amargo y vigoroso donde la pasión se transforma en baile, en palabra, en comunión, en vida.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«Presencia de Cántico en el flamenco»",
    "periodical": "candil",
    "issue_id": "1995-05",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1211,
    "article_char_count_full": 7504,
    "article_char_count_review": 4156,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "artejos"
      }
    ]
  },
  {
    "article_id": "1995-05-21-left-antiguos-cantos-de-linares-jos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMorena, si me arremango\n\ny se me quita la murria,\n\nno he de menester bandurria\n\npara tocarte el fandango.\n\n«El Fandango». Madrid, 15-7-1845\n\nE s mi propósito reunir en este escrito las información poco conocidas o nuevas que poseo sobre algunos cantos populares de Linares y situar éstos en el contexto histórico de la localidad para darlos. Como habitualmente los resultados se siguen de las circunstancias, la mejor comprensión de los unos obligará a analizar aspectos de las otras.\n\nAgradezco a Rafael Charquero y Miguel Aguilar, de Linares; a Luis García, de El Centenillo; a Eloy Martín, de Ceuta, y en general a los miembros de la «Peña Flamenca Cabrerillo», la ayuda y críticas con que me obsequiaron y la paciencia con que me han soportado.\n\nRecurriré con frecuencia al manuscrito de don\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\ny apuntes de tiempos antiguos» (1), la fuente más abundante y fiable sobre ciertas facetas del folklore local. En lo que respecta a los cantos de bodas, los entonados durante los bailes fúnebres que para «no llorarle la gloria» se organizaban alrededor del féretro del párvulo difunto y los que festejan el renacimiento de la vegetación (2), hoy extintos, la general falta de datos significativos me impide ocuparme de ellos. Los de Navidad, tendrán mejor testigo en quien los estudie en Baeza y los pueblos de su tierra, por ser los mismos mejor conservados ahí. Guardan entera vitalidad los de Semana Santa y los de trabajo, que resumiendo la tradición de lírica popular y las influencias externas, evolucionaron hacia estilos específicos: La saeta por Linares, señalada por “Para celebrar la festividad de la Candelaria se prendían grandes hogueras —candeladas— y cogidos por las manos, mozos y mozas, en ancho corro y Estando el señor don gato sentadito en su tejado, marramiau, marramiau, marramiau. Una mano en la cintura y la otra en el costado etc. ». terminar en un martinete y los cantos mineros, cuya más genuina expresión es la taranta. Sobre esta última, desdeñada la tradición que la considera de siempre y unánime como canto linarense y aprovechando vaguedades más o menos poéticas de flamencólogos, poco y nada al corriente de la historia de la minería y su técnica, se han levantado reivindicaciones enmarañadas en confusa polémica que aguijonea la ambición de ilustrar el lugar de nacimiento de cada cual. Tiende este localismo a fundar argumentos en supuestas calidades intrínsecas a diferentes geografías y es telurismo que mondado de oropeles, viene pronto al lirondo «lo da la tierra» y su colofón de las «raíces». Lunas morenas. En el desordenado farabusteo de la llave del pretendido arcano, los hay que basan sus teorías en saber si éste o aquél cantaba así o asá, imponiéndose generalmente el estrecho límite temporal de los vagos recuerdos de uno que dice haber escuchado a otro o de los discos de pizarra, nombrando padres implícitos a un tal Cabogatero o a Rojo el Alpargatero. Se espera para pronto que pues genitor y lugar tienen, certifiquen fecha. Enhorabuena. (3) Manuel Sánchez Martínez y Juan Sánchez Caballero, «Una villa giennense a mediados del siglo XVI: Linares», pág. 49. (4) Según el catastro de Ensenada, en 1752, controlado ya el estanco del plomo por Hacienda, cien vecinos se dedicaban a su comer\n\n[ENDING CONTEXT]\n\nAguila. Se inició en los ya tradicionales espectáculos Jueves Flamencos, organizados por Manuel Morao, de Jerez de la Frontera, en 1968. Seguidamente actuó en las salas madrileñas Florida Park y en Pasapoga, con diversos elencos flamencos, habiendo recorrido en varias giras toda la Península, así como también Baleares y Canarias; y fuera de nuestras fronteras: Africa del Sur y Europa. Ha colaborado con la Cátedra de Flamencología y Estudios Folklóricos Andaluces, entidad de la que es miembro, en sus Cursos Internacionales de Flamenco. Imparte enseñanza de su arte en el Conservatorio de Jerez.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antiguos cantos de Linares José",
    "periodical": "candil",
    "issue_id": "1995-05",
    "year": 1995,
    "language": "es",
    "article_type": "article",
    "pages": "20-35",
    "page_number": 20,
    "word_count": 16902,
    "article_char_count_full": 103643,
    "article_char_count_review": 4067,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  }
]
```
