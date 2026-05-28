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
    "article_id": "1982-03-14-left-una-penita-mu-grande",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen\n\n—¿Cómo es posible que un valenciano como usted haya vivido tan intensamente el flamenco e, incluso, tenga todo un nombre cantaor?\n\n—Bueno, yo nació en Burriana, provincia de Castellón, y no tengo ascendencia andaluza ni cantaora en mi familia. Mi padre sí, era aficionado y cantaba en las tabernas con sus amigos. La verdad es que en mi pueblo se canta-ba poco, y entonces mucho menos, y yo no sé por qué me interesó el cante.\n\nCuando tenía nueve o diez años, no recuerdo bien, me fui con mis padres a Barcelona, donde empecé a escuchar un poquito más y a los quince o dieciséis comencé a hacer algunas cositas de cante en los teatros que, como tú sabes, era donde entonces se hacía algo. Y, claro, ya seguí trabajando en esto hasta ahora, que tengo setenta y tres\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"recuerdo\"]\n\nc. Como puedes suponer, fue una época muy mala para el cante. También, después de cantar en el auténtico Villa Rosa, nos íbamos al de la Ciudad Lineal, que era un teatro al aire libre. las primeras grabaciones las hice con Miguell Borrull y, luego, con el Niño Ricardo. Y después de la guerra, en troupes flamencas por toda España. Por aquel tiempo estaba la de «Los Peines» —antes estaba Vedrines, que era un buen empresario— y otras que ahora no recuerdo. También yo fui empresario en algunas ocasiones y un par de veces con Canalejas como socio, que íbamos a medias en el negocio y, la verdad, sacábamos para vivir. Canalejas de Puerto Real tenía una rubia, una furgoneta, que, en alguna ocasión, cuando no éramos socios, me la alquila-ba para que yo llevara a los artistas. También estuve con las compañías de Marchena y Juanito Valderrama. —Puesto que menciona a Canalejas, Marchena y Val-derrama, ¿qué opinión le merecen estos artistas? —Todo lo que hacía Canalejas me gustaba mucho. Hacía muy bien las fiestas y otros cantes. Siempre hacía las bulerías. Nos conocimos en Valencia, él entonces actuaba en un tablao que tenía Miguel Borrull. También coincidí muchísimas veces con Marchena y, también, con Juanito Valderrama y, hasta no hace muchos años, he estado con él. Juanito ha cantado muy bien, y es un artista que conoce muy bien el cante; ten en cuenta que Juan ha cantado mucho y lo ha vivido de cerca y, como yo, desde niño. Figúrate si conoce el cante..., mira, yo creo en eso que dicen de las voces; cuando una cosa se hace bien y se siente da igual una voz que otra. Si es una voz flamenca, buena y bonita, ¡claro!, es mucho mejor. —Sigamos con su biografía, después vino «Zambra»... —En Zambra estuve actuando casi veinte años; claro que en verano no, porque en verano salíamos con alguna troupe, estábamos un mes o dos trabajando, y luego volvíamos a Zambra. Mira que han pasado artistas por Zambra, pues todos nos llevábamos bien. —¿Cómo es que actuando usted en Zambra y siendo tan amigo de Perico del Lunar —por cierto, usted es padrino de su hijo Pedro, el tocaor— no lle\n\n[ENDING CONTEXT]\n\nhay unos que lo hacen y otros que no. Pero no creo que molesten al cantaor; claro que los que saben. A mí me gustan las dos escuelas guitarrísticas, aquella y la de ahora. En mi época había muy buenos tocaores y también los hay en esta.\n\n—Juan, a lo largo de la entrevista he podido observar que usted es hombre de pocas palabras.\n\n—¡Bueno! Sí, es cierto. Pero hoy me habéis hecho hablar más que en toda mi vida.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados\n\nEspecialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Una penita mu grande",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1843,
    "article_char_count_full": 10182,
    "article_char_count_review": 3720,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "recuerdo"
      }
    ]
  },
  {
    "article_id": "1982-03-14-left-una-penita-muy-grande",
    "article_text_for_review": "Acre y cálido irrumpe el pan en la calle humo amasado oloroso por encima de hornos y tejas\n\nhumo de augurio sobre los trigales.\n\nSonora la soledad candente cuarzo cristaliza en definitiva forma. ¡Soleá!\n\ndolencia cotidiana un respiro en cada soplo.\n\nAlejada de todo rito a diario llama repartiendo calor pidiéndolo y conmueve los recuerdos presentes\n\ngesto sombrío de generosidad amarga.\n\nAspera y templada voz que une con hilo recio de son ronco la faena de vivir al ansia del decir\n\nfilillo de aire por encima de las penas.\n\nFrancisca Gerardín",
    "title": "Una penita muy grande",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 91,
    "article_char_count_full": 545,
    "article_char_count_review": 545,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-03-16-left-federacion-de-las-pe-as",
    "article_text_for_review": "Por Antonio Escribano\n\nSiempre he anhelado que la gran afición flamenca pasase de la individualidad al grupo o peña, y de aquí a la familia provincial o federación. Deseo este en el que vengo empeñado desde hace tiempo, por lo que remito al lector a mi ponencia sobre el mismo en el VI Congreso de Zamora.\n\nY este deseo, que no es cosa distinta a una verdadera necesidad sentida y apreciada que comparto con numerosos aficionados, viene chocando con esa especie de individualismo que parece consistancial a los íberos y con los prejuicios, celosos e infundados, de quienes consideran toda acción conjunta como intromisión en la natural independencia de las Peñas; olvidando que toda Federación, regida por el más puro sistema democrático, jamás puede inmiscuirse en el régimen autonómico de las entidades que la integran, y que su función se reduce, a través de los representantes designados por todas y cada una de las Peñas, a buscar el mayor beneficio conjunto de las propias Peñas y el cante.\n\nMas, por el momento, el construir una auténtica y operante red de Federaciones, por lo que he podido colegir tras numerosas consultas a Peñas no federadas, no pasa de mera quimera. Y ello, fundamentalmente, porque una buena parte de sus presidentes —y miembros— ejercen una conducta dictatorial y caciquil en su ámbito, por lo que nunca cambiarían su puesto de «cabeza de ratón» por el de «cola de león». Su egocentrismo —incluso sin darse cuenta— es tan extremado, que sólo encuentran razones en su propia sinrazón. Va siendo hora de que dejemos todos a un lado el orgullo y reconozcamos nuestro grano de caciquillos; extendamos nuestros lazos de amistad, intercambiando ideas e ilusiones, hacia otras peñas hermanas; y, si de verdad luchamos por un reconocimiento del cante, aunémonos, que bien certero es el axioma de «la unión hace la fuerza».\n\nNo podemos quedarnos en la simple satisfacción que proporciona la existencia de más de cuatrocientas peñas a lo largo y ancho de nuestra geografía, ni en que la afición se haya quintuplicado y sea más culta e iniciada que la de pasadas generaciones. El número en sí no es signo positivo y valor suficiente —ni aun doblándose el actual de peñas y aficionados— para obtener el necesario reconocimiento y apoyo por la Administración, ni para fijar el cante en toda su verdad y pureza. Reclama, por el contrario, algo tan importante como es la unidad —que no significa uniformidad— en una Confederación Nacional de Peñas Flamencas.\n\nSoy consciente, por las razones apuntadas y muchas otras más, que el éxito de esta propuesta que, repito, no es nueva, no será inmediato; pero, al menos, impulsemos la conexión entre las Federaciones ya existentes, lo que redundaría en beneficio de esas grandes manifestaciones de eco nacional como concursos, congresos, etc., así como en la necesidad irrenunciable de una auténtica Confederación Nacional de Peñas Flamencas.",
    "title": "Federaciones de las Peñas",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 479,
    "article_char_count_full": 2901,
    "article_char_count_review": 2901,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1982-03-16-right-ramon-j-sender",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Manuel Urbano\n\nSí, de entrada, afirmo que la novela de tema o inspiración flamenca no ha tenido suerte literaria —otra bien distinta será la de público lector—, con razón podrá objetárseme que presente como novedosa a la más vieja, contundente y conocida de las evidencias. Pero, si ello es cierto, es algo que no acabo de comprender, de explicarme. ¿Si lo jondo ha tenido brillantísimos ejemplos de aprehensión intelectual en el teatro, el ensayo o la poesía, por qué su entronque en el campo narrativo —salvo algún relato breve— osciló entre el tópico, el melodrama, el colorido escapista o lo vanal y, en el mejor de los casos, el más trasnochado de los neorromanticismos? ¿Por qué el cante, tan reconocido como la más terrible expresión de un alma colectiva que no niega la individualidad\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"pasión\"]\n\npo narrativo —salvo algún relato breve— osciló entre el tópico, el melodrama, el colorido escapista o lo vanal y, en el mejor de los casos, el más trasnochado de los neorromanticismos? ¿Por qué el cante, tan reconocido como la más terrible expresión de un alma colectiva que no niega la individualidad —todo lo contrario, es íntimo, personal, directo— no ha sido base para, al menos, una aceptable novela? ¿Por qué, si es palpabilísima su fuerza, su pasión e, incluso, toda una filosofía de la existencia? Que «El embrujo de Sevilla», del uruguayo Carlos Reyles, continúe aceptándose con incuestionable justicia como la mejor novela de encarnación flamenca, es algo que, al menos a uno, le incita a serias reflexiones y, perdóneseme, a otras interrogantes directas y con nombres propios. ¿Cómo narradores excelentes, Caballero Bonald, Félix Grande, Fernando Quiñones, Manuel Barrios, Martínez Menchén, por poner sólo unos concretos y limitados ejemplos, en quienes concurren vivencias y conocimientos fieles e insobornables de «este difícil mundo del flamenco», no han intentado siquiera una novela jonda en la que la torrentera y el arañazo de los negros soníos queden impresos? Sí, de entrada, afirmo que la novela de tema o inspiración flamenca no ha tenido suerte literaria —otra bien distinta será la de público lector—, con razón podrá objetárseme que presento como novedosa a la más vieja, contundente y conocida de las evidencias. Pero, si ello es cierto, es algo que no acabo de comprender, de explicarme. ¿Si lo jondo ha tenido brillantísimos ejemplos de aprehensión intelectual en el teatro, el ensayo o la poesía, por qué su entronque en el campo narrativo —salvo algún relato breve— osciló entre el tópico, el melodrama, el colorido escapista o lo vanal\n\n[ENDING CONTEXT]\n\nficción para el juego literario) tampoco acierta, ya que, a mi juicio, ese judío tentativo, que nada dice, muy bien pudiera haberlo endosado a alguno de los cantes a los que los estudiosos entreveen influencias músicas sinagogales.\n\nPero no vamos a concluir discutiendo la etnia de los duendes y dejemos al humor lo que pueda ser privativa-mente suyo. Sólo dejaría, si una coda final se me aceptase, una simple nota: Sender no efectúa, a mi parecer, dos novelas humorísticas e intrascendentes sobre el cante flamenco, sino algo que me parece más inconsentible: la novelación de lo que se desconoce.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Una visión «humorística» del cante: Ramón J. Sender",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "16-21",
    "page_number": 16,
    "word_count": 7417,
    "article_char_count_full": 43290,
    "article_char_count_review": 3391,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "pasión"
      }
    ]
  },
  {
    "article_id": "1982-03-21-right-manuel-torre",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Soto Loreto nació en el número 22 de la calle Alamos, del flamenquísimo barrio jerezano de San Miguel. Su padre, Juan Soto Montero, natural de Algeciras, fue cantaor de no muy reconocida fama, quien por su elevada estatura ostentó el apelativo del Torre, que heredaría su hijo. Manuel tuvo igualmente el sobrenombre artístico de el «Niño de Jerez»; entre sus allegados y a cauca de sus rarezas, se le conoció en los últimos tiempos por «Majareta».\n\nManuel Torre se hizo cantaor al lado de los grandes siguiriyeros gitanos de Jerez: Manuel Molina, El Marruro, Joaquín de la Serna, el Loco Mateo, Francisco de Perla, Curro Durse, el Viejo la Isla, etc., pudiéndosele considerar discípulo directo de Enrique el Mellizo, a quien conoció en Cádiz durante el servicio militar y al que le uniría una\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nde Perla, Curro Durse, el Viejo la Isla, etc., pudiéndosele considerar discípulo directo de Enrique el Mellizo, a quien conoció en Cádiz durante el servicio militar y al que le uniría una amistad casi fraternal desde sus asiduas visitas a la casa del cantaor gaditano, próxima al cuartel donde el Torre realizaba el servicio militar. Amistad que Antonio Mairena sitúa con anterioridad a esta época, argumentando que el padre de Manuel era amigo del gran siguiriyero y al que llevaría en ocasiones a su hijo para que tuviese la oportunidad de escucharle. Tempranas y redondas fueron las andanzas artísticas del cantaor en el café cantante de la Vera Cruz, que tenía Juan Junquera, donde arrancara la admiración y el entusiasmo de los grandes aficionados dejando muestra notable de los estilos más difíciles y flamencos que, aprendidos de los artistas antes mencionados, supo imprimirles su jondo sello personal. Muy joven fijó su estancia en la sevillanísima Triana; sus actuaciones en los cafés de cante, sobre todo el Novedades, encierran una serie de páginas del buen hacer y una no menor cantidad de anécdotas. En Sevilla conoció a Antonia Torres Vargas, «La Gamba», una excelente bailaora, que sería su mujer y de la que tuvo dos hijos. Falleció en la ciudad del Betis el 23 de julio de 1933. Si Manuel Torre ha pasado a la historia como el más estremecido intérprete de las siguiriyas, hay que reconocer que las soleares, tangos, tientos, bulerías, farrucas, fandangos, cantes de Levante, tonás, etc., alcanzaban en su voz una personalidad flamenca y una genialidad de la que ya es imposible prescindir, en ocasiones, verdadera\n\n[ENDING CONTEXT]\n\ncon mayor cultura en la sangre».\n\nPara cerrar esta sucinta biografía del mejor siguiriyero de este siglo, queden algunos testimonios definitórios de su personalidad y arte:\n\nSelecciona RAFAEL VALERA\n\nDon Antonio Chacón: «Majareta, cuando cantas eres como Castelar cuando hablaba».\n\nPericón de Cádiz: «Se te metía el sonío suyo en el oído y ya no lo perdias en tres semanas».\n\nAgustín Talega: «Parecía que tenía electricidad como cantaba».\n\nFernando de Triana: «Desde hace cuarenta años a la fecha el mejor cantaor fue Chacón, pero el que más gañafones le tiraba al alma a uno era Manuel Torre».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Manuel Torre",
    "periodical": "candil",
    "issue_id": "1982-03",
    "year": 1982,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1267,
    "article_char_count_full": 7534,
    "article_char_count_review": 3254,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  }
]
```
