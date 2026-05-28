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
    "article_id": "1987-03-3-right-fosforito",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAgustín Gómez\n\nCuando llegaron a Córdoba sus «bodas de plata» de actividad organizativa de concursos y festivales flamencos, quiso significar todo su esfuerzo y mérito en la persona de Fosforito, nombrándole «hijo adoptivo» y publicando un libro, con su nombre por título, que testimoniara el binomio cordobés de arte-afición flamenco. Si alguien pensó en oportunismo político de un Ayuntamiento democrático que olvidaba la importantísima gestión política que supuso en su día la creación en Córdoba de aquel Concurso primero de «Cante Jondo», 1956, con los esquemas lorquianos de 1922 en Granada, que olvidaba a tantos y tantos gestores que lo hicieron posible, pronto tuvo ocasión de comprender que, si bien el arte se puede mover por una política cultural más o menos oportuna u oportunista, sólo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nen oportunismo político de un Ayuntamiento democrático que olvidaba la importantísima gestión política que supuso en su día la creación en Córdoba de aquel Concurso primero de «Cante Jondo», 1956, con los esquemas lorquianos de 1922 en Granada, que olvidaba a tantos y tantos gestores que lo hicieron posible, pronto tuvo ocasión de comprender que, si bien el arte se puede mover por una política cultural más o menos oportuna u oportunista, sólo el arte verdadero deja huella en el pueblo, remueve su sentimiento y es agradecido por éste. Es por eso que cuando el Diario «Córdoba» hace una encuesta sobre «los cordobeses del año», los cordobeses votamos a Fosforito en la rama de «artistas». Hay otros muchos artistas en Córdoba de géneros muy diversos, no necesariamente escénicos; es igual, para el pueblo de Córdoba no hay otro artista máximo que Fosforito, «el elegido». Puente Genil, su pueblo natal, ya había vibrado en incordios y satisfacciones con todo el aire que respiraba quien, al fin, sería su «hijo predilecto». Muchos pontanos del pasado y del presente merecen ese título, pero ninguno tan mirado y tan presente en el propio pueblo como Fosforito. Nombres de calles, pasajes y paseos en sus pueblos naturales y de adopción son ya el nombre «Fosforito»; nombres de peñas flamencas aquí y allá... Me detengo en una distinción muy significativa por lo que tiene de irrepetible: por el mes de junio pasado, la institución jerezana de ámbito nacional denominada «Cátedra de Flamencología», nombraba presidente y director honorarios de la misma al escritor y CANDIL FOSFORITO flamencólogo hispano-argentino Anselmo González Climent, y al cantaor Antonio Fernández Díaz «Fosforito», respectivamente, en atención a los mérit\n\n[ENDING CONTEXT]\n\nel ritmo con acento sincopado es a partir de Fosforito en el Flamenco. Ha dado consecuencia lógica y remate a muchos cantes que, mentados en nuestra época de manera incompleta, les ha dado estructura de pieza musical completa. Ha dado perfil definido y contundente y evolución. Todo esto tiene tanto más mérito por cuanto coincide su tiempo y espacio profesional y artístico con el gran maestro, restaurador y mentor Antonio Mairena. El tiempo futuro tendrá en Fosforito una pieza clave para entender nuestra época cantaora y será entonces un hito, un motor de la historia íntima de nuestro pueblo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Fosforito",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "4-10",
    "page_number": 4,
    "word_count": 7130,
    "article_char_count_full": 43045,
    "article_char_count_review": 3359,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  },
  {
    "article_id": "1987-03-11-left-alto-sentido-y-grado-poetico-en-",
    "article_text_for_review": "J. Márquez Cabello\n\nCuando este fenómeno del Cante, en sus ya algo más que halagadores comienzos triunfales de «la Llana», lo solicita-\n\nron para grabar algunos palos en la prestigiosa discográfica Casa BEL-TER, tuvo el grato acuerdo para mí —por paisanja, amistad e identidad en apego a los versos— de pedirme unas cuantas «letras» de nuestro meollo, para llevarlas a discos suyos y a su repertorio.\n\nPara uno, como modesto emborrona-cuartillas, constituyó un inmenso honor el poder verse y escucharse por boca de tan prometedo-\n\nra figura, cuya trayectoria veía-se ir escalando peldaños triunfalmente en el mundo del flamenco Arte. Poco tardé en entregarle seis o siete composiciones. Y la primera que echó su vista encima fue a la siguiente seguiriya:\n\nLa noche y el día me pasé llorando, «pa» que de nuevo tu querer y el mío remienden su daño. (1) Al instante no más, señalándo- me la palabra subrayada, me di- ce: «Esto: ¿no te parece que es-\n\ntaría más bien conseguido: REMEDIEN..?». Yo, satisfecho, le contesté: «Claro que me parece, porque lo está de aquí a Lima». Y así quedó y lo grabó con su certera corrección. Alguna de las otras restantes coplas no se escapó sin su genial toque supervisor, y mi aceptación, agrado y reconocimiento a su indiscutible valía letrística.\n\nQuiero dejar sentado con esto que el pontanense FOSFORITO, a más de cantaor de época, patentizábase sencillamente como inspira-\n\ndo poeta popular, y tal nos lo tie- ne demostrado —con creces— en muchas grabaciones de su caletre exclusivo.",
    "title": "Alto sentido y grado poético en Fosforito",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 253,
    "article_char_count_full": 1521,
    "article_char_count_review": 1521,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-11-right-antonio-fern-ndez-d-az-ayer-segu",
    "article_text_for_review": "M. Yerga Lancharro\n\ninvitado por la dirección de esta prestigiosa revista para que escriba algo sobre tu personalidad de cantaor eminente, no he podido sustraerme a tal solicitud, a pesar de que, como todos conocéis, mi fuerte ha sido siempre rastrear en la biografía de los que ya no están con nosotros y de que mi pluma se vuelve torpe cuando trato de hablar de aquellos que, gracias a Dios, aún conviven con nosotros.\n\n¿Qué podré decir de ti que otros no hayan dicho? Difícil papeleta la mía. Sin embargo, acepta estas cuartillas que, de corazón, te dedico y que sin ningún género de dudas, mereces sobradamente. Te conozco desde hace muchos años. Creo que desde antes de ser catapultado, por tu Córdoba torera y cantaora, hacia el oscuro y complejo mundo del arte flamenco. Desde aquellos días inolvidables en que supiste y pudiste hacerte acreedor a los más importantes premios establecidos. ¿Cómo olvidar tu caso, único en la historia de la flamencología?\n\nLo que quizás no sepan muchos es que, cuando te dieron pública «alternativa» en Córdoba, como maestro del cante, ya llevabas registrados, en tu particular cuentakilómetros, muchos años de lucha y de deambular por ciudades, pueblos y aldeas de nuestra geografía cantaora, libando aquí y allá el néctar de la sabiduría flamenca en las más añejas flores: «El Seco de Puente Genil» y otros.\n\nA partir de tu merecido encumbramiento, no he dejado de seguir tu amplia y limpia trayectoria, celebrando «por lo bajini» tus éxitos, que no han sido pocos, a pesar de que no has interpretado, como otros, letras impregnadas de sabor político.\n\nSin embargo el tiempo, para los mortales, corre a demasiada velocidad. Aún Para mí, tú has sido, en vida del insuperable don Antonio Mairena, que gloria halle, EL SEGUNDO DE A BORDO, pero hoy puedo proclamar que, al producirse la desaparición del maestro, has dejado de serlo, y su lugar en el escalafón, el número uno, es tuyo y únicamente tuyo. Yo lo creo así y, como yo, también lo creen muchos aficionados.\n\nresuena en mis oídos la exhaustiva semblanza tuya que tuve el honor de tributarte en «Los Montitos» de Badajoz, aprovechando el homenaje que se te rindió como reconocimiento público a tu valiosa aportación al arte flamenco. Efectivamente, parece que fue ayer, pero ya han pasado 14 años.\n\nY queremos que lo sigas siendo.\n\nSoy consciente de que al cantar, en este momento, tus excelencias artísticas, voy a herir la susceptibilidad de no pocos buenos cantaores actuales. Pero no importa porque cuanto digo es lo que siento.\n\nAntonio, sólo unos cuantos aficionados ya maduros, sabemos que llevas muchos años (quizá demasiados) de ininterrumpida actividad flamenca, por eso, y apoyado en el aprecio que te profeso, me voy a permitir recomendarte una larga temporada de descanso absoluto y bajo los cuidados de un buen Otorrino. Así podrás salir de nuevo a la palestra «dando fuerte» y decir aquello que, cantando, nos dijera el gran Manuel Vallejo tras un período de inactividad para cuidar de su preciosa garganta:\n\nNi orgullo ni vanía yo no he tenido en mi vía, ni orgullo ni vanía; ahora decían que yo ya no podía cantar y sigo siendo el mejor (sic) Hay un dístico del escritor latino Ovidio, que yo me he permitido reformar un poco y, que desde mi punto de vista, expresa la situación en que puedes que darte si tus facultades te abandonaran por exceso de trabajo:\n\n«Mientras seas gran cantaor, contarás con muchos “amigos”, pero si dejas de serlo, la fortuna te volverá la espalda y te quedarás solo. Ya no tendrás amigos, únicamente tu esposa e hijos, y tus grandes éxitos serán cubiertos por el tupido manto del olvido».\n\nCreo que no debes olvidar esta lección del genial Ovidio, como tampoco el significado de este fandango que nos lega-ra Juan «Canalejas»:\n\nY no vienen mis amigos saben que me estoy muriendo, pero me dice mi madre hijo mío, estoy contigo ¿qué falta te hace a ti nadie?\n\nY ya termino con una última reco- mendación.\n\nDon Antonio Mairena se marchó para siempre, seguro de que tú podrías ocupar el lugar que él había dejado en el escalafón flamenco. Yo sé que tú, persona inteligente, conoces cuán enorme es tu responsabilidad, para no defraudarle, ante esa afición que te sigue y te aplaude, emociona, en tus felicísimas actuaciones.\n\nCuídate mucho. Otórgate un merecido descanso para que, como antes digo, puedas proseguir tu caminar, envuelto en tus propios éxitos por toda la geografía cantaora. No olvides el enorme socavón que la desaparición de don Antonio ha ocasionado en nuestro suelo flamenco, socavón que sólo tú por tu reconocida categoría, puedes volver a llenar.",
    "title": "Antonio Fernández Díaz ayer segundo de a bordo, hoy número uno",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 780,
    "article_char_count_full": 4605,
    "article_char_count_review": 4605,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-12-left-cuantos-habremos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLuis de Córdoba\n\nProbablemente estas líneas coincidan en su aparición con un disco mío en cuya contraportada explico muy sucintamente las razones de haberlo dedicado «A FOSFORITO, EN HOMENAJE»: Así título lo que con toda naturalidad y sencillez he querido que sea el sincero y público reconocimiento de un hombre a lo que otro significa para él en una parte importante de su vida.\n\nEn ocasiones he podido observar entre los flamencos cierta inclinación a no descubrir nuestras fuentes de información y formación flamenca, en unos casos, y en otros, a atribuir, si se nos pregunta al respecto, nuestros conocimientos e incluso nuestra afición a personas y circunstancias que en algún sentido nos sirvan para aumentar o fortalecer nuestro prestigio. (Tan absurda la primera como incómoda de sostener,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"segunda\"]\n\nortante de su vida. En ocasiones he podido observar entre los flamencos cierta inclinación a no descubrir nuestras fuentes de información y formación flamenca, en unos casos, y en otros, a atribuir, si se nos pregunta al respecto, nuestros conocimientos e incluso nuestra afición a personas y circunstancias que en algún sentido nos sirvan para aumentar o fortalecer nuestro prestigio. (Tan absurda la primera como incómoda de sostener, a veces, la segunda, son estas actitudes, pero ahí están). Parece ser que existe una especie de estado de opinión establecido en que hay situaciones y circunstancias que al parecer dan carácter y solvencia a nuestra personalidad flamenca y son, por ello, muy «rentables» a la hora de exponer los en nuestro currículum o simplemente en nuestras conversaciones; v.gr.: Tener unos antecedentes familiares importantes, lo que se llama una tradición familiar cantaora o artística, con nombres notables (a los que a veces se trata de encontrar como sea, el caso es tenerlos...). Que nuestros conocimientos, nuestra formación, hayan sido a través de cantaores de renombre, preferentemente que no sean contemporáneos nuestros, sino figuras históricas ya... En este aspecto se estima, por ejemplo, que a la hora de se ñalar a nuestros maestros, o simplemente nuestros gustos, resulta tan cómodo como, sobre todo, indiscutible citar a Chacón, pongo por caso, o a cualquiera otra de esas figuras que son ya pilares inamovibles del edificio flamenco; mucho más cómodo e indiscutible que nombrar a fulanito que es de nuestra época y está a nuestro lado, y cuya categoría artística está superreconocida, p\n\n[ENDING CONTEXT]\n\nFosforito, un hombre asido a un oficio en el que es maestro y al que, casi, casi, en su tremenda sinceridad, se considera condenado. Nos ha dido más cosas. Entre ellas, algunas que han chafado nuestro estudio astrológico. Al final, ha puesto un punto de emoción —desolación que sólo el tiempo ha podido superar, son sus palabras— cuando nos ha hablado de su madre, sin duda la gran frustración de su carrera de éxitos contemplada desde su propia intimidad. Así es tantas veces la vida de los artistas a quienes la violenta exigencia del éxito les regatea ese derecho tan humano: el de la intimidad.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "¿Cuántos habremos?",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "12-49",
    "page_number": 12,
    "word_count": 2577,
    "article_char_count_full": 15410,
    "article_char_count_review": 3256,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segunda"
      }
    ]
  },
  {
    "article_id": "1987-03-13-left-fosforito-el-hombre",
    "article_text_for_review": "Paco Vallecillo\n\na revista CANDIL nos pide unas cuartillas para el extraordinario que dedica a Antonio Fernández Díaz «Fosforito», dando de este modo cima a un proyecto que imaginaron sus directores hace ya más de un año y que hemos venido compartiendo en la imaginación y el propósito desde entonces.\n\nRecuerdo ahora que cuando una decisión análoga fue adoptada —y felizmente realizada luego— en relación con Antonio Mairena, si bien es verdad que tuvimos el privilegio de aportar abundante material que contribuyó al meritorio y logrado esfuerzo de los hombres de CANDIL, en cuanto resultaba concerniente con nuestra personal colaboración apenas si hicimos acto de presencia en aquel excelente extraordinario, excelente al extremo de poderse calificar como la más trascendente obra periodística dedicada al Maestro de los Alcores. Si la memoria nos es fiel, nuestro nombre solamente apareció en aquel número, unida a la del aficionado y estudioso Diego Alba Magriñan, al pie de un somero inventario de la discografía de Mairena. El por qué de nuestra inhibición estuvo entonces justificado y lo va a estar ahora en el caso de Fosforito: el concepto de la amistad, análogamente válido en ambos casos, nos aconseja quedar en un segundo plano, íntimo y modesto, familiar casi, para no hacer sombra ocupando el espacio que va a reclamar una pléyade de escritores, de poetas, de aficionados ansiosos de ejercitar su derecho a tejer entre todos la corona de laurel que simboliza una ejemplar obra literaria y tipográfica ofrecida al genio cantaor.\n\nAsí, pues, somos escuetos en esta ocasión y pensamos que añadir simplemente, casi pudorosa y tímidamente nuestra humilde firma a la crecida relación de quiénes van a cantar mil veces mejor que nosotros las grandezas del homenajeado es ya de por sí el máximo privilegio al que debemos aspirar. A fuerza de hacerlo mal muchas veces, ha adquirido una cierta práctica para hablar en público, al socaire pírrico de cuatro muletillas y tres desplantes retóricos. Así, más o menos, tuvo ocasión de decir algo a Fosforito una inolvidable noche cuando este otro Maestro recibió la Medalla de Oro y el título de Hijo Predilecto de su ciudad natal, La Puete Genil. La expresión no fue brillante, pero se primó de una carga de emotividad que difícilmente acude con idéntica intensidad a los puntos de la pluma. Por todo eso y por lo que este Antonio —también Antonio él— que de aquél se siente admirador profundo representa para un viejo aficionado, téngase por cierto y verdadero que muchísimo es lo que se calla aquí y bastante poco lo que se dice. Si acaso añadir, para su gloria y como testimonio de su grandeza de alma, de su sensibilidad extraordinaria, el recuerdo imborrable de una frase que en el curso de una conversación pronunció con absoluta naturalidad, como se dicen las cosas normales y cotidianas, sin el menor énfasis, como sencilla justificación a una mediata actitud suya: Porque ten en cuenta que también yo soy mairenista.",
    "title": "Fosforito, el hombre",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 492,
    "article_char_count_full": 2977,
    "article_char_count_review": 2977,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
