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
    "article_id": "1996-09-13-left-don-manuel-de-falla-y-el-cante-j",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nConferencia inaugural del XXIV Ramón Mª Serrera Congreso de Arte Flameco\n\n«Falla lanza su sed contínuamente hacia la fuente de su infancia, el lugar donde un pañuelo húmedo le consuela la fiebre. «La Vida Breve», «El Amor Brujo», «El Sombrero de Tres Picos», la «Fantasía Bética», las «Noches en los Jardines de España» son obras que están un-tadas de flamenco, ardidas y refrescadas con la sed del Flamenco. Toda su producción entre 1909 y 1922, es decir, desde el principio de su madurez como artista, hasta el Concurso de Cante Jondo de Granada, es un continuo ir y venir a aquel frescor de las nanas andaluzas en que su madre lo dormía, a aquella agua sedienta que le cantaba «La Morilla». $ ^{1} $\n\nDentro de dos meses el mundo entero conmemorará el cincuenta aniversario de su muerte. Y, con\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"Arte\"]\n\n1909 y 1922, es decir, desde el principio de su madurez como artista, hasta el Concurso de Cante Jondo de Granada, es un continuo ir y venir a aquel frescor de las nanas andaluzas en que su madre lo dormía, a aquella agua sedienta que le cantaba «La Morilla». $ ^{1} $ Dentro de dos meses el mundo entero conmemorará el cincuenta aniversario de su muerte. Y, con todo acierto y profunda sensibilidad, el comité organizador de este XXIV Congreso de Arte Flamenco ha considerado oportuno que la conferencia inaugural, confiada a mi modesta persona, más quizá por mi condición de historiador que de musicólogo puro, trate precisamente de Falla y una de las manifestaciones más profundas de nuestra tierra, el Cante Jondo, un tema que llegó a interesar al compositor hasta el extremo de organizar, recién llegado a Granada, uno de los acontecimientos que más trascendencia tuvo en el nacimiento y evolución —marcando un antes y un después— de la moderna Flamencología: el Concurso de Cante Jondo que se celebró con motivo de las fiestas del Corpus en la Plaza de los Aljibes de la Alhambra granadina durante los días 13 y 14 de junio de 1922. Nuestra aportación se centrará precisamente en una reinterpretación histórica del mítico certamen desde una perspectiva revisionista, más algunas notas sobre el magisterio musical que don Manuel ejerció en Federico García Lorca y un acercamiento a la obra que, sin duda alguna, mejor plasmó en pentagramas y escena ese mundo de magia y sortilegio de esta tierra de hechizos y cantares gitanos: El Amor Brujo. A ello dedicamos, en emocionado homenaje al maestro Falla, las páginas que siguen. Pág. siguiente: Manuel de Falla en 1920, por Pablo Picasso a) El I Concurso de Cante Jondo de 1922 desde una perspectiva revisionista Mucho, muchísimo, se ha escrito, en efecto, sobre el legendario Concurso de Cante Jondo que se celebró en junio de 1922 en la capital del Darro. Los estudios ya clásicos de Eduardo Molina Fajardo, Jorge de Persia, Félix Grande, Bernard Leblon, Federico Sopeña y tantos otros han de\n\n[ENDING CONTEXT]\n\ncobran así vida y se humanizan:\n\n«¡Soy la voz de tu destino! ¡Soy el fuego en que te abrasas! ¡Soy el viento en que suspiras! ¡Soy la mar en que naufragas!» $ ^{58} $\n\nNOTA DEL AUTOR: El autor de estas páginas quiere agradecer la colaboración prestada, para la preparación y redacción del texto, a Piedad Bolaños, José Luis y Mercedes Comellas, Nieves González y al Maestro Juan Udaeta. E igualmente a Esperanza Fernández, la gran Candela de nuestros días y a José Mª Sousa por el interés puesto para que este trabajo llegara a manos del lector y por algunas interesantes sugerencias sobre el mismo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Don Manuel de Falla y el cante jondo. Conferencia inaugural del XXIV Congreso de Arte Flamenco",
    "periodical": "candil",
    "issue_id": "1996-09",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "13-22",
    "page_number": 13,
    "word_count": 12441,
    "article_char_count_full": 75906,
    "article_char_count_review": 3671,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "Arte"
      }
    ]
  },
  {
    "article_id": "1996-09-23-left-flamenco-en-la-universidad-compl",
    "article_text_for_review": "«El flamenco explicado por los flamencos» es un curso al que hemos asistido este verano en la sede que la Universidad Complutense tiene decidida en Ronda. A fe que ha merecido la pena. Angel Alvarez Caballero, entrañable amigo y director del mismo, sabía lo que se hacía al confiar a los protagonistas del flamenco el testigo de la difusión de éste, que no la defensa, como han escrito algunos, ya que, afortunadamente, desde hace bastantes años, nuéstro arte no tiene que pedir perdón a nadie ni explayarse en explicaciones innecesarias que contenten a los que jamás van a tener sensibilidad para afrontar la pedagogía de este caudal andaluz con las suficientes garantías.\n\nYa lo sospechaba Angel y nosotros también, por eso acudimos ilusionados a la serranía malagueña para oírles a ellos hablar de lo que les pertenece, de todo lo que justifica sus vidas, ahora sin cortapisas, sin intermediarios culturales ni mandarines de por medio. Los protagonistas del flamenco son los que mejor conocen su arte, y por eso bailan, tocan y cantan. Los demás, a ser prudentes, si podemos, y a disfrutar.\n\nEntre aquellos riscos milenarios que, en otros tiempos, transitaran bandoleros, o donde Tobalo, el ne- vero del pellizco, desgranaba los ter- cios de su cante apolado, se reunieron, vencido ya el mes de julio, un puñado de artistas capaces de helar el corazón a los que vayan con él en la mano a presentarlo sin prejuicios: Calixto Sánchez, José de la Tomasa, José Luis Postigo, Inmaculada Aguilar, Pedro Bacán y su familia (inigualables Pepa e Inés), Manolo Flores, María Pagés, Chaquetón, Calderito, Paco Peña, Manolo Franco, Curro Lucena, Luis Caballero, Blanca del Rey y Felipe Maya. Pastoreaba el rebaño, con garrocha de seda, el citado Angel Alvarez, que habló en su intervención (único caso de intrusismo no artístico) para prestar su voz al elenco de los que ya no están, aquellos que, desde la fundación primigenia de nuestro arte, hasta nuestros días, aportaron acentos de verdad a una Andalucía que se conformaba, poco a poco, bajo los so- nes de un arte marcado por la heterodoxia y la rebeldía. El director del curso acompasó, con la candela viva del trabajo investigador, los ecos presentes en la efeméride rondeña.\n\nPodría pensarse que este curso discurrió por senderos reivindicativos de tipo profesional, una especie de tribuna, con gran efecto de resonancia, desde la cual los artistas invitados expusieran sus reivindicaciones y agravios, incluso, ¿por qué no pensarlo?, las resoluciones de un corporativismo de carácter económico y laboral, que buena falta les hace a esta gente, sometida a vejaciones tan antiguas como la historia misma del arte que justifica sus vivencias y los dictados de su sangre.\n\nNada más lejos de la realidad. Podía haber sido así, pero no lo fue. Ignoro las cláusulas secretas que el director del curso pactara con ellos a la hora de su contratación, pero estoy bien seguro que nada tuvieron que ver con el talante demostrado por los artistas en esta ocasión, en la que compaginaron los duendes del mediodía (bastante hostiles al flamenco) con el rigor y la precisión.\n\nacadémica imprescindible para un público heterogéneo, en el que abundaban los musicólogos provinientes de distintos Conservatorios nacionales y extranjeros, y un elenco de curiosos que no hubieran consentido nunca la estafa de un cursillo basado en los tópicos de siempre: «El flamenco no se pué aguantar», «Discurre por los maderos de la sangre», «Es para romperse la camisa»...\n\nLos artistas supieron compaginar lo apolíneo con lo dionisíaco, acordarse de la bondad de su arte, y ofrecérsnoslo en pinceladas breves (aunque en sitios y horarios inadecuados) pero, a la vez, hicieron memoria histórica de lo que cada uno de ellos vivió hasta poder alumbrar el quejío de una soleá, abrir los brazos para bailar por alegrías o abrazarse al madero pasionista de ese pozo con seis cuerdas que conduce hasta la cima más completa de la expresividad jonda.\n\nOír hablar a Chaquetón de sus vivencias en la Venta Manzanilla, o a José de la Tomasa transitar el río de sus temores lingüísticos para mostrarnos el venero del que proceden sus coplas, o presenciar a María, Inmaculada o Blanca, con los brazos caídos y las piernas forzosamente dormidas, intentar transmitirnos a los presentes, sólo con su voz, los secretos de una pasión que tantas veces nos ha atenazado, cuando todos ellos han volado en libertad, sin las ataduras académicas de un córsé universitario, es una experiencia única y muy recomendable.\n\nFrente a los sabios que afirman que el arte no se puede explicar porque es inefable, y sus códigos mistéricos, he aquí que, representantes del más popular de todos ellos, de una sensibilidad que pertenece al arroyo, ocupan un escaño en la más alta de las instituciones académicas del país, la más elitista y alejada del pueblo, nuestra Universidad, y durante una semana se convierten en catedráticos de saberes transmitidos en forma de arcano, pero que forma parte hoy de la sensibilidad de nuestro pueblo, la unión más importante entre oriente y occidente que, en materia artística, se haya realizado.",
    "title": "Flamenco en la Universidad Complutense",
    "periodical": "candil",
    "issue_id": "1996-09",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 844,
    "article_char_count_full": 5116,
    "article_char_count_review": 5116,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-09-24-left-un-centenario-el-personaje",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\ne cumple el próximo año el centenario del nacimiento de Sebastià Gasch. Una efemérides que no debiera pasar desapercibida para los aficionados flamencos en general, y muy en particular para los de Cataluña y para los amantes de la danza. Su obra literaria y periodística ha sido profundamente estudiada, comentada y citada en relación a las artes plásticas, la danza, el circo y el cine; sin embargo, su faceta de crítico y comentarista de flamenco lo ha sido muy escasamente a pesar de ser una fuente impagable de información de lo que acaecía flamencamente en la Barcelona de los años 20 y 30, principalmente, por medio de sus artículos publicados en «Mirador», y sus muy interesantes juicios y opiniones. Me honro en ser, probablemente, el único que hasta ahora le ha dedicado una cierta atención\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"cercan\"]\n\ne a pesar de ser una fuente impagable de información de lo que acaecía flamencamente en la Barcelona de los años 20 y 30, principalmente, por medio de sus artículos publicados en «Mirador», y sus muy interesantes juicios y opiniones. Me honro en ser, probablemente, el único que hasta ahora le ha dedicado una cierta atención y de haber sido el impulsor del homenaje que se le tributó en el XII Festival Flamenco de Cornellá'95. Aprovecho, pues, la cercanía del centenario para darlo a conocer un poco más ampliamente, aunque de forma sucinta, y para ofrecer unos cuantos datos nuevos de la programación del flamenco en Cataluña a finales del siglo XIX y comienzos del actual, como mínimo homenaje a él, que nos dejó constancia de los locales flamencos barceloneses. El personaje Sebastià Gasch i Carreras (1897-1980) es uno de los testimonios más sugestivos de la vida farandulesca del siglo XX; dedicado a cubrir ámbitos como los del music-hall, la danza, el teatro, el flamenco o el cine por el solo hecho de su propia afición y por la falta de atención que descubría, su obra literaria es diversa en orientación e interés. Dejó la escuela a los dieciséis años y trabajó hasta 1931 en empresas comerciales. Trabó amistad con Joan Miró en el Círculo Artístico Sant Lluc, del que fue asiduo, llegando a ser su bibliotecario. En 1925 inició su colaboración como crítico de arte en la «Gaseta de les Arts», y después en «D'Ací i d'Allà». Conocedor de la cultura francesa del momento, propagó sobre todo el ideario de Le Corbusier —quien visitaría Barcelona el 24 de marzo de 1932— y de vanguardia desde las páginas de «L'Amic de les Arts» (1925-29) de Sitges. Conjuntamente con Salvador Dalí y Lluis Montanyà firmó en 1928 el «Manifest Groc», y el 1929 publicó, con Montanyà y Díaz Plaja, el único número de unos «Fulls\n\n[ENDING CONTEXT]\n\ny Ramón Casas.\n\nUna noche, Canals le preguntó a la Macarena cómo lograba poner tan entonado sentimiento en el cante, y ella, como quien no da importancia a lo dicho, respondió:\n\n—Es que er cante jondo, como er flamenco, sacude las entrañas del querer.\n\n¡Toda una época ingenua y cordial—son palabras de Cabañas—, edad de oro de la flamenquería barcelonesa!\n\nNOTA DE LA REDACCIÓN: Ofrecemos este artículo como adelanto del ensayo que el autor del mismo ha escrito sobre el personaje y que se publicará el próximo año bajo el título de «Sebastià Gasch y el Flamenco (Barcelona, años 20 y 30)».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Un centenario El personaje",
    "periodical": "candil",
    "issue_id": "1996-09",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 3392,
    "article_char_count_full": 20267,
    "article_char_count_review": 3439,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "cercan"
      }
    ]
  },
  {
    "article_id": "1996-09-26-left-manuel-yerga-lancharro-ni-quito-",
    "article_text_for_review": "Hace ya años publiqué en esta Revista un trabajo sobre el fandango \"En lo alto de una loma/quién tuviera una casita\". Yo lo tengo grabado en la voz de Juan Varea y del Niño León. En Huelva decían que era de su paisano y yo tenía creído que era de Varea. Para deshacer la duda, me puse en contacto con Juan. En su domicilio de Madrid, le expuse el caso y me dijo que el fandango era suyo, que lo había hecho de uno que cantaba el guitarrista El Cojo Pará. De regreso a mi casa le escribió sobre el mismo asunto y me contestó:\n\n\"Madrid, 20-10-1983.\"\n\nQuerido amigo D.® Manuel: Me alegré mucho de recibir su carta porque asía bastante tiempo que no sabía de usted y pensaba haberle escrito, pero tengo ahora muy mala memoria y se me pasa los días sin darme cuenta. De lo que me dise del fandango, le puedo desir que es mío. El Niño León y yo éramos amigos y trabajabamos juntos muchas veces; así es que él lo aprendió de mí; yo le hise un fandango que cantaba El Rubio de Pará, que era guitarrista y también sabía cantar y cantaba un fandango que era muy sencillo, que hoy no me acuerdo ni como era, ni se paresía en nada al que yo ise inspirado en el Rubio Pará.\n\nQue ahora, después de fallecido Varea, sale un señor diciendo que lo creó Rafael Pareja, allá él.\n\nDije y digo que yo no me invento las cosas, sería una inmoralidad monstruosa. Por ello escribo para decir que “ni quito ni pongo rey”.\n\nDesde luego, diciendo como conocí a fondo al honrado Juan Varea, pongo mi mano derecha en el fuego para decir que creo a Varea y a nadie más. alladrid 20-10-1983: Deverido amigo-Dou allamuel me alegré mudo de reseribir su carta por que era Bartan. Tiempo que nosabía de este y pensaba aberle escrito pero el tengo a hijo muy malo menor irengaos en los dios sin darlo cuenta, delo que medice del Fando congo le puedo decir que que es mío, el niño leó. Iyo é amor amigo y tra bajamos juntos mudas veres antes que el lo aprendiendo me yo lo ir de un Fando congo que Cantaba \"El Rubio de Pard que era Guitarrista y, también sabícalcantar y Cantaba un Fando congo que era muy revisito que by nome adveredó ni Como era mi se parese en nada al que me are aunque lo iré inspirado del Rubio de Pará. También le digo que estado Bostante mal de los Brancقúo pero ya estoy Bostante mejor mudos rechendos a su misión y en juicio abraso para este desviamiento.",
    "title": "Ni quito ni pongo rey",
    "periodical": "candil",
    "issue_id": "1996-09",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 448,
    "article_char_count_full": 2343,
    "article_char_count_review": 2343,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-11-3-left-que-tu-voz-no-se-pierda",
    "article_text_for_review": "Sabía decisión la tomada por la Consejería de Cultura de la Junta de Andalucía por la que se declara Bien de Interés Cultural los registros sonoros de Pastora Pavón Cruz «Niña de los Peines», eminente mujer sevillana, creativa y prolífica cantaora. El mundo flamenco puede congratularse con tan acertada determinación. Tras la misma y para redondear la faena, a la andaluza Institución sólo le resta recabar las —creemos que 350 grabaciones que efectuó y editarlas como lo ha ejecutado con las de Antonio Mairena, Tomás Pavón y la primera entrega de Manuel Torre.\n\nPocos son los calificativos que pueden añadirse a los ya vertidos sobre la personalidad artística de la cantaora sevillana, pues desde Fernando el de Triana —el cual la incluyó en el mejor triunvirato cantaor de la época junto a don Antonio Chacón y Manuel Torre— pasando por Anselmo González Climent —quien efectúa posiblemente la más comedida y acertada descripción de su arte en el\n\nlibro «Flamencología»—, hasta Ríos Ruiz y Blas Vega —los que ofrecen todo tipo de detalles sobre la meritoria artista en su Diccionario Flamenco—, todos muestran una uniformidad de criterios a la hora de resaltar su arte.\n\nY es que no podía ser de otra forma, porque pocos artistas en la historia del flamenco han llegado a tener el seguimiento efectuado a Pastora. Pocos los que, careciendo de intencionalidad, han mantenido una pedagogía artística para las sucesivas generaciones cantaoras como la Niña de los Peines. Ninguno o casi ninguno los que no han creado polémica ante el unánime reconocimiento de su arte por parte de profesionales y aficionados. Y pocos o casi ninguno los que se mantienen —con inusitada fuerza— vigentes aún en nuestros días como la cantaora sevillana.\n\nPor otra parte, hay que insistir en que la dimensión cantaora de Pastora ha sido casi —por no referir totalmente— perfecta. Ha mantenido ortodoxia y un adecuado acrisolamiento de las enseñanzas de sus antecesores; ha desarrollado creatividad y bastante vanguardismo para su época al recoger la poesía de García Lorca o composiciones de conocidos autores latinoamericanos y adaptarlas a su inigualable compás: ha dominado el ritmo, la jondura, la melodía y un rico melisma en todos los estilos flamencos; y sobre todo fue una cantaora abierta a la promoción de los nuevos estilos de su tiempo como las bamberas, el garrotín, la farruca e incluso las sevillanas.\n\nLa Junta de Andalucía, como arriba apuntamos, tiene ahora la palabra. Una palabra que debe ser usada para publicar toda su obra—tan sabiamente como lo ha sido con la declaración de su voz como Bien de Interés Cultural.",
    "title": "Que tu voz no se pierda",
    "periodical": "candil",
    "issue_id": "1996-11",
    "year": 1996,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 430,
    "article_char_count_full": 2615,
    "article_char_count_review": 2615,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
