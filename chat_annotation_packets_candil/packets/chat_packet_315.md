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
    "article_id": "1996-01-17-left-multitudinario-homenaje-de-despe",
    "article_text_for_review": "Rafael Valera Espinosa\n\nGranada, en doble jornada, evidencia el cariño que a un hijo de la tierra le profesan sus paisanos\n\nHay que volver a reiterar los calificativos cariñosos (por merecidos) y profesionales del artista, ante el hecho de un nuevo y último? homenaje a Juan Carmona Carmona, \"El Habichuela\". Dice él que de despedida y nosostros —como Chano Lobato afirmó— pensamos que de eso nada. ¿Que se retira de los escenarios? ¡Pues qué le vamos hacer! Sin embargo, por el cariño que le profesamos los jiennenses y muy concretamente, los miembros de la Peña Flamenca de Jaén, a buen seguro que en más de una ocasión vendrá a la sede de la calle Maestra 11, para volver a deleitarnos con su toque.\n\nY es que, a pesar de que sus comparecencias profesionales se hayan acabado, el que fue alumno preferido del \"Ovejilla\", el que comenzó sus andanzas profesionales emparejado con Mario Maya, el rebuscador de triquiñuelas para escuchar al \"raro\" de Manolo de Huelva, el admirador del Niño Ricardo y Paco de Lucía, el que acompañó con vehemencia y calidad a Manolo Caracol, el que se inició como bailaor para seguida-mente convertirse en ejemplo a seguir de guitarrista ideal para acompañar al cantaor, el que perdía los pies para volver a su Granada para escuchar a \"Juanillo el Gitano\", el nato admirador del cante y toque por soleá, este menudo y elegante gitano, sólo se retirará del flamenco cuando se muera.\n\nEn cuanto al evento-homenaje en sí, han sido dos días multitudinarios de público y de artistas. El mismo se ha celebrado en dos jornadas, en las que la emoción de Juan ha traspasado el límite del agradecimiento para con sus compañeros y para con sus paisanos. Y es que presenciar el llenazo que se produjo el día 3 de febrero (alrededor de quince mil personas), sin menospreciar el del día anterior (más de cinco mil), tiene que ensanchar el alma de un profesional como \"El Habichuela\".\n\nComo es lógico y según queda expuesto con el dato de la asistencia, el personal en general acudió a lo que consideraba más atractivo,\n\nHubo de todo, como en botica: flamenco ortodoxo, neoflamenco, flamenco-jazz, flamenco-salsa y flamenco-rock, suponiendo que a todo este conglomerado de músicas pueda anteponérseles el calificativo flamenco, circunstancia esta última que me cuesta admitir y que en alguna ocasión tendré que explicar el por qué de mi criterio.\n\nuniversal, popular, comprensible y marchoso, o sea, lo programado para el día 3, y a ciencia cierta que se lo pasaron tremendamente bien. Casi todos ellos son neófitos de este arte aunque a la vez potenciales aficionados, amantes de otras músicas, devotos de Ketama y admiradores de la mezcla artística que representa Enrique Morente con el conjunto Lagartija Nick. Los aficionados al arte flamenco se dieron cita el mencionado día 2, fecha en la que participaron, si exceptuamos al grupo madrileño \"Los Losada's\", cantaores en la más pura línea ortodoxa flamenca con los diferentes matices personales de interpretación. Y ciertamente que los que allí estuvimos tampoco lo pasamos mal, siempre salvando las lógicas diferencias cantaoras de brillantez de cada uno de los artistas participantes.\n\nEn línea acertada estuvieron principalmente nuestras paisanas Rosario López y Carmen Linares, la primera con soleares (por Tomás Pavón, Triana y Juaniquin) y granaína y media granaína; y la segunda contaranto-taranta, bulerías y tangos. Chano Lobato —con su inigualable gracia para los estilos festeros— cantó por tangos y bulerías, acordándose de Caracol, El Chaqueta e Ignacio Ezpeleta. La guitarra de Moraíto Chico desarrolló brillantez igualmente.\n\nEn tesitura cumplidora estuvieron las bailaoras de Mariquilla (aunque algo monótonas al final), Calixto Sánchez, Manuel Mairena y Luis Heredia \"El Polaco\", \"El Pele\", así como el bailaor \"Manolete\". En este apartado hay que incluir a los guitarristas Antonio Carrión, Paco y Miguel Angel Cortés, Manolo Franco, Melchor de Santiago y Los Losada's.\n\nYa en una tónica más desenfada da actuaron \"El Tiriri de Málaga\" con tangos y bulerías, y Antonio Cortés \"Chiquetete\", que olvidándose de las coplas, abordó las cantiñas, los tientos-tangos y las bulerías, con sus clásicos matices comerciales.\n\nEn el intermedio y con bastante nerviosismo por la presión que para él suponía este grandioso homenaje, Juan Carmona desarrolló un toque por granaínas, en el cual evidenció su clase y calidad.\n\nArriba: Juan Carmona, \"El Habichuela\"",
    "title": "Multitudinario homenaje de despedida a Juan Carmona \"El Habichuela\"",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 716,
    "article_char_count_full": 4437,
    "article_char_count_review": 4437,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-01-18-left-la-noche-de-enrique",
    "article_text_for_review": "La XII Distinción \"Compás del Cante\" fue entregada el 22 de febrero del presente año en un acto que, no por conocido y vivido, deja de ser entrañable. Y siempre porque el reconocimiento a la labor desarrollada por un artista flamenco, ha de tener una aceptación por parte de los colectivos que conforman este mundo musical. Diferente es el grado de conformidad con que la misma se asume, porque cada uno es muy dueño de mostrar sus preferencias, mas el denso trabajo está en los anales de la historia flamenca y sería de necios no considerarlo.\n\nEnrique Morente Cotelo, trabajador, investigador e innovador flamenco donde los haya, fue elegido por el jurado, en esta ocasión formado por Imperio Argentina, Manuel Bohórquez Casado, Mario Bois, José Manuel Caballero Ronald, Luis Caballero Polo, Antonio Fernández Díaz \"Fosforito\" y Emilio Jiménez Díaz, como el ilustre intérprete que tal honor merecía. Y La Cruz del Campo, como entidad patrocinadora del premio, organizó el clásico prototocolo que supuso el realce a un acto, siempre singular a pesar de las similitudes.\n\nEn el Salón Real de Alfonso XIII hubo determinadas ausencias que, particularmente, atribuyo a compromisos profesionales e impedimentos viajeros. Pero al margen de este matiz, la casi totalidad de la plana habitual en las anteriores entregas estuvo presente para testimoniar a la empresa editora del galardón su reconocimiento a la labor de mecenazgo que desarrolla, así como al jurado el trabajo efectuado y al artista la acreditación de su premio.\n\nComo es habitual en el acto, tras los postres y la lectura del acta del jurado, los directivos de La Cruz del Campo tomaron la palabra para efectuar los agradecimientos consabidos y testimoniar su enorme interés por seguir contribuyendo a la promoción y realce del flamenco, tras lo cual, el director del Grupo Guinness, Allen Peeters, hizo entrega al homenajeado de la artística estatuilla que simboliza el preciado galardón.\n\nPor su parte, Enrique Morente comenzó su discurso de agradecimiento reconociendo las enseñanzas que le habían prodigado los viejos maestros, explicando seguidamente el porqué de su labor de investigación e innovación y matizando que es un enamorado de lo ortodoxo. Expresa de igual forma su criterio sobre el flamenco actual y abunda en lo que de positivo para el flamenco puede tener esta forma de realizar nuestra música por los jóvenes e innovadores artistas. Finalizó con la interpretación, por martinetes, de la letra:\n\nA mí me llaman el loco porque siempre voy callao. Llamarme poquito a poco que soy un loco de cuidao.",
    "title": "La noche de Enrique Morente",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 417,
    "article_char_count_full": 2575,
    "article_char_count_review": 2575,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1996-01-19-right-la-rumba-catalana-de-la-habanera",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA modo de introducción\n\nna manera irrefutable de descartar en cualquier discusión que los gitanos crearon el flamenco \"per se\" es recurrir al silogismo: Si está demostrado que proceden de una parte del norte de la India (cuenca del Indo) y se asentaron por todos los países de Europa, además de España. Si en ninguna de estas partes donde se han asentado, ni de donde proceden, se ha desarrollado el flamenco ni nada similar, está claro que los gitanos no trajeron a España el flamenco. Por la misma deducción, además de por hechos y razones ya suficientemente documentados, afortunadamente sabemos y podemos decir que el flamenco cristalizó en ese territorio, en tiempos llamado Al-Andalus, y no en ninguna otra parte del territorio español donde también había y hay importantes asentamientos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"reconocer\"]\n\nllado el flamenco ni nada similar, está claro que los gitanos no trajeron a España el flamenco. Por la misma deducción, además de por hechos y razones ya suficientemente documentados, afortunadamente sabemos y podemos decir que el flamenco cristalizó en ese territorio, en tiempos llamado Al-Andalus, y no en ninguna otra parte del territorio español donde también había y hay importantes asentamientos gitanos. Sin que esto nos lleve a negar o a no reconocer la gran importancia que en la gestación de una parte del Arte Flamenco tuvieron los gitanos que se asentaron en el Sur, porque sería indudablemente negar lo evidente. Por otra parte, contemplando la geografía del cante, o mejor para acostumbrarnos, del Arte Flamenco, trazado por los diversos estudiosos, cada uno en su momento, para no pararnos demasiado en discutir las razones de unos y otros que se diferencian en muy poco y la mayoría de las veces en matices puramente per-sonalistas y subjetivos, recogemos la que más o menos reconocen todos y que es la que delimita toda Andalucía, la provincia de Badajoz y Murcia, con irradiaciones vecinas y un foco en Madrid, capital de España, y que también lo fue del flamenco. Durante un largo período vivieron en ella los principales artistas del flamenco, por ser el lugar donde mejor se ganaban la vida y todo hay que decirlo, por ser Madrid una ciudad acogedora, cosmopolita y generosa, que hace que ningún forastero en ella se sienta extraño. Indudablemente que ahora\n\n[ENDING CONTEXT]\n\nmás exportable y comercial.\n\nEn definitiva después de todo este recorrido por orígenes, opiniones, reflexiones y consideraciones, el análisis y discusión de los datos y hechos que hemos expuesto, nos llevan a sacar las siguientes conclusiones:\n\n* El origen de la rumba catalana es similar al de los tangos y rumbas flamencas que cristalizan en Andalucía como cantes importados de América y, por tanto, considerados hispano-americanos o de Ida y Vuelta.\n\n* La rumba catalana, sin discutir su graduación flamenca, es un cante autóctono de Cataluña y más concretamente de Barcelona y su entorno.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La Rumba Catalana. De la habanera y el tango a la rumba catalana",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "article",
    "pages": "19-24",
    "page_number": 19,
    "word_count": 5537,
    "article_char_count_full": 32768,
    "article_char_count_review": 3099,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "reconocer"
      }
    ]
  },
  {
    "article_id": "1996-01-24-right-catalu-a-pionera-de-la-did-ctica",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAurelio Gurrea Chalé Comunicación al XXIII Congreso de Arte Flamenco\n\nHe leído los trabajos sobre la guitarra dentro de la Historia del Cante Flamenco, por cierto muy buenos, y veo nombres como Alfonso X el Sabio, el Arcipreste de Hita, Mateo Alemán, Vicente Espinel, Lope de Vega, Miguel de Fuenllana, Gaspar Sanz, y muchos más que citan a la guitarra o escriben tratados y composiciones para ella. Y observo que falta uno, uno que para mí es el primer enseñante de la guitarra en la forma que siempre se ha acompañado al flamenco y al cante popular: el sistema de rasgueado. Sobre todo me llamó la atención que el que escribe este apartado se sorprenda de lo temprano que sale a la luz un método de rasgueo, cuyo autor es Matías Jorge Rubio, el año de su publicación: 1860. Pues bien, yo les voy a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"ENSEÑ\"]\n\nn: 1860. Pues bien, yo les voy a hablar de uno aparecido casi tres siglos antes y estas mi comunicación. En el año 1984, cayó en mis manos un pequeño libro editado en Mónaco, que correspondía a una edición facsímil de un curso de guitarra rasgueada o por acordes, que es posiblemente el más antiguo aparecido en imprenta. Se trataba del libro \"GUITARRA ESPAÑOLA Y BANDOLA EN DOS MANERAS DE GUITARRA, CASTELLANA Y CATALANA DE CINCO ÓRDENES, LA CUAL ENSEÑA A TEMPLAR, Y TÁÑER RASGADO, TODOS LOS PUNTOS NATURALES, Y B, MOLLADOS, CON ESTILO MARAVILLOSO\". El autor de este libro es Joan Carles Amat, contemporáneo de músicos tan importantes como Antonio de Cabezón, Joan Pau Pujol, el compositor catalán más importante de esa época, cuyos trabajos han sido rescatados por Monseñor Higinio Anglés; así como de otros músicos y compositores que dedicaron gran parte de su obra a la vihuela y posteriormente a la guitarra: Miguel de Fuenllana, Alonso Mudarra, Luis de Narváez, Diego Pisador, Luis Milán, etc. También nuestro protagonista fue contemporáneo de Vicente Espinel, el rondeño causante del progresivo enriquecimiento de la guitarra en detrimento de la vihuela y a quien se atribuye la adición del quinto orden o cuerda a la guitarra, dándole así a ésta mayor categoría interpretativa. Joan Carles Amat nació en Monistrol (Barcelona) sobre el 1572. Doctor en Medicina, dedicó gran parte de su vida a ser el médico titular de la ciudad que le vio nacer, así como del monasterio cercano de Montserrat. Falleció el 10 de Febrero de 1642. Además de este método de guitarra, Amat publicó también otras tres obras: una en latín sobre medicina titulada \"Fructus Medicine\", otra cuyo título es \"Quatrecents Aforismes\", sobre enseñanza y preceptos religiosos en catalán y un tratado sobre la peste; y parece que fue también el autor de un entretenido bosquejo dramático en castellano denominado \"Entremés de la Guitarra\". De este método de\n\n[ENDING CONTEXT]\n\n\"...el fandanguillo abandolao de Miguel Brito\".\n\nCon este trabajo, he querido hacer un justo y merecido homenaje en este Congreso de Arte Flamenco al hombre que escribió el primer método de guitarra por el sistema de rasgueado, como hoy se acompaña al cante y el baile flamencos: Joan Carles Amat. Y por eso digo que Cataluña es pionera en la didáctica de la guitarra.\n\n$ \\underline{\\text{BIBLIOGRAFIA:}} $\n\nJoan Carles Amat: Guitarra Española. Ed. Chanterelle, S. A. Mónaco.\n\nEUSEBIO RIOJA: Historia del Flamenco. Ed. Tartessos.\n\nANTHONY BAINES: Historia de los Instrumentos Musicales. Ed. Taurus.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cataluña, pionera de la didáctica de la guitarra",
    "periodical": "candil",
    "issue_id": "1996-01",
    "year": 1996,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "24-26",
    "page_number": 24,
    "word_count": 2261,
    "article_char_count_full": 13070,
    "article_char_count_review": 3556,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "ENSEÑ"
      }
    ]
  },
  {
    "article_id": "1996-03-3-left-juan-talega",
    "article_text_for_review": "Hace veinticinco años que se nos fue, quizás algo sigilosamente, como había aparecido y gracias a la labor investigadora y promocionadora de un buscador de fuentes flamencas donde inspirarse: Antonio Mairena. Juan Fernández, \"Juan Talega\", ganador de los apartados —como no podía ser de otra forma— primero y segundo del Concurso Nacional de Arte Flamenco de Córdoba del año 59, posiblemente haya sido uno de los más fieles transmisores del arte flamenco de este siglo. Su declaración: \"Según mi padre, el abuelo de mi padre decía que cantaba su madre mejor que su padre, su abuelo, mejor que su abuela...\", es una verdadera manifestación de fe de la transmisión oral de los clanes gitanos sobre nuestro arte.\n\nCuando en los tiempos actuales comprobamos cómo el cante se devalúa ante los intentos innovadores de algunos, lo que apreciamos como meros hechos de imitación sin creatividad ni aporte personal, nos viene a la memoria el eco rancio y ronco del cantaor de Dos Hermanas para desarrollar los estilos serios y profundos de nuestro arte. Evocamos con satisfacción cómo las influencias de su tío Joaquín el de la Paula o Agustín Fernández, su padre, quedaron impregnadas en su cante, y cómo el matizado acrisolamiento que en su ser se efectúa, impregnaligualmente a numerosos contemporáneos flamencos suyos como el propio Antonio Mairena, Lebrijano, Menese o El Pele.\n\nComo acontece con la mayoría de las figuras legendarias del flamenco, no sabemos cómo cantaba Tomás el Nitri ni el citado Joaquín el de la Paula, pero gracias a las referencias estilísticas de Juan Talega, nuestra configuración de su arte puede estar más cercana a la realidad que fue. Es esta, en síntesis, aparte de lo referido, la maravillosa labor que un cantaor casi anónimo ha legado al cante flamenco.\n\nCon la desaparición de Juan Talega se nos fue una referencia cantaora importante para ahondar en la investigación de este arte. Con la muerte del gitano de Dos Hermanas se ensombreció el ambiente puro y ortodoxo que reinaba en su tiempo. Con el óbito de Juan Fernández Vargas, Alcalá de Guadaira perdió uno de los más firmes impulsores de su localismo solearero.\n\nComo meses antes de su muerte, en el festival-homenaje que Madrid le ofreciera en el Teatro de la Zarzuela, comandado por José Manuel Caballero Bonald, Candil quiere evocar y homenajear la figura de este cantaor cuando en el año en curso se cumple el veinticinco aniversario de su fallecimiento.",
    "title": "Juan Talega",
    "periodical": "candil",
    "issue_id": "1996-03",
    "year": 1996,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 404,
    "article_char_count_full": 2443,
    "article_char_count_review": 2443,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
