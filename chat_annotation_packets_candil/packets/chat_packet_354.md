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
    "article_id": "1998-09-25-left-cr-nica-del-xv-concurso-de-arte-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Valera Espinosa\n\nNuevamente el Concurso Nacional de Arte Flamenco de Córdoba, celebrado entre los días 27 y 16 de los meses de abril y mayo, ha tenido el fundamento por el que se instituyó. Proponer como supremo objetivo el reconocimiento y conservación, purificación y exaltación del viejo cante jondo.\n\nEs posible que se esté o no de acuerdo con el fallo del jurado, con la programación complementaria o con el trabajo de la organización, mas lo que supone de difusión de nuestro arte, el trabajo de búsqueda de nuevos valores en las tres facetas del flamenco y el reconocimiento a la historia de esta música, una vez más, ha quedado patente.\n\nAsí, por el lógico funcionalmente del evento, ha surgido un grupo de artistas que a corto plazo —si la valía apreciada en ellos por el jurado es\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"mantener\"]\n\nción complementaria o con el trabajo de la organización, mas lo que supone de difusión de nuestro arte, el trabajo de búsqueda de nuevos valores en las tres facetas del flamenco y el reconocimiento a la historia de esta música, una vez más, ha quedado patente. Así, por el lógico funcionalmente del evento, ha surgido un grupo de artistas que a corto plazo —si la valía apreciada en ellos por el jurado es acertada—deberán ir tomando el relevo para mantener y acrecentar la singularidad y la grandeza musical del flamenco. Es posible que alguno se haya quedado en el camino, sin embargo, no se ha de acabar sus opciones con esta decimoquinta edición, pues la plataforma artística que el Concurso representa está ahí y la oportunidad puede volver a presentarse, aunque también hay que tener en cuenta la lógica repercusión que su participación ha podido tener. Y dentro ya de lo que fue la fase de opción a premio, apuntar que hubo demasiados concursantes. Sí, esta era la opinión en general de la mayoría de los que siguieron las sesiones. No sé cómo concursaron en la fase de admisión, mas creo que debieron estar mejor que en las finales para colarse en estas últimas. Los concursos, con su faceta de jugárselo todo a una carta, tienen estas cosas. No es la primera, ni será la última vez que un participante canta m\n\n[ENDING CONTEXT]\n\ncon adecuación y aseo. En la petenera estableció una identificación plena del estilo y ciertas dotes de virtuosismo en las falsetas. En cuanto al acompañamiento del baile por bulerías, desarrolló bien la introducción, efectuando el acompañamiento que reclama el bailaor con total acoplamiento. En este premio lució igualmente Luis Calderito, subordinándose adecuadamente al cante y estableciendo perfectamente el papel de cada uno. Otro tanto efectuó en los estilos libres, y en el baile por bulerías, su introducción resultó de una belleza simple, realizando posteriormente un buen acompañamiento.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XV Concurso de Arte Flamenco de Córdoba",
    "periodical": "candil",
    "issue_id": "1998-09",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 1183,
    "article_char_count_full": 7346,
    "article_char_count_review": 2946,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "mantener"
      }
    ]
  },
  {
    "article_id": "1998-09-28-right-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Valera Espinosa\n\nTítulo: «Flamenco de Jerez a los Puertos»\n\nCantan: Chano Lobato, José Vargas «El Mono», Fernando Terremoto y Antonio Reyes\n\nTocan: Fernando Moreno, Luis Moneo y Diego Amaya\n\nDirección artística y musical: Ma- nuel Morao Productor: Luis Pérez\n\nReferencia: ECO 103. Manuel Morao. Gitanos de Jerez, S.L. Apartado 592. 11480. Jerez. 1998 Nuevamente Manuel Morao, con el sentido de la oportunidad que posee, amén del artístico, reúne a cuatro fieles exponentes del arte de sus respectivas localidades de nacimiento: Cádiz, Chiclana y Jerez. Mas por otro lado y propiciado el contraste, elige a dos —Chano Lobato y El Mono— añejos, y dos —Fernando Terremoto y Antonio Reyes— jóvenes, y cada pareja —joven y añejas— de sendas escuelas cantaoras, de ahí el título de «Jerez a los\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"nuevo\"]\n\ndos —Chano Lobato y El Mono— añejos, y dos —Fernando Terremoto y Antonio Reyes— jóvenes, y cada pareja —joven y añejas— de sendas escuelas cantaoras, de ahí el título de «Jerez a los Puertos». Estas grabaciones, plasmadas del directo durante la celebración del I Ciclo Flamenco «De Jerez a los Puertos», celebrado en el Puerto de Santa María, vienen a mostrarnos el contraste entre la campiña y la bahía gaditanas; entre lo añejo o consolidado y lo nuevo, o sea, el cante flamenco a través de dos formas diferentes de apreciarlo e interpretarlo, y que por razones de vecindad y estrechamientos de lazos, se complementan. Unos y otros —Jerez y los Puertos y viceversa— se transmiten influencias, son rivales y a la vez se unen para evidenciar sus localismos cantaores. Abre Fernando Terremoto por malagueñas del Mellizo con unas expresiones cantaoras matizadas de dramatismo y sonora melodía. Expone con naturalidad las cualidades de su casta cantaora (¡qué guitarra la de Fernando Moreno!), abundando en la jondura con la que el gaditano adornó su malagueña. En los tientos-tangos vuelve a dejar la singularidad de una voz nueva con resonancias paternas, acrisoladas en una personalidad que se atreve a dejarnos un compendio de Terremoto, Caracol y Pastora, con un adecuado compás en los tangos. El otro jerezano, el añejo, o sea, José Vargas «El Mono», acomete el estilo de la tierra: la siguiriya. Y lo hace con las garantías del intérprete que domina y conoce las dificultades para desarrollar el palo. Su veteranía y enjundia le da empaque a la evocación de Tío José de Paula, llevado en alas de maestría también por la guitarra de Fernando Moreno. El chiclanero Antonio Reyes evidencia su arte primeramente por bulerías por\n\n[ENDING CONTEXT]\n\nde la anterior.\n\nFrancisco Javier Jimeno demostró ser uno de los más dotados para el acompañamiento actual del cante y el baile flamenco. Por ello se hizo acreedor del premio Manolo de Huelva. Su encauzamiento del cante de Juan Reina por malagueñas, tiene una primorosa y virtuosa introducción; y en el resto del acompañamiento mantiene el arropo adecuado al cantaor. En las soleares se ajusta al compás del estilo y vuelve a encaminar a Juan Reina por los tonos adecuados, destacando el cantaor por El Mellizo y el guitarrista por sus falsetas a tiempo y por la sobriedad en los tercios cantados.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografia Flamenca",
    "periodical": "candil",
    "issue_id": "1998-09",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "28-29",
    "page_number": 28,
    "word_count": 1585,
    "article_char_count_full": 9742,
    "article_char_count_review": 3350,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "nuevo"
      }
    ]
  },
  {
    "article_id": "1998-11-11-right-origen-y-proyecci-n-flamenca-del",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAlfredo Arrebola\n\nE l flamenco es un sistema complejo de vivencias que debe ser estudiado a la luz de la razón. Ya ha pasado, afortunadamente, la época de decir vulgaridades e intentar demostrar verdades que sólo existen en la mente calenturienta de su autor. Desde siempre he creído que los Congresos de Flamenco son los llamados a dar luz y evidencia histórica, dentro de los límites posibles, a tantas inquietudes que vienen atormentando a los buenos aficionados al arte flamenco en su trilogía de valores: Cante, Bailey Toque.\n\nEsta inquietud me ha llevado, como cantaor e investigador, al máximo amor y veneración de la grandeza del Flamenco, y me ha impulsado hacia el campo de la investigación, con la grave responsabilidad que conlleva tal ejercicio; he procedido siempre con nobleza y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"históricos\"]\n\nlcra objeti- vidad. ¡Que hay errores!; —ha-berlos, haylos—, pero no por maldad o cabezonería, sino porque el complejo mundo del flamenco, desde su inicio, interesó más bien poco al intelectual, al historiador, al poeta e incluso al músico. Pero siempre hay sus excepciones. Sin embargo, esto no debe ser óbice para que nosotros, en las postrimerías del siglo XX, y con la humildad que caracteriza al flamenco, pongamos nuestras manos en los hechos históricos, e intentemos buscar la verdad de un arte que ha llegado a convertirse en plenipotenciario de la canción andaluza, como muy bien nos dejó dicho Ricardo Molina. Éste, y no otro sentido, me ha empujado y animado ;por qué no? a ofrecer a todos vosotros, amigos y compañeros de camino en el flamenco, a rastrear en mis escritos, revistas, periódicos y libros sobre un tema apasionante como es el que figura en el título de esta ponencia: «Origen y proyección flamenca del Fandango de Lucena». El fundamento de esta investigación radica, conforme a mi juicio, en la similitud entre poesía y cante, cuyos argumentos expuse en la apertura del Curso 1980-1981 en el Áula de Flamencología de la Universidad de Málaga. Allí dije, como ahora lo digo en este XXVI Congreso de Lucena, que mientras haya poesía habrá cante. Y por aquí podríamos vislumbrar en cierto sentido el origen común de todos los fandangos, y por tanto el del llamado Fandango de Lucena. Cerrad un momento los ojos y recordad el maravilloso mundo cultural y poético de este precioso pueblo cordobés. ¿Por qué puedo afirmar esta semejanza (poesía/cante)? Porque ambas manifestaciones artísticas coinciden en su temática: el hombre. Nacimiento, vida, muerte, sentido de la existencia, más allá, el Absoluto, la nada y otros interrogantes que se hace el hombre determinan la esencia de la poesía y del flamenco, cfr. «Antología de la poesía flamenca». Ed. Agora, Málaga, 1993, de A. Arrebola. El flamenco, semánticamente considerado, forma parte del folklore en cuanto que es «sabi-duría innata del pueblo». El folklore cordobés es vastísimo y riquísimo en matices psicoantropológi\n\n[ENDING CONTEXT]\n\nAntonio Fernández Díaz «Fosforito», Curro Lucena, Sr. Vilila, Frasquito Espada y... tantos y tantos buenos y fieles «aficiona» de la comarca lucentina.\n\nSi en algo ha sido útil este breve ensayo «Origen y proyección flamenca del fandango de Lucena», sea, en primer lugar, para la bellísima, emblemática y hospitalaria Lucena, para los «creadores y difusores» de tal fandango y por todos los que han hecho posible este XXVI Congreso de Arte Flamenco. El flamenco tiene «sus» razones históricas, literarias y musicales con fundamento «in re»: Hay que buscarlas a través del estudio y la investigación.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Origen y proyección flamenca del Fandango de Lucena",
    "periodical": "candil",
    "issue_id": "1998-11",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "11-16",
    "page_number": 11,
    "word_count": 5079,
    "article_char_count_full": 30821,
    "article_char_count_review": 3723,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "históricos"
      }
    ]
  },
  {
    "article_id": "1998-11-17-left-b-el-fandango-de-lucena",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNorberto Torres Cortés\n\nSobre el Fandango de Lucena La mayoría de las músicos y musicólogos que se han ocupado de describir la forma fandango han señalado reiteradamente el bimodalismo como uno de sus rasgos. $ ^{1} $ Sin embargo investigaciones más detalladas sobre este particular $ ^{2} $ amplían esta constatación subrayando que la bimodalidad no sólo afecta el contraste entre parte instrumental y copla, sino que se produce en el seno de la misma copla. Queremos, en primer lugar, desarrollar este último extremo, tomando como referencia el fandango de Lucena, una de las variantes de la forma fandango que más puede ilustrarlo, siguiendo los esclarecedores trabajos de Philippe Donnier sobre este fandango. Ampliaremos en segundo lugar la exploración de este bimodalismo interno a otras\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\ngar, desarrollar este último extremo, tomando como referencia el fandango de Lucena, una de las variantes de la forma fandango que más puede ilustrarlo, siguiendo los esclarecedores trabajos de Philippe Donnier sobre este fandango. Ampliaremos en segundo lugar la exploración de este bimodalismo interno a otras formas de fandango donde también ocurre, como algunas variantes de malagueñas, y sobre todo los estilos llamados «de Levante». En tercer lugar llevaremos a cabo un estudio comparativo entre todas estas variantes, lo que nos permitirá establecer una relación de estilos cercanos al fandango de Lucena y una hipótesis sobre su peculiar cromatismo. a) Las áreas del fandango En el espacio geográfico que nos interesa acotar ahora, la región andaluza, aparecen cuatro áreas etnográficas establecidas a partir de variables rítmico-melódicas de la forma fandango: la provincia de Huelva con los llamados «fandangos de Huelva», las provincias de Sevilla y Cádiz con los llamados «fandangos personales o artísticos», las provincias de Cádiz (Tarifa), Málaga, Almería, Granada, Córdoba (sur), con los llamados «fandangos abandolaos», parte de las provincias de Almería, Granada, Jaén, regiones murciana y castellano-manchega con los fandangos de la llamada «zona de cuadrillas». Lucena, que pertenece a la Subbética se incluye en la tercera área aquí señalada, la de los fandangos «abandolaos». b) El fandango de Lucena Philippe Donnier ha puesto de manifiesto varias veces en sus trabajos $ ^{3} $ y ponencias $ ^{4} $ el color disonante especial del fandango de Lucena dado por la reiteración de la nota sib en el desarrollo de la copla acompañada por el toque «por arriba», es decir con el patrón armónico modo de mi/doM. Esta nota y su lugar en las frases o «tercios» aparece claramente en el patrón melódico reducido de fandango de Lucena, que ha elaborado a partir de tres interpretaciones de Cayetano Muriel. Si sometemos a análisis musical esta melodía, aparece lo siguiente: 1) Incipit melódico. (Ver ejemplo musical núm. 1.) 2) Organización melódica y ámbito. (Ver ejemplo musical núm. 2.) (Las notas blancas señalan los descansos de las semi-cadencias y cadencia final. Las notas negras son las demás. El sib está señalado con X.) Ámbito: 7ª. 3) Particularidades melódicas. Cromatismo en torno a los IV y V grados. 4) Organización interválica: suele ser con intervalos conjuntos de medio tono y tono. Sin embargo encontramos intervalos de segunda aumentada o tercera menor en los tercios 1, 2, 3, 5 y 6, tercera Mayor en el 6°, cuarta en los tercios 3 y 4. 5) Incipit y cadencia de los tercios: 5.2 / 5b.4 / 3.2 / 5b.5b / 3.2 / 5b.1. Vemos que los tercios 1°, 3° y 5°, es decir los impares, descansan sobre el II grado, mientras los 2°, 4° y 6° descansan sobre el IV, el V bemolizado y el I. En cuanto a los incipits, destaca el V grado bemolizado en los tercios 2°, 4° y 6°, es decir los pares\n\n[ENDING CONTEXT]\n\nflamenco con el auge de los cafés cantantes, destacando entre ellos la pareja Chacón/Montoya, ordenaron y definieron las reglas de una parte de este arte multiforme que es el flamenco de hoy.\n\nSeñalamos a continuación la localización de las partituras que hemos utilizado para elaborar esta ponencia:\n\nl) Patrón melódico del fandango de Lucena (DONNIER, P.: Flamenco. Relations temporelles et processus d'improvisation. Tesis de doctorado, Université París X, 1996, pág. 480).\n\n5) Murciana del Cojo de Málaga (NAVARRO, J. L. y AKIO INO: Ob. cit. págs. 68, 69, 70).\n\n11) V.V.A.: Ob. cit., pag. 160.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Fandango de Lucena",
    "periodical": "candil",
    "issue_id": "1998-11",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "16-24",
    "page_number": 16,
    "word_count": 6628,
    "article_char_count_full": 40412,
    "article_char_count_review": 4529,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  },
  {
    "article_id": "1998-11-24-right-manuel-vila-el-gran-olvidado-del",
    "article_text_for_review": "Joaquín Rojas Gallardo\n\nSi antes de encaminarme hacia Lucena, para asistir al XXVI Congreso Nacional de Arte Flamenco, me dicen que no se va a hablar para nada del nombre de Manuel Ávila, me hubiese creído que se trataba de una broma. Pero la realidad, la triste realidad, ha sido esta y mucho más triste, ya que el temario era prácticamente un monográfico acerca del fandango de Lucena en sus distintos estilos y una exaltación merecidísima hacia la figura de su principal valedor, Cayetano Muriel «Niño de Cabra».\n\nUno a uno fueron pasando los ponentes con sus respectivos trabajos e igualmente fue saliendo a la luz el material discográfico y bibliográfico propio de la temática congresual y mi extrañezas ante tamaño olvido iba subiendo de tono hasta quedarme atónito y saltar reclamando su nombre con letras de oro, tras la última frase de la última ponencia, escrita por Alfredo Arrebola.\n\nDesde mi punto de vista, Manolo Ávila ha sido el mejor embajador del fandango de Lucena y el más fiel defensor y discípulo de Cayetano, como lo atestiguan los numerosos premios que venían relacionados en los dos libros presentados a los congresistas, uno sobre el fandango de Lucena en general y otro sobre la biografía del «Niño de Cabra» en diferentes concursos. Esa fue la única alusión a su nombre porque la historia es testigo de una realidad que aquí se ha querido ocultar.\n\nFue en el año 1980, con motivo del IX Concurso Nacional de Córdoba, cuando desde mi condición de miembro del jurado, apoyé sin tapujos la concesión del premio nacional Antonio Chacón de Cantes de Levante, según la reglamentación de aquella edición, a Manolo Avila, que destacó en fandangos de Lucena, no sólo por su indiscutible merecimiento, sino porque estaba totalmente seguro (como así fue) que en las palabras de agradecimiento al recibir el premio en los jardines de los Alcázares de los Reyes Cristianos, éstas serían un homenaje sentido y emocionado hacia la figura de Cayetano: «Tan olvidado por Córdoba y que tan poca gente conoce», en palabras casi textuales.\n\nEn su mente tenía sobre todo tres dioses: Cayetano, Juan Breva y Antonio Chacón. Si existiesen manicomios del arte, Manolo ocuparía la habitación número uno del correspondiente al flamenco, porque no había una sola hora del día en la que no hilara algo sobre el tema flamenco, tanto en conversación como en su cantiño, hasta el extremo que Canalejas de Puerto Real, le puso el sobrenombre del «transistor», porque siempre estaba cantando y defendiendo sus teorías. Servía de polea de transmisión de los viejos maestros, hasta quedar afónico a pesar de tener que disputar primeros premios, como por ejemplo en el Festival del Cante de las Minas de la Unión y del que fui testigo personal.\n\nPues bien, todo su entusiasmo convertido en locura hacia nuestro arte, ha sido olvidado en este Congreso.\n\nSi durante su vida siempre le dolió de sobremanera el olvido que durante muchos años sufriera Cayetano, no será menos cierto que si desde algún lugar ha podido ver el desarrollo de este Congreso, también habrá tenido una gran desilusión.\n\nPorque, repito, Manolo Ávila ha sido, después de Cayetano, el mejor embajador de los cantes lucentinos y todo ello sin ser hijo de ese espléndido pueblo de Lucena.",
    "title": "Manuel Ávila, el gran olvidado del Congreso de Lucena",
    "periodical": "candil",
    "issue_id": "1998-11",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 545,
    "article_char_count_full": 3247,
    "article_char_count_review": 3247,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
