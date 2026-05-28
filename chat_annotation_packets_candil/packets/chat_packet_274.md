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
    "article_id": "1993-05-24-left-el-flamenco-en-la-universidad",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA caba de ser distribuida —entre el alumnado— una orla muy especial en la que se acredita ser la primera promoción universitaria de estudios flamencos que se da en el mundo. Y, cómo no, esto había de suceder —otra vez— en Granada.\n\nLos flamencos de Granada está-bamos de enhorabuena. Granada la del primer concurso; Granada, último reducto para la mezcla de culturas que conforman el flamenco; Granada la primera Universidad pionera en dignificar el flamenco; Granada provinciana pero inmortal y pujante...\n\nHistoria de un desencanto\n\nAndábamos por el año 1987 cuando las autoridades académicas de la Universidad de Granada y la bailaora «Mariquilla» deciden, sin más, crear una «Cátedra de Flamencología» de la que la buena «Mariquilla», se hacía cargo.\n\nPor parte del vicerectorado de extensión\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"academia\"]\n\nbailaora «Mariquilla» deciden, sin más, crear una «Cátedra de Flamencología» de la que la buena «Mariquilla», se hacía cargo. Por parte del vicerectorado de extensión universitaria se establecieron unas becas suculentas que cubrían parte de la cara y bien cara matrícula que se estableció en setenta mil pesetas. Entre los alumnos que consiguieron beca y los que pagaron por entero su matrícula resultaron en torno a cincuenta. «Mariquilla», en su academia particular, comenzó a impartir lo que sabía, el baile. Y pasaron los meses... Y pasó el curso entero. Resultó que los alumnos —hubiesen sido becarios o no— habían pagado tan cara matrícula para recibir sólo la asignatura de la práctica de baile. Al curso siguiente se vuelve a abrir matriculación, pero la cosa sigue igual: matrícula cara sólo para recibir una hora de baile. Entrado el año 1989 la cosa no tiene visos de tomar otros rumbos. Los alumnos —en su gran mayoría alumnas— comienzan a inquietarse y a preguntarse si el prometido «Título propio de la Universidad de Granada de Grado Medio» lo será de flamencología en general o simplemente de prácticas de baile. Y así lo dicen a los medios de comunicación locales. La lentitud de reacción de las autoridades académicas hace que las alumnas/os de primero ya estén en tercero. Y de nuevo se abre matrícula, pero, eso sí, con una drástica reducción en lo de las becas y, con ello, se reduce el número de matriculados. Pero de todas formas era ya una bola de nieve que seguía creciendo y creciendo. Ya había alumnas/os en primero, en segundo y en tercero. Al cabo, las autoridades académi-cas caen en la cuenta de que no se puede dar un título universitario\n\n[ENDING CONTEXT]\n\nreconocida como tal por parte de todos.\n\nSería un paso de gigantes en el camino de dignificación y desarrollo del flamenco. Y ello sin entrar en la discusión —tantas veces mantenida— de si se trata de aflamencar a los universitarios o dignificar como universitarios a los flamencos que lo puedan ser.\n\nLa orla está en la calle y vista. Estamos a la espera de ver el tipo de título que se les entrega a esta primera promoción. Cuando lo tengamos será un buen momento para su difusión en estas páginas. Prometemos una segunda entrega porque entiendo que el asunto la merece. Esperemos acontecimientos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El flamenco en la Universidad José",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 1857,
    "article_char_count_full": 11340,
    "article_char_count_review": 3297,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "academia"
      }
    ]
  },
  {
    "article_id": "1993-05-25-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "De las tres ramas en las que suele dividirse el estudio de los escritos flamencos: Historia, análisis de los estudios y grandes biografías, quizá sea esta última la que más predicamento esté encontrando en los últimos años, dado que los principios generales a discutir en los dos primeros apartados ya están lo suficientemente debatidos, a veces hasta el empacho y el empecinamiento, mientras que narrar las trayectorias vitales y artísticas de los intérpretes constituye un caudal inagotable mientras que éstos existan y encuentren a un autor con las ganas suficientes para convertirse en sus biógrafos.\n\nSin embargo, quien esto escribe se muestra partidario de dosificar el uso biográfico del flamenco, dedicando los esfuerzos solamente a aquellos creadores que, o bien hayan dejado una huella indeleble en nuestro arte o que, por su singularidad vital y ejemplaridad interpretativa, merezcan ser resaltados como ejemplos generacionales. En general, prefiero la biografía del artista que, o bien ha desaparecido, labor en la que han brillado de forma contundente estudios como los de Manuel Yerga (Manuel Torre, Chacón, La Trini) o Eugenio Cobo (Macandé, Marchena) entre otros especialistas, o, por el contrario, artistas vivos pero que ocupan un puesto insustituible en esa condición conseguida por tan pocos a la que llamamos magisterio, tal es el caso de Francisco Hidalgo y su excelente biografía acerca de Fosforito.\n\nColectivo\n\nAyuntamiento de Posadas. Diputación provincial de Córdoba, 1992\n\nSALVADOR ARIAS NIETO\n\nDIEGO CLAVEL\n\nMAGISTERIO Y HONDURA DEL CANTAOR SEVILLANO\n\nDiego Clavel. Magisterio y hondura del cantaor sevillano Salvador Arias Nieto Edic. del autor. Santander, 1993 No sucede así en los dos libros que ocupan nuestro comentario, incluso el referido a Luis de Córdoba, que ni siquiera presenta tratamiento biográfico al uso, sino de homenaje, aprovechando el que su pueblo natal, Posadas, le realizara al nombrarlo Hijo Predilecto de la localidad. Ni este cantaor, ni el bueno de Diego Clavel, tienen aún una trayectoria vital y artística que justifique el hacer balance sobre ellos, y, lo que es más importante, quiera Dios que tardemos siglos en volver a hacerlo, puesto que ambos son dos pujantes valores que nada ni nadie va a parar en su marcha ascendente hacia la esencialidad jonda.\n\nEl único problema que se nos plantea, y lógicamente queremos transmitirles, es acerca de la inconveniencia de realizar biografías, aquí y ahora, de unos talantes humanos y artísticos que se mueven a la velocidad de la luz y que, por lo tanto, deberían aguardar el paso de los años para que ambas, vida y fruto artístico, sedimenten en resultados más definitivos.\n\nPero, en fin, el esfuerzo está hecho y, justo es decirlo, con resultados desiguales para quien firma este trabajo, ya que encontramos mucho más sensato y ponderado, por tanto pertinente, el tratamiento otorgado al artista cordobés, del cual, a través de dos conferencias, a cargo de Paco Hidalgo y Agustín Gómez, respectivamente, además de una, a manera de «poética cantaora», a cargo del propio ho-\n\nmenajeado, Luis de Córdoba, se nos perfila el retrato del hombre y del cantaor, no con la nitidez obsesiva en los primeros planos cinematográficos, sino con el arte al óleo de los buenos artistas plásticos, que saben captar el gesto, lo mejor del contrapunto. Tanto el tiro largo, de arquero fino, que desarrolla Hidalgo, como la minuciosa disección de Agustín Gómez, quien, por cierto, no desaprovecha ocasión, con fina coquetería crítica, para, a través de las opiniones del entrevistado, contarnos las suyas, constituyen una mesurada alabanza, tal y como la exigía el acto del homenaje, a la personalidad artística de Luis de Córdoba.\n\nP or el contrario, el libro de Salvador Arias, planteado desde el cariño y respeto hacia Diego, peca de adjetivación elogiosa y abunda en el ditirambo hasta el extremo que, estoy seguro, avergonzará la humildad que todos reconocemos en el cantaor de La Puebla.\n\nPara alabar a Diego, creemos que no es el camino acumular elogios excesivos, ni intentar demostrar que el Jurado que falló a favor de Calixto Sánchez —Diego quedó en segundo lugar— con motivo del cincuentenario del Concurso granadino, se equivocó de pe a pa, lo que motivó que: «Hasta hubo una insistente protesta por parte del público que no estaba conforme con la decisión». Ni así se logra atraer el interés de los lectores, ni se acrecienta el inmenso cariño y respeto que sentimos por el cantaor, pero, en fin, lo del incienso debe ser congénito en el señor Arias, puesto que, en el mismo libro, dedica dos páginas (171 y 172) a elogiar su propio trabajo, a través de un encendido artículo, publicado en el Diario Montañés. Posiblemente, allí, en las queridas y escarpadas tierras cántabras, este libro tenga un éxito que no le auguramos por aquí abajo.\n\n\"Quédate con el Cante\"\n\nFélix de Utrera. Nombre artístico de Félix García Vizcaíno. Canarias, 1929. Guitarrista y letrista. Hijo de padres naturales de Utrera (Sevilla), donde vivió desde los seis años y del que recoge el nombre artístico. De 1957 a 1959 hace gira por América, formando parte del ballet español de Roberto Iglesias. Conoce en Nueva York a Sabicas, grabando un disco con él. Igualmente trabajó con Carmen Amaya y Manolo Caracol. Su discografía es amplísima, teniendo grabados como acompañante unos doscientos cincuenta discos de larga duración y uno como solista titulado Guitarra con solera, además de figurar en numerosas e importantes antologías. Como letrista es igualmente prolífico, habiendo publicado el libro de versos titulado Acrósticos del arte flamenco. Se le considera un excelente conocedor de los toques más tradicionales y seguidor de la escuela de Niño Ricardo.\n\n(Del «Diccionario Enciclopédico Ilustrado del Flamenco».)\n\nTOCAORES DE HOY\n\nFélix de Utrera",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1993-05",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "25-27",
    "page_number": 25,
    "word_count": 935,
    "article_char_count_full": 5829,
    "article_char_count_review": 5829,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-07-3-left-editorial",
    "article_text_for_review": "Diez años después de la muerte del maestro, aún siendo exigua la perspectiva del tiempo, éste, año tras año, día a día, magnífica la personalidad de quien por muchos ha sido conceptuado como el más grande cantaor de todos los tiempos. Hiperbólica o no, tal afirmación, cuya certeza sólo el futuro desvelará en uno u otro sentido, es desde luego compatible con este otro aserto que entendemos debe de imponerse pacíficamente: el más grande «aficionao» de todos los tiempos.\n\nNadie puede cuestionar que la aportación mairenista al esplendor y conocimiento del cante, ha sido, sencillamente, incomensurable.\n\nAntonio Mairena no sólo fue un cantaor genial y su actitud frente a lo jondo, no asimilable a la de otros maestros pretéritos y presentes.\n\nAntonio Mairena fue, ante todo, paradigma de la investigación, siervo de la amorosa pesquisa, indagando los ecos y estilos de una y mil familias de viejos gitanos, a la busca siempre de un legado ya extinguido o en trance de desaparecer. Pudiera objetarse que no el Mairena de siempre; y sí, es cierto, porque singularmente, a partir de la década de los cincuenta, cuando es investido como portador de la Llave de Oro del Cante, Antonio Mairena acepta y soporta la responsabilidad de erigirse en maestro y guía del Flamenco, y en forjador de una dignidad que nadie antes fue capaz de ejemplificar. El propio maestro, en artículo publicado en el número 23 de esta Revista (extra Mairena) se declara conocedor del enorme compromiso que adquiere: «A mí me queda siempre la satisfacción de haber sido y seguir siendo, hasta el final de mi vida, responsable y solidario del compromiso que recibió junto con la Llave de Oro...».\n\nSe ha dicho, con cierto fundamento, que en la codificación de cantes que el maestro nos dejó, prima sobre la pura objetividad, en lo que a ecos y a atribuciones respecta, el sello personalísimo del recopilador, el marchamo jondo con que impregnó la memoria de rancios cantes, diluidos en el olvido. Tal vez sea verdad. Habría que alegrarse de que fuese verdad, porque ello no contribuye al desmérito sino a la grandeza y enaltecimiento de los evocados.\n\nEn cualquiera de los casos, lo que en modo alguno, resulta indiscutible es el grado de dignificación que lo jondo alcanza, merced a la callada labor y absoluta entrega de Antonio Mairena, no sólo al engrandecimiento del cante sino a la reivindicación de los valores singulares de su etnia. A los diez años de su triste desaparición, la historia empieza a hacerle justicia.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1993-07",
    "year": 1993,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 419,
    "article_char_count_full": 2496,
    "article_char_count_review": 2496,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-07-4-left-antonio-mairena-la-necesidad-del",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMiguel Acal\n\n¿Está dicho todo de Antonio Mairena? Hombre, depende. Unos pensarán que sí y otros lo contrario. Vamos, lo normal en estos casos. Cuando se habla mucho de una persona o de una proyección artística determinada, siempre hay los que están hartos y los que creen en la necesidad de seguir ahondando. Todos, eso sí, reconocen que la figura o la labor son importantes. De otro modo, ¿a santo de qué seguir la polémica?\n\nEl hecho de que Antonio lleve diez años desaparecido le da a la cuestión, además, una dimensión diferente. No es lo mismo hablar del presente que del ausente. Para uno el análisis tiene connotaciones que no se dan para el otro. No sólo por la impunidad de las apreciaciones, sino también por la lejanía de los sujetos, el cambio de los tiempos, las modas dominantes, la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"segundos\"]\n\ns, de capitania clara, aunque siempre polémica. Excesos de glorificación o de destierro, llevados a niveles que un mínimo de ponderación y conocimiento no puede admitir. Una línea a seguir Desde 1962 — fecha clave, aunque su trabajo comenzara mucho antes— hasta su muerte, Mairena logró imponer sus formas, sus ideas sobre el cante. En los primeros tiempos —Marchena, el de la Matrona— que estaban signados por una concepción estilística o en los segundos —Camarón de la Isla— que estuvieron dominados por la búsqueda afanosa de nuevas fronteras, Antonio mantuvo firme su papel de primero. Independientemente de lo que piense cada cual de la bondad o maldad de estos dos movimientos coetáneos, el papel del que tenía la Llave era un valor en alza constante. Atención —en esto hay que ser absolutamente claro— que no hablo de personas ni las cuestiono. Sólo tengo en cuenta a las corrientes aparecidas. Pero el líder muere y su figura, dejando a un lado los elogios funerarios o las críticas impunes —o quizás por ello mismo— sigue estando viva. Los primeros —a veces forzados y oportunistas— y los segundos —en ocasiones más fruto de ventoleras modernistas que de otra cosa— continúan sucediéndose. Y eso debe ser tomado como seña de actualidad. No se defiende ni se ataca lo que no tiene importancia ni vigencia. El pasado, en general, es motivo de revisiones, de enfoques diferentes, de nuevos análisis y, alguno de ellos, quita hermosura o grandeza a lo que, tradicionalmente, se consideraba hermoso o grande. En el caso concreto de Mairena nadie duda de su calidad como ele- mento cantaor. Es natural, por otra parte, que los análisis asentados en el inconformismo con su importancia, en la duda sobre su significación como engrandecedor, en su anclaje en lo arcaico, nieguen su capacidad evolutiva y aseguren la necesidad de otras motivaciones musicales más acordes con la realidad actual para dar vitalidad al hecho flamenco. Porque —según quienes defienden esta tesis— el flamenco no puede estar anclado en el pasado, porque el flamenco ha de buscar nuevas fronteras, porque el flamenco ha de vivir con su tiempo. Se pone en entredicho, eso sí, la transformación de lo aprendido, la apoyatura en el pasado para ser figura, incluso la fuerza real de sus razones cantaoras. Pero nadie controvierte el hecho de su capitania, de su calidad de líder. Y es cierto. Es totalmente cierto que el cante ha de estar con su época, ha de cantar a la realidad de sus días, ha de desprenderse de corés que lo comprimen. Pero, pongamos el oído y usemos el sentido común: ¿Cuándo ha cantado Mairena igual que el Ciego de la Peña o lo mismo que Manuel Torre o de forma idéntica al Nitri? Que se ha basado en ellos, es cierto. Lo mismo que Lucía en Ricardo y Ricardo en Molina y Molina en Patíño. Pero son distintos y, cada cual, ha ido aportando evolución y grandeza a la guitarra. ¿O no? Cada cual es muy libre de poner énfasis en aquello que conecta con su sentir. Pero, al igual que el cante no nace por generación espontánea, no es de recibo que se difumine entre músicas que\n\n[ENDING CONTEXT]\n\nde los flamencos. Y, además, considerar inútiles o perturbados a quienes otorgaron los premios y concedieron la admiración. Sea como fuere, con todos mis respetos, yo seguiré peleando —en sentido figurado— por lo que creo. Todos han de contar con el reconocimiento artístico de todos. Pero mezclar churras con merinas es propio de malos pastores.\n\n¿Se lucha contra Mairena o contra los mairenistas? ¿Es un enfrentamiento con la maravilla de su cante o contra los que creemos en la religión de su música y, consecuentemente, no comulgamos, sin despreciarla, con la idea de lo nuevo a ultranza?\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antonio Mairena: La necesidad del sentido común",
    "periodical": "candil",
    "issue_id": "1993-07",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "3-5",
    "page_number": 3,
    "word_count": 2166,
    "article_char_count_full": 12654,
    "article_char_count_review": 4689,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segundos"
      }
    ]
  },
  {
    "article_id": "1993-07-5-right-antonio-mairena-ante-los-valores",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Ríos Ruiz\n\nY a sabéis el parlamento de Heredia, el machadiano personale guitarrista de la obra teatral La Lola se va a los Puertos. Sí, el que dice en versos:...siempre fue seria / nuestra profesión. La copla / y la guitarra flamenca / —usted lo sabe— no son / cosas de broma. La juerga / —se entiende con cante hondo— / tiene función de iglesia / más que de jolgorio. No es / una diversión cualquiera, / donde se mete ruido / y se descorchan botellas. / Para alegrarse en flamenco / se ha de menester mucha ciencia, / mucha devoción al cante / y al toque...\n\nPues bien, así lo han entendido los más significativos intérpretes del arte flamento. Y a lo largo de la historia conocida del cante flamenco, se han venido sucediendo figuras ejemplares, artistas que junto a sus cualidades\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"expresión\"]\n\nnde se mete ruido / y se descorchan botellas. / Para alegrarse en flamenco / se ha de menester mucha ciencia, / mucha devoción al cante / y al toque... Pues bien, así lo han entendido los más significativos intérpretes del arte flamento. Y a lo largo de la historia conocida del cante flamenco, se han venido sucediendo figuras ejemplares, artistas que junto a sus cualidades interpretativas, en ocasiones portentosas, de su más ortodoxa y cumplida expresión, para constituirse en sus auténticos reivindicadores y revalorizadores, en beneficio de cuanto ese arte significa para los que viven su mundo y conocen sus formas, ya sean profesionales o sencillamente fervorosos aficionados. Y adquirir una responsabilidad de índole tan específica, entraña de entrada el don de poseer cierta seguridad personal, una confianza íntima superlativa y el carácter recio y preciso para adoptar una actitud abiertamente firme, dispuesta a enfrentarse a múltiples dificultades distintas y a sortear la incomprensión, a veces muy generalizada. Y en el devenir del cante flamento, esas figuras sumamente importantes para su mantenimiento y vigencia, conforme avanza la investigación, y el estudio al respecto, cada día cobran mayor magnitud histórica y artística. Este es el caso de António Mairena, el último cantaor que además de ser considerado un gran intérprete, se le tiene como el dignificador del flamenco de nuestra época. Algo que nos propone-mos analizar, porque como bien apuntó el filósofo, la reputación de un hombre es como su sombra, que a veces parece más larga que él y otras más corta. Antonio Mairena fue de esa clase de hombres que nace con el germen de la obra que ha de cumplir en esta vida. Y junto a las facultades fundamentales para interpretar el flamenco, tuvo la capacidad necesaria para llevar a cabo su exaltación en unos momentos difíciles, revitalizando sus motivaciones anímicas y raciales y demostrando sus verdaderos valores frente a los estropicios de una mixtificación realizada a mansalva, haciendo realidad los versos unamunianos que dicen: No se mide la verdad a fuerza de mayoría, de una mayor es la maestría, maestria es majestad. Los nombres legendarios y egregios de un art\n\n[ENDING CONTEXT]\n\nsus cantes? Tal vez muy poca o ninguna, pese a que nos afanemos en dársela. Y nos preguntábamos: ¿No será pura anécdota? Al final del tiempo lo que resplandecerá de Antonio Mairena será el cante que ha fijado en sus grabaciones, ese cante que gustará más o menos, que nos proponemos analizar detenidamente y escribir sobre sus características y esencialidades flamencas, pero que nadie podrá negar que ha llenado toda una época, todo este medio siglo flamenco, creando, además, escuela. Exactamente lo mismo que sucedió con don Antonio Chacón, en el siglo pasado, por paradójico que resulte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antonio Mairena ante los valores y la historia del cante",
    "periodical": "candil",
    "issue_id": "1993-07",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "5-11",
    "page_number": 5,
    "word_count": 7810,
    "article_char_count_full": 47602,
    "article_char_count_review": 3816,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "expresión"
      }
    ]
  }
]
```
