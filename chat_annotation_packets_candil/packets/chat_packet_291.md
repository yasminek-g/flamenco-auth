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
    "article_id": "1994-07-19-left-para-los-lectores-de-candil",
    "article_text_for_review": "Lo que dijo el periodista José Simón Valdivieso del extinguido cantaor Antonio del Pozo «El Mochuelo», después de cincuenta años «o la vida de un cantaor».\n\n«Antonio del Pozo \"El Mochuelo\", el cantaor de flamenco que ha tenido mayor popularidad».\n\n«Quién no ha oído alguna vez, có-mo un gramófono, después del inevitable carraspeo preliminar gritaba con su vocecilla lejana y agria de gnomo acatarrado: Farruca cantada por el Mochuelo».\n\n«El Mochuelo ha sido, entre los cantaores de flamenco, el de mayor popularidad. Los habrá habido más considerados por los inteligentes, pero más del dominio público, más universalmente conocido, no».\n\nPues bien, «El Mochuelo», el auténtico Antonio Pozo, viejo y pobre, luchando bravamente por la vida, simultanea la humilde ocupación de camarero en un bar excéntrico, con sus actuaciones en el único tablao que en Madrid queda, donde con voz temblona y como empañada de una amargura tácita que el público no advierte, canta unas coplas tristes de un simbólico dramatismo que resbala sobre la sensibilidad primitiva del auditorio (catetos que vinieron a vender sus hortalizas al Mercado de la Cebada, escribientes de juzgados, jaques y marchositos, maestros de obras, daifas de literatura rusa...) «Castillos he visto yo, abatíos por la tierra», canta «El Mochuelo» con su voz húmeda, de lágrimas, mientras el público apaga con el fragor de su orgía rural los dos últimos tercios de la soleá: naide se tenga por grande, que el\n\nmundo da muchas guertas.\n\nHemos hablado con «El Mochuelo». Frente al repórter, este hombre chiquitín y pulcro, se crecía recordando sus épocas de gloria y de triunfo, y en su mirada un brillo de juventud que desmentían canas y arrugas de consuno. He aquí lo que dijo el cantaor:\n\n—Tengo sesenta y tres años y soy casado reinsidente. ¡Valor que tié uno!\n\n—:Es usted andaluz?\n\n—De Seviya, pos claro. De Seviya na má. ¿De dónde iba a ser yo, cristiano?\n\n—Hace mucho que empezó us- ted a cantar?\n\n—Unos cincuenta años. Era yo un chavea con mis buenos dose añitos y trabajaba de aprendiz de cuchillero, dándole ar fuelle, que era pa lo único que servía y por lo que me daban reá y medio diario. ¡Osté carcule! Un día me oyó de cantá un guardia, que era amigo de mi padre, y me dijo: «Chavea, ¿tú quiés cantá ande te oiga la gente que chanela de cante?». «Pos claro que sí, contesté. Y fue y me llevó a un café que había en la Puerta de Carmona. Canté, gusté mucho... y no me dieron na, pero ar día siguiente me hisieron de vorvé y me dieron un duro. Lo primero que gané con er cante. Después hise una turné por Málaga, Córdoba y Ronda con el célebre Silverio Franconetti y las auténticas «viejas ricas», cobrando mi sueldo tos los días. De ahí arranca mi carrera.\n\n—¿Le ha producido mucho el cante?\n\n—He cobrao de suerdo hasta veinte duros diarios. Y de los discos de gramófono he llegao a cobrar hasta siete mil pesetas por impresionar una sola matriz. Pero el dinero, ande se ganaba de verdad era en las reuniones cuando había verdaderos y güenos afisionaos ar cante que pagaban como príncipes. ¡Aquella época güena de los altos de Fornos...! Así, en globo, creo que he venió a ganá en mi vida de cantaó unos cien mil duros.\n\n—;Y conserva usted?\n\n—Cuatro pesetas treinta y sinco céntimos, que es tó mi capitá en este instante.\n\n—¡Brava cifra!\n\n—Me ha gustao vivir bien y no le he dao nunca demasiada importancia al dinero. Se ha rosao uno con la grandesa, y algo se pega de sus costumbres.\n\n—¿Con la grandeza? Precisemos: ¿a qué llama usted grandeza?\n\n—Pues, hombre, yo he cantao en casa del marqués de la Romana, del gran duque Wladimiro; ante Su Majestad el Rey, dos veses... Si esto no es la grandesa, usté dirá.\n\n-¿Y de España, ha salido usted?\n\n—Estuve en América. He cantao en Buenos Aires, Rosario, Montevi-\n\ndeo, Méjico... Gusté mucho y traje plata; pero de todo ello sólo me que-da este dije, recuerdo de un homenaje que me hicieron en Buenos Aires. Ya ve usté que es de oro y tiene sus piedras finas, pues ni en los momentos más difíciles y de mayor desesperación, me ha pasao siquiera por la imaginación el desprenderme de él. Pué ser que un prestamista le diera bastante valor, pero pa mí vale mucho más. ¡Romantiquismo! Una mijita romántico que es uno.\n\n-¿Qué es lo que canta usted mejor?\n\n—Lo que canto con más cuidao, las malagueñas. Lo que me ha dao más fama, las guajiras y la farruca. Pero canto de to. Cante jondo propiamente dicho que es eso que llaman cante serio los profesionales y que comprende las siguirias gitanas, soleares, jaberas, polos, cañas, martinetes, tonás, livianas, etc., y cante tirao, como son las alegrias, bulerías, fandanguillos... Ah, y además canto jotas y asturianas.\n\n—¿Cuál es el origen de su apodo? —Pues verá usté. Cuando yo empese, estaban de moda entre los cantaores el Canario grande y el Canario chico. Una noche estaba yo cantando en un cuarto y unos que me escuchaban desde fuera estaban comentando: «El Canario grande no es, ni el Canario chico tampoco: ¿qué pájaro será éste? Y un chulón que les oía, contestó: ¿Pues no estáis viendo ustedes que canta de noche? ¿qué pájaro va a ser? Un mochuelo. Y con er Mochuelo me quedé. Hombre, si va usté a publicar ésto, no se olvide de desir que yo acabé con aquel tipo de cantaó vestío de corto, que subía al tablao con una varita pa hacerse son. Diga usté que yo he sío er primero que se presentó al público bien vestío y sin vara.\n\n—Y este hombrecillo, que ha sido en sus años mozos un ídolo de las multitudes, que con los jipíos y trinos de su garganta privilegiada, lo conquistó todo: amor, fortuna, popularidad, va ahora a exhibir su figura senecta y a cantar con voz rota aquellas mismas coplas que antes enardecían de entusiasmo y que ahora ni siquiera escuchan los que no ven que «El Mochuelo» en su triste decadencia, se ahoga, más que por falta de facultades, por exceso de pena».",
    "title": "Para los lectores de «Candil» Manuel",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1032,
    "article_char_count_full": 5852,
    "article_char_count_review": 5852,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-07-23-right-discograf-a-flamenca-rafael",
    "article_text_for_review": "Gracias a la colección «Flamenco Viejo» se está acabando con el egoísmo acaparador de ciertos coleccionistas de añejos discos de pizarra de las más señeras figuras de nuestro arte que los grabaron durante su momento de esplendor, el cual a veces coincidía con la denominada Epoca de Oro del Cante Flamenco. Ya se está terminando el peregrinaje rogador-adulador-humillante a las ermitas-discotecas regidas por ¿aficionados? abades inquisidores-presuntuosos-encorsetadores de normas obstructivas para la difusión del legado cultural más importante y universal de nuestra región: El Cante Flamenco. Ya pocos podrán presumir de que Pastora, Escacena, Chato de las Ventas, Paco Mazaco, Niño de las Marianas, Vallejo, La Rubia, El Mochuelo, Marchena, etc., canta-ban este o aquel estilo de tal o cual manera. El documento sonoro está al alcance de todos los aficionados, y por tanto, el debate flamenco se orienta hacia un horizonte clarificador y libre de oscuras manipulaciones crematísticas y artísticas. Ya son menos los favores que he de pedir para documentarme sobre la creatividad de los cantaores y poder así efectuar lo más acertadamente posible mis análisis sobre el flamenco.\n\nSin embargo, siempre cabe el temor de que esta labor quede interrumpida por no sé qué intereses. O que las grabaciones que en estos compactos se incluyen sean seleccionadas en virtud de tal o cual concepto. Ante estos temores considero que la tarea iniciada hay que\n\nCantan: Pastora Pavón «Niña de los Peines» y «Niño Escacena»\n\nTocan: Niño Ricardo, Currito de la Geroma y Antonio Moreno (a la Niña de los Peines). Miguel Borrull, Román García y Pepito Cilera (a Niño Escacena).\n\ncontinuarla hasta su más largo alcance, y que no debe de haber selección alguna de las grabaciones, a no ser que sean inaudibles, pues la escucha total de su obra flamenca dará auténtica medida de la calidad del cantaor de turno.\n\nPoco he de apuntar sobre las grabaciones contenidas en los compactos que se reflejan, pues tanto la personalidad de la Niña de los Peines, como la de Escacena o Vallejo, más ampliamente por su dimensión artística la de la primera y el tercero que la del segundo, son sobradamente conocidas. Mas a pesar de dicho conocimiento, he de abundar en determinados conceptos que aún me siguen maravillando de sendos artistas.\n\nReitear el magnífico compás de Pastora en sus cantes por bulerías, con un tratamiento a veces desenfadado al acometer coplas por el estilo, pero siempre dejando claro su dominio de la ortodoxia en algún que otro tercio de la grabación, así\n\nNiño Jorge\n\ncomo su personal juego melismático y su dominio de los tonos. La ejecución y estructura de sus cantes mineros con ciertos matices chaconianos en la cartagenera y la taranta, que no malagueña como figura en la carpeta. La triste versatilidad de su vidalita. La simpleza de sus sevillanas y fandangos de Huelva y su arrogante personalidad tonal en el garrotín.\n\nEn cuanto al Niño Escacena, aludir al determinado enciclopedismo cantaor que se transmite a través de sus registros, y muy especialmente en sus cantes por soleares y soleá por bulerías, con claras resonancias del Mellizo y del Carbonerillo, aunque en el último personalismo me pregunto quién aprendió del otro. Muestra, igualmente, su conocimiento de los estilos mineros, malagueñas, marianas —con la misma estructura tonal que Bernardo el de los Lobitos—, garrotín y saeta. Rinde perfecto homenaje a Cayetano Muriel por fandangos y aprecio cierta similitud estilística en la denominada granadina con los fandangos de Frasquito Yerbabuena.\n\nPor su parte, Manuel Vallejo aumenta con este compacto su creatividad por fandangos, algunos de ellos con letras de contenido político —fandangos republicanos los llama— y otros a compás de soleá por bulerías. Su tratamiento personal se muestra una vez más en las tarantas y media granaína con introducción musical de zambra. Evidencia su seguimiento de Manuel Torre en la siguiriya, del Canario o Chacón en las malagueñas, del Gloria en el villancico por bulerías o del Mellizo en las soleares. Por último, patentiza su incorporación a la moda de cantar coplas —en esta ocasión un tango argentino— por bulerías, como otras grandes figuras de su época.\n\nCarlos Cruz\n\nTeléfono (953) 441028",
    "title": "Discografía flamenca Rafael",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 684,
    "article_char_count_full": 4253,
    "article_char_count_review": 4253,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-07-24-left-a-manuel-martin-martin-el-de-la-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAgustín Gómez\n\nM anolo, mírame a la cara cuando me hablas. Observo que hablas de mí, no a mí. Tu «valentía» conmigo es la del perro asustado. Yo te hablo a ti. ¿Te ha enrabietado que use este tono «paternalista»? Pero, ¿cómo voy a hablarte si para mí serás siempre el muchacho que conocí cuando yo iba a la peña de Ecija a dar conferencias sobre Mairena y Caracol? ¿Cómo vas a meterme miedo a mí si siempre te he visto manejando papeles, carpetillas y haciendo listas y circulares; haciendo las preguntas más inocentes a los artistas del género que han visto siempre en ti al «payo del bloc»? ¿De qué desánimo y encogimiento hablas? ¿Es que fue endeble el varapalo que te endié en mi crítica sobre tu actuación en la Universidad? Hombre, insultos, ninguno por mi parte. Yo soy concreto y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"Recuerda\"]\n\nble el varapalo que te endié en mi crítica sobre tu actuación en la Universidad? Hombre, insultos, ninguno por mi parte. Yo soy concreto y contundente, pero no insulto. Tampoco voy a caer en ese estilillo tuyo ahora; le tengo mucho respeto a Candil y no me gusta verlo como un lavadero. ¿Que soy viejo y caduco? Cincuenta y cinco años acabo de cumplir. ¿Sin trabajo? El que tú me das y alguno más que cae sin pedirlo jamás. ¿Que busco protagonismo? Recuerda que me lo diste tú en aquella diatriba, con la que empezaste esta polémica, en la que Sevilla Flamenca puso «negrillas» a mi nombre, repetido hasta el mareo, para que se viera bien. Yo hasta entonces me había limitado en mi medio cordobés a opinar. No sé por qué me acusas de protagonismo cuando muy escasamente intervengo en estas revistas de Jaén y Sevilla, salvo para contestar. ¿Maleducado? Cuenta tus insultos y los míos. Mi palabra, eso sí, es más llana; yo nunca escribiría eso de «anfibológico», tal vez mi «educación» no llegue a tanto. Sí admito lo de «paternalista» contigo porque ya te he dicho que no puedo verte de otra manera. Que Antonio Mairena n\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_03 | trigger=\"escuela\"]\n\n, que las soleares perdidas en las series que indicas «Tiro el dinero mil veces...», «Qué dolor de esta gitana» y «Esta gitanilla tan hermosa...» son solearias de transición o de paso en la serie y, mira por donde, se trata del mismo estilo, esto es, de un solo cante con tres letras diferentes. Entiendo que las soleares, objeto de nuestra polémica, son de estructura melódica valiente y de amplia arquitectura, aquéllas que corresponden a la rica escuela de Silverio en donde se enmarcan estilos tan diferentes como los de Juan Breva, el Matrona, Pepe el de Jun, Cobitos, El Tenazas, El Mulato, Fosforito, Camarón... —por citar los que se pueden escuchar todavía— hasta llegar al Charamusco, sólo éste, con los antecedentes de Marchena y Valderrama y el consecuente de Morente, para que Mairena se lo tomara en serio. ¡Qué lástima que fuera tan tardío! Pero, por favor, dejemos esto ya, al menos por escrito. Vayamos si quieres a una confrontación oral, cara a cara y con tu material. Ah, y con testigos, como en aquella asamblea de críticos en Jaén. Hombre, en Jaén cada uno eligió su ponencia, tú hablaste de «deontología» y yo hablé del lenguaje de la crítica en los medios de comunicación. Te demostré que nuestra polémica era para tratarla en la radio y no en la prensa. Pero no tú en\n\n[ENDING CONTEXT]\n\nmás letras hay, tengo yo a mi amigo José el de mi barrio, que te puede dar miles y miles para que otro las cante, pero tú no querrás que el maestro pierda el récord, ¿verdad? Claro que esto sería entretenerse en contar las rayas que tiene el canto de una peseta. ¿Quién se va a entretener contigo de ahora en adelante, Manuel Martín Martín? ¿Has contado ya las palabras que te he dedicado en esta contrarrequeterréplica? ¿Te las digo pulsando al ordenador en el que te escribo? No, ya sé que tú quieres contarlas. ¿Lo que disfrutas! Querido amigo, termino; pero ni sueños que estoy acabado para ti.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A Manuel Martín Martín, el de la rabieta",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 2429,
    "article_char_count_full": 13989,
    "article_char_count_review": 4098,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "Recuerda"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "escuela"
      }
    ]
  },
  {
    "article_id": "1994-07-26-left-pastora-pab-n-cruz-afortunada-en",
    "article_text_for_review": "Manuel Yerga Lancharro\n\nSi alguno de los presentes en nuestras reuniones periódicas, en los años cuarenta, le decía a Pastora, por supuesto que de forma desenfadada: «Qué fea eres puñetera, pero qué bien cantas». Ella le contestaba también con el mismo tono: «Es cierto que soy fea, porque lo que está a la vista, a la vista está, pero no me negarás que soy grasiosa. Fea y todo lo que tú quieras, pero he sido una mujer afortunada en amores y he traído a algunos hombres de cabeza».\n\nMe consta que así fue. Manuel «Torre» uno de ellos.\n\nYo, que tanto tiempo estuve a su lado, puedo afirmar que, efectivamente, no fue guapa, pero tenía un «ange», unos ojos y un rostro que cautivaba a cualquiera. La verdad es que fue una mujer muy vistosa.\n\nElla, algunas veces, me hablaba de sus aventuras en su juventud. Bien joven, a los veintiséis años de edad, tuvo su primer amor en Málaga, ciudad donde permaneció durante los años 1915 y 1916 actuando casi a diario. Compartió domicilio con la célebre cantaora «La Trini» en la calle Antonio Luis Carrión, número doce (ver mi libro biográfico). Después regresó a Sevilla, una vez que finalizaron sus contratos y rompió sus relaciones con el hombre a quien le hablaba. Por cierto que Pastora tuvo su recaero o enlace en la persona de «El Brevilla», viejo cantaor a quien llegué a conocer, como asimismo al señor Diego el Pijín, Niño de las Moras\n\ny Manoliyo el Jerraó. Fue, precisamente «El Brevilla» quien me contó la aventura de nuestra genial cantaora con más detalles.\n\nPoco tiempo después, en Sevilla, surgió en Pastora otro flechazo. Se enamoró de un hombre que no le fue sincero al no decirle, oportunamente, que era casado. Esto, para una mujer de carácter «bravío», le produjo tal rabreta que optó por marcharse decididamente a Madrid. Allí no le fue difícil desenvolverse localizando a sus guitarristas, Luis Molina y Ramón Montoya, con quienes en los años 1910-16 trabajó bastante grabando y actuando en locales de espectáculos.\n\nPor su indiscutible alta categoría de cantaora incopiable, nunca le faltó trabajo en la capital del Reino.\n\nActuó mucho junto a Manolo Escacena García y formaron la pareja más cotizada del momento. Y como era normal en la vida de Pastora, pronto se enamoró de Escacena. Convivieron juntos algún tiempo, el suficiente para que ella quedara encinta. Pastora me aseguró que se hubiera casado con él, pero éste ya lo estaba con la hija del célebre caricaturista Demetrio, el hombre que en la década de los años treinta, en plena República, dibujaba en «La Guindilla» a los curas acompañados de sus exuberantes «sobrinas».\n\nEn cierta ocasión me dijo María Montoya, hija del famoso guitarrista, que Escacena y su esposa tenían unas buenas y estrechas relaciones de amistad con sus padres y que les visitaban con frecuencia en su domicilio de García de Paredes. (La calle que dio cobijo a casi todos los artistas flamencos).\n\nMe dijo asimismo la hija de Montoya que la señora de Escае-na pesaba más de cien kilos y que para sentarse cómodamente tenía que utilizar dos sillas. A todo ésto, yo le dije: María, ¿cómo es posible que Escaena se enamorase de una mujer tan obesa? Ella me contestó: «Mira, Yerga, mi madre me contó que cuando se casaron, ella no pesaba ni setenta kilos y que tenía una cara bellísima como para enamorar a cualquiera. La obesidad le sobrevino después de contraer matrimonio».\n\nLa hija de Pastora vive en Sevilla. En La Campana tiene una administración de lotería y se siente muy orgullosa cuando dice que se llama Pastora Escacena Pabón. Así, al menos, lo dijo a un periodista y así apareció publicado en los medios de comunicación.\n\nPACO DE LUCIA Y FAMILIA: El Plan Maestro\n\n«En este libro, Don Pohren ha conseguido con acierto dar una visión rigurosa sobre el flamenco contemporáneo a través de la familia Sánchez (Paco de Lucía y Familia)». Manuel Martin Martin, Guía de Sevilla de Diario 16.\n\nPrecio de venta en librerías 2.600 Pts.\n\n«Completo, antológico... D. E. Pohren, gran conocedor de la magia flamenca, ha sabido retratar, sabiamente en su libro, la figura irrepetible de Paco de Lucía. Daniel Pineda Novo.\n\nPedidos a: Sociedad de Estudios Españoles Apartado de Correos, 83. LAS ROZAS (Madrid)\n\nVicente Amigo. Guadalcanal (Sevilla), 1967, aunque se le considera cordobés, puesto que cuando tenía cinco años su familia se estableció en esta ciudad. Cordobeses fueron sus primeros maestros en la guitarra: Juan Muñoz «el Tomate» y «Merengue de Córdoba». Más tarde fue discípulo de Manolo Sanlúcar, con el que inició su carrera profesional formando parte de su grupo. Intérprete y compositor lúcido, Vicente Amigo tiene muy claro lo que su profesión le exige. Cuando el crítico Agustín Gómez le preguntaba qué era más importante para él, si el corazón o la cabeza, el artista respondió: «Las manos tienen que estar en función de la cabeza; pero por supuesto hay que tener la técnica y hay que estudiar mucho..., lo suficiente para que las manos luego estén al servicio de la cabeza, de la sensibilidad, y de lo tú quieras hacer. Tú tienes que tener las manos para que ellas no sean ninguna barrera». Honores recibidos: Primer premio de Guitarra en el XXVIII Festival Nacional del Cante de las Minas de La Unión (1988); Premio Ramón Montoya de Guitarra en Concierto del XII Concurso Nacional de Arte Flamenco de Córdoba (1989); Premio Ojo Crítico, II Milenio, de Radio Nacional de España (1991); Cordobés del Año 1991 en Popularidad y Música; Premio Ícaro de «Diario 16» (1991), y Premio Ateneo de Córdoba, Fiambrera de Plata (1992).\n\n(De fascículos «Arte Flamenco»)\n\nTOCAORES DE HOY\n\nVicente Amigo",
    "title": "Pastora Pabón Cruz, afortunada enamores Manuel",
    "periodical": "candil",
    "issue_id": "1994-07",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "26-27",
    "page_number": 26,
    "word_count": 951,
    "article_char_count_full": 5604,
    "article_char_count_review": 5604,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-09-3-left-editorial",
    "article_text_for_review": "Editorial\n\nL en público de un nuevo sello discográfico, Discos «probeticos», nos sugiere esta reflexión, cuyo interés estimamos que trasciende incluso a la personalidad artística, sobradamente conocida y consagrada, de su promotor, el cantaor Enrique Morente. La iniciativa no es nueva. Otros artistas como «El Cabrero» y Gerardo Núñez la han emprendido, con desigual éxito. Y otros, en el futuro, la emprenderán. Ello, no obstante, la singularidad de la noticia radica en los objetivos que se dicen perseguir, así como en la significación que entraña la puesta en marcha de una empresa discográfica regida por un «aficionao»; con lo que eso comporta: Persona que no sólo cuenta con criterio y conocimiento de ese universo, sino que ama la materia que amasa en sus adentros. En éste y no en otro sentido cabe entenderlo, si, como ardientemente esperamos, resulta sincera la afirmación del cantaor granadino, cuando en declara-ciones a un diario madrileño, mantenía: «Los músicos de flamenco no tienen por qué ser esclavos de los grandes magnates del disco».\n\nA nadie se le oculta la tiranía que ejercen las grandes casas discográficas que limitan y determinan la versión propia del artista flamenco, imponiéndole concesiones basadas exclusivamente en criterios comerciales que desfiguran el rostro íntimo de la obra. Y de tal imposición no se libran ni aun aquellos artistas que pudieran considerarse como consagrados; las dificultades para grabar con un mínimo de la dignidad exigida por el flamenco se refieren no sólo a los artistas incipientes, sino también a los que pueden acreditar una trayectoria tan plena de\n\naciertos como la de Enrique Morente. Con ello no estamos extendiendo una patente de corso a todas las producciones que ha realizado o pueda realizar en el futuro este cantaor. Es más, en casos puntuales hemos dejado constancia de nuestro disentimiento. Pero nadie puede escatarle el arrojo, el sentido de independencia que muestra, máxime si se tiene en cuenta que, en ninguno de los casos, el flamenco puede derivar al negocio espectacular que alimentan esos grandes magnates del disco. O dicho de otro modo, lo subrayable es el esfuerzo por erradicar cualquier suerte de servidumbre frente a quienes priorizan la cuenta de resultados sobre valores genuinos del flamenco.\n\nLa denominación del sello discográfico ya es todo un signo. Discos probéticos se presenta como un proyecto que evoca precariedad de medios, modestia, sinceridad. Deseamos a Enrique Morente que, en consonancia con los propósitos que públicamente ha expuesto, alcance toda clase de éxitos. El haber optado por la independencia constituye ya el primer y, acaso, el más importante éxito.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1994-09",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 423,
    "article_char_count_full": 2676,
    "article_char_count_review": 2676,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
