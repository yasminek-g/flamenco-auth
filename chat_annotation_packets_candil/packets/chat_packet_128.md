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
    "article_id": "1986-03-15-right-30-aniversario-del-concurso-naci",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFOSFORITO\n\nPor Manuel Martín Martín\n\nEl Concurso Nacional de Arte Flamenco de Córdoba cumple treinta años.\n\n- Bases del XI Concurso Nacional.\n\nBases del Premio Ricardo Molina Tenor.\n\nEllos, los protagonistas, dicen:\n\nAntonio Cruz Conde Fosforito Antonio Alarcón Constant José Menese Matilde Coral Manolo Cano Herminio Trigo\n\nPremios Nacionales de Arte Flamenco.\n\nPremios Ricardo Molina Tenor.\n\nT reinta años de rigurosa singla-dura por las ventas del flamenco, dentro de una historia acotada por experiencias efímeras, merecen algo más que la humilde referencia que hoy presentamos a nuestros lectores de CANDIL. Pero es que, además, si esa loable trayectoria ha incorporado elementos esenciales para la perfecta compren-\n\nsión del fenómeno flamenco, en la actualidad, como en verdad ha sucedido con\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"Arte\"]\n\nemios Ricardo Molina Tenor. T reinta años de rigurosa singla-dura por las ventas del flamenco, dentro de una historia acotada por experiencias efímeras, merecen algo más que la humilde referencia que hoy presentamos a nuestros lectores de CANDIL. Pero es que, además, si esa loable trayectoria ha incorporado elementos esenciales para la perfecta compren- sión del fenómeno flamenco, en la actualidad, como en verdad ha sucedido con el Concurso de Arte Flamenco cordobés, este pequeño esfuerzo resulta aún más aminorado. Ello, no obstante, no hemos querido que transcurra la efemérides sin esta sencilla contribución, consistente en una serie de entrevistas con quienes, de una forma u otra, fueron protagonistas ya en la edición inaugural de 1956 y con quienes son protagonistas hoy. Políticos, cantaores, guitarristas, etc., trazan, por la vía del coloquio, una aproximación al sentido y significado del concurso cordobés. Sólo nos queda congratularnos con los amigos del flamenco de la bella ciudad de la Mezquita y, particularmente, a quienes tienen asumida la responsabilidad de organizar el concurso, nuestra sincera felicitación y solidaridad. Con la undécima edición, que este año, se celebra del Concurso Nacional de Arte Flamenco de Córdoba, cuya presidencia de honor ha aceptado Su Majestad el Rey, se cumplen treinta años desde que el Ayuntamiento de la ciudad, a iniciativa del poeta y flamencólogo Ricardo Molina, del entonces alcalde, Antonio Cruz Conde, y su concejal de Cultura, Francisco Salinas, creara el que, en el transcurso de sus diez ediciones, trienales, se ha convertido en el más importante de todos los acontecimientos flamencos de carácter competitivo y el único reconocido con más categoría nacional en todos los ámbitos de la cultura flamenca. Su Majestad el Rey preside el Comité de Honor A lo largo de estos treinta años las más importantes figuras del arte flamenco —cantaores, bailadores y bailaoras, y guitarristas— han pasado por el Concurso de Córdoba a fin de revalidar su categoría artística y de conseguir, con un premio en el «Nacional», el reconocimiento, el prestigio y la fama que de manera señera otorga\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_02 | trigger=\"aficionado\"]\n\n. En la organización del undécimo Concurso Nacional viene trabajando desde el mes de marzo del año pasado una comisión creada a tal fin por acuerdo del Ayuntamiento. Componen dicha comisión, que se reúne regularmente cada quince días, los concejales José Luis Villegas, teniente de alcalde, de Cultura, y Marcelino Ferrero en calidad de presidente y vicepresidente respectivamente, y, como vocales, Agustín Gómez, crítico; Antonio Povedano, pintor y aficionado; Andrés Raya, de la Editorial Demófilo; Rafael Romero, aficionado; Rafael Salinas, crítico, Juan Velasco, presidente de peña flamenca; José Arrebola, presidente de la Federación de Peñas de Córdoba; Rafael Reina, presidente de peña; Rafael Guerra, presidente de peña crítico, y Juan Ramón Medina, aficionado, con Rafael Román como secretario. Plazo de Inscripción El plazo de inscripción en el Concurso concluye seis días antes del comienzo del mismo, es decir, el día 6 de mayo. Como es sabido, pueden tomar parte en él los cantaores, tocadores y bailadores, de uno y otr\n\n[ENDING CONTEXT]\n\nLosada, «El Yunque» (Premio Don Antonio Chacón).\n\nBaile:\n\nCarmen Ledesma (Premio Juana la Macarrona).\n\nPepa Montes (Premio La Malena).\n\nMeme Reina (Premio Pastora Imperio).\n\nConcha Calero (Premio Encarna- ción López, La Argentinita).\n\nToque:\n\nPaco Peña (Premio Ramón Montoya).\n\nJosé Luis Postigo (Premio Manolo de Huelva).\n\n1971: Don ALFONSO SERRANO, Diario «Córdoba».\n\n1974: Don LUIS MELGAR, Diario «Córdoba».\n\n1977: Don FRANCISCO SOLANO MARQUEZ, Diario «Córdoba».\n\n1980: Don RAFAEL SALINAS, Radio Cadena Española en Córdoba.\n\n1983: Don EMILIO JIMENEZ DIAZ, Radio Popular de Sevilla.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "30 Aniversario del Concurso Nacional de Córdoba. Bases y entrevistas con: Antonio Cruz Conde, Fosforito, José Menese, Antonio Alarcón Constan, Matilde Coral, Manuel Cano y Herminio Trigo, alcalde de Córdoba",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "15-23",
    "page_number": 15,
    "word_count": 10484,
    "article_char_count_full": 62745,
    "article_char_count_review": 4866,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "Arte"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionado"
      }
    ]
  },
  {
    "article_id": "1986-03-25-right-y-va-de-humor-don-cayetano-y-el-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(Relato verídico)\n\nPor Juan Calderón Rengel\n\nA aquél médico —don Cayetano de nombre—, Perote por nacimiento, le faltaba, sin embargo, un puntillo para serlo también por temperamento y vocación: Afición al cante flamenco. Porque, ¿dónde se ha visto a un aloreño que no le guste el cante? Hemos dicho en alguna ocasión que en Alora se toman en serio el flamenco y muy pocas cosas más.\n\nPero aquel médico no es que pasara de esta faceta de nuestro folklore, sino que le tenía declarada la guerra sin cuartel, que sentía por ella verdadera aversión y repugnancia. Cualquier ocasión le era propicia para ridiculizar a los aficionados al arte de Chacón y del Canario, que, naturalmente, él no consideraba tal arte ni con mucho. Cuando en el casino querían oírle desbarrar, ya se sabía: Se sacaba a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\ne, naturalmente, él no consideraba tal arte ni con mucho. Cuando en el casino querían oírle desbarrar, ya se sabía: Se sacaba a colación cualquier motivo relacionado con el cante andaluz, y sólo con ello ya tenía cuerda para rato. Por eso creo que calibré en su justo valor la sorprendente manifestación que hizo un día en la tertulia, estando yo presente, cuando dijo que la única copla que le había agradado a lo largo de toda su vida fue una que escuchó, hacía ya más de cuarenta años, cantada de madrugada, en la puerta de su casa, por un joven paisano, novio de una muchacha que vivía enfrente, en la otra acera de la calle. La noche estaba apacible; la calle solitaria; la luna preciosa. Era un grupo de mozos que iban de ronda. En el encalmado silencio de la noche surgió la magia andaluza hecha lirismo y sentimiento, amor y deseo, petición y gracia. Yo formaba parte de aquel grupo juvenil (¡Dios, qué tiempos!), y repasando ahora a sus componentes me doy cuenta de que faltan casi todos. Y recuerdo perfectamente el fandango que entonó el enamorado galán. Decía así: «Son las dos de la mañana. La luna da en tu tejado. Abreme, que soy tu dueño, y hazme de tu cama un lado, que vengo muerto de sueno». Hay veces en que las mismas causas producen distintos y hasta contrapuestos efectos. Tal ocurrió en el caso de nuestro doctor. Dicen los entendidos en música que para que ésta llegue a gustar hace falta haber oído mucha, aunque las primeras audiciones sean un cruel tormento para el profano. Me parece que con el flamenco y especialmente con el llamado «jondo» o grande, ocurre lo mismo. A nadie que no haya conocido nunca ningún cante andaluz le agradan unos martinetes o unas «seguirillas» escuchados por primera vez. Y es que el flamenco es también clásico, muy hecho, contrastado, logrado y decantado y hay que oírlo durante años, rodeándose del adecuado ambiente, para que empiece a agradar su ejecución. Por eso es también muy difícil y peligroso tratar de crear formas y estilos nuevos, a no ser que se tenga la\n\n[ENDING CONTEXT]\n\napoyada en su cayada. También le amonestó nuestro médico:\n\n—¿Quiére cerrar el pico y callar de una vez? Entonces intervino el acompañante:\n\n—Don Cayetano, ¿y por qué no van a poder cantar estos hombres? Están trabajando en sus tierras, no hacen daño a nadie y nada les prohíbe acompañar de unas coplas las faenas que realizan.\n\nY el protagonista de este verídico relato replicó muy convencido:\n\n—¡Claro! Estas gentes se ensayan ahora aquí, y por las noches se van al patio de la taberna de mi casa y no me dejan pegar ojo.\n\nEl hombre, como buen médico, quería atajar el mal por sus mismas raíces.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Y va de humor: Don Cayetano y el flamenco",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 1156,
    "article_char_count_full": 6688,
    "article_char_count_review": 3646,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1986-03-26-right-hablan-las-penas",
    "article_text_for_review": "VI Encuentro Flamenco. IV Curso de guitarra clásica en Córdoba\n\nOrganizado por el Centro Flamenco Paco Peña de la ciudad cordobesa y del 14 al 26 de julio, tendrá lugar el VI Encuentro Flamenco y el IV Curso de guitarra clásica; con el patrocinio del Ayuntamiento de Córdoba y la colaboración de la Consejería de Cultura de la Junta de Andalucía, Diputación de Córdoba, Patronato Provincial de Turismo y el Conservatorio Superior de Música.\n\nflamenca para principiantes, Ricardo Mendeville.\n\nLos cursos tienen carácter internacional; para más información los interesados deberán dirigirse a Centro Flamenco «Paco Peña», Plaza del Potro, 15, Córdoba - 14002.\n\nEstos cursos están divididos en cinco grupos dirigidos de la siguiente manera: Grupo A, guitarra flamenca, Paco Peña y Tito Losada. Grupo B, guitarra clásica, John Williams y Benjamin Verdery. Grupo C, baile flamenco, Loli Flores. Grupo D, baile flamenco, Carmen Cortés. Grupo AA, guitarra\n\nOrganizado por la Peña Flamenca de Huelva, ha sido convocado el II Concurso de cante flamenco, para el que se han establecido tres grupos de cantes con igual número de premios, siendo el primero de 150.000 pesetas, y el segundo y tercero de 100.000.\n\nLa gran final tendrá lugar el día 6 de junio. Los interesados podrán inscribirse dirigiéndose a la Peña Flamenca de Huelva, Avda. de las Adoratrices, núm. 24, Huelva - 21004.\n\nCon el patrocinio de la Diputación de Málaga y los Ayuntamientos de Mijas y Fuengirola, la Peña Flamenca «La Unión del Cante» de Las Lagunas, Mijas-Costa (Málaga) ha convocado su «III Velero Flamenco», Concurso para aficionados. Dotado con seis premios, el primero de 75.000 pesetas.\n\nLa final tendrá lugar el día 28 de mayo. Los interesados podrán dirigirse a la citada peña, calle San Félix, 15 o a los teléfonos 470653 y 471968.\n\nNueva distinción a «CANDIL»\n\nLa junta directiva de la Unión de Periodistas proclamó oficialmente sus premios anuales, después de un proceso de varias semanas en las que se han aportado nombres.\n\nUno de estos premios recayó en la Revista «CANDIL», por su destacado papel en la investigación y difusión del flamenco.\n\nUna nueva distinción que nos honrra, puesto que viene de los representantes de los medios de difusión, a la vez que nos compromete a seguir superándonos día a día, pero sin triunfalismos, sin otra relevancia que el mismo amor que sentimos por el flamenco.\n\nDamos las gracias en nombre del equipo «CANDIL» y de la Peña Flamenca de Jaén a la Unión de Periodistas.",
    "title": "Hablan las peñas",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 410,
    "article_char_count_full": 2487,
    "article_char_count_review": 2487,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-03-27-left-buzon-flamenco",
    "article_text_for_review": "Sr. Director de CANDIL:\n\nMuy sêõr mó.\n\nEstoy suscrito a la revista que Vd. tan dignamente dirigente, y abusando de su bondad, quisiera a través de esta nuestra revista, hacerle unas preguntas al señor Yerga Lancharro.\n\nEn el mes de febrero leí el libro editado por CANDIL «Apuntes y datos para las biografías de Rojo el Alpargatero, la Trini, Chacón y Manuel Torre», del cual es autor el señor Yerga. Dicho libro núm. 589 en la página 11, da la fecha de nacimiento de Antonio Grau Mora (Rojo el Alpargatero) 8-12-1847. Luego en la página 12 da la fecha de María del Mar Dauce Moreno (la esposa del Alpargatero) 24-9-1840.\n\nPor lo tanto, según estos datos ella era siete años mayor que su marido.\n\nEn la página 18 el acta de defunción de Grau Mora dice: «que el referido finado se encontraba en el acto de su fallecimiento casado con María del Mar Dauce Moroño, de cincuenta años de edad».\n\nEsto ocurría en 1907.\n\nMi pregunta es: que a la vista de estas fechas hay un error, ¿pero dónde?, ¿es de imprenta o del acta original? Porque lo que es indudable a la vista de estos datos, es que cuando murió Rojo el Alpargatero no podía tener su esposa cincuenta años y él, a punto de cumplir los sesenta. Otro dato que me gustaría saber es, ¿en qué año grabó Chacón? Sin otro particular, aprovechó la ocasión para quedar de Vd. muy atento y s.s. José Corona Gómez Miembro de la Tertulia Flamenca de Badalona\n\nEjemplares del disco Antonio Mairena - Cantes en Londres y La Unión, que fue grabado para allegar fondos con destino al mausoleo que se erige actualmente al maestro, pueden ser adquiridos mediante carta dirigida a esta Fundación en el Iltmo. Ayuntamiento de Mairena del Alcor, a la que se acompañará giro postal o talón bancario conformado por la suma de 1.700 pesetas, importe del donativo más gastos de embalaje, franqueos y certificado.",
    "title": "Buzón flamenco",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "27-27",
    "page_number": 27,
    "word_count": 327,
    "article_char_count_full": 1840,
    "article_char_count_review": 1840,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-03-27-right-d-iscografia-flamenca",
    "article_text_for_review": "Por Manuel Martín Martín\n\nQuizás no sea la flamenquísima Revista CAN-DIL el medio idóneo para acunar una reseña sobre las sevillanas, una de las muestras más importantes del folklore andaluz y, concretamente, sevillano. Pero por aquello de que tenemos la fortuna de ser sevillano y de que entre col y col podemos meter una lechuga, que no se alarmen los falsos puristas porque ya va siendo hora de que en este encanallado mundo del flamenco llamemos a las cosas por su nombre y sepamos reconocer, además que es el folklore andaluz el germen del que brotaría nuestra preciada manifestación jonda, amén de valorar en su justa medida la labor improba que la casa discográfica Hispavox acaba de realizar por este patrimonio musical de tanto arraigo popular.\n\nAl parecer, y según todos los indícios, las sevillanas aparecen como baile a mediados del siglo pasado, tomando su origen de las seguidillas sevillanas y casi siempre las cantaban y bailaban las mujeres. A partir de ahí mucho ha llo-\n\nvido y no pocos han sido los cantaores- as que han ejecutado este estilo con una dignidad harto elocuente. En la ac- tualidad, poetas y músicos tienen una cita anual y un compromiso con la Fe- ria de Abril sevillana —termómetro inequívoco de esta seguidilla—, obli- gando con ello a la búsqueda de nue- vos caminos, acomodando la temática de sus letras a la situación social, lo que ha conlevado a través del tiempo a modificar la métrica de las mismas.\n\nPese a ello, y sin pretender sentar cátedra, esto fue motivando la proliferación de innumerables grupos y solistas, algunos de ellos de juzgado de guardia, el abuso de instrumentos ajenos a la verdadera esencia de las sevillanas, le-tristas populacheros y simplones, casas discográficas más pendientes de la comercialidad de los temas que de la calidad de lo impresionado, arreglistas musicales empeñados en aparcar de un plumazo a la guitarra, las palmas o el tamboril, repercutiendo todo ello en la bajísima calidad de la letra y la música de muchas de ellas y convirtiendo a las sevillanas en una música paupérrima, ramplona, insulsa, sin entidad alguna, pero que, eso sí, llega con una facilidad pasmosa al corazón sensible-ro de las masas.\n\nTodo cuanto exponemos nos hacía poner en guardia cada vez que afloraban nuevos discos y «pasábamos» muy mucho de los mismos. Por fin, hemos podido comprobar que existe —entre otros—, quien se ha tomado el tema en serio, ha sabido coger el toro por los cuernos y, sin perder la lógica evolución del tiempo y la técnica, ha sabido ofertarnos ese sabor inmarchitable que siempre han desprendido nuestra sevillanas, esas hermosas letras que le cantan a la feria, al amor al Rocío, a la marisma, o aquellas otras que cantan las excelencias de nuestros pueblos. Esto y mucho más nos lo ha brindado la Casa Hispavox con diez larga duración que merecen unas líneas acompasadas en $ 3 \\times 4 $ (o $ 3 \\times 8 $) en nuestra Revista CANDIL para que sea ella la que ilumine la labor importante de los hombres de Hispavox que continúan en la brecha difundiendo el acervo folklórico de un pueblo que durante el mes de abril canta sus penas, sus alegrías y tristezas por sevillanas.\n\nLos diez volúmenes a que hacemos referencia son los que siguen:",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1986-03",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "27-27",
    "page_number": 27,
    "word_count": 551,
    "article_char_count_full": 3226,
    "article_char_count_review": 3226,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
