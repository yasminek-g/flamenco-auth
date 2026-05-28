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
    "article_id": "1984-11-19-left-discograf-a-flamenca",
    "article_text_for_review": "El hombre es él y sus circunstancias. Dentro de eso, lo que hay que hacer es arte. Yo creo que en definitiva lo que importa es hacer arte. La ortodoxia, y estoy plenamente seguro, a uno de los que le gusta es a mí, pero yo creo que la ortodoxia debe servir, repito, para hacer arte, para invitar a ver nuevas vereas, nuevos caminos y, si esto está hecho con sinceridad y honestidad, siempre sirve para ver nuevas experiencias y los errores para abrir nuevas ventanas.\n\nEnrique Morente a CANDIL\n\nTITULO: Viviré CANTA: Camarón de la Isla TOCAN: Paco de Lucía y Tomatito REFERENCIA: Philips 822-719-1\n\nNunca el mármol podría reproducir tu imagen como el espejo rehúye el malestar de lo perfecto. Poner en libertad tanto caos, tanta caricia excluida,\n\nEn principio reconocer y recoger la capacidad artística y creadora del cantar, aunque últimamente su profesionalidad esté más al servicio de la innovación que de la ortodoxia flamenca. El poeta, sensible al hecho, dibuja en palabras su presencia en el mundo del cante:\n\nNo es flamenco. Es Andalucía. Una Andalucía musical, entrañablemente nuestra y a la vez universal, como los músicos andaluces, los pintores andaluces, como los poetas andaluces... Camarón, Paco de Lucía y Tomatito, en esta ocasión, bajan al ruedo discográfico para, desde su profundo conocimiento, realizar toda clase de suertes musicales. No podíamos, ante este hecho, querer analizar el disco desde una perspectiva de estilos flamencos. Porque aquí la voz de Camarón se escapa en libertad y se hace música jonda, con el respaldo de Paco de Lucía y Tomatito, junto a otros músicos que acuden a esta llamada del sur. parece ser el destino de quien hizo del dolor el continuo escenario de la brasa en una geografía hostil a la experiencia, al reconocimiento de la memorable expansión de tu desgarro más allá del abrazo domesticado, en el exacto lugar donde termine su [presencia.\n\nF. Chica\n\nRetomando la escucha del disco, anotar, eso sí, varias precisiones: En principio parece un intento innovador, no el primero, y sin contenido homogéneo. No hay un trabajo definido —Camarón lo puede hacer—, equilibrado, significativo. No hay historia concreta. Conforman el L.P. varios cortes, bien construidos musicalmente, pero que, en conjunto, caen en la monotonía. No hay línea creciente a la que toda obra debe aspirar. No obstante, significar que de vez en vez, Camarón nos sorprende con el quejó-compás que un determinado estilo tiene en su argumento melódico y rítmico. Es como un mirar a la esencialidad flamenca de su tierra.\n\nNos gustaría que Camarón, al grabar, dejara constancia de su saber flamenco o, partiendo de esos esquemas, abordara unos rigurosos intentos de recreaciones verdaderamente suyas, enriquecedoras del patrimonio cultural andaluz. Un artista grande como él, necesariamente, tiene la obligación de huir de lo fácil para someterse a la disciplina de la más completa creación.\n\nREFERENCIA: Hispavox 130-127\n\nNos llega ahora en reedición, el homenaje discográfico que don Antonio Chacón recibiera de Enrique Morente. Malagueñas —en amplio concepto de matices y estilos—, siguiriyas, granánas, cartageneras, peteneras y tonás, constituyen y avalan la gran capacidad artística y comunicativa del cantaor granadino que supo recoger, en su día, los conocimientos chaconianos que hicieran suyos por transmisión oral, Pepe el de la Matrona, Bernardo el de los Lobitos y Aurelio Sellés.\n\nCon la guitarra de Pepe Habichuela, la voz, transparente y musical, registra y ahonda en ecos de desnuda brillantez. Aquí, el lamento es dolor lacerante. El cantaor, con exacto sentido de lo que dice, perfila resonancias estilísticas difíciles de encontrar en otras voces. Enrique Morente sabe perfectamente dosificar los tonos para buscar y llevar al aficionado sensaciones nuevas dentro de ecos clásicos y entroncados en la personalidad del maestro homenajeado. Morente abre la voz en cada salida para, en un momento determinado, quebrar el sonido buscando la natural belleza de nuestro cante. El ¡ay! de Morente arranca desde aquella raíz del grito que, en cantaores como Chacón, se hiciera cauce melódico. La armonía se hace perfecta en el desorden de los sueños, Late la rosa bajo la piel, hiriendo de rojo la garganta. Huye, aterrado, el desorden en el perfecto filo de la noche y el alba. F Chica\n\nLa guitarra de Pepe Habichuela presiente la voz dolida y trata de acompañarla en el trance. Siempre lo consigue. No se puede pedir más. Arte en la guitarra para una voz-arte.\n\nDOSCANDIL\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 740,
    "article_char_count_full": 4571,
    "article_char_count_review": 4571,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-11-20-left-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Romero, «El Gallina», este gitano perpendicular, de aquellos años de edad, que peina el viento airado de la vida, contrabandista de ecos de más allá, especialista de la caña del saber que pesca del mundo el Arte de lo íntimo, este Rafael Romero tan nuestro y de Andújar, encerrado aquí, en el cuartito de tantas amistades y del Condestable, recibe hoy el choque de las manos y las voces acompañantes de su voz acompañada, en este noviembre de castañas de Galaroza, recibe hoy, repito, el impacto de este homenaje que no siempre la vida ofrece, que no siempre es merecido, pero merecido en él por tan cabal.\n\nAmigo Rafael, éste es el símbolo que te entrego y lectura:\n\n«La Peña Flamenca de Jaén a Rafael Romero. El reconocimiento más sincero a tu Arte y a tu persona».\n\nAmigo Rafael, persona\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nepito, el impacto de este homenaje que no siempre la vida ofrece, que no siempre es merecido, pero merecido en él por tan cabal. Amigo Rafael, éste es el símbolo que te entrego y lectura: «La Peña Flamenca de Jaén a Rafael Romero. El reconocimiento más sincero a tu Arte y a tu persona». Amigo Rafael, persona lo cursi que pueda resultar, pero, amigo Rafael, gracias por haber nacido. C on estas palabras ofrecía el home-naje el presidente de la Peña Flamenca de Jaén, Alfonso Fernández Malo, a la vez que le hacía entrega de una placa recordatoria. A este homenaje se sumaron las autoridades provinciales así como numerosas peñas de la provincia. También CANDIL se unió al acto, haciendo su director, Ramón Porras, una bella y breve biografía del homenajeado. Pasando a lo que fue el Festival Homenaje a Rafael Romero, diremos que resultó brillantísimo por la calidad de los cantaores, pero tuvo el handicap de la escasa asistencia de público. Ante un cartel de calidad, la afición jiennense demostró su apatía, mientras que la de la provi\n\n[ENDING CONTEXT]\n\ncon el nombre del que fuera gran estudioso del flamenco, ANTONIO MACHADO Y ALVAREZ (Demófilo).\n\nEl objeto de esta convocatoria será premiar un libro cuyo tema esté relacionado con el Arte Flamenco en cual-quiera de sus facetas y enfoques.\n\nLos originales habrán de remitirse antes del día 10 de marzo de 1985, a la Delegación Municipal de Cultura, Plaza del Potro, 10, Córdoba. 14.002.\n\nEste premio estará dotado con la cantidad de 400.000 pesetas más la publicación de la obra premiada.\n\nPara más información, los interesados deberán dirigirse a la ya citada Delegación Municipal de Cultura.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1443,
    "article_char_count_full": 8833,
    "article_char_count_review": 2658,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "1984-11-21-right-discograf-a-placas-del-archivo-y",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLlora el que tiene una pena Yo quiero llorar Me está chorando Ameia de mar se cieza A mi la verdad me la fama Estan en lucha constante Ef landano es traicionado Delante de la presencia Comprende la yo no prmedo Que un querer\n\nGuajira Fandango \" Blerias Fandango \" Folea Fandango \" Folea Tang Fandango \" Folea Tang Fandango \" Folea Tang Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \"\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mujer\"]\n\n\" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" Fondango \" F Quando eu frat忻ouzao empietn I também por el amor que la honra de una mujer Els les boca un landam grillo Esta noche no hay repíque Hasta el reloj de la Pludiencia Restarag neperas y rias Hasta las rias le tienen A las taides me agavo Dobre y sin calor de mache Hos de la quiso llevar Tras que si fuera candela Con catorca de tormento Cerca de los Fataones Hormonía la conciencia Fastaste un dinero Como me ha queis mi mate Com mi casón bien montao Solera cati Aunque mi mal papel me hizo En castigarme tan fuerte Los ojos del ella en los niños Lotaba muy amargamente Del hombre la inhilecencia Yo soy el índio más fuerte Yo teña salud y dinero Esta Hueba ofrquilosa Qué grande es esta pena mía Por consejo de mi amigo Al hombre que ella quería Con esas das tan grande id idem Rosa con tanto valor A la mare de mi alma Blanca como una paloma Contrabandista puntero For lo alto delدی تلگاربه agües. Él que don Antonio Chacón no ha El destino y su sentencia En el cristal de mi copa La mare de mi alma En la zierra una serrana ¿Qué van por medio de oliva? Con el tiempo de seco Junto su cara sobre la mía ¿Qué adarme tiene un navio El por qué tú me dejados De ser mujer desertada Aunque sea fin enemigo Deía un minero así Hago hombres en vez de letras Hueja del mar se c\n\n[ENDING CONTEXT]\n\nAttorpo de qué presumes Cazador no me lo matez Hasta las tieras le tienen Era una mujer decente To seas mujer de la vía Cima plana con arena Comparación puede tener For hoas partes donde mito Letque he suas que eras mía Estaba pensativo aquel hombre Los Doctores me aconsejan Yo me he echado por perituencia Quieto digas a mis amigos Cada vez lque siento tu nombre Solo quiero time a una montaña En mi capón bien montas Aunque me hido am mal, pase!\n\nidm\n\nCon mi aspecto y simpatía para los lectores de \"Candif\", deseñadole un nuevo año de 1985. Plegatorio de salud, felicidad y flamenguismo. Mergana\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía Placas del archivo Yerga",
    "periodical": "candil",
    "issue_id": "1984-11",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "21-23",
    "page_number": 21,
    "word_count": 1863,
    "article_char_count_full": 10106,
    "article_char_count_review": 3272,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mujer"
      }
    ]
  },
  {
    "article_id": "1985-01-3-right-editorial",
    "article_text_for_review": "Editorial\n\nBlas Infante\n\nE n 1985 se celebra el primer centenario del nacimiento de Blas Infante. No es éste el lugar adecuado para glosar la dimensión política del que, con estricta justicia, es llamado padre de la Patria andaluza. El fervor con que acomete las reivindicaciones de su hambreado pueblo, su pasión nacionalista, la audacia de sus exégesis, rezumantes de frescura, sobre la historiografía andaluza, merecen rigurosamente una valoración puntual que no puede ser atendida en este comentario editorial, sin incurrir en puro desatino. Ello, no obstante, si queremos dejar constancia de sus específicas aportaciones, aunque sólo sea sumariamente, la investigación del cante jondo. Blas Infante es el primer estudioso que pergeña una sistemática del Flamenco, como expresión vitalista y cultural de un pueblo, y, concretamente, quien, en primer lugar, contempla en lo «jondo» una vía de recuperación de la identidad andaluza. Tal análisis entraño todo un cúmulo de sugestivas secuelas que, al menos, como hipótesis de trabajo, la bibliografía posterior no ha querido ignorar.\n\nPueden ser discutibles sus interpretaciones sobre la base sociológica que sustentó el cante —campesinado andaluz desposeído de la tierra—, así como sobre el sentido hermético de lo jondo conectado a minorías marginales. Pudiera incluso argüirse que, en Blas Infante, lo «jondo» queda instrumentalizado. Pero ello no debe entenderse con un sentido peyorativo, por cuanto, en definitiva, nos conduce a la afirmación, por nosotros íntegramente asumida, de que el Flamenco no es sólo susceptible de una contemplación estética sino que hay que inviscerarlo en la historia de este Sur doliente.\n\nCon Blas Infante, pues, se produce la primera aproximación a una teoría política del cante.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 269,
    "article_char_count_full": 1767,
    "article_char_count_review": 1767,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-01-4-right-aproximaciones-a-la-c-rdoba-flam",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHay que subrayar, en la más reciente historiografía flamenca, el hecho de que las Instituciones hayan mostrado su público reconocimiento a la cultura jonda. Reconocimiento que es fruto, en general, de una más acentuada sensibilidad hacia las expresiones genuinas de los diferenciados pueblos que constituyen el Estado español.\n\nHa sido sintomática la deferencia de la más alta Magistratura del país, la Corona, hacia el malogrado maestro Antonio Mairena, cuando, pocos meses antes de su desaparición, el propio Rey Juan Carlos le entregaba la medalla de oro de Bellas Artes.\n\nPara quienes, desde nuestro modesto esfuerzo, hemos investigado toda la normativa represiva y opresora que se dictó en este país, desde los Reyes Católicos contra aquellas minorías que se supone eran depositarias de los\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"fiel\"]\n\nnuestro modesto esfuerzo, hemos investigado toda la normativa represiva y opresora que se dictó en este país, desde los Reyes Católicos contra aquellas minorías que se supone eran depositarias de los arcanos del cante, para quienes hemos leído con estupor pragmáticas, disposiciones, sancionadas por numerosos monarcas españoles, para quienes conozcan el escarnio y la persecución de que ha sido objeto parte de este pueblo por el solo hecho de ser fiel a su propia identidad cultural, tuvo que conmoverle la noticia de que un ilustre descendiente de aquel Carlos III hiciera entrega personal del galardón al entonces más conspicuo cantaor vivo, don Antonio Mairena. No es, sin embargo, la primera vez que la Institución de la Corona ha entrado en contacto con el flamenco, si bien de manera cualitativamente distinta a como lo hiciera el Rey Juan Carlos I. Existen diversos precedentes. Y dos de ellos, merecen nuestra atención, aparte del supuesto que constituye el motivo principal de estas notas: Juan Breva y Antonio Chacón. El cantaor de Vélez-Málaga, Antonio Ortega de nombre, y conocido por Juan Breva, cantó en el Palacio Real, en presencia de don Alfonso XII y doña María Cristina. Desconocemos la fecha exacta, pero nos arriesgamos a inferir que tal acontecimiento debió producirse hacia 1884 ó 1885, fecha en las que Juan Breve gozaba de enorme popularidad en Madrid. Según Julián Pemartín, llegó a actuar en tres locales distintos al mismo tiempo, el teatro del Príncipe, el café del Barquillo y el del Imparcial. Alfonso XII había tenido otros acercamientos al flamenco, cuando sólo era Príncipe de Asturias. Como más adelante destacaremos, pormenorizadamente, en el viaje que hiciera por las ocho provincias andaluzas, con la entonces Reina Isabel II, tuvo ocasión de presenciar, en Córdoba, un singular espectáculo flamenco, tal vez el primero del que tengamos noticia haya sido organizado en honor de un monarca, y Juan Breva que, según José Carlos de Luna, hizo llorar al propio Julián Gayarre, escuchándole, debió de atraer la atención de Alfonso XII, sensibilizado hacia el cante y el baile como consecuencia de lejanas experiencias infantiles. El segundo precedente se refiere a don Antonio Chacón y en fechas más próximas, concretamente, según noticia de Yerga Lancharro, el 12 de junio de 1914. Don Antonio Chacón canta ante periodistas y séquito del Rey de Italia, en Palacio, y al día siguiente repite actuación, esta vez para deleitar a los reyes y sus familiares. Una vez más, fue el enorme prestigio de un artista flamenco el que determinó la llamada del Rey. Ignoramos la afición de este monarca, en 1914, y de las personas próximas a él. Todavía el general Primo de Rivera, jerezano de nacimiento y buen degustador del flamenco, no ha asumido la presidencia del Directorio Militar. Y sería poco riguroso presumir en el monarca un interés por lo jondo que no sabemos si tuvo. Lo que, en cualquier caso, par\n\n[ENDING CONTEXT]\n\nrio, y le vereis morijerado, industrioso, trabajador, buen padre, buen hijo, recatada hija ó virtuosa esposa. Porque también para ellos han alcanzado los beneficios de la civilización, y merecen el que se borre el negro estigma que en sus semblantes habian estampado nuestros padres. Ya son alguna cosa sobre el siervo que solo inspiraba desprecio y lástima; ahora se ha querido obsequiar á la pesona de más elevada gerarquía dentro de la nacion, y el zingaro contribuye por su parte á que el general deseo se realice, porque tambien él es andaluz y está interesado en el buen nombre de su pátria».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aproximaciones a la Córdoba flamenca de 1862",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 2655,
    "article_char_count_full": 16472,
    "article_char_count_review": 4556,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "fiel"
      }
    ]
  }
]
```
