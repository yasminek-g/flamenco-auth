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
    "article_id": "1987-07-13-left-ellos-los-protagonistas-dicen-pa",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n¿D e dónde te viene la afición? ¿Cuáles\n\nson tus raíces flamencas?\n\n—Mís raíces son que en la familia de mi padre ha cantao todo el mundo. Mi abuela cantaba estupendamente, mi padre también, mis tíos... Y yo creo que estoy cantando desde que tenía ocho o diez años. Estaba guardando cochinos con ocho años y creo que cantaba corriendo detrás de los cochinos.\n\n—¿Cómo se produce tu pase al profesionalismo?\n\n—Se produce cuando mi hermano y yo grabamos el primer disco. Entonces éramos los Hermanos Toronto. Tengo dieciocho discos hechos con mi hermano y siete u ocho hechos en solitario.\n\n—¿En tu familia ha habido algún artista con renombre?\n\n—No. Eran aficionaos. Mi padre cantaba muy bien. Era un hombre de campo, no fue nunca profesional. Era imitador, le gustaba las cosas de Marchena, del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombres\"]\n\na Madrid... por siguiriγas. Me gusta hacer la salía y luego arrancarme por fandangos. Cada fandango lleva un son, unos fandangos hay que cantarlos por arriba y otros por medio y los del medio son los que canto yo del Alosno, donde yo naci. Como estos fandangos hay que cantarlos por medio y la siguiriγa también, pues sale el fandango. —¿Qué piensa Paco Toronjo de cantaores de su tierra como José Rebollo, Antonio Rengel, Paco Isidro...? —Fueron hombres que no fueron, no fueron... Paco Isidro era un hombre muy mayor, era taxista. No lo hacía mal. Y Rengel era un hombre que no duró mucho. Rebollo creo que murió joven, yo no lo conocí, a Rengel sí y con Paco Isidro he estao muchas veces. Ellos se defendían. ¿Sabes dónde fueron mucho a aprender?, a mi pueblo. Estuvieron mucho en el Alosno. No porque yo haya nacido en ese pueblo, pero la madre del fandango de Huelva es Alosno. —?Es verdad eso de que para cantar por Huelva hay que haber nacido en Huelva? —Yo creo que sí. Tú sabes que ha habió mucha gente que lo ha intentao y han fracasao casi toos. No han llegao a darle lo que hay que darle al fandango de Huelva. Un ejemplo lo tienes aquí en Jaén en ese gran amigo mío que es Juanito Valderrama que ha cantao muy bien por fandangos naturales, pero cuando se mete por Huelva ya no es igual. Yo he hecho giras con él. Es una gran persona, es un peazo de artista, pero no sé lo que tiene... Yo creo que sí porque... ¿Toca bien Paco de Lucía? Yo tengo tres discos hechos con Ramón, su hermano, toca fenomenalmente a compás..., pero le falta el aire de allí, el aire de Huelva, un aire que es de allí igual que las alegrías son de Cádiz, igual que la malagueña es de Málaga, igual que la granaína es de Granada. —Antes te has referido, en relación con tu entrada por siguiri-yas, a que la realizas así por hacer el fandango más flamenco. ¿Es que los fandangos no son tan flamencos? —El fandango sí. Pero tú sabes que el fandango de Huelva, antes de salir yo con mi hermano, no le llamaban fandango, le llamaban fandanguillo de Huelva y además se cantaban para bailar, eran folklóricos, que yo los canto también pa bailar, pero los canto libres, a mi forma, porque pa bailar es como una letanía. Pá bailar. —¿Por qué en estos últimos tiempos cantaores de renombre y mucha popularidad están incrementando la interpretación de los fandangos de Huelva y acordándose de la personalidad de José Rebollo? —Rebollo tenía un fandango que yo lo hago a mi manera, y hay un cantaor que no es de Huelva, que es un gran amigo mío, que es «El Cabrero», que después de no ser de Huelva no lo hace mal, pero amigo mío lo ha cogió too de aquí —se señala él—, de los discos, de las cosas... pero se defiende. —¿Se acuerdan los cantaores actuales a la hora de cantar por Huelva de cantaor\n\n[ENDING CONTEXT]\n\nPaco Toronjo de la costumbre de Vallejo, Fernanda y otros artistas de meter los fandangos a compás de soleá o bulerías por soleá?\n\n—La bulería por soleá es una cosa que han hecho, porque la bulería es bulería y la soléa es soleá. No está mal, han metió más ritmo a la soleá y está bien. Yo he escuchao a Pastora porque ella fue la primera que empezó a hacer eso, a Vallejo no le he escuchao nunca eso; Vallejo si tenía que cantar por soleá, cantaba por soleá, y si lo hacía por bulerías, pues cantaba bulerías. Yo no le escuché eso nunca a Vallejo.\n\nDoctor Arroyo, 12\n\nTeléfono 210058 - J A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas dicen: Paco Toronjo",
    "periodical": "candil",
    "issue_id": "1987-07",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 2596,
    "article_char_count_full": 14175,
    "article_char_count_review": 4392,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombres"
      }
    ]
  },
  {
    "article_id": "1987-07-15-right-algunos-datos-nuevos-para-la-his",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nArie Sneeuw\n\nonsultando fuentes documentales para un trabajo sobre la presencia del flamenco en Madrid (1), hemos encontrado unos documentos aún inéditos cuyo interés rebasa evidentemente el marco de dicho trabajo, extendiéndose a la historia y teoría del flamenco como tal. Se trata de dos textos periodísticos del año 1853, uno con carácter de noticia y el otro una extensa crónica musical, que aparecieron a raíz de un espectáculo de flamenco en el diario madrileño La España, y que reproduciremos y comentaremos brevemente aquí. Solamente uno de los textos apareció firmado, pero por el contenido es obvio que los dos son del mismo autor, Eduardo Velaz de Medrano, que era lo que hoy llamaríamos «crítico musical» del periódico. Su sección fija Revista musical aparecía más o menos cada mes y en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"reseñaba\"]\n\nel otro una extensa crónica musical, que aparecieron a raíz de un espectáculo de flamenco en el diario madrileño La España, y que reproduciremos y comentaremos brevemente aquí. Solamente uno de los textos apareció firmado, pero por el contenido es obvio que los dos son del mismo autor, Eduardo Velaz de Medrano, que era lo que hoy llamaríamos «crítico musical» del periódico. Su sección fija Revista musical aparecía más o menos cada mes y en ella reseñaba los principales acontecimientos musicales en los teatros madrileños de la época, entre los cuales incluyó, en la entrega de febrero del 1853, el mencionado espectáculo de flamenco. Los textos forman tal unidad que los reproduciremos aquí abajo como un todo, subdividido en I y II. El primero lo transcribiremos integralmente y del segundo únicamente la parte correspondiente a la función de flamenco, respetando la grafía de los originales. MUSICA FLAMENCA. No se trata de ningún compositor de la escuela de los Tinctor y Josquin Desprez: la música flamenca que motiva esta gacetilla, es la que en la tierra de María Zantízima se conoce con ese nombre. Sin perjuicio de hablar más detenidamente en nuestra próxima Revista musical, queremos decir hoy cuatro palabras acerca de la fiesta puramen- te nacional que se verificó antes de anoche en los salones de Vensano, calle del Baño. Los protagonistas fueron lo más escogido entre los flamencos que se hallan actualmente en Madrid; así es que los aficionados pudieron admirar tres escuelas diferentes a la vez. Ejecutaron con el más admirable y característico primor, todo el repertorio andaluz de playeras, cañas, jarabes, rondeñas, seguidillas afandangadas, etc., etc. También hubo algunas señoras, entre otras, una preciosa gitanilla de rumbo y singular salero, que amenizó la fiesta con su baile. La reunión se componía de más de cien personas de todas clases y condiciones; diputados, gobernadores de pr\n\n[ENDING CONTEXT]\n\ntan palpitante desde la aparición del manual de Molina y Mairena, nos parece poco constructiva ni saludable, sobre todo cuando se llega a plantear la presunta culpabilidad por algo de una etnia u otra. Si nos hemos detenido aquí especialmente en el dato de la actuación en Madrid de Juan de Dios, no es para culpar o disculpar a nadie, sino simplemente porque sobre este cantaor tenemos hasta ahora más información que sobre otros en cuanto al status de los primeros intérpretes, sea cual sea la etnia a que pertenezcan.\n\n(24) Ob. cit., pág. 57.\n\n(25) Ob. cit., págs. 21-27, 35-37 y 53-55 passim.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Algunos datos nuevos para la historia del flamenco",
    "periodical": "candil",
    "issue_id": "1987-07",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "15-19",
    "page_number": 15,
    "word_count": 5763,
    "article_char_count_full": 34899,
    "article_char_count_review": 3542,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "reseñaba"
      }
    ]
  },
  {
    "article_id": "1987-07-19-right-aunque-no-quepa-en-el-papel-le-f",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMiguel Alcalá\n\nTexto de Alfonso Eduardo Pérez Orozco\n\nPaco Vallecillo\n\nMiguel Alcalá, autodidacta, hispanófilo y gitanófilo, ha recogido en este hermoso libro una soberbia colección de dibujos a lápiz que reflejan, vivos y en movimiento casi, las imágenes de una larga serie de personajes del flamenco, especialmente de la época actual. El artista revela una insólita capacidad para representar el gesto, la actitud, el leve scherzo identificativo de algunos artistas que ha dejado aprisionados en su maravillosa visión reproductiva de la efigie: Farruco, la Negra y su hija Lole, Fernanda, Manuela Carrasco, Lebrijano, captados todos ellos en el transcurso de la última Bienal y tomados, por tanto, en lo que puede llamarse plena cocción, son pie\n\nzas sobresalientes de un museo flamenco hasta\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestra\"]\n\ntistas que ha dejado aprisionados en su maravillosa visión reproductiva de la efigie: Farruco, la Negra y su hija Lole, Fernanda, Manuela Carrasco, Lebrijano, captados todos ellos en el transcurso de la última Bienal y tomados, por tanto, en lo que puede llamarse plena cocción, son pie zas sobresalientes de un museo flamenco hasta ahora escasamente conocido. El rostro pétreo, como labrado a buril del inolvidable Tomás Torre, constituye una obra maestra. El libro, espléndidamente editado, se adorna con un texto de Alfonso E. Pérez Orozco, escrito primorosamente: un breve pero riguroso estudio del pueblo gitano y una segunda parte que también sabe a poco en la que afronta en cuatro páginas una bastante exacta definición del flamenco para presentarse finalmente a unos personajes esenciales en el Gotha del Cante, deteniéndose en Chacón, Caracol y Mairena especialmente. Alguna pequeña falta de exactitud (La Llave al Nitri que no fue de oro, y su entrega en Málaga está por demostrar. Mairena no alcanzó a escuchar a Tomás el Nitri...) no empecen en absoluto el muy válid\n\n[ENDING CONTEXT]\n\nejemplo, que no se debe hablar de Manuel Torre, sino de Manuel Torres, como aparecía en los programas de antaño) que le llevan a honrar y dignificar a figuras del cante ensombrecidas por opiniones particulares; tal es el caso de Silverio Franconetti, al que califica como «el mejor cantaor de todos los tiempos» y al que libera del sambetino que le colgaron en su día Ricardo Molina y Antonio Mairena de haber sido culpable de la pérdida de la pureza del cante como consecuencia del paso que este arte experimentó al salir de esa supuesta atmósfera íntima y ser exhibido públicamente en los cafés.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel: «Le Flamenco et les gitans», de Miguel Alcalá, y «Los cafés cantantes de Sevilla», de José Blas Vega",
    "periodical": "candil",
    "issue_id": "1987-07",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1259,
    "article_char_count_full": 7730,
    "article_char_count_review": 2701,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestra"
      }
    ]
  },
  {
    "article_id": "1987-07-21-right-iscografia-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNo hace muchas fechas solicitábamos desde estas páginas la necesidad de ver reeditadas auténticas reliquias de lo jondo, no sólo por su carácter orientador, pedagógico y dominante en la presente movida, sino porque el material que albergan —grabadas en décadas pretéritas—, permite una visión más completa del panorama estilístico y constituyen, por tanto, un venero imprescindible para la «moderna flamencología».\n\nHoy, pues, nos congratulamos de esta nueva puesta en escena. Una vez más, HISPAVOX, S. A.,\n\nManuel Martín Martín\n\nha desempolvado su considerable archivo sonoro y nos sitúa ante unos forjadores que siempre permanecerán fijos en nuestra memoria por cuanto musicaron los sótanos y galerías del edificio flamenco contemporáneo. Y esto, cuando menos, es para que la afición se muestre\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\nlos sótanos y galerías del edificio flamenco contemporáneo. Y esto, cuando menos, es para que la afición se muestre complacida, por más que nunca satisfecha. En efecto, siete han sido en esta ocasión los LP's que ha sacado al mercado del disco la acreditada empresa madrileña. Corresponden a originales de las tres décadas que nos preceden, todos con temas convenientemente seleccionados y de buena calidad sonora a excepción del que nosotros hemos escuchado del admirado Melchor de Marchena, donde el abuso de agudos y el extraño sonido metálico no permite el deleite de una música diferente por jonda y enduendada. De todas formas, hemos consultado con otros compañeros y el comentario, unánime por otra parte, es que les suena a coro de ángeles; luego damos fe de que el nuestro está defectuoso. Sea como fuere pasemos a detallar los argumentos que justifican nuestro encabezamiento. Canta Jerez. Ref. 530 / 4032331 Se trata, como acertadamente señala Pepe Marín, de «uno de los más valiosos documentos sonoros que puedan darse en el panorama cantar de todos los tiempos». La grabación original data de 1967 y de la mano de Paco Cepero y Paco de Antequera nos sumergimos en el inacabable pozo de enjundia jerezana. Allí se entrelazan voces que han sido y que son piezas fundamentales e indispensables para un acercamiento a las inveteradas esencias de Jerez de la Frontera: Manuel Soto «Sordera» (fandangos del Gloria y siguiriyas), Terremoto (bulerías por soleá y siguiriyas), El Diamante Negro (siguiriyas y martinete), Tío Borrico (soleares), Sernita de Jerez (cabales atribuidas al Loco Mateo, aunque todos los indicios apuntan al Tío Cabeza), Romerito (bulerías de Nochebuena y soleá de Juanini, mal etiquetadas en otra edición como soleá de Juaniquín), y el broche de oro para un hombre genial, Parrilla el Viejo, en una maravillosa e incomparable fiesta por bulerías donde todos confirman el lugar de gestación de este cante tan gitano. Maestros del Cante: El Chocolate. Ref. 530 / 4032341 Estamos a catorce años de esta docena de soníos negros que nos oferta Antonio Núñez Montoya, un jerezano afincado en Sevilla desde su más tierna infancia, con muchos lustros de andadura flamenca —hecho este que le impulsará a conseguir el II Giraldillo del Cante—, y con un metal de voz que cautiva y arrastra hasta lo indefinible. A mi juicio, tarantos,\n\n[ENDING CONTEXT]\n\na un tiempo. Ello no ha supuesto obstáculo alguno toda vez que las grabaciones corresponden a tres «singles» que Hispavox produjo en 1959 y que, con el título de «Guitarra Gitana» causó una grata y duradera impresión en quien esto escribe, a excepción de los tientos y las peteneras que no fueron incluidos entonces. Sirviéndose de una segunda guitarra, la de A. Duque, la genialidad de Melchor queda plasmada —además de los ya reseñados— en bulerías, alegrías, tanguillos de Cádiz, malagueñas, sevillanas, medias granaínas, tarantas, fandangos de Huelva, serranas, zapateado, siguirias y soleares.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1987-07",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1032,
    "article_char_count_full": 6618,
    "article_char_count_review": 3978,
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
    "article_id": "1987-07-22-right-noticiario-flamenco",
    "article_text_for_review": "Maestros del Cante: Pepe Pinto Ref. 530 / 4032591\n\n¡Y qué verdad más contundente! Pepe Pinto, Maestro del Cante... A las siete horas de un seis de octubre de 1969, una cirrosis hepática acababa con toda una vida de entrega y amor a este arte, con la generosidad hecha persona, con un aficionado de pies a cabeza que goza de mi más profunda admiración y respeto y que supo venerar con fidelidad absoluta a la cantaora más larga de todos los tiempos, a la inigualable Niña de los Peines. Algunas de estas impresiones acusan el paso de los años, otras, el gusto imperante de un tiempo pasado, la mayoría, el sabor de un cantaor por derecho que fue víctima de la época que le tocó vivir. En cualquier caso, la guitarra de Melchor de Marchena pone al descubierto el buen hacer de una reliquia que, junto con el resto ya reseñado, vale su peso en oro, a pesar de algunas concesiones carentes de significación para ser consideradas como primordiales. Los cantes que se recogen quedan así ordenados: soleares, fandangos personales, siguiriyas, tarantas, fandangos de Huelva y tonada navideña, ésta última con la guitarra de Manolo Sanlúcar.\n\nE 1 Seminario de Estudios Flamencos, que tiene su sede en la Escuela Universitaria de Magisterio de Sevilla, durante los pasados días 27, 28 y 29 de mayo, unas JORNADAS SOBRE LA INVESTIGACION EN EL FLAMENCO, que contaron con el patrocinio de la Dirección General de Renovación Pedagógica de la Consejería de Educación de la Junta de Andalucía. Como es habitual en la línea seguida por este seminario desde su creación, se trataba con ello de incidir en el acercamiento del flamenco a los enseñantes de cualquier nivel, con el objetivo de abrir esta rama del arte al ámbito de la escuela. En esta pretensión, varias fueron las conferencias y charlas que se desarrollaron durante las citadas JORNADAS.\n\nAbrió el ciclo el profesor de Literatura Antonio Carrillo Alonso con una conferencia que, con el título de «La Copla y el Cante en Gustavo Adolfo Bécquer», aportó datos acerca de la influencia de las coplas flamencas en la poesía y prosa becqueriana.\n\nA continuación, José Luis Ortiz Nuevo nos acercó al mundo íntimo de figuras tan destacadas como Pepe de la Matrona, Pericón de Cádiz o Tía Anica la Periñaca. Con su verbo fácil y ameno nos introdujo en el entorno y las vivencias de esos personajes, con los que sostuvo largas conversaciones que sirvieron de base a la elaboración de distintos libros, en los que el propio lenguaje del protagonista nos conduce a través de su figura y su obra.\n\nEl jueves 28 tuvo lugar la segunda JORNADA, interesante por partida doble. De un lado, porque nos ofreció la oportunidad de conocer en primicia, antes de su futura publicación, aspectos destacados de la historia y desarrollo de los Cantes de Levante que José Luis Navarro incluye en un profundo y documentado trabajo. De tro, la voz mágica y grandiosa de Calixto Sánchez nos adentró, a modo de ilustración sonora, en una hermosa gama de cantes, tan bellos como inusuales, cuya sola mención nominal puede hacer formar al aficionado una idea sobre el valor que su conocimiento aporta: cantes de El Morato, cartageneras clásicas, tarantas cartageneras, fandangos de Cartagena, tarantas, tarantos, murcianas, levanticas, mineras...\n\nA través de él y de su voz portentosa y sabia, revivimos los estilos de El Morato, El Rojo el Alpargatero, La Peñaranda, Chacón, Manuel Torre, Cayetano Muriel, Escacena, El Cojo de Málaga, Vallejo, Pastora Pavón, y tantos otros que incorporaron a su quehacer artístico la hondura de los Cantes de Levante.\n\nLas JORNADAS culminaron con la intervención de José Blas Vega, destacado investigador, autor de numerosos libros, que desglosó en su charla las líneas fundamentales que deben presidir una investigación en el terreno del flamenco. Fue asimismo revelando su recorrido histórico por la Flamencología, siendo destacable, por lo inédita, sus documentadas afirmaciones sobre las actividades cantaoras de Serafín Estébanez Calderón.\n\nEn resumen, queremos destacar la importancia que estas JORNA-DAS han tenido para nosotros.",
    "title": "Noticiario Flamenco",
    "periodical": "candil",
    "issue_id": "1987-07",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 673,
    "article_char_count_full": 4084,
    "article_char_count_review": 4084,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
