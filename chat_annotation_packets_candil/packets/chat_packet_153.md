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
    "article_id": "1987-05-12-left-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "Aunque no quepa en el papel\n\n«Yo tenía mu güena estrella»\n\nEscritos de memoria, recogidos y ordenados por José Luis Ortiz Nuevo\n\nJosé Luis Buendía López\n\nDe nuevo una incursión de Ortiz Nuevo en la memoria de los viejos flamencos. Siguiendo la labor iniciada con los testimonios de Matrona, Pericón, Borrico de Jerez y Enrique el Cojo (ahí es nada), este incansable avivador de las vivencias jondas, realiza una labor que yo me atrevería a calificar de modélica, al montar una sencillísima puesta en escena de aquellos inquietantes avatares vitales y artísticos de sus personajes. Tan sólo unas sugerencias musitadas en la intimidad de los viejos flamencos, un hábil reconducir la conversación, y aquellos, como un torrente, se lanzan a la bellísima aventura de contarnos cómo vivían y sentían su arte, qué circunstancias influían en sus comportamientos jondos, cuáles eran las dificultades de todo tipo que empañaban el normal desarrollo de éstos. etc. Esta labor, sencilla sólo en apariencia, merece una alabanza sin límites, porque detrás de las vivencias e inquietudes manifestadas por los protagonistas, lo que de verdad se adivina es el telón de fondo en el que se proyecta la cara oculta del flamenco.\n\nEn el caso de Tía Anica asistimos perplejos a esa torrencial explosión de afirmaciones, negaciones, aplausos o descalificaciones. Nuestra añeja jerezana, que un día explicara el sabor de la sangre en el cante verdadero, es una institución en su barrio de Santiago. Allí, en el ambiente tórrido del verano andaluz, va a ir desgranan-\n\ndo el rosario de gracias y desgracias que le han ido acaeciendo a lo largo de su vida, ya casi centenaria. Y podemos asegurar que en el cómputo final pesan más las satisfacciones que los desabrimientos. Lo más hermoso de este libro es el contemplar a esta vieja jovencísi-ma gozar de una vitalidad sorprendente, capaz de borrar de golpe la pesadilla de los días pasados (necesidades acuciantes, precariedades económicas, etc.) para tornar en cañas las lanzas más aguzadas: habla con orgullo de sus diez hijos nacidos, de su fortaleza frente a una viudez con semejante carga, de sus primeros escarceos en las fiestas, en las reuniones íntimas. Esta primera parte del libro encierra una filosofía popular que emociona; ese mirar para atrás sin ira de la Periñaca se compendia en una serie de frases que sorprenden por su sencillez y por su honrada jondura, cual si de una copla se tratara: «Dios me ha dao mucha salú», «yo tenía mu güena estrella». Así de simple y así de claro. Y para confirmar esos tonos positivos del discurrir de su memoria, Anica realiza una incursión en las viejas raíces que aún la mantienen en pie: las reuniones familiares, el cante de la madrugá en cuartos iniciáticos, y, sobre todo, esa bellísima descripción de las bodas gitanas que obligan a pensar en la enorme grandeza de tanto primitivismo. Bautizos o Nochebuenas de ámbito familiar en los que la alegría está signada por la aparición del cante jondo, y un sinfín de acícates que nos llegan al alma\n\ny nos conmueven porque están expresados con la fuerza terrible de la verdad. Esta segunda parte es algo así como la justificación del por qué de tanta alegría vital en medio de la pobreza o la ignorancia.\n\nLa tercera y última parte de estas memorias constituyen un homenaje a los maestros, a los amigos; aquí los recuerdos navegan por los afluentes de ese magno río que es el cante de la protagonista: alude al arte de Tío José de la Paula, Joaniquín o Manuel Torre, y hasta a los momentos inolvidables en los que el maestro Antonio Mairena bajaba a Jerez a beber, junto a la delicia de sus vinos, las más puras esencias del cante de la tierra. De esta forma se compone una galería de retratos íntimos, adobada con el lirismo que solamente la sencillez popular es capaz de lograr; así, dice de Tío José que era «mu bonito», o evoca a Rafael el Carabinero en sus noches de cante con estampas sacadas de un manual de arte: «Cantaba mu bien, la noche se lo traía y el día se lo llevaba».\n\nEl libro, transcrito no en un correcto castellano, sino en una difícil y pienso que bien resuelta escritura fonética, termina con un álbum de fotos de la época y un repertorio lírico de letras jondas del repertorio de Tía Anica.\n\nCreemos que aquí no sobra ni falta nada. Estamos ante un monumento vivo, fresco, que nos muestra como ningún otro, la entraña abierta de ese pueblo jondo que todos invocamos, pero que resulta tan difícil de desentrañar.",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1987-05",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 766,
    "article_char_count_full": 4461,
    "article_char_count_review": 4461,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-05-12-right-ullos-uos-protagonistas-dicen-lo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nacía tiempo que nuestra redac-\n\nción evidenciaba un interés inusitado por introducirse en el vivo túnel del tiempo de esta familia flamenca, los Pinini, oriunda de Le-\n\nVanuel Martín Martín\n\nbrija y asentada en Utrera y con muchas páginas gloriosas en su haber. Una familia que acumula gestas indefinibles por cuanto posee la magia de la música, los secretos de la danza o la musa laboriosa y contrastada de una bajañi eternamente gitana. Ellos, primorosas ramas del frondoso árbol de Pinini, conforman una casta próvida y fascinadora, una generación escalonada y prolija que, por su dispersa ramificación, complicaban esta entrevista.\n\nTuvieron que ser los hombres de la junta directiva de la Peña Flamenca de Jaén los que, en base a su desmesurada afición, posibilitaran este hermoso encuentro\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicación\"]\n\nnada y prolija que, por su dispersa ramificación, complicaban esta entrevista. Tuvieron que ser los hombres de la junta directiva de la Peña Flamenca de Jaén los que, en base a su desmesurada afición, posibilitaran este hermoso encuentro que, en honor a la verdad, mereció la pena. Ejercieron de notarios, como testigos de excepción, Rosario López, cantaora de buena ley; Ramón Porras y Pedro Sánchez, batalladores incansables y «culpables» de esta publicación. Estamos en los albores del mes de marzo. Frente a nosotros un semicírculo humano heterogéneo en edad, de diversas tipología somática, pero todos encadenados por un nexo diferenciador: son Pinini por los cuatro costados. Justo es reseñar que no están todos los que son, pero sí son todos los que están. Queremos infiltranos en sus vidas, invadir sus recuerdos, recuperar de sus memorias tiempos pretéritos, arrancar de sus anales familiares un cachito importantí-simo de la historia de un flamentes son de Cái?», y él siempre decía que su cante era creao por él, que los de Cái eran otro estilo... Después, mis hermanas se fueron casando y seguían haciendo los cantes de mi padre. Mi hermana Fernanda se casó en Lebrija. M.M.M.: Te refieres a Fernanda la Vieja, la madre de Bastián Bacán. co sempiterno y pr\n\n[ENDING CONTEXT]\n\nacontecimientos; aún quedan algunos meses, tiempo suficiente para que el Ayuntamiento reconsidere su postura absurda, beba grandes tazones de cordura flamenca y tenga en cuenta la conveniencia o no de que la Caracolá pierda su sabor de más de cuatro lustros. Confiemos en que se imponga el sentido común porque a la postre sólo habrá un triunfador, la ciudad de Lebrija, un pueblo sabio que en materia jonda no puede cometer el atropello de prescindir de la profunda capacidad expresiva de sus hijos ilustres... Y quien no ama a sus hijos, ¿cómo demonios va a querer a su madre?\n\nCANDIL $ * $ 29\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen: Los Pinini, casi al completo",
    "periodical": "candil",
    "issue_id": "1987-05",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "12-16",
    "page_number": 12,
    "word_count": 3971,
    "article_char_count_full": 22289,
    "article_char_count_review": 2894,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicación"
      }
    ]
  },
  {
    "article_id": "1987-05-16-right-pepe-aznalc-llar",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA proponerme sa- car adelante un par de folios o tres referente a Pepe Aznalcólar como cantaor y persona, me sugiere el recuerdo la metamorfosis de su voz y su cualidad artística para resolver —como tantos otros— su desastre fonético. Todo aquel que sepa conjugar el sentimiento con el conocimiento flamenco, llegará a ahondar hasta profundidades incomprensibles e inaceptables por parte de todos aquellos que sin dejar de ser adeptos al flamenco tampoco dejan de ser neófitos. Traspasar el misterio supone distinguir entre el virtuosismo espectacular y el sentimiento puro, porque una cosa es la forma y otra el tuétano de formas. ¿Qué importa que la agilidad dactilar del guitarrista convierta el toque en un torbellino de notas si lo que verdaderamente importa a los\n\nLuis\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nl sentimiento puro, porque una cosa es la forma y otra el tuétano de formas. ¿Qué importa que la agilidad dactilar del guitarrista convierta el toque en un torbellino de notas si lo que verdaderamente importa a los Luis Caballero especializadas en ese instrumento físico que es la garganta. El tan vinculado a las voces cantaoras Dr. Muñoz Cariñanos, no quiso que faltásemos los flamencos a la reunión que él presidía. Se generalizó en cuanto a la voz desde el ángulo médico al artístico. Natu- que saben de la guitarra flamenca es la risa y el llanto reposado de su alma y nunca, jamás, el regreso al mecanismo del laud y la púa? ¿Cuantas veces nos aburren —por su preponderancia gimnástica— bailaoras y bailaores de éxitos massivos? ¿Y la voz, qué nos dice la Dueño de un caudal sonoro admirablemente portentoso. Voz «laina» pero al mismo tiempo pastosa y vibrante de sentimiento voz, aún siendo maravillosa, del cantaor carente de rasgo dramático o delicadeza espiritual? Tan sólo hace algún tiempo intervine como cantaor (quién lo iba a decir) en una mesa redonda convocada por hombres de ciencias ralmente yo expuse los razonamientos que corresponden a nuestra manera de expresar el fenómeno espiritual que conforma el cante jondo o flamenco, y en el que la voz de ningún modo adquiere prioridad fundamental. Al citar ejemplos prácticamente personales aparece como uno de los más típicos, el del cantaor Pepe Aznalcóllar. Quien como yo, por parentesco y paisanaje, lo conoció desde siempre, puede enjuiciarlo sin trabas ni lagunas. Los aficionados jóvenes que han llegado a escucharlo ya apenas con un susurro de voz, les costará creer que fue dueño de un caudal sonoro admirablemente portentoso. Voz «laina» pero al mismo tiempo pastosa, lenta y vibrante de sentimiento. Sin la veloci\n\n[ENDING CONTEXT]\n\nChiquetete, el padre de la excelente tonadillera Isabel Pantoja. A Chiquetete le tocó la triste tarea de prestarle humanamente los primeros auxilios.\n\nTrasladado a La Paz y agradeciendo el ánimo que le daba una monjita, todavía sacó fuerzas para —con la voz ya casi muerta— cantarles este fandango: «Las hermanas de la Cruz / las santas que hay en la tierra. / Tienen una gran virtud / y en su corazón encierra / lo que predicó Jesús».\n\nLuego siguió llamando a su hija. Cuando ésta llegó todavía le brillaba una lágrima en el rostro. Murió con la humildad que siempre había vivido y cantado.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantaores conocidos, compañeros y amigos: Pepe Aznalcóllar",
    "periodical": "candil",
    "issue_id": "1987-05",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1015,
    "article_char_count_full": 6044,
    "article_char_count_review": 3384,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      }
    ]
  },
  {
    "article_id": "1987-05-17-right-in-memoriam-francisco-valls-tori",
    "article_text_for_review": "Hace unos días me fue comunicada la triste noticia del óbito ocurrido en la persona de «Paquito el Americano», como cariñosamente se le decía dentro del ámbito flamenco.\n\nM. Yerga\n\nPaco nació en la ciudad de Buenos Aires, el día 19 de agosto de 1907. Fue un cantaor de los llamados preciosistas en la década de los treinta. Estuvo en posesión de una voz limpia, muy apta para la ejecución de los mal llamados cantes de «ida y vuelta» (colombianas, milongas, guajiras y vidalitas). Su fandango personal gustó sobremanera a una gran mayoría de aficionados, como lo demostraba esa abrumadora afluencia de público en todas sus actuaciones.\n\nNos ha legado una bien medida malagueña chaconiana: «Allí fueron mis quebrantos». También algunas soleares, bulerías y dos preciosos pregones.\n\nSu fallecimiento, según manifestación de uno de los presentes, se produjo en la tarde del miércoles, día 28 de enero pasado, en el Hospital Provincial «Francisco Franco», de Madrid. Pocas horas antes se hallaba en una peña establecida en la Plaza de Tirso de Molina, y nadie se percató de que «sentado en una silla se iba muriendo, como adormecido, sin darse cuenta».\n\nEstoy seguro de que pocos aficionados sexagenarios sabían de su existencia.\n\nYo, por motivaciones especiales, sentí siempre una gran simpatía hacia el finado y quizá por ello me llevé casi cinco años investigando para dar con su paradero, sin conseguirlo. Hasta que un buen día vino a visitarme un sobrino nieto de Manuel Blanco «El Canario», esposo de una joven de Fuente de Cantos, quien informado de mi afición por el arte flamenco y por averiguar la vida de los cantaores que nos dejaron para siempre, me visitó —como digo antes— para pedirme los cantes de su tío, a lo que accedí muy gustoso. Al día siguiente volvió a visitarme, esta vez para entregarme dos fotografías, una de su tío y otra de «El Americano», ambas hechas en Buenos Aires, el día 13 de marzo de 1930. Y fue este joven quien ¡por fin!, me dijo que «El Americano» vivía unas temporadas en su piso de Madrid, y otras con su hija Dolores, en el pueblo de Recas, de la provincia de Toledo.\n\nDe inmediato me puse en contacto telefónico con\n\nun funcionario del Ayuntamiento de dicho pueblo, para que me informase si el viejo cantaor se encontraba en el pueblo o en Madrid. Al decirme que en Recas, me dispuse a visitarle, pero el funcionario me dijo: «no debería usted desplazarse a ésta, porque estoy seguro que el señor Paco no le recibirá». Por lo visto estaba muy dolido de la afición española, porque le habíamos echado al rincón del olvido. Esto es lamentablemente verdad, aunque yo puedo jurar ante Dios que le recordaba y escuchaba sus cantes con frecuencia.\n\n¡Pobre Paco! Cómo ignoraba que no solamente él había sido olvidado, sino que lo habían sido todos los cantaores de su época y anteriores. Así es la vida.\n\n¿Cuánto tiempo estará en nuestra memoria el gran don Antonio Mairena? Yo aseguro que muy poco. Y quien viva me dará la razón.\n\nMi admirado y llorado «Paco el Americano». Que descanses en la Paz del Señor.",
    "title": "In memoriam Francisco Valls Toribio «El americano» (1907-1987)",
    "periodical": "candil",
    "issue_id": "1987-05",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 526,
    "article_char_count_full": 3044,
    "article_char_count_review": 3044,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-05-18-left-buz-n-y-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«Inconmiserativamente»\n\nSr. director de la Revista CANDIL:\n\nSu última revista publicada, en el apartado de «La Picota», e identificado con «Ventolera»; critican mi libro de flamenco de una forma casi vil, y me atrevería a decir que se pone el flamenco en un sitio bastante bajo.\n\nQuisiera empezar diciendo que, para escribir en una prodigiosa revista como CANDIL, no se pueden emplear palabras como Inconmiserativamente, porque si el flamenco nació del pueblo llano, al pueblo no se le puede escribir palabras raras y sofisticadas. Para seguir centrados en el tema que nos preocupa cual es el del flamenco, mire usted: De sobra sabemos los que nos gusta este arte tanto, que don Antonio Mairena debe de escribirse con mayúscula; porque don Antonio es la última fuente del saber.\n\nQuiero decirle al\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionado\"]\n\nndan. Así que de descuidado por mi parte, nada de nada. No me voy a extender mucho, aunque este tema me gustaría recrearlo con toda su salsa y sinceramente creo que me gustaría saber quién escribió esta «Picota» con el seudónimo de «Ventolera», y tener un cambio de impresiones sin ánimo de disgustos ni distorsiones que pudieran perjudicar el flamenco, y estoy seguro que tendría que tener un vasto conocimiento del cante para igualar a un modesto aficionado como yo, capaz de dar lo que fuera por nuestro arte andaluz. También quiero decir que Francisco Delgado «El Tato», al que menciona en su «Picota», me encanta- ría que lo escuchara en «el cuarto» y contemplaría su largueza cantaora y su especial sensibilidad cual su carácter tan dolorido últimamente y por eso tan expresivo y tan flamenco; para entonces opinar si es un cantaor importante o no. Como verá usted, sigo; porque hay cosas que obviamente tengo que aclarar. Efectivamente «La Serneta» no dio nombre a los cantes de Alcalá; pues, como el buen aficionado sabe, fue Joaquín «el de la Paula»; pero no es cuestión de de\n\n[ENDING CONTEXT]\n\nBernardo Palla- rés.\n\nSecretario: Jesús Casado. Vicesecretario: Manuel Aguirre. Tesorera: Catalina León.\n\nVocales: Carmen Díaz, Manuel León, Eulalia Pablo.\n\nLas actividades de la Tertulia se abrieron con un recital de José de la Tomasa, acompañado a la guitarra por Pedro Bacán, el día 27 de marzo, y continuaron con un recital de saetas el 9 de abril, a cargo de Encarna Franco, Antonio Castro, Juan Manuel Castro, Antonio el Manta y Quiqui de Castilblanco.\n\nA falta de locales propios, la Tertulia está establecida en: Escuela Universitaria de Magisterio, Avda. Ciudad Jardín, s/n.º 41005 Sevilla.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Buzón y Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1987-05",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 1485,
    "article_char_count_full": 8925,
    "article_char_count_review": 2716,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionado"
      }
    ]
  }
]
```
