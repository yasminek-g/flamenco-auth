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
    "article_id": "1990-01-6-right-a-pepe-polluelas-ram-n-porras",
    "article_text_for_review": "A PEPE POLLUELAS Ramón Porras\n\nlovía entonces más; o era más lóbrego el invierno, más dramático el viento, más inmensa la noche, noche de sabanones en las manos y sienes hambreadas. Daba miedo esta ciudad —década de los cincuenta—; apagones de luz, los partes de Radio Nacional, el chuzo de los serenos golpeando la calle como un pulso maldito, la campana de ánimas, el ladrido interminable de perros callejeros... Entonces no había yonki, ni navajeros, ni déstellos de neón... El miedo no venía de fuera, sino de dentro, casi transmundano, de dentro de uno mismo, del oscuro sitio de la opresión, del recuerdo reciente de la guerra y sus nocturnos ajusticiamientos, de la indigencia, de espeluznantes signos emblemáticos de la «Victoria», «queremos a España porque no nos gusta». Tristeza inconmensurable de la noche, tu noche, Pepe Polluelas.\n\nCuando en Córdoba se convocaba a los maestros aún vivos —Concurso Nacional de Cante—, cuando las viejas voces de lo jondo (Antonio, Aurelio, Juan Talegas) junto a las más jóvenes (Fernanda, Fosforito...) comenzaban a conseguir pública estimación, cuando toda una pujante generación de escritores andaluces paraba su admiración en el descarnado grito de la siguiriya, cuando en definitiva se recuperaban las esencias del flamenco frente a desinformados detractores y atenorados cupleteros, en esta ciudad persistía el silencio, los torpes desconocimientos, la indignidad del flamenco en los prostíbulos. Señoritos altaneros, azules autoridades, puteros impenitentes. Y tú —La Sole, La Fidela, el Manco...— como oficiante de esa oscura ceremonia de sexo y hueras flamenquerías:\n\n«-Niña, que entren los cantaores». Severiano, Marchenilla, Simón y tú, Pepe Pollyelas. Cante deleznable para quien sólo podía digerir lo deleznable. Pero con los primeros albores coronando San Juan, de regreso ya a tu casa, frío y frustración mordiéndote en los huesos, qué tristísima soleá no se te reventaría dentro:\n\nQué tristeza la mía hasta en el andar que los pasitos que palante daba se me van pa atrás.\n\nEn Cádiz, en Jerez, en Triana, dentro de un clima hermético y tribal, el flamenco pudo guardarse en su más prística jondura. En bodas, en bautizos se cantaba para uno mismo —cante todavía como expresión existencial— o para el hermano, el padre o el amigo. Pero tú, Pepe Polnuelas, en esta tierra inhóspita y desinformada sólo podías hablarle a Undebé, cada madrugada del Viernes Santo: Padre Jesús entrañable, el «Agüelo». Quién podía entender esa soleá que como dulce mordedura, te hería dentro:\n\nA quién le contaré yo las ducas que estoy pasando, se lo contaré a la tierra cuando me estén enterrando.\n\nEl haz y el envés de una misma moneda: flamenco en la Universidad, escuchado con devoción por quienes años más tarde van a constituir la generación que proclame el extraordinario sentido cultural, histórico y artístico del flamenco, y el otro flamenco, casi vomitado en casa la Fidela, como música de fondo de una farsa de payasos y puteros andaluzados. Y una vez más, frente al aire frío de la madrugada, esa inefable soleá que tú dijiste como nadie:\n\nA mí no me asusta «naide» ni con el hambre ni con la guerra y ahora quieren asustarme con los temblores de tierra.\n\nLlegó tarde pero llegó al fin. Al principio fue sólo un grupo de neófitos amantes de lo jondo. Intuyeron que por encima de las cositas fáciles y frívolas que se te escuchaban, había raudales de jondura, cantes hermosísimos que se te arrinconaban dentro. Y empezaron a admirarte y a quererte y por primera vez te llamaron maestro. Y de ti, como no era menos de esperar, surgió, con toda su dignidad, el cante, tu cante:\n\nPorque te llaman Aurora me acuesto al rayar el día; si te llamaras Rosario de la Iglesia no salía.\n\nQuienes con anterioridad, desconociéndote, te habían considerado como un mendigo de tabernas, ahora se vanagloriaban de haber gozado contigo una, cinco o veinte juergas memorables. (¿Qué no hiciste tú, Vicente, amigo mío, por conseguir esa generalizada dignificación de la persona y el arte de Pepe Polluelas?). La Peña Flamenca de Jaén fue su casa; y tú, Pepe, lo supiste desde el principio. Allí no se cumplía la vieja soleá:\n\nPobrecito del que es pobre y come por mano ajena siempre mirando a la cara quien la tiene mala o buena.\n\nSin ruido, sin molestar a nadie, te apagaste en esta ciudad que tanto tardó en comprenderte. Una botella de vino en tu féretro y un desgarrado cante por soleá:\n\nMataron al Marquesito se acabaron los valientes...\n\nDescansa en paz.\n\nCarta a Pepe «Polnuelas»\n\nA l igual que a Fernando «Terremoto» y el gran Antonio Mairena les escribí algo póstumo, hoy no me queda más remedio que evocar unas cuantas letras a mi querido compañero y amigo inmensamente flamenco cual fue nuestro Pepe «Polluelas».\n\nPepe, aunque he tenido la suerte terrenal de convivir contigo algunas horas de «tablao», quizá no te conocía demasiado, posiblemente por la diferencia de edad, o porque tú y yo somos tímidos y no lo demostrábamos, sin embargo en mi corazón siempre había un departamento para tu indiscutible calidad flamenca, amasada con una montaña de bondad infinita.\n\nPepe, ¿cómo olvidar tu empaque tan flamenco, a lo gaditano?; ¿y esa gracia misteriosa que poseías sin tú darte cuenta? Pepe, ahora recuerdo cuando cantabas una noche por cantiñas, hace ahora un año en la Peña, en tu Peña Flamenca de Jaén, y lo hacías sin facultades ya; pero la llevabas a compás, le dabas dulzura hasta el final; ninguno de los cantaores allí presentes éramos capaces de superar. Después canté yo por soleá y me invitaste a una copa de vino.\n\nPepe, voy a terminar esta pequeña carta, aunque amigo, en mí siempre estarás hasta que nos voltamos a ver algún día. Pero no puedo dejarte sin hacer una exclamación, quiero decirle al flamenco que no se te ha hecho justicia, Pepe, tú sabías de flamenco, tú eras todo flamenco, tú has sido fuente cristalina y nadie o casi nadie te ha bebido.\n\nTu amigo y compañero.\n\nEl Tato\n\nDescansa en paz.",
    "title": "Pepe Polluelas Ramón Porras",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 998,
    "article_char_count_full": 5961,
    "article_char_count_review": 5961,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-01-7-right-eduardo-de-la-malena-in-memoriam",
    "article_text_for_review": "In memoriam\n\nManuel Martín Martín\n\nLos flamencos seguimos de luto. Negros crespones penden ahora por la calle Relator. Un silencio angustioso se ha eternizado en la plazuela de la Mata. A falta tan sólo de una semana para cumplir los sesenta y cinco años, el pasado lunes 15 de enero, fallecía de un paro cardíaco Eduardo de la Malena, el último romántico de la Alameda de Hércules.\n\nHijo de María la Bonita y huérfano desde los dos años, había tenido crianza con su tía abuela La Malena, la genial bailaora jerezana. Estuvo haciendo platos en la Cartuja y arreglando bicicletas en Casa Gaitán, mas el ambiente familiar tan favorable al flamenco hizo que se inclinara por la guitarra.\n\nSe jactaba, y no sin razón, de ser ricardista puro, de quien aprendió todos los secretos. Con el tiempo llegaría a expresar todo lo inexpresable. Iba mucho más allá de lo que la inteligencia podía definir o las palabras significar. Terrate, Chocolate, El Sevillano, Pepe Pinto, Fernanda y Bernarda de Utrera o La Tomasa fueron, entre otros, los que tuvieron el privilegio de su acompañamiento.\n\nHace poco más de dos años, la Cátedra de Flamencología de Jerez le concedió el Premio Nacional a la Maestría. Y es que Eduardo fue el último romántico de una generación de artistas cuyas raíces se remontan al siglo XIX y cuya labor fue emparejar las tradiciones con una técnica deslumbrante adquirida a través de una vida de continuo aprendizaje y dedicación.\n\nBohemio, sarcástico, distante y seco o ingenioso y satírico, se nos ha ido un guitarrista muy emocional, de interpretaciones llenas de gran color y fluidez. Su arte al servicio del cante daba prioridad a la concentración de la expresión y del sonido. Tenía tal técnica y talento natural suficientes como para hacer lo que le diese la gana con la precisión de un relojero. De intensa preparación y densa ejecución, su horizonte musical era sorprendentemente amplio; incluía desde los más clásicos hasta los innovadores, pero su corazón pertenecía sin duda a Niño Ricardo. En sus interpretaciones tenía Manuel Serrapi el papel primordial.\n\nEste jodido mes de enero se ha llevado al mejor traductor del espíritu ricardiano. Corazón y espíritu estaban perfectamente equilibrados en su guitarra, y juntos creaban la base de su poder para el acompañamiento. Eduardo de la Malena, la séptima cuerda de Niño Ricardo, ha dejado de latir. Descanse en paz.",
    "title": "Eduardo de la Malena, in memoriam Manuel",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 398,
    "article_char_count_full": 2387,
    "article_char_count_review": 2387,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-01-8-right-m-sica-culta-y",
    "article_text_for_review": "(Un día después de la concesión de la Llave de Oro del Cante...)\n\nAnselmo González Climent\n\nLos músicos que concurrieron por vez primera al Festival de Córdoba (Mauricio Ohana, Narciso Yepes y Odón Alonso) fueron congregados por Ricardo Molina en un reservado junto con Capuletti, Juan Talega, Antonio Mairena, Melchor de Marchena, el Tomate y quien escribe. La idea inicial consistía en encontrar la posibilidad de establecer las claves musicales de las siguirías. Los flamencos se prestaron dócilmente a las marchas y contramarchas auditivas de los músicos cultos. Mairena, en especial, se vio obligado a repetir hasta el cansancio enfoques, tercios, enlaces, machos, compases, todo. Los músicos deliberaron seria y largamente. Con papel y lápiz ensayaron micropistas pentagramables. No daban de barato el menor detalle. Al principio cundió un entusiasmo que hacía suponer enrejar el cante. Pero poco a poco comenzaron a desmembrarse los apuntes, se apocaban los ánimos, y los papeles caían de silusionadamente al suelo. Entre tantas y tantas soluciones mestizas y pasajeras, lo único que surgió de aquel escrutinio fue deducir que la pantomima del Amor Brujolleva compás por siguiriγas. Y llévelo todo Satanás... Con pitos, nudillos y palmas sordas, Mairena trató de reenquiciar el despiste general. En balde. Ohana ensayó unas palmas que obligaron a Mairena decirle que así no podía seguir cantando. Yepes, a su vez, intentó sus palmas consiguiendo la aparición del camarero que entendió debía renovar el servicio...\n\nEs inolvidable la forma magistral con que Narciso Yepes cogió la guitarra de el Tomate para improvisar a su manera unas extraordinarias y esquemáticas soleares.\n\nNo se le notaba a Mairena dispuesto a «valer a los que poco pueden». Por imposición de Ricardo se sometió a aquel estudio que visiblemente le parecía tan errático como innecesario. Sin más ni más manifestó rechazo e impaciencia. Se sintió observado como un raro fenómeno musical. No podía entender que no entendieran.\n\nO dón Alonso apuntó que en su condición de músico y director de orquesta consideraba muy complejo inventar un convencionalismo de «escritura musical flamenca». Prefería concentrarse en el trasfondo emotivo del cante. De todos modos, manifestó su acuerdo con los elementos que Ohana proponía para diferenciar los estilos personales, haciendo hincapié en el «estilo melódico» y en los «ritmos internos». Odón añadía a esa diada un tercer elemento: el modo de «ataque» de cada intérprete.\n\nRicardo y yo nos regocijábamos al ver aquellos desesperados intentos para reglar el cante. Cuando los músicos no tuvieron más remedio que renunciar, nos quedó una mezcla de extrañezas y alegrías al ver una vez más indemne el misterio del sonido flamenco.",
    "title": "Música culta y compás flamenco",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 430,
    "article_char_count_full": 2744,
    "article_char_count_review": 2744,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-01-12-left-bailar-flamenco",
    "article_text_for_review": "El fandango de José Cepero es recortado, de buena expresión y sabor. Por saber cantar ha podido ser primera figura. Es jondo... y sin embargo le falta corazón. Sigue siendo talentoso y muchos se le arriman para adoctrinarse. Le aclaro que su fandango derivaba de la malagueña.\n\n12 Volviendo a Pepe Marchena. En verdad, Pepe no se ha tomado muy en serio el cante grande. Es típicamente anárquico y rebelde. Yo creo que él mismo se sorprende cuando se echa a cantar. Por lo menos hay que reconocer que ha mejorado la malagueña del Mellizo. Lo malo son las falsetas que hace con las soleares y las siguiriyes, cuándo deben ser, cómo son, cantes de corazón y de «voz natural».\n\n13 Veo perfectamente razonable el parentesco espiritual entre cante, toros y baile. Yo quisiera compararme con Pepe Luis Vázquez por ser sabio y tener duendes.\n\nManuel Torre fue el cantaor más genial. Artista desordenado, sorpresivo, único. Don Antonio se tiraba del balcón al escucharle. Chacón, contrariamente, era un cronómetro. Manuel Torre tenía la misma divina desigualdad que Rafael Gómez el Gallo. Chacón estaba más cerca de Joselito.\n\nA cordarse del baile a la hora de organizar unos actos de homenaje al flamenco, es mucho más que un alarde de sensibilidad, puesto que constituye un acto de amor y reparación; es el rasgo justiciero que necesitaba este arte incomparable, cumbre de todo el proceso jondo y que, siendo pieza indiscutible del ritual flamenco primitivo, en ocasiones preexistente al cante mismo, lleva décadas de decaimiento y malos tratos por parte de organizadores de espectáculos y público de poca finura que lo han relegado, o al papel de «teloneros» del cante, o al cierre de espectáculos de madrugada, cuando nadie atiende ya y la meteorología, el cansancio o el alcohol han hecho estragos en el público.\n\nPor eso ahora el flamenco se viste de gala al anunciarse el Seminario de Baile Flamenco organizado por la Asociación Cultural Andaluzas de Hospitalet de Llobregat y el Aula de Cultura de la Florida. En él se va a contar con la presencia impagable de Matilde Corrales González, Matilde Coral, la sevillana más universal de cuantas al baile se dedican en la actualidad y que, durante medio siglo, viene siendo la antorcha viva de este arte en el que ella se consume para nuestro deleite, acompañada de su familia bailaora: su marido, Rafael el Negro, y sus hermanos, Pepa Coral y El Mimbre. Nombres de nómina indestructible, cuya sola evocación llega al tuétano del baile. Matilde, que cuando baila y anda por el escenario o, simplemente recoge con gracia el mantoncillo, está convocando los ritos más ancestrales y las cadencias artísticas más ocultas, va a hacer en Hospitalet lo que hace en todas partes: crear escuela, enseñar, perfeccionar a los que se inician en esta difícil andadura. Envidio a aquellos que posean el talento y la sensibilidad necesarios para asimilar sus enseñanzas; su caudal humano y artístico se enriquecerán con este aporte de gracia y sabiduría, pues, como reza la añeja copla: «De los buenos manantiales / se forman los buenos ríos».\n\nEl seminario va a culminar con una Muestra de Baile Flamenco en la que todos podrán ver los frutos recogidos: la co-secha va a germinar y ellos, alumnos y maestros, van a deleitar a este público ansioso de engrandecer el flamenco. Los organizadores han tenido la delicadeza de dedicar esta efemérides a Doña Pilar López, aquella Pilar del baile eterno, que a sus setenta y siete años va a emocionarse con este merecido homenaje que ella, desde los vuelos de su traje de volantes, desde la dulzura de su gesto bailaor, elevará a su vez, no les quepa la menor duda, hasta la evocación de su hermana, la figura más grande que haya pisado un escenario, aquella inolvidable Argentinita. La ocasión está servida, les aconsejo apreciarla en lo que vale.",
    "title": "Bailar Flamenco",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 645,
    "article_char_count_full": 3820,
    "article_char_count_review": 3820,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-01-12-right-cante-y-toque-dos-aires-distinto",
    "article_text_for_review": "Luis Caballero\n\nDedicado al guitarrista Eduardo Rodríguez\n\nE 1 flamenco en sus tres facetas es algo así como un enorme y viejo libro sin principio ni fin. Un libro deteriorado y enigmático que vamos leyendo con dificultad, que cada lector va entendiendo a su manera. Un libro donde cada página puede decir lo que aparentemente dice y mucho más si la releemos y comentamos sin prisa, sin descanso, con sumo interés y en colaboración con mil lectores distintos. Por ejemplo: yo siempre recordaré lo que ya hace tanto tiempo nos dijo un famoso director de orquesta centroeuropeo refiriéndose al toque de la guitarra flamenca como acompañante del cante. Le ofrecíamos un recital ese íntegro y fiel tocaor que se llamaba Eduardo el de la Malena y yo. Era su primera audición, en cuanto a cante y toque, se entiende, en el sentido más estrictamente tradicional de la cuestión. Con la atención de un alto profesional de la llamada música culta, se extrañó primero de ¿cómo se podía decir tanto con la guitarra sin haber estudiado música? Y después —y esto es lo que no habíamos visto en la página correspondiente— lo que él consideraba disparidad musical entre cante y acompañamiento. Algo así como si la música del cante fuera una y la de la guitarra otra, independentemente del natural, lógico y perfecto acompañamiento armónico. Guitarrista. Talla en madera de Sebastián Miranda\n\nDe entonces a la fecha —y ya han debido de transcurrir más de treinta años— yo he ido estudiando, o como mínimo reparando, en este fenómeno, y es por lo que me atrevo a opinar.\n\nSabemos que el cante grande fue solo y como de hecho lo sigue siendo. Solo, individual y, por consiguiente, ajeno al dúo, al coro y a cualquier acompañamiento instrumental, evidencia que recuerdan las distintas tonadas que aún nos quedan,\n\naunque ya se comiencen a salpicar con apoyaturas acompasadas.\n\nLa incorporación de la guitarra acompañante, que llega más tarde y de la que en todos los cantes se puede prescindir, debió ir desarrollándose mediante un lento, pausado y elemental problema de dudas, desorientaciones y quién sabe de cuántas renuncias por parte de los no superdotados por la vocación y capacidad creativa. Aún se aprecia el adosamiento, la fusión. Pensemos en el toque que acompaña al cante para escuchar, no para bailar: la preponderancia de una abusiva cuadriculación mutilará en mucho la libertad expresiva del cantaor sobrado de facultades.\n\nPero el hombre hereda y mejora al hombre en su labor y el que nace y se hace tocaor, a semejanza del cantaor, crea o recrea, compone o arregla en el tiempo intuyendo una ortodoxia atávica, obedeciendo a unas raíces no escritas pero sí profundamente sentidas, girando, en su autorrealización, alrededor del eje que sostiene y centraliza toda la remota identidad genuinamente andaluza que caracteriza esta forma musical.\n\nAsí entiendo el toque; como una consecuencia del cante, como un adosamiento que sigue al cante pero sin imitarlo, sin repetirlo, sin recordarlo en ningún pasaje porque tiene vida propia como lo vienen a demostrar los toques solos, y si el solista osa tocar el cante que corresponda al aire que esté interpretando su labor desmejorará por ordinaria y de mal gusto.\n\nAhora bien: la guitarra flamenca tiene la obligación de sonar flamenca, de ser, como acompañante o solista, eminentemente flamenca, de expresar sus propios e íntimos argumentos sentimentales como corresponde a su índole, porque de nada nos valdrá el virtuosismo si el toque desborda la naturaleza que lo origina.",
    "title": "Cante y toque: ¿Dos aires distintos?",
    "periodical": "candil",
    "issue_id": "1990-01",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 581,
    "article_char_count_full": 3517,
    "article_char_count_review": 3517,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
