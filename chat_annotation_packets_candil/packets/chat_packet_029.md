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
    "article_id": "1981-07-15-left-ceaso-del-idolo",
    "article_text_for_review": "(Para Miguel Ayala y Manuel Urbano) La serpiente busca el pálpito de la garganta y zumban los oídos presintiendo la formidable erupción del triángulo ¿Cómo interceptar la suave ebullición de los anillos, la lenta agonía de unos silbidos que bañan de luz la frente coronada del sacerdote? El verde horizonte tiñe los perfiles ceremoniales de los miembros que exponen su cobarde repulsa a la consumación, y entre el abrazo desesperado y el planetario alarido de unos labios que se saben condenados a la fértil hermosura del silencio, surge la provocación del brocado que lentamente desgarrael brillo de su factura para alojar en su interior el frío crepitar de la víbora. ¡Qué escalofrío recorre la espalda tras el latigazo del grito! La convulsión del encuentro arranca las más bellas palabras que jamás pudieran oírse, los ayes de quien siente el celeste audir del veneno ascender por la garganta apagando la nieve del silencio, el furor envolvente de una voz loca de respirar persecuciones, la pertinaz llamada de quien rehuye la horizontalidad del olvido, el alarido circular que devuelve incansable la pared del teatro, aprisionando en su cerco las múltiples sombras del gesto que lucha por desasir su nudo.\n\nFco. Antolín Chica\n\nDescubrimos de nuevo la suave línea que la luz nos ayuda a desvelar entre el fragor del sufrimiento y la rotundidad del mármol, para acabar aceptando el descanso que impone la contemplación distanciada del campo de batalla.",
    "title": "Ocaso del ídolo",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 237,
    "article_char_count_full": 1455,
    "article_char_count_review": 1455,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-07-15-right-viejas-paginas-flamencas",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Ríos Vargas, nuestro colaborador de Alcalá de Guadaira, nos envía dos viejos textos de su ilustre paisano Manuel Beca Mateo —escritor, abogado y presidente que fuera del Ateneo de Sevilla—, fechados, respectivamente, en julio y agosto de 1926 en Sevilla.\n\nEstas colaboraciones, a mi parecer, no sólo son interesantes para redondear la biografía de Joaquín el de La Paula, sino lo que nos parece de mayor importancia, para conocer las relaciones sociales y humanas por las que discurrió el cante y sus protagonistas.\n\nLA JUERGA\n\nPor Joaquín el de la Paula, era conocido un gitano alcalareño, con cara de dolor de estómago, negro como el porvenir de un condenado a muerte, y cantaor admirado del flamenco de verdad, que en el momento de nuestro relato, una fría noche de invierno fue despertado\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nquín el de La Paula, sino lo que nos parece de mayor importancia, para conocer las relaciones sociales y humanas por las que discurrió el cante y sus protagonistas. LA JUERGA Por Joaquín el de la Paula, era conocido un gitano alcalareño, con cara de dolor de estómago, negro como el porvenir de un condenado a muerte, y cantaor admirado del flamenco de verdad, que en el momento de nuestro relato, una fría noche de invierno fue despertado por una voz que desde la ventana de la casa le decía: «Joaquín, levantate y llégate a casa de la Coneja, que hay una reunión y quieren que le cantes». «Diles que ya voy», contestó Joaquín con la amabilidad de quien ve las utilidades indudables de un buen negocio. «Pues no tardes, que tienen ganas de jarana», replicó la voz. Tan solícito acudió Joaquín al llamamiento, que no quiso entretenerse ni siquiera en tomar su diario y rápido baño, como él llama al acto de pasarse los dedos por los ojos. Cuando se disponía a salir, su mujer, cariñosa, sospechando la crudeza de la noche, le advirtió que se abrigase bien, a lo que Joaquín, ya en la calle, contestó tranquilizándola: «Pierde cuidiao, que me he puesto el ruso», e introduciendo sus manos en los bolsillos de los pantalones, después de levantarse el cuello de su raída americana, se encaminó hacia la Plazuela, donde esperábanle los juergistas que habrían de sufragar el futuro cocido. La presencia de Joaquín el de la Paula en la taberna de la Coneja, fue acogida con gran alborozo. El alcohol, excitando los sentimientos afectivos de los reunidos, les hacía proferir los mayores encomios en honor de los indiscutibles méritos del cantaor que al mismo tiempo soportaba los abrazos de Enrique, el mayor capitalista de la partida, quien en tono imperativo le decía: «Mira Joaquín, nos vas a cantar de chipén; nada de fandanguillos ni cantes ligeros, sino por lo jondo; vamos, lo tuyo. Píes lo que quieras y ya te estás apunt\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\nchaquetilla de los alamares negros por Dios no la vendas. Y aunque Joaquín no podía olvidar que toda su indumentaria consistía en el traje que llevaba puesto y que su mayor pena no era el temor de perder una jacarandosa chaquetilla corta de airosos alamares, sino carecer de un abrigo de pieles, tan largo que le obligase a llevar recogida la cola, por un fenómeno de autosugestión, puso tanto sentimiento en sus lamentaciones que hubo un flamenco-escucha que llegó a ver, con sus propios ojos, la tan estimada prenda sobre el mostrador del Monte de Piedad. El espíritu flamenco se había manifestado ostensiblemente: Joaquín cantó sin cesar durante toda la noche, bebiendo y comiendo hasta saciarse. Al amanecer, agotadas ya las energías por el vino y la vigilia, se acordó, por unanimidad, dar por terminada la juerga... y llegó la hora de la dolorosa. El abono de las consumiciones actuó de amor co e\n\n[ENDING CONTEXT]\n\nyo quiero hacer un brindis muy breve: Toma esto para que tus niños puedan celebrar nuestra fracasada cena». Y en sus negras manos puse unos duros de plata.\n\nAl recibirlos, Joaquín besó mis manos, y como si el ingenioso fuese yo, soltó una carcajada de emoción, mientras, abrazándome me decía: «Don Manué de mi arma, cuando me vean llegá mis churumbeles a la cueva en coche, tan vestío de blanco, con este sombrero de paja, y sonando en el borsillo estos machacantes, van a preguntarme: ¿Bato, viés de Puerto Rico?».\n\nY en su cara renegrida, convulsionada por la risa, vi cómo temblaba una lágrima.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Viejas páginas flamencas",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1503,
    "article_char_count_full": 8715,
    "article_char_count_review": 4514,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1981-07-16-right-el-flamenco-en-el-arte-espa-ol-d",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSi hubo algún libro que, desde su rótulo, marcara épocas abonando su título para el refrito, no me caben dudas de que este sería «Señas de identidad», de Goytisolo; por cierto, bien a pesar de su autor, buen conocedor del garrulo nacionalmímetismo. A la conciencia e incuestionable realidad de ser catalán, vasco, gallego o andaluz, pongamos por caso, se le trata de recortar, aunque crean lo contrario, con el imperativo de la búsqueda de unas señas —algo así como tic ocultos— que, a la realidad me remito, más que identificarle e integrarle sean señales de diferenciación, cuando no de disidencias o lamentable exclusión maniquea. No acierto a comprender, cuando contamos nuestra historia por milenios, como por una y otra esquina de nuestro solar se buscan raíces en un dilettantismo cultural\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"histórico\"]\n\nde recortar, aunque crean lo contrario, con el imperativo de la búsqueda de unas señas —algo así como tic ocultos— que, a la realidad me remito, más que identificarle e integrarle sean señales de diferenciación, cuando no de disidencias o lamentable exclusión maniquea. No acierto a comprender, cuando contamos nuestra historia por milenios, como por una y otra esquina de nuestro solar se buscan raíces en un dilettantismo cultural con pretensiones histórico-sociofilosófico-regionales, olvidándose con más frecuencia de la debida de que la Historia no tiene más que una lectura y que las raíces se fortalecen ubérrimas derramándose por las frondas inmarchiables del inmenso árbol que somos. Se olvida, Machado dixit, que si alguien quiere filosofar comience, como él, por nuestro folklore. De una vez y por todas, la cultura e identidad de un pueblo surgen más de su decidido camino de futuro amarrado a la memoria, que de un voluntarista rastreo arqueológico de esencias y mitologías. Estas y otras consideraciones se me agrupan en el costado tras la visita a la espléndida y elocuente exposición de dibujos, esculturas y pinturas que durante estos días se muestra en la madrilena sala de exposiciones del Banco de Bilbao y que, si la memoria no me es infiel, supone la cuarta monográfica sobre «El flamenco en el arte español de hoy», de la que me parece responsable, gozosísimo responsable, ese grandísimo conocedor del flamenco y mejor artista que se llama Antonio Povedano. Mas si esta exposición arrastra a conclusiones, no es menor el número de garfios interrogantes que suscita desde sus propios planteamientos, entre los que, tal vez sea primero el que se cuestiona Antonio Gala en la introducción al catálogo, respondiéndolo con certeza: «Si el flamenco es\n\n[ENDING CONTEXT]\n\nnace y habita el cante, nos quedemos sin poder admirarla y no, por cierto, por culpa de los artistas que en ella figuran. De aquí que, una vez más al tratar de temas culturales, termine por clamar —ya no solicito— ante quienes corresponda —y esa correspondencia no necesariamente es exclusiva de las instancias oficiales; pero sí prioritaria— de que esta exposición viaje por Andalucía porque está enmarcada con la más noble manera de nuestras raíces, tan específicas como universales; nuestras raíces jondas, que no hondas. Nuestras evidentes señas de identidad, que no ocultas.\n\nManuel URBANO\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«El flamenco en el arte español de hoy» NOTAS CON OCASION DE LA IV BIENAL",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1170,
    "article_char_count_full": 7074,
    "article_char_count_review": 3392,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "histórico"
      }
    ]
  },
  {
    "article_id": "1981-07-17-right-de-cayetano-muriel-y-una-ltima-g",
    "article_text_for_review": "Al tener noticias de que Pepe Arias había lanzado al mercado dos cassettes conteniendo cantes del genial egabrense, Cayetano Muriel Reyes, y a pesar de que los conservo en mi archivo en muy buenas condiciones auditivas, me apresuré a adquirirlas ya que las reconstrucciones técnicas que hacen las casas grabadoras resultan más perfectas al suprimirles un poco de ruido de fondo.\n\nDice Arias que nadie se ha preocupado de cantar las excelencias artísticas de Cayetano y puede que todos los aficionados lo crean así. En el año 1969, cuando tuve el gusto de conocerle, en el Congreso de Córdoba, ya había escrito yo sobre el extraordinario cantaor, aunque no pudo ser publicado, porque sus familiares no me lo permitieron.\n\nQuiero recordar que sostuve correspondencia con un hijo que, según creo, fue Cartero en Puente Genil. Este hombbre, del que ignoro si aún vive, fue un caballero para mí, porque, sin conocerme, me atendió como no es muy frecuente que se haga hoy, por desgracia. Le pedí una fotografía de su padre y me la envió rápidamente. En ella aparece el genial Cayetano, con unos cuarenta años, y tocado con sombrero de paja, muy corriente en aquella época entre las gentes de categoría.\n\nEn cuanto a sus cualidades artísticas decía Pastora que tanto a ella como a su esposo les encantaba escuchar al Niño de Cabra por la escuela chaconiana. Aseguraban que fue el mejor cantaor por don Antonio. Yo también puedo dar fe de ello.\n\nSobre su vida logré recopilar bastante material entre Sevilla y Madrid; material que, como queda dicho, no pude publicar. Y no porque su publicación pudiera oscurecer al artista o a su familia. Nada de eso. Me consta, por La Niña de los Peines, Pepe Pinto, Ramón Montoya y Manolo de Badajoz, quienes trabajaron con él en muchas ocasiones, que fue un excelente compañero, hombre de pocas palabras y corto de genio. Quizás sea éste el motivo por el que no quería cantar ante un público muy numeroso. Asimismo, al ser de carácter introvertido, tampoco le gustaba grabar. (De todas formas su discografía es buena y no es corta). Si grabó pocos años antes de morir fue gracias a que Ramón Montoya y Manolo de Badajoz, casi le obligaron. Los motivos que ambos tuvieron me los reservo.\n\nTermino diciendo a Pepe Arias (dejando a un lado ya el tema de Cayetano Muriel) que, como don Antonio Chacón, fue Capitán General en la interpretación por granaína y media granaína, «ordenó que se cantara primero por granaína y a continuación por media; y todos los cantaores de aquella época, sin excepción, cumplieron exactamente la orden dada por el gran maestro. Me consta que tal orden ha producido equivocaciones entre los aficionados y lo que es peor, entre algunos profesionales, que ya no distinguen entre una granaína y una media. Ha habido algún que otro profesional que ha dicho públicamente: «señoñres, voy a cantar por Aurelio una malagueña terminando por granaína». Y lo que ha cantado ha sido por granaína amalagueñada (estilo Aurelio) terminando por media granaína.\n\nGranaína: «Engarzá en oro y marfil».\n\nY me temo que Pepe padezca también ese error porque ha llamado, en las cintas recién editadas, granaína a una media, así:\n\nGranaína: «Río del Genil, viva Graná que es mi tierra».\n\nY debió denominarlas así:\n\nGranaína: «Engarzá en oro y marfil».\n\nMedia Granaína: «Río del Genil, viva Graná que es mi tierra».\n\nY, lo lamentable del caso, es que existen señores que no suelen dar a esto importancia alguna.\n\nManuel Yerga Lancharro",
    "title": "De Cayetano Muriel y una última grabación",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 587,
    "article_char_count_full": 3466,
    "article_char_count_review": 3466,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-07-18-right-los-escritos-de-demofilo-en-la-r",
    "article_text_for_review": "Coincidente con el centenario de la aparición impresa de la «Colección de Cantes Flamencos recogidos y anotados por Antonio Machado y Alvarez», la editorial Demófilo reedita en conmemoración del mismo los escritos que el patriarca de los estudios flamencos diera a la luz en el trienio 1869-1871 en la sevillana «Revista de Literatura, Filosofía y Ciencias», posteriormente recogidos —1884— en el quinto volumen de la «Biblioteca de las tradiciones populares españolas» y que, hasta ahora, parecían prácticamente inexpugnables para buena parte de los aficionados, que sólo poseían de ellos la antología de sus coplas elaborada por Arcadio Larrea como apéndice de «El flamenco en su raíz»; antología que carecía de estas prosas, verdadero soporte, armazón y juicio de los cantares.\n\nAnotando que no está en nuestro ánimo ofrecer una crítica del volumen, sino una simple y gozosa reseña de la aparición impresa de estos iniciales textos machadianos, no queremos dejar pasar la ocasión de anotar que el folklorista se nos viene aquí con una personalidad intelectual muy cercana a la de los institucionistas, a la que, en verdad, siempre sería fiel.\n\nNueve son los artículos que, aunque no muy ortodoxamente, sí de modo atinado, rotulan esta reedición: «Primeros Escritos Flamencos»; una serie de trabajos que vienen a darnos cumplida y muy concreta noticia del primer parecer flamenco de Antonio Machado y los que, si bien no fueron redactados por su autor con planteamiento unitario para una edición sistemática, de alguna forma contienen una mirada redonda y casi total a las coplas como conjunto a la búsqueda de «lo que expresan y significan, como obra de un pueblo que a todos nos interesa conocer».\n\nEl mundo del cante en su expresión psicológica y fonética es la preocupación primordial de Demófilo en estos breves ensayos, por encima de «los que los inteligentes llaman estilo». Y algo más podemos descubrir en la siguiente confesión a retener: «El pueblo descubre, sin duda, en estos cantes (ópera suya) armonías desconocidas para nosotros».\n\nNo obstante algunas contradicciones con posteriores escritos y ciertas aseveraciones, a mi juicio, rotundas en exceso —«Esta predilección hacia esta música especial, lúgubre y sombría, patentiza, con la necesidad íntima y profunda de sentir, propia de la raza andaluza, una degradación moral, aunque menos afeminada, ánaloga a lo de nuestras aristocráticas clases, ardientes admiradoras de las producciones francesas—, a lo largo de este breve y enjundioso librito, imprescindible en la bibliografía flamenca, encontramos más, mucho más que unos primeros estudios flamencos serios y críticos, estamos frente a juicios y aseveraciones repletas de vigencia y necesarias de recordar.\n\nSin lugar a dudas, la edición de este libro cumple con exceso, si me apuran, sus objetivos dentro de una colección denominada cuadernos de cultura popular; pero estimo que la obra flamenca de Antonio Machado y Alvarez, Demófilo reclama con urgencia una edición total y crítica, sistemática, científica y desapasionada. En unos años como éstos, en los que el cante y no pocos de sus estudiosos reclaman libertad en la expresión artística, considero que este arte inconmensurable reclama los más serios replanteamientos y no veo mejor forma de iniciarlos que desde el estudio de los textos de Demófilo, el primer investigador científico del cante.\n\nManuel URBANO",
    "title": "Aunque no quepa en el papel.-Los escritos de «Demófilo» en la Revista de Literatura",
    "periodical": "candil",
    "issue_id": "1981-07",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 530,
    "article_char_count_full": 3391,
    "article_char_count_review": 3391,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
