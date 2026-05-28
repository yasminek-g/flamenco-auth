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
    "article_id": "1997-11-20-right-juan-de-loxa-me-presta-dos-poema",
    "article_text_for_review": "Juan Antonio Ibáñez\n\nMorente acude a Nueva York, nuevamente, para reencontrarse con Lorca. Al sur, siempre al sur, queda el río con pintadas de sangre en sus ocultas paredes:\n\n¡Oh cuello mío recién degollado! ¡Oh río grande mío! ¡Oh brisa mía de límites que no son míos! ¡Oh filo de mi amor, oh hiriente filo!\n\nambién:\n\n¡Quién dirá que el agua lleva un fuego de grillos!\n\nLa veleta del tiempo señala el infinito; y es posible que la Luna, con crespones de nieve, adorne geranios de plata. En este escenario, Enrique Morente, una vez más, nos entrega su arte:\n\nNo es sueño la vida. ¡Alerta! ¡Alerta! ¡Alerta!\n\nNo es hora, ahora, de hacer detallado análisis de su periplo a través del mundo flamenco. No intento, por otra parte, abordar el camino ya trazado, cuando sólo se trata de reafirmar nuestra creencia en una personalidad definida y reconocida en amplísimo abanico de opiniones. Sí decir que esta aportación deja, en nosotros, una sabia actitud de compromiso con la esencialidad de lo jondo. Porque Morente abre, no cierra. Y no entraré pues, en el círculo de la ortodoxia, ni aplaudiré la llamada pureza que, de voz en voz, la historia me dice. Sólo deseo sumergirme, con el cantaor, en un mar de desasosiegos. Invadir, con la mirada, los limpios sonidos de lo oscuro. Quiero libre al artista. Morente, lo es: De la fuente de la plaza\n\nha llegado un pregonero:\n\n¡Cántaros de libertad\n\npa que beba el mundo entero!\n\nY desde esa libertad, el granadino clava, con su voz, cristales al silencio, o bien, acaricia la herida abierta de la melancolía. Es posible que araña la conciencia de un pueblo, o que burla, burlando, penetre en los callejones prohibidos para sorprender, de madrugada, a las estrellas primeras del universo. Es posible. Seguro que Morrente cuando canta pone música al pensamiento, compás a la palabra, grito a la creación. Es:\n\nTu voz que arroja lava, crestas de gallo, tempestades.\n\nLa voz que me circunda, y que,\n\nque me ametralla y clava\n\naguijón rojo al yunque,\n\nvolcán y fiera indómita panza arriba\n\nen mis brasas, y que,\n\ny que en mi brazo es onda, y ahonda,\n\ny se sumerge allí hasta donde el mar\n\npierde, de sus abismos, potestad\n\npara el canto.\n\nEs posible que Lorca y Morente puedan ser uno, paridos ambos, de un mismo sueño llamado Granada. Cierto es que:\n\nUn hombre aquí crepita, huye,\n\nse desintegra, forma parte de un río\n\nal cauce de los dioses, vuelve su vista\n\nhacia la muerte: Sal se encuentra.\n\nY qué espolón feroz al fondo de los ojos.",
    "title": "Juan de Loxa me presta dos poemas, mientras Morente mira a Lorca",
    "periodical": "candil",
    "issue_id": "1997-11",
    "year": 1997,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 433,
    "article_char_count_full": 2477,
    "article_char_count_review": 2477,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-11-21-left-gloria-y-honores-a-t-o-evaristo",
    "article_text_for_review": "Manuel Ríos Vargas\n\nHace ya algún tiempo que dejé de asomarme al balcón flamenco de la Revista Candil, ello motivado porque creía y creo firmemente que, las distintas parcelas están magnificamente cubiertas, aunque entendemos, y por creerlo así, osamos efectuar esta incursión, que a lo que nos queremos referir bien merecía la pena, pues las opiniones son libres y obviamente tienen cabida para todos los que amamos este Arte, por muy modesta que pudiera ser nuestra aportación.\n\nDesde nuestra óptica y perspectiva, queremos agradecer a los buenos amigos José L. Vargas Quirós y Pedro Sánchez Ortega la extraordinaria entrevista recabada a Evaristo Heredia Maya, aparecida en el número 111 de tan digna Revista y, donde a la vez queremos felicitar efusivamente y de forma entrañable al bueno de Tío Evaristo, flamenco cabal y hombre de bien donde los haya.\n\nTuve el honor y el placer de conocerle hace ya muchos años en que realicé mi primera visita veraniega a Algeciras y recuerdo como si fuese ayer nuestro primer encuentro, gracias a la recomendación que de Tío Evaristo me hiciera el amigo y añorado Paco Vallecillo, quien me facilitó su número de teléfono, quedando a través del hilo telefónico citados en una determinada cafetería local sin que aún nos conociésemos.\n\nNunca podré olvidar ni agradecerle la recepción que me ofreció y de la que fui objeto en una venta si tuada en un lugar conocido como La Almoraima —Paco de Lucía tiene un disco titulado de tal guisa—, sencillamente por mi recomendación, por mi condición de calorró y por mi nacencia alcalareña, con Tío Joaquín el de la Paula por bandera; en dicha reunión estuvimos presentes mi esposa —quien recibió de sus manos un magnífico ramo de flores—, Alejandro Canela y dos hermanos que atendían al posible seudónimo de Aparecida, donde uno de ellos cantaba y el otro tocaba la guitarra.\n\nRecuerdos imborrables, Tío Evaristo, fueron los muchos momentos vividos en tu Sociedad de Cante Grande, donde nos dábamos cita con muchos y buenos flamencos algecireños. así como también estaba presente nuestro común amigo José Parrondo, quien por aquellos entonces estaba desplazado en Algeciras cumpliendo sus deberes laborales y, donde igualmente era también asiduo —le pido perdón por no recordar su nombre—, un amigo que era representante del vino Tío Pepe y que hacía de tocaor oficial en dichas reuniones.\n\nNunca podré olvidar la amistad y el cariño con que me recibió ni la gentileza de su familia al conocerme y conocer a los míos.\n\nEnhorabuena, Tío Evaristo, porque tú ya te merecías una entrevista de tal calibre y, porque ésta no podía venir mejor que de quienes te la han elaborado y recabado.\n\nEn definitiva, y ya para terminar, ésta ha sido una entrevista sabrosí-sima, donde Vargas Quirós y Sánchez Ortega han dado muestras de una encomiable sagacidad y donde Evaristo Heredia Maya ha puesto de manifiesto su enorme sapiencia. A aquéllos les felicitamos por la cualidad antes citada y a Tío Evaristo le seguimos queriendo y adorando por sus muchos conocimientos, por la veracidad y frescura de sus palabras y por su lealtad flamenca.",
    "title": "Gloria y honores a Tío Evaristo",
    "periodical": "candil",
    "issue_id": "1997-11",
    "year": 1997,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 515,
    "article_char_count_full": 3107,
    "article_char_count_review": 3107,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-11-21-right-t-o-mollino-tras-un-a-o-de-su-mu",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLuis Soler Guevara La vida sigue igual. Es normal. Lo que ya no lo es, ni justo y por ende ni honesto, es que tras la muerte de un artista como Tío Mollino, los munícipes algecireños al frente de su alcalde no tomen acuerdos para inmortalizar su arte y su memoria, ni tan siquiera con el nombre de una calle de su pueblo. Y tal vez ello porque la desaparición del gita- no Tío Mollino no vende. No fue famoso. Sí famosos, ese es el término acuñado a gran parte de esa legión de botarates que tienen cancha en todas las cadenas y programas telebasuras del país. Puesto a sacar conclusiones no es fácil imaginar que, todo ello, también, es producto de la incultura a la que sirven esos medios. Pero no es este el único motivo. A él se une la apatía y la falta de sensibilidad de quienes, teniendo la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"raíces\"]\n\nérmino acuñado a gran parte de esa legión de botarates que tienen cancha en todas las cadenas y programas telebasuras del país. Puesto a sacar conclusiones no es fácil imaginar que, todo ello, también, es producto de la incultura a la que sirven esos medios. Pero no es este el único motivo. A él se une la apatía y la falta de sensibilidad de quienes, teniendo la obligación por sus capacidades y responsabilidades de prestigiar y defender nuestras raíces en Andalucía, hacen dejación de ello. ¿Es ello un olvido? Olvidar como dijera hace muchos años Eugenio Noel, $ ^{1} $ “es enterrar magníficamente. ¿Podéis imaginaros losa de bronce más pesada y espléndida que el olvido?”. El olvido es la negación del referente existencial de uno mismo. Olvidar es en cierto modo la mutación de la memoria, la enajenación de nuestro propio yo. La memoria de este gitano trovador de “sonidos jondos” nos exige su recuerdo, nos invita a la reflexión de un Arte como el nuestro que se nos cae a jirones por los pueblos de nuestra Andalucía. “Las instituciones públicas y también las privadas —por el bien de la cultura de sus pueblos—, no pueden silenciar, ni deben ser cicateras, con aquellos que han dedicado y vaciado su vida por el arte, y el Flamenco es tan arte como las Letras, la Pintura, la Escultura, etc., y por tanto, no debiera permitirse que nuestro maravilloso Arte Flamenco sea ignorado cuando\n\n[ENDING CONTEXT]\n\nde su arte con el nombre de una calle en su pueblo natal?.\n\nSepan de una vez para siempre que la deuda histórica que contrásemos con estos hombres y mujeres muchas veces vilipendiados por la mediocridad de muchos, es impagable, que el pensamiento humano por y para lo flamenco, tiene una fuerza irresistible e inagotable de vivencias. Vivencias como las que narró en sus cantes por soleá, por siguiriγas, por fandangos, por toná, por bulerías, por tientos, por saetas, por muchos palos del cante. Todo esto y mucho más, está en el hacer de este gran gítano que se nos fue para siempre.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Tío Mollino, tras un año de su muerte",
    "periodical": "candil",
    "issue_id": "1997-11",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1380,
    "article_char_count_full": 8075,
    "article_char_count_review": 3009,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "raíces"
      }
    ]
  },
  {
    "article_id": "1997-11-23-left-paul-villmoare-cante-puro",
    "article_text_for_review": "M i intención en este mi primer artículo a Candil no es proponer un nuevo horizonte para descubrir nuevas parcelas de creación al flamenco; sólo pretendo exponer mi reflexión sobre lo factible que resultaría abonar campos afines por influencia recíproca a la música española y especialmente flamenco.\n\nSoy norteamericano, llevo años dedicado a la música y soy un rendido enamorado del arte flamenco sobre el que estudio sin descanso.\n\nUn artículo de mi paisano D. E. Pohren, aparecido en el número extraordinario de Candil (105), dice sobre lo que es flamenco “tradicional” y lo que son otras expresiones, que Pohren menciona como flamenco fusión. El entiende “como flamenco “puro”, no sólo lo ya establecido y tradicional, sino cualquier creación, innovación o fragmento espontáneo, basado en la tradición flamenca que caiga dentro de los límites del palo que pretende ser (compás, aire, estructura musical, etc.) y que suene flamenco”.\n\nLa prehistoria y la protohistoria del cante flamenco, con razón aplastante, siempre ha sido hallada en el área musical mediterránea. Todas las aportaciones musicales que en el devenir de los siglos recalaron en Andalucía, fueron mediterráneas en sí, dándose a sí el milagro del cante flamenco. Esto fue posible al ser el carácter andaluz muy afín a este tipo de música.\n\nCuando el cante ya es historia, empieza a conquistar parcelas de la vecindad andaluza en orden semejante al que sigue: Cádiz, Sevilla, Málaga, Córdoba, Granada, Almería, Jaén, Murcia y Huelva. Pide tonadas tradicionales a Extremadura, Castilla y el resto de la península, terminando momentáneamente con los cantes de influencia americana. Todo lo mencionado, siempre por acierto y sentido musical flamenco de los cantaores que han quedado en la historia del flamenco. Si se han podido aflamencar los cantes de influencia americana, como la rumba que viene de Cuba y tiene influencia afro-cubana, ¿no podría hacer lo mismo con otros aires?\n\nCuando hice mención de esta miriflexión a mi maestro D. Antonio Escribano, recibió su beneplácito y sus inolvidables palabras: \"No prosperan los intentos de nuevas aportaciones flamencas porque falta profundidad y falta de conocimiento en los nuevos autores. Estos van en pos del dinero y no en la evolución del flamenco. Crean bodrios por desconocimiento de los prototipos tradicionales. Es una falacia lo de neoflamenco porque su contenido de flamenco no tiene nada\". Es evidente que si alguien va a intentar aportar algo nuevo al flamenco, tiene que tener un dominio del pasado del flamenco. Esto no llega de un día a otro, muchas veces se tarda toda una vida. No dejaré de ponderar los ensamblajes musicales entre el cante flamenco y el acompañamiento marroquí logrados por \"El Lebrijano\". A mi entender, éste sí es un buen camino, aunque aún quede más por evolucionar.\n\nSi un día fueron la rondeña, la malagueña, la granaína, la cartagenera, la milonga, la farruca (escuchad la praviana del Niño de Rosa Fina), la sevillana y los fandangos de muchos..., ¿por qué desechar que en un futuro un cante tenga título de un país vecino de nuestro mar? Es más fácil conseguir un ensamblaje musical entre músicas que ya comparten raíces que no de músicas totalmente diferentes y sin elementos comunes.",
    "title": "Paul Villmoare ¿Cante puro?",
    "periodical": "candil",
    "issue_id": "1997-11",
    "year": 1997,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 525,
    "article_char_count_full": 3247,
    "article_char_count_review": 3247,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-11-23-right-dos-genios-cruzan-sus-corazones",
    "article_text_for_review": "II Concurso de Guitarra para Jóvenes Aficionados\n\nFernando Arévalo\n\nAsado mes de diciembre. En la Peña Flamenca de Jaén. Segundo Concurso de Guitarra para Jóvenes Aficionados de toda Andalucía. Una iniciativa interesante y necesaria de la Diputación Provincial de Jaén. La idea todavía está en embrión. Posiblemente, casi seguro, le hace falta un poquito más de publicidad. Sin embargo, principio quieren las cosas y este certamen puede convertirse en algo serio e importante dentro del mundo flamenco. La colaboración de la Peña Flamenca de Jaén es fundamental y se nota. En las dos jornadas de la pasada edición no había mucho público, pero estaban los cabales. En la noche de la finalísima concurrieron distintas emociones:\n\nCinco jóvenes tocaores, dos de ellos tan sólo tenían 23 años, acudieron a la cita del II Concurso de Guitarra para Jóvenes Aficionados. Los sevillanos Víctor Manuel García Oviedo y Rubén Díaz Levariego, el jerezano Eduardo Lozano Camacho, el esteponero Gaspar Rodríguez Román y el jiennense José Rojo Moreno.\n\nTras dos jornadas de intensa y reñida competencia, se proclamó vencedor el jovencísimo guitarra sevillano (15 años de edad) Rubén Díaz Lavariego. La segunda posición fue para Gaspar Rodríguez Román, quedando en esta oportunidad en tercer lugar el jiennense José Rojo Moreno.\n\nSegún la valoración del jurado, el nivel de los finalistas alcanzó cotas de gran calidad. El ganador, Rubén Díaz Lavariego, ya es algo más que una firme promesa.\n\n¡Ay, los girones del alma! Y sin embargo, Chano estaba allí. Pletórico, chispeante, intuitivo, cercanamente galáctico y genial. Peña Flamenca de Jaén, noche del primer viernes de noviembre. Epílogo de la finalísima del II Certamen de Guitarra Flamenca organizado por el Area de Cultura de la Diputación. Rubén Díaz, un chavalillo sevillano, se había metido al jurado en el bolsillo y a todo el respetable que se meció con la ensoñación suave de su toque y la reciedumbre de sus desplantes. Cuando la guitarra ríe y llo-ra, grita y susurra, te araña y te posee.\n\nJuan Carmona “Habichuela”, el maestro, le echó el ojo desde el primer día. El toque del niño le producía cosquilleo, tenía ángel. Rubricaron el acuerdo el quevediano Manolo Urbano, el impecable guitarrista Antonio Anguita y Valera, don Rafael, el “dire” de Candil. Bendijo\n\nel acta el joven diputado, linarense para más señas, Juan Fernández. El santuario de la Peña pedía silencio de complicidad. Se mascaba en el ambiente el indefinible tufillo de las grandes ocasiones. Chano, el irrepetible Lobato, estaba tocaído. Ya digo, los girones del alma y los achuchones del destino. ¡Ay Señor, que mi hijo está malito! El suyo, el mayor, internado en una clínica de Grecia, peleando a dentelladas con la muerte. Pero Chano es llama, fuego, destello, pellizco, torbellino de madrugadas y vida. Y tenía que explotar. Y contagió al senequismo profundo, de ojera estoica y rictus de sonrisa cansada de Juan Carmona, \"El Habichuela\". Con el Ondas todavía debajo del brazo, recién llegado de Barcelona. Momento mágico, que se dice. De los que se viven una sola vez, pero se viven. Juntitos. Picaítos los dos. La complejidad del duende de los genios. El Chano provoca al Habichuela. Le saca de sus casillas, lo lanza al ruedo celeste. Toca, toca. Suspira, jadea, pincha, ahonda, enjuaga la lágrima, mordisquea los visillos del alma.\n\nY al final el provocador cae en su propia provocación. Y se produce la catarsis de los elegidos, el desnudo integral de los grandes. El infierno y la gloria del quejío. La embestida de la guitarra. Dos titanes en el tablao. Chano y El Habichuela, El Habichuela y Chano. Y el crujiente susurro de la oración de los devotos. Fervor y silencio. Dos genios cruzan sus corazones.\n\nOs queremos, os necesitamos. Que nadie os arranque de la raíz. Que se pare el reloj de los años. Dios tiene que comprender que vuestro embrujo es eterno. Aquella fue la noche en que Chano y El Habichuela nos descubrieron el secreto de su inmortalidad.",
    "title": "Chano y Habichuela: Dos genios cruzan sus corazones",
    "periodical": "candil",
    "issue_id": "1997-11",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "23-24",
    "page_number": 23,
    "word_count": 657,
    "article_char_count_full": 3989,
    "article_char_count_review": 3989,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
