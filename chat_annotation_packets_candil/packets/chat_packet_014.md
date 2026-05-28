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
    "article_id": "1980-09-12-right-la-garganta-en-el-cante-es-un-ri",
    "article_text_for_review": "LA GARGANTA EN EL CANTE ES UN RITMO DE PIEDRA\n\nLa garganta en el cante es un ritmo de piedra, una chispa en un asma de jadeo y gemido, una voz que se enrosca y dilata los lomos de las venas que aprietan sus culebras robustas.\n\nLas palmas asfixiadas por el cóncavo esfuerzo de contener un aire que no ahuyente el sonido, excitan el rescoldo del bordón y la prima con el vuelo insistente del pájaro del plectro.\n\nEl cantaor es álamo que se enreda en la cepa donde el eco enronquece con un ansia de arcada, donde el jondo delirio palpitante del cuello contiene desazones de granadas y espasmos.\n\nPero estalla la copla derribando afonías, en el áspero esfuerzo de un temblor masticado, y el gemido no es llanto, ni una queja andaluz, sino que es la punzada que desguaza la boca, grieta o grito de gallo que cumplió su reyerta, y el fandango se alza en su trono de fusta reventando el estrépito de las últimas palmas.\n\nRafael Duarte",
    "title": "LA GARGANTA EN EL CANTE ES UN RITMO DE PIEDRA",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 171,
    "article_char_count_full": 927,
    "article_char_count_review": 927,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-09-13-left-los-viejos-conservadores-y-las-b",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n《LOS CONSERVADORES》\n\nAfirman algunos flamencólogos, que el cante flamenco ha llegado hasta nosotros, debido a que los gitanos conservaron en el seno de su raza modalidades de viejos cantables que, de no ser rescatados por ellos, se hubiesen perdido en el tiempo; que debido a tal conducta para con el Cante, no solamente el gitano ha sido un celoso conservador de los géneros consistanciales a su carácter y vivencia, sino que también ha conservado y conserva, estilos de creaciones de sus hermanos de raza, que nutren las filas de los grandes maestros en las listas históricas del cante flamenco.\n\nCon su razón —pues cada uno poseemos la nuestra— hay quien ha escrito, que las etapas evolutivas del cante flamenco, casi siempre se han producido como quiebro de la línea recta seguida por los\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"pureza\"]\n\nter y vivencia, sino que también ha conservado y conserva, estilos de creaciones de sus hermanos de raza, que nutren las filas de los grandes maestros en las listas históricas del cante flamenco. Con su razón —pues cada uno poseemos la nuestra— hay quien ha escrito, que las etapas evolutivas del cante flamenco, casi siempre se han producido como quiebro de la línea recta seguida por los conservadores, o lo que es lo mismo, por el abandono de la pureza que ellos custodiaban. Así, en las primeras décadas de los cafés cantantes, Silverio Franconeti se deslindó de lo puro creando formas y giros que vulgarizaron lo selecto y hacían olvidar la línea tradicional de su maestro «El Fillo». Todo lo contrario de Tomás «El Nitri» sobrino y discípulo del mismo maestro. Cuando los cafés cantantes comienzan a declinar y se impone la conquista de mayores recintos, para espectáculos de masas es don Antonio Chacón quien abandona la verdad e impone su falsete; barroquiza ciertos géneros y crea estilos en base a las dos debilidades anteriores. Pero el castigo a su propio quehacer —ese olvido voluntario de la línea\n\n[ENDING CONTEXT]\n\nen la creación y en la evolución de nuestros querido Arte, porque ambas circunstancias las he observado producirse en mis años de dedicación al cante flamenco, pero no puedo admitir que se impongan estilos. Es la afición, más o menos iniciada (no las masas ciegas), la que acepta, y no la testarudez de ciertos cantaores empeñados en levantar su columna en lo básico del Cante Flamenco Andaluz. ¡Ah, que no se me olvide!: La teoría de la raza conservadora hace muchos años que para mí no dispone de consistencia. Pienso que ha intervenido en esa creencia más la fantasía que la razón.\n\nA. Escribano\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Los viejos conservadores y las bulerías",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 1587,
    "article_char_count_full": 9282,
    "article_char_count_review": 2732,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "pureza"
      }
    ]
  },
  {
    "article_id": "1980-09-14-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAunque no quepa en el papel\n\nCon él llegó el escándalo:\n\n«Proceso al Gitanismo», de Manuel Barrios\n\nmanuel barrios\n\nLa recientísima y ya prestigiosa editorial andaluza «Edisur», inicia su colección de cuadernos de cultura popular, con un libro inquisicionesco (de inquirir, investigar) sobre los gitanos españoles y, como es lógico, de sus relaciones con el cante flamenco; su paternidad responsable corresponde a Manuel Barrios, también más de un lector le hará responsable por esta paternidad. Quiero decir, desde un principio, que estamos ante un libro polémico, de los que\n\nlevantan polvaredas de denuestos y adhesiones. Ya se sabe, tocar este tema supone avivar las áscuas —no tan en rescoldo— de gitanófagos y gitanófilos: dos posturas maniqueas, las que, prácticamente, encuadran a toda la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"crítica\"]\n\noteo —algo de lo que el autor es consciente—; pero de lo que no cabe duda, es que los primeros, y sabiéndole hacer, son los que él atiza e ilumina. De aquí esa gracia tan suya que aguanta toda la arquitectura del libro; de aquí esa erudición cargada de amenidad; de aquí ese centenar largo de páginas escritas como quien no quiere la cosa, para que la cosa sea más suya. Sépase de una vez: Barrios no hace un libro antigitano; lo que realiza es una crítica agudísima contra los gitanófilos, entre los que se esconde buena parte del «señoritismo intelectual, que ha encontrado un filón para su rebeidía sin causa, para su actitud contestataria por demás inoperaníe»; evidentemente, estos gitanófilos a ultranza se nos vienen como cargados de mala conciencia y, en definitiva, con una actitud igual, aunque de signo contrario, que la de los gitanófagos: racista, o, lo que me parece peor, de un señoritingo y señoritesco paternalismo. Y Barrios nos aclara al final del ensayo rotundamente su intencionalidad y criterio: «En modo alguno pretendo un alegato contra los gitanos (...) El gitano está discriminado, sí; pero sólo en la medida en que están discriminados, en España, todos los humildes, que, desde luego, no son iguales ante la Ley, ni son tratados con equidad. El gitano, encerrado en su caparazón racista, tiene todos los derechos —faltaría más—, pero convencerlos de que están sujetos a todas las obligaciones supone para ellos un atentado contra las peculiaridades de su pueblo. El gitano está discriminado, sí, pero no más de lo que podamos estarlo todos cuantos, ajenos a las tentaciones del Dinero o del Poder, no vendemos, por nada ni por nadie, nuestra parcela de independencia, de libertad». Aquí podríamos dar por concluida la reseña de este libro, circunscribiéni d o l a al soporte ideológico —en definitiva,!o verdaderamente i n t eresante— de la entrega; no obstante, su abundancia de referencias al cante flamenco —las más del volumen—, y el hecho de que esta revista sea exclusivamente de temas flamencos, nos obligan con agrado a ocupar-nos de ellas. ¿Cuál es el criterio actual de Barrios en las relaciones gitanas-cante? Con el riesgo de incidir, de algún modo, en lo dicho, estimo que el escritor sevillano no niega ni las aptitudes ni las aportaciones gitanas\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_03 | trigger=\"título\"]\n\nsi centenar de citas contrarias a la paternidad gitana del cante, o el interesantísimo vocabulario final en el que se exponen una serie de palabras de germanía consideradas como propias del lenguaje caló, e, incluso, su propia apreciación de la teoría andaluzí de Blas Infante. Puede que esta crítica, mejor, que estos resultados de mi lectura del libro, no sean concordes con los más de los lectores. A éstos me permito aconsejarles que repasen el título de la obra, «Proceso al gitanismo» —ya se sabe, el ismo como modo— y no proceso a los gitanos. No es a los gitanos a quienes Barrios sienta en el banquillo de la Historia, sino a los gitanistas del cante. Estos, a mi parecer, son los criterios de Manuel Barrios y por ello el proceso, aunque el autor es consciente de que «el caso no tiene solución». Y, precisamente, por la necesaria búsqueda de esa solución; porque el libro puede y debe influir\n\n[ENDING CONTEXT]\n\nlos nuestros, en los que se impone un conocimiento —y no definiciones, aunque parezca algo perogrullesco— de la esencia-lidad andaluza, del alma de Andalucía, son necesarios no pocos procesos sobre temas y conceptos con patente de corso más peligrosos y nocivos que los «pandereteros»; y entre estos temas, Barrios con el libro que reseñamos ha dado el primer paso, uno de los más sobresalientes es el de las etnias y culturas que han configurado el ser andaluz; por ello y desde aquí, una vez más, confiamos en ver impresa la noble y hermosa teoría del mes-tizaje de mi admirado Manuel Andújar.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1154,
    "article_char_count_full": 6841,
    "article_char_count_review": 4880,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "crítica"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "título"
      }
    ]
  },
  {
    "article_id": "1980-09-15-right-julio-romero-de-torres-y-la-copl",
    "article_text_for_review": "propósito del Centenario\n\nSi mal no recuerdo, un crítico del primer tercio de siglo resumía los amores todos de Julio Romero en la copla, no en otra cosa; algo que, a mi parecer, no sólo fue un juicio válido en su época, sino, incluso, que se acrecienta en nuestros días redoblando su valoración.\n\nNo nos quedan dudas, de entre los pintores modernistas, tan dados a la representación plástica del flamenco, Romero de Torres es quien ahonda en la esenciabilidad de lo jondo. Por ello, ya casi al filo del I Centenario de su nacimiento y cincuentenario del fallecimiento, pienso que, si bien la historia del arte podría pasar, no sin cierta injusticia, por el desconocimiento de la obra del pintor cordobés, de ningún modo la cultura andaluzay, muy en especial, la flamenca, pueden olvidar su importancia. Y aquí en el Sur y, en concreto, en el mundillo flamenco, se suele tener más aprecio por lo que brilla o truena que por lo que pesa y deja poso. Las cosas.\n\nDe sobra es conocida la pasión cantaora de Julio Romero. Agustín Gómez nos ha recordado las actuaciones casi profesionales que, como cantaor, realizara el artista cordobés en su juventud por los cafés de cante de La Unión, de lo que, como de tantas otras cosas, la copla guardaría imborrable recuardo:\n\n«Del alto cielo y sin guía yo vi bajar un lucero que en altas voces decía: Ya se despide Romero; me voy para las Herrerías».\n\ny esta pasión juvenil la mantendría en su intensa pero no larga vida; recordemos su sentimiento de ser una especie de cantaor frustrado, su presencia en los más reputados concursos —así, fue presidente del celebrado en 1925 en el madrileño Teatro P a v ó n—. Su amor por la copla fue tal, hasta el punto de llevarle a confesar en un derroche de sinceridad, que prefería haber sido Juan B r e v a a Leonardo de Vinci.\n\nPero no vamos a insistir en el prestigio que, como aficionao, tenía el pintor de «Cante hondo»; tampoco vamos a señalar, por ser sobradamente conocido, sus retratos de artistas flamencos de la época: Dorita la Cordobesa, Pastora Imperio, La Argentinita, etc., etc.; simple y someramente dejaremos constancia de que en numerosísimos lienzos suyos se encuentra una interesantísimay personal simbología plástica del cante jondo. Simbología esta, si quieren, altamente idealizada, pero en la que, ciertamen-\n\nte, está patente la esencia de la copla. ¿Cómo entender «Cante hondo»; no se congregan en él con firme presencia los temas y las laceradas motivaciones de la copla: el amor y los celos, la pena, las pasiones, la religión y la muerte? ¿No es «La consagración de la copla» la más rotunda afirmación de que lo jondo es privativo de todo el pueblo andaluz, así como una casi panteísta divinización del cante, al igual que en «Nuestra Señora de Andalucía»? Pero no es sólo una simbología idealizada del ser de la copla lo que encontramos en muchísimos de sus cuadros (de lo que por razones de espacio ofrecemos tan contados como representativos e j e mplos); la copla misma en la precisa forma, incluso, músico-vocal en que se configura tiene su fiel expresión pictórica, véase, pongamos por caso, «Carceleras», o «Alegrías», donde, como muy bien señalara Francisco Zueras, a quien seguimos: «supo conjugar admirablemente el movimiento —de la figura central de la bailaora—, con lo estático, representado esto en la aquietada expresión de el guitarrista,\n\nsimbolización de las alegrías cordobesas, tan matizadas por la sobriedad».\n\nQueden aquí estas escasas líneas que pretenden ser algo más que una precisa llamada en fechas tan señaladas; sirvan, sobre todo, para dejar constancia, una vez más, de que el cante jondo no puede entenderse a estas alturas sin esa cultura específica y privativamente suya que arrastra. Porque Andalucía, como con agudeza señalara hace medio siglo Pedro Salinas, como tiene un cante jondo, posee una cultura jonda; algo, a mi entender, indisoluble.",
    "title": "Julio Romero de Torres y la copla",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 655,
    "article_char_count_full": 3887,
    "article_char_count_review": 3887,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-09-16-right-quienes-fueron-los-maestros",
    "article_text_for_review": "DON ANTONIO CHACON\n\nNació en Jerez de la Frontera en el año 1865, y en su infancia fue aprendiz de zapatero, -oficio de su padre - y, después, tonelero. Empujado por su amistad con Javier Molina, el que fuera magistral tocaor, y gracias a su gran afición, se lanzó a la profesión de cantaor. Cuando aún no contaba 16 años actuó en el café cantante de la Vera Cruz que regentaba en Jerez el cantaor Juan Junquera. Posteriormente, al cerrarse el mencionado local, Don Antonio, con Javier Molina y un hermano de este último, de nombre Antonio, que fue bailaor, realizaron un peregrinaje por pueblos de las provincias de Cádiz, Sevilla, Badajoz y Huelva, peregrinaje que hicieron en gran parte a pie. Este recorrido fue muy positivo para Chacón, sobre todo, cuando en Huelva se encontró con el cantaor Salvaoriyo (cantaor de Jerez, muy largo en repertorio, que pasó gran parte de su vida en Sevilla, donde cantó con Silverio. Se cree que nació antes de la mitad del siglo XIX) quien entusiasmado por sus facultades y estilo, estuvo casi un mes aleccionándole. Volvió de nuevo a Cádiz, donde actuó en el café cantante de verano del Peregril, alternando con Enrique el Mellizo, quien le enseñó a cantar por malagueñas. Poco después fue llevado a Sevilla por Silverio Franconetti a su café, donde Chacón se consagró definitivamente. Silverio debió influir profundamente en el y gran parte de su vida actuó con Chacón en los cafés cantantes andaluces y madrileños.\n\nIndudablemente fue Chacón el heredero de Silverio Franconetti, cuyo «enciclopedismo» supo asimilar y mantener. Pero si bien el maestro sevillano era especialista en las siguiriyas, Don Antonio se inclinó más por las malagueñas y cantes levantinos.\n\nSegún Julián Permartín: «Fue Chacón un cantar portentoso y genial, con una voz muy hermosa, dotada además de un falsete prodigioso con que enriquecía asombrosamente algunos cantes andaluces. Recreó las malagueñas y también los caracoles, fue insuperable intérprete del mirabrás, la media granaina y la cartagenera, sin que esto quiera decir que no dominara los otros cantes, incluso los más grandes y difíciles».\n\nChacón reunió en su persona cualidades notorias de honradez, de señorío innato, de elegante apostura, desprovista empero, de afectación o engrimiento. La corrección de sus modales y el comportamiento con los aficionados y compañeros de\n\nHacia 1890 se trasladó a Málaga, al café de Chinitas y, desde aquí, a Madrid, donde residió hasta su muerte, ocurrida el 21 de enero de 1929. Durante su residencia en Madrid, Don Antonio se trasladaba a los más diversos lugares, incluso Hispanoamérica, donde fueron muy admiradas sus actuaciones.\n\nprofesión le valieron para que se le fotorgara el título de don, tantas veces nombrado, que llegó a sustituir al apellido, pues era casi siempre llamado tan sólo Don Antonio.\n\nSi los cafés de cante encontraron su símbolo en Silverio Franconetti, la época teatral fue centrada por la poderosa personalidad de Don Antonio Chacón. Según Ricardo Molina: «La intención de realizar socialmente el flamenco que animaba a Chacón dio un resultado artístico negativo. El peligro que en los cafés amenazaba, lo potenció en grado sumo el teatro, y tal Pigmalión, el mismo Don Antonio fue destruido por su propia obra. En los últimos años de su vida, hemos sido testigos de su fracaso en teatros andaluces, donde el público mayoritario prefería a los entonces maestros nuevos del fandango». Y sigue diciendo Ricardo Molina: «Pocas personas de mi generación habrán tenido el Privilegio de oír cantar a Chacón personalmente, como le oí yo, a los 12 años, en el Duque de Rivas, en un espectáculo flamenco donde fueron más aplaudidos que el viejo maestro, los astros nuevos que se llamaban Manuel Vallejo y José Tejada «Niño de Marchena». Quiero Recordar que Chacón y Vallejo cantaron medias granainas y gustaron más las de Vallejo, quien estaba entonces en la plenitud de sus facultades».\n\nLo que ganó cantando - más que nadie en su época - se lo gastó escuchando cantar. Se sabe que siempre que podía, se trasladaba a Jerez de la Frontera para poder oír el gitanísimo compás de Curro Frijones. Don Antonio era consciente de que por siguirias, martinetes, soleares y bulerías - cantes perfectamente dominados por él - era superado por algunos intérpretes gitanos, de ahí que aplicara sus geniales facultades a los fandangos, granainas, medias granainas, malagueñas y carta generas, engrandeciéndolos todos ellos.\n\nEl cancionero flamenco, la poesía popular, el cuplé andaluz, la poesía culta, cantaron y cantan a Don Antonio Chacón, que ha sido el artista más celebrado. Tomás Borrás le dedicó un conocido poema que recoge bastante bien el carácter del maestro:\n\n«Es la hora de Chacón. La madrugada lívida como una ahogada, llega, es puntual, a las cinco, a la cita del viejo cantaor. Ya no hay gente, ya están solos la vida, la pena y los amigos. Don Antonio Chacón bebe en un «reservado» con la Rita, Montoya y el largo Fosforito y un famélico grupo de escorias de personas que esperaban afuera en la calle, ateridos, titiríti de helada, pidiendo a los curdelas y royendo pan duro con dientes amarillos. Don Antonio Chacón se ganó unos billetes y ahora lo paga todo, con rumbo y señorío: alquila para ello su garganta sonora; para eso, vendiéndolo, prostituye su espíritu. Los espejos humanos son flamencos y cantan. Don Antonio Chacón les paga -como a él- por oirlos. Don Antonio Chacón que es el Papa del cante va a celebrar con ellos, sacerdotes del rito... Selecciona: Rafael Valera\n\nPor último, dejar constancia de que la Cátedra de Flamencología de Jerez, conmemoró el centenario de su nacimiento dedicando a Chacón su tercer curso de arte flamenco, en el que intervinieron destacados flamencólogos y cantaores, llevando los trofeos que simbolizan los premios nacionales de flamenco, en la citada ocasión, el perfil en bronce del gran cantaor «payo» de Jerez.",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 973,
    "article_char_count_full": 5906,
    "article_char_count_review": 5906,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
