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
    "article_id": "1991-09-15-right-por-carceleras",
    "article_text_for_review": "Paco V. Vargas\n\nPrimer tercio\n\nA las seis en punto de la tarde un funcionario —bigote y porte militar— sale de la prisión, nos saluda y nos recuerda las estrictas normas de seguridad que rigen el lugar adonde vamos a entrar. Después entramos al gran patio que da acceso adonde están los presos —curiosamente la animada charla de afuera se convierte en cómplice silencio adentro—. Los guardias civiles, sentados a uno y a otro lado de donde permanecemos de pie, nos miran con extrañezas y algo de desconfianza: allí nadie se fía de nadie.\n\nAl escuchar el primer cerrojazo me da jindama; pero también siento una curiosidad morbosa por saber lo que voy a encontrar detrás. Allí, detrás, hay juventud —algunos casi niños—, viejos —muy pocos—, güena gente, hijos ilegítimos de la suerte, lumias, bujarrones, mala leche, marginación, tontos, listos, sinvergüenzas, rabia, miseria..., injusticia. Es decir, como la propia vida. También, como ocurre afuera, algún que otro buen cantaor.\n\nEn la lista de participantes que se nos entrega a los miembros del jurado leo nombres con apellidos ilustres: Francisco Moreno Maya, Ana de los Santos Bermúdez y Antonio de los Santos Bermúdez. Vienen de casi todas las cárceles de España, aunque abundan las andaluzas, y casi todos son de raza gitana. También, casi todos, intentan cantar como Camarón —jalguno hasta jalea a Paco y Ramón, aunque el tocaor es Rafael Trenas!\n\nDe los veintisiete clasificados sólo han venido diecisiete. Desconocemos razones de tan numerosa ausencia, pero alguien apunta a la burocracia como la presunta culpable de que esos \"carcelera\" (vol. sola) (sin modida) (vol. salida) (vol. texto) diez hombres y mujeres no estén con nosotros.\n\nSon las 7,30 de la tarde. El educador, Antonio Estévez —alma máter del concurso dentro de la prisión—que hace de secretario del jurado llama al primer concursante. Así uno tras otro, sabiamente asesorados por Rafael Trenas, el guitarrista de apellido carcelario que ha estado ensayando con todos ellos durante casi un mes para poder meterlos en verea, hasta los ocho previstos para el primer día.\n\nDentro del salón de actos —construído con materiales de segunda mano por los internos de la prisión— me llama poderosamente la atención la relacion de amistad que mantienen un preso y un gorrioncillo. Este revolotea fugazmente por la sala e inmediatamente vuelve sobre el hombro de su amigo que, a cambio, comparte con él la comida dándosela de su propia boca.\n\nYo, emocionado por la escena, recuerdo aquella preciosa copla que cantara por rondeña Rafael Romero:\n\nCogí un pájaro de un nió p'acabarlo de criar. Y fue tan agradecido que, cuando lo eché a volar, se vino hacia el hombro mío.\n\nDe nuevo en la calle, libres ya del agobiante calor que hace dentro de la cárcel, sentados en la terraza de un bar próximo, un preso —de los de régimen abierto— y Agustín Gómez hacen alardes —medio en broma, medio en serio— de sus conocimientos operísticos. La porfía la acaba ganando claramente el crítico de Flamenco. (Por cierto que ha sido el único que ha seguido el concurso durante sus tres días, a pesar de la abundancia de medios de comunicación que han estado presentes).\n\nSegundo tercio\n\nTercer tercio\n\nLa deliberación del jurado es larga. Solamente cuatro pasarán a la gran final. Después, dada la calidad y el bien hacer que se aprecia, la organización —a propuesta del jurado— decide ampliar el número de finalistas hasta siete, creando tres accésits.\n\nA las diez de la mañana estamos citados en la puerta de la cárcel. Como el día anterior se vuelve a repetir, con exactitud matemática, el proceso de entrada a la prisión. A todos se nos nota más relajados, la conversación es distendida y afloran las bromas. Comenzamos puntualmente: a las 10,30 ya está el primer concursante sobre el escenario. Ocho más completarán el número total previsto para este día.\n\nLa opinión general es que hoy ha habido más calidad, sin olvidar a dos otros cantaores del día anterior. Al final es José Serrano Campos quien consigue el primer premio y esos treinta mil duros que, a buen seguro, le han de sacar de más de un apuro en este lugar donde el dinero es un bien escaso.\n\nA la gran final ampliada llegan Luz Divina Silva Jiménez, Antonio de los Santos Bermúdez, Sebastián Heredia Cádiz, José Serrano Campos, Juan José Heredia de los Reyes, Manuel Martin Povedano y Antonio Gómez Corrales. Se canta y se canta bien, unos mejor que otros; pero todos con el corazón, entregándose en cada tercio, sin dar ojana.\n\nResaca\n\nDespués de la entrega de premios, el ganador, junto al resto de concursantes, nos obsequian con un fin de fiesta a petición de la televisión que quiere grabarlos. Se canta y se baila por bulerías, tan bien que nos hacen olvidar que estamos entre rejas. Pero no, un pitido de sirena, cansino y feo, nos recuerda que aún seguimos dentro. Ella pone el punto y final. Podría parecer más lógico llamar remate —por más flamenco— o epílogo —por más ortodoxo— a estas líneas finales, pero las titulo resaca porque, como ella, tienen un sabor amargo. Sabor a retama y tuera, sabor a injusticia aceptada por todos los que estamos afuera.\n\nPero aquéllos y éstos somos hombres y mujeres nacidos iguales. La fiesta continúa.\n\nPorque entre esas mujeres y esos hombres que viven privados de libertad y estas mujeres y estos hombres que la disfrutamos, tal vez sólo medie un instante, unas circunstancias, un barrio, una familia, una vida, un momento de locura, una pasión, unas copas de más, el hambre, el lujo que nos muestran para que no lo cojamos, una madre... Quién sabe.\n\nO'Donnell, 3, 4.° Piso Teléfonos (954) 22 20 58 y 21 69 20 SEVILLA\n\nJ. A. PULPON Espectáculos Internacionales\n\nParticular: Teléfono 27 80 78",
    "title": "Por carceleras",
    "periodical": "candil",
    "issue_id": "1991-09",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 964,
    "article_char_count_full": 5705,
    "article_char_count_review": 5705,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-09-16-right-el-jaleo-fue-madre-de-la-solea",
    "article_text_for_review": "El autor contesta a la pregunta que le hace un aficionado, lector de esta Revista.\n\nQue el «jaleo» fue la madre de la soleá, lo dijo, por primera vez, hace treinta y ocho años, el autor Julián Pemartín. Así, pues, no es cosa de hoy. No es invención reciente ni fruto de ninguna investigación.\n\nPara mí, lo manifestado por el autor, es un despropósito imper-donable.\n\nCon toda sinceridad le diré, amigo, que el «jaleo» no existió ni existe en la actualidad como cante en Andalucía. Como su propio nom-¡Arsa!, ¡Toma!, ¡Eso es!, ¡Olé! ¡Así se canta!, ¡Venga ya!, ¡Como tú lo haces, no lo hace nadie! ¡Ay qué bien, ay qué bien!\n\nbre indica, sirvió para jalar a quienes bailaban y cantaban por fiesta en las ciudades de Cádiz y de Jerez, y ese «jaleo» lo producían aquéllos que escuchaban, animando con estas y otras exclamaciones, cuando los jaleadores se hallaban en la plenitud de su éxtasis: El extinto señor Permartín, nos dijo: «No hay que confundir el nombre de «jaleo» con el del baile que iba acompañado de cante, de compás ternario, que se practicaba en tiempos antiguos en Cádiz y en Jerez y que pudo dar origen a la Soleá».\n\nYo le diría: ¿Pudo dar origen a todas las escuelas por soleá? ¿A las de Triana, Utrera, Alcalá, Jerez, Cádiz, Los Puertos y Córdoba? ¡Eso no es posible!\n\nLos cantes por fiesta iban, casi\n\n«El jaleo», óleo de John Singer Sargent\n\nsiempre, acompañados de baile y de un jaleo enorme (escuchad la bulería grabada por Manuel «Torre», acompañado a la guitarra por Miguel Borrull hijo y veréis cómo el artista interrumpe su actuación para decir a una jaleadora que no dé tantas voces).\n\nEl profesor, Sánchez Romero, nos dice en su obra «La Copla Andaluzia», «...que los Tangos, Tangguillos y las Alegrías son cantos por alegrías para bailar acompañados de guitarra y de su correspondiente jaleo».\n\nDespués de lo expuesto le diré, que si nos atenemos al significado del vocablo «jalear», según el diccionario que he consultado, no hay lugar a duda en cuanto a que el «jaleo» no existe como cante. El Diccionario dice textualmente: «Jalear es animar o excitar con palmas, ademanes y expresiones a los que bailan y cantan».\n\nY por último, he aquí lo que dice don Francisco Rodríguez Marín:\n\n«…lo forman tres figuras inseparables: Una, el que canta. Otra, el que baila. Y la otra, el que murmura. (Yo hubiera dicho el que jalea)».\n\nEn cuanto a lo que me dice respecto del «jaleo» extremeño, permí-tame que le conteste con toda sinceridad que de cantes extremeños no entiendo una patata. Y la verdad es que no me avergüenzó de mi ignorancia porque toda mi juventud la pasé buceando entre Triana, La Alameda y La Campana, donde convivía con Francisco González Sanromán, Manuel Jimé-nez Martínez de Pinillos, Rafael Ramos Antúnez, Los Pabones, José Torres Garzón, Rafael Pareja Majarón, Manuel Jiménez Centeno, Manuel Infante Martín y su hermano «El Chato».\n\n¿Cómo podría yo encontrar en Badajoz esas extraordinarias figuras del saber flamenco? ¿Cómo podría yo aprender en Badajoz lo que aprendí en la tierra de mis mayores junto a los artistas citados? No hubiera podido. ¿Por qué? Pues sencillamente porque en Badajoz, si exceptuamos a don José Porras (maestro y padrino de Porrinas) y a don Antonio Martínez Pirón, que en paz descansen, ambos excelentes aficionados de las décadas de los veinte y de los treinta, los demás eran individuos que decían —por decir— saber de cantes, pero que la verdad era muy distinta: entendían de cantes tanto como yo de perdices «encintas».\n\nTermino recomendándole que haga caso omiso de tantas y tantas cosas como se dicen hoy del cante. No se asesore de Yerga Lancharro ni de nadie. Acuda únicamente a los cantaores profesionales de alta categoría, tales como Fosforito, Menese, y también a los jóvenes estudiosos, Calixto Sánchez y Luis de Córdoba, por ponerlos como ejemplo.\n\n¡Ah!, señor Romero, no se extra- ne de mi desconocimiento de los cantes extremeños, aunque supong go que sabrá que estudié a fondo los de mi paisano «Pepe el Molinero», y fui yo precisamente quien catalogó su taranta como personal.\n\n¿Quiere que le diga que nunca tuve un solo cante de Porrinas, y que después de fallecido es cuando en pocos meses he logrado reunir, para mi archivo, casi todo lo que grabó? ¡Pues así es! Nunca mejor dicho: «en casa del berrero...».",
    "title": "¿El «jaleo» fue madre de la soleá?",
    "periodical": "candil",
    "issue_id": "1991-09",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 736,
    "article_char_count_full": 4299,
    "article_char_count_review": 4299,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-09-18-left-xxvii-concurso-nacional-de-taran",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n¿Están en desuso los concursos? A esta pregunta se podría contestar con cierta ambigüedad aduciendo que para la gran mayoría de los artistas sí, mas no para un determinado colectivo de cantaores que suelen ser asiduos a los mismos hasta que alguno de ellos consigue el galardón deseado.\n\nS in embargo, este último grupo de cantaores se va reduciendo año tras año, ya que los nuevos valores que se podrían ir incorporando al mismo se están encontrando con unas determinadas facilidades para mostrar su arte con las que no pudieron contar los primeros.\n\nPor otro lado, la fecha de celebración del concurso linarensa casi a finales del mes de agosto, motiva que muchos de los presuntos participantes mantengan en su agenda una serie de contrataciones, aunque posiblemente bajas, lo suficientemente\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"participación\"]\n\nan en su agenda una serie de contrataciones, aunque posiblemente bajas, lo suficientemente atractivas como para no arriesgar —por muy elevado que sea el premio y su trascendencia— su «pájaro en mano». Ante esta serie de adversidades, los organizadores y patrocinadores de concursos y muy concretamente el de tarantas linarensa, se encuentran en la tesitura de, año tras año, incrementar su imaginación en la búsqueda de incentivos para acrecentar la participación de concursantes. La edición de este 91 ha tenido una elevada cuantía en premios, lo que ha supuesto una mayor y más variada participación que ha incrementado la calidad del certamen anterior pero que no ha conseguido la necesaria para seguir dignificando una convocatoria como la que nos ocupa. Y conste que los esfuerzos realizados han sido intensos, mas el reclamo sigue sin fructificar lo suficiente para conseguir el fin deseado. El jurado de esta edición estuvo compuesto por don Ignacio Ortega Campos, concejal delegado de Cultura; don Francisco González Ramírez, colaborador de Sevilla Flamenca y crítico de Radio Palma; don Francisco Zambrano Vázquez, presidente de la Federación de Peñas Extremeñas; don Gonzalo Rojo Guerrero, crítico de Radio Nacional de España en Málaga, y don Juan Cardeñas Cantudo, representante de la Peña Flamenca «Cabrerillo». Como secretario del mismo actuó don Julián Mesa Ciriza. La celebración del concurso tuvo lugar los días 22, 23 y 24 de agosto y en el mismo se homenajeó a la cantora local Carmen Linares, la cual no pudo estar presente por enfermedad. La primera jornada nocturna y pública —hubo dos f\n\n[ENDING CONTEXT]\n\nvoz flamenca y jondura, cantó por siguirias con inclinación hacia Jerez y una magnífica evocación de Curro Dulce. Anteriormente, su enamoramiento por los ecos caracoleros quedaron patentes en una zambra inicial. Su personalidad flamenca se mostró por cantiñas-alegrias, bulerías y tangos con fases de auténtica gracia y compás y algo de forzamiento en los remates. El doble acompañamiento realizado por Paco Cortés y Juan Alcalá no le favoreció en estos tres últimos estilos. Las guitarras oficiales de Paco Cortés y Juan Ballesteros demostraron su valía y habitualidad para este tipo de trabajo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "XXVII Concurso Nacional de Tarantas Rafael",
    "periodical": "candil",
    "issue_id": "1991-09",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "17-19",
    "page_number": 17,
    "word_count": 1058,
    "article_char_count_full": 6737,
    "article_char_count_review": 3236,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "participación"
      }
    ]
  },
  {
    "article_id": "1991-09-19-right-el-cantar-del-arriero",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nApéndice del relato vivencial que trataba de pregones y fandangos malagueños, donde se explica el porqué del alias de Juan Jiménez Soler —«Juanillo el loco»— y donde se trata de buscar el eslabón perdido de los fandangos «abandolaos».\n\nP or razones familiares y para satisfacción de sus descendientes, tengo la obligación moral de corregir un «lapsus calami» que cometí en mi relato anterior. Juan Jiménez Soler —«Juanillo el loco»—, el apelativo con que se le conocía en las playas de San Andrés, en el Bulto malagueño y perchelero, fue debido a su valentía y arrojo en momentos difíciles para salvar una situación difícil y comprometida, tanto en la mar como en tierra. Sus pies hollaron las arenas de las playas malagueñas, descalzo por el rebalaje, desde La Carihuela al Rincón de la Victoria.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nero, redero, atarazanero, calafate, etc., todos sus oficios tuvieron un denominador común: la mar. Si, en mi anterior relato, afirmaba la veracidad de cuanto contaba, a partir de estas líneas no puedo decir lo mismo. Lo que transcribo a continuación es producto de la lectura de algunos libros que poseo y que de antiguos, ya ni me acordaba. Empecemos, pues, la búsqueda de un eslabón perdido, que quizás en un próximo trabajo nos puede deparar una gran sorpresa, para lo cual es necesario el desarrollo del presente artículo. Cuentan Charles Davilliers y Gustavo Doré en su libro «Viaje por España» —París, 1874— que una calurosa tarde de estío, asistieron en un patio de la calle Granada, a una fiesta prologada por una tertulia, en la que bailaron la «Malagueña del torero» y el polo «Del contrabandista». A continuación se interpretaron con acompañamiento de guitarra, esas coplas tan preciosas conocidas en Andalucía como «malagueñas». Y transcriben la letrilla de la malagueña, que dice así: Adiós Málaga la Bella adiós Málaga que sí tierra donde yo nació para todos fuistes madre y madrastra para mí. Davilliers y Doré permanecieron algún tiempo en Málaga —año 1862— contando que también escucharon de cantar por carceleras, cañas y playeras. Deciden seguir el viaje hasta Ronda, pero dando un gran rodeo: por la costa oriental, camino de Vélez-Málaga, ciudad muy elogiada por los viajeros. Tras proveerse de caballerías y de un arriero que les servía de guía, emprenden camino hacia la Sierra Tejada y por abrup- tos caminos frecuentados por arrieros y bandoleros, llegan a la granadina ciudad de Alhama. Se instalan en una vieja y ruinosa posada, apropiada a los tipos que frecuentaban estos caminos y prosiguen después su viaje hasta Loja. Después de permanecer en Loja, siguen la ruta por los caminos serranos hasta Antequera. Durante estos proyectos, comentan Davilliers y Doré, mil y una historias de bandoleros y contrabandistas, señalando incluso los lugares donde hubo cualquier ejecución o víctimas de peleas enterradas. De Antequera parten hacia el macizo de la Serranía de Ronda, ciudad a la que llegan por agrestes y peligrosos caminos. En Ronda, en una noche de estío, escuchan de cantar al son de la guitarra por rondeñas, de la que incluyen algunas letrillas. Vbgr.: Los ojos de mi morena se parecen a mis males los ojos de mi morena, grandes como mis fatigas negros como mis pesares. Parten de Ron\n\n[ENDING CONTEXT]\n\nescucharon en un patio de la calle Granada, de Málaga, los dos viajeros franceses: Davilliers y Doré. Pero... ¿fueron realmente «malagueñas» de cante lo que escucharon nuestros visitantes? Y «playeras»... ¿por qué? ¿Quizá porque las escucharon en las playas cantadas por marengos y jabegotes?\n\nDejémoslo para otra excursión por el valle del Guadalhorce, en busca de la partida de nacimiento de las malagueñas de cante.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al Mérito del Trabajo)\n\nRecepción diaria de MARISCOS Y PESCADOS ESPECIALIDAD EN ASADOS\n\nROLDAN Y MARIN, 7\n\nJ A E N\n\nTELEFONO 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El cantar del arriero",
    "periodical": "candil",
    "issue_id": "1991-09",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1283,
    "article_char_count_full": 7762,
    "article_char_count_review": 4046,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "1991-09-21-left-por-siguiri-as",
    "article_text_for_review": "Por siguiriγas\n\nF lujo tiránico cuya esencia compacta inunda el compás en brazos de mares y remos abandonados.\n\nHueco desolador en la cantera melódica; el musgo no aflora por el umbral del hogar vulcanio.\n\nSoplos ardientes volutas estiradas tragico travestí...\n\nSe escapa de la inagotable rojez incineral el arménico incienso en ofrenda en difraz en Seguiriya.\n\nFrancoise Gérardin",
    "title": "Por siguiriγas",
    "periodical": "candil",
    "issue_id": "1991-09",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 58,
    "article_char_count_full": 380,
    "article_char_count_review": 380,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
