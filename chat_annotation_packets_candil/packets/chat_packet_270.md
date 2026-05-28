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
    "article_id": "1993-03-9-left-antonio-ranchal-y-alvarez-de-sot",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAl comienzo de mi interés por el arte flamenco, cuando trasteaba los casilleros discográficos flamencos de la discoteca de RNE, siempre μελlamaba la atención el largo apelativo artístico de nuestro entrevistado, apelativo en el que se configura su identidad personal. Por otro lado, también me había imaginado a un personaje de leyenda aristocrática que había querido mostrar, con su participación en el Concurso de Córdoba del cincuenta y nueve, que el flamenco era apreciado y escuchado en todos los estamentos de la sociedad. Cierto es que desde aquel tiempo de trasteador a nuestros días han transcurrido veintisiete hermosos años. Hoy, con la madurez que da el tiempo y con la humildad que se va adquiriendo conforme se aprecian y asumen las matizaciones de los buenos aficionados, me encuentro\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\nmatizaciones de los buenos aficionados, me encuentro cara a cara con el personaje. De talante afable y educado, orgulloso de sus tiempos flamencos vividos, humilde y sentencioso en sus respuestas, reivindicador y enamorado de los cantes de su tierra, Antonio Ranchal responde a mis preguntas con la misma claridad que refleja su encanecida melena. ces vengo cantando lo que sé y lo que puedo. A partir de aquí mi dedicación se intensifica por este arte. En el año 59 un grupo de amigos fuimos a Córdoba a tomar unas copas y coincidió con el Concurso de Arte Flamenco. Los amigos me animaron a que me —¿Cómo se produce su iniciación flamenca? —Pues como casi todo el mundo: por afición. Cierto que fue tempranamente, de niño. Había una muchacha que trabajaba en mi casa a la que le gustaba el cante y ella fue la que me aficionó. Desde enton- presentara y lo consiguieron. Cierto es que llegué algo tarde y que me dijeron «¿Pero hombre, ahora te presentas?», «Bueno, si puede ser...». «¿Usted a qué grupo se va a presentar?». «A todos». «Eso ya no puede ser. Aún queda el grupo séptimo, el de las granaínas, medias granaínas, fandangos de Lucena, Huelva y Almería». El premio lo conseguí. -¿A qué personajes conoció allí? —Conoci a Anselmo González Climent, Aurelio Sellé... A todos los que componían el jurado. Con quien tuve más contactos fue con Anselmo, que para mí ha sido un gran conocedor de este arte. También mantuve cierta relación con don José Carlos de Luna, ya que fue el asesor de la casa Osborne, casa en la que yo hice la antología del ca\n\n[ENDING CONTEXT]\n\nque grabar, que la casa Hispavox estaba dispuesta y que me convenía para mi carrera de artista. «¿Pero es que yo voy a ser artista?». «Que sí, que tal y que cual...». Y ahora fíjate, si yo me hubiera esmerado un poco más, hubieran salido mejor.\n\n-¿Tiene predilección especial por algún estilo?\n\n—Tengo mucha predilección por los cantes de mi tierra, Lucena, pero la soleá es el cante que más me gusta.\n\n—Me gustan todas las soleares. La de Córdoba tiene unos bajos de mucha categoría, y cuando puedo llevarla a feliz término me encanta cantarla. No es frecuente oírla con pureza.\n\n-¿La de Córdoba?\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antonio Ranchal y Alvarez de Sotomayor Rafael",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "8-10",
    "page_number": 8,
    "word_count": 1850,
    "article_char_count_full": 10630,
    "article_char_count_review": 3178,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  },
  {
    "article_id": "1993-03-11-left-viii-semana-de-estudios-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Valera Espinosa\n\nLa fecha clave del 16 de febrero de mil ocho-cientos noventa y tres, no podía pasar desapercibida ante un colectivo de aficionados laboriosos que se agrupan entorno a la Peña Flamenca de Jaén. La dedicación de la VIII Semana de Estudios Flamencos a conmemorar el Centenario del nacimiento del insigne cantaor sevillano Tomás Pabón Cruz, es muestra de la sensibilidad, conocimiento y oportuno reflejo de la historia que aporta el citado colectivo. Con la serie de actos programados en la citada Semana de Estudios Flamencos, la Peña jiennense creyó muy conveniente reconocer los méritos de Tomás Pabón Cruz.\n\nDía primero: Jaén y su protagonismo flamenco Este que les relata intenta dejar patente el intenso caudal de artistas flamencos que ha parido nuestra tierra con la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"estudiosos\"]\n\nense creyó muy conveniente reconocer los méritos de Tomás Pabón Cruz. Día primero: Jaén y su protagonismo flamenco Este que les relata intenta dejar patente el intenso caudal de artistas flamencos que ha parido nuestra tierra con la charla «Aproximación al flamenco jiennense». Una exposición —y perdonen mi ¿osadía?— en la que también quiero reflejar determinados aspectos artísticos y sociológicos —siempre basándome en las investigaciones de los estudiosos— que puedan llevarnos al posible convencimiento del nacimiento de estilos mineros en la cuenca Linares-El Centenillo-La Carolina. Como el primer tercio de este siglo se erige en la consagración de artistas de la tierra como Personita de Linares, La rubia de las Perlas, Basilio, La Niña de Baeza, el ubetense Juan Barba, el galduriense Antonio Rubio «El Calaco», La Niña de Jaén o los quesadeños hermanos Revuelta. La ilustración cantaora la puso Manuel Pérez Mesa «Canalejas hijo», que en su línea habitual —la evocación de su progenitor— cantó por tarantas, soleares, fandangos de Lucena y bulerías. El acompa- VIII Semana de Estudios Flamencos Homenaje a Tomás Pabón Cruz en el centenario de su nacimiento Peña Flamenca de Jaén ñamiento guitarístico lo ejerció el jiennense Paco Aguilar. Día segundo: Noche de cabales con Manuel Mairena El sufrimiento, el estado de ánimo entristecido, la predisposición a la queja o el ansia de compartir el dolor, son elementos emocionales que propician la entrega cantaora de un artista que se aferra a los estilos más jondos del flamenco. La receptividad de los cabales aficionados ante unos ecos reposados y gitanos, matizados de compás, reforzaron un entorno donde el cantar pudo transmitirnos su personal arte. La reciente muerte de Curro Mairena, y el sentirse el último de la casta del fragüero Rafael, evidenciaron la sensibilidad espiritual y artística de un cantaor: Manuel Mairena. Noche de cabales porque el duende nos acarició hasta erizarnos el vello en momentos de álgido flamenco y porque nos encontramos con un artista que supo ser fiel reflejo de cómo hay que expresar el\n\n[ENDING CONTEXT]\n\ndel cantaor.\n\nLa clausura de esta VIII Semana y del XXII aniversario de la fundación de la Peña, tuvieron como colofón el desenfadado y alegre cante de la gaditana Mariana Cornejo. Con un repertorio basado en los estilos festeros y en el recuerdo de personalismos relevantes de su tierra, como los de La Perla, Ignacio Ezpeleta, Antonio El Chaqueta, etc. Acometió primeramente los tientos-tangos y las cantiñas-alegrías con ciertas fases de apresuramiento. Cantó también por los estilos mineros, bulerías, fandangos, nuevamente por tangos, más bulerías y unos singula-res tanguillos de la tierra.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "VIII Semana de Estudios Flamencos Rafael",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 1282,
    "article_char_count_full": 8001,
    "article_char_count_review": 3712,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "estudiosos"
      }
    ]
  },
  {
    "article_id": "1993-03-12-right-cantes-de-levante-cantes-mineros",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJuan Ruipérez Vera\n\nDurante los muchos años que tuvimos la oportunidad de ser amigo del maestro de los cantes de Cartagena, don Antonio Piñana, siempre sacábamos a relucir un tema de conversación que, cuando lo poníamos sobre la mesa, sirvió para mantener largas charlas que, a veces, duraban varias horas; el tema: Los cantes de Levante. También es cierto que siempre estuvimos de acuerdo en lo referente a que esta definición, desde nuestro particular criterio, no era la más correcta y adecuada si con ella se quería recoger la extensa gama de cantes y estilos que, a lo largo del tiempo, habían manejado —con mayor o menor acierto— muchos de los importantes autores y conocedores del cante flamenco.\n\nTodo esto nos permitió —dado el interés del tema— escudriñar y analizar pacientemente esas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_01 | trigger=\"dentro\"]\n\ndefinición, desde nuestro particular criterio, no era la más correcta y adecuada si con ella se quería recoger la extensa gama de cantes y estilos que, a lo largo del tiempo, habían manejado —con mayor o menor acierto— muchos de los importantes autores y conocedores del cante flamenco. Todo esto nos permitió —dado el interés del tema— escudriñar y analizar pacientemente esas situa- ciones creadas —escritas— por diversos autores, comprobando que dentro de la cronología de estos cantes, y en su definición de Cantes de Levante, existen numerosas lagunas que no nos permitieron en- lazar los eslabones lógicos de tal nacimiento con dicha definición. Siendo ésta una de las razones por las que, junto al maestro Piñana, llegamos a tres conclusiones: a) Que llamar a determinados estilos Cantes de Levante es, con toda seguridad, un grave error, debido al confusionismo que la propia palabra Levante en sí misma encierra. b) Hacer referencia a un apelativo conocido como Cantes Mineros, no estando todos los posibles estilos en él fielmente contenidos, es —sin duda— multiplicar por dos el confusionismo a que antes nos referíamos; y c) Pensamos que el haber encuadrado y definido a varios estilos de cantes con el nombre genérico de Cantes de Cartagena, quizá, por lo novedoso, pudiera ser un tanto «revolucionario» dentro del marco y la Historia del Arte Flamenco (aunque, evidentemente, tal definición la asumimos porque ello —los cantes— responden a la propia historia musical autóctona del Campo de Cartagena). La palabra Levante Si hemos llegado a la conclusión de que la palabra Levante puede llevar emparejado en sí misma un posible confusionismo cuando esta acepción es asociada a los Cantes de Levante, tal aseveración se fundamenta en la descripción que la Real Academia de la Lengua Española\n\n[ENDING CONTEXT]\n\nles puede llamar Cantes de Levante, por derecho propio.\n\nComo ya hemos podido comprobar, hacer esto con los cantes autóctonos de Granada, Jaén, Almería o Málaga (tal y como han sido calificados y clasificados por determinados autores —y aficionados— de la bibliografía flamenca), es, sin duda, intentar crear un engorroso confusionismo (a río revuelto...), amén de caer, concienzudamente, en un craso error.\n\nAñadiendo, por último, algo más, decir que dentro de los Cantes de Cartagena, a solape de su genuina denominación de Cantes de Levante, convive el Canteminero, a saber:\n\nCANTES DE CARTAGENA\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cantes de Levante; cantes mineros; cantes de Cartagena Juan",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1892,
    "article_char_count_full": 12023,
    "article_char_count_review": 3427,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "dentro"
      }
    ]
  },
  {
    "article_id": "1993-03-14-left-cr-tica-y-flamencolog-a-aurelio",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA veces, leo o escucho una crítica de un artista determinado en distintos medios de comunicación, y mientras que es alabado en uno, apenas ha satisfecho al crítico de otro.\n\nOtras, suelo ir a un festival o a un concurso, y, a la mañana siguiente, leo o escucho una crítica que hace que me pregunte si el autor de la misma y yo, estábamos en el mismo recinto.\n\nNormalmente, un crítico debe ser hombre de aprendizaje y experiencia, por lo que debe merecer respeto. Siempre recordaré las palabras del inolvidable crítico de cíne Alfonso Sánchez, cuando le preguntaban que cómo podía saber tanto de cine y dónde lo había estudiado. A lo que contestaba él sin inmutarse: «calentando butacas...».\n\nTambién algunos críticos merecen un cierto grado de escepticismo al pecar de cierto aldeanismo o influencia\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"crítica\"]\n\ndebe merecer respeto. Siempre recordaré las palabras del inolvidable crítico de cíne Alfonso Sánchez, cuando le preguntaban que cómo podía saber tanto de cine y dónde lo había estudiado. A lo que contestaba él sin inmutarse: «calentando butacas...». También algunos críticos merecen un cierto grado de escepticismo al pecar de cierto aldeanismo o influencia de paisanje. Otros, por la rapidez con que el medio para el que trabajan les solicitan la crítica o el comentario, no realizan un verdadero análisis de lo visto y oído. Por eso, algunos, no se encuentran agusto escribiendo crítica, sino cuando escriben artículos no redactados contra-reloj, ofreciendo reflexiones verdaderamente interesantes sobre el cante o el toque, revelándose como auténticos eruditos al escuchar o comparar grabaciones antiguas con los cantes y toques actuales, así como revisar bibliografías y hemerotecas a fin de dar datos sobre la gestación de los cantes o toques que reseñan en su artículo. La crítica flamenca, como la crítica musical, rara vez tiene profesionales puros, es decir, personas que no se dediquen más que a eso. Por ello, existen también casos de simples aficionados sin conocimiento teóricos y técnicos que se dedican a esos menesteres. Decía Bernard Shaw: «El crítico debe recordar constantemente a su lector, que está leyendo la opinión de un solo hombre y que debe tomarla en lo que vale». Pero en el fla\n\n[ENDING CONTEXT]\n\nlo que está viendo o escuchando y que, a veces, causa en su ánimo emociones estéticas; para que sus juicios no resulten apasionados.\n\n3. El crítico debe velar por la pureza del flamenco, enjuiciando con dureza la inclusión de «armonías extrañas» en el toque, así como de instrumentos musicales que no sean la guitarra o cualquiera de percusión primitivo que no desvirtúe el cante o el toque —caja de madera, yunque, bastón, etc.\n\n4. El crítico de flamenco no debe entrar nunca en el empleo de palabras musicológicas y extraflamencas para enjuiciar la labor de un artista.\n\nTeléfono (953) 441028\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crítica y Flamencología Aurelio",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1827,
    "article_char_count_full": 10782,
    "article_char_count_review": 3032,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "crítica"
      }
    ]
  },
  {
    "article_id": "1993-03-16-left-desdramatizar-la-copla",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCuando los escritores sobre flamenco nos acercamos a este turbión sin fondo que es la copla jonda, normalmente quedamos aterrados ante el claroscuro de sensaciones fuertes, muchas veces insoportables para cualquier sensibilidad bien educada, ya que en ellas se nos habla de castigos y sufrimientos, humillaciones sin fin que han soportado por igual comunidades tan distintas como el pueblo gitano o esa tropa callada de andaluces pobres que pueblan los campos, las gañanías o deambulaban oscuros por los caminos, intentando aproximarse a un destino incierto en el que sus vidas merezcan mejor trato, tocar con los dedos un porvenir que se les niega.\n\nSentaíto en la escalera, esperando el porvenir pero el porvenir no llega.\n\nCopla que, en su terrible desamparo, nos habla bien a las claras de esa\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"histórico\"]\n\nmpos, las gañanías o deambulaban oscuros por los caminos, intentando aproximarse a un destino incierto en el que sus vidas merezcan mejor trato, tocar con los dedos un porvenir que se les niega. Sentaíto en la escalera, esperando el porvenir pero el porvenir no llega. Copla que, en su terrible desamparo, nos habla bien a las claras de esa orfandad sin límites, de la autoconmiseración de nuestros cantaores ante la negrura incierta de un decurso histórico, que ellos presiden tan preñado de amenazas que no pueden concebir siquiera un respiro junto a los cauces sonoros, porque saben que la esperanza no se ha hecho para el pobre y pronto, muy pronto, las cañas se han de tornar lanzas: Arroyo, no corras más, mira que no has de ser eterno, que te ha de quitar el verano lo que te ha daído el invierno. Hasta el verbo «dar» se conjuga en esta ocasión partido por ese mazazo, aparentemente inocuo, del diminutivo, y es que dar, lo que se dice dar, hemos dado muy poco a estas comunidades primitivas que nos donaron, hace más de dos siglos, las músicas y las letras más estremecedoras del pueblo andaluz. Pero todo esto es sabido de sobra por los que se asoman con cierta frecuencia al hecho flamenco. Quizá por ello, debido al dramatismo que nosotros mismos hemos acentuado en las posturas vitales de nuestros artistas, y que resumía mejor que nadie la vieja sentencia de «Tía Anica La Piriñaca» («Cuando canto a gusto, me sabe la boca a sangr\n\n[ENDING CONTEXT]\n\no espinas que hagan más inhabitables esos arriates de flores que cantaba el anónimo poeta. No es el andaluz un pueblo al que agobie demasiado el día de mañana, ya saben, por si el oscuro porvenir no se presenta; pero, entretanto, su exquisito fondo vital se queda con el goce, con la carnalidad abierta y desbordante que promete paraísos increíbles para ser habitados al instante, si no se quiere caer en la imperdonable falta de oportunidad que denunciaba esta bulería, pletórica de desgarro:\n\nEl que muere sin probar el cuerpo de una morena se va de este mundo al otro sin saber lo que es canela.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Desdramatizar la copla José Luis",
    "periodical": "candil",
    "issue_id": "1993-03",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 1169,
    "article_char_count_full": 6897,
    "article_char_count_review": 3073,
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
  }
]
```
