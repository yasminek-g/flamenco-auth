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
    "article_id": "1983-03-18-left-el-x-congreso-de-actividades-fla",
    "article_text_for_review": "S alvó su propio reto con parecido decoro a los anteriores y, cómo no, ofreció sorpresas al igual que los que le antecedieron.\n\nMuy luchadores sus organizadores; pintoresquísima la capital de Jaén; variopinta su provincia y agradabilísimo el parque donde escuchamos el programa de Cante Flamenco que el Congreso nos brindó.\n\nMeollo del Congreso:\n\nSería muy lamentable que en futuros congresos los congresistas acudieran un día después de su apertura, dado el lentísimo sistema que hemos creado para su puesta en marcha. Perdemos horas preciosas en votar los miembros de la mesa que dirigirá y moderará el Congreso, para al final emplazar a personas (no todas) desconocedoras de la mecánica a seguir, por factores obvios de mencionar. Hemos remontado el tope —sabiamente empleado en el pasado— del número de folios a emplear en ponencias y comunicaciones y se da lectura a trabajos propios para confeccionar un tratado de flamenco dado su extenso contenido.\n\nSalvo excepciones, las ponencias tratan más la corteza que el núcleo flamenco, por lo que vuelvo a aplaudir a Manuel Cano, Alfredo Arrebola y a Génesis García, aunque recuerdo la indicación que un congressista hizo a esta flamenca sobre editar un libro, como asimismo la respuesta, que vino a decir: «Posiblemente lo haré, pero antes debo concluir la tercera y última parte, la cual afrontaremos con respeto y dificultades, pues trataremos “aspectos musicológicos y su desarrollo”, por supuesto del cante de las minas». ¡Flamenca, aprétate el refajo! Pues ese es el toro miura que quien se precie de cabal debe lidiar, y que tu amigo Escribano llama corazón (núcleo) del Cante.\n\nCulminarás felizmente esa simpar tárea a la que vienes dedicando años, aunque vengas a «quedar reducida al monólogo erudito de unos cuantos estudiosos», según expresión de Ramón Porras, en CANDIL, número 24, pues «ha faltado debate en este y en anteriores congresos».\n\nHe llegado a la conclusión de que a los congresos acudimos más por snobismo que por sentar bases. Tengo la sospecha de que el setenta por ciento de los congresistas no «está puesto en el Cante», por lo que me atrevo a afirmar que los debates, aunque resulten manoseados por unos cuantos, deben existir, pues en ello va implícito el esclarecimiento, la ayuda velada y el refrescar datos de lo histórico de nuestro querido Arte. Lo contrario es aventurarse en aparatos de altos vuelos, cuando el auditorio desconoce aviónica, aeronáutica y hasta física elemental. ¿Supongo valga el símil?\n\nQue Granada cuide lo flamenco esencial y que tomando el bagaje de anteriores congresos, que Jaén le cede, confíe en el éxito.",
    "title": "El X Congreso de Actividades Flamencas desde mi prisma",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 426,
    "article_char_count_full": 2619,
    "article_char_count_review": 2619,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-03-18-right-lo-jondo-en-federico-garcia-lorc",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nN la década de los sesenta, un conocido y presti- gioso crítico de arte, José María Moreno Galván, al hacer la presentación de una etapa más en la vida discográfica del cantaor de la Puebla de Cazalla, José Menese, decía: «Esta es la hora en que valdría la pena escribir una estética de lo jondo; del cante, y de todo lo demás. De la pintura, del cine, de la literatura, de la danza... Y sentenciaba: Menese, que no es intelectual, ve una obra de Picasso, de Miró o de Tapies y dice siempre: Yo no entiendo nada, pero esto está bien; esto debe ser como lo jondo de la pintura. ¿Claro que entiende! Creo, dice José María Moreno Galván, que esas palabras encierran una profunda formulación estética».\n\nPues bien, esa estética jonda ya era una constante en un andaluz universal. En Federico García\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"pobre\"]\n\nconstante en un andaluz universal. En Federico García Lorca. Su obra nos deja claros y contundentes indicios de cuanto afirmamos. Pero antes de adentrarnos en el verso del poeta —su teatro podría ser objeto, en otro momento, de un estudio—, habría que intentar definir qué entendemos por estética jonda. El mismo, al analizar la imaginación del escritor, nos da una pista para tratar de definir la estética de lo jondo. Escribe: «La imaginación es pobre, y la imaginación poética más. La realidad visible, los hechos del Por Juan A. Ibáñez mundo y del cuerpo humano están mucho más llenos de matices, son más poéticos que los que ella —la imaginación— descubre. Entonces podríamos decir que lo jondo es la forma de exponer y comunicar el sentimiento de unas raíces y vivencias humanas, que cobran vida en situaciones concretas recreadas por el artista —cantaor, escritor, pintor, etc.—, en simplificación de formas, buscando la belleza en lo natural, en lo real, huyendo de todo lirismo hueco. Para mí, dice el poeta, la imaginación es sinónimo de aptitud para el descubrimiento. La imaginación fija y da vida clara a fragmentos de la realidad invisible donde se mueve el hombre. La hija directa de la imaginación es la metáfora, nacida, a veces, a golpe rápido de la intuición, alumbrada por la lenta angustia del presentimiento». Quien se haya acercado, aunque sea ligeramente, a la obra de Federico García Lorca, habrá comprobado que su hacer está presidido por lo jondo. Y lo jondo en Federico no es una mera circunstancia de estudio momentáneo. Fue, es —nunca muere lo que no queremos que muera— una auténtica preocupación que él se impuso atendiendo a su ser andaluz. Una imposición del subconsciente y que se avivó haciéndole buscar los porqués de nuestras gentes. En la jondura del hombre/poesía encontramos un cierto regusto por el fatalismo. Y es que García Lorca, como ser del sur, como persona marcada por una idiosincrasia propia de nuestro pueblo, ahonda a la hora de vagar por su entorno. Y en las situaciones límite gusta de fijar su atención en lo verdaderamente grande. Diríamos que al escribir responde a una imperiosa necesidad de desahogar su pena. El no canta, pero escribiendo cumple su compromiso de artista jondo. Deslizémonos muy quedamente hasta su poesía. Jondo es quien dice: El grito deja en el viento una sombra de ciprés. (Dejadme en este campo llorando) Todo se ha roto en el\n\n[ENDING CONTEXT]\n\npozo de lo jondo para saciar la sed de vida que todo andaluz padece. Y Federico, uniendo sus conocimientos de las letras flamencas al propio ser andaluz, nos dice: «En el cante más fuerte que la muerte es el amor». Y el pueblo siente amor por Federico. Su vida se agiganta en todos los ambientes. Su estética jonda es un mar de producción literaria... Su talante andaluz envuelve cuanto dice y hace.\n\nEs íc mismo que:\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados\n\nEspecialidad en Asados\n\nRoldán y Marín, 7 J A E N Teléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Lo jondo en Federico García Lorca",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "18-20",
    "page_number": 18,
    "word_count": 2291,
    "article_char_count_full": 13316,
    "article_char_count_review": 4029,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "pobre"
      }
    ]
  },
  {
    "article_id": "1983-03-20-right-viaje-por-la-solea-mesa-redonda-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCoordina: Pedro Sánchez\n\nDesde este punto de vista, el juicio de los aficionados, que no ha nacido de libros o de siempre subjetivas referencias, centra nuestro interés. Sería prolijo reflejar literalmente todas y cada una de las investigaciones que allí se vertieron. Bastará con que consignemos las conclusiones a las que se llegaron. Helas aquí.\n\nRESPECTO AL ORIGEN.\n\nSe habla de soleá de Triana, de Alcalá, de los Puertos, de Utrera, etc. Teniendo en cuenta que de los distintos tipos de soleá que han llegado hasta nuestros días — según nuestro criterio— la menos contaminada es la de Triana, y que la intérprete más antigua que se conoce cantando soleares —La Andonda— era de Triana, nos inclinamos por decir que el origen de este cante está en Triana, irradiado posteriormente a otras zonas\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"origen\"]\n\nos, de Utrera, etc. Teniendo en cuenta que de los distintos tipos de soleá que han llegado hasta nuestros días — según nuestro criterio— la menos contaminada es la de Triana, y que la intérprete más antigua que se conoce cantando soleares —La Andonda— era de Triana, nos inclinamos por decir que el origen de este cante está en Triana, irradiado posteriormente a otras zonas cantaoras como Alcalá, los Puertos, etc., etc. AY muchas teorías sobre el origen, antigüedad y evolución de la soleá. Se considera este cante como columna vertebral del flamenco, por su sobriedad y belleza, y por ser el cante a compás por excelencia, además de por el amplio número de cantes que de ella se derivan. Existen, a nuestro juicio, tres zonas cantaoras donde la soleá es un cante popular, un cante enraizado, como son, la mencionada Triana, los Puertos y Alcalá, aunque Antonio Mairena y Ricardo Molina, en «Mundo y formas del cante flamenco», analizan los diversos tipos de soleá que actualmente se cantan, excepto la soleá de los Puertos; esto no concuerda con nuestro criterio, puesto que esta soleá tiene su personalidad original. auténticamente pura, auténticamente originaria, sólo es la primera. Con alguna frecuencia, se dice que posiblemente, la soleá de Alcalá sea anterior a la de Triana, por ser más simple, puesto que en arte, lo simple suele ser lo primero. Examinando la historia de cualquier manifestación artística, observamos que existen ciclos; románico, gótico, renacimiento, barroco, etc. Lo simple suele identificarse con lo que es primero en el tiempo, pero no necesariamente. Est\n\n[ENDING CONTEXT]\n\nTomás Pavón, a nuestro entender, le dio al cante por soleá una nueva dimensión. El cante, que duda cabe, es comunicación y esta debe ser transmitida sin excesivos preámbulos. Este cantaor, quizás basado en sus portentosas facultades y en la «manía» que tenía de ligar los cantes, hacía un tipo de soleá de una belleza incomensurable, alargando los tercios, o mejor dicho, ligándolos. Aunque hay quien piensa que alargar un cante no es mejorarlo, nosotros en este caso opinamos que sí.\n\nPor este motivo nosotros diríamos que en el cante por soleá hay que hablar de antes y después de Tomás Pavón.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "VIAJE POR LA SOLEA (Mesa redonda en la Peña Flamenca de Jaén)",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1481,
    "article_char_count_full": 8713,
    "article_char_count_review": 3212,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "origen"
      }
    ]
  },
  {
    "article_id": "1983-03-22-left-las-letras-flamencas-de-ramon-po",
    "article_text_for_review": "Las letras flamencas de Ramón Porras tienen la enorme ventaja de no tener que guardar turno para pasar a ser verdadero trino popular. Desde que salen de la inspiración directa y encendida del autor, se precipitan como un rayo a la voz de una cantaora, Rosario López, que las acrisola, las hace suyas, las mece al compás inefable de ese estremecimiento que aprisiona las gargantas y hace callar las razones para entregarse a esa catarsis sin fondo del cante sin destinatario. Quizá por eso las letras de Ramón nacen calientes, como el pan recién sacado del horno de la vida, porque saben, desde el origen primigenio de su concepción, que no han de buscar un nido donde reposar, sino que, desde siempre, el cante verdadero de una mujer, su compañera, las aguarda como los campos aguardan la lluvia de abril, y fructifican así, con la esperanza como acompañante.\n\nDe estas tierras aplomadas iba arrancando mi pan ahora que la mina cierra veré si, llorando canto, llorando, puedo cantar.\n\nEsa voz desenterrada que de tiniebla hace el cante y de sol y de olivares, ¡Ay sólo enterrarse puede en las minas de Linares!\n\nMinerito de la India azul como la galena qué madre te dará tierra si en esta tierra te mueres, ¿a quién llenarás de pena?\n\nPonga remedio, doctor, al mal que me niega el aire, que siendo tan buen minero no quiero que, sin retiro, mis niños pasen más hambre.\n\nEl relevo de la mina se hace al alborear; cuando la pena te arrancas con aguardiente y con cante, cante de la madrugá.\n\nUn día vi una gacela caminando yo a la Paz, que es mina del Centenillo, y supe qué es ser minero y supe qué es libertad.\n\nEra tan buen patrón que diez minutos me daba, al cambio de pozo a pozo, para llenarme de vida la vida que yo enterraba. Bajo a la mina, pensando, mis niños, si han de crecer vestíos de amargo luto y una madre «echá» a la vida «pa» llevarles de comer.\n\nLa taranta hay que cantarla con la sangre ennegrecida, que los soníos te arranquen el dolor de ser minero y haberla oído en Linares.\n\nCuando la alarma sonaba en la mina del «Lentejo» hasta el más duro apretaba el metal de una medalla, la Virgen de Linarejos.",
    "title": "Las letras flamencas de Ramón Porras",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 387,
    "article_char_count_full": 2122,
    "article_char_count_review": 2122,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-03-22-right-la-experiencia-que-es-madre-de-l",
    "article_text_for_review": "N esta sección daremos gustosa cabida a cuantas experiencias, individuales o colectivas, se lleven a cabo en relación al conocimiento y difusión del arte flamenco.\n\nLa periodicidad bimensual de «CANDIL» determina nuestra imposibilidad de estar puntualmente al hilo de la noticia jonda, ya que ésta exigiría, por esencia, un tratamiento inmediato. Ello, no obstante, consideramos importante destacar aquellas noticias y experiencias que tengan especial relevancia, y con virtualidad que trascienda a lo simplemente noticiable.\n\nHoy, y estamos convencidos de que no será la última vez, nos hacemos eco de las experiencias que nuestro buen amigo Ricardo Rodríguez Cosano ha puesto en práctica, en un colegio de Lebrija. El flamenco en la escuela hay que hacerlo realidad, hay que potenciarlo. Los cursos de flamenco que Rodríguez Cosano promueve con tanta tenacidad y eficacia, son un ejemplo que debiera cundir por toda la geografía andaluza. Las ponencias y las comunicaciones en los congresos de Actividades Flamencas, cumplen ese papel de fijar criterios más o menos genéricos o esclarecedores. Criterios que, a la postre, hay que hacer operativos, porque sólo la aplicación concreta de los mismos, es decir, la experiencia, nos puede dar la medida exacta de su valor.\n\nSabemos que la Junta de Andalucía pretende llevar nuestro arte a los centros de enseñanza; y en ello está rigurosamente empeñado el departamento de flamenco del ente autonómico. Por primera vez en nuestra historia, una institución está sensibilizada, con conocimiento y rigor, respecto a esta parcela tan degradada en otras épocas, de nuestra cultura.\n\nNuestro apoyo más estimulante al trabajo de Rodríguez Cosano, en cuyo humilde homenaje publicamos dos pequeñas muestras de su importante labor.\n\nCurso 82-83 Antigüedad: Curso 78-79 (5 años) Alumnos matriculados: 46\n\n3. $ \\text{°} $..... 5 4. $ \\text{°} $..... 19 5. $ \\text{°} $..... 13 6. $ \\text{°} $..... 9\n\nUltimas audiciones: Cinta-cassette de PACO DE LUCIA y MANOLO SANLUCAR.\n\nPalos interpretados por los alumnos en festivales: Fandangos de Huelva (dos estilos) Petenera Chica Rondeña (un estilo) Verdiales (un estilo)\n\nCantaores que han pasado por Aula: PEPE MONTARAZ, MONOLO SIMON, DOMINGO SANCHEZ (EL MELON), etc.\n\nComentarios a libros: TODO LO BASICO SOBRE EL FLA- MENCO (Carlos Almendros); CANTE FLAMENCO (Ri- cardo Molina).\n\nFestivales: Diciembre (en el Centro). Febrero (Feria del Libro Local. Plaza pública).\n\nProyectos: En el Centro, Peña Flamenca PEPE MONTARAZ, y en el Hogar del Pensionista (Fin de Curso).\n\nExcursión a Cádiz: Visita a una Emisora (Programa flamenco). Visita a la Peña Flamenca ENRIQUE EL MELLIZO (actuarán para los socios y sus hijos). AULA de ARTE FLAMENCO COLEGIO ELIO ANTONIO Dia 16 IIc. 6 TARDE\n\nGRAN FESTIVAL FIAMENCO\n\nCANTE: BELLA SANCHEZ M² O RODRIGUEZ BENI SANCHEZ JUAN C. SANCHEZ JUANI ROMERO INMACULADA CALVO PEDRO GARCIA VIRGINIA RUIZ M² JOSE VIDAL\n\nPOESIA: ISABEL FUENTES\n\nBAILE LOLI BERNAL-JOSE L. VIDAL VIRGINIA RUIZ-JUAN J. RUIZ PILI JIMENEZ. FRANC. J. ARJONES\n\nPRESENTA: M! CARMEN NAVARRO\n\nESPECTACULOS INTERNACIONALES Como es lógico, ya lo hemos dicho en otras ocasiones, nosotros no estamos en contra de cualquier innovación en el flamenco, porque él, así lo entendemos, ha de ser progresivo, pero ojo!, dentro de los canones básicos con que nació. De todos modos, cuando en una materia (en este caso musical) se ha llegado a la máxima perfección y ésta está revestida de grandes dificultades interpretativas y rodeada de una gran belleza, se hace casi imposible su superación, quedando, en este caso, para la pos\n\nSEVILLA\n\nO'Donnell, núm. 3 - 4.° Tlf. 22 20 58 - 21 69 20\n\nPARTICULAR: Teléfono 27 80 78",
    "title": "La experiencia, que es madre de la ciencia",
    "periodical": "candil",
    "issue_id": "1983-03",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 580,
    "article_char_count_full": 3680,
    "article_char_count_review": 3680,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
