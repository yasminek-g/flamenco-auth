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
    "article_id": "1992-09-3-right-apuntes-sobre-la-sole-y-la-sigui",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLa mujer en la soleá\n\nEste ángulo diedro en tensión de los cantes grandes es simbólicamente la tensión entre los estratos más subterráneos y elevados del hombre. Esto es suficiente para entender que los cantes grandes sean casi específicamente pertenencia masculina. En cambio, la soleá que maneja más desenfadadamente el dato existencial (y con ponderada dialéctica), ha sido el camino elegido por la mujer para traducir su aporte sublimador al cante grande. (Ni qué decir tiene, al baile).\n\nExisten excepciones tanto en cantaores como en estilos determinados. Sería ridículo, por caso, sugerir que la Niña de los Peines no puede cantar por otros cantes, salvo por soleares, o que Gracia de Triana o Rocío Vega no le dan esencia a la saeta. Descontando estos casos aislados —y algunos más que no\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"memoria\"]\n\nica), ha sido el camino elegido por la mujer para traducir su aporte sublimador al cante grande. (Ni qué decir tiene, al baile). Existen excepciones tanto en cantaores como en estilos determinados. Sería ridículo, por caso, sugerir que la Niña de los Peines no puede cantar por otros cantes, salvo por soleares, o que Gracia de Triana o Rocío Vega no le dan esencia a la saeta. Descontando estos casos aislados —y algunos más que no nos llegan a la memoria—, surge como tope común para la mujer el cultivo de los cantes grandes jondos que no sean la soleá. De un breve repaso histórico recordamos el famoso triángulo de la Serneta, la Andonda y la Gómez. Soleareras contemporáneas de renombre son la mencionada Niña de los Peines, la Trinitaria, la Niña de la Puebla, la Andalucita, etcétera, y más atrás: Antonia la de San Roque, Anilla la de Ronda, la Marrancho, la Fandita, la Parrala y la Loro. En la soleá, la mujer ha expresado mejor su temperamento. Su instantaneidad no dubitativa, su necesidad de sentir en el cante (o en la vida) bases estables, la imposibilidad o improbabilidad de expresar estas tensiones fuera de las afectivas ligadas al amor, y agregado a todo ello el garbo de este cante realmente único entre los grandes —amén del esfuerzo físico supremo que requieren los otros cantes de igual jerarquía— ha plantado a la mujer en la soleá mejor que en cualquiera de los otros estilos jondos. La cantidad y calidad de mujeres hondas que cantan por soleá llama la atención. No cabe duda que cantaoras redondas como la Parrala y la Niña de los Peines acome\n\n[ENDING CONTEXT]\n\nde que hay muchos diestros de una zona geográfica determinada que en nada responden a la tradición formal (ambiental) a que deberían pertenecer. Estos son una suerte de tránsfugas del paisaje vital. Pero, insistimos, en términos generales existe un concepto vívido y definido de toreo cordobés, de toreo sevillano, de toreo rondeño, etcétera. Si bien tales caracterizaciones no podrían resistir una demostración técnica desarrollada hasta el último detalle formal, bullen ciertas claves regionales de captación intuitiva de las que nadie, con perspicacia estética, podría desentenderse.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Apuntes sobre la soleá y la siguiriya (y 4)",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-6",
    "page_number": 3,
    "word_count": 3590,
    "article_char_count_full": 22474,
    "article_char_count_review": 3187,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "memoria"
      }
    ]
  },
  {
    "article_id": "1992-09-7-left-ocho-im-genes-de-ocasi-n",
    "article_text_for_review": "Francisco Chica\n\nE n el gran supermercado que vivimos hoy (un espectacular revoltijo en el que todo se cambia, se compra y se confunde), consuela encontrarse aún con algunos restos del derribo que ayudan a recomponer, como en un gran puzzle, la imagen de un pasado que se niega a desaparecer para siempre entre las turbulentas aguas de la actualidad. Son precisamente esas sorpresas (chispazos esporádicos y anecdóticos pero llenos de vida), las que nos reconcilian con una identidad envuelta cada vez más en amplias zonas de sombra. Quizá sea el mundo del arte flamenco, que ha perdurado hasta el presente como la magnífica cristalización de una cultura, la parcela de esos viejos tiempos que más esté acusando el desgaste que supone mantenerse vivo en un medio cada vez más de predador y olvidadizo.\n\nEncontré estos retratos de artistas flamencos casualmente, mientras buscaba ciertos documentos de archivo. Estaban metidos en un gran sobre blanco en el que podía leerse: «Viejas fotografías de Málaga». Yo vi en ellas un síntoma de la capacidad que sigue teniendo el viejo mundo para sobrevivir en medio de tanta banalidad circundante. Aunque posiblemente algunas de ellas sean conocidas por los especialistas,\n\nAgradezco al Archivo de la Excma. Diputación Provincial de Málaga el permiso para la reproducción de las fotografías.\n\nme pareció oportuno ofrecerlas como curiosidad los lectores de Candil. Todas ellas forman parte de la colección de fotografías del escritor malagueño Narciso Díaz de Escovar y están dedicadas a él o a su familia. Díaz de Escovar, que vivió muy de cerca la era de oro del cante en Málaga (sobre todo la de los célebres Cafés Cantantes como El Chinitas, España, El Turco, La Loba, etc.), fue autor de algunas importantes colecciones de coplas flamencas y sus letras se hicieron muy populares al ser interpretadas por Juan Breva.\n\nEstán dedicadas y fechadas las de Pastora Pavón (30-I-49), Juan Varea (28-VIII-48) y la de Niño Ricardo (10-VI-54). La de Pastora Imperio no tiene fecha y la de Manolo Caracol tiene sólo la dedicatoria. En el reverso que reproducimos de la de Manuel Torre (seguramente uno de los pocos autógrafos del mítico cantaor) puede leerse difícilmente una dedicatoria y su firma. Niño de Jerez, que fue como se le conoció durante su juventud. Las dos fotos de Merced la Serneta son las mismas que aparecen, aunque muy retocadas, en el libro de Ricardo Molina y Antonio Mairena, Mundo y formas del cante flamenco.",
    "title": "Ocho imágenes de ocasión",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 407,
    "article_char_count_full": 2465,
    "article_char_count_review": 2465,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-09-8-right-por-malague-as",
    "article_text_for_review": "Pues resulta ser que la Diputación Provincial de Málaga (y sus asesores, claro está) no han logrado encontrar en Málaga cantaores capaces de grabar 31 malaqueñas. Claro, que lo que no sabemos es si los han buscado, pero mucho me temo que no han tenido ese detalle con los cantaores de la «Málaga cantaora»; esa, con cuyo nombre llenan su boca algunos mandamases que cada vez que se acercan al Flamenco es para meter la pata. Como la han metido una vez más al contar con un cantaor de fuera de la tierra, cuando aquí en Málaga estamos sobrados de excelentes malagueñeros que hubieran cumplido muy dignamente el encargo, al tiempo que habrían contribuido a difundir, sin confundir —que es lo que se ha conseguido—, el variado y hermoso cante por malagueñas.\n\nDiego Clavel, el autor de tan desafortunada obra, es cantaor con más voluntad que talento, pero, y esto creo que nadie lo pone en duda, con una honradez profesional incuestionable. Y eso, precisamente, es lo primero que se nota en su obra de reciente aparición «Diego Clavel 31 Malagueñas»; mas como no tiene un conocimiento global del cante por malagueñas, pues nos encontramos ante una obra coja que contribuye más al despiste del aficionado que a una divulgación seria del cante malagueñero.\n\nNo voy a entrar en un análisis crítico pormenorizado, pero sí daré una visión general del desagü-sado cometido con la grabación del mentado álbum. Decir, en principio, que toda la obra está privada del gusto preciso para cantar por malagueñas. En la malagueña de El Canario hace Diego Clavel una recreación aportando matices que a mí particularmente me desagradan, pero además se nota la falta de sabor propio de la tierra. El fandango de Cayetano Muriel por malagueñas ni es fandango ni es malagueña. La malagueña de Juan Trujillo «El Perote», simplemente, la desconoce.\n\nLa cara B del disco 1 la abre con una versión del estilo de El Maestro Ojana que desvirtúa al sonar más a Peñaranda que a Ojana. En la versión del estilo de Baldomero Pacheco nos alegra los oídos, pues demuestra conocer dicho cante. Como es sabido y está demostrado, Paca Aguilera no creó estilo alguno de malagueñas; sin embargo, Diego —mal informado— canta una malagueña que él atribuye a la cantaora rondeña: es el conocido segundo estilo de La Trini que se solía y se suele cantar con la no menos co-\n\nnocida copla «Paloma mía...». Las cuatro versiones de Chacón más la que atribuye a El Pena están hechas con dignidad. En las versiones de Fernando el de Triana, El Niño del Huerto y Personita, Diego demuestra que no ha escuchado suficientemente las versiones originales —o más fiables—que, es de suponer, sus asesores le proporcionaran en su día. Recomiendo a Diego Clavel que vuelva a escuchar a El Cojo de Málaga, Juan de la Loma y al propio Personita.\n\nDesconozco por qué canta, Diego, dos malagueñas de Concha la Peñaranda cuando es de todos sabido que de la cantaora cartagenera sólo existe un estilo basado en el cante por jabegotes. Las malagueñas perotas no son un saco donde cabe todo y Diego parece querer darnos a entender que sí con sus desafortunadas versiones de tales estilos. (¿Por cierto, señor Luque, en qué venta escuchó a Joaquín Tabaco?). Su desconocimiento del estilo de El Niño Vélez es patente. Y en cuanto a lo que en el disco se titula como Malagueña de Juan Breva y Fandango de Málaga por Malagueñas, podríamos catalogarlo dentro del apartado de cosas raras. En fin, cuando de una creación personal se trata, según los títulos (en el folleto explicativo el señor Luque le adjudica tres), el tiempo es el encargado de poner a cada uno en su sitio; pero su salía a lo Antonio Molina, su falta de gusto y sus excesos vocales consiguen un refrito de dudoso gusto que no ha de pasar, sospecho, a la Historia del Flamenco como ejemplo a seguir en el cante por malagueñas",
    "title": "Por malagueñas Paco",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "8-8",
    "page_number": 8,
    "word_count": 663,
    "article_char_count_full": 3823,
    "article_char_count_review": 3823,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-09-9-left-loli-la-revoltosa",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen...\n\n(Nieta de Diego Antúnez)\n\nLuis Soler Guevara, Pedro Sánchez Ortega\n\nHoy traemos a nuestras páginas a otra artista descendiente de una gran estirpe cantaora. Una mujer que se incorporó tarde al mundo del Flamenco, y que estamos por decir, que de haberlo hecho antes, hoy ocuparía un lugar de privilegio dentro de nuestro Arte.\n\nElla es Dolores Jiménez Antúnez, conocida en el mundo llamenco como «Loli la Revoltosa», quien ha tenido la gentileza de recibirnos en su casa de la calle Alcalde Manuel de la Pinta, número 19, en pleno corazón del Barrio de Loreto de Cádiz. Con la presencia de su esposo, José Roldán, y la compañía de ese viejo patriarca del cante que es Tío Evaristo Heredia Maya. «Loli la Revoltosa» es descendiente directa de un legendario artista\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mujer\"]\n\n—Loli, ¿quién era Diego Antúnez? —Mi abuelo. Mi abuelo por parte de madre. -¿Diego tuvo más hijos? —Dos hembras, mi madre y mi tía. Mi madre se llamaba María Antúnez Fernández, y mi tía, Isabel. —Fernanda y Juana Antúnez Fernández eran dos gitanas nacidas allá por el año mil ochocientos sesenta y tantos; eran bailaoras y también hacían sus cantes. ¿Tienes algún parentesco con ellas? Interviene José Roldán. Es posible. Lo que pasa es que mi mujer desconoce el parentesco por parte de su madre, porque en la familia se ha hablao poco, pero creo que eran primas. Le decían «La Pompi». —No. No me estoy refiriendo a la hermana de «El Gloria». Estoy hablando de estas dos hermanas que bailaban a finales del siglo pasado. Estos apellidos de Antúnez Fernández y los de la madre de Loli, aun siendo cincuenta años más tarde, tienen algún parentesco? José Roldán. Posiblemente sí, porque el apellido Antúnez es raro que no estén emparentaos unos con otros, además, es un apellido que está bastante distribuido por la geografía. Por ejemplo, el abuelo era de Sanlúcar de Barramada, las hijas de Sevilla, criás en Cádiz. También tenía familia en Jerez, por lo tanto era una familia muy distribuida por Andalucía. —Loli. Tu madre sí era pariente de «El Gloria» y «La Pompi». ¿Qué te tocaba? —Primos hermanos. —Entonces, Diego Antúnez sería familiar de «El Gloria», ¿es así? —Claro, lo que pasa es que yo era mu pequeña y no lo he conoció, pero por oídas de mi madre sí. Pero el que mejor conocía eso era mi padre, que era flamenco y conocía la tradición de la familia. —Parece ser que Diego nació sobre 1875... —No. Exactamente nació en 1868 en Sanlúcar de Barrameda. —¿Vosotros habéis escuchado si Diego a pesar de nacer en Sanlúcar, es posible que venga de Jerez? José Roldán. Mira, os voy a contar una cosa que me contara mi suegra, hija de Diego Antúnez. Diego siempre llevaba a su casa a muchas amistades, y lo primero que hacía era encerrar a sus dos hijas en un cuarto y nos las dejaba salir para nada. Era otra mentalidad distinta a la de hoy. Por eso mi suegra y su hermana tenían muchas lagunas sobre la vida y costumbres de su pad\n\n[ENDING CONTEXT]\n\nque fue «La Perla»...?\n\n—¡Oh! Con «La Perla» había que acabar... cuando decía:\n\nsi te veo venir por la calle\n\nA pesar de Loli no encontrarse bien de la voz, en ella están los matices y esos lejanos recuerdos de «La Perla de Cádiz», en la que vemos su más firme puntal.\n\n-¿Loli, es verdad que has grabado un disco?\n\nQue me peguen cuatro tiros\n\n—Sí, está al salir, lo hemos grabao con Pasarela, porque no es un disco mío sola. Ahí estamos varios que cada uno hace una cosita.\n\ny a los ojos yo te miro.\n\nEstá claro que éste no es mi disco..., el que yo siento; el día que lo haga sola lo haré más puro.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Loli La Revoltosa",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "9-11",
    "page_number": 9,
    "word_count": 2862,
    "article_char_count_full": 15582,
    "article_char_count_review": 3761,
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
    "article_id": "1992-09-12-left-i-congreso-de-cr-ticos-de-arte-f",
    "article_text_for_review": "Los días 23, 24 y 25 de octubre, se celebró en Jaén, organizado por la Peña Flamenca de esta ciudad y la colaboración de diferentes entidades, a la cabeza de las cuales figuraba la excelentísima Diputación Provincial, el I Congreso de Críticos de Arte Flamenco, que, desde que se anunciara, tanta expectación había despertado en los ambientes jondos.\n\nDesde todos los puntos de España acudimos a esta llamada los implicados en la crítica flamenca, para trabajar, en unas apretadas sesiones, que fueron presididas por los señores Onofre López, Francisco Hidalgo y Francisco Moyano, mesa elegida por los congresistas y que respondió con creces a la confianza que en ellos se depositó.\n\nLas reuniones tuvieron dos partes bien diferenciadas: la teórica y la práctica. En la primera se desarrollaron cinco ponencias de tono similar, encaminadas a analizar la realidad del lenguaje crítico flamenco, su posible definición associativa y los cauces por la que tal actividad pudiera desarrollarse en el futuro. Fueron, por orden de lectura y debate las siguientes: Miguel Acal: «La unión como fuerza necesaria», Francisco del Río: «La pureza en el flamenco», Aurelio Gurrea: «Crítica y flamencología», Agustín Gómez: «El lenguaje de la crítica en los medios de comunicación» y Manuel Martín: «Deontología y funciones de la crítica flamenca». Todas ellas fueron ampliamente discutidas y valoradas por los presentes, extrayéndose\n\nconclusiones para clarificar los motivos fundamentales para los que allí nos reuníamos, esto es, el futuro de la Asociación.\n\nLa parte práctica, y yo diría que esencial del Congreso, fue la lectura, debate, y en su caso, aprobación con las modificaciones oportunas, del anteproyecto de Estatutos por los que habrá de regirse la Asociación de Críticos de Arte Flamenco. Aquí afloraron las lógicas tensiones, los intereses contrapuestos y hasta los pequeños arrumacos biliares que suelen estar tan presentes en todo tipo de discusiones similares.\n\nFinalmente el Estatuto, que habrá de desarrollar un posterior Reglamento, fue aprobado con el texto íntegro que ofrecemos aparte a los lectores de CANDIL, procediéndose más tarde a la presentación de candidaturas para la Junta Directiva provisional de la A.C.A.F., resultando elegida la encabezada por Gonzalo Rojo Guerrero, de Málaga, y formada por los señores que relacionamos en nuestro anexo informativo adjunto, fijándose la sede para los próximos dos años en la Peña Flamenca «Juan Breva» de Málaga.\n\nLa totalidad de los miembros del Congreso se constituyó en el núcleo fundacional constitutivo de la Asociación, cuyos directivos deberán presentar ahora los Estatutos ante los organismos competentes para su aprobación definitiva y oficial, así como convocar más tarde una nueva reunión de la Junta General, una vez aprobado dicho trámite, que será la que ratificará de forma plena los nombramientos y acuerdos realizados en este Congreso constituyente.\n\nDiversos actos sociales y recreativos, varios recitales y almuerzos, además de la presentación del número monográfico de CANDIL dedicado a Camarón, la exposición de pintura flamenca de José Olivares o la conferencia de Manuel Ríos sobre el periodismo flamenco, complementaron tan gratas jornadas, que intensifican más aún los empeños comunes por hacer del estudio del flamenco algo más sólido y reputado por el conjunto social. Es de esperar que, en años venideros, y con la colaboración de todos, este esfuerzo colectivo haya merecido la pena.",
    "title": "I Congreso de Críticos de Arte Flamenco José Luis",
    "periodical": "candil",
    "issue_id": "1992-09",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 536,
    "article_char_count_full": 3471,
    "article_char_count_review": 3471,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
