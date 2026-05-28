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
    "article_id": "1991-07-9-right-de-pregones-y-fandangos-malague-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOpinión\n\nN o está en mi ánimo convertirme en pregonero de una investigación que no he llevado a cabo, sino que deseo reflejar en este breve escrito unas vivencias de mi niñez y juventud al recordar una Málaga muy tradicional en sus costumbres, muy lejos de la que me ha tocado vivir en mi tercera «juventud», es decir, con sonidos ruidosos de la automoción y el pop-rock anglosajón. Ya lo dijeron dos poetas andaluces: Escuché el coro inmenso de [tus pregones y cantas como ninguna ciudad [del mundo...] Salvador Rueda (Benaque, 1857-Madrid, 1947)\n\n...Málaga, cantaora... Manuel Machado (Sevilla, 1874-Madrid, 1947)\n\nEfectivamente, así era: en la Má-laga de antes no sólo había arte en los cafés cantantes de «La Loba», «Sin Techo», «Chinitas», etc., lo había también en la gente del pueblo llano,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nlo había también en la gente del pueblo llano, que sin tener por profesión la de artista flamenco, se ganaba la vida en la venta ambulante de las cosas más peregrinas que imaginarse pueda. Estos vendedores pregonaban su mercancía por calles y plazas, con un estilo muy peculiar, posiblemente derivado de los antiguos fandangos «abandolaos», descendientes directos de los ancestrales «verdiales», que de esta forma Dios sabe quién intentó liberar la voz del sonido de la fanfarria que el toque de los verdiales lleva consigo para ser bailados. De estos fandangos «abandolaos» —(Bándola: del latín pandura, instrumento musical parecido a una guitarra pequeña)— nacieron las mal llamadas «Mala-gueñas de Juan Breva» que en realidad deben ser llamados «Cantes de Juan Breva». Cenachero: Vendedor ambulante de pescado, siglo XIX. Grabado de Gustavo Doré, tomado del natural en la ciudad de Málaga en 1862, para ilustrar las crónicas de viaje publicadas en la revista «Le Tour du Monde». Estas crónicas fueron recogidas como libro por primera vez en París en 1874 bajo el título «Viaje por España», escrito por el barón Charles Davilliers y las ilustraciones de Gustavo Doré. Antonio Ortega Escalona «Juan Breva» (Vélez-Málaga, 1844-Málaga, 1918) en su juventud fue vendedor ambulante de frutos de la Axarquía veleña y hasta nosotros ha llegado su pregón que dio origen a su nombre: ¡Brevas de los montes ¡Brevas de los montes ...de Vélez-Málaga! Son las más dulces... ¡las doy «pa» probarlas! Así los cantes «abandolaos» bajaron desde la sierra hasta el borde de la mar, formando un grupo de cantes vernáculos (grupo de cantes propiamente malagueños) que generalmente se distinguen con el nombre de bandolás: Cantes de Juan Breva, rondeña, fandangos de Lucena, fandangos de Cabra, fandangos de Almería y jabegotes.\n\n[ENDING CONTEXT]\n\nde pescados variados o mariscos. Veteá: Habla andaluza: veteada. Vitorianos: Boquerones de tamaño mediano, muy jugosos, pescados en la zona de la bahía del Rincón de la Victoria.\n\nNOTA:\n\nLa letrilla arreglada por Juan Jiménez Soler —«Juanillo el loco»—, perteneciente a los Cantes de Juan Breva, éste la cantaba así:\n\nEn la Cala hay una fiesta mi madre me va a llevar como iré tan compuesta me sacarán a bailar con mi par de castañetas.\n\nA Juan Breva, muchas de sus letras, se las componía su madre y quizá por motivos de amor filial, Juan Breva las canta-ba tal cual se las entregaba su madre.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "De pregones y fandangos malagueños",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "9-11",
    "page_number": 9,
    "word_count": 2339,
    "article_char_count_full": 13788,
    "article_char_count_review": 3428,
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
    "article_id": "1991-07-12-left-luis-el-de-la-venta-premiado-en-",
    "article_text_for_review": "Norberto Torres Cortés\n\nEl pasado sábado fui a La Unión con dos guitarristas almerienses y otro francés para asistir a la final del veterano Concurso Nacional de Cantes de las Minas y del Concurso de Guitarra Flamenca que se viene celebrando en el pueblo minero desde hace ya treinta y un años consecutivos. Dos cosas se destacan del conjunto de actos del famoso festival nacional. Córdoba y la guitarra flamenca\n\nEl concurso de guitarra flamenca ha puesto de manifiesto una vez más los resultados extraordinarios del constante y acertado trabajo de Córdoba en pro de la guitarra. Gracias a las repercusiones del mítico concurso cordobés, que empezó sus andaduras en el año 56, las clases de Merengue de Córdoba y Rafael Muñoz «Tomate», las del catedrático de guitarra flamenca del Conservatorio Superior de Córdoba recientemente fallecido, Manuel Cano, los cursos internacionales del centro Paco Peña y la celebración, por décima vez ya este año, del festival internacional de la guitarra que reúne a los mejores intérpretes de la sonanta en todos los géneros, especialistas del instrumento, guitarreros, editores de partituras, organizadores de otros festivales similares, etc..., y que convierte a Córdoba en capital mundial de la guitarra durante un mes, está saliendo de la ciudad califal una de las generaciones de guitarristas más importantes de la historia flamenca. No es una casualidad, que el guitarrista que más acompañe en los festivales este ve\n\nrano sea un joven cordobés, Manuel Silveria. Tampoco que uno de los concertistas más prestigiosos se llame José Antonio Rodríguez y uno de los solistas más creativos, de la última generación flamenca, Vicente Amigo. Ambos son cordobes. Paco Serrano, otro tocaor de la misma categoría y artista confirmado a pesar de su juventud, ganó con diferencia el Bordón Minero frente al sevillano Ismael Guijarro. Como broche de oro a la noche flamenca, Vicente Amigo demostró con su derroche de sensibilidad, compás e ideas originales, que las posibilidades de la guitarra flamenca son infinitas.\n\nLuis «El de la Venta»\n\nTres grupos constituían el concurso de cante: cantes mineros, cantes de Málaga, Granada y Córdoba y cantes bajo-andaluces. Entre los doce finalistas encontramos algunos «profesionales» de los concursos como Juan Casillas, José Parrondo o Jesús Heredia, varios cantaores locales y de otras comunidades, básicamente de Madrid y Barcelona. Pero lo más interesante es que por primera vez en la lar-\n\nga vida de este concurso se presentaba un almeríense: Luis López Cerdán, Luis «El de la Venta». Luis pertenece a la escasa nómina de aficionados que tenemos en Almería e hizo prueba de un admirable valor al presentarse por iniciativa propia, sin contar con el apoyo inicial de peñas, instituciones o de la Federación Provincial de Peñas (¿qué está haciendo?). Quedó clasificado en el apartado d) del grupo primero, el de otros cantes mineros (taranto, murciana, levantina, fandango minero, etc...). Competía con El Rampa, joven cantaor unionense ganador del máximo galardón en 1989, el de la Lámpara Minera. Han sido precisos 31 concursos de La Unión para que un almeríense se atreva a participar y ganar (a pesar de ser Almería cuna de los cantes mineros). Luis y algunos almerienses más que nos hubiera gustado ver en el programa la Puerta del Cante dedicado a Almería o en el cartel de nuestro festival de verano, ha demostrado que el que se propone, lo consigue. ¡Ojalá tomen nota los demás aficionados y las instituciones almerienses se muestren más sensibles con su gente!",
    "title": "Luis el de la Venta, premiado en La Unión Norberto",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 572,
    "article_char_count_full": 3545,
    "article_char_count_review": 3545,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-07-12-right-manuel-vallejo-o-el-ruise-or-fla",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFicha biográfica\n\nManuel Jiménez Martínez de Pinillo, el genial cantaor que conocimos con el sobrenombre de Vallejo, no fue natural de Sanlúcar la Mayor ni de Sanlúcar de Barrameda, como algunos estudiosos del arte han dicho sin fundamento. Manolo, como puedo demostrarlo documentalmente, tuvo el alto honor de nacer en Sevilla, en la casa número 1 de la calle Pinilla, el día 15 de octubre de 1891. Hijo de Manuel Jiménez y de Manuela Martínez de Pinillo y Vara. Nieto por línea paterna de Joaquín Jiménez y de Dolores Vallejo, y por línea materna de Francisco Martínez de Pinillo y de Antonia Vara. Falleció en Sevilla el día 7 de agosto de 1960.\n\nVallejo como persona\n\nManuel Vallejo tuvo los defectos y virtudes que encarnan a toda persona, pero como artista fue perfecto, y sólo desde este\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"criticarlo\"]\n\n15 de octubre de 1891. Hijo de Manuel Jiménez y de Manuela Martínez de Pinillo y Vara. Nieto por línea paterna de Joaquín Jiménez y de Dolores Vallejo, y por línea materna de Francisco Martínez de Pinillo y de Antonia Vara. Falleció en Sevilla el día 7 de agosto de 1960. Vallejo como persona Manuel Vallejo tuvo los defectos y virtudes que encarnan a toda persona, pero como artista fue perfecto, y sólo desde este ángulo hemos de contemplarlo y criticarlo. Leí un comentario sobre Mano- lo y me consta que afectó profun- damente a sus familiares, muy especialmente a sus sobrinos Joa- quín y Pilar, que le adoraban. De Vallejo se sabe muy poco, y si queremos hablar de cómo fue y cómo actuó entre sus semejantes, hemos de investigar en su biogra- fía con toda honradez para no sacar a la luz cosas y casos irreales que perjudiquen a nuestro arte y hieran la susceptibilidad de los suyos. Del artista podemos afirmar, sin lugar a equivocarnos, que fue hombre raro y muy hermético, llevado, sin duda, de su peculiar carácter que no le permitía convivir más que con un escogido número de amigos que se conocían lo suficiente como para aguantarse mutuamente sus rarezas e impertinencias. Por lo demás, Vallejo tuvo buen corazón y en ocasiones se comportaba como un niño. Fue muy tímido, medroso por excelencia y rayano en la inocencia, siendo, por estas condiciones humanas, por lo que sus sobrinos le querían c\n\n[ENDING CONTEXT]\n\nel día 5 del corriente en el Teatro Pavón y en el que tomaron parte Manuel Torre, Escacena, Cepero, Angelillo, El Chato Vicálvaro, Faro I, Villarrubia y otros. Madrid, 8 de octubre de 1926. Periódico “El Paso atrás”, revista de toros, teatros y espectáculos».\n\nTermino diciendo que si Pastora Pavón «Niña de los Peines» recibió en vida el premio que indiscutiblemente se mereció, ¿por qué a Vallejo no le pagamos la deuda que los puros aficionados tenemos contraída con él? ¿Por qué no creamos una Peña Flamenca que lleve el nombre inmenso de Manuel Vallejo? Los hijos de Sevilla tienen la palabra.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Manuel Vallejo o el Ruiseñor flamenco",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 1626,
    "article_char_count_full": 9579,
    "article_char_count_review": 3036,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "criticarlo"
      }
    ]
  },
  {
    "article_id": "1991-07-14-left-homenaje-al-veterano-cantaor-mad",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nC on la alegría que produce el disfrute de una salud que le ha permitido llegar a sus 81 años de edad, nos dice nuestro entrevistado que nació en Madrid el 21 de enero de 1910, en la calle Princesa, siendo bautizado en la parroquia San Antonio de la Florida, tratándose, por tanto, de un madrileño puro y casticísimo.\n\nCursó estudios primarios hasta que, atraído por la influencia cantaora de su padrino, empezó como aficionado a tener algunas actuaciones en público, siendo la primera en el Salón Luminoso, cine de la barriada de Cuatro Caminos, donde actuó en unión de su padrino Ricardo «El Tora», Jesús «El Gordo», Lorencín de Madrid, Chaconcito y Eusebio Villarrubias, quedando en el Cine Olimpia en tan buen lugar que le propició una nueva actuación en público situado en la Plaza de Lavapiés\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestro\"]\n\nación en público situado en la Plaza de Lavapiés en un cartel compuesto por el guitarrista Manuel Bonet y con los cantaores Ricardo «El Tora», Carmen Espinosa, Diego «El Personita», Lorencín de Madrid, Eusebio Villarrubias, destacando tanto en su actuación por Malagueñas que le concedieron un trofeo (pequeña copa de plata) que le fue entregado por don Antonio Chacón, circunstancia que aumentaría la admiración que ya de por sí sentía por el viejo maestro a quien considera como el mejor de los intérpretes que ha tenido nuestro arte flamenco. Estos éxitos iniciales como aficionado le abrieron las puertas del Café Veneziano, situado en la calle Preciados, próximo a la Plaza de Santo Domingo, donde comenzó a codearse con artistas de renombre, recordando con especial cariño a Bernardo «el de los Lobitos», de quien aprendió grandemente, recordando que por entonces le asignaron un sueldo diario de 10 pesetas con derecho a su «cafelito». Con este refuerzo económico, se procuró e\n\n[ENDING CONTEXT]\n\nen los espectáculos que se forman.\n\nLa gran experiencia que adquirió en el transcurso de su vida, apartándose de todo cuanto de nocivo rodeó el entorno del mundillo flamenco, le ha granjeado una legión de amigos, que todavía se disputan su amistad y compañía, estimando que es este el mejor y más grande premio ganado en el curso de su vida, que discurre familiarmente dentro de una gran armonía con su señora, hijos, nietos y biznietos, y al cerrar este trabajo que, como homenaje, dedicamos a este veterano artista, le renovamos nuestra ya vieja amistad y consideración deseándole larga vida.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Homenaje al veterano cantaor madrileño Alfonso Chozas",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 1114,
    "article_char_count_full": 6850,
    "article_char_count_review": 2607,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestro"
      }
    ]
  },
  {
    "article_id": "1991-07-15-left-la-intertextualidad-como-recurso",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPorque agüita pasá, compañera,\n\nGüérvete al cariño\n\nLos autores del Diccionario Enciclopédico del Flamenco, Blas Vega y Manuel Ríos Ruiz, al hablar de las características retóricas de la copla flamenca, nos dicen que el recurso de la intertextualidad es uno de los más frecuentes. No se encuentran, sin embargo, en ésta —concluyen— referencias a otros textos poéticos, sino a motivos paremiológicos. Ponen estos ejemplos:\n\nNo mueve molino.\n\nQuisiera yo por horitas\n\nSer nasio e las yerbas,\n\nPorque ojitos que no ven\n\nCorasonsito no quiebran.\n\nEn ambos casos, se cita un refrán, aunque cambiando siempre algo: el diminutivo resulta más flamenco y expresivo (agüita, ojitos, corasonsito) y la rima impone su ley (yerbas-quiebran, cuando el remate de ese refrán es «que no siente», una forma menos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"mujer\"]\n\nque no ven Corasonsito no quiebran. En ambos casos, se cita un refrán, aunque cambiando siempre algo: el diminutivo resulta más flamenco y expresivo (agüita, ojitos, corasonsito) y la rima impone su ley (yerbas-quiebran, cuando el remate de ese refrán es «que no siente», una forma menos enfática y dramática). p Podemos aportar otros ejemplos, comentados por otros autores. E. Soria, en La copla, comenta que el sino fatalista que atribuye a la mujer el pecado de la infidelidad está patente en el terceto que nos transcriben los hermanos Quintero y que dice: «cabrita que tira al monte» Me lo decía mi madre, no hay cabrero que la guarde. De la misma copla dice Cansinos Assens en La copla andaluzay que, dando nombre a la obra Cabrita que tira al monte, ríndense los Quintero al fatalismo de la copla andaluzay. La «cabrita» representa para este preclaro escritor «el fracaso de una redención y confirma ese fatalismo oriental que late en la entraña de la copla andaluzay». Acaba diciendo que «la alusión a ese monte simbólico (...) nos habla de la existencia de una protesta y de una nostalgia latentes en el fondo del alma andaluzay». Agustín García Chicón, en Valores antropológicos del cante jondo, nos ofrece otras coplas en las que se cita de manera clara un refrán o sentencia. Una de corte senequista es: Hijito mío, no llores, ni hagas cuenta de nada, que siempre se ha oído desí: a mal tiempo buena cara. Séneca decía que las cosas que mucho suben, al mejor tiempo caen. A esta idea y a la de la avaricia perniciosa responden estas coplas y sus sentencias: Un\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_01 | trigger=\"fuera\"]\n\ns, y quien llora los aumenta; no es llorar un hombre afrenta, cuando las causas son tales (...) Un letrista casi analfabeto, Balmaseda, en su Primer cancionero flamenco, tiene coplas que también prestan uno o dos versos a un refrán: No es tuyo, no le eches pan, que perderás pan y perro como lo dice el refrán. F En otros casos, la intertextualidad busca alusiones bíblicas de todo tipo. E. Soria comenta la copla: Mi niño se va a dormir ojalá y fuera verdad y le durara el sueñito tres días como a San Juan. No faltan parodias de motivos religiosos, como estas aleluyas que trastocan nada menos y nada más que los diez mandamientos. Los recitaba una vieja mendiga en Triana, según R. Marín: Los mandamientos del probe son: Er primero, roá po'r suelo. Er segundo, roá po'r mundo. Er tersero, no comé vaca ni carnero. Er cuarto, ayuná después de jarto. Er quinto, no bebé blanco ni tinto. Estos mandamientos se encierran en dos: En matá piojos y peí por Dios. García Chacón nos ofrece otro ejemplo: Nadie diga en este mundo no necesito consejo, Salomón con ser tan sabio murió de un niño aprendiendo. Se refiere al Salomón de la Bíblia, famoso por su sabiduría y justicia. Otra parodia, también en tono de protesta o de súplica-réplica, es el Padrenuestro que cantaba por bulerías Manolito el de María: Padre nuestro que está en lo cielo que toito lo oye y toito lo ve, por qué me abandona con mi niño, por qué no te acuerda darle de comé. En las coplas flamencas hay pervivencias de antiguas composiciones tradicionales, como bien ha estudiado el profesor Gutiérrez Carbajo. Bernardo el de los Lobitos cantaba una bulería cuya letra le dio nombre artístico: Anoche soñaba yo que los lobitos me comían que enojados me miraban. y eran tus hermosos ojos y eran tus ojitos negros que dos negros me mataban que me miraban y me decían... Anoche soñaba yo Ricardo Molina y Antonio Mairena, en Mundo y formas del cante flamenco, hablan del dinamismo de las co- Una co\n\n[ENDING CONTEXT]\n\npolos falsamente antagonistas del enfrentamiento de la pareja.\n\nEn el caso de la lidia, la relación que se entabla entre los protagonistas del drama es modificable en cada momento. Cada uno puede rehacerse, imponer su propio discurso, su propia ley. Aquí está el núcleo de una dialéctica del discurso, es decir, de un diálogo que evoluciona por la dinámica de los contrarios y de las oposiciones. Lo mismo pasa en el choque que pone frente a frente a los bailadores.\n\nEl enunciado del discurso\n\nEl escenario del discurso se limita a un espacio reducido, como en el teatro. El ruedo y el tablao son\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La intertextualidad como recurso en la copla flamenca",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 2283,
    "article_char_count_full": 13513,
    "article_char_count_review": 5218,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "mujer"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "fuera"
      }
    ]
  }
]
```
