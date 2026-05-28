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
    "article_id": "1984-07-21-left-enrique-morente",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen:\n\n—Tras tu trabajo de zapatero, ¿qué pasó?\n\n—Bueno, ese ha sido uno de los muchos oficios que he tenido. Una de las muchas cosas que he hecho en mi vida mientras me dedicaba a cantar. Empecé a meterme con los profesionales, a escucharles como aficionao y donde yo hacía también algo. Me di entonces cuenta que algunos «equivocaos» querían escucharme cantar. Y yo creo que han metío la pata, podían haber elegio a otro que no fuera Enrique Morente. Pero ésto ha pasao así y aquí estoy cantando.\n\n—¿Hablanos de tus vivencias con Pepe el de la Matrona?\n\nuno de los genios, tal vez el genio más grande que ha tenido el cante flamenco.\n\n—Pepe fue un gran maestro, fue un gran amigo y fue un hombre del que aprendí muchísimas cosas de la vida y del cante. Coincidió mi\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nber elegio a otro que no fuera Enrique Morente. Pero ésto ha pasao así y aquí estoy cantando. —¿Hablanos de tus vivencias con Pepe el de la Matrona? uno de los genios, tal vez el genio más grande que ha tenido el cante flamenco. —Pepe fue un gran maestro, fue un gran amigo y fue un hombre del que aprendí muchísimas cosas de la vida y del cante. Coincidió mi pasión por el flamenco con la entrega que él tenía por el cante, que a pesar de ser un hombre con tantísimos años, todavía vivía y soñaba para el cante, estaba siempre haciendo proyectos, hasta el último momento, para el futuro. —A pesar de las no muy numerosas grabaciones de Chacón, ¿cómo has llegado a tener un conocimiento, por decirlo de alguna forma, tan exacto de la personalidad del cantaor de Jerez? —¿Quién te despertó el interés por don Antonio Chacón? —El primero fue Pepe el de la Matrona. Fue una de las muchísimas cosas que le escuché hablar y aprendí y me di cuenta después escuchando las grabaciones de este hombre que fue —Se lo debo en gran parte a Pepe el de la Matrona y Bernardo el de los Lobitos. Aurelio también me habló mucho de él y Manolo de Huelva. Pero básicamente a Pepe, Manolo de Huelva y a Bernardo. —¿Esta misma circunstancia te sucedió con Perico el del Lunar padre? —¡No!, una vez llegado a Madrid le conocí, porque me lo presentó Pepe en el rastro madrileño y ya no volvió a tratarlo más. —¿Qué circunstancias hicieron que cantaras en la Feria de Nueva York? —Pues porque me contrató un ballet, el ballet de Mari Emma, para cantarle a ella y a los bailarines que iban en el mismo: Quintero y Trini España. —¿Qué impresión te causó la posible y potencial afición americana por nuestro arte? —Yo entonces no tenía conciencia de esas cosas. Fui allí a trabajar y me encontré que conocía sólo «Graná», algo de Málaga, Jaén, Madrid y entonces Nueva York. Imaginate conociendo estas ciudades la impresión que me causó Nueva York. Claro, me quedé atontao. Cuando yo vi aquellos edificios de Mannhatan, te puedes suponer la impresión que tuve. Luego el festival tan grande en el recinto de la feria, la misma feria... Me acuerdo que yo conocía d\n\n[ENDING CONTEXT]\n\nque llevarlo a todos sitios. —¿Antonio Mairena...?\n\n—Antonio Mairena un grandioso cantaor. Un cantaor larguísimo por soleá y siguiriγas. En fin, no tengo palabras para él, todas las que diga serán siempre pocas.\n\n□ «Al principio mis discos chocan, luego, con el tiempo, los aficionados los aceptan».\n\n—¿Mi futuro? Pues estoy proyectando la grabación de una antología sin salirme de los cánones como decimos, pero no de museo, una cosa que tenga actualidad y que sirva. Quiero que sea en la línea de mi homenaje a Chacón.\n\n—?Tu futuro?\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Enrique Morente",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "16-18",
    "page_number": 16,
    "word_count": 2869,
    "article_char_count_full": 15978,
    "article_char_count_review": 3758,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombre"
      }
    ]
  },
  {
    "article_id": "1984-07-19-left-vocalizar-el-cante",
    "article_text_for_review": "Por Manuel Yerga Lancharro\n\nI escuchamos las grabaciones de principios de siglo, de los cantaores Chacón, Torre, El Herrero, Pastora, Tomás y Pepe Pinto, por ponerlos como ejemplo, nos daremos cuenta, sin esfuerzo, que todos ellos vocalizaban correctamente. Cantaban como se deben ejecutar los cantes, sin apoyaturas labiales que tanto afean utilizando, a pecho descubierto, su garganta, sus pulmones y su estómago y respirando al mismo tiempo. ¿Cómo respiraba cantando el gran Pepe Pinto! ¿Lo saben hacer así nuestros artistas?\n\nHoy tenemos que lamentar la desastrosa vocalización de algunos cantaores. ¿Cómo destruyen el verso de la copla y cómo utilizan esos apoyos sin fundamento, incluso sobrándoles facultades para interpretar sus cantes!\n\nYo me atrevería a decir a algunos profesionales que procuren erradicar de sus actuaciones este afeamiento; veamos:\n\n1) Destrucción del verso: «Al pie de un naranjo dulceeeeuuuuuu».\n\n2) Be, be, be, ib, ib (labial).\n\n3) Ay, qué (en Polos y Cañas).\n\n4) Guin, guin, guin.\n\n5) «meter» la nariz.\n\nMe agradaría, ésta es la verdad, que todos los versos de la copla terminaran, por muchos regodeos y florituras que haga el cantaor, en la letra final del mismo:\n\nAl pie de un naranjo dulceeeeeeeeeeee de roiyas me postréeeeeeeeeeee yo le conté mis agravios, 0000000000 de la pena que le diooooooooo de dulce se volvió agrioooooo.\n\nAsí, tal y como lo dejo escrito, es como se debe vocalizar para no destruir la rima, como lo hacía un genio del cante ya fallecido, pero que por desgracia tiene muchos seguidores:\n\nAl pie de un naranjo dulceeeuuuuuuuuuuu de roiyas me postréeeeeuuuuuuuuuuuuuuuu yo le conté mis agravios, ooouuuuuuuuu de la pena que le dioooooouuuuuuuuuuu de dulce se volvió agriooooouuuuuuuuuu\n\nSi escuchamos un cante sin aconpañamiento (Debla, Tona, Martinete) en la voz de ese genio al que me he referido, nos daremos cuenta del efecto malsonante que nos ofrece su ejecución. Y es incomprensible, señores, que así lo hiciera un cantaor de primerísima categoría. Existen intérpretes que no solamente destruyen el verso en la forma que queda dicho, sino que, además, utilizan las vocales al final del último tercio de la copla, como lo hiciera un colegial en el inicio de su aprendizaje:\n\nAaaa,eee,iii,ooo,uuu\n\n¿Por qué terminar en «u»? ¿Por qué terminar fatalmente en «u» flácida y tan poco flamenca? Esta letra, por lo repelente que resulta al oído, únicamente debería ser utilizada por el cantaor cuando el verso termine en ella. Así':\n\n...y la culpa tuviste túuuuuuuuuuu\n\nEs curioso: ¿por qué en la primera década de este siglo los grandes cantaores por Levante, como Chacón, Escacena, Cojo Málaga, si al final del verso o tercio se encontraban con una «u» la cambiaban por la «i» a mitad del recorrido del fragmento musical? Porque la «i», al contrario que la «u», es gratísima al oído de todo aficionado. Lo mucho que de grato conlleva la «i» lo percibimos también cuando escuchamos el penúltimo tercio de la taranta chaconiana del árbol malacitano:\n\nSi a la derecha te inclinas si vas a San Antolín, y a la derecha te inclinas; verás en el primer camarín a la Pastora Divinaaaai.\n\nque es vivo retrato a tí.\n\nEn este penúltimo tercio es cuando el buen artista se recrea y explaya con modulaciones y preciosismo, terminando con una repetida caída vertical en «i».\n\nTengo que lamentar, muy de veras, la forma impropia que se viene dando a la ejecución de los cantes. Y ya que los maestros «creadores» de esas apoyaturas desechables y de la malísima vocalización, han pasado a mejor vida, aconsejaría que los jóvenes cantaores de la actualidad hicieran lo posible por erradicar de sus interpretaciones lo que les afea y estorba y, haciendo un «borrón y cuenta nueva», volvieran a ejecutar sus cantes como lo hacían aquellos artistas que he citado.\n\nQUE FALTA NOS ESTA HACIENDO UNA ESCUELA DE APRENDIZAJE EN NUESTRA GEOGRAFIA CANTAORA!\n\nCreo que el Gobierno Andaluz tiene la palabra.",
    "title": "Vocalizar el cante",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 646,
    "article_char_count_full": 3935,
    "article_char_count_review": 3935,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-07-19-right-memoria-de-terremoto",
    "article_text_for_review": "Aunque no quepa en el papel\n\n(Departamento de Publicaciones de la Cátedra de Flamencología y Estudios Andaluces. Cádiz, 1984)\n\nA Cátedra de Flamencología y Estudios Folklóricos Andaluces, ha editado la obra que hoy presentamos, «Memoria de Terremoto», como homenaje póstumo al fallecido cantaor jerezano. El libro está constituido por seis epígrafes: Terremoto en el recuerdo; Terremoto en la poesía; Terremoto en los libros; Terremoto en los periódicos; Terremoto en los discos, y discografía de Terremoto.\n\nEl contenido de esta obra viene, en gran parte, determinado por la colección de trabajos que en memoria del inconmensurable cantaor Fernando Terremoto, fueron leídos en la Cátedra, en sesión literaria y con ocasión de la apertura del curso académico 1981-1982. Como complemento, se han unido otras colaboraciones publicadas en diversos medios, con posterioridad al óbito del genial artista, considerado ya por muchos como uno de los más grandes siguiriyeros de las últimas décadas. «Memoria de Terremoto» cumple, tal vez, el propósito de los promotores de esta publicación: reunir en su volumen un variado y hetereogéneo florilegio que permita conocer lo que, por otro lado, para cualquier iniciado ya es un dogma, esto es, la tremenda categoría cantaora del que en vida se llamara Fernando Fernández Monje. Ello no obstante, esta obra presenta algunas lagunas, objeciones un tanto aleatorias por cuanto siempre vienen determinadas por los propósitos de quien promueve una publicación. Nos referimos a que se hecha en falta la homogeneidad que hubiera prestado a esta monografía la concurrencia de un trabajo no sólo biográfico, ni sólo anecdótico, ni sólo contemplativo de hermosas genialidades, sino todo ello simultáneamente y algo más: visión integradora y de profundización\n\nPor Ramón Porras\n\nen la vida del maestro desaparecido, captación del hilo conductor de una época flamenca en la que se inscribe con particular relevancia el cantaor jerezano, análisis de significados presentes y futuros de lo jondo, a la luz de los irremediables ya pretéritos.\n\nA colección «Biblioteca de Temas Flamencos», nos ofrece esta obra de Juan de la Plata, sobre un tema tan apasionante como desconocido y poco investigado. El opúsculo comentado, trata de reflejar la saeta como expresión de la pasión de Cristo según la canta el pueblo andaluz. Hemos dicho trata de reflejar, porque ya el subtítulo de la obra merece, a nuestro juicio, algunas precisiones. Así, entendemos que el concepto de saeta es mucho más restringido que el que se refiere a la pasión de Cristo que viene contemplada por la literatura flamenca con ocasión de otros misterios salvíficos, particularmente el de la Navidad. Y es que lo «jondo» enfoca el misterio de la Salvación de una manera teológicamente rigurosa, en el sentido de que Vida (singularmente, nacimiento), Muerte y Resurrección de Cristo se contemplan como un todo unitario no susceptible de análisis estancos.\n\nEl estudio de Juan de la Plata es, fundamentalmente, lírico. Adolece, evidentemente, de planteamiento sistemático; y aunque se maneje una apreciable bibliografía, Luis Montoto, Benito Más, Fray Diego de Valencia, etc., estas referencias no concurren en apoyo de una teología sobre aspectos historiográficos, sociológicos, e incluso estéticos de la saeta. No hemos apreciado ningún esfuerzo en este sentido y, desde luego, está por publicarse un estudio monográfico de la saeta que afronte con rigor muchos de los interrogantes que José Luis Buendía se formula en su trabajo «Una aportación al estudio de la saeta».\n\nEn cualquiera de los casos, esta obrita de Juan de la Plata cumple perfectamente su cometido, sólo en cuanto",
    "title": "Memoria de Terremoto",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 573,
    "article_char_count_full": 3671,
    "article_char_count_review": 3671,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-07-20-right-iii-bienal-de-arte-flamenco-ciud",
    "article_text_for_review": "Organizado por el Patronato de la Bienal de Arte Flamenco Ciudad de Sevilla y patrocinado por el Ayuntamiento sevillano, Ministerio de Cultura, Consejería de Cultura de la Junta de Andalucía y la Diputación Provincial de Sevilla, tendrá lugar, del día 12 de septiembre al 12 de octubre, la III Bienal de Arte Flamenco, dedicada en esta ocasión al TOQUE. Esta edición está programada en seis ciclos.\n\nLa III Bienal de Arte Flamenco Ciudad de Sevilla es, sin duda, el festival más importante en su especialidad en la bisecular historia del Arte Flamenco Andaluz. No sólo por el número y la importancia de los artistas que intervienen en los distintos ciclos, sino por el enfoque riguroso y el tratamiento de los montajes que conforman el certamen.\n\nEn esta edición, la organización pretende dar a la música flamenca el sitio que merece y que la Bienal de Sevilla se inscriba cuanto antes en el calendario de los festivales europeos de música, proclamando así el valor exacto de la capacidad creativa del pueblo andaluz.\n\nA continuación, detallamos el desarrollo del programa.\n\nDE LA MUSICA Y DE LA DANZA\n\nMonasterio de San Jerónimo, del 12 al 16 de septiembre\n\nDía 12: «GIRALDILLO DEL BAILE».\n\nMario Maya y su compañía de Teatro Flamenco. Rafael de Alcalá. Concha Távora. Juan Fernández. Pepa Herrera. Isidoro Carmona. Paco Carrillo. Manuel de Paula. Miguel López. Juana Amaya, Pilar Heredia. Charo Cruz. Juan de los Reyes y José El Lele.\n\nDía 13: «LA CASA DE LOS HABICHUELAS». Juan, Pepe, Luis y Carlos, José Menese, José el de la Tomasa, Carmen Linares, Tía Marina Habichuela, Manolete y Manolo Santiago.\n\nDía 14: ORQUESTA BETICA FILARMONICA. Víctor Monge «Serranito» y José Luis Gómez.\n\nDía 15: MANOLO SANLUCAR.\n\nDía 16: «TOQUE, BAILE Y CANTE EN FAMILIA». Manuela Carrasco y Joaquín Amador, Pepa Montes y Ricardo Miño, Concha Calero y El Merengue, Antonio Suárez Pansequito y Romerito de Jerez, acompañados por sus hijos.\n\n2. $ ^{0} $ TRIANA\n\nHotel Triana, del 17 a 21 de septiembre\n\nDía 17: «RECORDANDO A DIEGO DEL GASTOR» Fernanda y Bernarda de Utrera, Paco y Juan del Gastor, Diego de Morón, Joselero y Miguel Funi.\n\nDía 18: «SONIOS DE JEREZ» Manuel Morao, El Sordera, Paco Cepero, Fernando La Morena, Manolo Parrilla, La Paquera de Jerez y Ana Parrilla.\n\nDía 19: «LA NUEVA TRIANA» La familia de los Amadores: «La Pata Negra» (Raimundito y Rafael), Diego Amador y Juana Revuelo. Ramón Amador y El Boquerón. Angelita Vargas.\n\nDía 20: «SOLERAS» El Poeta y Naranjito de Triana. Manolo Brenes y Beni de Cádiz. Félix de Utrera y Luis Caballero. Perico el del Lunar y Miguel Vargas. Farruco y el Moreno.\n\nDía 21: «JOVENES GUITARRAS ANDALUZAS» Quique Paredes y Antonio Chacón. Manuel Palma y El Pele. Antonio Sousa y Diego Clavel. José Luis Postigo y Paco Taranto. Manolo Franco y Aurora Vargas. Ana María Bueno, Rafael Alarcón. Jarillo.\n\n3. $ ^{0} $ ALAMEDA\n\nPlaza del Lucero, del 22 al 25 de septiembre\n\nDía 22: «DESAFIO DE FANDANGOS» Alosno. Música de las Alpujarras.\n\nDía 23: «ECOS DE LA ALAMEDA» La Tomasa, Pies de Plomo, Eduardo de la Malena, Chocolate, Manolo Carmona, Enrique Montes, Fernanda Romero, Fregenal y Joselito el Colorao.\n\nDía 24: «LO QUE ES CADIZ» Fiesta del compás y de la gracia.\n\nDía 25: «LA FRAGUA DE TIO JUANI» Metales de Jerez.\n\n4. $ ^{0} $ VANGUARDIA\n\nCiclo Torre de Don Fadrique, del 26 al 29 de septiembre\n\nDía 26: I FINAL DEL CONCURSO NACIONAL DE JOVENES INTERPRETES DE LA GUITARRA FLAMENCA. Nino Miguel. II ESTRENOS DE PACO AGUILERA. Toti Soler. Día 27: I FINAL DEL CONCURSO INTERNACIONAL DE JOVENES INTERPRETES DE LA GUITARRA FLAMENCA. José Antonio García. II ESTRENOS DE GUALBERTO.\n\nDía 28: «DE LA TRISTEZA Y DE LA ALEGRIA» Encuentro de Jazz y Flamenco.\n\nDía 29: «HOMENAJE A SAN JUAN DE LA CRUZ» Enrique Morente.\n\n5. $ \\text{° CLASICOS} $\n\nReales Alcázares, del 30 de septiembre al 6 de octubre\n\nDía 1: EDUARDO FALU.\n\nDía 2: MANOLO CASTILLO.\n\nDía 3: ORQUESTA DE CAMARA DE GUITARRAS DE MADRID. Jorge Cardoso.\n\nDía 4: PEPE ROMERO.\n\nDía 5: PACO DE LUCIA.\n\nDía 6: ORQUESTA DE MUSICA ANDALUSI. 6.º CAMINO Teatro Lope de Vega, del 7 al 12 de octubre\n\nDía 7: CAMARON y TOMATITO.\n\nDía 8: JUAN PEÑA, PEDRO PEÑA, LA PERRATA y PEDRO BACAN.\n\nDías 9, 10 y 11: «GIRALDILLO DEL TOQUE» Pedro Bacán, Manolo Franco, Paco del Gastor, Rafael Riqueni, José Antonio Rodríguez y Tomatito. Día 11: Fin de fiesta con los Montoya.\n\nDía 12: «MAESTROS» Calixto Sánchez, Enrique Melchor, Mario Escudero, Matilde Coral, El Mimbre, Rafael Fernández, Manolo Domínguez, Chano Lobato y Paco Arriaga.",
    "title": "III BIENAL DE ARTE FLAMENCO CIUDAD DE SEVILLA",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 763,
    "article_char_count_full": 4495,
    "article_char_count_review": 4495,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-07-21-right-proximos-festivales",
    "article_text_for_review": "Mes de Septiembre\n\nDía 1.—En CULLAR VEGA (Granada). VIII Festival Flamenco «Frasquito Yerbagüena», en el que cantarán: EL AGUJETAS, LA SAYAGO, CURRO DE UTRERA, PANSEQUITO, EL POLACO y RAFAEL MUÑOZ, acompañados a la guitarra por: JUAN CARMONA «HABICHUELA» y MELCHOR CORDOBA. Al baile: LOS MONTOYA. Organizado por la Peña Flamenca Frasquito Yerbagüena.\n\nDía 1.—En JIMENA DE LA FRONTERA (Cádiz). III Festival Flamenco Novena/84. Con la actuación en el cante de: JOSE DE LA TOMASA, NIÑO DEL PARQUE, ANDRES LOZANO, PERRO DE PATERNA, MANOLO LIMON y ANA DIAZ, acompañados a la guitarra por JOSE CALA «EL POETA», CARBONERO DE JEREZ y SALVADOR ANDRADE. Al baile: MACARENA y DAVID MORALES. Organiza: Excmo. Ayuntamiento.\n\nDía 1.—En MAIRENA DEL ALCOR (Sevilla). XXIII Festival de Cante Jondo «ANTONIO MAIRENA», en el que cantarán: FOSFORITO, MANUEL MAIRENA, CALIXTO SANCHEZ, JOSE MENese, CURRO MALENA, CHOCOLATE, JOSE MERCE y FERNANDA DE UTRERA, acompañados a la guitarra por: ENRIQUE DE MELCHOR, PEDRO BACAN, JUAN HABICHUELA y PACO CEPERO. Al baile: PEPA MONTES y ANGELITA VARGAS. Organiza el Excmo. Ayuntamiento.\n\nDía 6.—En PORCUNA (Jaén). Organizado por la Peña Flamenca «La Temporera», tendrá lugar la VIII BESANA FLAMENCA, en la que actuarán: JUAN PEÑA «EL LEBRIJANO», JOSE MENESE, LUIS DE CORDOBA y CURRO DE UTRERA, acompañados a la guitarra por: MANOLO DOMINGUEZ y JUAN HABI-CHUELA.\n\nDía 7.—En ECIJA (Sevilla). Organizado por el Excmo. Ayuntamiento, con la colaboración de la Peña Flamenca Ecijana, XIII NOCHE FLAMENCA ECIJANA, en la que actuarán: MANOLO MAIRENA, CALIXTO SANCHEZ, EL SORDERA, EL CABRERO, RANCAPINO, CHANO LOBATO, ROMERITO DE JEREZ, PACO «EL CLAVERO», EL ECIJANO y ANTONO REYES «EL TOTO», con las guitarras de: PEDRO BACAN, MANOLO DOMINGUEZ y JOSE L. POSTIGO. Al baile: MARIO OLIVEROS.\n\nDía 8.—En LOS OGIJARES (Granada). Festival Flamenco en el que intervendrán: JOSE MÉNESE, CALIXTO SANCHEZ, «EL LEBRIJANO» y JUANITO VILLAR, acompañados a la guitarra por JUAN HABICHUELA.\n\nDía 16.—En BEAS DE SEGURA (Jaén). Festival Flamenco. Mano a mano entre JUAN PEÑA «EL LEBRIJANO» y «EL CABRERO», acompañados a la guitarra por PEDRO BACAN y JOSE LUIS POSTIGO.\n\nDía 21.—En CACERES. V Festival Flamenco a beneficio de la I.T.E.A.F. Organizado por el XII Congreso Nacional de Actividades Flamencas, en el que intervendrán, como en Congresos anteriores, las figuras del momento, puesto que se trata de un festival benéfico.\n\nFABRICA Y OFICINAS:\n\nPolígono «LOS OLIVARES» - Teléfonos 22 30 00 - 22 30 04 - J A E N\n\nDISTRIBUTOR OFICIAL DE:\n\nVIDRIO LAMINAR DE SEGURIDAD - ACRISTALAMIENTOS EN GENERAL\n\nTRABAJOS DE ALUMINIO PARA OFICINAS Y TERRAZAS\n\nII CONCURSO NACIONAL DE CANTE «YUNQUE FLAMENCO»\n\nORGANIZADO por el Ayuntamiento de Sta. Coloma de Gramanet y las Peñas Flamencas de la localidad, ha sido convocado el II Concurso Nacional de Cante «Yunque Flamenco», para aficionados.\n\nPara este concurso han sido establecidos dos grupos y premios. Para el primer grupo de: Siguiriyas, Toná y Tangos, se han fijado tres premios, siendo el primero de 80.000 pesetas.\n\nPara el segundo grupo de: Caña, Bulerías, Malagueña, Cantiñas y Soleares, otros tres premios, el primero de 60.000 pesetas.\n\nEl plazo de inscripción finaliza el día 20 de septiembre. Todos los aficionados que deseen inscribirse deberán dirigirse al Area de Educación y Cultura, calle Lavaderos, 1-3, teléfono 385 31 61, de Sta. Coloma de Gramanet.\n\nNUEVA JUNTA DIRETIVA DE LA PEÑA FLAMENCA CHICLANERA\n\nN reciente asamblea general de socios celebrada por la Peña Flamenca de Chiclana (Cádiz), ha sido nombrada nueva Junta Directiva que ha quedado compuesta de la siguiente forma: Presidente, JUAN RODRIGUEZ MORALES; Vicepresidente, JUAN JOSE SANCHEZ BEY; Secretario, JOSE LUIS MONTIEL FERNANDEZ; Tesorero, FRANCISCO MONTIEL FERNANDEZ; Vocales, JOSE HIDALGO AVILA, PEDRO LOPEZ VAZQUEZ, MANUEL NIETO VERDUGO y JOSE M. a GALVIN ESTRADA.\n\nDeseamos a la nueva Junta toda clase de éxitos.\n\nBar TOMAS\n\nAPERITIVOS SELECTOS\n\nEspecialidad en\n\nMesones, 18 Teléf. 23 40 46\n\nPLANCHA\n\nJ A E N",
    "title": "PROXIMOS FESTIVALES",
    "periodical": "candil",
    "issue_id": "1984-07",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "21-23",
    "page_number": 21,
    "word_count": 631,
    "article_char_count_full": 4034,
    "article_char_count_review": 4034,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
