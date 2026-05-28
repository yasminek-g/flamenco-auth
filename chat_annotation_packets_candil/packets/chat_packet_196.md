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
    "article_id": "1989-07-24-left-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nProgramación flamenca del Ayuntamiento de Jerez\n\nCinco Viernes Flamencos y la Fiesta de la Bulería es la oferta flamenca que el Ayuntamiento jerezano tiene previsto ofrecer en el Parque González Hontoria, a partir del 18 del próximo mes de agosto y hasta el 16 de septiembre.\n\nCada uno de los cinco viernes tiene una especial dedicatoria: al barrio de Santiago, al barrio de San Miguel, a lo Puro de Jerez, a Nuevos Valores del Flamenco y Nuevas Formas de Flamenco, respectivamente, sin que nos hayan sido facilitados —hasta el momento— nombres de intérpretes que habrán de protagonizar los referidos espectáculos. Sí hemos podido conocer el presupuesto aproximado que el Ayuntamiento destinará este año para la programación flamenca del verano: más de cuatro millones de pesetas importa la nómina\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nades Flamencas que, como es sabido, se celebrará en Jerez del 13 al 17 de septiembre, bajo el epígrafe Flamenco y Futuro, y dedicado a Silverio Franconetti. En Sanlúcar de Barrameda, víctima de fatal accidente cardíaco y cuando disfrutaba de un descanso en sus actividades empresariales, ha fallecido Luis de Pacote. Hace unos quince años que en la constelación cantaora jerezana apareció con fuerza y hondura el joven Luis de Pacote (Francisco Lara Peña), voz afila, redonda y cortante como una faca, tocado de las más altas capacidades para ocupar un lugar de privilegio entre los grandes cultores del cante del Barrio. Su decisión de encaminarse por el mundo empresarial, haciéndose cargo del popular restaurante «El Burladero», torció el rumbo de quien se presentaba ya como ungido por los dioses del arte gitano-andaluz. Quienes disfrutamos de su grandeza cantaora, iniciada con tan prometedores augurios, y nos honramos con una amistad de oro de ley, sentimos hoy la pena infinita que su desaparición\n\n[ENDING CONTEXT]\n\nla modalidad de cantes libres volvió a ganar el sevillano Paco Moya, que también el año anterior consiguió el primer premio.\n\nPara comer Jamón... Jamón VISITE\n\nTaberna Pepón\n\nC/. Doctor Arroyo, 12\n\nTeléfono 210058\n\nJAEN\n\nMaría Soleá nació en Jerez en 1932. Cantaora y bailaora. Hermana de «Terremoto» y sobrina de Tía Juana la del Pipa, Tío Parrilla y «El Borrico».\n\nDe sus características como cantaora se ha dicho que es rotunda y contundente, heredera del cante grande, gitano y puro de su hermano.\n\nSu quejío «terremotero» tiene todo el duende del mundo, auténticamente gitano.\n\nLA GENERAL\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "24-27",
    "page_number": 24,
    "word_count": 1092,
    "article_char_count_full": 6799,
    "article_char_count_review": 2625,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "1989-07-25-left-hablan-las-pe-as-amigas",
    "article_text_for_review": "HABLAN LAS PEÑAS AMIGAS ☐ ☐ ☐ ☐ ☐\n\nLa Peña Flamenca Antonio Mairena, de Hospitalet (Barcelona), y el Ayuntamiento de la misma ciudad, convocan el VI Concurso de Cante Jondo «Ciudad de Hospitalet», en el que podrán tomar parte todos los cantantes de ambos sexos que lo deseen y que cumplan con la condición de ser profesionales no consagrados o aficionados.\n\nLos interesados podrán solicitar su inscripción en la citada Peña, calle Mina, 16, antes del 23 de septiembre, si bien el Concurso se celebrará durante la primera quincena de noviembre.\n\nHan sido establecidos dos grupos de cantes, así como cinco premios, siendo el primero de 150.000 pesetas.\n\nEn asamblea general celebrada el pasado día 2 de junio por la Peña Flamenca «El Manantial», de Sevilla, fue elegida nueva junta directiva, quedando la misma compuesta de los siguientes nombres: presidente, Antonio Valverde López; vicepresidente, Joaquín Muñoz García; secretario, Jenaro Vázquez Parra; tesorero, Francisco Pérez Carmona; relaciones públicas, Juan Frías Alvarez; vocales, Manuel Llorca Tey, Enrique Morueta Pascual y Manuel Suárez Alvarez.\n\nVI Concurso de Cante Jondo «Ciudad de Hospitalet. Memorial «Antonio Mairena»\n\nDeseamos toda clase de aciertos a la nueva junta.\n\nNo podrán presentarse los ganadores del primer premio de ediciones anteriores.\n\nNueva Junta Directiva de la Peña Flamenca «El Manantial», de Sevilla\n\nEn reciente asamblea general celebrada por la Federación Extremaña de Peñas Flamencas, ha resultado elegido nuevo presidente Joaquín Rojas Gallardo, que viene a sustituir a nuestro buen amigo Francisco Zambrano Vázquez.\n\nA la citada asamblea asistieron las treinta y tres peñas que en la actualidad componen la Federación Extremeña.\n\nV Concurso de Cante Flamenco «Yunque Flamenco»\n\nNuevo presidente de la Federación de Entidades Flamencas de Extremadura\n\nOrganizado por la Federación de Entidades Culturales Andaluzas en Cataluña, ha sido convocado el V Concurso de Cante Flamenco con el patrocinio del Ayuntamiento de Santa Coloma de Gramanet. Para esta edición han sido establecidos tres grupos de cantes y cuatro premios, siendo el primero de 200.000 pesetas.\n\nDeseamos a la nueva junta una acerta- da andadura flamenca.\n\nEl plazo límite de inscripción finaliza el día 27 de octubre de 1989, siendo la final el día 2 de diciembre.\n\nPara más información, los interesados pueden dirigirse a la citada Federación, calle Balmes, 61, principal 3.ª, Barcelona-08007, o llamando al teléfono 93-2536000. Ganadores del IV Concurso de Cante de la Peña Flamenca de Huelva.\n\nFinal celebrada en el Club Raúl, de Lepe, el pasado viernes día 18 de agosto 1989",
    "title": "HABLAN LAS PEÑAS AMIGAS ☐ ☐ ☐ ☐ ☐",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 409,
    "article_char_count_full": 2634,
    "article_char_count_review": 2634,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-07-25-right-buz-n",
    "article_text_for_review": "Revista Candil Sr. D. Pedro Sánchez Ortega\n\nEstimado amigo:\n\nEstoy leyendo las declaraciones que hiciera, tiempo atrás, el gran Aurelio Sellé (q.g.h.) y, la verdad, no estoy de acuerdo con él, aunque me consta que lo mismo que fue el finado, fueron casi todos los artistas con quienes convivió en la década de los cuarenta y años posteriores. Nunca vi familia más deplorable. ¡Cómo se «rajaba»! Pero siempre aprovechándose de la ausencia de aquel a quien se «rajaba». Yo era entonces muy joven, pero mi juventud no me impidió hacer una justa valoración o apreciación del mundo en el que me había incrustado, llevado por mi innata afición. Por ello, a mí, aficionado ahora en el declive de mi vida activa, pero con la experiencia vivida, no me ha extrañado que Aurelio se hubiera expresado en esos términos ante mi recordado amigo Anselmo González Climent. A mí me da pena que los «alevines» que lo lean puedan, inteligentemente, desviarse del camino flamenco que han comenzado a recorrer.\n\nOtra verdad que me transmitió Pastora es que el viejo abandonó en Jerez a su esposa e hijos y se trasladó a Sevilla, instalándose en las afueras en una chabola inmunda y sólo Dios sabe cómo pudo vivir en tal situación.\n\nYo, que tuve la fortuna de tratar en innumerables ocasiones con los Pabones, Vallejo, El Gloria, Mazaco, Pareja Majarón y tantos y tantos otros buenos cantaores, tengo que decir rotundamente, con conocimiento de causa, que Aurelio no se ajustó a la realidad histórica de los hechos acaecidos, cuando dice que Pastora Pabón aprendió los cantes de los Medina jerezanos directamente del «Viejo Medina».\n\nLa verdad es que Pastora, según me dijo, aprendió los tangos, las bulerías y la petenera de oírselas, una y otra vez, a José, hijo del «patriarca» Medina. Más tarde, el resto de la familia Medina —esposa e hijos— se trasladaron a la ciudad hispalense cuando José ya se sintió cantaor y capaz de ganar dinero con su arte. Se instalaron en un caserón comunal en la calle de Calatrava, 14, donde precisamente vivían los Pabones.\n\nCuando «El Niño de Medina» organizó su primera compañía, con la que recorrería los pueblos de Andalucía, para ganarse el sustento, se llevó consigo a su vecinita Pastora, que ya a sus dieciséis años de edad dejaba entrever la calidad de artista inmensa que sería después: una cantaora sin par.\n\nPastora, hablando de José, me decía: ¡Qué cantaor más completo! ¡Cómo lo cantaba todo! ¡Cómo cantaba especialmente por mineras, bulerías, tangos y por la petenera de su padre!\n\nPor lo visto, para Aurelio no hubo ni un solo cantaor que no tuviera algún defecto. ¿Y él no los tuvo? ¡Pues claro que los tuvo! Para mí los tenía de forma tan destacada que rara vez pasaba por el gramófono una placa de las grabadas por él con Ramón Montoya a la guitarra. Ese «be, bé», ese «guin, guin» y ese meter la nariz hasta más no poder, los resaltó de tal manera en sus últimas grabaciones, con Andrés Heredia a la guitarra, que me obligaron a no volverlo a escuchar más.\n\nTampoco para él Antonio Mairena fue perfecto. Y es público y notorio que Antonio fue un gran copista por su excelente oído musical y, además, fue un intérprete genial de los cantes de unos y de otros. Negarle esas virtudes es tanto cómo negar la existencia del sol que nos calienta.\n\nEn fin, amigo Pedro, a pesar de todos los pesares, estoy seguro de que Dios perdonó al gran Aurelio en el momento de su expiración.\n\nNada más; reciba un saludo cordial de su affmo.,\n\nManuel Yerga Lancharro\n\nRevista Candil Señor Director:\n\nLe agradecería publicara esta carta para enderezar un entuerto, ya que leí en la revista número 62 lo que escribe don Manuel Yerga sobre la Bandolá, diciendo que no admite este cante flamenco como tal. Mi opinión de la Bandolá es la siguiente:\n\nEn Málaga, donde esta facilidad de procreación es más palpable, con un fandango especial que aquí recibe el nombre de Bandolá. Denominación que quizá provenga de Bandola, pequeño instrumento musical parecido a la guitarra que da notas muy agudas.\n\nDel árbol genealógico de los cantes vernáculos malagueños, el tronco es la Bandolá. Si los Verdiales, que serían la raíz, tienen el mérito de su antigüedad, ellas tienen el de ser el punto de conexión y la masa generatriz de todos los demás cantes de esta familia, pues al adquirir la elasticidad y la holgura que aquéllos no tenían, llegaron en su evolución a extremos insospechados, dando una rica variedad de tipos —dentro aún de las Bandolás— y provocando el nacimiento de Malagueñas y Jaberas.\n\nLa supremacía que en los Verdiales mantienen los montes de Málaga, en las Bandolás la ostenta Vélez-Málaga. Las Bandolás son de playa y de huerta, y su principal desarrollo está en la faja costera oriental de la provincia malagueña.\n\nÉs también en Vélez-Málaga donde, por obra y gracia de Juan Breva, las Bandolás adquirieron su más plena configuración.\n\nDejando aparte los cantes de Juan Breva, que merecen tratarse con más detenimiento, hemos de destacar de las múltiples variedades de Bandolá, el cante de los Jabegotes, más popularmente llamado Cante de los Morengos. Es probablemente la Bandolá más antigua que conocemos. Atentamente,\n\nAndrés Borrego Escudero-Alora",
    "title": "Buzón",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 881,
    "article_char_count_full": 5177,
    "article_char_count_review": 5177,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-07-26-right-iben-alcazar-premium-criviza-spi",
    "article_text_for_review": "Si señor. Si ha pedido una cerveza Alcázar: ¡bien hecho! Porque va a saborear una cerveza fresca, con cuerpo, en su punto. Una cerveza elaborada con las mejores cosechas de lúpulo y cebada, siguiendo la tradición de nuestros maestros cerveceros. Una cerveza que mantiene todo su aroma, porque va,\n\ncomo quien dice, de la fábrica directamente al consumidor. Si pide cerveza Alcázar, ¡bien hecho!. Disfrutará de una cerveza bien hecha.",
    "title": "IBEN Alcazar Premium CRIVIZA / SPICION HECHO!",
    "periodical": "candil",
    "issue_id": "1989-07",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 70,
    "article_char_count_full": 433,
    "article_char_count_review": 433,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-09-3-right-a-pedro-s-nchez",
    "article_text_for_review": "P or sólo un tiempo, esperamos, se aparta del colectivo «Candil» quien, desde su fundación hace más de una década, ha sido su motor, su obrero incansable, su paciente y riguroso animador: Pedro Sánchez. Pequeño como la esencia; enjuto como un asceta, preciso como la soleá, incómodo como un castigo paterno, indomable, sentimental, equívoco pero verdadero, amigo de Juan Ramón y de Neruda, amigo de la luna, amigo apasionado del Flamenco. Nadie, como él, desde primitivas ignorancias ha buceado con tanto fervor en el misterio de lo jondo, se ha sometido a la humilde disciplina de aprender, ha reflexionado con tanta insistencia como sensibilidad y, en último término, ha captado y, lo que es más importante, ha sabido proyectar la magia del Flamenco.\n\nSé que pudiera atribuirse a lo dicho amistosa subjetividad, encajes y florituras de una loa al uso, tan insustancial como irrelevante para los lectores de «Candil» y prometo que no lo pienso así. En el empeño de Pedro, en su paciente trabajo, se significan cientos de personas anónimas que con indesmayable tesón han ubicado el Flamenco en su actual cota de universal e indiscutida aceptación. Personas que\n\nEditorial A PEDRO SÁNCHEZ\n\nentregan tiempo y patrimonio por la salvaguarda, por la difusión de este hermoso legado, a veces sin otra contrapartida que la indiferencia y la incomprensión. Examinada la más reciente historiografía del Flamenco, con un mínimo de generosidad, se evidencia en la recuperación de éste, el rol importantísimo de estas personas que sin ser artistas, ni reconocidos escritores, ni prebostes de nada, desde su Peña, desde su tertulia, desde su anonimato, han generado una conciencia colectiva, respecto al Flamenco, motivadora de universal apreciación, una conciencia que ha precedido incluso a las doctas sistematizaciones, a teorías eruditas, la cual, como la vida, precede y se adelanta siempre a la sistematización.\n\nNuestro cabal homenaje a quienes, por encima de todo protagonismo, han hecho posible la dignificación del Flamenco, la profundización en el mismo, erigiéndose en permanentes vigías de su pureza frente a detractores o gentes simplemente desinformadas. Nuestro homenaje a Pedro Sánchez que, un día, puso candela en nuestro «Candil».",
    "title": "A Pedro Sánchez Editorial",
    "periodical": "candil",
    "issue_id": "1989-09",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 351,
    "article_char_count_full": 2236,
    "article_char_count_review": 2236,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
