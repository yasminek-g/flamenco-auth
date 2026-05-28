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
    "article_id": "1990-07-11-left-pedro-s-nchez-y-ram-n-porras",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPedro Sánchez-Ramón Porras\n\nDesde su fundación, «Candil», a través de la sección «Ellos, los protagonistas, dicen:» ha lleva- do a sus lectores las opiniones de los artistas más significativos del panorama flamenco, tanto del cante como del toque y el baile, los cuales manifestaron libremente su sentir.\n\nCreemos llegado el momento oportuno de que sean los «aficionanos» quienes nos aporten sus valiosos puntos de vista sobre el flamenco. Y no es que ahora advirtamos un protagonismo que siempre hemos reconocido; es que, en este tiempo, tal vez más que en ningún otro, junto a dignísimas realizaciones, se levantan voces perturbadoras, grabaciones deleznables y artistas que, amparados en intentos de originalidad, más que evolucionar, degradan. Ello hace aconsejable que concurra el criterio de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionanos\"]\n\nlos «aficionanos» quienes nos aporten sus valiosos puntos de vista sobre el flamenco. Y no es que ahora advirtamos un protagonismo que siempre hemos reconocido; es que, en este tiempo, tal vez más que en ningún otro, junto a dignísimas realizaciones, se levantan voces perturbadoras, grabaciones deleznables y artistas que, amparados en intentos de originalidad, más que evolucionar, degradan. Ello hace aconsejable que concurra el criterio de los «aficionanos» para que con la firmeza que no está reñida con la moderación, puntualicen, subrayen, acoten, aplaudan o desmientan. Hoy traemos a nuestras páginas a uno de los más populares de Andalucía. Se trata de José Cruz García, popularmente conocido en el mundo flamenco como Pepe Cruz; presidente que fue de la Peña Flamenca de Jaén durante diez años, merecedor de inaugurar esta sección, tanto por su dimensión humana como por su bien acreditada afición flamenca. —Pepe, ¿cuándo empieza a interesarte el flamenco? —Pues mira, desde muy joven. Porque yo vivía frente a la casa donde vivía la familia de José Serrapi «Niño Ricardo» y me fui familiarizando con el flamenco. Pero, ocurrió que cuando estalló la guerra le cogió en Jaén a una troupe de artistas flamencos y tuvieron que quedarse aquí bastante tiempo. Entre estos artistas estaban Canalejas, Marchena, Pepe «El Culata», «El Peluso», «El Niño Ricardo» y algunos más. Entonces se iban a casa de la familia de Ricardo y yo los escuchaba cantar. Esto era en el año 36, cuando yo tenía 13 ó 14 años. Entonces mi ídolo era Pepe Marchena, porque hay que reconocer que Pepe llenó toda una época del cante. Poco a poco mis conocimientos se fueron ampliando y fui escuchando a otros como Manuel Torre, Chacón, Tomás, Pastora... Más adelante, fui frecuentando el bar «Principal», donde había un buen ambiente flamenco, allí se reunían «El Niño Madrid», Severiano Cortés y otros buenos aficionados. Severiano Cortés era un gita- no que siempre iba muy bien vestido y muy educado; recuerdo que siempre que se despedía de la reunión decía: /señores, el que nos ha juntao aquí, que nos junte en la Gloria! Pero más que en el «Principal», donde nos reuníamos los flamencos era en el barrio de la Magdalena, en la taberna de «la Cachana», que tenía un patio enorme cubierto por una parra. Allí hacíamos nuestros cantes, la mayoría de las veces sin guitarra. En esas reuniones estaban «El Niño Ristra», que era muy buen aficionao y cantaba muy\n\n[ENDING CONTEXT]\n\nuna hija subnormal. Este señor se afincó en Jaén, bueno, ya le conocéis, se trata de Pepe Viñals. El, que nunca había tenido ninguna relación con el Flamenco, se emocionó muchísimo.\n\nOtra vez, estando en Jerez, se me acerca un gitano y me dice: ¿Usté es Pepe Cruz? Y dije sí; y me dice: ¿No se acuerda uste de mí? Pues ahora mismo no caigo, y me dice: Yo vivo gracias a uste, porque estando ingresado en el sanatorio no podían operarme si alguien no me daba sangre y uste me la dio. Cuando el resto de los gitanos se enteraron, estuvimos cantando toda la noche, hasta que sol nos dio en la espalda.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Pedro Sánchez y Ramón Porras",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "11-13",
    "page_number": 11,
    "word_count": 3848,
    "article_char_count_full": 21848,
    "article_char_count_review": 4070,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionanos"
      }
    ]
  },
  {
    "article_id": "1990-07-13-right-bamberas",
    "article_text_for_review": "BAMBERAS\n\nPaco Arana\n\nSe columpia esa gitana la balancea su novio. Mecida entre palabritas que la hablan de casorio.\n\nPastora Pavón, La Niña de los Peines, inmortalizó estos cantes en aquellos «incunables», hoy por obra y desgracia de la ciencia y el progreso casi inaudibles discos de pizarra, que duermen el injusto sueño de los tesoros empolvados.\n\nAquella mujer de embrujados ojos negros, pómulos orientales y boca sensual, que adornaba con peinecillos su pelo azabache, dominó todos los estilos flamencos.\n\nComo buena gitana, se distinguió en los cantes festeros y tiró pellizcos en los palos del cante grande. Los promotores de entonces, le ofrecieron dinero y fama por dedicarse al «cuplé», pseudo-flamenco de su época, a lo que renunció de plano, creando sin embargo, en favor del flamenco puro este semijuego del columpio, variación de una soleá apresurada con un final feliz en tono menor que imprime personalidad y gracia a las bamberas.\n\nPepe Pinto, su compañero y eterno enamorado, balanceó el columpio donde La Niña de los Peines meció su arte. Ambos consiguieron fama y fortuna durante su estancia en esta Tierra y alcanzaron ya ese rinconcito de Cielo en el que posiblemente se reúnan con otros artistas de la época en incomparable y celestial juerga flamenca. Allí, a la atención de Pastora. Dirección: Rincón del Cante y con destino: La Gloria, envío por correo certificado y urgente, estos cantes de columpio con esta dedicatoria:\n\nUn columpio de alegría te voy a hacer en la arboleda, con el asiento de mimbre y los cordeles de seda.\n\nPara La Niña de los Peines, gitana, cabal y artista de grata feliz memoria.\n\nLa bamba se vuelve loca cuando se mece Pastora. Seda y mimbre de alegria su columpio me enamora.\n\nHarás Bien",
    "title": "Bamberas",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 291,
    "article_char_count_full": 1740,
    "article_char_count_review": 1740,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-07-14-left-un-ejemplo-a-seguir",
    "article_text_for_review": "E 1 pasado día 2 de junio y como siempre en el Cine Consulado, se celebró el séptimo festival organizado por esta peña, con el mismo cuidado plantel de artistas a que nos tiene acostumbrados, superando ediciones anteriores en cuanto al éxito se refiere.\n\nNo vamos a entrar en análisis sobre la actuación de cada uno de los artistas que intervinieron en el festival, porque todos ellos brillaron a gran altura, ofreciéndonos una madurez artística que justifica-ba plenamente su incorporación a este grandioso cartel compuesto por Curro Malena, Chaquetón, Manuel Mairena, Fernanda de Utrera y José Menese, acompañados indistintamente por los in-discutibles maestros de la guitarra Juan Habichuela y Enrique de Melchor.\n\nLa adhesión del público a este acontecimiento con el lleno absoluto de la entrada al local, es la mejor justificación que tiene el acierto de la organización que deberán considerar para futuros festivales la posibilidad de elegir local de mayor capacidad para evitar que tantos y tantos aficionados se queden sin acceso para poder presenciarlo.\n\nDe entre lo mucho destacable que pude apreciar a lo largo del festival hay que resaltar la gran actuación del público, que con su saber estar y cariñoso recibimiento a los artistas, provocó en éstos reacciones de agradecimiento que, como en el caso de Manuel Mairena, fueron estremecedores, al besar el suelo del escenario en el que se encontraba y a cuyo gesto se sumaron los continuados piropos del resto de los artistas al pueblo de Madrid y a su afición, comunión de sentimientos que acreditan a esta capital como lo que siempre ha sido, la Catedral del Arte Flamenco.\n\nLa gran actuación de Curro Malena nos permite hacer la observación de que por su gran y potente voz, podría prescindir del micrófono para que llegaran con más perfección al público las sonoridades de su arte.\n\nEncontramos a un Chaquetón, siempre dentro de su línea de regularidad, pero quizás un poco apático y falto de garra seguramente por alguna influencia de tipo anímico que no nos es posible precisar.\n\nGrande fue el triunfo de Manuel Mairena, que en un momento extraordinario de madurez artística nos ofreció una serie de cantes de la mejor escuela, que nos hizo recordar la gran figura que fue su hermano Antonio, ganándose la entrega y el largo aplauso del público que llenó la sala. De Fernanda de Utrera podemos decir que al obsequiarnos con la rancia solera de sus cantes se metió al público en el bolsillo, siendo innecesaria la justificación que ofreció al respetable, porque toda su actuación rezumaba flamenquismo y sentimiento. José Menese repitió una de sus grandes actuaciones en este coliseo que, como en ediciones anteriores, fue acogida con grandes ovaciones y fervor del público asis-tente.\n\nLa gran actuación que una vez más nos ofrecieron los maestros de la guitarra Juan Habichuela y Enrique de Melchor, nos impide, por haberlos agotado ya en ediciones anteriores, aplicar ningún tipo de calificativo. Sigue siendo Juan Habichuela el gran maestro que supedita lucimiento personal para lograr un perfecto acompañamiento al cantaor, arrancándole sus sentires, pudiéndose aplicar la misma teoría al ya joven y consumado maestro que es Enrique de Melchor.\n\nOtro gran aplauso para la organización del festival que se ha fijado un nuevo reto que tendrá que superar en el próximo; cosa fácil de esperar al confiar en las inagotables capacidades del presidente de la Peña Chaquetón, D. Pablo Tortosa.\n\nGracias a Sintel por su continuado y desinteresado patrocinio del festival y a cuyo realce continúa contribuyendo el gracejo flamenquísimo de la presentadora Toñi Alvarez, a la que una vez más felicitamos desde esta columna.",
    "title": "Un ejemplo a seguir",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 598,
    "article_char_count_full": 3682,
    "article_char_count_review": 3682,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-07-14-right-legado-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(A propósito de la primera Muestra de Flamenco Joven, 1990) Los días 24, 25 y 26 del pasado mes de mayo se celebró en Málaga la primera Muestra de Flamenco Joven 1990 promovida por el Instituto de la Juventud, con la colaboración de la Digitalización Provincial y el Ayuntamiento de esa ciudad. A lo largo de estas jornadas se sucedieron las actuaciones de un nutrido grupo de artistas jóvenes, con la participación cada noche de un artista invitado de reconocido prestigio: en este caso esas figuras fueron Camarón de la Isla, el Güito y Manolo Sanlúcar. Aparte del triunfo clamoroso de Camarón, cabe destacar el éxito obtenido por algunos de estos artistas que pese a su juventud apuntan ya a cotas altas dentro del arte flamenco: los cantaores Fernando Terremoto hijo, la Macanita y Antonio\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"público\"]\n\nnocido prestigio: en este caso esas figuras fueron Camarón de la Isla, el Güito y Manolo Sanlúcar. Aparte del triunfo clamoroso de Camarón, cabe destacar el éxito obtenido por algunos de estos artistas que pese a su juventud apuntan ya a cotas altas dentro del arte flamenco: los cantaores Fernando Terremoto hijo, la Macanita y Antonio Carbonell, el guitarrista Jerónimo y el bailaor Javier Barón fueron algunas de las figuras más aplaudidas por el público. Las circunstancias en que se desenvolvió este singular acontecimiento artístico dan pie a reflexionar sobre algunos aspectos problemáticos del mundo del flamenco, aspectos que a nuestro entender ponen de relieve la precariedad en que se sigue sustentando hoy su manifestación pública. Nos parece muy oportuna la organización de una Muestra como ésta encaminada a promocionar y dar a conocer al gran público los nuevos valores del arte flamenco, que a falta de una difusión adecuada suelen quedar restringidos a un pequeño círculo de fieles para acabar extiguiéndose, en la mayoría de los casos, en un lamentable silencio. Ahora bien, este tipo de acontecimientos sólo tiene sentido si nace con el firme propósito de continuidad en años posteriores. Lo contrario supone abundar en esa política voluntaria, a la que tan frecuentemente se recurre en el espectáculo flame\n\n[ENDING CONTEXT]\n\nTokio existen en la actualidad más de una veintena de tablaos flamencos, resulta sorprendente que no se reclame desde aquí la paternidad de un fenómeno que ha rebasado ya con creces sus fronteras de origen. Deben ser los propios focos tradicionales de irradiación del flamenco los que posibiliten una infraestructura que dé a conocer internacionalmente un patrimonio (y se hace necesario recordar una vez más que estamos ante uno de los lenguajes artísticos más elaborados de la cultura occidental) que merece estar por encima de los planteamientos obsoletos en que se ha solido mover hasta ahora.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Legado flamenco",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1205,
    "article_char_count_full": 7468,
    "article_char_count_review": 2948,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "público"
      }
    ]
  },
  {
    "article_id": "1990-07-15-right-una-evidencia-ignorada-durante-c",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE n la conferencia internacional «Dos siglos de Flamenco», Jerez, 21/25 de junio de 1989, don José Luis Buendía López, presentó la ponencia «El flamenco en la poesía y en la novela», magnífico y extraordinario ensayo cuyo texto fue recogido en el libro de actas, publicado por la Fundación Andaluzía de Flamenco y reproducido en su día en la revista «Candil». En esta ponencia se cita la influencia del poeta nicaraguíense Félix Rubén García Sarmiento, Rubén Darío (1867-1916), sobre la nueva poesía «modernista» andaluzía. Textualmente se dice que el libro del vate americano «Tierra solares», publicado en 1904, es un canto al flamenco y a Andalucía en sí y que la herencia poética de Rubén Darío «...se desarrolla por Andalucía como una mancha de aceite, sus ecos estremezcan a discípulos como\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"leyenda\"]\n\nherencia poética de Rubén Darío «...se desarrolla por Andalucía como una mancha de aceite, sus ecos estremezcan a discípulos como Villaespesa o Salvador Rueda, y que junto a cis-nes y lagos decadentes, al lado de alejandrinos de perfecta tersura, asoma rabiosa la soleda, para que estos poetas den rienda suelta a su trágica sensibilidad meridional aprisionada». En el año mágico que puede ser el de 1992 ;terminará de una vez para siempre la falsa leyenda que tan bien sembraron los componentes de la élite intelectual de la llamada «Generación del 98»? Lo expuesto por el señor Buendía López, a mi modo de ver, no se adapta fielmente a la realidad, quizá motivado por las razones que encabezan estas líneas. Rubén Darío conoció a Salvador Rueda en 1892, fecha en que el vate nicaraguíense viene a España como embajador plenipotenciario de su país para los actos conmemorativos del IV Centenario del Descubrimiento de Amé- rica. En esta fecha, el genio de Benaque tenía ya publicados once títulos en los que desde un principio se le ven nuevas formas métricas, utilizando el verso dactílico proveniente de la canción popular, dodecasílabo de siete más cinco, también llamado dodecasílabo de seguidilla, empleado por Salvador Rueda en su poema «Bailadora». Cuando Darío conoce a Rueda por esta época, tenía publicados el americano cuatro títulos. Tres de ellos inspirados en los románticos y post-románticos y el cuarto fue su célebr\n\n[ENDING CONTEXT]\n\nRondalla de Paco Soler, gran músico malagueño empeñado en llevar la malagueña de baile al pueblo, y el director del aula, Profesor Arrebola, que cantó diversas composiciones poéticas de Rueda.\n\nPor otra parte he de hacer constar que por el Profesor Arrebola ha sido entregado a la Universidad de Málaga, un libro sobre poetas malagueños y sus letras flamencas, y entre ellos con gran preferencia, figura Salvador Rueda. Esperemos tener la suerte de verlo publicado pronto. Rubén Darío.\n\nRepresentante\n\nJ. A. PULPÓN\n\nO'Donnell, núm. 3 - 4.º Teléfs. 222058 - 216920\n\nSEVILLA\n\nPaticular: Teléf. 228078\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "UNA EVIDENCIA IGNORADA DURANTE CASI CIEN AÑOS (1892-1992)",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1482,
    "article_char_count_full": 8904,
    "article_char_count_review": 3059,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "leyenda"
      }
    ]
  }
]
```
