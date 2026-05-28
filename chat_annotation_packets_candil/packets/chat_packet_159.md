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
    "article_id": "1987-09-19-right-cantaores-conocidos-compa-eros-y",
    "article_text_for_review": "¿Que puedo decir yo de\n\nPastora que no se haya dicho? Pero mi intención no es decir nada nuevo, aunque la verdad es que siempre quedará algo nuevo que decir de todo lo que pasa por la vida dejando surcos geniales. Como mínimo, de Pastora Pavón tendremos que decir que seguiremos recordándola tanto los que la conocimos y la tratamos como los que sólo conocen y tratan su cante, que es lo que importa en definitiva.\n\nLuis Caballero\n\nPocas veces encontramos, en el diverso y amplio panorama del cante, intérpretes con esa cualidad específica que antes de Franconetti se llamó cantaores generales y después de Pastora completos. Precisamente Pastora fue genial e intachablemente completa, y si se alega que no cantó tonás bien podría pensarse que las excluyó instintivamente en su repertorio por inadecuadas a su exquisita feminidad. Todo cuanto se propuso cantar lo cantó perfectamente bien, pues si orientada en la línea de Manuel Torre llegó tan lejos como para ser ella sola, en la de su admirado Chacón destacó paralelamente. Son evidencias que me sugieren una cierta discrepancia respecto a aquellos viejos aficionados que con tanto entusiasmo, respeto y simpatía la consideraron como festera primordial.\n\nDice Climent que «Pastora, que puede vocalizar como el más engolado gaitero del momento, unge el cante con el mismo soplo de majestuosidad que cupo en la figura de don Antonio Chacón o con el mismo desgarro trágico que siempre palpitó en el gran Aurelio de Cádiz. Oírla implica temblar hasta el tuétano. Es la absoluta vibración del drama y la absoluta gracialidad de lo ligero, el perfecto vértice —¡sevillana tenía que ser!— de Andalucía Alta y Andalucía Baja. En ambos casos, indistintamente, nos produce sismos afectivos. De su garganta todo sale, denso, tajante, cabal».\n\nPastora fue muy mujer como mujer y muy mujer como cantaora. Rezumaba feminidad como humana y como artista: Peinecillos en su pelo negro de andaluz, gitana y flamenca, pulseras en sus brazos morenos de bailaora, tumbagas en sus finas y cuidadas manos, mantones de Manila para cantar como en un trono y ese concepto del hombre macho, valiente, decidido, que vislumbra el ángel de sus jaleos a través de sus propias intervenciones discográficas o las de otros en su presencia y afecto. Esa acentuada feminidad puede ocasionar en el oído receptor del aficionado hecho al rasgo varonil una sensación de excesiva facilidad, facilidad que puede trivializar lo que la «pelea» podría hacer hondo, pero el aficionado total, el que va más allá, el que no se anquilosa, distingue, valora y siente la diferencia entre mujer y hombre a la hora de cantar, encuentra en Pastora toda «la absoluta vibración del drama y la absoluta gracilidad de lo ligero.\n\nHuelga repetir una vez más lo que de sobra sabemos: que no siempre la manifestación psíquica de la situación-límite se traduce mediante el «tarab» y se puede sonar frío, pero aun así, Pastora sonaba flamenca, gitana y dueña de una técnica y conocimientos fuera de lo común.\n\nCon el respeto que merecen los ídolos, le pregunté, sentados una de aquellas mañanas en la puerta del famoso bar Pinto: «Pastora, ¿es verdad lo que cuenta Federico García Lorca en su conferencia \"Teoría y juego del duende\", que en una juerga en Cádiz usted no se \"encontraba\" y se levantó \"como una loca\" y se bebió de un trago un gran vaso de cazalla como fuego, y se sentó a cantar, sin voz, sin aliento, sin matices, con la garganta abrasada, pero... con duende?». Según deduje de su respuesta, lo único que le importó de la pregunta fue lo del gran vaso de cazalla, y se perdió explicándome cómo ella no había sido jamás bebedora. No pude lograr que me recordara aquella fiesta flamenca al lado de Federico. Sin embargo, insistí sobre su conocimiento con el poeta: «Sí hombre, sí, García Lorca. Ese me escribió a mí unas letras mu bonitas, las lorqueñas. Le gustaba mucho el cante». Y el Pinto, con una fotografía de ella en la mano: «Pastora, dedicasela a Luis y a su mujer». Pero Pastora se limitó a garabatear su nombre en la cartulina. «Pónganos usted algo, díganos alguna cosa en la foto». «¿Que yo ponga qué? Poner ustedes lo que quieran; que vía poné yo, mi arma, si con siete años ya estaba cantando y bailando pa poder vivir».\n\nLa conocí escuchándola de cantar, al lado de su marido, recién casados. Aunque muy joven y desligado del ambiente profesional del cante, recuerdo cómo aquel enlace no dejó de ser un acontecimiento, pues a pesar de la lentitud de los medios de comunicación y difusión de entonces la afición se interesó por la anécdota flamenca que encerraba el caso.\n\n¡Quién iba a decirme entonces que medio siglo después mi nombre iba a figurar entre el monumento que un buen día le erigimos junto a las columnas de la Alameda de Hércules.",
    "title": "Cantaores conocidos, compañeros y amigos: Pastora Pavón",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 813,
    "article_char_count_full": 4765,
    "article_char_count_review": 4765,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-09-20-left-las-soleares-de-juaniqu",
    "article_text_for_review": "uando se bucea en lo jondo del cante\n\nUa. lo jondo del cante flamenco en la búsqueda de alguna raíz flamenca, nos encontramos, en muchas ocasiones, con los cantes que interpretara Antonio Mairena. De esta manera, el aficionado que quiera estudiar alguna parcela del cante flamenco se encontrará, inevitablemente, con el maestro de Mairena, entre otros cantaores. Por supuesto, si los palos en estudio pertenecen a los cantes que hoy se les denomina básicos (gracias al maestro), no es que los demás cantes no sean básicos o fundamentales. Lo que ocurre es que había que agruparlos para su estudio de alguna manera. En todo caso, parece como si Antonio Mairena hubiera seguido la norma de la Real Academia de la Lengua Española por aquello de: «limpia, fija y da esplendor».\n\nLas soleares de Juaniquí\n\nMuchos cantaores, a través de la historia del cante, han interpretado, en su repertorio soleaero, las soleares que forjara Juaniquí. Sin embargo, fue Antonio Mairena el que dejó constancia de dichos cantes con la etiqueta puesta para la posteridad. Unas veces recordando a otros cantaores (soleares de Frijones, Illanda y Juaniquí), y otras en alguna serie de soleares dedicadas al insigne soleaero que pasara largos años de su vida por los pagos de Lebrija.\n\nEn cierta ocasión, Juaniquí asistió a una boda en Lebrija y, entre exclamaciones jocosas, se presentó a los asistentes con la siguiente frase: «¡Aquí está Juaniquí!». A dicha boda estaba invitado Antonio Mairena, que cantó, como es natural, al igual que Juaniquí, que nuevamente exclamó: «Éste —refiriéndose a Antonio Mairena—triunfará, aunque eso yo no lo veré». Ricardo Rodríguez Cosano\n\nPues bien, Antonio Mairena, a lo largo de su extensa discografía, canta los tres estilos de soleá de Juaniquí; tres estilos cuyas estructuras musicales estaban perfectamente definidas. El primer estilo se puede hacer por «arriba» o por «abajo». Es decir, haciendo el segundo verso en escala ascendente o descendente en cuanto a las notas musicales. Nos imaginamos a Juaniquí, ya en la tercera edad, interpretando este primer estilo iniciando el segundo verso por abajo casi con toda seguridad. Posiblemente calará más que por arriba. Antonio Mairena canta este primer estilo en la siguiente letra:\n\nPrimer estilo (por «abajo»)\n\n1. Esto sí que es cosa grande\n\n2. tiro piedras al agua\n\n3. y salen gotas de sangre.\n\nJunto a esta primera letra, el maestro de los Alcores canta esta segunda para explicar la diferencia que existe dentro de este primer estilo, pero ahora iniciando el segundo verso por «arriba». Es posible que Antonio escuchara por tierras de Lebrija las dos versiones o sólo por «abajo» forjando, con su sentido musical, la segunda versión a partir del segundo verso:\n\nPrimer estilo (por «arriba»)\n\n1. Cuando en la calle te encuen- [tro\n\n2. yo te hago tres cruces\n\n3. como si te hubieras muerto. También Antonio Mairena tiene grabada la siguiente letra flamenca dentro de este mismo grupo, es decir, primer estilo «por arriba»:\n\nPrimer estilo (por «arriba»)\n\n1. No siento en el mundo más\n\n2. que seas de tantos metales\n\n3. y yo de un solo metal.\n\nEn relación a los estilos siguientes, creemos, por la opinión de viejos aficionados de Lebrija, que sus estructuras musicales estaban totalmente acabadas, por lo que Antonio Mairena se limitó a interpretarlas según los patrones de Juaniquí. He aquí dos letras que cantara Antonio Mairena dentro del segundo estilo:\n\nSegundo estilo\n\n1. Desde que murió mi mare\n\n2. la camisa de mi cuerpo\n\n3. no encuentro quien me la [lave.\n\nSegundo estilo\n\n1. Por allí viene mi bata\n\n2. déjala pasar de largo\n\n3. que a mí sus dudas me ma- [tan. De la misma manera, todos los cantaores que interpretan este seg- gundo estilo lo hacen siguiendo es- ta estructura musical que dejara Juaniquí y que no se presta a nue- vas modificaciones.\n\nPor último, queda el tercer estilo, al que no pocos hacían referencia, pero que luego no se cantaba y si alguien lo cantaba no sabía que era de Juaníquí en la mayoría de los casos. Pues bien, gracias a Antonio Mairena quedó grabado este tercer estilo para que no hubiese dudas al respecto. Es una estructura musical más compleja que las anteriores, con menos connotaciones audibles, y que se podría considerar como una soleá de cierre o despedida. Antonio Mairena tiene grabado este tercer estilo con la siguiente estrofa que paradójicamente es una seguidilla castellana incompleta:\n\nTercer estilo\n\nLa media naranja\n\nen una laguna\n\nto aquel que la vea\n\ncreerá que es una.\n\nHe aquí la ingente labor de Antonio Mairena en el rescate de los cantes, como en este ejemplo de estilo de soleá que posiblemente se hubiese perdido o, en caso de haberse cantado, se hubiese ignorado su procedencia. Labor digna de encomio la de Antonio Mairena en el rescate y catalogación de estas reliquias sonoras que después de positó con nuevo brillo, en el gran archivo del cante flamenco.",
    "title": "Las soleares de Juaniquí",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 817,
    "article_char_count_full": 4897,
    "article_char_count_review": 4897,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-09-20-right-ablan-las-pe-as",
    "article_text_for_review": "En Asamblea General de Socios, celebrada el pasado 12 de julio salió reelegida como JUNTA DIRECTIVA la siguiente:\n\nPresidente: Manuel Martín Alcaide.\n\nNueva junta directive de la Peña Cultural Flamenca Miguel Vargas de Paradas\n\nVicepresidente: Francisco Fuentes Rubio.\n\nTesorero: Miguel González Recacha.\n\nVocales: Rafael Jurado Suárez, Manuel Barrera Recacha, Joaquín Jurado Suárez, Joaquín León Saucedo, José Suárez Aguilar, José Pavón Guisado.\n\nSecretario: Máximo López Jiménez.\n\nEl período de gestión es para 2 años.\n\nLa nueva sede de la peña se encuentra en calle Larga, 54 (Casa de la Cultura). Con el deseo de que den la oportuna publicidad a nuestro escrito, reciba nuestra más firma aprobación a la andadura flamenca de CANDIL y nuestro saludo cordial.\n\nGanadores del III Concurso de Cante Flamenco de la Peña Flamenca de Huelva\n\nLos ganadores en la final del III Concurso de Cante Flamenco, organizado por esta entidad, celebrado el pasado día 4 de julio son:\n\n2. $ ^{\\circ} $ premio: Jesús Carrillo Real.\n\n1.º premio: Juan Delgado Gálvez.\n\nGrupo A:\n\n3. $ ^{er} $ premio: Antonio Medreles Mera.\n\nGrupo B:\n\n1. $ ^{er} $ premio: Mariana Cornejo.\n\n2. $ ^{\\circ} $ premio: Francisco Moya Pedroza.\n\n3. $ ^{er} $ premio: Rafael Muñoz Barbero.\n\nGrupo C:\n\n1.º premio: Eduardo Hernández Garrocho.\n\n2. $ ^{\\circ} $ premio: Antonio Rodríguez Rodríguez.\n\n3. $ ^{er} $ premio: Mario Garrido Cabezas.\n\nIV Concurso de Cante Flamenco de la Peña Flamenca de Rute\n\nOrganizado por la Peña Flamenca de Rute (Córdoba) y con el patrocinio del Excmo. Ayuntamiento de la ciudad, ha sido convocado el IV Concurso de Cante Flamenco. Los concursantes deberán interpretar, tanto en la fase selectiva como en la final, dos cantes libres y uno obligado compuesto por: TONÁ, POLO, CAÑA, LIVIANA y SERRANAS.\n\nLa final tendrá lugar el día 28 de noviembre del presente año.\n\nPara esta edición han sido establecidos cinco premios, siendo el primero de 150.000 pesetas. Para más información los interesados deberán dirigirse a la Peña Cultural Flamenca de Rute (Córdoba), calle Blas Infante, o llamando a los teléfonos (957) 526515 y 526115.\n\nIV Concurso de Cante Rincón Flamenco\n\nCon el patrocinio de la Excma. Diputación, Ayuntamiento de Córdoba, la Federación Provincial de Peñas Flamencas y varias firmas comerciales, la Peña «El Rincón Flamenco» ha convocado el IV Concurso de Canté, en el que podrán participar todos los cantadores de ambos sexos, profesionales o aficionados que lo deseen, dirigiéndose a la citada peña, calle Rejas de Don Gome, número 4. Córdoba.\n\nSe harán tres grupos de cante, por lo que cada cantar deberá interpretar un cante de cada grupo más uno libre.\n\nHan sido establecidos seis premios, el primero de 125.000 pesetas.\n\nPresidente: José Arrebola Rivera.\n\nPEÑA FLAMENCA FOSFORITO. Tomás Conde, 2. Teléfono 484572. Córdoba.\n\nVicepresidente 1.º: José Arias Espejo.\n\nPEÑA FLAMENCA CAYETANO MURIEL.—Ronda Parque, s./n. Teléfono 530123. Benamejí (Córdoba).\n\nPEÑA FLAMENCA EL MIRABRAS.—Callejón del Mirabrás, s./n. Fernández (Córdoba).\n\nPEÑA FLAMENCA DE CORDOBA.—Romero Barros, s./n. Teléfono 470042. Córdoba.\n\nVice-secretario: Francisco del Cid Garcia. PEÑA FLAMENCA DE CORDOBA.—Romero Barros, s./n. Teléfono 470042. Córdoba.\n\nTesorero: José Muñoz Molina.\n\nPEÑA RINCON FLAMENCO.—Rejas de Don Go- me, 4. Teléfono 481106. Córdoba.\n\nRelaciones públicas: José Urbaneja Diéguez. PEÑA FLAMENCA RINCON DEL CANTE.—Carretera Palma del río, km. 10. Apartado de Correos 366. Córdoba.\n\nFrancisco González Ramírez.\n\nPEÑA FLAMENCA LA SOLEA.—Rioseco, 25. Palma del Río (Córdoba).\n\nFrancisco Gutiérrez Márquez.\n\nPEÑA CULTURAL FLAMENCA DE EL CARPIO. Castillo. El Carpio (Córdoba).\n\nCristóbal Díaz Salido.\n\nPEÑA FLAMENCA CASTREÑA.—Ronda de Granadilla, 14. Castro del Río (Córdoba).\n\nAlfonso Cabrera García.\n\nPEÑA FLAMENCA AGUSTIN FERNANDEZ.—Pozoblanco (Córdoba).\n\nAntonio Osuna Cobos\n\nPENA CULTURAL FLAMENCA LA BULERIA.—Arco de la Villa, s./n. La Rambla (Córdoba).\n\nPedro Muñoz Cordero.\n\nPEÑA FLAMENCA LA BARRERA.—Plaza de la Barrera, 1. Teléfono 501098. Lucena (Córdoba).\n\nTomás Castro Muñoz.\n\nPENA CULTURAL FLAMENCA ENCINA Y JARA.—Teléfono 121420. Villanueva de Córdoba.\n\nDiego Ayllón Vergara.\n\nPEÑA FLAMENÇA JOAQUIN GARRIDO.—Cal- vario, 1. Montoro (Córdoba).\n\nRafael Martín Ramos. PEÑA CULTURAL LUIS DE CORDOBA.—Los Naranjos, s./n. El Rivero-Posadas (Córdoba). Juan Castañeda del Rosal.\n\nPEÑA FLAMENCA BAENENSE.—L. Fdez. Martos, 5. Baena (Córdoba).\n\nManuel del Rosal Luna.\n\nPEÑA FLAMENCA EL MIRABRAS.—Callejón del Mirabrás, s./n. Fernández (Córdoba).\n\nAndrés Alcaraz Alcaraz.\n\nPEÑA CULTURAL FLAMENCA FRASQUITO DE PTE. GENIL.—Madre de Dios, 29. Teléfono 602793. Puente Genil (Córdoba).\n\nO'Donnell, núm. 3-4.º\n\nTeléfs. 222058 - 216920\n\nSEVILLA\n\nPARTICULAR:\n\nTeléfono 278078",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 702,
    "article_char_count_full": 4757,
    "article_char_count_review": 4757,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-09-21-right-las-letras-flamencas-de-emilio-b",
    "article_text_for_review": "Las letras flamencas de Emilio Bustos\n\nMe duele la boca cuando la maldigo, como me dolían sus besos calientes boy llenos de frío.\n\nCon las claritas del día me dabas tu beso fresco con sabor a despedida. Era pobre la porfía; un gitano y un gachó templarse por bulerías.\n\nTanto es lo que te deseo que hablo solo por la calle y cuando sueño, te veo.\n\nAquella buena gitana cantaba al son de la noche las ducas de su mañana.\n\nSuspirabas como loca cuando notaste en mis besos que tú estrenabas mi boca.\n\nTeniendo en mi patio un pozo estoy rabiando de sed porque el cubito está roto.",
    "title": "Las letras flamencas de Emilio Bustos",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 108,
    "article_char_count_full": 576,
    "article_char_count_review": 576,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-09-22-left-noticiario-flamenco",
    "article_text_for_review": "Esta Cátedra de Flamencología de la Universidad de Cádiz ha concedido sus Premios Nacionales 1984-86, en la siguiente forma:\n\nCante: Al cantaor Naranjito de Triana, de Sevilla.\n\nBaile: A la bailaora Angelita Vargas, de Sevilla.\n\nGuitarra: Al guitarrista Mario Escudero, de Alicante.\n\nMaestria: Al cantaor Enrique Orozco, de Olvera (Cádiz), y al tocaor Eduardo el de la Malena, de Sevilla.\n\nDivulgación: Al espectáculo «La Fragua del Tío Juane».\n\nDisco: Desierto.\n\nMención a «El Pele. La fuente de lo jondo» (Pasarela).\n\nEntidades: A la Peña Flamenca «Chaquetón», de Madrid.\n\nInvestigación: Indistintamente, a las obras: «Vida y cante de don Antonio Chacón», de José Blas Vega, y a «La Guitarra», de Manuel Cano (Córdoba, 1986).\n\nEnseñanza: Al Aula de Guitarra Flamenca del Conservatorio Elemental de Música de Jerez de la Frontera.\n\nMedios de comunicación: Al conjunto de la programación de flamenco de Radio Cadena Española en Andalucía.\n\nArtes plásticas: A la obra de temática flamenca del pintor Antonio Povedano, de Córdoba.\n\nPoesía flamenca: A la antología «Para las seis cuerdas», de Javier Salvago. Sevilla, 1984.\n\nPremios locales. Copa Jerez (1984-86)\n\nCante: A Manuel Moneo. Baile: A Manuela Carpio. Guitarra: A José María Molero\n\nNuestro entrañable amigo y colaborador en CANDIL, Antonio Povedano Bermúdez, ha sido distinguido con el Premio Nacional al Flamenco en las Artes Plásticas. El galardón le ha sido otorgado a este artista jiennense, afincado en Córdoba, «en mérito al conjunto de su obra pictórica sobre temática flamenca desarrollada a lo largo de tantos años y a la calidad y brillantez creadora de la misma».\n\nNuestra más cordial enhora- buena en nombre de todos los que hacemos CANDIL y de la Peña Flamenca de Jaén.\n\nPara fallar este PREMIO se reunieron en Sevilla el pasado 29 de agosto los miembros del jurado designados al efecto, a saber:\n\nAlberto García Ulecia, José M. Suárez Japón, Lucas López López, Antonio Murciano Alvarez, José M.ª Requena Barreras, Se presentaron al concurso os trabajos cuyo detalle sigue:\n\nDe Carlos Arbelos y M. $ ^{a} $ Rosa Fistbein: «Antonio Mai-rena, el gitano del siglo».\n\nDe Aquilino Duque: «La Llave de Bronce de Antonio Mairena».\n\nDe Fernando Durán Bonilla: «El Manantial de Mairena (un sueño de Fernando)».\n\nDe Aurelio Gurrea Chalé: «Mairena, un cantaor providencial».\n\nDe Joaquín Herrera Carranza: «Síntesis sobre Antonio Mairena».\n\nDe Manuel Martín Martín: «Reflexiones ante la revolución mairenista».\n\nDe Ángel Marín Rújula: «Visión histórica».\n\nDe Luis Melgar Reïna: «Ra- zones de la ciudad de Córdoba».\n\nDe José Montero Alonso: «Recuerdo de Antonio Mairena».\n\nDe Fernando Quiñones Chozas: «Memorándum Mai- rena».\n\nEl premio de doscientas cincuenta mil pesetas y diploma instituido por esta fundación fue adjudicado a don MANUEL MARTIN MARTIN por su artículo que, como dicho, lleva por título «Reflexiones ante la revolución maire-nista», que fue publicado en la revista CANDIL, número 49, enero/febrero del presente año.",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1987-09",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 465,
    "article_char_count_full": 2991,
    "article_char_count_review": 2991,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
