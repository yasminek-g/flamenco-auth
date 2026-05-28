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
    "article_id": "1990-09-25-right-que-nadie-se-fie-de-su-memoria",
    "article_text_for_review": "P or segunda vez me ha fallado la memoria. La primera fue cuando sin previa consulta con la ficha artístico-biográfica del cantaor dije que Bernardo el de los Lobitos se llamó José Alvarez Pérez, siendo realmente su nombre el de José Pérez Alvarez. En aquella ocasión fue el señor Martín Martin, quien me advirtió del error. Recordarán mis lectores que en la revista posterior apareció inserta la certificación del nacimiento de Bernardo donde constaba que la misma obraba en mi archivo desde el año 1975.\n\nLa segunda vez que he cometido idéntico error, ha sido cuando he dicho, a la ligera y fiándome de la memoria, que El Corruco falleció el año de 1937, cuando en realidad falleció el año de 1938.\n\nVean la certificación del óbito de El Corruco y se darán cuenta de que la misma está en mi archivo desde el año de 1975. Lo verdaderamente importante para mí es poder demostrar a los lectores que cometí el error, no por ignorancia, sino por no haber consultado los documentos.\n\nMe gustaría que lo mismo que yo he asumido mi error en estas dos ocasiones que comentamos, los demás hicieran lo mismo, por ejemplo, con la desafortunada edición del Lp de Benalmádena, donde los malagueños dicen haber grabado para la afición una malagueña de la Rubia de Málaga, cuando lo que en él figura es una malagueña de la señorita Encarnación Santisteban, la Rubia de Valencia. Lo denuncié oportunamente a través de este medio de comunicación entre los aficionados, pero nadie, hasta ahora, ha entonado el «mea culpa, señor».\n\nLo siento, amigo Gonzalo Rojo, esta vez no. En otra ocasión será, aunque lo veo difícil porque antes de lanzarme a escribir no me fiaré de mi memoria y consultaré mis ficheros que para eso los confeccioné.\n\nFICHA ARTISTICA Y BIOGRAFICA\n\nNombre artístico EL CORRUÇO DE ALGECIA Especialidad Cantáron Nombre y apellidos Jose Ruiz Arroyo Raza Castellano Naturaleza La Lineadela Concepción Hijo de Iniquel y de Ysabel Fecha de Nacimiento 21 de Emiro 1910 Estado Casado\n\nDEFUNCION\n\nFalleció en el frente de Feruel: Guerra 1936 y restaurado en Balaguet Causa del óbito heridas de fusil\n\nCERTIFICACION LITERAL DE INSCRIPCION DE DEFUNCIÓN..\n\nREGISTRO CIVIL DE BALAGUER Provincia de LERIDA\n\nEl asiento al margen reservado literalmente dic: así: REGISTRO CIVIL DE Balaguer. Múmero. 45. NOMBRE Y. APELLIDOS. JOSE AUIZ. ARRO-YO. - En Balaguer a las ence y minutos del día diez y siete de mayo de mil novecientos treinta y ocho, ante D. Sebas. Tión Armenter Monsnís. Juez municipal y D. Francisco Valí's Domenó Secretario, se procede a inscribir la defunción de José Ruiz Arroyo, de vaictiocho años, natural de la línea (Abdalucía), hijo de D. Miguel y de Dnía Isabel, domicilia-do en -----de-----número-----, piso-----de profesión soldado y de estado Casado, falleció en Campaña, el día cinco de abril pasado a las vainte y ___minutes, e consecuencia de heridas de fusil según resultado del certificado facultativo y reco-nocimiento practicado, y su cadavar habrá de recibir sepultura en el Comentario de esta Ciudad. Esta inscripción se prac-tica en virtud de declaración que hace José Bosch Aragó como brigada de su Comentario, consignándose adamés que se ignora haya otorgado testamento, habiáncola preanciado como testi-gos D. Fidel Umbón. Franco y D. Mariano Añón. Apostegui, mayo-ces de adad y vecinho 79 dos en Campaña. Leida esta acta, 90 sella con el del Juzgado y la firma el Sr. Juez, los tes-tigos y declarante de que certifico. Firmados: S. Armen-ter. José Bosch. Fidel Umbón. M. Añón. F. Valla. Rubricados. Day un sello en tinta violata que se lee JUZ GADO MUNICIPAL BALAGUER",
    "title": "Que nadie se fíe de su memoria",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 596,
    "article_char_count_full": 3590,
    "article_char_count_review": 3590,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-09-26-left-discografia-flamenca",
    "article_text_for_review": "Manolo de Badajoz (1889-1962) Historia y Arte en el Accompañamiento al Cante\n\nTítulo: MANOLO DE BADAJOZ. Historia y Arte en el acompañamiento al cante. Cantan: El Peluso, El Niño Gloria, José Palanca, La Niña de los Peines, El Carbone-rillo, El Pena, hijo, y El Niño de la Huerta. Toca: MANOLO DE BADAJOZ. Referencia: FONORUZ. D-223. Montilla (Córdoba). 1990.\n\nU na de las entregas a los congresistas del XVIII Congreso de Actividades Flamencas de Badajoz ha sido el disco que ocupa este comentario. Con el mismo se ha querido, y pienso que se ha conseguido, revitalizar la figura de este tocaor pacense que impresionó más de 620 títulos, acompañando por todos los estilos a numerosos cantaores.\n\nPara describir el toque que se aprecia en sus grabaciones, por acertado, exento de apasionamiento, sincero y ajustado, qué mejor que reflejar lo que su paisano Joaquín Rojas Gallardo escribe sobre Manolo de Badajoz en la contraportada del disco y que dice así: «Dotado de una especial sensibilidad a la hora de acompañar, concebía el cante como el verdadero protagonista de la interpretación y de esa manera de pensar, salía un dechado de virtudes de lo que debe ser un buen propósito de no molestar en ningún momento al cantar y de ayudarle en todo lo posible, no sólo con el instrumento de las seis cuerdas, sino también con su especial manera de jalar».\n\nPoco más se puede añadir sobre el toque de Manolo de Badajoz a lo arriba expuesto, porque toda la serie de virtudes que Joaquín describe de su paisano, son las necesarias e imprescindibles para desarrollar un acompañamiento como mandan los cánones. Además el tocaor extremeño solía iniciar su tarea con unas elaboradas falsetas plenas de matiz y conocimiento del compás, realizadas éstas dependiendo de la personalidad del cantaor que acompañaba, muestra de su amplio conocimiento de los artistas y de los cantes. Sus falsetas a tiempo elaboraban el enlace de los tercios del cante, completamente indispensable para el resultado armónico del estilo. Y ese no molestar se aprecia con auténtica claridad en un afán de sumisión al arte cantaor.\n\nEs una auténtica pena que los tres solos que se han plasmado en el disco no tengan la suficiente nitidez para apreciar en su totalidad toda la serie de variaciones que su creatividad aportaba, variaciones por otra parte que él introducía en sus acompañamientos.\n\nCantan: La Kaita, Francisco Dávila, Domingo Rodríguez, José Guerrero, Alejandro Vega y Prim Barquero.\n\nTocan: José Luis Postigo, David Silva y J. Antonio Conde.\n\nReferencia: PASARELA. PRD - 172. 1987. Sevilla. Patrocina: Caja de Badajoz.\n\nT ras la celebración del I Concurso Nacional de Cante Exte-meño celebrado en 1987, se ha querido plasmar con este disco la vigencia y la renovación del cante extremeno, en un intento de mostrar las cualidades y la creatividad flamenca de esta región hermana. Aunque las grabaciones de este disco evocan las personalidades de cantaores que en otros tiempos han patentizado estos logros, ciertamente que las nuevas voces del flamenco extremeno abonan aún más la gran afición que existe en estos lares con su aportación personal.\n\nSorprende por un lado el compás y las características de una voz —algo acama-róná— de una joven intérprete como es La Kaita para cantar los jaleos y los tangos extremenos, aunque con algún matiz de estentoreidad. Por su parte, Francisco Dávila hace un recorrido, con melodía, por los tres fandangos de Manolo de Fregenal, así como por los de Pérez de Guzmán con los matices que han difundido Paco Toronjo y Paco Isidro. Resalta aún más su melodía y su matiz cantaor al rememorar a Pepe el Molinero por tarantas. En cuanto a Domínguez Rodríguez patentiza la consecución del primer premio por tangos extremenos; José Guerrero matiza su recuerdo de Porrina de Badajoz; Alejandro Vega muestra sus condiciones de compás para los jaleos y Prim Barquero demuestra su asimilación a cantes foráneos de su tierra como los tientos-tangos.",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "25-26",
    "page_number": 25,
    "word_count": 651,
    "article_char_count_full": 3955,
    "article_char_count_review": 3955,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-09-26-right-premium-iben-alcazar-premium-cri",
    "article_text_for_review": "Si señor. Si ha pedido una cerveza Alcázar: ¡bien hecho! Porque va a saborear una cerveza fresca, con cuerpo, en su punto. Una cerveza elaborada con las mejores cosechas de lúpulo y cebada, siguiendo la tradición de nuestros maestros cerveceros. Una cerveza que mantiene todo su aroma, porque va,\n\ncomo quien dice, de la fábrica directamente al consumidor. Si pide cerveza Alcázar, ¡bien hecho!. Disfrutará de una cerveza bien hecha.",
    "title": "Premium IBEN Alcazar Premium CRIVEZA ELECTRICA HECHO!",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
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
    "article_id": "1990-09-27-left-discografia-flamenca",
    "article_text_for_review": "Tena, Lucero. Nombre artístico de María de la Luz Tena Alvarez. Méjico, 1939. Bailaora flamenca, bailarina de español y virtuosa de la crotalogía. Conoce a Carmen Amaya, su maestra, en 1954, donde se incorpora a su elenco, actuando con ella en diversos estados mejicanos y USA. En 1958 se traslada a España, debutando en el Tablao El Corral de la Morería. El 1964 significó un año de suma importancia en su trayectoria artística, al presentarse en el Teatro de la Zarzuela con un concierto de castañuelas, danzas clásicas y flamenco, acompañándole en esta última parte el cantaor Gabriel Moreno y el guitarrista Aurelio Garci; al año siguiente actúa además en los Festivales de España. En 1967 ofrece veinticinco conciertos en Rusia, con una excelente acogida de público y crítica, siguiendo de igual forma cosechando en años siguientes distinciones y premios. El crítico musical Antonio Fernández Cid, dice de ella: «Lucero Tena mezcla el nervio y la precisión, la gracia y el desplante y el ordenado cálculo que surge del estudio, el análisis y la más severa de las autocríticas. Para los bailes a la guitarra son las castañuelas termómetros de una vibración y soporte de un ritmo. Pero hasta aquí no había surgido la excepción. Lo sorprendente de Lucero Tena, lo pasmoso, lo distinto de cuanto conocíamos, surge por disposición genial de concertista de palillos. No hay que rectificar la expresión. Lucero Tena es una virtuosa, equiparable a las grandes figuras de cualquier campo». Otras opiniones sobre su singularidad artística: Angel de Campo: «Con las castañuelas sorprende y encanta, con ese canto de los palillos que, lejos de ser monótono, adquiere en sus dedos una voz cambiante». Enrique Franco: «Su personalidad es absolutamente singular, pues a su genio personal en el baile popular y en la danza española une especial dote de una verdadera concertista de castañuelas». José Télez Moreno: «Nos cautivó la bailaora, nos deleitó y asombró la concertista de castañuelas».\n\nLucero Tena",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "27-27",
    "page_number": 27,
    "word_count": 324,
    "article_char_count_full": 1996,
    "article_char_count_review": 1996,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-11-5-left-fernanda-y-bernarda-o-la-reclama",
    "article_text_for_review": "U n eximio poeta y es-tudioso de lo jondo, Ricardo Molina, di-\n\njo, hace varios lustros, que «acaso sólo la poesía puede expresar algo del cante de Fernanda». Y es cierto. La poesía como estremecida fábula que desoculta el ente, que nos revela lo mágico y lo ignoto. El ente, en este supuesto, lo constituyen verdades eternas, inmutables, como ideas platonianas cuyas sombras, en la caverna de los amorosos conocimientos, devienen en sangre, en encendida pasión, en dolor humano. Por ello Fernanda reconduce su voz, espontánea y visceralmente, a registros inauditos, a metafísicas claves, pugna por salir de esa sima en la que el arte la recluye, hasta límites crueles; y sale por soleá, roto el azogue de sus ojos, enfebrecida la sién, convulsionado el pecho y el alma herida. Nadie que yo haya oído, formula, en cada cante, una reclamación más honda de los duendes; nadie con más exactitud me ha ilustrado sobre realidades elementales del hombre, reductos tiernos o terribles: como los celos, como el amor filial, como la muerte, como la lealtad, como el tiempo... Nada en Fernanda viene edulcorado, porque su cante nos llega rescatado de las sombras. de recónditos fondos que ella desoculta. No es del todo cierto que, en Fernanda, el conocimiento preceda al amor porque sin éste, ¿cómo es posible tan penoso viaje a la constatación singularísima de realidades esenciales del hombre? Diría que esta reflexión, casi agustinia-\n\nye la médula de toda la interpretación de lo jondo, en cuyo centro, cabe situar, por méritos propios, a Fernanda de Utrera.\n\nEl reconocimiento de Bernarda viene dado por su personalidad en la ejecución de los cantes festeros. La genialidad de su hermana Fernanda no debe incidir en la frecuente minusvaloración del tamaño artístico de Bernarda. Cantaora de enorme temperatura jonda, la que pueden admirarse insólitos destellos. Me refiero a la generalizada opinión de que las bulerías u otros cantes festeros vehiculari siempre estados anímicos intrascendentes. Y al menos en Bernarda, no es así, porque, si bien su cante formalmente es festero, adquiere sin embargo, por virtud de gitanísimos pellizcos y de un oscuro metal de voz, gran densidad jonda, mordeduras al fin, que más nos adentran en el universo de la siguiriya que en el de la fiesta.\n\nFernanda y Bernarda, nietas del Pinini, orgullo de Utrera, representan ya un destacado lugar en la historiografía del cante. Y si de Bernarda puede decirse que es una magnífica intérprete de los cantes festeros, con la matización antes realizada, y excelente continuadora de una singular tradición cantaora, de Fernanda es justo decir, sin pudor alguno, que nadie mejor que ella ha hecho el cante por soleá, o lo que es lo mismo, nadie ha alcanzado cotas más altas de expresividad flamenca y de jondura.\n\nLa Fundación Andaluzía de Flamenco quiere felicitar públicamente a la revista Candil por el homenaje previsto a Fernanda Jiménez Peña y Bernarda Jiménez Peña, por considerar que en estas dos artistas, Fernanda y Bernarda de Utrera, nietas de El Pinini, se dan la mano lo mejor de la tradición flamenca con unas voces y un compás que han situado su nombre en la cima de las voces femeninas del cante, desde que iniciaran su intensa vida profesional, hace casi cuarenta años.\n\nEs hora de agradecer a Fernanda y a Bernarda tantos años de entrega y esfuerzo, tanta generosidad cantaora, tanto regalo que de ellas hemos recibido todos los aficionados, y tanto amor al Flamenco.\n\nExposición de José Olivares Palacios José Luis Buendía López\n\nCon motivo del homenaje que la Peña Flamenca de Jaén y la revista Candil rinden a las dos geniales cantaoras de Utrera, Fernanda y Bernarda, el pintor José Olivares Palacios ofrece una magnífica exposición con carácter monográfico de su obra, puesto que va a tratar de los artistas flamencos.\n\nLa obra de este artista, tradicional pintor de tierras expresivas, de la situación de sus moradores, de paisajes inquietantes y desnudos, que él viste de lujo con la riqueza de su paleta, se engalana ahora con una serie de óleos, algunos de los cuales han merecido el honor de nuestras portadas de Candil, que, junto a esos temas tradicionales de su pintura, asumen, en síntesis perfecta, la carga humana, el rostro de los protagonistas del flamenco, que se integra de forma espeluznante a la carga patética de sus ocres, de esos verdes de la tierra que viera al pintor nacer y en la que el cantaor dejara lo mejor de sus esencias.\n\nAntonio Mairena, La Piriñaca, Juan Tallega, Tía Juana y un larguísimo etcétera de figuras admiradas y fielmente transcritas a la blancura del óleo, que se llena así de las mejores esencias jondas.",
    "title": "Fernanda y Bernarda o la reclamación de los duendes. Editorial",
    "periodical": "candil",
    "issue_id": "1990-11",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 774,
    "article_char_count_full": 4644,
    "article_char_count_review": 4644,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
