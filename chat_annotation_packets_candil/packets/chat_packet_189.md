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
    "article_id": "1989-03-23-left-hablan-las-pe-as",
    "article_text_for_review": "HABLAN LAS PEÑAS\n\nI CONCURSO DE CANTE FLAMENCO PEÑA \"LA TRILLA\" DE SALOBREÑA\n\nCon el patrocinio del Ayuntamiento de Salobreña (Granada) y organizado por la Peña Flamenca la \"Trilla\", ha sido convocado el I Concurso de Cante Flamenco en el que podrán participar cuantos cantaores aficionados lo deseen, sin límite de edad y sexo.\n\nLos interesados podrán inscribirse dirigiendo a la Casa de la Cultura de Salobreña, Avda. del Mediterráneo, s/n, o llamando al teléfono 61 06 30.\n\nLa fase selectiva se realizará entre el 18 de Marzo y el 24 de Junio y la final tendrá lugar el día 19 de Agosto.\n\nPara ésta edición se han establecido tres grupos de cantes, así como tres premios, siendo el primero de 150.000 pesetas.\n\nALMUERZO DE HERMANDAD DE LAS PEÑAS MALAGUEÑAS\n\nEl pasado 14 de Enero tuvo lugar un almuerzo de Confraternidad entre las distintas Peñas Flamencas que componen la geografía Malagueña (Federadas y no Federadas), organizado por la Federación de Peñas Flamencas Unión del Cante (Fuengirola-Mijas) en la Caseta que tiene esta Peña, en el Recinto Ferial de Fuengirola.\n\nA dicho acto asistieron 130 personas, entre las que habría de destacar, aurtoridades políticas de Málaga, Confederación Andaluzas de Peñas Flamencas presidida por D. José Arrebola Rivera y su junta directiva, medios de radio, televisión y prensa, críticos del Flamenco (como Gonzalo Rojo y Salvador de la Peña) y algunas actuaciones flamencas, como las de Jesús Vega (12 años, guitarrista de Ronda), Antonio \"El Chaqueta\" de Fuente Piedra y Antonio de Canillas, como cantaores y acompañados por el tocaor Gabriel de la Peña Fosforito de Málaga.\n\nEn dicho almuerzo se habló del sentido de la Federación de Málaga, su pasado, y futuro y de la necesidad de Federarse.\n\nEl acto empezó a las 13'30 h. y terminó a las 19'00 h., transcurriendo en un abiente de amistad y camaradería flamenca.\n\nDIRECTIVA QUE SIGUE\n\nHabiéndose cumplido a principios de este año el mandato anual de la Directiva en la PENA FOSFORITO malagueña y siendo obligada su renovación para otro ejercicio, el 10 de marzo se celebró Asamblea General de votación, y, al no haberse presentado candidatura alguna, resultó elegida por unanimidad la que venía desempeñando los cargos en las personas siguientes:\n\nPresidente: Pablo Franco Cejas. Vice-presidente: Cristóbal Trujillo. Secretario: José Trigueros. Tesorero: Francisco Jiménez. Cajero: Salvador Trujillo. Portavoz: Francisco Franco. Vocales: Pedro Ruiz, Antonio Merino, Antonio Trujillo y Gabriel Cabrera. Consejeros de Cante: Gonzalo Rojo Guerrero, José Claros Mancilla, A. Cano Martín y José Márquez Cabello.\n\nSe da la circunstancia de que el presidente de esta Peña también lo es de la Federación de Peñas Flamencas de Málaga, a las que desde aquí hace un llamamiento para que se federen.\n\nNUEVA JUNTA DIRECTIVA EN LA PEÑA FLAMENCA DE SAN PEDRO DE ALCANTARA (MALAGA)\n\nEn reciente Asamblea General de Socios, celebrada por la Peña Flamenca de San Pedro de Alcántara (Málaga), resultado elegida nueva Junta Directiva, quedando la misma compuesta de la forma siguiente: Presidente: Benito Ductor. Vicepresidente: Vicente Benegas Flores. Secretario: Rufino Ruiz Ramírez. Tesorero: José Navarro Santiago. Relaciones Públicas: Francisco Valero Vargas. Vocales: Joaquín Rodríguez Ortega, José Lara Añón, Juan J. Morales Centella, Luis Casado Gallego y Miguel Guerrero Mena.\n\nNuestra enhorabuena a los amigos de San Pedro de Alcántara y muchos éxitos.\n\nNUEVA JUNTA DIRECTIVA DE LA TERTULIA FLAMENCA \"JOSE DE LA TOMASA\" DE CANTILLANA\n\nEn Asamblea General Ordinaria celebrada por la Tertulia Flamenca \"José de la Tomasa\" de Cantillana, el día 1 de Febrero pasado, resultó elegida Junta Directiva, la cual quedo compuesta por los siguientes socios: Presidente: José A. Fernández Cabrero. Vicepresidente: Antonio Durán Rodríguez. Secretario José Fernández Núñez. Tesorero: Eduardo Jiménez Romero. Vocales: Manuel Tirado Soares y José Jiménez Sánchez.\n\nDeseamos toda clase de aciertos al nuevo ejecutivo.\n\nLA PEÑA FLAMENCA \"LOS BORDONES\" DE CORDOBA ELIGIO NUEVA JUNTA DIRECTIVA\n\nEn Asamblea General de Socios celebrada a primeros de Febrero, la Peña Flamenca \"Los Bordones\" de Córdoba, eligió nuevo Presidente, y éste a su vez nueva Junta, la cual quedó compuesta de los siguientes nombres: Presidente: Antonio Abad Belló. Vicepresidente: Gregorio Canto Leal. Secretario: Francisco Cámara Gamero. Tesorero: Francisco Lacalle Aliaga. Relaciones Públicas: José Baena Sánchez. Vocales: Juan Martínez Serrano, Andrés Palacios Guerrero y José L. Gómez Arroyo.\n\nDeseamos una feliz andadura flamenca a los amigos de Córdoba.",
    "title": "Hablan las peñas",
    "periodical": "candil",
    "issue_id": "1989-03",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 710,
    "article_char_count_full": 4604,
    "article_char_count_review": 4604,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-03-24-right-discografia-flamenca",
    "article_text_for_review": "Canta: Curro Malena Toca: Pedro Bacan Título: \"Manantial Gitano\" Edita: Pasarela. Sevilla 1988\n\nE scribía allá por 1972 Manuel Ríos Ruiz sobre Curro Malena: \"Este joven cantaor es una de las más recientes revelaciones del cante. En su primera actuación pública obtuvo el premio Antonio Mairena en el festival de Mairena del Alcor, dejando su trabajo del campo para dedicarse al cante. Después de grabar su primer disco ha participado en los festivales andaluces y cantado en los tablaos de Sevilla. tiene gran voz, llena de modulaciones y con un rajo flamenquísimo, destacando en los cantes festeros, especialmente en las bulerías, y también en soleares y siguiriyas. Nació en Lebrija hace veintitantos años. De raza gitana\".\n\nQué duda cabe que este retrato de Curro Malena sigue aún hoy en día con los rasgos bastante definidos y muy concretamente en lo que se refiere a la fueza, modulación de voz e inclinación hacia los estilos festeros. También se aprecian con nitidez las líneas que definen su quehacer en las siguiriyas, pero donde más resaltamiento existe -por los dobleces que da la experiencia y la madurez artística- es en el cante por soleá. En este cante, Curro Malena evoca la figura de \"Charamusco\" que no es otra cosa que la rememoración del maestro Antonio Mairena. Y, sin embargo, el cantaor de Lebrija a lo largo del desarrollo de los diversos tercios, va dejando patente su propia personalidad adobándola de los matices que arriba indicaba Ríos Ruiz.\n\nOtro tanto acontece en la primera grabación de los cantes \"por bulerías\". Aquí Francisco Carrasco \"Curro Malena\", mantiene un buen compás con aires evocadores de su tierra y desarrolladas con entrega, oficio y facultades. Opino que en estas bulerías el cantaor mantiene con más incidencia su propia personalidad. Sin abandonar este cante -ya que existen en el disco dos grabaciones más del mismo-, nos encontramos en la siguiente interpretación con una ejecución del estilo en una línea más clásica y uniforme y manteniendo el buen compás. En las siguientes -encuadradas en el\n\nRafael Valera Espinosa\n\nlocalismo gaditano-, Curro Malena sigue manteniendo las características antes referidas a las cuales hay que sumarles fuerza y cierto recuerdo hacia Manolo Vargas.\n\nManteniendo su inclinación hacia los estilos festeros, el disco contiene unos tangos plenos de matización, en una línea clásica y con ciertos asomos a su tierra cantaora. En algunas fases de su interpretación existe cierta ida hacia sus facultades. Aunque en todas las grabaciones se mantiene un acompañamiento de la guitarra con dotes virtuosas a la vez que plenas de sencillez, en esta grabación, Pedro Bacán denota ser maestro en su oficio con un desarrollo del compás pleno de matiz y gustó.\n\nEn cuanto a la petenera, está desarrollada con sabor y matización. Este es un cante que el cantaor adapta muy bien a su personalidad y siempre sale muy airo-so del mismo. Antes del estilo citado hay grabado un garrotín con el cual el intérprete quiere evidenciar su largueza en el conocimiento de los cantes. Mantiene una buena ejecución aunque cierto abuso del estribillo.\n\nPara finalizar, significar el acierto en la elección de las letras de estos cantes y señalar igualmente un cierto abuso en la selección de las bulerías. Curro Malena es un cantaor que conoce bien los estilos y su forma de interpretarlos hubiera tenido un reflejo más amplio al no existir repetición en los mismos.\n\nFERNANDO MAIRENA\n\nSi desea recibir algún ejemplar de los indicados, diríjase a Pasarela, S. L., según las siguientes condiciones:\n\n— Para la Península: Contra reembolso o giro Postal, sin gasto de envío.\n\n— Para Baleares, Canarias, Ceuta y Melilla: Contra reembolso o giro postal más 15 % de gastos de envío.\n\n— Para Europa: Giro postal más 20% de gastos de envío.\n\nEl pedido mínimo debe ser superior a 1.000 pesetas, debiendo indicar las unidades y a continuación el número de referencia impreso, bien del disco o cassettes.\n\nJESUS DEL GRAN PODER, 7 - 2º J Teléfono (954) 37 58 98 - 41002 -SEVILLA\n\nManuela García Ortega\n\nPedidos: Teléfono (953) 22 94 50 Sta. Carmen o en las más importantes Librerías de Jaén\n\nJosé Antonio Díaz Fernández, \"El Chaquetón\", nació en Algeciras, Cádiz, en 1946. Reside en Madrid. Es hijo de \"El Flecha de Cádiz\", hermano de \"El Flecha\" y miembro de una dilatada e ilustre familia de \"Chaquetas\": Tomás, Antonio y Adela, de quienes es sobrino, así como de \"El Chaleco\" y de Salvador Pantalón. De él dice Angel Alvarez Caballero:...Esa voz que puede pasar de la caricia al grito casi sin transición, esos cortes bruscos que no hacen daño, porque en el arte de Chaquetón yo diría que hasta los silencios son cante...\n\nVOCES DE HOY\n\n«Chaquetón»\n\nCaja de Ahorros y Monte de Piedad de Granada",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1989-03",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "24-27",
    "page_number": 24,
    "word_count": 789,
    "article_char_count_full": 4743,
    "article_char_count_review": 4743,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-3-right-festivales-flamencos",
    "article_text_for_review": "Los problemas concernientes al Festival Flamenco se han debatido, hasta con reiteración, en congresos, mesas redondas, publicaciones y foros diversos, subrayando casi siempre, la necesidad de organizar ése espectáculo de masas con arreglo a determinados criterios de racionalidad: evitar festivales de seis u ocho horas de duración, repetición machacona de estilos, incumplimientos de los artistas, nómina incontrolada de éstos, etc. Sería inocuo reiterar lo ya dicho; pero sí estimamos oportuno plantear algunos aspectos de la cuestión por lo general, no contemplados. ¿Cuál es, en el tiempo, la razón de ser de los Festivales? La divulgación del Flamenco. Y nadie duda de la idoneidad del instrumento. Si por divulgación se entiende dar a conocer una manifestación que raras veces tiene que ver con las esencias jondas.\n\nCualquiera que esté iniciado en este Arte sabe, sin embargo, que el Festival Flamenco difiere sustancialmente de la reunión de los cabales en las que el duende en forma de mágico tarob se posesiona, sí llega a hacerse presente, de quienes participan. Un Festival Flamenco tiene siempre algo de spot publicitario; sólo que inidóneo porque no subraya lo más apreciable del producto artístico y ello aunque, circunstancialmente, se asome al espectáculo alguna jonda mordedura.\n\nAdemás de lo dicho para la divulgación, el Festival ha tenido, también, para sus promotores algo de ceremonia del desagravio, particularmente para las instituciones que, con loable sensibilidad, reivindicaron la grandeza de este hecho cultural propio frente a detractores desinformados. Se pretendía así pasar del entorno prostibulario o simplemente frívolo al marco respetable de plazas públicas y foros de teatro. Todo ello bajo el impulso incentivador que la nueva estructura del Estado, como Estado de las Autonomías genera, en orden a una recuperación de señas de identidad cultural perdidas. Claro, que los desagravios tienen siempre vida efímera y se constituyen por actos singulares. Nadie, por otro lado, desconoce y además deja de aceptar que los Festivales Flamencos han proporcionado digno peculio a una clase artística depauperada.\n\nEncomiable empeño el de muchos organizadores que lucharon y luchan por salvar de la indigencia a voces venerables hundidas en la miseria. Pero este aspecto de la cuestión, con ser muy digno de tener en cuenta por otros conceptos, no entra en los parámetros de una reflexión sobre la virtualidad artística de los Festivales. Esa es otra historia.\n\nEn cualquiera de los casos, es lo cierto que para la presente temporada el número de Festivales Flamencos organizados ha decrecido hasta menos que una quinta parte de los que hace cinco años se organizaban. Lo que significa un declive, a nuestro juicio irreversible, de esta forma de comunicación del Flamenco; crisis debida a causas complejas cuya atribución sería precipitado en este momento realizar, pero a la que no son ajenos los propios artistas, promotores, instituciones, etc. Creemos que la época del Festival Flamenco fenece, como en su día decayó el Café Cantante y la Opera Flamenca. Tal vez un nuevo hito esté determinando otra época en la historia del Flamenco. Sería precipitado el pronunciarse sobre ella. El futuro lo dirá.\n\nRamón Porras",
    "title": "Festivales Flamencos Editorial",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 506,
    "article_char_count_full": 3246,
    "article_char_count_review": 3246,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-4-left-desde-el-valle-flamenco-lecci-n-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nU na tarde de otoño calentita y luminosa. Los estudiantes se afanan en comentar un poemita que les ha propues-\n\nJosé M. $ ^{a} $ Pérez Orozco\n\nPara mi hija Isabel\n\nto el profesor de Literatura. El poema di- ce como sigue:\n\nSubí a una alta montaña buscando leña pa'l fuego y como no la encontraba, al valle abajé de nuevo...\n\nEl profesor éste se pirra por el Flamenco y, siempre que puede, coloca un trabajo así en la clase. Y a los estudiantes les interesan, no se imaginan cómo les interesan los distintos productos de la cultura andaluzia (El profesor «se aprovecha» de ello y estimula su interés). Ya los tiene acostumbrados y ellos escuchan con atención electrizada los datos que va dejando caer, casi como si fueran las pistas de un caso detectivesco: resulta que esa copla es la primera de una\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"recuerdos\"]\n\nun trabajo así en la clase. Y a los estudiantes les interesan, no se imaginan cómo les interesan los distintos productos de la cultura andaluzia (El profesor «se aprovecha» de ello y estimula su interés). Ya los tiene acostumbrados y ellos escuchan con atención electrizada los datos que va dejando caer, casi como si fueran las pistas de un caso detectivesco: resulta que esa copla es la primera de una serie por solea, incluida en El calor de mis recuerdos, el último disco que grabara el maestro Mairena. Y les dice unas cosillas sobre ese hombre y su trayectoria: sobre la Llave de Oro, sobre su «analfabetismo académico» y su finísima sensibilidad y su portentosa cultura; les habla de su garganta «con cuerdas vocales de caballo», de su sacerdocio dedicado a la dignificación del Flamenco, de la Medalla de Oro a las Bellas Artes, que, por primera y única vez, recayó en un artista flamenco... y de otras facetas sorprendentes de la figura y\n\n[ENDING CONTEXT]\n\ndices es lo que Mairena quería expresar aquí?\n\nLa clase entera están pendiente de la respuesta, que ahora es inmediata, directa y segura.\n\n—Es que Mairena hizo esta poesía para mí.\n\n—Pero, ¿tú conociste a Antonio Mairena? —Yo a él, no. Pero él a todos nosotros, sí: era un artista de verdad...\n\nEn la clase está todo el mundo inmóvil y casi se puede oír cómo hierven impresiones y pensamientos en las juveniles cabezas.\n\nNi un comentario. Creo que todos he- mos comprendido: Antonio Mairena y una estudiante de quince años nos han da- do una lección magistral difícil de olvidar. Que nos aproveche.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "DESDE EL VALLE FLAMENCO (Lección magistral de una alumna de quince años)",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "4-4",
    "page_number": 4,
    "word_count": 1369,
    "article_char_count_full": 7857,
    "article_char_count_review": 2579,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "recuerdos"
      }
    ]
  },
  {
    "article_id": "1989-05-5-left-viejo-carn-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAnselmo González Climent\n\n1 La reciente antología discográfica de Antonio Mairena no satisface plenamente el gusto y el juicio de Aurelio. «Las siguiriyas —dice— me resultan nada más que interesantes, en particular la que recuerda el “cambio” tradicional de Silverio. La solea de Enrique el Mellizo está bien encarada, pero está lejos de la marca de Enrique».\n\nPresta más atención al corrido que Mairena recoge de la tradición oral y del seno de su propia familia. Más por halago circunstancial que por asentimiento estilístico, lo aprueba como auténtico. Lo cierto es que me canta por lo bajini el corrido que él tomara de Enrique el Mellizo, no tan ajustado al compás de soleá que le imprime Mairena. Recuerda que en Sevilla alcanzó a escuchar el corrido sobre la base del martinente. Deduce que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradicional\"]\n\nPresta más atención al corrido que Mairena recoge de la tradición oral y del seno de su propia familia. Más por halago circunstancial que por asentimiento estilístico, lo aprueba como auténtico. Lo cierto es que me canta por lo bajini el corrido que él tomara de Enrique el Mellizo, no tan ajustado al compás de soleá que le imprime Mairena. Recuerda que en Sevilla alcanzó a escuchar el corrido sobre la base del martinente. Deduce que esta forma tradicional del romance elige en cada zona una apoyatura flamenca distinta. Es mera decisión del cantaor. Después de estos rodeos, su protogaditanismo le induce a canonizar la fórmula de Enrique, de quien rara vez se aparta para verificar la pureza de sus colegas. 2 Según testimonio de Ricardo Molina, Antonio Mairena suele afirmar que no sabe cantar polos, cañas y, con más razón, fandangos. La ironía de esta proclama tiende a frenar la intoxicación arcaísta de los jóvenes concursantes. Aurelio coincide con esta salida de tono, pero limita su alcance el acaso del fandango. «Jamás he escuchado en mi vida un polo o una caña completos. Y lo que recuerdo de esos cantes nada tiene que ver con los que ahora están en circulación. Conozco y canto algunos fragmentos, sobre todo el remate de la soleá apolá. Mi conocimiento llega hasta lo que recibí de Enrique el Mellizo y Paquirri el Guanté. Com más razón te digo que Torre, Chacón y el Niño de Cabra sabían de la misa la media. El Niño de C\n\n[ENDING CONTEXT]\n\nuna selección previa durante los dos años y pico que hay entre concurso y concurso. Ricardo y Salinas podrían ser los jurados y así traernos al concurso unos cuantos cantaores seguros y aguantables».\n\nYo creo que Aurelio piensa mucho en su comodidad personal. Hay cantaores que ni ellos mismos saben en qué cante pueden realmente triunfar. Hay que considerar el cambio de voz, por joven o por viejo. No hay que concentrar sospechas sobre solo dos jurados. Y de los dos, Salinas cedería su voto para calcar el de Ricardo. No hay vueltas. Se requiere la plenitud de los jueces. Caben más reparos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Viejo carné flamenco",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "5-10",
    "page_number": 5,
    "word_count": 7854,
    "article_char_count_full": 45972,
    "article_char_count_review": 3068,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradicional"
      }
    ]
  }
]
```
