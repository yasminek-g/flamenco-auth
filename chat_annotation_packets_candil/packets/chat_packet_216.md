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
    "article_id": "1990-07-17-left-poes-a-y-letras-flamencas-paco-a",
    "article_text_for_review": "Cantaor\n\nA todos nos han cantado en una noche de juerga coplas que nos han matado... Manuel Machado\n\nHoy has vuelto de nuevo con tu cante al tablao de tu pena sin consuelo, a vender tu amargura y tu desvelo a beberte la vida cada instante.\n\nTragedia en torno a ti, es la constante que se enjuga en quejos sin pañuelo, que se embriaga en jipíos muerte y cielo y se queja en un ¡ay! seco y vibrante.\n\nMírate en tu memoria antepasada, rebusca en tus lamentos de jondura que hallarás una copla que nos hiera.\n\nSerá tu voz más ronca y más sincera al eco de tu grito y desventura y al clamor de tu pena consolada.\n\nPaco Arana\n\nSoleares\n\nLas penas que tengo yo, ni mi mujer ni mi madre, sólo mi sombra las sabe. Si delante, si detrás la sombra por el camino nunca sabes dónde va. Mi sombra y yo somos dos; somos: la noche y el día, cuando yo canto mis penas ella baila de alegría. Si corro nunca la alcanzo. Sólo tendido en la tierra puedo cogerla en mis brazos. De pobre como yo era, mi sombra se marchó un día. No quiso estar a mi vera. Cuando te veo pasar mi sombra dobla la esquina no sea que la veas llorar. Mira si serás bonita que hasta las flores del campo con tu sombra se marchitan. Mi sombra por tu querer lanza al viento las campanas y las vuelve a recoger. Por un candil yo sabía que te acercabas a mí, tu sombra me lo decía. Tu sombra de madrugá en el calor de mis besos los colores se le van. Cuando a tu sombra la encuentro la acaricio con mis brazos y la duermo con mis besos. Como no se quería ir dejé mi sombra a tu lado; no quise hacerla sufrir. A la sombra de un ciprés juntas estarán las sombras y junto nuestro querer. Juan\n\nJuan Torres",
    "title": "Poesía y letras flamencas Paco Arana",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 327,
    "article_char_count_full": 1652,
    "article_char_count_review": 1652,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-07-18-left-camilo-salinas-y-rafael-e-bejara",
    "article_text_for_review": "Pepita Ortega resulta ser sobrina mía in-dependentemente de tan torero, filósofo y flamenquísimo apellido. Nació en Aznalcóllar y artista, lo que apenas puede sorprender siendo andaluzay Sevilla, aunque creciera de cuerpo y arte en Buenos Aires, donde dejó bien sentada su auténtica condición y magnífica calidad de canzonetista andaluzay, actividad profesional en la que sigue a través de las Américas. Se casó con el tocaor argentino Camilo Salinas, aficionado tan puro y cabal como el que menos presuma de serlo y lo sea, que ya es ser.\n\nCamilo Salinas es uno de esos argenti-nos a los que el Cante y la Guitarra le empiezan a deber la más firme razón de continuar siendo un arte singular entre las artes populares que se aprecian alternativamente en el mundo del espectáculo y la cultura. Es uno de los que sin haber pisado aún esta orilla espera la oportunidad como se espera abrazar a la madre con la que nos comunicamos diariamente sin conocerla. El encuentro epistolar fue fulminante. Una larga conversación por encima del océano, un intercambio de ideas, razones, críticas, libros, grabaciones, noticias y amistades siempre surgidas desde dentro de ese mundo apasionado, convulso y minoritario que se llama flamenco sin que sepamos por qué para más misterio.\n\nHispanoamérica, nuestra América, ésa que tan andaluzay dulce nos habla desde otros acentos, también se asoma a las ventanas del flamenco e inclusive contó y cuenta con naturales que buscaron y encontraron las puertas por donde entrar. De entre su larga y ancha geografía destacan dos extremos en este sentido: Argentina y México; pero mientras México parece quedarse en la asimilación del flamenco más alegre e intranscendente, Argentina penetra en el fenómeno de una forma más inteligente, culta y honda.\n\nEspañoleando bajo bandera andaluza, él con su guitarra flamenca y ella con sus flamencas canciones, Camilo y Pepita vie-\n\nLuis Caballero\n\nnen año tras año midiendo de punta a punta aquella otra gran España.\n\nPero un buen día encontraron un pueblo amigo y un buen amigo en el pueblo: Fueron Valencia del Rey, Venezuela, y don Rafael Ernesto Bejarano Fuentes. Otra Valencia verde y oro de naranjos y naranjas, taurina, alegre e iluminada de un sol caribeño, y un valenciano taurófilo de los de antes y aficionado de los punteros al arte de Silverio. El entusiasmo y conocimiento de Salinas hincharon las velas predispuestas de Bejarano hasta decidirse, sin dudas ni demoras, a navegar por los mares del flemenco.\n\nCuenta la historia que Valencia del Rey fue colonizada por sesenta familias de Cádiz, lo que debe ser cierto a juzgar por el espíritu urbano de su gente y una cierta y peculiar guasa graciosa mayormente resuelta entre el bar, el vaso y el natural con el de pecho al toro del aire. Azuzados por esa idiosincrasia tan próxima a lo andaluz, un día cualquiera, la Peña Taurina los Sauces fue objeto de la más entusiasca atención a las predicaciones del amigo-apóstol-profesional de la guitarra y el teórico-discípulo-iluminado por la pasión del cante: Camilo Salinas y Rafael E. Bejarano. Así fue cómo, dentro del capítulo de actividades directamente flamencas, un inolvidable día del mes de abril de 1976 me mandaron un billete de avión a fin de que me pasara con ellos una temporada charlando de cante y cantando, naturalmente siempre perfectamente acompañado a la guitarra por mi sobrino Camilo. Durante aproximadamente un mes las reuniones fueron continuas, incluyendo conferencias-recitales en distintos e importantes centros culturales donde puedo asegurar que el nombre de Andalucía brilló con fuerza en el sentimiento de todos, sin que faltaran los medios de comunicación con el mismo espíritu de afecto e interés.\n\nDe ningún modo, punto de vista o estimación se me ocurriría considerarme artífice del éxito. El éxito bien sabe Dios que fue de Salinas y Bejarano y de aquel grupo de amigos, amables, inolvidables, estupendos amigos que tan valientemente predispuestos y dispuestos me repitieron a los dos años acompañado en esta segunda ocasión por mi mujer y Naranjito de Triana.\n\nPor los salones del hotel París, Club Hípico, Hogar Hispano, Asociación de Ganaderos, restaurantes, villas y residencias particulares sonó, con amor recíproco, la cultura andaluza a través del cante y la guitarra. Sobran pormenores y faltan las cualidades necesarias de quien con la mejor brillantez literaria pudiera decir y agradecer el esfuerzo y la labor conseguida por los inefables Salinas y Bejarano. Magnífico y digno del mayor encomio lo que hicieron posible esos dos cabales de por allá ayudados hasta la última gota de whisky por aquel grupo de amigos que nunca olvidaré, como tampoco la fundación de una peña flamenca con mi nombre y la estrecha vinculación que siguen manteniendo con la plaza de la Real Maestranza de Sevilla durante su feria de abril.\n\nCon el alma en la tinta y el papel cuel- go mi agradecimiento y el del Cante en el tiempo, porque el cante también les de- be la parte que les corresponde.",
    "title": "Camilo Salinas y Rafael E. Bejarano",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 820,
    "article_char_count_full": 4996,
    "article_char_count_review": 4996,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-07-18-right-buz-n-flamenco",
    "article_text_for_review": "Buzón Flamenco\n\nSeõor mío:\n\ninteresante, el nombre del cantaor, el cante, la letra y el guitarrista que acompaña, que bien se merece figurar en la revista y no continuar ignorado cuando sin guitarra el cante queda viudo.\n\nCantaor: MANUEL ORTEGA JUÁREZ, «Manolo Caracol»\n\nMe complazco en comunicarle que, efectivamente, estoy de acuerdo con usted en cuanto a que la relación que figura en el diccionario de los señores José Blas Vega y Manuel Ríos Ruiz, carece de valor por cuanto se omite lo más importante: las letras de los cantes, como usted bien dice. Yo en mi colaboración con la revista «Candil» me he limitado a reseñar lo más Voy a complacerle facilitándole la relación que me pide, pero lo hago a través de «Candil» para que al mismo tiempo sirva a todos los lectores.\n\nManuel Yerga Lancharro",
    "title": "Buzón flamenco",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 138,
    "article_char_count_full": 801,
    "article_char_count_review": 801,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-07-19-right-evocaci-n-de-pastora-pav-n-ni-a-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Yerga Lancharro\n\nInolvidable Pastorica. (Así te llamaba tu maestro, el jerezano José Rodríguez de la Rosa, «Niño de Medina»). ¡Con qué velocidad se nos va la vida! Aún resuenan en mis oídos aquellas frases tuyas tan amables hacia mí:\n\n—Niño, qué buen afisionaito eres!\n\nPastora, ¿recuerdas cuántas veces discutíamos en el sótano de tu bar sobre este o aquel tema o aquel cantaor fallecido? ¿Qué sabrosas discusiones rebozadas de sabiduría flamenca! Todos participábamos en ellas, aportando nuestro granito del bien saber. Todos menos tu niño; tu Tomasito de tu alma que, pegado a ti como un sello de correos en un sobre, más parecía una estatua morena que una persona. Jamás abría su boca para dar una opinión sobre el tema que estábamos debatiendo, y si la abría alguna vez, era para\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\ntu Tomasito de tu alma que, pegado a ti como un sello de correos en un sobre, más parecía una estatua morena que una persona. Jamás abría su boca para dar una opinión sobre el tema que estábamos debatiendo, y si la abría alguna vez, era para quejarse del dolor del día (tenía uno distinto cada día). Tampoco puedo olvidarme de tus polémicas con algunos cantaores consagrados que sin ruborizarse habían acudido a ti como a la fuente inagotable de la mejor agua flamenca, en demanda de tus sabias lecciones. Y después de haberles ofrecido tus saberes acumulados durante más de sesenta años, te enojaban sobremanera porque se atrevían a discrepar de ti. Tu esposo que era dinamita, que era furia incontenible por su fuerte carácter, se llevaba enormes sofocones con toda la razón del mundo. Cómo te pedía, con aquel cariño exquisito que te profesaba, que ya que te habías «jubilado» artísticamente, hicieras una vida plácida, sin preocupaciones y que para ello era necesario que te quedases en casa y no hicieras acto de presencia en el bar, donde, por lo general, raro era el día que las voces encendidas de cantaores y aficionados, no traspasaban los muros del pequeño sótano para ir a estrellarse contra el amplio espacio de «La Campana». Con qué énfasis te decía: —Hazme caso, niña mía, te lo pido por nuestra hija y por la Virgen de la Macarena. No vengas al bar. Y era verdad, Pastora, a veces las reuniones se hacían poco menos que insoportables entre tantos «sabijondos» como había. ¿Recuerdas el jaleo que se armó cuando uno de los presentes sacó a relucir el tema del cante por Tientos? Tú trataste de reducirlo imponiendo tu sabiduría en el tema, pero no te fue posible, porque todos ellos te decían que no existía el Tango gitano, que lo que existía era «los Tientos». Y tú elevando el tono de tu voz, les replicabas diciendo: —Mirad, hijitos, ¿es que, acaso, nació el hijo antes que el padre? —Para mí, vosotros habéis nacido ayer, o sea, cuando\n\n[ENDING CONTEXT]\n\npartiendo del origen de estos cantes, tengo que decir, que no estoy de acuerdo con el señor Molina, porque lo correcto es cantar por Tango lento y terminar por Tango ligero, porque en realidad entre ambos Tangos no deben intervenir para nada los Tientos, que se deben interpretar por separado como siempre lo hizo Juan Ríos.\n\nCreo que todo este embollo que existe entre el Tango gitano y los Tientos, se lo debemos a los que grabaron en Pathé con el título de Tango de los Tientos y Siguiriyas verdes.\n\nAPERITIVOS SELECTOS Especialidad en Plancha\n\nC/. Mesones, 18\n\nTeléfono 26 35 46\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "EVOCACIÓN DE PASTORA PAVÓN, NIÑA DE LOS PEINES (A propósito de la denominación tiento-tango)",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1768,
    "article_char_count_full": 10109,
    "article_char_count_review": 3567,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  },
  {
    "article_id": "1990-07-21-left-bibliograf-a",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCarlos Cruz\n\nRepresentante:\n\nO'Donell, 3.°-A, piso Teléfs. 222058 - 216920 Particular: 228078 41001 SEVILLA\n\nComo disculpa global a autores, y en su caso editores, queremos explicar a unos y a otros las dificultades, a veces insoslayables, a las que nos vemos sometidos en el ejercicio del desarrollo de esta sección de libros, en la que nos encontramos solos para su análisis y con una\n\n«El duende tiene que ser matemático» creciente andanada bibliográfica que, a la vez que nos enorgullece, nos obliga a un esfuerzo continuado y a veces improbo para efectuar la lectura íntegra de los textos y trasladar a los lectores las pertinentes conclusiones. Al mismo tiempo las alteraciones en el ritmo de las entregas de Candil, casi siempre ajenas a nuestras responsabilidades, y en el presente número la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicaciones\"]\n\nos enorgullece, nos obliga a un esfuerzo continuado y a veces improbo para efectuar la lectura íntegra de los textos y trasladar a los lectores las pertinentes conclusiones. Al mismo tiempo las alteraciones en el ritmo de las entregas de Candil, casi siempre ajenas a nuestras responsabilidades, y en el presente número la dilatada presencia de los meses de verano, nos hace pecar de aparente descortesía con los que tan generosamente nos envían sus publicaciones con destino a nuestras críticas. Como ejemplo de nuestra actitud contrita, queremos públicamente solicitar la indulgencia del buen amigo Andrés Raya Saro, ejemplo de cuidadas ediciones que engrandecen nuestro patrimonio flamenco con su espléndida Biblioteca Virgilio Márquez. A él y a todos los demás nuestras disculpas y agradecimiento. Philippe Donnier Virgilio Márquez Editor, Córdoba, 1987 «Las coplas flamencas a la luz de las teorías métricas de los formalistas rusos» Francisco Gutiérrez Carbajo Virgilio Márquez Editor, Córdoba, 1987 La reseña de estos dos breves pero enjundiosos libritos, que merecieron respectivamente los prestigiosos premios de Ensa\n\n[ENDING CONTEXT]\n\népocas pretéritas, junto a otros contemporáneos como Alfonso Canales o Manuel Alcántara, a la vez que presta un gran servicio bibliográfico al presentar a escritores más desconocidos para el gran público como pueden ser Urbano Carrere o Sánchez Rodríguez entre otros, que, sin embargo, han sido importantes en el análisis malaqueño de los eventos flamencos.\n\nCon un estilo fácil y asequible, intercalando letras y citas de sus amigos flamencólogos (gracias, Alfredo, por las atenciones eruditas que en todos tus libros nos dispensas) logra el autor un libro de fácil lectura y agradable contenido.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Bibliografía",
    "periodical": "candil",
    "issue_id": "1990-07",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 1334,
    "article_char_count_full": 8545,
    "article_char_count_review": 2760,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicaciones"
      }
    ]
  }
]
```
