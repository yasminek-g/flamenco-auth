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
    "article_id": "1993-09-22-right-el-xxi-congreso-de-arte-flamenco",
    "article_text_for_review": "E n la Biblioteca de la Ciudad Internacional Universitaria de París se celebró del 22 al 25 de septiembre pasado, la vigésimo primera edición del Congreso de Arte Flamenco, organizado por la Peña Flamenco en France, con la colaboración de organismos nacionales de Francia y España, Junta de Andalucía, Comunidad Murciana y diputaciones andaluzas.\n\nEl miércoles 22, tras la elección de la mesa del Congreso, que quedó formada por Gonzalo Rojo Guerrero como presidente, Pablo Franco Cejas como vicepresidente y Antonio Mata Gómez como secretario, se procedió a dar la bienvenida a los congresistas, y a las siete de la tarde, visita optativa a la Embajada de España en París o a la Peña Flamenco en France.\n\nJueves 23\n\nPonencia de Norberto Torres sobre «Universalidad guitarristica del flamenco». Tras la misma se produjo el correspondiente debate.\n\nComunicación de Edwige Diou- donnat sobre «El flamenco: arte del siglo XX».\n\nComunicación de Claude Orsoni sobre «Flamenco, arte y teatro».\n\nComunicación de Philippe Garnier sobre «La mirada y la voz: el flamenco».\n\nComunicación de José Gelardo Navarro sobre «Cultura árabe, moriscos y arte flamenco».\n\nLa sesión de tarde se inició con la presentación del álbum discográfico «Verdiales. Música de Málaga», producido por Cambayá Records y editado por la Diputación Provincial de Málaga.\n\nComunicación de Philippe Donnier sobre «Magia francesa y derrota de los franceses».\n\nComunicación de Pompeyo Piño sobre «Investigación sobre la filmografía de Carmen Amaya». El comunicante presentó secuencias de varias películas.\n\nPor la noche, en el Teatro El Trianón, velada flamenca con la participación de Paco Toronjo con la guitarra de J. Carlos Romero, Juan Moneo «El Torta» con Moraito Chico y Luis el de la Venta con Niño Josele.\n\nViernes 24\n\nLa sesión de este día comenzó con la ponencia de Miguel Manzano Alonso sobre «Los orígenes musicales del flamenco». Fue muy debatida.\n\nComunicación de Olivier Cappe sobre «Técnicas de restauración de grabaciones históricas del flamenco».\n\nComunicación de Antonio Ca- bezas García sobre «El flamenco en Japón».\n\nComunicación de José Luis Navarro acerca de «Claves de la universalidad del flamenco». Fue leída por la congresista Beatriz Valdés.\n\nMesa redonda con las intervenciones de Marc-Alfred Pellerin sobre «Félix El Loco». Christian Bouvier, sobre «El flamenco en París antes de la guerra». Suzanne de Boye, sobre «Antonia Mercé La Argentina y el flamenco». Y Pierre Lefranc, sobre «París 1954-1960, los años del resurgimiento del cante».\n\nPor la noche, en el teatro El Trianón, gala flamenca con El Polaco con Miguel Ochanco, Jesús Heredia con El Niño Jero, Manuel Gerena con Pepe Núñez y el músico Gualberto y el guitarrista Pacco del Gastor.\n\nSábado 25\n\nEl día estuvo dedicado a la redacción de las conclusiones, ratificación de la próxima sede y abocación provisional de la siguiente, nombramiento del comité ejecutivo, etc.\n\nI. Con respecto a la ponencia de Norberto Torres, se decretó que «parte de las formas flamencas conocidas hoy día se establecen definitivamente entre principios de siglo y los años treinta. Para verificar esta hipótesis se precisa realizar un rastreo exhaustivo de todas las grabaciones sonoras de esa época del flamenco, por lo que se sugiere a las administraciones competentes, concentrar estos materiales en un mismo lugar de estudio e investigación, que puede ser el Centro Andaluz de Flamenco.\n\nIgualmente, que el flamenco constituye el fenómeno musical de tradición oral más complejo que ha dado el continente europeo y cuya importancia debe equipararse con otros fenómenos de alcance universal: el blues y el jazz. Por ese motivo debe, como ya ha sido expresado en acuerdos anteriores, recibir el tratamiento correspondiente a todos los niveles: a) reconocimiento y estudio en todos los conservatorios de música, b) reconocimiento en las universidades formando parte de los programas de etnomusicología, musicología y antropología cultural, y c) contenido obligatorio como materia de cultura andaluzas en los programas de la Consejería de Educación de la Junta de Andalucía.\n\nII. Urgir a la Junta de Andalucía para que por medio de las instituciones que dependan de ella y que se ocupan del patrimonio cultural andaluz, tramite la rápida publicación del fondo de documentos musicales de tradición oral de Andalucía que se encuentra en los archivos del Instituto Español de Musicología. La publicación de este riquísimo fondo contribuirá a un conocimiento más amplio de la tradición musical andaluz y a esclarecer en alguna medida los orígenes musicales del flamenco.\n\nIII. Que en sucesivos congresos los textos originales no se cambien a la hora de su presentación.\n\nIV. Que se solicite a la Consejería de Cultura de la Junta de Andalucía una ayuda económica para realizar un profundo estudio por un equipo multidisciplinario compuesto al menos por un musicólogo, un artista profesional y un estudioso del cante como asesores. Además, solicitar que los organismos oficiales consulten a expertos para la realización de obras mayores de flamenco.\n\nV. Ratificar a la ciudad de Estepona como sede del XXII Congreso de Arte Flamenco, cuya sede provisional fue aprobada en Huelva.\n\nVI. Aprobar como sede provisional del XXIII Congreso de Arte Flamenco a la ciudad de Santa Coloma de Gramanet (Barcelona), a petición del Ayuntamiento y Diputación barceloneses.\n\nVII. Quedó constituido el comité ejecutivo del XXII Congreso, formado por José Arrebola Rivera, por la Federación; Rafael Morales Montes, por las peñas flamencas de fuera de Andalucía; Jaime López Krahe por el congreso que acaba de finalizar; Aurelio Gurrea Chalé, por el próximo congreso, y Francisco Valero Vargas, Francisco Hidalgo Gómez y Pablo Franco Ceja, por la secretaría permanente.",
    "title": "El XXI Congreso de Arte Flamenco, en París",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 904,
    "article_char_count_full": 5774,
    "article_char_count_review": 5774,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-09-23-left-discograf-a-flamenca-rafael",
    "article_text_for_review": "Estamos inmersos, por acertada decisión de la Junta de Andalucía, en el año de la Cultura Tradicional. Las conmemoraciones de los centenarios de la muerte de Antonio Machado y Alvarez «Demófilo» y de los nacimientos de Tomás Pavón Cruz y Rafael Ramos Antúnez «Niño Gloria», así como el haber transcurrido dos décadas desde la muerte de Diego el de El Gastor y una desde la de Antonio Mairena, amén de otros significativos hechos, obligaba —por la importancia artística y cultural de los personajes— a declarar este 93 año cultural andaluz.\n\nAnte este reto, la Junta de Andalucía y su Consejería de Cultura y Medio Ambiente tenían que dar la cara, y lo han hecho con la edición de la colección completa de las grabaciones que realizara el singular Tomás Pavón Cruz, eminente y creativo artista flamenco donde los haya. La inestimable ayuda del coleccionista y no menos aficionado Dr. Reina, en la elaboración del trabajo, evidencia la categoría de los que verdaderamente aman nuestro arte. Su altruista labor queda patentizada una vez más —como también lo hiciera con el Centro Andaluz de Flamenco en la edición del disco de Pastora— como ejemplo a seguir en la difusión y conocimiento de nuestra cultura.\n\nPoco se puede abundar sobre el contenido de este compacto por lo reiterativo que sería analizar una vez más el acrisolamiento que Tomás realizó de los diversos estilos que impresionó, aunque habría que aludir nuevamente a sus facultades cantaoras y a su inigualable creatividad. Es un auténtico deleite es-\n\nReferencia: SE-1552. ALMAVIVA (Música de Andalucía. Sevilla, 1993)\n\ncuchar cada una de las grabaciones que contiene dicho compacto por la reafirmación ortodoxamente flamenca que efectúa de los cantes y en la que se clarifica que el engrandecimiento del flamenco sólo ha necesitado de intérpretes como Tomás para llevar a cabo dicha tarea.\n\nEl cuadernillo orientativo que acompaña a las grabaciones recoge un exhaustivo trabajo de investigación del estudioso astigitano Manuel Martín Martín. Su lectura nos conduce por los adecuados caminos para comprender y analizar los cantes de Tomás en toda su dimensión, las fuentes de su inspiración flamenca, su tratamiento tonal, los detalles y fechas en que se realizaron las aludidas grabaciones y las características y entornos en los que se desarrolló su trayectoria flamenca. No se puede ofrecer más.\n\nPor contra, encontramos algunas grabaciones —a pesar de la técnica empleada— en las que se escuchan determinadas «frituras» por la antigüedad de las mismas. Cierto que son pocas, mas pienso que se podría haber recurrido a las que poseen las casas grabadoras, ya que en reconstrucciones técnicas de dichas editoras, publicadas en diferentes colecciones, el sonido es más nítido y perfecto.\n\nEn definitiva, nos encontramos con un trabajo bien desarrollado, complementado en la labor de orientación e investigación y creo que junto con la edición de la obra sonora completa de Antonio Mairena, precursor de una colección oficial que pueda mostrar la grandiosidad y singularidad de un arte universalmente andaluz.",
    "title": "Discografía flamenca Rafael",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 491,
    "article_char_count_full": 3071,
    "article_char_count_review": 3071,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-09-23-right-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNavidad flamenca en las Peñas de Jerez\n\nConcursos de villancicos y zambombas populares serán la programación durante las fiestas navideñas en la totalidad de las peñas flamencas jerezanas. Todos los sábados y algunos viernes se llevará a cabo la exaltación del villancico tradicional de esta tierra andaluz a través de una larga programación en los distintos locales. En la peña de La Bulería se desarrollará, durante los días 20 y 27 de noviembre, con la actuación de los Coros de la Hermandad de la Yedra y Voces Jerezanas. Para diciembre, los días 3, 4, 10 y 11, actuarán los Coros de Chiquí de Jerez, Coros del Colegio Público «Carmen Benítez», Coro Rociero «Nuestra Señora de Almonte» y la Coral Flamenca «Divina Pastora» de Sanlúcar de Barrameda, destinándose el día 18 del mismo mes para la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nd de la Yedra y Voces Jerezanas. Para diciembre, los días 3, 4, 10 y 11, actuarán los Coros de Chiquí de Jerez, Coros del Colegio Público «Carmen Benítez», Coro Rociero «Nuestra Señora de Almonte» y la Coral Flamenca «Divina Pastora» de Sanlúcar de Barrameda, destinándose el día 18 del mismo mes para la final de esta sexta edición, con distribución de premios importantes que se ponen en litigio en este concurso de zambombas y villancicos. En la Peña «Buena Gente», en su sede de la Plaza de San Lucas, se hará la presentación de su Coro de Villancicos el día 11 y el día 18 se proyectará una Zambomba Popular Flamenca. Otro tanto ocurrirá el mismo día en la Peña de «El Garbanzo», Peña Tío José de Paula y el Centro Flamenco de Don Antonio Chacón. Nuevos valores flamencos en la Peña «El Garbanzo» Bajo este título «Nuevos valores del Cante y Baile Flamencos», se han desarrollado unas gratas jornadas en la peña jerezana de «El Garbanzo», enclavada en el corazón del flamenquísimo barrio de San Miguel —cuna de Manuel Torre y de Chacón—, durante el mes de octubre en las noches de los viernes, unas magníficas intervenciones que corrieron a cargo de distintos cuadros flamencos. Abría este especacular ciclo el cuadro de «Raquel Romero», que tuvo como protagonista a la destacada bailaora Virginia Rojas el 8 de octubre. El día 15 del mismo mes la noche estuvo dedicada a ese joven y extraordinario bailaor Fernando Galán, que a su vez presentó a su cuadro, uno de los mejores que estuvieron presentes en la fiesta de la Bulería organizada por el Ayuntamiento jerezano hace muy pocas fechas y que volvía con las mejores ilusiones a la sede de esta entidad de la calle Santa Clara. La noche del 22 la cubrió con seriedad y arte el cuadro flamenco de Silvia Oen, y cerraba el 29 con un exquisito compás de buen baile el Cuadro de la Academia de la Chiquí de Jerez, que tuvo como principal protagonista la jovencísima bailaora\n\n[ENDING CONTEXT]\n\nsus acompañantes lo pasen bien dentro de la propia zona, sin que tengan que hacer largos desplazamientos, de ahí que se hayan dejado las tardes libres. Dentro de las actividades, a falta de desarrollar y cuantificar definitivamente, se pretende hacer:\n\nPublicar dos libros.\n\nGrabar dos discos.\n\nHomenaje a Blas Infante en Casares.\n\nPresentaciones de otras obras.\n\nPresentación del Yunque Flamenco.\n\nPor supuesto, que tanto las publicaciones como las grabaciones serán dedicadas a la provincia de Málaga o a artistas muy vinculados por proximidad, vecindad, etcétera.\n\nXXII CONGRESO DE ARTE FLAMENCO\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "23-25",
    "page_number": 23,
    "word_count": 2392,
    "article_char_count_full": 14463,
    "article_char_count_review": 3552,
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
    "article_id": "1993-09-26-left-cartas-al-director-agust-n-g-mez",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCartas al Director\n\nC ontesto a su carta para de- cirle que yo también he recibido las dos cassettes con los cantes de «El Niño de las Mar rianas», padre del eminente gui- tarrista «Luis Maravilla».\n\nMe dice, y con razón, que no está usted de acuerdo con Manuel Cerrejón cuando le dice a «Luis Maravilla» que Escacena fue el primero en cantar por tarantas. Coincido con usted en todo. Esa pregunta me viene a decir que Cerrejón entiende po-quísimo de cantes y de sus orí-genes.\n\nPara mí es que existieron grandes intérpretes por ese palo, profesionales que no nos legaron su voz y su arte porque en aquellas fechas aún no existían grabadoras. Creo que las tarantas, cantes mineros por excelencia, ya se cantaban a finales del siglo dieciocho durante el reinado de Carlos III.\n\nSabido es de todos los\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionados\"]\n\nCoincido con usted en todo. Esa pregunta me viene a decir que Cerrejón entiende po-quísimo de cantes y de sus orí-genes. Para mí es que existieron grandes intérpretes por ese palo, profesionales que no nos legaron su voz y su arte porque en aquellas fechas aún no existían grabadoras. Creo que las tarantas, cantes mineros por excelencia, ya se cantaban a finales del siglo dieciocho durante el reinado de Carlos III. Sabido es de todos los buenos aficionados que Escacena fue un gran tarantero, pero que fuese el primero en ejecutarla, eso no es cierto. Antes que él naciera (1885) ya las cantaron muchos profesionales, especialmente en Almería, cuna del cante que nos ocupa. A continuación relaciono a algunos artistas que la cantaron antes que el sevillano Manuel Escacena: Por orden de nacimiento Juan Breva - Rojo el Alpargatero - Fernando el de Triana - Antonio Chacón - Niño de Cabra - Garrido de Jerez - Sebastián «El Pena» - Niño de la Isla - Niño Medina - Fernando «El Herrero» - Manuel «Torre» - «El Cojo de Málaga» - Rafael Pareja Majarón - «La Pompi» - Manuel Pabón - «Niño de las Moras» - Manuel Centeno - «Pepe el de la Matrona» - «El Chato de las Ventas» - José Cepero y «Bernardo el de los Lobitos». Sólo cuatro artistas de los enumerados nacieron dos años después que Escacena, pero como éste se inició en el cante, como profesional, después que ellos, es lógico y natural que las cantara después. Un dato histórico: Escacena apareció por Madrid de la mano de Francisco Lema «Fosforito», quien después de escucharlo cantar se lo presentó a Chacón diciéndole: «Antonio, quiero que esta noche escuches cantar por Levante a este muchacho, para que después me digas si estamos o no ante una gran promesa de cantaor». Efectivamente, lo escuchó y le dijo a «Fosforito»: «Mira, Paco, el muchacho me ha gustado una \"jartá\" por Levante y de forma especial por taranta. Creo, como tú, que estamos ya ante un fenómeno cantaor. Pero lo que no gusta de él es su cabeza en forma de pepino». Y con este apodo se quedó. Mi querido amigo Rafael Valera: Acabo de entregarme al número 87 de la Revista que tan acertadamente diriges, cuando tropiezo con las reflexiones y ejemplos prácticos que el compañero Agustín Gómez aporta al trabajo «El lenguaje de la crítica flamenca». Como quiera que se incardina el asunto de esta carta en el soporte radiofónico y ya le resulta imposible, después de 30 años, ampliar con rigor su didáctica doctrinal desde los micrófonos de COPE en Córdoba, en torno a si Enrique Morente escuchó previamente el cante de Charamusco a Antonio Mairena y luego decidió grabarlo —como así publiqué en su día con el refrendo de Juan Antonio Muñoz Pacheco, dueño de la grabación doméstica—, te a\n\n[ENDING CONTEXT]\n\nbulerías en el concurso jerezano de la Peña Los Cernícalos. Asiduo acompañante de Fernanda y Bernarda de Utrera en los Festivales andaluces. Participó en el Concurso El Giraldillo del Toque de la III Bienal de Arte Flamenco Ciudad de Sevilla y en los festivales de la III Cumbre Flamenca de Madrid, en 1986. Ha realizado grabaciones y comparte sus actuaciones con la dedicación a la enseñanza de su arte en su ciudad natal.\n\nTOCAORES DE HOY Paco del Gastor\n\nCAJA GENERAL DE AHORROS DE GRANADA Instituto de Estudios Giennenses. Candil : boletín de la Peña Flamenca de Jaén. N.º 89, 9/1993. Página 27\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cartas al Director Agustín Gómez y su vuelta a Charamusco",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "25-27",
    "page_number": 25,
    "word_count": 1169,
    "article_char_count_full": 6825,
    "article_char_count_review": 4347,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionados"
      }
    ]
  },
  {
    "article_id": "1993-09-26-right-qu-date-con-el-cante",
    "article_text_for_review": "canal sur RADIO\n\nDe lunes a viernes, de 9 a12 de la noche.",
    "title": "\"Quédate con el Cante\"",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "26-26",
    "page_number": 26,
    "word_count": 13,
    "article_char_count_full": 58,
    "article_char_count_review": 58,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
