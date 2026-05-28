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
    "article_id": "1992-01-30-right-qu-date-con-el-cante",
    "article_text_for_review": "Programa Flamenco\n\nSintonícenos de lunes a viernes, de 20,30 a 22,00 horas; viernes, sábados y domingos de 0,30 a 3,00 horas, FLAMENCO",
    "title": "\"Quédate con el Cante\"",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "30-30",
    "page_number": 30,
    "word_count": 22,
    "article_char_count_full": 134,
    "article_char_count_review": 134,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-01-31-left-tocaores-de-hoy",
    "article_text_for_review": "Habichuela, Pepe. Nombre artístico de José Antonio Carmona Carmona, heredero de su padre. Granada, 1944. Guitarrista. Hijo de Tío José Habichuela y hermano de Juan, Luis y Carlos Habichuela. Ha acompañado al cante, entre otros destacados intérpretes, a Pepe Marchena, El Camarón de la Isla y a Enrique Morente, grabando con este último un disco en memoria de don Antonio Chacón, con el que obtuvo el Premio Nacional de Discografía del Ministerio de Cultura, en 1975. Igualmente ha grabado en disco con Fernanda y Bernarda de Utrera, Jarrito, El Cabrero, Rafael Heredia y Carmen Linares, en otras destacan sus recitales en la Cumbre Flamenca de Madrid, en 1984 y 1985, en la III Bienal de Arte Flamenco Ciudad de Sevilla, 1984, y en ese mismo año en la Correfour de la Guitare de La Martinica. En 1986 participó con un concierto, en los actos organizados por el Ayuntamiento de Madrid durante el ciclo de los Veranos de la Villa.\n\nPepe Habichuela",
    "title": "TOCAORES DE HOY",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "31-31",
    "page_number": 31,
    "word_count": 161,
    "article_char_count_full": 945,
    "article_char_count_review": 945,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-03-3-left-editorial",
    "article_text_for_review": "Editorial\n\nN o es la primera vez que nuestra publicación se hace eco del tratamiento, ciertamente cicatero, que los responsables de la Exposición Universal han dispensado al Flamenco. Lo decíamos entonces como conjetura y haciendo votos para que la dinámica de los acontecimientos contradjeras nuestro pesimista pronóstico y nos impeliese a la rectificación. Pero no ha sido así. Nos han llegado noticias con alguna suerte de confirmación institucional que señalan recortes de hasta un setenta y cinco por ciento respecto de la dotación presupuestaria que en un principio se asignó a espectáculos flamencos. Dotación que si entonces calificábamos de exigua, hoy no debe de resultar hiperbólico que la llamemos miserable. Cabe preguntarse, desde la perspectiva de los organizadores, a qué razones ocultas o sobreentendidas obedece el criterio mantenido, es decir, por qué inexplicable causas se minusvalora el Flamenco, música genuinamente andaluzas, de contenido telúrico, singularísimo y universalmente enco-\n\nmiada, frente a otras músicas cultas igualmente respetables, hasta producirse la paradójica circunstancia de que, en concepto de gestión o de corretaje de las segundas se pague cantidades superiores a las que comporta el presupuesto completo de las primeras. En modo alguno, puede reputarse ignorancia, lo que, a nuestro modesto juicio, acaso sólo sean lógicas secuelas de una estrategia de marketing adoptada por los responsables de la Ex-\n\nposición Universal que diseñaron una imagen de esta Comunidad Autónoma tan lejana de símbolos representativos de los tópicos al uso como próxima a una sociedad moderna y evolucionada. Ello no admite ninguna objeción, siempre que lo jondo no resulte trivializado como consecuencia de una subliminal y en cualquier caso inaceptable identificación entre lo flamenco y lo tópicamente aflamencado. Parece como si la Expo adoleciera de esa cultura vergonzante, cuando lo riguroso es que se hubiese aprovechado esta incomparable cita para mostrar a propios y extraños, la mágica especificidad del Flamenco y salir, de una vez por todas, de la expropiación «topiquera» de que ha sido objeto. No fue así y, forzoso es decirlo, la Administración institucional andaluza, en este caso, ha frustrado, vía omisión, una de las reivindicaciones culturales más arraigada en amplios sectores de población de esta Comunidad Autónoma: la divulgación y, tal vez, absoluta dignificación del Flamenco.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1992-03",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 365,
    "article_char_count_full": 2431,
    "article_char_count_review": 2431,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-03-3-right-apuntes-sobre-la-sole-y-la-sigui",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n2) Enlaces: Uno de los aspectos que hoy mismo parece ser concretado es el del enlace estilístico de la soleá con el fandango. Especialmente es notorio en las últimas preferencias guitarristicas donde se ve claramente cómo muchos tercios de fandangos son preludia-dos por toques de soleá, o directamente acompañados en gran parte de su desarrollo vocal. En ambos casos, el fandango resulta más acompasado y más próximo al cante grande. Valga un ejemplo: el Niño Ricardo es el tocaor que mejor se acopla a los giros personales de cada intérprete, siendo en este sentido su capacidad de adecuación sencillamente única. Sobre esta base, es lógico suponerle como el guitarrista que mejor sabe imprimir al fandango ciertas reminiscencias solearescas muy oportunamente tercia-das. (Conviene consignar que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestro\"]\n\nue ha sido, conjuntamente con Manolo el de Badajoz, el promotor de las nuevas corrientes del fandango en la guitarra y, a la vez, de los cantes chicos modernos.) También se suman a este testimonio los discos grabados por Melchor de Marchena, Sabicas, Paco Aguilera, etcétera. Como punto aparte, figura Pa- quito Simón, eximio acompañante de la compleja personalidad de Pepe Mar- chena. En las exigencias que llamaríamos transitivas del autollamado «maestro», por cuanto acostumbra a aglutinar en enlaces perfectos los cantes que parecieran menos aptos para tales «composturas», Paquito Simón alcan- za a destacarse como un acompañante perfecto, aco- tando a través de hábiles fal- setas la inusitada movilidad de uno a otro cante que tan- to le place a Marchena. Por lo que a ejemplo de cantaores se refiere, es de notar que el enlace de fandango y soleá no se produce en un plano melódico, sino estrictamente rítmico, o por mejor decir en ciertas entradas vocales. Acostumbran cultivar este nuevo recurso «plástico» cantaores como Manolo Caracol, Pepe Marchena, la Niña de los Peines, Pepe Pinto, Juanito Varea, etcétera. A cuentas: el fandango sólo conseguirá definir su discutida posición en la escala de los cantes (grandes, chicos o fronterizos) en tanto y cuanto no se decida a fundirse rítmicamente con la soleá, sin necesidad —por otra parte absurdamente pleonástica— de tomarle el resto de sus rasgos. En resumen: el fluido perfecto en e\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_01 | trigger=\"dentro\"]\n\ndesenlace corriente-mente histriónico no apetecido) es el de sus combinaciones con el recitado flamenco. Esta moda del recitado —colmo del masismo— suele desarrollarse en base a tiradas poéticas que musicalmente se expresan por soleá. Baste citar el famoso «Toito te lo consiento» de Pepe Pinto o los célebres recitados semi-hablados, semi-cantados, por el incorregible Pepe Marchena. Discutible o no el buen gusto de estas infiltraciones poéticas dentro del cante, el caso es que no sólo no parecen decaer, sino que día a día ganan más preferencia en la estima de los públicos modernos. Lo notable es que la soleá es la víctima preferida, acaso por ser el molde que más instantáneamente expande el «color flamenco». 4. Estilos: A estas alturas, la opción es bien clara: o se cantan soleares clásicas, jondas, o directamente se las interpreta de acuerdo al gusto «flamenco». La transición o mezcla se p\n\n[ENDING CONTEXT]\n\nnovedosos, aunque para el entendido esta actualización pueda resultar impura con respecto a la soleá del buen cante.)\n\nSi como desarrollo, la soleá es un círculo, como estructura es un triángulo isóceles cuyos dos lados iguales (las alegrías y tristezas, las aspiraciones y realizaciones) inviablemente se resuelven en la base sólida que es la propia esencia del hombre. Su base está más allá del estado de ánimo de lo expresado y del ajetreo y la experiencia de la vida. De ahí la sensación de sosiego, estoicismo y conformidad de ánimo que deja en el aficionado, cuanto más en el intérprete.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Apuntes sobre la soleá y la siguiriya / 2",
    "periodical": "candil",
    "issue_id": "1992-03",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-5",
    "page_number": 3,
    "word_count": 2486,
    "article_char_count_full": 15408,
    "article_char_count_review": 4036,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestro"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "dentro"
      }
    ]
  },
  {
    "article_id": "1992-03-6-left-vicente-soto-sordera-la-ra-z-jer",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas dicen...\n\nCierto es que traía una determinada aureola como flamenco intelectualizado y maestro de ceremonias del «Madrid jondo, jondo».\n\nY la verdad es que no es para menos, porque dirigir técnicamente un ciclo de la envergadura del citado y por el que van a pasar más de cien figuras del arte flamenco, es para arrastrar dicha fama.\n\nT ambién confluyen en su persona una serie de trabajos discográficos que están a caballo entre su procedencia flamenca y la aportación de nuevos caminos? por los que se está inclinando últimamente determinado colectivo joven de cantaores flamencos. Sin embargo, el interés de los aficionados de la Peña Flamenca de Jaén estaba en escuchar a uno de los jóvenes vástagos del flamenco árbol genealógico formado por la savia de Paco La Luz,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"duende\"]\n\nbajos discográficos que están a caballo entre su procedencia flamenca y la aportación de nuevos caminos? por los que se está inclinando últimamente determinado colectivo joven de cantaores flamencos. Sin embargo, el interés de los aficionados de la Peña Flamenca de Jaén estaba en escuchar a uno de los jóvenes vástagos del flamenco árbol genealógico formado por la savia de Paco La Luz, por un lado, y del Gloria, por otro. Y la verdad es que hubo duende y determinada fiesta en su recital. También sabrosas respuestas a la serie de preguntas que le fuimos realizando en nuestra acostumbrada línea. —R. V. ¿Cómo comienzas a cantar? —Empecé de chiquitito. Como nací en Jerez, pues desde los seis o siete años sólo he escuchado cantar. La gente de mi padre, la gente de mi madre... Por la parte de mi padre, la gente es la de Paco La Luz; por la de mi madre, la del Gloria. Aunque no tardé mucho en irme a Madrid, lo cierto es que mi infancia la he pasado en Jerez y lo que allí viví se me ha quedado grabado en la memoria. Mi abuelo Vicente Varea —por el que yo me llamo Vicente— era primo del Gloria y un aficionao que tenía un conocimiento del cante increíble, aunque lo suyo era bailar magníficamente. El fue mi auténtica escuela, y por supuesto mi casa. Allí todos hemos cantao y hemos bailao. Era toda una auténtica casa de cante, baile y mucha comía. —R. V. ¿Qué recuerdos tienes del Barrio de Santiago? —Recuerdo que mi abuelo —que era compadre de Tía Anica, que en gloria esté— me decía: «Anda hijo al bar La Quinta y tráete dos medias botellas». Este era un bar que estaba en la calle La Sangre, que existe todavía. Traía las botellitas y los escuchaba cantar a ellos cua\n\n[ENDING CONTEXT]\n\nenorme en mi hermano, porque él es un cantaor importante en un nivel anárquico y rancio. Es un cantaor serio.\n\n—M. M. M. ;Te consideras profeta en tu tierra?\n\n—¡No! Indudablemente que no. Si no lo ha sido Terremoto, ni Sernita, ni tampoco «Sorderas», ni lo es La Paquera... ¿cómo voy a serlo yo? Sería el único.\n\n-M. M. M. ¿Te ha dado cariño Jerez?\n\n—A mí me da cariño mi familia, que son muchos. Jerez y los señores que mueven el flamenco en mi tierra, no. Yo no debo nada a Jerez. Pienso que esto es debido a la fuente inagotable de arte que tienen y por eso no le dan importancia a sus artistas.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Vicente Soto «Sordera», la raíz jerezana en Madrid",
    "periodical": "candil",
    "issue_id": "1992-03",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "6-8",
    "page_number": 6,
    "word_count": 2859,
    "article_char_count_full": 15708,
    "article_char_count_review": 3305,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "duende"
      }
    ]
  }
]
```
