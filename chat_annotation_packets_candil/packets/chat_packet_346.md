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
    "article_id": "1998-01-3-left-nuevos-aires-y-renovada-ilusi-n",
    "article_text_for_review": "No sabemos si el «renovarse o morir» es un ejercicio de presuntuosos en relación con este número que acaba de ver la luz. Sí entende-mos que nuestra intención es darle un enfoque más lustroso a nuestra revista, una apariencia nueva que de alguna manera sirva para que sus páginas sean más atractivas al aficionado lector.\n\nNo vamos a bajar la guardia en cuanto a contenido. Es más, queremos incentivar la vigilancia para que todos los artículos o trabajos tengan las suficientes garantías de calidad, veracidad y juicio crítico en un claro intento de revalorizar, incentivar y difundir nuestro arte flamenco.\n\nEstá claro que no es este el primer proyecto de «lavado de cara» que la publicación ha efectuado. Ya, a partir del número sesenta y uno, y creo que muy acertadamente -perdón por nuestra inmodestia-, Candil acometió una renovación que supuso un cambio de portada y contraportada, así como un enfoque diferente de la maquetación de sus páginas, donde la serie de trabajos encuadrados en el título \"Viejo carnet flamenco\", de Anselmo González Climent, tuvieron especial importancia. Pensamos que la transmutación fue bien aceptada por nuestros lectores y colaboradores. Una vez culminado el número cien, nuestro interés por volver a retomar la renovación de la revista vuelve a aflorar y, realizado el intento, hemos comprendido que no ha sido lo suficientemente aceptable por el actual Consejo de Redacción y parte de algunos lectores que así nos lo han comunicado, por lo que, como apuntamos al principio, efectuamos el reintento con la ilusión de acertar.\n\nMas no sólo esta labor se proyecta en función de la publicación solamente. Hemos querido igualmente introducir savia nueva con la renovación del aludido Consejo de Redacción, la Administración de la misma, el equipo de maquetación y una nueva estructura de servicio a los suscriptores y colaboradores, a fin de trabajar unidos con la intención de actualizar la periodicidad de Candil y sus contenidos. En este mismo sentido y para favorecer la difusión de nuestros trabajos, en colaboración con la Excma. Diputación Provincial de Jaén y más concretamente con su Patronato de Turismo, en las páginas de Internet de este último, Candil ha incluido un referente en el que\n\nQueremos también significar que nuestra línea base no va a cambiar y que estas páginas van a seguir recogiendo los trabajos de investigación y ensayo sobre el flamenco, las entrevistas, las críticas literarias y discográficas, así como los reportajes de los eventos más sonoros y señalados de nuestro arte. Tampoco vamos a abandonar la serie de números monográficos dedicados a las figuras o temas más sobresalientes de esta cultura musical. Y como ha venido siendo patente, seguiremos contando con las inestimables aportaciones de nuestros colaboradores y nuestros anunciantes mecenas.\n\nPor último y manteniendo siempre nuestro agradecimiento, renovar igualmente nuestro llamamiento a las instituciones de nuestra ciudad, provincia y comunidad, para que sigan prestándonos ese apoyo económico tan necesario para la supervivencia de esta publicación, que aunque sigue aflorando, las dosis se van reduciendo y el costo de la misma continúa incrementándose.\n\nGracias por toda la solidaridad prestada y abiertos estamos a cualquier sugerencia.",
    "title": "Nuevos aires y renovada ilusión",
    "periodical": "candil",
    "issue_id": "1998-01",
    "year": 1998,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 516,
    "article_char_count_full": 3277,
    "article_char_count_review": 3277,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-01-4-left-la-investigaci-n-del-flamenco-he",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nGerhard Steingress\n\nE n un primer paso intentaré demostrar cómo el auge de la investigación del flamenco estuvo vinculado en gran parte al nuevo papel que este género ocupó en el ámbito de su conversión en un elemento clave de la cultura andaluz, más aún, en una seña explícita de identidad colectiva. Ambos hechos han de ser comprendidos como fenómenos estrechamente vinculados al cambio social, político e ideológico acontecido en el transcurso del régimen franquista a la democracia, es decir, a partir de la década de los años sesenta hasta la de los ochenta de nuestros siglo.\n\nCon el evidente ocaso del régimen franquista se aceleró el acercamiento de España a una Europa que había pasado desde finales de la Segunda Guerra Mundial por una larga serie de transformaciones socio-culturales\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"nuevo\"]\n\nn de las economías y las estructuras socio-políticas. Sería sobre todo a partir de 1975, cuando España se alistase a este proceso a través de una acelerada modernización, es decir, euro-peización. Como consecuencia, se iniciaría una reorientación específica de la cultura popular. Se intentó revitalizar su capacidad representadora y expresiva de y para amplios sectores sociales en vista del futuro desarrollo de la sociedad española y el deseado nuevo orden social, democrático y más justo. En esta situación, el cante flamenco—elemento inquieto, inconformista, polifacético y subcultural—destacó por su capacidad de unificar formas estéticas con nuevas necesidades y valores culturales. Otra de las múltiples consecuencias de esta modernización fue la creciente profesionalización de la educación, de la investigación científica y la administración pública. Sus efectos empezarían a notarse también en lo que a la comprensión y evaluación del género flamenco se refiere: el flamenco se transformó no sólo en una adecuada manifestación transclasicista y polifacética para satisfacer las necesidades culturales de un nuevo tipo de público, en objeto de una amplia producción discográfica y de política cultural, sino también de estudios científicos, que contrastaron considerablemente con los anteriores conceptos neorrománticos y especulativos (véanse Steingress 1993, 1997a, 1997b; García Gómez 1993, Mitchell 1994, Washabaugh 1996). Debido a es\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_03 | trigger=\"tradiciones\"]\n\nInglaterra, $ ^{2} $ y sobre todo tras el emblemático «Concurso de la Llave de Oro» de 1962 en Córdoba, el cante había «renacido» tras largas décadas de su supuesta decadencia operística, que en realidad fueron los años de su diversificación artística acordes con los avances tecnológicos, el nuevo ambiente socio-cultural y una estética más actual. Como consecuencia de todo ello surgió el deseo entre los cantaores y aficionados de retomar ciertas tradiciones en el cante para evitar su olvido y desaparición. El cante «renació» entonces como «cante gitano-andaluz», modelado por la voz del gran cantaor de Mairena del Alcor y declarado propiedad exclusiva de los gitanos. $ ^{3} $ Pero este auge del interés público por el cante como manifestación minoritaria, subcultural y «mis-teriosa» no se puede explicar mediante el esfuerzo individual de un reconocido cantaor. Su obra sólo pudo triunfar debido a la s\n\n[ENDING CONTEXT]\n\nla Frontera (S.A.), págs. 343-380. Sociología del cante flamenco. Fundación Andaluzas de Flamenco. Jerez de la Frontera, 1993. Cante flamenco. Zur Kultursoziologie der andalusischen Moderne. Peter Lang. Frankfurt am Main, 1997(a). «Der Flamencogesang als künstlerischer Akt, ideologisches Instrument und Bestandteil der kulturellen Identität Andalusien». En: Österreichische Zeitschrift für Soziologie, 3/1997(b), págs. 30-53.\n\nSUBIRÁ, JOSÉ: La Tonadilla Escénica. 2 tomos. Tipografía de Archivos. Madrid, 1929.\n\nTURINA, JOAQUÍN: La música andaluza. Intr. de Manuel Castillo, Alfar. Sevilla, 1982.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La investigación del flamenco: Hechos, problemas, perspectivas (una aproximación crítica)",
    "periodical": "candil",
    "issue_id": "1998-01",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "3-8",
    "page_number": 3,
    "word_count": 4944,
    "article_char_count_full": 32415,
    "article_char_count_review": 4052,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "nuevo"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradiciones"
      }
    ]
  },
  {
    "article_id": "1998-01-21-left-ellos-los-protagonistas-dicen-ma",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen...\n\nRafael Valera Espinosa\n\nSin complejos, sin ataduras, con la gracia innata de su Cádiz... Así se presenta Mariana Cornejo. Y nos cuenta su historia con una sencillez y naturalidad asombrosa. Como si lo que le ha acontecido en su vida fuese algo común.\n\nAmante de este arte y muy concretamente del de su tierra. Amante igualmente de los artistas de su Cádiz y defensora a ultranza de Pastora y La Perla de Cádiz, Mariana nos evoca unos comienzos basados en la precocidad y en el talante excesivamente paternal de su progenitor por no incluirla en el mundo de las compañías de cante.\n\nSe siente artista y lo defiende con uñas y dientes. Admite los vanguardismos y los nuevos movimientos flamencos, mas siempre antepone la pureza de este arte. Siente predilección por\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"ritmo\"]\n\ndefensora a ultranza de Pastora y La Perla de Cádiz, Mariana nos evoca unos comienzos basados en la precocidad y en el talante excesivamente paternal de su progenitor por no incluirla en el mundo de las compañías de cante. Se siente artista y lo defiende con uñas y dientes. Admite los vanguardismos y los nuevos movimientos flamencos, mas siempre antepone la pureza de este arte. Siente predilección por las siguiriyas y también por los estilos de ritmo y compás, aduciendo que por ser ella muy alegre y festera. Y siempre su Cádiz por delante... Fotos: José Pamos Mariana Cornejo, entrevistada —¿Cómo son tus inicios? —Pues mis comienzos son desde chiquitita, como les suceden a todas las niñas que empiezan a cantar y a tocar las palmas y eso. Pero verdaderamente fue un día que estando yo en un patio de la calle Feluchi, pues mi madre trabajaba allí, me puse a cantar eso de «Cariño, cariño mío / ramito de mejorana...», ya que yo estaba barriendo el patio. Esto lo cantaba con mi hermano Pepe, que canta magnificamente, pero que no se ha dedicado a esto; él hacía el bajo y yo el tono alto y a dúo nos salía bastante bien. Estando cantando me escuchó un pianista que estaba en el segundo piso dando una clase; se asomó al balcón y le preguntó a la madre de la niña a la que daba la clase, que quién era yo con la voz tan bonita. La madre de la niña le contestó que era la hija de María Pepa. De momento dijo que subiera a verle porque le había gustao mucho mi voz. Subimos mi madre y yo y el pianista le dijo a mi madre que quería que me diera clases. Mi madre le contestó que ella no tenía dinero y que no podía pagarlas. Le dijo que no se preocupara que él me daría clases para educar la voz y así poder presentarme en Radio Cádiz, que entonces tenía un programa para estas cosas. Don Carlos Domínguez que era el pianista, me enseñó y entonces debutamos en Radio Cádiz. Entonces, al enterarse mi tío Paco le dijo a su primo Canalejas de Puerto Real lo bien que yo cantaba. Éste se presentó en mi casa bastante temprano —¡ya ves tú, estaba yo durmiendo todavía!— y le dice a mi madre: «Vísteme a la niña y sácala al patio que quiero que me cante un poquito de flamenco». Salí al patio y comencé a cantar y, nada más hacerle un poquito, me dijo: «¡Ya está, no cantes más!». Entonces\n\n[ENDING CONTEXT]\n\nes incomparable. Hasta el punto de que a mí me paran por la calle hasta los niños. Pero lo mismo en Cádiz que en Sevilla o en Málaga, o en otra ciudad.\n\n—¿Produce esto tristeza en un artista flamenco?\n\n—A mí no porque yo nunca he dejao lo mío ni lo pienso dejar. Y si me dijeran que me fuera a hacer teatro o televisión, como sabes que lo he hecho, y que dejara el flamenco, yo nunca dejaría el flamenco. Es indiscutible que una pantalla te da mucha popularidad pero amigo, este arte que nosotros interpretamos es único, y los que tenemos la suerte de poder desarrollarlo, nunca podemos abandonarlo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen... Mariana Cornejo",
    "periodical": "candil",
    "issue_id": "1998-01",
    "year": 1998,
    "language": "es",
    "article_type": "trofeo_section",
    "pages": "20-22",
    "page_number": 20,
    "word_count": 2499,
    "article_char_count_full": 13440,
    "article_char_count_review": 3916,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "ritmo"
      }
    ]
  },
  {
    "article_id": "1998-01-23-left-poema",
    "article_text_for_review": "Hay tantas cosas no dichas con la luz de la palabra. En la espera del espíritu por la carne que lo arana. En la hiel de la verdad por el surco de la lágrima. En las huellas que caminan por la tierra que separan. En la boca que no arde en el fuego que la abrasa. En todo lo que comienza en todo lo que no acaba. En el linaje cobarde que siempre nos acompaña. En el cuerpo traicionado por la sangre que lo arrastra. Y en tanta, tanta injusticia con que el mundo se levanta, llevando a rastras, hundido, el fardo de tanta carga. Hay tantas cosas no dichas con la luz de la palabra! En tanta piedad sin manchanta sombra proyectada. En tanto secreto antiguo tanta obstinación amarga. En la herencia de la carne, el placer y su venganza. Un camino inacabable de conciencia profanada.",
    "title": "Poema",
    "periodical": "candil",
    "issue_id": "1998-01",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 147,
    "article_char_count_full": 777,
    "article_char_count_review": 777,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-01-23-right-una-vida-llena-de-seis-figuras-d",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Martín Martín\n\nTengo para mí que la función más pura del arte es hacer de intermediario entre el poder secreto que opera en su fondo y la sociedad que lo demanda, es decir, sacar a la luz la impenetrable oscuridad que lo hace indescripible.\n\nEn ese ideal se movió siempre Pedro Bacán. Justo es significar que al principio tuvo que tomar conciencia de lo que era. Más tarde le excitó encontrar el modo de controlar lo que se ofrecía ante sus ojos, y, después de dominar la realidad, dejóse chorrear por ese estado de inspiración mística y exaltada a fin de lograr el milagro de la vida artística: hacer coexistir el conflicto entre el sentimiento y la razón bajo el lema de que para ser artista hay primero que captar y transformar los recuerdos vivenciales en experiencia, la experiencia en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nrolar lo que se ofrecía ante sus ojos, y, después de dominar la realidad, dejóse chorrear por ese estado de inspiración mística y exaltada a fin de lograr el milagro de la vida artística: hacer coexistir el conflicto entre el sentimiento y la razón bajo el lema de que para ser artista hay primero que captar y transformar los recuerdos vivenciales en experiencia, la experiencia en emoción y la materia de ésta en placer musical. Así, nuestro Pedro Peña Peña, primogénito de los hijos habidos en el matrimonio entre Ana Peña Vargas y Sebastián Peña Peña, conocido por Bastián Bacán y de quien heredó el apodo artístico, nació en Lebrija el 12 de febrero de 1951. Se forjó pues, en el seno de una familia gitana y lebrijana de gran tradición flamenca, no en vano era biznieto del legendario Pinini, nieto de Fernanda la Vieja y sobrino nieto de María Peña, amén de sobrino del Chache Lagañas, sobrino paterno de Fernanda y Bernarda de Utrera, y primo de Juan el Lebrijano, Miguel Funi y Pedro Peña, entre otros. Aunque recibió las primeras enseñanzas del lebrijano Benito Vázquez Caro, el maestro Penaca, sus principios quedaron influenciados por el legado familiar, más su conocimiento del cante y la guitarra, así como la reflexión profunda que hizo en torno al fondo musical lebrijano, le hicieron independizarse de todo tipo de atadura familiar, con lo que, andando el tiempo, daría rienda suelta a su libertad imaginativa hasta forjar su propio discurso expresivo. Hasta los 20 años Pedro\n\n[ENDING CONTEXT]\n\ncomo las anteriores lo hemos encontrado en la vida y obra de Pedro Bacán, un lebrijano de oro y andaluz universal que, después de absorber el mundo local y familiar circundante, consiguió ser algo más que él mismo: rebelarse contra el hecho de consumirse dentro de los límites de su propia vida, dentro de los límites transitorios de su propia personalidad, a fin de penetrar en los más profundos secretos del cante y la guitarra, fundirlos ambos con la totalidad de lo real y brindar al mundo el milagro que hemos pretendido demostrar: una vida musical llena de seis figuras de luces nuevas.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Pedro Bacán: Una vida llena de seis figuras de luces nuevas",
    "periodical": "candil",
    "issue_id": "1998-01",
    "year": 1998,
    "language": "es",
    "article_type": "trofeo_section",
    "pages": "23-26",
    "page_number": 23,
    "word_count": 4073,
    "article_char_count_full": 23478,
    "article_char_count_review": 3111,
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
  }
]
```
