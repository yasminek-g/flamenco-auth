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
    "article_id": "1987-03-37-right-por-sus-dichos",
    "article_text_for_review": "Pedro Sánchez\n\n«M e encuentro a mis anchas en los cantes difíciles. Porque la leña verde pesa más que la seca y exige más esfuerzo y duele más el hombro...».\n\n«El cante para mí es como vivir mi tierra y mi gente, vivir Andalucía, con la garganta bien dispuesta para absorber toda la sal y todo el sol de nuestra región...».\n\n«Es muy difícil explicar con palabras los sentimientos que acumulo en mi corazón...».\n\n«El pueblo andaluz ha tomado una mayor conciencia de nuestras raíces...».\n\n«Presumo de ser un acaparador de amigos... más que de trofeos...».\n\n«A los que empiezan hay que darles todos los medios posibles para acentuar su afición e indicarles el camino derecho...».\n\n«En el Cante todo es un continuo perfeccionamiento...».\n\n«He entregado mi vida al Cante porque soy andaluz y en el Cante palpita Andalucía...».\n\n«Mis inquietudes de tipo social no sé reflejarlas cantando...». «No seré yo quien diga que el Flamenco no vive una buena época...».\n\n«Las peñas aúnan a los amantes del Flamenco, y gracias a ellas nuestro Cante puede pervivir por tiempo indefinido con la categoría que merece...». «Confieso que mi mujer y mis hijos llenan mi vida de felicidad. Por ellos trabajo sin descanso, lamentando no poderles dedicar más tiempo...».\n\n«Lo flamenco yo lo defino como la expresión del pueblo andaluz en su más pura esencia...».\n\n«Mi cante expresa casi siempre mi estado anímico, penas, alegrías que pertenecen a otras épocas quizá, pero que, cantadas hoy, tienen si cabe la misma frescura...».\n\n«Creo que todos los cantaores deberían saber tocar algo la guitarra para así mejor comprender lo difícil que es sacarle a este instrumento todo lo que el mismo encierra...».\n\n«No soy un hombre fuerte, pero sé dosificarme bien...».\n\n«El Flamenco está atravesando el momento más importante de su historia...».\n\n«Mi éxito radica en la sinceridad con que me manifiesto...».\n\n«Dios reparte a muy pocos la enjundia y la hondura...».\n\n«Yo no canto para teóricos, sino para el Flamenco; el aficionado que pueda captar mi sensibilidad y el problema de la copla que le remito...».",
    "title": "POR SUS DICHOS",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "37-37",
    "page_number": 37,
    "word_count": 344,
    "article_char_count_full": 2075,
    "article_char_count_review": 2075,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-38-left-homenaje-a-un-cabal-cabalisimo",
    "article_text_for_review": "U na gran satisfacción me ha producido el requerimiento de nuestro querido director para contribuir con la modesta aportación de estas breves líneas al homenaje de esa persona y gran artista al que va destinado este número.\n\nConozco a Fosforito, para el que guardo mi mayor admiración y consideración, exclusivamente en su faceta artística, ya que no he tenido oportunidad de comunicarse con él a nivel personal, circunstancia que deseo pueda darse al estimar su gran proyección humana.\n\nTratar de analizar los méritos que concurren en un artista consagrado y aclamado por legión de aficionados, sería tanto como limitar premeditadamente el homenaje para el que se me requiere.\n\nEs curioso que desconociendo personalmente al homenajeado y hasta es-\n\ntando en parte en contradicción con la corriente bobalicona que se deja influenciar por la opinión de una masa posiblemente no muy exigente, en lo artístico, venga a proclamar mi adhesión a una figura a la que solamente en lo personal conozco por referencias pero de tan buena intención y contrastada seriedad dentro del ámbito flamenco que al merecer toda mi confianza, me hacen participar con el mayor grado a esta efemérides para resaltar virtudes de este gran hombre, ajenas por completo a su proyección flamenca en el más puro y aséptico sentido de la denominación ya que su condición es por completo contraria a la jactancia y altanería derivando tanto por inclinación como por indudable buen gusto por cauces de modestia y sencillez que a la vez que hacer desear su trato, le hacen doblemente atractivo.\n\nAmigos comunes, cuya referencia no es preciso detallar porque sus virtudes corren pareja similitud con las de nuestro admirado artista, me han hablado con extensión por el conocimiento que tienen del personaje de cualidades destacadísimas muy por encima de su magisterio artístico que lo sitúan a un nivel y categoría humanas tan francamente envidiables como incomparables por su aportación desinteresada a homenajes y festivales en favor de compañeros a instituciones que habrá que tener presentes para en su día que puede ser cualquiera de estos, proponer su nombre para solicitar la Gran Cruz de Beneficiencia que bien se tiene ganada.\n\nCon mi admiración y respeto al concurrir a este merecido homenaje, va el ruego de que acepte mi consideración y amistad que desde estas humildes líneas, le brindo.",
    "title": "Homenaje a un cabal cabalísimo",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "38-38",
    "page_number": 38,
    "word_count": 383,
    "article_char_count_full": 2365,
    "article_char_count_review": 2365,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-38-right-agenda-1987",
    "article_text_for_review": "14 DE FEBRERO\n\nEscucho a Fosforito una hermosísima serie de «soleá apolá». Al término de la comunicación el inevitable sanedrín de los notables enjuicia así o de similar manera la actuación del cantaor de Puente Genil:\n\n—Formalmente, es perfecto, pero...\n\n—Me deja frío, de puro bien que canta.\n\n—Demasiado control, y su pelea con ese tercio agudo de la soleá del Tenazas parecía calculada.\n\nMe abstengo de intervenir ante tanto desatino. Son los mismos comentarios que atribuían al malogrado maestro Antonio Mairena déficit de emoción en sus cantes, suma frialdad. Tremendo error. Ya en casa, releo una esclarecedora reflexión, sobre este particular, de Anselmo González Climent publicada hace justamente treinta años (Cante en Córdoba, Madrid, 1957, pág. 126) y que, pese a su extensión, no me sustraigo a transcribir literalmente: «Fosforito administra una intuición armónica del conjunto expositivo, intuición grave y difícil en materia de cante. Es, empero, de particular sugestión y sentido la lógica arrebatada de sus desarrolloos armónicos. El equilibrio de Fosforito es un equilibrio que enlaza con los desbordes de su sentimiento. Fosforito, en una palabra, se «equilibra» en función de mayores ahondamientos. Una mala interpretación de este enlace hace de Fosforito un cantaor frío, afectadamente dramático.\n\nGrave error de interpretación, pues es más aparencial la locura sin equilibrios (recurso de muchos cantaores gitanos y agitanados), ya que la receta del desorden está a la mano del primer aventurero flamenco. Es indudable que cuando Fosforito está en posesión del cante, va construyendo con viva serenidad la galería que le conduce al subsuelo del júpio, como si buscase un regodeo enmarañado con las raíces últimas, un afán de orígenes, una certeza definitiva. Fosforito conoce ya a fondo las resonancias del grito puro, concepcional. Por lo mismo, lo difícil y lo entrañable es dar cauce de sensatez a las fuerzas más irresistibles de lo jondo. Este es el milagro de Fosforito, su virtud máxima».\n\n18 DE FEBRERO\n\nLa preparación del número monográfico que la Revista CANDIL dedicará a Fosforito me ha permitido conocer la dimensión humana del artista. No es usual tanto respeto y solidaridd hacia sus compañeros artistas, tanta admiración hacia los maestros desaparecidos, tanta comprensión y tolerancia respecto a otros artistas, hoy justamente vilipendia-dos por la afición. Una observación: en todos aquellas reuniones de la Confederación Andaluza de Peñas Flamencas en las que se ha tratado el tema de la seguridad social de los Artistas Flamencos, allí ha estado Fosforito, apoyando cualquier iniciativa de esta natura-leza.\n\nDIA 25\n\nDe entre todas las anécdotas, en mis largos coloquios con Antonio, entresaco una. Más que anécdota, escenificación de un lugar, centro de intersección de hermosos perímetros del jondismo. Década de los cincuenta. Teatro Price. En el camerino de Pepe Pinto y Pastora Pavón. Antonio se templa por soleá. Sobre una mesita todas las estampitas de los santos del mundo. Pepe se santigua ante cada una de las ellas. Pastora hace voz, ¿por La Serneta?, ¿por Alcalá? Y entre tercio y tercio:\n\n—Para cantar bien mi hermano Tomasito.\n\nNo creo en la abstracción del Cante, no creo en la siguiriya sin encarnadura humana. ¿Cómo es posible determinar el grado de objetividad y de dinámica vital que ofrece una solea sin entender la circunstancia concreta del que la siente?\n\nEn tal sentido, me preguntó qué tipo de aspiración humana ocupa la atención del des- garro flamenco.\n\nR. Porras",
    "title": "AGENDA-1987",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "38-38",
    "page_number": 38,
    "word_count": 557,
    "article_char_count_full": 3533,
    "article_char_count_review": 3533,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-03-39-left-medalla-al-erito-en-el-trabajo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMOCION QUE LA ALCALDIA- PRESIDENCIA ELEVA AL AYUNTAMIENTO PLENO DE PUENTE GENIL\n\nEsta Alcaldía, con motivo de cumplirse, el 11 de mayo próximo, el XXX aniversario de haber conquistado el Primer Premio en el Concurso Nacional de Cante Jondo de Córdoba, propone al Ayuntamiento Pleno solicitar del Excmo. Sr. Ministro de Trabajo y Seguridad Social, a través del Iltmo. Sr. Director Provincial del ramo, la concesión a don Antonio Fernández Díaz, «Fosforito», de la Medalla al Mérito en el Trabajo, a tenor de lo dispuesto en el apartado b) del artículo 8.° del Reglamento aprobado por Real Decreto número 711/1982, de 17 de marzo, y en atención a su ejemplar conducta social y en el desempeño de los trabajos propios de su profesión de artista, en su modalidad de Cante Flamenco, a lo largo de 47 años\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"grandes\"]\n\nTrabajo, a tenor de lo dispuesto en el apartado b) del artículo 8.° del Reglamento aprobado por Real Decreto número 711/1982, de 17 de marzo, y en atención a su ejemplar conducta social y en el desempeño de los trabajos propios de su profesión de artista, en su modalidad de Cante Flamenco, a lo largo de 47 años de ejercicio y a su incansable labor a través del tiempo de su vida activa. Una somera exposición biográfica y una semblanza también, a grandes rasgos, de la vida de este trabajador infatigable como investigador y ejecutante del Cante Flamenco, se hace preciso diseñar para acreditar sus grandes merecimientos al galardón que se solicita. Nace Antonio Fernández Díaz, «Fosforito», en Puente Genil (Córdoba), el 3 de agosto de 1932, en el seno de una familia humilde, de ascendencia artística, cuyo entronque hay que buscarlo más allá de la tercera generación, pues su abuelo materno, conocido por «Juanillo el Cantaor», ya lo practicaba en su época juntamente con su hermano, «El Niño de Puente Genil», a quien se atribuye la creación del «garrotín», amén de otros parientes que destacaron como guitarristas. Hace «Fosforito» su primera incursión por el sendero del Cante cuando apenas tiene seis años, y tras este arranque afortunado afirma sus grandes posibilidades a través de una ejecutoria que no se interrumpiría. Pronto evoluciona hacia formas originales que su buen desarrollo gusto artístico capta en su infancia al contacto con los maestros, y de aquí surge más tarde ese peculiar y personal estilo que siempre le caracterizó y que tantos éxitos le ha dado en su constante peregrinaje por los caminos del ancho mundo. Su caminar por tierras españolas y extranjeras con espectáculo propio o como partícipe en festivales o concursos, se inicia en los primeros años de su vida, y desde esa plataforma surgiría más adelante la gran figura que hoy representa Antonio Fernández Díaz, «Fosforito\n\n[ENDING CONTEXT]\n\ndesde 10 Ptas.\n\nIX SEMANA CULTURAL FLAMENCA\n\nHOMENAJE A\n\nANTONIO FERNANDEZ DIAZ\n\n“FOSFORITO”\n\nORGANIZA: РЕЙА FLAMENCA DE CORDOBA\n\n6° FESTIVAL FLAMENCO\n\ncantan: fosforito, bailan: mario cal social “el meneses, lebri- maya y conchi ca- corral” km 10 ca- jano, terremoto lero. tocan: paco rretera palma del de jerez, luis de cepero, enrique rio. hora: 10,30 córdoba, curro de de melchor, pe- noche. servicio utrera, turronero, dro peña y meren- autocares, con juanito villar, chi- gue de córdoba. salida frente ho- quetete y el pele. presenta: agustin tel los gallos y re- gómez. lugar: lo- greso.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Medalla al mérito en el trabajo",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "39-44",
    "page_number": 39,
    "word_count": 1451,
    "article_char_count_full": 8831,
    "article_char_count_review": 3536,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "grandes"
      }
    ]
  },
  {
    "article_id": "1987-03-45-left-poemas-dedicados-a-perfecto",
    "article_text_for_review": "Y las olas de las playas, se convirtieron anoche en cuerdas de la guitarra.\n\nEn guitarra se ha trocado la caracola del mar, y al conjuro de tu cante en el tablao de las nubes quiere la luna bailar.\n\n¡Dale al cante libertad!\n\n¡Abre las puertas que tiene la cárcel de tu garganta!\n\nAlejandro Cintas Sarmiento\n\nSi aunque tú lo dejes libre él no se querrá marchar.\n\n¡Qué despacio viene el aire! No tiene prisa en llegar.\n\nEstuvo toda la noche escuchándote cantar, y le da lo mismo al aire llegar pronto, o no llegar.\n\n¡Pues no está contento el aire con tu voz de pedernal!\n\nCuando tú cantas, huele mejor el vino de las\n\ncopas. Se hace más ortodoxo el difícil compás, y hasta la guitarra suena diferente, porque son sus cuerdas las olas del mar.\n\n¡Poetas! ¡Buscad ideas! ¡Toreros! ¡A torear! Toro, pluma y cante grande, son de la misma hermandad.\n\n¿Dónde están las bailaoras? ¡Venid todas a bailar, porque ya tiene la copla su Torre y su Catedral!\n\nA «Fosforito»\n\nEn lo oscuro, una voz\n\nAlfonso Canales\n\nColmado el aire ya de vino y humo, se hace sobre las cuerdas un silencio que anuncia el gran desgarro del que sabe lo hondo de su llaga. Salta el frío de los espejos con memoria viva de amanecidas fiestas. Se enciende una bengala que transfigura rostros y alumbra el esqueleto de las sombras.\n\nTodo lo cambia el grito que se pliega a las largas arrugas de la noche. El tiempo ya no fluye: se acomoda al latir de una mano en la madera llena de negro aire, o con un vuelco es borbotón de instantes apresados. Dueña es la voz del ámbito: dirige el respirar de sótanos dormidos y de estatuas de bronce con un río de hormigas en la espalda De dolor o de amor, se nos desvelan los íntimos calambres.\n\nCANDIL",
    "title": "Poemas dedicados a Perfecto",
    "periodical": "candil",
    "issue_id": "1987-03",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "45-45",
    "page_number": 45,
    "word_count": 314,
    "article_char_count_full": 1700,
    "article_char_count_review": 1700,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
