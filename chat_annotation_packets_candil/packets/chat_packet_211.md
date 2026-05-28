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
    "article_id": "1990-03-24-left-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPoesía y Cante Flamenco en el Instituto Francés de Sevilla\n\n¿Cuál de las dos nociones de «creación» e «interpretación» es la más adecuada para explicar, desde un punto de vista histórico y estético, lo que es la esencia del incomparable arte jondo? ¿Qué relación existe entre la poética flamenca en andaluz y la poesía culta andaluz?\n\nA continuación de la charla, y tras saborear una copa de fino obsequiada por la casa Osborne, Kiki de Castilblanco, cantaor aficionado de gran autenticidad y sabiduría jonda en la interpretación de los cantes de Málaga y Huelva, ofreció un recital, acompañado por la guitarra del joven y muy prometedor José Luis Escot.\n\nLa emisora estatal francesa France Musique grabó el recital flamenco, y lo emitió el Domingo de Resurrección en el país vecino.\n\nEste acto\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\niki de Castilblanco, cantaor aficionado de gran autenticidad y sabiduría jonda en la interpretación de los cantes de Málaga y Huelva, ofreció un recital, acompañado por la guitarra del joven y muy prometedor José Luis Escot. La emisora estatal francesa France Musique grabó el recital flamenco, y lo emitió el Domingo de Resurrección en el país vecino. Este acto inauguró un ciclo de actividades dedicadas a la poesía francesa y analuza que tendrá lugar cada quince días en el Instituto Francés de Sevilla. A estas preguntas, y a muchas más de semejante interés, intentó contestar Jean Paul Tarby, el día 15 de marzo, con motivo de la conferencia que pronunció en el Instituto Francés de Sevilla. Este joven aficionado francés, afincado en Sevilla, autor de varios artículos y de un ensayo de próxima publicación sobre la cultura popular gitano-andaluza, supo destacar, en su conferencia, las principales características que hacen del cante flamenco una de las manifestaciones más originales del patrimonio poético oral actual. Bruno Merle XIX Volaera Flamenca de Loja Concurso de Cante Jondo 1. Podrán participar cuantos cantaores deseen sin limitación de edad ni sexo, excepto ganadores de las cinco últimas ediciones. 4. La no presentación a la selección previa de los concursantes inscritos se en- 2. Las inscripciones habrán de efectuarse antes del 15 de junio en el domicilio social de la Peña Flamenca Alcazaba, Cerrillo de los Frailes, 1, Loja (Granada), bien personalmente o por correo, especificando en este caso: nombre y apellidos del concursante, domicilio, edad, nombre artístico, s\n\n[ENDING CONTEXT]\n\nguitarrista gitano desaparecido se reflejó en la composición de piezas musicales de gran belleza.\n\nSabicas creó escuela y de él bebieron todos los grandes maestros de hoy, que han protagonizado una segunda revolución en la guitarra, llevando este instrumento a las más altas cotas de aceptación y popularidad. Sin caer en hipérboles, puede afirmarse que el actual esplendor de la guitarra flamenca, con la admirable evolución que le ha precedido, arranca de Sabicas, y por ello ha sido calificado, no sin razón, como el compositor más sabio y más importante de la historia de la guitarra flamenca.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario Flamenco",
    "periodical": "candil",
    "issue_id": "1990-03",
    "year": 1990,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 2688,
    "article_char_count_full": 16939,
    "article_char_count_review": 3218,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  },
  {
    "article_id": "1990-05-3-right-editorial",
    "article_text_for_review": "En cualquiera de los casos y con independencia de cualquier otro análisis historiográfico, resulta indiscutido el lugar preeminente que lo jondo ocupa en el elenco de bienes culturales que han de ser tutelados. Y al hilo de esta evidencia, las elecciones sanjuaneras al Parlamento andaluz, merecen nuestra atención; también nuestro desasosiego. El equipo de Gobierno que acaba de cumplir su mandato, en el área cultural, ha mostrado respeto al flamenco, notable sensibilidad, exquisita dedicación por parte de alguno de sus responsables, por desgracia, ya desaparecido y logros concretos que sólo vo-\n\nEditorial: ELECCIONES Y FLAMENCO\n\nluntades cicateras pueden ignorar. La constitución de la Fundación Andaluzía de Flamenco, la implantación de infraestructuras organizativas, Confederación Andaluzía de Peñas Flamencas, el otorgamiento de subvenciones sistemáticas y selectivas, Congresos de Actividades Flamencas, Revistas de Flamenco, justifican un juicio positivo de la política cultural del extinto equipo de Gobierno, aparte singulares y aislados despropósitos que también los ha habido.\n\nLas elecciones de junio al Parlamento Andaluz han determinado el que, sin que sufra modificaciones el color de la Administración autonómica gobernante, nuevos hombres, nuevos líderes institucionales accedan al poder. Desconocemos, salvo el caso de la Presidencia del Gobierno, quiénes serán los responsables del Departamento de Cultura, qué grado de motivación y sensibilidad tendrán respecto al flamenco, y si, al menos, se mantendrán las iniciativas ya emprendidas. De ahí nuestro desasosiego que se atempera con la convicción de que un paso atrás en esa política cultural es prácticamente impensable y constituiría tremendo error histórico. Antes al contrario, preferimos alimentar la esperanza de que no sólo se mantendrán los mecanismos de profundización ya implantados, sino que se incentivarán y desarrollarán nuevas formas de tutelar el jondo y mágico legado recibido.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1990-05",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 283,
    "article_char_count_full": 1971,
    "article_char_count_review": 1971,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-05-4-left-la-lealtad-y-la-amistad-en-paco-",
    "article_text_for_review": "Si algo enaltece la condición y la vida de los hombres, es a mi entender la práctica de estos dos sentimientos que tan noblemente resaltaron sobre las muchas virtudes que practicó en vida nuestro querido e inolvidable amigo y de las que tanto yo, al menos, aprendí en el curso de los largos años que duró nuestra inquebrantable amistad.\n\nMuchas veces y con gran dolor y sentimiento por mi parte, he tenido que asistir y soportar críticas que se hacían a nuestro amigo del todo injustificables, tratando de alinearle con intereses que él detestaba, ya que nunca quiso del arte flamenco nada que estuviera reñido con su generosa aportación en todos los aspectos haciendo gala de una moral tan rotunda que afianzaba la gran imagen que de él teníamos todos cuantos conocíamos la gran humanidad que irradiaba de su también gran personalidad.\n\nComo todos cuantos dedicamos algún cariño al arte flamenco, naturalmente nosotros también teníamos nuestra pasión artística y, conocedores de ella, tanto él por su acendrado mairenismo que de por siempre le honrará, como yo desde mi pasión marchenista, hablábamos, sin discutir nunca, en la defensa de los que fueron nuestros respectivos ídolos en el cante, sin acritud y rehuyendo siempre de cualquier tipo de revanchismo, precisamente para mantener, sin caer en ninguna trampa, nuestra real y sincera manera de mantener siempre impoluta nuestra gran amistad y, para conocimiento de cuantos le adjudicaron un anti-marchenismo que nunca sintió, quiero hacer público, porque es de justicia, el que con su aportación de ideas, muy beneficiosas por cierto, pudo recaudarse una muy importante cantidad de dinero con la que pudo terminarse el pago del monumento que en su pueblo se erigió a Pepe Marchena, aconsejando la edición del disco que se editó como homenaje a este genial artista del cante flamenco. Así entendimos en nuestra larga relación la conveniencia de mantener nuestra amistad dentro de unos principios de afecto y respeto que perduraron hasta los últimos días de su vida, entre los que recuerdo con auténtica emoción nuestra última conversación, ya en los últimos momentos de lucidez que precedieron a su muerte. Al rendirte este pequeño homenaje, querido Paco, desearía desde aquí contribuir a deshacer las dudas que algunos malintencionados pudieran tener, al hacer públicas sus opiniones sin respeto alguno para lo mucho que fuiste y representaste en defensa del arte al que tanto decimos querer, aunque de manera tan distinta en muchos casos.\n\nDescansa en paz, porque fue mucho lo que entregaste a cambio, posible y únicamente, de algún disgusto y alguna contrariedad de quienes sin justicia te criticaron, y ten la seguridad de que la semilla que dejaste en todos los ámbitos de la actividad, a los que tan generosamente te entregaste, dará sin duda alguna los frutos de una prometedora cosecha, si cuantos aquí quedamos sabemos o queremos sacar provecho de la gran lección que fue tu ejemplar vida.\n\nAntonio Corcobado Arroyo",
    "title": "La lealtad y la amistad en Paco Vallecillo",
    "periodical": "candil",
    "issue_id": "1990-05",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "4-4",
    "page_number": 4,
    "word_count": 484,
    "article_char_count_full": 2980,
    "article_char_count_review": 2980,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-05-4-right-jos-n-ez-de-castro-g-mez",
    "article_text_for_review": "a muerto un hombre importante en este complejo mundo del flamenco. Se le echa mucho de menos. Rara era la vez que en esta revista no apareciera algún trabajo o colaboración con su firma, y a menudo hasta en un solo número, se multiplicaban sus quehaceres tratando diversos temas de interés sobre nuestro cante. Fue un gran conocedor y un profundo estudioso. Gran parte de su vida la dedicó a esta noble y sana tarea de la investigación del arte flamenco y, muy especialmente, dentro de esta gran parcela, al gitano- andaluz. Tenía una gran pasión por el cante gitano; conocía la vida y milagros de todos sus cantaores, desde el más antiguo en la historia del cante, el jerezano Tío Luis, el de la Juliana, allá por los finales del siglo XVIII, hasta los actuales. Recuerdo haber leído, en uno de los números de la revista «Candil», una resumida, pero completa biografía, de todos los cantaores gitanos que han sido figuras en la interpretación del cante.\n\nConoci a Paco Vallecillo, por los años 1962-63, cuando empezamos a cartearnos, a cruzar correspondencias, en relación con determinadas materias de interés flamencológico, hasta que personalmente entablamos amistad, que fue fortaleciéndose a través de trabajos comunes en pro del flamenco, participando conjuntamente en colquios, conferencias, concursos, tertulias, etc., en las que destacaban sus sapiencias y grandes conocimientos sobre estos atractivos temas, convirtiéndose sus intervenciones y exposiciones en verdaderas lecciones magistrales.\n\nSu actividad y gestión en cuantas entidades e instituciones participó, fueron altamente positivas, como, por ejemplo, la Fundación Andaluzía de Flamenco de Jerez,\n\nla también Fundación Cultural Privada «Antonio Mairena», su gran amigo; y al frente, durante años, de la Asesoría de Actividades Flamencas de la Consejería de Cultura de la Junta de Andalucía. A través de estos estamentos, llevó a cabo una labor encomiable. Fue promotor e impulsor de una selecta discografia, que ha tenido un resonante eco entre los aficionados: «Grandeza y dulzura del cante», con Antonio Mairena y Juanito Mojama, bajo el patrocinio de la Fundación jerezana y en colaboración, en cuanto aportación de discos, del gran aficionado Antonio Reina; un LP a base de José Mercé, en un recorrido de los principales cantes, con letras del propio Francisco de la Brecha, seudónimo con que le gustaba firmar; otro dedicado a las saetas, tanto litúrgicas y flamencas, con su especialista Angelita Yruela, también de la Fundación, y uno, muy original, el de «Los cantes de ida y vuelta», que abarca a toda esa gama de cantes importados de hispanoamérica: guajiras, rumbas cubanas, vidalitas argentinas, milongas y colombianas, con motivo de la conmemoración del V Centenario del Descubrimiento de América, bajo auspicios de la Consejería de Cultura.\n\nComo hombre de letras y de buena pluma, fue el inspirador de la revista ceutí «Flamenco», cuyo total contenido era una verdadera fuente de conocimiento y sabiduría flamenca. Remitiéndonos a cuanto he manifestado al principio de este modesto homenaje a un amigo y aficionado que se nos ha ido, he de insistir una vez más, cuánto se siente el vacío que ha dejado, difícil de suplir, ya que Paco fue un hombre puntal en todo lo concerniente al flamenco.\n\nJosé Núñez de Castro Gómez",
    "title": "José Núñez de Castro Gómez",
    "periodical": "candil",
    "issue_id": "1990-05",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "4-4",
    "page_number": 4,
    "word_count": 530,
    "article_char_count_full": 3305,
    "article_char_count_review": 3305,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-05-5-right-se-ora-madre-polluelas",
    "article_text_for_review": "e impongo a usted, señora madre Pollyelas, mi beso en su pechera, entre el hueso del hombro y el inicio de su pecho, justito en la cabecera del corazón, donde empuja la buena sangre que llevamos dentro, si es que el beso se sabe dar justamente en ese sitio.\n\nCuando usted pronunció el «déjalo que se divierta», derribó, al modo tonto de las madres, la barrera entre la luz y la sombra, templó la noche y dejó al señor padre Polluelas en la simple amenaza del verbo, crucificado en la media docena de palabras del «¡qué horas de venir son éstas!».\n\nA partir de ese momento, se pretende nacido un hombre que se reprodujo en todos y murió para nadie.\n\nY del hombre nacido vino la paz, un resumen de calvarios con un resultado de paz, porque, ya nacido, Pepe Polnuelas, inventó la escaramuza, el agua submarina y, en definitiva, la guerra para la vida, para la vida inventó la guerra y no para la muerte, señora madre Polnuelas.\n\nInventó, también para nosotros, su vida vivida detrás de nuestras propias vidas, viniendo de atrás hasta darnos presencia y fe de un mundo contradictorio, de su mundo, claro está, incluido el Tío de la Vaca, de perenne acampada allá por el Cerrete de los Lirios, mandándonos al banquillo de lo sorpresivamente bello y plantándose, sin que nadie lo creyera fuera de juego, ante la portería de nuestra sensibilidad para marcarnos el inesperado, el doblemente hermoso, gol del cojo.\n\nTe agradezco, Pepe Polluelas, el alivio de mi medio siglo de huesos protestones, cuando me hablabas, como a cada uno le hablabas de su pequeña y diminuta cosa, cuando me hablabas de una paloma tan senora que renunció a beber agua de un arroyuelo porque tenía una cola que no se quería mojar.\n\nTe agradezco, por lo que a mí respecta, que tú que me sabías un exiliado semanal de mis palomas señoras, me apartaras de la rutina y, sin más trámites, me enviaras al corral de mi casa torreña, a la pileta grande donde beben los animales de mi casa, al borde del tejado donde mis palomas señoras se dan besitos por toda la garganta.\n\nTodo eso y más y tan barato nos daba su hijo, señora madre Polluelas.\n\nTe recuerdo, amigo Pepe, en la intimidad de mi familia, Carmela y Ana, te recordamos cuando cantabas y sobre el cante ponías el acento circunflejo de tu dedo meríque.\n\nAlfonso Fernández Malo",
    "title": "Señora Madre Polluelas",
    "periodical": "candil",
    "issue_id": "1990-05",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "5-5",
    "page_number": 5,
    "word_count": 409,
    "article_char_count_full": 2295,
    "article_char_count_review": 2295,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
