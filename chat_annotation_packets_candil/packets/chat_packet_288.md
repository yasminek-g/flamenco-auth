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
    "article_id": "1994-03-16-left-enrique-jim-nez-ram-rez-enrique-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Valera Espinosa\n\ndo. Su toque está plasmado de las más bellas falsetas, a la vez que es perfecto encauzador del cantaor por los caminos del duende. Como la mayoría de la nueva y virtuosa generación de guitarristas flamencos, Enrique alterna su acompañamiento con los conciertos como solista, en los cuales suele manifestar su arte como lo siente. Siempre con el recuerdo presente de su padre, «Melchor de Marchena», Enrique se deja llevar por el cálido ambiente de sinceridad que se crea entre pregunta y pregunta.\n\n-¿Cómo son tus comienzos flamencos?\n\n—Mi afición se desarrolla por la escucha que siempre he efectuado de mi padre, aunque en un principio sin mostrar mucha curiosidad. Mi interés se despierta una vez que estando mi padre en Madrid, trabajando en «Los Canasteros», me llama y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"tablao\"]\n\nr por el cálido ambiente de sinceridad que se crea entre pregunta y pregunta. -¿Cómo son tus comienzos flamencos? —Mi afición se desarrolla por la escucha que siempre he efectuado de mi padre, aunque en un principio sin mostrar mucha curiosidad. Mi interés se despierta una vez que estando mi padre en Madrid, trabajando en «Los Canasteros», me llama y conozco todo el mundo del espectáculo y de los artistas, coincidiendo, a la vez, con que en el tablao había un guitarrista joven, es por aquí que mi afición se hace más intensa, pues ya tocaba algo. Creo que, en el fondo, la guitarra me venía de familia y fue por lo que, tras apreciar lo que hacían los artistas, de pronto se desencadenó dentro de mí el afán por la guitarra y el flamenco. Había en el cuadro un guitarrista a quien llamaban «El Nani» y, por orden de mi padre, éste fue el que comenzó a enseñarme el toque y, aunque no sabía casi nada, él se dio cuenta de que yo iba quedándome con cierta facilidad con las cosas que me decía. Este hombre cogió a mi padre y le dijo: «Melchor, su hijo puede ser un buen guitarrista porque lo que le enseño en seguida lo coge». —¿Por qué te puso tu padre un maestro que no fue- ra él? —Creo que porque mi padre era un genio de la guitarra; no era un guitarrista para enseñar y de ahí que haya poca gente que toca por mi padre. El tenía un toque tan personal que particularmente pienso que sus cosas en otro guitarrista no suenan. Y volviendo a lo anterior, además del guitarrista flamenco, también me puso un guitarrista clásico, intuyendo que el mundo de la guitarra iba a evolucionar, y que en un futuro se iba\n\n[ENDING CONTEXT]\n\nfrío, ni técnico, ni na; es verdad que parezco frío porque intento concentrarme en el escenario, pero por dentro estaba que reventaba de los nervios, y esa concentración me llevaba a tocar como lo sentía y no de cara a la galería.\n\nRecuerdo que después de estar tocando durante un año en Japón, más de siete horas diarias, mi toque había cambiado bastante. Cuando al volver me escuchaba mi padre, éste me dijo una frase que nunca se me olvidará: «Enrique, te va a costar mucho trabajo llegar, pero cuando llegues no va haber en el mundo quien te pare, si sigues haciendo las cosas como las sientes».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Enrique Jiménez Ramírez«Enrique de Melchor»Rafael",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "16-19",
    "page_number": 16,
    "word_count": 4203,
    "article_char_count_full": 23182,
    "article_char_count_review": 3242,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "tablao"
      }
    ]
  },
  {
    "article_id": "1994-03-19-right-flamenca",
    "article_text_for_review": "R econforta comprobar la sensibilidad de los responsables del departamento de ediciones musicales de Radiotelevision Española hacia el arte flamenco. Cierto que no es el primer trabajo que en la Casa de la Radio se desarrolla, pues los anteriores compactos de «La Puerta Ronda» de José Menese, «Pessoa Flamenco» del propio Vicente Soto, o la selección efectuada en el compacto «El Angel» de los diversos rodajes en directo que a diferentes clanes flamencos se efectuaron en su día, son viva muestra del interés de dichos responsables por este arte. Además, por la periodicidad con que se están editando estos trabajos, podemos llegar a la conclusión de que a partir de ahora se comienza a reconocer lo justo que es la puesta en marcha de una colección flamenca por parte de una entidad pública, contrastando así con la indiferencia y el monetarismo de las casas grabadoras.\n\nEn este «Típtico Flamenco» escuchamos a un Vicente Soto «Sordeira» maduro como cantaor, sabiendo cuál es el cometido que personalmente ha de efectuar y patentizando la singularidad artística de su casta cantaora. Aunque en las diferentes grabaciones que en el mismo están contenidas, el cantaor evoca a señeras figuras gaditanas, nunca pierde el eco de los «Sordeira». Rememora a «La Perla», «El Mellizo», «Macandé»... Sí, se acuerda de la creatividad de los gitanos,\n\npero acrisolando dichos personalismos por las entonaciones de su familia. O mejor, por las de Vicente Soto. Pienso que es la adecuada recreación de lo gaditano por Jerez.\n\nQuizá se haya quedado corto Vicente en la inclusión de otros palos, pues pienso que la oportunidad estaba servida y el equipo de artistas predispuesto a seguir creando. Cierto que abundan los instrumentos que hasta no hace mucho poco han tenido que ver con el flamenco, mas Vicente está plenamente convencido de la vigencia y futuro de estos acompañamientos. Y la verdad es que suena bonito, ¿que se nos erice el vello?, ese es otro cantar. Me cuesta trabajo acostumbrarme a escuchar el cambio de la bella sonoridad de una guitarra por el cansino y rajado sonido de un cajón. Pero el artista es eso, el artista.\n\nBuen compás el efectuado en las bulerías y con claras resonancias de «La Perla». Adecuado tratamiento el de las soleares de Alcalá y muy flamenca la de Cádiz. Ritmo en las cantiñas y preponderancia de ecos personales. Cierto apresuramiento inicial en la malagueña del Mellizo, matizada de entonación después y clara apetencia de haberle escuchado alguna más. Vanguardista y moderna la línea de las colombianas, quizás algo desenfadada. Bello recuerdo a media voz de «Macandé» por fandangos y pleno de compás en las bulerías finales, a pesar del modernismo.",
    "title": "flamenca",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 442,
    "article_char_count_full": 2685,
    "article_char_count_review": 2685,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-03-20-left-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "Cómo se toca, con qué se toca. El alma y el instrumento. Nos sentimos orgullosos de poder dedicar hoy a la guitarra, compañera inseparable de nuestro arte flamenco, la integridad de nuestros comentarios bibliográficos. Estos dos libros se complementan para el buen aficionado y, sobre todo, para el profesional de la guitarra. Si Jerónimo Peña nos describe los orígenes del instrumento, y, sobre todo, el cuidadoso proceso de fabricación del mismo, descendiendo con mimo a detalles de temperatura, humedad y un sinfín de inapreciables —para el profano— condiciones necesarias para el resultado final, Andrés Batista arranca donde el de Marmolejo termina: ya está todo a punto, la madera cortada, curada y armada según los cánones del maestro guitarrero; va a servir ahora para levantar sensibilidades. Si en un momento determinado de su trayectoria artística, Batista ha acompañado, y seguirá haciéndolo, espléndidamente a distintos intérpretes de nuestro arte, que no han escatimado elogios a su genio, ahora nos entrega su corazón encuadernado en forma de dos conciertos para guitarra flamenca, los titulados «Paisajes» y «Trilogía». Sólo falta ya la encarnación del genio, capaz de despertar, como en\n\nCon voz propia. La guitarra Andrés Batista: Guitarra flamenca. «Paisajes y trilogía». Editorial Arambol, Madrid, 1993. Prólogo de Enrique García Asensio Jerónimo Peña Peña Fernández: «El arte de un guitarrero español». Soproargra, Jaén, 1993.\n\nla lira de la famosa Rima becque- riana, «la mano de nieve» que su- piera arrancar los mil y un sonidos que en su vientre de madre fecun- da aguardan dormidos, es decir, un guitarrista. En el comentario del próximo libro nos acercaremos a la biografía de uno de los más fa- mosos. «Paco de Lucía y familia» El plan maestro, de Donn E. Pohren Sociedad de Estudios Españoles. Madrid, 1992\n\nEn estos comentarios destinados a analizar aspectos bibliográficos relacionados con la guitarra, no podía faltar la estela biográfica que sobre la saga de Los Lucía ha realizado Pohren. Analiza aquí lo que él denomina el «Plan» del patriarca, Antonio Sánchez Pecino, padre de Paco y sus hermanos, para formar como artistas flamencos a sus hijos e impedir que se prostituyeran en los meandros indignos por los que deambulaba hasta hace poco la vida de los profesionales de este arte. Para ello les inculcó el verdadero sentido de dignidad que un artista debe de tener, fomentó su autoestima y, sobre to\n\ndo, un amor incomensurable al arte al que se deben.\n\nEl libro de Pohren desgrana para ello un rosario de biografías de todos y cada uno de los miembros de la familia, incidiendo en cómo, a partir de ese plan rector del padre, cada cual fue asumiendo su papel en el mundo flamenco, formándose una de las dinastías más compactas de la historia jonda, que, si en un primer momento, fue dirigida y planificada por el mencionado Sánchez Pecino, más tarde, y a causa de su enorme talento y genialidad guitarrística, vendría a caer bajo la dirección del inigualable Paco de Lucía.\n\nEspecialmente conmovedores son algunos testimonios en los que se viene a incidir en el grado de desprecio que muchos cantaores han manifestado, a lo largo de la historia, hacia la figura del guitarrista, haciéndole responsable de sus fallos y errores en el escenario, hecho que, en la familia Sánchez, nunca fue tolerado. El libro incide en los orígenes artísticos de todos los miembros de la familia, pero, en sus tres cuartas partes, y como era de esperar, se centra en la figura de Paco, desde sus primeros balbuceos con el instrumento, hasta la época actual de los grandes éxitos, señalando, a la par que su genio creador, su carácter íntimo reconcentrado, tímido, inseguro a veces, lo que da como resultado un perfil humano de gran interés, aunque sin el tufo a incienso inecesario que a veces acompaña a este tipo de trabajos.\n\nEl complemento de la reproduc- ción del archivo fotográfico de la familia Sánchez completa el atrac- tivo de este texto, que no dudamos en recomendar a todo buen aficionado. Rosario López",
    "title": "Aunque no quepa en el papel...José Luis",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 666,
    "article_char_count_full": 4036,
    "article_char_count_review": 4036,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-03-21-left-quejas-por-el-estado-del-centro-",
    "article_text_for_review": "Antonio Núñez\n\nEl portavoz del Gobierno Municipal del Ayuntamiento de Jerez, Carlos Manuel López Ramos, manifestó su protesta por los hechos acaecidos en el Centro Andaluz de Flamenco.\n\nSegún López Ramos, «El Centro Andaluz de Flamenco no se puede convertir en una institución puramente arqueológica y pasiva, o en un centro exclusivo de documentación donde no pueden practicar y ensayar los artistas jerezanos».\n\nEl centro está obligado a desarrollar funciones de promoción y a ayudar activamente a artistas modernos, ya que el flamenco es algo vivo y no debe ser tratado como una realidad muerta desde una perspectiva exclusivamente historicista de acumulación de datos.\n\nDiario de Jerez 20 de Abril'94\n\nEl diputado por Cádiz y portavoz adjunto del Grupo Parlamentario Mixto, José Guerrero, ha denunciado ante el consejero de Cultura y Medio Ambiente de la Junta de Andalucía, que el Centro Andaluz Flamenco «sufre el abandono por parte de la Consejería de Cultura y Medio Ambiente de la Junta de Andalucía —de la cual depende—, al no existir una financiación adecuada y una programación de actividades, que hagan que el Centro Andaluz de Flamenco pueda ejercer de dinamizador del flamenco en Andalucía como elemento cultural fundamental de las raíces del pueblo andaluz».\n\nDiario de Jerez 21 de Abril'94\n\nEl grupo municipal del Partido Andaluz de Progreso (PAP) ha manifestado su satisfacción por la decisión del grupo socialista de pedir responsabilidades por las obras realizadas por el director del Centro Andaluz de Flamenco, Joaquín Carreras, en el palacio de Pemartín, sin autorización de la Consejería de Cultura. El Centro Andaluz de Flamenco carece de directrices serias y objetivos claros de lo que debe ser la gestión del mismo.\n\nLa existencia del Centro Andaluz de Flamenco «es puramente vegetativa, carente de actividad, sin proyección en la sociedad y, lo más importante, sin promocionar el arte flamenco jerezano y andaluz».\n\nDiario de Jerez 22 de Abril'94\n\nEl guitarrista Manuel Morao y un grupo de jóvenes artistas flamencos han denunciado el cierre y «auténtico destrozo» de una de las dos aulas de ensayo con las que cuenta el Centro Andaluz de Flamenco. Morao ha calificado este hecho como «una auténtica barbaridad» y dice que su conocimiento del problema se produjo casualmente. «Llegué allí y me encontré el aula totalmente destrozada: el espejo quitado a martillazos y el entarimado hecho trozos. Entonces le pedí explicaciones al director, Joaquín Carreras, y me dijo que éste era un lugar de documentación, no para que ensayen los flamencos».\n\nDiario de Jerez 23 de Abril'94\n\nUn grupo de jóvenes artistas flamencos, entre los que se encuentran los bailadores José de los Reyes y María Bermúdez, y la cantaora Inmaculada Ortega, se enfrentaron también con Carreras, y han comenzado a recoger firmas para tratar de impedir el cierre de las aulas.\n\nSegún han manifestado, «hace ya dos semanas que, en lugar de ensayar tres o cuatro horas por semana, como era lo habitual, sólo podíamos tener una y, con mucha suerte, dos, porque una de las dos aulas estaba cerrada. Siempre nos ponían excusas, pero esta mañana ya nos hemos enterado de que van a poner oficinas, y el director rién-dose, nos contestó que, si queremos ensayar, nos vayamos a Sevilla.\n\nDiario de Jerez 25 de Abril'94\n\nSegún tu criterio, ¿cómo es la labor que el Centro Andaluz de Flamenco —antes Fundación— viene realizando? Y «Romerito» contesta: Con esta institución no guardo ninguna relación; tampoco se preocupa de los cantaores de Jerez ni viene haciendo nada importante por el flamenco. Nunca podrá alcanzar el prestigio de la Cátedra.\n\nDiario de Jerez 27 de Abril'94 ALREO de la FIESTA GITANA",
    "title": "Quejas por el estado del Centro Andaluz de Flamenco/Jerez",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 599,
    "article_char_count_full": 3690,
    "article_char_count_review": 3690,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-03-21-right-alre-de-la-fiesta-gitana",
    "article_text_for_review": "Dibujos de Miguel Alcalá del libro «Le Flamenco et les gitans», Editorial Filipacchi, París, Francia, reproducidos bajo licencia del autor.\n\n13 Textos de Manuel Martin Martin",
    "title": "Alreó de la fiesta gitana",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "21-23",
    "page_number": 21,
    "word_count": 26,
    "article_char_count_full": 174,
    "article_char_count_review": 174,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
