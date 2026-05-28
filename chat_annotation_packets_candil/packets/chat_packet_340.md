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
    "article_id": "1997-07-26-right-catalu-a-flamenca-una-exposici-n",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nUna exposición, cinco conciertos y Morente\n\nNo debe extrañarle ya a nadie, creemos, que periódicamente, y cada vez con mayor frecuencia, traigamos a estas páginas noticias del flamenco en Cataluña. Afortunadamente, el número de personas interesadas por este arte es mayor y las actividades, actos y programas organizados en torno a él se multiplican. No hay época alguna del año huérfana de ello. Durante el verano el epicentro se sitúa en Barcelona, básicamente gracias al Grec, el festival de verano de la Ciudad Condal. Y es que esta ciudad, en afirmación de su hasta ahora alcalde Pasqual Maragall, \"ocupa un lugar destacado en el panorama del flamenco gracias al trabajo, la profesionalidad y el talento de una generación de creadores e intérpretes que, como los mejores músicos del jazz,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"tradición\"]\n\nuérfana de ello. Durante el verano el epicentro se sitúa en Barcelona, básicamente gracias al Grec, el festival de verano de la Ciudad Condal. Y es que esta ciudad, en afirmación de su hasta ahora alcalde Pasqual Maragall, \"ocupa un lugar destacado en el panorama del flamenco gracias al trabajo, la profesionalidad y el talento de una generación de creadores e intérpretes que, como los mejores músicos del jazz, conocen y respetan profundamente la tradición, al tiempo que la renuevan y enriquecen día a día. Ellos están consiguiendo liberar el flamenco de todos los tópicos para devolverle lo que, en buena ley, le pertenece: su singularidad como género artístico, su capacidad de seducción como forma de expresión y de comunicación, su magia única\". Y en otro párrafo afirma también: \"Porque, aunque no siempre bien re- conocida, la tradición flamenca forma parte, desde el siglo pasado, de la vida y el patrimonio cultural de Barcelona. Una tradición que hoy se mantiene muy viva, más viva que nunca\". La exposición El palacio de la Virreina, en plenas Ramblas barcelonesas ha acogido durante los tres meses de verano la exposición “Flamencs”, un serio in- tento de colocar el flamenco y los flamencos autóctonos en el lugar que le corresponde. En el lugar que le corresponde en la cultura catalana y también en la oferta cultural barcelonesa. Organizada por el Instituto de Cultura de Barcelona, guión de la periodista Isabel Coderque, fotografías de Colita, diseño de Dani Freixa y el asesoramiento histórico de quien escribe, la exposición, con materiales gráficos, sonoros y textuales, ha estado dividida en dos grandes ámbitos complementarios: el histórico y el actual\n\n[ENDING CONTEXT]\n\nde su nacimiento a Macandé. No es escasa tampoco la cantidad, 1.200.000 pesetas, destinada por la organización, FÉCAC, a premios. El concurso presenta algunas novedades en esta edición, desaparecen las menciones especiales a algún cante determinado y los finalistas no podrán repetir los cantes que hubieran hecho en la fases selectiva. La final tendrá lugar el 20 de diciembre en Santa Coloma de Gramanet.\n\nY esas son tan sólo algunas de las actividades más significativas previsas. Muestra suficiente, no obstante, creemos, para poder afirmar que el otoño en Cataluña tiene color flamenco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Cataluña Flamenca Una exposición, cinco conciertos y Morente",
    "periodical": "candil",
    "issue_id": "1997-07",
    "year": 1997,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "26-30",
    "page_number": 26,
    "word_count": 3891,
    "article_char_count_full": 24281,
    "article_char_count_review": 3296,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "tradición"
      }
    ]
  },
  {
    "article_id": "1997-09-3-left-el-centenario-de-la-argentinita",
    "article_text_for_review": "S e ha denunciado con acierto, por parte del crítico flamenco de “El País”, que este año de 1997 se cumple el centenario del nacimiento de Encarnación López Júlvez “La Argentinita” y nadie parece haberse dado cuenta de ello, a pesar de la enorme importancia artística que la bailaora flamenca ha tenido en la historia de este arte.\n\nEs posible que las dudas habidas en la fecha del nacimiento de la eminente artista hayan condicionado a los responsables de cada uno de los posibles estamentos culturales e institucionales implicados en organizar los merecidos homenajes a la figura argentina. Cierto es que una vez conocida la fecha exacta de tan feliz alumbramiento, 25 de marzo de 1897, gracias a las investigaciones efectuadas por Carlos Manso, el cual ha conseguido copia de la partida de nacimiento de Encarnación, la tarea se debía haber puesto en marcha, aún y a pesar del poco tiempo disponible.\n\nLos merecimientos no son pocos, pues desde su debut a la edad de ocho años en el Teatro Circo de San Sebastián, hasta su muerte acaecida en Nueva York en 1945, Encarnación copó los titulares de la prensa y los carteles de los teatros de Europa y\n\nAmérica, mostrando su arte y genialidad con espectáculos u obras como “Las calles de Cádiz”, “Sevillanas del siglo XVIII”, “El Café de Chinitas”, “El tango del escribano” o “El amor brujo”. Por sus compañías desfilaron artistas de la talla de La Macarrona, El Gloria, Fernanda Antúnez, Rafael Ortega, Antonio de Triana, El Gloria, Ignacio Espeleta, La Malena, La Jeroma, Manolo el de Huelva o José Greco, sin olvidarnos de su hermana Pilar López. Fue igualmente la predilecta de los escritores de la Generación del 27, manteniendo una ligación especial con varios de ellos —y muy concretamente con Federico García Lorca—por su relación sentimental con el torero e intelectual Ignacio Sánchez Mejías.\n\nSin embargo, los organizadores del XIV Concurso Nacional de Arte Flamenco de Córdoba, celebrado en 1995, y creo que por barajar los datos sobre su nacimiento que figuran en el Diccionario Flamenco, sí cumplieron con su asumido compromiso de homenajear a la oriunda artista, con la elaboración de un cuadernillo monográfico sobre su figura en el libro del citado Concurso y una serie de actos que realizarían su creatividad artística.\n\nIgualmente creo que el trabajo sonoro efectuado por Carmen Linares casi a finales de 1993, con una magnífica promoción durante el año 1994 y consolidado como uno de los mejores en 1995. Viene igualmente a mostrar el interés que sobre la figura de \"La Argentinita\" habitaba y habita en la mente de la cantaora jiennense. Sus arreglos de aquellas viejas coplas rescatadas por García Lorca y grabadas por Encarnación, creo que han sido muestra de que el recuerdo de la bailaora aún perdura.",
    "title": "El Centenario de “La Argentinita”",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 463,
    "article_char_count_full": 2775,
    "article_char_count_review": 2775,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-09-3-right-antecedentes-y-consecuentes-cord",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEn los textos de flamenco escogidos por Eugenio Cobo de los escritores de la Restauración,ⁱ hacemos buena la frase que Génesis García pone imaginariamente en boca de Federico García Lorca: Que otros pongan los datos que yo pondré las ideas; pero, más aún, en algunos de estos textos creemos ver no solamente los datos, sino muchas ideas y no pocas imágenes poéticas para los nutrientes de su sensibilidad. Por supuesto que en esta recopilación está la versión flamenca de esa Andalucía del llanto y de la pena negra tan lorquiana.\n\nCuando Falla llega a Granada iluminado por la Acústica Nueva de Luis Lucas que nutre la sensibilidad despertada en su infancia por aquella Morilla de la Serranía de Ronda, ya el texto de Rosario de Acuña, $ ^{2} $ posiblemente, ha impactado al poeta, aquél que,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"voz\"]\n\nn su infancia por aquella Morilla de la Serranía de Ronda, ya el texto de Rosario de Acuña, $ ^{2} $ posiblemente, ha impactado al poeta, aquél que, hablando del fandango, dice: En la campiña el baile es vivo, alegre y juguetón, a veces demasiado expresivo; en las poblaciones degenera en grotesco, nunca vi al que nombran algo libre. En la sierra no es parecido a ninguno, es tan original como los que lo bailan... En la naturaleza de la sierra, la voz parece a la testigo eco perdido de una garganta sobria de palabras, ajena de las 1) El flamenco en los escritores de la Restauración (1876-1890), de Eugenio Cobo. Edita: Aquí-Más Multimedia. Cornellà de Llobregat. 2) Rosario de Acuña (Madrid, 1850 - Gijón, 1923). Poetisa defensora de los humildes. 3) Eugenio Cobo, o.c., págs. 31, 32 y 33. bellezas del arte, vibra con toda la energía del genio, se pliega, descien-de rápida o perezosa, aguda o leve, cortada en sus períodos más brillantes por un ¡ay! solitario...³ Es el cante en su estado de pureza e inocencia primitivas que músico y poe- ta proponían para el Concurso del 22 en Granada y que quieren preservar de la contaminación ciudadana y profesional, porque ya la de Acuña dice que en las poblaciones degenera en grotesco. Un poco más adelante continúa Rosario de Acuña:...Madre, grite usted a María que se venga a bailar, que nos vamos a casa el tío Vicente; pocos momentos después algunas parejas se mueven lánguidamente en torno del apagado hogar del cantaor o bajo el oscuro azul del firmamento... En el oscuro azul del firmamento se funden las noche\n\n[ENDING CONTEXT]\n\npopulista que impide la visión completa de otra imagen grandiosa y real. A pesar de que previniera Federico, en su comentario, del peligro que corría su Romancero de ser interpretado con flamenquerías, éste cayó en manos groseras, efectivamente.\n\nUnamuno, en el prólogo de Vida de don Quijote y Sancho, nos invitaba a formar \"la santa cruzada para ir a rescatar el sepulcro de don Quijote del poder de los bachilleres, curas, barberos, duques y canónigos que lo tienen ocupado\". Algo de eso habríamos de intentar también nosotros para rescatar el Romancero Gitano, pero esa sí que es tarea difícil.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Antecedentes y consecuentes cordobeses del lorquismo",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "3-9",
    "page_number": 3,
    "word_count": 6763,
    "article_char_count_full": 39626,
    "article_char_count_review": 3186,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "voz"
      }
    ]
  },
  {
    "article_id": "1997-09-9-right-una-utop-a-est-tica-y-reivindica",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nConcurso de Cante Jondo de Granada, 1922:\n\nDice una sentencia latina: “Errando, errando, tandem deponitur error”. Y es verdad que “andando, yendo de un sitio a otro, al final desaparece el error”. Pues bien, uno de los graves problemas que sigue teniendo el flamenco es el de siempre: su origen. Es cierto, por otra parte, que nos encontramos con un flamenco mejor conocido en sus aspectos históricos, literarios y musicales, gracias al entusiasmo e interés de cuantos se han acercado a él con\n\nla humildad y respeto que merece nuestro cante del que ha dicho Caballero Bonald —cfr. “El Cante Andaluz”, página 3— “que sus numerosas y siempre bellas variantes, la independencia de sus caracteres, el poderío arrasador de su mundo expresivo y melódico, su mismo origen oscuro y dudoso, prestan a esta\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"origen\"]\n\ncercado a él con la humildad y respeto que merece nuestro cante del que ha dicho Caballero Bonald —cfr. “El Cante Andaluz”, página 3— “que sus numerosas y siempre bellas variantes, la independencia de sus caracteres, el poderío arrasador de su mundo expresivo y melódico, su mismo origen oscuro y dudoso, prestan a esta modalidad de la canción una vigorosa personalidad llena de sugestivas significaciones humanas y raciales”. Cuando intentamos el origen del flamenco nos encontramos siempre con dos obstáculos insalvables: orígenes inciertos y la poca definida orientación de sus ciclos de desarrollo. Los tratadistas coinciden en que se trata de un fenómeno de la música popular, cuyo enigma parece coincidir con las más resistentes y contradictorias dificultades, en lo posible, por su procedencia. Sin embargo, lo único cierto es esto: no conocer los auténticos mecanismos so ciales y culturales que nos dieron el nacimiento de un arte donde se fusionaron los más ilustres sedimentos de la música oriental almacenada en Andalucía a todo lo largo de su historia. No tenemos argumentos apodícticos para demostrar la raíz primera de estos cantes que han invadido al mundo entero, lo que significa, para este Comunicante, fuente de riqueza cultural porque, sin ser en sí, existen. Creo no equivocarme, si afirmo que fue esto lo que impulsó a don Manuel de Falla, a Federico García Lorca y a tantos aficionados y artistas granadinos para llevar a cabo, posiblemente, el hecho más importante de la historia flamenca. Porque el Concurso de Cante de Granada/1922 se concibió en torno a la importancia y valores estéticos del Cante, como medio de revalorizar y reivindicar el arte flamenco; un arte que creían mixtificado y en decadencia. Y el grito de alerta salió de labios de uno de los más exquisitos poetas de nuestra literatura: Federico García Lorca, joven de sólo 24 años, el cual sentía profundamente la esencia y realidad de Andalucía. Y lo tuvo que decir un poeta, grito estético y reivindicativo, porque mientras haya poesía —he dicho muchas veces— habrá flamenco, pues éste también es “poesía” en sentido etimológico y semántico. Aquel granadino de Fuente Vaqueros, el día 19 de febrero de 1922 en el Centro Artístico de Granada, dijo: “...Esta noche os habéis congregado en el Salón del Centro Artístico para oír mi humilde, pero sincera palabra, y yo quisiera que ésta fuera luminosa y profunda, para llegar a convenceros de la maravillosa verdad artística que encierra el primitivo canto andaluz, llamado Cante Jondo. El grupo de intelectuales y amigos entusiastas que patrocina la idea del Concurso, no hace más que dar una voz de alerta: ¡Señores, el alma musical del pueblo está en gravísimo peligro. El tesoro artístico de toda una raza va camino del olvido! Puede decirse que cada día que pasa, cae una hoja del admir\n\n[ENDING CONTEXT]\n\nde Falla, como al poeta Federico García Lorca, no era otra que conservar y respetar lo que estaba bien hecho, como seguirias, soleares, Polo, Caña, y rescatar del olvido todo aquello que valía la pena. El Concurso de Cante Jondo de Granada/1922 tuvo —¡cómo no!— sus errores, tal vez por no comprender bien los entresijos del cante, pero este Concurso significó un paso trascendental en la historia flamenca, gracias a sus principales mentores: Falla y Lorca. Por tanto, creo que el Concurso del 22 no fue una utopía estética, sino un acto reivindicativo de los valores permanentes del Arte Flamenco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Concurso de Granada: Una utopía estética y reivindicativa de Falla y Lorca?",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "9-12",
    "page_number": 9,
    "word_count": 4163,
    "article_char_count_full": 25394,
    "article_char_count_review": 4460,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "origen"
      }
    ]
  },
  {
    "article_id": "1997-09-13-right-letras",
    "article_text_for_review": "A comé... er s'ha dó pa comé: y aquí está su mujersita dando la cara por é. Er queré d'esta serrana no es mentira ni es de veras: es un espejismo vano; jielo que, de frío, quema. Tengo anublao er sentido y er corasón jecho cachos dende que t'he conosío. Toito es sabé: nadie lo sabe la primera vé. Y esos güenos momentos qu'hemos pasao: con los tormentos qu'estoy sufriendo los he pagao. Disen y disen disen y es verdá: tú calientas una cama y otro la disfrutará: ¡sí qu'es verdá! De meterm'en aventuras mira tú lo qu'he sacao: la cabesa esclaresía y er corazón trascorca Mira que fatalidá: sufrí las penas ajenas sin poerlas remedía. Se m'han nublao los sentios y er corasón m'echa rayos esde que t'he conosío. La primera vé yo no sabía y ara lo sé.",
    "title": "Letras",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 140,
    "article_char_count_full": 750,
    "article_char_count_review": 750,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
