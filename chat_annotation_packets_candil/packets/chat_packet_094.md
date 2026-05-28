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
    "article_id": "1984-05-16-right-la-filosof-a-del-cante-de-luis-c",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA tierra imprime carácter, modela, inspira, motiva a los hombres y a los pueblos. La tierra conversa, cuestiona y desafía a los hombres. Los hombres viven y dan vida a la tierra. Los pueblos aparecen en el destino cuando el entendimiento entre el hombre y la tierra es capaz de componer sobre la filosofía de la humanidad. Y tú, Andalucía, mi raíz, dime cuál es tu orgullo, la tierra o el hombre.\n\nPor: Joaquín Herrera Carranza\n\nAndalucía vive por la lluvia y por el sol, por la sequedad y el solano, por la luz y las estrellas de miles de noches románticas desde tartesos hasta nuestro hoy. Empero, Andalucía está viva porque un puñado de hombres sufren para la vida y para la muerte.\n\nLuis Caballero. Luis Caballero Polo. Aljarafe sevillano, polvo y tinieblas de minas, olor a mosto otoñal,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"según\"]\n\naluz. L. Cernuda. —Conocer la filosofía de un pueblo presupone tanto como conocer al mismo pueblo en cuestión. Pero... ¿quién puede asegurar que conoce al pueblo andaluz? —Yo me atrevería a pensar que nuestra filosofía oscila por entre la resignación, el fatalismo y la esperanza; por entre el «mañana Dios dirá, sea lo que Dios quiera, estaría escrito, Dios aprieta pero no ahoga, y siempre que ha llovío ha escampao», que es como aceptar la vida según viene, mientras sentados esperamos soñando lo que no viene. Pero no olvidemos que somos una amalgama de pueblos y que aunque el tiempo y la tierra terminen definiéndonos, en Andalucía, como decía «El Guerra», «hay gente pa to». Es muy importante tener esto en cuenta. Luis Caballero respira a Andalucía. Yo me atrevería a decir que en cada quejío hondo y ondulado, como la brisa marina de las playas atlánticas, de este filósofo del cante se destina una nueva excitación por la tierra que le vio nacer. De noble le viene tutearse con Andalucía, su esencia, y por eso se atreve a nominar un libro ilustrado de su pluma ¿Somos o no somos andaluces?. Dime, Luis, por qué ¿somos o no somos andaluces? —La recopilación de artículos míos sobre flamenco que en forma de libro titulé «¿Somos o no somos andaluces?» interroga a la insistencia con que los tratadistas de este tema recurren a lo extraandaluz para encontrar lo andaluz. Necesariamente hemos de pensar y ahondar mucho en los orígenes, pero también, y mucho, en la tierra donde germinaron esos orígenes, orígenes ya largamente superados, reelaborados, totalmente absorbidos y metamorfoseados por la naturaleza, carácter e idiosincrasia de nuestra tierra-pueblo. —Lo judío, árabe, gitano, indúe, etc., aflora en los estudios con una independencia que ofende la causa central del fenómeno que en resumen es la propia Andalucía y lo andaluz. Sobre todo y ante todo la apología del gitano y lo gitano llega a superar cotas que anulan la base andaluza. El que escribe de flamenco debería saber cómo son los personajes que hacen posible el Flamenco, o sea: que mitos, egolatría, fanatismo, racismo, ignorancia, indiferencia, etc., puede encontrarse tanto en el flamenco-artista como en el flamenco-aficionado. Transcribir lo que estos dicen sin analizarlo cuidadosamente siempre supondrá un riesgo frente a la verdad aproximada. —Me parece maravilloso, por espiritual y artísticamente constructivo, que nuestros gitanos, los que se quedaron en Andalucía y se hicieron andaluces, hayan con el tiempo, llegado a cantar más emotiva y profundamente que el propio andaluz, pero en la inteligencia de que lo que cantan es andaluz, recogido y recreado en Andalucía por obra y gracia de la tierra que los ha hecho andaluces. —¡Ah, Andalucía! ¿Cuántos hombres han sembrado en la tierra? ¿Cuántos hombres han universalizado la tierra? Hércules, Adriano, Trajano, Sa Isidoro, Averroes, Velázquez, Murillo, Falla, Federico, Picasso... Tartesos, griegos, romanos, visigodos, mozárabes, árabes, judíos, indúes, gitanos. Es suficiente. ¿Cuántas culturas hay en Andalucía? —¿Cultura o culturas andaluzas? Pues... ¿qué puedo decir a este respecto yo desde mi incultura? —Actualmente —no sé si con más o menos razón—\n\n[ENDING CONTEXT]\n\ncon el destino. ¿Cómo es el andaluz frente a los problemas sociales?\n\n—El andaluz, o el pueblo andaluz, creo que tiene razones suficientes como para denotar cansancio frente a sus problemas sociales. Como tantos otros pueblos está cansado; cansado de esperar y también de luchar a su modo; de manera esporádica e individual. Ese cansancio tal vez no significa renuncia, puede que sea conciencia de un viejo convencimiento: la imposibilidad de la razón frente a la fuerza de la sinrazón, la injusticia y la opresión de los de siempre.\n\nTejidos nuevos para tiempos nuevos\n\nJ A E N\n\nCorrea Weglison, 9\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«La filosofía del cante de Luis Caballero» AUNQUE NO QUEPA EN EL",
    "periodical": "candil",
    "issue_id": "1984-05",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "16-19",
    "page_number": 16,
    "word_count": 3823,
    "article_char_count_full": 23034,
    "article_char_count_review": 4819,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "según"
      }
    ]
  },
  {
    "article_id": "1984-05-16-right-la-filosof-a-del-cante-de-luis-c",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA tierra imprime carácter, modela, inspira, motiva a los hombres y a los pueblos. La tierra conversa, cuestiona y desafía a los hombres. Los hombres viven y dan vida a la tierra. Los pueblos aparecen en el destino cuando el entendimiento entre el hombre y la tierra es capaz de componer sobre la filosofía de la humanidad. Y tú, Andalucía, mi raíz, dime cuál es tu orgullo, la tierra o el hombre.\n\nPor: Joaquín Herrera Carranza\n\nAndalucía vive por la lluvia y por el sol, por la sequedad y el solano, por la luz y las estrellas de miles de noches románticas desde tartesos hasta nuestro hoy. Empero, Andalucía está viva porque un puñado de hombres sufren para la vida y para la muerte.\n\nLuis Caballero. Luis Caballero Polo. Aljarafe sevillano, polvo y tinieblas de minas, olor a mosto otoñal,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"según\"]\n\naluz. L. Cernuda. —Conocer la filosofía de un pueblo presupone tanto como conocer al mismo pueblo en cuestión. Pero... ¿quién puede asegurar que conoce al pueblo andaluz? —Yo me atrevería a pensar que nuestra filosofía oscila por entre la resignación, el fatalismo y la esperanza; por entre el «mañana Dios dirá, sea lo que Dios quiera, estaría escrito, Dios aprieta pero no ahoga, y siempre que ha llovío ha escampao», que es como aceptar la vida según viene, mientras sentados esperamos soñando lo que no viene. Pero no olvidemos que somos una amalgama de pueblos y que aunque el tiempo y la tierra terminen definiéndonos, en Andalucía, como decía «El Guerra», «hay gente pa to». Es muy importante tener esto en cuenta. Luis Caballero respira a Andalucía. Yo me atrevería a decir que en cada quejío hondo y ondulado, como la brisa marina de las playas atlánticas, de este filósofo del cante se destina una nueva excitación por la tierra que le vio nacer. De noble le viene tutearse con Andalucía, su esencia, y por eso se atreve a nominar un libro ilustrado de su pluma ¿Somos o no somos andaluces?. Dime, Luis, por qué ¿somos o no somos andaluces? —La recopilación de artículos míos sobre flamenco que en forma de libro titulé «¿Somos o no somos andaluces?» interroga a la insistencia con que los tratadistas de este tema recurren a lo extraandaluz para encontrar lo andaluz. Necesariamente hemos de pensar y ahondar mucho en los orígenes, pero también, y mucho, en la tierra donde germinaron esos orígenes, orígenes ya largamente superados, reelaborados, totalmente absorbidos y metamorfoseados por la naturaleza, carácter e idiosincrasia de nuestra tierra-pueblo. —Lo judío, árabe, gitano, indúe, etc., aflora en los estudios con una independencia que ofende la causa central del fenómeno que en resumen es la propia Andalucía y lo andaluz. Sobre todo y ante todo la apología del gitano y lo gitano llega a superar cotas que anulan la base andaluza. El que escribe de flamenco debería saber cómo son los personajes que hacen posible el Flamenco, o sea: que mitos, egolatría, fanatismo, racismo, ignorancia, indiferencia, etc., puede encontrarse tanto en el flamenco-artista como en el flamenco-aficionado. Transcribir lo que estos dicen sin analizarlo cuidadosamente siempre supondrá un riesgo frente a la verdad aproximada. —Me parece maravilloso, por espiritual y artísticamente constructivo, que nuestros gitanos, los que se quedaron en Andalucía y se hicieron andaluces, hayan con el tiempo, llegado a cantar más emotiva y profundamente que el propio andaluz, pero en la inteligencia de que lo que cantan es andaluz, recogido y recreado en Andalucía por obra y gracia de la tierra que los ha hecho andaluces. —¡Ah, Andalucía! ¿Cuántos hombres han sembrado en la tierra? ¿Cuántos hombres han universalizado la tierra? Hércules, Adriano, Trajano, Sa Isidoro, Averroes, Velázquez, Murillo, Falla, Federico, Picasso... Tartesos, griegos, romanos, visigodos, mozárabes, árabes, judíos, indúes, gitanos. Es suficiente. ¿Cuántas culturas hay en Andalucía? —¿Cultura o culturas andaluzas? Pues... ¿qué puedo decir a este respecto yo desde mi incultura? —Actualmente —no sé si con más o menos razón—\n\n[ENDING CONTEXT]\n\ncon el destino. ¿Cómo es el andaluz frente a los problemas sociales?\n\n—El andaluz, o el pueblo andaluz, creo que tiene razones suficientes como para denotar cansancio frente a sus problemas sociales. Como tantos otros pueblos está cansado; cansado de esperar y también de luchar a su modo; de manera esporádica e individual. Ese cansancio tal vez no significa renuncia, puede que sea conciencia de un viejo convencimiento: la imposibilidad de la razón frente a la fuerza de la sinrazón, la injusticia y la opresión de los de siempre.\n\nTejidos nuevos para tiempos nuevos\n\nJ A E N\n\nCorrea Weglison, 9\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "«La filosofía del cante de Luis Caballero» AUNQUE NO QUEPA EN EL",
    "periodical": "candil",
    "issue_id": "1984-05",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "16-19",
    "page_number": 16,
    "word_count": 3823,
    "article_char_count_full": 23034,
    "article_char_count_review": 4819,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "según"
      }
    ]
  },
  {
    "article_id": "1984-05-19-right-pr-ximos-festivales-flamencos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA petición de numerosos aficionados que nos lo han solicitado en sus cartas, relacionamos a continuación los Festivales Flamencos más importantes que se celebrarán durante el mes de agosto. Hay que destacar que de 58 festivales que relaciona la guía editada por la Junta de Andalucía, sólo en 34 de ellos participa, de alguna manera, el baile. Y solamente uno está dedicado íntegramente a este arte; como es el de Almería.\n\nMes de Agosto\n\nDía 2.—En LOS CORRALES (Sevilla). Festival Flamenco con la actuación de: CALIXTO SANCHEZ, NARANJITO DE TRIANA y JUANITO VILLAR, con las guitarras de MANOLO DOMINGUEZ y PEDRO BACAN. Organiza el Excmo. Ayuntamiento.\n\nDía 3.—En BENAHAVIS (Málaga), recital a cargo de: LEBRIJANO, JUANITO VILLAR y PERRO DE PATERNA. Guitarra: ENRIQUE DE MELCHOR. Organiza: Ilmo.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nnco con la actuación de: JOSE MENese, CAMARON DE LA ISLA, EL CABRERO, PANSEQUITO y LA SUSI, con las guitarras de: TOMATITO Y JOSE LUIS POSTIGO. Organiza el Ilmo. Ayuntamiento. Día 4.—En AGUILAR DE LA FRONTERA (Córdoba), otro Festival en el que cantarán: LUIS DE CORDOBA, EL CABRERO, JUANITO VILLAR, JIMENEZ REJANO, RAFAEL ORDOÑEZ y JOAQUIN GARRIDO. Guitaristas: TOMATITO, MERENGUE DE CORDOBA y JOSE LUIS POSTIGO. Al baile Concha Calero. Organiza la Peña Flamenca «Luis de Córdoba». En ELCHE (Alicante). Organizado por la Peña Flamenca de la Ciudad Ilicitana, Festival-Recital a cargo de ANTONIO CHACON, acompañado a la guitarra por: MANOLO FRANCO. Al baile: ROCIO LORETO. XVIII Gazpacho Flamenco de MORON DE LA FRONTERA (Sevilla), organizado por el Excmo. Ayuntamiento. Al Cante: FERNANDA Y BERNARDA DE UTRERA, EL FUNI, RANCAPINO, EL ANDORRANO y PACO TORONJO, con las guitarras de PEDRO PEÑA y MARIO ESCUDERO (concierto). Al baile: El Güito. III Gran Festival Flamenco de la Vega en PINOS PUENTE (Granada). Organizado por el Excmo. Ayuntamiento, en el que cantarán: MANUEL MAIRENA, NARANJITO DE TRIANA, LA SUSI, LUIS EL POLACO, ANTONIO GOMEZ «EL COLORAO», CHOCOLATE DE GRANADA y LA BURRITA, con las guitarras de: JUAN HABICHUELA y MANOLO DOMINGUEZ, con baile de: JUANA LA DEL REVUELO y PACO VALDEPEÑAS. En PEGALAJAR (Jaén). Tradicional Festival Flamenco, organizado por el Excmo. Ayuntamiento, en el que actuarán: CALIXTO SANCHEZ, LEBRIJANO, CURRO MALENA, DIEGO CLAVEL y CARMEN LINARES, con PACO CEPERO y JUAN HABICHUELA a la guitarra y al baile PEPA MONTES. Festival Flamenco en POSADAS (Córdoba), donde cantarán: ANTONIO PATROCINIO, GABRIEL MORENO y PANSEQUITO. Guitarra: QUIQUE PAREDES. Organiza la Peña Flamenca Luis de Córdoba. Día 9.—En LA LENTEJUELA (Sevilla) Festival Flamenc\n\n[ENDING CONTEXT]\n\nPAQUERA DE JEREZ, EL FUNI, ORILLO DEL PUERTO y JUAN ORILLO, con las guitarras de: MANOLO DOMINGUEZ, JUAN HABICHUELA y PARRILLA DE JEREZ. Al baile: ANGELITA VARGAS y EL BIENCASAO y JUANITA AMAYA. Organizado por el Excmo. Ayuntamiento.\n\nDía 30.—VI Festival Flamenco. Con el cante de: CAMARON DE LA ISLA, FOSFORITO, CHOCOLATE, JOSE MERCE y JUANA LA DEL REVUELO. A la guitarra: TOMATITO y PEDRO PEÑA. Baile: MANUELA CARRASCO. Organiza el Excmo. Ayuntamiento.\n\nDía 31.—En OSUNA (Sevilla). Cantan: JOSE DE LA TOMASA, JUANA LA DEL REVUELO y MIGUEL FUNI. Guitarra: PEDRO PEÑA. Organiza: Exmo. Ayuntamiento.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Próximos Festivales Flamencos",
    "periodical": "candil",
    "issue_id": "1984-05",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "19-22",
    "page_number": 19,
    "word_count": 1370,
    "article_char_count_full": 8967,
    "article_char_count_review": 3405,
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
    "article_id": "1984-05-22-right-poema",
    "article_text_for_review": "De los cinco continentes ¡es otro! Está sumergida y alegre presa anclada entre algas y aire zarandea un aire sin edad llevando al son ventoso rima fluida, de orilla a marea.\n\nDelicia para el delfín, cielo y agua sueño de gaviota, agua y cielo respiro de campiña, sol y lluvias razón del humano, voz y amores.\n\nUn mundo se arrima sin codicia al ritmo universal en fiesta por Alegrías.\n\nFrancoise Gerardín",
    "title": "Poema",
    "periodical": "candil",
    "issue_id": "1984-05",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 71,
    "article_char_count_full": 403,
    "article_char_count_review": 403,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-05-23-left-quienes-fueron-los-maestros",
    "article_text_for_review": "OMERO el Tito es uno de los legendarios y enigmáticos cantaores nacidos en la «Tacita de Plata». Vio la luz según la tradición oral en el barrio de Santa María, aunque otros aseguran que fue en el de La Viña. Sobrino de Tío José el Granaino, a Romero el Tito se le atribuye un estilo de Caracoles que lleva su nombre y según se dice fue el creador de la Romera, nombre que toma este estilo del de su autor. Nació a mediados del XIX.\n\nAndrade da Silva se refiere al artista de la siguiente forma: «Estilista de instinto rítmico inimitable, tuvo estilo propio inconfundible, haciendo de los cantes con baile creaciones realmente personales. Por sus giros rítmicos, por la firmeza rítmica de su compás, era el cantaor preferido por las bailaoras de tronío, que lo llamaban de todas partes. De la garganta de Romero el Tito, las bulerías, las alegrías, los tangos, surgían ya hechos bailes. En los quie- bros de la voz de el Tito había un duende de la plasticidad y del movimiento. Un duende que daba nervio, vida y fue- go a los gestos de las bailaoras».\n\nSelecciona: Rafael Valera\n\nPor otra parte, Fernando Quiñones dice de él: «...desde el café de Silverio, inundó a Sevilla de cantiñas, se le adjudica un estilo de Caracoles, en Cádiz, algunos de “los más viejos del lugar” me aludieron también, harto vagamente a no se que soleá de Romero el Tito».\n\nY continúa Quiñones: «Fue, además de cantaor, un bailaor de muchos quilates, una tarde vi bailar en Cádiz, de modo fragmentario pero espléndido, lo que me dijeron ser “los Caracoles de Romero el Tito”, dotados de gracia, brio y elegancia, en efecto constitutivos de todo un espectáculo...».\n\n«Tanto por su capacidad creadora como por su fama de guapo y de marchoso y, sobre todo, por la alegre imagen juvenil que de él ha quedado flotando sutilmente en el ancho mundo del flamenco, la figura de Romero el Tito suscita siempre el recuerdo de su conciudadano y casi coetáneo Paquirri el Guante, si bien no alcanzó la perduración artística de éste, pese a su profesionalidad».\n\nOtros autores refieren que la figura de Romero el Tito es la misma que la de Romero el Artillero, atribuyéndosele al segundo igualmente la creación de la Romera, de ahí que se llegue a la conclusión que las dos denominaciones artísticas sean para la misma persona.\n\nEn cuanto a la Romera, y siempre según la tradición oral, se dice que en Sanlucar de Barrameda encontró Romero el Tito «el Torrijos», una cantiña para Andrade da Silva y un romance gitano para Quiñones, aunque Aurelio Sellés la realizaba por bulerías.\n\nEl día que mataron\n\na Torrijos el valiente\n\ngrandes guerrillas s'armaron\n\ny hasta el cielo se nubló\n\ny los güenos liberales\n\njuraron en aquel día\n\n«Yo también me vengaría\n\npero matarte a ti no».\n\nRomero el Tito la recreó de forma bailable y la llamó con su propio nombre, Romera.\n\nPero estas circunstancias quedan en la profunda nebulosa del cante flamenco, lo mismo que quedan muchos detalles de la vida y muerte de este gaditano, el cual ha dejado constancia oral de su alegría y bien hacer en los cantes de su tierra.\n\nAPERITIVOS SELECTOS\n\nEspecialidad en\n\nPLANCHA\n\nMesones, 18 Teléf. 23 40 46\n\nJ A E N",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1984-05",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 553,
    "article_char_count_full": 3150,
    "article_char_count_review": 3150,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
