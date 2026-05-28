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
    "article_id": "1983-01-10-right-dos-genios-revolucionarios-juan-",
    "article_text_for_review": "S I Juan Belmonte en valiente competencia con Joselito implantó normas nuevas del toreo, don Antonio Chacón, intuyendo los momentos artísticos por que atravesaba la afición, creyó y recreó ciertos estilos del cante.\n\nPor Antonio Reyes Marín\n\n«El Pasmo de Triana», tras unos comienzos muy crudos, muy adversos, donde sufrió muchas cogidas, pudo llegar a situarse como gran figura al lado del Gallito.\n\nChacón, que también sufrió agravios, calamidades y vicisitudes, tal como nos lo cuenta Javier Molina, en sus giras por ciertos pueblos andaluces. Cantó en Jerez en el café de la Vera Cruz y no gustó, después llega a Huelva y «Salvaoriyo» le anima mucho, conociendo ya parte de sus buenas facultades.\n\nBelmonte, durante los años anteriores a la muerte de Joselito, ha ido cimentando, aprendiendo, creando lo que más tarde sería el toreo contemporáneo.\n\nEl revolucionó las formas, acortó distancias, limitó los terrenos. Antes solamente, a veces, se pasaba, se templaba las manos y se mandaba muy raramente, ya que casi todo se limitaba al dominio del toro para la suerte de matar.\n\nJoselito fue el joven sabio, magisterio, compendio de sabiduría, predestinado a ser la máxima figura por su herencia de casta, de sangre, de historia. Pero éste no supo o no quiso ver, quizás por orgullo profesional, al Inmen-\n\nso Belmonte, que conjuntando las tres formas básicas de su toreo, o sea, parar, templar y mandar, creó el verdadero arte de torear. Pero pasado algún tiempo, los críticos taurinos cuentan que Joselito, al ver la grandeza, perfección y el dominio de Belmonte, empezó a admitir sus formas e incluso toreó algunas corridas antes de morir en Talavera, con aires belmontinos.\n\nVicente Pastor decía de Belmonte: «Cuando toreaba me asustaba, sobre todo en sus principios, porque le cogían los toros frecuentemente. Yo siempre estaba al cuidado».\n\nComo torero era un revolucionario del toreo. Por eso se dijo: «Hasta Belmonte y desde Belmonte».\n\nGitanillo de Triana, que también llegó a torear con Belmonte, decía de él: «Con Juan ha muerto el toreo».\n\nHasta Juan Belmonte el toreo había sido lucha defensi-\n\nva del hombre con una fiera, a la que había que abatir a fuerza de habilidad y majeza. A partir de Juan, los valores estéticos y artísticos adquieren importancia y tienen más categoría que los conocimientos técnicos y los alardes de valor. Belmonte es un torero valiente, pero está por encima, muy por encima, de los valientes. No, no sabemos encasillar como torero a Belmonte, posiblemente porque su personalidad escapa a toda definición; posiblemente porque, como queda dicho, Belmonte fue mucho más que un torero inconmensurable; porque fue el torero mismo.\n\nChacón después vuelve a Cádiz, conoce al misterioso y enigmático Enrique «El Mellizo», de quien aprende mucho en el cante por malagueñas. Aquí trabajó en el café de verano «El Perejil».\n\nAl poco tiempo fue llamado por «Silverio» a su propio café en Sevilla y allí es donde se consagró como maestro indiscutible del cante.\n\nA partir de entonces Chacón revolucionó el cante, pues dotado de gran intuición, de reconocerse a sí mismo y tras ver inteligentemente la transición del público aficionado y con las circunstancias más oportunas, empezó a crear y recrear estilos. El, con toda su sabiduría, pues por algo bebió ya desde niño en las fuentes puras del cante, oyendo a los maestros de su época, siendo un cantaor largo y reconociendo sus facultades de voz, empezó a crear varios estilos por malagueñas, después diversos cantes de Levante y sus famosos caracoles, cartagenera, mirabrás, media granaína.\n\nAunque también cantó siguiriγas, soleares y otros cantes grandes, donde adquirió su fama, fue en los cantes que él crearía y que eran muy apropiados a sus facultades de voz. Nunca cantó por bulerías.\n\nHacia el 1890 Chacón llegó a Málaga, al café de Chinitas, donde obtuvo resonantes éxitos. Más tarde pasa a Madrid y aquí se establece para siempre. Solamente hace una breve salida a Hispanoamérica y alguna que otra dentro de la península con actuaciones extraordinarias. Tras varios años actuando en «Los Gabrieles», en Madrid, muere el 21 de enero de 1929.\n\nPoco después se inicia el decaimiento del cante con la presencia en los teatros de la ópera flamenca, con la que adquirió gran renombre «Pepe Marchena».\n\nPRIMERA MARCA MUNDIAL DE TELEVISION VIDEO y SONIDO\n\nDistribuidor Oficial:\n\nAvenida Muñoz Grandes, 14 y 16 — J A E N Sucursal en BAILEN: Zarco del Valle, 8",
    "title": "Dos genios revolucionarios",
    "periodical": "candil",
    "issue_id": "1983-01",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 732,
    "article_char_count_full": 4446,
    "article_char_count_review": 4446,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-01-11-right-ellos-los-protagonistas-dicen-ma",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen:\n\n—¿Por qué eso de «El Sordera»?\n\n—Bueno, ésto me viene por mi abuelo. Mi abuelo era el «Sordo La Luz», sobrino carnal de Paco «La Luz»; era también primo hermano de La Serrana, hija de Paco «La Luz», y claro, de ahí me viene el apodo. Yo he heredao de ellos el cante que hago, porque en mi familia ha habido grandes artistas además de los que te he dicho. «El Borrico», «El Morao», el padre de Manuel Moreno... Yo traigo la trascendencia de estos artistas. Luego está la familia de mi mujer que también arrastra con mucha solera cantaora, «El Gloria» era primo hermano de mi suegro; Fernanda y Juana Antúnez, que eran bailaoras. En conclusión, creo que en mi familia se encierra mucha solera de cante flamenco.\n\n—¿Por qué existe siempre más inclinación a interpretar\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nmi suegro; Fernanda y Juana Antúnez, que eran bailaoras. En conclusión, creo que en mi familia se encierra mucha solera de cante flamenco. —¿Por qué existe siempre más inclinación a interpretar en Jerez por parte de los artistas, bulerías que siguiri-yas? —Si yo tuviera que hablar de este tema, tendría que mezclar muchas cosas que están pasando. Uno de los problemas existentes es que hay otra ciudad que se atribuye siempre el nacimiento de los mejores cantaores flamencos, y eso no es así. Como bien dices, en Jerez han existido grandes personalidades artísticas flamencas, como todos sabemos. Los artistas viejos, los viejos ahora, tenían que expresar la auténtica verdad del cante. Siempre que se ha desplazao el cantaor jerezano a cualquier otra ciudad, éste siempre ha ido enseñando, porque en Jerez ha existido auténtica sapiencia en cante. ¡Mira!: un ejemplo muy claro lo tenemos en un artista sevillano de reconocida valía como era Manolo Caracol. El siempre hacía los cantes, con su personalidad, pero como se hacían en Jerez. Moría por Jerez hasta el punto que se casó con una jerezana. Para comprobar lo que yo digo, solamente hay que leer la historia del flamenco. ¿Donde han salido los mejores cantaores? En Jerez y los Puertos; más cantidad y más calidad. Sevilla desde luego ha dao a «Los Pavones», «Los Caganchos», «Los Pelaos», Fernando «El Herrero»... ¿Cuántos ha dao el otro lao? ¿Del Cuervo «pa bajo»? Creo que todos los buenos los ha dao aquel rincón. ¡Ahora! ¿Sabes lo que ha pasao? Que aquel rincón no ha sabio mantener too lo bueno que allí ha salío. Se han muerto y desgraciadamente no han dejao «ná» grabao, pero «toos» están en los libros, lo mejor que ha salío. Todos los artistas teníamos que decir ésto que yo estoy diciendo para la revista CANDIL, pero parece que les da miedo, a mí no me da. Y a las primeras figuras sí que no les da miedo de ir en las ferias a Jerez y, desde luego, lo que han ido ha sío a aprender. —Aunque parezca que somos un poco insistentes ¿Por qué a nivel interpretativo imperó más, entre los artistas jerezanos, la bulería que la siguiriya? —Eso lo da la tierra. Igual que en Sevilla impera más la sevillana, en Jerez son las bulerías. Como se canta, baila y se tocan las palmas por bulerías en Jerez, hay pocos si- tios. Además también se toca la siguiriya. ¿Porque mejor que en Jerez quién ha cantao por siguiriyas? ¿O por soleá? Porque la soleá de Frijones es para relamerse de gusto. Y no ya por estos estilos sin\n\n[ENDING CONTEXT]\n\nque al cantaor. —Para terminar ¿Por qué no nos cuentas alguna anécdota?\n\n—Mira, estábamos en Madrid, tomando café, Pericón de Cádiz, Manolo Vargas y yo. Y me dice Pericón: «Manué ¿si supieras lo que me ha pasao? He ido a comprar lotería en la Administración y tenían un perro enorme que no me dejaba entrar. Ladrando continuamente. Total que me fui y al irme me dice el perro: Pericón, compra que te va a tocar».\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al mérito del trabajo)\n\nRecepción diaria de Mariscos y Pescados\n\nEspecialidad en Asados\n\nRoldán y Marín, 7\n\nJ A E N\n\nTeléfono 22 97 65\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen: Manuel Soto, «El Sordera»",
    "periodical": "candil",
    "issue_id": "1983-01",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "11-13",
    "page_number": 11,
    "word_count": 2644,
    "article_char_count_full": 14555,
    "article_char_count_review": 4101,
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
    "article_id": "1983-01-13-right-aunque-no-quepa-en-el-papel-la-s",
    "article_text_for_review": "E L fenómeno flamenco cuenta ya con una amplia bibliografía. Pero no abundan los estudios integrales de «lo jondo». Una extensa élite de miopes sabios del cante, escudriñadores de estilos, talentos de atribuciones, han contribuido a la difusión de un flamenco encorsetado, tópico y huérfano, desvinculado de sus hermosos precedentes. Y ello, a mi juicio, se ha debido a un importante déficit: El estudio integral de lo jondo. Quienes —o al menos la gran mayoría— se afanan en ostentar un discutible magisterio, han detenido sus vis investigadoras en lo superficial, cuando no en lo puramente anecódotico. Y es que no puede olvidarse que este estudio requiere una concurrencia de disciplinas dispares, un esfuerzo multidireccional para adentrarse en la esencia del flamenco. Esa, y no otra, es la única técnica para acceder a un conocimiento completo —dije antes integral— de lo jondo. Ese es el camino para reconducir —de forma precaria las más de las veces— los escasos datos fidedignos de que podemos valernos.\n\nPor Ramón Porras\n\nEn este sentido, «La Seguidilla Gitana» (1), significa un intento riguroso de retomar el hilo conductor del flamenco. Y conste, como el propio autor declara, que no se trata de un estudio exhaustivo del cante flamenco. Lo más apreóiable de esta obra es su propio planteamiento: El cante como expresión invicerada en la misma Historia de Andalucía, sociología, música, poesía andaluzas, etc. No es lo jondo, en contra de lo que mantienen tantos y tantos apologetas del misterio, una manifestación autóctona en el sentido de que pueda desentrocarse, por ejemplo, de la música y de la poesía andaluzas, sin menoscabo de su fundamental sentido.\n\nDejamos señalada la anterior reflexión, no sin adicionarle algunas necesarias matizaciones. Mercado subtitula su trabajo «Un ensayo sociológico y literario». Son dos los vectores en los que se incardina la rigurosa tarea de Mercado: sociología y literatura. Circunstancialmente surgen análisis de otro carácter que completan esta obra.\n\nEn relación a los aspectos sociológicos, el autor parte de una hipótesis de trabajo tan respetable como igualmente indiscutible: «Intento dar una serie de datos, noticias, referencias que ayuden a situar social e históricamente a hombres marginados y a su producto: El cante flamenco»; y en otro pasaje: «El flamenco es una actitud ante la vida; el gesto de desafío de una sociedad marginada frente a una sociedad convencional». Y el corolario: «...los cantes flamencos son el producto de una subcultura ur-",
    "title": "Aunque no quepa en el papel «La seguidilla gitana» o el esfuerzo por la inte- gridad",
    "periodical": "candil",
    "issue_id": "1983-01",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 399,
    "article_char_count_full": 2518,
    "article_char_count_review": 2518,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-01-13-right-aunque-no-quepa-en-el-papel-la-s",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE L fenómeno flamenco cuenta ya con una amplia bibliografía. Pero no abundan los estudios integrales de «lo jondo». Una extensa élite de miopes sabios del cante, escudriñadores de estilos, talentos de atribuciones, han contribuido a la difusión de un flamenco encorsetado, tópico y huérfano, desvinculado de sus hermosos precedentes. Y ello, a mi juicio, se ha debido a un importante déficit: El estudio integral de lo jondo. Quienes —o al menos la gran mayoría— se afanan en ostentar un discutible magisterio, han detenido sus vis investigadoras en lo superficial, cuando no en lo puramente anecódotico. Y es que no puede olvidarse que este estudio requiere una concurrencia de disciplinas dispares, un esfuerzo multidireccional para adentrarse en la esencia del flamenco. Esa, y no otra, es la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"técnica\"]\n\no, se ha debido a un importante déficit: El estudio integral de lo jondo. Quienes —o al menos la gran mayoría— se afanan en ostentar un discutible magisterio, han detenido sus vis investigadoras en lo superficial, cuando no en lo puramente anecódotico. Y es que no puede olvidarse que este estudio requiere una concurrencia de disciplinas dispares, un esfuerzo multidireccional para adentrarse en la esencia del flamenco. Esa, y no otra, es la única técnica para acceder a un conocimiento completo —dije antes integral— de lo jondo. Ese es el camino para reconducir —de forma precaria las más de las veces— los escasos datos fidedignos de que podemos valernos. Por Ramón Porras En este sentido, «La Seguidilla Gitana» (1), significa un intento riguroso de retomar el hilo conductor del flamenco. Y conste, como el propio autor declara, que no se trata de un estudio exhaustivo del cante flamenco. Lo más apreóiable de esta obra es su propio planteamiento: El cante como expresión invicerada en la misma Historia de Andalucía, sociología, música, poesía andaluzas, etc. No es lo jondo, en contra de lo que mantienen tantos y tantos apologetas del misterio, una manifestación autóctona en el sentido de que pueda desentrocarse, por ejemplo, de la música y de la poesía andaluzas, sin menoscabo de su fundamental sentido. Dejamos señalada la anterior reflexión, no sin adicionarle algunas necesarias matizaciones. Mercado subtitula su trabajo «Un ensayo sociológico y literario». Son dos los vectores en los que se incardina la rigurosa tarea de Mercado: sociología y literatura. Circunstancialmente surgen análisis de otro carácter que completan esta obra. En relación a los aspectos sociológicos, el autor parte de una hipótesis de trabajo tan respetable como igualmente indiscutible: «Intento dar una serie de datos, noticias, referencias que ayuden a situar social e históricamente a hombres marginados y a su producto: El cante flamenco»; y en otro pasaje: «El flamenco es una actitud ante la vida; el gesto de desafío de una sociedad marginada frente a una sociedad convencional». Y el corolario: «...los cantes flamencos son el producto de una subcultura ur- bana. De un fondo social de burdeles, corrales, canceles y encrucijadas urbanas truhanescas ascenderá a otros niveles y estratos sociales que los acogerá en el siglo XIX por moti\n\n[ENDING CONTEXT]\n\nsiglo XIX y primer tercio del XX, hasta nuestra guerra civil. 8. Desde el punto de vista formal, la “seguiriya gitana” es lo más peculiar dentro de los denominados “Cantes Flamencos”.\n\n9. La “seguiriya gitana” es una variante de la seguidilla común, las alteraciones son debidas a la necesidad de adaptación de las formas musicales.\n\n10. La “seguiriya gitana”, desde el punto de vista métrico, es un arcaísmo y titubea entre las formas de siguidilla y de endecha, fenómeno que también se repite en las primeras siguidillas recogidas por Foulché-Delbosc, denominadas, por él, seguidillas antiguas».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel «La seguidilla gitana» o el esfuerzo por la inte- gridad",
    "periodical": "candil",
    "issue_id": "1983-01",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 1109,
    "article_char_count_full": 6994,
    "article_char_count_review": 3965,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "técnica"
      }
    ]
  },
  {
    "article_id": "1983-01-14-right-la-etica-en-el-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nL mundo del flamenco, en lo que se refiere a su parcela bibliográfica, queda nutrido, una vez más, por una obra que, por boca de su autor (en la presentación de su libro, que tuvo lugar el 16 de septiembre pasado, en el Centro Cívico de La Unión —sede del Departamento de Estudios Flamencos—), sólo pretende hacer una recopilación de la inmensa mayoría de cosas que ya conocemos sobre los cantes libres y de Levante.\n\nUna vez leído el libro «Los cantes libres y de Levante», de don Andrés Salom, no es nuestra intención realizar una crítica exhaustiva al conjunto de las ciento cincuenta páginas de que consta, puesto que, a poco que nos lo propusiéramos, para aclarar, definir y justificar (en su realidad) ciertos errores y contradicciones que de él emanan sería necesario dedicarle un artículo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_01 | trigger=\"dentro\"]\n\ne, a poco que nos lo propusiéramos, para aclarar, definir y justificar (en su realidad) ciertos errores y contradicciones que de él emanan sería necesario dedicarle un artículo algo más extenso. Pero no es este el caso que nos ocupa, dado que, en un futuro próximo, lo intentaremos con mayor tranquilidad. Lo que en verdad nos preocupa, entre otras cosas, es todo cuanto forma parte y acontece en el flamenco, puesto que todo ello ha de ser tratado dentro de la más pura ética, tanto en la vertiente artística como en la humana. Por esta razón, sólo nos vamos a limitar a realizar unas puntualizaciones al apartado titulado «OTROS ACTUALES» —página 99—, del capítulo «LAS VOCES CANTAORAS POR LEVANTE», del libro reseñado. Por Juan Ruipérez Vera Empezaremos diciendo que los cantaores, a pesar de que algunos no nos agraden y otros, simplemente, no hayan alcanzado la fama como dice Andrés Salom en su libro «más allá de los límites de la comarca», merecen todo el RESPETO y admiración por quienes nos consideramos «entendidos, los que escribimos del cante, los que hablamos —casi siempre demasiado— en público del tema, los que constituimos los jurados de los concurso\n\n[ENDING CONTEXT]\n\nmarcar las pautas por donde debieran discurrir todos los elementos que desembocan en el llamado fenómeno flamenco, nos atrevemos a repetir una vez más que el flamenco es un arte que, para su perduración, requiere de estudios honestos y visto desde varios puntos de vista insoslayables como son el histórico-sociológico, el musical (musicológico) y el tradicional. Por ello, afortunadamente, coincidimos con otros al decir: que aún no se ha escrito con seriedad (incluida la ética) y —al fin y al cabo— con todas sus consecuencias LA GRAN HISTORIA DEL ARTE FLAMENCO.\n\nCorrea Weglison, 9\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La ética en el Flamenco",
    "periodical": "candil",
    "issue_id": "1983-01",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1200,
    "article_char_count_full": 7212,
    "article_char_count_review": 2789,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "dentro"
      }
    ]
  }
]
```
