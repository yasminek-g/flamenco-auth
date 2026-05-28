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
    "article_id": "1994-09-30-left-aunque-no-quepa-en-el-papel-bibl",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE n el mundo de las biografías especializadas en los artistas flamencos, parece claro que ésta que nos ocupa no va a pasar a la historia. Y lo sentimos profundamente, ya que la seriedad investigadora y la meticulosidad del autor, Gonzalo Rojo, que se plasmará no hace mucho en su espléndida biografía sobre Juan Breva, no parecen reflejarse en este libro de labazado y mal pergeñado en el que se analiza la trayectoria biográfica de «El Cojo de Málaga», pero que, una vez leído el mismo, nos encontramos con que solamente se nos ofrecen unos datos fríos, inconexos y sin elaborar, que no tienen el calado suficiente para entusiasmar a nadie y que se limitan a mostrar una sucesión de artistas que acompañaron a Joaquín José en su trayectoria artística y la enumeración caótica de los sitios donde\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"segunda\"]\n\ne el escribir sobre un personaje. Ni siquiera el análisis de las placas discográficas de «El Cojo» se efectúa con el rigor y amenidad que hemos visto, desde hace años, como características del trabajo de nuestro amigo Gonzalo, sino que es tan confuso y poco ilustrativo como las alusiones vitales anteriormente mencionadas. No dudamos, por ello, que las prisas del encargo, por parte del Congreso, han jugado una mala pasada al autor. Esperamos una segunda edición más completa y cuidada. La de hoy, y nos duele decirlo, es completamente prescindible. Joaquín José Vargas Soto. «Cojo de Málaga» Edición especial para el XXII Congreso de Arte Flamenco. Estepona (Málaga), 1994 Evanos entendamos la estudio flamencos se poco son buenas las prisas. Tarda en salir a la luz un libro que nos enseñe, de verdad, a los que estamos algo iniciados en el tema. Repeticiones machaconas, topicazos reiterativos, sí que sobran en el panorama jondo; también, por desgracia, afirmaciones categóricas que nadie se molesta en probar, porque al autor le falta la más elemental investigación sobre el asunto acerca del cual pontifica con tanto énfasis. Por ello, es de agradecer la aparición del libro de Manuel López sobre las onomatopeyes en el léxico flamenco, es decir,\n\n[ENDING CONTEXT]\n\nde grandes artistas del género, entre ellos: La Niña de la Puebla, Fosforito, Menese, Carmen Linares, Luis de Córdoba, José Mercé, Chano Lobato, etc. Paco Serrano ha intervenido en Cursos Internacionales de Guitarra Flamenca y Clásica en Castres (Francia), y Cursos Internacionales de Guitarra en Córdoba. A nivel internacional, ciudades como: Londres, Manchester, Frankfurt, Utrech, Fez, París, Lyon, Toulouse, Bruselas, etc., conocen la exquisitez de su guitarra y son eco de sus éxitos. Cuenta en el mercado con seis L.P., haciendo pareja con distintos artistas\n\nTOCAORES DE HOY\n\nPaco Serrano\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel... Bibliografía José Luis",
    "periodical": "candil",
    "issue_id": "1994-09",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "29-31",
    "page_number": 29,
    "word_count": 1447,
    "article_char_count_full": 8961,
    "article_char_count_review": 2877,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segunda"
      }
    ]
  },
  {
    "article_id": "1994-11-3-left-editorial",
    "article_text_for_review": "Dirigen: Ramón Porras González Rafael Valera Espinosa Redactor Jefe: José Luis Buendía López Consejo de Redacción: Alfonso Fernández Malo, Fausto Olivares Palacios, Manuel Martín Martín, Manuel Villarejo García y José Pamos Mozas. Secretario: Leovigildo Francisco Aguilar Burgos Administrador: Juan José Carrascosa Jurado Correspondal en Sevilla: Manuel Martín Martín Correspondal en Granada: Juan Cruz Maculet Correspondal en Almería: Antonio Zapata García Correspondales en Cádiz Pedro Sánchez Ortega y Luis Soler Guevara Correspondal en Málaga: Ramón Soler Díaz Correspondal en Extremadura: Joaquín Rojas Gallardo Correspondal en Cataluña: Francisco Hidalgo Gómez Diseño: José Viñals Fotografías: José Pamos, archivo y otros. Redacción y Administración: Mastra, 11 - 23001 Jaén, España Teléfono (953) 23 17 10 Edita: Peña Flamenca de Jaén Imprime: SOPROARGRA, S. A. Teléf. 228000 - Fax: 266009 C/. Villatorres, 10 - Jaén Depósito Legal: J. 133 - 1978 I.S.S.N.: 0212-8640\n\nPortada Eva Martínez Bueno\n\nNota Prohibida la reproducción total o parcial de textos e ilustraciones sin mencionar la procedencia. «Candil» no se hace necesariamente solidaria de los puntos de vista sostenidos en las colaboraciones firmadas. Es incluso consciente de que muchos de ellos versan sobre materias controvertidas e invita a los estudiosos al debate sobre los temas tratados.\n\nLa publicación de este número ha sido posible gracias a la Consejería de Cultura de la Junta de Andalucía\n\n*S pare un un gran hombre colaboro en la realización del wimmer usurrof'w \"Paco de Lucia\" y aguader a Contejo de Asumstración hable si do unidad a participar en tan mercado homenaje. Un juicio alw Verancio 21 × 94 D. E. Pohren De su libro «Paco de Lucia y Familia»",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1994-11",
    "year": 1994,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 265,
    "article_char_count_full": 1733,
    "article_char_count_review": 1733,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-11-3-right-paco-de-luc-a",
    "article_text_for_review": "E 1 monográfico que la Revista Candil dedica al mágico y excepcional Paco de Lucía, no sólo intenta ser una aproximación a la fulgurante trayectoria del, tal vez, más importante guitarrista de la historia reciente del flamenco, sino, sobre todo, ocasión de reflexionar sobre la evolución que cabe atribuir a este instrumento, en modo alguno asimilable a la que ha conducido el fenómeno del cante, y cuyo protagonismo, por méritos propios, hay que referirlo a Francisco Sánchez Gómez, Paco de Lucía. Acaso, siguiendo la atinada apreciación de Manuel Ríos Ruiz, habría que decir que más que labor evolutiva lo que ha realizado el tocaor algecireño es una auténtica revolución. Sea lo que fuere, creemos de interés significar dos apuntes que aunque de manera sucinta, centran, a nuestro modesto juicio, la personalidad artística de Paco de Lucía. Primero: en los hondones de su toque —como un mitológico animal enjaulado, diría Félix Grande— se instala toda la cultura de lo jondo. Exhibe Paco\n\nde Lucía una técnica tumultuosa, enfebrecida y como llena de cólera, que no es sólo fruto de esmeradísima digitación, sino memoria de cantes aduendados, pura jondura. Y si lo dicho entronca con la perspectiva evolutiva del Paco de Lucía, tocaor, el segundo apunte, se refiere a la dimensión revolucionaria del Paco de Lucía, creador, más allá incluso de sus asombrosas cualidades de intérprete. Paco de\n\nLucía ha sido todo menos un tocaor, un músico apacible; antes al contrario, su discurso musical siempre ha estado abierto a cuantos referentes pudieran enriquecer su música. El público reconocimiento que ha recibido de las leyendas vivas de la guitarra como Larry Coryell, John McLaughlin y, en general, el extraordinario prestigio de que goza en todos los foros musicales del mundo, lo erigen en el artista flamenco o arraigado en la cultura flamenca, más popular de este siglo.\n\nDe obligada referencia en esta sucinta aproximación a la figura de Paco de Lucía, deben de ser las numerosas monografías que sobre este incomensurable guitarrista existen en el mercado. Félix Grande, D. E. Pohren y, recientemente, Juan José Téllez, y un largo etcétera que sería prolijo enumerar.\n\nPor nuestra parte, con la presentación de este monográfico creemos poder contribuir humildemente al conocimiento y la difusión de uno de los andaluces más universales. Ese es nuestro pequeño homenaje.",
    "title": "Paco de Lucía",
    "periodical": "candil",
    "issue_id": "1994-11",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 381,
    "article_char_count_full": 2375,
    "article_char_count_review": 2375,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-11-5-right-paco-la-persona-d",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHace algún tiempo, durante una entrevista, Paco dijo que su signo del zodíaco era Sagitario y comentó, bromeando, que esto debería convertirle en bandido o músico. También hay otra característica común a los Sagitarios que ha moldeado la vida de Paco más que ninguna otra: una independencia discreta. Esto significa que cuando un Sagitario toma una decisión, él o ella hacen exactamente lo que les viene en gana, sin discusiones o puñetazos sobre la mesa. Claro que esto es cierto sólo en un sentido general de la frase, ya que nadie puede hacer lo que quiere a todas horas. Por ejemplo, está el pequeño detalle de ganarse el sustento, lo cual no siempre son «días de vino y rosas». En el caso de Paco, existen períodos en que querría perder de vista la guitarra. Durante años ha dado conciertos y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"práctica\"]\n\nla hacen exactamente lo que les viene en gana, sin discusiones o puñetazos sobre la mesa. Claro que esto es cierto sólo en un sentido general de la frase, ya que nadie puede hacer lo que quiere a todas horas. Por ejemplo, está el pequeño detalle de ganarse el sustento, lo cual no siempre son «días de vino y rosas». En el caso de Paco, existen períodos en que querría perder de vista la guitarra. Durante años ha dado conciertos y ha grabado discos prácticamente sin descanso, sumergido, como él dice, en un constante estado de ansiedad (Paco sigue padeciendo de miedo escénico cada vez que sube al escenario; pero no lo querría de ninguna otra forma, asegurando que su miedo genera la energía nerviosa tan necesaria para ejecutar el flamenco con agallas y emoción). Finalmente llegó a la conclusión de que se estaba quemando y decidió tomarse vacaciones durante dos o tres meses al año; cuando llega la ocasión, se retira sin su guitarra a algún paraíso terrenal donde se dedica a la pesca y al descanso en general. Por añadidura, cuando se encuentra en Madrid, reserva todo el tiempo que puede para practicar su pasión deportiva: jugar al fútbol o, en ocasiones, jugar al tenis con sus amigos. Asegura que en la actualidad sólo es totalmente feliz durante estos descansos, cuando puede relajarse y sus inseguridades se desvanecen. Sí, inseguridades, pues Paco, que declara ser un neurótico, se siente apresado por ellas. Para empezar, tiene la inseguridad sufrida por todos los seres inteligentes que no han tenido la fortuna de recibir una educación convencional. Estos seres tienden a atribuir grandes y misteriosas cualidades a la educación avanzada, no teniendo noción de cuánta parte de ella es innecesaria y cuánta más cae en el olvido antes de ser asimilada, y que lo auténticamente importante son la inteligencia y sabiduría innatas, cualidades que Paco posee en abundancia; uno sólo tiene que escuchar los incisivos análisis que Paco hace sobre su trabajo y la vida en general para darse cuenta de ello. También mantiene una relación amor-odio\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_03 | trigger=\"creatividad\"]\n\nllar». El buen amigo de Paco, John McLaughlin, bajo el mismo tipo de presión, lo resume de esta forma: «Ya conoces el viejo adagio, si no practicas un día, lo sabes tú; si no practicas varios días, tus colegas lo saben; y si no tocas durante una semana, lo sabe todo el mundo». Hoy día, Paco considera su actividad de nueve meses al año de giras y grabaciones como un trabajo duro. La constante tensión le hunde en depresiones que le hacen sentir su creatividad a punto de agotarse, que toca mal, que todo ello no vale la pena... y es durante estos períodos cuando realmente anhela cualquier descanso que su plan de trabajo permita, como cualquier trabajador del mundo. Afortunadamente, después de sus tres meses de descanso, Paco vuelve a la guitarra con renovado ardor, gusto y creatividad, atributos que empiezan a desgastarse de nuevo según pasan los meses. La relación de Paco con el flamenco es igual de\n\n[ENDING CONTEXT]\n\ntoda esa forma de vida. Hoy día, el sentido común y la razón imperan y los artistas consideran el flamenco como un negocio, exigiendo, por cierto número de horas trabajadas, una remuneración apropiada, de la cual cierta cantidad se gasta en posesiones materiales (algo que nunca preocupó a la mayoría de los flamencos), y otra se invierte para asegurarse una vejez digna y confortable. Esta es una actitud inteligente y recomendable, pero jcómo se echan de menos aquellas juergas despreocupadas que acababan solamente cuando la última peseta, botella y rastro de energía habían sido agotadas!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Paco, la persona D",
    "periodical": "candil",
    "issue_id": "1994-11",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 2702,
    "article_char_count_full": 15974,
    "article_char_count_review": 4656,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "práctica"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "creatividad"
      }
    ]
  },
  {
    "article_id": "1994-11-7-right-integrados-apocal-pticos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFélix Grande De su libro «Agenda Flamenca»\n\nLanzas a empresa grabadora Philips guitarra flamenca (1). No es un disco más. Ni siquiera hoy, cuando no escasean los intérpretes torrenciales de esta música y cuando las casas grabadoras han advertido que la demanda de discos de flamenco aumenta progresivamente y, en consecuencia, editan en cantidad considerable, ni siquiera hoy puede decirse que éste sea un disco más. Se trata de una decisiva aportación en la discografía flamenca. La reunión de una técnica no sólo rica, sino temeraria, una pasión enriquecida por el rigor, una documentación sobre la estética flamenca no averiguada en la frialdad de las pizarras y los pentagramas a horas fijas, sino vivida en la experiencia de las madrugadas laboriosas y en todo el complejo bloque de la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\ne, ni siquiera hoy puede decirse que éste sea un disco más. Se trata de una decisiva aportación en la discografía flamenca. La reunión de una técnica no sólo rica, sino temeraria, una pasión enriquecida por el rigor, una documentación sobre la estética flamenca no averiguada en la frialdad de las pizarras y los pentagramas a horas fijas, sino vivida en la experiencia de las madrugadas laboriosas y en todo el complejo bloque de la tradición de un arte de borrosos orígenes, unido y vitalizado todo ello con una extraordinaria capacidad de invención, es algo que únicamente puede superar, hoy por hoy, el mismo Paco de Lucía. Deseo insistir en algo fundamental dentro del campo de (En defensa de la guitarra flamenca) cualquier actividad artística, pero muy específicamente en el de la creación de música flamenca; se dice que la guitarra es un instrumento pasional (no obstante contener muy peculiares posibilidades para la expresión de emociones en donde predominen la ternura, la nostalgia o la soledad), eminentemente pasional, o, si se prefiere, apasionado. En este disco, la pasión no sólo asoma, sino que define constantemente el discurso expresivo, pero de tal manera acompañado por la serenidad, por el dominio emocional y técnico, que incluso allí donde la música traduce o configura la memoria de lo que podríamos llamar un quejido brutal o un vasto aullido, llega hasta la sensibilidad del oyente con un gesto de majestad. Gritar es fácil. Gritar en la guitarra flamenca requiere ya al menos una compleja elaboración mecánica. Pero gritar en ella de un modo convincente (esto es, transfigurar ese grito en un ademán de rigor y de fortaleza) es algo reservado a un intérprete dueño no sólo de una técnica sólida y matizada, sino también de una intuición musical y de una fortuna expresiva fuera de lo común. La música flamenca no tiene todavía su historia. L\n\n[ENDING CONTEXT]\n\nmúsicas que, con el nombre de flamenco, ya es justo llamar inmortales. Esta grabación es, pues, algo más que un nuevo disco de Paco de Lucía. Por sí solo, esto sería un acontecimiento. Pero cuando se escuche, escúchese algo más: el respeto y la gratitud de Paco de Lucía por don Manuel de Falla, el respeto y la gratitud de don Manuel de Falla hacia el flamenco, y el abrazo ya indisoluble de la música culta y la flamenca a través del abrazo que mediante esta guitarra y aquellas partituras se dan, por entre el ciego tiempo, un gran andaluz de Algeciras y un inmenso andaluz de la milenaria Gadir.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Integrados apocalípticos",
    "periodical": "candil",
    "issue_id": "1994-11",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "7-10",
    "page_number": 7,
    "word_count": 3239,
    "article_char_count_full": 19532,
    "article_char_count_review": 3489,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  }
]
```
