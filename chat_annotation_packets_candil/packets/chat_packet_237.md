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
    "article_id": "1991-07-17-left-dos-discursos-hom-logos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nF lamenco (cante y baile) y tauromaquia son dos elementos vivaces enraizados en la cultura popular andaluza que presentan numerosos denominadores comunes. Ante todo, son representaciones dramáticas de enfrentamientos singulares o duelos, en que domina la emoción de un cuerpo a cuerpo amoroso y/o sangriento siempre pasional. Los sentimientos se exacerban, los impulsos se desbocan. El término tauromáquico de «lidia» connota estas nociones.\n\nLos dos tipos de expresión traducen primero la voluntad de una vuelta a lo esencial, a lo auténtico, es decir, a los valores y sentimientos primordiales del Hombre. Esto puede explicar la gran abstracción, la estilización e intelectualización que permiten. Detrás de la búsqueda de las raíces de una identidad colectiva, se esconde, finalmente, la del ego,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nico de «lidia» connota estas nociones. Los dos tipos de expresión traducen primero la voluntad de una vuelta a lo esencial, a lo auténtico, es decir, a los valores y sentimientos primordiales del Hombre. Esto puede explicar la gran abstracción, la estilización e intelectualización que permiten. Detrás de la búsqueda de las raíces de una identidad colectiva, se esconde, finalmente, la del ego, de la conciencia individual. Entonces, se comprende mejor la recuperación y la sublimación relativamente recientes de una cultura que ya no se encierra en el perímetro estrictamente andaluz. Hoy día la reivindica el patrimonio nacional, incluso internacional, después de un largo período de ostracismo y de desprecio, como lo atestigua la actitud de los intelectuales españoles de la generación del 98. Una de las razones múltiples de este rechazo puede ser la composición sincrética del flamenco y de la tauromaquia en los cuales se vio, durante largo tiempo, el sello de las culturas árabe y gitana, marginadas por la mentalidad segregacionista de la época. Hoy, pues, la reprobación o la hostilidad afirmada dejan rienda suelta a una interrogación más profunda y más completa en cuanto a la significación de un fenómeno que ya no se puede desconocer. Simplemente porque es todo un pueblo que se expresa y revela su alma mediante estas dos artes populares. Para ello, utilizan un discurso, una retórica universales, y de este modo adquieren la legitimidad de una expresión estética del mismo tamaño. Del enfrentamiento al arte El toreo es, ante todo, un enfrentamiento, una oposición entre dos seres a priori antitéticos. Se oponen, Las puertas de la plaza se abrirán a la una y la función empezará a las dos y media de la tarde. Una banda de músicas amenizará el espectáculo. Entrada general sin distinción de Sol y Sombra, 75 céntimos de peseta.—Medias entradas para niños y militares sin graduación, 40 céntimos de peseta. Toda localidad tendrá de aumento cinco céntimos. Cartel anunciador de una corrida donde se especifica: «Se correrá y dará muerte a un becerro por el célebre bailaor flamenco Don Antonio Muñoz, «El Enano». De este modo, el espacio mítico cubre el campo de la memoria colectiva cuyos héroes se hacen los portavoces. Efectivamente, no puede haber lidia, cante o baile flamencos, sin participación activa de los espectadores-actores que ocupan el sitio y desempeñan el papel del coro de la tragedia antigua. El espacio propiamente dicho está sustituido por un espacio-tiempo comunitario sitiado por el mito social y colectivo. Todo el complejo del enunciado del discurso: personajes (héroes)-espacio-tiempo, se reduce\n\n[ENDING CONTEXT]\n\nfracaso y el drama. El diálogo abortado con el Otro hace volver al monólogo de la soledad y de la angustia. El triunfo tiene el sabor amargo de la sangre.\n\nLa problemática de un héroe universal\n\nFlamenco y tauromaquia desarrollan esta visión trágica del mundo en cada momento y en cada nivel de su discurso. Es la prueba de la muerte, omnipresente, que sublima a los personajes y hace de ellos héroes. Toreo y flamenco siguen la metáfora del aprendizaje de la vida y de la muerte. Pueden ser considerados como verdaderos discursos de iniciación cuya naturaleza y cuyos componentes serían homólogos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Arte flamenco y tauromaquia: dos discursos homólogos",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "17-19",
    "page_number": 17,
    "word_count": 2480,
    "article_char_count_full": 15106,
    "article_char_count_review": 4264,
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
    "article_id": "1991-07-19-right-la-alegr-a-de-la-huerta",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFernando Durán Bonilla\n\nPara hablar del cante por malagueñas es necesario referirse a temas tan dispares como lo puedan ser las sierras y las playas, las huertas y la manigua y la «escuela bolera» y los cafés cantantes. Aquí este modesto escribidor, se apoya en algunas vivencias personales y en la lectura de numerosos libros, que al final reseño, para intentar partir de un nuevo concepto para el estudio de los cantes malagueños, cuyas raíces preceden al cante flamenco.\n\nEn nuestros anteriores escritos, caminando por sierras y llanos, conviviendo con la gente del camino, trajinantes, arrieros, contrabandistas y bandoleros, pudimos comprobar cómo a través de las rutas de estos caminantes, los cantes malagueños pudieron pasar desde la serranía al rebalaje de las playas o viceversa. Málaga,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"raíces\"]\n\nminando por sierras y llanos, conviviendo con la gente del camino, trajinantes, arrieros, contrabandistas y bandoleros, pudimos comprobar cómo a través de las rutas de estos caminantes, los cantes malagueños pudieron pasar desde la serranía al rebalaje de las playas o viceversa. Málaga, ciudad andaluzade reminiscencias tartésicas, fenicias, bizantinas y árabes, no podía ser distinta a sus hermanas y perdida en la noche de los tiempos, tiene-unas raíces propias en su folklore. Sus bailes, sus cantes, sus melismas, su poética, su vestuario y su musicalidad, tienen nombres propios. Sigamos nuestro viaje a través del tiempo, con dos ilustres visitantes franceses: Charles Vailliers y Gustavo Doré. Estos viajeros nos cuentan que una calurosa tarde de estío, asistieron a una tertulia, seguida de una fiesta en el patio de una casa situada en la calle Granada. Indudablemente así debió ser, porque casi al final de la calle Granada, desembocan algunas calles muy estrechas, que no recuerdan las típicas y antiguas calles morunas. Todavía se conservan algunas mansiones en la calle de «Los caballeros», hoy de San Agustín, con sus patios de bellas arcadas, surtidores de agua el —el moro, con sed de desiertos, siempre ha buscado la bella sinfonía del rumor del agua—naranjos de cachorreña —que dan el azahar más oloroso—enredaderas de yedra adheridas a los muros y jazmines y damas de noche sobre una frágil pérgola, sin duda ofrecían la umbría perfecta para una tertulia y escuchar nuestra música, oír nuestros cantes y contemplar nuestras danzas. Cuentan nuestros huéspedes que se bailaron algunos pasos andaluces como «El polo del contrabandista», y «La malagueña y el torero». También escucharon «...al son de la guitarra, esas coplillas tan populares en Andalucía que se llaman \"malagueñas\"... cuyo ritmo es un poco extraño, rudo si se quiere, pero no tiene nada de vulgar ni frívolo. Lo mismo puede decirse de las cañas, carceleras, playeras, rondeñas y otros cantos populares de Andalucía. Lo mismo que todos estos aires, las malagueñas tienen sin duda un origen moro, y son, sin haber sufrido alteración alguna, las mismas melodías que cantaban, acompañándose al laúd, los súbditos de Ibn-Al-Kamar y de Boabdil. Probablemente, las mismas palabras no s\n\n[ENDING CONTEXT]\n\nCuando la Reconquista, los Reyes Católicos, pusieron al mando del pueblo a un tal Don Pero.\n\nSOPENA, Atlas geográfico e histórico. BEJARANO, P., Cafés de Málaga. NAVARRO, Pepe, Muestrario de malagueñas y malagueñeros. DAVILLIERS-GUSTAVO DORÉ, Charles, Viaje por España. GUERRA VALDENEBRO, Pepa, Así canta y baila Andalucía. MAJADA NEILA, Jesús, Viajeros románticos en Málaga. ALCOBENDAS, M., Málaga, Personajes en su historia, 100 autores, Recop. BIAS VEGA, José, Cafés cantantes de Sevilla\n\nBLAS VEGA, José, Cafés cantantes de Sevilla. GARCÍA CHICÓN, A., Apuntes para una Antropología Andaluzia.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La alegría de la huerta",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "19-22",
    "page_number": 19,
    "word_count": 3491,
    "article_char_count_full": 21187,
    "article_char_count_review": 3886,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "raíces"
      }
    ]
  },
  {
    "article_id": "1991-07-23-left-siguiriyas-gitanas",
    "article_text_for_review": "Siguiriyas gitanas\n\nNo me pás, gitana, que guarde silencio si ninguna faca ha cortao mi lengua ni ha llegao mi entierro.\n\nDéjame que hable de tus negras trenzas y de tus ojazos, tan llenos de asombro, en la noche aquélla.\n\nDeja que recuerde, sin morir, muriendo, tu temblor de vida al pasar mis manos por tus duros pechos.\n\nDeja que, en mis sueños, corra tu camino y que se remuevan el vino y las facas al oír tu grito.\n\nY que, bajo el surco carnal de tu vientre, germine tu nana que riega esta noche mi orgasmo de nieve.\n\nDiego Granados",
    "title": "Siguiriyas gitanas",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 101,
    "article_char_count_full": 537,
    "article_char_count_review": 537,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-07-23-right-cantaor",
    "article_text_for_review": "Cantaor\n\nA todos nos han cantado en una noche de juerga coplas que nos han matado...\n\nManuel Machado\n\nHoy has vuelto de nuevo con tu cante al tablao de tu pena sin consuelo, a vender tu amargura y tu desvelo a beberte la vida en cada instante.\n\nTragedia en torno a ti, es la constante que se enjuga en quejos sin pañuelo, que se embriaga en jipíos muerte y cielo y se queja en un ¡ay! seco y vibrante.\n\nMírate en tu memoria antepasada, rebusca en tus lamentos de jondura que hallarás una copla que nos hiera.\n\nSerá tu voz más ronca y más sincera al eco de tu grito y desventura y al de clamor de tu pena consolada.\n\nPaco Arana",
    "title": "Cantaor",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 122,
    "article_char_count_full": 626,
    "article_char_count_review": 626,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-07-24-left-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "Debe de ser un gustazo. Cantar lo que uno mismo ha compuesto. Repetir en forma alada, con la expresividad plástica del cante, con su ductilidad para los matices, lo que ha surgido del fondo del alma creadora torpemente, con la expresión agarrotada que tienen las palabras al lado de los vuelos inconmensurables del cante, porque éste, por mucho que nos empeñemos, nunca cabrá «en er pápé», ¿verdad Tío Joaquín?\n\nJosé Giorgio Soto, el de la Toma-sa, se viene dando, desde hace años, ese gusto. Porque sabe y porque puede. Cuando los conceptos se le agolpan en el corazón con demasiada violencia, él los traduce en palabras, y cuando éstas forman un montón volandero, una gavilla de sentimientos que el aire pudiera torcer hacia zonas indeseables, José las somete al imperio supremo de la siguiriya o las abanica con ese temblor de susurros que llamamos soleá.\n\n«Alma de Barco», por todo ello, es mucho más que un libro de poemas, porque éstos, con toda su grandeza, pertenecen todavía a esa servidumbre de la lectura individual, a un boca a boca tan hermoso como insuficiente. Pero en\n\nTomasa subyacen fuerzas ciegas que retoman el poema, le recortan las cadenas de metales preciosos con que han sido forjados y lo lanzan al aire en la garganta mágica del mismo que lo ha creado.\n\nPosiblemente esta entrega en libro de cantes tan profundos, sea tan sólo un premio de consolación a quienes no tenemos a diario la suerte de escucharlos cantados. Es como si este augusto descendiente de los Torre nos entregara congelado un corazón que se le ha quedado estrecho.\n\nEn este libro, como estudia perfectamente el equipo Arriate en su brillante introducción, están presentes todos los añejos registros del flamenco: penas, alegrías, amor, muerte y hasta reivindicaciones populares, pero, lo más hermoso de todo es que suena a una verdad tan directa que hay que rendirse a ella aun a sabiendas de su insuficiencia, con la conciencia clara de que nada estará completo hasta que no tengamos la dicha de tropezarnos con José y oírle gritar lo que un día, por pura asfixia existencial, escribiera en el papel:\n\nDonde no existe la pena el cante desaparece, porque siempre se ha criado donde las fatigas crecen.",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1991-07",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 373,
    "article_char_count_full": 2195,
    "article_char_count_review": 2195,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
