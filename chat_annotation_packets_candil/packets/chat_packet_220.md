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
    "article_id": "1990-09-20-left-musicolog-a-del-flamenco-3-er-ca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPonencia presentada al XVIII Congreso Nacional de Actividades Flamencas\n\nPhilippe Donnier\n\nEn el congreso de Córdoba hice un amplio recorrido de las posibilidades ofrecidas por la musicología en el campo del Flamenco sin entrar en detalle ni justificaciones. En Jerez de la Frontera enseñé unos gráficos obtenidos por computadora que permiten una transcripción fina y rápida de los melismas tan características del cante flamenco. En Badajoz quisiera, después de un año pasado en Córdoba entre Peñas y aficionados, dar cuenta de mis últimos trabajos realizados en contacto con la Peña Flamenca de Córdoba y dos jóvenes artistas: Manolo Millán (guitarista) y David Pino (cantaor). Presentaré estos trabajos en dos comunicaciones separadas, la primera en colaboración con Manolo Millán y la segunda en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"representación\"]\n\nde mis últimos trabajos realizados en contacto con la Peña Flamenca de Córdoba y dos jóvenes artistas: Manolo Millán (guitarista) y David Pino (cantaor). Presentaré estos trabajos en dos comunicaciones separadas, la primera en colaboración con Manolo Millán y la segunda en colaboración con David Pino. «POCO TRES POR CUATRO TIENEN LOS TOQUES FLAMENCOS» Esta comunicación será presentada en el congreso por Philippe Donnier y Manolo Millán. Toda representación de una realidad plantea problemas semiológicos. Se corre a menudo el riesgo de confundir signo y realidad. Esto le ha pasado al sistema clásico del solfeo, que ha acabado por tomar unos signos convencionales de representación de la música como materia prima de composición; cuando el mundo de las frecuencias sonoras y de los ritmos tienen muchas más riquezas potenciales. En la estética clásica, el sistema formal de signos y la mente mandan sobre la materia y la fisiología. En las músicas tradicionales parece que la fisiología y la materia mandan sobre la mente (puede ser por eso que Manuel de Falla haya hablado de música «natural» refiriéndose al Flamenco). Para entender mejor el problema planteado recurriremos a un ejemplo gráfico: Existen cuatro sistemas de compases «oficiales» en la teoría clásica de solfeo: 2/4; 3/4; 4/4; 6/8 y todas las combinaciones o amalgamas posibles. El primer tiempo de cada compás es fuerte. En las músicas tradicionales la escritura (cuando existe) viene después del nacimiento del hecho musical vivo. No hay ninguna razón para que este tipo de música respete las convenciones formales propias de la cultura burguesa occidental. Las posibilidades combinatorias de tiempos fuertes y\n\n[ENDING CONTEXT]\n\nde los «núcleos melódicos» de cada estilo le permite «aguantar» mucho mejor el tiempo y la armonía cuando sabe lo que «va a caer» después de una serie melismática inesperada propia del cantaor que aompaña en este momento particular (en otro momento el mismo cantaor, si es flamenco de verdad, podrá recrearse de otra forma y en otro sitio). Como en la primera comunicación, la exposición se hará en el congreso con ilustraciones musicales con el cante de David Pino y con esquemas proyectados en pantalla.\n\nAPERITIVOS SELECTOS Especialidad en Plancha\n\nC/. Mesones, 18\n\nTeléfono 26 35 46\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Musicología del flamenco 3.er capítulo",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "20-22",
    "page_number": 20,
    "word_count": 2218,
    "article_char_count_full": 14090,
    "article_char_count_review": 3317,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "representación"
      }
    ]
  },
  {
    "article_id": "1990-09-22-right-hondura-duende-y-gusto",
    "article_text_for_review": "Opinión\n\nLuis Caballero\n\nA y de aquel cantaor que cante los cantes sin que le asista en ningún momento ni una sola de estas tres imprescindibles cualidades sensibles: hondura, duende o gusto.\n\nDe nada servirán los conocimientos más extensos ni las técnicas más estrictas sonando ausentes de esos tres factores emocionales. De nada, naturalmente para el oído, la sensibilidad y predisposición solidificada del que sepa escuchar y lo que escucha. Por el contrario, sí puede servir, y de hecho sirve, a una mayoría néofita y libre del compromiso sentimental que encadena y exige al aficionado definitiva y apasionadamente inmerso en el mundo del cante.\n\nRaro es el caso del cantaor erradicado de manera total y absoluta de esas tres posibilidades, mas así como, en el sentido adverso, hallar esos tres privilegios unidos en un solo cantaor.\n\nCada cantaor tiene su mundo interior a través del cual se expresa cuando canta sincera y necesariamente. Hay un cantaor hondo —tal vez el que menos se repite— que hace un cante como depurado, aséptico y remoto. Parece como si no llegara o superara las formas musicales habituales del arte y el gusto para ahondando encontrar y encontrarse sólo con la tierra, con su tierra. Es un cante tosco y fecundo como la tierra donde se canta. Un cante esquemático y ciego de perspectivas externas. Es un cante regresivo. Un cante al que hasta puede estorbarle la apoyatura refinada de la guitarra por su aridez primitiva. Este es ese cante que no hiere ni pellizca però que transporta y eleva. Es como una filosofía abstracta e inalterable.\n\nA precio el duende desde otro ángulo. El duende, ese «poder mis-\n\nterioso que todos sienten y que ningún filósofo explica». Como el propio cante, el duende sólo puede explicarse sintiéndosele. Es como un sentimiento superior, extraordinario, anormal y sublime. No todos los artistas lo transmiten sin que por ello dejen de ser grandes artistas.\n\nEn el capítulo flamenco el duende juega un papel primordial: tanto el que canta como el que escucha le requieren, y esperan con la esperanza del que reza con devoción. Pero el duende es un misterio psíquico que exige un entorno circunstancial no siempre ocasional para el discurrir y el transcurrir ordinario de la profesionalidad contratada.\n\nEl duende es como un acceso- rio del cante, como un premio especial que vendrá a superen- grandecer lo que ya es grande por su naturaleza.\n\nPero quizás sea el gusto, como compendio más o menos positivo de la hondura y el duende, la cualidad que resuelva el indispensable impacto penetrante entre cantaor y auditorio. El gusto, el ángel, la gracia; ese otro don que tampoco se prodiga aunque parezcan, de las facetas, la más superficial.\n\nEn resumen, sobre estos tres ejes anímicos-sonoros gira el cante de cada cual por separado. La fusión de los tres en uno no creo que se dé de manera definitivamente implícita y natural. Por eso jamás existirá el mejor por unanimidad.",
    "title": "Hondura, duende y gusto",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 491,
    "article_char_count_full": 2932,
    "article_char_count_review": 2932,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-09-23-left-cante-hondo",
    "article_text_for_review": "Esta noche están todos, como otras noches, han traído su bagaje cargado de agobios y problemas de un día como tantos, de una jornada dedicada a su quehacer diario. Mientras Paco y Ramón afinan sus guitarras, José descorcha una botella y sirve el vino, Rafael ha puesto sobre la mesa su tabaco... El ciprés de las guitarras se ha fundido en un lamento profundo y lánguido. Antonio se apunta por cantiñas, justo al tono, cabal y acompasado, José le jalea y se ensimisma, para hacer un cante improvisado, gitanea con los ayes a compás en un trabalenguas alargado. Ha empezado el ritual de los flamencos. Pide tono Rafael —al dos por medio —para cantar unos tangos. Un torrente de ritmo se desboca con cortes y recortes perfectamente unisonados, donde palmas y guitarra se acompanían para dar entrada al cantaor, que hace una letra que Paco le ha pasado:\n\nNo bebas de la fuente de los milagros. Bebe tus lagrimitas, tus desengaños.\n\nTodo vibra en la noche y poco a poco, se ha creado un ambiente que trae ecos andaluces y sabor de vino amontillado. Gran parte de los cantes más genuinos se han evocado esta noche en este cuarto; soleá, peteneras, alegrías, marianas y fandangos; se han tañido falsetas legadas por Montoya, El Habichuela y El Niño Ricardo. Ha juntado el reloj sus manecillas. Es la hora de la verdad. Aquí en este cuarto, se ha puesto un halo de misterio que flota entre botellas vacías y pitillos apagados. Gime rasgueos la guitarra con trémolos brillantes y bordones quebrados, brotando así la siguiriya, la que más sabe de cante, de nostalgia y de presagios y dice Antonio una letra compungida que le quema las entrañas y casi nunca ha cantado. Maldita la pluma, maldito el papel; y el cartero que trajo la carta maldito también. Esta noche están todos, con los problemas y agobios de un día como tantos.\n\nPaco Arana",
    "title": "Cante jondo",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 321,
    "article_char_count_full": 1831,
    "article_char_count_review": 1831,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-09-23-right-campanilleros",
    "article_text_for_review": "Campanilleros\n\nP or la madrugada abajo una tormenta de aire anima los almanayes en la eterna navidad de los candiles y las sombras. Coral sola que canta a las primeras luminarias a los gallos encendidos o al fragor de las ventanas. Ya vienen los campanilleros en sus memorias traen estrellitas de plata. El paisaje de la mañana en el fanal del relente va feneciendo su cera cansada. Y la flama auroral de las voces juntas desprezan los cuerpos entre el universo de las sábanas en una revolera de labios blancos que besan el rojo invierno teñido por el arco iris de las campanas. Campanilleros Campanilleros seguid sueño a sueño no despertéis llorad con las guitarras por los pueblos del alma que la Niña de la Puebla os lleve hasta el lucero del alba.\n\nJesús Cuesta Arana",
    "title": "Campanilleros Cantes",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 136,
    "article_char_count_full": 771,
    "article_char_count_review": 771,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-09-24-left-bibliograf-a",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«La copla flamenca y la lírica de tipo popular»\n\nFrancisco Gutiérrez Carbajo\n\nEditorial Cinterco. Madrid, 1990, 2 vols.\n\nE l presente trabajo, agotador para el autor que lo ha compuesto y también para el lector que se detenga en la densidad de este hermoso estudio de 1.070 páginas, y que ve ahora la luz después de ser acabado en 1987, año en que sirvió de base para la tesis doctoral de Gutiérrez Carbajo y para ser galardonado con el primer premio de investigación de la Fundación Andaluzía de Flamenco.\n\nSin embargo, hemos de referirnos tan sólo a la fatiga intelectual que producen los textos acabados, con verdadero espíritu científico y sobrada documentación que demuestren lo que se afirma en ellos más allá de la pura especulación. Porque, por lo demás, el libro se lee sin problemas, es\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"improvisado\"]\n\ntigación de la Fundación Andaluzía de Flamenco. Sin embargo, hemos de referirnos tan sólo a la fatiga intelectual que producen los textos acabados, con verdadero espíritu científico y sobrada documentación que demuestren lo que se afirma en ellos más allá de la pura especulación. Porque, por lo demás, el libro se lee sin problemas, es más, nos engancha de principio a fin en el primor de su desarrollo y apenas nos deja respirar. Aquí no hay nada improvisado, sino estudio y seriedad por José Luis Buendía López todas partes. Como el mismo título de la obra sugiere, la copla flamenca es continuamente analizada y comparada sistemáticamente con otros tipos de poesía popular, tanto del ámbito español como de referencias periféricas, en un denso análisis estructurado en los seis capítulos siguientes: En el primero se analiza la poesía popular y Carbajo disecciona en profundidad esta parte básica de nuestra lírica y la historiografía a ella aparejada, haciendo especial hincapié en la concepción romántica, y distinguiendo siempre entre las coplas de autor y las de carácter anónimo. El segundo está dedicado al repaso sistemático de la poesía flamenca y cuanto de ella se ha escrito, estableciendo comparaciones con la anterior y revisando uno a uno los repertorios en los que la primera se ha difundido entre nosotros. En los capítulos tercero y cuarto se analizan respectivamente los aspectos temáticos y formales de ambas modalidades líricas, la popular y la flamenca, desgranando el primero toda una serie de temas (amorosos, religiosos, festivos, etc.), mientras que el segundo pasa revista a las diferencias entre verso y estrofa, o analiza unos y otras a la luz de los niveles fónicos, morfosintácticos o semánticos. El capítulo quinto y último es el que, a\n\n[ENDING CONTEXT]\n\no Martinete), a los intérpretes (Carmen Maya) e incluso a los instrumentos que completan el ritual flamenco (castañuelas, peineta, bata de cola...). Para cerrar, la impresionante Misa Flamenca que fuera grabada por Fonogram en marzo de 1966 y que es de las pioneras en el género.\n\nSuma de sentimientos jondos bien asimilados, este libro es la más palpable de mostración de que al flamenco es fácil aproximarse desde puntos de vista diferentes y complementarios, ya sean éstos los del investigador o simplemente los del artista que se siente herido ante el rayo mortal de la interpretación jonda.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Bibliografía",
    "periodical": "candil",
    "issue_id": "1990-09",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 1617,
    "article_char_count_full": 9788,
    "article_char_count_review": 3396,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "improvisado"
      }
    ]
  }
]
```
