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
    "article_id": "1993-09-10-left-joaqu-n-jim-nez-dom-nguez-el-sal",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Valera Espinosa\n\nCiertamente parco en palabras, con semblante serio y responsable, orgulloso de su bagaje artístico a pesar de su juventud —pues a los catorce años consiguió un premio por siguiriyas en Mairena del Alcor—, enamorado del arte de su cuna flamenca, Jerez, Joaquín Jiménez Domínguez «El Salmonete de Jerez», responde a las preguntas de «Candil» con la sinceridad de un cantaor que busca un camino legítimo para triunfar. Desecha los falsos halagos, las hueras promesas y antepone la ortodoxia del cante como baluarte principal en que basar la creatividad flamenca. Admira a los grandes maestros de su tierra, siente pasión por la obra flamenca de Juan Talega o Antonio Mairena y reconoce los méritos artísticos de Fosforito o Camarón. Pasea con sencillez su triunfo en los\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nsiente pasión por la obra flamenca de Juan Talega o Antonio Mairena y reconoce los méritos artísticos de Fosforito o Camarón. Pasea con sencillez su triunfo en los concursos cordobeses del 89 y el 92 y mantiene una especial inclinación por soleá, siguiriyas y bulerías. -¿Por qué lo de Salmonete? —Viene porque tengo el pelo rojo, soy de una tez rojiza y por eso me dicen «El Salmonete» desde pequeño. Y lo cierto es que el apelativo me lo puso un gran cantaor de Jerez que se llamaba Fernando Terremoto. —¿Has tenido antecedentes cantaores familiares? —La verdad es que no, aunque mi madre siempre ha tenido fama de buena saetera, ganando bastantes premios. Profesionalmente hablando no ha existido ninguno. Eso sí, buenos aficionaos sí que los ha habló. Actualmente, mi hermana la chica está cantan- do muy bien. Te puedo decir que el primer artista profesional en la familia he sido yo. Comencé cantando saetas desde muy chiquitito, luego vi que me gustaban otros cantes y comencé a cantiñarlos. Más tarde fui a Mairena del Alcor y me traje «ese» premio, en el año 77. A partir de entonces me he dedicado a cantar. -¿Qué escuela flamenca sigues? —Mi escuela es la de los cantaores que sienten el cante, que lo viven, que pellizcan... Me gustan Manuel Torre, Juan Talega, Juanito Mojama, Agujetas el Viejo, su hijo Manuel, de Caracol hay cositas que me llegan y, por supuesto, Fernando Terremoto. Yo creo que son cantaores que han dicho mucho, que transmiten... Con los cantaores que dicen algo es cuando empieza a funcionar el vello. También me gustan Fosforito y Antonio Mairena. -¿Cómo fue lo del concurso de Mairena? —Tengo un gran amigo mío que se llama Alfredo Benítez. Este hombre me preparó porque vio que yo tenía cualidades, fuimos a Mairena y conseguí el premio por siguiriγas. Después me retiré un poco de las actuaciones y ambientes flamencos porque hubo algunos que me trataron mal de palabra. Sin embargo, más tarde, Alfredo Benítez vuelve a animarme para que me presente al concurso de Córdoba, y después de pensármelo bastante decidi presentarme y conseguí el premio Manuel Torre en el año 89. Fue la primera vez que me\n\n[ENDING CONTEXT]\n\ndel mundo es cantar bien por bulerías. La siguiriya es un cante de transmisión, fuerte, de pena, de tristeza; sin embargo, la bulería es un cante alegre, de fiesta... A pesar de que la siguiriya es un cante muy de Jerez, tienes que pensar que hay muchos cantaores jóvenes en mi tierra y los jóvenes lo que queremos es fiesta, alegría, juerga, divertirnos... Entonces es por eso que las bulerías se cantan más. Los padecimientos de los antiguos cantaores no son los mismos que los de ahora, y aunque también se canta bastante por siguiriyas, por lo que te he dicho antes, se prefieren las bulerías.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Joaquín Jiménez Domínguez «El Salmonete» Rafael",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 1633,
    "article_char_count_full": 9237,
    "article_char_count_review": 3755,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "1993-09-12-right-deontolog-a-y-funciones-de-la-cr",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAsociación de Críticos de Arte Flamenco (A.C.A.F.). Ponencia pronunciada en la Asamblea Constituyente. Jaén, del 23 al 25 de octubre de 1992\n\nSi entendemos por crítica flamenca el arte de analizar una manifestación universal parida en el seno de Andalucía, por la que traducimos al lenguaje de las palabras el lenguaje de los sentimientos, mucho me temo que hablar de la crítica sea poco menos que expresar un imposible.\n\nPor el contrario, si aceptamos que no existe el conocimiento flamenco sin un conocimiento crítico, y admitimos que el crítico de flamenco es, ante todo, un aficionado cualificado que, por ende, está autorizado a ofrecer apreciaciones de validez objetiva, pero, sobre todo, es aquel agente de un medio de comunicación que con periodicidad contempla, estudia y da a conocer el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"crítica\"]\n\nr un imposible. Por el contrario, si aceptamos que no existe el conocimiento flamenco sin un conocimiento crítico, y admitimos que el crítico de flamenco es, ante todo, un aficionado cualificado que, por ende, está autorizado a ofrecer apreciaciones de validez objetiva, pero, sobre todo, es aquel agente de un medio de comunicación que con periodicidad contempla, estudia y da a conocer el Flamenco como fenómeno artístico, podemos convenir que la crítica, en sentido lato, no es más que la capacidad de un juicio de valor en torno al hecho flamenco. Partiendo de esta premisa, la crítica en general aparece en el siglo XVIII, pero la figura del crítico flamenco propiamente dicho, creemos que arranca en 1935 con el libro Arte y artistas flamencos, de Fernando el de Triana, y se afirma en el período de los Festivales Flamencos con el I Potaje Gitano de Utrera, iniciado el 15 de mayo de 1957 tras la primera salida penitencial de la Hermandad de los Gitanos. Es poesía determinada fecha —un año después del I Concurso de Córdoba y poco antes del manifiesto de la Cátedra de Flamencología de Jerez y d\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_03 | trigger=\"pública\"]\n\n, estamos luchando contra la intoxicación y la compra de voluntades, combatimos la desinformación con información y, aun a riesgo de la integridad física, enarbolamos la bandera de que silenciar la verdad no es más que confesar la hipocresía y alimentar la corrupción. Y todo porque partimos de que en Flamenco, como en cualquier orden de la vida, una de las formas de amarillismo informativo más repugnante es la de tratar de instalar en la opinión pública una idea fragmentada y, por lo tanto, falsa de cualquier realidad. Mismamente, hemos procurado evidenciar que la crítica no debiera limitarse solamente a comprobar el hecho flamenco, sino que estaba obligada, además, a desentrañar sus causas y justificaciones. Se ha llegado, asimismo, a precisiones lógicas y científicas, y, aunque seguimos siendo conscientes de que la crítica ejerce una escasa influencia sobre la actividad creadora, juzgamos con cierta anticipación el desarrollo de algunos hechos artísticos y asistimos tanto al resurgimiento y posterior afirmación de figuras como\n\n[ENDING CONTEXT]\n\nde la función crítica.\n\nPero carecería de sentido este intento histórico de asociación, si no rechazamos también a quienes se afanían en el trabajo sucio de meros repartidores de publicidad, a los voceros de la mediocridad, a aquellos que sólo saben reaccionar agachando la cabeza y a quienes han perdido el sentido del valor flamenco... En definitiva, y como dice mi admirado Manolo Urbano, la futura ACAF, llegado el caso, ha de estar dispuesta a denunciar a los granujas, si los hubiera, pero también a avalar continuamente a los honrados, con independencia de su leal saber y entender. Gracias.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Deontología y funciones de la crítica flamenca Manuel",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "12-15",
    "page_number": 12,
    "word_count": 3700,
    "article_char_count_full": 22752,
    "article_char_count_review": 3842,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "crítica"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "pública"
      }
    ]
  },
  {
    "article_id": "1993-09-16-left-el-coraz-n-manda-rafael",
    "article_text_for_review": "D entro del colectivo flamen-co siempre han marcado pauta como aficionados, ilustres profesionales de las letras por su asidua inclinación a saborear y deleitar con entusiasmo el quejio de una siguiriya, el acompasado tercio de una soleá o el festero ritmo de una bulería. Por lo general, la mayoría de estos aficionados pertenece al grupo de las letras, y sin embargo, cuando de las ciencias se trata, y en particular de la medicina, el aprecio por parte del resto del colectivo parece que se acrecienta. Cierto es que esa estimación\n\nse establece pluralmente en toda la población ante la seguridad que su profesión supone para todos nosotros, mas que un médico sea aficionado al flamenco y además posea los suficientes conocimientos sobre nuestro arte para darnos a más de uno un buen repaso, ello ya no es tan habitual.\n\nVarios son los nombres que me vienen a la memoria y que por sus investigaciones y dedicación están reconocidos como expertos conocedores del flamenco. Así los doctores Luque Navajas, Antonio Reina, Rodríguez Grande, Fernan-\n\ndo Lastra o Zambrano Vázquez, son claros ejemplos de lo expresa- do anteriormente.\n\nLa introducción está motivada por el homenaje que en Córdoba se le ha rendido a uno de estos hombres, el cirujano cardiovascular Manuel Concha Ruiz. El acto fue organizado por la Peña «Rinción del Cante», de Córdoba, a instancias de su presidente Miguel López Fernández. El mismo se celebró el sábado 18 de septiembre en el Palacio de Viana de la ciudad, contando con la participación de los artistas Luis de Córdoba, Juan Moreno Maya «El Pele» y David Pino al cante; Inmaculada Aguilar y su cuadro, al baile, y Manuel Silveria, Paco Serrano y Alberto Lucena, al toque. El evento fue presentado por Agustín Gómez.\n\nQuizá la crónica del homenaje pueda sintetizarse en las líneas que abren el librito que a tal fin, con colaboraciones muy especiales de artistas, literatos, magistrados, médicos, etc., se editó unas fechas antes y que, escritas por Miguel López, dicen: «A don Manuel Concha Ruiz, un hombre cabal, bondad, serenidad, inteligencia...; corazón en armonía con unas manos prodigiosas, un fuerte asidero de la Córdoba actual..., de sus amigos del Rincón del Cante».\n\nAparte de la serie de virtudes que Miguel vierte sobre la personalidad de tan afamado cirujano, las cuales son meritorias por sí mismas para emprender una tarea como la desarrollada, pienso que de lo que se trataba en esta ocasión era de llegar al corazón del ilustre aficionado de una forma artística, placentera, sentimental y netamente flamenca. Sus palabras de agradecimiento por el homenaje, después de numerosas adhesiones y muestras de reconocimiento personales en el propio escenario, evidencia-ron, por la sentida emoción con que fueron pronunciadas, que se había tocado la fibra sensible de un humanista. Hasta aquí la parte sentimental y placentera del homenaje; la artística y flamenca vino después.\n\nUn joven David Pino, con la guitarra de Alberto Lucena, abría el homenaje cantaor. Sus iniciales nanas fueron desarrolladas con ecos matizados y melodiosos, aunque con cierto apresuramiento al final. En las marianas mostró un tratamiento evocador de ciertos ecos cordobeses y reminiscencias de Bernardo el de los Lobitos. Finalizó con unas alboreás a través de tangos, con facultades y entonación, mas el citado compás le da-ba cierta rareza al estilo. Buen acompañamiento el realizado por Alberto Lucena, con un magnífico arropo al cantaor en las nanas.\n\nCon el melismático tratamiento que Luis de Córdoba da a los estilos considerados como libres, el de Posadas cantó primeramente por granaína y media granaína. Sus cantiñas-alegrías denotaron cierta rapidez al comienzo para posteriormente ir acrecentando su cali-\n\ndad cantaora, resolutivo compás y adecuado remate. El conocimiento de los localismos y personalismos en los cantes mineros fue expuesto con melisma personal y evocaciones levantinas y linarenses. Prosiguió en su línea habitual por colombianas y finalizó con fandangos, brillando en la rememoración de Juan Varea. La guitarra de Paco Serrano sonó comedida, denotando virtuosismo en las alegrías y una adecuada sincronización con el cantaor en los estilos mineros, colombianas y fandangos.\n\nLa habitual puesta en escena de Juan Moreno Maya «El Pele» tuvo en esta ocasión más dosis de modernismo? flamenco. Al margen de comenzar su actuación con la acostumbrada zambra caracolera y derivar seguidamente a las siguiríyas, tuvo un acompañamiento sustancioso: la guitarra de Manuel Silveria, la flauta de José Manuel Hierro, la percusión de Pachi y el violonchelo de Nicasio Moreno. ¿Que cómo se puede cantar siguiriyas con semejante acompañamiento? Pues como lo hizo El Pele, desarrollando su personalidad flamenca a los acordes del toque por siguiriyas y dejándose arrastrar por la musicalidad que los restantes instrumentos le ofrecían entre tercio y tercio. Mas su enduenda quejó surgió una y otra vez, ofreciendo unos ecos gitanísimos por la siguiriya y cambio de Manuel Molina. En los tientos-tangos volvió a mostrar su dualidad flamenca: ortodoxia y modernidad. Sus personales ecos y las evocaciones de Caracol y Pastora, fueron las características más sobresalientes del estilo. Las cantiñas-alegrias abundaron en su juego melismático, con ciertas fases estentóreas y singular rememoración del Chaqueta y Caracol. Hubo cierto olvido del compás al principio de las bulerías y posterior brillantez al centrarse en los ecos jerezanos.\n\nLa elegancia del baile de Inmaculada Aguilar se dejó ver a través de unas acompasadas alegrías, en las que mostró su plasticidad festera y una armonización artística de la figura rayana en la perfección. Su fuerza en el taconeo fue otra característica destacada.",
    "title": "El corazón manda Rafael",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 906,
    "article_char_count_full": 5738,
    "article_char_count_review": 5738,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-09-17-right-aunque-no-quepa-en-el-papel-jos-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nConocíamos los planteamientos básicos del libro a través de la entrega adelantada que los autores presentaron como Ponencia al Congreso de Arte Flamenco de Huelva en el verano de 1992. Ahora, corregidas y aumentadas sus referencias, Molina y Espín publican este libro importante acerca de esos estilos flamencos que algunos consideran menores, o simplemente no flamencos, y defienden su especificidad, el virtuosismo de sus interpretaciones si se ajustan a la línea melódica básica, y, en fin, su pertenencia al viejo tronco de nuestra cultura.\n\nPara analizar estos cantes, pasados por la influencia americana, los autores comienzan por discutir la misma denominación, que el tiempo ha consagrado, al llamarles «de Ida y Vuelta»; planteamientos del estado de la cuestión que lleva a algunos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_01 | trigger=\"verdadero\"]\n\nultura. Para analizar estos cantes, pasados por la influencia americana, los autores comienzan por discutir la misma denominación, que el tiempo ha consagrado, al llamarles «de Ida y Vuelta»; planteamientos del estado de la cuestión que lleva a algunos estudiosos a negarles el carácter «de ida» y considerarlos sólo «de venida», esto es, se trataría de estilos nacidos en América, que más tarde llegan a España y se aflamencan, o, lo que es igual, verdaderos cantes de importación. En el presente libro no se apuesta por una resolución globalizadora del dilema, sino que se procede examinando cante a cante, puesto que los autores no dudan en conceder, por poner un ejemplo, la denominación de «Ida y Vuelta» a la Guajira, basándose en el hecho de que, desde época muy temprana, en el Madrid del siglo XVIII, se introducían guajiras en determinadas tonadillas escénicas que, más tarde, cruzarían el charco, se impregnarían de ritmos nuevos y acabarían Flamenco de Ida y Vuelta Romualdo Molina y Miguel Espín VII Bienal de Arte Flamenco. Sevilla, 1992 por ser aflamencadas en Andalucía. No sucede igual con el Tango, al que los autores desvinculan de la tan repetida procedencia gitana, insistiendo en el hecho de ser utilizados en España mucho antes del siglo XV, fecha de la venida de esta etnia a nuestro país; un estilo que más tarde viajaría a América con los colonizadores, creando\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\nrte. En el mundillo jondo, como en el de las letras o las artes plásticas, hay biografías más o menos expresivas, capaces por ellas mismas de llenar el vacío tremendo que ocasiona la muerte de sus intérpretes. Qué duda cabe que glosar la biografía de Miguel Angel, Cervantes o Silverio Franconetti, es aportar datos decisivos para el esclarecimiento definitivo de las claves más recónditas de la pintura renacentista, la novela burguesa o la etapa primitiva del flamenco. Pero también sucede que hay otros artistas de los cuales es mejor recordar las aportaciones específicas que incorporan al arte que practicaban, que insistir en aspectos biográficos, más o menos conocidos y, en todo caso, irrelevantes, si se comparan con la inmensidad de su arte. Hablar de la biografía de Diego Amaya Flores, más conocido como Diego el del Gastor, pertenece a la segunda de las categorías. Con un arte inmenso en e\n\n[ENDING CONTEXT]\n\nsencillez expresiva que sirva para engastar en los mismos los ribetes de su sensibilidad. Aunque Agustín Gómez, en su empeño por resaltar la eficacia cantaora, niegue contenidos poéticos a la propuesta del letrista, la poesía verdadera asoma con frecuencia en este modesto joyel que se pone al servicio, como quería nuestro viejo Arcipreste de Hita: «de todo aquel que bien trovar supiere». Baste como muestra, la bella imaginería de este fandango de Huelva:\n\nPor los árboles que arrancan se miden los vendavales, por la rabia y los dolores de los celos de mi amante la hondura de sus amores.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel... José Luis",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1375,
    "article_char_count_full": 8612,
    "article_char_count_review": 3970,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_01",
        "family": "AUTH",
        "trigger": "verdadero"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      }
    ]
  },
  {
    "article_id": "1993-09-18-right-alre-de-la-fiesta-gitana-miguel-",
    "article_text_for_review": "Dibujos de Miguel Alcalá del libro «Le Flamenco et les gitans», Editorial Filipacchi, París, Francia, reproducidos bajo licencia del autor.\n\n10 Textos de Manuel Martin Martin\n\nAndorrano.—Francisco Torres Amaya (Morón de la Frontera, 1942). Hijo de Joselero de Morón, sobrino de Diego del Gastor, hermano de Diego de Morón y apadrinado por Curro Travilla. Profundo conocedor de los territorios soleareros de Juan Talega y reivindicando la aspereza melódica de su progenitor, destaca para el público por bulerías, con un estilo peculiar de cante y baile que ha extendido por el mundo, en el que se vislumbra dos clases de ritmos: cinético, producido por la personalidad tan gitana de su baile, y sonoro, nacido de la puridad de unos sonidos emitidos con maternal delicadeza y simpatía tierna, que se distingue por la espontaneidad de sus quiebros. Agustín Ríos.—Agustín Ríos Amaya (Morón de la Frontera). Guitarrista y sobrino por línea materna del gran genio Diego del Gastor. Forjado en la atmósfera espiritual y familiar de su tío, de quien sigue los temas y el estilo, ofrece pocas fisuras en su ejecución, residiendo, a mi juicio, su verdadera trascendencia en la ejemplaridad que muestra por bulería, donde es posible encontrar las claves de un toque a cuerda «pelá» que llega a ser una droga colectiva de alcance insospechado. Es hermano de Pepe Ríos y reside desde hace algunos años en San Francisco, donde compagina las clases con sus actuaciones en solitario.\n\nPepe Ríos.—José Ríos Amaya (Morón de la Frontera). Hijo de Teresa Amaya Flores, hermana de Diego del Gastor, contrajo matrimonio con la gran bulearera Amparo Soto, hija de Manuel Torre. Recorrió el mundo con las compañías de Vallejo, Juanita Reina, Manolo Caracol y Concha Piquer, figurando además en los espectáculos de La Niña de los Peines y La Paquera de Jerez. Por su academia sevillana han pasado artistas de la nombradía de Inmaculada Aguilar, Manuel de Palma, Concha Vargas y Javier Barón, Giraldillo del Baile. Aparte de decir como pocos los cantes de La Moreno, su filosofía del baile se define por una terminante solidez interior y con una arquitectura de movimientos tan perfectos como un friso griego.",
    "title": "Alreó de la fiesta gitana Miguel Alcalá-Manuel",
    "periodical": "candil",
    "issue_id": "1993-09",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "18-21",
    "page_number": 18,
    "word_count": 356,
    "article_char_count_full": 2183,
    "article_char_count_review": 2183,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
