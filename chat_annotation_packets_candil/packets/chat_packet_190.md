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
    "article_id": "1989-05-11-left-la-propaganda-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nProgramas y carteles 1867-1984\n\nJosé Blas Vega\n\nA lo largo del tiempo, la propaganda del flamenco y la imagen gráfica del mismo ha tomado las más diversas y curiosas formas de impresión y realización, a través de cromos, cajas de cerillas, abanicos, entradas de toros, etiquetas de vinos, cajas y envoltorios de pasas, azulejos, figuras de barro, postales, dibujos, grabados, fotos, carteles, cuadros, tablitas, castañuelas, panderetas..., con un sentido artístico en la mayoría de los casos de neta originalidad andaluza, al querer representar los distintos mensajes que encierra el flamenco, desde el matiz más ingenuo hasta la carga más expresiva. Todo un atrayente mundo de colorido, sugerencias e información, del que hoy extraemos, como muestra imperecedera, una de las manifestaciones\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\narteles, cuadros, tablitas, castañuelas, panderetas..., con un sentido artístico en la mayoría de los casos de neta originalidad andaluza, al querer representar los distintos mensajes que encierra el flamenco, desde el matiz más ingenuo hasta la carga más expresiva. Todo un atrayente mundo de colorido, sugerencias e información, del que hoy extraemos, como muestra imperecedera, una de las manifestaciones estéticas que en la actualidad cuenta con gran interés y colecionismo, consecuente con la realidad cultural que en sí encierra el cartel como conjunto y contenido de actividades artísticas. El cartel, que según el Diccionario de la Real Academia Espa- ñola, es el «papel, pieza de tela o lámina de otra materia escrita que contiene noticias, anuncios, propaganda, etc..., y se exhibe eventualmente», nace por una necesidad originada en el comercio y en su relación social. Su trayectoria histórica, partiendo ya de los famosos pasquines romanos, ha pasado por distintas fórmulas, sobre todo a partir de 1789 en que Aloy Senefelder inventa la litografía, que con su posibilidad técnica juega un papel decisivo. Desde 1827 la cromolitografía o litografía en color y los intentos de anuncios comerciales de grandes dibujantes como Devería, Lalance y Manet, abren la puerta al gran cartelista y litógrafo francés Jules Cheret (1836-1933), para crear (1869) el cartel moderno ilustrado, que llega a su cumbre con Toulouse-Lautrec en 1891. En España, durante el siglo XVIII, el cartel funcionó como aviso o anuncio comercial, de sencilla composición tipográfica, y, entre los que se conservan, abundan los de tema teatral y los taurinos anunciando corridas de toros, cuyo primer cartel conocido data de 1761. A lo largo de ese siglo y parte del XIX, los carteles se irán ornamentando tipográficamente con orlas, viñetas y pequeños grabados, constituyendo este tipo de carteles el antecedente más antiguo del cartel ilustrado español. Y es precisamente en esta época cuando arranca la prehistoria del cartel flamenco, con todo un hallazgo de\n\n[ENDING CONTEXT]\n\nimportantes en la pintura española contemporánea, pero sí vamos a señalar que en la mayoría de estos casos se da en ellos la condición de excepcionales aficionados al flamenco, de auténticos cabales. Y presididos y simbolizados por un artista vinculado especialmente a Granada y al flamenco: nos referimos al pintor universal Manuel Ángeles Ortiz, quien no podía sospechar al pintar, en 1922, el famoso cartel del Concurso, que sesenta y dos años después, con sus últimos trazos inacabados, acudiría de nuevo a una cita con su arte, con su tierra y con su cante: la Pintura, Andalucía y el Flamenco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La propaganda flamenca",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "11-13",
    "page_number": 11,
    "word_count": 2717,
    "article_char_count_full": 16982,
    "article_char_count_review": 3661,
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
    "article_id": "1989-05-13-right-la-osada-ignorancia",
    "article_text_for_review": "En la Universidad que se llama Don Antonio Mairena habita todo el Cante. Cuanto no recuerde Mairena no lo recordaremos nunca.\n\nFélix Grande\n\nE n el anterior número de Sevilla Flamenca dos periodistas radiofónicos acaban de irrumpir con estrépito en estas páginas dedicando nada menos que catorce (casi el 25% del espacio total de la revista) a una entrevista al joven cantaor Juan Moreno Maya, El Pele. Nunca se vio caballero, de damas tan bien servido... La aparición de la entrevista, surtida con siete fotografías en las que se ve en variadas posturas al aspirante a Maestro, se ha producido días después de su presentación ante el público de Mairena del Alcor en los locales de la Casa del Arte Flamenco.\n\nEl comentario que podríamos hacer de esta presentación no resulta ahora aconsejable. Digamos que, en puridad, fue muy aplaudido a pesar de que toda su actuación no se distinguió precisamente por la moderación, aunque a nuestro juicio se apreciaron evidentes progresos en su mejorada técnica interpretativa, con bastantes posibilidades por delante y, en contraste, haciéndose evidente la falta de una tutoría correctora de defectos propios de la inexperiencia.\n\nNi estas consideraciones —que obviamente no tiene por qué compartir El Pele ni sus esforzados entrevistadores y paisanos— ni otras que aconseja la prudencia, la moderación y la mesura a partes iguales, han puesto coto a unos pensamientos expresados con la rotundidad de quien ya se tiene por Primum inter pares y no tiene ningún recato en calificar e incluso descalificar a tirios y tro\n\nyanos. El Pele, no ciertamente con demasiada habilidad de parte de sus entrevistadores y paisanos, entra siempre por derecho al trapo que se le ofrece para que pueda emular las glorias pretéritas de El Guerra y Manolete y se lanza incontenido y desbordante:\n\nMairena ha dejado una escuela... Una escuela fácil de aprender, con esquemas fáciles. Porque Mairena, queramos o no queramos no ha sido un hombre de pellizco, no ha sido un cantaor de «voz calentita», sí ha sido un hombre que todo lo ha hecho perfecto. Los aficionados cuando cantan suenan, creo, algo a Mairena. Es fácil sonar así.\n\nNos imaginamos a El Pele dando un suspiro de satisfacción en un gesto de sabiduría infinita y ponderación cordobesa, y a sus entrevistadores y paisanos con los ojos brillantes y un gesto de malicia como si pensaran: ¡Hay que ver lo que ha aprendido este niño! (En otro lugar de la entrevista el joven cantaor cordobés declara que él ha bebido en las fuentes de Juan Talega, en las de Caracol, «ese pedazo de genio que se nos fue»; ha bebido de Mairena, El Perrate, Matrona... Cuando él asegura que ha bebido en las fuentes de Mairena, habrá que creerlo. Si no canta como Mairena será porque no quiere, es decir, porque lo considera demasiado fácil).\n\nLos entrevistadores y paisanos siguen jugando al ratón y el gato y le tienden la celada de un Juan Talega desprendió (sic) que puso todo su cante en manos de Mairena «que Mairena se las atribuye» e inmediatamente salta el espíritu valeroso y magistral del joven cantaor cordobés: Exactamente. Es en eso en lo que yo quería hacer hincapié. Él nunca decía esta segui-riya o este cante es de Juan Talega o del tal otro, sino que no ponía nada y al no poner nada se quedaba eso así, que lo había hecho Mairena y ya está.\n\n(Curiosamente este párrafo se termina en boca de El Pele con una declaración de fanático enorme de Antonio Mairena... como artista. De Antonio Mairena como artista, repite).\n\nDe modo que ahora descubren los sagaces entrevistadores y paísanos de El Pele que Antonio Mairena ocultó siempre todo lo que había aprendido de Juan Tallega (quien en un disco memorable que seguramente ni El Pele ni sus amigos cordobeses habrán escuchado, por aquello del cante calentito, jalea con su voz de campana gorda: «¡Antonio Mairena, el mejón de tos los tiempos!».\n\nAntonio Mairena nunca dijo este cante es de Juan Talega o del tal otro. Antonio Mairena se atribuyó los cantes de Frijones, de Paco la Luz, del Loco Mateo, del Viejo de la Isla, del Ciego de la Peña, de Manuel Molina, de José de Paula, de Joaquín Lacherna, de Enrique El Mellizo, de Francisco La Perla, de Perico Frascola, de José Iyanda, de Joaquín de la Paula, de la Roezna, de Teresita la de Mazzantini, de Juaníquí, de Pastora Pavón, de Curro Dulce, los de Cagancho, de Manuel Torre, de la Serneta, del Marrurro, del Cojo Pinea, de Jilica de Marchena... ¿seguimos?. No, vamos a dejarlo ahí. Todos estos nombres los ocultó Antonio Mairena, recogió a su escuela fácil todos esos estilos y otros tantos más, ocultó a sus creadores y se los apropió como invento suyo: ¡hay que ver lo que sabe El Pele! (Y lo que no sabe y, sobre la marcha, se lo apuntan sus esforzados entrevistadores y paisanos).\n\nVivir para ver.",
    "title": "LA OSADA IGNORANCIA",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 823,
    "article_char_count_full": 4785,
    "article_char_count_review": 4785,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-14-left-la-malague-a-es-de-m-laga",
    "article_text_for_review": "Opinión\n\n(Se creó, nació y creció en Málaga)\n\nFernando Durán\n\nE n un programa emitido por la 2.ª cadena de T.V.E. y que se llamó «CAMINOS FLAMENCOS», auténticos bodrios por lo mal tratado que fue el arte flamenco y en el transcurso de uno de ellos dedicado a Jerez y no tuve ocasión de ver en su día, por motivos que no vienen al caso, pero que sí he visto hace unos días, don Juan de la Plata fue interrogado por el locutor sobre el insigne cantaor jerezano, don Antonio Chacón y contestando muy solemnemente y sentando cátedra, contestó así:\n\n«...D. Antonio Chacón, de un fandango, creó la malagueña...»\n\nY la cámara, locutor e interviudado, pasaron a otro tema, sin más comentario, quedando así la cosa. Como esta aseveración u opinión de don Juan de la Plata, puede inducir a error a los no iniciados en el arte flamenco, deseo putualizar algo sobre el tema de las malagueñas y los malagueñeros, para que cuando se diga algo para millones de personas, sea objetivo e imparcial.\n\nDeseo en primer lugar, expresar mi extrañezas, ante el hecho evidente, que ninguna peña flamenca, malagueña o foránea, no haya salido en defensa de la verdad de la naturaleza de la malagueña. Y más aún no lo haya hecho esa peña veterana y de gran solera que dispone de ilustres y documentados investigadores. Por este motivo, un modesto componente del Aula Universitaria de Flamencología, le va a recordar a don Juan de la Plata, algo que vivimos juntos.\n\nHace ya varios años (si no recuero mal fue en 1984) el Aula Universitaria de Flamencología, de Málaga, se desplazó a Jerez de la Frontera, para un intercambio\n\ncultural: visitar y conocer la Cátedra de Flamencología y celebrar posteriormente una mesa redonda sobre «Los cantes de Málaga». Por parte de la Cátedra de Jerez, estuvieron presentes don Juan de la Plata, el guitarrista jerezano «Parrilla de Jerez», miembros de la Cátedra y miembros de la Peña «Los Cernícalos».\n\nPor parte del Aula de Flamencología de la Universidad de Málaga, lo hicieron su director don Alfredo Arrebola, don José Cueto, y el que esto suscribe, amén de numerosos componentes del Aula de Flamencología. En el transcurso de la reunión, quedó bien claro el por qué don Manuel Machado la llamó «...Málaga cantora...». En dicha mesa redonda pasamos desde el desgaje verdialero a los cantes abandolaos (jaberas, jabegotes, rondeñas, hileros, etc.) hasta llegar a la joya de los cantes malagueños que supuso la\n\naparición de la malagueña, en dos zonas tan diametralmente opuestas como son la Axarquía veleña y la perota de Alora. Ya a estas alturas de la tertulia, se sacó a colación el tema de don Antonio Chacón, quedando bien patente y suficientemente aclarado, la dilatada presencia en la ciudad de Málaga del gran maestro jerezano. Aquí se empapó de los cantes de la Trini, de los que se declaraba perdidamente enamorado musicalmente hablando. Tanto las malagueñas de «La Trini», como las de «El Perote», «El Caribe», Baldomero Pacheco, del Maestro Ojana, de «La Chirrina», de «La Chilanga», las adaptó a sus portentosas facultades, a su gran personalidad, dejó amoldados a su gusto y a su forma de interpretar el cante. Así sacó del entorno local y provinciano, unos cantes que derivados de un folclore primitivo, pero nacidos en Málaga, elevó a una categoría suprema dentro del cante flamenco; lo mismo que también llevó a cabo con todos los cantes derivados del tronco malagueño y conocidos como «Cantes de Levante»: Granaínas, medias granaínas, cartageneras, murcianas, mineras, tarantas, etc., dotando a estos cantes de tal personalidad, que hoy cuando se cantan y se quieren semejar a tan altas cumbres, se dice «...al estilo de don Antonio Chacón...».\n\nMe remito a las autorizadas opiniones de don José Luque Navajas y a su libro «Málaga en el cante» y a don José Blas Vega y a su libro «Vida y cante de don Antonio Chacón», libro este último por una beca de la «Semana de Estudios Flamencos» de Málaga y Premio de monografías sobre temas flamencos Antonio Machado y Álvarez «Demófilo».",
    "title": "La malagueña es de Málaga",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 681,
    "article_char_count_full": 4011,
    "article_char_count_review": 4011,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-05-14-right-sentimiento-y-raz-n",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLuis Caballero\n\nSeguimos repitiendo el símil, el ejemplo, la comparación más próxima al fenómeno desde sus más profundos orígenes a sus más recientes florecimientos: el Cante, un árbol frondoso de vida milenaria desde sus más ocultas raíces a sus más altas y últimas ramas. Arbol, más que extraño, invisible para muchos, uno más, entre tantos, para otros y el único en su especie para pocos. Un árbol cargado de coplas, de gritos, de penas, de gracia y de arte. Un árbol movido por los cuatro vientos sentimentales de la tierra donde crece vivificado por las voces espirituales que lo cantan.\n\nA su sombra se cobija todo un mundo de contrariedades humanas movidas por fanatismos, pasiones, gustos, conceptos, sentimientos, chavacanerías y elegancia. ¡Qué difícil encontrar un solo corazón inmerso en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"profundo\"]\n\ntos, conceptos, sentimientos, chavacanerías y elegancia. ¡Qué difícil encontrar un solo corazón inmerso en el laberinto del cante con poder de asimilación total, capacitado cultural y sensiblemente para «digerir» el resumen de todas las tendencias, aires y estilos. Pero... cada cual proclama su razón como única: «Que tos llevamos razón / cuando la razón nos brota / del centro del corazón». Sin embargo «el Cante viene de los más hondo, de lo más profundo de la pena». «Nuestra tierra es nuestro cante, es nuestro hombre, es nuestra singular historia a lo largo de la pena negra y su olvido. Por eso hoy como ayer, como siempre, queremos (debemos) todos, rendir homenaje a un pueblo tan creativo que tiene fuerzas para cantar y convertir lo negativo en bueno». Los cantes son todos buenos. El cantaor será quien los haga malos; evidencia que ya he oído repetir hasta a entusiastas ni tan siquiera españoles. ¿Por qué entonces los propios aficionados y profesionales autorizados siguen establecien- do distinciones discriminatorias? «¡Qué buen cante tenéis!», dijo Mairena en Levante, mientras que, por el contrario, un mairenero mairenista de Mairena me llamó a mí, despectivamente, amarchenado porque cantaba algo de Levante en los festivales. (No tiene ninguna importancia si pensamos por un instante que el hombre puede llegar a ofender a Dios, para con más fuerza y fervor gritar viva la Virgen\n\n[ENDING CONTEXT]\n\nrazón por la que la minoría perdura y pervive más resignada que consecuente con el mal gusto facilón y comercial que salpica y molesta a nuestro arte espiritual en su más depurada exquisitez.\n\nYa sabemos, por experiencia ajena e íntima, que el cante es casi siempre un milagro, un arte que sólo pueden comprender totalmente los que lo sienten, pero el ARTE es la expresión de una emoción con medios inteligentes e intuitivos, una presencia cultural sobrada de sentimientos e imprescindiblemente necesitada de razones.\n\nO'Donnell, núm. 3 - 4.º Teléfs. 222058 - 216920\n\nPaticular: Teléf. 228078\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sentimiento y razón",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 991,
    "article_char_count_full": 6088,
    "article_char_count_review": 3024,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "profundo"
      }
    ]
  },
  {
    "article_id": "1989-05-16-left-siguiriyas-nombre-de-un-cante-pl",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOpinión\n\nPedro Camacho Galindo\n\nE n esto de ponerle nombre a los cantes y, sobre todo, a los de elaboración gitana, hay un antecedente común muy aleccionador: ninguna de las denominaciones más o menos descifradas hasta ahora, las tomaron de la métrica o estrofa en que se cantan. Los que más y los que menos han tenido otra motivación: su temática (carceleras, martinetes); una frase o nombre que se destaque en alguna de sus coplas (caña, soleá, petenera); y en los cantes de más pura ascendencia andaluza, el apelativo toponímico (malagueña, granadina, cartagenera); sus características expresionales (alegrías, jaleos); su funcionalidad (trillera, calesera, nana, minera) y, en fin, todo, menos su relación con el carácter literario de sus versos.\n\nEsta experiencia nos da la voz de alerta ante\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_01 | trigger=\"fuera\"]\n\na voz de alerta ante las hipótesis ofrecidas para desentrañar el nombre del príncipe de los cantes: las siguirias gitanas. Hasta donde se conocieron como «playeras» todo parece estar descubierto. Serían al principio una «tonás tristes», más del grupo especial de las llamadas, en la célebre relación de Juanelo, «tonás», calificativo que les iría como anillo al dedo por su temática y melodía quejumbrosa. Que una vez desprendidas del seno materno, fueran conocidas como «playeras» (de plañir), con cuyo nombre se las distinguió hasta bien entrado el siglo XIX, nos lo ilustra con lujo de rigor documental y musicográfico el maestro García Matos en el «Bosquejo histórico del cante flamenco» que sirve de introducción al álbum «Una historia del Cante Flamenco» de Hispavox, en el que discurre sobre la conexión literaria entre algunos tipos excepcionales de seguidillas antiguas, que se manifiestan en estrofas de cuatro versos de seis sílabas y se confunden con las «endechas», es decir, con las «canciones que en los pasados siglos acostumbraban a en tonar en los cortejos fúnebr\n\n[ENDING CONTEXT]\n\nidiomática de las seguidillas, o ya por su parecido con las «sevillanas», no en su aspecto estrófico o literario, sino en la forma de ejecutarse. Téngase en cuenta que las «seguidillas» sevillanas son un cante seriado de varias coplas que se cantan y bailan de seguido y de diferentes tonalidades, y que, por ello, precisamente, es por lo que se llaman, de primer nombre, seguidillas.\n\nAdviértase, además, el modo de cantarse en la actualidad las siguiriyas, utilizando, con más o menos rigor clásico, ese sistema de integración de varias coplas, en diferentes tonos y con un remate de cambio.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Siguiriyas: nombre de un cante plural",
    "periodical": "candil",
    "issue_id": "1989-05",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1099,
    "article_char_count_full": 6686,
    "article_char_count_review": 2701,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "fuera"
      }
    ]
  }
]
```
