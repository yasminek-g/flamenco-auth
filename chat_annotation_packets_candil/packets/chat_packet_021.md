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
    "article_id": "1981-02-19-right-aunque-no-quepa-en-el-papel-bibl",
    "article_text_for_review": "Aunque no quepa en el papel\n\nSi bien es verdad que cada vez que muere un viejo cantaor es como si un archivo ardiera, en este caso, la sentida pérdida del Matrona deja tras de sí, junto al espanto de su vacío, la 'huella-testimonio de una interesante bibliografía en la que el narrador es el propio artista. Mas si los recuerdos, juicios y biografía del cantaor sevillano despertaron un gran interés por parte de los estudiosos e investigadores flamencos, se impone, por ser de justicia, dejar constancia que el propio Matrona era consciente de la importancia de este caudal para la historia de nuestro arte; de aquí que según confesara a Ricardo Molina en 1.963, se encontraba en plena redacción de sus memorias. ¿Qué fue de ellas? ¿Permanecen inéditas o, por el contrario, fueron utilizadas para otras publicaciones?\n\nEl primer documento literario y gráfico que poseemos sobre el Niño de la Matrona es el que proporcionara en 1.935 —como de tantos otros artistas— Fernando el de Triana en su insustituible libro. Después, que sepamos, poco más que el silencio, hasta que Ricardo Molina en las páginas de «Córdoba» se ocupara de él. Hubo de llegar el «boom» de la Antología de Hispavox y, sobre todo, su descubrimiento (?) —a los ochenta y tres años— por la insustituible grabación de Blas Vega, «Tesoros del flamenco antiguo», para que las riadas de tinta, los homenajes, entrevistas, etc. se sucedieran Y aquí, con esto y una vez más, nos encontramos con la grandeza y miseria de nuestro arte: si José Núñez no supera los ochenta años no se le hubiese reconocido en su valía. ¿Por qué tanta demora para el justo aprecio?\n\nDecididamente, dos son los trabajos monográficos con los que hoy contamos, no sólo para conocer la biografía, andadura y cantes del Matrona; sino para recorrer con ellos una etapa casi igual a un tercio de la historia del cante.\n\nEl primero de ellos, «Conversaciones entre cante y cante», es obra de José Blas Vega —a mi juicio, y si el vocablo se me acepta, el primer «matronólogo»—, que acompaña a las grabaciones de «Tesoro del cante antiguo». Un hermoso trabajo que, como el propio título, indica, más que entrevista es una amenísima conversación en la que el cantaor sevillano, a la vez que recuerda y rememora muchos pasajes de su vida, con inigualable sencillez y gracia, nos va dando noticias abundosas de cantaores, cafés de cante, estilos, etc., etc., que ya sólo habitaban en su memoria.\n\nA esta entrega, sucedería en 1.975 el excelente libro, por varias razones, de José Luis Ortiz Nuevo «Recuerdos de un cantaor sevíllano», en edición de la meritísima «Demófilo» y bajo los auspicios del Centro de Estudios de Música Andaluzay de Flamenco, con el patrocinio de la Unesco.\n\nSi bien este libro incide en lo ya publicado por Blas Vega, de algún modo se nos viene distinto y a la vez uno. Uno —empecemos por\n\naquí—, porque Pepe el de la Matrona era constantemente fiel en todas sus manifestaciones; distinto porque muchos de los datos que sólo apuntara Blas Vega, aquí se desarrollaron plenamente, hasta el punto de valernos por toda una panorámica del cante y una apretada y\n\namenísima biografía de nuestro artista realizada con un estilo coloquial y andalucísimo\n\nAGE\n\nseguros s. a.\n\nSubdirector: Francisco Barruz Vidal\n\nAvenida de las Cruces, 10 Teléfono 22 09 85 - Particular 22 33 01 JAEN que, a no dudarlo, de algún modo abrió camino, en la depauperada literatura del Sur después del bombazo de los «narraluces». Aquí sí, aquí hay narrativa andaluz, y el libro todo un punto a tener en cuenta dentro de la prosa del Sur, aunque como es lógico, quede abierto al debate y a serias revisiones.\n\nPero, al parecer, con estas publicaciones no se acaba el rastreo literario de la vida y mundo flamenco de Pepe el de la Matrona. Antonio Escribano en estas mismas páginas de «Candil» nos dá la noticia —ni que decir tiene, gozosa— de que prepara un amplísimo trabajo sobre su maestro partiendo de largas conversaciones y del impresionante archivo que dejara el cantaor sevillano. Esperamos, pues, con ansias este anunciado trabajo.\n\nQuede aquí esta apretada reseña sobre la bibliografía de Pepe el de la Matrona, una de las más interesantes y completas de nuestros cantaores y, lo que me parece de mayor interés, de las más enjundiosas por lo que encierra de noticias para la historia de la más hermosa de las artes andaluzas.\n\nManuel Urbano\n\nCAMIONES DE PEQUEÑO Y GRAN TONELAJE CARGAS COMPLETAS A TODA ESPAÑA\n\nAGENCIA DE TRANSPORTES\n\nFrancisco Barruz Vidal\n\nAvenida de las Cruces, 10 Teléfono 22 09 85 - Particular 22 33 01 J A E N",
    "title": "Aunque no quepa en el papel.-Bibliografía del Matrona",
    "periodical": "candil",
    "issue_id": "1981-02",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 783,
    "article_char_count_full": 4563,
    "article_char_count_review": 4563,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-02-20-right-pepe-el-de-la-matrona-notas-para",
    "article_text_for_review": "Pepe el de la Matrona Notas para una discografía\n\nárdua tarea y empresa difícil, por cuanto hay riesgo evidente de no saber medir en su justa dimensión una serie de connotaciones que giran en torno a la personalidad de una voz, un estilo. Sí, hay algo que quisiéramos significar y que es factor dominante: Su definitoria categoría artística. El legado de cantes que el recogió en su largo, ajetreado y rico viaje por el mundo, llegan a su decir en personal visión. Los cantes tenían el aire de Pepe el de la Matrona, sin más. Como escribió Agustín Gómez «se trata de la lucha entre la forma preestablecida y la propia personalidad que deforma artísticamente». Deformación que, ciertamente, el cantaor hacía desde la perspectiva de un enjundioso conocimiento. De un saber decir.\n\nEl haberte conoció mira lo que ha dao lugar que yo me vea perdió y tú hecha una desgraciá porque perdimos el sentido.\n\nSin entrar en eternas polémicas de definiciones de estilos, a través de complicados esquemas, quizás por un deseo de prefigurar viejas y desconocidas formas cantaoras en nobles afanes de comunicar un conocimiento, y, sin abundar en su labor interpretativa -altamente reconocida y admirada por el aficionado - expuesta además, en otros estudios, sí hay aspectos que quisiéramos destacar en este esbozo valorativo. La Discografia de Pepe el de la Matrona enriquecedora, variada y extensa en contenido-, con visado extranjero, especialmente difundida en Francia, supone para el mundo flamenco, la oportunidad de exponer un valor artístico, una verdad, como contrapunto a la otra Andalucía que se ofrecía a nuestros vecinos. Así, frente a la Andalucía de la «grasia», la pan-dereta y la juerga, el de la Matrona y otros artistas, depositaban, en las audiencias, otro\n\ndecir que enlaza con la auténtica raíz de una tierra, de unas gentes. Lo jondo, en su expresión cantaora, podía apreciarse en los discos «francaises» que contenían la sevillana voz de José Núñez,\n\nNo se pué decir más\n\nLuego, nos vendría la entrañable realidad de «los tesoros del flamenco antiguo». Dos volúmenes con sentido y sentimiento. Fue como si Triana se resolviera en sus viejas voces, clamando, en un grito, las vivencias de una añeja época que, en gran medida, transcribía el veterano cantaor. Documento sonoro de una vida, de «una hermosa epopeya del hombre que vivió, al aire de la libertad, los fecundos territorios del flamenco».\n\nIn-Tex-Gar\n\nIndustria Textil del Hogar\n\nDOSCANDIL\n\nCortinas\n\nColchas\n\nTápicerías\n\nAlfombras\n\nRieles e Instalaciones\n\nRafael Raya Hernández\n\nCorrea Weglison, 3 - Teléf. 23 31 24 JAEN ACADEMIA DE ENSEÑANZA Básica, Media y Superior\n\n《JOSE LUIS LOPEZ》\n\nGraduado Escolar\n\nE. G. B.\n\nB. U. P.\n\nc. o. u.\n\nSelectividad\n\nMagisterio\n\n1.ºs Cursos Universitarios\n\nOposiciones Magisterio\n\nCalle Hurtado, 10 - Bajo izquierda JAEN Este número ha sido posible gracias también a la colaboración de las firmas comerciales siguientes:\n\nAislamientos Cruz Carmona\n\nAlmacenes de Materiales de Construcción Cañada\n\nAsesoria Laboral León\n\nBoutique Trapo's\n\nCalzados Migolo\n\nCasa de las Guitarras\n\nComercial Justo\n\nCompañía de Seguros La Patria Hispana\n\nConfitería Barranco\n\nConstrucciones Cruz García\n\nChamburçy\n\nExcavaciones «El Pipi»\n\nHogar y Moda\n\nManuel Rubio y Cía.\n\nMesón «La Reja»\n\nMesón Tito Adri\n\nOlimar School, English From Zero\n\nPapelería e Imprenta Gutiérrez",
    "title": "Pepe el de la Matrona.-Notas para una",
    "periodical": "candil",
    "issue_id": "1981-02",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "20-23",
    "page_number": 20,
    "word_count": 530,
    "article_char_count_full": 3352,
    "article_char_count_review": 3352,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-03-3-right-editorial",
    "article_text_for_review": "En abril de 1981, Demófilo publica su «Colección de Cantes Flamencos». La efemérides, cien años después, tiene por sí misma, la relevancia y virtualidad suficiente para que, con ocasión de ella, centremos nuestro análisis en el momento jondo de hoy.\n\nEn su prólogo de colección de coplas, Antonio Machado Alvarez presenta a los «aficionados» de la época, una panorámica del mundo flamenco, de inestimable valor histórico, de exquisita sensibilidad. Es, acaso, el primer ensayo que, pese a lo dogmático de sus aservaciones y a la heterogeneidad de planteamiento, afronta con ambición y rigor la temática jonda. Sus juicios de valor, sus observaciones respecto a personajes y entornos, su, en definitiva, particular visión de lo flamenco, de sutiles connotaciones sociológicas, han condicionado la bibliografía jonda más valiosa, hasta épocas recientes. Y es lo cierto, que algunos interrogantes que hace cien años Demófilo se planteaba, siguen ahí, irresolutos y plenos de actualidad.\n\nAlguna precisión de Demófilo, por lo contundente y demoleadora que ahora nos parece, merece destacarse. Así, en la época en que todavía cantaban Tomás Nitri, Silverio, Enrique el Mellizo y un largo etcétera, resultan por lo menos inocuos, los temores de Machado, cuando sentaba que «los cafés cantantes matarán por completo el cante gitano en un lejano tiempo». Tal vez cien años no es un lejano tiempo, pero desaparecidos los cafés cantantes y la ópera flamenca, apuntándose ya el ocaso del tablao como manifestación mínimamente flamenca, el cante sigue ahí, sin diluir su contenido, ante nuevos y sensibles receptores. Sigue siendo expresión genuina y, esencialmente, conserva su puridad.\n\nNo nos cabe duda -y así lo hemos mantenido en reiteradas ocasiones -que el flamenco como expresión creadora agoniza, se muere un poco más, cada vez que un viejo maestro se nos va. Y si, apriorísticamente, es aventurado rechazar la posibilidad de una evolución creativa en el cante, los intentos realizados son demasiado torpes para que valoresos esta posibilidad. En cualquier caso, persiste la memoria, una memoria respetuosa del pasado pero que se instrumenta aquí y hoy, una memoria con fuerza -por no se que mágicas evocaciones- para que el cantaor vibre y nos haga vibrar respecto a su propia realidad actual, como hombre y como pueblo; y surja así la emoción, el frenesí de la expresión jonda y un nuevo concepto de creatividad se afirme, en el flamenco: el cómo y el porqué de la rememoración.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 395,
    "article_char_count_full": 2477,
    "article_char_count_review": 2477,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-03-4-right-lo-jondo-en-fernando-villal-n",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Manuel Urbano\n\nEl próximo día 31 de mayo se cumple el centenario del nacimiento de Fernando Villalón, ganadero de reses bravas y poeta, una de las figuras de la llamada generación del 27 sobre la que ha recaído más poesía de reverbero y literatura de la que se literatura que auténticos estudios serios y desapasionados. Sin lugar a dudas, se le ha venido prestando más atención a sus gestos que a su propia obra y persona; quede un dato: el Conde de Miraflores de los Angeles, según me confirmaran recientemente en Madrid, descansa en un osario común. Qué contradicción e ironía del destino para quien reclama-ra con la fuerza de su casta decimonónica, aristocrática y campera:\n\nQue me entierren con espuelas y el barbuquejo en la barba.\n\nSi bien es cierto que a Villalón se le ha considerado,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"expresión\"]\n\nma —rotundas, amén de escasas y sin justificar plenamente— se han vertido de manera global para toda su obra y, por lo general, encasillándole en los míticos y distantes mundos del caballo y el toro. Cierto, que el propio poeta hablara de un «taurinismo racial ibérico»; pero no es menos cierto que ello no puede entenderse, al menos en su caso, sin la médula vital de lo jondo. El nudo central de su obra toda es el cante, nuestra copla. Ella es la expresión, sólo ella es la que siente y dice. Villalón fue, fundamentalmente, poeta por la copla y no por esa especie de determinismo geoliterario, por esos indescifrables e insostenibles estímulos externos, casi cósmicos, del país andaluz, que con tanta fe y reiteración sostiene el crítico hispalense Juan de Dios Ruiz-Copete. Estamos, en principio, más cercanos al criterio de José María de Cossío, quien señalara: «El popularismo poético andaluz, que en gran medida informa la poesía de Fernando Villalón y la de otros andaluces, como Alberti y García Lorca, por no citar sino a los más caracterizados, tiene su raíz más íntima, más auténtica, en el sentimiento directo de aquel ambiente, de aquel paisaje, de aquella tradición y de aquellas costumbres (...) Mas entre todos los poetas de aquel momento Fernando Villalón es el que tiene una mayor experiencia campera y popular. Por eso es el que puede lograr una expresión más directa, más certera, del pueblo andaluz, el que, pese a su sangre aristocrática y a su título de Conde de Miraflores de los Angeles, pertenece, por elección propia y por indominable apego a sus costumbres y a su sensibilidad. Aquel comportamiento aparentemente inurbano e incivil que he anotado al fijar mi recuerdo personal del poeta no era sino expresión de este bien hallarse e\n\n[ENDING CONTEXT]\n\nalgunas coplas flamencas. Así, la popular que nos llegara por Tomás Pavón\n\nQué se me importa a mí que se sequen las salinas mientras que te tenga a ti.\n\nPor cierto, no deja de ser curioso que José María Hinojosa en \"Canción final\", incluida en «Poema del campo» (1925), incida en la misma 'e-tra popular flamenca:\n\nY qué se me importa a mí, que la helada se deshiele. Y qué se me importa a mí, que los pájaros no vuelen...\n\nQuede aquí este apretado recorrido por la poética de Fernando Villalón, al que la cultura popular andaluzay, muy en especial, la jonda no puede olvidar en su centenario.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Lo jondo en Fernando Villalón",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "4-7",
    "page_number": 4,
    "word_count": 3759,
    "article_char_count_full": 22359,
    "article_char_count_review": 3387,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "expresión"
      }
    ]
  },
  {
    "article_id": "1981-03-7-right-notas-in-ditas",
    "article_text_for_review": "Joaquín,como cariñosamente se le conoce, tenía por apellidos los de Fernández Franco. Nació de la unión de José Fernández Torres (El Gordo) y de Paula Franco. De humildísima cuna, tiene cinco hermanos: Agustín (padre de Juan Talegas), José (padre de Manolito María), Manuel, Carmen y Vicente. Vio la luz el día doce de febrero de mil ochocientos setenta y cinco.\n\nPor MANUEL RIOS VARGAS\n\nFigura mítica y legendaria; gitano bueno, simpático, afable, cariñoso, lleno de humanidad y friolero cien por mil. Siempre estaba aterido. Cuando le preguntaban si no tenía frío, él solía contestar, que para qué lo quería si no tenía abrigo.\n\nSe dedicaba, como casi todos los gitanos de la época, a pelar ganado. De él se pueden contar infinidad de anécdotas como para no acabar nunca. He aquí una de ellas: Estado en la Plazuela tomando el sol, se le acercó un panadero diciéndole: «Joaquín, ¿cuánto me vas a llevar por plearme al burro?»; a lo que contestó, «un duro, como a to er mundo». «Ea, pues toma el dinero y te llegas a mi casa, en la calle Marea Chica número 12». Y, devolviéndole el duro, le contestó: «Coge tu dinero, que yo allí no voy, que esa es la calle donde jase más frío der mundo».\n\nSobre Joaquín han sido muchos los que han escrito; entre ellos, cabe destacar el libro de Eugenio Nõel: «Martín el de la Paula en Alcalá de los panaderos».\n\nDe mozo participó en la guerra de Cuba, donde enferma de fiebres amarillas; posteriormente padece la enfermedad de Adison, cuyas características son: el color quebrado de la piel, indolencia, apatía, etc. Se casó con La Cholona, de nombre Caridad Vargas Carrillo, teniendo dos hijos: Enrique, fallecido no hace mucho, e Hiniesta, la que aún vive en la bella localidad de Lora del Río casada con D. Juan Ramos López.\n\nJoaquín, en la ya lejana época de los Carnavales, sacaba las comparsas en Alcalá, siendo su\n\nverdadero artífice, y de las cuales aún se recuer- dan algunas coplas:\n\nJoaquín el de la Paula se compró un paletó se lo puso en septiembre y en junio se lo quitó.\n\nGenio del cante por soleá, le dio fama a su pueblo cantando por el estilo de esta ciudad —una de las pocas que tienen un cante propio: Soleá de Alcalá—. Era, a pesar de ser analfabeto, autor de sus propias letras:\n\nYo te tengo compará con la que está en el Castillo del Aguila de Alcalá.\n\nCuentan que, un día en la célebre Venta de Platilla, estando de juerga los divos de aquel entonces, Manuel Torre, Pastora Pavón, su hermano Tomás y Antonio Chacón, entre otros, le dijeron que cantara, y cómo lo haría, que los oyentes se tiraron al pilar, desgraciadamente, hoy seco y extinto.\n\nEl gran cantaor de nuestros días, Antonio Mairena, cuando venía a Alcalá acompañado de su padre, solía buscarle y se iban de juerga a la taberna del Cachito, junto a la antigua Plaza de Abastos, en la calle Nuestra Señora del Aguila. Con sumo orgullo suele comentar Antonio Mairena, refiriéndose a Joaquín, que él fue su gran maestro. No llegó a grabar porque eran malos tiempos para el cante flamenco y porque, como buen gitano, tenía sus muchas rarezas, clásicas de nuestro espíritu e idiosincracia.\n\nSu pueblo, Alcalá, le rindió merecido homenaje al ponerle a una calle su nombre: «Camino de Joaquín el de la Paula», la que se encuentra justo a la entrada del Castillo por la calle San Fernando.\n\nComo anteriormente hemos citado, tomó parte en la guerra de Cuba, y lo hizo junto a Paquillo Ruiz, pajero de oficio, y de Antonio Rodríguez Bozada (El Pelao), hornero de profesión. Por cierto, en una taberna de La Habana encontraron dentro de un recuadro de azulejos una estampa de la Virgen del Aguila, y es que el tabernero era también de Alcalá, conocido como Blas Manta al Hombro, quien marchó a Cuba gracias a la ayuda de D. Plácido Comesaña, esto ocurría en el año 1897.\n\nSegún certificado de defunción, murió de tuberculosis pulmonar fibrosa, siendo costeado su entierro por el insigne alcalareño D. Agustín Alcalá y Henke, esto ocurría el aciago día del 10 de junio de 1933.\n\nCONSTRUCCIONES\n\nOBRAS EN GENERAL\n\nPOLIGONO •LOS OLIVARES»\n\nCalle Alcaudete, 10\n\nJ A E N\n\nSan Clemente, núm. 19 Teléfono 23 20 66\n\nJ A E N",
    "title": "Notas inéditas...",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 728,
    "article_char_count_full": 4128,
    "article_char_count_review": 4128,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
