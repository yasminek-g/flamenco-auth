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
    "article_id": "1987-11-11-left-cuando-del-flamenco-se-hace-rito",
    "article_text_for_review": "Manuel Ríos Vargas\n\nEn la pintoresca urbanización Pinares de Oromana, sita en la localidad cantaora de Alcalá de Guadaira, posee Curro Fernández una parcela y hacia allá nos fuimos el pasado mes de julio para festejar el cumpleaños de sus hijos Esperanza y José Manuel, ambos componentes del grupo flamenco Familia Fernández.\n\nTras los lógicos y consabidos aperitivos, en una especie de tablao urgentemente acondicionado y habilitado para tal ocasión, quedamos maravillados del buen espectáculo flamenco que se nos ofrecía en una total intimidad; un flamenco verdad el que tuvimos la suerte de degustar gracias a la gentil invitación de que fuimos objeto por parte del anteriormente citado Curro Fernández, donde cada artista cantó y bailó a su aire y sin que sus interpretaciones estuviesen mediatizadas por ningún tipo de compromiso o contrato; en definitiva, tuvimos el gusto de asistir a una «juerga» de las que hacen época.\n\nEn dicha fiesta se dieron cita infinidad de artistas y personas relacionadas con el mundo del arte flamenco y así notamos la presencia de Aurora Vargas y su esposo Jarillo, Manuel Molina, Juana Amaya, María la Burra, Carmelilla Montoya, Pepa Montes y su esposo Ricardo Miño, Antonio Pulpón y Carlos Albelos y su señora María Rosa, además de la familia Fernández al completo, aunque no todos los reseñados actuaron en la clásica «juerga», al menos durante el tiempo que nosotros estuvimos presentes.\n\nSobre el escenario y en un momento determinado se encontraron las buenas guitarras de Paco Fernández y Manuel Molina, los cuales desarrollaron unos toques llenos de sabor y sensibilidad; el baile y el cante estuvo magistralmente representado por Esperanza Fernández y Aurora Vargas, ambas derrochando gracia, arte y compás por todos los poros de sus gitanísimos cuerpos, y cómo no, reseñar el extraordinario baile por bulería de Juanita Amaya, antaño y pese a sus dieciocho años, primera bailaora en el grupo de Mario Maya.\n\n¡Qué derroche de arte el derramado por estas buenas bailaoras y cantaoras! y qué guitarras más flamencias.\n\nEn una de las pausas nos comentaba el tocaor Ricardo Miño que aquí hay compás hasta por la mañana, aunque lógicamente nosotros hubimos de despedirnos mucho antes, no sin haber degustado una extraordinaria y original tarta en forma de guitarra.\n\nFue una noche inolvidable y flamenquísima la que pasamos en dicha propiedad de Curro Fernández, donde ya para terminar y apelando al principio diría... ¡Es que cuando del flamenco se hace rito!",
    "title": "Cuando del flamenco se hace rito",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 403,
    "article_char_count_full": 2501,
    "article_char_count_review": 2501,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-11-11-right-la-familia-fern-ndez-entre-trian",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nllos, los protagonistas, dicen:\n\nManuel Martín Martín\n\nY no podía ser de otra forma. Tuvo\n\nque ser Triana la que fijara sus ojos en Lebrija: halagos, piropos y una siguiriya jerezana pusieron el resto. Sencillo y duradero a un tiempo, como la vida misma, tierno y encantador, como la sonrisa de un niño. Así fue el encuentro de un hombre y una mujer que dan gracias a Un Devel por pertenecer a un grupo étnico muy especial, a una raza con notables privilegios para lo jondo. Ambos, Curro y Pepa, disponen de un boyante tesoro heredado: sus «batos», Juan José y Quintín en el recuerdo, jamás dieron su brazo a torcer por aquello que se apartaba de la etérea pureza, de esas normas y pautas que marcaron nuestros predecesores y que se fijan en el firmamento flamenco con el rótulo de clásico.\n\nDe esta\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"ritmo\"]\n\nboyante tesoro heredado: sus «batos», Juan José y Quintín en el recuerdo, jamás dieron su brazo a torcer por aquello que se apartaba de la etérea pureza, de esas normas y pautas que marcaron nuestros predecesores y que se fijan en el firmamento flamenco con el rótulo de clásico. De esta suerte, los vigorosos y enlazados tercios trianeros vibra-ron al rebujo de una lozana lebri- jana. Reciedumbre y sensibilidad, amor por decreto, cuadratura del ritmo y sabia gitanería prescribieron la fórmula. El resultado no se hizo esperar: Esperanza, Paquito y José Manuel, tres claveles reventones que hoy se adornan con dos ramitos de romero, Manoli y el Piripi. La donosura de Pepa Vargas y el requiebro ceremonioso de Curro Fernández completan y encabezan una estirpe flamenca que hoy cabalga por la cresta del éxito. Ellos son, hoy, los protagonistas de CANDIL. Hemos pretendido con esta entrevista conocer a cada uno de sus JOSÉ ANTONIO.—Yo soy José Antonio Camacho Vargas, me dicen el Piripi, y me considero un miembro más de la familia, me encanta trabajar con ellos y tengo 25 años. JUAN JOSÉ.—Yo soy el patriarca de la dinastía de los Fernández, voy a cumplir 68 años y desciendo de Curro Puya, de los Pelaos de Triana, de Gitanillo de Triana, en fin, de esos buenos gitanos de la Cava gitana de Triana. PEPA VARGAS.—Yo nació en Lebrija, soy hija de Quintín y de Curra, tengo 42 años y mi padre era un gitano fenómeno, muy ran- miembros antes de reseñar la exigua, pero importante, trayectoria de la familia Fernández. A la misma faltó Paquito, cumpliendo el servicio militar en Badajoz. Empero, tuvimos un testigo de excepción, Juan José Fernández Vega, el patriarca de una casta flamenca que blasona la modestia y que hoy se muestra orgullosa porque ha sabido trazar una sinuosa y flamenquísima línea entre Triana y Lebrija. —¿Quiénes forman la familia Fernández? CURRO.—Bueno, yo soy el padre de to\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_03 | trigger=\"peñas\"]\n\nLI RÍOS.—Yo soy sevillana, no trianera, y soy la más chica, tengo 17 años. ESPERANZA.—Yo soy Esperanza Fernández Vargas, soy hija de Curro y tengo 21 años. —Antes de formarse el grupo, Curro ya era bastante conocido en el mundillo jondo. Desde que te destapastes, allá por 1967, en el Concurso de Mairena del Alcor mucho ha llovido, ¿no? CURRO.—Sí, bastante. Ese premio fue muy importante para mí porque me hice un nombre y los aficionados de las peñas se interesaron por mí. Antes cantaba para bailar y de ahí me vinieron muchas cosas importantes, como la Misa Flamenca con Antonio Mairena, en la iglesia del Salvador, —Aparte de las tres figuras que citas le has cantado a casi todas. CURRO.—Hombre, le he cantado mucho tiempo a Manuela Carrasco, a Trini España y a todas las que consiguieron un premio en Córdoba, cuando decían que los premios tenían valor, como Loli Flores, y muchas más que ya no me acuerdo. —¿Hay que reunir unas cualidades específicas para triunfar en el cante atrás? CURRO.—Yo creo que en principio para cantar, para bailar, aparte de saber cantar y tener buena voz y mejor compás, es que le guste a uno mucho el baile. Entonces yo creo que me he acoplado a todas por el amor que le tengo al baile, el baile me gusta más que el cante y lo conozco mucho mejor, por eso cuando le canto por primera vez a una bailaora rápidamente le cojo su baile. —¿Quieres decir que hay diferencias ostensibles entre el baile de un castellano y un gitano? CURRO.—Eso es fundamental. Mira, yo le he cantado a Merche Esmeralda, que era paya, y ento\n\n[ENDING CONTEXT]\n\nse habla sólo del cante, y hay más cosas.\n\n—Curro, para terminar, en base a esa difusión, ¿habéis pensado grabar un disco?\n\nCURRO.—Sí, porque ya ha venido una casa en busca nuestra, pero eso no se puede hacer así por las buenas, eso requiere muchas horas de pensar y estudiar. Puede que esa sea la mejor publicidad para un grupo de baile. Lo que sí quiero, si me dejas, es felicitar a la revista CANDIL por la labor sorda, pero maravillosa, que hace por el flamenco.\n\n—Te dejo y te lo agradezco en nombre del grupo CANDIL, si bien la felicitación hay que enviársela a los aficionados por leernos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Ellos, los protagonistas, dicen. La familia Fernández entre Triana y Lebrija",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "11-15",
    "page_number": 11,
    "word_count": 5345,
    "article_char_count_full": 30179,
    "article_char_count_review": 5146,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "ritmo"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "peñas"
      }
    ]
  },
  {
    "article_id": "1987-11-16-left-plagio-de-rdago",
    "article_text_for_review": "n la evidencia del plagio cometido por Alfredo Arrebola con mi escrito del 75, SOBRE JUAN BREVA, en la Revista de Ceuta «FLAMENCO», dirigida por don Francisco Vallecillo, y ante su obstinada negativa en varias ocasiones, me veo obligado a presentar pruebas contundentes para que la afición a nuestro arte juzgue por sí misma al ver nuevamente mi artículo de hace doce años y el de Arrebola hace algo más de dos años en su libro Los cantes preflamencos y flamencos de Málaga.\n\nEn mi comunicación del reciente XV Congreso Nacional de Actividades Flamencas de Benalmádena: «Letras y algunas disonan-\n\nJ. Márquez Cabello\n\ntes», traté de sacarle los colores, y arguyó —sin inmutarse— que él no había copiado nada, y que el no haberme mencionado en «cita de fuentes» había sido error de imprenta. Ante tales argumentos, remito a los aficionados a que confronten y vean si hay plagio o es que yo me lo imagino...\n\nSobre Juan Breva. (Artículo nuestro en la Revista «FLAMEN-CO», Ceuta, 1975). — A Breva le llamaban «El ruiseño de Vélez», porque cual tal avecilla, sus trinos eran bellos, dulces y reidores, a la par que tiernos, quejumbrosos y transmisores de los más hondos sentires del corazón...\n\nJuan Breva. (Escrito de Arrebola en la página 182 de su libro Los Cantes preflamencos...).—«A Juan Breva le llamaban “El ruiseño de Vélez” porque, cual tal avecilla, sus trinos eran bellos, dulces y reidores, a la par que tiernos, quejumbrosos y transmisores de los más hondos sentires del corazón...».\n\nY sigue el plagio de más de cincuenta palabras; pero para una muestra con un botón basta. Asimismo, agrega en una LEVE COLECCION DE LETRAS PARA LOS CANTES DE MALAGA en el mismo libro, páginas 174 y 175, seis «letras» de nuestro «coco» y sin la más leve mención en ninguna parte. Son las que siguen:\n\nPura sangre lloro yo diciéndote mis quebrantos por ver si tu corazón se doliera de mi llanto y cambias de condición. Rincón de sal y solera —1.º y 3.º barrio de la Trinidad, de artistas de calidad, bravas jembras de bandera y hombres a carta cabal. Después de algún tiempo ausente [—1.º y 3.º lloré cuando te he mirao porque triste he recordao la alegría de tu gente que en penas se le han troncao. Málaga yo no te olvido —1.º y 3.º por lejos que esté de ti y has de ir en mi sentir en tanto que el pecho mío tenga suerte de latir.\n\n¡Qué rabia me da mirarte —1.° y 3.° barquita de mis faenas comía por las arenas sin poder ni repararte, con la mar la mar de buena. que una buena compañera, una salud cortijera, el corazón puesto en Dios y diez «verdes» en cartera (1).\n\nY cónstele al cantaor Arrebola que, con todas las de la ley, «podía —como dice el amigo Yerga Lancharro en «CANDIL», 47—acudir a un Letrado, pero bien sabe Dios que no soy capaz de hacerlo...».\n\n(1) Faltan dos que no estoy muy seguro...).",
    "title": "Plagio de órdago",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 501,
    "article_char_count_full": 2803,
    "article_char_count_review": 2803,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1987-11-16-right-copia-parcial-de-una-carta-ya-hi",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Yerga Lancharro\n\nobre los cantes de Levante te diré que sucede hoy lo mismo que aconteció hace tiempo en Málaga. Pocos aficionados jóvenes conocían los cantes antiguos de su tierra, a pesar de contar casi a diario, en la peña de «Juan Breva», con los viejos cantaores, «El Brevilla», Diego «El Pijín», «Niño de las Moras» y con «Manoliyo el Jerrao». Esto no deja de ser lamentable y, pienso yo, que inadmisible. Por ahí sólo los conocéis con el aire que se vienen interpretando en la actualidad y tú sabes que a esos cantes les falta «sal y pimienta», como se dice en Andalucía. Es posible que, como cosa natural, influya en ese modo de interpretar la avanzada edad de algunos de sus «maestros» más representativos. Esa «falta de vida» nos ofrece una imagen muy distinta de la que conozco; de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\namentable y, pienso yo, que inadmisible. Por ahí sólo los conocéis con el aire que se vienen interpretando en la actualidad y tú sabes que a esos cantes les falta «sal y pimienta», como se dice en Andalucía. Es posible que, como cosa natural, influya en ese modo de interpretar la avanzada edad de algunos de sus «maestros» más representativos. Esa «falta de vida» nos ofrece una imagen muy distinta de la que conozco; de la que realmente tuvo en su mejor época chaconiana. Los encuentro vanos, fríos y sin contenido flamenco. Además, observo que por los jóvenes que ahora se inician, se viene interpretando con excesivos «ayes», expresión lastimera que está de más en la mayoría de esos cantes y, lo que es peor aún, omiten lo más importante, como es la ligazón de los ter- cios, en los momentos en que los cantes así lo piden. Esto mismo sucede con otros cantes fuera de tu región, concretamente con la solea de la Serneta. Menos mal que mi tímido amigo el gran Tomás Pabón, no deja de ofrecernos, a diario, con su feliz grabación, una lección magistral de cómo la debió cantar su creadora, la bellísima jerezana María de las Mercedes Fernández Vargas. ES NECESARIO DES A CONOCER LOS CANTES DE TU TIERRA Estoy de acuerdo contigo, en cuanto a que se hace apremiante la necesidad de dar a conocer a las nuevas promesas los auténticos cantes de esa tierra. Para ello tienes que destruir los ídolos de paja, si es que cuentas con algunos y construir los que representen al verdadero cante levantino. A todo esto me dirás: ¿Y cómo y con qué «material» voy a vivificar nuestros cantes? Yo te diría: con los que te faciliten las peñas flamencas y que deberán ser: las del «Cojo de Má-laga» (muy valientes y flamencos); de Paca Aguilera (muy flamencos y preciosistas); de «Fernando el Herrero» (excepcionales); de Manuel «Torre» (muy buenos por taranta corta y por cartagenera); del «Niño de las Marianas» (muy aceptables); del «Niño Me\n\n[ENDING CONTEXT]\n\nmal titulados «de ida y vuelta» tan vilipendiados por unos y otros y hoy, por lo visto, tan necesarios para ser utilizados como banderín de nuestro ejército flamenco, con motivo de la Expo-92.\n\nPor mi edad estoy apartado del trajín del mundo del arte flamenco. No sé, por tal motivo, si la situación que plantee hace años en el escrito que precede, persiste en la actualidad o ha cambiado para bien. De haber, cambiado para mejorar, yo me alegraría de corazón.\n\nNo tengo por menos que felicitar al cantaor y decirle: ¡Adelante y a cantarlo todo!\n\nUn abrazo que abarque a toda la afición flamenca.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Copia parcial de una carta ya histórica",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "16-18",
    "page_number": 16,
    "word_count": 2483,
    "article_char_count_full": 14745,
    "article_char_count_review": 3555,
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
    "article_id": "1987-11-18-right-aunque-no-quepa-en-el-papel-leta",
    "article_text_for_review": "Andaluz que vas errante,\n\nde Enrique Alcalá Ortiz, Ediciones Cajal. Almería, 1985\n\nJosé Luis Buendía López\n\nEl poeta, nacido en Priego de Córdoba en el año 1942, es además uno de los más serios investigadores del folklore local que suelen darse por estas tierras, en las cuales, y aun siendo semilleros del mismo, parece que no se percibe la enorme categoría humana y artística que de su estudio dimana; los dos tomos aparecidos de su Cancionero popular de Priego (de un vasto plan que incluye seis volúmenes más) son una muestra de rigor y seriedad a la hora de mostrar las facetas artístico-folklóricas del pueblo que lo viera nacer. Como poeta, Alcalá ha elegido la forma de la letra jonda para expresar esos anhelos andalucísimos que antes comentábamos y que ha plasmado en obras como El viejo olivo, Los barrotes del Adarve, etc., además de estas letanías del andaluz errante, en las que todo el\n\nañadida que constituyen las malas comunicaciones entre las provincias andaluzas y la casi nula penetrabilidad cultural entre ellas, hasta hace unos días no me ha sido dado a conocer este hermoso poemario, andaluz por los cuatro costados, y en el más puro sentido del término, es decir, nada de lunas luneras ni de ojeritas de amor, sino fibra auténtica, verdadero pronunciamiento humano de un poeta al que le duelen sus tierras, sus gentes, y por ello se desahoga en estas letanías que cantan el más duro de los martirios de nuestro pueblo, la emigración.\n\nP or esa dificultad añadida que cons- libro, concebido en forma métrica de solearias de tres versos, repite en el primero de ellos la angustiosa llamada, el apóstrofe apasionado que le da título al libro: «Andaluz que vas errante», completando en los dos versos siguientes el pensamiento que quiere expresar, en el cual, el primer verso, lejos de hacerse pesado por reiterativo, nos sueña a grandioso golpe de bordón que no nos deja separarnos un ápice de la idea central del poemario.\n\nEn cuanto a la estructura del libro en sí, hay que señalar que consta de un poema introductorio, «Paraíso andaluz», escrito en verso libre, que da el tono emocional a la obra y, enseguida, dos pequeños capítulos o «entradas» que el autor califica como Primera o Segunda puerta, ya en verso de soleá, que preceden a nueve capítulos dedicados respectivamente a cada una de las provincias andaluzas, y que van separados por unos sobrios dibujos de Torres Aceituno que con su trágico esquematismo constituye la mejor apoyatura visual imaginable.\n\nPero, ¿de qué tratan estas 320 pequeñas estrofas? Yo diría que forman un nuevo cancionero andaluz que muestra el desamparo de sus gentes y que puede resumirse en la estrofa catorce de la «Segunda puerta»:\n\nNacieron bastante tarde, estaba todo ocupado cuando quisieron sentarse. Los tonos rotos, desgarrados y\n\ntotalmente jondos de muchas de esas coplas se complementan con otros en los que predominan acentos más cultos, en los que la voz de diferentes poetas (Villalón, Hernández, Lorca sobre todo), forman un engaste literario que abrocha perfectamente el ramilete de coplas:\n\nAndaluz que vas errante esperas lunas crecientes y te dan lunas menguantes.\n\nAsí discurre este libro, recomendable no sólo para su lectura, sino también para ser vivero de letras de tantos cantaores, aficionados y profesionales, a los que no parece importarles el hermoso contenido de las letras y repiten una y mil veces las mismas a lo largo de sus recitales y actuaciones. Y que no me venga nadie a decir en serio aquello de que «el cante no cabe en el papel» que se atribuye al bueno de Joaquín el de la Paula, porque en ningún otro repertorio musical se consentiría esa abusiva repetición de motivos literarios.\n\nHora es ya de que nos demos cuenta de que en el cante flamenco tan importante es una buena letra como una bien acompasada melodía. El libro de Enrique Alcalá es un muestrario de esas enormes posibilidades abiertas en el campo de la renovación de las letras jondas, que puede y debe seguirse por los intérpretes que aspiren a ser algo más que unos meros repetidores del último cassette adquirido en cualquier sitio.",
    "title": "Aunque no quepa en el papel. Letanías",
    "periodical": "candil",
    "issue_id": "1987-11",
    "year": 1987,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 692,
    "article_char_count_full": 4100,
    "article_char_count_review": 4100,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
