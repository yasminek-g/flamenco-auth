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
    "article_id": "1994-01-27-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n«Porque me acuerdo de lo que he vivido», podía decir, y dice, aunque en otros términos, Juan de la Plata, acordándose de aquel Manuel alcalaíno que arañaba con sus ayes las noches, junto a las ventas del Guadaira. Este amable folleto del crítico jerezano, que, o se lee de tirón, a golpes de la sangre, o no se lee nunca, es una apasionada retrospectiva sobre la aventura flamenca jerezana y sobre sí mismo. Sobre todo, esto último. Juan no quiere que a la memoria flamenca se la trague la tierra, y lo proclama con voz solemne: «Mis queridos inmortales flamencos, ya en silencio, pero no en olvido. Nunca en olvido. Nunca».\n\nPero, como decíamos anteriormente, Juan, al historiar con tono poético y evocativo los sucesos jondos jerezanos, homenaje a sus vivencias, aquellos años del descubrimiento\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"nuevos\"]\n\nimo. Juan no quiere que a la memoria flamenca se la trague la tierra, y lo proclama con voz solemne: «Mis queridos inmortales flamencos, ya en silencio, pero no en olvido. Nunca en olvido. Nunca». Pero, como decíamos anteriormente, Juan, al historiar con tono poético y evocativo los sucesos jondos jerezanos, homenaje a sus vivencias, aquellos años del descubrimiento flamenco en los viejos registros del fonógrafo, el paréntesis de la guerra, los nuevos rumbos vitales que, a partir de los años cuarenta, lo llevarían a zambullirse en estos mundos... Escritas en una especie de ampuloso versículo blanco, que le presta empaque y solemnidad, estas breves servilletas de recuerdos están impregnadas con la voz cálida del autor, quien, al presentar en la moviola del tiempo, su tiempo, a Tio José, Tia Anica, y a cuantos contribuyeron a su formulación sentimental, realiza a la vez el mejor ejercicio posible de aproximación entre sí mismo y aquello que lo encandió cuando apenas apuntaba cuatro palmos sobre el suelo. Leerlo es sumergirse de lleno en la Memoria jonda Juan de la Plata Cátedra de Flamencología. Jerez, 1993 HOMENAJE A DEMOFILO Revista «El Folk-Lore andaluz» (2.ª época). N.º 10. Fundación Manchado. Sevilla, 1993 intimidad de los recuerdos, en esa oficina privada de las personales evocaciones. En medio del aluvión de publicaciones acerca del centenario de la muerte de Demófilo que este mismo número de la revista, en sus páginas centrales, se encarga de glosar y resumir en lo posible, nos encontramos con una entrañable, representada por el hecho de que se le rinda homenaje especial en la revista El Folk-Lore andaluz (2.ª etapa), publicación de la que Don Anton\n\n[ENDING CONTEXT]\n\na Tomás Ortiz Ibáñez.\n\nLa Junta Directiva está formada por Marcos Gutiérrez Melgarejo, como vicepresidente; Leovigildo Francisco Aguilar Burgos y José Pamos Mozas, secretarios; Juan José Gay Torres y Juan J. Carrascosa Jurado, tesoreros; Rafael Valera Espinosa, vocal de prensa; Miguel Hernández Martínez, vocal relaciones públicas; Francisco Cañada Cejudo, vocal contratación artística; Ramón Montoro Campos y Vicente Cumberro Ruiz, vocales mantenimiento; Manuel Pérez Mesa y Ramón Cañada Morales, vocales.\n\nDeseamos toda suerte de éxitos en la nueva andadura que emprende este equipo directivo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "27-28",
    "page_number": 27,
    "word_count": 1099,
    "article_char_count_full": 7108,
    "article_char_count_review": 3307,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "nuevos"
      }
    ]
  },
  {
    "article_id": "1994-01-28-right-and-jar-en-paz-con-rafael-romero",
    "article_text_for_review": "S e ha cumplido el tercer aniversario de una tarde de frío enero, en que murió en Madrid uno de los iliturgitanos más conocidos en el mundo entero y menos conocido y valorado por su pueblo. Nos referimos a Rafael Romero «El Gallina», cantaor flamenco de fama universal.\n\nNació en la calle de San Lázaro, en una tarde de octubre de 1910, en el seno de una familia gitana. Su padre, Pepe «El Bocón», tratante y esquilaor, como corresponde al prototipo de gitano de aquella época. Su madre, Herminia, conocida por Armenia, ¿quizás al influjo de aquella seguiriya gitana que nos habla de los montes de Armenia? De pequeño acompaña a su padre en sus andanzas por los campos mineros de La Carolina, Baños, Linares o El Centenillo, y aprende los cantes de el «Tonto Carica Dios»,\n\nde André's Heredia y de tantos otros, sin olvidar la influencia de su padre, de quien recibe los ecos soleareros de su paisano Yllanda. Después quiso ser bailaor, y como tal interviene en numerosas fiestas y grupos de «cabales», donde se solicita su presencia. La Guerra Civil, marca su trayectoria artística, pues, como hombre bastante cul- to que era, llega a ser brigada en el Ejército republicano, por lo que al terminar la contienda marcha a vivir a Madrid, donde pronto conoce a «Perico el del Lunar», «El Viejo», que tanto influyó en su trayectoria artística.\n\nY a tenemos a un Rafael Romero cantaor, que en los famosos tablaos flamencos de Villa Rosa, Los Gabrieles o Zambra, alterna con «Pericón de Cádiz», con Rosa Surán, con Juan Varea, con Pepe «El Culata» y tantos otros maestros del cante, dejando cada noche la impronta de su arte. En 1955, participa en la grabación de la Antología del Cante Flamenco de Hispavox, que obtiene el premio de la Academia Francesa del Disco, y a la que aporta sus seguiriyes, tonás, martinetes, peteneras, la caña, alboreás y mirabrás. Su forma reposada y solemne al decir los cantes, es reflejo de su personalidad y su elegancia. Esto le aporta un gran prestigio en el extranjero, sobre todo en Japón, donde llegó a cantar para el emperador Hiro Hito y se venera su memoria. En España, interviene en los más importantes festivales y aconteciimientos flamencos, y en 1973, la Cátedra de Flamencología de Jerez le otorga el Premio Nacional del Cante.\n\nAndújar reconoce al fin la categoría artística de Rafael, y pone su nombre a la calle donde nació, pero poco duró su alegría, pues la primera Corporación democrática suprime esta nominación, con gran dolor para el viejo cantaor. Cuántas veces nos decía: «Niño, tú que puedes, haz algo para que me pongan otra vez la calle» Y se murió con esa pena, en Madrid, al atardecer del 5 de enero de 1991...\n\nUn año después, un grupo de buenos aficionados de la Peña Fla-menca de Tokio, presididos por el profesor Jiro Hamada, se desplazan a Andújar, el 4 de septiembre del mítico 92, para proceder a la inau-guración de un precioso monu-mento por ellos costeado, que el Ayuntamiento sitúa en el Parque de San Eufrasio, en un bello rin-cón, entre cuatro olivos añosos, patrocinando una serie de impor-tantes actos flamencos, con la co-laboración de la Peña Flamenca «Los Romeros» y en los que inter- vienen importantes figuras del Cante.\n\nAhora, la Corporación Municipal, en Comisión de Gobierno celebrada el 23 del pasado noviembre, acuerda poner el nombre de Rafael Romero «El Gallina», a una calle de nueva apertura.\n\nTanto la Peña «Los Romeros», que mucho se dolió con Rafael por aquella desafortunada decisión municipal, ahora corregida, como los numerosos y buenos cantaores amigos y discípulos de «El Gallina», agradecen este acuerdo. Estamos seguros de que el viejo cantaor iliturgitano, que prestigió el nombre de Andújar por los caminos del mundo, cantará ahora más alegre por los caminos de la Gloria.",
    "title": "Andújar, en paz con Rafael Romero",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "28-29",
    "page_number": 28,
    "word_count": 643,
    "article_char_count_full": 3772,
    "article_char_count_review": 3772,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-01-29-right-noticiario-flamenco",
    "article_text_for_review": "Desde el mismo día 29 de julio de 1993 en que Utrera dijo adiós a su hijo predilecto, el inolvidable artista Enrique Montoya, el pueblo todo y numerosas voces entrañables, que nos llegaron desde distintos puntos de la geografía andaluza, coincidía en señalar que la figura de Enrique debería quedar plasmada en un lugar señero de su siempre querida y bien cantada ciudad, para que ambos quedaran unidos en la historia por ese amor recíproco de tan perfecto maridaje.\n\nUtrera ha reaccionado rápida-mente: un acuerdo de su Ayuntamiento-Pleno (propuesta unánime de todos los grupos políticos) con la creación de una comisión organizadora, bajo la presidencia de su alcalde y con la activa participación de los distintos estamentos socioculturales de la ciudad, aprobó el boceto presentado por el artista local Salvador García, designó la Plaza de la Constitución como emplazamiento más idóneo y abrió cuentas en todas las entidades bancarias para que el deseo mostrado, de forma tan amplia y abierta, sea una realidad en el primer aniversario de la desaparición de tan singular artista.\n\nLa Comisión Pro-Monumento «Enrique Montoya», conocedora del reconocimiento que todas las Peñas y entidades flamencas de Andalucía prestaban a Enrique y a su arte, hace llegar a la comunidad del sentimiento flamenco la invitación a participar en esta suscripción promonumento, realizando su colaboración —pequeña o grande— en cualquier entidad bancaria o de caja de nuestra ciudad, de la que recibirán el consiguiente acuse de recibo y la gratitud de Utrera por contribuir a perpetuar la figura artística y admirada de Enrique Montoya Fernández, Hijo Predilecto de Utrera.\n\nJosé Dorado Ale Presidente de la Comisión\n\nEn el momento en que escribo estas líneas hace ya dos semanas que dejamos de oír su voz. Esa voz emblemática y desgarradora, ejemplo más puro de las nuevas corrientes jóvenes de nuestro Flamenco. Ya nunca más volveremos a sentir su grito moldeado y modulado por tanta sabiduría y «jondura», su «elipse» gutural y sentida, elevada al máximo exponente del arte y de la «flamencura». Eso era para mí Casillas, por otra parte, gran amigo mío.\n\nNos dejó y se fue como todos los que se van: sin decir adiós. Ninguno de sus seres más queridos y de sus amigos que hoy y ayer lo apreciábamos podíamos creer lo que irremediablemente ya había sucedido. La noticia de su muerte nos llenó de dolor.\n\nEn estos días después de su pérdida, mi mente y mi alma no han dejado ni un solo momento de entrever su imagen. Hacía casi un año que no hablaba con él, pero recordaba una a una todas nuestras animadas conversaciones, todas nuestras charlas apasionadas sobre el flamenco que ambos sentíamos y compartíamos muy dentro. Volví a oír sus cantes en mi memoria o a través de sus oportunas grabaciones, repasé una a una todas sus explícitas letras personales (dicen mucho de él y sus vivencias, como se podría esperar). Quizá poca gente puede ver retrospectivamente a Casillas en la manera en que yo lo veo, como yo lo conocí. El era un hombre extraordinariamente sensible (permitidme que subraye esta palabra), ¿qué mejor prueba que su cante? Pero, a la vez, Juan era una persona de una inteligencia y apertura encomiables, unidas ambas características a la base de un temperamento fuertemente pasional y vital. Puede que ahora todo el mundo pueda hablar bien y mucho del\n\nCasillas cantaor. También yo podría hacerlo ampliamente. Fue, para mí, sin duda, un gran maestro, pese a su juventud. Fue un claro ejemplo a seguir por cualquier buen aficionado, circunstancia de la que yo mismo me serví. Juan nos mostraba con su cante toda esa mezcolanza comedida de virtudes y facetas que son de exigir a un artista para poder ser considerado como tal: pureza y ortodoxia (pilares básicos del cante), conocimiento y sabiduría, «justeza» emocional e interpretativa, empuje y estudiada entonación en su voz, compás marcado y virtuosamente perfeccionado; pero, sobre todo, «flamencura» (y déjenme ahora repetirme), arte a raudales, «sentimiento sentido» y sabiamente transmitido en la ejecución de sus cantes y con todo su cuerpo, que movía con «ángel» en el escenario, que bailaba y se iba al compás de las alegrías, de los tangos, de una soleá bien «llorada», ya no cantada. Son estos rasgos últimos los que venían a encajar de lleno en su personalidad. Son pocas las personas con las que puedes tener, cuando conversas con ellas, la sensación de que están vivas, de que son sensibles (de nuevo), de que están colmadas de emociones y de sensaciones que pueden compartir y a las que, en gran parte, necesitan dar rienda suelta. El usó, para ello, su flamenco... Y quizá fue ese carácter ebrio y fácilmente expugnable el que se lo llevó...\n\nNosotros, los que lo admirábamos, tendremos que conformarnos simplemente con seguir escuchando el eco de su voz en las noches de verano de Santa María la Mayor, donde alguna vez una seguiriya suya, o una «salía» suya por soleá, nos dio de lleno en el alma...\n\nJuan: donde quieras que estés, sí- guenos cantando...\n\nFrancisco Veredas",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "29-29",
    "page_number": 29,
    "word_count": 840,
    "article_char_count_full": 5044,
    "article_char_count_review": 5044,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-01-30-left-hablan-las-pe-as-nuevas-juntas-d",
    "article_text_for_review": "Hablan las Peñas Nuevas Juntas Directivas\n\nPeña El Taranto Almería XI Trofeo «Lucas López»\n\nEl jurado del premio, tras repasar las actuaciones habidas durante el año, proclama por unanimidad como ganador del trofeo «Lucas López» a Enrique Morente por su actuación en la Peña «El Taranto» el día 16 de enero de 1993. En este magnífico recital destacaron dos facetas distintas, una recordando a los viejos maestros, y otra, en que sin apartarse de las raíces, realizó las innovaciones que le han consagrado como el músico más importante del flamenco actual.\n\nD. E. POHREN\n\nPedidos a: D. E. Pohren Apartado de Correos, 83. LAS ROZAS (Madrid)\n\nMerengue, El. Nombre artístico de Rafael Rodríguez Fernández. Córdoba, 1944. Guitarrista. Hijo del tocaor del mismo apodo. Casado con la bailaora Concha Calero. Discípulo de Antonio del Lunar. Ha actuado con Juanito Valderrama, Pepe Marchena, Manolo Caracol, Manolo El Malagueño, La Tomata, Curro de Utrera, Lola Flores y Juanita Reina, entre otros. Entre los galardones conseguidos, El Merengue fue premiado en el Concurso Nacional de Arte Flamenco de Córdoba, en 1968. Por su excelente trayectoria y sus cualidades artísticas, se le considera uno de los tocaores más significativos de la actualidad. Entre sus más recientes actuaciones, cabe destacar su inclusión en la II Cumbre Flamenca de Madrid. El crítico Agustín Gómez, opina así de su arte: «Merengue sabe hacer de su guitarra el espejo amable que devuelve la más bella imagen del cante y el baile que a ella se mira. Su condición va más allá de la guitarra flamenca básica porque revela secretos de musa, ángel y duende con silencios oportunísimos, pianísimas caricias de sonidos y fuertes vibraciones de compás. Su guitarra es un delicado poema al cante».\n\n(Del «Diccionario Enciclopédico Ilustrado del Flamenco».)\n\nTOCAORES DE HOY\n\nEl Merengue",
    "title": "Hablan las Peñas Nuevas Juntas Directivas",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "30-31",
    "page_number": 30,
    "word_count": 296,
    "article_char_count_full": 1845,
    "article_char_count_review": 1845,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-03-3-left-editorial",
    "article_text_for_review": "Editorial\n\nL'a única, y tal vez la última, institución benefactora de los flamencos de la tercera edad, ha cerrado sus puertas. Desde hacía tiempo sus exiguas arcas alimentadas, en gran parte, por el trabajo y el patrimonio de unos pocos, eran incapaces de atender prestaciones y de dispensar beneficiencia.\n\nDecimos bien, beneficiencia, con toda la carga peyorativa que comporta este término; porque, con independencia del respeto y la admiración que nos haya merecido el trabajo solidario de quienes han regido los destinos de la institución ahora desaparecida, siempre pensamos que los flamencos, al igual que otros colectivos como el de los toreros, demandaban algo más que pura beneficiencia. Esos flamencos de la tercera edad resultan acreedores no a dispendios o a limosnas bienintencionadas, sino a una cobertura adecuada que les proteja de cualquier contingencia, es decir, resultan acreedores a un derecho fundamental a la Seguridad Social.\n\nHace casi una decena de años se publicó un estudio en esta re-\n\nvista, sobre la Seguridad Social de los artistas flamencos. En el mismo se analizaba la situación precaria, en ocasiones de pura indigencia, en que se encontraban figuras señeras del cante, entonces vivas, que se veían impulsadas a vivir de la mendicidad. Y los más afortunados de los artistas flamencos que por haber cotizado como autónomos en una determinada profesión, gozaban de una expectativa exigua de jubilación, tampoco resultaban tan afortunados ante determinadas contingencias como una incapacidad permanente, un accidente de trabajo o una enfermedad profesional. Porque\n\nquien legalmente estaba cotizando como zapatero, qué importancia tenía el que perdiese la voz; y el que, siendo guitarrista, cotizaba como albañil, qué podría reclamar, si se le amputaba un dedo.\n\nPese a que, en distintos casos, se ha reiterado esta reivindicación y hubo un tiempo en el que, incluso un grupo parlamentario, asumió la defensa de esta conquista social, es lo cierto que, pese al tiempo transcurrido, nada se ha hecho. Dentro de unos meses se celebrarán elecciones al Parlamento Andaluz. Qué magnífica ocasión para que algún grupo político se comprometiera a defender este derecho en el Parlamento nacional que resulta, de momento, el competente para legislar en esta materia. Ciertamente, constitutía un espectáculo bochornoso el que quien era objeto de tesis doctoral —Perrate de Utrera por ejemplo— se viera impedido a vivir de la generosidad de los demás.\n\nEl Pueblo andaluz no debería consentir tan innobles ultrajes a su propia Cultura.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 398,
    "article_char_count_full": 2556,
    "article_char_count_review": 2556,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
