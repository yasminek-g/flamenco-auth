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
    "article_id": "1983-11-12-right-editorial-manuel-torre",
    "article_text_for_review": "N O sólo era la tortura de su voz, que la extrajo de la antigua quejumbre del tamaño de los siglos, con sus negros perfiles, al descubierto, de manera que pudieron salpicarnos sus heridas. No sólo era su grito, que a pura dentellada, se acercaba a la boca para que el eco de desgarrados éxtasis, se hiciera inteligible, tal si la memoria de la sangre pudiera dulcificarse con el labio levemente sonrosado de la madrugada. No sólo era su cante insondable legado, sin posible alternancia, corazón de caoba, soterrado fragor de morenas tribus que pugnan por otras relevancias de vida.\n\nTambién eran su gesto pedernal que al rozarlo los ojos, destelleaba fuego, su gesto majareta de gallos de pelea, renqueantes pollinos y galgos afilados. También era su vida tierna y displicente, agrupada de aullidos, ebria por el sudor de tantos venerandos testimonios, rota desde dentro, antes de nacer, hollada por el tropel de estirpes vengadoras; su vida imprevisible, sin aseados hábitos ni costumbres corteses, sólo fiel al empuje de la inspiración como una torrentera, como un súbito arrebato de elementales ternuras. Su vida inadaptada al coro, nacida de los mismos epicentros del dolor.\n\nCincuenta años después se sabe, al menos, que el mismo cante jondo tuvo en Manuel Torre hermosa e irrepetible encarnadura.",
    "title": "Editorial Manuel Torre",
    "periodical": "candil",
    "issue_id": "1983-11",
    "year": 1983,
    "language": "es",
    "article_type": "editorial",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 213,
    "article_char_count_full": 1302,
    "article_char_count_review": 1302,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-11-13-left-manuel-torre-cantaor-largo",
    "article_text_for_review": "Por Pacò Vallecillo\n\nA anécdota —narración breve de algún suceso particular—, la petite histoire de Manuel Torre es sobradamente conocida y popularizada hasta extremos muy cercanos al tópico. Los galgos, el borriquillo garabito en moruno desde cuyos enclenques lomos casi arrastraba las largas piernas el Niño de Jerez, los gallos de pelea, sus amoríos y sus rarezas son del dominio público; y ese mismo público no es ajeno a más de una invención nacida de la desbordante fantasía andaluzay flamenca.\n\nUno llegó también a conocerlo a través de la directa referencia familiar —paterna en este caso— y conserva una vieja fotografía hecha al final casi de la década de los años 20, en la desaparecida plaza de toros de Ceuta, en ocasión de una fiesta flamenca. En el centro de un nutrido grupo de aficionados, militares y civiles, Manuel aparece con la cabeza levantada en gesto de altivez, un rebelde mechón caído sobre la frente, bien visible la color entre ceetrina y renegrida, las palmas de las manos apoyadas en las rodillas y en un gesto hierático y solemne, consciente de su realeza faraónica. Presto a oficiar la ceremonia ritual, a su alrededor los fieles acólitos de turno: Juan y Pepe Torre, Pepe Pinto, el Manquito de Jerez y el Músico Mayor del Reino, Manolo el de Huelva...\n\nDe Manuel se ha dicho todo y hasta se han dicho inconveniencias sobre lo que muchos han dado en llamar su limitada riqueza de coplas y estilos. Manuel Torre, se suele aseverar electrizaba en una media docena de cantes y siempre que los duendes acudieran generosos a la invocación. En una de las muchas tardes perdidas —y religiosamente halladas para un indeleble recuerdo— con Antonio Mirena, cuando repasábamos cuentas pendientes para ponernos al día, como él solía decir, salió —¡cuántas veces salió!— la conversación sobre Manuel. Como uno tenía por archiconocida su reacción, no tuvo que hacer gran esfuerzo para tirarle de la lengua: —Sí, Antonio, enorme, grandioso..., pero ¿no crees que un poco corto?, ¿no te parece que no se le podía llamar cantaor fundamental, largo? Antonio se enardeció y al desbordarse en sus explicaciones, sólo pude —una vez más— asentir mudamente. Todos los cantes fundamentales fueron dominados por el genio jerezano, aunque forzoso sea reconocer que no la totalidad de estilos o modalidades personales que, especialmente en la Soleá, gozan de una extensísima gama, fueran cantados por él. Pero Manuel no se limitó exclusivamente a esos cantes que suelen ser llamados, con mayor o menor impropiedad, gitanos. También fue grande en toda la gama de cantes del oriente flamenco: la taranta, la malagueña, el taranto, que en su voz y su maestria se convirtió en un cante grande y solemne; la cartagenera, el garrotín, la farruca... Y también la petenera, añadió Antonio. —¿La petenera?, demandó mi infatigable curiosidad. —Sí, la petenera. Y en el silencio de su recoleto estudio, con aquella deliciosa media voz que no rebajaba un ápice la grandiosidad expresiva del artista, me hizo ver la enorme diferencia entre la petenera de Pastora—escuela de Medina el Viejo transmitida a través del Niño Medina— y una forma más desarrollada y, sobre todo, aunque esto parezca exageración, infinitamente más flamenca. Muchos aficionados y no pocos eruditos abrigan ciertas dudas en cuanto a la capacidad de Manuel Torre, sosteniendo en cierto grado la sospecha de que, además de irregular no tuvo la jerarquía máxima que posiblemente se derivó más de la leyenda que la realidad. Error craso que, al margen incluso de unas referencias orales que nos han llegado de primera mano y en época inmedia, puede fácilmente comprobarse por la amplia discografía que del jerezano se conserva. Las envidiables colecciones de Antonio Reina y Manuel Yerga, aparte de algunas más ubicadas en Málaga de la que uno sólo tiene referencias indirectas, están ahí para demostrarlo. Y están ahí con todos los defectos de la técnica incipiente, agravada, además, con la nula preparación que pudiera —y no pudo jamás— predisponer al cantaor, revisar y rectificar los resultados inicialmente obtenidos; darle a cada cante un tempo suficiente para su desarrollo completo y demás cuidados cuya omisión resultaría absolutamente inconcebible en la actualidad. Por esos discos/placas tantas veces defectuosos, apresurados, tantas veces pobres de guitarra, puede extraerse la consecuencia y calcular con bastante aproximación hasta qué punto Manuel Torre fue grandioso y largo en el sentido que la longitud tiene en el Cante.",
    "title": "Manuel Torre, cantaor largo",
    "periodical": "candil",
    "issue_id": "1983-11",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 730,
    "article_char_count_full": 4502,
    "article_char_count_review": 4502,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-11-13-right-manuel-torre-50-a-os-despu-s",
    "article_text_for_review": "Por Juan de la Plata\n\nReproducimos el artículo galardonado con el deseo de contribuir a su difusión o, lo que es lo mismo, a que se conozca más profundamente la figura inconmensurable de Manuel Torre.\n\nN O ha habido, en toda la historia del cante flamenco andaluz, un nombre de cantar más sonoramente redondo, ni más gitano, que el de Manuel Torre, el «hombre con más cultura en la sangre» que llegó a conocer, ese otro andaluz universal que fue Federico Garcia Lorca.\n\nSólo por esta insólita definición del poeta de Granada, ya se merece el cantaor de Jerez que Andalucía le recuerde y le rinda homenaje, en este cincuentenario de su muerte que el mes próximo va a cumplirse. Ahora, precisamente, cuando los andaluces andamos echando cuentas y haciendo balance y rebusco de la tremenda herencia cultural que arrastra nuestro pueblo. Manuel Torre es ya un nombre legendario que forma parte, por derecho propio, de la milenaria cultura popular andaluza. Pienso que su nombre de piedra y campanario debe colocarse junto al de Telethusa descalza, la madre grande y prolífica de todas las que fueron y son bailaoras del Sur; junto al del propio Lorca y al de su amigo y admirador Joselito el Gallo.\n\nLlevaba en sus venas el arte de aquellos primitivos gitanos, que llegaron a nuestras tierras, desde la lejana orilla del Sind pakistaní, para asentarse con sus canastas, sus mimbres y sus yunques de fragua, en el mismo corazón de Jerez, a la que Federico, en un poema alucinante, habría de llamar luego la «ciudad de los gitanos» por antonomasia.\n\nPorque los gitanos supieron hacer suyos, como nadie, los melismáticos suspiros que el árabe Ziryab, el «pá-jaro negro» de Persia, nos había dejado flotando en el aire mágico de la corte de los Abderramanes. De esta forma, la raza gitano-andaluza a la que pertenecía Manuel Torre, llegó a fundirse, con el tiempo, en los crisoles de las fraguas de Triana, de Jerez, de Cádiz, Los Puertos y Málaga, acuchillando los vientos con el escalofrío cortante de sus cantes y lamentos.\n\nEn medio de ese mundo gitano- andaluz de principios de este siglo, Manuel Torre fue como un dios gig- gante para su pueblo; columna de tra- diciones, ecos y leyendas. Un ídolo\n\nhermoso y moreno, al que rendían pleitesía y vasallaje todos los hombres y mujeres de su raza, y al que incluso ofrecían vírgenes de ojos azabache, galgos corredores, borriquillos morunos y policromados gallos de pelea.\n\nManuel Torre, plantado en medio de los tiempos y las mareas del cante, fue un héroe popular en la Andalucía incontaminada de su tiempo, cuyos cantes todos pretendían escuchar y sólo escucharon los elegidos. Porque el cante de Manuel no estaba hecho de la empalagosa miel que tanto gusta a las masas. El suyo era un cante sentimental y hondo: un dolor amasado de furia y tristeza, que nacía de la misma pena de su corazón gitano, de llanto de siglos derramado por su raza, en busca de una libertad presentida.\n\nCon Torre nació y murió el duende. El duende era todo Manuel. Su voz recia diciendo la copla, masticando el cante, hablándole a la pena de tú, era la propia voz de las tinieblas, de lo infinito y lo profundo, hecha «soníos negros», como él mismo definiera su propio duende. «Todo lo que tiene soníos negros —le dijo un día a Falla— tiene duende». Y la vieja copla gitana emergía de él como un caudal de negra e infinita tristeza, soterrada angustia de vitales quejas, arañando la noche de los tiempos.\n\nSu cante era corto, de inspiración nunca fácil, de saberse agusto con su gente, con los cabales que le seguían. Jamás alargaba las melismas, sino era como muy preciso. Los tercios le brotaban con toda naturalidad. Cantanba hablando. Y era sobrio en sus apoyaturas musicales, como era clásico en su forma de interpretar. Como le ocurría, por ejemplo, en la solea, de la que hacía un cante de mensaje directo, de comunicación inmediata.\n\nY lo mismo le ocurría con todos sus demás cantes, algunos de los cuales los reducía a su más mínima expresión, como su bulería para escuchar. La síntesis y lo emocional, eran las notas que siempre prevalecían en el cante del gigante jerezano. El ay de su seguiriya y el ay de su saeta, son dos ejemplos de notas afiladas y breves, que traspasaban como dardos, sin necesidad de que hubiera que alargar esos quejos, que con la infinita tristeza que Manuel les imprimía, era más que suficiente y sobraba todo lo demás.\n\nPorque en el cante de Manuel Torre no había adornos, floreos, ni empalagosos vibratos labiales. Todo era directo, simple y llano. Cante gitano de corazón a corazón. Y la perfección del drama de su raza únicamente se convertía en tragedia, en esa hermosa obra cumbre de «Santiago y Santana», su cambio por seguirias, en la que se abría las entretelas del alma, en un ay interminable, dando paso al vuelo aleante de una angustia metafísica, infinita y dolorosa. Entonces sí que el quejío corto y seco de Manuel se hacía como una puñalada en medio de la noche.\n\nCincuenta años después de su muerte, yo evoco hoy aquí a Manuel Torre, abriendo sus grandes manos, negras de sol y de pátina de siglos, arrancándose sus propias entrañas, hechas coplas de soledad y de muerte amarga, sin flores ya para su silencio.",
    "title": "Manuel Torre, 50 años después",
    "periodical": "candil",
    "issue_id": "1983-11",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 901,
    "article_char_count_full": 5190,
    "article_char_count_review": 5190,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1983-11-14-right-la-expresi-n-jonda-de-manuel-tor",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Antonio Núñez Romero\n\nN julio del presente año se cumplió el cincuentenario de la muerte del gran cantaor jerezano Manuel Torre «Niño de Jerez», de quien dijera el poeta de Fuentevaqueros, Federico García Lorca, que era el gitano de mayor cultura en la sangre.\n\nEn diciembre de 1978, el Excmo. Ayuntamiento jerezano con una comisión nacional nombrada al efecto, conmemora con los más altos honores el centenario de su nacimiento. Acto que fue promovido por la Cátedra de Flamencología de Jerez. Se le rotuló una calle de nueva apertura en la Plaza Madre de Dios. Se celebraron actos culturales, con exposiciones, conciertos y un gran festival flamenco en el que se contó con la presencia de las hijas de Manuel Torre. Hubo además, unos juegos florales dedicados a exaltar su memoria y que\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"flamencólogo\"]\n\npor la Cátedra de Flamencología de Jerez. Se le rotuló una calle de nueva apertura en la Plaza Madre de Dios. Se celebraron actos culturales, con exposiciones, conciertos y un gran festival flamenco en el que se contó con la presencia de las hijas de Manuel Torre. Hubo además, unos juegos florales dedicados a exaltar su memoria y que sirvieron de homenaje a la raza gitano-andaluza, a la que perteneció Manuel. Como mantenedor actuó el escritor y flamencólogo Juan de Dios Ramírez Heredia; seguido de una ronda poética a cargo de Antonio Murciano, Manolo Ríos Ruiz, José Luis Tejada y Juan de la Plata. Manuel Soto Loreto, nace en Jerez el día 5 de diciembre de 1878 en la calle Alamos, número 22; en el corazón de la Plazuela, según consta en la placa que se instaló en la fachada de la casa, el día 12 de noviembre de 1959, para honrar su memoria, por iniciativa de la Sección de Flamencología del Centro Cultural Jerezano y el Excmo. Ayuntamiento. Muere este gitano de «duendes» y «melismas» y de especial «jondura» en la Sevilla del año 1933. En esta Sevilla que muchos años antes lo consagrara y lo confirmara como uno de los mejores cantaores de todos los tiempos. La Junta de Andalucía, a través de su Departamento de Flamenco, que dirije el escritor y amigo Paco Vallecillo y la gran familia del mundo flamenco, le tributan, en estas fechas del cincuentenario de su muerte, el más cariñoso y justo de los recuerdos. Sus soleares, sus siguiriyas, sus campanilleros, sus tarantas y otros estilos de cante, donde Manuel Torre ponía los mejores sentimientos, se escucharán de nuevo y servirá de recuerdo para los que saben, aprecian y entienden el «eco» gitano, de un gitano de Jerez, que supo granjearse la admiración y el respeto de la Cava Trianera —emporio flamenco de aquellos tiempos—, donde se formara Manuel, el más gitano, el más jondo, el de los sonidos estre- mecedores, el del compás exacto, el de la música honda, que tuvo su propia letanía y en el que se inspiraron muchos poetas. Y Antonio Murciano, d\n\n[ENDING CONTEXT]\n\nlo profundo del alma, (manos toscas para el son, manos curtidas, de azada) Manuel El Torre bebía en su vino la esperanza, mientras en su propia sangre, tan antigua, tan arcaica, de gitano puro y recio, bullía lo que penaba.\n\nLuego, mandando el silencio con su mano negra y larga, fue sacándose las penas que en su corazón guardaba, convirtiéndole en coplas con su dura voz quebrada.\n\nPlantó su gente en la tierra y el eco se lo llevaba —ya crecido y hecho flor—, pájaro seco y sin alas.\n\nLa figura de Manuel verde y sepia, boca árida, bebió de nuevo su vino, pero ya sin esperanza.\n\nManuel Ríos Ruiz\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LA EXPRESION JONDA DE MANUEL TORRE, VISTA POR TRES FLAMENCOLOGOS",
    "periodical": "candil",
    "issue_id": "1983-11",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "14-18",
    "page_number": 14,
    "word_count": 3783,
    "article_char_count_full": 21504,
    "article_char_count_review": 3650,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "flamencólogo"
      }
    ]
  },
  {
    "article_id": "1983-11-18-right-discografia-placas-de",
    "article_text_for_review": "Discografía (placas) de artistas flamencos\n\nMANUEL TORRE",
    "title": "DISCOGRAFIA (PLACAS) DE",
    "periodical": "candil",
    "issue_id": "1983-11",
    "year": 1983,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 7,
    "article_char_count_full": 56,
    "article_char_count_review": 56,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
