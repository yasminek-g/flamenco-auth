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
    "article_id": "1997-09-14-left-entrega-de-premios-de-la-c-tedra",
    "article_text_for_review": "Aires de Cádiz\n\nAntonio Núñez Romero\n\nLos denominados premios nacionales por la Cátedra de Flamencología de Jerez de la Frontera, fueron entregados en la Real Bodega de la Concha de González Byass, la noche del 24 de octubre, en una cena de gala ante un centenar de invitados.\n\nEl jurado encargado de conceder, en esta edición, los Premios Nacionales y Locales “Copa Jerez”, todos ellos patrocinados por la citada firma, lo integraron los siguientes señores: D. Francisco López-Cepero García (Paco Cepero), guitarrista y compositor, Premio Nacional de la Cátedra del año 1975; D. Juan de la Plata Franco Martínez, director de la Cátedra de Flamencología y Estudios Folklóricos Andaluces; D. Manuel Pérez Celdrán, subdirector de la misma; D. José Marín Carmona, relaciones públicas de la entidad; D. José María Lozano Romero, en representación de los patrocinadores González Byass, y D. Manuel Naranjo Loreto, que en su calidad de secretario de la Cátedra y del jurado de los premios, acordaron emitir el siguiente fallo:\n\nConceder los Premios Nacionales de la Cátedra de Flamencología, correspondientes a su XIX edición, a la mejor labor desarrollada por los siguientes artistas, autores y entidades, en los siguientes apartados de la actividad flamenca: PREMIO DE HONOR A LA MAESTRÍA: A Antonio Fernández Díaz \"Fosforito\" por su maestría, conocimientos y meritoria labor artística a lo largo de más de medio siglo (1940-97).\n\nMejor Labor en Cante: A José Mercé, de Jerez, por su labor artística desde hace 30 años (1967-97).\n\nMEJOR LABOR EN BAILE: A Eduardo Serrano Iglesias “El Güito”, por su labor artística a lo largo de 40 años (1957-97).\n\nMejor Labor en Guitarra: A Vicente Amigo, de Córdoba, por su labor como compositor e intérprete.\n\nEN DISCOGRAFÍA FLAMENCA: A la firma “Auvidis Ibérica” por su colección de discos “Flamenco Vivo”, en CD, editada en los últimos años.\n\nEN INVESTIGACIÓN: A “Historia del Flamenco”, obra en 5 tomos y 40 CD, dirigida por José Luis Navarro García, Miguel Ropero Núñez, Luis Soler Guevara y Ramón Soler Díaz, editada por Ediciones Tartessos, de Sevilla.\n\nEN PRENSA: A Angel Alvarez Caballero, crítico de Flamenco del diario “El País”, de Madrid.\n\nEN RADIO-TV.: A José Mª Velázquez, por su programa “Nuestro Flamenco”, emitido durante 13 años consecutivos en Radio Clásica de RNE, en Madrid.\n\nA ENTIDADES FLAMENCAS: A la Peña Flamenca “Buena Gente”, de Jerez, por su promoción del flamenco durante 20 años.\n\nA LAS ARTES PLÁSTICAS: A la pintora sevillana Luisa Triana, por el conjunto de su obra sobre flamenco, especialmente en baile, expuesta en España y en América.\n\nLas distinciones “Copa Jerez”, para estimular la labor desarrollada por los artistas locales, en los apartados de Cante y Guitarra Flamenca, recayeron en los siguientes artistas:\n\nMEJOR LABOR EN CANTE: A Die- go Rubichi, de Jerez.\n\nMEJOR LABOR EN BAILE: A António el Pipa, de Jerez.\n\nMEJOR LABOR EN GUITARRA: A Gerardo Núñez, de Jerez.\n\nEstos premios nacionales tendrán de nuevo su continuidad bianualmente y seguirán patrocinados por esta importante empresa jerezana, ofreciendo esos trofeos originales, de una figura en bronce que simboliza el cante, el baile y la guitarra en forma de una mujer gitana y que es obra de la escultura Nuria Guerra.",
    "title": "Entrega de premios de la Cátedra de Flamencología",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 526,
    "article_char_count_full": 3254,
    "article_char_count_review": 3254,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-09-16-right-cr-nica-del-xxv-congreso-de-acti",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nJosé Luis Buendía López\n\nArrullados por las olas mediterráneas, un Congreso no parece un Congreso. Tal vez sea un crimen convocar a gentes, venidas de todas partes de España, y hasta de fuera de ella, para encerrarlos en un salón, alimentado por tubos de neón y aire acondicionado, mientras que a cien metros mal contados, el aire verdadero embalsama con los gemidos placenteros de un Mare Nostrum, decano de todos los mares, que tienen algo que contarnos, y llegan des- (Málaga, 1997)\n\nde el Limonar miles de aromas y sugerencias sensuales, que hacen encabritarse, como caballo sin freno, a las sensibilidades desde allí enclaustradas bajo el estigma de un flamenco que, en esta ocasión, pudiera parecer más cadena que afán liberador.\n\nSin embargo, los flamencos podemos con todo. Atravesamos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\ne tienen algo que contarnos, y llegan des- (Málaga, 1997) de el Limonar miles de aromas y sugerencias sensuales, que hacen encabritarse, como caballo sin freno, a las sensibilidades desde allí enclaustradas bajo el estigma de un flamenco que, en esta ocasión, pudiera parecer más cadena que afán liberador. Sin embargo, los flamencos podemos con todo. Atravesamos España de norte a sur, recalamos en cualquier sitio donde acojan con cariño nuestro arte y formamos una cadena de sólidos eslabones jondos, para amarrar con ciertas garantías la barca de nuestros sueños, sujeta a la furia de excesivos temporales y que, por ello, no quisiéramos ver definitivamente destruida. En esta ocasión, nos sobran motivos para el entusiasmo a los más de cien congresistas, cumplidamente acompañados, que nos acercamos hasta Málaga para la celebración que habíamos proyectado. El Congreso de Arte Flamenco, que primero fuera de organización de festivales y más tarde se denominara de Actividades Flamencas, cumplía veinticinco años. Unas bodas de plata que todos deseábamos solemnizar con el boato que tal efeméride merece, por lo que, al igual que las parejas de enamorados, elegimos para ello la Costa del Sol, el corazón de la misma, Málaga cantaora, el émbolo administrativo que mueve la maquinaria del turismo y la cultura de un litoral privilegiado desde tiempos inmemoriales. Pero no quisiera que el entusiasmo me negara la visión de otras parcelas de la realidad que no debo mencionar. He dicho que elegimos Málaga y no es cierto del todo. Mas bien lo es que Málaga nos eligió a nosotros, y como ya pasara con el Congreso celebrado en Benalmádena hace una década, la generosidad y capacidad organizativa de las Peñas malagueñas y de su afición de oro, salvaron un Congreso que pudo no celebrarse. Si en aquella ocasión fue Madrid la que declinó sus compromisos, en esta ocasión fue la ciudad francesa de Arlés, bellísimo enclave romano en el que todos soñábamos desde su aprobación como sede en la ciudad de Sevilla, la que nos dejó compuestos y sin Congreso, y ante la testiura de ver esparcidas nuestras ilusiones por ese mar de amargura de lo que no llegó nunca\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\nmbio ha ascendido, una vez más, a todos los cielos jondos. No sería justo pues que el cronista olvidara, en estos preliminares narrativos, su agradecimiento a cuantas personas e instituciones hicieron posible, no sólo enmendar el desaguisado, sino elevar a una altura digna la seriedad de nuestros trabajos congresuales y hacernos pasar unos días inolvidables. Desde luego, la Peña Juan Breva y su equipo de entusiastas flamencos ocuparía el primer lugar, pero también las entidades locales y regionales, y, ¿cómo no?, esa Caja Rural malagueña, que cuidó de que no faltara nada y nos brindó sus instalaciones a los que estábamos huérfanos de todo. De los cuidados del director del Congreso, Gonzalo Rojo, o las atenciones del coordinador del mismo, Rafael Mellado, pendientes cada uno en su parcela de que todo funcionara a la perfección (desde la entidad de las ponencias hasta la letra menuda de mil pe\n\n[ENDING CONTEXT]\n\nde la despedida.\n\nCon ese sabor agridulce, y el inmenso agradecimiento hacia ese puñado de malagueños que han impedido que nuestro Congreso peligra-ra en su edición número veinticinco, nos dijimos adiós, pero será hasta pronto.\n\nEl cielo azul de una ciudad cordobesa, de buenos vinos y mejor gente, nos aguarda, y la mirada serena de la Virgen de Araceli, allí, en aquel nido de águilas en donde tiene su camarín, hará la espera mucho más llevadera y más certero el rumbo de nuestros pasos, para que coincidamos, una vez más, en los senderos jondos que se abren camino por las telitas del corazón.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XXV Congreso de Actividades Flamencas de Málaga",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "16-20",
    "page_number": 16,
    "word_count": 4541,
    "article_char_count_full": 27801,
    "article_char_count_review": 4748,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  },
  {
    "article_id": "1997-09-20-right-las-bodas-de-plata-del-congreso-",
    "article_text_for_review": "Fotografías: José Pamos\n\nAnte la falta de compromiso, de resolución o de capacidad económica por parte de los adjudicatorios de Arles para la organización del XXV Congreso de Arte Flamenco, una vez más han sido los malacitanos los encargados de asumir la responsabilidad de la puesta en escena de las cuatro jornadas de estudio, de las dos complementarias y de las tres de aspectos lúdicos y culturales de los acompañantes. La valentía de los miembros de la Peña Flamenca \"Juan Breva\", que sopesaron y comprobaron la dadivosidad de la Caja Rural de Málaga y que contaron con el apoyo de su Ayuntamiento, la Diputación, la Consejería de Turismo y Deportes de la Junta de Andalucía, la Universidad de Málaga, la Asociación de la Prensa malagueña, el Patronato de Turismo de la Costa del Sol, el Ayuntamiento de Coín, de Fausto Muñoz, S. A., de Bodegas Gomara, de D. Manuel Rodríguez Becerra, de la Peña \"Pepe de la Isla\", de D. Ramón Eire y del Grupo\n\nCruzcampo, esa citada valentía ha servido para que el Congreso de las \"Bodas de Plata\" se llevara a cabo. No era esta empresa que gustara o apeteciera, mas la responsabilidad de responder a una necesidad perentoria, a un no cortar la trayectoria de los Congresos de Flamenco, hicieron que la entidad Peña Flamenca \"Juan Breva\" pusiera manos a la obra aún sabiendo que el tiempo era limitado y que por tanto podían surgir determinadas carencias en el desarrollo del evento cultural-artístico. Sin embargo, dichas eventualidades aparecieron como en cualquier otro Congreso, y siempre motivadas por circunstancias imprevistas y, a veces, por falta de puntualidad o formalidad de segundos. Claro, que esto siempre se elimina comprometiendo a los cumplidores o formales. Pero he de insistir —ante la premura de fechas—, que damos por bien organizado este XXV Congreso de Arte Flamenco de Málaga, y por muy bien cumplida la labor de sus organizadores.\n\nLa jornada del miércoles, 10 de septiembre, que servía —en sus aspectos burocráticos— como inicio del citado Congreso, tuvo en su tarde-noche y, más concretamente, en el Jardín Botánico Histórico de La Concepción, la actuación de Antonio Canillas, el cual estuvo acompañado por la guitarra de Gabriel Carrera, volviendo a ser fiel exponente de los cantes de su tierra, con un recorrido que evidenció su conocimiento de los personalismos malacitanos por los diferentes estilos que se enmarcan en la provincia mediterránea. No fue tan brillante en los fandangos de Huelva, y es que los años no pasan en balde.\n\nEl acto artístico que complementaban las actividades académicas de-sarrolladas durante la jornadas del\n\njueves, 11 de septiembre, se celebró en un improvisado escenario de la Venta San Alberto, donde actuaron Francisco Javier Sánchez Bandeira “Bonela hijo”, La Cañeta y José Salazar, sendos acompañados por la guitarra de Antonio Soto.\n\n“Bonela hijo” comenzó rindiendo homenaje a su tierra con malagueñas de La Trini, rematadas con cantes abandolaos en los que sobresalió el javegote. Sus siguiri-yas siguientes tuvieron el enfoque hacia “El Nitri” con cierta estento-reidad, “El Marruro” en la misma tesitura y Silverio con cierto apresuramiento final. Los tientos-tango es evidenciaron asomo a Juanito Mojama, Frijones, Cádiz y Triana. Finalizó su participación con bu-lerías. Difente y comedida fue la actuación de “La Cañeta” y su marido, José Salazar, comenzando su arte con cantiñas-alegrias con ciertas resonancias de “La Perla” y algo de apresuramiento en las de Pastora. Las soleares fueron centradas en Jerez, con ecos del Borrico y Tomás Torre. Los fandangos tuvieron matices de Huelva y las bulerías finales, en una verdadera demostración de profesionalidad y espectáculo flamenco, sellaron una velada que en su parte final contó con la gracia festera de la cantaora malagueña que, arropada por Jerez y Utrera-Lebrija, complementando su cante con graciosos pasos de baile.\n\nLa velada flamenca del viernes, 12 de septiembre, celebrada en la misma sede en que se desarrollaban las sesiones del Congreso, o sea, en la Caja Rural de Málaga. La misma contó, con el cante de “El Tiriri”, el toque de Juan Santiago y las palmas de Quico y Yaya; además de el arte del desaparecido José Manuel Ruiz Rosas “El Chino”, y de la\n\ncual no puedo ofrecer referencia por mi no asistencia a la misma.\n\nY llegamos a la gala final: la del sábado, 13 de septiembre, siendo esta la más esperada por los congresistas y por tanto la que generalmente resulta más completa. La misma estuvo protagonizada por el toque del jovencísimo Daniel Casares en el inicio, y por el arte de Rafaela Reyes \"La Repompa\" como parte central del espectáculo.\n\nDaniel Casares evidenció por malagueñas fuerza en su salida e identificación pronta del estilo, soltura en el toque y dotes de composición, así como mantenimiento del tema base de su creación malagueñera, derivando a los aires abandolaos seguidamente con cierto apresuramiento. A pesar de sus dieciséis años, Daniel muestra cualidades para la guitarra flamenca, siendo esta la segunda ocasión que tenemos de apreciar su arte, tras conseguir el Primer Premio para Jóvenes Guitarristas de la Diputación Provincial de Jaén de 1996.\n\nRafaela Reyes “La Repompa”, haciendo honor a su linaje artístico, comenzó su actuación con soleá iniciada al compás de soleá-bulería y basando su arte en el taconeo y derivando a las bulerías con determinada gracia. En la misma tesitura abordó las bulerías, mostrando formas y giros propios y un acompañamiento cantaor por el palo que evidenciaba su casta artística. Buen final de fiesta el ofrecido por esta artista malagueña y su hija.",
    "title": "Las bodas de plata del Congreso de Arte Flamenco",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 912,
    "article_char_count_full": 5602,
    "article_char_count_review": 5602,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1997-09-22-right-juan-manuel-vill-n-y-su-cancione",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDaniel Pineda Novo\n\n1) JUAN MANUEL VILLÉN: Novísimo/Cancionero/Erótico, Sentimental/Y/Flamenco/Escrito por... Sevilla, Librería de José G. Fernández, 1887; 96 págs. I Vol. 8°. Enc. rústica. (Su precio era de una peseta. Según Palau, valía en 1943: \"1 libra, 5 chelines, Dolphin\"). A pesar de que el librito se realizó \"por encargo\" y por necesidades económicas, mereció la pena.\n\n2) “Este libro ha sido editado por la Asociación de Amigos del Libro Antiguo. Se terminó de imprimir el día 9 de noviembre de 1990”. Todas las citas y referencias que hacemos en nuestro artículo se han tomado de esta edición, más asequible al público.\n\n3) Vid. mi libro Antonio Machado y Alvarez, “Demófilo”. Vida y Obra del Primer Flamencólogo Español. Madrid, Editorial Cinterco, 1991; págs. 78-100.\n\nI.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionado\"]\n\n. 2) “Este libro ha sido editado por la Asociación de Amigos del Libro Antiguo. Se terminó de imprimir el día 9 de noviembre de 1990”. Todas las citas y referencias que hacemos en nuestro artículo se han tomado de esta edición, más asequible al público. 3) Vid. mi libro Antonio Machado y Alvarez, “Demófilo”. Vida y Obra del Primer Flamencólogo Español. Madrid, Editorial Cinterco, 1991; págs. 78-100. I. INTRODUCCIÓN Desde hace años, como buen aficionado al flamenco y bibliófilo, andaba tras las huellas del librito de Juan Manuel Villén: Novísimo Cancionero Erótico, Sentimental y Flamenco, publicado en Sevilla, en febrero de 1887. Obra rarísima, ya que no se encuentra en los valiosos fondos de la Biblioteca Nacional de Madrid; aunque, cuál fue mi sorpresa, cuando en el otoño de 1990, mientras deambulaba entre las casetas de la \"Feria del Libro de Oca-sión\", de Sevilla, me encontré con una reedición facsímil, realizada, ex-presamente, para este acontecimiento cultural, por la \"Asociación de Amigos del Libro Antiguo\". No pude contener mi alegría y, rápidamente, lo adquirí. Llegado a casa, lo leí con avidez, aunque comprendí que distaba bastante de la Colección de Cantes Flamencos que, en 1881 publicó, con sentido científico, Antonio Machado y Alvarez, Demófilo. Pero seguía interesado por la misteriosa personalidad de su autor, del que no existían datos a pesar del artículo que su buen amigo, el folklorista Manuel Díaz Martín, le había dedicado en el diario El Tribuno, de Sevilla, el 20 de agosto del mismo año, elogiando la obra... Pero, ¿quién era Juan Manuel Villén? Indagando en archivos y bibliotecas y repasando libros antiguos, pude encontrar, por fin, algunos datos. Localicé, además, varias de sus obras, tanto en la citada Biblioteca Nacional como en la Biblioteca Universitaria de Sevilla, así como sendos ejemplares de su Cancionero: uno en la Biblioteca de la Facultad de Geografía e Historia, de la Universidad Hispalense, en los ricos fondos de la Colección Hazañas, y otro en la Biblioteca Capitular y Colombina, también de Sevilla. Este ejemplar, por cierto, lleva una sentida dedicatoria del autor al canónigo de la Catedral Sevilla. na, D. Eloy García Valero, compañero suyo en el diario El Español. $ ^{4} $ Así como otro ejemplar adquirido por el Centro Andaluz de Flamenco de Jerez de la Frontera del que, generosamente, me enviaron fotocopia. Asimismo, en sus Memorias, $ ^{5} $ el poeta y escritor sevillano Luis Montoto y Rautenstrauch, colaborador del citado diario político (conservador), nos habla de Villén, ya que D. Luis fue redactor del mismo desde 1873 a 1885... Ya íbamos tejiendo la madeja biográfica de Villén, aunque seguía preguntándome dón-de habría nacido. Por supuesto, y según se desprende de sus libros, era hombre muy enamoradizo, con experiencias femeninas y, sobre todo, muy andaluz, por su esti\n\n[ENDING CONTEXT]\n\nYo dije: ¡quién fuera uno Para verse entre tus muelas.\n\nMujer ninguna pretendas Si de su buena conducta No tienes seguras prendas.\n\nTiene mi calabocito Seis vigas y cinco claros; Los he contao mir veces Para entrenerme en algo.\n\nSi la maresita mía A su hijo viera preso Se gorbería á mori De dolor en er momento - Es un matrimonio unido La felicidad de dos Aves en un solo nido. ***\n\nComo estos, y mejores muchos de ellos, son todos los cantares que componen el nuevo Cancionero del señor Villen.\n\nLo dicho: vale el libro la peseta; que es más salado que las propias pesetas. Manuel Díaz Martín.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Juan Manuel Villén y su “Cancionero” Erótico-Flamenco",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "22-28",
    "page_number": 22,
    "word_count": 7099,
    "article_char_count_full": 42278,
    "article_char_count_review": 4476,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionado"
      }
    ]
  },
  {
    "article_id": "1997-09-30-right-noticiario-flamenco",
    "article_text_for_review": "S e convoca esta XV edición del Concurso Nacional de Arte Flamenco, que se celebrará del 27 de abril al 6 de mayo de 1998, y que se regirá entre otras, con las siguientes bases:\n\n* Los interesados en participar en el Corcurso deberán solicitar su inscripción mediante escrito dirigido a la Sra. Presidente de la Comisión Organizadora, F.P.M. Gran Teatro, Avda. Gran Capitán, 3. 14008 Córdoba, hasta el 1 de abril de 1998, indicando en su escrito: Nombre y apellidos; nombre artístico, si lo tuviera; fecha y lugar de nacimiento; lugar de residencia, domicilio, teléfono, sección (cante o toque) en que concursa y premios a los que se presenta.\n\n* Los gastos de viaje de los concursantes, dentro del territorio español, serán abonados por la Organización a razón de 24 Ptas. el Km. Los no residentes en el término municipal de Córdoba devengarán una dieta diaria de 8.000 pesetas durante los días en que, a requerimiento del jurado, intervengan oficialmente en el Concurso.\n\n* El Concurso pondrá a disposición de los concursantes profesionales cualificados para acompañamiento en cualquiera de las secciones del Concurso. * La fase de clasificación se celebrará durante los días 27 de abril al 5 de mayo de 1998. El lugar, día y hora será comunicado previamente a los concursantes admitidos.\n\n* Las pruebas de la fase de opción a premio serán públicas y se celebrarán duranre los días 8 al 13 de mayo de 1998.\n\n* Los concursantes premiados quedan obligados a actuar en un acto público que se celebrará el día 16 de mayo de 1998, en cuyo transcurso tendrá lugar la entrega de premios.\n\n* Se establecen, entre otros, los siguientes premios:\n\na) Sección Cante. Premio especial “Silverio”, al cantaor o cantaora más completo. Dotado con una escultura de Venancio Blanco y 1.000.000 de pesetas. Premio “Manuel Torre” (1 seguiriγas, 2 tonás); Premio “Niña de los Peines” (1 soleares, soleá por bulerías o bulerías por soleá, 2 bulerías); Premio “Dolores la Parrala” (1 serranas, livianas, tonás campesinas, y 2 El Polo, La Caña, peteneras); Premio “Enrique el Mellizo” (1 alegrías, mirabrás, romeras, caracoles, rosas, y 2 tientos, tangos): Premio “D. Antonio Chacón” (1 malagueñas, granaínas, cartagenera, y 2 tarantas, tarantos y otros cantes mineros); Premio “Cayetano Muriel” (1 fandangos locales, y 2 fandangos personales); Premio “Pepa Oro” (1 guajiras, punto cubano, colombianas y 2 milongas, vidalitas), todos estos premios con diploma y 250.000 pesetas.\n\nb) Sección Baile. Premio Especial “Antonio”, al bailaor o bailaora más completo. Dotado con una escultura de Venancio Blanco y 1.000.000 de pesetas, además de los Premios “Juana la Macarrona”, alegrías, mirabrás, romeras y rosas; “La Malena”, tangos, tientos, garrotín, tarantos y zambra; “Vicente Escudero”, farruca, zapateado y martinetes; “La Mejorana”, soleares, seguiriyas, La Caña y El Polo; “Encarnación López”, caracoles, rondeñas, serranas, peteneras, guajiras, jaberas...; “Paco Laberinto”, bulerías, zorongo, alboreá, rumba y tanguillo, con diploma y 250 pesetas.\n\nc) Sección de Toque de Guitarra. Premio especial “Ziryab”, al guitarrista más completo. Dotado con una escultura de Venancio Blanco y 1.000.000 de pesetas, además de los Premios “Ramón Montoya”, sólo flamenco (concierto); y “Manolo de Huelva”, acompañamiento a cante y baile, con diploma y 250.000 pesetas.\n\nE1 día 6 de septiembre del corriente, se celebró en Mairena del Alcor, su tradicional Festival Flamenco, en el patio de la Academia. Contó con un cartel de lujo como nos tiene acostumbrados. Fue una asistencia masiva, calculando unas tres mil personas.\n\nAbrió dicho evento, el cantaor Manuel Campos, que fue el ganador del concurso, interpretando varios cantes, siendo muy aplaudido en la soleá. A éste le siguió José de la Tomasa, con la guitarra de Manolo Franco. Fue muy aplaudido en todo su repertorio, sobre todo cuando interpretó la siguiriya. A continuación le tocó el turno a Manuel Mairena, el verdadero protagonista del Festival, hermano del genial Antonio, al cual su pueblo le arropó con el calor en toda su actuación. Posteriormente, por turno, le co-\n\nFINAL DEL XXXVI CONCURSO DE CANTE JONDO 《ANTONIO MAIRENA》\n\nrrespondió al baile —se esperaba con mucha expectación a la bailaora cordobesa Inmaculada Aguilar—, ya que era la primera vez que participaba en dicho festival. Bailó por soleá acompañada en el cante por Mano-lo Cortés, las guitarras de Mano-lo Flores y Ramón Rodríguez y las palmas de Finito y El Pipa. Obtuvo un magnífico triunfo, siendo de los artistas flamencos más aplaudidos.\n\nLlegado el descanso, abrió la segunda parte el cantaor de la Puebla de Cazalla, Diego Clavel. Estuvo poseído por la profesionalidad y las ganas de agradar, sobre todo por tientos y siguiriyas; le acompañó la guitarra de Antonio Carrión. Más tarde le tocó el turno a Calixto Sánchez, otro hombre de la tierra y acompañado por Manolo Franco, fue uno de los triunfadores. Le siguió La Macanita con su grupo, que sólo apuntó detalles por soleá y bulería.\n\nMás tarde, el fin de fiesta, con Fernando de la Morena que nos enganchó con una soberana lección de compás y de cómo cantar gita- no. Secundado por el baile añejo de Luisa Torrán, Diego de la Mar- gara, La Bastiana, Rafita y Curro de la Joaquina, más la guitarra de Fernando Moreno. Si quieres descubrir nuevos horizontes..",
    "title": "Noticiario Flamenco",
    "periodical": "candil",
    "issue_id": "1997-09",
    "year": 1997,
    "language": "es",
    "article_type": "article",
    "pages": "22-30",
    "page_number": 22,
    "word_count": 861,
    "article_char_count_full": 5324,
    "article_char_count_review": 5324,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
