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
    "article_id": "1986-05-17-right-el-peso-de-una-localidad-cantaor",
    "article_text_for_review": "Por: Manuel Ríos Vargas\n\nA lcalá de Gua- daira tiene un gran peso espe-\n\ncífico en el ámbito del cante flamenco, esto es indudable. Alcalá —parafraseando un poco al Piyayo—, causa un respeto imponente.\n\nHay quién afirma que el área geográfica cantaora la conforman y componen las localidades que quedan dentro de un cierto triángulo; otros por el contrario aseveran que no es un triángulo sino un cuadrilátero, de todas formas y sea cual fuere la figura geométrica que defina dicha zona cantaora, Alcalá de Guadaira siempre queda dentro de la misma. Incluso para los que creen que el cante está en el margen izquierda del Guadalquivir —el río grande—, tampoco le importa a Alcalá que esta creencia sea más o menos correcta, pues también estamos en la orilla izquierda de este hermoso río.\n\nAún hoy día, en que para nuestra desgracia Alcalá ha enmudecido en cuanto a flamenco se refiere, los cantaores cuando vienen aquí se esfuerzan mucho más que en cualquier otro sitio. Son varias las razones para que esto\n\nocurra, y vamos a ir desgranando estas mismas razones una a una. Alcalá tiene un público entendido y respetuoso y, es respetuoso quizás, por poseer ese gran don cual es el entendimiento, todo esto hace que nuestro público —respetable y soberano—, sepa escuchar, y a la vez que escucha sepa discernir, digerir y saber lo que escucha; otra de las razones pudiera ser la ubicación donde bianualmente se celebra el Festival local, ese grandioso Patio de Armas de nuestro castillo árabe, donde el marco no puede ser más bello y, donde quizás a una cierta hora de la madrugada sintamos como un hálito, hálito cual es el alma de Joaquín el de la Paula vagando en nuestro derredor. Porque no hay duda de que al menos cada dos años, Joaquín vuelve a Alcalá para estar con nosotros y degustar los cantes que se hacen en el Festival que lleva su nombre.\n\nEntonces el Soberano le da licencia a éste para que baje a la tierra; pero él antes de estar con nosotros para escuchar el cante, se ha entretenido un poco; ha visitado su cueva, aquella cueva donde tantos años viviera en compañía de su esposa e hijos, se ha aso- Otra de las razones es porque en Alcalá siempre ha existido una buena escuela de cante, cuyos alumnos muy aventajados por cierto, han sido entre otros, y sólo por citar a algunos: La Roezna, Tío Frasco, Joaquín el de la Paula, El Gordo, Paco el de la Malena, Algodón, Bernardo el de los Lobitos, Eloy Curraga, los hermanos Castejón, El Curilla, El Platero, El Sevillanito, Manolito el de María, Enrique de Joaquín, Mercedes de Joaquín, Manolo el de la Gorda, etc., etc.\n\nEntonces sí, es ahora ya realizadas todas estas obligaciones cuando llega al Patio de Armas para escuchar el cante. Todo esto es lógico que lo sepan los artistas, y de ahí su esfuerzo sobrehumano para cantar bien pues, saben que los está escuchando un patriarca del cante y un soberano por excelencia.\n\nmado al arco de la calle San Fernando, divisando así la célebre y antigua Venta de Platilla y, ha entrado en la ermita para, postrarse a los pies de la Virgen del Aguila.\n\nExplicado queda aquí el peso de la localidad cantaora de Alcalá de Guadalra.",
    "title": "El peso de una localidad cantaora",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 553,
    "article_char_count_full": 3140,
    "article_char_count_review": 3140,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-05-18-left-m-s-sobre-el-ni-o-de-alcal-y-el-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nS uena el teléfono. Lo cojo y al otro lado del hilo co-\n\nmunicante está un andaluz de residencia forzada en Cataluña.\n\n¡Qué gran aficionado y qué buen andaluz es!\n\nMe dice: «Señor Yerga: ¿Ha leído usted el trabajo que firma Ríos Vargas?». Sí lo he leído, le dije. Y prosigue: «¿Cómo dice ese señor que “El Sevillanito” se llamó Antonio Ribero, si en las placas gramofónicas consta Manuel Carrera?».\n\nTraté de resolverle su duda, pero no me fue posible debido a que en aquel momento me encontraba soportando un catarro gripal imponente que me impedía hablar. Ante tal imposibilidad decidimos cortar nuestro diálogo.\n\nHoy, repuesto físicamente, me decido, muy gustoso a escribir a mi amigo andaluz para decirle que estoy de acuerdo con él en cuanto a que, «prima facie», el título, como aparece el\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionado\"]\n\nme fue posible debido a que en aquel momento me encontraba soportando un catarro gripal imponente que me impedía hablar. Ante tal imposibilidad decidimos cortar nuestro diálogo. Hoy, repuesto físicamente, me decido, muy gustoso a escribir a mi amigo andaluz para decirle que estoy de acuerdo con él en cuanto a que, «prima facie», el título, como aparece el trabajo del señor Ríos Vargas y con letras relevantes, puede inducir a equivocación a todo aficionado poco adentrado en estos temas. Por supuesto que, al igual que yo, estoy seguro que han debido ser muchos los que no han tenido la menor duda en la interpretación de lo escrito por el buen colaborador de la revista. Creo que en la nómina general de cantaores sólo existen dos, «Niño de Alcalá» y un «Sevillanito». El primer nombre artístico de «Niño de Alcalá» fue para Bernardo, pero como todos sabemos, al poco tiempo de instalarse en Madrid, fue rebautizado por sus seguidores con el de «Bernardo el de los Lobitos» y me consta que con este título o sobrenombre artístico se sintió feliz y contento hasta sus últimos días de vida. El segundo fue para Antonio Ribero. Será cierto, no lo dudo, que en Alcalá y en principio, le llamasen «El Sevillanito», pero en su corta vida, como cantaor fue nominado «oficialmente» como «Niño de Alcalá». La verdad es que Antoñito no podía grabar con el título de «El Sevillanito», porque otro artista que aún vivía y que había grabado con ese mismo título, se lo hubiera impedido de alguna forma legal, ya que es de suponer que en la Sociedad General de Autores de Espa- Por: Manuel Yerga Lancharro ña, no lo hubiesen admitido porque estimo que no pueden figurar inscritos dos artistas con el mismo título. Dado que Bern\n\n[ENDING CONTEXT]\n\notra cosa que transmitirnos las variantes recogidas en su patria chica entre los amigos y familiares octogenarios del extinto cantaor alcalareño. Si le han equivocado, él nos ha equivocado. Si le han dicho la verdad, esa verdad es la que él nos ha ofrecido generosa y filantrópicamente.\n\nSepa, amigo mío, que los que nos dedicamos a investigar la vida de los artistas, estamos siempre expuestos a ser manipulados por personas de mala fe. Pero sepa también que nunca nos presentaremos, a sabiendas, a ofrecer a nuestros lectores versiones que vayan en contra de la verdad.\n\nPUBLICIDAD\n\nRepresentante\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Más sobre el Niño de Alcalá y el Sevillanito",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 1094,
    "article_char_count_full": 6420,
    "article_char_count_review": 3345,
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
    "article_id": "1986-05-19-left-un-adi-s-a-manolo-heras",
    "article_text_for_review": "Por: José Manuel Gamboa\n\nPero su débil constitución física se iba deteriorando por momentos y hu-\n\nManolo Heras había nacido en Madrid el 2 de diciembre de 1919, y por sus venas corría sangre andaluzay castellana. Con apenas siete años ya cantaba las saetas que aprendió en la Semana Santa de Jaén y a los nueve se le podía escuchar en los cafés cantantes de Madrid, formando pareja artística con el también prematuro guitarrista David Moreno. Tras un paréntesis en su carrera artística, motivado por la Guerra Civil, vuelve a incorporarse a su mundo buscándose la vida en las fiestas. Frecuentaba por aquellas fechas Villa-Rosa, Los «Grabieles», La Venta de la Peque..., coincidiendo con artistas de la talla de Pericón, Chaqueta, Perico el del Lunar o Juanito Varea, por nombrar sólo algunos.\n\nM il novecientos ochenta y cindo se lle- vó consigo varios «flamencos». De los más conocidos ya se han hecho eco los medios de comunicación, pero no debemos olvidar a otros que, desde un segundo plano, colaboraron igualmente a escribir una página más en la historia de nuestro arte. Manolo Heras fue uno de ellos. En septiembre se nos marchó definitivamente aquel entrañable y «menuito» cantaor.\n\nUn buen puñado de años separaban aquella actuación en la función organizada para contribuir a la edición del libro «Arte y artistas flamencos», de Fernando el de Triana, donde compartió cartel con Bernardo el de los Lobitos, Palanca, La Quica... y La Argentina, excepcional anfitriona del evento —corría entonces 1935— y su última aparición en público en la Cumbre Flamenca de 1984, esta vez al lado de Rafael Romero, Perico el del Lunar (hijo) y del llorado Joselero de Morón. Entre tanto, medio mundo supo de su buen hacer, primero en la compañía de Antonio junto a otro Antonio inolvidable: Mairena; después y tras un descanso forzoso ocasionado por una bronquitis que jamás le abandonaría, de la mano de Rafael de Córdova.\n\nbo de recurrir a otros menesteres menos premiosos que le asegurasen su manutención. Trabajó en muy diversos oficios, el último: vendedor de lotería. Sin embargo, su alejamiento de las tablas no fue óbice para mantener vivo su espíritu flamenco, que como él decía «es una enfermedad crónica que tiene uno, desde que la coge hasta que se muere. Se muere uno con esa enfermedad, no hay médico que se la cure a uno. Ná más que tomando pelotazos y fumando cigarros». «Eso sí, como un señor».\n\nEstuvo y, como a menudo quiso predecir, se fue de este mundo solo. Muchos buscamos y disfrutamos, a tragos cortos, de su amistad, pero era como si un designio de soledad envolviera su existencia. Solo y con lágrimas en los ojos hubo de escuchar por el transistor, desde la cama del hospital, el homenaje que le tributaron quienes bien le quisieron. Allí encontramos a Carmen Linares, Vicente Soto, Ramón el Português, Enrique Morente, José Mercé, Enrique Escudero, Pepe Habichuela y muchos más.",
    "title": "Un adiós a Manolo Heras",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 488,
    "article_char_count_full": 2903,
    "article_char_count_review": 2903,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-05-19-right-los-fandanguillos-de-huelva",
    "article_text_for_review": "Por: M. Yerga Lancharro\n\nA migo Antonio Mata Gómez, los fandangui-\n\nllos de Huelva nada tienen que ver con el árbol-fandango de Málaga-verdial. Esto que voy a decir, amigo lectores, lo he repetido ya en varias ocasiones. Hoy, conservando mi tesis, vuelvo a escribir sobre este tema, para dar más consistencia al contenido de la carta de don José Sánchez, vocal cultural de la Peña Flamenca de Huelva, publicada en la revista «Sevilla Flamenca» en su número «Especial Navidad'85», porque, ciertamente, en Huelva tienen sus cantes propios, tan variopintos como no los tiene ninguna otra provincia andaluza.\n\nQue yo sepa, tienen sus cantes propios, Alosno (inmensa cuna), Almonaster, Cañañas, Cerro Andévalo, Cortegana, Cumbres, Encinasola, Huelva, Paymogo, Puebla de Guzmán, Riotinto, Santa Bárbara, Tharsis, Valdelamusa, Valverde y Cabezas Rubias.\n\nSi los cantes de la vieja Onuba tienen por nominación FANDANGUILLOS, no es porque se trate de cantes inferiores en flamenquismo y «jondura», ni mucho menos, sino que son llamados así para que sean distinguidos de los nacidos del gigantesco árbol malacitano, padre de casi todos los cantes.\n\nHe publicado, en cierta ocasión, que «el fandanguillo» de Huelva, ningún foráneo lo sabe cantar, porque para ser cantado con total fidelidad, el artista tiene que haber nacido y haberse criado en cualquier pueblo de los enumerados. Ningún artista foráneo —repito— podrá interpretarlo en su más prístina pureza por muy buen cantaor que sea. Este es un fenómeno que no lo puedo explicar, porque no tiene explicación.\n\nSí digo para conocimiento de mi amigo Antonio Mata, que en Huelva, a principios de este siglo se «tocaba» para acompañar al cante a ritmo abandolao. Pero no solamente se «tocaba» así en Huelva, sino en toda Andalucía.\n\nMás tarde apareció en dicha provincia su «toque» característico, como lo tiene, por ejemplo, el fenomenal Piñana hijo, para los cantes de Levante.\n\nTambién es cierto, y esta pudiera ser la teoría del señor Mata, mal explicada, que los cantes de Rebollo Piosa y algunos de Antonio Rengel, eran de origen malagueños, quizá porque ambos cantaores bebieron en la abundante fuente flamenca de Dolores Parrales, a través de su discípulo Antonio Silva.\n\nDesde hace ya veinte años, saben los buenos aficionados de Huelva que en su tierra existe, hoy un poco en desuso, un fandango abandolao de Vélez, que les llevó la polifacética cantaorqamoguereña. Mis amigos onubenses creyeron que se trataba de un cante autóctono y tuvo que ser Yerga Lancharro, con el asentimiento de «El Muela», quien les demostrase que no les correspondía.\n\nSe trata del célebre fandango que grabara Manuel Torre, Cayetano de Cabray poco más.\n\nYo fui a un nió y la cogí blanca paloma te traigo, yo fui a un nió y la cogí; queó su mare llorando como yo lloro por ti, la sorté y salió volando (sic)\n\nY ya para terminar diré a don José Sánchez, a quien seguramente conozco, que está dentro de lo posible que, como yo me he expresado, haya querido hacerlo el bueno del señor Mata, pero que, posiblemente, no ha acertado a transmitirlo con acierto, siendo éste el motivo por el cual, sin pretenderlo, porque este señor es incapaz, ha herido la sensibilidad de un buen aficionado defensor de los valores artísticos-flamencos de su tierra.\n\nLos cantes de Huelva, ninguna relacionación de «consanguinidad» tienen con los del árbol malacitano. Estos son nominados fandangos, aquéllos fandanguillos, pero ambos se tutean en los escenarios, porque los dos son parejos en grandeza, flamenquismo y «jondura».",
    "title": "Los fandanguillos de Huelva",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 574,
    "article_char_count_full": 3535,
    "article_char_count_review": 3535,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-05-20-left-hablan-las-pe-as-concurso-de-can",
    "article_text_for_review": "Con el patrocinio del Excmo. Ayuntamiento y la organización de la Peña Flamenca «Niño de Vélez» de Vélez-Málaga, ha sido convocado el IV Concurso de Cante Jondo.\n\nLas inscripciones podrán hacerse hasta el día 28 de junio en el domicilio de la Peña, calle Tejeda, Edificio Granada, bajo, o llamando al teléfono 501469.\n\nLa final se celebrará el día 16 de agosto de 1986.\n\nSe han establecido tres grupos de cantes y uno especial a la Malagueña del «Niño de Vélez».\n\nEn esta edición se concederán cinco premios, así como el especial a la citada Malagueña del «Niño de Vélez», siendo el primer premio de 100.000 pesetas, escudo de oro, así como contrato para actuar en el XI Festival de Cante «Juan Breva». 2.º Aniversario de la Tertulia Flamenca de Badalona\n\nLa Tertulia se preocupa de estudiar los cantes, to- no por tono, con todo el rigor y seriedad que nues- tro arte merece.\n\nComo cada año desde su fundación, la Tertulia organizó su acto cumbre, si el pasado año se le tributó un homenaje a Manuel Avila, este año hemos creído hacerlo diferente, al traer al cantaor, Manolo Simón... El cartel estaba formado por los cantaores de la Tertulia; que son los siguientes:\n\nDomingo Romero, Miguel Reyes, Rubito de Pastora, Agustín El Cacerreño, Antonio Heredias, Bella María al baile y Lucerito a la guitarra. Como figura central Manolo Simón. La noche empezó con la actuación de los cantaores locales, que dieron cumplida muestra de su bien hacer y entender el cante, la elevada calidad de los mencionados cantaores, crearon el clima necesario para que el cantaor jerezano saliera con enormes ganas de quedar bien y por supuesto, lo consiguió.\n\nLa Tertulia cree conveniente, hacer un pequeño comentario sobre este joven cantaor de la Escuela Jerezana, un cantaor, con sonidos del Torre, de los Agujetas y Chocolate, la más pura y rancia escuela jerezana.\n\nManolo Simón, vino, cantó, se entregó y convenció.\n\nNosotros escuchamos a Manolo Simón, la primera vez por su disco, la casa discográfica: Pasarela, está haciendo una labor muy importante con los cantaores andaluces, las grabaciones son de gran calidad, pero pensamos que le hace falta un poco de promoción. Aquí en Cataluña, es imposible de encontrar un disco de Manolo Simón.\n\nSalvador Castro, Badalona.\n\nNueva junta directiva en la Peña Flamenca de Elche\n\nEn asamblea general celebrada por la Peña Flamenca de Elche (Alicante), resultó elegida nueva Junta Directiva, quedando la misma compuesta de la forma siguiente: Presidente: Jesús Bono Parra. Vicepresidente: Emilio Bustos Cámara. Secretario: Manuel Quesada Moya. Tesorero: José Esclapez Miralles. Relaciones Públicas: Elías López Martínez. Relaciones Artísticas: Jesús Chaves Rodríguez. Vocales: Juan Benítez Romero, Francisco García Manrique, Antonio Iniesta Noguera, Antonio Rus Rueda, Juan Fernández Gil y Antonio Cortés Horca.\n\nDeseamos toda clase de aciertos a los compañeros de Elche.\n\nXII Concurso de Cante Flamenco para Aficionados\n\nEsta edición está dota- da de seis premios, el pri- mero de cien mil pesetas.\n\nPara más información, los interesados deberán de dirigirse a la Peña Flamenca Chiclanera, calle Carmen Picazo, 20, Chiclana (Cándiz).\n\nLa fase selectiva tendrá lugar del 28 de junio al 19 de julio, siendo la final el día 2 de agosto.\n\nCon el patrocinio del Excmo. Ayuntamiento de Chiclana (Cádiz) y varias firmas comerciales, ha sido convocado el XII Concurso de Cante Flamenco para Aficionados, para el que se han establecido cuatro grupos de cantes.\n\nLa Peña Cultural Flamenca Alzacaba, bajo el patrocinio del Excmo. Ayuntamiento, Ministerio de Cultura y Excma. Diputación Provincial, convoca la XV Volaera Flamenca (Concurso de Cante Jondo), cuya final se celebrará el día 28 de agosto.\n\nLas inscripciones habrán de hacerse antes del 30 de junio en el domicilio de la Peña, calle Cerri-llo de los Frailes, 1.\n\nSe han establecido tres grupos de cantes, debiendo los concursantes, obligatoriamente, hacer un cante de cada grupo.\n\nSe concederán cinco premios, siendo el primero de 125.000 pesetas y Volaera de Plata.\n\nLos interesados pueden dirigirse a la citada Peña o llamando a los teléfonos 320438 y 320251.\n\nPor reciente acuerdo del consejo rector de la Cátedra de Flamencología y Estudios Folklóricos Andaluces, adscrita a la Universidad de Cádiz, han sido designados Presidente de Honor de la misma y Director Honorario, respectivamente, el escritor y flamencólogo hispano-argentino Anselmo González Climet, y el cantaor Antonio Fernández Díaz «Fosforito», en atención a los méritos que en los mismos concurren por su brillante actividad, en los campos intelectuales y artísticos del flamenco.\n\nTanto el señor González Climent, como «Fosforito», ya pertenecían a la Cátedra de Flamencología, desde hace tiempo, como académicos numerarios.\n\nAmbos vienen a sustituir, por fallecimiento, al escritor Tomás García Figueras y al cantaor Antonio Mairena, que ocuparon durante muchos años los cargos de Presidente y Director Honorario de la citada institución jerezana.\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N",
    "title": "HABLAN LAS PEÑAS Concurso de Cante Jondo «Niño de Vélez»",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 804,
    "article_char_count_full": 5066,
    "article_char_count_review": 5066,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
