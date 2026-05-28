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
    "article_id": "1988-09-18-right-del-alma-de-andaluc-a",
    "article_text_for_review": "Del alma de Andalucía\n\nA la prestigiosa Revista CANDIL, que mantiene viva el ascua del Flamenco.\n\nEn recuerdo del «XI Potage Gitano» de Utrera.\n\nCOMO VENUS flamenca y soberana —luna y ensueño, fragua y azucena—, escultura de amor, carne morena...¡Pastora... clavelina de Triana...!\n\nPastora Imperio\n\nDaniel Pineda Novo\n\nFue La nieta de Carmen... española, luciendo la ritual bata de cola en sus bailes de embrujo y de misterio...\n\nY en sus brazos —¡octava maravilla!—, vibró el alma profunda de Sevilla por su Arte inmortal: Pastora Imperio.\n\nFuego, duende y pasión... ¡Musa gitana...!, con grandes ojos verdes de agarena: Lo mismo le cantó a La pena pena..., que a la eterna alegría sevillana.",
    "title": "Del alma de Andalucía",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 112,
    "article_char_count_full": 694,
    "article_char_count_review": 694,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-09-19-left-tiempo-igual-a-evoluci-n",
    "article_text_for_review": "Aunque la historia del cante la intuyamos hipotéticamente milenaria y por el contrario a penas centenaria la conocida, más por tradición oral que por escrita, tal vez de mucho, si las exprimimos, nos puedan servir las pocas letras que aquellas, aún menos, personalidades dispensaron a este fenómeno músico-espiritual allá por los finales del pasado siglo.\n\nPoco o algo leemos los que también cantamos. Me refiero al libro, al tratado, a la historia, a lo que nos pueda ayudar a dilucidar por sí solos y no, por cierto, porque es distinto, al periódico de turno donde la crónica, la reseña, la entrevista cuenta cómo canta, cómo es o cómo habla el cantaor en cuestión. Eso sí lo leerá el cantaor, pués según los más preocupados por su imagen prefesional, cualquier palabra que pueda prestarse a la menor tergiversación deteriorará su prestigio ¿...? Bien, pero bastante más arriba espera reposando el otro prestigio, el del conocimiento de la causa total por la que transcurre el misterio y la evidencia del cante en su interminable diversidad de maítices evolutivos.\n\nY de evolución intento tratar a mi manera y riesgo, sospechando que no está asistido este capítulo con la atención y asiduidad que me-rece.\n\nPienso, o me hace pensar la abundante reiteración de citas al respecto, todo lo que don Antonio Machado y Álvarez, «Demófilo», ha servido a la historia del cante y a los que leemos lo que del cante se ha escrito y se escribe. Releyendo su «Colección de Cantes Flamencos», surge la inevitable diferencia que el tiempo va estableciendo entre un ayer dolorosamente abrupto y un hoy más cercano a la pulidez artística y clara dicción.\n\nEn la borrosa historia del cante las razones no por tales dejan de contradecirse: ¿Gana o pierde el cante cuando, según Demófilo, Silverio lo saca de las catacumbas donde un sector determinado lógicamente lo repetía girando sobre sí mismo? «Demófilo» pronostica entonces que «Los cafés matarán por completo el cante gitano en no le- Luis Caballero\n\njano plazo, no obstante los gigantescos esfuerzos hechos por el cantaor de Sevilla —Silverio— para sacarlo de la oscura esfera donde vivía y de donde no debió salir fuera si aspiraba a conservarse puro y genuino». Es la muy generalizada confusión existente entre el arte popular y el folklórico a la hora de sopesar la pureza. El folklor conserva, momifica la expresión genuinamente típica, llana y sencilla que los pueblos repiten conmemorando inalterablemente sus acontecimientos tradicionales. Parte del cante procedente de ciertos aires folklóricos lo sigue siendo en la voz del pueblo sin cualidades artísticas, pero esa misma riqueza colectiva mediante la individualización del auténtico cantaor-artista tomará forma de arte por obra y gracia de la calidad e inspiración evolutiva.\n\nSi los gitanos andaluces amoldaron recreativamente el cante andaluz a su forma de sentir, inconscientemente lo logran formulando un indispensable, normal y natural proceso evolutivo, al igual que, por el contrario, «la idea dominante de Silverio de abrir al cante gitano nuevos horizontes para ennoblecerlos», no es otra que la misma, aunque intencionada desde una perspectiva distinta.\n\nA «Demófilo», con el oído «hecho» a los cantes de los gitanos de la Cava trianera, debieron asustarle los propósitos innovadores de íos mercantilistas al uso. Exactamente lo mismo que ocurre hoy. Porque no es igual innovar que evolucionar. Innovación equivale a novedad, a invento, a romper o alterar lo construido para dar a conocer algo nuevo, lo que en el cante origina el desequilibrio básico de los órdenes y moldes establecidos por la larga y ancha sedimentación evolutiva del tiempo y sus circunstancias. La evolución desarrolla, mueve, transforma, pero sucesivamente, progresivamente, serenamente. Ni el anquilosamiento arqueológico con Tío Luis el de la Juliana a la cabeza diciendo Mari por Madrid, afaitigaito por fatigao y cuantos muertos tenga por deblica deblá ni una caña tan nueva que no la conozcan ni en Cuba.\n\nEl cante es discutible porque está vivo, y como vivo se mueve, anda, tropieza y se endereza para seguir viviendo, desarrollándose, puliéndose, alambicándose, evolucionándose en el seno y el cultivo de la genialidad recreativa, restauradora, emancipadora y hasta loca del artista que lo conoce y siente visceralmente. Dos ejemplos prácticos de evolución en forma de pregunta: ¿Canta Paco Taronjo el fandango de Huelva según es o según él? ¿Ha cantado Antonio Mairena todo lo adjudicado a esa relación de nombres clásicos como esos nombres o sólo, exclusiva y personalmente como Antonio Mairena desde el tiempo que le tocó vivir?\n\nLo del cante de mengano y zetano cantado por zetanito y menganito siempre será más de estos dos últimos y su tiempo que de los dos primeros y el suyo.",
    "title": "Tiempo igual a evolución",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "19-19",
    "page_number": 19,
    "word_count": 770,
    "article_char_count_full": 4775,
    "article_char_count_review": 4775,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-09-19-right-sobre-la-nana",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCorrespondo a un musicólogo de Tomares para ofrecerle, a través de esta revista, lo poquísimo que sé de la NANA o CANCIÓN DE CUNA.\n\nEs cierto que en España se sabe muy poco del origen de ese «cantar de madres».\n\nHe leído y he investigado sobre dicho cantar arrullador, de ámbito mundial, y, la verdad, he sacado muy poco fruto de la investigación. Pero a pesar de ello puedo asegurarle que la NANA no forma parte de la nómina general de cantes. Sé que existen estudiosos que opinan lo contrario.\n\nEse cantar y algunos otros fueron sometidos por algunos cantaores a las disciplinas de ciertos estilos flamencos, allá por los años treinta. Primero fueron los cantes asturianos (asturianas y montañesas) y los grabaron, entre otros, «El Niño de Medina», «El Niño de la Isla» y el «Niño de la Rosafina\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"profesor\"]\n\ninta. Primero fueron los cantes asturianos (asturianas y montañesas) y los grabaron, entre otros, «El Niño de Medina», «El Niño de la Isla» y el «Niño de la Rosafina de Casares». Después fueron aflamencadas algunas letras, como «Ay, Maricruz», «Con sombrero negro y chaqueta corta», «Ay, mi Rocío», «María de la O», etc., siendo Manuel Vallejo, Canalejas y Angelillo tres intérpretes punteros en esta nueva modalidad de cantes por bulerías. Hubo un profesor que dijo: «La Nana es de formas y metros variados, que se aviene con el balanceo de la cuna y el niño oye a su madre poesías delicadas y maravillosas. Es de ternura emocional inmensa. Todas las madres y en todas las latitudes han cantado la Nana. Así, la Nana de Grethmaninoff; Canción de Cuna para dormir a un negrito, de Montsalvage; Una hebrea, de Aubert (de Ben Darracha Al-Castalli. «La Azucena», 958 - 1.030). He aquí algunas letras de Nanas. No me asuste a mi niño que ya no viene la mora; lo tiene su madre en brazos, y por eso ya no llora. Duérmete niño mío que viene el coco; y se lleva a los niños, que duermen poco. Mese, mese, mese tetita no mereses; yo me mesí, tetita meresí. A los niños que duermen Dios los bendice; y a las madres que velan, Dios las asiste. También García Lorca nos explica más ampliamente que nadie, que la Nana se canta en toda España, y cuyas letras, de temas diversos, se salen, a veces, del principal cometido para el que fue destinada en principio: dormir al niño. Dice: «En el pueblo de Navia (Asturias), se canta la Nana por una pobre mujer, cuyo niño es para ella una carga, una cruz pesada, con la cual muchas veces no puede». Este nešín que tečno nel collo a d'un amor que se tyama Vi- [torio Dios que manden, treveme [llongo por non andar con Vitorio nel [collo. Y prosigue: «No olvidemos que el objeto fundamental de la Nana es dormir al niño que no tiene sueño. Son canciones para el día y a la hora en que el niño tiene ganas de jugar. En Tamames (Salamanca) se canta: Duérmete niño que tengo que hacer lavarte la ropa ponerme a coser». Y en cuanto a Andalucía dice: «Que la canción de cuna es más racional, si no fuera por las melodías. Pero las melodías son dramáticas, siempre de un dramatismo incomprensible para el oficio que ejercen»: Granada: A la na\n\n[ENDING CONTEXT]\n\ncantaría ese fandango, no solamente en la región de Yebala, sino en todo el territorio marroquí. Y no es así. Yo tuve ocasión de recorrer el Protectorado Español, y en ninguna otra región llegué a oír ese tipo de música (cante).\n\nLa raza que puebla Yebala, en su mayoría, es distinta de la del Rif o de Gomara, por ejemplo, y esto nos hace suponer que por estar esa región tan próxima a Andalucía, los moros españoles se establecieran en ella.\n\nTermino diciendo lo que siempre he creído: que el cante flamenco fue creado por los hijos de Andalucía, para nuestro deleite y envidia de otros pueblos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Sobre la Nana",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1633,
    "article_char_count_full": 9180,
    "article_char_count_review": 3891,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "profesor"
      }
    ]
  },
  {
    "article_id": "1988-09-21-left-voces-del-futuro-juan-de-la-male",
    "article_text_for_review": "«M i nombre artístico viene de haberme criado en el barrio más flamenco de Jaén, que es el barrio de la Magdalena. Ya cuando trabajaba en el Museo Tablao, del conde Colombí, usaba este nombre artístico».\n\nJuan de la Malena pertenece a esa generación de cantaores que, sin concesiones de ningún tipo y al amparo de especificar colectivos, conforman, hoy día, una avanzadilla del jondo más puro y respetuoso de la tradición.\n\n«Mi afición flamenca no es de ahora, en mi casa han cantao toos, tengo tradición cantaora porque toda mi gente ha cantao, pero lo hacían en el único sitio que antes se podía hacer, en las ventas; ahí es donde se buscaban la vida».\n\n«Cantaores me han llegado muchos, pero los que más, Manuel Torre, Tomás Pavón, Juanito Mojama, Juan Breva y varios más».\n\n«En cuanto a los que he visto en persona, Terremoto y Antonio Mairena. Antonio Mairena ha sido el más completo de todos; y Terremoto fue el duende».\n\nLas primeras andaduras de Juan se producen en tablaos, cantando «atrás», en un imprescindible aprendizaje del compás.\n\n«Bueno, comencé cuando tenía dieciséis años, aproximadamente, que me marché a la costa y estuve trabajando en tablaos como: El Jaleo, de Torremolinos, en Fuengirola, en toda la costa. Después me marché a Madrid y estuve trabajando en el Patio de Reyes, que creo que ha desaparecido, y en varios sitios más. Después me marché a Barcelona donde también estuve tiempo haciendo cositas. Luego me vine pa Jaén, haciendo festivales y peñas que es donde se asimila, donde se aprende, porque en los festivales escuchas a mucha gente».\n\nJuan de la Malena es un excelente pintor, cuya cotización ha ido subiendo en los dos últimos años. Ha expuesto en numerosas salas y esta afición la conecta con el Flamenco y con los toros.\n\n«Mis temas están unidos al mundo del Flamenco y de los toros, porque soy un buen aficionao a los toros. En estas dos artes veo una gran belleza plástica. Yo pin-to todos los días porque de eso vi-\n\nvo, porque llegar en el Flamenco cuesta mucho. Lo que procuro en mi obra es que rezume andalucismo, y bueno, creo que en cierto modo lo consigo porque parece que tiene aceptación».\n\n—Juan, ¿la soleá o la siguiriya?\n\n—Juan, la solea o la siguiente? —Depende del momento. La solea es tan rica en matices, tan hermosa, que tiene mucho campo; la siguiente es más directa por su seriedad. Yo, de quedarme, me quedo con la solea.\n\n—¿Tú crees en los concursos? tana, se casó de segundas con «El Niño Madrid» que era payo, creo que cantaba muy bien, yo no alcanzo a recordar su cante, porque aunque puedo decir que fue él quien me crió, cuando murió, yo era muy joven para recordar su cante.\n\nAlguna vez que otra, «Pepe Polluelas» me dice: Mira, así canta-ba Lorenzo, y me hace sus cantes por soleá. Le decían «El Niño Madrid» porque era de Madrid, y que creo que cantaba mu flamenco y mu gitano, a pesar de ser payo y de Madrid.\n\n—No, aunque yo me he presentado a pocos de ellos. Y no creo porque he visto mucho partidismo en los jurados, que muchas veces no han sido todo lo competentes que deberían ser. Esto lo he visto yo aquí mismo, en la provincia de Jaén, que ha habido quien ha sido miembro de un jurado flamenco sin tener conocimientos suficientes para ello. —¿Tú crees que las Peñas ayudan lo suficiente a los aficionados que empiezan?\n\n—Veréis. Mi abuela, que era gi-\n\n—«El Niño Madrid», ¿qué te recuerda? —En términos generales, sí; porque las peñas es en el único sitio donde se puede ir a cantar y charlar de Flamenco; es el único sitio que encuentras donde tienes calor flamenco, donde hay una gran familia que entiende lo que tú dices, porque los flamencos somos una gran familia. Gracias a las peñas el Flamenco no está más contaminao de lo que ya está.\n\n—¿Existe cante payo y cante gi- tano?\n\n—Eso no lo sé. Lo que sí digo es que el gitano lo dice de otra manera. Yo he ido a sitios a cantar y he dicho que era gitano y no me han creído hasta que no he cantao. La voz del gitano suena de otra forma.\n\n—Tú perteneces a una generación de jóvenes cantaores que han surgido en esta provincia, prácticamente de quince años para acá, algunos con bastante futuro. ¿A qué crees tú que se debe esto?\n\n—Efectivamente, esto me está sorprendiendo cada día más, porque han surgido en ese tiempo una serie de gente que se lo ha tomado muy en serio y muy bien, caso de Rosario López, Carlos Cruz, luego tenemos a «Joselete», Manolo González, Pepe Soto, «Niño Jorge», «El Maeras» y varios más, en fin, que esto es muy importante para la historia flamenca de Jaén.\n\nMi agradecimiento y un saludo cordial,\n\nTengo enorme interés en conseguir una buena grabación de su famosísima y personalísima Soleá, porque estoy recopilando los cantes de todos los extremenos que grabaron antes de la guerra civil y tengo tres placas a cual más estropeada. No cabe duda de que fue un disco muy escuchado. ¿Podríais facilitarme la grabación del disco que por una cara tiene la Soleá y por la otra unos fandanguillos de Huelva?. Os agradeceré os pongáis en contacto conmigo y así os podré facilitar la cassette virgen o bien dinero para la compra de la misma y gastos de envío.\n\nManuel Yerga Lancharro\n\nEstimados amigos en el flamenco.\n\nC/. Llerena, 58\n\nOs comunico que mi paisano y gran cantaor, «Manzanito de Castuera», falleció no hace mucho tiempo en la ciudad de Don Benito, poco menos que recogido de caridad.\n\n06240-Fuente de Cantos (Badajoz)\n\nTelf. 50 02 67\n\nP. D./ Necesito las grabaciones para una acción altruista.",
    "title": "Voces del futuro: Juan de la Malena",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 973,
    "article_char_count_full": 5464,
    "article_char_count_review": 5464,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-09-22-left-podio-y-picota",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA qui en todo lo alto el Ayuntamiento de Utrera que una vez más ha acreditado su sensibilidad para con el pueblo —el pueblo llano— al que sirve. En esta ocasión rotulando, en emotivo acto público, con los nombres de Fernanda y Bernarda la avenida que parte por gala en dos el centro de la ciudad. Exquisito detalle y acto de estricta justicia que ahí queda para ejemplo de tantas entidades como en Andalucía olvidan u ocultan la importancia fundamental del flamenco. Avenida de Fernanda y Bernarda: así de sencillo, dos nombres nada más y nada menos.\n\n* * *\n\nHoy se puebla este Podio de buenos ecos jerezanos. Un espectáculo serio, digno, pleno de arte y de enjundia flamenca lo viene a ocupar por derecho propio. Cuando el aficionado asiste atónito a tantas aberraciones. «Esa forma de vivir» le\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"Maestra\"]\n\nmás y nada menos. * * * Hoy se puebla este Podio de buenos ecos jerezanos. Un espectáculo serio, digno, pleno de arte y de enjundia flamenca lo viene a ocupar por derecho propio. Cuando el aficionado asiste atónito a tantas aberraciones. «Esa forma de vivir» le llega como una brisa fresca y purificadora. Rodríguez Pantoja y Manuel Morao, moviendo diestramente un grupo de gitanos, han salido por la Puerta del Príncipe en su presentación ante la Maestranza flamenca. Y han demostrado que se puede hacer teatro flamenco sin concesiones a la galería, sin flamenquismo al uso, sin sombras de tricornios y camborios: nada más que con buen cante, baile ajeno al academicismo, y toque y palmas a compás. Que no es poco. * * * Y un ex presidente de Peña nuestro tercer homenajeado hoy, caso infrecuente en el tortuoso mundo del flamenco. Don Manuel Centeno Fernández, Manolo Centeno para los viejos aficionados. Fundador y primer presidente de la asolerada Peña Cultural Flamenca Torres Macarena. En su boletín «Arco Flamenco» ha sido publicado un DECÁLOGO PARA EL BUEN AFICIONADO que merece la categoría de norma de obligado cumplimiento. No bastaría\n\n[ENDING CONTEXT]\n\nno conocemos aún el resultado del Giraldillo del Baile. Dado que hemos optado por la ausencia, no estamos —no estaremos nunca— en condiciones de juzgarlo. Ni siquiera guiándonos por la sospecha, quién sabe si poco o mucho razonable, de que el inevitable tufillo academicista que desprende el baile flamenco ensalzado previamente en la Bienal (a salvo pocas y elogiables excepciones del genio intuitivo de los duendes morenos) presenta de entrada una perspectiva escasamente prometedora. El tiempo dirá. Vale.\n\nAPERITIVOS SELECTOS Especialidad en PLANCHA\n\nTOMAS\n\nMESONES, 18 TELF. 26 35 46 JAEN\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Podio y Picota",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 963,
    "article_char_count_full": 6115,
    "article_char_count_review": 2766,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "Maestra"
      }
    ]
  }
]
```
