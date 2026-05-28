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
    "article_id": "1981-02-12-right-conversaciones-entre-cante-y-can",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPero cuéntenos algo de su vida profesional. ¿Cómo empezó a desarrollarse?\n\nIba la algunos pueblos, «pa» cantar como otro más. Hasta que llegó el Conejo a Sevilla. Era «mataor» de toros, y por la feria hicieron una fiesta, y entró Pepe Villalba en la juerga, y como yo les caía bien a los mayores, porque les respetaba, Villalba me llevó. Era de la província de Huelva, de Villalba, y me anunció: «Vais a escuchar a un chiquillo, está por aquí ahora cantando». «Bueno, pues que venga»... Y me llamaron. Gusté. Aquel mismo año, sería por el cinco «pa» el seis, llegó a Sevilla un señor de Córdoba que tenía negocios de juego y se los había suspendido el Gobernador, y le dijo Conejo: «No te apures, hombre —el de Córdoba tenía la Cervecería de la calle Gran Capitán, esquina a Gon-\n\ndomar— que tu\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\nr, y le dijo Conejo: «No te apures, hombre —el de Córdoba tenía la Cervecería de la calle Gran Capitán, esquina a Gon- domar— que tu negocio es bueno y si está «cerrao» el juego, ¿por qué no pones algo de cante para los forasteros? Busca a un chiquillo que ha estado con nosotros cantando, o pregunta por Villalba, que él sabe donde encontrarlo». Fue al Pasaje del Duque y Villalba, que me llevaba a mí veinticinco o treinta años, le contestó: «Sí, hombre, sí, yo le puedo encontrar. Venga Vd., vamos a llamarle». Entonces se puso al habla conmigo. Yo ya estaba comprometido «pa» venir a Madrid, al «Café del Gato», que lo tenía una señora casada con Guerrerito el torero, la llamaban la Igorrota, pero yo no tenía contrato ni nada, simple- mente estaba apalabrado, y médicen: «¿Te quieres venir a Córdoba a cantar?» Yo, que estaba deseando que me llevaran a cantar a donde fuera, fui a Córdoba por un mes. Y estuve allí seis; por una razón; porque entonces a Guerrita le respetaban mucho los toreros y Guerrita se puso en la primera fila que pusieron de sillas para oír cantar, y me dijo el amo del café, que era muy amigo de él: «Bríndale una copla, niño». Guerrita era el amo de Córdoba, y se me ocurrió decirle: «Don Rafael, este cante va por usted». Y le cayó bien. Con decirle que me quedé allí y estuve seis meses... Venía\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_04 | trigger=\"segunda\"]\n\nno se hace «na», pues se les rebaja la mitad del sueldo. «Entonces yo me voy a Andalucía, y en las fiestas de los pueblos voy a ganar, en dos días, más que aquí en una semana. Si usted quiere, cuando llegue el invierno nos ponemos de acuerdo y me vengo otra vez». Y en eso quedamos. Me voy a Sevilla y El Ceniza, picaor que iba con Rafael el Gallo, me llevó una carta de Miguel Borrull diciéndome que se había abierto en Madrid el «Café Fornos» por segunda vez —lo abrieron Tomás Mazzantini, el hermano del célebre Mazzantini, y Bernardo Hierro, otro banderillero de la cuadrilla—. Me Decía Borrull en la carta: «Si usted quiere, vén-gase; en «Fornos» no entran más artistas que usted, Escacena y Fernando el Herrero». Los tocaores serían él y Luis el Jorobao, que era un hombre «mu» gracioso. «Véngase usted», seguía diciéndome en la carta, «porque aquí va a vivir muy bien y no tiene necesidad de actuar en cafés cantantes ni nada de eso, porque entre «los Gabrieles», que e s t á recién abierto, «Fornos» y «Los Burgaleses», tendremos trabajo de sobra». Y me calenté y me dije: me voy «pa» Madrd. Todavía no había llegado el invierno. Yo veía que ganaba el dinero con más facilidad que en un café cantante con cuatro cuadros, donde tenía que cantarle a las mujeres\n\n[ENDING CONTEXT]\n\nuna ovación, y dije yo, ¿será posible esto? ¡A la guitarra, señores! Me quedé emocionado. Ha sido la única vez que he «llorao» de verdad, de verdad. Y canté, y, por mis nietos, que abrieron veinte veces las cortinas, ¡a las once de la mañana!; y el telón de acero, que no lo abren «na» más que en contadas ocasiones, me lo abrieron tres veces «pa» arriba. Subían las gentes por las candeleras, las familias enteras, llorando por la cara abajo. Yo no he visto emoción más grande «pa» mí, estaba «asustao», ¿esto que es?, me van a quitar «toa» la ropa, y me voy a tener que ir en cueros «pa» España!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Conversaciones entre cante y cante",
    "periodical": "candil",
    "issue_id": "1981-02",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "12-14",
    "page_number": 12,
    "word_count": 2112,
    "article_char_count_full": 11400,
    "article_char_count_review": 4286,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "hombre"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segunda"
      }
    ]
  },
  {
    "article_id": "1981-02-14-right-un-encuentro-sevillano",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor José Luis Buendía López\n\n«Pues verá usted, señor, es Pepe el de la Matrona, el de Manolita la Matrona, la que nos sacó de la tripa de nuestra madre a medio barrio de Triana».\n\nLos farollillos de la ribera hacen guiños a la noche. Sentados junto al bordillo del río contemplamos la figura rechoncha y venerable que tras un «buenas noches», dicho con voz de goma, se aleja con paso juvenil del brazo de un desconocido y en compañía de Daniel «El Cura», mi guía ocasional sevillano por los difíciles senderos, buscones y apicarados, de un poco de cante, mucho vino, y «lo que caiga», como dice Daniel, ahuecando la voz rufianesca con ribetes maliciosos.\n\nHasta esa hora no había caído nada. Bueno, ha caído la noche y la bruma en el barrio más bonito de Andalucía. Francisco de Paula y yo nos\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"reconocemos\"]\n\nde cante, mucho vino, y «lo que caiga», como dice Daniel, ahuecando la voz rufianesca con ribetes maliciosos. Hasta esa hora no había caído nada. Bueno, ha caído la noche y la bruma en el barrio más bonito de Andalucía. Francisco de Paula y yo nos quedamos solos, de serenos invisibles del silencio, impresionados por el breve e imprevisto capítulo que ese viejo, amigo de Paco, acaba de abrir ante nosotros: «Pepe el de la Matrona». Ahí es nada. Y reconocemos que a los dos nos resultaba vagamente familiar ese rostro apergaminado, de indio viejo, y esos dedos rolizos que se ligaban voluptuosamente a un habano, tan habitual en él que parecía un sexto dedo ortopédico transplantado de las Antillas a la mano del cantaor. Y en seguida la frustración: ¿por qué todo ha sido tan rápido?, ¿por qué Daniel, siempre acaparador se lo lleva, lo secuestra de nuestra admiración, de nuestra disparada curiosidad?... Tras un rato de charla y de malestar invisibl\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"experiencia\"]\n\nacaparando al cantaor con el fondo de dos copas de cazalla y ese mosaico de azulejos amarillos que a mí tan poca gracia me ha hecho desde siempre. La decisión está tomada. Ahora es inevitable el sentarse y emprender una conversación a la que no hemos sido invitados. Pero el carácter abierto y magnánimo del buen Matrona, rompe de inmediato el hielo que nuestra osadía juvenil pudiera haber creado en el ambiente. Y surgen ochenta y pico de años de experiencia vital, de viajes fantasmas a través de continentes y de nuestra propia piel de toro. Viajes centrados en una única actividad: el cante. Con insobornable independencia, cantando solo para «según quién». Con la tranquilidad que proporciona la charla íntima, sin periodistas ni fotógrafos, a la caza de la última parida del genio, va surgiendo la conversación lenta y profundamente, como el cauce del mutilado río que atraviesa la ciudad a cien metros de nosotros. Y el bar de Salvador se transforma de repente en una academia de sabiduría, en un archivo de re- cuerdos, a veces en un manual de filosofía vital: —«He viajao por todas partes de\n\n[ENDING CONTEXT]\n\nllevaba razón. Ni se canta ni se torea a destajo. Nos despedimos con un apretón de manos y cuando, pasado el puente, nos detuvimos los tres amigos a darnos las buenas noches, junto al viejo postigo del Aceite, pensábamos con pena que nunca oíríamos cantar en vivo a uno de los intérpretes más verdaderos del flan.\n\nUn mes más tarde en Jaén, desde otro arco, el del Consuelo, escribía a mis amigos s-villanos una postal con un tex-to tan sencillo y entrañable qu-no lo he podido olvidar: «A las ocho de la tarde canta Pepe de la Matrona en el Colegio Far-macéutico. Luego iremos a la Peña Flamenca».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Un encuentro Sevillano",
    "periodical": "candil",
    "issue_id": "1981-02",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "14-15",
    "page_number": 14,
    "word_count": 1169,
    "article_char_count_full": 6639,
    "article_char_count_review": 3755,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "reconocemos"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "experiencia"
      }
    ]
  },
  {
    "article_id": "1981-02-15-right-pepe-el-de-la-matrona-nos-habla-",
    "article_text_for_review": "su cante\n\nPor Ricardo Molina (1)\n\nHemos aprovechado el paso por Córdoba del maestro Pepe Núñez («Pepe el de la Matrona») para saludarlo y charlar un buen rato sobre cante. La primera noticia que hemos tenido del gran cantaor sevillano fue la exigua que de él nos da el libro de Fernando de Triana «Arte y artistas flamencos». Allí aparece además en una fotografía de hace bastantes años que lo representa en su madurez. Pepe el de la Matrona es en la actualidad un hombre iovial de setenta y tantos años de edad, que mantiene en su carácter y en su conversación el fuego misterioso de una inextinguible juventud. Enamorado de Córdoba, ha permanecido aquí varios días con el sólo amable propósito de conocer la ciudad, recrearse en sus rincones típicos y admirar sus ronumentos. El huertecillo de la taberna de la «Sociedad de Plateros», en la calle María Auxiliadora, fue escenario de nuestra larga conversación con este apasionado amante de los puros cantes tradicionales.\n\nA Pepe el de la Matrona le interesa primordialmente I c s dos cantes flamencos por excelencia, que son las seguiri-yas y las soleares. Conoce y canta una variedad asombrosa de ellas. Entre todos los can-\n\ntes por seguirias concede una importancia capital al de «Frasco el Colorao», viejo maestro trianero del siglo XIX que según parece influyó poderosamente sobre aquel titán que se llamó Manuel Molina. Le rogasmos que nos apunte siquiera ese cante para que nos hagamos idea de su son y Pepe canta a media voz... Dan las dos, las tres de la madrugada... Y Pepe Núñez nos pone a todos en peligro pidiendo otros medios de «platino». A su edad bebe más que nosotros y no experimenta el menor cansancio. Hablamos de cante interminablemente. Nos explica que está componiendo un extenso libro sobre el cante que pronto verá la luz. En él recoge sus vastas y profundas experiencias. Porque Pepe ha recorrido triunfalmente el mundo y concretamente en Francia lo adoran.\n\nComo gran cantaor y maestro de los cantes gitanos, estima que la cuna de su arte es Triana, donde él nació a final del siglo pasado. Ha convivido con los maestros supremos que se llamaron don Antonio Chacón, Juan Breva, Manuel Torre, Niña de los Peines, Salvaorillo, etc., y testimonia una profunda admiración a Tomás Pavón.\n\nPor la vastedad de sus conocimientos, por su personalísimo arte de cantar en todos los estilos con pureza tradicional, por su dedicación plena al arte flamenco, a Pepe el de la Matrona lo estiman todos los buenos aficionados como una de las raras fuentes donde se puede uno ilustrar sobre los cantes auténticos sin mixtificaciones ni impurezas.",
    "title": "Viejas páginas.-Pepe el de la Matrona nos habla de su cante",
    "periodical": "candil",
    "issue_id": "1981-02",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 443,
    "article_char_count_full": 2609,
    "article_char_count_review": 2609,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-02-16-right-persistes-tu",
    "article_text_for_review": "El Pepe el de la Ilatrona\n\nLa desvencijada voz entre oración y alarido; la cadencia de este Sur, estertorizada siempre, el son que brama o se humedece de oceánicas ternuras, han enmudecido un solo instante, porque una espadaña, la más vieja y sonora, la más llena de pájaros viajeros destruida está, y sus ecos bastanteados con raíces de seculares cicatrices de históricos estremecimientos, se nos han muerto; Pero, un solo instante... porque la voz persiste entre tantas depauperaciones y desgarros. Persiste el mismo Sur, las mismas inflexiones del dolor compartido y devocionado de mi pueblo, en otros tremedales. Y si no persistieran, la memoria destilaría una larga mestruación de pretéritos horrores para que la voz persista, con la misma calor persista el cante, y tú persistas...\n\nRamón Porras",
    "title": "Persistes tú",
    "periodical": "candil",
    "issue_id": "1981-02",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "16-16",
    "page_number": 16,
    "word_count": 128,
    "article_char_count_full": 801,
    "article_char_count_review": 801,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-02-17-right-glosa-y-recuerdo-de-pepe-el-de-l",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Manuel Ríos Ruiz (1)\n\nSolamente le faltaban siete años para cumplir el siglo a Pepe el de la Matrona, cuando el pasado agosto le llegó la muerte, en este Madrid tan distinto de aquel otro al que arribara el año seis, procedente de su Sevilla natal, después de pasar una temporada en Córdoba, cantándole a Guerrita y hecho «más amigo que un rucho» de Julio Romero de Torres.\n\nLe conocí personalmente en septiembre de 1966, en casa de Carlos Gayango, en la calle Núñez de Arce. Me lo presentó Pepe Blas Vega, que luego sería el productor discográfico de su antología Tesoros del cante flamenco (Hispavox). Y allí estaban con él aquella noche Moscatel, un «tocaor» gitano y francés que buscaba promocionarse; don Francisco, maestro de escuela natural de Lucena, que cuenta en un periquete un chorro\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"memoria\"]\n\ncanta abaritonadamente por cabales, mi paisano Juanele Calle, con todo el cante de Jerez infuso; Bernardo el de los Lobitos, aún vivo en dicha fecha, con su azoriniana fisonomía, y Enriquito Morente, ya bien encaminado en el mundillo del flamenco. Peceucho la reunión como si la estuviera otra vez viviendo, tal vez porque eran mis primeros días en la capital, días de ansiedad y de búsqueda, y por eso todos los pormenores quedaronme fijos entre la memoria y el paladar. Fuimos del mostrador al sótano. Se habló y se discutió, se apuntaron cantes, mientras que el gitanito francés no dejaba de hacer dedoz y pulir falsetas. Pepe el de la Matrona me impresionó. José Núñez Meléndez tenía una apabullante personalidad y una mente lúcida, sabía del cante la «intemerata». Y me pareció algo así como la salud del flamenco puro. Hacía, entonces, ochenta años que había nacido en plena cava trianera, núcleo gitano de lo más legendario, aunque él «era más payo que un olivo», donde las fraguas y las herrerías, donde los faluchos por el Guadalquivir y hacia Bonanza traían y llevaban coplas de amor, de vida y de muerte, el sitio donde cantó La Andonda, la «soleaera» más remota que registra la histo- ria, y donde El Fillo se quedó hasta morir, legando toda la sustancia de su\n\n[EVIDENCE WINDOW 2 | retrieval_hint=CRIT_03 | trigger=\"granadino\"]\n\nn de comprobar su maestría unas noches después, en el mes de diciembre, casi en las pascuas, cuando Eduardo Rojas, el conde de Montarco, nos reunió a un grupo de amigos en el castizo Mesón de Santiago, con motivo de una cena en honor de Pauwels, el teórico de la «ciencia-ficción», que había hablado por la tarde de las musarañas en el Club Pueblo. Después de comer faisán y lechuga, o sea, carne y verde, como las liebres, tocó finamente el artista granadino Manolo Cano, bailó el orondo Edgar Neville unas chuflas con Carmen la mesonera, un servidor pronunció algún poema y, finalmente, cuando McNoyita sacaba aires aljiberos de su sonanta, allá a las tantas de la madrugada, Pepe el de la Matrona se elevó a los países del escalofrío. Le cogió en vena. Tanto fue así que hasta nos dijo la petenera cuadrada y los cambios «siguirieros» de María Borrico. La biblia en pasta. Me quedé «atraganta», tieso por e\n\n[ENDING CONTEXT]\n\nrevista literaria de su literatura cantada. Pero ahora hay que pensarlo no aquí sobre el asfalto, sino en el alto cielo, preguntándole a los alados duendes en qué estela recóndita canta su amigo Monterito el Tísico y baila La Chorrúa, porque quiere saludarlos, porque tiene una «soleáo» en la punta de la lengua, la terrible «soleáo» de Miguel Macaca, el que estuvo en la cárcel por mor de un crimen pasional:\n\nAntes lloraba por verte\n\ny ahora que solo me veo\n\nbeso la tierra mil veces.\n\nY una paloma señora le acompaña segu- ramente.\n\nTejidos nuevos para tiempos nuevos\n\nCorrea Weglison, 9\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Glosa y recuerdo de Pepe el de la Matrona",
    "periodical": "candil",
    "issue_id": "1981-02",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1441,
    "article_char_count_full": 8260,
    "article_char_count_review": 3882,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "memoria"
      },
      {
        "window": 2,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "granadino"
      }
    ]
  }
]
```
