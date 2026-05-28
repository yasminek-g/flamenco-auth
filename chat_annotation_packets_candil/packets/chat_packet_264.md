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
    "article_id": "1992-11-15-left-dos-soleares",
    "article_text_for_review": "Rafael Guillén, con ocasión de la abundante correspondencia que mantiene con González Climent, envía a éste toda una colección de poemas, dentro de los que hemos seleccionado aquellos más significativos en la personalidad del poeta granadino. El agua lleva tu sueño, dormida te lleva el agua, el agua que yo no bebo.\n\nEl agua lleva tu sueño.\n\nDormida sobre el silencio del cante que no te canto porque lo canto por dentro.\n\nEl agua lleva tu sueño.\n\nQue de noche me desvelo de tanto soñar de día lo que sueño cuando duermo.\n\n¡Ay, que el agua lleva tu sueño!\n\nTocando están con tus hierros los hierros de mis balcones, y el viento pasa por medio.\n\nEntre el dejarte y la duda tú me pusiste a elegir. Te dejé. Tuya es la culpa. Pero no dudé de ti.\n\nY es tu calle tan estrecha para que quepa tu cuerpo pero no quepa mi pena.\n\nRafael Guillén",
    "title": "Dos soleares",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 155,
    "article_char_count_full": 835,
    "article_char_count_review": 835,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-11-16-left-derroteros-pedro",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE n 1881, Demófilo hizo sonar la alarma. Estaba en peligro la pureza del cante gitano. El café cantante amenazaba aniquilarlo. Su enfrentamiento con el heterogéneo público andaluz lo exponía a una contaminación peligrosa de «aga-chonamiento». Y pronunció su ya clásico vaticinio: «Los cafés cantantes matarán por completo al cante gitano en no lejano plazo».\n\nHa transcurrido ya más de un siglo, y cabe preguntar: ¿Ha muerto el cante gitano? ¿Ha desaparecido, al menos, en su original pu-reza?\n\nMuy difícil es apreciar esto último, porque nadie sabe en qué consistía esta pureza. El gitano actual la busca y la cree encontrar precisamente en los maestros posdemofilianos (Molina, Frijones, el Marruco, el Loco Mateo, el Nitri, el Mellizo, Joaquín el de la Paula, La Serna, Torre, Pavón, etc.). De un\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"puro\"]\n\nparecido, al menos, en su original pu-reza? Muy difícil es apreciar esto último, porque nadie sabe en qué consistía esta pureza. El gitano actual la busca y la cree encontrar precisamente en los maestros posdemofilianos (Molina, Frijones, el Marruco, el Loco Mateo, el Nitri, el Mellizo, Joaquín el de la Paula, La Serna, Torre, Pavón, etc.). De un Planeta o de un Fillo es casi imposible cosechar ecos. Es decir, se nos está facturando como gitano puro lo que ya consideraba como contaminado y moribundo el gran folklorista sevillano. Y en cuanto a la primera pregunta, tenémoslo por muy «vivo» y, lejos de contagiado patológicamente, rejuvenecido por la transfusión de sangre generosa recibida de su nodriza Andalucía. Porque, conviene no olvidarlo: el cante gitano sustentaba su gitanidad en dos apoyaturas: origen y destinatario; y su pretendida pureza en dos polos: el párroco y la parroquia. Su templo era el hogar gitano, la taberna, la fragua, la reunión de amigos; y el oficiante un «verdadero rey» faraónico revestido de una infalibilidad oracular. Cambiar o sustituir cualquiera de estas apoyaturas era, se pensaba, resquebrajar la integridad del cante: es decir, su pureza. Silverio quiso «abrirle nuevos horizontes», elevar a la categoría de espectáculo\n\n[ENDING CONTEXT]\n\nde embriaguez, no queda otro camino que hacerlo regresar del tablao y del disco a la fragua y al coloquio familiar. Si lo que se pretende es retrotraer los esquemas supuestamente clásicos de los maestros gitanos profesionales, se tendría que echar mano del género «impuro» y ocioso, supuestamente victimado por el café cantante. Es decir: hacer las paces con el gitanicida.\n\nOjalá que unos y otros no se dejen cegar por las pasiones, y el cante, bien ganancial y comunal, al cabo, que hoy está en peligro de desmembrarse, resurja potente y engrandecido como hace un siglo emergió del Café Cantante.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Derroteros Pedro",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 1479,
    "article_char_count_full": 9385,
    "article_char_count_review": 2892,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "puro"
      }
    ]
  },
  {
    "article_id": "1992-11-17-left-la-intimidad-flamenca-de-ricardo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE ncontrar una literatura epistolar que haga vibrar a quien la lea no es tarea fácil. Hoy, cuando nadie escribe cartas, resulta toda una aventura. Por ello considero imprescindible analizar la correspondencia sostenida entre el poeta Ricardo Molina Tenor y el flamencólogo argentino (de origen español) Anselmo González Clement, entre el 1 de julio de 1955 y el 31 de diciembre de 1965, puesto que el cordobés moriría tres años más tarde, y su grave enfermedad le impidiría diversificar esfuerzos. Desgraciadamente conocemos sólo las epístolas enviadas por Ricardo Molina, cuidadosamente transcritas, y hasta anotadas, por Anselmo; no así las respuestas de éste, que hubieran contribuido a formar una diadema preciosa de juicios y opiniones flamencas y literarias, dada la sagacidad de su\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"declarara\"]\n\nido en su encuentro con motivo del concurso cordobés, momento en que la correspondencia se tiñe de una calidad mayor aún, que desembocaría en el respetuoso elogio hacia los padres de Anselmo, a los que Ricardo conoce en el verano de 1957, o el dolor con el que le da el pésame por el fallecimiento de su progenitor, o, poco después, le explica la enfermedad y muerte de su propia madre; incluso da detalles espeluznantes del edema pulmonar que se le declarara en octubre de 1965 y que, a la postre, acabaría con su vida. En resumen, una trasmisión de sentimientos cálidos, íntimos y cordiales que elevan aún más el talante humano de ambos escritores. En el aspecto literario son muchísimas las opiniones que Ricardo Molina manifiesta, tanto en lo concerniente a la literatura española como a la escrita en hispanoamérica, a la que reconoce como propia; resulta emocionante advertir cómo la lectura de libros de César Vallejo o Neruda, enviados por Anselmo, representan para el cordobés una «vuelta a casa», que dice anteponer a las lecturas de Eliot o Brecht, que entonces regentaban los gustos intelectuales de casi todos los escritores. No ceja Molina de emitir en sus cartas destellos de entusiasmo hacia los nuevos «descubrimientos» debidos a la generosidad bibliográfica de Anselmo: el «Facundo» de Sarmiento, los cuentos de Borges, etc., a la vez que muestra su disgusto con otras personalidades literarias como Benítez Carrasco, al que considera «un cero a la izquierda», o la que él considera insufrible pedantería de Victoria Ocampo. Interesantísimas son las reflexiones, comunicadas con urgencia a su interlocutor, a propósito de los trabajos sobre lírica tradicional de Menéndez Pidal, que tientan a Molina a considerar a las jarchas: «como remoto y posible ascendiente del cante», señalando la estructura similar de éstas con los machos pareados, soleares o siguiriyas. Pero van a ser los aspectos flamencos los más apasionantes de los expuestos en este tratado de amistad que son las cartas que comentamos. En estos diez años que dura la correspondencia, se observa el iry venir de filias y de fobias de Ricardo Molina en el tema flamenco, que yo recomiendo confrontar, con atención exhaustiva a las fechas (toda vez que carecemos de las respuestas de Anselmo) con los escritos de éste sobre flamenco, y, sobre todo, la valoración de artistas y circunstancias que concurrieron en la celebración de los concursos cordobeses. Con este cotejo, que, lógicamente, y por razones de espacio, no podemos apurar aquí, es fácil observar un daguerrotipo fiable de los demonios particulares que persiguen desde siempre a nuestro arte flamenco, en lo que concierne a los juicios que sobre él vierten sus estudiosos. Desde el principio, Ricardo muestra una gran proclividad hacia las manifestaciones jondas realizadas por los gitanos. Por ejemplo, está fuera de duda su continuada admiración por Fosforito, del que sólo critica su difícil trato personal (hasta el punto de calificarlo de «vaina» y definir su carácter como: «una mezcla de soberbia infantil y de pendonería o chusmeterí\n\n[ENDING CONTEXT]\n\nque requieren ser conocidos de inmediato. Creo que será un ejercicio de primer orden flamenco, pues debo decir que la sinceridad de inmediatez comunicativa son sus dos rasgos más característicos. No es la voz del poeta la que se dirige al amigo rioplatense, sino la sensatez del crítico avezado que aventura opiniones, afianza criterios o pone en solfa verdades admitidas rutinariamente pero nunca demostradas. Por eso, porque su escritura está desprovista de intenciones literarias y sí de honestos deseos develadores, pensamos que su lectura constituye un ineludible deber para con nuestro arte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La intimidad flamenca de Ricardo Molina",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "17-18",
    "page_number": 17,
    "word_count": 1416,
    "article_char_count_full": 8975,
    "article_char_count_review": 4704,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "declarara"
      }
    ]
  },
  {
    "article_id": "1992-11-18-right-gonz-lez-climent-y-la-d-cada-pro",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPuesto que ya hay perspectiva histórica suficiente, creo que los estudiosos del flamenco deberían analizar el inicio de una etapa de resurrección casi milagrosa, que nace cuando los más pesimistas se disponían a entonar las definitivas exequias del cante. No era para menos en vísperas de los años 50, desaparecidas las figuras míticas —desde Manuel Torre hasta don Antonio Chacón—, la Niña de los Peines encabezando el cartel de un espectáculo híbrido y Manuel Centeno fantasmal y silencioso entre las sombras equívocas de la Alameda de Hércules. En vísperas de los año 50 sólo quedaban algunos, con un fandango en los labios para echar un remiendo a la vida (Fregenal, Pepe Aznalcóllar, Maravilla, el Cojo de Huelva), en tanto a Pepe el Culata lo dejaban tirado cuando, en la reunión del vino y la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"duendes\"]\n\ntanto a Pepe el Culata lo dejaban tirado cuando, en la reunión del vino y la muchacha rota, quería templarse por soleares, y Tomás Pavón iba de un lado a otro con su tristeza a bordo, sin claudicar a la exigencia del señorito de turno. ¿Dónde aquel cante bravo y terrible, nacido de las entrañas de la tierra? Acaso, alguna noche, en el patio de la fonda que Antonio Rengel tenía en la calle San Eloy, de Sevilla, y entonces era la conjura de unos duendes secretos en el tercio valiente de la serrana. Acaso en la juerga de vino largo y malesquinado de Caracol o en la presencia esporádica de una chiquilla de Jerez a la que llamaban la Paquera... Recuerdo todo esto como una nebulosa en la que se desdibujan los perfiles. Algún cante bien dicho, en el encuentro con Juanito Varea; muchos atardeceres en casa de Pepe Torre —el hermano de Manuel— y, de vez en cuando, la rara ocasión en la que Pepe Marchena se acordaba de sus raíces y arremetía por derecho un buen estilo de taranta. No era mejor el panorama al hablar de libros dedicados al tema: los tímidos intentos de «Gitanos de Granada», de Ortiz de Villajos, «La copla y el cante popular de Andalucía», de Rodríguez Mateo, «Arte y artistas flamencos», de Fernando de Triana (corregido por Blas Infante), «Cantaores andaluces», de Núñez de Prado... y, sobre todo, «De cante grande y de cante chico», de José Carlos de Luna que, al menos, y a pesar de sus discutibles «dogmas», significaba u\n\n[ENDING CONTEXT]\n\nincreíble sentido de la improvisación».\n\nPero sobre todo, la amante síntesis de su «Flamencología, porque —como escribió Ricardo Molina— «pocos libros ejercieron tan profundo influjo como éste, ni promovieron con tanta eficacia hechos de trascendentes consecuencias». En el momento preciso, cuando más falta hacía la crónica sin la cual no hay batalla que se gane del todo: en la década prodigiosa, cuando, para el misterio gozoso de la resurrección, el cante iba a tener, ya para siempre, en la vida y en la obra de Anselmo González Climent, «su mármol y su día, su infalible mañana y su poeta».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "González Climent y la década prodigiosa",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1106,
    "article_char_count_full": 6671,
    "article_char_count_review": 3073,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "duendes"
      }
    ]
  },
  {
    "article_id": "1992-11-20-left-discurso-de-ingreso-en-la-real-a",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nIlustrísimos señores:\n\nOcurre, pues, que un hispanoamericano —andaluz por los cuatro costados previos— viene desde la babélica Buenos Aires a ingresar en la docta Real Academia Sevillana de Buenos Letras. Por la eminencia de la Congregación, y no por la modestia leal del nuevo colega —de cuyo rango sois responsables—, venimos con esto al principio:\n\nDecía el licenciado don Sebastián de Cobarruvias Orozco, capellán de Su Majestad, maestrescuela y canónigo de la Santa Iglesia de Cuenca, y consultor del Santo Oficio de la Inquisición, año del Señor de 1611, en su Tesoro de la lengua castellana o española, que Andalucía es «la provincia en España más fértil y abundosa de todo lo que se puede desear por mar y tierra». Tres siglos y medio después seguimos sabiéndolo, y sabemos que en Andalucía,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"pasiones\"]\n\n—quien os habla— despiertan la fuerza de la sangre y el ánimo leal totalmente seducido por esta província —reino de fertilidad y abundosa de todo lo vital que es válido desear en este mundo. Conste, doctos amigos, que rechazo de plano la teoría del tropicalismo andaluz, de la manera que niego la torpe teoría del tropicalismo de mi América. Eso sí, aquí y allí (y confieso que se me confunden, a las veces, aquello y esto como doble recinto de mis pasiones, anhelos y confianzas), aquí y allí, decía, generosidad, generosidad que es lo cordial del alma, se ha transformado para mí en estímulo esencial. Americano en España y español en América, por esa misma aparente dicotomía me siento siempre como Pedro en su casa, y con sencillez y gratitud acepto la generosidad fraterna, aun aquella que, como la vuestra, por la responsabilidad que reclama, puede cohibir al más plantado. Andalucía entera Suele dislocarse el concepto o la unidad fundamental del ser andaluz con amputaciones inspiradas en prejuicios políticos, estéticos, pedagógicos, etc. Siempre existe una Andalucía negada o temida. Siempre hay una Andalucía opósita en candelero. Rara vez se ha reaccionado para bucear sus sentido envolvente y de fondo. Dentro del siglo, y con alarmante simplicidad (¡Ortega y Gasset complicado en esto!), se han sucedido superficiales interpretaciones de Andalucía que han pretendido acomodar toda su verdad en una veta, en un sentido concreto, en una «manera». Nada de eso. El contexto vital y metafísico de esta tierra no se presta a fácil deducción. Por lo mismo, no hay derecho a concentrar su acento en una sola y exclusiva dirección de su estilo vital. Luis Cernuda enc\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"generación\"]\n\no de lo andaluz, que de él no quedó nada perceptible. Se fabricó una seudonaturaleza, desvitalizada y huera de la tierra de María Santísima. Total: trampas dialécticas a cargo de una serie de parásitos de lo que se pensaba o se dejaba de pensar en una vaga Europa de minorías. Encontráronse fuera y aun en posesión al genio telúrico. Esta inconsecuencia con lo propio fue simple evasión de la realidad inmediata (contagio confusamente prendido de la generación del 98), y ansia de un mero «cachet» de refinado occidentalismo. A todo ello prestaremos especial atención, porque aun hoy no se quiere valorar del todo la fuerza original de esta corriente flamenca, ni menos que menos su central razón de ser. Bueno es observar que lo flamenco ha sido usualmente limitado —y con mirada torcida— a las expresiones del baile, del cante y del toreo. Pero flamenco —lo estamos viendo cada día más claro— es un estilo d\n\n[ENDING CONTEXT]\n\ny con ella— la que de mil formas posibles debe recobrar sobre sus hijos en América para renovarles constantemente la seguridad de que ser andaluces (españoles, monta tanto) significa en cualquier parte del mundo tan honroso, profundo, mediterráneo privilegio como antigua responsabilidad.\n\nY, al término, me alegra, desde aquí, vuestra Santa María del Buen Aire, y desde allí, mi Sevilla, romper con el inveterado «he dicho» para convertirlo en un fervoroso plural, en el que me acompaña mi ilustre lector, maestro y amigo don Juan Rodríguez Mateo.\n\nPues: «Hemos dicho».\n\nAnselmo González Climent\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discurso de ingreso en la Real Academia Sevillana de Buenas Letras Anselmo",
    "periodical": "candil",
    "issue_id": "1992-11",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "19-24",
    "page_number": 19,
    "word_count": 3749,
    "article_char_count_full": 24013,
    "article_char_count_review": 4279,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "pasiones"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "generación"
      }
    ]
  }
]
```
