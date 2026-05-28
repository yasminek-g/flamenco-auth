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
    "article_id": "1992-01-3-right-apuntes-sobre-la-sole-y-la-sigui",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFalta un estudio directo y especializado sobre la soleá, pese a lo mucho dicho y escrito, siempre de forma aislada si no pasajera.\n\nConcluido ya «Viejo Carné Flamenco», hemos creído interesante la publicación del presente trabajo, de Anselmo González Climent. La mayoría de las reflexiones que en el mismo se vierten, el lector ha podido ya encontrarlas en la abundante bibliografía del maestro argentino. Ello, no obstante, reiteramos el interés de su publicación, por cuanto estas reflexiones se realizan con posterioridad a la redacción de «Viejo Carné Flamenco». Hemos de precisar que con mucha probabilidad, González Climent, hubiese sometido a revisión algunas de las posturas aquí explanadas. Pese a lo cual, ofrecemos su publicación íntegra, tal y como nos ha sido legada, en la convicción\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"histórico\"]\n\no. Ello, no obstante, reiteramos el interés de su publicación, por cuanto estas reflexiones se realizan con posterioridad a la redacción de «Viejo Carné Flamenco». Hemos de precisar que con mucha probabilidad, González Climent, hubiese sometido a revisión algunas de las posturas aquí explanadas. Pese a lo cual, ofrecemos su publicación íntegra, tal y como nos ha sido legada, en la convicción de que nuestros lectores apreciarán su indudable valor histórico. Generalidades A la par, el descuido de su cultivo en nuestros días puede implicar la desvertebración toda del cante jondo, pues como queda dicho, la soleá regentea el gesto humano y musical del flamenquismo. Acceptable, aunque no perdonable, es la desaparición de otros estilos como los ya mencionados de cañas, polos, livianas, deblas, etcétera —los dos primeros, antecedentes inmediatos de nuestro cante—, pero el caso de la soleá no es cuestión de un estilo entre tantos, sino de una vertebra esencial en la salud toda del arte flamenco. Burlar su culto supondría modificar el rumbo del cancionero andaluz, tarea utópica para los que se dispongan llevarla a cabo, por cuanto la soleá lleva a remolque, directa o indirectamente, la inmensa mayoría de los estilos flamencos conocidos, desde la serrana al tango gaditano, desde el fandango —en todas sus modalidades— hasta el grupo de cantes por alegrías. Y ello es así por cuanto corresponde reiterar el sentido enciclopédico que gobierna a la soleá, vale decir su carácter representativo de la psicología y estética andaluza. Los demás cantes apuntan un estado particular del hombre, de una forma expresiva concreta irrebasable. Son, por decirlo así, cantes especializados, subsidiarios. La soleá, en cambio, acusa todas las gamas de la vitalidad sureña. Este concepto no pareciera ciertamente captable a primera vista. Sólo un estudio analítico o una experiencia subjetiva directa, podrían prestarle razón. Dentro de las clasificaciones usuales de cante jondo, grande y chico como de jondo y flamenco, la soleá participa de la categoría de cante jondo grande. En la actualidad solamente ha trascendido, para mal o para bien, dentro del campo flamenco usual. Precisamente, un ejemplo típico del difícil deslinde de lo jondo y lo flamenco, se da en el caso de nuestro cante. Comprése a estos efectos la soleá jond\n\n[ENDING CONTEXT]\n\nEsta tendencia, en esa dirección, posiblemente sea hacedera, pero no aceptable si ascendemos a la conservación de los cantes grandes, intrínsicamente lentos, graves, no dispuestos al barroquismo rítmico. La soleá —insistimos— puede resistir tales transformaciones virtuales por cuanto no sólo posee una especial disposición para ello, sino que asimismo puede transmitir esa riqueza tempológica a gran parte de los cantes que le son afines o derivativos. Con todo, sobra advertir que no es exactamente que la soleá vaya hacia los cantes chicos, sino que éstos le deben pleitesía.\n\n(Continuará)\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Apuntes sobre la soleá y la siguiriya / 1",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-6",
    "page_number": 3,
    "word_count": 3075,
    "article_char_count_full": 19501,
    "article_char_count_review": 3948,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "histórico"
      }
    ]
  },
  {
    "article_id": "1992-01-6-right-juan-g-mez-belmonte",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSerio, sabiendo la responsabilidad que había asumido, y, en el fondo, pienso que con determinada satisfacción por ello. Por otro lado, valiente, con criterios propios y firmes, consciente de su reiterada inclinación hacia Manuel Torre, mas no sin satisfacción. Conocedor de la verdad del cante almeriense y de la poca ayuda que el mismo recibe. Así se presentó en la Peña Flamenca de Jaén, Juan Gómez Belmonte, después de atravesar en el caluroso septiembre del noventa y uno, la serpenteante carretera que por el desierto almeriense, cruzando la vergel Guadix, desemboca en la ciudad del Santo Reino.\n\nLas siete en punto y con el sofocante y húmedo bo- chorno que reina en el local de la Peña, comienza el diálogo:\n\n-¿Dónde y cuándo naces?\n\n—En Almería y en marzo del cuarenta y dos.\n\n-¿Cómo surge\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"afición\"]\n\npresentó en la Peña Flamenca de Jaén, Juan Gómez Belmonte, después de atravesar en el caluroso septiembre del noventa y uno, la serpenteante carretera que por el desierto almeriense, cruzando la vergel Guadix, desemboca en la ciudad del Santo Reino. Las siete en punto y con el sofocante y húmedo bo- chorno que reina en el local de la Peña, comienza el diálogo: -¿Dónde y cuándo naces? —En Almería y en marzo del cuarenta y dos. -¿Cómo surge tu afición al flamenco? —Pues desde muy niño. En mi casa nadie ha síó profesional de esto, pero sí ha habido muy buenos afícionaos. Mi padre, mis tíos, los hombres viejos que yo escuchaba de niño, amigos de mi familia. De ahí me viene la afición. —En realidad no se puede decir que yo sea profesional, pues vivo de mi propio negocio. Con los camiones resuelvo mi vida y el cante me gusta realizarlo en sitios donde me encuentro con los amigos y en el momento justo. —Háblanos de esos viejos aficionados que citas —Eran hombres del mar como mi padre. Aficionaos que venían de la pesca y se ponían a cantar en las tabernas recordando a grandes figuras como Vallejo, Cepero... —Mi padre fue un hombre que había conocido al Pena. Se había criao en Málaga y escuchó a los cantaores que entonces había allí. Oyó al Pena, al Niño de las Moras —aunque ya éste muy viejo, lo escuché en El Palo—. Mi padre hacía la cartagenera de Chacón, malagueñas... Esto era lo que de pequeño yo escuchaba en mi casa. En el aspecto profesional lo hacía con Cobitos, con el Perote... Pero primordialmente oía a los\n\n[ENDING CONTEXT]\n\nflamencos, pues existe mucha tensión, muchos nervios... Creo que no hay justicia a la hora de otorgar los premios. Hay miembros del jurado que saben lo que es el flamenco, pero hay otros que no son auténticamente flamencos. Entonces no te puede sentenciar un miembro del jurado que no tiene ni idea de lo que estás cantando. Recuerdo que una vez estaba realizando la cabal de Silverio en La Unión y me dijeron que eso parecía una saeta. Me otorgaron un accésit y no el premio, argumentando que había acabao con una saeta. Exigí escuchar la grabación y demostré que era la cabal de Silverio.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Juan Gómez Belmonte",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 1583,
    "article_char_count_full": 9015,
    "article_char_count_review": 3152,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "afición"
      }
    ]
  },
  {
    "article_id": "1992-01-8-left-la-ni-a-de-ecija",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE 1 hecho de que Ecija celebra-se, el pasado día 17 de septiembre, la final del I Concurso de la Copla Andaluza como homenaje a la Niña de Ecija, me sirve en bandeja la oportunidad de dar a conocer parte de la biografía, inédita hasta ahora, de esta artista excepcional y flamenca de altos vuelos. No creo, por consiguiente, encontrar mejor ocasión para aportar datos desconocidos y así hacer justicia a un personaje considerado del todo nonato en la historia del flamenco.\n\nSi bien Francisca Martín Freire, hija de los ecijanos Antonio, desbravador de caballos, y Valle, afamada costurera, nunca negó en entrevistas su procedencia astigitana, lo cierto es que fue concebida en Ecija pero nació por accidente en el número 4 de la antequerana calle de Juan Adame, el día 10 de mayo de 1909, siendo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"nuevo\"]\n\nsonaje considerado del todo nonato en la historia del flamenco. Si bien Francisca Martín Freire, hija de los ecijanos Antonio, desbravador de caballos, y Valle, afamada costurera, nunca negó en entrevistas su procedencia astigitana, lo cierto es que fue concebida en Ecija pero nació por accidente en el número 4 de la antequerana calle de Juan Adame, el día 10 de mayo de 1909, siendo bautizada en la iglesia de San Pedro. A los pocos meses, y de nuevo por motivos del trabajo paterno, marcha la familia a Puente Genil, hasta que en 1917 regresan definitivamente a Ecija. Las enseñanzas flamencas de su tía Valle, unidas a una vocación musical temprana y el querer imitar a su hermana Valle —mayor que ella 11 años y notable cantaora—, hace que Paquita actúe en reuniones familiares y, pese a la negativa materna, decide hacerse artista. La oportunidad le llegó cuando su hermana Valle, ya casada, se la lleva a Madrid, concretamente al cuartel de la Benemérita de Carabanchel Bajo, donde vivía su pri- ma Dolores Freire Chía, es\n\n[ENDING CONTEXT]\n\npor ideas más profundas que el péndulo de la moda operística. Recorrer sin liviandad el escalonamiento de los registros canoros, fue la ruta transitada por una mujer que fue punta de lanza de su tiempo y que dio un espíritu nuevo al flamenco y a la auténtica copla andaluza. Decía Helvencio que la carencia de pasiones hace al hombre estúpido. Confío en que estas torpes líneas sean suficientes para justificar la tardanza de un homenaje. Pero con los argumentos expuestos o sin ellos, quien firma siempre rendirá pleitesía a quienes con su arte dieron gloria y nombradía a la tierra de origen.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La Niña de Ecija",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1513,
    "article_char_count_full": 8784,
    "article_char_count_review": 2650,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "nuevo"
      }
    ]
  },
  {
    "article_id": "1992-01-12-right-viii-distinci-n-comp-s-del-cante",
    "article_text_for_review": "Tuvo que ser en las vísperas de dos días «señalaítos» en nuestro calendario jaenero, tan castizos como la Distinción misma que nos disponemos a glosar y que no es otra que «El Compás del Cante», generosamente otorgada por la Cruz del Campo, S. A. y magníficamente llevada a cabo en su desarrollo por la amabilidad y finura exquisita de don Enrique Osborne, amigo como hay pocos y eficaz como ninguno.\n\nEn efecto, la Distinción se falló el día 22 de noviembre pasado, cuando los jiennenses nos preparábamos para honrar a la Virgen Catalina, que aguarda su día en las alturas del castillo que lleva su nombre, y se entregó al ganador el 16 de enero de este año, el mismo día en que Jaén ardía entre las hogueras que la devoción popular tributa a San Antón, patrón de los animales domésticos y animador de melenchones populares y tertulias. Este año, la modalidad elegida para la Distinción, sometida a la votación previa del jurado, reunido en ese nido de cordialidad que es el restaurante «La Albahaca», fue la del baile, lo que parece coherente, toda vez que en las siete edicio-\n\nnes anteriores, tan sólo en una ocasión recayó tan importante galardón en esta parcela, imposible de olvidar en el flamenco, aunque, eso sí, al premiar aquella vez a Pilar López, los duendes se coaligaron para tejer una corona de oro para adornar el arte insuperable de la hermana de Encarnación, «La Argentinita».\n\nGreemos sinceramente que los que componemos el Jurado también hemos acertado de pleno en la elección de este año, al margen, naturalmente, de las preferencias individuales por tal o cual artista que siempre son sumamente respe- tables, puesto que, tras un arduo y encendido debate, sin duda para quien esto escribe el más apasionante de la historia del trofeo, se acordó su concesión al genial bailar o gitano Antonio Montoya, «Farruco», premiándose así, no sólo la categoría excepcional, la pureza interpretativa de este auténtico monstruo, sino también la fidelidad de una estirpe, el arte de toda una dinastía de la que Antonio es patriarca indiscutible e indiscutido y que forma una cadena de duendes que no se ha interrumpido a pesar de los largos años que lleva incardinando la espina dorsal de nuestro arte, aunque, por desgracia, falte algún eslabón insustituible, como aquel «Farruquito», hijo del bailaor, muerto en la década de los setenta en desgraciado accidente y a cuyo homenaje póstumo asistimos, en el polideportivo de Chapina, todo el mundo flamenco sin excepción, formando la más impresionante manifestación artística que conozco, con el dolor a flor de piel atenazado en la rabia de la siguiriya, pero con el bálsamo de alegría que nuestros cantes festeros poseen para aliviar quemazones del alma como aquella.\n\nLa noche del 16 de enero, en los salones reales del hotel Alfonso XIII, todo era brillante: se celebraba el triunfo de los Montoya en un acto íntimo a pesar de lo rebosante del local, puesto que íntimos son los sentimientos de solidaridad que aunaron a los reunidos bajo el auspicio de la jondura de Farruco. Al término de la cena, la voz amiga de Enrique Osborne dirigía la singladura de la entrega con estas palabras: «Un año más, bienvenidos a este acto de entrega de la Distinción Compás del Cante, que reúne en este Salón Real a personalidades del mundo de la política, de la empresa, de los medios de comunicación, del flamenco y, ¿cómo no!, a los pasados ganadores de esta Distinción, que nos honran de nuevo con su presencia». A continuación, y tras la lectura del acta del jurado por la portavoz del mismo, Marta Carrasco, tomó la palabra don José Ruiz de Castroviejo y Serrano, consejero delegado del grupo Cruzcampo, para glosar, con verbo fácil y entrañable, la figura del distinguido en esta VIII edición, tras de lo cual se dirigió a nosotros don Robert\n\nHermans, presidente ejecutivo del grupo cervecero, para destacar el acierto de la elección y elogiar al distinguido en un correcto castellano, plagado de anécdotas simpáticas y con el encanto del neófito que se entusiasma por vez primera con el flamenco.\n\nA continuación, y tras la entrega del galardón a Farruco por parte del señor Hermans, la familia Montoya impartió una impagable lección de baile gitano andaluz, enlazándose los dos extremos de la dinastía con las interpretaciones de Antonio, el abuelo, pasando por la generación intermedia y terminando, por ahora, en Juan, el Farru-quito de nueve años que puso en pie a los asistentes con un baile de garra, ensolerado en las esencias de la casa.\n\nAl terminar el acto, todos quedamos satisfechos del galardón y de la brillante entrega del mismo, si bien nunca faltan los disconformes y hasta los agoreros del mal fario que hacen notar sus voces negativas. A mí, como al poeta, «la luz del entendimiento me hace ser muy comedido». ■",
    "title": "VIII Distinción «Compás del Cante»",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 810,
    "article_char_count_full": 4786,
    "article_char_count_review": 4786,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-01-13-right-viii-festival-de-oto-o",
    "article_text_for_review": "A unique un poco fuera de tiempo por la fecha de la celebración de este grandioso festival, no podíamos dejar sin comentario la gran trascendencia de este suntuoso acontecimiento celebrado en el majestuoso marco del Monumental madrileño, renovado y acondicionado para esta clase de excepcionales eventos musicales, del que en esta ocasión fue especial protagonista esta gran realidad cantaora que es Carmen Linares, mostrándonos el momento esplendoroso de la gran madurez artística que ha alcanzado en plena juventud, derivado de su gran vocación profesional no exenta del sacrificio que requiere el riguroso estudio a que somete su gran capacidad creadora que como premio merecidisimo la ha llevado a conquistar, por derecho propio, uno de los primerísimos puestos de nuestra nómina flamenca.\n\nNo podemos olvidar que la inspiración de este acontecimiento musical tiene en parte sus raíces en artistas tan egregios como fueron García Lorca y Encarnación López «La Argentinita», que salvando las diferencias que impone la distancia en el tiempo y la gran orquestación musical conseguida, marcan de manera muy definitoria la calidad artística entre lo que pudo ser rutinario entretenimiento sin ambiciones y la esplendorosa realidad de este concienzudo trabajo, donde a las ligeras cancioncillas que lo originaron, hoy se les aporta una enjundia y profundidad flamenca de las que entonces carecieron.\n\nEl perfecto ensamblaje conseguido entre esa gran agrupación que\n\nes la Orquesta Sinfónica de Madrid y el grupo musical aportado por Carmen, nos ofrecieron una primera parte del espectáculo pleno de belleza en todos sus aspectos, donde a pesar de no encontrarse muy bien de voz por la inclemencia del tiempo, se superó para cerrar esta primera parte de una manera brillantísima.\n\nEn la segunda parte tuvo una destacada intervención la Orquesta Sinfónica con una magnífica interpretación de «El sombrero de tres picos» y «El amor brujo», porque especialmente en esta última composición la violenta confrontación de pasiones en que se basa la obra del insigne Falla deja poco margen a la intervención flamenca, donde, no obstante, también se apreciaron las diferencias de calidades existentes entre esta gran cantaora y otras creaciones que recientemente se han hecho de esta misma obra.\n\nNiño Jorge\n\nCreo que la gran calidad artística de este espectáculo del que se ofrecieron tres representaciones dentro de nuestra Comunidad, puede tener una gran influencia en el desarrollo musical del flamenco que a partir de aquí puede continuar enriqueciéndose con otras aportaciones orquestales de nuestra gran cantera musical, sin desvirtuar para nada la ortodoxia y pureza del flamenco, cerrando el paso a esa corriente que pretende extranjerizar nuestro arte.\n\nTeléfono (953) 275687 La suntuosidad del Monumental y su gran capacidad, no fueron inconveniente para que se agotaran las localidades, a pesar de su alto precio, dejando a una gran cantidad de público sin tener acceso al disfrute de esta bella y original creación que tanta relevancia puede dar al arte flamenco, abriéndole puertas y fronteras con dignidad y elegancia, aunque suponemos que el alto costo de este espectáculo pudiera ser justificadamente un inconveniente que, dada su gran calidad, debiera prodigarse aun a costa de bonificaciones que permitieran pasearlo por toda nuestra geografía para deleite y conocimiento de la numerosísima afición flamenca afincada en todo nuestro territorio, promocionando también su salida al extranjero, donde por su calidad cultural nos atrajera con nuevos éxitos de admiración y consideración de los países por donde pudiera pasearse, y convencidos de esta seguridad, nos permitimos recavar la atención del Ministerio de Cultura para que considere este generalizado deseo de la afición.\n\nLa unanimidad en el elogio que al término de la función se palpa-ba en la sala, es el mejor aval del grandioso éxito obtenido por Carmen Linares, a quien renovamos, con nuestra admiración, nuestra sincera felicitación.\n\n¡Adelante, cantaora! ■\n\nManolo Canalejas\n\nTeléfono (953) 254981",
    "title": "VIII Festival de Otoño",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 625,
    "article_char_count_full": 4069,
    "article_char_count_review": 4069,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
