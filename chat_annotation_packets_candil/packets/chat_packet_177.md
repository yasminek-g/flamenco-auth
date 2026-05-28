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
    "article_id": "1988-07-21-right-buz-n-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n¿Quién tiene la culpa?\n\nLos aficionados en Cataluña como los de toda España (supongo), nos las prometíamos muy felices, al tener conocimiento sobre la serie que TVE estaba preparando sobre el flamenco. Las noticias que nos iban llegando a través de los medios de comunicación nos hacían pensar que por fin el ARTE FLAMENCO recibiría el trato que siempre se le negó, en la famosa caja tonta.\n\nEsperando que llegara la fecha prevista para su puesta en antena, nuestro gozo se vio una vez más en un pozo, cuando comprobamos que el circuito catalán 2.ª cadena no emite ni piensa emitir tan esperado programa. Parece ser que la anulación de dicho programa ha sido de interés de audiencia, ¿interés de quién?\n\nPienso que el flamenco ya no puede ni debe quedar aislado en el marco y alrededores que un día\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\nel circuito catalán 2.ª cadena no emite ni piensa emitir tan esperado programa. Parece ser que la anulación de dicho programa ha sido de interés de audiencia, ¿interés de quién? Pienso que el flamenco ya no puede ni debe quedar aislado en el marco y alrededores que un día lo viera nacer. Está demostrado que hace tiempo empezó a andar él solito dándose a conocer a personas que físicamente y geográficamente distan muchos miles de kilómetres de su lugar de nacimiento, y es que para el arte no existen fronteras. Otra cosa muy diferente es la opinión de ciertos directivos que parecen más preocupados en satisfacer al mandamás de turno que al conjunto de la población, que, en definitiva, somos los que pagamos sus salarios con nuestros impuestos. Se nos podrán dar todas las razones que quieran los responsables de anular el programa, pero posiblemente ninguna sea tan convicente como la de nuestra queja. ¿Quién tiene que velar por el patrimonio cultural de un pueblo?, naturalmente todos los ciudadanos y a la cabeza los organismos oficiales; pero los responsables de TVE, circuito catalán 2.ª cadena, esto parecen ignorarlo. Señores responsables de matar el programa POR LOS CAMINOS DEL CANTE, en Cataluña, los aficionados estamos cansados de que se siga maltratando al Flamenco, en favor de no sé qué intereses. ¡Ah!, pero cuando nos visita un personaje extranjero de cierta rele- Flaco favor le hac\n\n[ENDING CONTEXT]\n\nentre todos lentamente llenemos nuestro Gran Museo Flamenco —único, mundial— nuestra Casa Universal del Flamenco, nuestra esperada y al fin conseguida Fundación Andaluza del Flamenco.\n\nRecuerdo en este momento que tengo una estupenda fotografía del que fue gran cantaor «El Mochuelo», precisamente de la época en que se hallaba en la plenitud de su vida artística; y os recuerdo que «El Mochuelo» era unos años mayor que don Antonio Chacón. Desde luego la enviaré a la Fundación, lo contrario sería predicar y no dar trigo.\n\nHasta la próxima os envío un saludo muy flamenco.\n\nAntonio Escribano Ortiz\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Buzón Flamenco",
    "periodical": "candil",
    "issue_id": "1988-07",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1815,
    "article_char_count_full": 11278,
    "article_char_count_review": 3033,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  },
  {
    "article_id": "1988-09-3-right-editorial",
    "article_text_for_review": "D el día 17 al 23 de octubre de este año, se ha celebrado en Córdoba, la XVI Edición del Congreso Nacional de Actividades Flamencas. Un Congreso bien organizado, con excelente material de apoyo y un contenido que ha suscitado abundante polémica entre los asistentes y que merece atento análisis, con independencia de otros aspectos accidentales del Congreso que reiteramos ha discurrido con la proverbial exquisitez cordobesa. Una objeción generalizada que me apresuro a matizar, se refiere al tono excesivamente intelectualizado de las Ponencias. La expresión esperpéntica que resume este malestar se concreta en el controvertido «Huevo de Silverio». «Pero, hombre! venir yo aquí para que un señor me diga que a Silverio le faltaba un huevo...». Anécdotas aparte, es lo cierto, que en los últimos años proliferan las reflexiones eruditas sobre el flamenco o, más exactamente, sobre perspectivas vehiculadoras del fenómeno flamenco. Y ello no debería constituir, en modo alguno, motivo de desconcierto para quienes, con espíritu pueblerino, con enorme simpleza, sólo acepta la experiencia viva de lo jondo o, a lo más, las referencias directas a esa contemplación estética. Si de verdad pretendemos profundizar en el fenómeno flamenco, es necesario que éste sea sometido al dictamen multidisciplinar de estudios científicos, por árido que resulte a los habituales asistentes a estos Congresos. Y realizada esta precisión hemos de retomar el juicio global que, a nuestro modesto entender, nos merecen las Ponencias presentadas en el Congreso de Córdoba. Me consta que los organizadores han realizado notables esfuerzos por aplicar criterios selectivos en la elección de las Ponencias presentadas. Y, amigo, de momento no hay más cera que la que arde. Cierto que determinados congresistas rozan la patanería con su demanda insistente de concreción, pero cierto también que alguno de los trabajos leídos se mece en cuestiones tan inocuas como la determinación del sexo de los ángeles. ¿Dónde está el justo equilibrio? En el Congreso de Córdoba yo no lo he encontrado, salvo puntuales excepciones. Insustancial «El año de Silverio» y «Una aproximación a los principios históricos de la Etnomusicología». Admirable el esfuerzo de Philippe Donnier, pero desmesurado en su intento de transcribir al pentagrama, lo que nunca es transcribible, ni aún por razones didácticas. Sin apoyatura ni probanza alguna, por el momento, el trabajo de Garrido Nieto «Sociología Sefardita del Flamenco», injustamente denostado por una gran mayoría de los congresistas. Y discretas las demás Ponencias, sin que sea éste el lugar de valorar la escasa solidez de algunas de ellas, con ambiciones metafísicas.\n\nDesde otro punto de vista, la Mesa del Congreso, que actuó con rigor en la moderación de los debates en las últimas sesiones de trabajo, careció de energía, en momentos en que, no sé si la oligofrenia o la memez de quien sólo puede hacerse notar por sus absurdas salidas de tono, pedía votos de censura para personas ausentes y para Instituciones respetables. Más concretamente, la última sesión fue paradigma de cómo jamás debe de actuar quien está llamado a organizar y no a desorganizar los debates y controversias del Congreso.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 504,
    "article_char_count_full": 3215,
    "article_char_count_review": 3215,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-09-3-right-xvi-congreso-nacional-de-activid",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLlegamos a Córdoba el lu- cicatrizar que horas antes nos abriera Fernanda de Utrera en la lebrijana «Choza de Juani- quín». Las alforjas cargadas de ese misterioso duende lla- mado a desaparecer y las ex- pectativas depositadas en la sensatez del espíritu cordobés, hacían presagiar un Congreso que posibilitara nuevas líneas de investigación —así se nos anunció—, tratamiento riguro- oso a la génesis e historia de este arte aún reciente, a- portaciones a la plagiada bió- grafía, análisis de la decadente realidad que nos circunda, etc. Y nos hemos encontrado con una metodología basa- mentada en buscar la chorrea- da emoción ante un ordena- dor, enseñar esta expresión musical en el pentagrama o lo- calizar nuestros orígenes en la soleá de los candelabros (?).\n\nArribamos, por tanto, a la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"peña\"]\n\ntc. Y nos hemos encontrado con una metodología basa- mentada en buscar la chorrea- da emoción ante un ordena- dor, enseñar esta expresión musical en el pentagrama o lo- calizar nuestros orígenes en la soleá de los candelabros (?). Arribamos, por tanto, a la ciudad califal al objeto, entre otros, de ampliar nuestros menguados conocimientos y hete aquí que el lenguaje del Congreso no tuvo nada que ver con la gravosa problemática del mundo de las peñas, con la urgente alternativa a los festivales o con la «ojana» cantada y mecanizada que a diario nos azota. Y es que la comisión organizadora, o no disponía de claridad de ideas (podían haber pedido asesoramiento a Antonio Mata, al que luego nombraron primer congresista de honor), o no han encontrado a las personas idóneas, o viven en una onda protagonista totalmente ajena al Flamenco contemporáneo. Sea como fuere, lo cierto e incuestionable es que el desencanto ha sido mayúsculo. Ignoramos el criterio o intereses seguidos para seleccionar las ponencias, no entendemos qué han hecho durante los dos años que han contado para la organización de un Congreso de Flamenco para flamencos y, nobleza obliga, resaltamos la valía personal de Rafael Román, secretario ejecutivo, y de Dionisio Ortiz, delegado de Cultura. Y todo porque no se ha sometido a debate la autenticidad de lo jondo, lo de «actividades flamencas» es ya historia pretérita, se intenta dilucidar el sexo de los ángeles y, como viene siendo tónica habitual, muchos han equivocado el púlpito de su folklorismo intelectualizado. MIERCOLES, 19 Las sesiones de trabajo, tras dos plomizas horas para elegir la mesa del Congreso, comenzaron el miércoles 19 por mor del profesor Gutiérrez Carbajo, premio de investigación de la Fundación Andaluzas de Flamenco, quien, basándose en las teorías métricas de los formalistas rusos, disertó sobre «La rima y la pausa en la copla flamenca». Destacó que la rima (factor eufórico más importante de la copla flamenca), la distribución regular de los acentos y el metro, constituyen los elementos más importantes del ritmo, para posteriormente clasificarla por su distribución en la estro\n\n[ENDING CONTEXT]\n\nMata Gómez, por sus méritos flamencos y su asistencia a los 16 Congresos celebrados hasta la fecha. (Los que allí estuvieron dicen que faltó al celebrado en Zamora).\n\n16. La Mesa Intercongreso estará formada por la de este Congreso y por igual número de miembros nombrados por la Confederación de Peñas.\n\n15. La Asamblea ratifica Jerez como sede del XVII Congreso. Que la Mesa Intercongreso se ponga en contacto con Jerez para que el Ayuntamiento ratifique su postura, y si no lo hace, que la Confederación de Peñas sea quien busque una sede para el próximo, y se lo comunique a los congresistas.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "XVI Congreso Nacional de Actividades Flamencas",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "4-8",
    "page_number": 4,
    "word_count": 5049,
    "article_char_count_full": 30674,
    "article_char_count_review": 3760,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "peña"
      }
    ]
  },
  {
    "article_id": "1988-09-8-right-el-flamenco-en-la-pintura",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAntonio Povedano\n\nCon el patrocinio de la Fundación Andaluzas de Flamenco, ha sido inaugurada en el Palacio Pemartín de Jerez una exposición monográfica de Flamenco en el Arte Español Contemporáneo. En esta exposición se recogen obras de pintores y escultores desde finales de siglo hasta la actualidad. Una exposición rica en contenido y variada en el campo de las tendencias. Cada movimiento artístico, ha tenido en cada una de ellas, representaciones de calidad y prestigio; se ha ido cumpliendo el propósito inicial que las motivó, sacar al flamenco como plástica de la baja calidad en que se encontraba sumergido.\n\nEl deterioro era tan manifiesto que los artistas de prestigio huían del tema, ignorándolo como si nunca hubiera existido. Así estaban las cosas y así las encontramos a nuestra\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\ncada una de ellas, representaciones de calidad y prestigio; se ha ido cumpliendo el propósito inicial que las motivó, sacar al flamenco como plástica de la baja calidad en que se encontraba sumergido. El deterioro era tan manifiesto que los artistas de prestigio huían del tema, ignorándolo como si nunca hubiera existido. Así estaban las cosas y así las encontramos a nuestra llegada a la palestra artística; estaba claro que si queríamos tener un arte digno había que luchar por la recuperación de los valores olvidados, y quedaba igualmente claro que tenían que ser los artistas de prestigio que en ese momento tenían entablada la lucha por la renovación, tratando de imponer un arte expresivamente vivo. El flamenco, como tema, casi desde su arranque, estuvo desasistido de los artistas más representativos de aquel tiempo. Aquella prestancia que alcanzó con el famoso grabador Gustavo Doré y algún español como Lamayer, la había perdido. Con la cabeza fría y el corazón caliente, los pintores interesados realmente por el flamenco y su problemática, iniciamos una labor de captación que no dio los resultados deseados. Los artistas en los que habíamos pensado como compañeros de viaje, se mantenían alejados, ajenos y un tanto despectivos; naturalmente tenían sus razones para mantener esa actitud. El flamenco como tema en la plástica estaba en manos de pseudo artistas y, naturalmente, tenía mala prensa. No había pues, muchas posibilidades de entendimiento entre los que ardorosamente deseábamos incorporar las experiencias de aquel momento, incluida la atracción, y los que podían aportar en bloque los resultados de su lucha. La actualización del tema, que nos habíamos propuesto como punto de partida fallaba en una parte importante con estas ausencias, y sabíamos que la labor de transformación que queríamos realizar, tenía que estar basada en una amplia plataforma de colaboración; si se hiciera, había que hacerlo en una bien organizada labor de conjunto; no bastaba el buen deseo de unos pocos, por mucho entusiasmo que lleváramos a la empresa. Esta era la situación real, cuando al fin, varios amigos, pintores, escultores y poetas, decidimos montar una exposición dedicada al flamenco, la primera colectiva monográfica que se hacía. No ér\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_01 | trigger=\"fuera\"]\n\nEs preciso decir aquí, que pusimos tanto interés en la organización de aquel primer acto colectivo, que fue, al tiempo que primicia, un éxito que se valoró alto por lo que representaba como defensa artística de una parcela olvidada. El haber contado en diversas ocasiones la historia completa de estas exposiciones, me libera de hacerlo en ésta. La lucha mantenida por su calidad, los avatares de su financiación, etc., etc., en este caso estarían fuera de lugar. Creo que puede ser más interesante, y no se ha contado nunca, dedicar estas líneas a la historia de las dificultades, no de la organización, sino de la que pintores y escultores han encontrado a lo largo del tiempo para nutrir su necesidad de información documental; los contratiempos han sido realmente serios en algunas etapas o épocas de la historia del flamenco. El artista, no hay que decirlo, necesita ver, observar, apreciar en su\n\n[ENDING CONTEXT]\n\npero precisamente esta circunstancia me lo impide, no sería ético. Son muchos los inconvenientes que encontramos a la hora de incorporar al cuadro la sensación de un cante, la representación o interpretación de un baile, y muy especialmente, el conseguir que campe en la obra ese sentimiento tan etéreo y a la vez tan consistente, que te hace ver flamenco en lo pintado para que sobre eso hagamos crítica. Crítica que, naturalmente, como visión, sería siempre subjetiva y por supuesto polémica, y que yo sepa, la polémica por radicalidad, siempre se queda en el distinto parecer del que polemiza.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Flamenco en la Pintura",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "8-10",
    "page_number": 8,
    "word_count": 2304,
    "article_char_count_full": 13857,
    "article_char_count_review": 4843,
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
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "fuera"
      }
    ]
  },
  {
    "article_id": "1988-09-10-right-temas-flamencos",
    "article_text_for_review": "Paco Vallecillo\n\nUN par de amigos bastante seguidores de la televisión —por lo menos, mucho más concordantes que nosotros— nos refirieron en su momento ciertas (creemos) manifestaciones de una persona que después de investigaciones realizadas, se llegaba a la conclusión de que antes de Silverio Franconetti nada se sabía del Cante. Del cante que hoy, generalizando, llamamos flamenco. Haciendo reserva de la mayor o menor exactitud en cuanto no fue transcrito y sin tomar el rábano por las hojas, pensamos que no será ocioso rebatir esa argumentación si, en efecto, se acomoda la realidad referida.\n\nNos viene de perlas para nuestra teoría un excelente trabajo que conocimos hace un par de años y que en el actual fue presentado a la Conferencia organizada por la Fundación Andaluza de Flamenco. De él (2), que para nosotros goza del respeto que siempre nos ha impuesto la investigación rigurosa y, como tal, seria y formal, entresacamos datos suficientes que pasamos a exponer:\n\nSilverio nace en Sevilla en 1831. Cabe pensar que en 1867 debía estar en su plenitud de facultades, cuando en ese mismo año aparece públicamente en el Teatro Principal de Jerez. Incluso si existieran pruebas de que ya empezó a cantar bastante antes de ese año, a los efectos de nuestra argumentación nos da igual. Pues bien, en ese mismo teatro, y justamente el 4 de julio, el señor Silverio canta la caña de El Fillo y el polo de Tobalo. Cuando el gran sevillano incorpora en su programa los nombres del puertorrealeño y del rondeño (3) hay que pensar que no serían, ni mucho menos, coetáneos suyos; y a esta conclusión se llega perfectamente sin invocar la maltratada tradición oral, bastante intranscendente en el campo de la investigación, aunque jamás prescindible. Evidentemente, El Fillo y Tobalo cantaron antes que Silverio. El 31 de octubre de ese mismo año el Teatro Principal prosigue su campaña mezcla de sinfonía, comedia, danza española y flamenco, y atención a estos nombres: Juan Fernández, el hijo de Curro Dulce (literal). Francisco Fernández, conocido por el Dulce (literal). El señor Loreto, (a) La Cherna (4).\n\nAdemás del maestro Patiño (5) y Francisco Cantero, conocido por Paco el Barbero (literal). Todos los nombres que anteceden están tomados del semanario «El Porvenir de Jerez» de la misma fecha antes indicada.\n\nAquí ya no aparece ninguna du- da al respecto de que Curro Dulce (6), padre de esos dos Fernández\n\nque cantan en 1867, tenía que haber sido cantaor antes que Silverio. Y habrá que admitir también que Joaquín Lacherna y Patiño y El Barbero debieron haber aprendido a cantar y tocar respectivamente alrededor de la fecha en que lo hiciera Silverio, pero cabe preguntarse: ¿Aprendieron sus aptitudes en lo que hoy se llaman cursillos acelerados o les vino de voces y toques mucho más antiguos que la aparición del gran Silverio?\n\nPara unas consideraciones finales habrá que anotar también que en este último espectáculo teatral actuaron el bailaor Mingoli (Mangoli para otros, suponemos, también y en este caso supuestamente gitano) y que se cantaron y bailaron seguirillas, polo, caña, jaleo y tango americano. Este 31 de octubre no actúa el maestro Silverio; se trata de una función benéfica (¡ya desde entonces, amigo Pulpón!) a favor de Diego García.\n\nAnte esta evidente prueba documental, cabe preguntarse cómo se puede aventurar un juicio tan temerario cual el que se nos ha referido. Mil y una veces hemos repetido que el Cante no nació por generación espontánea ni se inventó como la penicilina o el avecrem. El Cante —antes el baile— devino hasta su situación presente a\n\n(1) «España y el Norte de Africa». El protectorado en Marruecos. Víctor Morales Lezcano. Universidad Nacional de Educación a Distancia, 1986, Madrid.\n\n(2) «La aparición del cante flamenco en el teatro jerezano del siglo XIX», Gerhard Steingress.\n\n(3) Francisco Ortega Vargas y Cristóbal Palmero, respectivamente.\n\n(4) Joaquín Lacherna, tío carnal de Manuel Torre y primer maestro del irrepetible jerezano.\n\n(5) José Patiño González, tenido por inventor de la cejilla, una de las pocas cosas que se pueden inventar en el flamenco.\n\n(6) También nombrado Curro Durse, cabeza de la dinastía de los Ortega: Dos días señalaitos de Santiago y Sant'ana... través de una ósmosis, un proceso evolutivo, germinal se nos consiente. Que hablemos, por ejemplo, de El Planeta, no significa absolutamente nada, porque de él lo ignoramos todos. Seamos más humildes, más socráticos y en este movedizo campo de la historiografía flamenca, huyamos de las verdades absolutas.\n\nEntre quienes defienden que el cante es excluyentemente andaluz y niegan la trascendencia de los gitanos, vamos a reprimirnos todos un poco. El Cante y sus apéndices (tan importantes como el baile y la guitarra) nace, crece, se desarrolla (y esperamos que no muera jamás) en Andalucía y sus aledafios cantaores. Los gitanos lo han marcado profundamente, los gitanos andaluces, como, según acabamos de ver y por citar nombres concretos junto a Silverio: los hijos de Curro Dulce, el señor Loreto (La Cherna), el señor Patiño y hasta un inidentificado Mangoli-Mingoli; y muchísimos otros que harían prolija su relación.\n\nPUBLICIDAD\n\nJ. A. PULPÓN\n\nO'Donnell, núm. 3 - 4.º Teléfs. 222058 - 216920\n\nSEVILLA\n\nParticular: Teléf. 228078\n\nPágina 20 CANDIL",
    "title": "Temas Flamencos",
    "periodical": "candil",
    "issue_id": "1988-09",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "10-11",
    "page_number": 10,
    "word_count": 862,
    "article_char_count_full": 5302,
    "article_char_count_review": 5302,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
