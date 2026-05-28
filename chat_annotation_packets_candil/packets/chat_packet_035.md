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
    "article_id": "1981-09-23-right-quienes-fueron-los-maestros",
    "article_text_for_review": "DIEGO BERMUDEZ CALA, «EL TENAZAS DE MORON»\n\nD IEGO Bermúdez Cala nació en Morón de la Frontera (Sevilla) allá por el año 1854. Cantaor profesional de poca popularidad en sus comienzos, ha sido considerado figura del arte flamenco a raíz del famoso concurso flamenco de Granada en 1.922, donde obtuvo el premio de soleares, serranas, cañas y polos que le permitió grabar su primer disco. Desde muy joven se trasladó a Puente Genil, localidad en la que ejerciera su oficio de pastor en la finca de don José Malta. Cuenta la tradición oral que Diego realizó el viaje a Granada para participar en el concurso, andando, desde Puente Genil; así mismo, se ha comentado reiteradamente —con verdad o no— que había tenido una reyerta en la que resultó herido y, como consecuencia, perdió un pulmón, Algo que, de ser cierto, hace más meritorio el galardón conseguido en el certamen granadino.\n\nSelecciona: Rafael Valera\n\nEl Tenazas fue paisano y discípulo personal de Silverio Franconetti, de él aprendió los cantes por cañas y, posiblemente, la famosa solea: Correo de Vélez\n\nse espantaron las mulillas\n\nsoleá que, según Ricardo Molina «a pesar de su estructura literaria de soleariya, Tenazas la engrandece (lo cual no quiere decir que la mejore)».\n\nse perdieron los papeles.\n\nContinuando el testimonio de Ricardo Molina, la soleá transmitida por el Tenazas\n\nQue lo tengo muy presente\n\nlo gitano que yo he sio\n\nserrana para quererte.\n\nes una de las ocho o nueve soleares trianeras bien diferenciadas que revelan pureza gitana. Estimo que Diego, payo de nacimiento, representa en los cantes que grabara un fiel testimonio de pureza y limpia transmisión.\n\nFRANCISCO LA PERLA\n\nVarias ciudades se atribuyen la cuna del cantar: Sevilla (Triana), San Fernando y Cádiz; aceptándose, por lo general, su nacimiento trianero, aunque bien es verdad que desde su infancia residió en la provincia de Cádiz, ciudad en la que falleció y en la que desarrollara principalmente su arte.\n\nFrancisco La Perla está considerado como cantaor contemporáneo de Enrique el Mellizo, y su cante es muy similar al del maestro gaditano. Destacó principalmente por siguiriγas, con creación de una propia y personal a la que varias ilustres plumas flamencas comparan con la de El Viejo de la Isla. Para Fernando Quiñones, las dos siguiriγas fueron reelaboradas con posterioridad por la mítica figura de Manuel Torre.\n\nEn nuestros días ha quedado memoria de una tragedia de sangre en la familia de Francisco La Perla (afectó a su hijo e hija política, la cual fue muerta por este, su marido) que demuestra la profundidad dolorosa de su arte por siguiriyes. Tras el asesinato, su hijo huyó a Marruecos y la ausencia del mismo arrancó del cantaor una lacerada letra que se difundió rápidamente en los medios flamencos de su época y que aún recordamos con estremecimiento:\n\nCurro de mi alma,\n\nescribeme una carta,\n\nque con saber que tú te encuentras bueno me sobra y me basta.",
    "title": "Quienes fueron los maestros",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 487,
    "article_char_count_full": 2931,
    "article_char_count_review": 2931,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-09-24-left-discograf-a-placas-de-artistas-f",
    "article_text_for_review": "DISCOGRAFIA (PLACAS) DE ARTISTAS FLAMENCOS\n\nPor Manuel Yerga Lancharro\n\nE S C A C E N A",
    "title": "Discografía (placas) de artistas flamencas",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 17,
    "article_char_count_full": 87,
    "article_char_count_review": 87,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-11-3-right-editorial",
    "article_text_for_review": "Editorial\n\nEn 1881, hace ahora justamente un siglo, el doctor Roque Barcia escribía en el tomo segundo de su Diccionario de la Lengua Española: «Hoy priva, extraordinariamente, en el gusto público un estilo llamado flamenco, que viene a ser como la germanía de la música y que durará poco, puesto que se prodiga mucho, cuando un género se exagera acaba necesariamente por empalagar. Es muy probable que cuando pase el estilo, la noble música española no tenga más que un recuerdo triste, semejante a la gasa negra con que se cubre el rostro de un difunto».\n\nDe esta agorera, inintelectual y desafortunada predicción, tan presuntuosamente expuesta hace cien años –por cierto, 1881 sería conocido popularmente como el de las peteneras–, no queda ya más que el recuerdo como «desbarre» –uno más– «intelectual». Hasta la noble música española y universal se nutrirían de la más noble, hermosa y profunda de las canciones del pueblo español, el cante jondo.\n\nPero no vamos a refutar aquí algo tan obvio que el tiempo ha desmentido de modo rotundo, ni a analizar, una vez más, el despego distante de buena parte de los intelectuales del arte flamenco. Nuestros propósitos son bien distintos: la denuncia de los adobados intereses, de las basuras retóricas intencionadas, los bizantinos debates, las visiones maniqueas, las tomas de posiciones, los racismos mal disimulados, las organizaciones tribales, los «tejemanejes»..., ante lo que expresamos nuestra más decidida y firme repulsa. Si hoy clamamos contra todo esto, no es por mero voluntarismo. El lector encontrará, incluso, entre las páginas de este número de «CANDIL» opiniones, posturas y juicios que avalan cuanto hemos expuesto. Si las publicamos es por el respeto que nos merecen todos nuestros colaboradores y, fundamentalmente, por ser exponente gráfico de los anteriores asertos. Pero nosotros no somos fatalistas. Estas tristes y verídicas realidades que arañan impunemente el flamenco no podrán destruir, y de ello estamos plenamente convencidos, la más honda, auténtica y noble de las músicas andaluzas, la que, por cierto, no nos avergüenza por sus humildes orígenes.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 337,
    "article_char_count_full": 2129,
    "article_char_count_review": 2129,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-11-4-right-el-caso-especial-de-los-caracole",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\npor Fernando Quinones - José Blas Vega\n\nEl séptimo tomo de la Enciclopedia LOS TOROS, la obra monumental de José María de Cossío editada por Espasa-Calpe, verá la luz en 1982. Figurará en él, entre otros muchos trabajos de interés, el titulado «Toros y arte flamenco», cuyos autores son F. Quiñones y J. Blas Vega y del que ofrecemos la primicia de su último capítulo, páginas finales del que puede considerarse un libro en sí, tanto por la extensión de su texto -unas cien páginas - como por la abundancia y riqueza de sus ilustraciones, según es habitual en esa prestigiosa Enciclopedia. Agradecemos a los autores, y especialmente a la editorial «Espasa-Calpe», la deferencia para con «CANDIL» al cederle este breve adelanto. Ultimamos este estudio con unas nociones sobre el más tauroflamenco de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Granaíno\"]\n\nuromáquico que fue uno de sus motivos; antes bien, lo enriquecieron y ensancharon. Dos fuentes parecen confluir, con toda claridad, en el nacimiento del cante de caracoles; una de ellas —la esencial— es andaluza; la otra, posterior y temática, es madrileña. Respecto a la primera, la andaluza, existe el precedente básico de una cantiña gaditana —acaso sanluqueña— donde ya aparece el pregón de los caracoles; luego, el torero y cantaor Tío José «El Granaíno» salió cantando una variante con unos versos burlescos sobre la cuadrilla del espada José Redondo «El Chiclanero», quien había colocado en ella a «El Cuco» en lugar de «El Granaíno», mermado ya por los años de agilidad para parear. La multitud de noticias en torno a este último y andalucísimo personaje, nacido a comienzos del pasado siglo, incluye informes o teorías contradictorios: E. Molina Fajardo aventura, con algún dato bien razonado, que nació en Granada y que, al fracasar como matador, se hizo banderillero; Aurelio Sellés y el autor de «De Cádiz y sus cantes» sugieren, también con argumentos razonables, su cuna gaditana; Ramón Gómez de la Serna lo menciona como al «salvador del cante de caracoles», cuando realmente fue uno de sus creadores y el más antiguo; Estébanez Calderón cita a un «Granaíno» que bien pudiera ser nuestro hombre... Tal contienda de datos no empaña, sin embargo, los probados y seguros: su nombre, José Giménez; su militancia en la cuadrilla de «Paquiro» hasta 1845, en que pasó a la de «El Chiclanero», habiendo toreado también a las órdenes de «El Lavi» (padre) y tal vez a las de Curro Cúchares; la cornada que sufrió en Barcelona y en 1852, disminuidora de su efí- cacia y rendimiento en los ruedos; su campaña en América como subalterno de «El Lavi» en 1858; su dilatada vinculación con Sanlúcer de Barrameda, Cádiz y Chiclana de la Frontera; su condición de excelente cantaor, de quien se sabe que re- dujo ayes en el segundo y cuarto versos del cante de cañas, imprimiendo así a esa modalidad un aire más vivo y valiente, creó y propulsó torrijos, romeras y otras cantiñas, cantó bien los polos y fue asimismo «acceptable guitarrista». Pues bien: en aquella irónica letra suya, que ahora damos, ya aparece el clásico estribillo tau- rino del cante de caracoles, que había de llegar hasta nuestros días: Nicolasillo\n\n[ENDING CONTEXT]\n\nGarcía «El Espartero» y el mejó que es Rafael. ¡Caracoles caracoles, etc...\n\nJuan de la Plata da en sus «Flamencos de Jerez» esta variante del pregón de cierre:\n\n¡Caracoles, caracoles!\n\nMocita, ¿qué ha dicho uste?\n\nQue el senó Paco Frascuelo\n\nva en caballo garabito\n\na los toros de Jeré.\n\nPero la fuerza y la supervivencia del tema tauromáquico en una o en más modalidades flamencas no es como hemos ido viendo, un hecho aislado casual, sino una consecuencia o testimonio de la secular vecindad de dos mundos, andaluces y españoles, tan distintos como identificados.\n\nF.Q. y J.B.V.\n\nMadrid, 1981.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El caso especial de los «caracoles»",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 1156,
    "article_char_count_full": 6975,
    "article_char_count_review": 3945,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "Granaíno"
      }
    ]
  },
  {
    "article_id": "1981-11-7-left-hacia-el-x-congreso-de-actividad",
    "article_text_for_review": "por Ramón Porras\n\nCONGRESO NACIONAL DE ACTIVIDADES FLAMENCAS≈ JAEN 1982\n\nEl X Congreso de Actividades Flamencas se celebrará en Jaén y la comisión organizadora, trabaja ya desde el pasado mes de Octubre. Se pretende estudiar el fenómeno flamenco en su más amplia dimensión, y mediante un tratamiento riguroso, inferir ideas y visiones generales sobre la situación actual del flamenco, así como la problemática futura de esta manifestación artística. Somos conscientes de que esta ambiciosa empresa va a exigir para llevarla a feliz término, análisis y esfuerzos foráneos al flamenco; va a requerir la aportación de otras disciplinas que contemplen «lo jondo» desde perspectivas sociológicas, culturales, antropológicas, etc.\n\nTodos los esfuerzos serán pocos; todas las aportaciones económicas serán exiguas si se trata de reconstruir, de recuperar, de situar en sus justos términos esta manifestación básica de la cultura andaluza. Y hay que discrepar del análisis realizado por Kayros en la revista Andarax n.° 22, «El IX Congreso de Actividades Flamencas» cuando sostiene que hay millones para el flamenco, mientras que otras manifestaciones menos ruidosas y de más relieve cultural, carecen de medios. Creemos que es poco respetuosa la aseveración de Kayros, y, cuando menos, evidencia desconocimiento y falta de sensibilidad en el flamenco; éste no es una manifestación ruidosa, ni tiene más o menos relieve cultural que cualquier otra expresión artística, (¿Dónde se encuentra el módulo para tasar cuantitativamente o cualitativamente una manifestación cultural?) Lo que no quiere decir que no asumamos la posición crítica de Kayros respecto a cenas, reuniones y demás ecos de sociedad, planteados, acaso, con frivolidad.\n\nEn cualquiera de los casos, •El X Congreso de Actividades Flamencas» está en marcha. La experiencia de las nueve ediciones anteriores, propiciará sin duda, un planteamiento más riguroso del flamenco, en el que tengan cabida las correcciones críticas razonables que contra el IX Congreso se han arguido\n\nCONSTRUCCIONES\n\nJ A E N",
    "title": "Hacia el X Congreso de Actividades Flamencas",
    "periodical": "candil",
    "issue_id": "1981-11",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 311,
    "article_char_count_full": 2054,
    "article_char_count_review": 2054,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
