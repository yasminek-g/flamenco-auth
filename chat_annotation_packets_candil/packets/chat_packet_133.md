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
    "article_id": "1986-05-21-left-buzon-flamenco",
    "article_text_for_review": "Antonio Grau Mora, «Rojo el Alpargatero» nació el día 7 de diciembre de 1847 y falleció el año de 1907 a la edad de sesenta años.\n\nMuy señor mío: Le felicito por haber descubierto el error de fecha de nacimiento que efectivamente figura en mi libro adquirido por usted.\n\nROJO EL ALPARGATERO\n\nVoy a aclararle el error con mucho gusto.\n\nSu esposa nació el año de 1849 y quedó viuda a la edad de 58 años y no a los 50 como consta en el acta de defunción del Registro Civil de La Unión. Estos son datos recabados por el Juez «ad libitum» y para aclararlo están los además certificados de que dispongo.\n\nEvidentemente también existe error en el libro al figurar el nacimiento de la viuda el año de 1840 en lugar de 1849.\n\nA continuación y con el fin de hacer llegar el esclarecimiento del error a los lectores de «Candil» hago figurar tres fotocopias importantes ísimas para la historia de la biografía del genial cantaor y de su esposa.\n\nIgnoro si el error es de imprenta o es mío al facilitar los datos. Es igual. Ya queda aclarado. MINISTERIO DE JUSTICIA Regiிறes Civiles\n\nCERTIFICACION LITERAL DE INSCRIPCION DE DEFUNCIÓN ___ (1)\n\nREGISTRO CIVIL DE La Unión\n\nProvincia de ___\n\nEl asiento al marcas resenado literalmente dice así: \"Número 439, = ANTO-NICO GRÁT MORA = En la ciudad de La Unión a las 1990, nora-del afte santiuno de Abril de mil novecien-y-sista, unte de Prado-calmarón, su-to-Juez Múni-pés y D. Miguel López Izviera, Supla-a Searstarlo, compareció D. Miguel Fructuoso Tome-m-tural de La Unión, término municipal de Iden, Pro-dis-la-Múria, edad de 37 años, acado, minero, dom-sitiato-en-s-ta-cidad, sillo de Zuvora, númico vali-ta, según acredita de su edula personal que exhba-número sinco mil ochociento- veinticuatro, manifestan-do que ANTONIO GRAU MORA, natural de Jallosa, provin-cia de Aljanta, de incuenta y nueve años de edad, ca-ado, industrial y vesino de o-ta-ciudad, domiliado en la calle Mayor, ha fallecido en su domiliio a la una y treinta minuto del día de hoy, a con-cuen-ia-Fuen-mónia; de lo cual daba parte en debida forma, co-mo encargado al éxito para ello. En vista de esta-maníf-estación y de la certificación facultativa pre-sentado, el cr. Juez Municipal dispuso que se exten-disse la pre-ente asta de in-cripión, con-ignando-se en ella, adama de lo expuesto por el declarante y en virtud de la noticia que se han podido adquirir las cir-cun-tana-iguientes. Que el referido finado de encontraba en el a-to da su fallecimiento acado con María Denset Morano, de incuenta año de edad, natural de Almeria, de cuyo matrimonio viven tre-hijo-llamado- Antonió, Jés-y Pedro, manores de edad.--que era niño de Samuel y Múria.- Que no otorgó-te-tamento Y que su adáver no le habrá de dar capú-tura en al-lente-nero de esta ciudad. = Fueron te-tigo pre-encia-la D. Antonio Merader Sanch.z, natural de Pozo E-trécho, provin-ia de Múria, mayor de edad, ac-ado, mi-ro y domiliado en c-ta-ciudad, sillo de Jala-de-Herader, número..., y D. Miguel Gil de Paraja, na-tural de Ciudad-Real, provin-ia de iden, mayor de edad, acado, empleado y domiliado en c-ta-ciudad, calle mina dierva, núm...= Leía integra esta a-ta-\n\nPARTIDA DE BAUTISMO DE LA MUJER\n\n«D.ṙ Rafael Castañedo Oña presbítero auxiliar del Sr. Cuira Rector de la Yglesia parroquial de S. Sebastián de la Ciudad de Almería Certifico: que en el libro cuarenta y tres de bautismos de la misma al folio noventa se halla la siguiente: PARTIDA: En la Yglesia parroquial de S. Sebastián extra-muros de la Ciudad de Almería, Capital de provincia y Obis-pado a veinte y cuatro de Septiembre de mil ochocientos cuarenta y nueve: yo D. Juan José de Osorio Cura Teniente de ella bauticé solemnemente a María del Mar Jesualda que nació en el mismo día a las tres de la mañana hija lejítima de Manuel Doncet jornalero y de Antonia Moreno de esta Ciudad: Abuelos paternos Nicolás Doncet y Rafael de Orta maternos José Moreno y Ana Borraz esta natural de Institución, los demás de esta.\n\nPARTIDA BAUTISMO DE «ROJO EL ALPARGATERO»\n\n«En la Parroq.¹ de S. Martin de la Villa de Callosa de Segura, Prov. a de Alicante, Obispado de Orihuela, dia ocho de Diciembre de mil ochocientos cuarenta y siete: Y Yo D. Antonio Galvez Vic.° Em.° de la misma, Bautize solemnemente a un niño, hijo de Manuel Grau, y M.° Manuela Mora, consortes, nat.s de esta, jornaleros. Ab.s Pat.s José y Carmela Canales; Mat.s Manuel y Manuela Grau; Le puse por nombre Antonio. Nació a las cuatro de la tarde del día de ayer, segun relacion de los Pad.s que lo fueron Antonio Macia y Antonia Marco, a quienes adverti el parentesco espiritual y demas de que Certifico = Antonio Galvez, V.° E.°». Y yo Lisardo Guede Fernández, Archívero del Diocesano de Málaga, CERTIFICO: Que, esta partida es copia literal de la que obra en el Expediente Matrimonial de Antonio GRAU MORA con M.° del Mar Doucet Moreno. Y para que conste, a instancia de Manuel Yerga Lancharro, firmo y sello esta copia en Málaga, a veinte de noviembre de mil novecientos ochenta.",
    "title": "Buzón flamenco",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 839,
    "article_char_count_full": 4979,
    "article_char_count_review": 4979,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-05-21-right-festivales-flamencos",
    "article_text_for_review": "(Relación facilitada por la Asesoría de Flamenco de la Consejería de Cultura de la Junta de Andalucía)",
    "title": "Guía de Festivales Flamencos",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "21-23",
    "page_number": 21,
    "word_count": 17,
    "article_char_count_full": 102,
    "article_char_count_review": 102,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-05-23-right-xiv-congreso-nacional-de-activid",
    "article_text_for_review": "El próximo mes de septiembre, concretamente entre los días 8 al 12, se va a celebrar en Hospitalet (Barcelona) el XIV Congreso Nacional de Actividades Flamencas que con exquisito rigor y eficacia está organizando el Ayuntamiento de Hospitalet y la «Peña Cultural Recreativa Antonio Mairena, cuya presidencia de honor la ostentará el Excimo. señor don Felipe González Márquez, presidente del Gobierno.\n\nNo es la primera vez que un Congreso Flamenco se celebra fuera de las fronteras andaluzas, puesto que existen los precedentes de Zamora y Cáceres. Esperemos que esta nueva A Cataluña le corresponde, en esta ocasión, la responsabilidad de alentar esa refrescante corriente de opinión que esclarece conceptos y alerta sobre desviacionismos, cosa muy frecuente en el flamenco.\n\nSabemos que un congreso no es un aula, ni su finalidad es aprender algo, entre otras razones, porque lo aprendible en el flamenco, en términos de absoluta objetividad, es bien poco. Lo que sí tenemos que conservar es la pureza de los aficiona-\n\nva salida a la diáspora esté dirigida a la ordenación de un congreso operativo, riguroso en la selección de ponencias y seriedad en los debates. dos, su capacidad de contemplación ante la hermosura de nuestro arte, porque el cante seguirá siendo puro mientras no se malogren sus aficionados.\n\nEstamos seguros que nuestros hermanos flamencos catalanes lo van a conseguir, fundamentalmente, por el dinamismo que les caracteriza.\n\nPara la inscripción en este congreso, los interesados habrán de dirigirse al Secretariado del Ayuntamiento de Hospitalet, Unidad de Relaciones Institucionales y Protocolo. Plaza del Ayuntamiento, 11 Hospitalet. Teléfono (93) 337 03 66.\n\nPROGRAMA:",
    "title": "XIV Congreso Nacional de Actividades Flamencas",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 262,
    "article_char_count_full": 1696,
    "article_char_count_review": 1696,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-05-24-left-a-placas",
    "article_text_for_review": "-Discografía (Placas)\n\nPor Manuel Yerga\n\nTaranta minera Granaína Alegrías de Córdoba Granaína Taranta minera Caracoles\n\nChufla Malagueña Fandango de Lucena Chufla Garrotín Marianas Soleares Taranta Tango Chufla Guajira Taranta Fgo. de Cartagena Malagueña\n\nFandango\n\nEL SOTA\n\nLlégate un momentico a mi casa Tu enfermeá se curaba Dígale al cochero, cochero Porque no hables de la gente Que ya nos falta la respiración Calle de Atocha\n\nGARRIDO DE JEREZ\n\nLa pícara vaca Mi vía por aborrecerte Que to se tenía que acabar Soy zapatero de viejo Yo tengo una prima hermana Madrecita de mi alma Tengo mi ropita en venta Ay, María del Carmen Cada vez que paso y miro Porque no le tengo ley Le rompi la cantimplora Pongo delante de Dios En la corriente del agua Moro, yo soy más moro...\n\nR. Montoya R. Montoya R. Montoya R. Montoya R. Montoya R. Montoya R. Montoya\n\nGUERRITA\n\nYo vi una noche mortal Ay, mare, mare perdona Después de tantas promesas Nochecita clara Que yo me voy a morir Están llenos de terrones La naranja y el limón Junto a este arroyo se cría Mi pena tengo perdía De rodillas me postré Levanta tu frente Que se acerca la muerte Mis lágrimas me acalló Calle la boca charrán Que la guardia me vigile Que tú no vengas a buscarme Que estaba detrás de una mata Yo le tiro al corazón No creas que por tu querer Se me presente un espanto Que le llaman la Alcazaba No niego que te he querido Que lo mejor de Cartagena Le pedí un ramo de olor Falta no me hace ninguna Nochecita clara Yo vi una noche mortal Junto a este arroyo se cría Yo le tiro al corazón Dame la espuela Tú redoblas mis martirios Porque te llaman Dolores Con mi poncho y mi reverte Que ella es buena y volverá Que juntas estaban las dos\n\nRomán García Román García Román García Román García Román García Román García Román García Lebita Lebita Lebita Román García Román García Román García\n\nM. Borrull",
    "title": "Discografía placas",
    "periodical": "candil",
    "issue_id": "1986-05",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 338,
    "article_char_count_full": 1868,
    "article_char_count_review": 1868,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-07-3-right-editorial",
    "article_text_for_review": "E l cincuenta aniversario de la execrable muerte de Federico García Lorca, una de las figuras sin duda más relevantes de la lírica y el teatro en el presente siglo, ha merecido la atención de numerosos colectivos, la mayoría de los cuales subrayaron la vigencia del poeta y dramaturgo de Fuente vaqueros, su proyección internacional y, en general, las hermosas virtualidades de un autor que, partiendo de localistas dimensiones, representa los valores de una épica universal.\n\nFederico fue también un poeta, activamente, preocupado por el flamenco. Su intervención en el Concurso Granadino de 1922, junto a otros conspicuos personajes de la Generación del 27, estuvo impregnada de un protagonismo vehemente, pleno de curiosidad y salpicado de comprensibles errores. No es éste el momento de reiterar análisis sobre aciertos y desaciertos del famoso Concurso, lo que ha sido objeto de una, tal vez, desmesurada bibliografía. Nos interesa destacar aquí el personalismo lorquiano, adentrándose en lo jondo, herido, con una singular intensidad, por ese grito que rompía el azogue de los espejos. Dicho de otra manera, qué grado de captación del flamenco tuvo el autor del Romancero Gitano. Han existido respuestas ciertamente antagónicas desde quienes estimaron que nadie mejor que él conoció y profundizó en lo jondo, hasta quienes lo desautorizaron, al igual que a la totalidad de los intervinientes en el Concurso Granadino, sobre la premisa de que aquél y éstos contemplaron el flamenco de una forma rigurosamente abstracta, exclusivamente estética, como un motivo más de inspiración, lo que, conectaba con el gusto por lo exótico del último romanticismo de Bécquer, tan del gusto de los prohombres del 27. Creemos que ni lo uno ni lo otro.\n\nFedericò se sumergió, como pocos, en los hondones del flamenco; captó toda su grandeza y supo transmitir tan bellos estremecimientos. Cómo puede, si no, explicarse la exactitud con que aprehende el mundo mágico de las siguiriyas de Silverio, su poema del cante jondo? Sin embargo, no supo percatarse de quienes, en aquel entonces, encarnaban la verdad flamenca, lo que evidencia, a mi juicio, una forma de desconocimiento de aspectos existenciales del cante. ¡Empeñarse en escudriñar dónde se hallan las vetas auténticas de jondura, conversar sobre esta futil busca con quienes —Manuel Torre, Tomás Pavón, Niña de los Peines— eran depositarios de todas las jonduras! Ello, empero, no es argumento bastante para desautorizar a Federico. Nadie como él ha derramado tanta sensibilidad, nadie ha reflexionado con más firmeza y talento sobre los criterios del duende. Nadie, en definitiva, como él ha creado lo que pudiera denominarse evangelios apócrifos del flamenco, con sus mágicos personajes, la descripción de hermosísimos entornos, el dramatismo de relatos que demandan ecos de terribles siguiriyas.\n\nAlgunos puristas, encorsetadores de cualquier manifestación de arte, con la ventaja que les da la perspectiva de medio siglo, señalan como aquellos intelectuales, con acento peyorativo, a García Lorca, y sus coetáneos. Tales, desconocen que sin el amor de los escritores del 27 y el de otras generaciones de escritores que tomaron el relevo, el flamenco, con mucha probabilidad, hubiera consumado el proceso de degradación en que se encontraba cuando aquéllos y éstos acertaron a investigar y difundir su incommensurable virtualidad artística; y sobre todo, por la vía de cernerlo de otras espúreas manifestaciones, iniciaron su dignificación.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1986-07",
    "year": 1986,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 541,
    "article_char_count_full": 3488,
    "article_char_count_review": 3488,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
