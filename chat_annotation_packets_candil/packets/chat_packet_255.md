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
    "article_id": "1992-05-28-left-buz-n-flamenco-r-plicas",
    "article_text_for_review": "Réplicas\n\nD. Francisco Zambrano Vázquez (Badajoz)\n\nApreciable aficionado:\n\nSoy profesional del cante, en cuanto me dedico a cantar desde la edad de 7 años, con la idea de que lo económico sirva para el engrandecimiento del arte que mi persona pueda aportar y aficionado para hacer un seguimiento que redunde en el mismo sentido.\n\nEs estimulante poder reflexionar opiniones contigo (permitame que le tutee) que aunque discordantes, llenas de savia flamenca, que al fin y al cabo es lo que nos importa.\n\nYa me guardaría yo de poner en entredicho la honestidad e insobornabilidad del Jurado que me calificó en la XXVII edición del Concurso de Linares, en tanto sería aventurado por mi parte ni siquiera pensarlo; lo que reitero y pongo en duda es la valoración que se hizo a mis cantes, porque poniéndose en mi pellejo (que no en el de un jurado que no es el que canta), que me levanté a las 2 de la mañana para estar en Linares a las 9, con diez horas de volante y el consiguiente peligro que para un padre de familia supone tal hándicap de 1.000 kilómetros, a razón de 10 pesetas por dieta, ¡hay que tener mucha afición! mereciendo, al menos, encontrarse una organización más acorde con la importancia del evento.\n\nSe podría pensar que este enorme sacrificio es el que me ha hecho llenarme de valentía para enfrentar mi criterio al vuestro, o la pérdida del metálico, o mi herido orgullo; sin embargo no, entrando ya en materia, es que la voz se vuelve más «laía» conforme se avanza en el número de tractel-tono de la guitarra y como recordarás la soleá apolá la canté en un tono alto (5), porque una voz laí-na es aquella que se manifiesta en tonos bajos o graves, como podría ser la de (q.e.p.d.) Antonio Molina, que cantando una serrana al 3 le sonaba la voz como una sirena. Pero no es este mi caso, ni los cantes por medio de Vallejo o Camarón que son cantaores de poderío, adjetivo éste a años luz del truco laíno, porque es muy fácil para nosotros cantar en grave o afiliao aun a riesgo de perder brillantez y transmisión, ¿pero lo es para los que cantan en grave subir trácte-les sin que le aparezca el rozamiento y la afonía?, ¿supone la misma dificultad al torero componer la figura a distancia que con los cuernos rozando la taleguilla? Si después de pasar tanta penalidad se me «confunde» con tener la voz laína, no ha sido por menos empezar mi contestada crítica con aquello de....«siempre mirando a la cara si la ponen mala o buena».\n\nEn cuanto a las tarantas de las que me aleccionas en algo, debo informarte que utilicé como patrón las del varias veces premiado con la Cabria Minera, el linarense Manuel González, sin meterle ningún matiz, que no lo admite ni que yo lo he mencionado que lo admita.\n\nAdemás no tuve la suerte de presenciar la final del Concurso, pero según el comentario sobre su desarrollo en el «Candil» número 77, efectuado por don Rafael Valera Espinosa, el ganador de la Cabria Mariano Morillas cantó sus dos tarantas «basadas en los melismas marcheneros» ¿y no usaba Marchena la voz «laína»? De todas formas ha sido muy honrosa tu actitud de contestar a mi escrito, de lo que quedo satisfecho por lo que de bueno pueda tener para el flamenco el estudio de sus contradicciones en el seno de esta prestigiosa revista.\n\nSaludos cordiales.\n\nNiño de Miguel (cantaor y aficionado)\n\nSeñor don José Luis Pimentel Fagoaga\n\nContesto a su carta a través de esta prestigiosa Revista «Can-dil» para su conocimiento y el de los lectores de la misma.\n\nEfectivamente, puedo decirle sin lugar a equivocarme que Alora es a la Malagueña, como Jerez es a las Bulerías. Esa ciudad malagueña nos ha dado infinidad de buenos cantaores.\n\nY con respecto a su consulta le diré que «El Perote» fue un cantaor por malagueñas bastante bueno. Según su cuñado, el gran guitarrista «Niño Pérez», la malagueña que cantó fue creada por él sin proponérselo. Se llamó Juan Trujillo García. Nació el año 1833, fue hijo de otro buen cantaor, Francisco Trujillo, y de doña María García. Sus abuelos fueron Juan García y Antonia Gil y Francisco Trujillo y María García. Falleció en la ciudad de Sevilla, en estado de casado.\n\nSobre Juan de Reyes Osuna «El Canario», de Alora, le informaré en otra ocasión facilitándole los datos biográficos y los de sus padres y abuelos.\n\nCordialmente le saluda,\n\nManuel Yerga Lancharro",
    "title": "Buzón flamenco",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "28-28",
    "page_number": 28,
    "word_count": 758,
    "article_char_count_full": 4317,
    "article_char_count_review": 4317,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-05-28-right-noticiario-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAteneo de Córdoba Fallado el Premio Nacional de Letras Flamencas\n\nLa obra «La copla sale sola», del escritor sevillano Romualdo Molina, ha resultado ganadora del Premio Nacional de Letras Flamencas que convoca anualmente el Ateneo de Córdoba.\n\nEl jurado, formado por Agustín Gómez, flamencólogo; Roberto Loya, poeta; Andrés Raya, editor de la colección Demófilo; Luis de Córdoba, cantaor; y Francisco Martínez, crítico flamenco, ha elegido la obra ganadora entre más de cincuenta originales presentados a la presente edición de este premio que consiste en la publicación en número de 2.000 ejemplares para su distribución a todas las peñas flamencas del territorio nacional de forma gratuita. Romualdo Molina desarrolla una amplia labor de estudio, divulgación, promoción y valoración del flamenco y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"publicado\"]\n\nCórdoba, cantaor; y Francisco Martínez, crítico flamenco, ha elegido la obra ganadora entre más de cincuenta originales presentados a la presente edición de este premio que consiste en la publicación en número de 2.000 ejemplares para su distribución a todas las peñas flamencas del territorio nacional de forma gratuita. Romualdo Molina desarrolla una amplia labor de estudio, divulgación, promoción y valoración del flamenco y sus cultivadores. Ha publicado diversos trabajos sobre temas flamencos, varios de ellos en colaboración con Miguel Espín, tales como «Juan Varea, un rey sin corona», «Pepe el de la Matrona, una roca de cristal de roca», «El año de Silverio», «Bailar, siempre bailar», «Silverio, ecos de su talento». En trámites de edición «Flamenco de ida y vuelta». Creación de la Asociación Alurican Se ha constituido oficialmente en enero pasado la Asociación Alurican, cuya misión consiste en la promoción y difusión del arte flamenco en la región de Franche-Comté (este de Francia). Esta asociación, dirigida por Jean Paul Tarby (presidente), Christine Jeanningros (secretario), Christine Brezard (tesorero), tiene su sede social en 7, rue Pasteur, Besancon 25000 (Francia), y está apadrinada por la Peña Cultural Flamenca «Torres Macarena», de Sevilla. Centro Andaluz de Flamenco (Jerez) IV Seminario Internacional de Guitarra Flamenca El Centro Andaluz de Flamenco convoca el IV Seminario Internacional de Guitarra Flamenca, del 19 al 30 de octubre, con el siguiente programa: —Armonía y estructura musical del Flamenco. —Desarrollo de la técnica especí- fica de la guitarra flamenca. —Desarrollo de la temática fla- menca. —Perfeccionamiento. Inscripciones en el Centro, Palácio Pemartín, Plaza de San Juan, 11403, Ierez. Centro Andaluz de F\n\n[ENDING CONTEXT]\n\nen el Concurso Nacional de Córdoba, el Premio Manolo de Huelva, y ese mismo año la Cátedra de Flamencología y Estudios Folklóricos Andaluces de Jerez le otorgó el máximo galardón en su género: el Premio Nacional de Guitarra Flamenca. Su discografía es ya numerosa, y entre sus actuaciones más significativas sobresale su participación en la Bienal de Arte Flamenco Ciudad de Sevilla, los años 1984 y 1986. Ha realizado diversas giras por el extranjero con distintos elencos artísticos, y son continuas sus actuaciones en Peñas Flamencas y Centros Culturales.\n\nTOCAORES DE HOY\n\nJosé Luis Postigo\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Noticiario flamenco",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "news_roundup",
    "pages": "28-31",
    "page_number": 28,
    "word_count": 2523,
    "article_char_count_full": 16737,
    "article_char_count_review": 3389,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "publicado"
      }
    ]
  },
  {
    "article_id": "1992-05-30-right-qu-date-con-el-cante",
    "article_text_for_review": "Programa Flamenco\n\nSintonícenos de lunes a viernes, de 20,30 a 22,00 horas; viernes, sábados y domingos de 0,30 a 3,00 horas, FLAMENCO",
    "title": "\"Quédate con el Cante\"",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "30-30",
    "page_number": 30,
    "word_count": 22,
    "article_char_count_full": 134,
    "article_char_count_review": 134,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-07-3-left-editorial",
    "article_text_for_review": "Pese a que ciertos pronósticos hacían temer lo peor, nadie esperaba un desenlace así. El último reportaje en televisión que glosaba su estancia en hospitales estadounidenses y la vuelta esperanzada a su casa, nos hizo ver a un Camarón menos huidizo, más firme, dentro de su fragilidad, más ilusionado por un futuro que trágicamente se le ha quebrado. Siempre resulta demasiado paradójico, demasiado cruel el que, cuando más apostaba por la vida e incluso comenzaba a hacer números en beneficio de los suyos, sin crispación, casi con dulzura, la muerte nos arrebatara al último mito populista del Flamenco: José Monje Cruz, «Camarón de la Isla».\n\nTal vez nos falta aún perspectiva para enjuiciar a una figura artística tan genial como controvertida, pero hay datos que ya no admiten discusión. Tanto al Camarón de la Venta de Vargas y la calle del Carmen, como al Camarón que en un local madrileño cerca de la Plaza del Callao hiciesa amistad con Paco de Lucía, durante su estancia en el Tablao Torres Bermeja, amistad que fructificó en la edición de grabaciones ya paradigmáticas, no se le puede negar dos cualidades: hondura y genialidad. Con posterio-\n\nridad al Camarón que es admirado por Mike Jagger, por Miles Davis, por Peter Gabriel, etc., y por un público gitano incondicional que llenaba los locales en los que actuaba, se le han lanzado diatribas en relación a sus últimas producciones que indagan el rumbo hacia la innovación, hacia el desasirse de lo repetitivo y de lo inventariado. En ninguno de los casos se debe cuestionar la ortodoxia básica de Camarón, con independencia del juicio que merezcan tales innovaciones. Camarón ha sido siempre un cantar puro, elemental, pleno de rajo y de jondura, un cantaor de duende y de mágica inspiración.\n\nDijimos antes que fue, que sigue siendo un mito populista, sin que tal adjetivación contenga referencias peyorativas. Antes al contrario, el populismo del cantaor de la Isla Blanca es un fenómeno sociológico que sólo encuentra precedentes en don Antonio Chacón, tal vez en Manuel Torre, pero en menor medida o si se quiere con otra significación. Su penetración en públicos no tiene parangón alguno con cantaores vivos o muertos de los últimos cincuenta años. Y con ello no intentamos priorizar el cante de Camarón, la genialidad.\n\nPor encima de cualquier otra consideración, nadie puede negarle a «Camarón de la Isla», primero, su personalísima manera de decir el cante, hasta el punto de crear entre sus múltiples seguidores un eco, un sonido perfectamente identificable; segundo, un cante que más allá de su perfección formal, suele estar asistido por los duendes; y tercero, una ortodoxia básica que, pese a las controvertidas innovaciones de algunas de sus últimas grabaciones, está ahí, en decenas de placas y constituyen un hermosísimo legado para la posteridad.\n\nHa muerto Camarón y no es una hipérbole el mantener que, un poco, también ha muerto el duende, entre nosotros.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1992-07",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 483,
    "article_char_count_full": 2940,
    "article_char_count_review": 2940,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-07-4-left-aproximaci-n-a-la-figura-de-cama",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\na muerte de José Monje, «Camarón de la Isla», ha constituido un auténtico impacto social, equiparable al que pudiera producirse con cualquier otro mito consagrado de músicas populares o cultas. La prensa, al hilo de esa sensibilidad, no ha regateado medios, titulares, entrevistas, loas y comentarios apologéticos. Me parece bien, aunque rechace el «topicazo» tan celtíbero, por otra parte, de esconder, en alguna sentina de nuestras envidias, el elogio merecido al artista vivo, para sacarlo al sol cuando el referente ha desaparecido. Huyó de las generalizaciones, pero, personalmente, pienso que gran parte de las manifestaciones que ahora se producen traen un pútrido olor, como si, de pronto, se evacuará lo que durante años no pudo ser digerido.\n\nNada es del todo nuevo, y el tratamiento con\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"pura\"]\n\nse evacuará lo que durante años no pudo ser digerido. Nada es del todo nuevo, y el tratamiento con mayor o menor profusión que se ha dado a la triste noticia, tiene sus precedentes que no viene al caso analizar. Otra constatación. La muerte magnifica a quienes son víctimas de ella, y al cantaor de la Isla no se le ha sustraído de esa regla general. Y ello lo decimos no en demérito del artista, como más adelante pondremos de manifiesto, sino por pura coherencia con pretéritas valoraciones que se hicieron de su trayectoria cantaora. Con la perspectiva que sólo el tiempo puede aportar, son siempre aventuradas —cuando no frívolas— las sentencias maximalistas. Algunos de los que hoy se muestran más positivamente realizadores, prescribieron sin matices el talante innovador del Camarón. La muerte, por sentida que sea, no legítima tal mudanza de criterio. Otras, en el vértice de la exaltación, demandan la Llave de Oro, a título póstumo, para el cantaor desaparecido. Preciso es que atemperemos los fervores y, sin arrebatarle al César lo que es del César, tratemos de objetivar —en la medida en que ello sea posible por falta de perspectiva en el tiempo, reiteramos— la significación de un cantaor genial, «Camarón de la Isla», y su proyección en el tiempo. El epíteto «genial» no obedece a exigencias de retórica. Camarón fue un genio y tal afirmación requiere precisiones. E\n\n[ENDING CONTEXT]\n\ndejamos constancia, tal vez a contracorriente, de que preferimos, sin ningún género de dudas, al Camarón clásico, respetuoso con las antiguas formas, aunque elevándolas con su cuño personalísimo, antes que el Camarón de la Royal Philharmonic Orchestra de Londres. Sobre gustos nada hay definitivo.\n\nEn cualquiera de los casos, la muerte le sobreviene a Camarón cuando se halla en plena madurez. ¡Quién sabe lo que hubiera podido obtener...!\n\nLo que sí sabemos es que se nos ha muerto una de las figuras más señeras y paradigmáticas del flamenco, en la segunda mitad de este siglo. Descanse en paz.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aproximación a la figura de Camarón de la Isla Ramón",
    "periodical": "candil",
    "issue_id": "1992-07",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-5",
    "page_number": 3,
    "word_count": 1310,
    "article_char_count_full": 8016,
    "article_char_count_review": 3005,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "pura"
      }
    ]
  }
]
```
