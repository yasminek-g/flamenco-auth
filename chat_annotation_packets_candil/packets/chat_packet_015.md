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
    "article_id": "1980-09-17-right-flamenca",
    "article_text_for_review": "CALIXTO SANCHEZ, Giraldillo del Cante\n\nCon sugerente nombre, ALJARAFE, nace, en estas tierras del Sur, una nueva productora discográfica que se dispone —es propósito de sus profesionales creadores— grabar y editar flamenco, música y canción andaluzas desde la propia Andalucía. Como objetivo de ALJARAFE, que nuestros artistas no tengan que ir a Madrid a conseguir, en ocasiones a mendigar, un contrato, para luego grabar y estar pendientes de la compañía que les haya firmado. Bienvenida sea, ALJARAFE. «La muestra que escuchais en esta grabación es el reflejo del gran momento en que se encuentra Calixto, y así resonaron sus portentosas facultades durante las tres noches».\n\nAntonio Mairena\n\nY para andar por este camino de hechos, he aquí en disco, la voz de Calixto Sánchez, Giraldillo del Cante en la I bienal de Arte Flamenco celebrada en Sevilla el pasado mes de Abril. Digamos que, en directo, el L. P., recoge algunos cantes de los que dijera en el Teatro Lope de Vega de la ciudad hispalense. Minera y cartagenera, el polo, cantiñas, peteneras, fandangos, siguirillas y malagueñas, componen esta primera muestra de ALJARAFE.\n\nSignifiquemos, en juicio generalizado, el conocimiento de Calixto Sánchez, su dominio, la mayoría de las veces, de la voz, su inteligente dosificación de las facultades. El disco, bien en casi todos los estilos —peteneras y el polo, dichos con menos fortuna—, es una confirmación del hacer jondo del de Mairena que, hoy por hoy, es uno de los primeros cantaores de la nueva generación flamenca, capaz de proyectar nuestro arte por un sendero de alta calidad artística. Acaso, haya que señalar en el apartado de peros, la frialdad que, en ocasiones, deja entrever, motivada, quizás, por una excesiva concentración en lo que dice y cómo lo dice. Mención especial para Juan Habichuela y Pedro Bacán, guitarras acompañantes. Esperamos el nuevo disco de Calixto Sánchez, ya en preparación.\n\nMANUEL MAIRENA, con la verdad del cante\n\nManolo Mairena se prodiga poco discográficamente. Ahora, RCA, presenta una nueva grabación de su arte, en natural herencia de su casa Mairenera. Dos guitarras apoyan su voz en este recorrido por tangos, soleá, tarantos, siguiriyas, bulerías, cartageneras... las de Enrique de Melchor y Manolo Domínguez.\n\nFiel a su acento, a su raíz, el menor de los Mairena, nos ofrece un acercamiento a formas definidas y concretas del arte gitano andaluz. Tiene una redonda voz que sobresale en aquellos cantes que mejor domina, aunque, hay que decirlo, se prodiga en alargamientos innecesarios que hacen perder el sentir exacto del estilo que dice. No nos gusta cuando abusa de las facultades imetiéndose en extrañas situaciones melódicas que, con frecuencia, no puede o no sabe resolver. Sin dudar de su conocimiento, es profesional titubeante en sus actuaciones. De todas formas, hay aspectos muy positivos y valorativos dentro de Manuel Mairena que él deja caer en este disco.\n\nDOSCANDIL",
    "title": "Flamenca",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 472,
    "article_char_count_full": 2941,
    "article_char_count_review": 2941,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-09-18-left-apuntes-flamencos-a-la-feria-de-",
    "article_text_for_review": "La Peña Flamenca de Jaén abrió como es habitual en los últimos años, con las fiestas y feria de San Lucas, la Caseta Tres Morillas.\n\nCinco cantaores ilustraron, con desigual fortuna, las cinco noches flamencas de las fiestas de Octubre: Carmen Linares, Carlos Cruz, Chocolate, Diego Clavel y Emilia Pérez.\n\nNuestro juicio sobre la actuación de estos artistas ha de tener en cuenta una premisa de enorme relevancia: el difícil entorno, el ambiente lógicamente fiestero de gentes hetereogéneas que nada tiene que ver con el climax que los verdaderos aficionaos creamos en las íntimas veladas celebradas en los locales de la Peña. No es extraño que a algún cantaor, en ocasiones, le rozara la bullanguería, desapaciblemente, sobre todo, cuando el palo preferido era la siguiriya.\n\nCarmen Linares cumplió con mucha dignidad. Se recuerda, sobre todo, el tercio brillante y pleno de comunicación de una siguiriya.\n\nCarlos Cruz, que en el pasado año, obtuvo el trofeo «San Lucas» al mejor cantaor, no estuvo a la altura de sus últimas actuaciones. Seguimos pensando que Carlos es uno de los cantaores más puros y temperamentales desde este momento. Tiene un gran futuro y nos hemos habituado a exigirle demasiado.\n\nEl Chocolate, disperso, desigual, al filo de auténticas genialidades y, al mismo tiempo, mojado de reiterados prosaismos, en el cante.\n\nNo fue tampoco la noche de Diego Clavel, aunque su meritorio oficio le hiciera salvar la situación con dignidad.\n\nPor último, Emilia Pérez, a la que hemos escuchado, en otras ocasiones, con admiración, estuvo fuera de tono y desacompasada. Sabemos que ella es capaz de hacerlo mejor. Será preciso escucharla en la intimidad de una reunión de cabales.\n\nEn definitiva, las Fiestas de San Lucas, transcurrieron discretamente en lo que a cantaores se refiere. Exito de público que, como es sabido no lleva aparejada siempre la presencia del duende. El cante es asi...\n\nJ A E N",
    "title": "APUNTES FLAMENCOS A LA FERIA DE SAN LUCAS",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 313,
    "article_char_count_full": 1916,
    "article_char_count_review": 1916,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-09-18-right-dimensi-n-social-del-cante-flame",
    "article_text_for_review": "CERVEZAS\n\nEl Alcázar\n\nAlcázar Premium\n\nespecial\n\nextra\n\nElcázar 50\n\nlas que todos prefieren",
    "title": "Dimensión social del cante flamenco",
    "periodical": "candil",
    "issue_id": "1980-09",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 13,
    "article_char_count_full": 91,
    "article_char_count_review": 91,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-11-3-right-editorial",
    "article_text_for_review": "Sorprende el poco rigor con que se plantea, por lo general, el problema de la evolución del arte jondo. La cuestión, que no reina pacificamente, adquiere en estos momentos particular relevancia por cuanto no podemos ignorar los numerosos intentos de innovación que se vienen realizando, ni, desde luego, eludir su valoración. Desde el llamado flamenco-pop, hasta esa nueva modalidad de ópera flamenca que con indiscutible gusto dramático, en no pocos casos, pretende escenificar la misma sustancia de lo jondo, cuando no configurar lo que nos atreveríamos a denominar historia apócrifa del flamenco.\n\nParece que lo fácil es dejarse encorsetar por los cánones y lanzar la cómoda alegación del desviacionismo. Para quienes ligan el citar con la guitarra, las palmas con los violines u osan crear «su siguiriya», queda el heroísmo. Este planteamiento maniqueo no es, a nuestro juicio, correcto. Ni progresistas ni reaccionarios porque en el flamenco tales conceptos operan desdibujados y sin apenas significación.\n\nPero debemos definirnos y plantear el tema desde una perspectiva rigurosa, partiendo siempre de presupuestos válidos que nos merezcan, al menos, un mínimo de certidumbre: ¿Qué es éso susceptible de evolución? ¿Conocemos, acaso, lo esencial, lo inmutable en el flamenco? ¿Qué es cualitativamente el arte jondo?. ¿Hasta donde la evolución puede producirse para que lo jondo siga siendo jondo y no una manifestación (¿artística?) distinta?\n\nNos estamos refiriendo a una forma de expresión humana de volúmenes tremendos de emoción y, por lo mismo, difícilmente objetivable, pero, sin dilucidar este presupuesto, es, siempre, ambiguo mentar la evolución. Sin embargo, aunque parezca paradójico, sí podemos valorar las experiencias que en este sentido se vienen realizando. Sin entrar en matizaciones –intuitivos, al menos, aquello que no puede ser jondo - los ensayos para abrir nuevos horizontes al flamenco han sido, a nuestro juicio, tremendamente desafortunados. No sólo porque rezuman poco respeto a la raíz, sino porque vienen motivados por un objetivo falso y peligroso: ampliar el campo de receptores del flamenco, incorporar nuevos públicos sensibilizados, a formas genéricamente bellas de expresión pero no sintonizados a esa textura especíicamente jonda, a la que se accede por la vía del apasionamiento hacia unos hombres, hacia una tierra, por el camino del respeto a una cultura, nunca insolidaria, pero orgullosa de sus propias singularidades.\n\nNo es momento de dogmatizar si la evolución es posible o no es posible. Por el momento, limitémonos a señalar nuestro rechazo a ese «flamenco renovado».",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 399,
    "article_char_count_full": 2619,
    "article_char_count_review": 2619,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1980-11-4-right-el-comp-s-de-la-sole",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Ricardo Rodríguez Cosano Hay dos maneras cantaoras bien diferencias en el Cante Flamenco. Por un lado, una expresión libre donde el cantaor al recrearse en los tercios, generalmente sin aditamento de palmas, alarga a voluntad según la inspiración del momento. No obstante, también tienen que marcarse los tercios en unos tiempos claves por parte del cantaor. Estamos refiriendo el cante largo: expresión de facultades portentosas donde el regusto en los remates, en los repliegues de los bajos, ha de aflorar si queremos que este cante cale. Muchas veces estos cantes se expresan a media voz para poder subir con fuerza los empinados tercios y poder bajar con seguridad. De otro lado, esa expresión cantaora encajada en los diferentes compases marcados por la guitarra: Los cantes a compás. En\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"compas\"]\n\nen los repliegues de los bajos, ha de aflorar si queremos que este cante cale. Muchas veces estos cantes se expresan a media voz para poder subir con fuerza los empinados tercios y poder bajar con seguridad. De otro lado, esa expresión cantaora encajada en los diferentes compases marcados por la guitarra: Los cantes a compás. En esta manera de expresar el cante, los diferentes tercios de los variados estilos flamencos van encajados en múltiples compases definidos por el guitarrista e impulsados por palmeros o marcados por las propias palmas del cantaor. Aquí, el cantaor al tener una sujeción a los sucesivos compases tiene que ir recortando los tercios, lo que supone una gran dificultad. Dentro de los cantes a compás, para muchos aficionados al cante rey, sobresale la soleá. Cuando la noche se va «cargaía» con la pena yo canto por soleá. Cuando el cantaor, en los albores incipientes de su afición explosiva al Cante, se enamora de la soleá, se da cuenta pronto que puede ser un amor imposible. Palpa a menudo que la soleá es sencilla, hembra de modales finos, pero esa sencillez le desconcierta; le puede dejar en ridículo cuando menos se\n\n[ENDING CONTEXT]\n\nlo importante, para algunos, sea el sentir y el actuar. Pero no cabe duda que la investigación, para otros, sacará a la luz ciertos aspectos y matices que nos harán comprender la grandeza de nuestro Arte. No estamos sistemáticamente con ninguno de los dos grupos y sí con ambos según los momentos.\n\n¿Cómo se inventaría ese compás de los nudillos?\n\nCRUZ\n\nMármoles Extranjeros y Nacionales\n\nPuerta de Martos, 7 JAEN\n\nACADEMIA DE ENSEÑANZA Básica, Media y Superior «JOSE LUIS LOPEZ»\n\nGraduado Escolar E. G. B. B. U.P. C.O. U. Selectividad Magisterio 1.ºs Cursos Universitarios Oposiciones Magisterio\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El compás de la Soleá",
    "periodical": "candil",
    "issue_id": "1980-11",
    "year": 1980,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 1385,
    "article_char_count_full": 8237,
    "article_char_count_review": 2773,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "compas"
      }
    ]
  }
]
```
