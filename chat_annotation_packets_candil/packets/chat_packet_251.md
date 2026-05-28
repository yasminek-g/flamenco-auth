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
    "article_id": "1992-03-30-left-buz-n-flamenco",
    "article_text_for_review": "Estimado Sr. Director de la revista Candil.\n\nEl punto de vista que pretendo darles sobre el Festival de Jóvenes Aficionados al Flamenco, no es si no la visión que puedo ofrecerles como uno más de los participantes, y posteriormente finalista, que decidimos inscribirnos y participar en el concurso que el citado Festival conlleva y del que es punto culminante.\n\nEn la colombina ciudad de Huelva, nos dimos cita todos los participantes que tuvimos el honor y la suerte de poder asistir, como parte directamente implidada, a la celebración de la fase final del concurso: el consabido III Festival Andaluz de Jóvenes Aficionados al Flamenco. Previamente, habíamos tenido que superar la fase preliminar de clasificación para la final, que consistió en el simple envío de una «cassette», ya fuese en forma de vídeo para los participantes en la modalidad de baile individual o por grupos, o en forma de cinta magnetofónica para los participantes en la modalidad de guitarra o cante, en la que debían aparecer grabados los diversos estilos o palos flamencos que, dependiendo de la modalidad bajo la que se presentaba(n) el(los) concursante(s), obligaban las bases del concurso, estableciendo diversos grupos de palos, a interpretar. El que ahora les escribe se incribió bajo la modalidad de «cante». Sin más que esa pobre selección a través de las cintas, el jurado competente declaró finalista a una serie de jóvenes, entre los que yo, agradecidamente, me encontraba. Si tuviese que hacer una valoración global del Fes-\n\ntival, sin duda empujaría la balanza ampliamente hacia el lado positivo. Fue meritorio, sin lugar a dudas, el comportamiento, por un lado, de la organización (Delegación Provincial de Asuntos Sociales de la Junta de Andalucía, Servicio de Juventud, en Huelva), que estuvo acertada en la mayoría de las ocasiones, dando como resultado la consecución de un espectáculo de un más que aceptable nivel artístico. Contribuyeron a dicho éxito, por un lado, claro está, mis propios compañeros y compañeras participantes, que ofrecieron al público todo el arte y todo el «bien hacer» que llevan dentro; por otro lado, también puso su gran grano de arena la renovada Peña Flamenca de Huelva, que ofreció su casa, sus grandes aficionados y su insuperable solera para que el evento tuviera lugar; por último, el público asistente, que fue capaz de dar su silencio, su atención inmutable y sus más calurosos aplausos a todos los que íbamos desfilando por el entarimado del bello escenario de la Peña de Huelva (incluidos artistas invitados). Sin embargo, y no con el afán de criticar desmesuradamente a las partes responsables, sino de hacer alusión a mi interés por que estos errores que citaré se solucionen para futuras celebraciones del Festival, hay\n\nalgunos puntos negativos que no voy a dejar de mencionar. En primer lugar, creo que la fase selectiva, y que conste que en mi caso estuvo a mi favor y no en contra mía, fue excesivamente parca como para poder reflejar fielmente las posibilidades de cada uno de los participantes de cara a su acceso a la fase final. Con esto quiero decir, que el procedimiento de las «cassettes» no me parece apropiado para la selección de finalistas en un concurso y más aún en un concurso de esta índole. Sí sería apropiado como fase preselectiva o bien si el número de participantes que pudieran llegar a la final fuese más elevado, pero no es éste el caso. Por otra parte, me veo en la obligación de aludir, una vez más, un hecho que se viene repitiendo casi a diario en la mayoría de los concursos de Flamenco que tienen lugar en el solar andaluz. Me estoy refiriendo, sin intención de caer en el tópico, a la «imparcialidad de los jurados». Es una imparcialidad que, sin lugar a dudas, brilla por su ausencia. Y fue esa injusticia, y no lo digo como víctima, puesto que mi clasificación me pareció la que correspondía al nivel que yo ofreció, sino como espectador y defensor de aquellos que «lo hicieron mejor» y no se vieron recompensados debidamente, la que desencadenó un sin fin de descontentos, disgustos y desagravios para con el jurado y la organización, que empañaron la cordial y agradable atmósfera que se había forzado hasta el momento. Es una pena que hasta en este tipo de acontecimientos ocurra esto.\n\nFrancisco Javier Veredas Navarro Antequera",
    "title": "Buzón Flamenco",
    "periodical": "candil",
    "issue_id": "1992-03",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "30-30",
    "page_number": 30,
    "word_count": 720,
    "article_char_count_full": 4305,
    "article_char_count_review": 4305,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-03-30-right-qu-date-con-el-cante",
    "article_text_for_review": "Programa Flamenco\n\nSintonícenos de lunes a viernes, de 20,30 a 22,00 horas; viernes, sábados y domingos de 0,30 a 3,00 horas, FLAMENCO",
    "title": "\"Quédate con el Cante\"",
    "periodical": "candil",
    "issue_id": "1992-03",
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
    "article_id": "1992-03-31-left-tocaores-de-hoy",
    "article_text_for_review": "Cepero, Paco. Nombre artístico de Francisco López-Cepero García. Jerez de la Frontera (Cádiz). 1942. Guitarrista. Sobrino nieto del cantaor José Cepero. Alterna su dedicación a la guitarra con la composición de canciones y cantes, principalmente para «El Chiquetete». Tiene una amplia discografía y entre otros los siguientes premios: Nacional de Guitarra Flamenca de la Cátedra de Flamencología de Jerez, Manolo de Huelva, de acompañamiento, del Concurso Nacional de Arte Flamenco de Córdoba y el Yunque de Oro de la Tertulia Flamenca de Ceuta. Su arte ha sido glosado por numerosos escritores y críticos flamencos, entre ellos por Manuel Ríos Ruiz, el cual manifiesta: «El compás tocaor de Paco Cepero, nacido del son más legítimo de su tierra jerezana, tiene una brillantez inusitada y un ritmo poco común, alcanzando cierto paroxismo musical casi laberíntico, algo personalísimo por intenso y clamoroso, que él resuelve gracias a un virtuosismo sorprendente, asombroso, producto de un consumado dominio de la técnica».\n\nPaco Cepero",
    "title": "TOCAORES DE HOY",
    "periodical": "candil",
    "issue_id": "1992-03",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "31-31",
    "page_number": 31,
    "word_count": 158,
    "article_char_count_full": 1035,
    "article_char_count_review": 1035,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-05-3-left-editorial",
    "article_text_for_review": "Editorial\n\nLígida entre quienes, convocados por la Peña Flamenca de Jaén y Revista Candil, intervinieron en aquel ya lejano encuentro de críticos de flamenco. Por primera vez, profesionales de la información y de la crítica flamenca, en torno a una misma mesa, reflexionaron conjuntamente sobre la trascendencia de su propia función de críticos, así como de la situación actual —entonces primavera de 1986— del flamenco. No hubo discursos corteses entre los intervinientes, sino cuestiones du- ramente controvertidas, no se produjeron loas recíprocas ni polémicas edulcoradas por el «fair play» al uso, sino juicios directos y sólidos, con independencia de aciertos o desaciertos. —¿Quién puede sentar cátedra sobre un arte tan difícil de objetivizar?—. En algo sí hubo ple- na coincidencia: en la necesidad de que tal reflexión conjunta se sistematizara periódicamente, por vía de constituir una asocia- ción que ordenase y precisara los debates y el sentido de las conclusiones, en su caso. La re- presentación de una provincia hermana asumió el compromi-\n\nso de organizar el siguiente reencuentro, un año después, pero por razones que desconocemos y que, en modo alguno, preten-demos ahora valorar, la esperada convocatoria no se produjo, frustrándose así la posibilidad de que un foro tan singular y lleno de especificidades, enriqueciera, en la divergencia de los puntos de vista allí expuestos, a quienes vivimos el flamenco como se vive un placer del alma o como se padece una enfermedad. Los colectivos, en otro tiempo, convocantes han decidido, creemos que con buen criterio, retomar la organización\n\nde un segundo encuentro de críticos flamencos. No sabemos los frutos que del mismo se derivarán y ni aun si la propia convocatoria resultará, hoy día, tan estimulante que determine la asistencia de un razonable número de participantes. Desde estas páginas queremos apelar al buen sentido y a la responsabilidad de aquellos sobre los que diariamente pesa como un cierto compromiso de limpiar, fijar y dar esplendor a este arte. La dignificación actual del flamenco se debe, en gran medida, al callado trabajo de personas, pasadas o presentes, que desde posiciones no siempre entendidas y remuneradas tuvieron el coraje de proscribir lo espúreo y subrayar lo genuino, aunque sus juicios, e incluso sus personas, fuesen descalificadas, sin fundamento alguno por la torpeza de los interesados o por la de sus lacayos. Creemos sinceramente que este segundo encuentro de críticos flamencos resultará, sin duda, muy positivo para redefinir el presente del flamenco, e indagar sobre posibles proyecciones del futuro. Nuestro deseo ferviente de que resulte todo un éxito.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 417,
    "article_char_count_full": 2672,
    "article_char_count_review": 2672,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-05-3-right-apuntes-sobre-la-sole-y-la-sigui",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE 1 paralelo principal pa- ra diferenciar la soleá del resto del cante grande, puede establecerse a costa de la siguiriya (por mutuos contrastes y por supuesta contemporaneidad de ori- gen).\n\nSalvo la soleá, la siguiriya y el resto del cante grande, jondo, son una unidad. Esta unidad está dada por la profundidad cruel o desgarrada, la visceralidad machuña en disparo. En otros términos: esos cantes no atacan los sentimientos vitales en el estrato más elevado y específico con que lo hace la soleá, sino los sentimientos más directamente sensitivos, los de mayor impresionabilidad externa, los propiamente instintivos y los de más arraigadas cenestesias en lo que a interioridad del hombre cabe. El hombre jondo tiene el poder de expresar artísticamente los elementos de la capa primigenia de lo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"hombre\"]\n\ncantes no atacan los sentimientos vitales en el estrato más elevado y específico con que lo hace la soleá, sino los sentimientos más directamente sensitivos, los de mayor impresionabilidad externa, los propiamente instintivos y los de más arraigadas cenestesias en lo que a interioridad del hombre cabe. El hombre jondo tiene el poder de expresar artísticamente los elementos de la capa primigenia de lo humano, en un matiz que ya es captable por el hombre civilizado. Un concepto que metafóricamente colorativo aclara ciertos lindes de lo jondo y lo ligero, y sobre todo de lo inequívocamente jondo con respecto a lo que ya tiende a enlazarse con lo liviano —que es el caso de la soleá— es el de distinguir entre lo «negro» y lo «claro». Para el flamenco de ley, lo «negro» simboliza definitivamente la idea de lo jondo absoluto, de lo jondo faraónico para el gitano, del duende para el bético. Lo «claro» es lo que distingue el mensaje de los cantes chicos: la superficie, la simplicidad. Lo negro, para el flamenco, es un concepto de entrega incondicional, indiferenciada, el paso audaz hacia lo caótico pasional, en fin, hacia lo total humano. Lo negro es una integración no ordenada sino acumulativa de la tensión total de la energía primigenia, no tamizada, no analítica, no desarrollada. Pero entre el concepto vívido de\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"compas\"]\n\nte significativa es la que se produce entre el vocabulario de los conceptos flamencos y el vocabulario de los sistemas existencialistas modernos. En el día a día más despuntado lenguaje filosófico, sobre todo en las corrientes existencialistas, se usa y ahonda el concepto de lo negro, referido sea a la vitalidad, a los humores y también a los topes metafísicos). Dando más vueltas: lo negro equivale a la Andalucía Alta, campera, hermética, poco acompasada, danzariamente cerrada. Lo claro equivale a la Andalucía Baja, marítima, acompasada y de bailes intermedios con inclinación abierta. El aficionado flamenco, cuando comienza a compenetrarse del mundo puesto en juego del cante grande, suele iniciarse cómoda y afinadamente con la soleá, por su accesibilidad sentimental, literaria, rítmica. Pero una vez iniciado, identificado ya con el sentimiento de lo grande, procura superar la soleá con el re\n\n[ENDING CONTEXT]\n\nLa siguiriya es invariablemente actualidad vital, no analítica sino desnudamente vivida. La soleá es la historia equidistante, tamizada, el afán sublimado. El fuego de su incendio se amortigua con graduada pirotecnia. Hasta puede cantarse bien con voz aguda. En un sentido más que acústico, la actitud expresiva de la siguiriya solicita gravedad de voz.\n\nLa soleá es de cierto modo el miraje de cima, en altitud, en posesión de sí mismo. La siguiriya, un plegamiento hacia abajo, que requiere en la garganta de los intérpretes partir de un rebuscamiento entrañable, ensimismado de su motor primero.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Apuntes sobre la soleá y la siguiriya / 3",
    "periodical": "candil",
    "issue_id": "1992-05",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-5",
    "page_number": 3,
    "word_count": 1780,
    "article_char_count_full": 11214,
    "article_char_count_review": 3923,
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
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "compas"
      }
    ]
  }
]
```
