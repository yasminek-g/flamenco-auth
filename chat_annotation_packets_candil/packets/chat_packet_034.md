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
    "article_id": "1981-09-20-left-el-misterioso-final-del-programa",
    "article_text_for_review": "por C. Ramos Pellicer\n\nSabedores por nuestra parte de la próxima finalización de la serie, comentemos, por otro lado, que el aficionado, el simple televidente y no digamos los «cabales», han puesto el grito en el cielo, con toda razón, por el vituperable trato dado al programa «Flamenco», de la Segunda Cadena, en esta su última etapa.\n\nYa sabemos que en Prado del Rey reinan muchos desmadres y que TVE no es lo que debería ser. Pero de ahí a que, de la noche a la mañana, desaparezca de sus informaciones semanales un programa (y no es el único) que ganó en justicia el Premio Nacional de Musicales RTVE y que ha sabido mantenerse en la misma línea de calidad y de generalizada respetabilidad pública, al que la crítica de toda España ha tratado siempre con unánime aprobación, en el que no había señas de decaimiento o «quema-zón», y que contrapesa en parte tanta extranjerización como la que nos hecha encima a diario la pequeña pantalla, eso ya pasa de castaño oscuro. Las quejas son muchas, y es natural que lo sean.\n\nConocemos de buena tinta que la realización de esta última serie de ‘Flamenco’, ya se vió envuelta en demoras, atrasos, encarecimientos económicos, desórdenes y apatías, o bien evitables o bien totalmente injustificados, aunque consiguió mantener el tipo en cuanto a su contenido y grabación, sostenidos a duras penas por el esfuerzo del programador Pablo Rodríguez, el último realizador, Lisardo García, el seleccionador de artistas y asesor Miguel Espín, y el presentador y guionista Fernando Quinones. Pero he aquí que un buen día, ‘Flamenco’ desaparece sin aviso de las listas semanales anunciadoras de las programaciones; que lo cambian de día sin aviso; que, de pronto, no se da; que vuelven a anunciarlo una semana para escamotearlo de nuevo la siguiente.....Nadie, de entre sus muchos seguidores (y no es porque lo digamos desde la revista del «ramo»), supo ni sabe a qué atenerse.\n\nSeguidores no sólo aficionados y no sólo adaluces, aclaremos. Porque «Flamenco» había conquistado y seguía conquistando público para el flamenco en todo el territorio español, o creando para este arte unánimes y merecidas difusión y respetabilidad. Así que continuamos no entendiendo, aunque sí reprobando, por todas las irregularidades y desatenciones señaladas, el trato residual dado a ese espacio. Y no lo decimos ya como puros aficionados, sino también como sencillos usuarios de «la cajita mágica».",
    "title": "Desmadres TVE.-El misterioso final del programa «flamenco»",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 398,
    "article_char_count_full": 2419,
    "article_char_count_review": 2419,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-09-20-right-el-centenario-de-george-borrow",
    "article_text_for_review": "El pasado 26 de julio se cumplía el centenario de la muerte de George Henry Borrow, don Jorgito el inglés, autor, entre otros títulos, de «La Bíblia en España» y «Los Zincali», dos piezas singulares e imprescindibles en la cuantiosa bibliografía que forman los relatos de viajeros extranjeros por la España de los siglos XVIII y XIX, que merecieron las detracciones de Marcelino Menéndez y Pelayo y el panegírico de Manuel Azaña, entre otras cosas, su traductor de lujo y quien señalase las influencias de Serafín Estébanez Calderón, El Solitario, en el narrador inglés.\n\nSi importancia capital tienen estas crónicas viajeras por la España de la regencia de María Cristina, no es menor la que arrojan por sus datos sobre el cante flamenco y, sobre todo, de los gitanos españoles. Lástima que, por lo que he podido apreciar, esta fecha pasará prácticamente desapercibida y en unos momentos cruciales, a mi juicio, en los que se impone la revisión de criterios y, sobre todo, de lecturas de temas flamencos; pues, si bien, en el caso de estos libros, están escritos con un lenguaje y un estilo fresquisimos, repletos de figuras populares trazadas con rasgos firmes y escuetos a las que el autor demuestra su simpatía - sobre todo si son exponentes de las clases oprimidas, de los grupos marginados- desbordados de noticias, etc., etc., que les hacen insustituibles, va siendo hora de tener en cuenta aquello que es realidad y, sobre todo, lo que es producto de ficción. Con dema-\n\nsiada frecuencia se olvida en el ámbito flamenco que, por encima de todo, Borrow era un novelista y no una máquina de retratar costumbres. Hasta su propia autografía, «Lavengro», tiene más de imaginación y fantasía que de realidad; algo que, por cierto, no le perdonarían sus contemporáneos y cuya incomprensión, le convertiría en un misántropo de Oulton Hall, apedreado e insultado por los muchachos del lugar, cuya única satisfacción consistía en invitar, de cuando en cuando, a alguna banda de gitanos para que acampasen en sus jardines, ante el horror de sus circunspectos vecinos.\n\nPor lo que hace a nuestro país, y advirtiendo que todavía existe más de un flamenólogo que le cita de refrito pese a las excelentes y asequibles traduciciones, parece ser, predominan quienes utilizan sus textos como dogma para luego hacer estudios a la brasa, es décir, arrimándole el ascua a su sardina. No vamos a ser nosotros quienes, nuevamente, incidamos sobre el legódo flamenco que contienen los libros de las andanzas españolas del mítico vendedor de Bíblias - ya adelantamos nuestro parecer en un extenso ensayo publicado en el n.° 9 de «Candil» y en el que, de seguro, debería haber arota-do un número mucho mayor de las siguiriyas recogidas por don Jorgito - y que ya fuera estudia-do con anterioridad por Arcadio Larrea en «El flamenco en su raíz». Nuestro objetivo es otro, dejar constancia del centenario de la muerte de Borrow para que se lean sus libros, los que, por cierto, continuan vivos y vigentes.\n\nManuel Urbano",
    "title": "El centenario de George Borrow",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 502,
    "article_char_count_full": 3000,
    "article_char_count_review": 3000,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-09-21-left-las-letras-flamencas-de-pe-pe-cr",
    "article_text_for_review": "S I alguien pone en duda que la letra flamen- ca no nace fresca y jugosísima, repleta de vida, de la más cercana realidad por auténticos poetas populares, las que siguen destrenzarán esa sospecha. Son letras aladas de las purísimas e irrenunciables vivencias jondas de Pepe Cruz, na- cidas entre las grietas del alma, desbordadas en el chorro cálido de su voz mineral y unidas a los más puros cantes; unas letras que han adoptado, cantándolas, Rosario López y Carlos Cruz o, por señalar a artistas de distinta geografía, la Fer- nanda y Pedro Peña.\n\nTu te mueres, yo me muero; toma sangre de mis venas, que yo me muera primero.\n\nEn esta esquina me paro a ver si viene la muerte y me coge de la mano.\n\nPonme la mano en el costao y mira que puñala, por quererte, a mí me han dao.\n\nEl río Guadalquivir está preso en el pantano, llora por salir de allí.\n\nEn mi garganta un quejío y en la guitarra las notas del pobre corazón mío.\n\nDejadme por los rincones, dejadme a mi llorar, que es mi pena muy chica y a naide le importa ná.\n\nSi me despierto llorando, a qué sueño yo contigo, eres mi vía y mi muerte, eres mi cruz y mi castigo.\n\nAguardientillo barato yo bebo de madrugá, los gallos cantan al alba cuando me voy a acostar.\n\nBendita sea la tierra que a mí me ha dao la luz, tengo raíces de sangre, yo soy del pueblo andaluz.\n\nQue no quiero ser minero, a la mina yo no voy, se gana poco dinero par trabajito que doy y me pué matar un barreno.",
    "title": "LAS LETRAS FLAMENCAS DE PE PE CR U Z",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "21-21",
    "page_number": 21,
    "word_count": 276,
    "article_char_count_full": 1438,
    "article_char_count_review": 1438,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-09-21-right-algunas-rese-as-de-la-m-s-nueva-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAlgunas reseñas de la más nueva Bibliografía Flamenca\n\nS INCERAMENTE, uno an- da en difícil equilibrio de decisión estimativa —en veleto, como decimos aquí— ante la ex- tensa bibliografía flamenca y para-flamenca que se viene su- cediendo de forma casi torren- cial. De veras, no sé a qué palo quedarme en esta inundación de letra impresa. Si ante la alegría que produce ese alto número de publicaciones, por cuanto de- notan la existencia de un merca- do ámplio y un público deseoso de información, o, por lo contrario, en ese intermedio del asco que origina la amargura y la defraudación ante no pocas basuras retóricas y páginas de refrito que, por no ser benefi- ciosas no lo son ni para el autor que las firma. En este terremoto literario-flamenco me permito entrever mucho de moda, más de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"entendido\"]\n\ntencia de un merca- do ámplio y un público deseoso de información, o, por lo contrario, en ese intermedio del asco que origina la amargura y la defraudación ante no pocas basuras retóricas y páginas de refrito que, por no ser benefi- ciosas no lo son ni para el autor que las firma. En este terremoto literario-flamenco me permito entrever mucho de moda, más de snobismo y no poco de una especie garrula, alienante, de re- gionalismo farandulero mal entendido. Y no digamos nada de esa búsqueda de raíces desde la copa del árbol, que tanto im- pide ver el bosque, por más de uno que hace años cayera del nido. Si me apuran en este fluir constante de lecturas, he de confesar que, por lo general, encuentro mayores datos de interés e inéditos en el repaso de una edición crítica del Romancero de Lorca que ante todo un tratado monográfico de Sevilla-nas; o más clima y latires jondos en un libro de poemas que en todo un tratado con pretensiones etnológicas. Digámoslo de una vez, así las cosas, a lo más que llegaremos es a hacer montón y no a construir el edificio necesario, que contenga este bien inalienable del pueblo andaluz con profundidad y rigor. De aquí que, una vez más, insista en la necesidad de un estudio serio, conjunto, desapasionado y sin prisas —prisa la tienen sólo el enfermo y el ambicioso, como denunciara Ortega— por todo un ámplio equipo de especialistas: historiadores, sociólogos, músicos, etnólogos, etc. etc. entre los que entrarían, a lo sumo y si mucho me aprietan, media docena de esa rara avis que llaman flamencólogos. Mientras no contemos con una mínima bibliog\n\n[ENDING CONTEXT]\n\nlo que nos trae datos de interés, junto a otras de los copleros de hoy mismo.\n\nFinalmente, dejemos constancia de que el libro plantea y, a mi parecer, no resuelve, el controvertido tema de las actuales sevillanas; las que en su estructura literaria y musical son ya otra cosa. Un movimiento que, con indudable interés, iniciaran los Toronjo y que hoy ronda el desmadre por culpa de la moda, el popularismo regional mal entendido y, claro, los intereses económicos. Todo ello adobado con que, por lo general, los artistas (?) buscan la fama y no la popularidad, cosas bien distintas.\n\nMANUEL URBANO\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel.-Algunas reseñas de la más nueva Bibliografía Flamenca",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1513,
    "article_char_count_full": 9156,
    "article_char_count_review": 3221,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "entendido"
      }
    ]
  },
  {
    "article_id": "1981-09-23-left-flamenca",
    "article_text_for_review": "Disco: VEREAS NEGRAS\n\nCanta: JOSE «EL DE LA TOMASA»\n\nGuitarra: Pedro BACAN\n\nDiscos BELTER, S. A. Rfa. 2-37.019/81\n\nDesde Sevilla al mundo del disco, por vereas sonoramente pintadas de negro, llega hasta el aficionado, el decir de José «El de la Tomasa». Y, en las primeras estрыas de este nuevo disco, canta, fandangos -evoca «al Niño de la Calzá», para luego continuar con Rondeñas, Alegrías, Aires de Huelva, Tarantos, Granainas, Cartageneras, Serranas y Bulerías por Soleá. Como soporte, fiel exponente de profesionalidad y dominio de la guitarra, Pedro Bacán.\n\nA través de una atenta audición nos encotramos a un José «El de la Tomasa» en una interesante joven-madurez que él entrega al oyente con muy buena voz, amplia en sonoridad, rica en tonos y dosificada para mandar en el cante. Diriamos que, con seriedad y galanura, va al cante el hijo de «la Tomasa» y «pies de plomo». Matizar, no obstante, que hay cantes donde la cadencia melódica, en continuo itinerario es básica y esencial. Y, en ocasiones, José «El de la Tomasa» entrega, en los primeros tercios, todo el empujón de conocimiento y arte que tiene, para luego, la voz ir perdiendo musicalidad, ritmo y, en cosecuencia, belleza interpretativa. Y lo decimos porque en este disco, debido a los estilos que contiene, así lo deja entrever. Nos gustaría pues, que intentara, en futuras empresas discográficas, redondear la faena —permitasenos el simil taurino -, ahondando y fijando en los contenidos artísticos.\n\nY aparte matizaciones, nosotros, que creemos en José «El de la Tomasa», nos adentramos con él - invitamos a todos los aficionados - por estas «Vereas negras».\n\nDOSCANDIL",
    "title": "Discografía flamenca",
    "periodical": "candil",
    "issue_id": "1981-09",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 269,
    "article_char_count_full": 1645,
    "article_char_count_review": 1645,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
