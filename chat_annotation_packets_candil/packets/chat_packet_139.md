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
    "article_id": "1986-09-19-right-discograf-a-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nos flamencos no tenemos remedio.\n\nDe la maleable y encubridora masificación que nos asola han brotado en esta década unos artistas de cuarenta y ocho horas y pico, que pretenden «evolucionar» este arte a fuerza de impresionantes y escandalosas facultades, reconstruyendo en minutos lo que se tardó en gestar —al menos documentalmente— unos doscientos años. Por supuesto que mejorar lo ya hecho es tarea de genios. Pero lo que no llego a comprender es el mecenazgo partidista de algunos «flamencólogos solubles» que insisten una y otra vez, con influencias más que dudosas, en dar gloria y fama a «jóvenes valores» carentes de afición y conocimientos, para los que su principal y único objetivo es ganar dinero (cosa lícita dentro de un orden), y de «dobrarla», ni hablar.\n\nMe explico. No existe\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\ne dudosas, en dar gloria y fama a «jóvenes valores» carentes de afición y conocimientos, para los que su principal y único objetivo es ganar dinero (cosa lícita dentro de un orden), y de «dobrarla», ni hablar. Me explico. No existe reunión, tertulia radiofónica o diálogo tabernario donde no salga a colación el consabido problema de la discografía flamenca. La mayoría, y así lo observo, se decanta por las impresiones discográficas, y cuantas más mejor, de los «nuevos valores» (entiéndase por tales a un ramillete de cantaores con algunos premios irrelevantes en localidades de nulo peso específico, más folklóricas que cantaoras, y apoyados por quienes desconocen la todavía joven edad media de los consagrados y la mucha cuerda que aún les queda). Los menos, sin dejar de apostar fuerte, pero con las lógicas reservas por el lanzamiento de algunos de estos «no consagrados», pedimos encarecidamente la reedición de reliquias magistrales correspondientes a la década de los cincuenta, sesenta y setenta, respectivamente, así como la puesta en curso de los atractivos documentos sonoros que nos aportan las placas antiguas y cilindros fonográficos. Pensamos, sinceramente, que con la segunda opción sacíamos las apetencias de los aficionados, la demanda imperante de pureza cantaora e ilustramos a los que, con excesivas prisas y gargantas desabridas, quieren llegar a ser alguien en este alocado mundo. En resumen, primero conocer, ahondar en lo legado; después aprender (gesta nunca acabada) y, posteriormente, proyectar su personalidad en la ejecución y enseñar. Claro que reflexionar sobre esto es como predicar en el desierto y sin sombrero. Porque, como en flamenco todo es opinable y suje-to a crítica, si usted com\n\n[ENDING CONTEXT]\n\nlentas— con cierta ligazón en los tercios que recuerda en algunos momentos las interpretaciones de las viejas figuras. Buen gusto y melodiosidad en el desarrollo de la Malagueña rematada con verdiales.\n\nAires de el Piyayo es el comienzo de la B, con cierto eco comercial tras dejar los propios ecos del cantaor malagueño y entonar —en la misma grabación— los Tangos de Málaga. En las Alegrías rinde homenaje a las figuras de la Tacita de Plata a la vez que deja constancia del compás. Facultades en los fandangos de Huelva y deja para el final las siguiriyas a las que le imprime quejío y fuerza.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 1593,
    "article_char_count_full": 9621,
    "article_char_count_review": 3345,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      }
    ]
  },
  {
    "article_id": "1986-09-22-left-buz-n-y-noticiario-flamenco",
    "article_text_for_review": "stimados amigos. Por segun-\n\n¡Cuánto saben algunos copian- do literalmente a los demás!\n\nda vez —que yo sepa— he sido plagiado.\n\nEsta vez lo he sido por algún... aficionado residente en Cataluña, de forma cobarde al omitir su nombre y modificar el título que impuse a mi trabajo: «¿De qué enfermedad mueren nuestros artistas?» (Candil, número 29). Que sepa, si llega a leer este escritoprotesta, que ante mí y ante cualquier persona honrada, se ha manifiestado como un consumado «plagiario» falto de escrúpulos y de responsabilidad.\n\nSeñores, ya estoy harto de estas acciones impropias entre personas «cabales» y civilizadas. No debí callar ante el «caso» anterior (un crítico de Madrid), conociendo nuestras leyes civiles, ni he debido comportarme igual en este caso. He debido acudir con Letrado ante los tribunales ordinarios pidiendo la aplicación del artículo 534 (1) del Código Penal, que es lo que merecen con su conducta. En fin, esperemos que estos casos no se repitan hacia otros escritores, porque lo que es hacia mí no volverán a hacerlo. Por sí acaso, voy a dejar de colaborar en las revistas flamencas y me evitaré más disgustos. Bien sabe Dios que soy incapaz de hacer mal a nadie, aunque se lo merezca.\n\nNo estaría mal que el señor di-rector de esta revista dedicara, con su preclara pluma, un editorial a los «plagiarios» para que despier-ten, si es que están dormidos, y se-pan el delito que cometen.\n\nTermino. Pensé publicar el resultado de mis investigaciones sobre la vida privada y artística del gran Silverio, pero hoy no pienso igual. Lo siento. No publicaré nada.\n\n(1) El mencionado artículo 534 del Código Penal dice, en síntesis, que el plagio está penodo con arresto mayor y multa Un cordial saludo. Manuel Yerga Lancharro\n\nde hasta cuatrocientas mil pesetas. (Si el plagio es para lanzar un libro, éste es más grave económicamente).\n\nNOTA:\n\nLa Asesoría de Actividades Flamencas de la Consejería de Cultura nos informa que su propósito de confeccionar un Censo de Cantaores Profesionales y Semiprofesionales y Aspirantes, se está viendo dificultado en algunas provincias por la torcida y malintencionada actitud de algunos irresponsables que han hecho correr el rumor de que este propósito encubre fines fiscales en perjuicio (?) de los interesados.\n\nEs muy lamentable que a estas alturas el Flamenco pueda ser mediatizado y su difusión y conocimiento a través de sus intérpretes perjudicada por estos residuos de épocas desfasadas que, al parecer, pretenden que nuestro arte más importante continúe escondido en las cavernas y marginado de la realidad social de un país en plena evolución y desarrollo.\n\nA estos animadores podrán de- ber los artistas del Cante y una abnegada pléyade que lucha por su consagración, el que sus nom- bres permanezcan escondidos, co- mo si luego no necesitaran ser anunciados en grandes caracteres tal que verdaderos profesionales que son los unos y aspiran a ser reconocidos y conocidos los más.\n\nLa Asesoría enviará modelos de fichas a cuantos interesados y/o peñas lo soliciten.",
    "title": "Buzón y Noticiario Flamenco",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 495,
    "article_char_count_full": 3040,
    "article_char_count_review": 3040,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-09-22-right-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOrganizado por la Peña Flamenca de Elche (Alicante), ha tenido lugar el IX Festival-Concurso de Cante Jondo «Zapato de Oro», en el que han intervenido: Itoly de los Palacios, Luis Heredia «El Polaco», Gabriel Moreno, Juana la del Revuelo, Tina Pavón, José de la Tomasa y José Mercé, acompañados a la guitarra por Miguel Ochando, Martín Revuelo y Antonio Ruz.\n\nReunido el jurado dio ganador por mayoría al cantaor jerezano José Mercé.\n\nEl trofeo consiste en un «Zapato de Oro» valorado en 250.000 pesetas.\n\nNuestra más cordial enhorabuenal al gran amigo José Mercé y a la Peña Flamenca de Elche por el éxito del Festival.\n\nEn asamblea general celebrada por la Peña Flamenca «Rincón del Cante» de Córdoba, resultó elegida nueva Junta Directiva, quedando la misma compuesta de la siguiente\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"segundo\"]\n\nl trofeo consiste en un «Zapato de Oro» valorado en 250.000 pesetas. Nuestra más cordial enhorabuenal al gran amigo José Mercé y a la Peña Flamenca de Elche por el éxito del Festival. En asamblea general celebrada por la Peña Flamenca «Rincón del Cante» de Córdoba, resultó elegida nueva Junta Directiva, quedando la misma compuesta de la siguiente manera: Presidente, Miguel López Fernández. Vicepresidente, José M. a Salor Solís. Vicepresidente segundo, Juan Urbaneja Diéguez. Adjunto al presidente, José García Rodríguez. Secretario, José L. Otero Nieto. Vicesecretario, Bernardo Mesa García. Tesorero, Manuel Nogueras Barrientos. Vicetesorero, Francisco Gil Arjona. Relaciones públicas, Antonio Ruz Espinosa. Vocales, José Castellano Asensio, José Salinas Martin, Jorge Gil Arjona y Rafael Delgado Centella. Deseamos toda clase de éxitos a la nueva ejecutiva. Tras la asamblea general anual celebrada por la Peña Flamenca «Niña de la Puebla» de Santa Coloma de Gramanet (Barcelona), resultó elegida nueva Junta Directiva, la cual quedó compuesta por los siguientes nombres: Presidente, Antonio Moreno Leiva. Vicepresidente, Diego Ri\n\n[EVIDENCE WINDOW 2 | retrieval_hint=AUTH_04 | trigger=\"comerciales\"]\n\nforma que detallamos: Presidente, José A. García Dobao. Vicepresidente, Antonio Iniesta Noguera. Secretario, José Sánchez Quiles. Tesorero, José Esclapez Mirállez. Vocales, Francisco García Manrique, Juan Benítez, Antonio Rus Rueda, Antonio Cortés Horca y Andrés Gallardo Torrente. Deseamos muchos éxitos a la nueva Junta Directiva. Organizado por la Peña Flamenca «Los Cernícalos» de Jerez (Cádiz) y patrocinado por distintas entidades y firmas comerciales, se ha celebrado en la ciudad jerezana el XV Certamen Nacional de Guitarra Flamenca con el siguiente resultado: Primer premio: 250.000 pesetas y Trofeo Excmo. Ayuntamiento de Jerez, para Manuel Moreno Junquera, de Jerez. Segundo premio: 100.000 pesetas y Trofeo Excma. Diputación Provincial, para Pedro Peña Dorante, de Lebrija. Tercer premio: 25.000 pesetas. Trofeo Peña «Los Cernícalos y guitarra de artesanía, para José Luis Rodríguez García,\n\n[ENDING CONTEXT]\n\nde Plata.\n\n2.º Isabel Navarrete: 20.000 pesetas y Anfora de Plata.\n\n3.º Juan José Rosado: 10.000 pesetas y Anfora de Plata.\n\nInició el espectáculo el cuadro de Pilar Muñoz, cerrando como artista invitado José Martínez «El Bolo», acompañado por el joven guitarrista Curro Terrón, acompañando también a los distintos participantes.\n\nEste primer concurso Ciudad de Melilla que pretende ser la base de otros incluso a nivel nacional, fue patrocinado por la Fundación Municipal Socio-Cultural, organizado por Radio Melilla de la Cadena SER y con la colaboración especial de la Peña Flamenca de Melilla.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1087,
    "article_char_count_full": 7157,
    "article_char_count_review": 3726,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "segundo"
      },
      {
        "window": 2,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "comerciales"
      }
    ]
  },
  {
    "article_id": "1986-09-24-left-placas",
    "article_text_for_review": "Discografía (Placas)\n\nPor: Manuel Yerga",
    "title": "Discografía Placas",
    "periodical": "candil",
    "issue_id": "1986-09",
    "year": 1986,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 5,
    "article_char_count_full": 39,
    "article_char_count_review": 39,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1986-11-3-right-editorial",
    "article_text_for_review": "Desde luego la cosa nos parece clara. El flamenco es andaluz por los cuatro costados. El viejo crisol de esencias artísticas que es la región andaluza, ha venido seleccionando desde tiempos remotos, cuyos vértices cronológicos se pierden en el más allá de la historia, lo mejor y más quintaesenciado de las manifestaciones culturales de los numerosos pueblos que en ella han convivido: desde los oscuros orígenes de la mítica Tartessos, hasta las correcciones civilizadoras introducidas por numerosas etnias, comunidades humanas y culturas diferentes que se aclimataron en nuestras tierras solares, todas y cada una de ellas han aportado su granito de arena para la construcción de esta catedral, de fina sensibilidad y atormentado temperamento, que llamamos arte flamenco. Entre todas, parece lógico destacar las influencias musicales y poéticas procedentes del mundo árabe, que no en vano fueron andaluces durante ocho siglos, creando aquí la que sería la más floreciente civilización de la entonces culturalmente desabastecida comunidad de pueblos europeos, y, ¿cómo no?: la llegada de los gitanos a España desde el siglo XV constituye la mejor aportación posible a la génesis y desarrollo de nuestro arte andaluz, que si bién estaba ya inmerso en ese bullir cultural embrionario que describíamos, con su aclimatación entre nosotros, contribuyeron de manera decisiva a su grandeza.\n\nHasta aquí todos los acuerdos: el flamenco es andaluz en su origen. Pero, isolamente andaluz en nuestros días? Afirmamos rotundamente que no. Ahora que tanto se habla de concesiones de títulos tan rimbombantes como Patrimonios de la Humanidad, aplicados a ciertas manifestaciones culturales, nos atreveríamos a decir que hoy en día el flamenco pertenece a todos los caminos hollados por el arte. Y no hablamos sólo de su extensión americana (Cantes de Ida y Vuelta) o de sus brazos largos y jondísimos que igual penetran las regiones manchegas, catalanas, extremeñas o levantinas; habría también que referirse al entusiasmo generalizado en Europa; a los núcleos franceses de estudiosos de prestigio que en Toulouse, Perpignan, Besancon, etc., sienten el flamenco como un patrimonio espiritual compartido. También a esos flamencos en el exilio, que pueblan de quejos y añoranzas jondas las noches hóstiles de Alemania, Suiza (¿cómo olvidar la fidelidad de nuestros amigos de Winterturz), etc.\n\nNo. No podemos ni debemos seguir considerando, hoy, algo exclusivamente andaluz al flamenco. Eso sí, se trata de un precioso regalo que nuestra comunidad del Sur ha legado a todo el mundo. Con su protesta entrecortada, con su capacidad para el llanto y la alegría, la soledad o el entusiasmo compartido, el flamenco es ya un rodrigón fortísimo en el que se apoya un sector muy grande de la sensibilidad universal.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1986-11",
    "year": 1986,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 436,
    "article_char_count_full": 2792,
    "article_char_count_review": 2792,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
