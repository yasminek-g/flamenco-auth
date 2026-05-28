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
    "article_id": "1984-09-4-left-estudio-historico-literario-del-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE L por qué hemos elegido como tema de estudio el cante por Serranas es algo que en sí no necesita justificación. Todo buen aficionado al flamenco, o el simple enamorado de la poesía popular, conoce la calidad poética y musicológica de este cante sin igual, almizclado con olor a monte, lleno de esencias andalucísimas y en cuyas letras se desgrana todo el sentimiento de la Andalucía campesina y labradora. Este estilo de cante, tan próximo a otras áreas del folklore no estrictamente flamenco, tiene una larga tradición literaria y cultural en nuestra historia peninsular, que se ha dado en llamar genéricamente «literatura pastoril», y que, aunque ancla sus raíces en la más rancia tradición greco-latina, es en nuestra literatura y en nuestro folklore (sobre todo en las letras del cante\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_01 | trigger=\"verdadera\"]\n\nla Andalucía campesina y labradora. Este estilo de cante, tan próximo a otras áreas del folklore no estrictamente flamenco, tiene una larga tradición literaria y cultural en nuestra historia peninsular, que se ha dado en llamar genéricamente «literatura pastoril», y que, aunque ancla sus raíces en la más rancia tradición greco-latina, es en nuestra literatura y en nuestro folklore (sobre todo en las letras del cante flamenco) donde ha adquirido verdadera carta de naturaleza. Sin embargo, la práctica cotidiana del cante jondo actual, parece alejarse de esas raíces nutricias de la Serrana. Dificilmente los intérpretes de hoy suelen ejecutar ese cante de tan ensolerada textura. Parece como si rehuyeran sus dificultades, sus esquinas angustiosas o, tal vez, ¡quién sabe!, sean sus letras dulzonas, en ocasiones empalagosas para el gusto presente, las que lleven a los artistas a no prodigar demasiado esos cantes y al público a no solicitar excesivamente su ejecución. Nosotros, convencidos de la belleza y arraigo jondo de estos cantes, vamos a aproximarnos a su historia, y a sus contenidos literarios y flamencos, convencidos de que prestamos un gran servicio a los verdaderos degustadores de nuestro siempre problemático arte andaluz. Para ello, vamos a estructurar nuestro trabajo en tres grandes apartados que serán los siguientes: 1) la tradición culta de la Serrana.\n\n[ENDING CONTEXT]\n\nha tenido a lo largo de su evolución y desarrollo, como lo demuestra el hecho de que las propias letras que se interpretan por este estilo, hagan, en ocasiones, alusiones diversas, más o menos afortunadas, al posible parentesco de la Serrana con el resto del tronco cantaor:\n\nAy, mi serrana,\n\nsi tú supieras\n\nque eres primica hermana\n\nQueremos advertir que citaremos siempre por la edición que en cada caso hayamos manejado, independientemente del año de su publicación.\n\nde Petenera.\n\nVELEZ, Julio, Flamenco, una aproximación crítica. Madrid, 1976.\n\nZUGASTI, Julián, El bandolerismo. Madrid, 1982.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ESTUDIO HISTORICO-LITERARIO DEL CANTE POR SERRANAS",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "4-10",
    "page_number": 4,
    "word_count": 7591,
    "article_char_count_full": 45724,
    "article_char_count_review": 3003,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_01",
        "family": "AUTH",
        "trigger": "verdadera"
      }
    ]
  },
  {
    "article_id": "1984-09-11-left-introduccion-a-la-discografia-an",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Antonio Reina Gómez\n\nRESUMEN HISTORICO\n\nDesde al año 1854 en que Scott de Martinville inicia la investigación, para grabar los sonidos del aire, hasta 1953, en que aparece el disco de microsurco, transcurre una etapa de cien años, que se caracteriza por descubrimientos, avances y perfeccionamiento de la fonografía.\n\nLos hechos más importantes que jalonan esta época son los siguientes:\n\n12 de agosto de 1877, en que Eddison inventa un registrador sonoro, que bautiza con el nombre de phonógrafo.\n\n16 de mayo de 1888, es decir, once años más tarde, en que Emilio Berliner muestra su gramófono ante el Instituto Franklin de Filadelfia.\n\nUn año después, el 24 de mayo de 1889, en que Eddison comienza la producción de cilindros ya grabados, para reproducirlos en fonógrafos transformados en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"reproductoras\"]\n\njalonan esta época son los siguientes: 12 de agosto de 1877, en que Eddison inventa un registrador sonoro, que bautiza con el nombre de phonógrafo. 16 de mayo de 1888, es decir, once años más tarde, en que Emilio Berliner muestra su gramófono ante el Instituto Franklin de Filadelfia. Un año después, el 24 de mayo de 1889, en que Eddison comienza la producción de cilindros ya grabados, para reproducirlos en fonógrafos transformados en máquinas reproductoras. Es decir, se inicia así la comercialización de grabaciones, para ser reproducidas en fonógrafos previamente preparados. En 1891, se publica el primer catálogo de cilindros grabados por la casa Columbia y a principios de 1892 Bettini fabrica también sus primeros cilindros grabados. Así se llega hasta 1893, en que se fabrica en EE.UU. el primer disco de pizarra. Es de siete pulgadas de diámetro y se obtiene por estampación sobre goma dura. En Europa, la aparición del disco de pizarra, tiene lugar a partir de 1900. Es grabado primero por una sola cara y a partir de 1904 por las dos,\n\n[ENDING CONTEXT]\n\nabsorbido por el fandango, que alcanza su cénit en la década de los cuarenta, en la que se erige, elemento fundamental e imprescindible de todas las grabaciones.\n\nLa lucha entre la calidad y la comercialidad aún continúa en pie en nuestros días y, como es natural, queda recogida perfectamente a través de la discografía.\n\nEste es a grandes rasgos el valor y el significado de la ponencia que nos ocupa y aunque en más de una ocasión me han calificado de necrófilo, considero que es una de las fuentes en la que los estudiosos, los entusiastas y los profesionales del cante, tenemos que beber.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "INTRODUCCION A LA DISCOGRAFIA ANTIGUA (Ponencia presentada al XII Congreso",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 2106,
    "article_char_count_full": 12489,
    "article_char_count_review": 2671,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "reproductoras"
      }
    ]
  },
  {
    "article_id": "1984-09-12-right-la-llave-de-oro-del-flamenco-y-e",
    "article_text_for_review": "HORA que se habla, y es lógico, pero cada vez menos, de la obtención de ese galardón máximo que es la IV LLAVE DE ORO DEL CANTE FLAMENCO, que para unos no tenía que hablarse del tema, y que para otros la concesión de la misma supone un hecho inevitable de continuidad histórica en el flamenco, se nos ocurre relacionar LA LLAVE DE ORO DEL FLAMENCO con EL CANTE POR SAETAS.\n\nEn la oportunidad del momento de conceder la LLAVE DE ORO DEL CANTE, el tiempo dirá como en todo. Sin embargo, cuando llegue el momento, oportuno para unos e inoportuno para otros, de conceder el preciado galardón, como es natural, la elección recaerá sobre un cantaor que domine una amplia gama de estilos o palos flamencos; tendría que ser el cantaor más completo posible sobre el papel. Esto se ha repetido en las dos ediciones anteriores que conocemos: Manuel Vallejo y Antonio Mairena. Precisamente, fueron dos extraordinarios saeteros; dos mundos diferentes en la saeta y, cómo no, en el cante. Como son los genios en el arte, diferentes. Como son diferentes todas las figuras del cante flamenco; cada cual tiene su impronta, su metal y su medida. Manuel Vallejo, valentía indescripible en los tonos:\n\n$ Ay... $\n\nPor Ricardo Rodríguez Cosano\n\nAhí, presente,\n\nAhí, presente lo tenéis.\n\nAy, ay, ay...\n\nAntonio Mairena, flamencura en el eco seguiriyero: Av.\n\n¿Por qué razón y por qué ley,\n\npor qué ley y por qué razón?\n\nAy, ay, ay...\n\nDe esta manera, el cantaor que consiga la LLAVE, a nuestro juicio, debería cantar saetas como las cantaron Vallejo y Mairena. Con ello no queremos decir que las cantaran como los mencionados maestros del flamenco, sino que el cantaor aspirante hubiese cantado bien por saetas a lo largo de su carrera artística.\n\nEn el cante por saetas, cuando se hace en directo, hay pocos trucos; sólo hay una verdad desnuda: cantar. En un cuarto, no hay necesidad de cantar a pleno pulmón, ya que se encuentran los cabales en intimidad y cuatro paredes para que nada se escape; es el escenario ideal (nos referimos a un grupo reducido de personas); aquí se suele\n\ncantar a media voz. En los festivales, la voz, al ser ampliada por la técnica, el cantaor juega con una ventajosa posición frente a su audiencia. Sin embargo, en el cante por saetas, generalmente, no hay cuatro paredes, y cuando las hay, se tiene por techo el cielo. De todas maneras, el cante por saetas supone para el cantaor un auténtico calvario, ya que el silencio de la calle casi nunca es perfecto. Además, la saeta hay que hacerla de un tirón; no hay tiempo para descansar y al final de la oración hay que subir un empinado tercio (desde los últimos ayes al término de la saeta). Se puede decir, sin temor a equivocarnos, que este cantar supone el riesgo de contener un grito durante el tiempo que dure la oración comunitaria de todos los oyentes (una saeta de Vallejo: 1 minuto y 25 segundos; una saeta de Antonio Mairena: 2 minutos y 20 segundos).\n\nPor todo ello, se podría exigir, creemos que no es pedir mucho, a los cantaores que opten a la IV LLAVE DE ORO DEL CANTE FLAMENCO que canten bien por saetas.",
    "title": "LA LLAVE DE ORO DEL FLAMENCO Y EL CANTE POR SAETAS",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 546,
    "article_char_count_full": 3078,
    "article_char_count_review": 3078,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-09-13-left-homenaje-a-rafael-romero",
    "article_text_for_review": "CASETA DEL CONDESTABLE\n\nSábado, día 24. Noviembre - Hora: 10 noche Organiza: PEÑA FLAMENÇA DE JAEN\n\nCANTAN\n\nJuan Barea Curro de Utrera Chano Lobato Rosario López Carmen Linares Carlos Cruz Pepe «Pollielas» Juan de la Malena José Menese Enrique Morente Diego Clavel José el de la Tomasa Miguel Vargas José Mercé Manolo Catato Rafael Maeras\n\nA LA GUITARRA\n\nJuan Carmona «Habichuela» Pedro Peña Perico el del Lunar José Luis Postigo Luis el Calderito Pepe Toques\n\nPRESENTAN\n\nJuan Antonio Ibáñez Fernando Arévalo José Gutiérrez Paco Carriño\n\nEl orden de programas no altera la categoría de los artistas Por ser un homenaje desinteresado por parte de los señores artistas, no nos hacemos responsables si el mencionado programa sufre alguna alteración",
    "title": "HOMENAJE A RAFAEL ROMERO",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 118,
    "article_char_count_full": 745,
    "article_char_count_review": 745,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1984-09-13-right-pe-a-flamenca-de-jaen",
    "article_text_for_review": "Flota su mirada en el hundido cuenco de los ojos que apenas dulcifican las cejas, caídas hacia la leve sien, acompasadas, como un signo de clave, dulcemente, trazado en el pentagrama de la frente.\n\nLos afilados pómulos centran una nariz saliente y hambreada, hecha para los olores fuertes, para el aliento a borbotones, tensos los orificios por el marcado surco que le parte el mentón. La boca, sin norma, se retuerce un instante después de la sonora dentellada, le puede más la ira de dientes disonantes, pero sus labios pugnan por suaves mordeduras de frutales tristezas.\n\nY la extendida mano, firme pero crispada, tal si fuera un apéndice del rostro, una última mueca de la voz, desnuda, plena de elementales acentos, tan primarios y eternos como una siguiriya.\n\nRamón Porras",
    "title": "PEÑA FLAMENCA DE JAEN",
    "periodical": "candil",
    "issue_id": "1984-09",
    "year": 1984,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 130,
    "article_char_count_full": 778,
    "article_char_count_review": 778,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
