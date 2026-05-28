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
    "article_id": "1985-01-17-right-constituida-la-fundaci-n-antonio",
    "article_text_for_review": "E n Mairena del Alcor ha que- dado constituida, mediante escritura notarial, la FUNDACION ANTONIO MAIRENA, de cará- ter cultural privada, cuyos fines se encamina a la promoción, desarrollo, protección y fomento del arte flamenco como expresión cultural y a la exaltación de la figura artística del desaparecido cantaor.\n\nLa fundación agrupa a un número de amigos personales de Antonio Cruz García y se rige por un patronato cuyo Consejo de Gobierno ha sido elegido y queda compuesto así:\n\nLa sede social de la fundación ha quedado establecida inicialmente en el Ayuntamiento de la villa maire-nera, Plaza de Antonio Mairena, núm. 1, con delegación permanente en Sevilla, calle María Auxilia-dora, núm. 6.\n\nPresidente: Don Rafael Alvarez Colunga.\n\nCompletan el Patronato Fundacional (ampliable en el futuro hasta un máximo de cincuenta miembros) los siguientes señores, además de los antes indicados:\n\nVicepresidente: Ayuntamiento de Mairena del Alcor, representado por su alcalde-presidente, don Manuel Bustos Lozano.\n\nDon Francisco Asencio Montes\n\nSecretario: Don Antonio Cruz Madroñal.\n\nTesorero: Don Francisco Celaya Tebar.\n\nVocales: Don José Núñez de Castro y Gómez; don Manuel Herrera Rodas.\n\nAyuntamiento de Mairena del Alcor. Delegación de Cultura, representada por la delegada doña María Dolores García Gutiérrez.\n\nAyuntamiento de Sevilla, representado por su alcalde-presidente, don Manuel del Valle Arévalo.\n\nDon Sebastián Carvajal Cobano. Casa del Arte Flamenco «Antonio Mairena», representada por su presidente don José María Jiménez Sánchez.\n\nDona Matilde Coral. Don Francisco Cruz García. Don Manuel Cruz García. Don José Luis Cuberta Graña. Don José Escala Macero. Don Rafael Escuredo Rodríguez. Don Antonio Fernández Fernández.\n\nDon Rafael García Rodríguez. Don Felipe González Márquez. Don Lucas López López. Don Francisco Moreno Galván. Don Juan Antonio Muñoz Pacheco.\n\nDon Jesús Antonio Pulpón González.\n\nDon Antonio Reina Gómez.\n\nDon Manuel Rodríguez Granado.\n\nDon Rafael Román Guerrero. Don Francisco Vallecillo Pecino.",
    "title": "Constituida la Fundación Antonio Mairena",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 297,
    "article_char_count_full": 2040,
    "article_char_count_review": 2040,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-01-18-left-quienes-fueron-los-maestros-migu",
    "article_text_for_review": "C antaor gaditano nacido hacia mediados del siglo XIX, pocas son las noticias o referencias que de él se tienen. La transmisión oral alude a que su nombre era Manuel, sin embargo, Fernando el de Triana, en su libro «Arte y artistas flamencos» lo menciona con el nombre de Miguel.\n\nAunque cantaor gaditano, Miguel Cruz «Macaca» pasó gran parte de su vida artística en la ciudad de Sevilla, a la cual acudía con mucha frecuencia por ser reclamado por Silverio Franconetti.\n\nFernando el de Triana dice de él: «...Cantó para bailar, como no cantó nadie mejor, aquellos cantes conocidos por Romera, Mirabrás, La Contrabandista, la Tía “Pretola” y Los Caracoles, en el más castizo compás para bailar por alegrías, y no esa monótona lata de que disfrutamos hoy por la evolución del tiempo.\n\nMiguel «Macacá» fue el que alternó años y años con los colosos de aquel cante que se llamaron Paco el Sevilla- no, José Barea, Romero el Artillero, el Quiqui y otros muchos.\n\nFue un cantaor de extraordinarias facultades, completísimo en todos los cantes grandes por soleares y seguiriyas, las diferentes cañas y polos y serranas. Y para qué decir más: fue un verdadero maestro de cante, al que el gran Silverio contrataba por años consecutivos».\n\nAntonio Mairena y Ricardo Molina, en su libro «Mundo y forma del cante flamenco» lo tienen encuadrado dentro de los grandes intérpretes de siguiriyas.\n\nAl igual que otros muchos intérpretes de nuestro arte, Miguel Cruz «Macaca» ha estado olvidado en los anales de la historia del cante gaditano, quizás por la arrolladora personalidad de Enrique el Mellizo o Paquirri el Guanté. A través de estas páginas que remos dejar constancia de este gran intérprete gaditano.\n\nMucha polémica se ha creado sobre la posible cuna de este gran cantaor. Su nacimiento se le atribuía por parte de unos al pueblo de Lebrija y por parte de otros al de las Cabezas de San Juan. Sin embargo, ha sido el investigador extremeño Manuel Yerga Lancharro el que ha dejado constancia de su verdadero lugar de nacimiento, constancia que quedó plasmada en el número 15 de esta revista.\n\nAsí, Juan Moreno Jiménez «Juani-quín de Lebrija», hijo de Juan y de Rosario, nació en Jerez de la Frontera, el 28 de marzo de 1862/64. Se casó con Dolores Vargas Sánchez y ha sido uno de los intérpretes que han sabido darle personalidad a los cantes «por soleá».\n\nJuaniquín de Lebrija vivió trabajan- do por todos los cortijos de la zona de Jerez, Utrera y Lebrija. Murió en San- lúcar de Barrameda, a consecuencia de una bronconeumonía, en 1946.\n\nHacer referencia al arte de este cantar jerezano es citar parte del texto de Ricardo Molina y Antonio Mairena en su libro «Mundo y formas del cante flamenco»: «Juaniquín, a quien hemos conocido durante muchos años, fue uno de los grandes soleareros del siglo. Había nacido en Lebrija hacia 1860 y vivió casi siempre cerca de Utrera, cuando no en el mismo pueblo. Su choza era lugar de peregrinación para los buenos aficionados de la comarca. Aquel gitano complaciente, gracioso e ingenuo, no negó a nadie el placer de oírle. Su influjo ha sido enorme no sólo en Utrera y su término, sino en las comarcas de Alcalá, Carmona, Mairena, Morón, Jerez, Coria y Lebrija».\n\nSelecciona: Rafael Valera",
    "title": "Quienes fueron los maestros: Miguel Cruz Macacá y Juaniquín de Lebrija",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 550,
    "article_char_count_full": 3228,
    "article_char_count_review": 3228,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-01-20-left-hablan-las-pe-as",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nII Concurso de Cante Flamenco «Frasquito Yerbaguëna»\n\nO rganizado por la Peña Flamenca «Frasquito Yerbaguena» y patrocinado por la Diputación de Granada, el Ayuntamiento de Cúllar de Vega y firmas comerciales, ha sido convocado el II Concurso de cante Flamenco «Frasquito Yerbaguena». Podrán participar cuantos cantaores aficionados y profesionales lo deseen. El plazo de inscripción finaliza el día 15 de febrero.\n\nTodos los concursantes habrán de hacer tres cantes de cada uno de los tres grupos establecidos.\n\nPara este certamen se han establecido dos premios, siendo el primero de ellos de 50.000 pesetas.\n\nNueva Junta Directiva de la Casa del Arte Flamenco Antonio Mairena, de Mairena del Alcor\n\nE n Asamblea General celebrada por la Casa del Arte Flamenco «Antonio Mairena», de Mairena del\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"Peña\"]\n\nBAUTISTA LEON. Vicesecretario, ANTONIO BAUTISTA LEON. Vocales, JOSE M.ª DOMINGUEZ ORTIZ, JESUS GAVIRA ALBA, TOMAS REVELLE SANCHEZ, ANGEL TRONCOSO NAVARRO, ANTONIO OJEDA PONCE, LUIS COTAN DE LA FUENTE, MANUEL CRESPO REYES, JOSE M. JIMENEZ SANCHEZ, ANTONIO CASTRO JIMENEZ, JOSE CARMONA MALDONADO, ANTONIO BORRERO POZO, ANTONIO MORILLO JIMENEZ y ANTONIO CRUZ MADROÑAL. Deseamos toda clase de aciertos a este numeroso colectivo. Nueva dirección en la Peña Flamenca Fosforito de Los Barrios E n Asamblea General celebrada por la Peña Flamenca «Fosforito» de Los Barrios (Sevilla), fue elegida nueva Junta Directiva, quedando compuesta la misma de la siguiente manera: Presidente, JUAN RONCERO BAREA. Vicepresidente 1.º, FRANCISCO GONZALEZ RODRIGUEZ. Vicepresidente 2.º, MANUEL GARCIA LOZANO. Tesorero, MANUEL JIMENEZ PADILLA. Secretario, PEDRO FERNANDEZ CHAVES. Vocales, MANUEL RUIZ MARTINEZ, ANTONIO ESPINOSA CORRERO, ALFONSO TAPIA JIMENEZ, MANUEL MUÑOZ GARCIA y ANDRES CUSTODIO JURADO. Nuestra felicitación a los amigos de Los Barrios. VIII Concurso de Cante Jondo «Peña La Platería» de Granada L a Peña Flamenca «La Platería» de Granada ha convocado la VIII edición del concurso de Cante Jondo, ésta mejor dotada económicamente que en ediciones anteriores. La inscripción deberá hacerse por escrito dirigido a la «Peña La Platería», Placeta de Toqueros, 7, 18010-Granada, hasta el día 16 de abril inclusive. Se han establecido dos grupos de cantes que son los siguientes: A) Siguiriγas, Serranas, Tonás, Tientos, Soleá, Caña, Polo, Bulerías por Soleá, Cantiñas y Bulerías. B) Granaína, Media Granaína, Malagueñas, Peteneras, Tarantas, Cartageneras y Mineras, Farruca, Mariana. Se ha dotado este certamen de los siguien\n\n[ENDING CONTEXT]\n\ny un par de Peñas Flamencas.\n\nA estas alturas la floración de estas Entidades es tan espectacular como cuantitativa y su inmensa mayoría nacieron al calor y vivencia del «Tocadiscos Flamenco». La propia Casa de Andalucía de Barcelona, fue fundada bajo sus auspicios.\n\nCon este motivo, durante todo el mes de marzo se celebrarán una serie de actos, en los que participan las casas andazas.\n\nDesde Jaén CANDIL felicita cariñosamente a Ricardo Romero a la vez que envía un fuerte abrazo a todos los andaluces.\n\nAPERITIVOS SELECTOS\n\nEspecialidad en\n\nPLANCHA\n\nMesones, 18 Teléf. 2340 46\n\nJ A E N\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Hablan las Peñas",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1724,
    "article_char_count_full": 10937,
    "article_char_count_review": 3330,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "Peña"
      }
    ]
  },
  {
    "article_id": "1985-01-22-left-discograf-a-flamenca",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA PROPOSITO DE UN ARTISTA FLAMENCO\n\nLos arqueros oscuros a Sevilla se acercan Guadalquivir abierto. Anchos sombreros grises, largas capas lentas. ¡Ay, Guadalquivir! Vienen de los remotos países de la pena. Guadalquivir abierto. Y van a un laberinto. Amor, cristal y piedra. ¡Ay, Guadalquivir!\n\nF.G.L. JOSE ROMERO, con su FANTASIA-SUITE ANDALUZA-IBEROAMERICANA, se incorpora con indeclinable carácter representativo, a un testimonio de Cultura común avalado por la realidad de los siglos. E n un contraritmo, el Guadalquivir enfila la empinada cuesta que le lleva a su ser primero, y nos deja la verticalidad emocional de una música-agua que traduce, a través del piano, un mundo de sentimientos que sólo se puede dar cuando el conocimiento se enriquece de la plenitud artística del compositor e\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"profundo\"]\n\nel Guadalquivir enfila la empinada cuesta que le lleva a su ser primero, y nos deja la verticalidad emocional de una música-agua que traduce, a través del piano, un mundo de sentimientos que sólo se puede dar cuando el conocimiento se enriquece de la plenitud artística del compositor e intérprete. Sea lo que decimos, a propósito del disco «FANTASIA-SUITE ANDALUZA-IBEROAMERICANA» que José Romero nos presenta como fruto de su personalidad y de un profundo trabajo. Preludio en Mi menor Opus 51, Polo Venezolano en Mi Mayor, Opus 53, Colombianas en Mi Mayor, Opus 52 y Guajiras en Mi Mayor Opus 50, configuran el sentir de unos ecos y ritmos, capaces de dibujar, sonoramente, unas tierras, unos paisajes y unas culturas que nos dan «los enclaves históricos de la salada claridad, del agua oculta que llora, de la orilla de las tres carabelas, fundidos en los evocadores pasajes andinos, Antillanos, Chacúes». ¡Hermoso recorrido el vivido! La grabación nos deja oír, en perfección de sones, no sólo la técnica, también la pasión como cauce de innumerables saberes que tienen sus raíces en el alma de un pueblo que hace de lo popular máxima expresión artística. Tenía que ser un andaluz quien fijara, en una obra, elementos de lazos comunes. La escucha del disco supone un encuentro de emociones que, partiendo de la soledad-preludio, nos empuja, sin querer, al amor inmenso de nuestro mundo. Nadie mejor que el compositor para llevarnos a lo descrito por él. Dos poetas, desde la letra impresa, esculpen, a golpes de versos, la piedra firme de nuestro fondo-historia-cultura, paisaje y paisanaje-escenario natural. Al alimón Lorca-Neruda, Neruda-Lorca, en esta o aquella orilla. La fina sensibilidad\n\n[ENDING CONTEXT]\n\ntuvo y se murió de perfil. Viva moneda que nunca se volverá a repetir. Un ángel marchoso pone su cabeza en un cojín. Otros de rubor cansado, encendieron un candil. Y cuando los cuatro primos llegan a Benamejí, voces de muerte cesaron cerca del Guadalquivir. Pero, permanece. Está. Están. Viven en otra generación de artistas como José Romero.\n\nSólo decir que la obra FANTASIA-SUITE ANDALUZA-IBEROAMERICANA, queda incompleta. Hay que andar más en este camino. Es menester abundar más en lo expuesto. Y nuestro —perdona la posesión, Jose— jondo compositor tiene las claves para hacerlo.\n\nL.\n\nDOSCANDIL\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Discografía Flamenca",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 1107,
    "article_char_count_full": 6666,
    "article_char_count_review": 3325,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "profundo"
      }
    ]
  },
  {
    "article_id": "1985-01-23-right-discograf-a-placas",
    "article_text_for_review": "POR: MANUEL YERGA LANCHARRO\n\nDE ANTONIO RENGET",
    "title": "Discografía Placas",
    "periodical": "candil",
    "issue_id": "1985-01",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 7,
    "article_char_count_full": 46,
    "article_char_count_review": 46,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
