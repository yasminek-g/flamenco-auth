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
    "article_id": "1988-11-21-right-desaparece-el-aula-de-flamencolo",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFernando de Gamarra\n\nEmálaga el Aula Universitaria de Flamencología por el entonces rector magnífico de la Universidad de Málaga, don José M.a Smith Agreda y el vice-rector de Extensión Universitaria don Domingo Sánchez-Mesa Martín. Vasco el primero y granadino el segundo, según creo. Mientras estos señores estuvieron al frente de estos puestos en la Universidad malagueña, el Aula de Flamencología tuvo un magnífico funcionamiento bajo la dirección del doctor don Alfredo Arrebola Sánchez y el patrocinio de esa gran solera flamenca que se llama «Peña Juan Breva».\n\nDesde aquella fecha de inauguración, se ha venido desarrollando una amplísima divulgación de la cultura flamenca andaluza, con tal éxito que hubo que trasladar el aula desde las reducidas dimensiones de la «Peña Juan Breva» al\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_01 | trigger=\"de verdad\"]\n\nue se llama «Peña Juan Breva». Desde aquella fecha de inauguración, se ha venido desarrollando una amplísima divulgación de la cultura flamenca andaluza, con tal éxito que hubo que trasladar el aula desde las reducidas dimensiones de la «Peña Juan Breva» al salón de actos de la Facultad de Filosofía y Letras, por entonces en el Colegio de San Agustín, en el centro de la ciudad. Por esta aula, han pasado conferenciantes, cantaores y concertistas de verdadera categoría; se han extendido las actividades a peñas de la capital y centros culturales, estando presente en muy diversas actividades ciudadanos: exaltación de la saeta, conferencias en APAS de Colegios de EGB, jurados de concursos, exaltación de los verdiales, etc. Desde el cese de sus fundadores, todos los que han venido detrás, han puesto cada uno su piedrecita, para hundir cada vez más el navio del arte flamenco en la Universidad. A pesar de los años transcurridos, esta aula no dispone de un asentamiento definitivo. Nunca ha dispuesto de una oficina con el mobiliario necesario para ir formando nuestra biblioteca, una fonoteca con equipo de sonido, despacho para su director dotado de máquina de escribir y máquina fotocopia-dora para la confección de comunicaciones y texto de las conferencias; un salón de actos debidamente sonorizado y una sala\n\n[ENDING CONTEXT]\n\npor personas ajenas a esta tierra malagueña, al llegar a ser regentada por los propios malagueños, esté prácticamente desaparecida o a punto de llegar a este extremo.\n\nY sugiero: en Málaga hay material humano no para un Aula de Flamencología, sino para una cátedra. Y no cito a nadie, porque no quiero dejarme a nadie olvidado. Pero hay gente «mú güena» y mucho interés por la cultura flamenca. ¿Qué pasa pues? ¿No hay ganas de servicio a la cultura popular? ¿Es que nuestro folklore, nuestras costumbres, nuestras tradiciones, no forman parte de una verdadera antropología del pueblo andaluz?\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "¿Desaparece el Aula de Flamencología de la Universidad de Málaga?",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "21-22",
    "page_number": 21,
    "word_count": 1228,
    "article_char_count_full": 7422,
    "article_char_count_review": 2941,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_01",
        "family": "AUTH",
        "trigger": "de verdad"
      }
    ]
  },
  {
    "article_id": "1988-11-22-right-jaima-cultural-andaluza-en-catar",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPepe Cózar\n\nUna Jaima Cultural Andaluza estuvo instalada en la última quincena de septiembre en la localidad valenciana de Catarroja. Allí hubo muestras culturales andaluzas como cerámica, pintura, teatro, poesía...¡Y Arte Flamenco!\n\nComo parte importante de la cultura del pueblo andaluz, no podía faltar, de entre los actos programas todos un magno festival flamenco, en el que se dieron cita el cante, el toque y el baile.\n\nAlrededor de unas 900 personas —no sólo andaluces— nos dimos cita en el salón de actos de la Jaima Cultural Andaluza para vivir y vibrar con los misterios gozosos del Arte Flamenco.\n\nManuel Moreno Maya «El Pele», debutó con su cante en tierras valencianas: «De verdad que la acogida ha sido fabulosa. No me lo esperaba y ha habido momentos de emoción», nos manifestó tras\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"afición\"]\n\nen el salón de actos de la Jaima Cultural Andaluza para vivir y vibrar con los misterios gozosos del Arte Flamenco. Manuel Moreno Maya «El Pele», debutó con su cante en tierras valencianas: «De verdad que la acogida ha sido fabulosa. No me lo esperaba y ha habido momentos de emoción», nos manifestó tras su actuación. Lucas de Écija, cantaor acompañante de Inmaculada Aguilar, hizo algunos cantes en solitario, siendo fuertemente aplaudido por la afición. Inmaculada Aguilar, bailaora ya habitual por tierras valencianas nos diría: «Venir a Valencia a bailar es ya tan normal en mí, como actuar en cualquier punto de Andalucía. Diríamos que actuar aquí es como hacerlo en mi propia tierra». Las jóvenes y sabedoras manos de Vicente Amigo y Rafael Trenas pusieron con sus guitarras la «guinda» a la «tarta flamenca». Abrió la noche flamenca Lucas de Écija con Bulería por soleá, que dedicó a su guitarrista Rafael Trena, ya que en llamada telefónica a Córdoba, dos horas antes de comenzar el festival, anunciaron al guitarrista que su señora había dado a luz un hermoso niño. (Enhorabuena, Rafael, por ese nuevo tallo, posible heredero de tu maestría con la «sonata»). Hizo luego Lucas de Écija granaína y media granaína, recreándose y meciéndose en los tercios con buen gusto y paladar. Siguiendo por los caminos auténticos del cante, nos deleitó seguidamente con unas malagueñas rematadas con verdiales. Terminó su actuación con unos bellos fandangos valientes de Cané. Fuertes aplausos premiaron la entrega del cantar astigitano. Fue luego Inmaculada Aguilar la que, con belleza y plasticidad, agigantándose en cada tercio, nos obsequió con un baile de bulerías por soleá, acompañada al cante por Lucas, la guitarra de Rafael Trena y las palmas de José Plantón «El Pipa» y Manuel Fernández «Finito». Un artista que actuaba por primera vez en tierras valencianas y que era esperado con expectación, fue Manuel Moreno Maya «El Pele» deleitándonos con una Zambra, recordándonos vivamente la voz señera de Manolo Caracol. Luego cantó —muy bien acompañado por la guitarra de Vicente Amigo— alegrías de Cádiz, para luego finalizar por tientos-tangos. Cuando se disponía a abandonar el escenario, y ante la insistencia del público de que cantara otra, el cantaró calé, nos obsequió con una serie de fandangos de la provincia de Huelva, de excelente factura. Y ahora sí, ahora, y tras cantar con ritmo y sabiduría, unas muy buenas bulerías, abandona el escenario ante el clamor de un público totalmente entregado. Por eso y con justificada razón decimos que «El Pele» llegó, cantó y convenció. Y vino de nuevo el baile hecho arte en la persona de Inmaculada Aguilar. Se olvidó de\n\n[EVIDENCE WINDOW 2 | retrieval_hint=PED_02 | trigger=\"Compás\"]\n\ntarroja (Valencia). Organizado por la Federación de Entidades Flamencas de Extremadura, F.E.F.E.X., se celebró en dos fases el II Concurso Internacional de Guitarra Flamenca el día 18 de noviembre en Mérida, con una primera selección, y el 19 en Badajoz, en dos sesiones, por la mañana la segunda selección y a partir de las 20 horas la gran final, en la que actuaron también los cantaores extremenos: Niño de la Ribera, Niño de Badajoz y el grupo «Compás Extremeño», y de forma espontánea «El Pele» de Córdoba. Los premios eran sustanciosos: 250.000 pesetas para el primer premio, 150.000 pesetas para el segundo y 100.000 pesetas para el tercero, además de tres accésit de 15.000 pesetas cada uno y un premio de 50.000 pesetas al mejor guitarrista extremeño. Todos los premios estaban patrocinados por la Consejería de Cultura de la Junta de Extremadura, colaborando también el Excmo. Ayuntamiento de B\n\n[ENDING CONTEXT]\n\nRueda Fernández, Manuel Gutiérrez Cañada, José Rosendo Maqueda, José Sabin Pérez, Francisco Domínguez Pérez, José Piñero Ojeda, Vicente López Vera, José Barragán Ariza, José Rodríguez Alonso, José Jiménez Perea, Antonio García Avila.\n\nDeseamos toda clase de éxitos a la numerosa ejecutiva.\n\nPropietario: CARLOS GUERRERO MURILLO (Medalla al Mérito del Trabajo)\n\nRecepción diaria de MARISCOS Y PESCADOS ESPECIALIDAD EN ASADOS\n\nROLDAN Y MARIN, 7\n\nJ A E N\n\nTELEFONO 22 97 65\n\nPágina 46 CANDIL\n\nInstituto de Estudios Giennenses. Candil : boletín de la Peña Flamenca de Jaén. N.º 60, 11/1988. Página 24\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Jaima cultural andaluza en Catarroja",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "22-24",
    "page_number": 22,
    "word_count": 2349,
    "article_char_count_full": 14723,
    "article_char_count_review": 5270,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "afición"
      },
      {
        "window": 2,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "Compás"
      }
    ]
  },
  {
    "article_id": "1988-11-24-right-discograf-a-flamenca-placas",
    "article_text_for_review": "Por: Manuel Yerga\n\nCANDIL Página 47",
    "title": "Discografía Flamenca (Placas)",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 6,
    "article_char_count_full": 35,
    "article_char_count_review": 35,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-01-3-right-candil-a-o-12-doce",
    "article_text_for_review": "nce años, tiempo efímero para otras empresas, es ya una larga vida para un empeño de cultu-\n\nra. «Candil» nació en la turbulencia de nuestra transición a la democracia y la naturaleza coyuntural, traumática y revisora de este período de la historia de España, ha condicionado el que se tornen obsoletos gran parte de los proyectos de cultura que florecieron a su amparo. Por fortuna, al menos así lo creemos, no es éste el caso de «Candil», singladura que puso su periplo en la memoria adormecida de las ocho provincias andaluzas para recuperar un hermosísimo legado que la ramplonería y el dirigismo cultural de la dictadura habían envilecido.\n\nExitos y torpezas deben de contabilizarse, por igual, en ese balance de urgencia que sólo nuestros amigos lec-\n\ntores pueden diseñar. Pero lo cierto es que la vigencia de la idea que puso en marcha esta Revista, sigue alimentada por el peligro que constituyen nuevas amenazas, nuevos y más sutiles detractores: consumo, desconocimiento y osadía en los enjuiciamientos de prebostes y sanedrines de notables, inadecuación entre la autenticidad y el éxito de los artistas, intervencionismo institucional de concejos y Diputaciones cuyo sentido del oportunismo, que no de la oportunidad, confunde el populismo con la esencia-lidad del grito jondo, crítica desinformada y un largo etcétera.\n\nUn ciclo se ha cerrado para esta Publicación. Era preciso renovarse y en ese último empeño estamos. No se trata de crispar o de que se dulcifiquen nuestras reflexiones; queremos, sí, modificar el tamaño y acaso el lugar de nuestra mordedura, o tal vez que ésta se haga más patente. En cualquiera de los casos, un nuevo enfoque, un tratamiento distinto irá paulatinamente definiendo el estilo noventa que intentamos promover. Para ello contamos con una nueva estructura en el Consejo de Redacción, prestigiosos colaboradores y una renacida ilusión por reflexionar conjuntamente, sin prepotencia, pero con rigor, sobre el flamenco de ayer, sobre el flamenco de hoy. Que Dios reparta suertes.\n\nR. Porras",
    "title": "Candil Año 12 Doce",
    "periodical": "candil",
    "issue_id": "1989-01",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 324,
    "article_char_count_full": 2033,
    "article_char_count_review": 2033,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-01-4-left-reflexiones-sobre-la-etapa-de-re",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Ríos Ruiz\n\nAlguien ha dicho —y se lo agradecemos— que con la publicación del Diccionario Enciclopédico Ilustrado del Flamenco se cierra la etapa de revalorización del arte andaluz, inicia-da hace treinta años, por lo que se impone un nuevo tratamiento de su análisis histórico y en la apreciación de su evolución y de sus intérpretes. Efectivamente, el flamenco está revalorizado y reconocido como nunca, tanto dentro de España como en el extranjero. Pero esto no quiere decir que haya llegado en su divulgación al esplendor y la extensión de otras músicas aborígenes o contemporáneas, como el jazz, por ejemplo, aunque el flamenco ha dado un salto cualitativo muy importante últimamente.\n\nLas causas del actual estado del flamenco son diversas, pese a que existe una tendencia muy marcada de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"imit\"]\n\nente, el flamenco está revalorizado y reconocido como nunca, tanto dentro de España como en el extranjero. Pero esto no quiere decir que haya llegado en su divulgación al esplendor y la extensión de otras músicas aborígenes o contemporáneas, como el jazz, por ejemplo, aunque el flamenco ha dado un salto cualitativo muy importante últimamente. Las causas del actual estado del flamenco son diversas, pese a que existe una tendencia muy marcada de limitar su revalorización a solamente dos o tres acontecimientos y a determinados, pocos, artistas. La verdad no es esa. Y sin ánimo de quitarle méritos a nadie, sino todo lo contrario, en la revalorización del flamenco han intervenido muchas circunstancias y muchas personas que no se han valorado al respecto, incluso que se ignoran por ciertos arribistas. Creo que ha llegado la hora de reconocerlo así y de puntualizar detenidamente el fenómeno de la revalorización flamenca, desde un punto de vista objetivo, sin dejarnos llevar por la gustativa personal y sin ceñirnos a una sola parcela de un arte tan rico en sus variantes. Hay que abrir la vista en abanico, para contemplar la gama de matices del flamenco y poder realizar un compendio de ac ciones y hechos positivos a su favor, desde los primeros años de la posguerra. Pero para hacerlo tenemos que admitir una conclusión, si queremos situarnos en una posición de salida válida: la aspiración del flamenco desde los primeros años del siglo XIX, ha sido ser un espectáculo. En este sentido no hay que llamarse a engaños. Y en consecuencia directísima, todo intérprete con posibilidades ha intentado ser profesional, salvo muy raras excepciones. Por ello resulta desmesurada la exaltación que se viene haciendo de algunos ancianos, que nunca pasaron de ser meros aficionados, presentándolos como genios incomprendidos del cante o cantaores maravillosos que nunca quisieron vivir del cante. Es un aspecto que debemos dejar en su lugar exacto: una cosa es conocer el cante y apuntarlo con cierta calidad, y otra ser creador de estilos o un verdadero artista flamenco. Para quien ha nacido y crecido en un núcleo cantaor, la diferencia está bien clara, se sabe que todo intérprete que no ha llegado a profesional, no lo ha conseguido porque, generalmente, no poseía las cualidades —de una u otra índole— necesarias para ello. Todo lo demás que se diga en su torno son achaques más que r\n\n[ENDING CONTEXT]\n\nque atenderemos en una próxima ocasión.\n\nLa etapa de revalorización ha tenido, tal hemos examinado, diversos componentes, algunos de los cuales habíamos ignorado en buena parte, y era de razón recordarlos. Lo extraordinario de la cuestión es que todos esos componentes, personas y circunstancias, cada uno en su medida y eficacia, han hecho posible esa etapa de revalorización, que no de resurrección, que ha vivido el arte flamenco en todas sus vertientes y variantes. El flamenco está revalorizado, es verdad; ahora corresponde engrandecerlo, porque todo verdadero arte no se entiende momificado.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Reflexiones sobre la Etapa de Revalorización del Arte Flamenco",
    "periodical": "candil",
    "issue_id": "1989-01",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 3423,
    "article_char_count_full": 21074,
    "article_char_count_review": 4015,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "imit"
      }
    ]
  }
]
```
