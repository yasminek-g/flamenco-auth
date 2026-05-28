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
    "article_id": "1998-09-3-right-el-sitio-de-lucena-en-el-arte-fl",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nRafael Marín escribía del fandango, en su Método de Guitarra (Flamenco) por música y cifra, allá por 1900, que «por la parte donde mejor se canta es por la provincia de Córdoba, teniendo fama el fandanguillo de Lucena, pueblo de esta provincia». No tiene más texto su artículo sobre el fandango, salvo esta premisa: «Con éste pasa lo mismo que con la malagueña». De ésta, escribe inmediatamente antes: «...aunque se canta en toda España (y en particular en la región andaluzia), es exclusivamente de Málaga, pues el ritmo y la cadencia sin afectación que allí le dan no se oye en ninguna otra parte.» Rafael Marín escribe en Madrid cuando acaba una temporada de conciertos en la capital de Francia, habiendo nacido en el pueblo sevillano de Pedroso de la Sierra y recibido clases, entre otros, de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"mejor\"]\n\nSierra y recibido clases, entre otros, de Paco de Lucena. Agustín Gómez En esta primicia informativa de los estilos flamencos que nos da el concertista «por lo flamenco» observamos lo que viene a ser caballo de batalla en nuestra didáctica, por lo que no nos duelen prendas reiterativas y machaconas. Aparte de la asociación Lucena y Málaga, hay una clara distinción entre malaqueña y fandango, aunque sólo sea por las partes respectivas en las que mejor se cantan. El guitarrista del siglo pasado —nació el 7 de julio de 1862—distingue entre «se canta...» y «don-de mejor se canta...». «La malagueña se canta en toda España —nos dice—, pero donde mejor se canta es en Málaga», y añade el rasgo característico: «...pues el ritmo y la cadencia sin afectación que allí le dan no se oye en ninguna otra parte.» Es la manera de decir y no el dicho concreto lo que distingue al flamenco. Ese era el saber distinguir de Manuel Torre. Así, el habla andaluzas consiste en cómo se dice de manera distinta el Castellano en cada uno de nuestros pueblos; así, el flamenco es cómo se expresa el mismo cante de manera distinta en cada pueblo, zona o comarca. Ese «cómo», y no el «ser», es lo que nos trae de cabeza a payos y gitanos. No es la falta de acuerdo en lo que pensamos, sino en lo que sintonizamos, porque no sabemos de lo que hablamos. En su Método de Guitarra, Rafael Marín aconseja al principiante que «al ejecutar la malagueña, debe procurar de no marcar ese ritmo que estamos acostumbrados a oír en las orquestas y bandas militares, las que acentúan demasiado la primera parte de cada compás, pues esto da un carácter especial a la malagueña, el cual difiere bastante a cómo se toca en la guitarra.» Vean la insistencia en el cómo. Habla de ritmos de orquestas, bandas y cómo han de tocarse en la guitarra. Está claro que no habla del ritmo interno, de la voz; sino del externo, la guitarra. Esa guitarra no marca compases ni ritmos, sino sugerencias melódicas, entonaciones y acordes para acompañar a las malagueñas nuevas de competencia c\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\nxpulsados al fin los invasores franceses que habían dejado a España despojada, sobreviene una depresión económica que en Lucena se resuelve con el contrabando, especialmente de tabaco. Grupos de contrabandistas y delincuentes rurales luchan por sobrevivir en una pobreza de solemnidad. Lucena fue entonces conocida por la proliferación y temeridad de sus partidas de bandoleros. Acaso sea ésta la serrana más cantada de todos los tiempos, la que dio lugar la popularidad del famoso bandolero nacido en la pedanía lucentina de Jauja el 21 de junio de 1800 y muerto en 1833, José María «El Tempranillo»: Por la Sierra Morena va la partía, y al capitán le llaman José María. Sus compañeros, Francisco el de la Torre, Juan Caballero. Otras veces, el macho es este tro: No será preso, mientras su jaca torda, tenga pescuezo. Tiene esa copla tanta asociación a la etapa lucentina del «Tempranillo» que hizo\n\n[ENDING CONTEXT]\n\nLucena (Caja Provincial de Ahorros de Córdoba).\n\nJ. BLAS VEGA y M. RÍOS RUIZ: Diccionario Enciclopédico Ilustrado.\n\nJOSE GARCÍA LÓPEZ: Historia de la Literatura Española (Editorial Vicens Vives).\n\nJosé Osuna Pineda: Gentes de mal vivir.\n\nMARÍA JOSÉ PORRO HERRERA: Artículos en Enciclopedia Los Pueblos de Córdoba (Caja Provincial de Ahorros de Córdoba).\n\nJUAN DÍAZ DEL MORAL: Ha de las Agitaciones Campesinas Andaluzas (Alianza Universal).\n\nRAFAEL MARÍN: Método de Guitarra (Flamenco). Por Música y Cifra (Único publicado de aires andaluces (Publicaciones del Ayuntamiento de Córdoba, 1995).\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El sitio de Lucena en el Arte Flamenco",
    "periodical": "candil",
    "issue_id": "1998-09",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "3-9",
    "page_number": 3,
    "word_count": 6792,
    "article_char_count_full": 39972,
    "article_char_count_review": 4625,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "mejor"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      }
    ]
  },
  {
    "article_id": "1998-09-9-right-los-fandangos-de-lucena-un-libro",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAntonio Cruz Casado\n\nE 1 31 de diciembre de 1921, el célebre músico gaditano don Manuel de Falla solicitaba ayuda al Ayuntamiento de Granada para celebrar el Concurso de Cante Jondo, que tendría lugar el próximo año, en los términos siguientes: «De seguir así, al cabo de pocos años no habrá quien cante y el cante jondo morirá sin que humanamente sea posible resucitarlo [... ]. Técnicamente —añadía— es imposible hacer la notación musical de estos cantes, y por lo tanto, no pueden archivarse en ningún documento con la esperanza de ser desenterrado un buen día en el transcurso de los tiempos.»¹\n\nAlgunas de las ideas, expresadas en este fragmento de Falla, tienen aún vigencia, sobre todo en cuanto se refiere a la necesidad de apoyar y estudiar uno de los bienes patrimoniales más singulares de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"Arte\"]\n\ntanto, no pueden archivarse en ningún documento con la esperanza de ser desenterrado un buen día en el transcurso de los tiempos.»¹ Algunas de las ideas, expresadas en este fragmento de Falla, tienen aún vigencia, sobre todo en cuanto se refiere a la necesidad de apoyar y estudiar uno de los bienes patrimoniales más singulares de los andaluces. Afortunadamente se sigue trabajando en esa dirección desde distintos frentes y este XXVI Congreso de Arte Flamenco que celebramos en Lucena, con todas sus actividades anejas, es una buena muestra de ello. Las instituciones públicas deben apoyar, como lo ha hecho nues- tro Ayuntamiento en esta oca- FRANCISCO CALZADO GUTIÉRREZ LOS FANDANGOS DE LUCENA (Cantes de viejos oficios, ambientes y artistas lucentinos) Prólogo de Antonio Cruz Casado 1) Apud. EDUARDO MOLINA FAJARDO: Manuel de Falla y el «Cante Jondo», Granada, Universidad, 1998, pág. 164. Se trata de una reedición facsímil, el libro original es de 1962. sión. Desde la Corporación no se han regateado esfuerzos, ni reuniones, ni aportaciones económicas, como habrá podido comprobar. Le debemos, en consecuencia, un agradecimiento to general, por parte de los inte- resados en el flamenco, en la cultura andaluz, así como a los restantes organismos y entida- des que han colaborado en esta actividad. Pero, aunque exista aportación oficial, si no existen los hombres, las mujeres, las personas, que se encarguen en la práctica de llevar a cabo el proyecto, las ideas no se concretan, las cosas no se realizan. Don Francisco Calzado, autor del libro que presentamos, ha sido uno de los pilares fundamentales en la puesta en práctica de lo que en un momento fue solamente una idea. Y Paco representa, no sólo al buen aficionado lucentino, que lo es, sino también al intelectual que se preocu\n\n[ENDING CONTEXT]\n\ncuando se han realizado tres cosas: tener un hijo, plantar un árbol, escribir un libro. Si no has plantado todavía un árbol, debes hacerlo inmediatamente, porque tienes estupendos hijos (y una mujer, un tanto en penumbra, pero que es un apoyo fundamental para la actividad intelectual) y este es un buen libro, al que podríamos aplicar una frase que tú has rescatado de la memoria o del olvido popular: «Te lusiste, Tamarín». Tú sabes lo que significa, al igual que el público cuando lea estas páginas. Y ya que has aprendido a hacer libros, pues sigue. Adelante, pues, y enhora-buena maestro.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "“Los fandangos de Lucena”, un libro de Paco Calzado",
    "periodical": "candil",
    "issue_id": "1998-09",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "9-12",
    "page_number": 9,
    "word_count": 3196,
    "article_char_count_full": 19521,
    "article_char_count_review": 3417,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "Arte"
      }
    ]
  },
  {
    "article_id": "1998-09-12-right-cr-nica-del-xxvi-congreso-de-art",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLucena. Del 31 de agosto al 5 de septiembre de 1998\n\nJosé Luis Buendía López\n\nFotografías: Bernardo Estepa\n\nLucena gusta de llamarse a sí misma: «Perla de las tres culturas», significando con ello el orgullo de su población al recordar para la Historia que, en su trazado blanco, suavemente inclinado sobre un talud que derrama sus calles y sus plazas hacia la campiña, convivieron a lo largo de siglos tanto árabes y judíos, como los cristianos vencedores de las escaramuzas bélicas del medioevo. De todas aquellas presencias quedan hermosos restos, a través de los cuales podemos reconstruir ese pasado de tolerancia que ha influido en el carácter ubicuo y acogedor de sus habitantes actuales.\n\nHasta esta espléndida ciudad, si en otro tiempo significativo testigo de la historia, hoy próspera\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"artesanas\"]\n\nron a lo largo de siglos tanto árabes y judíos, como los cristianos vencedores de las escaramuzas bélicas del medioevo. De todas aquellas presencias quedan hermosos restos, a través de los cuales podemos reconstruir ese pasado de tolerancia que ha influido en el carácter ubicuo y acogedor de sus habitantes actuales. Hasta esta espléndida ciudad, si en otro tiempo significativo testigo de la historia, hoy próspera población plegada de industrias artesanas y un campo fértil y generoso con el esfuerzo de sus habitantes, nos trasladamos el grupo habitual de congresistas que formamos esa gran familia de entusiastas del flamenco que, desde hace más de un cuarto de siglo, nos reunimos en diversos lugares para dilucidar el estudio de los temas jondos, intercambiar experiencias y animarnos en la tarea, no siempre bien entendida por algunos, de intentar apuntalar el ya vetusto edificio del flamenco. Prólogo precongresual Vencidos los rigores del terrible mes de agosto, capaz de secar las ideas y hasta los sentimientos, Lucena mostró su faz suave para recibirnos el lunes, 31 de agosto, con un clima dulce y perfumado, que parecía anunciar la placid\n\n[EVIDENCE WINDOW 2 | retrieval_hint=COMM_01 | trigger=\"fuera\"]\n\npertenencias en ambas materias y poder suplir, mediante la adquisición de las mismas, nuestras carencias. El castillo, concretamente su patio de armas, sirvió de escenario, como habría de servir durante todo el Congreso, del primer espectáculo flamenco para deleite de los que en su búsqueda habíamos acudido. En esta ocasión, y con un sabio criterio de progresión, se procuró comenzar con actuaciones amables y teñidas del sabor del folklore, como fueran la Panda de Verdiales de Comares y el grupo local de baile de Araceli Hidalgo, que llenaron de alegría festera el primer tramo de nuestra estancia lucentina. Cuando nos marchábamos por el portillo de salida, pudimos disfrutar una luna casi llena que se ocultaba en los contraluces de la iglesia de San Mateo, vecina de la fortaleza, y, a lo largo de estos cinco días, la hemos visto engordar, alegrarnos con su luz las veladas artísticas, hincharse\n\n[ENDING CONTEXT]\n\nla cena de clausura en los reputados salones de «La Abadía». Después de la misma, y tras los discursos de rigor, nos echamos a la calle a respirar ese olor a flores de sus barrios judíos, a disfrutar de los últimos minutos de la luna que nos había acompañado en esta semana inolvidable y que ahora se esconderá y desaparecerá, once veces consecutivas, entre los muros del castillo para alumbrar, al mes siguiente, un camino de reencuentro que conducirá a la isla de San Fernando, entre salinas y esteros, donde todavía se escucha a los chiquillos cantiñando a compás el último cante de Camarón.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XXVI Congreso de Arte Flamenco",
    "periodical": "candil",
    "issue_id": "1998-09",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "12-15",
    "page_number": 12,
    "word_count": 3579,
    "article_char_count_full": 21927,
    "article_char_count_review": 3747,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "artesanas"
      },
      {
        "window": 2,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "fuera"
      }
    ]
  },
  {
    "article_id": "1998-09-16-right-manolo-de-caracol-a-prop-sito-de",
    "article_text_for_review": "Joaquín Rojas Gallardo\n\nHace meses ha salido al mercado un compacto con las reediciones de viejos discos de pizarra del año 1930, primeras grabaciones del llamado por entonces «Niño Caracol», acompañado a la guitarra por Manolo de Badajoz y editado por la casa Pasarela.\n\nUna vez más tenemos que celebrar el magnífico trabajo de producción de ese gran aficionado que es Manuel Cerrejón al ponernos en bandeja unas audiciones con un sonido muy alejado de aquel de «freír pescado» que hemos tenido que sufrir los cuatro locos que estamos en el manicomio del cante y que de esta manera ser-\n\nManolo de Badajoz acompañando a “Niño Caracol”\n\nEn página siguiente, Manolo de Badajoz con Fernando el Herrero, Perico el del Lunar, Manolo Pavón y Pepita Caballero\n\nvirá para deleite de un público más numeroso.\n\nIndependientemente de la categoría cantaora que ya apunta el joven Caracol y que yo ahora no voy a analizar aquí porque otros ya lo han hecho en estas mismas páginas, sí he de resaltar la riqueza de datos biográficos contenidos en la cará-tula estratégicamente ordenada en la que se puede tener una idea muy rica en conocimientos del insigne cantaor.\n\nPero (siempre hay un pero en un buen trabajo) observo con gran desilusión, que de Manolo de Badajoz no se mienta más que su nombre y gracias. Creo que no es justo, más aún cuando la reseña biográfica del cantaor es generosísima.\n\n¿Tan mal guitarrista era Manolo de Badajoz para figurar sólo su nombre?, o ¿es que volvemos a los tiempos antiguos en los que no se mencionaba el nombre del tocaor y que tantas veces hemos criticado?; o ¿acaso no merece mayor referencia porque no es andaluz? Yo quisiera achacar este error a un lapsus (imperdonable si se quiere) pero no a mala intención.\n\nManolo de Badajoz (1889-1962), ha sido uno de los mejores guitarristas de acompañamiento que ha dado la historia del flamenco. Discípulo de Ramón Montoya y Javier Molina, se llevó a la tumba, entre otras cosas, la satisfacción de ser uno de los líderes, junto a Miguel Borrull, Niño Ricardo y Montoya en grabaciones de discos de pizarra. Hay constancia de alrededor de setecientas de estas grabaciones y lo hizo con todas las grandes figuras de su tiempo. Por tanto, no se puede concebir el estudio de las fuentes del flamenco que es el que está contenido en las llamadas placas de pizarra, sin acudir a su manera de ejecutar, en definitiva a su arte. Fue asesor de la casa Odeón aunque grabó con casi todas las existentes por aquel entonces.\n\nSe erigió por derecho propio tanto como artista como por persona de exquisitos modales de educación y buen vestir, en el mandamás de Villa Rosa, núcleo flamenco por excelencia del Madrid de aquellos años y en donde paraban todos los artistas. No había fiesta en la que no interviniera. Siempre se le pedía su opinión y siempre respetó al máximo la categoría de cada cual, como por ejemplo en el caso de D. Ramón Montoya, a quien siempre recomendaba sacrificando su propia posibilidad de trabajo. Y no son estas opiniones entusiásticas en razón de mi paisanja, son comentarios hechos por los que le conocieron; y si no, pregunten a\n\nEnrique Orozco o a Chano Lobato.\n\nPor todo ello nos tenemos que preguntar: ¿A qué se debe esa parquedad de referencias biográficas hacia su persona, en un trabajo que es ejemplo de todo lo contrario? Me gustaría comprobar qué reconocimiento artístico tendría mi paisano, en el caso de que hubiese nacido en Andalucía.\n\nQuede constancia del análisis crítico que de su arte esbozamos en la presentación del disco editado con motivo del XVIII Congreso Nacional de Actividades Flamencas celebrado en Badajoz en el año 1990:\n\n«Dotado de una especial sensibilidad a la hora de acompañar, concebía el cante como verdadero protagonista en la interpretación y de esa manera de pensar, salía un dechado de virtudes de lo que debe ser un buen acompañamiento: tonos armónicos, compás justo, falsetas a tiempo y firme propósito de no molestar en ningún momento al cantaor, de ayudarle en todo lo posible no sólo con el instrumento de las seis cuerdas, sino también con su especial manera de jalear».",
    "title": "Manolo Caracol, a propósito del “Niño Caracol”",
    "periodical": "candil",
    "issue_id": "1998-09",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "16-17",
    "page_number": 16,
    "word_count": 702,
    "article_char_count_full": 4103,
    "article_char_count_review": 4103,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-09-17-right-rafael-do-a-fern-ndez",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nLuis Soler Guevara y Ramón Soler Díaz\n\nRafael Doña Fernández es un aficionado nacido en Málaga en 1918. Testigo directo del flamenco que se ha desarrollado en los últimos setenta años, es hoy toda una institución para los amantes del flamenco en Málaga. Su sabiduría le ha hecho apartarse siempre del halago fácil y de la foto de oportunidad con una discreción y humildad ejemplares. A Rafael le interesa más la tertulia, el trato amistoso con las personas, por eso se le puede ver normalmente en el bar que tiene en la Plaza Bailén, donde tuvimos esta entrevista, y en la barbería de Agustín Alcaide, lugar habitual de encuentro de muchos flamencos malagueños. En estos sitios imparte sus enseñanzas a todo aquel que lo desee, así como en cualquier punto donde haya flamenco de su agrado y que no\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"pasión\"]\n\nsta entrevista, y en la barbería de Agustín Alcaide, lugar habitual de encuentro de muchos flamencos malagueños. En estos sitios imparte sus enseñanzas a todo aquel que lo desee, así como en cualquier punto donde haya flamenco de su agrado y que no le coja muy lejos de casa. En las reuniones este octogenario aún se entona por malagueñas, por siguiriyas de Manuel, por fandangos del Carbonerillo y del Corruco o por guajiras, con el mismo candor y pasión con que un chaval de veinte años abre la boca por primera vez en esto de lo jondo. Toda una vida dedicada al cante merece la atención de la afi- ción, por ello creemos que esta charla puede resultar de interés para conocer un poco más la apasionante historia de nuestro arte. —¿La afición al cante de dónde le viene? —Cuando tenía siete años me quedé huérfano con cuatro hermanos y mi madre. Iba yo a ayudarle a varear unos olivos, ahí en Teatinos, con un señor que se llamaba Andrés, que cantaba algo. Las primeras coplas que le escuché eran unos fandangos que decían: El rey se pasea en coche y el gobernador en calesa, y yo con tanta grandeza ando de día y de noche. Puñales que son de acero que me ha dicho un guapetón que me va a cortar el cuello si será del camisón. —Hacia la siguiriya. Como resulta que éramos tan pobres, que yo vendía periódicos usaos, ífijate tú!, pa la cocina, muchas —Luego, esa afición, ¿hacia dónde fue derivando? veces había una gramola de esas de un tal Ezequiel, que en aquel tiempo estaba de moda. Allí escuchaba a Manuel cantando siguiriγas, y aquello me llegaba al alma, Cuando decía la siguiriγa esa de Joaquín Lacherna... tanto como estaba sufriendo mi madre, que hasta se tomó cajillas de mixtos pa envenenarse porque no podía la pobre en aquel tiempo... Y entonces sentí la siguiriγa como algo propio. Aluego ya seguí con tos los cantes, mayormente los cantes grandes, los cantes por soleá. Conoci a Frasquito Jiménez y a Loriguillo, que eran de Coín y cantaban serranas, pero a palo seco, sin meterle la liviana ni lo de María Borrico. Después me metí a hacer canastos en un taller. Venían mucha gente de Vélez, un tal Federo, Manolo Lupiáñez, que cantaban bien, y éstos fueron los que me llevaron a ver a Manuel Torre cuando vino. Eso fue por el 29, en el Parque de la Merced, por donde está la Plaza de la Merced, un circo que daban allí también cosas de boxeo y cosas de esas. Estuvo mu bien. Cuando yo sentí a Manuel, me acuerdo que estaba cantando por siguirias divinamente y como hay alguien del público siempre que mete la p\n\n[ENDING CONTEXT]\n\nlas reuniones que había antes en los bautizos, los casamientos y eso... Hoy, el rock ha abarcao a mucha juventud. Además, hay otra cosa: a los festivales la gente no va a escuchar, va a divertirse. Antes, en el teatro, sí iban a escuchar. Eran dos horas de cante, pero dos horas que estaban atentos, y cuando uno cantaba mal o estaba en desacuerdo, el público lo abucheaba y tenía que meterse pa dentro. El público exigía. Hoy no se exige, hoy sale un cualquiera y le aplauden y el tío se hincha de cantar y otra vez y otra vez... Y está uno diciendo: pero bueno, ¿qué le está viendo el público a\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Rafael Doña Fernández (Entrevista)",
    "periodical": "candil",
    "issue_id": "1998-09",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "17-21",
    "page_number": 17,
    "word_count": 5091,
    "article_char_count_full": 27593,
    "article_char_count_review": 4153,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "pasión"
      }
    ]
  }
]
```
