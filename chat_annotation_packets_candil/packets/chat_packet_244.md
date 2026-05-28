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
    "article_id": "1991-11-23-left-bendito-seas-t-tulo-de-opera-fla",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Yerga Lancharro\n\nEn la revista Candil núm. 76 y en la página 747, aparece la primera parte de este trabajo titulado jBendito seas, título de Opera Flamenca!\n\nY como me temía, nadie me ha complacido publicando una nueva colaboración que aclarara a los lectores el porqué de tal título. Nadie lo ha hecho. ¿No será porque no lo saben? Lo que me viene a confirmar que cuando escriben sobre aquella época, lo hacen sin base alguna y de forma peyorativa, como refiriéndose a algo de lo que nos tenemos que avergonzar; a un arte flamenco que deja mucho que desear por su bajísima calidad. Se expresan así porque creen, por su ignorancia, que decir «Opera Flamenca» es de lo más bajo que se pueda expresar. Y no es así. Ignoran, asimismo, que si aquellos artistas cantaban principalmente por\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"público\"]\n\neyorativa, como refiriéndose a algo de lo que nos tenemos que avergonzar; a un arte flamenco que deja mucho que desear por su bajísima calidad. Se expresan así porque creen, por su ignorancia, que decir «Opera Flamenca» es de lo más bajo que se pueda expresar. Y no es así. Ignoran, asimismo, que si aquellos artistas cantaban principalmente por fandangos, fandanguillos, colombianas, milongas y vidalitas, era porque le obligaba a ello el gusto del público de aquella segunda mitad de la década de los años veinte y la de los treinta. Había que cantar a gusto del que pagaba, porque así conseguían llenar los teatros, tablaos, etc., que era lo que al fin y al cabo les interesaba. Durante el mandato de don Miguel Primo de Rivera, el espectáculo flamenco proliferó en Madrid de forma desmesurada. Cierto es que también hubo infinidad de empresarios, de todas las categorías, que contribuyeron sobremanera a que la capital de España se convirtiera en la única «Meca» de nuestro arte en sus tres facetas. Todo artista que conseguía elevarse un poc\n\n[ENDING CONTEXT]\n\ny mucho más lo estuvieron cuando a las pocas semanas supieron que don Miguel había cumplido su palabra. El Ministro dio marcha atrás y de esta forma fue cómo el arte flamenco no murió para siempre en Madrid.\n\nPor todo ello, tengo que terminar esta segunda parte de mi trabajo informativo, como titulé la primera:¡Bendito seas, título de Opera Flamenca!\n\nNOTA IMPORTANTE\n\nTodo cuanto expongo en este trabajo se lo debo a mi recordado y querido amigo José María López-Cepero, que fue quien me informó detenidamente sobre este tema, en su domicilio de la calle de Mesón de Paredes en Madrid. ■\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": ";Bendito seas, título de Opera Flamenca! (y 2)",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "23-23",
    "page_number": 23,
    "word_count": 1068,
    "article_char_count_full": 6396,
    "article_char_count_review": 2659,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "público"
      }
    ]
  },
  {
    "article_id": "1991-11-24-left-los-cantes-flamencos-de-daniel-p",
    "article_text_for_review": "Este libro, Cantes flamencos (*), constituye la última entrega poética del escritor aljarafeño Daniel Pineda Novo y, que sepamos, la primera incursión del autor en los terrenos de la poesía flamenca. Que autores de poesía culta se den a componer coplas en metros populares es algo antiguo y hasta clásico, una característica propia de la literatura española, tan cercana siempre de lo popular. Recuérdese, por ejemplo, el caso de don Luis de Góngora, autor del hermético Polifemo o de las hipercultas Soledades y al mismo tiempo de las letrillas y romances de vuelo y sabor tan popular. Incluso en las profundidades de lo «hondo», o de lo jondo —si se prefiere— tenemos precedentes notorios, como Salvador Rueda o Manuel Machado. Y si de ejemplos mucho más recientes se tratara, ahí tenemos los casos de Aquilino Duque, Félix Grande o José M. Caballero, Donald —por citar sólo tres nombres de poetas actuales consagrados—, autores de letras de flamenco. Así, pues, Pineda Novo se inscribe en una corriente que viene de lejos y que cuenta con suficiente solera y tradición.\n\nEstos Cantes flamencos que hoy nos entrega vienen precedidos de un prólogo del profesor de Literatura Española de la Universidad de Sevilla Pedro M. Piñero Ramírez, que es además presidente de la Fundación Machado, una institución dedicada a la recopilación y estudio de la cultura tradicional andaluza. El libro en sí es breve y su autor lo ha dividido en seis apartados titulados, respectivamente, «Soleares», «Siguiriyas», «Coplas y cantares», «Fandangos», «Saetas» y «Serranas», siguiendo un criterio temático y musical más que métrico. En todo caso, hay en el libro coplas de tres, cuatro, cinco y siete versos. La temática de estas coplas es la propia y característica de la poesía flamenca, y en este sentido hay hallazgos de precioso lirismo, como la soleá que dice: «Pon tu boca con la mía / y bésame poco a poco / jasta que claree el día». Pero hay también innovaciones y actualizaciones temáticas, incorporando al patrimonio de la poesía flamenca preocupaciones del hombre actual o personajes en otro tiempo desconocidos o proscritos. Así, por ejemplo, la temática del paro que surge en esta siguiriya: «Por la caye arriba, / por la caye abajo, / yo camino e noche y e día / buscando trabajo». O el nombre del impulsor mayor de de la moderna autonomía andaluza, en este fandango: «En esta tierra nasió / “El Ideal Andalú”... / Un buen hombre lo creó / y fue un portento de lú: / Blas Infante se llamó». O, también por fandangos, esta exaltación de la simbología autonomista andaluza: «Es verde y blanca la lú / que se mese en mi bandera; / es la ilusión der que espera / bajo este cielo andalú / que najca la primavera».\n\nUn punto que puede ser polémico es el de la ortografía que se utilice para recoger la peculiar pronunciación andaluza que es consustancial a estas coplas. Es un aspecto éste muy discutido y son varios los criterios que distintos especialistas defienden. Daniel Pineda ha optado por acomodar la grafía a la pronunciación, tal vez con excesiva aproximación que, nunca, por otro lado, puede ser completa, ya que para ello se necesitaría hacer una trascripción fonética de cada actualización oral concreta. Es este un punto polémico, como decimos, y bueno sería que algún documentado y fundamentado estudioso echase su cuarto a espadas poniendo los puntos sobre las és de esta debatida cuestión de la ortografía que debe emplearse para las coplas flamencas.\n\n(*) Daniel Pineda Novo, Cantes flamencos, Sevilla, Ediciones Aljarafe, 1991. Prólogo del Dr. Prof. D. Pedro M. Piñero Ramírez.\n\nEn cualquier caso, y cuestiones lingüísticas aparte, en todas estas coplas —desde las más clásicas hasta las de temática más moderna— se nota que su autor es un consumado flamencólogo y que conoce bien el paño que se guarda en el arca de lo jondo. Su identificación con lo popular, con lo sentidamente flamenco, es verdade- ramente notable. Algunas de estas coplas ya han sido cantadas e incluso grabadas en disco por cantaores y cantaoras. Es imposible adivinar cuáles de ellas queda- rán, porque el pueblo las haga suyas. Tal vez dentro de algunos años, en alguna noche flamenca no importa de qué lugar, alcancemos a oír una letra que comenzó su vuelo popular en las páginas de este libro, en el estro fértil y flamenco de Daniel Pineda... Dijimos al principio que los Cantes flamencos del poeta aljarafeño se insertaban en una tradición de añeja y noble solera. Hemos comprobado que Daniel Pineda Novo ha sabido estar a la altura de los mejores hitos de esta tradición que los estudiosos J. M. a Pérez Orozco y Alberto Fernández Bañuls acertaron a denominar en feliz expresión «poesía flamenca, lírica en andaluz».",
    "title": "Los «cantes flamencos» de Daniel Pinedo Novo",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 792,
    "article_char_count_full": 4707,
    "article_char_count_review": 4707,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1991-11-24-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nSiguiendo una costumbre muy extendida en los modernos estudios flamencos, esto es, realizar bosquejos biográficos de los artistas jondos, acaba de publicarse un libro muy agradable y bien presentado, en el que el gran aficionado de la Isla, Salvador Aleu, insiste de forma profunda en esta vena de los datos personales de los artistas en la que han destacado nombres importantes como José Blas Vega, Yerga Lancharro (mencionado expresa y justamente por el prologuista Antonio Murciano), Ríos Vargas o Eugenio Cobo, entre otros nombres de interés.\n\nLa nómina de artistas de San Fernando es, a fuer de completa, bastante desigual, puesto que, junto a fenómenos de la categoría de María Borrico, Camarón o el mítico Juan de Dios, se nos ofrece una multitud de nombres y apellidos, cercana en número a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionados\"]\n\nRíos Vargas o Eugenio Cobo, entre otros nombres de interés. La nómina de artistas de San Fernando es, a fuer de completa, bastante desigual, puesto que, junto a fenómenos de la categoría de María Borrico, Camarón o el mítico Juan de Dios, se nos ofrece una multitud de nombres y apellidos, cercana en número a los setenta, que poco o nada dicen en la historia del flamenco, si bien todos tienen interés humano, porque éste nunca falta en los buenos aficionados. Uno, sin embargo, hubiera preferido que el autor ahondara en las biografías y rasgos cantaores más significativos de los cabeza de serie de cada época, en los fundadores de las dinastías y familias cantaoras de La Isla, que tanto brillo han aportado al cante gaditano, en vez de tanta y difusa minihistoria de nombres poco relevantes. De todos modos, el intento es honrado, el libro se lee con facilidad y no siempre es posible, ni tan siquiera imaginable, contar con obras maestras en esta bibliografía jonda que camina por senderos tan confusos. Título: «FLAMENCOS DE LA ISLA EN EL RECUERDO» Autor: Salvador Aleu Zuazu Prólogo: Antonio Murciano Editora: Isleña de Prensa San Fernando, 1991 FLAMENCOS DE LA ISLA EN EL RECUERDO Salvador Aleu Zuazo Título: «ANDALUCES A COMPÁS» (Mi poesía flamenca, 1950-1990) Autor: Antonio Murciano Editora: Fundación Andaluzas de Flamenco Jerez, 1991 Desde que se abre de capa, Antonio Murci\n\n[ENDING CONTEXT]\n\nirónico de impotencia respondía: «pues, en eso».\n\nMás interés tiene la parte postrera del libro, dedicado a un catálogo, según el autor «incompleto», de artistas flamencos de Linares, pero que nosotros consideramos el más amplio de los efectuados hasta ahora de forma metódica, en el que, junto al rasgo biográfico, no faltan las anécdotas humanas que hacen que la relación sea leída sin desmayo de ningún tipo.\n\nEn resumen, un libro ameno, que si bien no derrama luces definitivas sobre nuestro arte, sí tiene la virtud de motivarnos con elegancia para proseguir en su busca, lo que no es poco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "24-25",
    "page_number": 24,
    "word_count": 1123,
    "article_char_count_full": 6726,
    "article_char_count_review": 3016,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionados"
      }
    ]
  },
  {
    "article_id": "1991-11-26-left-discografia-flamenca",
    "article_text_for_review": "E n un primer lugar hay que reconocer la loable labor de archivo sonoro desarrollada y difundida por aficionados como Gonzalo Rojo, Andrés Zarrías, Yerga Lancharro, Antonio Reina, Miguel Aguilar o Pepe Claro. A través de la misma, aficionados que con trabajoso esfuerzo ampliamos humildemente nuestros conocimientos, con la audición de discos como el que nos ocupa, estamos complementando sonoramente toda la documentación que llega a nuestro\n\npoder y con las suficientes garantias para comprobar cuál es la verdaderamente cierta y, así, poder desechar el aluvión de inexacta información que entorpece la formación de cualquier aficionado que se precie.\n\nHe sentido satisfacción al ir comprobando cómo, en esta tierra que suele ser la «convidada de piedra» en la historia del flamenco y, aunque no con la proliferación que hubiéramos deseado, también se han dado claros ejemplos de creatividad, personalidad artística y adecuado tratamiento de los estilos que conforman nuestro arte. Así, la tan aludida e interpretada malaqueña de Diego Moreno «Personita», está recogida en el disco con la suficiente nitidez para calibrar el caudal creativo de su autor. O como el reiterado modismo «marchenero» de la taranta linarense se subalterna a la valentía y matiz melismático de Juan Soler «El Pescaero» (1), segundo premio en el famoso concurso de Granada del año 1922. Y por qué no citar igualmente la calidad de los artistas linarenses en la ejecución de otros\n\nCantan: Gallico de Linares, Niña de Linares, Lucas Soto «Luquitas de Marchena», Diego Moreno «Personita», Rubia de las Perlas y Juan Soler «El Pescaero».\n\nTocan: José Arqueros, «El Pepe», Miguel Borrull, A. Peana, Ramón Montoya, El Malagueñito, Miguel Valencia y Pepe de Badajoz\n\nEdita: XIX Congreso de Actividades Flamencas de Linares. Comisión organizadora.\n\nReferencia: Fonoruz. D-256. MONTILLA-Córdoba. 1991 estilos que tienen negada raíz en esta tierra, mas no en su adecuado desarrollo como queda demostrado en esta edición.\n\nEn cuanto al contenido total del mismo, a la citada malagueña de «Personita de Linares» hay que sumar, en la voz del propio artista, siguirias efectuadas con el sabor y la costumbre de los cantaores añejos; la variada gama de estilos por Carmen Espinosa «Niña de Linares», con modulaciones de la época; los ecos mineros de La Rubia de las Perlas en la más pura línea ortodoxa de su tiempo y mostradora de la enjundia flamenca de la popular artista del primer tercio de este siglo; el mecenazgo influenciador de Pepe Marchena sobre Lucas Soto «Luquitas», el modismo entremezclador de estilos de la «opereta flamenca» en la voz de Sebastián García «Gallico de Linares» y nuevamente la personal voz de Juan Soler «El Pescarero», en esta ocasión por fandangos. Y todo ello corroborado por el acompañamiento de guitarristas que con sólo leer su nombre, nos vienen a la memoria los más sustanciosos ecos de la añeja discografía flamenca.\n\nEspero que esta primera entrega tenga la continuada elaboración de siguientes ediciones, en las que se puedan ahondar en la solera cantaora de esta tierra. El Ayuntamiento así lo ha sugerido y los aficionados así lo esperamos.\n\n(1) En la contraportada Pedro «El Pescaero».\n\n\"Quédate con el Cante\"\n\nPrograma Flamenco\n\nSintonícenos de lunes a viernes, de 20,30 a 22,00 horas; viernes, sábados y domingos de 0,30 a 3,00 horas, FLAMENCO\n\nVargas, Concha. Natural de Lebrija (Sevilla). 194? Bailaora. Ha participado en las obras de teatro flamenco «Camelamos naquerar», con Mario Maya; «Díálogos con Dios», junto a Curro Fernández, Rafael Riqueni, Antonio Chacón y otros destacados artistas, presentado en el Teatro Lope de Vega sevillano en 1983; y «Persecución», al lado de El Lebrijano. En este mismo año, actúa en la V Quincena de Flamenco y Música Andaluza, celebrada en Sevilla. Su personalidad artística ha sido enjuiciada por Emilio Jiménez Díaz, con las siguientes palabras: «Para atrevernos a decir que es la bailaora gitana más completa de la actualidad, y estar de acuerdo entendidos bien dispares, mucho tiene el baile de Concha Vargas... Su baile es de fiera, de araña, de gitana con arte propio de raza y técnica de pocos».\n\nBAILAORAS DE HOY\n\nConcha Vargas",
    "title": "Discografia flamenca",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "25-27",
    "page_number": 25,
    "word_count": 671,
    "article_char_count_full": 4188,
    "article_char_count_review": 4188,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1992-01-3-left-editorial",
    "article_text_for_review": "Editorial\n\nE n el periódico «Diario 16 Andalucía», correspondiente al día 3 de diciembre de 1991, se publica un interesante trabajo, bajo el título «Cultura de fandango y saeta» del que es autor José M. Vaz de Soto. Se trata de una erudita reflexión sobre los significados del término «cultura», desde una perspectiva histórica del pensamiento europeo, que funda la afirmación del propio autor de que no existe cultura andaluza sino «cultura en Andalucía o con acento andaluz». De no ser por las referencias que Vaz de Soto realiza a lo que, según su criterio, constituye el contenido de la denominada «cultura andaluza», el problema a discutir quedaría reducido a una cuestión puramente semánica, con independencia de que el término traiga o no causa del concepto «Volkgeist» de Herder, en el prerromanticismo alemán. Así, Vaz de Soto reacciona frente a una nueva versión de la España de charanga y pandereta, representada, dice, por el fandango, saeta, chistes, romerías, toros, el flamento, las ferias y las procesiones. Tan desatada e imprecisa generalización evidencia, a nuestro juicio, absoluto desconocimiento de una manifesta-\n\nción artística, peculiarísima, plena de sentido trágico, y musicalmente única: nos estamos refiriendo, lógicamente, al «flamenco». Que a éste se le meta en el mismo saco junto a chistes, romerías, ferias y procesiones nos parece tan poco riguroso como de mal gusto y desde luego, pone de manifiesto que quien tal afirma, no ha siquiera tocado la sustancia de lo jondo. En las tres últimas décadas ha surgido ya una amplia y sólida bibliografía que realizan y fundamentan los valores culturales de lo jondo, y que, cier-\n\ntamente, desconoce Vaz de Soto. No sabemos si la cultura andaluza deba reducirse o no al andalucismo. Probablemente no, máxime si se tiene en cuenta la dimensión política que tal término hoy entraña; tampoco sabemos si resulta riguroso acompañar el concepto cultura con el apellido «andaluza». De lo que no nos cabe la más mínima duda es que el flamenco no es equiparable a un chiste, a una romería o a una procesión. Y si en alguna ocasión hemos hablado de flamenco como cultura andaluza, era precisamente para diferenciarlo de lo que no se integra en el «espíritu del pueblo al que se pertenece...» y también para acentuar el carácter específicamente andaluz de lo jondo. No creemos que por tal afirmación compartamos conviciones con el señor «Le Pen», y ni estamos desarrollando una doctrina reaccionaria. Resulta lamentable el advertir con qué propiedad Vaz de Soto descubre la línea de enlace entre Herder, Maistre y Spengler y, sin embargo, no es capaz de distinguir entre las frivolidades de tanto y tanto payaso andaluzado y el grito estremecedor de una si-guiriya.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1992-01",
    "year": 1992,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 446,
    "article_char_count_full": 2731,
    "article_char_count_review": 2731,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
