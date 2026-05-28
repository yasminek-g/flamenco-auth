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
    "article_id": "1990-11-7-left-fernanda-y-bernarda-de-utrera-el",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Martín Martín\n\nE n un tiempo que está llamado a la invasión del tecno-ritmo y al tormento de soportar con locura el flamenco malo; cuando el poder gobernante en Andalucía dilapida el erario público potenciando las mamarrachadas de Juan el Lebrijano en Canal Sur TV y patrocinando desde el cante bufo de Diego Carrasco a la nociva fenomenología de Camarón de la Isla, llega el momento de salir al encuentro de ese flamenco cautivador y mágico que, si bien no satisface las apetencias de las masas —el flamenco es el arte más impopular que existe—, goza de los más altos significados históricos.\n\nA la postre, queremos dibujar la silueta de quienes ejecutan una tradición musical de rango superior. Fernanda y Bernarda de Utrera están en el secreto de que la valía\n\nde un arte se hace más\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"conservar\"]\n\na el momento de salir al encuentro de ese flamenco cautivador y mágico que, si bien no satisface las apetencias de las masas —el flamenco es el arte más impopular que existe—, goza de los más altos significados históricos. A la postre, queremos dibujar la silueta de quienes ejecutan una tradición musical de rango superior. Fernanda y Bernarda de Utrera están en el secreto de que la valía de un arte se hace más perdurable por lo que es capaz de conservar que por las precipitadas y efímeras construcciones instrumentales que nos asolan, más destructoras que innovadoras. La bendita casa donde habita el duende está en el número 20 de la utrerana calle Eduardo Dato. Sevillanamente concebida, destaca tanto por la pulcritud como por la buena acogida que los moradores deparan a sus visitantes. De las paredes penden las pinturas de Capuletti, Pepe Moreno y Rafael Guerrero, así como los innumerables reconocimientos y galardones de quienes se saben poseídas por ese totem enduendado que distingue a las cantaoras geniales de los comerciantes de hábil talento. Para que el clima familiar llegara a una intensidad extrema, me hice acompañar de Antonio Torres —Fernanda le debe muchas horas de sueño— y de Ramón Amaya, un fotógrafo profundo al que sólo le salen planos los retratos que hace a Naranjito de Triana. Durante la inolvidable jornada hablamos de todo y de\n\n[ENDING CONTEXT]\n\nque se revuelvan en las sillas.\n\n—Concluimos. No sin antes agradeceros a las dos, así como a vues- tros sobrinos Inés y Luis, las atenciones que habéis tenido con el colectivo Candil. ¿Queréis añadir algo más?\n\nFernanda: Yo hablo ahora en nombre de toda la familia y quisiera despedir esta entrevista con un cante por soleá, como si fuera un abrazo flamenco muy fuerte para todos los lectores y, especialmente, para todos los que hacéis esta revista tan maravillosa que se llama Candil:\n\nAy, por donde quieras que tú ibas decías que yo era tuya, qué caenita m'has echao que me tienes tan segura.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Fernanda y Bernarda de Utrera Ellas, las protagonistas, dicen",
    "periodical": "candil",
    "issue_id": "1990-11",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "6-10",
    "page_number": 6,
    "word_count": 3964,
    "article_char_count_full": 22155,
    "article_char_count_review": 2987,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "conservar"
      }
    ]
  },
  {
    "article_id": "1990-11-10-right-por-cabales",
    "article_text_for_review": "Antonio Escribano Ortiz\n\nNadie ha conseguido jamás que de mis labios brote el nombre del mejor cantaor/ora de esta o aquella época, y mucho menos el/la mejor de la historia del flamenco o de todos los tiempos, como comúnmente suelen expresarse muchos aficionados. En flamenco, el personalismo es el preescolar y el partidismo la Enseñanza General Básica, basándonos en la enseñanza.\n\nNunca existió, existe o existirá el mejor cantar/ora. Innegablemente han existido, existen y existirán los más largos, más completos y más populares. Ahora bien, si tomamos como base la segunda mitad del siglo que estamos viviendo, difícilmente pueda superarse, en mi modesta opinión, el obstáculo flamenco originado por Fernanda y Bernarda Jiménez Peña, cantaoras que en su nombre artístico esgrimen y ondean responsablemente la bandera de su amado lugar de origen, Utrera, rincón de Andalucía donde cultivaron los cantes que vienen diciendo, cantes que no los han igualado los siempre cuestionados mejores cantaores/oras.\n\nCon gratitud manifiesta me permito decir, a fuer de sincero, que en la andadura de mi vida flamenca he sabido de grandes intérpretes del cante por soleá y he tomado partido en mi particular gusto de determinadas escuelas y estilos, pero situado en el plano de la escucha directa y personal, nadie me ha conmovido y llenado tanto flamencamente como Fernanda interpretando las soleares, soleares que para entendernos los aficionados las llamamos de «Utrera».\n\nSin salirme del contexto, envido más al atreverme a decir que, con frecuencia, suelo estremecerme cuando acudo al cante en conserva —discos— y escucho esa tanda que comienza de esta forma:\n\nDe noche yo no caía nunca en cama...\n\nY tras la escucha, soy consecuente y rememoro a La Serneta y Juaniquín rematando aquel cante que dice así:\n\nPor maligno que tú eres, por maligno que tú eres, así coma de mis carnes no has de lograr lo que quieres\n\nFernanda de Utrera, amigos lectores, se instala en el espíritu y compás del cante por soleá de forma magistral y cuasi inimitablemente, cuando menos desde sus inicios hasta la actualidad.\n\nY de Bernarda, su hermana, ¿qué puede decirse de su cante por fiesta? Bernarda de Utrera lo canta todo, lo mucho que sabe de cante, que no es poco, todo lo que sabe de coplas y todo cuanto sabe de la vida, que es bastante. Al definir a Bernarda puede decirse libremente sin temor al yerro, que es una «extraordinaria cantaora festera con un gran talante de existencia por fiesta».\n\nNo deseo en mi breve espacio dejar fisuras que permitan supuestas argumentaciones, que desde ya las cubro diciendo: amigo lector, nadie más que ellas para añorar sus mejores momentos, para padecer porque estas dos joyas no resplandecen hoy con el brillo fulgurante que ayer irradiaban, ni para deplorar el paso de los años.\n\nEl año 1967 tuve la dicha de conocerlas en el Tablao Flamenco Villa Rosa, de Madrid. En el espectáculo hacía su flamenco al piano don José Romero. En el descanso de sus actuaciones acudíamos al bar La Espuela, sito en la plaza de Santa Ana, a tomarnos unos pinchitos morunos regados con alguna copichuela de vinito andaluz animador de ambiente. Todos éramos todavía más jóvenes...\n\nRecuerdo una ocasión en la que el citado pianista sentenciaba de la siguiente manera: «si la mejor bailaora es Trini España, la cantaora festera más larga es Bernarda de Utrera. ¿Qué opina de Fernanda?, le pregunté. Fernanda es un monstruo por soleá, respondió.\n\nAl igual que Las Jaberas, Las Parralas, Las Coquineras y otras, estas dos geniales hermanas han entrado en la historia del flamenco, en cuyo libro figuran grabados sus nombres artísticos con letras de oro.\n\n¡Fernanda y Bernarda de Utrera!, ovaciones para vosotras por los siglos de los siglos...",
    "title": "Por cabales",
    "periodical": "candil",
    "issue_id": "1990-11",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 617,
    "article_char_count_full": 3745,
    "article_char_count_review": 3745,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-11-11-left-las-ni-as",
    "article_text_for_review": "Manuel Peña Narváez E scribir de Fernanda y Bernarda... Como ya expuse cuando Pasarela me pidió una ilustración literaria para la carpeta de su último LP, descubrir a estas alturas algo nuevo en la expresión jonda de Fernanda o en el exquisito compás del cante de Bernarda, encontrar nuevos matices en sus modos y en sus formas, adivinar un éxtasis en la contemplación de sus figuras gitanas y en la interpretación de sus cantes, es algo tan sumamente difícil como buscar en el recuerdo el apasionante eco de una Mercé la Serreta o la refrescante brisa de una Pastora genial, cosas que ya se fueron y que nunca más y por nada del mundo habrán de volver.\n\nY a antes, cuando en la antesala de su feria septembrina Utrera descubrió los azulejos que rotulaban una calle dedicada a sus hijas muy preclaras, Fernanda y Bernarda de Utrera —7-9-90—, dije también: enhorabuena, Utrera; enhorabuena, porque Fernanda y Bernarda, son el más claro exponente del estilo jondo y del sello y compás que te caracteriza. Enhorabuena, Utrera, porque has sabido hacer justicia ante una petición lógica, marcando en la historia contemporánea del cante, el reconocimiento a una verdad que se patentiza.\n\nPor si todavía Fernanda y Bernarda —reconocimiento mundial— no le hubiesen dado bastante a Utrera, en la noche del 18 de enero de ese año, en Sevilla, en los salones regios del Alfonso XIII, cuando Fernanda recibía la VI distinción «Compás del Cante», en la grandiosidad de la noche y en la emoción del silente abrazo de Fernanda a su hermana Bernarda, —desplazada incomprensiblemente del premio, no del homenaje—, Utrera resonó con brillantes estelas de oro y su nombre se categorizó en la solemnidad del acto, donde el todo flamenco, el arte y la nobleza, rendían homenaje a algo muy suyo como eran las «niñas» de Utrera.\n\nTambién, con motivo de aquella noche inolvidable, escribí en Sevilla Flamenca de la majestad y empaque de Fernanda, ¡señora!, dueña de sí misma, consciente del papel estelar que le tocaba representar.\n\nY, en verdad, que no resulta nada fácil escribir sobre Fernanda y Bernarda, sobre todo, cuando plumas tan importantes y tan autorizadas como las de Anselmo González Climent, Edgar Neville, Blas Vega —excellentísimas señoras las llama Manuel Martin Martín— y otras, se han ocupado de ello.\n\nSiempre digo, y no me importa repetirlo una vez más, que si de Serafín y Joaquín Alvarez Quintero —utreranos ilustres— se dice que un mismo aliento impulsa las dos velas de su barca literaria, de Fernanda y Bernarda habría que decir que un mismo suspiro hondo anida las dos veces morenas y que un mismo compás alimenta y guía las gargantas supremas de estas gitanas de lujo, donde la soleá y la bulería —entre la rica gama del cante reciben la más alta devoción y la más profunda forma de consagración al arte. Fernanda, expresión del dolor del cante; sugestión del embrujo y del misterio y grandeza sublime de un arte; Bernarda, duende y dulzura a la vez; quiebro y compás a un tiempo, cofre moreno y crisol donde se funden los cantes de Utrera.\n\nPor lo demás, ¿qué más puede decirse de estas niñas fabulosas, emblemas de la pureza, perfil de lo jondo, herencia natural de lo sublime...? Como decía Fernanda muy recientemente a José Antonio Blázquez en ABC: «Mira, José Antonio, lo nuestro es a nuestra forma, a nuestra manera, a la manera de Utrera».\n\nComo su padre, José el de Aurora, traía en sus entrañas el aire flamenco de su estirpe gitana, y su madre, la chacha Inés, hija del legendario Pinini paseaba majestuosa «jechura» gentil de viva estampa de raza, puede decirse que Fernanda y Bernarda aprendieron a cantar en el patio de su casa, entre los suyos.\n\nPues, eso.",
    "title": "Las Niñas",
    "periodical": "candil",
    "issue_id": "1990-11",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 627,
    "article_char_count_full": 3675,
    "article_char_count_review": 3675,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-11-11-right-a-fernanda-y-bernarda",
    "article_text_for_review": "Antonio Corcobado\n\nCuando los sonios negros se generan en la cultura de la sangre nace en su creación más esplendorosa el CANTE.\n\nInvitado por nuestro director a participar en el homenaje a estas dos grandes artistas, por las que de siempre he sentido una gran admiración, no he dudado un momento en facilitar mi aportación porque la grandeza de su arte así lo estaba reclamando.\n\nTuve la ocasión de conocer a estas dos hermanas en su primera llegada a Madrid, donde tuvieron siempre una gran acogida, manteniendo con ellas un trato amistoso y personal muy estimable, hasta que un mal entendido, al que creo que fuimos ajenos ambas partes, nos distanció, no habiendo sido nunca ello causar que redujera en lo más mínimo mi admiración artística que, en las diferentes ramas que con preferencia practican las dos hermanas, y siempre dentro de una pura ortodoxia flamenca, llegando a la expresión de lo perfecto. Son muchas las veces que hemos disfrutado de la intimidad de su arte tanto en los diferentes tablaos y salas de la capital como en el más reducido ambiente de los cuartos de Villa Rosa, desde donde fuimos apreciando la superación de sus ya iniciales buenos estilos.\n\nEn la dificultad de superación que suele poner Fernanda desde el arranque de sus actuaciones, se deja entrever lo elevado del sentimiento de cuanto va a cantar, llevándolo a veces a extremos que parecen insuperables pero que los vence para terminar expresando lo que quiere decir, decires en lo que pone en juego tanto sus capacidades físicas como sentimentales, situándola hoy en el panorama flamenco como artista sin igual dentro de su estilo.\n\nSi difícil es expresar la conjunción física y mental de Fernanda, no menos dificultades representa el enjuiciar a Bernarda, aunque la aparente facilidad de adaptación que tiene Bernarda para expresar con gracia y salero su personal estilo «bulearero», no tenga nada de fácil, como aparentemente parece para sacar su jugo a esta difícil rama del flamenco.\n\nNacidas del mismo tronco familiar y artístico son dos ramas en las que se han decantado diferentes sentimientos que se aprecian muy claramente en la definición de sus gustos cantaores, en las que han llegado al más alto nivel cada una en su estilo, aunque Fernanda es fácil deje una mayor huella dentro del arte por practicar con mayor asiduidad los estilos más ortodoxos del cante.\n\nNo cabe ninguna duda de que cada día es más cierta la definición lorquiana de la cultura de la sangre, que de manera tan extraordinaria se advierte en estas dos hermanas, si tenemos la curiosidad de revisar sus ancestros familiares que también dejaron huella entre los buenos artistas de sus generaciones anteriores.\n\nAunque el paso del tiempo, como en todas las cosas, va dejando su huella en estas dos grandes cantaoras, la merma de facultades se suple crecientemente con un doble sentimiento creando una solera artística que enriquece el gusto de cuantos, con admiración y simpatía, continuamos siguiendo su actuación profesional que quiera Dios sea aún por muchos años.\n\nAl sumarme a este homenaje quiero renoverles mi admiración y respeto, deseándoles los más grandes éxitos en su carrera profesional, en la que aún tienen mucho que decir y cantar.",
    "title": "A Fernanda y Bernarda",
    "periodical": "candil",
    "issue_id": "1990-11",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 534,
    "article_char_count_full": 3217,
    "article_char_count_review": 3217,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1990-11-12-left-expresi-n-vivencial-del-cante-gi",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAlfredo Arrebola\n\nY o también quisiera, compañeras mías, aportar mi granito de arena al homenaje que la Revista Candil, fiel portadora de los diversos sentires flamencos, os quiere tributar. Homenaje más que merecido: habéis consagrado toda vuestra vida —¡que Dios/UNDEBE os conceda larga estancia entre nosotros!— a un arte que, aunque mal conocido y despreciado, forma parte del acervo cultural del pueblo andaluz: EL FLAMENCO en su trilogía de Cante, Baile y Toque.\n\nEs verdad: pocas han sido las ocasiones que hemos figurado en los carteles de festivales flamencos, más bien ha sido en reuniones particulares, la de los «cabales» donde cada uno expone lo que gratuitamente le concedió la «madre naturaleza»: Cantar, Bailar... Tocar. Pero aquí deseo olvidarme de mi condición de cantaor, y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionao\"]\n\ndel acervo cultural del pueblo andaluz: EL FLAMENCO en su trilogía de Cante, Baile y Toque. Es verdad: pocas han sido las ocasiones que hemos figurado en los carteles de festivales flamencos, más bien ha sido en reuniones particulares, la de los «cabales» donde cada uno expone lo que gratuitamente le concedió la «madre naturaleza»: Cantar, Bailar... Tocar. Pero aquí deseo olvidarme de mi condición de cantaor, y ofrecer unas palabrillas como un «aficionao» que siente y vive el flamenco como símbolo de su alimento espiritual. Y digo más: no he sido capaz de poner mis manos sobre la máquina de escribir, si antes no me he emborrachado del cante de Fernanda y Bernarda. De ese cante que las define como un capítulo de gloria en la cultura gitano-andaluza. Sois la vieja tradición del cante puro, del cante gitano, la que reina en vuestro arte; un arte que no se aprende, que se lleva amasado misteriosamente a la propia subsistencia que, como la nobleza, se hereda; un arte que es casta y raza, como magistralmente os definió Ricardo Molina, cfr. «Obra flamenca», pág. 181. Y porque sois casta y raza, deseo ardientemente manifestar qué siento yo por el «CANTE GITANO» que tuve la suerte de mamar en los que jios gitanos del Sacromonte: María l\n\n[ENDING CONTEXT]\n\nbreve, pero profunda, historia del flamenco? Creo que sí.\n\nYo me siento orgulloso de conocer a los gitanos, porque he convivido con ellos; conozco sus vicios, sus defectos... pero también sus muchas cualidades y virtudes morales. Y en el cante son «PIEDRA FUN-DAMENTAL»: La Andonda, Anica Amaya, María Armento, María Borrico, Rosario la Mejorana, Mercedes la Serneta, La Serrana, Niña de los Peines, La Perrata... y vosotras, Fernanda y Bernarda, base inamovible del cante por Soleá y Bulerías. Estoy de acuerdo con Ricardo Molina en que «...sólo la poesía podría expresar lo que sois en el cante».\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Expresión vivencial del cante gitano",
    "periodical": "candil",
    "issue_id": "1990-11",
    "year": 1990,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 968,
    "article_char_count_full": 6011,
    "article_char_count_review": 2871,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "aficionao"
      }
    ]
  }
]
```
