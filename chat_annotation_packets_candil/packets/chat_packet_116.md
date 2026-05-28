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
    "article_id": "1985-09-3-right-editorial",
    "article_text_for_review": "Editorial\n\nXIII Congreso de Actividades Flamencas\n\nE l pasado septiembre, Huelva fue exquisito anfitrión para los doscientos escasos congresistas que asistimos a esa cita anual «de actividades flamencas». Algunos análisis que, sobre el desarrollo de este Congreso, se han realizado, en letra impresa, incurren, a nuestro juicio, en superficial cicatería.\n\nSe subrayan carencias, que sólo atañen a los aspectos accesorios de la cita; se contempla el programa y su grado de realización, por la vía de un intranscendente anecdotario. En definitiva, se enjuicia el XIII Congreso, desde una, en ocasiones, frívola perspectiva, sin que aquél merezca una reflexión más global y rigurosa sobre aspectos que deben considerarse esenciales en el mismo. Con ello, no pretendemos enaltercer un Congreso, que al igual que en ediciones anteriores, ha venido adoleciendo de falta de operatividad, de sentido del rigor. Los debates no fluyeron, se atropellaron, en gran parte, como consecuencia de una Mesa que no supo, en ningún momento, moderar. Pero este déficit, con ser importantísimo, no es, en ningún caso, imputable a la Organización de Huelva, sino a la imprevisión, a la falta de madurez de los propios congresistas que elegimos, año tras año, y con sistemático sentido de la inoportunidad, para dirigir los debates del Congreso, a los miembros más probos o que gozan de más carisma popular, pero no a los más capacitados. El XIII Congreso tropezó en los debates, se embarulló innecesariamente. Fue poco operativo.\n\nHuelva se limitó a ofrecernos un entorno adecuado, un ambiente cordial que propiciara el trabajo que muchos hicimos mal. No es equitativo el que proyectemos sobre la bella ciudad del descubrimiento, nuestra propia incapacidad.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "editorial",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 270,
    "article_char_count_full": 1735,
    "article_char_count_review": 1735,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-09-4-left-hip-rbole-po-tica-en-las-coplas-",
    "article_text_for_review": "D e poesía plena y sugestiva, bien surtida siempre anduvo la tema—tica en las «letras» del cante flamenco. Vamos a repasar unas pocas para el debido coteje de nuestra aseveración y, en tanto, el reguste de nuestros sentidos de aficionados netos a ambas parcelas artísticas.\n\nTendida sobre la arena eran sus muslos morenos dos delfines de canela. Y no sé porqué será el agua que lleva el río va diciendo: Soledad. (Juan Rejano)\n\nEl poeta así lo ve y oye y así lo manifesta y es muy bonito, aunque en realidad el agua vaya gorgoreando intermitido o a rachas una especie de «gluá-gluá-gluá» en su deslizamiento por el natural cauce.\n\nQue tu corazón y el mío pusieron a repicar toítos los campanarios que siembra la cristiandad. (F. Moreno Galván)\n\n¿Habrá consecución mayor que echar al vuelo tantísima campana en son de ¡hosanna! por dos corazones enamorados?\n\nYo te voy a regalar si tú te vienes conmigo una casita de viento y una torre de suspiros. (J. Luis Buendía)\n\nInmensurable oferta en lo espiritual al ser amado.\n\nRafaeles de piedra por toítas partes a que en Córdoba, amigo, no te desmandes. (J. González Estrada)\n\nEl patrón cordobés multiplicado en estatuillas que vigilan tus pasos por entre toda la ciudad de callejas y «rincones».\n\nQuerer no es decir te quiero que es coger el corazón y dárselo al compañero. (A. Fernández Malo)\n\nAquí sobran las palabras ante el hecho tajante, hermosísimo, incomparable de dar lo más valioso que poseemos desinteresadamente. Por J. Márquez Cabello\n\nEn el cristal del arroyo donde mi yegua bebía se reflejó tu hermosura y ella beber no quería por no romper tu figura. (J. M. C.)\n\nMayúscula ilusión tomar la desgana del sumiso animal equino como admiración y respeto a la dama de sus sueños, cuya imagen no puede haberse reflejado por el lógico movimiento de oda en las aguas de la acequia.\n\nCuando la alarma sonaba en la mina del «Lentejo» hasta el más duro apretaba el metal de una medalla: la Virgen de Linarejos. (Ramón Porras)\n\nEn ese crítico instante, todo humano, sin excepción, el más descorazonado, echa mano al diminuto amuleto que tal pueblo en general venera. Y para finalizar, traemos otras cuantas muestras donde las similitudes y lisonjas rayan a años luz, pero por ello, quizá, contengan enormes connotaciones poéticas.\n\nCómo quieres que las olas no den perlas a millares si a la orillita del mar te vi llorar una tarde. (Chacón)\n\nAquí atribuye el autor de la «letra» a las lágrimas de la amada poderes mágicos en su consternación para convertir en alhajas cuantas arenas mueven las olas del inmenso mar.\n\nLa mar se viste de luto, los peces mueren de pena, los árboles no dan fruto porque ha muerto mi morena con la que vivía a gusto.\n\n(Don Antonio Chacón) Son apreciaciones del lamentador, ficticias en lo real pero verídicas en su fuero interno que cree lo que expresa deshecho por su infortunio. Esta malagueña, un tal Gayarrito se la escuchó a Chacón en los madriles y de él la recogió y se la apropió. Don Antonio fue, según la tradición oral, quien la fraguó extrayéndola del modelo «Ni mancha ningún linaje...» del malagueño cantaor «Maestro Ojana». Y al oírse la Chacón luego a Gayarrito, más dulzona y melismática, renunció a ella intuyendo que en boca de un «tercer clase» no llegarían muy lejos ni una ni otro. En efecto, no se equivocó un ápice.",
    "title": "Hipérbole poética en las coplas del cante",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "4-4",
    "page_number": 4,
    "word_count": 573,
    "article_char_count_full": 3315,
    "article_char_count_review": 3315,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-09-4-right-prop-sito-de-la-palabra-payo-y-d",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nE n el número 38 de CANDIL, Manuel Barrios recuerda su conferencia del Congreso de Cáceres, después de la cual sucumbi a la tentación de intervenir para aclarar el sentido o la etimología de alguna palabra del lenguaje caló citada por el conferenciante. Una intervención, en tales circunstancias, tiene que ser muy breve para no ser molesta y no parecer demasiado pedante, de modo que pudiera\n\nPor Bernard Leblon\n\nresultar tajante o ambigua; por eso, le agradez-co a mi interlocutor la oportunidad que me ofrece de contestarle más detenidamente y de disipar, eventualmente, cualquier equívoco.\n\nEstoy perfectamente de acuerdo con Manuel Barrios sobre el fondo de la cuestión que expone en su artículo, o sea, sobre los abusos de un caló espúreo, improvisado o reconstituido por los que Borrow\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_02 | trigger=\"expresión\"]\n\nado pedante, de modo que pudiera Por Bernard Leblon resultar tajante o ambigua; por eso, le agradez-co a mi interlocutor la oportunidad que me ofrece de contestarle más detenidamente y de disipar, eventualmente, cualquier equívoco. Estoy perfectamente de acuerdo con Manuel Barrios sobre el fondo de la cuestión que expone en su artículo, o sea, sobre los abusos de un caló espúreo, improvisado o reconstituido por los que Borrow designaba con la expresión: «los de la afición». El problema es que el propio Borrow ha sido uno de los primeros en falsear el vocabulario caló mezclándolo con el de la germanía e inventando alegremente las palabras que le hacían falta para sus «traducciones». Todos los diccionarios que se han sucedido hasta nuestros días proceden del vocabulario publicado por Borrow en 1841, añadiendo, cual más, cual menos, otras fantasías y rarezas; es cosa que puedo asegurar después de haber recopilado y cotejado pacientemente durante años trece de ellos, que son, según creo, los únicos publicados desde el siglo XIX, dejando aparte algunas reediciones con modificaciones de títulos e incluso, a veces, disfraz del nombre del autor. Por lo tanto, es sumamente difícil encontrar en esta marña el recto camino de las\n\n[ENDING CONTEXT]\n\naficionados al flamenco y amantes de Andalucía, seamos naturales o extranjeros. Me es grato recordar, en cambio, que si el flamenco nació en Andalucía y no en otra parte, ello se debe en gran parte a la tradición hospitalaria y antirracista de los andaluces, gracias a la cual se establecieron en sus pueblos, desde el siglo XVI —en tiempos de persecuciones—, algunas de aquellas familias gitanas de donde salieron dinastías de cantaores.\n\n(1) A.G.S., Estado, leg. 157, fol. 12. (2) Archivo General de Simancas, leg. 1.006, exp. 3, fls. 8-9. (3) BARRIOS, M., Proceso al gitanismo, pág. 22, 36.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A propósito de la palabra «payo» y de algunos excesos verbales",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 1781,
    "article_char_count_full": 10629,
    "article_char_count_review": 2856,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_02",
        "family": "CRIT",
        "trigger": "expresión"
      }
    ]
  },
  {
    "article_id": "1985-09-6-left-a-vueltas-con-el-fandango",
    "article_text_for_review": "«El origen de la palabra fandango es incierto; posiblemente se derive del vocablo português «fado», que sirve para designar un canto y baile típico. Y aunque era conocido hacia fines del siglo XVII —en Portugal a comienzos del XVI se usó «esfandangado» para designar un canto popular—, la primera vez que hallamos el término es un entremés de 1705, El novio de la aldena. (Del libro «Magna antologia del cante flamenco»).\n\nF andangos jerezanos de Manuel Torre. Fandangos personales de Rafael Ramos Antúnez, El Gloria. Fandangos-granános de José Cepero que uno alcanzó a escucharle con el toque del viejo (no tanto entonces) Perrico del Lunar; otros fandangos oídos en muchas ocasiones de su incopiado creador, los de Gabriel Díaz, Macandé. Fandangos limpios y briosos del inolvidable Frasquito granaíno. Fandangos de Lucena, en la voz hermosa y valiente limpiamente conservada de Cayetano Muriel, Niño de Cabra. Fandangos en compás de bulerías por soleá de Manuel Vallejo. Fandangos hasta de la gran Pastora. Mínimos y excelsos fandangos de Enrique el Almendro. Fandangos recios (en su primigenio creador) que en la aspereza de su voz tuvo uno la suerte de escuchar centenares de veces a Rafael de la Rosa, Rafael el Tuerto, que de él saltaron a través de El Rubio y de Ramón el Português al maestro José Monge, Camarón de la Isla. Fandango verdialero (que no malagueña) de Antonio Ortega Escalona, Juan Breva. Fandangos del Gordito de Triana; del Niño León (Leoncito para sus íntimos); de Chiquito de Triana, hace años radicado en México y por cuya suerte hacemos los más fervientes votos. Fandangos aunando el amor, la sentencia y la pena de José Torres Garzón, Pepe Pinto, que para ser más flamenco que nadie se esposó con\n\nel Cante. Fandangos airosos y fuertes de la tierra adamuceña, del Niño de Adamuz. Y como no podían faltar ni debían omitirse, fandangos inimitables de don José Tejada, Niño (mejor que Pepe) Marchena, prodigio de una garganta irrepetible.\n\nFandangos hasta de Tía Pastora Pavón, la genial Niña de los Peines; fandangos de la Niña de la Puebla y, quizás sobre todos, fandangos de Manuel Ortega Juárez, Caracol, que si no creador, fue acaso y junto al coloso Manuel, quien tuvo el mérito de engrandecerlos y magnificarlos.\n\nMención especial también es obligado hacer del fandango minero de Antonio Piñana, ya\n\nsea titulado como tal fandango o como otros cantes de la rica tierra cantaora de Cartagena que se copa de distintas denominaciones, no disimulan su estructura de puro fandango, incluso en versiones tan hermosas como la minera o la levantica.\n\nFandangos que señalaron una época y personificaron unos nombres que constituyen una parte no indiferente del cante que llamamos flamenco. Fandangos de especialistas o fandangos como los que hacían voz, se colocaban para entrar en el cante grande, quienes sin ser exclusivos intérpretes de estos cantes, supieron cantarlos y engrandecerlos. Incluso uno, entre otros nombrados, que tuvo su época y su fama y se coció en los hornos trianeros, hoy, también como otros citados, perdido en el olvido: el de El Peluso. Viene después el alud, la riada desbordada del fandango o marcha-martillo que todo lo invade y todo lo domina, no ya el Perosanz y hermanos mártires de la copla andaluz, sino el popular que avenan nuevas voces: el Carbonero, el Sevillano, el Niño de la Calzada, Palanca, Aznalcóllar, Corruco de Algeciras, Rosa Fina de Casares, Angel de Alora, Manzanito de Castuera, Fregenal, dichos sean enumerados en un orden atrabiliario que no denota, obviamente, ni preferencias ni postergación.\n\n* * *\n\nAños de 1984 y 1985. Vuelve de nuevo el fandango. Durante este período, incluso un poco antes, nadie que se precie de su propio arte ha grabado un disco (acaso una sola excepción) sin incluir los inevitables fandangos. En los festivales, el fandango ya no es cante de apertura, de entonación, de abrir sendero; antes al contrario, es la coda y remate, el macho de todos ante los cantes. Y cuando el artista de turno no ha logrado encender la lámpara de Aladino del entusiasmo ni con soleares ni siguiriyas, ni con cantiñas ni con malagueñas, su tabla de salvación, el ¡Viva Cartagena! que deja impoluta su categoría es —quién lo diría— el fandango. El fandango mientras más largo mejor. Porque pareciera hoy que la grandeza cantara se mide por el tiempo durante el que el artista deja de respirar, como un submarinista que a pulmón libre bajara a las profundidades marinas.\n\nEl fandango, ahora más que nunca, mueve y convueve multitudes. (Bien es cierto que el cante hasta hace poco mal podía moverlas y conmoverlas, cuando no las reunía). Ante este entusiasmo exaltado que contemplamos no sin un incurable escepticismo, cabe preguntarse: ¿Pero por qué no ahora, como entonces, se hacen fandangos personales? ¿A qué nivel de imaginación y de capacidad creadora (recreadora está mejor dicho) hemos descendido? Si el retorno del fandango es inevitable, máxima cuando se trata de infundirle un rango de cante superior, lo menos que hay que pedir es que se huya de mimetismos, copias e imitaciones, tan deleznables siempre como todas las imitaciones, incluso en este cante menor que no requiere de inspiración jonda. Venga en buena hora el fandango, valga el fandango regional o localista (Jerez, Granada, Lucena, Verdiales...), pero vamos a conservar los fandangos personales como patrimonio de sus creadores que sólo a título de evocación singular y en actos definidos, deben ser cantados. (Del fandango de Huelva, que es otra cosa, no procede hacer mención aquí). Mientras tanto, dejemos todos estos tradicionales para ocasiones de excepción y esperemos que florezcan nuevos estilos personales.",
    "title": "A vueltas con el fandango",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "6-6",
    "page_number": 6,
    "word_count": 930,
    "article_char_count_full": 5674,
    "article_char_count_review": 5674,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1985-09-7-left-versos-de-oto-o-para-macande",
    "article_text_for_review": "Con escasa medición tu cuerpo, se aligeraba sorteante en un dédalo de estrellas. Y tu voz como un trueno espantaba las nubes aguitarradas hasta dejar caer un torrencial de caramelos sobre el enigmático santuario del cante.\n\nY oigo aquí cerquita, Macandé, el pregonar flamenco de tu agridulce mercancía (de aguardiente, menta y limón) que retumba en cada esquina universal en donde tu divina locura, sin aliento se entretiene en ponerle al viento los cuernos.\n\nHoy eres fotografía histórica, amarillenta, con Carlos Montoya a tu lado con una guitarra continental y tu voz presentida en la solemnidad hierática de tu pose.\n\nY tu memoria\n\nse me va cayendo poquito a poco, como las hojas de otoño, metamorfoseadas en sutiles cromos que al lubricán, las manos fosforescentes de unos chiquillos se disputan en volver.\n\nY sobrevives, Gabriel, a pesar de las campanas al amanecer.",
    "title": "Versos de Otoño para Macande",
    "periodical": "candil",
    "issue_id": "1985-09",
    "year": 1985,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 142,
    "article_char_count_full": 872,
    "article_char_count_review": 872,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
