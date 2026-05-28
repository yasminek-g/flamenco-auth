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
    "article_id": "1988-11-15-left-viejo-carn-flamenco",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(Madrid, ma $ \\underline{\\text{yo}} $ de 1967)\n\na publicación periódica de una de las obras inéditas de Anselmo González Climent, en esta Revista, va a proseguir, pese al imprevisto fallecimiento del maestro y amigo. «Viejo Carné Flamenco» es una colección de entrevistas con relevantes protagonistas del Cante y de la Fiesta, realizadas en las décadas de los cincuenta y sesenta.\n\nEs cierto que Anselmo González Climent pretendía modificar algunos puntos de análisis que, ciertamente la perspectiva del tiempo, aconsejaba su revisión. Ello, no obstante, el Grupo CANDIL, previa autorización de la esposa del escritor desaparecido, ha querido ofrecer a sus lectores este trabajo tan lleno de frescura, pese al tiempo transcurrido, tan penetrado en agudeza, rigor y sentido de la\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Granada\"]\n\nta. Es cierto que Anselmo González Climent pretendía modificar algunos puntos de análisis que, ciertamente la perspectiva del tiempo, aconsejaba su revisión. Ello, no obstante, el Grupo CANDIL, previa autorización de la esposa del escritor desaparecido, ha querido ofrecer a sus lectores este trabajo tan lleno de frescura, pese al tiempo transcurrido, tan penetrado en agudeza, rigor y sentido de la oportunidad. M\\textsuperscript{orente.—Nací en Granada, el día 23 de enero de 1963, perdón, de 1942. Es que el número del carnet... Anselmo González Climent.—En tu tierra ¿tomaste el cante ambiental, de paso, o tuviste algunos maestros concretos? Morente.—No definitivamente. Yo cantaba lo que se me pegaba de los chavales. A mí desde niño me gustaba el cante. Había una peña en mi barrio —la mayoría borrachos— y solían llamarme a mi casa para ir a la taberna. Ni me acuerdo apenas cómo hacía eso y cómo cantaba. Anselmo González Climent Después me juntaba con unos primos míos y otros muchachos aficionados de Granada. Y lo que pasa: luego las fiestas familiares es donde más cantaba. Pero no a mi manera. Ni yo sabía los cantes que cantaba. De manera que todo era improvisado, a la forma que salía. Más tarde, cuando ya\n\n[ENDING CONTEXT]\n\ntiene una facilidad extraordinaria en la garganta. Y luego, es curioso, ha escuchado muchísimo y de lo mejor. Tiene motivos y sabe. Eso es lo peor.\n\nPUBLICIDAD\n\nSISTEMA DE ENSEÑANZA POR CORRESPONDENCIA PARA PERSONAS YA INICIADAS QUE DESEEN:\n\n• PERFECCIONAR SU DESTREZA\n\n• AMPLIAR SUS CONOCIMIENTOS\n\nMensualmente se van estudiando los distintos toques en profundidad\n\nIdeal para quienes no puedan asistir a las clases por dificultades de cualquier tipo\n\nPARA MAYOR INFORMACION, ESCRIBIR O LLAMAR A:\n\nFrancisco Fabián Donoso\n\nC/. FLORENCIO QUINTERO, 16-3.°-A - TELEF. (954) 37 21 87 - 41009 SEVILLA\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Viejo carné flamenco",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "15-17",
    "page_number": 15,
    "word_count": 3125,
    "article_char_count_full": 18383,
    "article_char_count_review": 2829,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "Granada"
      }
    ]
  },
  {
    "article_id": "1988-11-18-left-cantaor-el-mochuelo",
    "article_text_for_review": "J. Márquez Cabello\n\nCasi todos los cantes en discos, Cuna treintena, que del cantaor cordobés Antonio Pozo «El Mochuelo», reseña el dilecto escudriñador Yerga Lancharro en el número 30 de esta revista CANDIL, a excepción de las jotas y los aires montañeses, los tenía mi hermano el cantaor Antonio «Mediaolla» en nuestro café-bar en Puente Genil para su esparcimiento y el de la clientela. Por tanto, a uno, «esclavo» del establecimiento, les eran familiares las malagueñas y las soleares del no despreciable cantaor de «La Llana». Así que cuando en la Peña Juan Breva nos enfrascábamos en sesiones de estudio averiguando la cantidad de malagueñeros y «malagueñas» habidas, Pepe Navarro, gran aficionado, y muy apasionado de los cantes de Málaga, se empecinaba en que Pozo «El Mochuelo» no había creado ni recreado nada de nada, ni lo que tenía de tal palo valía un duro. Uno, por su cosilla de cordobés provinciano o pontanés y recordando lo escuchado en su mocedad del «Mochuelo», le parecía excesivo el parecer del consocio y amigo Pepe, y disentía de manifestaciones mientras no se hablase al pie de su discografía, la cual no se disponía, aquel momento, como después la Peña consiguió hacerse con todo lo habido de dicho artista.\n\nHemos subrayado lo que así se ve porque de tal guisa lo llevó luego Navarro a su libro «Muestrario de malagueñeros y malagueñas», y que no se llegue a creer que nos lo hemos sacado de la manga. Pepe era algo impulsivo y no se recata-ba de decir las cosas cual las pensaba, aunque al rato le pesara... porque se percataba que iba perdiendo amigos.\n\nPara mí, Antonio Pozo había sido, si no un creador afortunado de malagueñas, sí un fiel intérprete y hasta rehacedor con «letras» diferentes a maestros de aquella época.\n\nNo era totalmente como Sebastián «Pena Padre», con quien se le comparaba en su aspecto de copista. «El Pena» hacía perfectamente el cante de «La Trini», con fidelidad absoluta y así lo grabó. En tanto, «El Mochuelo»variaba unas décimas por aquello de inflular un algo de sí mismo, cosa que —a mi entender— no iba en demérito suyo, cuando es más admirable evitar el mimetismo.\n\nEn aquellas dichas sesiones en la Peña Juan Breva sobre malagueñas y malagueñeros, se contó por alguien una anecdotilla curiosa del cantaor cordobés. Decíase que éste, cuando terminaba de actuar en reuniones o salas, se removía rabiando por cobrar e irse, aduciendo: «Hay caguéca elala y cá mochuelo a su olivo...». De aquí que, los graciosos que nunca faltan, le pusieran de mote el nombre de la avecilla nocturna.\n\nTambién Navarro se enfureció conmigo porque se empeñaba en quitarme la razón al mantener yo que el abandolao «En criticar y murmurar» se había fraguado en Puente Genil, aunque con esencias del llamado «...del Guadalhorce». Él se engañitaba gritando «Eso tó es de Málaga como el zángano y los fandangos de aquellas zonas, Lucena, Baena y más». Había que dejarlo porque se movía de un lado a otro, estirando sus brazos en señal de que no admitía réplica.",
    "title": "Cantaor «El Mochuelo»",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "18-18",
    "page_number": 18,
    "word_count": 511,
    "article_char_count_full": 3001,
    "article_char_count_review": 3001,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-11-18-right-aunque-no-quepa-en-el-papel",
    "article_text_for_review": "Envío «In Memoriam»: a don Anselmo González Climent, del que todos somos discípulos.\n\nJosé Luis Buendía López\n\nDe magno acontecimiento para el flamenco hay que catalogar la obra aquí reseñada. Y nunca mejor dicho lo de reseñada, que no leída en su totalidad, ya que la monumentalidad de sus contenidos y lo ambicioso de su plan la convierten en una obra de carácter general, de consulta sobre tal o cual aspecto concreto, y no una monografía que se lee en un momento dado y nos aporta tales o cuales informaciones que, una vez asimiladas, nos olvidamos del libro donde provienen.\n\nEste caso es distinto. Aquí tenemos el esfuerzo investigador de estos dos tenaces estudiosos del flamenco que, por separado, y ahora conjuntamente, tantos y tan buenos servicios han prestado a este arte que conocen como pocos.\n\nEn esta obra todo es monumental, desde la presentación, lujosa y\n\napabullante, hasta su contenido, cercano a las mil páginas de tamaño folio, sus cinco mil trescientas voces, ordenadas, como corresponde a un diccionario, alfabéticamente, dos apéndices dedicados a discografía y bibliografía respectivamente, y en fin, innumerables ilustraciones, en absoluto superfluas, puesto que iluminan a la perfección la atmósfera flamenca descrita en el texto teórico correspondiente a la voz analizada.\n\nEn este vasto y ordenado friso, que no loco cajón de sastre, se contienen atinadas informaciones sobre los estilos, protagonistas, bio-\n\ngrafías, etc., que forman el mundo flamenco, a la vez que una reflexión sobre cómo las bellas artes se han ocupado de él, y una serie de apuntes históricos, sociológicos, lingüísticos, etc. Podemos asegurar que muy pocas cosas escapan a este ilustradísimo compendio, que se nos antoja imprescindible para la consulta futura de toda información referida a nuestro arte. Naturalmente que en esta pléyade de datos de distinta significación, para la que los autores se han servido de muy diversas fuentes, incluidas las facilitadas por otros estudiosos y aficionados flamencos, que son debidamente citados en el capítulo de agradecimientos, no ha de resultar imposible que encontremos algún error aislado, algún baile de fechas o de nombres, lo que entendemos no resta mérito alguno a este enorme esfuerzo investigador al que se nos antoja pudiera suceder lo que a la obra de Cossío, Los Toros, cuando analizó una empresa similar en el ámbito taurino, esto es, que con todas sus primeras imperfecciones, resulta hoy el material más importante del que partir en cualquier investigación referida a ese campo. Tal vez sucesivas ediciones que, estamos seguros, habrán de generarse, pulan los posibles errores que pudiéramos hallar. Entretanto, los que sabemos del enorme esfuerzo que supone todo tipo de investigación sistemática, debemos estar de enhorabuena.\n\nTOMAS\n\nEspecialidad en PLANCHA\n\nAPERITIVOS SELECTOS\n\nMESONES, 18 TELF. 26 35 46 JAEN\n\nJosé Luis Buendía López\n\nEn una bella edición que nos ha- ce añorar épocas pasadas en las que el arte de imprimir era algo más que una simple necesidad in- dustrial, nos llega esta segunda aventura pública del libro de Luque Navajas sobre los cantes de su ti- erra, manual que resultaba inacce- sible al ser publicado hace veinte y tres años y no encontrarse un solo ejemplar disponible. Por voluntad de su autor, se respeta el texto de aquella época, si bien en ocasiones rectifica, como buen sabio, opinio- nes hoy insostenibles, como aquella defensa, vía Pepe Navarro, de la supuesta malagueña del Marrur- ro, totalmente descartada después de la refutación magistral de nue- stro colaborador Yerga Lancharro.\n\nPoco a poco, y tras justificar el porqué del calificativo de «cantaora» que Málaga mereciera a Manuel Machado, se abordan con profundidad no exenta de claridad to-\n\nda una serie de temas que afectan a esta importante provincia cantaora, analizándose los «puntos fuertes» en los que el cante ha anclado su personalidad provincial: la capital, Ronda, Vélez, Alora, etc., separando el autor los cantes que considera malagueños de los interpretados por malagueños y que adquirieron un fuerte carisma y por tanto personalidad propia, pese a no pertenecer al tronco original de esa provincia.\n\nEn todo momento, la contundencia de sus opiniones, matizada por las buenas maneras de que Luque siempre ha sabido hacer uso, hace su aparición, es el caso, quizá el más llamativo del libro, en el que valientemente refuta la idea tan extendida en la flamencología de la formación del cante a partir de los llamados «cantes matrices» y la sustituye por la conjunción de dos sectores sociales andaluces: el de la picaresca gitana y los elementos camperos de influencia castellana, que unidos desembocarían en el fandango, uno de los orígenes del flamenco más fácilmente demostrable para Luque Navajas.\n\nPolémico y elegante, expone sus ideas sobre la génesis de los verdiales, bandolás, cantes de Juan Breva, Javeras y Malagueñas, adornando sus descripciones con las letras más representativas, constituyendo un conjunto agradable en el que el lector tiene oportunidad de constatar sus opiniones con las de un especialista que en la teoría y en la práctica ha demostrado sobradamente su competencia en esta parcela.\n\nPUBLICIDAD\n\nJ. A. PULPON\n\nRepresentante\n\nO'Donnell, núm. 3 - 4.º\n\nTeléfs. 222058 - 216920\n\nSEVILLA\n\nParticular: Teléf. 228078\n\nPágina 36 CANDIL",
    "title": "Aunque no quepa en el papel",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 847,
    "article_char_count_full": 5337,
    "article_char_count_review": 5337,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-11-19-right-las-alas-del-coraz-n",
    "article_text_for_review": "Paco Vallecillo\n\nHace muchos años que en una revistilla de efímera vida (Flamenco, Ceuta.\n\nTertulia Flamenca) publicamos una pregunta dirigida a unos amigos nuestros, los doctores Agustín Jiménez, Rafael Belmonte y Antonio Herola, inquiriendo de ellos, en su alta capacidad profesional y flamenca a un tiempo, aclaración o explicación posible al frecuente uso de la locución alas de corazón en numerosas conlas flamencas.\n\nNo resultado suficientemente atendida nuestra curiosidad y al cabo del tiempo hemos ido conociendo más y más letras que se apoyan en la misma frase empleada como tema tanto de lamentación como de maldición o por queja o pena irremediable otras veces. Hace unos meses comentamos este hecho con nuestro amigo José María Pérez Orozco cuando repasábamos la variedad rica e inacabable de la temática en las coplas que enriquecen el tejido musical del Flamenco. De José María recibimos posteriormente una relación comprensiva de dieciocho letras en las que desde apelaciones distintas se menciona a las alas del corazón. De ellas seleccionaremos media docena solamente en mérito a las limitaciones de espacio que requiere este modesto ensayo periodístico.\n\nLa riqueza en la selección de tantas coplas conteniendo una expresión que se nos apareció como meramente vulgar e incluso inculta continuaba picando nuestra curiosidad y así fue cómo tentados por unos insólitos deseos de investigación —senda por la que pocas veces hemos transitado y siempre con escasos y magros resultados— quisimos empezar la tarea por el principio, como debe ser lógico. Y al principio pensamos que debiera estar en el diccionario de la Real Academia de la Lengua Española (castellana, preferiríamos nosotros) en el que ¡albricias! encontramos entre las distintas acepciones de la voz ALA: Ala del corazón, aurículo. / Pl. figurado: Ánimo, valor, brío.\n\n2.ª, el significado de alas de corazón en sentido figurado que le otorga igualmente el DRAE (ánimos, valor, brío) resulta asimismo amplia y definitivamente expresivo.\n\nAsí, pues, nuestra tantas veces denostada RAE nos da la doble respuesta: 1.ª, ala del corazón significa justamente aurícula. Y aurícula, siguiendo la definición anatómica, es cada una de las dos cavidades situadas por encima de los ventrículos. No habrá que perderse en nuestra infinita ignorancia para tratar de explicar la importancia de las alas del corazón en el cuerpo humano que justifica su abundosa cita en el Cante de los flamencos.\n\nUno recuerda una vieja letra perdida en la lejanía de su infancia y que solía ser cantada como remate (valga llamarlo macho) en la Soleá de Cádiz que decía así:\n\nPermita Dios que te veas sin luz, sin boca y sin riendas. Que los dineros t'ajoguen. y hora de salud no tengas.\n\nTerrible maldición: sin luz, ciego; sin boca, mudo; sin riendas, loco; sin hora de salud: enfermo, doliente. Pero muy posiblemente la mayor execración imaginable pueda resumirse en el deseo de que caigan las alas del corazón, la vida en suma, o que se pierda el ánimo, el valor o el brío, es decir, la fuerza.\n\nAl muestrar a continuación algunas letras alusivas al motivo de este ensayo queremos avanzar nuestra creencia de que el número de las que se nos han proporcionado no es, ni mucho menos, exhaustivo. Aún quedan muchas más por recopilar; pero basten para la ocasión las siguientes:\n\nEn el alma la tenía y eya el alma me estrosó: estrosaítas le bea las alas del corazón!\n\nPor causa de tu querer yo tengo perdío el timón, las penas y el rumbo norte y las alas del corazón.\n\nCuando más a gusto estés las alas del corazón se te caigan a los pies. F. Bañuls y P. Orozco\n\nAquel que tuvo la culpa, mare, de mi perdición a cachitos se le caigan las alas del corazón.\n\nAmarilla la naranja color de caña el limón; con las penas que me has dado de la cera es mi color que poco a poco has dejado sin alas mi corazón.\n\nF. Moreno Galván\n\nAquel que tenga la culpa que fatigas pase yo, a peasos se le caigan las alas del corazón.\n\nMe acuesto sobre la cama, a mi corazón de ducas se le cayeron las alas. G. Núñez de Prado\n\n(Según el autor, esta copla la cantaba La Serneta).",
    "title": "Las alas del corazón",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 694,
    "article_char_count_full": 4099,
    "article_char_count_review": 4099,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1988-11-20-right-pilar-l-pez",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA hora no sé por qué me acompañaba un ejemplar de «Poeta en Nueva York» durante aquel viaje a Granada, pero sí sé por qué sobre una de sus páginas dejé escrito —bajo la profunda impresión de los acontecimientos— unas líneas que hoy me devuelven al jovial recuerdo de unos días inolvidables en compañía de la eximia bailarina Pilar López: «Hoy ha sido para mí, tal vez, el mayor día de emociones que haya podido sentir en mis 35 años de edad. He visitado con Pilar López, Fuente Vaqueros, he escuchado hablar a doña María de su primo Federico, he estado en la casa y habitación donde nació el poeta y por último en el monte donde lo asesinaron. Le hemos dejado unos claveles, unas magnolias y unas lágrimas. 29-6-1954».\n\nY todo empezó porque con la guitarra de Eduardo el de la Malena, el entusiasmo\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Granada\"]\n\n1954». Y todo empezó porque con la guitarra de Eduardo el de la Malena, el entusiasmo de Rafael Belmonte y la mitad de la edad con que ahora escribo le ofrecimos una tarde, en aquel lindo salón de la antigua sede de Radio Nacional en Sevilla, un largo recital a dos damas de la alta aristocracia española y al director de cine José L. Sáenz de Heredia. La dama más joven y elegante me dijo al final: «Usted va a ir al festival de Música y Danza de Granada». Y fui. En la bendita hora que fui. Eramos al cante Juan Varea, Luis el de las Marianas, el Pili, Ramón de Loja, Pepe el Culata, Albaicín y yo. Bailaban Paquita Rodríguez, Teresa Amaya, Adela Borja, Delia Montenegro, Curro Vélez, Paco Aguilera, Fernando Terremoto y Mariano, y tocaban Luis Maravilla, Juan Hidalgo, Manolo Amaya, Miguel el Santo y Juan Amaya. «Granada quieta y fina, ceñida por sus sierras y definitivamente anclada», encerrada como siempre en su color y calor altoandaluz, más por aquellos días, distinguida y solemnemente aureolada dentro de aquel su III Festival de Música y Danza, 1954. De entre los organizadores del capítulo flamenco anexo al Festival destacó como amigo, ya para siempre, esa gran persona, andaluz sin par, inteligente aficionado y prestigioso doctor en medicina Fernando Lastra. Con él y con mi viejo compañero de sabores y sinsabores hoteleros, don Antonio Díaz, subi- mos hasta el hotel Washington Irving donde doña Pilar esperaba ensayar con Fernando la «Baladilla de los Tres Ríos», con música de éste por tientos. A doña Pilar, como figura del baile, la conocía desde la butaca del teatro. Me había gustado mucho la serena delicadeza de su baile y su concepto coreográfico. Ahora íbamos a hablar, tal vez a comenzar una amistad. Después del brevísimo ensayo una larga conversación me fue descubriendo a una mujer culta, sencilla, elegantemente flamenca y dueña de un oportuno sentido del humor. Había terminado su intervención en el Festival, pero se quedaba unos días en Granada apurándola como a una exquisita copa de néctar. Nos hablaba de su hermana Encarnación, de Lorca, de Sánchez Mejías, del baile... «El baile flamenco con hondura y arte es de tierra llana. En las zonas montañosas se baila para arriba, verticalmente, con menos brazos —los brazos son los que bailan— y más pies. Federico hacía teatro con cualquier ocurrencia histriónica y un pañuelo de la nariz como telón. Al único que le permitía ciertas bromas era a Ignacio. Federico era un músico nato, genial hasta hablando de lo más simple, vulgar y común». «Doña Pilar, le dije, precisamente yo mañana voy a Fuente Vaqueros. Qu\n\n[ENDING CONTEXT]\n\nuna hierba dura y fina distinta a toda la que crece por aquellos alrededores. ¿La sembró alguien como señal del lugar exacto donde enterraron a Federico? (Así se lo conté a Rafael Alberti cuando aún vivía en Buenos Aires).\n\nPágina 40 CANDIL\n\nFuente Vaqueros, Víznar, Huerta de San Vicente, donde Fernando leyó el prólogo de Alberti al Romancero Gitano, Terremoto iniciándose como cantaor, mi recomendación del Farruco a Pilar que más tarde conoció y contrató. ¡Pilar, inteligente, culta, sencilla, un poco Venus de Milo disfuminada y un mucho juego de agua y danza por el Generalife de hace 32 años!\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Pilar López",
    "periodical": "candil",
    "issue_id": "1988-11",
    "year": 1988,
    "language": "es",
    "article_type": "article",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 1240,
    "article_char_count_full": 7190,
    "article_char_count_review": 4227,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "Granada"
      }
    ]
  }
]
```
