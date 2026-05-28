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
    "article_id": "1994-01-15-left-flamenco-en-la-universidad",
    "article_text_for_review": "Los alumnos descubren sus propias raíces musicales\n\nManuel Martín Martín\n\nCon esta filosofía, y en aras de extinguir una cierta imprecisión terminológica y conceptual que oscurece un tanto el proceso a través del cual el flamenco se define como el género musical más precia- do y diferenciador de la cultura tradicional andaluz, ha nacido Flamenco en la Universidad, un ciclo que, con la escasa pretensión eco- nómica de Lebrijano y Enrique de Melchor, y con los comentarios de quien firma, está teniendo una ma- siva aceptación al tiempo que invita a reflexionar sobre el interés por lo propio y los valores intrín- secos de este arte.\n\nEl punto de partida en este curso 1993-94, que arrancó en el Año de la Cultura Tradicional, es Tomás Pavón y Antonio Mairena, tanto por presentarse como eslabones esenciales de la cadena transmisora del flamenco tradicional, y conmemorarse el centenario del nacimiento del primero y los 10 años de ausencia del segundo, cuan-\n\nE s claro que a las puertas de un nuevo siglo, y cuando se ha salvado con éxito la batalla por la universalización del flamenco, resultan del todo insuficientes los caminos recorridos para su divulgación. Urge, pues, una nueva metodología didáctica y descubrir otros foros de debate que, a fin de situarlo en unas claras coordenadas ambientales, contribuyan a iluminar ciertos aspectos nebulosos de su ideario historiográfico y crítico.\n\nA mayor abundamiento, justo es añadir que ambos simbolizan Una encrucijada de caminos —núcleo central de la conferencia ilustrada—, la que señala el período 1929-1983, fechas significativas y coincidentes con el óbito de Chacón y Mairena, tenidos por los más grandes maestros de este siglo.\n\nto por tratarse de dos andaluces universales que otorgaron dimensiones profundas en un flamenco sumido en situaciones de nula entidad.\n\nDesde estos presupuestos, la juventud universitaria, que con sepulcral silencio abarrota las más de las veces hasta los pasillos, se hace un poco exploradora de sus propias raíces, y descubre esencias olvidadas que tal vez pueden cambiar el curso de sus vidas musicales.\n\nLa curiosidad por otros aspectos de los siglos de vida flamenca conocidos, se despeja en el ulterior coloquio, si bien la mayoría de las preguntas giran en torno a las más de ochocientas variantes que conforman el espectro estilístico, con lo que se constata su interés y se aviva el prurito por conocer hasta dónde llegan los estudios de flamencología. A la luz de esta experiencia histórica tenida en Córdoba, Huelva, Granada y Jaén, que se ve refrencada con la presencia de las fuerzas vivas del flamenco local, ya está dejándose traslucir una notable mutación estimativa: de la imagen banal y frívola que de este hecho cultural algunos tenían, el flamenco ha pasado a encarnar, en el extremo opuesto, un compromiso al servicio de quienes desean potenciar su amor por esta seña de identidad única en el mundo.\n\nQueda por conocer, empero, las experiencias de Almería, Cádiz, Málaga y Sevilla. Hasta entonces, el denominador común es la acogida respetuosa y el espíritu abierto de unos jóvenes, con hambre homérica de duendes, a los que la seriedad y urgencia de sus preocupaciones los están inclinando por el enriquecimiento de su formación andaluz. Estas son, a no duardo, las semillas que harán germinar la definitiva dignificación y el futuro del flamenco.",
    "title": "Flamenco en la Universidad Manuel",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 545,
    "article_char_count_full": 3364,
    "article_char_count_review": 3364,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-01-16-left-una-encrucijada-de-caminos-rafae",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFlamenco en la Universidad\n\nRafael Valera Espinosa\n\nSe ha retomado la idea de acercar el flamenco a los universitarios por parte de Juan Peña «El Lebrijano», Enrique de Melchory Manuel Martín Martín. Mas han sido los estudiantes del profesorado de EGB —quizás el terreno donde mejor puede caer la simiente flamenca— y los aficionados en general, los que han arropado en cierta medida a los protagonistas de la puesta en escena de estos encuentros flamencos. No he vislumbrado interés alguno por este arte en el «campus» universitario, si se exceptúan los pocos más de cincuenta alumnos que confirmaron la regla, en la sesión que se celebró en el salón de actos de la naciente universidad jiennense. Se me podrá argumentar que las inclemencias del tiempo influyeron en la mínima asistencia, y en\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"Granada\"]\n\npero me pregunto si en un colectivo de más de diez mil personas, aun a pesar de los contras, no existe una motivación por ésta, nuestra universal cultura, que se acerque al cinco por ciento; tan alto no es el porcentaje. ¿Ha sido precipitada la programación? ¿Ha faltado material propagandístico...? Lo primero creo que no se puede justificar por cuanto estos actos se habían desarrollado ya en otras universidades andaluzas como las de Córdoba o Granada, y los concernientes a la de Jaén se conocían desde principios del mes de diciembre. En cuanto a la segunda interrogante, consideró dicha posibilidad; sin embargo, en los actos celebrados en Linares y Ubeda, y según las referencias de la prensa, la asistencia ha sido muy aceptable, y no creo que en dichas localidades se haya contado con más material propagandístico. Me viene a la memoria la entrada a la famosa copla de Luisa Linares que apunta: «Hay quien dice de Jaén / que no es mi tierra andaluza...», porque atisbo que en la citadá canción existía un afán sincero de reafirmar su pertenencia a esta maravillosa región. Sin embargo, ¿qué pasa con la capital?, ¿por qué somos tan reacios a volcarnos con esta serie de actos, cuando somos tan generosos en otros menesteres?, ¿tan ufanos estamos de nuestro nivel cultural para soslayar cualquier complemento al mismo?, ¿no será que estamos pecando de presuntuosos? A nadie se le escapa —y menos al aficionado— que la trayectoria artística de Juan Peña «El Lebrijan» está jalonada de méritos flamencos e intentos de contrastar musicalmente culturas que pudieran mantener alguna afinidad con su arte, lo cual le ha permitido ocupar un lugar preferente en el escalafón histórico y actual de los artistas andaluces. Si al interés que puede despertar en el ciudadano el escuchar su trabajo flamenco, sumamos que venía acompañado por uno de los más brillantes y vi\n\n[ENDING CONTEXT]\n\nbulerías, en las que Juan Peña «El Lebrijano» desarrolló una pequeña síntesis de determinados trabajos flamencos grabados por él, así como ciertas evocaciones de los aires gaditanos y de su comarca natal.\n\nEl complemento artístico de Enrique de Melchor estuvo realizado con la brillantez que denota a un virtuoso de la guitarra flamenca, acometiendo un toque a veces subordinado al cantaor para encauzar los caminos adecuados del cante, y otras erigiéndose en protagonista del flamenco con el desarrollo de artísticas falsetas y explicaciones demostrativas del compás de los estilos.\n\nCarlos Cruz\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Una encrucijada de caminos Rafael",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "15-17",
    "page_number": 15,
    "word_count": 1350,
    "article_char_count_full": 8580,
    "article_char_count_review": 3489,
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
    "article_id": "1994-01-17-right-entrega-de-la-x-distinci-n-comp-",
    "article_text_for_review": "C on la escrupulosidad con la que siempre se cumplen los plazos establecidos, el pasado 11 de febrero, se procedió a la entrega de la X Distinción «Compás del Cante», que, año tras año, y ya ha transcurrido una década desde el comienzo, patrocina el grupo cervecero Cruz Campo, S. A., a cuyo jurado tenemos el honor de pertenecer desde su creación.\n\nEn el acto, celebrado con toda la brillantez acostumbrada, en el espléndido marco del salón Real del hotel Alfonso XIII, estuvieron presentes, además de los miembros del Jurado y familiares y amigos del galardonado Mario Maya, autoridades y aficionados, que contribuyeron, con su calor, al reconocimiento de los méritos del distinguido en esta ocasión. La dirección de Candil estuvo representada por Rafael Valera Espinosa, invitado expresamente al acto.\n\nA los postres del mismo se sucedieron una serie de entrañables intervenciones, centradas previamente por las palabras de Julio Cuesta, como representante de la empresa patrocinadora, puesto en el que ha sucedido a Enrique Osborne Isasi, «alma mater» hasta ahora en la brillante conducción de la elección anual, a quien desde\n\naquí agradecemos sus desvelos, a la par que deseamos todo tipo de éxitos en sus nuevos rumbos profesionales.\n\nA continuación, Eduardo Osborne, primero, y el señor Allén F. Peeters, presidente del grupo Cruz Campo, después, ofrecieron y entregaron respectivamente el preciado galardón en bronce que diseñara Jesús Gavira al genial Mario Maya, tras glosar ambos su trayectoria y dedicación a este arte que, gracias a artistas como él, se engrandece y universaliza día tras día.\n\nCuando ya las estrellas guiñaban bien altas y la lunita del alto cielo se solazaba con el éxito del bailaor, un grupo de artistas amigos ofrecieron lo mejor de su arte como brillante colofón del acontecimiento: así, durante una corta pero interesante velada, los Chano Lobato, María Pagés, Mistela, Mono de Jerez y un interesante elenco de amigos, bailaron y cantaron para realizar más aún la belleza de este salón de Reyes, ocupado hoy por duendes morenos. Fue el propio Mario el que trenzó los últimos pasos de la noche flamenca de su celebración. En la boca nos quedó a todos un sabor aromatizado de frambuesa.",
    "title": "Entrega de la X Distinción «Compás del Cante»",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "17-17",
    "page_number": 17,
    "word_count": 362,
    "article_char_count_full": 2222,
    "article_char_count_review": 2222,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-01-18-left-las-llaves-de-oro-del-cante-la-n",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nFrancisco Zambrano Vázquez\n\nC on este mismo título en septiembre de 1992, presentábamos una ponencia al XX Congreso Nacional de Arte Flamenco, celebrado en Huelva. Esta ponencia fue aprobada por amplísima mayoría, y así constará en las actas y conclusiones del Congreso que todavía no hemos recibido. Sin embargo, sobre la propuesta aprobada ninguna acción ha podido iniciarse hasta la actualidad, ya que la Comisión encargada de realizar las gestiones, que no era otra que la Comisión Ejecutiva elegida en el citado Congreso, no ha podido hacerlas por no disponer toda-vía de medios, con el fin de que se convocara pública y libremente, en base a valoraciones objetivas y con un jurado representativo y ecuánime: «La IV Llave de Oro del Cante o del Arte Flamenco».\n\nEn este punto interesa recordar\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"críticas\"]\n\niva elegida en el citado Congreso, no ha podido hacerlas por no disponer toda-vía de medios, con el fin de que se convocara pública y libremente, en base a valoraciones objetivas y con un jurado representativo y ecuánime: «La IV Llave de Oro del Cante o del Arte Flamenco». En este punto interesa recordar que, al objeto, entre otras cosas, de hacer posible la función de la Comisión Ejecutiva, con el fin de que los Congresos decíamos, ante muchas críticas en contra de ellos, «valieran para algo» y que las conclusiones y propuestas aprobadas en los mismos se cumplieran, este humilde aficionado que hoy escribe presentó en el XVIII Congreso Nacional de Actividades Flamencas celebrado en Badajoz una ponencia titulada: «Los Congresos, aportaciones para su realización y organización», aprobada en su mayoría, y a consecuencia de la misma se pedía la elaboración de un nuevo Reglamento para los Congresos, cosa que llevó a cabo la Comisión Ejecutiva elegida en el mismo Congreso y que fue presentado también por este humilde escritor en el XIX Congreso Nacional de Arte Flamenco celebrado en Linares, aprobándose por unanimidad. En el mismo, se destinan un tercio de las cuotas de los congresistas para las actividades de la Comisión Ejecutiva al objeto de que pueda cumplir con las funciones que le otorga el mismo Reglamento. Tal cantidad todavía no se ha recibido nunca en ninguno de los Congresos realizados desde entonces, y por tanto la Comisión Ejecutiva, a la que he pertenecido hasta el Congreso de París (de funesto recuerdo), primero para ejercer como organizador, después como puente para la organización de Linares y después del Congreso de Huelva, para poder trabajar en el cumplimiento de la ponencia a la que hoy me refiero que había sido aprobado por el Congreso y que yo había presentado, ha sido hasta ahora inoperativa. Como quiera que estamos en 1994 y que en septiembre se celebrará un próximo Congreso en Estepona, que por lo que conozco en su anteproyecto es\n\n[ENDING CONTEXT]\n\nen base a un baremo publicado con criterios objetivos.\n\n— Jurado en el que participen:\n\na) Las Federaciones y Peñas Flamencas.\n\nb) Los críticos, escritores y entendidos del Arte Flamenco.\n\nc) La Consejería de Cultura de la Junta de Andalucía, a través de la Asesoría Flamenca y la Fundación Andaluzas de Flamenco.\n\nY hasta aquí la ponencia aprobada que hoy recuerdo y espero que su cumplimiento lo lleve a cabo la primera Comisión Ejecutiva, que tenga medios y que creo y deseo que será la resultante del Congreso de Estepona, dada la generosidad y la seriedad con que han planteado su organización.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Las Llaves de Oro del Cante. La necesidad de una referencia",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 1833,
    "article_char_count_full": 11240,
    "article_char_count_review": 3614,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "críticas"
      }
    ]
  },
  {
    "article_id": "1994-01-20-left-los-romances-antiguos",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDe Andalucía, hacia mediados del pasado siglo, nos dejó Estébanez Calderón, pintura muy viva de «un baile en Triana», en el cual se cantan romances llamados corridos (por contraposición a los polos, tonadas y tiranas, que constan de una copla suelta). «El Planeta, cantaor veterano y de gran estilo, inteligente técnico de la guitarra, después de un preludio ejecutado con su vihuela a la que acompañaban dos bandolinas, y tras un prolongado suspiro, comienza a cantar un corrido, que no fue otro que el romance de la «boda estorbada». También otras veces, según Estébanez Calderón, los romances se cantan en los intermedios del baile y, además del ya dicho, se cantan mucho el de Gerineldo y el de Roldán, que es una contaminación de cierto romance viejo con el que comienza «por el mes era de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"representación\"]\n\na a cantar un corrido, que no fue otro que el romance de la «boda estorbada». También otras veces, según Estébanez Calderón, los romances se cantan en los intermedios del baile y, además del ya dicho, se cantan mucho el de Gerineldo y el de Roldán, que es una contaminación de cierto romance viejo con el que comienza «por el mes era de Mayo». D on Juan Valera, en el capítulo VI de «El comendador Mendoza» (1877), refiere de una villa andaluza una representación popular del Jueves Santo en la que se canta un romance sobre el sacrificio de Abraham. Este mismo romance lo vuelve a suponer recitado en otra Semana Santa andaluza por «Juanita la Larga». En su novela «Mariquita y Antonio 1906», tiene un capítulo titulado «Un corrido», advirtiéndonos que la gente del campo canta aún a la guitarra, en algunos lugares apartados de Andalucía, los antiguos romances; pero los romances y la música se van perdiendo y la costumbre de cantarlos acabará también por perderse. Ya en aquella época, dice Valera, era hasta raro oír en boca de un habitante de las ciudades un corrido (que así se llamaban los romances cantados). El hecho es que Mariquita tenía la guitarra y, aunque unos le piden que cante malagueñas, otro la caña, otros el fandango, ella lo que canta es un romance. Hoy difícilmente puede oírse un romance salvo a los sefardías, ya en las fiestas de bodas, ya por las noches «nochadas de viela y de velar». Entre las gentes de campo, en Andalucía y posiblemente en otras regiones, todavía puede que se conserve algo de esta tradición entre los ganaderos, pastores, cabreros, boyeros, muleros, etc. En mi infancia y adolescencia, allá por los años treinta, ya pasados, el «Romance de Gerineldo» lo escuché muchas veces a una niñera que había en mi casa (qué cosa inexplicable para aquellos tiempos: era protestante, ya que se había educado en un colegio que hubo en Utrera de esta religión, a finales del siglo pasado o primeros de éste, sito en la calle de San Francisco) que me lo cantaba al estilo, podríamos decir, popular culto, puesto que era letrada (para aquellos tiempos) y marisabedilla; lo cantaba a una sola voz, de forma melódica y no al compás de bastón o instrumento. Por el contrario, el «velaor» o «pensaor» que cuidaba los mulos, en el cortijo que mi padre labraba, en esa misma época, cuando yo tenía 7 u 8 años, y me escapaba del caserío con mis hermanos de mi tanda para ir al atardecer a las cuadras y escucharle contar cuentos y cantar coplas, lo hacía de forma muy diferente. En primer lugar nos decía que para cantar romances se precisa guitarras\n\n[ENDING CONTEXT]\n\ncambiando de pareja.\n\n»La aventura de Gerineldo es un largo romance; en él muestra aquél su agilidad y resistencia al bailar, que no tiene respiro como lo tienen sus dos compañeras».\n\nLa música, para probarle, acelera a tiempo el compás, pero él no se da por cansado aunque tiene que echar mano del pañuelo para enjugarse el sudor.\n\nEste baile, que nos parece rústico y exclusivo de ese pueblo de Avila, fue antes un baile común y elegante. Las costumbres populares fueron por lo general antes costumbres aristocráticas, abandonadas por las clases superiores al verlas extendidas entre el pueblo.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Los Romances antiguos",
    "periodical": "candil",
    "issue_id": "1994-01",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "20-20",
    "page_number": 20,
    "word_count": 1370,
    "article_char_count_full": 7938,
    "article_char_count_review": 4208,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "representación"
      }
    ]
  }
]
```
