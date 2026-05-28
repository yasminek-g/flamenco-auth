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
    "article_id": "1993-01-10-right-ix-distinci-n-comp-s-del-cante",
    "article_text_for_review": "lía es la impronta personal que le haya insuflado. De la ortodoxia, una vez registrada, hay que pasar inmediatamente a la valoración de la originalidad. Y para esto no existe catón específico, es la intuición y sensibilidad del crítico quien tiene la palabra.\n\nEl periodismo flamenco, sin dejar de ser divulgativo constantemente, es una faceta que se debe seguir considerando, tiene por delante un destino cifrado en realizar una crítica ecuánime, objetiva siempre, constructiva por ende, y sensitivamente inclinada a configurar el futuro de un arte entrañado en la vida misma y en el sentir ancestral de un pueblo, enardecido por su estética y su sentido del fatalismo, pero englorizado por unos giros musicales sustantivados por el sentimiento y la conmoción anímica.\n\nY finalmente un deseo posiblemente unánime: el periodismo flamenco, sus ejercitantes, debe abandonar toda clase de ínfulas y fobias, de dimes y diretes, de polémicas baratas, porque todo ello le resta credibilidad de cara a los artistas y a la afición más seria y cierta. El prestigio a ganar por la crítica flamenca, no ha de estar en lo anecódico, sino en la integridad y en su consecuente criterio. No hay que olvidar que de un magistrado ignorante, es la toga lo que se saluda. Y que filosóficamente entendido, el honor es la poesía del deber.\n\nSe apagaba el año 92, allá por las brumas de noviembre, y los miembros del Jurado nos reuníamos, una vez más, para distinguir la trayectoria profesional de un artista flemenco, bajo el patrocinio admirable de la sociedad cervecera «Cruz del Campo, S. A.», que, una vez más, supo estar a la altura de las circunstancias, con exquisito respeto a las decisiones del Jurado y sin más preocupación que el conseguir que todos nos sintéi-ramos como en casa.\n\nTras una votación previa, los componentes del mismo decidimos que la modalidad premiada en este 1992 fuera la del Cante, toda vez que guitarra y baile habían sido elegidas en ediciones anteriores. Después vino la discusión, la apasionada pero reflexiva polémica con la que, cada uno de nosotros, defendíamos nuestras propuestas. Debo confesar que todos los años, el oír a mis compañeros argumentar al respecto, constituye para mí la más sabia y ecuánime lección teórica que recibo sobre flamenco.\n\nAl finalizar la noche, cuando Sevilla casi quebraba albores en silencio perfumado, y tras varias votaciones, fue elegido por una amplia mayoría José Menese como receptor del hermoso galardón.\n\nParecía que las sombras nos felicitarán por nuestra decisión cuando abandonábamos la hospitalidad de Enrique Osborne para acogernos a la de esta hermosa ciudad,\n\naún más brillante tras el lavado de la cara de la Expo. Menese, con su plenitud madura, ha dado al cante lecciones de sabiduría y, al mismo tiempo, ha dictado, en momentos difíciles, importantes dictados éticos que nos conmocionaron a los demócratas del país.\n\nPor ello, cuando el presidente del grupo cervecero, en nombre de todos nosotros, entregaba, el pasado 4 de febrero, en los salones regios del Hotel Alfonso XIII, esta IX Distinción a su legítimo ganador, era justo el que nos felicitáramos con él, puesto que somos legión los que opinamos que este arte añejo, preñado de siglos y de sufrimientos, va mucho más allá de la momentánea emoción estética, para fundirse con la lección trascendente de la historia.",
    "title": "IX Distinción Compás del Cante José Luis",
    "periodical": "candil",
    "issue_id": "1993-01",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 548,
    "article_char_count_full": 3342,
    "article_char_count_review": 3342,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-01-11-left-recordando-al-beni-de-c-diz-jos-",
    "article_text_for_review": "Cuando aún permanece en nuestra mente la triste y aflictiva retentiva por el fallecimiento, en plena vitalidad cantaora, de «Camarón de la Isla», acaece un nuevo luctuoso acontecimiento, la muerte de Benito Rodríguez Rey, el «Beni de Cádiz», cantaor gaditano; de la tierra machadiana de «salada claridad», que tipificó sus cantes con el de «Cádiz y los Puertos», que con su voz «acaracolada» supo transmitir ese eco tan singular de quiebro y rajo connatural de su propia personalidad humana y artística. Su cante, me hacía vibrar. He tenido la fortuna de saborear con verdadera emotividad —entre «cabales»— todo el mensaje de jondura gaditana que salía espontáneamente de su alma flamenca, sin olvidarse de acentuar en el mismo un cierto matiz gitano.\n\nDentro del amplio campo de la vieja amistad que nos unía, mi primer encuentro, precisamente el día que lo conocí —y de esto hace casi veinticinco años, un cuarto de siglo—, no pudo ser más desconsolador. Al enterarme de que estaba gravemente enfermo, fui con Naranjito de Triana al Policlínico, en donde se hallaba encamado, con su goteo correspondiente, a raíz de una dolencia de úlcera duodenal. Pensé que era el momento de conocer, aun en circunstancias dolorosas, al Beni humano. Tenía una gran humanidad. Su sentido del humor, su manera hiperbólica de manifestarse, y su visión optimista de la vida, influyeron de tal forma en nuestro ánimo que, al\n\nabandonar la habitación, salimos del hospital totalmente enmudecidos y al mismo tiempo maravillados de tan singular manera de ser.\n\nEn su faceta artística, era genial. Aparte de su propia idiosincrasia para interpretar el cante, se sentía orgulloso de su maestro «el Caracol», y a la inversa. Al morir Manuel Ortega, el eco inigualable de su voz se vería plasmado en la de «el Beni», su fiel discípulo. Tan era así, que en cantes tan populares como «Tientos de la rosa», «Romance de Juan Osuna», «Carcelero, carcelero», etc., cuando los interpretaba el gaditano, a veces era difícil clarificar su original versión. Pero «el Beni» tenía su peculiar forma de decir el cante, y en particular el de su tierra. Dominaba con arte y gusto toda la gama de las cantiñas, especialmente el cante por alegrías, el gracejo de sus «titirimundis», con esos «juguetillos» que solía intercarlar, sin perder el obligado compás. Los fandangos «acaracolados» los bordaba, llevando lenta y acompasadamente con la mano derecha —como en él era habitual—, todo el recorrido de los tercios del cante. Como bailaor que fue en sus inicios en el mundo difícil del flamenco, el compás por bulerías, y si éstas eran de su tierra natal, mejor que mejor, no tenía para él secreto alguno.\n\nCompartió su vida artística, en múltiples elencos, con grandes figuras de nuestro cante, a quienes les hacía pasar buenos ratos con sus ocurrencias y genialidades, y en su personal ejecutoria de actuaciones en tablaos y festivales, hacen de este insigne cantaor que su recuerdo permanezca vivo en el sentimiento de todos los que amamos esta forma de expresión tan nuestra, como es, lo que encierra en sí el misterioso arte flamenco. jDescanse en paz nuestro inolvidable Beni de Cádiz!",
    "title": "Recordando al «Beni de Cádiz» José Núñez de Castro Gómez",
    "periodical": "candil",
    "issue_id": "1993-01",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 520,
    "article_char_count_full": 3149,
    "article_char_count_review": 3149,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-01-12-left-flamenca",
    "article_text_for_review": "Creo sentirme como uno de los privilegiados del mundo flamenco en la recepción de publicaciones flamencas. Sin embargo, no todo lo editado llega a mi poder y pienso que por cuestiones puramente burocráticas. Sé de antemano que la distribución comercial ha de contar con un casi perfecto trabajo, mas si la promoción a los medios de comunicación no se realiza adecuadamente, la labor difusora de nuestro arte queda bastante mermada y la rentabilidad económica de los productores y editores casi en números rojos, de ahí la desgana de las casas grabadoras por desarrollar trabajos sobre el flamenco. Las ventas de las grabaciones de Camarón, por citar un ejemplo puntual, son fiel reflejo de lo citado. Quiero dejar constancia de que esta reivindicación es general para todos los medios de comunicación y nunca desde una postura egoísta y tacaña.\n\nEsta introducción la he formulado en función del enorme esfuerzo efectuado para poder hacerme con «Al compás del sueño», pues, una vez agotado el recorrido por los comercios discográficos jiennenses sin resultado positivo alguno, he apelado con cansinería a la buena disposición de José para poder realizar el comentario que sigue.\n\nVarios son los conceptos nuevos que se le aprecian a José el de la Tomasa en este «Al compás del sueño». Pienso que la madurez que el artista va adquiriendo con el desarrollo de la personalidad y la pro-\n\nfesión, mediatizan sus gustos y su afán por evocar añejos ecos que han tenido especial significación en la historia del flamenco. Así, las figuras de Manuel Vega «El Carbonerillo», en los fandangos; la de Cayetano Muriel, «Niño de Cabra», en sus propios cantes abandolaos; la de Pastora Pabón, en las bulerías gaditanas y en los tangos —estos últimos con cierta monotonía—, o la de Joaquín Vargas, «El Cojo de Málaga», en la murciana, son personalismos que José aborda con determinada formalidad y rigor. Otro de los aspectos que el cantar se villano ha querido dejar patente en este disco es su amplio conocimiento de los estilos que conforman el repertorio flamenco. No sólo son tientos, alegrías, soleá, siguiriyas o bulerías los cantes habituales en el suyo, sino que lo incrementa con toná-liviana —con cierto apresuramiento en el remate—, romeras, granaínas y los citados cantes de Cayetano Muriel, la murciana del Cojo, las bulerías, los tangos, los fandangos del Carbonerillo, los de Huelva y soleares.\n\nSin embargo, el aspecto más importante —particularmente para mí— que José ha plasmado en este trabajo, es el redondeo que de los estilos citados efectúa. El dejarse llevar a veces por sus facultades se ha traducido en una matizada redondez en su cante, a la que hay que sumar melodía en los estilos que la requiere, melisma personal y tratamiento tonal propio en todos ellos y la predisposición innata para adaptar su lírica a la estructura de los cantes.\n\nEn cuanto al acompañamiento de las guitarras, resaltar, una vez más, la labor de los tres, pues la calidad de cada uno de los elegidos es manifiesta por trayectoria, escuela y personalidad. Sus trabajos en «Al compás del sueño» son comedidos, justos, sin abusadoras falsetas y cediendo el protagonismo al cantaor.",
    "title": "flamenca",
    "periodical": "candil",
    "issue_id": "1993-01",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "12-12",
    "page_number": 12,
    "word_count": 522,
    "article_char_count_full": 3166,
    "article_char_count_review": 3166,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-01-13-left-a-prop-sito-de-la-conferencia-de",
    "article_text_for_review": "C on el presente artículo de opinión quisiera expresar mi decepción después de la conferencia de Angel Alvarez Caballero en las IV Jornadas de Estudios sobre Historia de la Guitarra. Intentaré ser breve en la exposición.\n\nJornadas de Estudios sobre Historia de la Guitarra\n\nPara el lector no informado, y con permiso del Sr. Rioja, coordinador de dichas jornadas, me atreveré a recordar que «El propósito de las Jornadas a lo largo de sus tres años de existencia, ha sido el de invitar a su tribuna a personalidades de alta reputación especializada en la realización de estudios históricos sobre la guitarra, que con sus aportaciones iluminarán este aún oscuro espacio intelectual».\n\nY así es. Basta para comprobarlo una simple lectura de los volúmenes que recogen las conferencias de las tres Jornadas anteriores. El espíritu misceláneo de las jornadas que considero totalmente acertado, ya que refleja el carácter variado y universal de un instrumento que paradójicamente algunos definentodavía como «propio de lo español», la personalidad investigadora de su coordinador, y el marco ideal del Festival Internacional de la Guitarra de Córdoba, son además tres condicionantes que realizan aún más el prestigio de las jornadas, conceptuadas como curso desde su tercera edición. La conferencia de Angel Alvarez Caballero\n\n¡Qué decepción! ¡Qué indignación!\n\nBajo el título «La guitarra flamenca: un esquema histórico», escuchamos una agradable pero superficial exposición, síntesis arbitraria de lo que podemos encontrar sobre guitarra flamenca en la bibliografía actual.\n\nY es que no llego a entender có-mo trabajan algunos conferencian-tes flamencos. Creo que más que conferenciantes cabe hablar de oradores o predicadores, que deleitan el oído y el ego con una prosa «sen-cilla», salpicada de «referencias poé-ticas», cuyo mensaje subliminal viene a ser siempre el mismo: «qué buenos somos».\n\nEl caso de Alvarez Caballero no es una casualidad.\n\nLo escuché por primera vez en Almería hablando sobre cantes de las minas, fortuitamente al día siguiente de acabar la lectura del libro de José Luis Navarro y Akio Iono, y me quedé con la desagradable impresión de oír un resumen.\n\nEn la segunda conferencia que presencié, también en Almería, «Momentos cumbres del flamenco» o algo por el estilo, oímos fragmentos escogidos de su publicación en Alianza Editorial. Esta vez lo que me sorprendió fueron la forma y la falta de pudor por ganarse al respetable.\n\nEl caso de Córdoba es más grave, ya que el marco requiere una responsabilidad mayor, al ser internacional y constituido por un\n\npúblico fundamentalmente guitarristico y especializado. Sin entrar a detallar la conferencia, no puedo evitar dejar constancia de mi sorpresa cuando el Sr. Alvarez terminó su esquema hablando muy de pasada de lo que él calificó como época actual (y la actualidad justificó la ausencia de análisis), refiriéndose brevemente a Paco de Lucía, olvidándose de sus compadres de fatiga, y de la generación actual de nuevos creadores, heredera de la anterior. No entiendo cómo el Sr. Alvarez, que vive en el lugar con mayor concentración de guitarristas, que escribió en El País un suplemento cultural dedicado a las nuevas tendencias del flamenco, no se ha percatado de las diferentes corrientes que existen hoy en la guitarra flamenca.\n\n¿Acaso esta obsesión por el pasado, tan frecuente en el mundo flamenco, no traduce cierta incapacidad para observar e interpretar el presente?\n\nConclusión a modo de sugerencias\n\nPara evitar caer en el lugar común llamado crítica destructiva, y proponer lo constructivo, dos sugerencias.\n\nCreo que la guitarra flamenca es tan digna como sus hermanas, y por consiguiente merece un trato si no mayor, por lo menos igual. ¡Ya está bien de interpretar la producción guitarrística flamenca con historias de aldeas y graciosas anécdotas! ¡Ya es hora de escuchar y analizar de verdad lo que hicieron y componen los concertistas flamencos! Por este motivo, sugiero que sea el Sr. Rioja el que haga las aportaciones necesarias para iluminar la aún oscura historiografía de la guitarra flamenca.\n\nPor otra parte, rogarle como coordinador de los cursillos del Festival de Córdoba, que dé al flamenco el mismo trato que a las demás modalidades. (Me explico: mientras todos los cursillistas empezaron a las 10 horas y con salas preparadas, los «flamencos» tuvimos que esperar hasta las 11,30 horas que se nos dieran salas no preparadas e inadecuadas). ¿Acaso estar para los restos forma parte del contexto necesario para una puesta en situación flamenca?\n\nPara terminar, esperaremos con impaciencia la publicación de las conferencias de las IV Jornadas con los interesantes trabajos de Bernard E. Richardson, de Angelo Gilardino sobre el desarrollo acústico de la guitarra, los estudios para conciertos desde Fernando Sor hasta nuestros días, la magnífica conferencia de Leo Brouwer sobre su música, y poder contar a los niños que empiezan a tocar, las historias de Angel Alvarez Caballero en el país de la guitarra flamenca.",
    "title": "A propósito de la conferencia de Alvarez Caballero en Córdoba o el crítico criticado Norberto",
    "periodical": "candil",
    "issue_id": "1993-01",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "12-13",
    "page_number": 12,
    "word_count": 795,
    "article_char_count_full": 5023,
    "article_char_count_review": 5023,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1993-01-13-right-alre-de-la-fiesta-gitana",
    "article_text_for_review": "Dibujos de Miguel Alcalá del libro «Le Flamenco et les gitans», Editorial Filipacchi, París, Francia, reproducidos bajo licencia del autor. Textos de Manuel Martín Martín",
    "title": "Alreó de la fiesta gitana",
    "periodical": "candil",
    "issue_id": "1993-01",
    "year": 1993,
    "language": "es",
    "article_type": "article",
    "pages": "13-15",
    "page_number": 13,
    "word_count": 25,
    "article_char_count_full": 170,
    "article_char_count_review": 170,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
