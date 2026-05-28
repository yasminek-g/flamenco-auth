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
    "article_id": "1989-09-19-left-el-desprop-sito-estriba-en-m",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHarás Bien\n\nM. Yerga Lancharro\n\nDecir Cantes de ida y vuelta es un despropósito a todas luces. Mientras no se me demuestre lo contrario, tendré que decir que es pura invención fantástica, como lo fue elegir un dibujo a plumilla de José María «El Tempranillo» para mostrarnos con él la imagen del legendario cantaor «El Planeta».\n\n¿Por qué nos comportamos así con la sana afición de España y de fuera de nuestras fronteras? ¿Es que no debe merecernos el mayor de los respetos? ¿Qué beneficio reportamos con tal proceder a nuestro arte y a nuestros artistas? ¿Y qué queremos decir de forma tan poco acertada a esa afición desprovista de maldad? Yo no lo comprendo. Por favor, seamos más serios y más responsables cuando tratamos de temas flamencos. Yo diría, mejor, del supremo arte flamenco.\n\nCuando\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_04 | trigger=\"Nuevo\"]\n\nos artistas? ¿Y qué queremos decir de forma tan poco acertada a esa afición desprovista de maldad? Yo no lo comprendo. Por favor, seamos más serios y más responsables cuando tratamos de temas flamencos. Yo diría, mejor, del supremo arte flamenco. Cuando el ya lejano día 12 de octubre de 1492 fue descubierta América, ¿no es verdad que aún no se cantaba en Andalucía por milonga, guajira, vidalita y colombiana? Y si se cantaba, ¿cuándo llevamos al Nuevo Continente esos cantes, cuyos nombres son tan hispanoamericanos? ¿Es que, acaso, existían en Andalucía guajiros, colombianos y argentinos? Yo creo que no, a menos que me confiese ante la afición como un consumado ignorante. Hace cincuenta años se decía entre los cantaores que el cante por guajira fue aflamencado por Sebastián «El Lebrijano» y por «El Piyayo», quienes lo trajeron de Hispanoamérica, con ocasión de haber hecho el servicio militar en la isla de Cuba. Esta versión hay que recharzarla, porque ¿cómo es posible que Sebastián Fernández «El Lebrijano», coincidiera con «El Piyayo» haciendo el servicio militar en Cuba, cuando la diferencia de edad entre ambos era de diecisiete años? Esto, para mí, no deja de ser una «historieta». Si, como antes he dicho, en Andalucía no vivían guajiros, colombianos ni argentinos antes del descubrimiento de América, ¿cómo pudieron llevar estas denominaciones de cantes aflamencados a aquellas tierras? Esto no puede convencer a nadie. Esto es para mí increíble. Por el contrario, sí pudiera ser que andaluces diseminados por el Nuevo Continente, en el pasado siglo, escucharan a los indígenas de Cuba, Colombia y Argentina sus cantes por guajira, milonga, colombiana y vidalita, los captasen y después se los trajeran a Andalucía. Esto sí pudiera ser creíble. Vamos a meditar un poco, concentrándonos al máximo para después explicar a la afición lo que significan los nombres impuestos a los «Cantes de venida»: Decir guajiro es identificar un canto popular de la isla de Cuba. Decir milonga es identificar un cante popul\n\n[ENDING CONTEXT]\n\ny envidia del mundo entero.\n\nCreo que no exagero, porque, precisamente, no hace mucho tiempo me llamó por teléfono un japonés, bastante aficionado, quien desde Canarias se desplazó a Sevilla, trayéndome, como regalo, una placa de «El Chato de las Ventas», con cantes por mala-gueña y fandangos que, precisamente, yo no tenía en mi archivo. Este detalle, unido a las visitas de algunos franceses a mi hogar, no viene a decirnos que el arte flamenco es conocido y deseado universalmente?\n\nRecepción diaria de mariscos y pescados Especialidad en asados\n\nRoldán y Marín 7 - Teléfono 22 97 65 - Jaén\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El despropósito estriba en M.",
    "periodical": "candil",
    "issue_id": "1989-09",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "18-20",
    "page_number": 18,
    "word_count": 1881,
    "article_char_count_full": 11313,
    "article_char_count_review": 3648,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_04",
        "family": "CRIT",
        "trigger": "Nuevo"
      }
    ]
  },
  {
    "article_id": "1989-09-21-left-la-minor-a-de-los-caballistas-ba",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOpinión\n\nJosé Gelardo Navarro\n\nDedicatoria: A Diego Corrientes, el de la tonía de la cárcel A Juan Caballero, caballista, cantaor y andaluz A Luis Caballero, cantaor andaluz\n\nAllá por el año 1847 publica Serafín Estébanez Calderón sus «Escenas Andaluzas», en las que da un repaso al mundo flamenco de la época; nos habla entre otros cantaores de El Planeta —curioso nombre—, El Fillo, La Jabera, los Costenos, etc... Recordemos que ya estamos en una etapa del Cante Flamenco en la que la comercialización de éste ya está servida y la misma irá en aumento con el paso del tiempo. También conviene anotar, por las reseñas y puntualizaciones que nos transmite Estébanez Calderón, que resulta evidente la configuración y estructuración de toda una serie de estilos flamencos; pensemos también que aunque\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"comercial\"]\n\nstumbrista, prerromántica y romántica de ensalzamiento de los tipos populares por parte de escritores, pintores, poetas, etc., ciertos gitano-andaluces supieron conectar con aquellos movimientos y/o modas y engancharse al carro del casticismo. Empiezan, pues, a surgir como novedad los cantaores profesionales que, aunque poco, algo cobran —migajas— en especies o en dinero, aprovechando estos cantaores el impulso romántico: surge la comunicación (comercial) del flamenco. [En ningún momento utilizamos las palabras «comercialización» y «comercio» con valor peyorativo. Constatamos un hecho real]. Otras minorías andaluzas marginadas social o culturalmente, también empiezan a hacer lo mismo que los gitano-andaluces, aunque en menor medida; y esto sucederá así en líneas generales porque de entre los grupos marginados el más cohesionado, el que tiene más sentido «grupal» y de casta es el gitano-andaluz. Y éste acaba de descubrir a través de la comunicación un medio de subsistencia: el Flamenco. Corre el año 1881 y aparece en Sevilla el libro de Demófilo: «Collección de Cantes Flamencos». En este libro, amén de las coplas flamencas que nos da a conocer, viene una importantísima lista de cantaores suministradas al autor por el cantaor de la época Juane-lo de Jerez, cantaor gitano-andaluz. Este, siguiendo un espíritu gremial y de casta —perfectamente comprensible, por otra parte— hace que prevalezcan los cantaores de origen gitano en su mayoría. Sin embargo, no podía ignorar a Silverio (1829-1889), admirado por los propios gitanos y dominando prácticamente todos los estilos flamencos. Queremos apuntar respecto a Silverio una conversación con nuestro amigo y estudioso del cante flamenco Andrés Salom, quien nos comunicaba lo siguiente: no es posible que la tradición flamenca más cabal considere a Silverio como el mejor y más completo cantaor de todos los tiempos y que constituya un caso totalmente aislado. Dicho de otra manera: no es posible que este cantaor haya adquirido toda su sabiduría fla\n\n[ENDING CONTEXT]\n\n(cantes). Resulta también evidente que no sabremos nunca cómo y cuáles fueron los matices musicales de estos cantes de Diego Corrientes, pero lo que sí podemos afirmar, teniendo en cuenta los elementos antes reseñados respecto a su vida, personalidad, persecución, encarcelamiento, etc., podemos afirmar —repetimos— que estamos en presencia de una música absolutamente trágica, desgarrada, una música procedente de los bajos fondos oscuros de los calabozos: lo que cantaba Diego Corrientes eran, sin ningún lugar a dudas, tonadas carcelarias.\n\nDoctor Arroyo, 12 / Teléfono 21 00 58 / JAEN\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La minoría de los Caballistas-Bandoleros José",
    "periodical": "candil",
    "issue_id": "1989-09",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "20-22",
    "page_number": 20,
    "word_count": 1513,
    "article_char_count_full": 9585,
    "article_char_count_review": 3636,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "comercial"
      }
    ]
  },
  {
    "article_id": "1989-09-22-right-el-mito-de-las-bailarinas-gadita",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nOpinión\n\nManuel Villarejo García\n\nE l objetivo inicial de este trabajo era recopilar, para el aficionado al flamenco, los textos antiguos más significativos que se han esgrimido como prueba de lo añejo de nuestro arte por quienes defienden unas primeras manifestaciones que se remontan a la antigüedad romana. Sé que, cuando afirman esto, son conscientes de la gran distancia que hay que salvar.\n\nEste planteamiento inicial suponía, en principio, dar por aceptada la tesis de la relación entre aquellas manifestaciones y las del flamenco de los tiempos modernos, que es el flamenco propiamente dicho.\n\nSin embargo, al repasar esos textos en sus fuentes me ha sido imposible prescindir de una valoración y, consecuentemente, tomar partido por la tesis contraria precisamente, a saber, que tales\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"referente\"]\n\nesis de la relación entre aquellas manifestaciones y las del flamenco de los tiempos modernos, que es el flamenco propiamente dicho. Sin embargo, al repasar esos textos en sus fuentes me ha sido imposible prescindir de una valoración y, consecuentemente, tomar partido por la tesis contraria precisamente, a saber, que tales fuentes están sobrevaloradas y a la ligera en lo que se refiere a su relación con el flamenco; podrán significar algo en lo referente a costumbres de la antigua Cádiz que pueden haber persistido en algún rasgo del folklore gaditano general, que en todo caso impregne a su vez alguna zona muy específica del flamenco, pero nunca podrán, a mi entender, estimarse como primeras manifestaciones del arte flamenco. Son varios los autores que en la bibliografía de temas flamencos, al hacer historia de este arte, se han remontado a la Roma antigua en un deseo legítimo de encontrar las raíces más profundas. Es, sin duda, una empresa arriesgada, pues si para cualquier investigación la búsqueda de pruebas Grabado de Gustavo Doré. suele estar llena de obstáculos, muchas veces insuperables, en el caso del flamenco, por su propia naturaleza, las dificultades pueden ser insalvables. Es indudable que en esta vida todo tiene una prehistoria, unas etapas oscuras en las que se han ido desarrollando unos gérmenes que con el paso del tiempo p\n\n[EVIDENCE WINDOW 2 | retrieval_hint=HERIT_03 | trigger=\"lugar\"]\n\nn algo, me da la impresión, co-mo decía al principio, de que se les atribuye una significación exagerada como precedentes del flamenco (bailaor preflamenco), y además de que el origen esté en Cádiz, porque ésta es otra consecuencia indirecta de esa valoración. Parece que se parte del supuesto aceptado de que el flamenco es un fenómeno muy antiguo, cuanto más antiguo más valioso, y de un deseo acendrado de demostrarlo y ubicarlo en un determinado lugar, Cádiz; a este respecto, fue una suerte encontrarse con las citas literarias consabidas que parecían hechas a propósito. Pero la relación entre los testimonios citados y el fenó-meno del flamenco tal como lo conocemos es, a mi parecer, de-masiado arriesgada, tendenciosa y voluntariosa; todo lo más que se podría sacar de ellos es la compro-bación de una especial predisposición de ciertos pueblos a la música y a la danza, a ciertos ti-pos de músic\n\n[EVIDENCE WINDOW 3 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\ndisponemos sobre nuestro tema cronológicamente hablando; los demás son de la época imperial; el término Mousiká no especifica si eran cantoras o danzarinas, o ambas cosas a la vez; esto sería lo más probable; el texto por sí solo no permite inferir que se tra- tasen de naturales de Cádiz, úni- camente que partieron de Cádiz; podían ser de allí, de otros lugares de la Bética. (Parece que el roma- no Sertorio, en tierras de Huesca, se entretenía escuchando cantar a mozos cordobeses durante su es- tancia en España, cuando las guer- ras civiles) o incluso proceder de Alejandría, Nápoles o Marsella; mejor habría que reducir el testi- monio a la importancia del puer- to de Cádiz en el tráfico marítimo de la época y, en este caso, el más indicado geográficamente para el periplo que se proponía Eudoxos por las costas de Africa. En cual- quier caso, el motivo del embar- que de estas muchachas no serí\n\n[ENDING CONTEXT]\n\nfenómenos irreconocibles entre sí, que no tienen nada que ver lo uno con lo otro, salvo la coincidencia en el espacio, que no en el tiempo. Con todos mis repetos hacia esas costumbres, el flamenco en sus orígenes debió ser y es en todas sus manifestaciones algo más noble. Acepto que se puedan aportar estos textos como antecedentes del carácter gaditano, si se aceptan que se refieran exclusivamente a lo gaditano, en su aspecto burlón, cachondo que se refleja en las chirigotas y los carnavales, pero esa faceta, interesante sin duda, del folklore gaditano es algo muy tangencial al arte flamenco.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El mito de las bailarinas gaditanas",
    "periodical": "candil",
    "issue_id": "1989-09",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "22-23",
    "page_number": 22,
    "word_count": 2037,
    "article_char_count_full": 12039,
    "article_char_count_review": 4927,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "referente"
      },
      {
        "window": 2,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "lugar"
      },
      {
        "window": 3,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1989-09-24-left-andaluc-a-siempre-maltratada",
    "article_text_for_review": "Opinión\n\nA rmonía: Unión o combinación... Proporción y correspondencia de las partes de un todo... Concordancia, acuerdo, simetría... Combinar los detalles proporcionando un equilibrio concordante de acuerdo con el concepto que corresponda: Conjunción entre arquitectura y paisaje, música de acordeón bajo los puentes del Sena, el águila en los picachos y el caballo en la llanura, ritmo de maracas por los cañaverales de Cuba, el ruiseño en la zarza y la calandria en la mañana, como la cal en Osuna y la nieve en el Veleta. Lo que no combina, corresponde o concuerda, lo que parece que no «va» es lo azul con lo marrón, ni una tarde de toros en Alaska, como un cortijo marismo en los Picos de Europa o cantar por siguirias vestido de tirolés... ¿Es que no es todo eso, entre todo lo demás, lo que debe saber y conjugar el profesional de montajes audiovisuales con destino a proyecciones públicas? Lo digo y no es porque uno se sienta más andaluz que el presidente Rodríguez, por aquello de ser cantar y de Aznalcóllar, sino más bien por ser sensible y de «elarte».\n\nFuncionaba Telesur sostenido o sostenida por la voz morena y grave de su joven mantenedor cuando por una franja del confín de Andalucía un caballo andaluz, montado por una jineta con zahones andaluces, galopaba con las crines al viento... ¡Maravilloso! («Soledad de mis pesares, caballo que se desboca...»). Una vieja andaluza toda vestida de negro tendía al sol y al aire andaluz unas sábanas blanquísimas. («Tierra vieja del candil y la pena. Viento en el olivar, viento en la sierra.»). Mientras un Séneca bajoandaluz, quemado por el sol y la desesperanza, meditaba frente a unos naipes. («Siete gritos, siete sangres, siete adormideras dobles.»).\n\n¿Qué fusión, qué tatuaje, qué equilibrio musical corresponde, concuerda, combina con esa intencionada o casual pincelada-compendio de la más honda índole cantaora? Un caballo al paso puede pedir «livianas», al galope soleó por bulerías, y corriendo sólo palmas ligeras por fiesta. Una mujer enlutada sobre el llano y bajo el sol, la guitarra sola por peteneras, y a la soledad del hombre que medita a la sombra de su quietud cansada, la «toná». Es una simple idea sobre tres motivos, pero hay cien más tan ricos por ser andaluces como andaluces por ser tan hondos.\n\nA ese argumento lírico-andaluz de expresivas imágenes mudas, pero vibrantes de autenticidad, hay que hacerlo hablar meciéndolo en la cuna musical de sus propias raíces. Injertarle aires de moda incubados al calor de los más extraños y sofisticados experimentos de laboratorio urgente es una aberración sin otro sentido que el del absurdo. (¿Qué conocedor de las virtudes al paladar de una copa de jerez admitiría mezclarlo con unas gotas de whisky?).\n\nPara más intensidad negativa en el choque, la música amenizante o amenazante que ilustraba la muestra cultural bajoandaluza iba cantada no sé si en inglés o en noruego.\n\nSin ánimos de molestar y menos ofender, yo tomo partido y lo defiendo desde el ángulo que defi- ne y demuestra la importancia de Andalucía y lo andaluz a través de la belleza y el arte.\n\nEs de lamentar, para sonrojo nuestro, que tengamos que reconocer como el máximo respeto e interés que pueda tenerse y sentirse por los más profundos valores andaluces nos llegue de fuera; de fuera de la propia Andalucía y de España. Compositores, poetas, literatos, artistas e intelectuales de todo el mundo reparan, se sorprenden o asombran ante esa extraña expresión melódica o rítmica —única por irrepetible en el mundo de la música— que sin que sepamos por qué ha terminado llamándose Flamenco. Un arte popular nunca solicitado —atención— por los bajos fondos o la alegre jet-set social, sino por las más altas esferas de la Cultura y el Arte internacional.\n\nNo quisiera que la pasión por el encanto nublara mi innata transigencia. Comprendo y apoyo la noble idea de que precisamente el encanto sea compartido, y aunque el caso concreto que me ha sugerido esta «lamentación» sea tal vez el menos significativo de entre tanta abundancia de la misma índole, nos sobran indudables razones a los flamencos para considerarnos de siempre maltratados desde los medios más eficaces de comunicación y difusión con que contamos. El reconocimiento y el amor por nuestros propios valores no pueden concluir tan sólo en el deleite. Es necesario, además, fomentar una amplia y permanente labor de información, ilustración y educación orientada hacia esa gran mayoría del pueblo andaluz que sigue ignorando el capítulo espiritual más importante de su propia cultura. Alguien deberá hacerlo. Alguien deberá, si lo dejan y puede, tratar, con el mejor trato, defender, corregir y armonizar el curso milenario de esta parcela «jonda» de lo andaluz en Andalucía.",
    "title": "Andalucía, siempre maltratada",
    "periodical": "candil",
    "issue_id": "1989-09",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 784,
    "article_char_count_full": 4741,
    "article_char_count_review": 4741,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1989-09-24-right-podio-y-picota-ventolera",
    "article_text_for_review": "Telegrama Paco de Lucía / Cambrils 12 / 28034 MISASIERRA (Madrid)\n\nEnhorabuena, tu valiente gesto en defensa del flamenco, que representa nuestra seña de identidad artística y cultural más definitoria y diferencia-dora de Andalucía. Tu actitud admirable se corresponde perfectamente con tu aportación inigualada y universalmente reconocida a nuestro arte impar. Abrazos.\n\nPaco Vallecillo\n\nEl guitarrista explica las razones de su incomparecencia en el concierto de Sevilla\n\nNACHO SÁENZ DE TEJADA, Madrid \"Mi nombre fue anunciado junto a los precios; sentí que se ofendía a mi cultura y dije que no tocaba\", declaró Paco de Lucía: ayer a este periódico para explicar su incomparecencia en el concierto que el pasado jueves se celebró en Sevilla. La actuación del guitarrista español, que actualmente realiza una gira por Europa, estaba programada junto a las de Plácido Domingo y Julio Iglesias, en\n\n“Me he sentido mal tratado, y me ha dolido mucho porque era en mi pueblo, en Sevilla”, declaró ayer Paco de Lucía a este periódico minutos antes de emprender viaje por avión hacia París, donde continuará su gira europea. “Mi nombre fue anunciado junto a los precios; sentí que se ofendía a mi cultura y dije que no tocaba. No sé si me he equivocado o no, pero creo que he hecho bien. Ya no paso más por el aro”.\n\ntre otros artistas, en el festival Soñadores de España. \"En el contrato de Paco de Lucía no estaba estipulado el tamaño de su nombre en la publicidad\", ha declarado un portavoz de la empresa promotora del concierto.\n\nLa actuación de Paco de Lucía en Sevilla, por la que iba a cobrar cinco millones de pésetas, tenía una duración prevista de 20 minutos, incluyendo una canción, Sevilla, junto a Plácido Domingo. \"Creo que no es arrogancia\", continúa el guitarrista. \"Intento reivindicar el flamenco, reconocido en todo el mundo como una de las músicas más importantes, y ya es hora de que en España se trate con dignidad. Ya está bien de la España de pandereta\".\n\nPrograma\n\nEl programa del festival incluía, además de las actuaciones de Plácido Domingo, Julio Iglesias y Paco de Lucía, las del guitarrista Ernesto Bitetti, que sólo interpretó una canción; la mezosoprano norteamericana Julia Migenes Johnson, que llegó a Sevilla procedente de Estados Unidos; la soprano española Guadalupe Sánchez, y el compositor Manuel Alejandro. El espectáculo, organizado por la empresa Tres In, tenía un presupuesto de 340 millones de pesetas, de los que 15 millones fueron aportados por la sociedad Expo 92.\n\nEn un principio se alegó una lesión en la mano de Paco de Lucía como causa de la incom parecencia del guitarrista. Según informa Justo Romero, un portavoz de la empresa promotora del concierto declaró durante el mismo: \"Paco de Lucía tenía un contrato firmado, y, en éste no estaba estipulado el tamaño de su nombre en la publicidad\".\n\nEl guitarrista, que en ningún momento de sus declaraciones a este periódico alegó esta lesión, declaró: \"No soy nada divo ni protagonista, pero representa a toda una cultura, a un pueblo, que son los flamencos. Todavía hoy, ser flamenco es ser un ciudadano de segunda o tercera categoría. Me estoy dejando el culo por los aeropuertos y no lo hago por fama ni por dinero. Lo hago por mi pueblo. Estoy seguro de que si este concierto se hubiese celebrado en Londres, París o Viena, el tratamiento hubiera sido igualitario. No tengo nada contra Plácido ni contra Julio. Es la organización la que ha creado las diferencias. No quiero reivindicar mi nombre, sino lo que yo represento\".\n\nEl concierto, al que asistieron 45.000 espectadores, comenzó con la interpretación de la obertura de Carmen por una orquesta formada por 80 profesores, informa Justo Romero. Tras las actuaciones de Plácido Domingo, Julia Migenes Johnson, Ernesto Bitetti y Guadalupe Sánchez apareció Julio Iglesias, que interpretó a dúo con el tenor español canciones compuestas por Manuel Alejandro. En la canción Soñadores de España tuvieron que interrumpir su actuación: \"Hay mucho ruido y me he equivocado\", se justificó Julio Iglesias. El cantante español cerró el espectáculo con canciones pertenecientes a su último disco, Raices.\n\nA la Picota:\n\nLa Empresa de Espectáculos Tres In, por su ofensa y desprecio al Flamenco en la persona de quien más que nadie ha contribuido a afianzar en el ancho mundo el máximo crédito y la universal apreciación que hoy disfruta la música flamenca.\n\nEl portavoz de Tres In, don Justo Romero, quien a juzgar por la información de la prensa necesita de un contrato firmado para adecuar al mismo el tamaño tipográfico que corresponde utilizar en la publicidad que anuncie a Paco de Lucía cuando actúa con otros músicos también famosos.",
    "title": "Podio y Picota / Ventolera",
    "periodical": "candil",
    "issue_id": "1989-09",
    "year": 1989,
    "language": "es",
    "article_type": "article",
    "pages": "24-24",
    "page_number": 24,
    "word_count": 775,
    "article_char_count_full": 4683,
    "article_char_count_review": 4683,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
