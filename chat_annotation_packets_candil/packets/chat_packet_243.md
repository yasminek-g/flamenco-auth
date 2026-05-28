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
    "article_id": "1991-11-11-right-cr-nica-del-xix-congreso-de-acti",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNo estaba Rafael Román, y eso tenía que notarse a la fuerza, como tiene que echarse en falta en todos los actos flamencos que se celebren de ahora en adelante. ¡Qué le vamos a hacer!, quizá deba ser así, cada vez menos, un poco más solos y tremendamente doloridos. Candil se une al sentimiento de congoja por la desaparición, una semana antes del Congreso, de uno de los hombres que más duramente había luchado para su eficaz desarrollo.\n\nDesde el lunes, 7 de octubre y hasta el domingo 13, la ciudad minera acogió a los Congresistas de modo magnánimo y con ese señorío especial, no expansivo sino intimista y cordial, que es propio de estas tierras. Eso sí, acordándose del deber primero para con los agricultores de la provincia, nos obsequió con una cantidad tal de agua, que parecía que nuestras\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_01 | trigger=\"fuera\"]\n\nque más duramente había luchado para su eficaz desarrollo. Desde el lunes, 7 de octubre y hasta el domingo 13, la ciudad minera acogió a los Congresistas de modo magnánimo y con ese señorío especial, no expansivo sino intimista y cordial, que es propio de estas tierras. Eso sí, acordándose del deber primero para con los agricultores de la provincia, nos obsequió con una cantidad tal de agua, que parecía que nuestras actividades de investigación fueran oceanográficas o de corrientes fluviales, circunstancia que favoreció el que los acompañantes tuvieran la dicha de ver llorar a Cazorla por los ojos de sus farallones milenarios, o que todos nos asombráramos ante la noche de duendes de una Baeza más castellana que andaluza. Durante los días 7 y 8, nuestra reunión científica, que a partir de ahora pasa a denominarse: «Congreso de Arte Flamenco», evitando el rígido corsé de «Nacional» que apretaba sus fronteras, ofreció un exquisito aperitivo en el que participaron por igual conferenciantes y artistas jondos y plásticos de primera magnitud. Ya en la tarde del lunes y en la sede del Congreso, los salones del hotel Aníbal, se inauguraba la exposición pictórica del granadino David Zaafra, inspirada en el flamenco y en la tauromaquia; muestra exquisita, más de sombras que de luces, más de gestos expresivos que de poses académicas. Ese mismo día, Fernando Lastra derramaba sobre nosotros una ciencia tan ancha como su personal humanidad, el tema: «Camino por Andalucía», un peregrinar a través del cante de nuestra tierra, con parada y fonda en la hondura de Joselete, quien, acompañado a la guitarra por Paco Serrano, llenaron de hermosura ese errabundo caminar del sabio Fernando a través de nuestras quimeras. Estando en Linares, no tiene\n\n[ENDING CONTEXT]\n\ncena de clausura, que tendría su continuación en la despedida dominical, con la misa flamenca incluída. En una noche lluviosa que aumenta la belleza de sus piedras, despedimos este Congreso, y a todos nos invade la melancolía de los vetustos edificios cercanos. De nuevo a casa, pero sabiendo que atrás dejamos momentos irrepetibles, una ciudad hermana, amigos de verdad, y que, como estamos en tierra de froteras, estas se abrirán dentro de un año para llevarnos en peregrinación flamenca a la vieja Huelva, donde desembarcaremos como Colón, en busca de nuevos territorios..., naturalmente jondos.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Crónica del XIX Congreso de Actividades Flamencas. Linares",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "11-14",
    "page_number": 11,
    "word_count": 2333,
    "article_char_count_full": 14281,
    "article_char_count_review": 3380,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_01",
        "family": "COMM",
        "trigger": "fuera"
      }
    ]
  },
  {
    "article_id": "1991-11-14-right-d-nde-moraron-los-duendes",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nComo si estuvieran abocados a un triste sino, o como si una maléfica maldición imperara sobre ellos, los espectáculos artísticos que en los congresos de flamenco complementan las sesiones de trabajo o debate, llevan bastante tiempo sin ofrecer un evento que, por su calidad, pudiera haber perdurado en la memoria del congresista aficionado. Puede que, en estos menesteres, la posible intimidación que ejerzan los oidores sobre los oficiantes artistas, por ser considerados los primeros como público generalmente entendido, algo tenga que ver. Sin embargo, una vez analizado el tema con detalle, tenemos que hacernos la siguiente pregunta: ¿Es que no hay más cera que la que arde? Y la respuesta no puede ser más negativa. Si exceptuamos algunos casos, el panorama artístico actual no es muy\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"raíces\"]\n\nnos la siguiente pregunta: ¿Es que no hay más cera que la que arde? Y la respuesta no puede ser más negativa. Si exceptuamos algunos casos, el panorama artístico actual no es muy brillante. P or otro lado, he de reiterar mi desacuerdo con la actual denominación del festival más importante que se celebra durante el congreso: «Festival de Cantes Autóctonos». ¿Por qué han de estar obligados los artistas a realizar cantes que se identifican con sus raíces provinciales, no siendo éstas a veces totalmente asumibles? Arrimando el ascua a mi sardina, he de expresar que los cantaores de mi tierra también muestran su calidad por otros estilos que no son la taranta y, como ejemplo, la actuación de «Joselete» en Badajoz. ¿No sería mejor denominar al espectáculo «Muestra actual de arte flamenco» o «El flamenco actual en Andalucía y Extremadura»? La singularidad del cante de Rafael Romero iba asimilada al nombre de la provincia jiennense y en su repertorio no era muy asidua la taranta. Lo vertido está en la línea de lo escuchado el viernes 11, en Linares. Como sucediera en Badajoz, la organización no podía dejar pasar esta oportunidad para ofrecer el arte provincial y así lo hizo. El resultado, como apuntaba antes, fue otra cosa. Ante la imposibilidad de poder asistir a los dos actos primeros, las conferencias de Fernando Lastra, el lunes 7, y la de Agustín Gómez, el siguiente día, ilustradas respectivamente por el cante de «Joselete» y Gabriel Moreno, por referenci\n\n[ENDING CONTEXT]\n\nadoleció de la garra y el quejío necesarios. En las malagueñas de La Trini y El Canario desarrolló melodía y demasiado melisma. Los tientos-tangos carecieron de entonación y de la acertada evocación de Juan Mojama y Pastora, en los fandangos volvió el florido melisma; los tangos que aprendiera de su madre mantuvieron la tónica de la noche, así como las tarantas y bulerías finales.\n\nEn cuanto al baile de la iliturgitana Rocío González y la linarense María José Martínez, ésta evidenció avance, fuerza, tesón y una enorme afición que les ha de permitir un mayor perfeccionamiento de su arte.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "¿Dónde moraron los duendes?",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "14-16",
    "page_number": 14,
    "word_count": 1232,
    "article_char_count_full": 7464,
    "article_char_count_review": 3090,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "raíces"
      }
    ]
  },
  {
    "article_id": "1991-11-17-left-el-flamenco-evoluci-n-y-tradici-",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDesde las primeras etapas de su historia, el flamenco se ha visto sacudido siempre entre los dos polos opuestos: tradición y cambio, y podemos sospechar que este vaivén es la condición misma de su existencia, porque es creación personal tanto como obra anónima y arte singular más que folklore sin cara.\n\nLa primera etapa del flamenco, la menos conocida, la que por eso se suele llamar «secreta» o «hermética», ya era el resultado de una evolución increíble y tardó siglos en manifestarse. Se trata de la transformación radical de una porción del folklore y de la música popular de Andalucía, de lo que había sido, desde el siglo XV, el repertorio profesional de los músicos gitanos, contratados, según la documentación de la época, para las fiestas públicas o privadas de los pueblos (me refiero a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"modelo\"]\n\núblicas o privadas de los pueblos (me refiero a los romances, las seguidillas, los villancicos, dejando aparte las músicas hoy desaparecidas, como el polvico o la zarabanda). No puedo detenerme aquí en las circunstancias ni en las modalidades de esta evolución, pero todo el mundo sabe que la música que aparece a finales del siglo XVIII o más concretamente a principios del XIX, y que luego se va a llamar flamenco, ya no tiene mucho que ver con el modelo original. Los romances ya no se parecen —fuera de la letra, ¡claro!— a los que sigue cantando el pueblo por toda España. En cuanto a las siguirías, ¿quién pudiera creer en su parentesco con las tradicionales seguidillas (vivas todavía hoy en las sevillanas), si no fuera por el nombre o por algunas coincidencias métricas de la copla escrita? Sin esta evolución básica, sutil enlace de tradiciones hispánicas y de modalidades musicales de tipo oriental, no existiría lo que hoy llamamos flamenco. La segunda evolución —o revolución, y eso se puede decir para ambos casos— se debe a la intervención masiva del elemento andaluz, que va a transformar en cante jondo esta enorme porción del folklore de la tierra conocida bajo el nombre de fandangos, injertándole sangre nueva con aliento de voces antiguas. Los autores de esta metamorfosis extraña son artistas como Silverio Franconetti, Juan Breva o Don Antonio Chacón. Es una verdadera revolución, en efecto, el paso desde los cantes silábicos del folklore hasta el canto largo oriental y desde el compás ternario de los verdiales hasta el ritmo si\n\n[ENDING CONTEXT]\n\nqueda fuera. La cuestión se plantea, por ejemplo, para las sevillanas y muchos fandangos de tipo folklórico.\n\n5. Entre dichos criterios, que quedan por estudiar y definir de manera rigurosa, pueden considerarse como básicas las oposiciones siguientes:\n\na) concepción modal y concepción tonal de la música.\n\nb) Cantos largos sin compás y cantos silábicos de compás binario o ternario. (Por ejemplo: oposición malagueña/verdiales folklóricos).\n\nc) Secuencias rítmico-silábicas de doce tiempos y compases de tipo occidental asociados con el canto silábico (por ejemplo: oposición siguiriya/sevillanas).\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "El Flamenco: Evolución y tradición",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "16-18",
    "page_number": 16,
    "word_count": 2507,
    "article_char_count_full": 15174,
    "article_char_count_review": 3182,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "modelo"
      }
    ]
  },
  {
    "article_id": "1991-11-19-left-ensayo-de-una-tipolog-a",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPonencia presentada en el XIX Congreso Nacional de Actividades Flamencas (Linares) José Luis Navarro García\n\nInnovar, nos dice la Real Academia, es mudar o alterar cosas, introduciendo novedades. Esto, aplicado al Flamenco, equivale a aportar matices nuevos, desarrollos melódicos diferentes, algunas veces insospechados, es decir, enriquecer la herencia de los maestros del pasado. Innovar en el Flamenco es, pues, ofrecer generosamente savia nueva, nueva sangre, para que un arte, que ya es rico en historia, pueda seguir siendo, sea, un arte vivo en el presente, y fértil de cara al futuro. Porque, ¿qué sería, qué habría sido del flamenco si nadie nunca se hubiese atrevido a alterar nada, si nadie nunca hubiese introducido novedades? Hoy, sin duda, no sería más que algo pobre y fosilizado,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"aficionados\"]\n\nun arte vivo en el presente, y fértil de cara al futuro. Porque, ¿qué sería, qué habría sido del flamenco si nadie nunca se hubiese atrevido a alterar nada, si nadie nunca hubiese introducido novedades? Hoy, sin duda, no sería más que algo pobre y fosilizado, una pieza de museo etnológico ignorada y tal vez olvidada para siempre. Y sin embargo, el término innovación es todavía una de esas palabras malditas que sólo con desdén pronuncian algunos aficionados, esos que se autodefinen como aficionados de siempre, peñistas cabales. Porque innovar es una palabra, un concepto, que suscita temor, que provoca recelos en ese viejo aficionado, en ese entrañable aficionado, que sólo disfruta saboreando, paladeando los mismos cantes que ha escuchado toda su vida, que prefiere sentarse junto a otro afi- cionado para, en el calor de la amistad, escuchar ese cante que él conoce, porque ya lo ha oído cien, mil veces antes. Es una palabra que levanta ampollas a ese otro aficionado que se arroga la autoridad de decir con el mayor dogmatismo: Esto sí es flamenco y eso no, porque cualquier novedad le desorienta, le saca de sus casillas. Son precisamente éstos los que hoy cuestionan con mayor aspereza el empleo de la percusión, la viola, o el violín, en compañía de la clásica guitarra; algo que adorna superficialmente los cantes, pero que en absoluto los adultera. Tal vez ignoren que en las reuniones flamencas que se organizaban en la Triana de mediados del siglo pasado, en aquellas en las que cantaban El Fillo y El Planeta, sonaban también vihuelas y bandolines. Por esto, por todo esto, nos hemos decidido a escribir estas páginas. Su propósito será comentar y analizar las líneas básicas que siguieron las aportacion\n\n[ENDING CONTEXT]\n\no adornar su melodía primera, primitiva. Nos estamos refiriendo a esa siguiriya del Planeta que Antonio Mairena, ese insigne reinventor de formas añejas, intentó recuperar de las brumas de la época alboreal del Flamenco. Mairena la aprendería de Pepe Torre, para después, en un verdadero alarde de sabiduría y creatividad, manteniendo, eso sí, lo esencial de su línea melódica, recrearla. Suprimió esa delicada caída que Pepe anunciaba en el temple y después hacía en el tercer tercio, y la hizo en dos jípios; finalmente, la despojó incluso de la guitarra, acercándola así, al tronco de las tonás.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "La innovación en el Flamenco: Ensayo de una tipología",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "19-21",
    "page_number": 19,
    "word_count": 3074,
    "article_char_count_full": 18894,
    "article_char_count_review": 3353,
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
    "article_id": "1991-11-22-left-villancicos-de-francisco-almagro",
    "article_text_for_review": "Oro, incienso y mirra\n\nⅠ\n\nYa vienen los Reyes Magos por los caminos de Oriente, con los camellos cargados de regalos y presentes. Llevan juguetes al Niño y además llevan también, bajo sus capas de armiño, buen vinillo y rica miel.\n\nEstribillo\n\nLa estrella los guía en su caminar, la noche está fría y hay que aligerar. Al mundo han venido la paz y el amor; y es por que ha nacido el hijo de Dios.\n\nⅡ\n\nAl amanecer el día los Reyes están llegando; y los recibe María, que José está trabajando. Los pajaritos cantaban y los Angeles también, cuando los Reyes llegaban a las puertas de Belén.\n\n(Al estribillo)\n\nZambra pastoril\n\nⅠ\n\nLa Virgen está lavando con el agüita que llueve, la ropa, que huele a nardo, es blanca como la nieve\n\nEstribillo\n\nQue los pañalitos parecen luceros, cuando están tendíos entre los romeros. Que los pajaritos echan a volar, para ver al Niño que está en el Portal.\n\nⅡ\n\nLa Virgen está bordando rayitos de luna nueva, en un pañolito blanco más blanco que la azucena. (Al estribillo)\n\nIII\n\nSu madre lo está vistiendo al Niño recién nacido, y el padre le está diciendo ¡ay! ¡qué bonito es mi Niño!\n\nTururú celestial\n\nⅠ\n\nA las puertas de la media hoche, con algarabía, llegaba el amor, y entre cantos y gran alegría al mundo venía el hijo de Dios, el hijo de Dios. Cuando estaban sonando las doce, en un portalito el Niño nació.\n\nEstribillo\n\n¡Ay que bien, que ha nacido en Belén! Pastorcitos venir al portal, que ha salido por leña José, la Virgen María lo quiere buscar. ¡Tururú, que con el tururú!, el portal se ha llenado de luz. Pastorcitos, venid a cantar al Niño Jesús, que ha nacido ya. Pastorcitos venir, pastorcitos llegar\n\nⅡ\n\nCuando daba en la torre la una ya estaba en la cuna el Niño Manuel, le acompaña el buey y la mula y los angelitos le vienen a ver, le vienen a ver. Cuando daba en la torre la una, ya estaba en la cuna el Niño Manuel.\n\n(Al estribillo)\n\nCatapún-Catapún!\n\nEn un portalito del viejo Belén, y en un pesebrito que encontró José, puso paja nueva, rubia como el sol antes que naciera el hijo de Dios.\n\n¡Catapún, catapún, catapún! La zambomba suena, cantemos, bailemos, que es la Noche Buena. ¡Ay catapún! ¡Catapún, catapún, catapún! Que suene el pandero, que esta noche es fiesta en el mundo entero.\n\nⅡ\n\nEn la Noche Buena vamos a cantar; hoy todas las penas tienen que acabar. La bota que ande, dame de beber y antes que se acabe me das otra vez. (Al estribillo)",
    "title": "Villancicos",
    "periodical": "candil",
    "issue_id": "1991-11",
    "year": 1991,
    "language": "es",
    "article_type": "article",
    "pages": "22-22",
    "page_number": 22,
    "word_count": 441,
    "article_char_count_full": 2408,
    "article_char_count_review": 2408,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
