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
    "article_id": "1981-03-8-right-joselero-y-el-quino",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPor Alejandro Fernández Cotta\n\nAllá por los años cincuenta, cuando conocí a Joselero y al Quino, Morón de la Frontera —nuestra patria chica— estaba viviendo su época de mayor esplendor y riqueza, no comparable a ninguna otra.\n\nDecir esto de un pueblo como Morón significa mucho. Al igual que su escudo, que representa un caballo sin jinete, ensillado y desbocado, Morón es un corcel suelto de brida, como ya escribí en un soneto que le dediqué hace tiempo. Un pueblo del que no basta decir que es generoso o pródigo, incluso dilapidador. Morón es, valga el tópico, eso y muchí-simo más.\n\nSuperados ya los años de «la jambre», con su cohorte de miserias traídas por la guerra y por el boicot que desde la ONU se nos impuso, el campo y la industria comenzaron a brillar y arrojaron sobre Morón un río\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"superar\"]\n\nco, eso y muchí-simo más. Superados ya los años de «la jambre», con su cohorte de miserias traídas por la guerra y por el boicot que desde la ONU se nos impuso, el campo y la industria comenzaron a brillar y arrojaron sobre Morón un río de dinero, que lejos de ser ate-sorado, corrió por sus calles y por sus gentes, a toda velocidad. No hay freno que sujete a ese pueblo. Sus fiestas eran únicas. Sus ferias, con un egregio despilfarro, llegaron a superar en calidad a las de Sevilla. No eran pocos los sevi-llanos que se volcaban en ellas como si fueran propias. Entre los artífices y protagonistas de aquel estallido, desde el exclusivo punto que ahora nos interesa, destaco especialmente uno: Antonio Camacho. «Don Antonio», para Joselero. Y aquella fue también, a mi entender, la edad de oro de ese cantaor, que se inició y quizá se consumó bajo su mecenazgo. Antonio Camacho e ra un gran aficionado al flamenco, sobre todo al cante de fiesta. No pretendo decir con esto que no gustara del cante grande —precisamente el recuerdo que Joselero le dedica al Quino lo es bailando por soleares—, pero según mi experiencia personal, cada vez que lo vi metido en faena o le oí comentar alguna juerga, siempre predominaba esa faceta, que digamos sea de paso, tan difícilmente alcanza alturas auténticas de verdadera valía. Y ahí, en ese terreno, el Quino y Joselero le venían a la medida. Poco hay que contar de Joselero —o quizá mucho, o de otro modo distinto; no lo sé—. Pero ¿quién\n\n[ENDING CONTEXT]\n\ncreo que se metieron cantando por bulerías. Fueron los años de oro del Quino y de Joselero, no en su aspecto económico, al que no me estoy refiriendo aunque bien pudiera quedar sobreentendido, sino en su cante y en su felicidad.\n\nEl Quino murió pocos años después, no recuerdo la fecha. Antonio Camacho se vio obligado a suspender pagos, se casó y se retiró a una vida má s tranquila. También ha Con ellos se fue también Morón 'a dormir el sueño de una historia que no volverá a repetirse.\n\nMillán de Priego, 21 - Teléf. 22 66 55 JAEN ACADEMIA DE ENSEÑANZA Básica, Media y Superior 《JOSE LUIS LOPEZ》\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Joselero y El Quino",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 1282,
    "article_char_count_full": 7350,
    "article_char_count_review": 3110,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "superar"
      }
    ]
  },
  {
    "article_id": "1981-03-10-right-algo-sobre-los-cantes-de-la-madr",
    "article_text_for_review": "Con motivo del Octavo Congreso de Actividades Flamencas, he tenido la suerte de escuchar, a requerimiento de Antonio Murciano y Deogracia Fernández, LOS DOS CANTES grabados por el gran cantaor Rafael Romero, «El Gallina», y que, según él, son los cantes de la «madrugá». Ambos amigos me pidieron que los escuchara con atención para después darles mi opinión. Los escuché con la máxima atención de que soy capaz, y en cuanto al primero de ellos les dije que se trataba de un cante que no me decía nada, de un cante muy frío, sin vida ni flamenquismo. Que tampoco me dijo nada acerca de quiénes fueron sus padres ni de dónde podía proceder y que, escuchándolo, me encontraba ante un cante al que no podía relacionar con ninguno de los que conservo grabados en las voces de D. Antonio Chacón, El Herrero, Escacena, Niño Medina, Niño de la isla, etc., etc. En realidad, señores aficionados, el «hallazgo», desde mi punto de vista, no merece ser festejado por esa carencia de flamenquismo a que he hecho alusión.\n\nEn cuanto al segundo cante de la «madrugá» no tuve necesidad de prestarle una especial atención, porque, una vez más, estaba escuchando la TARANTA MINERA que construyera Chacón con el material que recogió en Levante. He aquí la letra:\n\nAy, el corazón\n\nel corazón se me parte\n\ncuando pienso en tu partía (1)\n\nAlgunos aficionados me aseguran que el cante de la «madrugá» existió, y además como padre de la numerosa prole que hoy escuchamos. Esta aseveración no la pongo en duña. Tampoco pongo en duda la honradez profesional de Rafael, pero, amigos, lo que yo no puedo hacer es colaborar con nadie diciendo que los cantes que he escuchado son los auténticos de la «madrugá». Es más, en este caso con\n\nPor M. YERGA LANCHARRO\n\ncreto de «El Gallina», tengo que oponerme a sus propósitos de dar vida a ese segundo cante utilizando para ello la conocida TARANTA MINERA DE CHACON.\n\nNi que decir tiene que Rafael pueda recoger el primer cante, bautizarlo y darle el nombre de la «madrugá», de la «trasnochá» o del «relevo», y en vista de que la criatura no tiene padre, puede adoptarlo y obrar con absoluta libertad para hacer de él cuanto se le antoje.\n\nTermino diciendo a aquellos aficionados interesados por hallar la raíz de estos cantes, que investiguen sin desmayo hasta ver si tienen la suerte de localizar un aficionado con más de cien años al pelo, para que les pueda asegurar que existió el cante de la «madrugá» y cuál fue su estilo.",
    "title": "Algo sobre los cantes de lu «Madrugá»",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "10-10",
    "page_number": 10,
    "word_count": 429,
    "article_char_count_full": 2444,
    "article_char_count_review": 2444,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-03-11-left-checo-jean",
    "article_text_for_review": "a Elena y Manuel Urbano\n\n¡Qué rosario de ayes, qué pespuntes de entrañas, qué gemir de garganta, qué médula de escalofríos, qué fuego de huesos, qué temblores de alba! Cerrados los ojos, más apuñalan; entreabiertas sus pupilas filtran la queja añejada; y la mano en el muslo es repique de alma. Para mi recuerdo -de guitarra ilustre, «lunar» - «Charo» cantó en Jaén aquel sábado: una noche ensalzada.",
    "title": "Checo Jean",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "11-11",
    "page_number": 11,
    "word_count": 68,
    "article_char_count_full": 400,
    "article_char_count_review": 400,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1981-03-11-right-rafael-romero",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen...\n\n—En el número de «Candil», donde publiquemos tu entrevista, aparecerá una colaboración en la que, no obstante reconocer tu gran categoría cantaora, se sostiene que tus cantes de la madrugá son, uno un «hallazgo»; y el otro, la taranta minera de Chacón. ¿Qué tienes que decir sobre eso?\n\n—Pero, bueno... ¿quién ha dicho eso? Yo los cantes de la madrugá no los he inventao, na más que los he aprendío. Mira, el padre de Perico, que está aquí delante, Pedro el del Lunar, ha sío uno de los que me han hablao de esos cantes que eran cuando se hacía el relevo en las minas. Y él habló también de esto con Chacón.\n\nYo no me invento ná porque er cante no se inventa.\n\nEn Linares, donde estaba yo cuando era joven, les decían cantes de la madrugá por los re-fevos y yo me\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_02 | trigger=\"escuch\"]\n\ncantes de la madrugá no los he inventao, na más que los he aprendío. Mira, el padre de Perico, que está aquí delante, Pedro el del Lunar, ha sío uno de los que me han hablao de esos cantes que eran cuando se hacía el relevo en las minas. Y él habló también de esto con Chacón. Yo no me invento ná porque er cante no se inventa. En Linares, donde estaba yo cuando era joven, les decían cantes de la madrugá por los re-fevos y yo me acuerdo de haber escuchado estos cantes en Linares por aquel tiempo al «Tonto Carica Dios», que era un gitano aficionao que cantaba de maravilla. Vuelvo a repetir que yo no he inventao nada. Yo me limito a cantar lo que he escuchado y aprendío... y siempre procuro hacerlo honradamente con sentimiento y arte. Yo creo que en el flamenco, y sobre todo en estos cantes sumergidos en el tiempo, es muy difícil saber con certeza quién ha sido el padre o la madre de los mismos. Por otro lao, como tú sabes, yo he actuado mucho con el padre de Perico y de él he aprendió muchas cosas. Y tú sabes que Perico el del Lunar fue tocaor de Chacón y, como compañero suyo, habló muchas veces de cante con él. Y luego Perico me ha hablado a mí y en algunos cantes me ha aconsejado cómo tenía que empezarlos o rematarlos. Precisamente, al escucharme a mí esos cantes, fue Perico el que me dijo una vez que también los cantaba Chacón y que se llamaban de la madrugá. También te quiero decir que en Linares ha habió gente que ha hecho los cantes mineros maravillosamente, incluso mejor que en otros sitios, porque Linares ha tenido mucho arte palcante, y si no, ¡que se lo digan a Illanda! ¿Y porqué no se iban a cantar en Linares los cantes de la madrugá? ¿Quién me los va a discutir a mí, que los he oído cuando era un chaval? ¡Y tengo más de 70 años! —Rafael, ¿tu padre cantaba? —No. Mi padre era aficionao, tocaba la guitarra y hacía los cantes de Illanda; porque en Andújar había mucha gente aficioná que cantaba las cosas de I\n\n[ENDING CONTEXT]\n\nqué sé. Quizás sea por que yo tengo un estilo mu especial... ¡Ojo!, cuando canto pa bailar; porque yo he cantao pa bailar'mu poco. A mí me ha gustao cantar siempre solo; lo que pasa es que...\n\n—Por último una pregunta indiscreta. ¿Por qué te dicen El Gallina?\n\n—En Madrid, en aquellos años malos de después de la Guerra, canté aquello de «la gallina papanata». Entonces, un marqués le dice a uno: «oye, quién es ese? Y el otro, como no sabía a quién se refería le pregunta, que cual, que quién decía. Y el marqués le dice, «ese, el gallina». Y desde entonces... las cosas. Es que en aquellos años...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Rafael Romero",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 1763,
    "article_char_count_full": 9508,
    "article_char_count_review": 3574,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_02",
        "family": "PED",
        "trigger": "escuch"
      }
    ]
  },
  {
    "article_id": "1981-03-13-left-las-letras-flamencas-de-antonio-",
    "article_text_for_review": "Si, como es cierto, Arcos de la Frontera es el pueblo de España con más poetas por metro cuadrado, a no dudarlo, Antonio Luis Baena ocupa el espacio literario de varios kilómetros. No vamos a ser nosotros quienes recordemos ahora el significado de sus libros; tampoco incidiremos en lo que es sabido: muchas letras que nacieran de los hondos y serenos sentires flamencos de Antonio Luis Baena, corren ya por la superficie flamenca como anónimas y populares —¿hay algo mejor que se pueda decir en beneficio de una copla y su autor?—. Hoy, tan sólo, desde «Candil, queremos felicitarnos por dar a la luz esta decena de soleares inéditas del poeta de Arcos quien, precisamente con ellas, rompe un largo silencio y su militancia en la que el donomina «poesia secreta». Por cada esquina que paso voy rebuscando los besos que tú has ido olvidando.\n\nMira si tengo mal sino que puse un barco en la mar y me lo encontré en el río.\n\nNo me lo preguntes más. Si te quise o no te quise arreglo no tiene ya.\n\nQue yo el camino no encuentro y tú has perdido el camino: Hay que darle tiempo al tiempo.\n\nMira tú que contradiós, tú por la acera de enfrente, por la otra acera yo.\n\nTu puerta tiene un cartel: El que quiera entrar que entre... Y yo sin saber leer.\n\nTanto buscarte pa na. Y ahora que ya no te busco te he tenio que encontrar.\n\nPara dejar de quererte no tiene fuerzas la vida ni tiene poder la muerte.\n\n¿Cómo te voy a olvidar si hasta el aire que respiro me lo tienes tú que dar?\n\nMira lo que son las cosas... Con lo mucho que te quise, lo poquito que me importas.",
    "title": "Las letras flamencas de José Luis Baena",
    "periodical": "candil",
    "issue_id": "1981-03",
    "year": 1981,
    "language": "es",
    "article_type": "article",
    "pages": "13-13",
    "page_number": 13,
    "word_count": 293,
    "article_char_count_full": 1558,
    "article_char_count_review": 1558,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
