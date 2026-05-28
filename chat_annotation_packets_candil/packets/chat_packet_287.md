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
    "article_id": "1994-03-7-left-si-fu-ramos-catalanes",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPero no lo somos. Y, con todo el respeto y la admiración —sí, la admiración, ¿por qué no decirlo si es verdad?—, hemos de arrepentirnos de la cobardía permanente a la hora de defender lo nuestro. Los andaluces nos miramos tanto el ombligo que, si nos lo ensucian o lo desprecian, pensamos casi siempre: si es que no saben, ¿qué más da? Y nos quedamos, como al perro que le quitan pulgas, orondos, satisfechos. Con eso de ser el pueblo más antiguo de Occidente estamos en la gloria, somos los más sabios. Lástima que seamos los más sabios uno a uno, porque juntos ya es otro cantar.\n\nY de cante vamos a hablar. O mejor dicho, de la ausencia de cante, de la falta de cante a que nos han condenado los poderes fácticos de la comunicación pública. De cante y de otras cosas, absolutamente andaluzas y\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"arte\"]\n\no que le quitan pulgas, orondos, satisfechos. Con eso de ser el pueblo más antiguo de Occidente estamos en la gloria, somos los más sabios. Lástima que seamos los más sabios uno a uno, porque juntos ya es otro cantar. Y de cante vamos a hablar. O mejor dicho, de la ausencia de cante, de la falta de cante a que nos han condenado los poderes fácticos de la comunicación pública. De cante y de otras cosas, absolutamente andaluzas y válidas que, por arte de birlibirloque, han desaparecido de la Radio que pagamos todos. Hace unos años desapareció Radio 4. Había aparecido los magazines y estaban ganándole a la audiencia de los programas especializados. En ellos había todo y posibilitaban la aparición de los llamados «comunicadores». La radio, al parecer, no empezó a «comunicar» hasta ese momento. Los nombres de Deglané, Pecker, Matas, Prats, Quintero, Del Olmo, Soler Serrano, González, Mateo, Pérez Orozco, Prat y decenas más, no habían comunicado. Como a este país le cabe bastante, también le cupo este cuento del alfajor, y hete aquí que la radio pública —maestra de todas las demás y a los hechos me remito— comienza a ser discípula de las cadenas privadas. Y, claro, como el español medio no puede pagar los dineros astronómicos que cuestan los comunicadores, lo mejor es ir desprendiéndose de los «curritos» y las sintonías que, entre sueldos —dignos sueldos, esto es cierto—, seguros sociales y mantenimiento son poco rentables. Pero si están hartos de repetir que la rentabilidad de la radio pública no es e\n\n[ENDING CONTEXT]\n\ny aquí paz y después gloria.\n\nEn Cataluña —porque es una evidencia, he titulado así este escrito—esto no hubiera pasado. Somos viejos y, hasta es posible, sabios. Pero todavía tenemos mucho que aprender de quienes sí saben luchar por su identidad y sus valores. No tenemos que copiar nada de nadie. Pero el saber no ocupa lugar y aprender de quienes saben es un hermoso ejercicio. Se corren riesgos por decir esto, en la seguridad de que tampoco habrá voces que se levanten por protestar, si ocurren.\n\nPero mi tierra es mi madre y no ha nacido el gobierno que la ofendida sin que yo grite en contra.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Si fuéramos catalanes...Ramón",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "7-7",
    "page_number": 7,
    "word_count": 1150,
    "article_char_count_full": 6813,
    "article_char_count_review": 3145,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "arte"
      }
    ]
  },
  {
    "article_id": "1994-03-8-left-todos-por-el-oro",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nHomenaje a Fernanda y Bernarda en Algeciras\n\nPedro Sánchez Ortega\n\nLos pueblos, sus gobernantes, los colectivos culturales y la sociedad en general, tenemos la obligación de fomentar la cultura partiendo desde la base de cultivar sus tradiciones más ancestrales, y el flamenco es la más secular y genuina del Pueblo andaluz.\n\nPor ello, para reconocer las aportaciones que los viejos maestros hicieron al flamento, la junta directiva de la Sociedad del Cante Grande de Algeciras, hemos querido compendiar todo ese agradecimiento, en forma de homenaje, en las irrepetibles Fernanda y Bernada de Utrera, como actuales máximos exponentes de la grandeza cantaora de nuestra tierra.\n\nFernanda ha situado el cante por soleá en un plano superior, con la sabiduría de su estirpe y la jondura de su ancestral\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_02 | trigger=\"pura\"]\n\ne y la jondura de su ancestral tierra utrerana, habiendo alcanzado unos límites de expresividad difícilmente igualables. Si para esculpir un tercio por soleá hacen falta siglos, para encontrar una figura de la dimensión de Fernanda acaso hagan falta milenios. En una época donde el flamenco intrascendente y pueril es moneda común, Fernanda de Utrera ha aportado las esencias constitutivas de lo trascendente, de lo hermosamente bello, como la más pura esencia de la plástica de la expresión jonda. En Fernanda nada es repetitivo, nada es calculado, nada es banal, ni siquiera sus angustiantes silencios. Un quejío de Fernanda es como una tragedia que surge de improviso; como un volcán que con el solo estallido desordena nuestros sentidos, pero con la angustia vital de su lucha con el cante tercio a tercio. Fernanda jamás rehúye su compromiso con el cante, porque tiene conciencia del valor singular de su raza en el devenir histórico del flamenco. Fernanda, cuando canta, no se parece a nadie ni a nada, sólo a Fernanda. Ahí es donde radica la verdadera dimensión y grandiosidad del cante de esta utrerana. Ahí radica el verdadero concepto que como arte tenemos del Flamenco. Solterita y sin compromiso, por ahora, a veces me pregunto: ¿Creerá en el amor esta mujer que canta al amor por soleá? Pleito tengo con mi gente yo no sé si perderé, porque quieren que me case con quien no\n\n[ENDING CONTEXT]\n\na su tío, el inolvidable Diego del Gastor.\n\nEsto fue, en síntesis, lo que dio de sí esta noche flamenca. Una noche de las que hacen historia, y un homenaje que siempre estará en nuestro recuerdo.\n\nActos de esta naturaleza, son los que hacen que sintamos orgullo de amar este hermoso arte, a la vez que nos estimulan para continuar trabajando por él.\n\nNosotros, en nuestra modestia, y sin eludir un ápice la responsabilidad que como aficionados nos corresponde, vamos a seguir luchando para que este hecho diferencial de la cultura andaluza, sea universalmente conocido en su más genuina pureza.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Todos por el oroPedro",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "8-9",
    "page_number": 8,
    "word_count": 994,
    "article_char_count_full": 6026,
    "article_char_count_review": 3007,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_02",
        "family": "AUTH",
        "trigger": "pura"
      }
    ]
  },
  {
    "article_id": "1994-03-10-left-tertulia-en-torno-a-fernanda-y-b",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEntre los actos paralelos organizados por la Sociedad del Cante Grande de Algeciras, con motivo de la V Palma de Plata, en homenaje a Fernanda y Bernarda de Utrera, destaca sobremanera la mesa redonda celebrada en torno a la figura artística de ambas, no sólo por la categoría de los contentulios, sino por la relevancia de lo que en ella se dijo.\n\nCon gran asistencia de aficionados, la tertulia estuvo protagonizada por: Manuel Martín Martín, crítico de gran circulación de Diario 16; Aurelio Gurrea Chalé, musicólogo; Onofre López, crítico flamenco de Radio Huelva Cadena Ser; Luis Soler Guevara, investigador flamenco, a quien se le debe gran parte de la energética de este acto; Evaristo Heredia Maya, reconocido aficionado y directivo de la Sociedad del Cante Grande; Antonio Rubio Díaz,\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"grandeza\"]\n\nto; Evaristo Heredia Maya, reconocido aficionado y directivo de la Sociedad del Cante Grande; Antonio Rubio Díaz, presidente de honor de la misma y José García Navarro, directivo. La moderación corrió a cargo de José Luis Vargas Quirós. Durante el transcurso del debate también intervinieron algunos de los asistentes. Quede claro que en la tertulia que a continuación transcribimos, no se pretendió patrimonializar en Fernanda y Bernarda toda la grandeza del arte flamenco, pero sí enfatizar que siempre serán hontanar y espejo donde deberían beber y mirarse las generaciones venideras. Moderador: Manolo, ¿son Fernanda y Bernarda los últimos exponentes del cante gitano? Manuel Martín: Pienso que las que yo llamo ilustres Fernanda y Bernarda de Utrera son el último eslabón de la cadena flamenca de tradición oral. Me explico: Vivimos actualmente un flamenco microemotivo, un flamenco que está en franca decadencia, un flamenco que está perdiendo sus valores existenciales, sus valores vitales y, sobre todo, sus valores espirituales. Hay muchas causas que me inducen a pensar esto; una de ellas es que, salvo raras excepciones, el flamenco contemporáneo está ejecutado por intérpretes de plástico. Intérpretes que conocen el cante a través de la discografía; intérpretes que carecen de lo más fundamental que son las vivencias. La prueba la tenemos, por citar un caso gráfico, en que observamos que los guitarristas aprenden a través de la discografía. Carecemos de guitarristas de acompañamiento. Y no hablo a nivel de aficionao, sino de quienes cobran por encima de las doscientas mil pesetas. Entonces, si contemplamos la época de oro que fue marcada por Tío Juan Talega, Perrate de Utrera, Joselero, Antonio Mairena y por tantos y tantos hombres y mujeres que quedaron condenaos al ostracismo, cuando no al anonimato, pienso que hoy en día los eslabones que nos unen a lo que el flamenco fue, y a lo que el flamenco debiera ser, entre ellos se encuentran precisamente Fernan\n\n[ENDING CONTEXT]\n\nme atrevería a decir, que más que crear la cantiña lo que hacía era repetir los cantes que habían hecho otros, y que fue la familia la que se convirtió en depositaria de ese cante, de donde viniera, redefiniéndolo y dándole la dimensión musical que hoy tiene.\n\nEn esto han tenido mucho que ver Fernanda, Bernada y El Funi.\n\nEs decir, que la cantiña quien la define tal y como hoy la conocemos es esta familia. Entre Tío Benito, Diego, Fernanda La Vieja, María Peña, etc. Pero quien la eleva a la máxima categoría son Fernanda y Bernarda.\n\n(Y aquí, naturalmente, concluyó la tertulia). ■\n\nCarlos Cruz\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Tertulia en torno a Fernanda y Bernard de UtreraPedro",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "9-14",
    "page_number": 9,
    "word_count": 6216,
    "article_char_count_full": 35331,
    "article_char_count_review": 3607,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "grandeza"
      }
    ]
  },
  {
    "article_id": "1994-03-15-left-por-soleares-romualdo",
    "article_text_for_review": "Por Soleares Tengo surrapas de pena y en cuanto caen cuatro gotas se me pone el arma negra.\n\nSin respeto ni cuidao m'has roto la prenda mía; permita Undebé der sielo que te lo degüerva un día.\n\nQue tú te tienes que vé, comío de los gusanos por lo qu'acabas d'hasé.\n\nAquér que pis'a los pobres y martrat'ar desvalío, que tenga mucho cuidao no le jechen mar vajío.\n\nEn fin, ¿qué se le va a hasé? Totá, lo que fimos, somos; ni más ricos ni más pobres, yo tan triste y tú tan tonto,\n\nNo tienes perdón de Dios: ¡mardita sea tu sangre, mardito tu corasón!\n\nNo sé qué será peó, si un listo con su mardá o un tonto sin ton ni son.\n\nLa jangá que t'has cargao, payo de malos pañales, no la pagas ni quemao.\n\n¡Mardita sea! vete a La Porra, ¡que no te vea!",
    "title": "Por Soleares Romualdo",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 148,
    "article_char_count_full": 744,
    "article_char_count_review": 744,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1994-03-15-right-eleg-a-a-dolores-parral",
    "article_text_for_review": "Elegía a Dolores Parral\n\nLas cuatro de la mañana. Cuatro versos que desgrana la gitana campana de San Grabié.\n\nSe desnuda de gente el café...\n\nEl ambiente lo maltrata maloliente colonia barata y suspiros de aguardiente.\n\nLas sillas. como traviesas chiquillas, se acurrucan en las mesas. Bajo la atmósfera espesa lagrimean los espejos de la sala.\n\nE. González de Hervás\n\nAl pasar junto a la estufa la gata de Angora bufa. Después... silencios de mujer mala.\n\n¿Qué ha pasao aquí esta noche que no ríe «La Parrala»?...\n\n¿Dónde su risa espantosa, mariposa de la sala?...\n\n¿Qué le ha pasado esta noche ar cante de «La Parrala»?\n\nEl aire la retorcía y un desfleque del mantón rompía la melodía brutal de su corazón.\n\nDe su libro «ER CANTE Y OTROS ESCRITOS» «Amanesía y llovía, y en brazos de Manuer Torre \"La Parrala\" se moría».",
    "title": "Elegía a Dolores Parral, «La Parrala» E. González de Hervás",
    "periodical": "candil",
    "issue_id": "1994-03",
    "year": 1994,
    "language": "es",
    "article_type": "article",
    "pages": "15-15",
    "page_number": 15,
    "word_count": 143,
    "article_char_count_full": 822,
    "article_char_count_review": 822,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
