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
    "article_id": "1998-11-25-left-letras-flamencas",
    "article_text_for_review": "Bulerías\n\no me llames la atención que es que estoy ya más loquito que el relojito de la estación.\n\ntraigo lo que quería la calle pa corré,\n\ncafé y los güenos días.",
    "title": "Letras flamencas",
    "periodical": "candil",
    "issue_id": "1998-11",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "25-25",
    "page_number": 25,
    "word_count": 32,
    "article_char_count_full": 163,
    "article_char_count_review": 163,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-11-25-right-fernando-el-de-la-morena",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nEllos, los protagonistas, dicen...\n\nRafael Valera Espinosa\n\nFernando Carrasco Vargas, «Fernando el de la Morena», se nos presenta sereno, expectante y deseoso de responder a nuestras preguntas con la frase «Ya era hora de que os\n\nacordárais de mí». Orguloso de su linaje y de su cuna «santiague-ra», muestra una y otra vez su pasión por cantes de su tierra, las siguirias y las bulerías. «Eso sí, sin olvidarme de las soleares!...». Está satisfecho de su soniquete flamenco y de sus vivencias. Desarrolla una especial filosofía en sus respuestas y siempre ante-pone la conservación de las raíces flamencas ante cualquier nuevo\n\nmovimiento innovador. Tiene los criterios claros sobre el baile flamenco. Ensalza y valora con pasión el arte de Tío Borrico, Tío Agujetas, Tío Sordera, Manuel Agujetas o\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"Recuerda\"]\n\nas y las bulerías. «Eso sí, sin olvidarme de las soleares!...». Está satisfecho de su soniquete flamenco y de sus vivencias. Desarrolla una especial filosofía en sus respuestas y siempre ante-pone la conservación de las raíces flamencas ante cualquier nuevo movimiento innovador. Tiene los criterios claros sobre el baile flamenco. Ensalza y valora con pasión el arte de Tío Borrico, Tío Agujetas, Tío Sordera, Manuel Agujetas o Fernando Terremoto. Recuerda con especial nostalgia su época de taxista por lo vivido y aprendido durante la misma y nos sorprende con una memoria amplia y un lenguaje cultiparlante. —¿Quién es La Morena? —Es mi madre. —¿Tu casta cantaora? —Nosotros estamos emparentados con los Jiménez, los Colas, los Moreno o «Moraos»... Pienso que tenemos bastante arte en nuestra sangre. —¿Cuándo comienza tu tra- yectoria artística? —Pues como la de cualquier gitanito de Jerez. Cuando era chiquillo, no es que repasara, digamos, la lista de las parroquias donde había bautizos, pero sí que me enteraba bien dónde había uno de los nuestros. Por aquel tiempo era muy normal que se corriera la voz de quién bautizaba. Tío Fernando, Tío Pedro... Aquello funcionaba mejor que la agencia Efe. Entonces llegaba yo a la fiesta correspondiente y no paraba hasta desgañitarme. Luego, como eran de otra generación, yo escuchaba a los viejos del lugar. Entre ellos puedo citar a Tío Borrico, Tío Cuquejo, Mari Bala, Antonia la Peña... Ya te he dicho que era muy chiquitito y vivía en la calle Nueva, donde Terremoto. Y estaba en todo el cogollo porque me gustaba. Recuerdo cuando la Perla, que en gloria esté, ese peazo de cantaora, venía a Jerez a casa de Tío Paulera —la casa del Mercé—, a la que también venían «Chano Lobato», «Jineto»... Todos venían de fiesta\n\n[ENDING CONTEXT]\n\nuna condición innata. La verdad es que las vertientes que hoy aflo-ran en la danza, en el baile..., son las que existen, son la propuesta...\n\n—¿Se abusa, por parte de la mujer, del taconeo?\n\n—Carmen Amaya era una bailaora que taconeaba maravillosamente, pero también movía los brazos, la cintura y todo lo demás con calidad inigualable.\n\n—¿No crees que, a veces, los remates bailadores por bulerías se hacen interminables?\n\nYo creo que para bailar hay que utilizar todos los elementos de tu cuerpo. Hasta los ojos...\n\n—Los gitanos antiguos decí- an: «¡Ni un arre que trote, ni un so que te pare!»...\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Fernando “el de la Morena”",
    "periodical": "candil",
    "issue_id": "1998-11",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "25-27",
    "page_number": 25,
    "word_count": 1539,
    "article_char_count_full": 9009,
    "article_char_count_review": 3402,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "Recuerda"
      }
    ]
  }
]
```
