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
    "article_id": "1998-03-29-right-premio-de-ensayo-gonz-lez-climen",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAntonio Coca-Bonilla\n\na Fundació Gresol Cultural convoca una nueva edición del Premio de Ensayo González Climent, que este año alcanza su sexta edición y que como en los anteriores vuelve a patrocinar, caso único, el cantaor de Posadas Luis de Córdoba. Asimismo cuenta, nuevamente, con la colaboración de la Confederación Andaluzas de Peñas Flamencas y, por primera vez, con la Federación de Entidades Culturales de Catalunya (FECAC), entidades con las que ha suscrito un convenio de colaboración.\n\nEl premio, cuyo tema versa sobre el Arte Flamenco en cual-quiera de sus facetas (cante, toque y baile) y enfoques (histórico, geográfico, musical, lingüístico, social, etc.), presenta como novedad más importante en esta edición, sexta, el aumento de la dotación económica que es de 300.000 pesetas.\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"público\"]\n\nen esta edición, sexta, el aumento de la dotación económica que es de 300.000 pesetas. Asimismo, al autor galardonado se le hará entrega de la «Distinción Gresol Cultural», creación del artista plástico Abdó Martí. El trabajo, original e inédito, y con una extensión compren- dida entre ochenta y cien folios a doble espacio y por una sola cara, que resulte premiado será de ensayo González Climent editado por Aquí+Multimedia y presentado en acto público expresamente convocado a tal efecto, coincidiendo con la celebración del Festival de Arte Flamenco de Catalunya, que anualmente se celebra en Cornellà de Llobregat. Cada autor presentará cinco ejemplares que no contendrán nombre ni firma del autor. En la hoja de cubierta se consignarán el título del ensayo y un lema. Se adjuntará sobre cerra-do en cuyo exterior figure el lema y en su interior nombre, dirección, teléfono y cuantos datos biográficos se crean oportunos referidos al autor. Los envíos se harán a: Fundació Gresol Cultural. Mossèn Andreu, 13-19. 08940 Cornellà de Llobregat (Barcelona), antes del 18 de octubre de 1998. La Fundación en\n\n[ENDING CONTEXT]\n\npor generosa cesión de sus anteriores entidades organizadoras y a sugerencia de Luis de Córdoba, la Fundación Gresol Cultural tomó el relevo en 1995, convocando la tercera edición, que ganó Daniel Pineda Novo con su excelente trabajo «Juana, La Macarrona y el baile en los Cafés Cantantes». Al año siguiente, cuarta edición, el triunfador fue Eugenio Cobo con su interesantísimo rastreo de materiales y noticias, muy escasamente conocidos, «El Flamenco en los escritores de la Restauración (1876-1890)». Por fin, el premio de la quinta edición, 1997, quedó desierto por decisión del jurado.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Premio de Ensayo «González Climent»",
    "periodical": "candil",
    "issue_id": "1998-03",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "29-30",
    "page_number": 29,
    "word_count": 943,
    "article_char_count_full": 6083,
    "article_char_count_review": 2726,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "público"
      }
    ]
  },
  {
    "article_id": "1998-05-3-right-editorial",
    "article_text_for_review": "Nadie podrá argumentar que la dedicación de este número de Candil a la memoria de Federico García Lorca, pueda resultar un tópico en este año en el que se conmemora el centenario de su nacimiento. Ya, a comienzos de los ochenta, miembros de la Peña Flamenca de Jaén —Rosario López y Juan Antonio Ibañez, por ejemplo—, ofrecían un reconocido homenaje al poeta granadino con la edición del disco «Entierro para Federico García Lorca». También en la revista número 26 de 1983, Juan Antonio Ibañez volvía a mostrar su admiración por el poeta granadino con su trabajo «Lo jondo en F. García Lorca». Y más, el editorial de la revista número 46 igualmente estuvo dedicada a este autor, así como Félix Grande exponía sus teorías en «García Lorca y el flamenco», en la número 69; Agustín Gómez lo hacía en el 112 con su «Antecedentes y consecuentes cordobeses del lorquismo», y en la misma edición, Alfredo Arrebola abundaba sobre el tema con su trabajo «Concurso de Granada: ¿Una utopía estética y reivín-\n\ndicativa de Falla y Lorca?».\n\nMas, si alguien quisiera acusar de topicazo a este número, el argumento quedaría nulo desde su origen, porque quién mejor que el poeta de Fuente Vaqueros para recibir un homenaje por parte de los flamencos? Los méritos de Federico no sería menester enumerarlos por ser suficientemente conocidos por todo el mundo, literario o no, ya que su obra goza, afortunadamente, de un contenido jondo y una enorme popularidad.\n\nSi la revista Candil ha elegido la figura del poeta grandino, es porque fue uno de los más fervientes defensores del Arte Flamenco, pues gracias a su esfuerzo, colaboración y entusiasmo en la organización del Concurso de 1922, de Granada, nuestro arte sufrió un impulso potenciador importantísimo en su difusión y en su apreciación como música del pueblo andaluz. Quizá sólo esto último habría bastado para justificar el número. Sin embargo, también queremos resaltar cómo el flamenco está presente en su obra y es su «Poema del Cante Jondo» uno de los más conocidos, representando un auténtico hito en la poesía de nuestro país.\n\nPor otro lado, la defensa que efectuó de este nuestro arte frente a las críticas injustas y desaforadas que ejercieron intelectuales como Pío Baroja, Ortega y Gasset o Eugenio Noel, procuró que otros literatos de su época reconocieran y estimaran lo que el flamenco ha supuesto y supone, como música singular y universal para nuestra cultura.\n\nCierto es que no fue un profundo conocedor del flamenco, como así se traduce de la lectura de su conferencia sobre el mismo. Pero no importa, son sus desvelos y sus luchas por hacer resurgir lo esencial y verdadero de este arte, los que dan la auténtica medida de un aficionado cabal, que sí entendía el pellizco que su alma sufría con el quejío siguiriyero de Manuel Torre, la De los Peines, Chacón o El Tenazas.",
    "title": "Editorial",
    "periodical": "candil",
    "issue_id": "1998-05",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "3-3",
    "page_number": 3,
    "word_count": 480,
    "article_char_count_full": 2834,
    "article_char_count_review": 2834,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-05-4-left-una-existencia-intensa",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nManuel Ruiz Amezcua\n\nFederico García Lorca nace, el 5 de junio de 1898, en Fuente Vaqueros, pueblo de la vega granadina. Su padre era dueño de tierras y cortijos en la zona. Casó en segundas nupcias con su madre, maestra de escuela. «De mi padre heredé la pasión, de mi madre la inteligencia», dirá Lorca años más tarde. Su madre fue la que le enseñó las primeras letras y lo guió por el mundo de la sensibilidad, la artística y la humana. A los 5 ó 6 años Federico se traslada con su familia a Valderrubio, llamado entonces Asquerosa, pueblo junto al río Cubillas. «Toda mi infancia es pueblo. Pastores, campos, cielo, soledad...», escribió en una ocasión. La dedicatoria de su Libro de Poemas es aún más explícita: «... Tendrá este libro la virtud de recordarme en todo instante una infancia\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_02 | trigger=\"declaró\"]\n\nco se traslada con su familia a Valderrubio, llamado entonces Asquerosa, pueblo junto al río Cubillas. «Toda mi infancia es pueblo. Pastores, campos, cielo, soledad...», escribió en una ocasión. La dedicatoria de su Libro de Poemas es aún más explícita: «... Tendrá este libro la virtud de recordarme en todo instante una infancia apasionada correteando desnuda por las praderas de una vega sobre un fondo de serranía». $ ^{1} $ En cierta entrevista declaró una Españolito que vienes al mundo, te guarde Dios. Una de las dos Españas ha de helarte el corazón ANTONIO MACHADO vez: «Siendo niño viví en pleno ambiente de naturaleza. Como todos los niños adjudicaba a cada cosa, mueble, objeto, árbol, piedra, su personalidad. Conversaba con ellos y los amaba. En el patio de mi casa había unos chopos. Una tarde se me ocurrió que los chopos cantaban. El viento, al pasar por entre sus ramas, producía un ruido variado en tonos, que a mí se me antojó musical. Y solía pasarme las horas acompañando con mi voz la canción de los chopos... Otro día me detuve asombrado. Alguien pronunciaba mi nombre separando las sílabas como si deletreara \"Fe... de... ri... co...\".² Miré a todos lados y no vi a nadie. Sin embargo, en mis oídos seguía chicharreando mi nombre. Después de escuchar largo rato, vi que eran las ramas de un chopo viejo, que al rozarse entre ellas, producían un ruido monótono, que jumbroso, que a mí me pareció mi nombre». Desde muy niño, se sintió unido a la tierra, no podremos entender a Federico y su mundo $ ^{3} $ si no lo situamos plantado en el paisaje que lo vio nacer y crecer. El paisaje y el paisanjaje, la cultura oral y tradicional en los labios del pueblo. Historias que oía contar a familiares y criados. Las criadas, tantísimas, como Mariquita la Recovera, Dolores la Colorina o Anilla la Juanera. Todas le enseñaron canciones tradicionales, historias de bandidos, o cuentos populares. Federico lo dejó escrito: «¿Qué sería de los niños ricos si no fuera por las sirvientas que le ponen en contacto con la verdad y la emoción del pueblo?». En su conferencia sobre las nanas infa\n\n[ENDING CONTEXT]\n\n«Llagas de amor». Sonetos del Amor Oscuro. Op. C.\n\n80) Soneto «El amor duerme en el pecho del poeta». Sonetos del Amor Oscuro. Op. C.\n\n81) Del Llanto por Ignacio Sánchez Mejías.\n\n82) M. A. ARANGO: Símbolo y simbología en F.G.L. Op. C.\n\n83) «Romance sonámbulo». Del Romancero Gitano.\n\n84) CARLOS BOUSOÑO: Teoría de la expresión poé- tica. Gredos, Madrid, 1973.\n\n85) RAMÓN MENÉNDEZ PIDAL: Castilla, la tradi- ción, el idioma. Austral. Madrid, 1966.\n\n86) MIGUEL ARTIGAS: Don Luis de Góngora. Real Academia Española de la Lengua. Madrid, 1925.\n\n87) FRANCISCO GARCÍA LORCA: Federico y sumundo. Op. C.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "Una existencia intensa $ ^{*} $",
    "periodical": "candil",
    "issue_id": "1998-05",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "4-13",
    "page_number": 4,
    "word_count": 11067,
    "article_char_count_full": 64278,
    "article_char_count_review": 3725,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_02",
        "family": "HERIT",
        "trigger": "declaró"
      }
    ]
  },
  {
    "article_id": "1998-05-14-left-canci-n-de-jinete-poema",
    "article_text_for_review": "Canción de jinete\n\nCórdoba. Lejana y sola.\n\nJaca negra, luna grande, y aceitunas en mi alforja. Aunque sepa los caminos yo nunca llegaré a Córdoba.\n\nPor el llanto, por el viento, jaca negra, luna roja. La muerte me está mirando desde las torres de Córdoba.\n\n¡Ay qué camino tan largo! ¡Ay mi jaca valerosa! ¡Ay que la muerte me espera, antes de llegar a Córdoba!\n\nCórdoba. Lejana y sola.\n\nFederico García Lorca",
    "title": "Canción de jinete (poema)",
    "periodical": "candil",
    "issue_id": "1998-05",
    "year": 1998,
    "language": "es",
    "article_type": "article",
    "pages": "14-14",
    "page_number": 14,
    "word_count": 72,
    "article_char_count_full": 409,
    "article_char_count_review": 409,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "1998-05-14-right-garc-a-lorca-y-el-flamenco-un-am",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMiguel Ángel González\n\nMi agradecimiento a Enrique Morente y a Curro Andrés por sus declaraciones y grabaciones, y muy especial gratitud a Juan de Loxa por su generosa colaboración.\n\nH a suscitado controver- sias y se han escrito ensayos acerca de ella, se ha comentado y analizado minuciosamente la atracción que el cante y su mundo ejercie- ran sobre Federico García Lorca, evidenciada de manera\n\nejemplar en su entusiástica dedicación y su compromiso con el mítico Concurso de 1922. Pero, pese a tratarse de otro hecho inusual y con importantes repercusiones artísticas, no se ha concedido ni una mínima parte de la atención que merece a la reciprocidad de esa pasión, es decir, no nos hemos detenido a considerar en profundidad el exaltado amor que el universo flamenco y sus artífices\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_03 | trigger=\"origen\"]\n\nese a tratarse de otro hecho inusual y con importantes repercusiones artísticas, no se ha concedido ni una mínima parte de la atención que merece a la reciprocidad de esa pasión, es decir, no nos hemos detenido a considerar en profundidad el exaltado amor que el universo flamenco y sus artífices manifiestan por Federico. Sin embargo, jamás la obra de un poeta culto ha sido tan copiosamente asimilada por el flamenco en sus varias facetas ni dado origen a un fenómeno tan esplendoroso de identificación de sensibilidades como en el caso del granadino, y ello a pesar de su insistencia en rechazar con energía el sambenito de autor popular: «Mi arte no es popular. Yo nunca he considerado que lo sea», declaraba tajante-mente en 1933. No es este el momento de plantear las objeciones que pudieran hacerse a esa afirmación; sólo recordaremos que cualificados estudiosos y críticos literarios, como el profesor Álvaro Salvador, han señalado certeramente que el acercamiento lorquiano al flamenco «no obedece, únicamente, a una intuición genial del poeta de Fuente Vaqueros, sino que es más bien el resultado de toda una preocupación intelectual “neopopularista”... arraigada en los principios estéticos del romanticismo». Personalmente consideramos que esa opinión se ve avalada por la actividad de Lorca antes y después del Concurso del 22. La citada inquiet\n\n[ENDING CONTEXT]\n\nde Granada, Cátedra «Manuel de Falla». Granada, 1962.\n\nGRANDE, FÉLIX: Memoria del flamenco. Editorial Espasa Calpe. Madrid, 1979.\n\nArchivo Manuel de Falla: I Concurso de Cante Jondo, Edición conmemorativa, 1922-1992. Granada, 1992.\n\nVarios: A Morente. Peña Flamenca «La Platería». Granada, 1994.\n\nBLAS VEGA, JOSÉ Y RÍOS RUIZ, MANUEL: Diccionario Enciclopédico Ilustrado del Flamenco. Editorial Cinterco. Madrid, 1990.\n\nVarios: Enciclopedia Universal Ilustrada Europeo-Americana. Editorial Espasa Calpe. Madrid, 1979.\n\nDiario «Ideal». Entrevista con Pepe Albayzin. Granada, 12 de diciembre de 1983.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "García Lorca y el flamenco, un amor correspondido",
    "periodical": "candil",
    "issue_id": "1998-05",
    "year": 1998,
    "language": "es",
    "article_type": "essay_opinion",
    "pages": "14-18",
    "page_number": 14,
    "word_count": 4064,
    "article_char_count_full": 24848,
    "article_char_count_review": 2975,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_03",
        "family": "AUTH",
        "trigger": "origen"
      }
    ]
  }
]
```
