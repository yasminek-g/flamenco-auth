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
    "article_id": "JALEO_1981_12::A11",
    "article_text_for_review": "translated by Brad Blanchard We have always thought that one of the principle characteristics of the flamenco styles of Cádiz was the compás. The cantes originating from this Andalusian area -- from the different local cantinas to the great many variations of alegrías, mirabrás, romeras and caracoles -- so bound to the rhythmic unfurling of the soleares, have always shown a masterful subjection to the compás. We do not mean to say that the fundamental measure of the cante -- and perhaps its most decisive attribute -- originated in Cádiz, but that it has been in this city that the cantes have had the highest guarantee of being adjusted to the demands of the compás. Until a few years ago, when one could still see some spontaneous flamenco fiestas in the streets, it was not difficult to find on any corner or tavern, a display of palmas and the best put-together cantes of the entire flamenco geography. In spite of those changes in life and ways of thinking that we have so often alluded to, the dominating characteristic of the compás continues serving as the essential and unmistakable support of the songs of this area. It is possible that many of the worthy forms of the old cantinas have been lost, but the compás keeps on beating in the maritime heart of Cádiz, which is the equivalent of saying that the city possesses one of the most difficult and categorical foundations of the cante. control. The broken voice of Mellizo Chico crawls out in an anguished, final impotence. He doesn't have the faculties -- so neces-sarily powerful in this case -- to express what swarms in his memory. He now rarely sings; he lives somewhat outside of the local flamenco scene, which is sparse today. What he knows he learned from his father, from his uncle, Enrique Hermosillo, from Ignacio Espeleta and from Aurelio Sellé, the last great cantaor of Cádiz. But the cantes of Mellizo Chico are now like a shadow, difficult to reproduce. Santiago Donday is a temperamental gypsy, very much belonging to that typical branch of flamenco which is based on the lack of insight into its formulations. The aforementioned fact that a cantaor at times may not be able to sing or else it is difficult for him to express himself according to his best intentions, is perfectly explicable in the terrain of flamenco communication. We already spoke of this when we referred to Antonio Calzones. It is an understandable problem of state of mind, of personal inspiration or of the surroundings. In flamenco there is no middle ground: Either one arrives at the limit of exaltation or else everything is reduced to a hollow and tedious hero-worship. At the beginning, Santiago didn't seem to identify with the cante. But at the end of the fiesta, he sang with all of his popular lightning-like wisdom -- unorthodox, if you like, although no less brilliant because of it -- in a trance of emotional delivery. Donday doesn't yield to previous stylistic norms; he is situated in that terrain of improvisation -- of legitimate causes, but perilous effects -- that has been able to give, in the best cases, some really valid examples, like certain creations by the anarchic genius of Manolo Caracol. Santiago Donday is a blacksmith by profession. He works in a forge in Puerta Tierra, next to the cemetery and near the rocky beach. It is a characteristic setting of the cante primitivo, which some call \"fraguero\" (of the forges) because of this presumed origin in labor. The old tonás of the blacksmiths were born on those forges in Triana and Jerez, Alcalá and Utrera, Cádiz and los Puertos. The most logical thing is that the variant of the toná called martinete was gradually incorporated into the blacksmith trade -- so frequently that of gypsies -- and that it didn't come about as a consequence of the work itself. For such a hard occupation, no cante could fit in better than the toná, so intense, solemn, traditionally sung without guitar, and where all of the terrible naked social experience of the gypsies fit in.",
    "title": "ARCHIVO: PART XII - CADIZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "29-30",
    "page_number": 29,
    "word_count": 689,
    "article_char_count_full": 4003,
    "article_char_count_review": 4003,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_12::A12",
    "article_text_for_review": "photos by Yvetta Williams ABOVE: BENITO IS PRESENTED WITH CAKE BY JESUS SORIANO & MIRCHYA MONMARTTER; LITTLE DOMINIC MADRID MAKES HIS DEBUT DANCING AT THE JUERGA. BELOW: GUITARIST YVETTA WILLIAMS, SINGING DUO BENITO & JESUS, MARLENA DANCING\" AFICIONADO NIGHT by Juana De Alva At last we can say that one of our brainstorms was a total success! On October 17th, along with celebrating Benito Garrido's birthday, we inaugurated our plan to dedicate the early hours of the juergas to the aficionados. It took so little effort and worked so well that it's a pity it wasn't tried long ago. (text continues on page 33) ABOVE: PACO SEVILLA ACCOMPANIES SINGERS SUSANA & REMEDIOS; LEFT: SINGERS REMEDIOS & RAFAEL DANCING BELOW: SUSANA (DANCING), MARIA \"LA CAMARONA\", PACO & REMEDIOS",
    "title": "SAN DIEGO SCENE -OCTOBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "31-32",
    "page_number": 31,
    "word_count": 127,
    "article_char_count_full": 773,
    "article_char_count_review": 773,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_12::A13",
    "article_text_for_review": "December finds us back at the Ocean Playhous in El Cajon taking advantage of Cathy Johnson's standing invitation to Jaleistas. The juerga will be early in the month so as not to conflict with anyone's Christmas plans. DATE: Dec 12 TIME: 7:00pm -? PLACE: 691 E1 Cajon Blvd PHONE: 442-8542 Directions; 8 east to first exit for El Cajon City (El Cajon Blvd). The Playhouse is your right after the second signal light. Remember to come and utilize the early hours to learn, to practice, to share. (ARCHIVO -- ) Mojiganga, have not been removed -- or at least only in part -- from certain traditional orbits of the cante: the blacksmiths shops, the butcher shops, street-vending of articles and other professions related to the initial attempts at becoming sedentary by Andalusian gypsies. Logically, they must have been the most integral depositories of genuine flamenco inheritance. But it is clear that things don't happen that way. What has been lost on one hand has perhaps been enriched on the other. We refer to the liquidation of the social ties of the cante and the evolving abundance of its new creative riches. *** THE \"ARCHIVO\" IS ONCE AGAIN AVAILABLE! Publisher's Central Bureau is again featuring \"The History of Cante Flamenco: An Archive.\" Every time we mention this in Jaleo, the five record set disappears from the following month's catalogue; that means that the flamenco aficionados are jumping at the opportunity and buying up the supply. The Bureau must be astounded at the popularity of that item because they keep restocking it. This is the $ \\underline{\\text{only}} $ flamenco anthology that is still commercially available and when it goes off the market it will mean the end of a whole flamenco era for the aficionado who does not own any of the old anthologies. To order, send $11.99 plus $2.25 handling (and appropriate tax if you live in N.Y. or N.J.) to: Publisher's Central Bureau Department 124 1 Champion Ave. Avenel, NJ 07131 Ask for the record set, \"History of Cante Flamenco,\" item # S43601.",
    "title": "-DECEMBER JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_12",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "33",
    "page_number": 33,
    "word_count": 343,
    "article_char_count_full": 2023,
    "article_char_count_review": 2023,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_01::A1",
    "article_text_for_review": "POR: Gabriel Ruiz de Galarreta English Translation p35 Vicente Escudero fué un gran visionario del baile flamenco, en aquella época, refiriéndonos a la 3a. Década del siglo actual. No hay duda alguna de que, con su enorme genio y originalidad -- en cuanto al baile español se refiere -- aportó ideas e imágenes nuevas, desconocidas hasta ese entonces por todos los de tal ambiente. Gracias a él, desde su tiempo se han interpretado bailes que ningun bailaor anterior a Vicente pensó en hacer! Cuando llegó Escudero al baile, los bailadores (no bailarines) interpretaban farruca, alegrías, bulerías, tanguillos, la giga (baile inglés), garrotín, zambras, rumba catalana, y pare de contar! Escudero ideó y aportó -- para hombres -- lo que podríamos llamar \"Bailes Mayores\", tales como la cana, el polo, la romera, soleares, siguiriyas, martinetes, la debla, alborea, tarantos (corregidos y aumentados éstos, más tarde, para bien del arte flamenco, por \"la única\", Carmen Amaya), etc. Tambien aportó Vicente ideas nuevas en el vestir del bailarín y bailaor; aún recuerdo como vestía para la \"Farruca del Molinero\", de Falla: Una pierna enfundada en la media, la otra al aire, un brazo con la camisa remangada, y el otro al aire también. Además, con frecuencia en el escenario, solía redoblar sus unas fuertemente, imitando el sonido de minúsculos \"palillos\" o castanuelas, mientras se retiraba a un lado del proscenio, y al hacer su \"mutis\", reicaba con sus dedos sobre la tapa del piano y desaparecía entre atronadores aplausos. Claro que esto ocurria en Francia, en París, donde él tenía su \"cuartel general\" y era ya famoso. Me honra mucho el haber sido amigo suyo y acompañarle en sus bailes con mi guitarra (no muchas actuaciones), en la capital francesa. Repito que admíre enormemente a Vicente Escudero, y me enorgullece el haber sido amigo suyo, pero únicamente en esa faceta suya, en ese aspecto....Al Vicente Escudero, como bailaor, ya es otro el pensar mío, estando seguro de que, al igual que yo, hay y hubo, muchos otros que pensaron lo mismo. Pero pasemos a \"la historia\"..... Sobre el año 30, más o menos, se presentó en Madrid Vicente Escudero, con su mujer y fiel companera Carmina, encantadora y minuscula bailarina (de igual categoría que él, en cuanto al baile se refiere). Los demás componentes del elenco, no vienen al caso. Vicente ya era un poco conocido en España debido a que un tiempo atrás, con gran boato y propaganda, se estrenó en Madrid una película titulada, si mal no recuerdo, \"El Bodegón\", filmada en Francia y, como tal y lógica consecuencia, una \"Espanolada\" auténtica. Yo era un niño por entonces, un joven-zuelo, pero asisti a la \"premiere\" con mis padres y aún recuerdo algo de ella: La estrella femenina era María Albaicín, auténtica gitana española, bella, hermosa, joven, buena bailarina, y que murió prematuramente en París luego de hacer tal film. Vicente Escudero figuraba como bailarín y semi-protagonista. Fabían de Castro como guitarrista; era español, mayor él, de porte y maneras correctas, y no mal guitarrista en esos dias. Creo que tambien tocó su guitarra en esa película Carlitos Montoya. (Si estoy equivocado, como imagino que él leera Jaleo le ruego lo diga a través de la revista; Carlos y yo somos viejos amigos, desde hace muchos años.) No recuerdo quién era el galán principal, pero me \"suena\" el nombre de \"Amalio Cuenca. ¿Te acuerdas tu, Carlitos? La Película fue calificada en España como \"bodrio\", y estuvo poco tiempo en cartelera. Sin embargo, debido al \"Bodegón\", ya el nombre de Vicente Escudero \"sonaba\" algo en España. Por entonces había un Semanario madrileno, titulado Estampa, con buenos reportages, buenas fotos, y audaz para aquel tiempo. En esta revista se hizo una gran propa- ganda al la presentación y debut de Vicente Escudero, en el Cine Avenida, de Madrid. Creo recordar que habían tres o cuatro Enorme el ambiente y la expectación; se oyen comentarios no muy agradables para Vicente Escudero. Nosotros estamos sentados cerca del escenario, sobre la quinta o sexta fila. La tensión aumentaba, según transcurreron los minutos que faltaban para alzarse el telón. Yo había observado aquella noche que Estampio portaba en una mano una bolsa, no muy grande, de papel marrón, conteniendo algo que me intrigaba. Le pregunté varias veces que \"qué\" era aquello, y él me respondía siempre, con su sonrisa maliciosa y su voz fina, \"Ya lo vas a ver, niño...Ya lo verás, Grabielito...!\" Y yo, cada momento más curiosidad. Y llegó la hora; el telón se alzó; la cortina se abrió; apareció Vicente Escudero, los suyos, y aquel \"recital\" de baile flamenco y español transcurría ante el asombro de todos los flamencos y artistas que allí está-bamos. Todo nos parecía absurdo y, desde luego, fuera de compás, \"esparrabaos\" toitos los bailes. Naturalmente, si la figura lo hacía mal, cómo lo haría el elenco.... Sacô un par de botas flamencas que guardaba en aquella bolsa de papel, misteriosa para mi hasta ese momento; las lanzó al escenario, y siquio gritando, \"Póngaselas usté! Ellas le enseñarán a baila flamenco....! Y ocurió que Escudero, quien creo que venía por un mes prorrogable de contrato, no llegó a las dos semanas con su espectáculo en el Cine Avenida, y se regresó a París, su \"cuartel general\", completamente fracasado como bailaor, hablando mal de los flamencos Madrileños, jurando tomarse venganza contra ellos algún día. Pasaron unos anitos más. Me encontraba en Oran con la Compania Teatral Alcoriza acompañando con mi guitarra a los cantaores \"Pena Hijo\", Florencio Castelló y al bailaor",
    "title": "VICENTE ESCUDERO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_01",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 918,
    "article_char_count_full": 5572,
    "article_char_count_review": 5572,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_01::A2",
    "article_text_for_review": "Dear Jaleo: A really great dancer is thrilling because he or she can combine beautiful technique with emotional maturity, passion and spiritual energy. It is a rare privilege to be able to see or work with great dancers. Many live too far away, while others tour infrequently or teach irregularly, if at all. It is sad to think of all the great dancers who will never be seen again because they have passed away or are retired. Many died anonymously, while others are represented in books by static pictures or written recollections. Only recently have dancers begun to appear in films. This is why I feel lucky to have been able to attend Teo Morca's workshop. Teo is an active and vital dancer, a great dancer. Basic technique is practiced regularly, but creative new choreographies keep the purpose of the art alive. Teo loves flamenco, and the dancers in his classes can feel this. He is contemporary, but at the same time possesses years of priceless experience. He has hundreds of wonderful stories to tell. I appreciate all of this. Teo is a great dancer and he is here now. I don't want to forget this or take it for granted. Susan Cole Studio City, CA",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_01",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 206,
    "article_char_count_full": 1160,
    "article_char_count_review": 1160,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
