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
    "article_id": "JALEO_1981_05::A7",
    "article_text_for_review": "The trio section which followed was what you'd expect from a jam session including these three players. Head-solo-solo-solo-optional riff trading-head, on fusion standards like \"Spain,\" \"Short Tales From the Black Forest,\" and \"Birds of Fire.\" The occasional passages arranged for three guitars, like the introductions of \"Manha De Carnaval\" and \"Birds of Fire\" were beautiful mixtures of thick chords and lightning unisons — and much too rare, considering the extensive European tour the trio had just finished. Paco De Lucia's guitar was faint during the jams (it wasn't plugged into the mixer) but his playing stood out — not only for technical brilliance, but for the rhythmic subtlety and melodic form he maintained even in the fastest phrases. The crowd kept on screaming at every superfast scale, often drowning them out entirely. They rushed the stage, stood and stomped for three encores, and left as soon as the house lights went back on, having proved to themselves that the evening was a great success. — Chris Doering The concert built from solos to duets, to trio jams. DiMeola led off, mixing his trademark percussive scales with some bogus Bach. Then McLaughlin attempted some rock 'n roll on his classical guitar. He seemed uncomfortable on that instrument all night, since it does not respond to the string bends and the very forceful pick attack which are major parts of his style. Paco De Lucia played a sprightly major key flamenco dance, exhibiting all the technical wizardry that led Guitar Review to call him \"possibly the most advanced guitarist in any idiom.\" He is certainly — believe it or not — faster and cleaner than either McLaughlin or DiMeola. Flamenco, in which he is considered the ranking modern virtuoso, is the music of fast scales, but also of varied and flexible rhythms which adapt to fusion perfectly. The respect and admiration of his fellow guitarists was especially evident in the duets. The inevitable \"Mediterranean Sundance,\" with DiMeola, and an untitled piece with McLaughlin were models of musical cooperation and support. DiMeola and McLaughlin, on the other hand, let their duet collapse in \"anything you can do...\" ego battles and quote trading, during which it was revealed that Al doesn't know the Bach Bouree and John hasn't learned the Pink Panther Theme.",
    "title": "MCLAUGHLIN II QUICENA DE FLAMENCO Y MUSICA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "11",
    "page_number": 11,
    "word_count": 379,
    "article_char_count_full": 2314,
    "article_char_count_review": 2314,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_05::A8",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nPROGRAMA GENERAL Co aboración especial de ENRIQUE EL COJO Cantaores Chano Lobato - Romerito de Jerez - Nano de Jerez Guitarras Pedro Peña - Pedro Bacán Eelbale Merche Esmeralda Gutarra Quique Paredes - Manolo Do- minguez - Postigo Carracres Chano Lobato - Romerito de Jerez DIA DEL CANTE Y BAILE DE CADIZ Presenacor Paco Herrera SABADO 13 DIZ - JUANTO VILLAR - PANSEQUITO CHANO LOBATO DIA DEL CANTE Y BAILE DE SÉVILLA Cantaores MANUEL MAIRENA - FERNANDA Y BERNARDA DE UTRERA - CHOCOLA- TE - JOSE DE LA TOMASA - CHOILETETE Guarra Parrilla de Jerez - Tomatito En el baile Juana la del Pipa y su cuadro gitano Guitarras El Poeta - Manolo Dominguez En el Baile ANA MARIA BUENO Cantaores António Saavedra · Chano Lobato con el Estudio de Dánza de Caracolillo Orquesia de acompañamiento LUNES 8 Guitarra\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"MANOLO\"]\n\nTRERA - CHOCOLA- TE - JOSE DE LA TOMASA - CHOILETETE Guarra Parrilla de Jerez - Tomatito En el baile Juana la del Pipa y su cuadro gitano Guitarras El Poeta - Manolo Dominguez En el Baile ANA MARIA BUENO Cantaores António Saavedra · Chano Lobato con el Estudio de Dánza de Caracolillo Orquesia de acompañamiento LUNES 8 Guitarra Manolo Dominguez Presentador Miguel Acal DIA DE LOS CANTES ROCIEROS Intervienen DIA DE LA GUITARRA FLAMENCA Recitaiistas MANOLO SANLUCAR MANOLO DOMINGUEZ LOS MARISMENOS - LOS ROMEROS DE LA PUEBLA - LOS DE LA TROCHA DE LA PUEBLA - LOS DE LA TROCHA EL PALI - LOS ESTENAZAS de Villamanrique LUNES 15 - 20.15 horas RECITAL DE PIANO por ANGELES RENTERIA Y JAGINTO MATUTE DIA DE LOS CUADROS GITANOS LOS FARRUCOS - CARMELIYA MON- TOYA Y FAMILIA - CONCHA VARGAS y FAMILIA FERNANDEZ Concierto en colaboración con Juventu- des Musicales de Sevilla y bajo el patroci- nio del Banco Urquijo MIERCÕLES 10.- 19,30 y 22,30 horas DIA DEL BAILE Y DEL CANTE GITANOS MANUELA CARRASCO - ANGELITA VARGAS - EL BIENCASAO Y JOSELITO LA NEGRA Y JUAN MONTOYA - EL MONO DE JEREZ Venta anticipada localidades para toca la QUINCENA 12 a 2. Con 5 días de antola- With Los Farrucos, the high point was reached. La Farruquitas por alegrías, Pilar por bulerías, and later, that immeasurable Antonio Montoya Flores, who gives off the aroma of \"clavo y canela.\" There is no way to define him or analyze him. One feels, with him, only a sweet anguish that grips the body until it explodes in an \"Olé!\" It is a way of dancing that rises above time and space, technique and age. Compared to Antonio, to his baile, almost all of the others are insignificant. La Familia Fernández opened the second part. With them we find a conception of flamenco and gypsy music that is more elemental, simplistic, and direct; all is pure and immediate with this family, with three children who have a clear future, and should gradually eliminate the natural defects that give \"gracía\" (charm) to little children, but don't belong in pr\n\n[ENDING CONTEXT]\n\nsurprise. He did some bulerías por soleá of a very good cut and, later, some tangos and bulerías with force, well-placed, \"cuadro.\" In the evening the scenery completely changed. After some very long cantes por soleá, with the voice clearly strained by the effort, he sang por bulerías and siguiriγas. Perhaps he didn't realize it, but there were many who left the theater fed up with cante and bad cante. It could have been a night of triumph and it turned into El Mono de Jerez, acompañado por Manolo Dominguez. Manolo Dominguez Angelita Vargas La Familia Fernández Juan Montoya Fernando Terremoto\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ANDALUZA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "12-17",
    "page_number": 12,
    "word_count": 1352,
    "article_char_count_full": 7730,
    "article_char_count_review": 3637,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "MANOLO"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_05::A9",
    "article_text_for_review": "DAY OF THE GUITARRA FLAMENCA. $ \\underline{ABC} $ Dec 80', translated by Roberto Vázquez Objectivity, as far as flamenco is concerned, is in fact always difficult and, in some cases, impossible. Each artist has his own followers who, naturally, consider him to be the best in the world. And subjectivism starts here, because to follow a certain gentleman or lady, to accept a particular line of ideas, does not mean, therefore, that it is the best. Simply, we favor the one who agrees more with our personal spirituality. That doesn't deny the existence of others of greater stature and quality, but those others do not \"get to us\" in such a clear form, and with them an artistic empathy is not so easily produced. The day of the flamenco guitar has brought these considerations to the typewriter because, when two such artists were presented in a show, it was necessary to compare, and their different situations, the great difference between one and the other, made the comparison even more hateful. Manolo Domínguez brought with him to this performance his repeated performance in the Quincena as an accompanist. The song and dance hold no secrets for Domínguez and, in accompanying, he received fervent applause from a public that was able to appreciate the quality that \"El Rubio\" has. But he was now faced with a solo concert performance. It is not the same thing to construct a flamenco solo as it is to prepare only the embellishments, that is, the falsetas in the toque for accompaniment. Manolo Domínguez was nervous and he did not attain a solid flamenco presentation, but only isolated moments full of beauty. Manolo Sanlúcar is a different case. Manolo is very profoundly knowledgeable about guitar technique, an avaricious student of guitar and a professional Andalucian musician. Manolo Sanlúcar, unlike his colleague, was not tired from the Quincena. His ideas were clear and his potential was intact. Alone, and accompanied by the guitar of his brother, Isidro, and the flute of Javier Muelas, Sanlúcar gave a whole course in virtuosity, in musical quality, and perfect performance. The meager public that attended the theater enjoyed at great length the simple, bare guitar of Manuel Domínguez, and the very high concert ability of Manolo Sanlúcar. sort out information and compare the different records. This comparison is not referring to praise or criticism, but rather to pure observation. Many artists perform the same material in different ways, and it is fun to reach the stage where you can compare. It is not necessary to choose the better way or the artist who you prefer. Doesn't all aggressive behavior begin with this clinging to a simple opinion, which later is defended? The brain becomes more refined in associating one album with another in several ways. It recognizes how different artists are similar and how the same artist sounds different on different recordings. Here are some cuts on a few recordings for the collector to compare for similarities and differences. (Records listed in pairings of two's): \"Flamenco, Lucero Tena\" Hispavox HH(S) 10-339 - Side B - Quisiera Ser Perla Fina (Colombiana) \"Flamenco,\" Carmen Amaya-Sabicas - Side 2 - Colombiana Flamenca \"La Fabulosa Guitarra De Paco De Lucía Philips Stereo 58 43 139 - Cara 2 - Impetu (Mario Escudero) (bulerias) \"Mario Escudero Plays Classical Flamenco Music\" MHS 995 Musical Heritage Society - Side 1 - Impetu \"Artistry in Flamenco,\" Sabicas ABC/S 614 - Side 1 - Los Caireles (farruca) \"La Guitarra Flamenca,\" Paco Peña London Phase 4 4.095 - Side 2 - Los Caireles (Sabicas) \"Antonio Cortes, Chiquetete\" RCA/NL 35206 - Side 1 - Un Amor Inmenso (bulerías) \"Lole Y Manuel\" Movieplay 17.073/1 - Side A - Nuevo Día (bulerías) $ ^{*} $ Manuela de Cadiz 10620 Esther Avenue LOS ANGELES, CALIFORNIA 9C064 PRIVATE & GROUP LESSONS FLAMENCO DANCES CLÁSICO –(Escuela Andaluzia) PANADEROS (Escuela Bolera) LAGARTERANA (Toledo) \"Mario Escudero and His Flamenco Guitar\" Montilla FM 57 - Side 1 - Piropo A La Soleá",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 661,
    "article_char_count_full": 4001,
    "article_char_count_review": 4001,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_05::A10",
    "article_text_for_review": "by Caballero Bonald PART V - JEREZ DE LA FRONTERA translated by Brad Blanchard Jerez was -- along with Triana -- the most fertile and decisive founding nucleus of flamenco. It suffices to simply enumerate the great local cantaores, in order to comprehend the categorical transcendence of this city in the historic development of the cante. Since Tío Luis el de la Juliana -- who lived at the end of the eighteenth century and is the first name known in the history of flamenco interpreters -- many of the most renowned artists of the cante have been from Jerez: Manuel Molina, Merced la Serneta, Paco de Luz, Salvaorillo, El Loco Mateo, Diego el Marrurro, Carito Joaquín la Cherna, María la Jaca, El Chato, Manuel Torre, Antonio Frijones, El Puli, Antonio Chacón, Tío José de Paula, Juan Junquera, El Gloria, La Pompi, Juanito Mojama, Tía Anica la Piriñaca, etc. All of them represent to perfection the most fertile, creative contributions carried out in the area of the cante since the middle of the nineteenth century. Our repeated visits to this basic region of flamenco were, therefore, particularly obligatory. Read interviews with Segovia, Tomas, Romeros, Pujol, and many more. Find out about instrument builders, festivals, competitions, and master classes. Play our new music and lute tablature. Find out what is happening around the world in guitar and lute through- guitar & lute Magazine 1229 Waimanu Street Honolulu, Hawaii 96814 Send for Free Brochure. $2.00-sample copy, $10.00-4 issues survival of the old social outlines and styles of the cante. She learned from her countrymen -- from Manuel Torre, from Antonio Frijones, from Tío José de Paula -- until she became a prodigious cantaora of almost forgotten flamenco knowledge. She could have been an unsurpassed source of learning; rarely has she considered herself to be what she really is -- an ignored and prodigious example of human truth and of the dramatic expressiveness of the cante. Tía Anica la Pirinaca lives in the Jerez barrio of Santiago, close to the gardens of Tempul, in the heart of one of the most famous regions where the beginnings of the cante were forged. The man who knows most about the history of flamenco in Jerez accompanied us to her house; Juan de la Plata is director of the Cátedra de Flamencología, an enterprising type of conservatory of gypsy-Andalusian art. A worn out entrance, a noisy, flowered communal patio, a hollow setting of a poor villager, a bedroom that is small and tidy. Tía Anica la Pirinaca -- Ana Soto, the same last name as Manuel \"Torre\" -- is small and chubby, a simple, sprightly old woman with a kind, smiling face. She is dressed in black all the way to her feet. We explain to her the best we can our objectives. But she resists with a touching sense of humility and disenchantment, as if she were remembering something that she allowed to accompany her, without having done anything to deserve possession of it. Finally, she seems to give in a little and we agree to return for her in the middle of the afternoon. We wanted to walk, without a fixed course, through the barrio -- along the streets Nueva, Las Animas, La Sangre -- perhaps the most characteristic gypsy fortification in lower Andalucía. We later approached the broad barrio of San Miguel, less well-defined than Santiago from the perspective of flamenco, but where some important episodes of its development took place. On the wall of one decaying house -- specifically on Calle Alamos, 22 -- there appears a commemorative stone: \"On the fifth of December of 1878, Manuel Soto y Loreto, artistically known as Manuel \"Torre\" was born...\" Who would have been able to tell that enigmatic and brilliant holder of the most pure secrets of the cante that the city government -- who probably ignored him while he lived -- would dedicate to him this fervent remembrance. All that surrounds us has an air of miserable immobility as if abandoned by the rush of time. We could not avoid a certain imagined association between the ruinous popular decorations and the social demolition of some of the most authentic aspects of the cante. Flamenco was born in places like these, surrounded by a solid physical and spiritual climate, living precariously as part of some almost inpenetrable social mannerisms and never identifying itself with the traditions and preferences of the surrounding Andalusian neighborhood. Only when the cantaor was able to choose other, more well-to-do, ways of life could he also adapt the cante to these new public ways. But the basic germ of truth in flamenco could not -- and never will be able to -- be dispersed while there exists only one person capable of preserving it. Manuel Torre is, in this sense, an important link with the original integrity of the cante and its most uncontaminated historical development. No one knew as well as he how to gather the secrets of the flamenco legacy together with such deep intuition and illumination.",
    "title": "ARCHIVO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "20-21",
    "page_number": 20,
    "word_count": 838,
    "article_char_count_full": 4952,
    "article_char_count_review": 4952,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_05::A11",
    "article_text_for_review": "by Marie Bitting \"The Big Snow\" talk didn't keep Philadelphia aficionados from attending juerga number four. Mesón Don Quixote was filled to capacity! Arriving at 8 pm, we were warmly greeted by our hostess, Julia López. She stood by the door, all in black, her dark eyes sparkling above her red scarf. \"Buenas noches,\" she said, smiling. We returned the greeting and turned to go to our table. Carlos Rubio was walking toward us arms extended. \"Enjoyed the article about juerga number two in $ \\underline{\\text{Jaleo}} $,\" he said.",
    "title": "PHILADELPHIA JUERGA SAN DIEGO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_05",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "22",
    "page_number": 22,
    "word_count": 89,
    "article_char_count_full": 532,
    "article_char_count_review": 532,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
