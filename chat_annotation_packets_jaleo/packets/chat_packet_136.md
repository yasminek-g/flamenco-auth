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
    "article_id": "JALEO_1982_03::A11",
    "article_text_for_review": "AN INTERVIEW WITH FLAMENCO GUITARIST BENJAMIN SHEARER by Ron Spatz Anyone who has been to or seen a movie about London has probably heard Big Ben pealing out the time of day in loud tones. Rare is the Los Angeles aficionado who has not heard loud tones expertly pealing out from the guitar of Ben Shearer. Besides his activities as a professional flamenco guitarist, Ben is an instrument repair craftsman of the first order, operating his own repair business in the San Fernando Valley. The following interview took place on a typical afternoon in his busy shop, between clients and phone calls: JALEO: How long have you been playing the guitar? BEN: About 22 years. I started around the age of 19. BEN: My primary teacher was Jeronimo Villarino. I studied with him for eight years and I consider him the one most responsible for my style of playing and my attitude toward flamenco as a whole. I personally feel that Villarino was one of the greats of his day, in that he developed his own toque, his own style -- influenced by Ramón Montsya, Niño Ricardo, and Manolo de Huelva -- but definitely his own style. Now, I have not tried to totally duplicate his style, but I have certainly been influenced by it. The other gentleman that I have had the good fortune to study under is Julio de los Reyes. Julio, in my opinion, is one of the best all-around guitarists to ever hit Los Angeles -- an excellent classical guitarist and an outstanding accompanist, both of the cante and baile flamenco. He helped me to make the transition from a very traditional, old-style of playing into a more contemporary vein and to blend the two into some sort of harmony so that I am now able to play without sounding like a 7B recording from 1920. JALEO: We all know, of course, that Villarino is no longer with us. What about Julio? BEN: The last time I heard, he was in New York doing portraits. Besides being a terrific musician, he is an equally skilled artist with the brush. JALEO: There seems to be a widespread opinion that one must be a gypsy born and bred in Andalucía to perform flamenco properly. What are your thoughts about this? BEN: Well, I don't believe that. While many gypsies may take issue with this, some of the really great artists in flamenco are not gypsy. Your attitude and the way you feel about the art form is far more important than where you were born or which parents you happened to be conceived by. JALEO: Do you extend this to non-Spanish persons as well? BEN: Yes, with the exception of cantaores. It is difficult to sing flamenco properly without having been raised in Andalucía because you don't have the correct voice inflection or dialect. You could make an argument for dancers and guitarists, but I don't think the problem is as critical. Even cantaores from northern Spain don't sing flamenco as well as those from Andalucía.",
    "title": "BIG BEN TONES STRIKE OFTEN IN L.A",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 27,
    "word_count": 508,
    "article_char_count_full": 2850,
    "article_char_count_review": 2850,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_03::A12",
    "article_text_for_review": "PART III -- CONCLUSION [Editor's note: We find Gabriel Ruiz in Paris, where he just met Vicente Escudero.] by Gabriel Ruiz de Galarreta Months later, Vicente called me on the telephone, inviting me to his home. So, one afternoon I went to see him. He lived at that time in a popular neighborhood that I don't remember the name of, and he had rented the large basement floor of an old Parisian home. He lived with his wife, Carmina and three or four young men -- he told me they were his nephews, dance students, and gypsies as well (I realized immediately that this was not true). There in his \"basement\" he had his studio. He told me he would soon be doing a benefit performance in the \"Sala Pleyel\" (one of the most important salons in the world in those times) and he wanted me to play the guitar. Frankly, I knew his \"manera de bailar\" -- from what I had seen and from what I had been told by El Chileno, Relámpago and everyone -- and I told him that, although it would be a pleasure for me, I wasn't free at that time to make new artistic commitments; that was the truth, for I was then accompanying Emita Martínez (today, known as Mariemma), although I had complete freedom to play for anybody I wanted. But he insisted, assuring me that he would arrange it with Emita -- they were, \"paisanos,\" both from Valladolid and very good friends. I knew well what Emita thought of Vicente and the type of \"friendship\" they had. But I didn't say anything and asked him what dances he was going to do. He replied that he was doing only one, a tanguillo de Cádiz, but he would appear with the whole \"cuadro\" doing palmas for him. I figured that, with a dance so easy as the tanguillo, nobody could go wrong, so I said I surely would be able to play for him and that I myself would arrange it with Emita Martínez. He was delighted and said that when I was able, we would rehearse. I answered, \"But Vicente, a tanguillo needs no rehearsing! You dance, and when you want to finish, you let me know by saying, 'Ahí es,' then call with a foot stomp, I give the cierre and it's over.\" \"Sí, I know that! That's true! It is not for me, but for my 'tribu' (he always enjoyed using gypsy terms) so that everything will come out well!\" I returned a few days later. We rehearsed the \"cuadro\" and it was a disaster; his \"nephews\" knew nothing at all about rhythm and compás, and they only stomped the floor when it seemed like the right time to Vicente. Finally, with much You can imagine the jokes that were made by Encarna, Pilar, Antonio, El Chileno, and Don Ramón. The latter, who liked me a lot (he was a friend of my father and had known me since I was very young) said to me, laughing and making like he was reproving me, \"That is not done, Gabrielito! How could you do that with a great bailaor...?\" This was said while he was threatening me with his cane. Two days later, Escudero called me on the phone, telling me that, yes, he had made a mistake and then he invited me again to his house. That night I told my friends at the \"Gran Duc\" about the new invitation and Relámpago again joked, \"If you go this time, he will really kill you!\" So I got together with Vicente on several occasions in the afternoons, but not in his house; I asked him to meet me instead in the Bar de Pigalle. He asked me to teach him the compás for the siguiriyas, with palmas sordas. He would buy me a coffee \"con leche\" and two croissants, and in a corner of the Grand Duc: \"Un, dos, tres...uno y dos...un, dos, tres...uno y dos...\" And so we were for a number of days. Later, life took us our separate ways and I didn't see him again until some time later. When our civil war was over, I was in the Plaza de Toros Monumental in Madrid one afternoon -- after the bull-fight -- when I heard somebody calling my name from some distant seats above mine, \"Gabrielillo...Gabrielillo!\" I looked back, into the upper \"tendidos,\" and there was Vicente Escudero, unmistakeable, laughing and doing compás for me \"por siguiriyas.\" I waved to him cheerfully, and we made signals to agree to meet at the exit of the plaza. But there was such a tremendous crowd that afternoon that it was impossible for us to find each other. I knew that he lived in the Hotel Regina, right in the center of Madrid, since the news-papers frequently wrote about Vicente Escudero; he was beginning to be an important star in Spain for many people, ever since he left France in 1939-- due to World war II -- and returned to his hometown, Valladolid, where he did some dancing with Marienna and then, afterward, remained in Madrid with his wife. I believe that, by that time, Vicente was dancing in compas. But, in reality, I did nothing about trying to see him in those days, after our meeting in the Madrid Bullring. In the fall of that same year, I saw him again. I was coming out of the \"metro\" (subway) at the station at \"Alcalá\" and \"Peligros\" -- both, important and central streets in Madrid -- when I heard his voice, \"Gabrielillo, and Vicente Escudero opened his arms toward me. We embraced with joy. In reality I appreciated Escudero for his genius and innovations, for all that he brought to the Spanish dance and flamenco. In that sense, I admired him; I felt proud that he called me his friend and that I had been the one to teach him something about \"our compases.\" He wore, as always, his colorful hat and his \"gypsy curles\" on his forehead; I noticed his small feet, now larger and heavier -- Vicente was now \"mayor\" (older, elderly), but he looked good, \"buen tipo, fino y aflamencao.\" Vicente was accompanied by a young man, very proper and likeable, about thirteen or fourteen years old; he introduced us: \"Mira, Gabriellillo, I want to introduce you to a 'nephew' of mine who is beginning to play the guitar and already plays better than his father. His name is Mario Escudero.\"",
    "title": "VICENTE ESCUDERO: PART III",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "27-28",
    "page_number": 28,
    "word_count": 1083,
    "article_char_count_full": 5834,
    "article_char_count_review": 5834,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_03::A13",
    "article_text_for_review": "Dear Jaleistas: We are up to our brace of activity, going for three weeks to New Zealand, the 10th of Feb., plus many more concerts, workshops and school activities. I just finished a great workshop in Albuquerque which I think will be yearly. Would love to do one in San Diego, no cost to sponsor. Just find 16 to 20 students with $150.00 and a space for me to teach and I will come and give them a week full of great material. I will fly in and sponsor just has to organize the students in regard to time and payment. We can talk about it if there is any interest, as I am starting to do them regionally. Teo Horca [Juana's Note: Those interested in the San Diego or Los Angeles area contact me at 714/442-5362 or 444-3050. Teo gives a great workshop for beginning through advanced levels.]",
    "title": "LETTER TO SAN DIEGO JALEISTAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 30,
    "word_count": 149,
    "article_char_count_full": 792,
    "article_char_count_review": 792,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_03::A14",
    "article_text_for_review": "Our new juerga site in the \"Gas Lamp District\" was enthusiastically approved by those present at last month's juerga. We will have a repeat performance and hope that more members will check it out this month. This site not only meets, but exceeds, our requirements for juergas. Depending on how you count, it has between nine and twelve rooms at our disposal! Some are carpetad, some wood or linoleum over wood. It has ample storage space for our dance boards and supplies. There are two bathrooms and a kitchen with refrigerator. To allay anyone's worries about the downtown area, it is brightly lit and constantly petraled. If you are coming alone and don't find a nearby parking space, double park in front of the entrance, come upstairs and someone will accompany you to park your car.",
    "title": "MARCH JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "29",
    "page_number": 30,
    "word_count": 136,
    "article_char_count_full": 789,
    "article_char_count_review": 789,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_03::A15",
    "article_text_for_review": "The JUNTA is the organizational board which steers the course of JALEISTAS. Meetings are held on the SECONO TUESOAY of every month at JALEO HEADQUARTERS, 1628 Fern Street, at 7:00 p.m. Our next meeting will be on March 9th. EVERYONE IS WELCOME. FEBRUARY JUNTA MEETING The meeting was held on February 9th, with complete board attendance. Juerga Coordinator Vicki Dietrich suggested that we advertise for free in the San Diego Creative Directory. It will come out on April 1, 1982, and it would be a good way of advertising the local Flamenco talent available locally. JALEO: It was again explained that Jaleo is the common media of Jaleistas, and that the finances of one cannot be kept separate from the other's. This is also part of the incorporation requirements. Starting with the March issue, Jaleo's type will be shrunk 25%, this will enable the publication to remain at 32 pages, which, in turn, will reduce the price of printing and mailing. The following motion was made, seconded and approved: \"That, as of April 1, 1982, subscribing members and local active members shall be charged equally according to category, as follows: Single Membership - $17.00 Single Plus Guest - $25.00 Family Membership - $25.00 These annual memberships will include a subscription to Jaleo. Applicable first class or overseas postal charges will be added, as listed on the inside cover of Jaleo. JUERGAS: In view of the fact that offers of homes as Juerga locations are scarce, almost inexistent, the need to rent is obvious. Therefore, the following motion was made, seconded and unanimously approved: \"That a nominal donation be made per person for admission to each Juerga to defray expenses. Anyone over the age of 15 shall be charged the adult fee.\" WE WILL OFFER FREE AOMISSION TO THOSE WHO NOTIFY US BEFORE- HAND THAT THEY ARE WILLING TO HELP OURING THE JUERGA. FLAMENCO DIRECTORY: The special Flamenco Directory offer was raised $2.00 and extended to March 15th in order to reach the 200 minimum orders for a bulk mailing. JALED HEADQUARTERS: It was moved, seconded and unanimously approved that: \"An increase of $20.00 shall be paid for rental of Jaleo Headquarters for a total of $50.00 a month.\" This is necessary due to increase in costs. There being no further business, the meeting was adjourned at 10:00 p.m. cm LA JUNTA La JUNTA es el grupo que organiza y guía el curso de JALEISTAS. Se reúne el SEGUNDO MARTES de cada mes en las oficinas de JALEO, 1628 Fern Street, a las 7:00 p.m. La próxima reunión será el 9 de marzo. TODOS ESTAN INVITAMOS. REUNION OF FEBRERO La reunión se celebró el 9 de febrero, con asistencia completa de la directiva. La Coordinadora de Juergas, Vicki Dietrich, sugirió que aprovecháramos la oferta de anunciar gratis en el San Diego Creative Directory, directorio que se publicará el primero de abril de 1982. Esta sería una buena forma de anunciar el talento flamenco disponible en San Diego. JALED: Se explico nuevamente que jaleo es el medio de comunicación de Jaleístas, y que los asuntos financieros del uno no pueden mantenerse separados de los del otro. Este es, además, uno de los requisitos de incorporación como sociedad no lucrativa. Comenzando con la edición de marzo, el tipo de Jaleo será 25% más pequeño, lo cual mantendrá la revista en 32 páginas, disminuyendo de esta forma los costos de impresión y de correos. Se hizo la siguiente proposición, la cual fue secundada y aprobada unánimemente: \"Que, desde el primero de abril de 1982, a todos los miembros de subscripción y a los miembros activos locales se les cobrará igualmente de acuerdo con las categorías siguientes: Categoria de un Solo Miembro - $17.00 Miembro Solo más un Invitado - $25.00 Miembros de Familia Inmediata - $25.00 Estas categorías de socios serán renovadas anualmente e incluirán una subscripción a Jaleo. Se le agregarán los gastos correspondientes de primera clase o al extranjero, de acuerdo con la lista que aparece en el interior de la cubierta de Jaleo. JUERGAS: En vista de que las ofertas de casas para Juergas escasean, es ovio que necesitamos alquilar locales. Por lo tanto, se hizo la siguiente proposición, la cual fue aprobada por unanimidad: \"Que se pague un donativo módico por persona como admisión a cada Juerga para cubrir gastos. Todas aquellas personas mayores de 15 años pagarán la cuota de adultos.\" SERAN ADMITIÓOS GRATUITAMENTE TOOOS AQUELLOS QUE OFREZCAN DE ANTEMANO SU AYUDA DURANTE LA JUERGA. DIRECTORIO FLAMENCO: La oferta especial para el Directorio Flamenco fue aumentado $2.00 y estendida hasta el 15 de Marzo para poder llegar al mínimo necesario de 200 pedidos para mandar tercera clase. JANA CENTRAL DE JALEO: Se propuso, secundó y aprobó que \"Se aumentará en $20.00 la cantidad que se paga como alquiler de la Oficina Central de Jaleo, a un total de $50.00 por mes.\" Esto es necesario debido al aumento en el costo de vida. Al no haber más asuntos que discutir, se cerró la reunión a las 10:00 p.m. cm",
    "title": "JUNTA REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_03",
    "year": 1982,
    "language": "en",
    "article_type": "article",
    "pages": "30",
    "page_number": 31,
    "word_count": 839,
    "article_char_count_full": 4959,
    "article_char_count_review": 4959,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
