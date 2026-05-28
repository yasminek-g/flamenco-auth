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
    "article_id": "JALEO_1978_01::A6",
    "article_text_for_review": "Louis Ernest Lenshaw Ernest Lenshaw, a legend in San Diego and our most enthusiastic aficionado, is a tall outstandingly featured man who radiates self-confidence with his erect posture and beret perched jauntily on his head. He speaks with a Danish accent, paints, plays flamenco guitar, dances, is famous for the castanets he makes, and attends as many flamenco events as possible -- amazing accomplishments, but even more so, considering that he has just turned the striking age of eighty-five. Ernest was born in a fishing village on the west coast of Denmark in the year 1892. His family had always danced, and he learned folk-dancing at an early age. This style of dance has continued to interest him and he is at present an active member of the San Diego Folkdance Club. In his early years, Ernest worked as a decorative painter, played the violin in cafes and movie houses and traveled around Europe. When he was twenty-one, he came to the United States and settled in the San Francisco area where he began his life-long career as a painter, decorating furniture and painting murals on the walls of such places as restaurants, bars, and hospitals. In 1924 he married Hilma (they are now celebrating their fifty-third anniversary) and eventually they had two children, Vilma and Normand. They also have six grandchildren and one great-grandchild who was born in Cadiz, Spain. In the late 1940's, Ernest took his daughter to the studio of Elisa Cansino and there he would sketch the dancers as they took their lessons. In 1950, he met Guillermo del Oro, student of Realito and teacher of the Spanish dance in the classical tradition. They became friends and Ernest often went to Guillermos studio to watch the classes. During this time, Ernest's son was taking a course in Spanish guitar when he had to leave town and was unable to complete the course. Ernest took over the classes and, thus, began his guitar playing career. When the course was completed, he went on to other teachers and eventually learned to play for the Spanish dance under the instruction of Guillermo del Oro, who used to accompany his own classes (although at that time most Spanish dances performed in concert were accompanied by piano or orchestra). Once he had mastered the sevillanas, alegrías, and tanguillo, Ernest played regularly for the dance classes which had for students such people as Isabel Morca (not her name at that time) and Carmen Ruiz, who eventually married Mariano Córdoba, the Spanish guitarist who introduced the flamenco guitar to the San Francisco area. Ernest was given a pair of castanets which he copied to make a Christmas gift for Guillermo del After his retirement, Ernesto went to visit Denmark, and, while in Europe spent some time in Spain. He stayed primarily in the Málaga-Torremolinos area, but during a visit to Madrid, he had the opportunity to meet the famous dance teacher, La Quica. After his visit to Spain, he became more serious about his dancing, practicing regularly with different partners and performing non-professionally. Oro. Guillermo was astounded and thrilled by them and Ernest was off on another career, making and selling castanets, which he continues to do. The Spanish dance had always been of great interest to Ernest -- he had seen most of the great companies, including those of La Argentinita, Carmen Amaya, and Antonio -- and so, at almost sixty years of age, he began to take dance lessons with Guillermo del Oro and at the Cansino studio with Margarita Torres and Jose and Lolita de Ramon. He also studied with Gabriel and Lita Cansino and later with Guadelia Arroyo in Guadalajara, Mexico. During the early 1960s, Ernest was active in the San Francisco Spanish dance scene, spending time at the guitar building shop of Warren White, where he sold his castanets and came to know many of the flamencos of the time. He held open house at his home on Mondays and served lunch to many of San Francisco's flamencos, including the world famous Ciro, guitarists Paco Juanas and Mariano Cordoba, and guitar builder, Tony Murray. In 1968, the Lenshaws moved to San Diego, and with the help of the newly issued flamenco directory, Ernest was soon acquainted with most of San Diego's flamencos. His home became the site of many juergas, some especially good ones occurring on his birthdays, and he kept active promoting flamenco any way he could. THE SAN FRANCISCO SCENE Luisa Triana is leaving Los Angeles and the San Francisco Chronicle-Examiner of Nov. 20 reports that some of her company have gone to San Francisco to work with Cruz Luna. The \"Ole Ole! Spanish Dance Company\" gave its debut concert in San Francisco and will travel to other major cities, including Los Angeles and Phoenix (dates unknown). Former members of the Triana company who are now working with Cruz Luna are dancers Reyna Alcalá, Angelita Macías, and Ester Moreno, plus singer Chinin de Triana. Also with them is singer Isa Mura.",
    "title": "LA LUZ",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "6, 6",
    "page_number": 6,
    "word_count": 835,
    "article_char_count_full": 4944,
    "article_char_count_review": 4944,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_01::A7",
    "article_text_for_review": "Luisa Triana is leaving Los Angeles and the San Francisco Chronicle-Examiner of Nov. 20 reports that some of her company have gone to San Francisco to work with Cruz Luna. The \"Ole Ole! Spanish Dance Company\" gave its debut concert in San Francisco and will travel to other major cities, including Los Angeles and Phoenix (dates unknown). Former members of the Triana company who are now working with Cruz Luna are dancers Reyna Alcalá, Angelita Macías, and Ester Moreno, plus singer Chinin de Triana. Also with them is singer Isa Mura.",
    "title": "THE SAN FRANCISCO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "6",
    "page_number": 6,
    "word_count": 91,
    "article_char_count_full": 536,
    "article_char_count_review": 536,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_01::A8",
    "article_text_for_review": "PAELLAS AROUND AT NEW YEAR'S EVE JUERGA New Year's Eve was the \"Night of the Spanish Rice.\" There was shrimp-rice with almonds, rice with sausage, chicken-rice, traditional Paella with shellfish, and others. Mexican delicacies also abounded. Of special note was the spicy Chicken Veracruzana brought by new member Hank Mandler, a whole bag of homemade tamales compliments of Lucia Flores, and a steaming pot of chile. Not to be overlooked was a twenty pound turkey seasoned \"a la Stan Schutze\" and homemade bread provided by Kathy and Roger Knight-- all in all a beautiful spread. For the first time since the Alumni Cottage has been a juerga site, there was action in all three rooms most of the evening. Fewer tables and more dance boards made room in the dining area for several couples to dance sevillanas simultaneously. There was a great deal of \"room hopping,\" with a change of atmosphere in each room. A \"guitar room\" evolved into a \"practice room\" and finally into a mini-juerga. Then activity died down only to spring up elsewhere. The evening was punctuated by these lulls and bursts of activity. A constant element throughout the evening was guitarist Yuris Zeltins who is seldom seen at a juerga without guitar in hand, ready to accompany dancer, singer, other guitarists, or to strum away by himself. Rayna was able to attend for the first time in a long while and a dance highlight of the evening occurred when Rosala and Luana joined her for a bulerias rhythm interlude. We had the pleasure of seeing more of Laura Crawford's dancing as she alternated with her sister Tina Oggel in copla after copla of fandangos. She is developing a nice flamenco style with razor sharp turns. We heard by the grapevine that most of our Spanish contingent was previously committed to an annual New Year's Eve party elsewhere. We missed the special spark they lend to the juergas. The tradition of the grapes was observed to the delight of all, a pan and spoon sufficing as bell chimes. There were some complaints that the chimes came too close together and even that there were only eleven chimes instead of twelve. I protest! After fifteen years of alegrias, bulerias, and soleares, I couldn't lose a beat in a twelve count rhythm and not know it! In any case most people were able to stuff down their grapes followed by a bedlam of abrazos, salutations and firecrackers. JANUARY JUERGA The upcoming juerga will be held at the home of Mr. & Mrs. Stephen Oggel on January 28th at 1152 Santa Barbara St. in Point Loma (see map). Jose Luis Esparza will be in town so we won't accept any excuses for his not attending this one. We were low on drinks again last month; don't forget to bring food and drink. Here's the key for the month--if your last name begins with: A-E Bring a dessert F-J Bring bread, or chips and dip K-O Bring a main dish P-T Bring a salad U-Z Bring a main dish In an effort to encourage people to join Jaleistas rather than pay at each juerga and in fairness to those who bring food and drink, we are raising the non-member juerga donation to $1.00. Here's hoping that the New Year's Eve Grapes will bring you luck in the year to come and that the Flamenco Association may continue (with your support) to be a source of information and enjoyment for you all. We hope to see Juan and Mercedes Molina from Los Angeles with us at the January juerga. He is a guitarist and she is a cantaora who has studied with gypsies in the Sevilla-Jerez area.",
    "title": "JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_01",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "7, 7",
    "page_number": 7,
    "word_count": 615,
    "article_char_count_full": 3461,
    "article_char_count_review": 3461,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A1",
    "article_text_for_review": "ILLUSTRATED We are opening with our February juerga report this month to celebrate the first appearance of juerga photos. This last juerga was a noisy and wild affair with lots of good food and drink. The home of the Trujillo-Breckes was crowded with at least seventy-five people most of the night, including some from Los Angeles and Mexicali, and a number of new faces. The following are some of the impressions of one of our Spanish members, Valentín Cabeza. The English translation follows the Spanish version. Bravo! Seguimos creciendo en gran número cada mes. La juerga, esta última, es un éxito más que apuntar, porque, el que más y el que menos sale contento y alegre de estas veladas, impaciente porque llegue la siguiente. Cada vez nos acompañan nuevos artistas, a los que aplaudimos muy de veras. Los más fiel-es Jaleistas de la guitarra y el baile flam-enco siempre nos honran con su presencia y su alegría. También estamos muy contentos de en-contrar en cada fiesta a los amigos y amigas de siempre y, sobretodo, a los \"duendes\" de la guitarra que, pese al delirio de las palmas nos deleitan con su mágico sonido Y porqué no recordar ahora a la dinámica y salerosa Juanita, que con su sangre sevillana y alma andaluza, hace palpitar hasta las mismas paredes. María Clara y Álvaro, su acompañante, nos dieron un toque flamenco muy lucido Rosala estuvo energica y sensual con sus grandes ojazos, logrando hechizarnos con su fandango. Es digno de hacer notar el entusiasmo de Lenshaw, este caballero que, a pesar de sus anos, nos emocionó con el vigor que bailó, una y otra vez, las sevillanas (mire la foto). Luana Moreno and Ernest Lenshaw dancing sevillanas delirious sound of the palmas delight us with their magic sound. And why not recall here the dynamic and witty Juanita, who with her Sevillan blood and Andalusian soul, makes even the walls tremble! Also, Maria Clara and her companion, Alvaro, who gave us a touch of lucid flamenco. Rosala was energetic and sensual with her big eyes bewitching us in her fandangos. It is worth noting the enthusiasm of Ernest Lenshaw this gentleman who inspite of his age, makes us emotional with his vigor in dancing again and again the sevillanas. Again we were honored by the presence and the marvel of the voice of the cantaora, Mercedes Molina, accompanied by her husband, Juan (both down from Los Angeles). Rafael, with his piropos and oles, animated whoever danced and sang. I also have to recall many more who, with their spontaneous participation, helped to make possible these moments, and I would like to finish by thanking the proprietors of the Rafael Santillana and Paco Sevilla house for being so gracious as to allow us the use of their hom. Taking my leave of you all now, I will see you at the March juerga. Valentin Cabeza Joe Kinney, Yuris Zeltins and Juan Molina",
    "title": "FEBRUARY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2",
    "page_number": 1,
    "word_count": 490,
    "article_char_count_full": 2838,
    "article_char_count_review": 2838,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A2",
    "article_text_for_review": "Dear $ \\underline{\\text{Jaleo}} $: Enclosed is a check for a family subscription, the first five issues and a contribution for THE CAUSE!!! We will do our best to keep you informed of what is happening in N.Y.C., flamenco-wise, and will try to help to get more subscribers...We think $ \\underline{\\text{JALEO}} $ is a great idea! Fondly, La Vikinga and Roberto Roberto Reyes New York, N.Y. Good to hear from you and that things are cooking in San Diego. I think your idea is a good one, although knowing how paranoid flamencos are, I don't know if it will work eventually. But I can surely appreciate the sincerity and effort of the newsletter, and will be happy to contribute whatever I can in way of writing, etc... Hasta, Chuck Keyser The Academy of Flamenco Guitar, Santa Barbara, Ca. ... I think it's great and necessary to keep the momentum going for all concerned. Greg Wolfe Minneapolis ... Marvelous idea! Will enjoy hearing about your activities. I enclose a member-ship and extra shot for a juerga or whatever. I want everyone to have a drink on me and to do a fast buleria- He! He! Shame I have to be lost in the swamps here with all that going on. Sounds glorious! Warmest regards, Martin Tressel New Orleans ... we will forward your copy of JALEO to Carol Whitney... Meanwhile, of course, I could not resist reading it myself and enjoyed it very much. Glad to see that back issues are still available, and am enclosing my check to bring me up to date and to include a subscription. Martha Nelson Guitar Review New York LETTERS TO THE EDITOR ARE ALWAYS WELCOME.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "3, 4",
    "page_number": 3,
    "word_count": 280,
    "article_char_count_full": 1574,
    "article_char_count_review": 1574,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
