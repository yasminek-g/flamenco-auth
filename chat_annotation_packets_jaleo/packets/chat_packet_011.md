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
    "article_id": "JALEO_1978_02::A14",
    "article_text_for_review": "In the late 1950s and early 1960s, there was a relatively large amount of printed flamenco music available in this country as well as in Spain. One could find single pieces or collections of solos by guitarists like Luis Maravilla, Nino Ricardo, Mario Escudero, and Jose Azpiazu, along with many others. Much of the music wasn't worth much, being either written down incorrectly or very inferior material to begin with. However, some of it was useful, especially to material-starved beginning guitarists in this country. Two things have happened to change the situation with regard to written flamenco music. First, most of the above mentioned material has disappeared and, second, flamenco has changed. Fortunately, what little music is now being published is being written down more accurately than in the past (in most cases). This may be due to the fact that it is being done more and more by non-Spaniards, who have had to really understand the structure of flamenco in order to learn it, rather than relying on intuition as does the Spaniard who learns the music by absorbing it from his surroundings (contrary to popular Spanish belief, the feeling for flamenco does not originate in the blood). The second change, that of flamenco itself, has, in one sense, made almost all written flamenco music obsolete. It is as if one were to study the be-bop music of the early 1950s in order to learn to play modern hard rock; one would end up playing in a very outdated manner. Supposing, however, that one has no alternative method of study, there are some positive aspects to learning this way. One is at least learning something that will form a basis for later learning. In both rock and flamenco, the modern forms are relatively complex and an understanding of the earlier simpler music can be helpful in learning and understanding the more complex. Also, some of the early music by people like Sabicas, Mario Escudero and Carlos Ramos is very beautiful and worth playing. I have found that most Spaniards and many American flamencos under the age of twenty-five are astounded upon hearing a rendition of a Sabicas solo -- they recognize that the style is different, perhaps less complex than that of Paco de Lucía, but it is too beautiful to be overlooked and they have never heard it before; what was once considered old-fashioned and worn out may soon be brand new again! Sabicas and Escudero, Selected Solos for Gui- tar by Sabicas and Escudero, c.$2.50, Hansen Publications, N.Y., 1962. Five solos in music only. The danza mora by Sabicas is good but his alegrias is from Flamenco Puro (see below) and totally useless; tientos, rondeña and \"Danza Cale\" by Escudero are excellent. Intermediate-advanced. Pepe Martinez, $ \\underline{\\text{Flamenco Guitar Album No. 3, As Played by Pepe Martinez,}} $ transcribed by Ivor Mairants, Belwin Mills Ltd., London. Rondeña, alegrías, and tan-guillo in music only. Okay for Intermed-advanced. Emilio Medina, Complemento del Metodo para Guitarra Flamenca, Album 1, c. $4.00, Ricordi, 1961. Rosas, danza mora, fandangos de Huelva, malagueña, jota; in music only. Okay for intermediate. Carlos Montoya, $ \\underline{\\text{Flamenco Guitar Solos by Carlos Montoya, c.2.00, Hansen Publications, N.Y., 1957. Well written examples of six Carlos Montoya solos. Music only. Good for all who enjoy his style, especially if used with discretion.}} $ Pepe Martinez, $ \\underline{\\text{Flamenco}} $ - $ \\underline{\\text{Six Pieces for Guitar by Pepe Martinez}} $, $ \\underline{\\text{transcribed by John Magarshack, Scholt & Co. Ltd., 48 Great Marlborough St., London, England. Six very short solos in music only. Poor for intermediate.}} $ Richard Rightmire, $ \\underline{\\text{Flamenco Without Tears, and More Flamenco Without Tears,}} $ William J. Smith Co., N.Y. Each volume has six solos in music and tablature. In the first book, the pieces are very simple and very short; for beginners only. $ \\underline{\\text{More Flamenco}} $ has more material in each piece; mediocre for beginner-intermediate. Jack Buckingham, $ \\underline{\\text{Flamenco Guitar - Music of the Andalusian Provinces of Spain, c.200}} $, Carl Fischer Inc., 62 Cooper Square, N.Y. 10003, 1966. Thirteen solos that are more advanced than those in his first book (see flamenco method books next month). In music only. Poor for beginner-intermediate (due mainly to his lack of feeling for what flamenco should sound like).",
    "title": "FLAMENCO MUSIC IN PRINT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "13, 14",
    "page_number": 13,
    "word_count": 709,
    "article_char_count_full": 4427,
    "article_char_count_review": 4427,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_02::A15",
    "article_text_for_review": "The March juerga will be held at the home of Isabel Tercero in Del Mar on Saturday, larch 18th. Isabel says she is inviting flamencos from as far north as San Francisco and is making paella. The address is 482 15th St. (phone: 755-9409). To get there, take Interstate 5 north to Del Mar Heights Road, about 0 miles from San Diego. Here is the food key for this month....f your last name begins with: A - E bring a main dish F - J bring a salad K - O bring a main dish P - T bring a dessert U - Z bring bread or chips & dips Please fulfill your food commitment and ring drinks (alcoholic or otherwise according to your taste); we could use more non-alcoholic drinks. See the map below for directions.",
    "title": "MARCH JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_02",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "15",
    "page_number": 15,
    "word_count": 137,
    "article_char_count_full": 699,
    "article_char_count_review": 699,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_03::A1",
    "article_text_for_review": "ILLUSTRATED We are opening with our February juerga report this month to celebrate the first appearance of juerga photos. This last juerga was a noisy and wild affair with lots of good food and drink. The home of the Trujillo-Breckes was crowded with at least seventy-five people most of the night, including some from Los Angeles and Mexicali, and a number of new faces. The following are some of the impressions of one of our Spanish members, Valentín Cabeza. The English translation follows the Spanish version. Bravo! Seguimos creciendo en gran número cada mes. La juerga, esta última, es un éxito más que apuntar, porque, el que más y el que menos sale contento y alegre de estas veladas, impaciente porque llegue la siguiente. Cada vez nos acompañan nuevos artistas, a los que aplaudimos muy de veras. Los más fiel-es Jaleistas de la guitarra y el baile flam-enco siempre nos honran con su presencia y su alegría. También estamos muy contentos de en-contrar en cada fiesta a los amigos y amigas de siempre y, sobretodo, a los \"duendes\" de la guitarra que, pese al delirio de las palmas nos deleitan con su mágico sonido Y porqué no recordar ahora a la dinámica y salerosa Juanita, que con su sangre sevillana y alma andaluza, hace palpitar hasta las mismas paredes. María Clara y Álvaro, su acompañante, nos dieron un toque flamenco muy lucido Rosala estuvo energica y sensual con sus grandes ojazos, logrando hechizarnos con su fandango. Es digno de hacer notar el entusiasmo de Lenshaw, este caballero que, a pesar de sus anos, nos emocionó con el vigor que bailó, una y otra vez, las sevillanas (mire la foto). Una vez más nos honró con su presencia y con la maravilla de su voz y de su arte, la cantaora, Mercedes Molina, a quien acompanaba su marido, Juan, tambien de Los Angeles. Rafael, con sus piropos y olés, animando a cuantos bailaban y cantaban, aunque un poco coloreados por el vino, alegró la audiencia también. Creo, habría que recordar a muchos más, que con su espontanea aportación, ayudan también a hacer posibles estos encuentros.Termino dando las gracias a los propietarios de la casa, al ser tan gentiles cediendo-nosla para celebrar tan festiva juerga y... despidiéndome de todos...hasta la próxima en el mes de marzo!!! Bravo! We continue growing in great numbers each month. The last juerga was a greater success than I can describe. All who leave these night gatherings are impatient for the arrival of the next one. Each time we are accompanied by new artists whom we sincerely applaud. The most faithful Jaleistas, guitarists, singers and dancers, always honor us with their presence and gaity. Also we are happy to find at each fiesta our old friends, and above all, the \"Duendes de la guitarra\" that in spite of the delirious sound of the palmas delight us with their magic sound. And why not recall here the dynamic and witty Juanita, who with her Sevillan blood and Andalusian soul, makes even the walls tremble! Also, Maria Clara and her companion, Alvaro, who gave us a touch of lucid flamenco. Rosala was energetic and sensual with her big eyes bewitching us in her fandangos. It is worth noting the enthusiasm of Ernest Lenshaw this gentleman who inspite of his age, makes us emotional with his vigor in dancing again and again the sevillanas. Again we were honored by the presence and the marvel of the voice of the cantaora, Mercedes Molina, accompanied by her husband, Juan (both down from Los Angeles). Rafael, with his piropos and oles, animated whoever danced and sang. I also have to recall many more who, with their spontaneous participation, helped to make possible these moments, and I would like to finish by thanking the proprietors of the Rafael Santillana and Paco Sevilla house for being so gracious as to allow us the use of their hom. Taking my leave of you all now, I will see you at the March juerga. Valentin Cabeza Joe Kinney, Yuris Zeltins and Juan Molina",
    "title": "FEBRUARY JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_03",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "1, 2",
    "page_number": 1,
    "word_count": 676,
    "article_char_count_full": 3918,
    "article_char_count_review": 3918,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_03::A2",
    "article_text_for_review": "Dear $ \\underline{\\text{Jaleo}} $: Enclosed is a check for a family subscription, the first five issues and a contribution for THE CAUSE!!! We will do our best to keep you informed of what is happening in N.Y.C., flamenco-wise, and will try to help to get more subscribers...We think $ \\underline{\\text{JALEO}} $ is a great idea! Fondly, La Vikinga and Roberto Roberto Reyes New York, N.Y. Good to hear from you and that things are cooking in San Diego. I think your idea is a good one, although knowing how paranoid flamencos are, I don't know if it will work eventually. But I can surely appreciate the sincerity and effort of the newsletter, and will be happy to contribute whatever I can in way of writing, etc... Hasta, Chuck Keyser The Academy of Flamenco Guitar, Santa Barbara, Ca. ... I think it's great and necessary to keep the momentum going for all concerned. Greg Wolfe Minneapolis ... Marvelous idea! Will enjoy hearing about your activities. I enclose a member-ship and extra shot for a juerga or whatever. I want everyone to have a drink on me and to do a fast buleria- He! He! Shame I have to be lost in the swamps here with all that going on. Sounds glorious! Warmest regards, Martin Tressel New Orleans ... we will forward your copy of JALEO to Carol Whitney... Meanwhile, of course, I could not resist reading it myself and enjoyed it very much. Glad to see that back issues are still available, and am enclosing my check to bring me up to date and to include a subscription. Martha Nelson Guitar Review New York LETTERS TO THE EDITOR ARE ALWAYS WELCOME.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_03",
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
  },
  {
    "article_id": "JALEO_1978_03::A3",
    "article_text_for_review": "JUERGAS... \"IT'S NEVER TIME TO LEAVE\" As the day of the juerga approaches, I find myself getting more and more impatient. The reason why something that a year ago wasn't even in my imagination has become so important in my life, is one of those mysteries that I don't fully understand. Maybe they are feeding us something in the food, or perhaps flamenco itself is habit-forming. But whatever the reason, the truth is that I enjoy myself so much that I look forward to the juergas as I do to my vacation: eager, impatient, and restless. Juergas can be approached from many different points of view; they are for the aficionado, for the performer, and for the spectator. They have something for everybody. To begin with, and above all, I enjoy the people. There is such a variety of personalities, such a diversity of backgrounds, that it is almost impossible not to find something interesting in everyone of them. People from all over the world, with all the possible accents and pronunciations, all of them with the common interest of flamenco. I enjoy the dancing too; the \"formal\" and the \"improvisations.\" The force of flamenco is such that sooner or later persons who never danced in their lives start feeling \"ants\" creeping along their toes and up and down their spines, and logically enough, they are unable to resist the call of the guitars. I find this terribly amusing, and some of the best performances that I have witnessed have started like that. There must be something in flamenco that touches places inside of us that we didn't know existed. And I enjoy the music; the guitars that play, sing, weep and laugh; the flamenco guitars, so serious and so light, so soft and so vibrant, so powerful. When the guitar plays, the notes fill the air and something unique is created; a statue of sound that dissipates in the atmosphere. PALMAS I would just like to say that last month's article on palmas was one of the most interesting articles I have read yet. It was very informative and beneficial to me and I hope that more articles will be written on different techniques in the dance, guitar, and cante. One suggestion to subscribers to $ \\underline{\\text{JALEO}} $ who attend the juergas; if you are interested in joining in during the dancing, there are guit-arists, dancers, and singers who can teach you the basic palmas for whatever rhythm is going on at the time, so don't be afraid to ask. Thank you very much again Rayna and $ \\underline{\\text{JALEO}} $ for such a nice article. Rosala",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_03",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "4, 5",
    "page_number": 4,
    "word_count": 437,
    "article_char_count_full": 2506,
    "article_char_count_review": 2506,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
