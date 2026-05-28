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
    "article_id": "JALEO_1980_03::A17",
    "article_text_for_review": "by Isa Mura The Patri Nader Spanish Dance Co. will be performing \"Bailes de España\" at the Steinbeck Forum, Monterey Conference Center, in Monterey on April 26. To illustrate the breadth of the program, the first half will consist of El Baile de Luis Alonso performed by four girls, La Vida Breve by Cruz Luna and Patri, Jota España by the company, La Cana by Patri, Zapateado by Cruz Luna, Alegrías by the Company and Tangos with cante and baile by Isa Mura accompanied by Juan Moro. The major work in this half is a special ballet, \"La Despedida\" (The Farewell), based of the rhythms of the siguiriya and martinete. It is the story of a condemned man who relives past moments with his novia. The story is poignant, the work is extremely moving and has been received with great enthusiasm by the public. The cante of Isa Mura and the vibrant strains of Juan Moro's toque set the stage for a truly emotional experience in flamenco song and dance. The work was conceived by Patri; Cruz Luna collaborated in the choreography. The second half of the program opens with Ravel's Bolero. Dedicated to the memory of La Quica, the work is a ballet which gives a sweeping panoramic look at Spanish Dance from the delicate opening to the passionate finale. It is a constantly changing spectrum of the dance, including a segment of beautiful cape-work by Cruz Luna, a charming moment of Cara-coles by Patri and somber moments of Soleares by the company. The cuadro which follows, brings the company together for a grand time with fandanguillos, sevillanas, mirabras, tientos, soleares, guajiras and bulerías. Company members are: Angeli Jimenez, Margarita Favel, Carolina Flores, Delys Loxas, Diego Sequira and Rosana Vela.",
    "title": "\"BAILES DE ESPAÑA\"",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 291,
    "article_char_count_full": 1712,
    "article_char_count_review": 1712,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_03::A18",
    "article_text_for_review": "San Diegans have been very slow in responding to the idea of bringing Anzonini to San Diego. Most ticket reservations have come from Los Angles and people who are not members of Jaleistas. If you are interested, send a note (don't send money) with the number of tickets you would like ($12 single; $10 each for two or more) to Paco Sevilla, care of JALEO. The maximum number of tickets to be sold is 120. The performance, which will be held on a Wednesday evening in the Andalucia restaurant, will either be sold out soon or cancelled if there is not sufficient interest. ? LATE ANNOUNCEMENTS \"ANDA JALEO\" flamenco group will perform April 11th 8:30 pm at La Peña on Shattuck Ave. Berkeley. Dancers are Patri Thomas, Anita, Paci and Joanna; guitarists: Agustin Quintero and David with Cantaora-bilaora Isa Mura. THEATER FLAMENCO'S major works to be presented at the Victoria Theater in San Francisco (see announcements) will be Missa Flamenca, Triana, Leyenda, Siguiriya and Caña. El Cuadro will include Garrotin, Alegrías, Farruca, Tangos and Bulerías. PAST EVENT: On March 4 Julio Clearfield and Pamela Kingsbury performed for \"Spanish Alliance of Philadelphia\" at the restaurant Don Quixote. (All flamenco program.)",
    "title": "ANZONINI IN SAN DIEGO?",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 199,
    "article_char_count_full": 1218,
    "article_char_count_review": 1218,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_03::A19",
    "article_text_for_review": "Back to the National U.! Cuadro B, Staphanie Levin's cuadro, will be in charge. In case you don't know who you are, these are the members of Cuadro B: Stephenie Levin, Raquel Reyes, Deanna Davis, Armando Dubon, Earl Kenvin, Gunnar & Carolina Mouritzen, Jess & Mariana Nieto, Victor Gill, Mary Ferguson, Bill Stuart, Mary Palmer, Don Edson, Walter & Maria Teresa Kranzler, Simon Andrews, Yuris Zeltins and Vickie Drietch. If Stephanie has not contacted you, give her a call and see how you can help at 296-6490. This will be a dinner juerga so cook up your best Spanish recipes. DATE: March 22 PLACE: National University Alumni Cottage, 4141 Camino del Rio South TIME: 7:00 pm (Junta meeting 6:00pm) BRING: food corresponding to the first letter of your last name. A-E Main Dish F-L Dessert or chips and dip M-SE Main Dish SF-Z Salad and bread GUESTS: There will be a guest limitation of 20 non-members and a limitation of two guests for any member. To make reservations for your guests call Deanna Davis at 277-6141. If you have any problems - if the booze runs out at the juerga, if there are no pictures or juerga report in JALEO - refer your complaints or suggestions to CUADRO B.! CUADRO C, Brand and Paca Blanchard's cuadro will be in charge of putting on the April juerga. The National U. cottage will again be available on the third Saturday - April 19- if they wish to use it. Members of CUADRO C are: Brad & Paca Blanchard, Gisella Duarte, Roberto Vasquez, Alfredo Larin, María Soleá, María Jackson, Gerry Day & Wick Hauser, Gene & María José Jarvis, Julia Romero, Alvaro & María Clara Lizano, Nora Sheker, Paco Sevilla, Herbert Goullabain, Ricardo Rico, Jesus Benayas and Juana De Alva & Jack Jackson who are ex officio members of all cuadros. It is the cuadro's responsibility to: set up, clean up, tend door & bar, take pictures, write a juerga report, act as hosts, take charge of food, have contact person for guest reservations and organize any planned activities. To find out in what area your assistance is needed call Brad or Paca at 281-4887.",
    "title": "JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_03",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "26",
    "page_number": 26,
    "word_count": 362,
    "article_char_count_full": 2061,
    "article_char_count_review": 2061,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_04::A1",
    "article_text_for_review": "by Carlena Gerheim My first glimpse of Marina was a shocker. I happened in on the end of a rumba, barely able to see her from the wings. My husband and I were finishing our last set on the Colony tablao and I had hurried down to guide the newcomers to our stage. All I knew at the moment was the surge of my own blood and the sudden chill to my skin that identified this performance as something special. That Marina and José Luis, her husband and guitarist, are Spaniards and authentic representatives of their country's dance was only partially to blame. During fifteen years of involvement in flamenco, I've seen the baile performed by gitanos, Españoles and those of foreign extraction like myself, who so strongly feel the call to Spain that it seems we were truly Andaluces in another time and the present bears witness to an Iberian past. All this obsession for flamenco has made me discriminating against imitation of any sort, be it inadequate technique and a lack of \"aire\"; or at the other extreme, excessive technique accompanied by cold delivery - again, no \"sentido\". So, here was Marina Torres, following her rumba with an alegrias that drove under any prejudices I might have had for overkill, because she danced with footwork unbelievably intricate and at once filled with that elusive element, that creative energy that makes sound speak of other dimensions. Her alegrías was full of gracia. The sounds of her feet were far more than a drone. There was color, accent, and an earthy fire that burned low, then raged, then flickered, teasing, threatening and appeasing every whim. As pleasantly devastated as I was by Marina's taconeo, I was in for another surprise the following night. She nearly replaced her usual concentration of footwork with that of palillos. With the exception of llamadas to indicate changes in the dance and a finale of taconeo interspersed with carretillas, there was little footwork. Instead she explored the alegrías rhythm with speed and agility that I have rarely seen. Marina captures our attention with her hands, not as disastrously cupped distortions of the braceo and therefore the beauty of the dance, but working nimbly, she spins a web that entwines and enraptures us. I would like to mention that the degree of her technique in no way seems to interfere with spontaneity. Every time I've seen her she reflects a different mood, her alegrías always open to change. With aficionados in the audience, be it only one table full, she lapses into every improvisation. Their last night on our tablao, José was calling to her to hurry so he could pack \"las maletas\". She was so involved, so lost in her dance and the moment! Marina seems to have a blend to her dance that is old and new. The technique is so developed, I immediately thought of the \"new\" flamenco, but as I thought back to Carmen Amaya movies, there was nothing new. In fact, her baile seemed old, so earthy, bringing to mind that ancient figurine of the Minoan snake goddess. When I questioned Marina about the gypsy element of flamenco she said \"Of course they grow up with the ritmo and can feel it naturally, but the great bailaoras have expanded themselves to study with professionals...Los gitanos lo For the past three years she has lived in Miami, Florida, working in the tablaos at the Hotel Doral and the Hotel Carrillón. Nearly a year ago she married José Luis López, now her guitarist. Since October of last year, they have had an open-ended engagement at the Colony in Cleveland. But due to an emergency in José's family, they have returned to Miami. During their brief stay in Cleveland, Marina and José have permeated the fabric of our flamenco community. Marina has performed magic on the tablao. She has taught classes in flamenco with emphasis on a base of footwork, braceo and choreography for fandango de Huelva. Her classes have not been cut and dry technique; throughout, she has worked to give us the source of improvisation; at times having us do palmas in soleares tempo for beginners, alegrías and bulerías for the more enterprising. And with the rhythm, each one of us was to create a dance complete with entrada, desplantes, escobilla and finale. Along with the ritmo ever present in our minds, Marina hummed the paseíllos in the fandango and sang the coplas. She emphasized the role of the cante, showing its relation to the dance, where and how it fit the compás. She does not claim to be a cantaora, but she sang a rumba in performance that had all the charm and magnetism of her dance. There is a rajo quality to her voice. Then it will clear and she will belt out a high note at the peak of the letra that sends the chills once again. And I find myself recalling the first moments I saw her with those of her last performance. In between, her time with us was intense, her talent multi-faceted, her personality rich and generous. To the critics of today's flamenco and its supposed degeneration into empty shells of sound, bombardments of footwork, I recommend Marina Torres. The origin of her baile goes far deeper than a pretty face. Marina is a force to be reckoned with -- and loved. (see related article page 17)",
    "title": "MARINA TORRES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_04",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "3-4",
    "page_number": 3,
    "word_count": 901,
    "article_char_count_full": 5164,
    "article_char_count_review": 5164,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_04::A2",
    "article_text_for_review": "by Paco Sevilla This issue of $ \\underline{\\text{Jaleo}} $ is a thin one, intended to get us caught up and on schedule again. Each December we seem to fall behind and then, this February we hit a low point in our morale. We were just about out of money, subscriptions were dropping off instead of increasing, our staff was reduced to a barely workable minimum and some of us, particularly me, were discouraged by the general apathy of the flamenco world and the frustration of trying to make $ \\underline{\\text{Jaleo}} $ work. Three years is a long time to pound one's head against a wall--it would seem to indicate a lack of rationality. Then, a number of things happened to give us new life and renew our efforts. Stan Schutze returned from almost two years in the middle and far East and decided to take on the challenge of putting Jaleo on its feet. He is the one who established much of our structure in the beginning and is now tackling some of the business aspects that neither Juana nor I had the time nor ability to handle. Martha Sid-Ahmed in Atlanta, Georgia, put together a list of potential library-subscribers and started us in a direction we have talked about but never got around to. A group in New York (see the following letter) involved Jaleo in the importation of Spanish flamenco records and evolved a scheme that will not only bring you the latest records, but will promote Jaleo. We discovered that we have been losing many subscribers, not because they lost interest in the magazine, but because our renewal system was not working properly; we now have a new system and hope that it will result in the return of many old subscribers. It gave us a big boost to hear from the Cátedra de Flamencología de Jerez de la Frontera and to learn that our magazine will now become part of their flamenco museum. We regret to say that we are pretty sure that we are now the $ \\underline{\\text{only}} $ flamenco magazine being published (there are a few newsletters); as far as we know, the Spanish $ \\underline{\\text{Flamenco}} $ has not been continued (no wonder -- their subscription rate didn't even cover mailing), and we haven't heard from the German $ \\underline{\\text{Flamenco}} $ for a long time. These and many other small events have combined to ensure that we will have $ \\underline{\\text{Jaleo}} $ around for awhile yet. If any readers would like to put some energy into the magazine, there are some things they can do. We can always use new subscribers, of course, so we welcome names and addresses of people we can send a complementary copy of $ \\underline{\\text{Jaleo}} $. We also need articles -- original or from magazines and newspapers. There has been no response to the idea of an issue dedicated to Diego del Gastor and others of his era in Morón de la Frontera; if we don't receive any input in the next month, we will give up the idea and publish the few things we have. Where we really could use help is in the area of advertising. If you have ever considered running an ad in $ \\underline{\\text{Jaleo}} $, now would be an excellent time to do it. Or if you know of potential advertisers, you could send us the names and addresses.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_04",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 565,
    "article_char_count_full": 3167,
    "article_char_count_review": 3167,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
