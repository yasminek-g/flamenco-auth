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
    "article_id": "JALEO_1980_04::A3",
    "article_text_for_review": "Dear Jaleo, First of all let me personally congratulate you on your progress with the newsletter. It is with this in mind that Roberto Reyes, La Vikinga and I have made New York's effort to find a way of increasing Jaleo membership. We are all too concerned with the possible decline in the interest in flamenco. We reasoned that in order to increase interest in flamenco, the public must have it made available to it in the form of recitals, lecture demonstrations and most of all, in terms of continued appreciation, RECORDS. Availability of a broad spectrum of old and current recordings is paramount in following trends and is the lifeblood of any effort to interest people in a growing art form. One of the ongoing gripes we have in New York is that we don't have current records available to us, and on the rare occasion that one is found on the shelves, it's not there long. So what does all this have to do with increasing Jaleo membership? Here's how. If we can help the importer/distributor of flamenco records increase it's sales by ordering the records we need, perhaps they can help us increase membership by letting people know we exist. With this revelation firmly in mind, I was elected to try to interest International Book & Records (IBR) of Long Island City, N.Y. in the concept. I met with it's charming General Manager and Sales Chief, Kay Jackson, who runs a most impressive operation. If you've bought any flamenco records at all, you have most likely found IBR's sticker on the record jacket, as they are the largest importer/distributor of records so vital to our interests. In the course of our conversation it was revealed that there was little way of IBR",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_04",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "5",
    "page_number": 5,
    "word_count": 295,
    "article_char_count_full": 1682,
    "article_char_count_review": 1682,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_04::A4",
    "article_text_for_review": "submitted by Paco Sevilla This a rather provocative excerpt from the book Antonio and Spanish Dancing, by Elsa Brunelleschi, (Adam and Charles Black, London 1958) who was the founder of a Spanish dance school in London, writer for the magazine Ballet and a frequent reviewer of the dance scene. She was born in Argentina, studied dance in Spain and performed as a professional Spanish dancer. In writing of Antonio's and Rosario's performing, at the time of their return to Spain in the early 1950's after fourteen years of touring, she says (pg. 34-36): The appearances of Rosario and Antonio soon after their return to Spain showed little change in the style of their dance. Flamenco, which was eventually to be the very root of their success, was yet to assert itself, and as yet occupied only a small proportion of their performances. The partners were mostly what was known as Andalusian castanet dancers, and danced more often with orchestra and pianos than to the guitar. The legacy of Argentina, their famous predeces-sor, had not yet spent itself. Granados \"Dance No. 5\" in E minor also called \"Andaluza\", which Argentina was the first to set as a dance, and which started the fashion for this type of dance composition, incorrectly termed \"classical\", usually accompanied by long passages of trilling castanets, was still one of Rosario's and Antonio's numbers. This dance belongs to a type of stylized neo-Spanish dancing which was personal to its creator, and Antonio was later to realize it and to stop dancing it himself. Altogether he had grown well above the \"Leyena del Beso\" or \"El Relicario\", those rather long, commonplace dances which everyone was dancing when, fourteen years previously, he had left Spain for South America. Popular theatre dances like \"La Boda de Luis Alonso\" or the \"Peanut Vendor\" remained for a while in their repertoire, but in order to achieve the rank of a recital artist, all traces of previous cabaret and music hall influence had to be discarded. The dances to piano pieces of Albeniz, Granados, or Turina were, in a way, still \"de riguer\". It had become almost traditional for a recital to begin with a dance of this genere. Antonio had made several arrangements for himself and Rosario, and none was more beautiful to watch than \"Triana\" of Albeniz, the very first dance to be seen at their London debut. I had been commissioned to write some explanatory notes for the programme of the Cambridge Theatre performances, and of this particular work I had this to say: \"The stylized choreography of this dance is in the noble and purest conceptions of the Andalusian tradition. It offers a visual parallel to the music which is part of the famous 'Iberia' Suite composed by Albeniz on the melodic themes of Southern Spain. The castanets are not used as mere rhythmical accompaniment, but as a fine musical enrichment, faithfully following and underlining the pattern of the music.\" On re-reading these lines, one discovers the reason why this type of Spanish dancing is outdated. It had too many of the personal characteristics of one particular dancer, and anybody else dancing in this manner would only be reproducing to a great extent Argentina's mannerisms and personality. The other reason is that it represented a Spain just a little too romantic and idealistic. The dancer using this type of music cannot bring to the surface the other, truer Spain of lust and violence. Everyman's idea of what Spain is, is not as wrong as the \"purists\", quick at crying \"Espagnolade!\" would have us believe, and in this respect the \"idealized\" Spain of these composers is much less true than the \"picturesque\" one which the latest catchy pasodoble might show us. From a pure dancing point of view, most of these tunes, whether they are called \"Almeria\", \"Cádiz\", \"El Puerto\", Malaga\", or \"El Albaicín\", offer too little variety or scope to the choreographer. The titles matter little, they are only an excuse for changing from one picturesque costume to another, the costumes, for instance, of fisher folk for pieces like \"Malaga\" or \"El Puerto\", or the costume of gypsies for \"El Albaicín\". The musical structure is nearly always the same, starting with a rhythmical entrance and followed by a melodic phrase to which is invariably added, in a minor key, a cadenza borrowed from cante jondo -- this being used as a link which returns one once more to the opening bars. The trouble is that these cadenzas are not very suited to dancing. The origin of the cadenzas were grace notes, on which a singer would embroider at will, subtle, indefinable, oriental subdivisions of notes, but once written down and \"organized\", they become weighed down by an unconvincing formality. This cry from the soul, so typical of flamenco singing, is transformed into well ordered Italianate \"fiorituri\". With this undanceable ad-libbing, the dancer generally resorts to near words with his castanets and to plain miming instead of dancing. The mime is never very enlightening either -- being mainly concerned with lamenting over an absent lover, describing a scene of jealousy, or perhaps a little flirtation with some undefined member of the audience. The dancer usually advances towards the footlights and picks her victim. The castanets, cleverly, it must be said, provide the \"faithful reproduction\" and help the dumb show, which is meant to say: \"I think I love you and I might like to come with you,\" or, quickly fickle, the dancer might wander to the opposite corner of the stage and decide she would rather have a different admirer. But it all turns out, as we notice by her look of disappointment, that he does not want her after all, whereupon, shrugging her shoulders and her castanets, she returns to her gay little dance. These so-called evocative pieces and the numberless choreographic versions, often with more than one or two participants, that have been made of them, have had their day. Nobody can develop that theme any further.",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_04",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 999,
    "article_char_count_full": 5968,
    "article_char_count_review": 5968,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_04::A5",
    "article_text_for_review": "by Paco Sevilla At the age of 23, Greco met La Argentina; it was she who changed his name to José. Soon he became her partner and worked alongside such artists as guitarist Carlos Montoya, Argentinita's sister Pilar López, and Manolo Vargas. When Argentinita died in 1945, José went to Spain for the first time PHOTO ABOVE: JOSE GRECO IN FOREGROUND; GUITARISTS (L to R), ANTHONY BRAND, MIGUEL GARCIA, MANOLO BARON; (L to R) PAQUITA BUSTAMANTE, MARIA SOTO, PEPE DE LA ISLA, VICENTE PASTOR, PACO DONIZ, EL MILIONARIO, BARRILITO, CORAL DE LOS REYES, LOS SALAOS. In addition, many artists eventually settled in the United States to become part of and enrich the flamenco world here, as for example, Chinín de Triana, Carlos Ramos, José Molina, and Manolo Barón. Also, Greco provided a training ground and a showcase for many non-Spanish artists who later went on to head their own companies or star in other groups, people like Luis Rivera, Roberto Lorca, Pasqual Olivera, Timo Lozano, and Roberto Amaral. Jose danced and acted in a number of movies, including \"Manolete\", \"Sombrero\", \"Ship of Fools\", \"Around the World in 80 Days\", \"Holiday for Lovers\", and \"The Proud and the Damned\". The Greco companies were featured on the following records: \"Spanish Dance Spectacular\" (Columbia ML6296 and MS6296), \"Spanish Songs and Dances in Motion\" (Columbia ML5665 and MS6265), \"Flamenco Fury\" (M.G.M. 3741), \"Noche de Flamenco\" (M.G.M. E3802), \"Presenting José Greco\" (RCA Victor 2300), \"José Greco Ballet\" (Decca DL9757, ED788), \"José Greco, Danzas Flamencas\" (Decca DL9758, ED786). José has been responsible for a number of educational projects. He founded the A.C.A.D.E.M.A. (Academia y Conservatorio de Arte de Marbella y Andalucía) in Marbella, Spain, where he offered courses in Spanish dance, guitar, language, literature and other arts. In the United States he offered courses in dance and guitar under the auspices of the Northwood Institute. The José Greco Foundation for the Hispanic Dance attempts to further Hispanic dance in this country. In 1974, he founded \"La Campana\" (Centro de Arte Espanol) in Marbella. In addition, since 1973, when he stopped touring with large companies, José has toured each year with lecture demonstrations, classes, and symphony appearances. In recognition of his contributions to the Spanish arts, José Greco was presented the \"Cruz de Caballero del Mérito Civil\" by the The following graph shows how the size of the Greco companies changed over the years. The exact date for each company is not cer- C. 1959; (BACK ROW L TO R) ENRIQUE HEREDIA, MIGUEL GARCIA, JOSE MOLINA, CURRO RODRIGUEZ, RAMON VELEZ; (SEATED, L TO R) MANUELA DE JEREZ, RICARDO BLASCO, MARIA MERIDA, LUPE DEL RIO, MARIA ANGELES, DOLORES DEL CARMEN; (FRONT ROW, L TO R) TERESA MONTEZ, JOSE GRECO, ROSARIO CARO. NANA LORCA The dancers are listed below in alphabetical order. The guitarists and singers are listed in the order of their appearance with Greco. Keep in mind that the dates are only approximate: BAILARINES - BAILAORES Angel Peralta (1956-57) Angel del Rey (1956-57) Angel Soler (1953-55) Antonio Candela (Alicante; 1971) Antonio del Castillo (Madrid; 1967) Antonio Jaen (Jaen; 1967) Antonio Jiménez (1953-55) Antonio Monllor (Spain; 1963) Antonio Montoya \"Farruco\" (Andaluz; 1965-66) Benito Albéniz (Spain) Curro Rodríguez (Sevilla; 1959-63) Domingo Móntez (1952 and 1956) Edo (Spain; 1970) El Milionario (Granada; 1965-66) Enrique Ruben (Argentina; 1961-63) Felix Granados (Madrid; 1962-64 and 1966) Gitanillo Heredia (Spain; 1956-65) José Antonio (Madrid; 1971) José Granero (Argentina; mid-1960's) José Heredia \"Josele\" (Madrid; 1966) José Luis Greco (U.S.A.; late 1960's) BAILARINAS - BAILAORAS Alba Merce' (1967) Amalia Jiménez (Madrid; 1970) Amparo Lozano (Madrid; 1968-70) Anita Ramos (1954-55) Antonia Granados (Jerez; 1962-64) Antonio Rojas (U.S.A.; 1969) Azucena Flores (Madrid; 1968-70) Carla Enrique (U.S.A.; 1970) Carmen Dávila (Puerto Rico; 1971) Carmen Domínguez (Spain; 1960) Carmen Mora (Madrid; 1962) Carmen Quintero (Sevilla; 1965-68) Carmen Villa (1963) Coral de los Reyes (Granada; 1964-66) Curra Jiménez (Madrid; 1962-63) Dolores del Carmen (U.S.A.; 1954-59) Elba Ocampo (1952) Elena Santana (late 1960's) Encarnación (Madrid; 1971) Estrella Flores (late 1960's) Gracia del Sacromonte (Granada; 1952) Irene Alba (Spain; 1965) Isabel Miranda (U.S.A.; 1968) Linda Rivera (U.S.A.; 1952) Lola de Ronda (Madrid; 1952-63) Luisa Fabiola (Madrid; 1962-63) Luisa Heredia \"La Chichi\" (Málaga; 1966) Lupe del Río (U.S.A.; 1954-64) Lydea Torea (U.S.A.; 1963-65) ANTONIO MONTOYA \"EL FARRUCO\" PAGE RICARDO MODREGO GUITARISTS Juan Hidalgo (Spain; 1952) Manuel Hidalgo (Spain; 1952 and 1956) Vargas Araceli (Spain; 1952) Miguel García (Sevilla; 1953-65) Ricardo Blasco (Spain; 1953-56 and 1959) Carlos Ramos (Málaga; 1956) Sami Martín (1957) Enrique Heredia (U.S.A.; 1959) Francisco Espinosa (Cádiz; 1960) Emilio de Diego (Madrid; 1962) Martín Pelta (Spain; 1963) Ricardo Modrego (Madrid; 1964, 1967 and 1971) Manolo Barón (Spain; 1964-66) Antonio Brand (U.S.A.; 1965) Paco de Lucía (Algeciras; 1966) Beltrán Espinosa (Cádiz; 1966-68) Juan Jiménez (Spain; 1967) Antonio Maravilla (Sevilla; 1968) Julio de los Reyes (Málaga, 1969) Roberto Rico (U.S.A.; 1969) Pablo Marchena (late 1960's) Agustín Reyes (Madrid; 1970) Luis Cuadra (Spain; 1970) Gino D'Auri (1971) Luis Adame (?) CANTAORES Chinín de Triana (Spain; 1952-54) Manuela de Jerez (Jerez; 1956-63) Pepe de Algeciras (Algeciras; 1964) Pepe de la Isla (Cádiz; 1965-66) Barrilito (Andaluz; 1965-66) Juan Vallejo (Cádiz; 1968-70) Manolo Fernández (Algeciras;?)",
    "title": "JOSE GRECO AND HIS COMPANIES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_04",
    "year": 1980,
    "language": "en",
    "article_type": "poem",
    "pages": "8-13",
    "page_number": 8,
    "word_count": 847,
    "article_char_count_full": 5632,
    "article_char_count_review": 5632,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_04::A6",
    "article_text_for_review": "(from: $ \\underline{\\text{The}} $ $ \\underline{\\text{Atlanta}} $ $ \\underline{\\text{Journal}} $ $ \\underline{\\text{and}} $ $ \\underline{\\text{Constitution}} $, February 17, 1980) By W.C. Burnett It was a cold, raw morning in Cedartown. An ice storm had been forecast, and even though it didn't occur, the weather was dreary. Even so, about 60 people had gathered in the small auditorium of the Georgia Power Co. office downtown to hear an \"informance\" by guitarist Ronald Radford. He already had performed and talked to audiences at a factory and several other sites during the previous few days. The week would culminate with a performance at Cedar-town Auditorium. It was CART Week in Cedartown. CART is the acronym for Community Artist Residency Training, a program of Affiliate Artists, Inc., of New York City. CART also is supported by the Southern Arts Foundation. The organization is familiar to many Atlantans, since its activities include the Exxon-Endowment conductor's post of the Atlanta Symphony Orchestra (now filled by Sung Kwak and formerly by Michael Palmer), and the Xerox Affiliate Artist with the Atlanta Symphony (now pianist Leon Bates). At the Cedartown informance, guitarist Radford looked out over the audience and said, \"I see a few repeat customers from previous occasions. This is a dialogue and",
    "title": "AUDIENCES KNOW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_04",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "14",
    "page_number": 14,
    "word_count": 206,
    "article_char_count_full": 1323,
    "article_char_count_review": 1323,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_04::A7",
    "article_text_for_review": "Radford's informance was one of nine such events he conducted in Cedartown that week. Long before his arrival, community volunteers had prepared for the week, raising funds for his fee and obtaining sites. The object was to put people into close contact with an artist, letting them hear him talk about his career and what it means to him to perform. And they could ask him questions. Radford is from Tulsa, Oklahoma, and his background and experience enable him to establish an immediate bond with his audiences in the small towns of the South. In his informal discussion at Cedartown, Radford explained how he got into the business of making music. \"My mom taught me to play the ukelele when I was 7 years old,\" he said. He then learned the piano, took up the cello, \"and then discovered the guitar. I immediately became a fan of Chet Atkins, playing the basic country music. I discovered rock'n'roll, like most people in junior high school did at that time.\" He joined a rock'n'roll band, he said. Then his mother bought a \"1.98 special record at a supermarket check-out station. Side No. 1 featured a cha-cha band, but side two was the guitar music of Carlos Montoya. \"There I was, 17 years old, never even heard about flamenco guitar. But when I put the record on, it was as though my whole life was changed in a matter of a few moments. I'm not exaggerating. I began to listen to the sounds of this music and it was as if a new door had been opened for me and there was a whole new world out there. I walked through that door and never played another note of rock'n'roll music.\" Radford played several flamenco selections for his audience and, toward the end, even succeeded in getting them to back him up with the syncopated hand-clapping which is normally part of the performances. Attempts to get them to shout \"ole'!\" and other Spanish exclamations were less successful, although a few voices were raised at the end of the session. A question-and-answer session followed the performance. Manuel Torre (from: Nueva Andalucia, Sept.21, 1978; sent by Bettyna Belen; translated by Roberto Vazquez.) by Ángel Marín Rújula ANTONIA \"LA GAMBA\" It is known by all aficionados that Manuel Soto Loreto, \"Manuel Torre\", spent most of his life in this Sevilla that was able to savor the miracle of his voice and that, later, watched him die in the worst of miseries in the small outbuilding on Amapola Street. Because of these circumstances, it is logical that all descendants of Manuel Torre have been Sevillanos and that some of his children still live in our city. From his early youth, Manuel Torre was very fond -- aside from his obvious love of the greyhounds and cockfights -- of \"jaleos de faldas\" (skirt chasing), as some people who knew him told us. People comment about many of his love affairs at that time, in his wanderings through the steps of fame. Among them, Pepita la Murciana, his first passionate love, Antonia la Gamba, with whom he lived and had two children, Tomás and Juan, and in the last years of his life, María, with whom he had five children, Tomasa, Amparo, María, Consuelo, and Gabriela. Amparo and María, both of them married now, had the good luck, although they were very small, of knowing their father, of listening to him sing sometimes, and of knowing his habits and obsessions. In order that they would tell us everything they remember about such a singular father, so enigmatic and so brilliant, we wanted to speak at length, filling voids that we were unaware of and going deeper, once more, into the life of the great Manuel Torre in the commemoration ANTONIA \"LA GAMBA\" --Did anybody worry about your fate after your father's death? \"Yes, a man who had a big heart and who organized a gigantic festival where much money was collected for our upkeep during the time we were with our uncle. That man was Pepe Marchena; aside from him, nobody remembered Manuel Torre, as much as they said they loved him.\" --It is strange that nobody took an interest in his remains. Is there any explanation? \"None! We were too young to know about that. The one who should have done it was his brother; he is the one who picked us up, although we don't know why he didn't do it.\" --Do you think that Manuel Torre has had someone to continue his cante? \"The only one to continue our father's cante was his son, Tomás Torre, although the only one who can sing his cante is Antonio Mairena and Curro Mairena who have a little of his 'eco'. The rest have only made false imitations without stature of any kind.\" --I don't know, but I believe that the gypsies -- mind you, I speak from a different point of view, that of the payo, gacho, or whatever you want to call them -- are too radical and you go from one extreme to the other with nothing in between. You tell me how thankful you are to Antonio Mairena, but then you will never forgive him. Can you justify this? \"What we won't forgive is that Tomás, in the hour of his death, asked for Antonio and he didn't appear. And then, one day in the Tertulia de Radio Sevilla, he said that he was with Tomás until the end, which was not true because Tomás called out for him and Antonio Mairena did not appear. If his sensitivity and his heart problem prevented him from attending the burial, okay, but he",
    "title": "HIS DAUGHTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_04",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 946,
    "article_char_count_full": 5269,
    "article_char_count_review": 5269,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
