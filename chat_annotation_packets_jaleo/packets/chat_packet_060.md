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
    "article_id": "JALEO_1979_12::A11",
    "article_text_for_review": "This is the first in a series of articles on flamenco artists currently in the Miami area. From a very early age, Cuban born Chucho Vidal was involved in the flamenco world. Before becoming the accomplished flamenco guitarist that he is, he was a classical guitarist. And before exclusively becoming a flamenco guitarist, he was a bailarín. In the mid 1940's, he began his professional career with a South American tour as a dancer in the Ballets de Ana María. He then went on tour with the María Antinea Company, and then with Conchita Piquer as both a bailarín and guitarrista. Chucho has exten-",
    "title": "GUITARIST CHUCHO VIDAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "article",
    "pages": "36",
    "page_number": 36,
    "word_count": 104,
    "article_char_count_full": 597,
    "article_char_count_review": 597,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_12::A12",
    "article_text_for_review": "The purpose of the Flamenco Society of Minnesota is to promote and keep healthy the art of flamenco in Minnesota and surrounding states. The ultimate aim is to form an organization which will not only carry out this function, but will reach out and make contact with other such groups around the country, such as the Flamenco Society of Detroit, and Jaleo, of San Diego. The Society will hold monthly meetings which would involve mercifully short business sessions followed by any one of a variety of subjects of interest to flamenco aficionados, such as lecture-demonstrations on various aspects of the art, mini-concerts, etc. Not only will members of the Society provide the preceding, but professionals from outside the area will also be invited to contribute. The Society will also attempt to sponsor flamenco concerts from time to time, featuring local artists, and whenever possible, artists who are in the area, on tour, and artists from other flamenco organizations around the country. Bi-monthly “Juergas” will also be a part of the Flamenco Society. A membership in the Society will entitle the holder to attend all meetings, receive a bi-monthly newsletter which will list all upcoming flamenco events, attend juergas, and all sorts of exciting things yet to be determined. A Special Membership will entitle the member to also receive a monthly copy of “Jaleo”, the informative, lengthy newsletter of the Flamenco Society of San Diego. This newsletter contains many articles and stories of interest to flamenco people and is a ‘must’.",
    "title": "FLAMENCO SOCIETY OF MINNESOTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "37",
    "page_number": 37,
    "word_count": 250,
    "article_char_count_full": 1546,
    "article_char_count_review": 1546,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1979_12::A14",
    "article_text_for_review": "ANNOUNCEMENTS Announcements are free of charge and will be placed for two months; they must be received by us by the 15th of the month previous to their appearance, earlier if possible. Send to: JALEO, P.O. BOX 4706, SAN DIEGO, CA. 92104 JALEO CORRESPONDENTS If you would like to assist Jaleo by acting as a correspondent for your city, please contact our P.O. Box number and let us know. We need to have an update at least every two months. Correspondents are listed as staff members. DANCE INSTRUCTION: Esteban de Leon (212) 724-4918 Intermediate & Advanced Spanish Dance Jerane Michel (212) 222-4973 Beginning Spanish Dance Estrella Morena (212) 489-8649 Flamenco & Classical Spanish Azucena Vega (212) 989-0584 Began a special 5 week course on flamenco in November. Victorio Korjhan (Flamenco) (212) 927-7220 231 W. 54th St. 4th floor Mariquita Flores 212-582-3350 Alicia Laura (Long Island) 516-928-3244 Michael Fisher (Ithaca) 607-257-6615 washington d.c.... Mariquita Martorell 301-992-4792 Tom Krauzburg (Crofton Md.) 301-261-0261 Raquel Peña (Virginia) 703-537-3454 flamenco, Jota and 18th century Bolero georgia DANCE INSTRUCTION: Marta Cid (Atlanta) 404-993-3062 florida EL CID RESTAURANT & LOUNGE now features dancers Ernesto Hernández, La Chiquitina, and guitarist is Chucho Vidal and cantaor is Miguel Herrero. Two shows nightly on Le Jeune Rd. one block from W. Flagler St., N.W. Miami. EL BATURRO RESTAURANT feature bailaor José Miguel Herrero, guitarrista Miguel Mesa, and cantaor Carlos Madrid; Fri. and Sat. nights at 11:00 PM; 2322 NW 7 St., Miami. BODEGON CASTILLA features guitarrista and cantaor, Leo Heredia. 2499 SW 8 St.; Fri-Sun. Luisita Sevilla Studio 576-4536 (Luisita, José Molina, Roberto Conchita Espinosa Academy 642-0671 (Rosita Segovia) La Chiquitina (flamenco) 442-1668 Maria Andreu 642-1790 (flamenco, bolero, regional) HAJJI BABA - Cuadro Flamenco Rayna Spanish Ballet, three shows on Sunday evenings, 834 Camino de la Reina Tel., 298-2010 ANDALUCIA RESTAURANT features Paco Sevilla playing solo guitar from 8:00 - 11:00 P.M. on Tues. and Wed.; Thurs-Fri-Sat from 9:00-12:00 he is joined by Luana Moreno (dancer) and Pilar Moreno (singer). 8980 Villa La Jolla Dr. (just off I-5 on La Jolla Village Dr.) RAYNA'S SPANISH BALLET in Old Town features dancers: Rayna, Luana Moreno, Theresa Johnson, Bettyna Belen, Rochelle Sturgess, and Jeanne Zvetina and guitarist Yuris Zeltins. Sundays from 11:30am- 3:30pm at Bazarr del Mundo. DANCE INSTRUCTION: <table><tr><td>Juana De Alva</td><td>442-5362</td></tr><tr><td></td><td>444-3050</td></tr><tr><td>Juanita Franco</td><td>481-6269</td></tr><tr><td>Maria Teresa Gomez</td><td>453-5301</td></tr><tr><td>Rayna</td><td>475-4627</td></tr><tr><td>Julia Romero</td><td>297-7746</td></tr><tr><td>GUITAR INSTRUCTION</td><td></td></tr><tr><td>Joe Kinney</td><td>274-7386</td></tr><tr><td>Paco Sevilla</td><td>282-2837</td></tr></table> etc... GUITARISTS AND STUDENTS are welcome to accompany dance classes. Call Juana at 442-5362. BACK ISSUES OF JALEO AVAILABLE: Vol.I No.1-6 are $1.00 each; all others, $2.00 each; add $1.00 per copy for overseas orders. GUITAR MUSIC AVAILABLE. Music of many top artists, both modern and old-style, transcribed by Peter Baime. Write Peter Baime, 1100 W. River Park Lane, Milwaukee, Wis. 53209 THE BLUE GUITAR in San Diego carries books by Donn Pohren, Music by Mario Escudero and Sabicas, and a complete line of guitar supplies (strings ½ price). Flamenco guitar lessons by Paco Sevilla. See ad for location ADELA: Available for seminar teaching in your area. Adela is an experienced teacher of danse oriental and flamenco (specializing in the Moorish style). Classes for beginners through advanced. For information, write: Adela, 1611 S.W. 19 Terrace, Miami, Fla 33145 \"THE NEW ART OF BELLY DANCING\" an illustrated textbook of belly dancing. Send $5.00 to: Adela, 1611 S.W. 19 Terrace, Miami, Fla. 33145 PANADEROS FLANENCOS, by Esteban Delgado, recorded by Paco de Lucía - accurately notated sheet music; $2.75 in the USA, $4.50 foreign, ppd. Southwest Waterloo Publishing Co., 6708 Beckett Rd., Austin, Texas 78749",
    "title": "JUERGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1979_12",
    "year": 1979,
    "language": "en",
    "article_type": "other",
    "pages": "38-40",
    "page_number": 38,
    "word_count": 580,
    "article_char_count_full": 4123,
    "article_char_count_review": 4123,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_01::A1",
    "article_text_for_review": "by Paco Sevilla For the better part of a year, Jerry Lobdill of Austin, Texas has been working to put together the main feature of this issue of Jaleo -- a tribute to the guitarist Edward Freeman, whose teaching and music transcriptions have a had a significant influence on the American flamenco scene. I welcomed this project because Mr. Freeman had always been a mystery to me. His name has come up so often during the years that I have been involved with flamenco, but I never knew who he was nor what he did. In such unlikely places as Encinitas and Oceanside, California, I would hear about amazing local guitarists who had studied with Ed Freeman in Texas (I never did meet one of them though); eventually I had a guitar student who had known Ed, but didn't play well enough for me to judge the teacher (we should never judge teachers by their students anyway, they may have had only a few lessons!); in San Francisco there were two guitar makers up from Texas who also spoke of Freeman. And so it went. Others have expressed to me that they also had heard of the man but knew nothing of him. We want to thank Jerry Lobdill for his work and invite other readers to consider putting together part of an issue of $ \\underline{\\text{Jaleo}} $. This month San Diego's Jaleistas have the opportunity to help determine on the future of the organization and the monthly juergas. An open meeting will be held (see \"Juergas\" for details) for the purpose of discussing whether the organization will continue to exist, whether the juergas will continue and, if so, what form they will take. For many juerga-goers, the monthly juergas have lost their appeal and attendance has dropped off -- especially on the part of the flamenco artists. Some of the newer members may not understand this, since they have little to compare with. The juergas of a couple of years ago had so much to offer: The food was incredible -- roast turkeys and hams, paellas, homemade cakes and pies, etc.-- and one could eat all night long; there was a warm \"family\" feeling, with all ages included -- dancers in their seventies and eighties and lots of children, many of whom could dance; the juergas were reunions of friends, a chance for old friends to meet, and new friendships to be made; most of San Diego's flamenco artists attended and flamenco would often be going on in three rooms at one time; finally, many artists visited from other areas, especially Los Angeles, and really sparked the juergas. Today, most of those qualities are gone or greatly reduced. There are a number of suggested causes. Among them: The formation of factions or cliques that have become alienated from each other; large numbers of non-performers putting pressure on artists to perform and not knowing how to contribute correctly to flamenco; problems in the organization and running of the juergas -- a few people have carried the load for 2½ years and cannot continue to do so; the decline in attendance by artists has reduced the appeal of the juergas for others, so that the attendance drops off even more. Members need to consider these and any other contributing factors they might think of and come to the meeting with suggestions. I feel that special emphasis and consideration must be given to the loss of interest by artists. Flamenco artists are attracted by the opportunity to interact with other artists and, as fewer performers attend, the juergas become less attractive to the remaining artists; a vicious cycle is set up, with the end result that attendance continues to decline. If the juergas are to be more than just cocktail parties, artists must be attracted back. I can think of only one way to do that. In a Spanish juerga, artists are normally paid. Jaleistas could pay a couple of artists, who would then not mind working hard all night long. To take this a step further, if the hired artists (for example, a guitarist and dancer or guitarist and singer singer) were from out of town, most of San Diego's artists would turn out to see them and interact with them; with all of those artists there it would seem that good juergas would result. It might mean a cover charge at each juerga, but it might also be worth it.",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_01",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 740,
    "article_char_count_full": 4199,
    "article_char_count_review": 4199,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_01::A2",
    "article_text_for_review": "Dear Jaleo This letter is to alert the Flamenco Association that my $ \\underline{\\text{Jaleo}} $ issues are arriving too late to attend events in my area. I saw María Benítez-Estampa Flamenca on television. I was very unhappy when I received my November issue on November 29, missing María Benítez at The Dance Umbrella in New York City, November 21, 23, 25. Luis Rivera Spanish Dance Co., Feb. 16 & 17...There are not that many events in my area and learning $ \\underline{\\text{TOO}} $ LATE THAT THEY ARE PERFORMING IN MY AREA AND ARRIVING TOO LATE AT THE STAGE DOOR IS $ \\underline{\\text{EXTREMELY DISHEARTENING}} $..... Please try to at least get Announcements out if the issues are to be mailed late. Sincerely, Leonard D. Kaminsky New York Editor's comment: This letter was probably not intended for publication, but we print it to focus attention on this problem. There are two major contributing factors to the delay in appearance of announcements. The first is our bulk mailing. We have discovered that bulk mailing is slow -- some readers get their Jaleo two months after the date they are mailed. We can do nothing about that, but you can; if you will send us $5.00 to cover the cost (or $17.00 for the total subscription), we will mail your newsletter first class. The other part of the problem lies in the fact that we don't receive information until too late; what good would it do to mail out the Jaleo earlier, if the announcements come in after the mailing? The \"Announcements\" continue to be one of the most frustrating parts of the Jaleo. At present their main value is as a record of what is happening around the country. Thanks for your input, Leonard -- in 2½ years, this is the first indication we have had that anybody even reads the announcements. ester luisa moreno international flamenco artist HOLLYWOOD (213) 506-8231 Dear Paco, After a long absence from a regular mailing address, I returned to San Diego and settled myself down for a long, long reading session of the last twenty issues of JALEO magazine. The first thing to strike me was how the quality of your numerous feature articles, columns, comments and notes has drastically changed. In the beginning they were good, for sure, but now they are superb. Your writing style has become professional, without doubt. Your articles are interesting and captivating-- I found myself completely absorbed in my reading and unable to stop until all twenty issues had been consumed. Thank you for your unrelenting devotion to bring us the beauty and pleasure of flamenco. Stan Schutze San Diego Editor's reply: Thanks for the kind words Stan and any time you want to come back and show us how to make some money, you would be most welcome; we haven't done anything really innovative with the publication since you left. Dear $ \\underline{\\text{Jaleo}} $, I sold my flamenco shoes, which I had placed an ad for in your magazine. Thanks a million! I don't know why I didn't think of advertising in $ \\underline{\\text{Jaleo}} $ before this. I received about 3 responses. Thanks, Nan Feinberg Oregon LETTERS TO THE EDITOR ARE ALWAYS WELCOME.",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_01",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 533,
    "article_char_count_full": 3113,
    "article_char_count_review": 3113,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
