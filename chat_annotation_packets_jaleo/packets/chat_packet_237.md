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
    "article_id": "JALEO_1985_SPRING::A16",
    "article_text_for_review": "AROUND THE TOWN Flamenco really seems to be picking up in San Diego with regular shows at the Tabloa Flamenco, La Posada del Sol, Drowsy Maggie's and Old Town. Concerts have been presented by Fuego Flamenco, Remedios Flores and Isabel Tercero. Performers from out-of-town have appeared such as: dancers Mariana (Stalian-gypsy from Switzerland), Juan Talavera (Los Angeles), Eva Encifas (New Mexico); singer-dancer-guitarists El Pollitó and Solomon (Santa Cruz), Sarita Heredia (Spanish-gypsy from Los Angeles); gypsy-singer Manuel Agujetas and others. Thanks to the juerga committee headed by vice-president Paul Runyon and juerga coordinator Rafael Diaz and the generous offers of juerga locations from our members we have had a juerga almost every month. Please send pictures of your flamenco activities for the $ \\underline{\\text{Jaleo}} $. We need $ \\underline{\\text{clear}} $ black and white or colored shots accompanied by a few lines or paragraphs about the event. JALEO - VOLUME VIII, NO. 2 The image is too blurry to recognize any text content. ABOVE: GUITARISTS DAVID DE ALVA AND \"EL PINTOR\" ACCOMPANY DANCER LISA MELLIZO, BELOW: \"EL PINTOR\" ACCOMPANIES JUANITA FRANCO ANGELA FINISHES ZAPATEADO ANGELA GIGLITTO AND JUANITA FRANCO--FARRUCA ANGELA - ZAPATEADO",
    "title": "SAN DIEGO SCENE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1985_SPRING",
    "year": 1985,
    "language": "en",
    "article_type": "other",
    "pages": "34-36",
    "page_number": 34,
    "word_count": 190,
    "article_char_count_full": 1267,
    "article_char_count_review": 1267,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SPRING::A1",
    "article_text_for_review": "AMERICAN GUITARIST GARY HAYES (RIGHT) IS ACCOMPANIED BY A FRIEND IN A SEVILLA BAR Flamenco uses a 12-beat rhythm that's much more intricate than that of most other Western music. It features two instruments -- voice and guitar. Hand claps and heel-stomping -- \"taconazos\" -- can also be added. Although performed on stages and \"tableaus\" in Spain and throughout the world, flamenco is at its purest when shared between musicians and a few onlookers in marathon, all-night sessions. These \"juergas\" usually occur in intimate settings and can sometimes move participants to rip their shirts or bite their hands until they bleed. Traditional flamenco, however, is becoming more scarce as Spain leaves 40 years of isolation behind and moves into the European mainstream. Younger players, encouraged by the commercial success of such fusion artists as guitarist Paco de Lucía, have developed a flashier style, integrating jazz and blues chords into their music. The more complicated the technique, the more difficult it is to maintain flamenco's emotional essence, Hayes said. \"In a way, the new style is natural because it reflects the way society is changing, but I've never been much in tune with that anyway.\"",
    "title": "SPAIN'S STYLE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 194,
    "article_char_count_full": 1208,
    "article_char_count_review": 1208,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SPRING::A2",
    "article_text_for_review": "JALEO FLAMENCO CONTACT Dear Jaleistas: Enclosed is my $20.00 check for another year of Jaleo. Please don't let me skip an issue. I've enjoyed your periodical for many years. As a busy bond broker, it allows me continuous contact with the flamenco elements of the world and makes worthwhile the thousands of hours I have spent sitting on my ___ playing resqueados and falsatas. Keep up the good work. Sincerely, Peter Landay Los Angeles, CA 要若要 EXCHANGE FROM NEW JERSEY Dear Jaleo: First, I want to congratulate you on the fine magazine you publish. I find it very interesting and look forward to receiving each issue. I am also searching for a copy of the book by Donn Pohren called \"Lives and Legends.\" If you have any information as to where I could purchase a copy and the cost of it, I would appreciate receiving this information from you. In response to JoAnn Zugel's letter requesting information regarding a good teacher or cante flamenco, I suggest that she contact \"Chinin de Triana\" in Hollywood. I know him personally and find him to be very dedicated to cante and all aspects of flamenco. His correct phone number is (213) 463-1614. Please correct his phone number in your directory. [Done. Thank you.] Finally, I would like to place a classified ad in your magazine to sell a few guitars. Please send me the information and rates for this. JALEO - VOLUME IX, No. 1 FLAMENCOS NO COMEN* I am always happy to see one of Donn Pohren's knowledgeable articles. Unfartunately Denn Pohren's writing sometimes tries to serve Denn Pohren as well as serving aficionados. Pohren's beautiful days in Morón did not end when Diego and artists in Morón \"discovered money\" (p. 14). His days ended when these artists finally discovered that Señorito Pohren was charging the American guests st his finca Casta dei Sal pricas and paying the artists olive-picker fees. The awa thet Pohren felt for Diego was real, but somehow went along with the eí- too-common belief that Diego end the rest could live on fino end ginebra. The irony of the expression \"Flamencos no comen\" was evidently lost on Pohren. Yes, he provided work for the artists, but then so did the olive-grove owners provide work for the campesinos. No one argues about the work that Pohren provided or the publicity for Marón, it is the pay that is at issue. Diego was supporting many peopla, Manolita el de la Maria didn't liva ins caws by choice, and the others had hungry children. Yes, the artists \"discovered what money could buy\"--it could buy food, clothes, and it could pay the rent. By the time the lete Sixties came around, Pohren was still paying them early Fifties money for fiestas. Most who stayed at the fincs were unaware of this. Some of the foreign \"townies\" did become aware of what Pohren charged and what he paid, and told the artists things that Pohran avidently preferred they not know. It's also ironic that his article ends on e page that has an advertisement for Morón tapes. The ad makes no mention of royalties for the artists or for the impoverished families of those artists now dead. Buan proveche a todos! El Cumparasita [Editor Comment: With regard to your last paragraph, concerning the selling of tapes, let me assure you--es one who has been involved in selling flamenco materials--that nobody will ever make money selling the sort of thing. The income generated would probably bsraly psy the pastage needed to contact the \"impoverished families.\" We should be thsnkful that, hopefully, s few of these tapes will circulate and the memory of these artists will not die away completely. The materiel should, in fact, be put in same more permanent fsm, such ss a record, bsfare these 20 year old tapes degenerate to the paint of being unlistenable. There is another way to look at the whale situation. If a person could get rich selling tepe of dead famencos (impossible) and didn't pay the families, you would end up with one rich famence, many happy aficionados, a tribute end memorial to the artists, and poor families. If no one sells the tapes, you have nothing and the families are still poor. If I had to choose between just those options (there are others, of course), I would prefer the former. Just one person's opinion. --Paco Sevilla IN SEARCH OF GUITAR INSTRUCTION IN ARIZONA Dear $ \\underline{\\text{Jales}} $, I am writing to tell you how much I enjoy reading Jaleo. I am an sficionado flamenco guitarist but unfortunately in my home town there is absolutely no flamenca culture et all. I was fortunate to live in Denver, Cajaredo for about six months and I took some flamenco lessons from one of the best guitarists I have ever heard (Rene Heredie) an excellent teacher and friend. I practice daily the little I learned from Rene. Since there is no teacher in Douglas or anybody that has any knowledge or tsste for this (musicia iniqualable) I would appreciate any information or ideas that would further assist me in my playing. Sincerely yours, Radolfo R. Acedo Pirtieville, AZ [Editor: A good place to start is by contacting the people listed in the Jales directory under \"Arizona\". If our readers have any suggestions we will print them or pass them on to Rudolfo.]",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "4-5",
    "page_number": 4,
    "word_count": 895,
    "article_char_count_full": 5174,
    "article_char_count_review": 5174,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_SPRING::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMonths had passed when, by chance, a flamenco show came to my little suburb just outside Tokyo. The artists were all Japanese, so I got a friend to come and help me talk with the performers after the show. I was excited; Japanese flamenco at last! The curtain hadn't lifted when a man appeared up stage and started playing the guitar. He was joined by a man who did palmas. Another guitarist and a singer appeared down stage. Sort'a living stereo, as I had come early and was front row, center. They all had good technique and performed in that small space on stage between the curtain and the audience. The singer sang from his heart. I was thoroughly enjoying myself when, to my astonishment, the show stopped. There was sort of a slide show that projected Japanese characters on the wall. My\n\n[EVIDENCE WINDOW 1 | retrieval_hint=HERIT_03 | trigger=\"represent\"]\n\nhere she was, poised as a crouching figure at the back of the stage (photo A), with mist drifting smoke-like around her. This mist reminded me of the dust in the factory scene of Antonio Gades' \"Carmen\". It was a slow, building soleares. She was dressed in black, her face covered by a shawl. Slowly she uncoiled revealing a face that had so much white make-up that she could have been in a Kabuki theater. I got the impression that her shawl was to represent the wings of a bird. I found myself struggling with the idea that this is not the way that flamenco should be. Yes, her technique was good; but still? Her performance did appear to be original and had a jondo feeling. I could see the sweat flying from her head when she executed her turns. The curtain fell and another slide show began. The next number was an alegrías. As the curtain rose, the singer began. The guitarist pulsated this familiar rhythm, the palmas flowed. The dark mood seemed to have vanished with the first number. She donned a white dress that was not typical of flamenco. It was a peasant type dress that had elastic at the waist and neckline (photo B). She walked and skipped around the stage. There was a basket that had pretty flowers sticking out of the top, which she picked up as she danced. When the escobilla came around, she turned the basket upside down. Flowers\n\n[ENDING CONTEXT]\n\nSUB TOTAL Postage & Handling ___ TOTAL DUE ___ Please add $1.50 for Postage & Handling for 1st tape, $0 for each additional tape. 1349 Franklin Bellingham, Washington 98225 Ph. (206) 676-1964 TEODORO MORCA IS NOW OFFERING ON VIDEO TAPE, A COMPLETE APPROACH TO STUDYING FLAMENCO DANCE, IN TECHNIQUE, INTERPRETATION REPERTOIRE AND UNDERSTANDING, WRITE OR PHONE FOR A \"MENU\" OF TAPE SELECTIONS.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FLAMENCO IN JAPAN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "6-12",
    "page_number": 6,
    "word_count": 2191,
    "article_char_count_full": 13172,
    "article_char_count_review": 2773,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "HERIT_03",
        "family": "HERIT",
        "trigger": "represent"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_SPRING::A4",
    "article_text_for_review": "\"TO RELEASE WHAT YOU CARRY INSIDE\" by Enrique Calduch Photo: Heinz Hebeisen [from: Ronda Iberia, Jan. 1986; sent by El Chileno; translated by Paco Sevilla.] It is said that they have it inside them, and it must be so, because, if not, then there is no explanation for the transformation. María José is 21 years old and arrives at the flamenco school with her hair loose, wearing a colorful sweater, jeans, and tennis shoes; she is a normal young girl. When she comes out of the dressing room, she seems like someone else -- her hair gathered in a ponytail, a fitted top revealing her figure and leaving her arms free, and, in place of jeans, a tight skirt opening into wide ruffles, and some shoes with sturdy heels. When she begins to dance in front of a mirror in the classroom, she no longer seems like another person -- she is another person. The concentration, the body movements, the rhythm of her feet on the wooden floor, all combine to make it seem incredible that this marvel of sensitivity is the same normal girl that came in the door a few minutes earlier with a dance bag over her shoulders. The movie \"Carmen\", by Antonio Gades and Suara, had little to do with the rebirth of flamenco, at least for Spaniards; this art form has for some time now been increasing its influence and its aficion has been rising like foam. The days are long gone when it was identified with the Spain of the \"charanga\" [party, juerga, jaleo] and tambourine, from which most Spaniards who aspired toward a more modern country readily fled. Those same people are taking a second look now at this popular culture, they value it, declare it an art, and support it. Perhaps this is the reason the flamenco schools are proliferating in all of the major cities, especially Sevilla and Madrid. There are good schools and bad, but basically they consist of a room covered with large mirrors and floors of wood to give good sound to the feet. Many start students in classes of ballet as a form of preparation. Later some classes of technique - arms, legs, footwork, turns, and \"paliillos\", or castanets. And, finally, flamenco puro, in classes that begin at around 2,000 pesetas a week [c. $15.00]; if the class is private, it will cost more than twice that, plus the room rental and the guitarist. Surprisingly, about half of the students in the expensive schools are foreigners and many of those, Japanese. MARIA MAGDALENA GIVING PRIVATE LESSONS SKIRTS WHIPPING LIKE OCEAN WAVES JALEO - VOLUME IX, No. 1 street and goes to the bar on the corner to have a beer with her friends. There is another student there also, a young blond girl from Zurich, Switzerland. When asked why she studies flamenco, she doesn't understand the question. A friend of hers translates. The girl says something in rapid German and the improvised translator answers tranquilly, \"She says it is something she has inside!\" 'GYPSY GENUIS' HISTORIC - EXCLUSIVE VIDEO RELEASE BY MANUEL AGUJETAS DE JEREZ (CANTAOR) For the first time in flamenco history the legendary Manuel Agujetas de Jerez performs on video cassette. The world famous maestro of the Jerez dynasty of gypsy flamenco singing gives an historic performance that will remain forever. Beautiful cantes por Solea, Fandango Grande, Siguiriyas, Malagueñas, Romeras, Taranto, Tientos, Bulerfas. Length-90 minutes in color. This video features the special collaboration and original guitar accompaniments of recording and concert artist RODRIGO. Don't miss out on this first world release as it is a collectors item. No studio video of this kind has ever been made. Order Beta or VHS. Only $49.00. Send cash, check or money order to Alejandrina Hollman. 148 Taft Ave., #11, El Cajon, CA 92020. The performance took place on August 5, 1985. An educational \"must\" for guitarists and singers. Allow 3 to 4 weeks for delivery.",
    "title": "FLAMENCO SCHOOLS IN SPAIN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_SPRING",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "13-16",
    "page_number": 13,
    "word_count": 657,
    "article_char_count_full": 3836,
    "article_char_count_review": 3836,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
