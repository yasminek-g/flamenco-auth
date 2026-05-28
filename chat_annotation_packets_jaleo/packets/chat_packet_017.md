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
    "article_id": "JALEO_1978_04::A14",
    "article_text_for_review": "The task of learning flamenco guitar without a teacher would seem to be an impossible one. No matter how good the book, no matter how precisely the music is written, and no matter how many recordings are listened to, it just doesn't seem to work out. Compás, technique and \"aire\" don't come out of books. However, I think there are some valid uses of method books. They can be used with a teacher as supplements, as sources of additional exercises and material. They can be used for learning new material during periods when a teacher is not available (after the basics have been learned), and, for the desperate individual who finds it impossible to get to a teacher, books might be a better-than-not-high way to become familiar with the music and the guitar (but be prepared to unlearn and relearn everything when proper guidance is finally found. With that in mind, here is a list of guitar methods. It is divided into two groups; the first group I have personally evaluated, the second I have not seen. Juan Grecos, $ \\underline{\\text{The Flamenco Guitar}} $, Sam Fox Publishing Co. Inc., 1540 Broadway, New York, N.Y. 10036. Thorough analysis of technique plus nine solos written in music and tablature. Rasgueados are difficult to decipher in some cases, but falsetas are clear and accurate. Excellent for beginner-intermediate. Emilio Medina, $ \\underline{\\text{Metodo de Guitarra Flamenca}} $, Ricordi American, Buenos Aires, 1958. Written in Spanish; music only. A com- 50% DISCOUNT TO MEMBERS OF JALEISTAS HOFF CLEANERS CLEANING - PRESSING - ALTERATIONS 4940 EL CAJON BLVD., SAN DIEGO, CALIF. PHONE 583-4636 plete method of study with ten excellent solos that are loaded with material, and nine short examples of less common to-ques. Excellent for all. Mariano Córdoba, $ \\underline{\\text{Flamenco Guitar}} $, Oak Publications, 33 W. 60th St., New York, N.Y. 1971. A complete method with both music and tablature. There are eleven solos in traditional style. The notation and compas are accurate. Good for beginner-intermediate. Ivor Mairants, $ \\underline{\\text{The Flamenco Guitar,}} $ Southern Music Publishing Co. 1619 Broadway, New York, N.Y. 10019, 1958. A complete method of study in music and tablature, with twenty-four short examples of toques and eight solos. It is accurately written, although the rasgueados are confusing. Good for beginner-intermediate. Anita Sheer and Harry Berlow, $ \\underline{\\text{An Introduction to Flamenco Guitar}} $, Franco Columbo Publications Inc., Belwin Mills Publishing Corp., Melville, N.Y. 11746, 1964. Very little technique discussion. Ten solos in music only. The rhythm and notation are accurate, but the music is simple and not very exciting. For beginners only. Jack Buckingham, $ \\underline{\\text{El Arte Flamenco}} $, Spanish Music Center, New York, N.Y. 10019, 1957. A method book plus eight \"solos\" in music only. The music is very simple and, in my opinion does not accurately convey the feeling of the different toques. Not recommended. The following methods I have not seen: Juan de la Mata and Ronny Lee, $ \\underline{\\text{Flamenco Guitar Method}} $, available from Alfred Music Co., Inc., Port Washington, N.Y. 11050. Includes an LP record containing material in the book. \"The book is brief, but with the record it manages to give a good idea of the tricks of the trade for 7.50.\" (from \"Flamenco Methods,\" by Brook Zern in $ \\underline{\\text{The Guitar Review No.37}} $, 1972.) Chuck Keyser, $ \\underline{\\text{Introduction to Flamenco}} $, The Academy of the Flamenco Guitar, P.O. Box 1292, Santa Barbara, Ca- 93102. Contains 100 pages with an accompanying tape. Concentrates on music fundamentals, phrasing, chord progressions, and compas, and covers 15 basic rhythms. It is written in tablature and cost $85.00 a few years ago; write for up to date details. Chuck Keyser, The Flamenco Guitar, The Academy of Flamenco Guitar, P.O.Box 1292, Santa Barbara, Ca. 93102.",
    "title": "FLAMENCO Guitar Method Books",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "article",
    "pages": "15, 16",
    "page_number": 15,
    "word_count": 624,
    "article_char_count_full": 3941,
    "article_char_count_review": 3941,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_04::A15",
    "article_text_for_review": "... NEWS OF OUR JALEISTAS The dance group, \"Fantasía Española,\" performed for a dance convention at San Diego State University; in the company are Juana de Alva, Jorge \"El Calláo,\" Kevin Linker, Deanna Davis, and guitarist, \"El Tomas\"... rumor has it that Masami Hopper is in Spain for a visit of several months...Joe Kinney performed with dancers Juanita Franco and Carmen Camacho at the Cote D'Azur Restaurant...congratulations to members Tom Reineking and Betty Jobe, who will be married on April 25th...guitarists, Cynthia Jackson, Joe Kinney, John MacDonald, Tony Picksley, Tom Smith, and Digby Welch, played flamenco solos in a guitar recital presented by Paco Sevilla and classical guitar teacher (and Jaleista), John Lyon...Juana De Alva celebrates another action-packed year of life on the 18th of this month - Happy Birthday Juana!",
    "title": "EL OIDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 131,
    "article_char_count_full": 841,
    "article_char_count_review": 841,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_04::A16",
    "article_text_for_review": "\"FLAMENCO GUITAR - MARIO ESCUDERO\" transcribed by Joseph Trotter and published by Charles Hansen Inc. This is a new transcription of the previous Escudero book (see JALEO, March 1978) with old material rewritten and three new pieces, \"Caminos Malagueños\" (verdiales), \"Romance Gitano\" (siguiriya), and \"Homenaje a Ramón Montoya\" (rondeña). If you cannot find this book in your local music store, the distributor is: Educational Sheet Music and Books, 1860 Broadway, New York, N.Y. 10023. DANCE MAGAZINE this month (April) features a look at Spanish dance today, with several articles and many photographs. The major article presents a rather pessimistic view of the dance (readers might like to respond to this), but the pictures of many of the top Spanish dancers currently working in this country are outstanding. If you can't find this magazine at the newsstand, try the library or your local ballerina! MEETING OF ALL MEMBERS OF JALEISTAS!! Sunday, April 16th at the home of Jack Jackson At 7:00 P.M. There are important decisions which need to be made in determining the course of Jaleistas in the future. We don't want these decisions made by only a handful of members. Please come and contribute your votes and ideas. Address: 4990 Foothill Blvd. Pacific Beach Phone: 272-5748. See April Juerga for directions.",
    "title": "New Flamenco Guitar Music",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "article",
    "pages": "17",
    "page_number": 17,
    "word_count": 212,
    "article_char_count_full": 1317,
    "article_char_count_review": 1317,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_04::A17",
    "article_text_for_review": "The March juerga was one of spontaneous gaiety and typical flamenco flavor. The Jal-eistas were out in full force, and what better setting than the rambling, Spanish-style home of Isabel Tercero, with its arched doorways and rustic atmosphere. We are especially indebted to Isabel as she spent the night of the juerga in the hospital, but still allowed us to continue the festivities in her absence. The other residents of the house - Bernard, Vismaya, Lane, Peter, and Dana (all non-Jaleistas) - graciously pitched in and helped set up and keep things running smoothly. Once again it was a delight to see such a melange of people, young and old, from different ethnic backgrounds and walks of life, all gathered together with the same devoted enthusiasm for flamenco that makes us all one for at least one spellbound night. Perhaps that's why no one seems to bother much with names or formalities and the conversation flows as freely as the wine. The food was delectable and the first few hours, quite understandably, were spent sampling the cuisine and rekindling previous acquaintances. There was always that electrical current floating around in the air as excitement and anticipation were generated amongst the crowd. Before too long, the sparks ignited and there was no putting out the fire in our souls. It seems that sevillanas has become the official warmer-upper, and it is interesting to note how each person brings a different interpretation - notably Juanita Franco with her fiery spontaneity while, in contrast, Juana De Alva demonstrates classical style and graceful brace. Julia Romero was captivating as always, followed by the sensual intensity of Rosala and the radiating delight of the youngest members as they so enthusi- astically grasped the opportunity to join in. Not to mention Ernest Lenshaw, who is managing to end up embracing to ladies in the finale of sevillanas these days. There were some notable first-timers from near and far. From the Laguna Beach area were guitarists, Ken Sanders, Alex Peck, and Bob Florcyk (who played some wild Lucía-style bulerías). Brian, from the San Francisco area, plays the guitar and violin, among other instruments. Juanita Franco introduced two young students to the juerga who are learning to dance the way a Sevillana learns - movement, arms and aire first, steps later. Little Jacky and Reyes Barrio ended their sevillanas with a flourish that brought the juerga to a halt with a spontaneous ovation. A second-timer, Federico, fresh from Spain, surprised us Masami Hopper dancing soleares by grabbing Yuris' guitar and accompanying himself as he sang verse after verse of rumba Pilar Coates, from the Canary Islands, sang a lyrical guajiras and a tanguillo. Guitarist David Cheney could be heard storytelling in the quieter downstairs salas, punctuating his words with occasional strums on the guitar. As the night progressed, the guitarists played relentlessly over rhythmic palmas that drove the dancers on to ecstacy, and we sampled fandangos, alegrías, soleares, and a few unexpected variations on rumba. I would like to add here one note of criticism. Ponder for a moment how diligently our guitarists apply themselves at the juer-gas and that, without their efforts we would be without a basis for the juerga. Perhaps we could show them a little more respect and silencio during their performances so that we could further enjoy their talents and even save them from having sore fingers from trying to play over the noise. Let's show them how much we really do appreciate them and at the same time, give the dancers an opportunity to hear their accompaniment. Other than that, I would just like to say that flamenco is alive and well and flourishing in San Diego....Olé!",
    "title": "MARCH JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "17, 18",
    "page_number": 17,
    "word_count": 615,
    "article_char_count_full": 3745,
    "article_char_count_review": 3745,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1978_04::A18",
    "article_text_for_review": "The April juerga will be held at the home of Lora Lavis in La Jolla on Saturday, April 15th. Take Garnet turn off of freeway 5, right on Mt. Soledad to top of hill; turn left and left again on Cardeno. Cardeno runs into Via Anita. Address: 2261 Via Anita Phone: 454-3466. Here is the food key for April - If your last name begins with: $$ \\mathrm{~A~-~E~}\\quad\\mathrm{~B r i n g~a~s a l a d~} $$ $$ \\mathrm{~F~-~J~}\\quad\\mathrm{~B r i n g~a~m a i n~d i s h~} $$ $$ \\mathrm{~K~-~O~}\\quad\\mathrm{~B r i n g~a~d e s s e r t~} $$ P - T Bring bread or chips and dip $$ \\mathrm{~U~-~Z~}\\quad\\mathrm{~B r i n g~a~m a i n~d i s h~} $$ Because we are unable to give sufficient advanced notice of juerga dates, we are going to plan the juergas for the third Saturday night of each month so that you can plan ahead. Due to the degeneration in quality of the last two juergas, the coordinators of Jal-eistas are holding an open meeting to discuss this and other issues. It is hoped that all San Diego members will try to attend this meeting which will be held at the home of Jack Jackson at 4990 Foothill Blvd (take Garnet to Ingraham; go north on Ingraham which becomes Foothill; look for Loring, a cross-street near Jack's house). The meeting will be the day after the juerga, on the 16th of April. Since there will be another juerga before the meeting, we have decided to put several measures into effect for this juerga. The results of these measures can then be discussed at the meeting. First, we are going to ask members to co-operate by: 1. not inviting large numbers of guests or people who are not interested in the flamenco. 2. fulfilling your food and drink commitments for yourselves $ \\underline{\\text{and}} $ your guests. 3. going easy on the quantity of each food you eat. Food comes in all evening, so if you take just a little of each, everyone will enjoy more variety. 4. cleaning up after you eat (better trash containers will be provided) and saving your eating utensils in the special containers provided. 5. showing some interest in and respect for the singing and dancing. Do your socializing in an area away from the dance floor. If you don't know the correct way to do palmas, keep them to a minimum or at least clap softly. It is frustrating to performers and aficionados to be unable to hear each other. 6. being conscious of your behavior; if you can't control yourself after drinking too much, don't drink so much! The following measures will be in effect for this juerga: 1. All persons entering the juerga will sign in at the entrance and show that they are bringing food and drink; after 10:00pm you need not bring food. 2. Non-member donations will be raised to $2.00 per person. Non-members arriving without food or drink will be asked to donate an extra $2.00. 3. In order to cut down on party-crashers, non-members will be asked to identify the member who invited them before they will be admitted to the juerga. Uninvited persons have the option of joining Jaleistas at the door. ANNOUNCEMENTS Announcements are free of charge. They must be in our mailbox by the 15th of each month and will be discontinued after publication in two issues unless we are notified to renew them. Businesses may display their cards for $6 per month or $15 per quarter. Please send all correspondence to: $ \\underline{\\text{JALEO}} $，Box 4706，San Diego，CA. 92104 new york...",
    "title": "APRIL JUERGA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1978_04",
    "year": 1978,
    "language": "en",
    "article_type": "other",
    "pages": "18, 19",
    "page_number": 18,
    "word_count": 606,
    "article_char_count_full": 3379,
    "article_char_count_review": 3379,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
