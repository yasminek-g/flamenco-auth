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
    "article_id": "JALEO_1981_01::A5",
    "article_text_for_review": "by Caballero Bonald Editor's note: With this article, we begin an incredible journey that will require close to a year to tell in JALEO. In 1967, an anthology of flamenco cante was published by the Spanish recording company, Vergara, under the direction of the flamencologist J. M. Caballero Bonald. The \"Archivo del Cante Flamenco\" was six LP records containing material recorded in juerga situations in Andalucía. The booklet that accompanied the records contained photos and extensive text written by Bonald, telling of the adventure of making the recordings. Like most good flamenco recordings, this \"Archivo\" was on the market for a short while and then disappeared. For who knows what reason, an American company reproduced the \"Archivo\" on five records with a different accompanying booklet that contained a brief translation of Ronald's introduction and all the words to the songs (not included with the original). Recent reports indicate that this booklet often no longer accompanies the record. The record set (a great one) can still be found in some American stores and perhaps ordered from Everest records (last known address: 10920 Wilshire Blvd., Los Angeles, Calif. 90024) under the title \"The History of Cante Flamenco: An Archive.\" What we plan to do here is to translate the original, out of print story of how the recordings were made, so that a truly valuable piece of flamenco writing will be made available to a wide audience. For those who are familiar with many of the names and places, the story is a truly interesting one. For those who are not, it is an incredible education in flamenco and its roots. Here is the first installment -- an introduction. PART I PREPARATIONS (Translation by Brad Blanchard) These pages don't claim to be anything but a simple, informative guide for the listener, especially conceived as a literary complement to the recordings that make up the \"Archive.\" We wanted to offer a living chronicle of the work that was undertaken. In a certain way, we have enclosed our personal information about the complex moral and material world of flamenco in a type of travel journal, paying primary attention to our experiences during the search for sources and the carrying-out of the taping One cannot deny that the job of gathering and ordering with even slight coherence the dispersed and diverse abundance of cante flamenco has yet to be done. We naturally are referring -- and even counting some partial and praiseworthy efforts achieved in this sense -- to the important discographic aspect of the question. In spite of the growing bibliography produced in the last few years and enthusiastic attention by the most heterogeneous sectors of the public, flamenco continues to be a phenomenon of popular music very fragmented and shallowly known. One cannot doubt that the most effective and complete recorded archive is yet to be seen and is the only visible possibility for fixing the purity of the older forms, conserving and allowing to become known with a precise historical guarantee, the totality of the greatest examples of cantes that have survived until our time. We are conscious that a job of this type brings with it a whole series of obstacles that are difficult to overcome. There was established in the beginning the ticklish problem of finding creditable sources in the native zone of the cante and the no less arduous stumbling block of the more and more frequent professionalism of the cantaores. The character of our \"Archive\" could not stray from the fundamental idea of finding non-professional interpreters, anonymous in many cases or only known in the limited sector of their respective places of birth. Once the general work-plan was established, it was obligatory to carry out an advance exploration of the ambiente (social atmosphere of the surroundings). Our personal experience -- or that of third parties -- guided us on an attentive and detailed sweep of the concrete geographic band that goes from Sevilla to Cadiz and that constitutes, without a doubt, the territorial nucleus where flamenco was formed and developed. In the first trips we were able to prove something that we already suspected: the increasing absence of cantaores in their native zones. Little by little the social foundation of the cante has been experiencing a series of logical transformations, subordinated to the normal and progressive changes in the life style of the cantaor and the absorbing influence of profession-alism. What began by being an intimate way of expressing so many episodes of hunger and persecution atavistically submerged in the memory of the Andalucian gypsies, became changed by the passage of time into a few initiated repetitions of those original",
    "title": "ARCHIVO: PART I",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_01",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "10",
    "page_number": 10,
    "word_count": 772,
    "article_char_count_full": 4725,
    "article_char_count_review": 4725,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_01::A6",
    "article_text_for_review": "comments that could be disturbing? And how to tie together and give the guitar adequate continuity to compás when it would be necessary to extract a few examples from among a group of cantes, especially when they were closely linked to each other? Aside from well-known technical methods for accomplishing these tasks, we have been true to our belief that our \"Archive\" should avoid, in accordance with our basic objectives, the usual character of sound that is associated with recordings that are made in a studio. The term \"archive\" is sufficiently self-explanatory in this respect. It means filing all this living and expressive documentation of the flamenco that can be found in its area of origination and taking advantage of the most unusual moments and occasions. One true fragment of cante or an isolated moment of brilliance standing out from the usual confusion of the fiesta -- these were especially valuable for our purposes. It would have been preposterous to try to carefully plan the performances or to select the guitarists -- very bad in some instances -- who would best serve to support each cante. From the time we first began our task, we have fundamentally been concerned with the truthfulness and maximum effectiveness of the material we would record. From this point of view we believe that our \"Archive\" includes the most traditional repertoire of cantes that can still be found in their native setting: In a tavern in Triana or Jerez, in a home in Puerto de Santa Maria or Mairena del Alcor, in a \"venta\" (roadside inn or tavern) Cádiz or Alcalá de Guadaira, in a small restaurant in Arcos or Utrera, in a courtyard in Morón or Lebrija... It should not be necessary to allude to the fact that the massive undertaking of grouping all of the cantes, each of the many known variants of flamenco, would have made our ambitious project practically impossible. With the hundreds and hundreds of styles of cante that could be classified in Andalucía -- not to mention Extremadura, La Mancha and Murcia -- and the uncountable individual styles and distinctions made by each interpreter, the huge task of recording this flood of divisions and sub-divisions of flamenco would have demanded the employment of means far in excess of what we could permit ourselves in this private venture. Our \"Archive\" is not intended to be more, nor less, than a basic panorama of the most genuine cantes that have survived to our time, authenticated by tradition and by the reliability of the artists we have chosen. But we insist that this labor of compilation would not have attained its validity without the support of the recordings made in the zone of flamenco's birth and surrounded by the natural climate in which it takes place. We wanted to contribute a useful and necessary effort to benefit the popular culture, to create an archive with integrity that would preserve the most authentic expressions of flamenco.",
    "title": "CONVERSANCIONES CON GINO D'AURI",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_01",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "11-12",
    "page_number": 11,
    "word_count": 493,
    "article_char_count_full": 2921,
    "article_char_count_review": 2921,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_01::A7",
    "article_text_for_review": "(From: $ \\underline{ABC} $, Aug. 28, 1980; sent by Gordon Booth, translated by Paco Sevilla) by Miguel Acal We have to receive the return of the Gazpacho almost like a prodigal son. It was lost one day some years ago when they wanted to mix, for the sake of an ill-advised commercialism, flamenco and politics. But the Gazpacho has returned; that is the important thing. Now we must go carefully, with far-seeing vision and short steps so that we don't fall again into the difficulties that forced the previous disappearances... Like all festivals, there have been good nights and others not so good. Like all of them, there have been moments of Heaven and of Hell. But -- and if it were otherwise, they wouldn't exist -- there were more of the first than the second...Now there is no longer the solemn bass strings of Diego, he who in 1964 or '65 made us cry when, the festival over, he played the \"What'd I say\" of Ray Charles. In the soul and hands of Diego, everything became profoundly gypsy. That Amaya, of the dark ones from Ronda, has put aside forever his perfumed guitar. Juan and Paco del Gastor will be, on this night, the successors to that indescribable flavor. And Pedro Peña: Maestro compañero / compañero del alma, en mi corazoncito / llora tu guitarra.",
    "title": "GAZPACHO DE MORON",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_01",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "13",
    "page_number": 13,
    "word_count": 226,
    "article_char_count_full": 1270,
    "article_char_count_review": 1270,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_01::A8",
    "article_text_for_review": "(From: $ \\underline{ABC} $, Sept. 3, 1980; sent by Gordon Booth, translated by Paco Sevilla) by Miguel Acal It was not yet ten o'clock at night when I entered the site where the site where this edition of the Gazpacho would be celebrated. I wanted to arrive early because the celebration was, to me at least, very important. With Pedro Peña and Luis García Caviedes -- I ran into them in the gas station where they were giving a drink to their steel horse -- I had a beer and some snacks. We were given some magnificent olives. We were celebrating their quality when we saw the cap of Bernabé Coronado. Greetings, embracings, and memories. So many good memories. Those times when Manolito de María charged five hundred pesetas ($8) or a little more and the Gazpacho was an extended fiesta in which Diego gave symphonic lessons in purity, or Fernandillo threw -- my God, with such art! -- his handkerchief on the floor in order to pick it up again, or Mairena who made absolutely clear his quality, or Menese who changed out of his work clothes just minutes before going out to sing. Those Gazpachos in which, at the end, the artists would sign Bernabé's shirt, turning it into a trophy. The night was beautiful. The beer was hot within five minutes of being served. But the heat is good for flamenco festivals. Like MIGUEL VARGAS WITH PEDRO PEÑA; WITH HIS USUAL SERIOUSNESS AND STRNGTH, HE HAD MORE QUALITY THAN OTHER TIMES, BUT DID NOT HAVE A TRULY GREAT NIGHT. DIEGO CLAVEL HAD STRENGTH AND DELIVERY; HE FULFILLED HIS OBLIGATIONS COMPLETELY AND HARVESTED DESERVED APPLAUSES. (photos by Paco Sánchez) The others -- Antonio Savedra, Ytoli, Diego Clavel, and Miguel Vargas -- fulfilled their",
    "title": "MUCH CEMENT AND LITTLE GRAVEL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_01",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "13-14",
    "page_number": 13,
    "word_count": 295,
    "article_char_count_full": 1690,
    "article_char_count_review": 1690,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_01::A9",
    "article_text_for_review": "GAZPRCHO DE GUILLE RM RIGHT HAND TECHNIQUE PROBLEMS? by Guillermo Salazar I have met \"montones de guitarristas\" who have technique problems with the right hand. I have had periods in which I struggled with technique problems so badly that I thought of quitting flamenco. What is it that causes these problems? Is it all in your mind? People who cannot help will tell you that you have a mental block or that you enjoy searching for some answers that do not exist. So the question is: Are there secrets to technique and are the masters holding back these secrets? My first observations were that on certain days I felt really hot, and other days I felt really off. I believed biorhythms were responsible, then later, thought the weather, humidity, flexibility of the nail, and many other variable factors were involved. I tried using different soap, moved to a different climate, changed my diet -- all to no avail. I consulted with top professionals who said things like \"practice,\" or \"take a walk in the park and listen to the birds,\" or \"hold your hand like this.\" the flamenco buzz. However, disregarding this, try your right hand techniques with your strings at different heights. This may be the root of all your troubles. --Your flamenco guitar may have a classical bridge. The flamenco bridge is very low to the top of the guitar; the classical bridge is higher. If you have a guitar with a classical bridge, you can lower it considerably, but don't start filing away the wood to get it even lower. A new guitar is the answer to the problem. Don't sell your old guitar until you have replaced it with something more compatible -- especially since this may not be the problem you have with your technique. --Try a thicker golpeador. Sometimes this simple process makes the right hand feel very comfortable. It gives a different feeling to the operation of the right hand, but has the disadvantage of slightly deadening the brilliance of the guitar. White styrene, available at hobby shops, comes in many thicknesses and may give you a new comfort in playing. Be very careful when removing your old golpeador, as it can be a sorrowful experience. Have a professional do this for you unless you know a lot about guitars. If you do it yourself, don't use crazy glue or epoxy when gluing the new golpeador on. Maybe you can experiment by taping a piece of styrene to the guitar to see if you really want to go through with the experiment. --Try playing with no thumbnail. If you develop a callous on the thumb, you can play effectively with no nail. Many guitarists play this way, but it is a give and take matter. The hand balances out nicely for some guitarists with technique problems, but they sacrifice alza pua and other effective thumb playing techniques. If you do this, have a higher golpeador, at least on the bottom part of the guitar, which pushes the hand into place and permits playing effectively with a very short thumbnail. Listen to a Juan Serrano record and you'll hear Serrano's two different sounds, especially in the tremolo. The notes played by the fingers are nail sound, the thumb is skin sound. --Try sitting in different postures. My favorite position is flamenco style, with the right foot elevated on a rung of a chair or a footstool. Also sitting on a pillow helps if you have long legs. These aids seem to throw the right hand into place. These are just a few suggestions to try. I cannot guarantee anything because everyone is different. Find what works for you. The problem with many flamenco teachers is that they try to make others do it their way. There is no one way that is right!",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_01",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "15-16",
    "page_number": 15,
    "word_count": 639,
    "article_char_count_full": 3621,
    "article_char_count_review": 3621,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
