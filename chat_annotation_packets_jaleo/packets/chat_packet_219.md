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
    "article_id": "JALEO_1990_04::A4",
    "article_text_for_review": "PROGRESS VERSUS TRADITION There has been a lot of heated discussion about Paco de Lucia's new directions in guitar playing over the years. I received my share of criticism when I wrote a little piece of fiction some time ago in Jaleo about the direction in which flamenco seemed to be heading. A recent article, \"Paco de Lucia — A Guitar Player Crossing the Border of Flamenco at the Zenith of His Life\", Jaleo, March, 1989, seems to focus on and defend the changes Paco has brought to the flamenco guitar in his career. It is stated that in flamenco today the choice must be made between tradition and progress and between purity and freedom. And then the subject of purity is elaborated upon. Basically, what we are told is that Paco is happy with the changes he is making and that Sabicas is responsible for telling him that he must play his own music if he is ever to be considered a great force in flamenco. This is all highly interesting information. Who is more qualified to make such pronouncements than Sabicas? And who is more qualified to define the direction in which flamenco will evolve than Paco de Lucia? I certainly would not presume to challenge their authority in these areas. Yet, I, you, and we, all are entitled to our opinions, whatever they may be, on these points. We are certainly entitled to our personal emotional responses to whatever music reaches our ears, and we are entitled to express those responses. We are not required to agree that Paco's new direction is as emotionally satisfying to us as was Sabicas', or Niño Ricardo's, and there is no logic that can ever prove that we should feel differently about it. The attraction to a particular type of music is an intensely personal experience which may have nothing to do with the music's origin or the meaning of its lyrics. It defies rational explanation. There was an article in the newspaper recently about the growing audience for American country music in Poland. Some of the fans have guitars and have learned to sing the English words phonetically. These Poles don't understand the meaning of the words they are singing, but they say they are not interested in the meaning anyway. The voice in country music is like another instrument to them. Knowing the meaning of the words might destroy the ineffable emotional message they get from the music. The Japanese people seem to have a similar ability to enjoy and get deeply involved with American country music, bluegrass, flamenco... and who knows what else? In view of all this it is not so strange that those who are the most insistent on rules of tradition and purity in flamenco are frequently those whose very involvement in flamenco breaks those rules. Those who are deeply moved by music — of whatever genre — simply feel strongly about it. They want more like it and they don't appreciate others trying to kill what they love. Count me among this group of reactionaries. If the Phrygian mode is abandoned in \"modern\" flamenco, and all that remains is the guitar and the compás, there are bound to be many of us who will not be happy about it. Why, we wonder, is this necessary? Why couldn't the new music be given a new name? Why must the new kill the older? The greats themselves have declared that each true great must produce his own distinctive music, which must remain largely unwritten. While delighting record publishing companies for profit reasons, this attitude virtually guarantees that only cliches will survive, while the best music will be played by its composer only and will die with him. In spite of the availability of perfectly adequate notation techniques which could preserve the best pieces in written form for posterity, it is considered almost heresy to do this, and no aspiring great would dare admit to being able to read the notation or even having any interest in it. As our phonographs become obsolete and tape recorders evolve, even our ability to hear the recorded pieces will be stripped away. There will be no profitable market for old flamenco in new digital tapes or CDs. When I think of what was probably lost with the deaths of Favier Molina and Manolo de Huelva, and what was surely lost with the passing of Niño Ricardo, and SEND SELF ADDRESSED STAMPED ENVELOPE FOR A FREE LIST (Flamenco music of the greats: Sabicas, Paco de Lucia, Ser- ranito, Capero, Ramon Montoya, Ricardo, Sanlucar - many RARE FLAMENCO RECORDS - $5 Plus postage. (818) 789-1453 MAURICE SHERBANEE 5329 NORWICH AVE. VAN NUYS, CA 91411 Lester DeVoe Flamenco and Classical Guitars Played by: Joaquin Amador Juan Martin Sabicas Free Brochure 2436 Renfield Way San Jose, CA 95148 USA (408) 238-7451 FLAMENCO GUITAR Play the compositions of PEPE HABICHUELA, MANOLO SANLUCAR, NIÑO MIGUEL, PACO CEPERO, TOMATITO, ENRIQUE MELCHOR, ETC. From the transcriptions of their recordings in TABLATURE NOTATION. Respecting note for note the original fingering. OVER 190 TITLES AVAILABLE PLUS 25 ANTHOLOGIES OF FALSETAS ALAIN FAUCHER, 28 RUE DE LA RÉINE BLANCHE, 75013 PARIS, FRANCE FLAMENCOI HEBASY WAY An introduction to flamenco dance for beginners. A clear simple step-by-step instruction of the following dances, first by count and then with guitar accompaniment, in two speeds - slow and medium. • Sevillas - Tanguillo - Bulerías Send check to A. Vergara, 1825 Echo Ave, San Mateo, CA 94401 - $49 includes Video, postage and handling.",
    "title": "PUNTO DE VISTA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "6-7",
    "page_number": 6,
    "word_count": 920,
    "article_char_count_full": 5381,
    "article_char_count_review": 5381,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1990_04::A5",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nphoto and text by Curtis Fukuda Juan Martín lives and breathes art. You can see it in the lightness and grace of every step he takes. You can hear it in the poetic lilt to his voice, in the imagery of his words. Every fiber in Juan's body is full of expression, and for the public, it is his magnificent guitar playing that communicates so much art. Juan Martín occupies a place among the best of modern flamenco guitarists. He performs with authenticity and accompanies cante at the festivals in Andalucía every summer. While being reverent with regard to flamenco tradition, Juan carefully interprets the falsetas of the masters (Ricardo, Manolo de Huelva, Ramon Montoya, etc.) with enough life to avoid the sense that someone is merely dredging up the archives. Additionally, Juan is a flamenco\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"master\"]\n\nce among the best of modern flamenco guitarists. He performs with authenticity and accompanies cante at the festivals in Andalucía every summer. While being reverent with regard to flamenco tradition, Juan carefully interprets the falsetas of the masters (Ricardo, Manolo de Huelva, Ramon Montoya, etc.) with enough life to avoid the sense that someone is merely dredging up the archives. Additionally, Juan is a flamenco scholar. He has written the masterful El Arte Flamenco de la Guitarra (published by Theodore Presser), and contributed to Evans' Guitar Anthology. These achievements would be enough to earn him a place in flamenco history, but Juan goes further. He explores new expressions in the flamenco context. Listen to the \"Painter in Sound\" album with Mark Isham; Juan explores the fusing of flamenco themes and rhythms with the meditative atmosphere of New Age Music. His newest album, \"Through the Moving Window,\" with jazz keyboardist Todd Cochran, is a continuation of the New Age explorations. While not \"pure,\" as are his solo albums, these recent collab\n\n[ENDING CONTEXT]\n\neverything if you are not careful. So I just stick with the guitar. That's me... a guitar and a suitcase and off I go. Juan just played for Laura del Sol in a film to be released this Fall — also starring Denholm Ellot and Julie Walters. MORCA 1349 Franklin BELLINGHAM, WA 98225 Pn. (206) 676-1864 \"Morca Castanets\" Made by a Dancer for Dancers who want the finest concert quality, musical instruments. Morca Palillos are made from \"Tela de Musica\", an unbreakable material that has all of the qualities of the finest granadillo. Hand tuned... all sizes. Write or call for more details or brochure.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "JUAN MARTIN IN AMERICA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "poem",
    "pages": "8-14",
    "page_number": 8,
    "word_count": 4110,
    "article_char_count_full": 23175,
    "article_char_count_review": 2698,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "master"
      }
    ]
  },
  {
    "article_id": "JALEO_1990_04::A6",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nby Paco Sevilla It has been many years since I last wrote for Jaleo, so I thought I would share some of my experiences in Spain during August and September of 1988 while doing research for books on flamenco. The research required me to run all over Andalucía, as I needed information from most of the major cities. I wanted to visit many of the places where I had lived in the 1960's as a teenager. MADRID In Madrid, I took a taxi to the Plaza Santa Ana to look for a place to stay. The Plaza Santa Ana was once a center of flamenco nightlife. There were still touches of flamenco remaining when I was there in 1964, but then they dug up the park and put a parking garage underneath. Now the bars and cafés that had once served as outdoor tables in the plaza are gone. In their place are jazz and\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"clubs\"]\n\nwhere I had lived in the 1960's as a teenager. MADRID In Madrid, I took a taxi to the Plaza Santa Ana to look for a place to stay. The Plaza Santa Ana was once a center of flamenco nightlife. There were still touches of flamenco remaining when I was there in 1964, but then they dug up the park and put a parking garage underneath. Now the bars and cafés that had once served as outdoor tables in the plaza are gone. In their place are jazz and rock clubs, an ice cream parlor, and various retail stores. Where, at one time, families gathered, people dined under the trees, and old, beret-clad men played chess every afternoon, there are now drug-addicts — everywhere, sprawled on the scraggly grass, slumped on benches, or accosting potential customers as they pass through the plaza. The people of the neighborhood still try to use the park, but children can only play comfortably in a few areas, side-by-side with stupefied bodies and drug-dealers. On the steps of a bank that borders the plaza, the druggies gather every night after closing. All over the steps lie bodies, some with needles hanging out of their arms, others clutching bottles of wine. Young boys sit motionless for hours, their heads hanging between their knees. In the morning, the steps are literally covered with needles, broken glass, razor blades, and vomit. A man hoses the steps clean and business goes on as usual. Next to the bank is the historic Villa Rosa, the café cantante (forerunner of today's tablaos) that was the site of so much important flamenco in the first half of this century. It fell into disrepair in the early 1960's, was reopened as a tablao in 1964, and has now become a sevillanas bar — a sad fate for what was once such a prestigious establishment, but, at least it remains intact and somewhat on the fringe of flamenco. No such luck with Los Gabrieles nearby on Calle Echegaray. Los Gabrieles was competition for Villa Rosa and esteemed for the magnificent tilework that covered the inside and the outer facade, as well as for its special fiesta rooms upstairs and downstairs. For some reason, all of the outside tiles were recently chiseled off, leaving bare stone. The inside fortunately remains, but the bar has become a trendy student hangout that plays blaring new-wave rock music. The juerga rooms are now used for storage and I couldn't get access to them. I found a place to stay above the Villa Rosa, where greats like Ramón Montoya and Antonio Chacón used to perform for nobility and where Manolo de Huelva lived during the last years of his life (at least, during the brief period when he was playing in the tablao La Zambra in the 1960's). I felt a sense of reverence to be there, but it was no fun trying to rest when the sounds of the night echoed up from the Callejón del Gato, making sleep almost impossible. (Sle\n\n[ENDING CONTEXT]\n\ntrain. It appeared to me that Jerez has not yet been hit by the rush to modernize and the exploitation of real estate investors and big business. I'm sure it is happening, but it is not as apparent as in Sevilla. There is still a great deal of the past left for us to experience. Hopefully, with the awareness that seem to be awakening in Spain concerning the value of its historic buildings, Jere will stand a better chance of surviving than did Sevilla. I think Jerez will be the next (and maybe last) major center of flamenco activity. I know that's where I'm going from now on. Barrio San Miguel\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "SPANISH WANDERINGS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "poem",
    "pages": "15-24",
    "page_number": 15,
    "word_count": 5148,
    "article_char_count_full": 28802,
    "article_char_count_review": 4457,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "clubs"
      }
    ]
  },
  {
    "article_id": "JALEO_1990_04::A7",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[Selected and translated by Paco Sevilla] \"You can't teach a non-Spanish person to sing flamenco. You have to grow up with the language, with the form; it comes from outside... The people themselves have a particular quality in the voice which is just like an accent, which you learn to speak as a child.\" —Paco Peña, Classical Guitar, Sept. 1988 \"Everyone is crazy about the latest novelty [in flamenco], without thinking about the treasure that makes flamenca different from every other music — feeling!... Looking for new and more complicated melodies has never been the way to more profundity and feeling... \"Flamenco fusion is done with making money in mind; that which sounds familiar is swallowed more easily. Flamenco is adapted to the masses, when the opposite should be true — the masses\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Many\"]\n\n], without thinking about the treasure that makes flamenca different from every other music — feeling!... Looking for new and more complicated melodies has never been the way to more profundity and feeling... \"Flamenco fusion is done with making money in mind; that which sounds familiar is swallowed more easily. Flamenco is adapted to the masses, when the opposite should be true — the masses should be educated so that flamenco can reach them... \"Many American aficionados know how to mark compás por bulerías. Here [in Andalucía], 99% don't know how and will never learn.\" —Pedro Bacán, El País, Dec. 4, 1988 “Granada, land of good guitarists and bad cantaores...the cante has been destroyed [hecho polvo]...[the aficionados] pay more attention to the letras [words] than the cante...” —José Carmona (patriarch of the \"Habichuelas\"), El País, Dec. 4, 1988 \"Today there are many people who can devour the guitar, who play phenomenally. But, when\n\n[ENDING CONTEXT]\n\nthere are differences [between the gypsy dance and the non-gypsy dance]! The gypsy is not as well preparad as the non-gypsy, has less schooling, which causes the dance to develop on the basis of the stimulus of the moment, intuition. It is more salvaje [wild, primitive, savage] and more influenced by temperament than by studied pos ares. That is not to say that one is better than the other, just that they are different. However, the dance that dominates today is the non-gypsy, that of much preparation and study. If we gypsies, who are a privileged race, worried as much as they do, it would\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FROM THE MOUTHS OF THE ARTISTS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "25",
    "page_number": 25,
    "word_count": 1244,
    "article_char_count_full": 6986,
    "article_char_count_review": 2570,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_04",
        "family": "COMM",
        "trigger": "Many"
      }
    ]
  },
  {
    "article_id": "JALEO_1990_04::A9",
    "article_text_for_review": "INTERPRETING THE CANTE IN FLAMENCO DANCE PART I Flamenco singing is the soul-voice of the total art of flamenco. Long before there was the music of the flamenco guitar, the flamenco voice was crying out the feelings, the joys, the sorrows, the inner and outer life's happenings of the people of flamenco. Who knows when the dance and song became intertwined. We can only assume that the inspiration and need to dance — to move and be moved by this incredible singing — sprang forth together as twins born of the mixed blood of the many cultures that have made Southern Spain their home. We often hear the words to describe flamenco singing as either \"cante hondo, cante grande or cante chico\". This is a very limited way to describe the many interpretations of the cante. The categories in reality are neither all black or all white but many shades of gray, as flamenco singing expresses the endless variety of emotions that we, as human beings, can experience. It is into this endless variety of feelings and interpretations of the singing that the dancer blends. Ever since flamenco became a performing art in cafe cantantes, tablaos, concerts and festivals it has evolved and expanded, with many new dance styles. Most of these styles and forms were, previously, only meant to be sung. ( .) There are basically three styles or ways of hearing the cante. First there are the free forms such as the granamas, the fandangos grandes, the tarantas, the many other forms of free fandangos, the many forms of palo seco or songs without guitar accompaniment. Many of these forms are acquiring danceable compás and I will discuss this in a future article. Secondly, there is the style of singing that is a solo type performance. The singer is singing with his or her own flavor and interpretation, with all of the freedom of the solo cante. This is much like the solo guitarist who has the freedom of flexing and flowing in and out of the compás to add drama, interpretation or whatever else that will make it his own. At times, the compás is almost secondary, as the guitarist is not performing with a dancer and is following the singers \"stampa\" or personal feeling and expression. The third form is the singing with the dance. This is the total trilogy of music, song and dance that makes up the foundation of the flamenco art. The singer has to be sensitive to the compás, the form, the musicality of the particular form that is being performed. The latter form is the one the dancer must be sensitive to and involved with. From day one of a dancers study, it should be stressed that it is dancing, not only the compás, the music of guitar and all of the dance technique but it is to dance with the cante, and to interpret the cante to its fullest, that is of prime importance. Without this awareness, knowledge and sensitivity a dancer will never bare the whole fruit of the art of flamenco. What is a dancer interpreting when working with a singer? What is the total picture when a singer, dancer and guitarist get together? What should a dancer know, feel, think of, do, express, emote, etc.? There are many answers to these questions but the primary and most important factor is that all of the artists — singer, dancer and guitarist — honestly interpret the feeling and tradition of the form itself. An alegrias is an alegrias and all should be sensitive to that fact — it should be interpreted as an alegrias. A dancer should know the structure of whatever toque is being performed just as well as the singer and guitarist. When a cantaor starts to sing that particular compás that the dancer will dance, the singer is locked into that compás as much as the guitarist and dancer and the dancer must think and feel just as the singer does. Let me give a few examples: I have seen dancers do a set routine of steps that are using the singer as music. They have steps that fit the compás; they are moving around in dance and the moves are in compás but they do not \"say anything\" because they are dancing steps and not being sensitive to the nuance and expression of the cante. A dancer need not know the words of a letra and in fact the words can be very secondary to the basic interpretation of the cante (just as the \"blues\" are interpreted with feeling first and words second). The shaping of the dance steps are in the way that the singer interprets the cante — the accents, the sound, the phrasing, the maice, the dynamics and the tradition. This is what many dancers miss as they get into their own titillation of steps or routine. To give another example: The music begins... the singer begins to sing, perhaps a solo letra... the dancer listens, feels, senses the feeling of the music and cante blending as one,. The dancer begins to dance, not consciously thinking of steps or the cante, but letting the cante move him or her to \"move into the cante\". The cante is now shaping the dance and dancer in feeling and expression. Flamenco, being an improvised art form within the various compases, means that there are never two interpretations alike. The dancer is dancing as if this is the first and last dance that she or he will ever do. This is also what is happening to the singer and guitarist. The dancers whole body is being moved, shaped and accented by the interpretation of the cante and guitar. This is what it is to \"become the dance\" — as you, the dancer, are also singing within (not words) but the feeling and soul of the cante. You do not have to think of steps. The steps happen in complete harmony with the soul and spirit of the cante if you stay sensitive to it and have a good knowledge of your basic flamenco dance technique and style. --Teodoro Morca",
    "title": "MORCA: SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1990_04",
    "year": 1990,
    "language": "en",
    "article_type": "other",
    "pages": "26,39",
    "page_number": 26,
    "word_count": 1023,
    "article_char_count_full": 5681,
    "article_char_count_review": 5681,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
