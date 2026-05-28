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
    "article_id": "JALEO_1981_02::A1",
    "article_text_for_review": "I SEMANA DE ESTUDIOS FLAMENCOS (MALAGA) *** 1963 *** (from; $ \\underline{\\text{LAE}} $, 1963; sent by Marilyn Bishop; translated by Paco Sevilla.) by Jesus Hermida (photos: Francisco Ontanon) The \"I Semana de Estudios Flamencos\"(the First Week of Flamenco Studies) has been celebrated in Málaga. This was a good thing. With your permission, let me say that all is worthwhile when it comes to studying that incredible thing, that profound, jondo thing that racial thing that God has given us \"porque si!\" (just because) In Málaga there have been many learned, wise, and emotional things about flamenco. And many people heard them with devotion and great feeling. This is good! In Málaga there were facts and poetry, investigation and intuition. In Málaga, senões -- not senhoritos -- spoke about the cante and the baile; all spoke with science, knowledge and depth: Francisco Bejarano, president of the Peña Juan Breva, José and Jesús de las Cuevas, alternating in their lectures, back and forth like a \"polo\" and a \"caña\" mixed together, Rafael León, Ricardo Molina who knows the most, Jose Manuel Caballero Donald, and Edgar Neville, he who struck home most often. In Málaga it was a week filled with cante and baile. It was organized by Coca-Cola, a drink that would like to become flamenco... In Málaga there was endless pure air to breathe. To tell the truth, I would have to talk until I became red in the face if I were to tell all that happened and was said. At one or two o'clock -- in the morning, you understand -- while passing through the Pasaje de Chinitas and then in front of the Cathedral, I was asking Fernando Quinones (who, besides writing stories like nobody else can and being an Andalucian and a \"gaditano fino\" like nobody else, won top honors in the \"Semana de Estudios\" for his Manual de los Cantes de Cádiz) what method I should choose to explain to you -- who will include the wise and knowledgeable, no doubt -- something about flamenco that would not be an erudite lecture nor a cliche \"por bulerías.\" LA FERNANDA AND LA BERNARDA DE UTRERA \"Bueno,\" pronounced Quiñones, as he tasted a rich, clear wine, \"you go to the fiesta tomorrow night, you put yourself into the guitar and the throat of the singer, you die with the anguish of a siguiriya, and later you tell it to the uneducated...\" EL NINO DE CANILLAS AND ANTONIO VARGAS A voice at my side, a tactless and hoarse voice, speaks without being asked, \"Vamos a ver...\" (We shall see...). We $ \\underline{\\text{are}} $ seeing! Appetizers of the juerga. A song to whet the appetites. To Málaga: \"Estando cortando piña en er piná der amó, der tronco sartó una astilla y se clavó en mi corasón. Muerto estoy; llórame niña\" (While cutting pinecones in the pine tree of love, from the trunk flew a splinter and pierced my heart. Now I am dead; cry for me, girl)",
    "title": "CANTE JONDO O ABRIRSE LAS CARNES CANTANDO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "3,6",
    "page_number": 3,
    "word_count": 497,
    "article_char_count_full": 2837,
    "article_char_count_review": 2837,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_02::A2",
    "article_text_for_review": "Dear Jaleo: I enjoy $ \\underline{\\text{Jaleo}} $ and I read it from cover to cover every month, but some of the ideas and attitudes expressed in Paco Sevilla's editorial in the December issue bothered me. I don't agree that flamenco is a strictly traditional, almost provincial art form rooted among a certain society in a particular part of the Iberian peninsula. Flamenco thrives all over the world now, and some of the great flamenco artists do not live in Spain anymore. We should be open to the idea of flamenco happening anywhere. I also don't believe that there is much to be learned from reviews of flamenco festivals and contests. I read those articles; they are fun, but they are just gossip. There's nothing in them that would help you play a note or dance a step. The flamenco quiz bothered me, too. The editorial states \"...the true 'cabal' in flamenco will have a knowledge that is much more extensive than that required here.\" If that means cafe habitués in Andalucía would know all sixty names in Part I of the test, then I agree. But I don't agree that name dropping is equal to knowledge of flamenco. All these things add up to chauvinism, that \"in group/out group\" mentality that seems to collect around every art form, especially those imported from one culture to another. It would be ironic and a great loss if $ \\underline{\\text{Jaleo}} $, which is dedicated to spreading information and interest in flamenco, should become elitist. Respectfully, David J. Wolf Monterey, CA (Editor's comment: I think we all hope that Jaleo can avoid becoming elitist and continue to serve all extremes of flamenco interest. Aside from that, I don't agree with any of your statements or your interpretations of my statements. But I $ \\underline{\\text{do}} $ appreciate your taking time to express your views. There is much potential for valuable discussion in these subjects and perhaps other readers will provide further input.) Dear Jaleo, My name was Bill Regan. I was a mixture of Yugoslav, Irish and German. Now I'm Guillermo Salazar! My father is from Valladolid, and our family speaks Castillian. Shortly after the name change I got a guarantee check card under the new name, and later got checks also. The original idea was to try an experiment for a year. Within a month business tripled at my language school. I began to get standing ovations at my occasional concert appearances. I had a good thing going and wasn't about to lose it. I've had to make a few changes in my outlook during the period of adjustment. The first change was to be more tolerant of other people, more accepting of their weaknesses. I am no longer angered by fortune-tellers, tarot readers, actors, or children. I realize that I almost believe that the end justifies the means; but I thought I could try this philosophy for a while, because I do have a good product--the bottom line. So, this \"mentira\" is a sincere one. I am an authentic fake, if not genuine. Life now is more interesting and business is better. I am planning a new album to show my progress on the guitar. My new friends, who eventually find out about me, seem to take it all with a grain of salt. I think I'll be Guillermo Salazar for a while longer. Guillermo Salazar Denver, Colorado Guillermo: So that you won't feel alone, here are some other people who have benefitted from name changes (a similar list could be made for Spanish as well as American flamenco artists): William Claude Dunkenfield (W.C.Fields) Allen Stewart Konigsberg (Woody Allen), Norma Jean Baker Marilyn Monroe), Issur Danielovitch Demsky (Kirk Douglas), Benjamin Kubelsky (Jack Benny), Nathan Birnbaum (George Burns), Margarita Carmen Cansino (Rita Hayworth), Sophia Scicoloni (Sophia Loren), Archibald Leach (Cary Grant), Joseph Levitch (Jerry Lewis), Joe Yule (Mickey Rooney), Rodolpho d'Antonguolla (Rudolph Valentino), Jill Oppenheim (Jill St. John), Marion Michael Morrison (John Wayne), Samuel Goldfish (Samuel Goldwyn), Dino Crocetti (Dean Martin), Leonard Slye (Roy Rogers), Dear Jaleo Well, Philadelphia had it's first big organized juerga in a long time, Monday November 10th. It took place at the La Meson Don Quijote. As the restaurant is closed on Mondays, the gracious owners Julia and Enrique Lopez allowed the event to take place there. A fun filled night was enjoyed by a large crowd, and it all came about on very short notice. I want to express many thanks to $ \\underline{\\text{Jaleo}} $ for being the great inspirational force that you are. Yours truly, Dan Di Bona Philadelphia, PA",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 763,
    "article_char_count_full": 4538,
    "article_char_count_review": 4538,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_02::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nAn unsolicited review sent to Antonio David. Durante más de tres décadas de conciertos he utilizado y recomendado muchos tipos de cuerdas para guitarra, pero nunca obtuve tanta satisfacción como la que me proporciona el utilizar las cuerdas FLAMENCO SUPREME. Desde hace un año uso solamente sus cuerdas y me complazco en informarle que son las que más se ajustan a las características de la guitarra. Su tono es de una gran brillantez y fuerza, y su tensión es perfecta, bien equilibrada. Pero lo que realmente más cabe destacar son las tiples, cuya entonación es mucho más perfecta que la de cualquier otra que he utilizado antes. Y, por si estas cualidades no fueran suficientes, debo anadir que estas cuerdas retienen su brillo por un período de tiempo muy superior al de cualquier otro tipo de\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"gran\"]\n\nmás cabe destacar son las tiples, cuya entonación es mucho más perfecta que la de cualquier otra que he utilizado antes. Y, por si estas cualidades no fueran suficientes, debo anadir que estas cuerdas retienen su brillo por un período de tiempo muy superior al de cualquier otro tipo de cuerda que yo haya utilizado. Por estos motivos, y aunque no se haya solicitado mi opinión, me satisface recomendar las cuerdas para guitarra FLAMENCO SUPREME con gran entusiasmo. Agustin Castellón Agustín Castellón (Sabicas) (translation) During my more than three decades of concert performance I've used and endorsed many guitar strings, but have never received such satisfaction from any as I have from using FLAMENCO SUPREME strings. For the past year I've used your strings exclusively and I'm happy to inform you that they are most suited to the characteristics of the guitar. Their tone is brilliant and powerful, and their tension is perfect, well balanced. But most outstanding are the trebles which had the most perfect intonation of any that I've ever used. And, as if these features were not enough, I must add that these strings retain their brilliance far longer than any I've ever played. It is for these reasons that I offer you my most enthusiastic and indeed unsolicited endorsement of FLAMENCO SUPREME guitar strings. (ARCHIVO - ) cantaor. We remembe\n\n[ENDING CONTEXT]\n\nfloor with a great shudder and rolled in the grease -- in ecstasy and filled with duende... This happened in Andalucía, Spain. And it still happens. And he who wants it, has it. And he who feels it, feels it: \"Aunque el mundo me critique, yo te seguir queriendo...\" (Even if all the world criticises me, I will continue to love you...) Cante jondo. Uy, madre mía huelvana... what a serious thing... I wanted to say that, in Malaga, there were some very learned things said about flamenco. Rubina Carmona Instruction in Cante and Baile Flamenco Personal Costume Design (213) 660-9059 Los Angeles, Ca.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "FLAMENCO SUPREMES: A REVIEW",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "5-10",
    "page_number": 5,
    "word_count": 1593,
    "article_char_count_full": 9169,
    "article_char_count_review": 2982,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "gran"
      }
    ]
  },
  {
    "article_id": "JALEO_1981_02::A4",
    "article_text_for_review": "Rhythm is that magical something that is everywhere in life. It is the pulse of the universe, the rhythm of the stars, the planets going around the sun, the heartbeat of our bodies, the visual rhythms of the ocean waves, the rhythm of daily walking, or a cricket chirping or the rhythm of an eagle's wings beating the stratosphere. We are literally surrounded by rhythm all of our lives, both visually and aurally, whether from nature itself or the music of our own making. This total palette of rhythm and music in nature has been the inspiration for man from the beginning of time, inspiration to create his own music and his own dance, which were forms of communication, even before there was a spoken language. Flamenco was born among peoples who lived close to nature and used nature as a source of inspiration for the music that grew from their feelings and emotions. From a simple beating of a stick to produce a rhythm to move to, to the anguished cry of a pre-flamenco song expressing hunger or loss, to the complex rhythms of today's many flamenco styles, the evolution has brought flamenco music to its complete blend of many musical elements. The music of the guitar, the song of the singer, the audible rhythms and visual movements of the dancer, the complex jaleo of the complete flamenco experience. It is this approach of flamenco music in its entirety that I speak of, this listening to flamenco in its totality that will reveal its complete majesty. The more one listens to flamenco, the more one hears. At first, you might just listen to the rhythms. This is important, to absorb the different rhythms, their structures and their pulses. Of course, when there is no live flamenco, then good tapes and records are the way to learn what flamenco sounds are like, and there are still many records available, old and new. There is nothing like listening and listening to soak up flamenco like a recorder. Back to the rhythms. When one takes up flamenco dancing, one of the first things to begin learning is the structure of the different rhythms. Since there is no written music in flamenco, you must train your ear to hear the \"frame\" of the compás or 4 or 6 count compás. Once you understand where the compás begins and ends, then you can start to \"feel\" the base accents of which there are many. Again, I say that the more one listens and absorbs, the better it is and the more you understand without counting. This is very important because this is one musical form in which it is best to try not to count once you know the compás, because it will inhibit freedom of expression later on. Counting is very good and important to know, but once understood, best forgotten. Trust your inner ear, your feel within so that, when you dance, you will be listening to the total music and dancing with it, not counting it and being late or early. Counting can become a crutch and hinder movement and true expression. For example, too many get caught up with the base accents of 3,6,8,10,12, in a 12 count compás and if anything deviates from that, they are thrown. You do not want to create a mentronome out of the base accents, but use them to spring from and create from.",
    "title": "MORCA SOBRE EL BAILE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "11",
    "page_number": 11,
    "word_count": 570,
    "article_char_count_full": 3182,
    "article_char_count_review": 3182,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1981_02::A5",
    "article_text_for_review": "FLAMENCO FOR THE SAKE OF FLAMENCO Quite a few people have asked me if flamenco is a scam that I use to attract women. The first time I heard this from a friend of mine, I was very surprised, since I really never had thought of this. It is true that people who are in the public's eye are attractive. They are getting attention, and have an aura of importance. Many people see this as the most important part of all. Another friend once said to me, \"If I could play the guitar like you, I would have at least twenty girlfriends.\" This mentality seems to want to use flamenco as a stepping stone to popularity. In reality, any art form can be used this way by people who have the art form itself low on the list of priorities. Let's face it, it is extremely difficult to excel in flamenco. Flamenco excellence, on the other hand, is a kind of excellence that is generally not appreciated by the masses. The popularity received is not directly proportional to the hours of dedication. The true artist is not going to get involved in this type of situation, when he or she could find an easier way to obtain popularity. Those who do get the popularity through flamenco receive it as a by-product, rather than a conscious effort. However, it is true that some fine artists lose interest in their art and get side-tracked into enjoying popularity. Inevitably the artistic ability seems to deteriorate. Just as there are natural athletes, there are natural flamencos. Others have to work harder at it to attain their top form. The worst thing is seeing a natural performer who has lost form, either through lack of practice or ruined by success, and living on the reputation of past glories. Natural flamencos have to practice, too. If you make a full or partial income from flamenco, you have to present yourself in front of other people. By its very nature flamenco is ostentatious, or showy. The audiences expect it, except for initiated aficionados, who are looking for duende. The fact that you go on stage does not necessarily mean that you are on a big ego trip. Don't forget that if you don't \"Falsetas por Bulerías\" begins the second side. This has moments of sounding like an \"al golpe\" type bulerías and other moments of observing strict twelve beats. My opinion is that he goes out of compás, or more mildly put, is playing \"para escuchar,\" as Diego del Gastor would defensively claim. \"Mi Farruca Gitana\" is a strict dance farruca, well done except for the ragged tremolo. \"Alegrias de Cádiz\" is my favorite piece on the record. It's an alegrias por rosa, supposedly given birth to by Ramón Montoya. The serrana is called \"Por la Sierra de Córdoba.\" It has a metronome like regularity, more reminiscent of baile than cante flamenco. \"Guajiras Cubanas\" is a nice showpiece, the falsetas being played \"a cuerda pelada\" or one string at a time, for the most part. The album ends with \"Tientos y Marianas,\" the only piece that favors the cante. Luis doesn't seem as sharp technically on this record as he does on some earlier ones. This is a good sample of his playing style, and I'd recommend getting it, if still available. --Guillermo Salazar «Candela» A RECORD REVIEW by Gordon Booth (Editor's note: For the last six months, Jaleo has benefitted from the contributions of Gordon Booth. Living in Andalucía, he has sent us considerable material that has allowed us to be better informed about flamenco in Spain. Unfortunately, Gordon has had to return to the United States. We thank him for his efforts and hope he will find some way to continue to be involved.) Wander through the narrow streets of the town which shaped Manolo Sanlúcar and his music, Sanlúcar de Barrameda, full of sun and shadow, past the silent bodegas where the young manzanilla is quietly dancing its way from barrel to bottle, to the beach where fishermen are setting out their nets to dry beside the timeless Guadalquivir.",
    "title": "GAZPACHO DE GUILLERMO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1981_02",
    "year": 1981,
    "language": "en",
    "article_type": "other",
    "pages": "12,19",
    "page_number": 12,
    "word_count": 685,
    "article_char_count_full": 3902,
    "article_char_count_review": 3902,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
