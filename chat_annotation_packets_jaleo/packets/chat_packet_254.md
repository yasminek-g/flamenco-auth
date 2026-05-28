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
    "article_id": "JALEO_1987_SUMMER::A19",
    "article_text_for_review": "The revised Theater Spanish and Flamenco Dance Syllabus for 1987 has been completed and released. This syllabus was first compiled in 1985 by the Spanish-Flamenco Committee, under the leadership of Jimmie Crowell, of Oklahoma City in Oklahoma. The revised edition is much larger, more complete, and makes a place for itself as a valuable reference work as well as being entertaining reading. A big part of the syllabus is made up of notated dance steps, with contributions by people such as Teodoro Morca and Maria Benítez. However, there are many other sections, a number of them taken from faleo magazine: There are sections on jaleo, palmas, castanets, where to buy costumes, castanets, records, etc., and a large and quite complete list of definitions of flamenco and dance terms. As I said, it makes for interesting reading for any aficionado of flamenco. I have only one critical suggestion to make: In the next revision, please have a careful proofreading done by someone who knows Spanish and flamenco; There are many typos and minor errors (poor Teodora Morca -- he gave so much and became a \"she\"). Copies can be ordered by sending $7.50 to: Dance Masters of America, Inc, PO Box 117, Wanchula, FL 33873 --- THE FRAME STATION The Finest in Custom Picture Framing 20% DISCDUNT TO ALL MEMBERS OF JALEISTAS 1011 FORT STOCKTON DRIVE SAN DIEGD, CALIFORNIA OWNER TOM SANDLER (714) 298-8558 (Hillcrest/Mission Hills area) ANNOUNCEMENTS",
    "title": "SPANISH AND FLAMENCO DANCE SYLLABUS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1987_SUMMER",
    "year": 1987,
    "language": "en",
    "article_type": "other",
    "pages": "48",
    "page_number": 48,
    "word_count": 237,
    "article_char_count_full": 1438,
    "article_char_count_review": 1438,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_WINTER::A1",
    "article_text_for_review": "SONGS OF THE EARTH (OR WHERE THE HECK IS THE FLAMENCO?) ...and in his brain, Which is as dry as the remainder biscuit After a voyage, he doth strange places cramm'd With observation, the which he vents in mangled forms. At a first glance, some of these \"cuentos\" may seem to lack a central \"flamenco\" theme, and the reader may wonder where the punch-line is. After all, you might say, Jaleo is first and foremost a serious, flamenco-oriented publication, and shouldn't become a half-witted dodge-podge of travel brochures, kitchen recipes, and crash-course in Andalucian slang. The answer is that in Andalucía, flamenco is everywhere. It is a way of life so deeply interwoven in the very fabric of Andalucian life, that it cannot be separated from it. It is something that comes from deep within the earth, drunk along with \"finos\" (frenquently in excess), tasted in the \"aceitunas\", breathed in the dusty air, and passed along with mother's milk. The visitor who tries to pry away \"just\" the music, or the dance, may fool himself into thinking he/she learned \"flamenco\", but will miss out on the essence of things. For being a true \"flamenco\" means being first an Andaluz or Andaluza at heart. Although certainly much less pleasurable, there can be as much \"flamenco-ness\" in the incessant drone of the'mopeds' that crissc cross Andalucian roads, as there is in a flashy falseta by Paquito. Thus, some of these \"flamenco-less\" stories are Instamatic snapshots of that way of life. Accept the quality for what they are. Whether flamenco will \"disappear\", as some predict, as Spain joins the rest of Europe, and Andalucia joins the rest of Spain, I do not know. I rather think not, for no matter what happens here, there will always be a little \"flamenco\" within every one of us, wherever we are. ...AND A DISCLAIMER (OF SORTS)",
    "title": "ESTAMPAS Y CUENTOS DE ANDALUCIA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 310,
    "article_char_count_full": 1826,
    "article_char_count_review": 1826,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_WINTER::A2",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nNEW ARTISTIC DIRECTOR FOR THEATER FLAMENCO Dear Editor, I would like to take this opportunity to announce that Mr. Miguel Santos has been named artistic director of Theater Flamenco of San Francisco succeeding Ms. Dini Roman, who will be returning to her native Boston after serving as artistic director for three years. Miguel Santos has been with the company since shortly after it was founded by Adela Clara in 1967. We warmly thank Ms. Roman for her unending dedication and artistic contribution and look forward to the promising leadership and artistic direction under Miguel Santos. Rosa Aguilar San Francisco, CA *** 'GYPSY GENIUS' HISTORIC - EXCLUSIVE VIDEO RELEASE BY MANUEL AGUJETAS DE JEREZ For the first time in flamenco history, the legendary Manuel Agujetas de Jerez performs on video\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_01 | trigger=\"maestro\"]\n\nfter it was founded by Adela Clara in 1967. We warmly thank Ms. Roman for her unending dedication and artistic contribution and look forward to the promising leadership and artistic direction under Miguel Santos. Rosa Aguilar San Francisco, CA *** 'GYPSY GENIUS' HISTORIC - EXCLUSIVE VIDEO RELEASE BY MANUEL AGUJETAS DE JEREZ For the first time in flamenco history, the legendary Manuel Agujetas de Jerez performs on video cassette. The world famous maestro of the Jerez dynasty of gypsy flamenco singing gives an historic performance that will remain forever. Beautiful cantes por Soleá, Fandango Grande, Siguiryas, Malagueñas, Romeras, Taranto, Tientos, Bulerías. Length - 90 minutes in color. This video features the special collaboration and original guitar accompaniments of recording and concert artist RODRIGO. Don't miss out on this first world release as it is a collector's item. No studio video of this kind has ever been made. Order Beta or VHS. Only $49.00. Send cash, check or money order to Alejandrina Hollman, 148 Taft Ave. #11, El Cajon, CA 92020. The performance took place on August 5, 1985. An educational \"must\" for guitarists and singers. Allow 3 to 4 weeks for delivery. We Appreciate Our Advertisers Please Patronize Them <table><tr><td>The Blue Guitar</td><td>16</td></tr><tr><td>Magdalena Cardoso - Escuela de Danza</td><td>7</td></tr><tr><td>A. Casillas - Flamenco Guitar</td><td>33</td></tr><tr><td>Chula Vista Travel</td><td>32</td></tr><tr><td>Antonio David - Supreme Strings</td><td>11</td></tr><tr><td>A. Fauc\n\n[ENDING CONTEXT]\n\nin small coastal town where he settled, he is easily recognized by a much larger than average 6'4\", 260 lb frame and tousled red hair. Has taken up with younger girl who would like to marry him and have his child. Has not told her of his vasectomy, done twenty years ago after the birth of his last daughter who recently made him a grandfather. Has no plans to return to the US, although he might consider it if the \"right\" job came along. MIDNIGHT TALK During the Andalucian summer, nights seem to just go on and on. We have stopped at a \"venta\" in the outskirts of town for a nightcap after the\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "poem",
    "pages": "4-6",
    "page_number": 4,
    "word_count": 2756,
    "article_char_count_full": 16728,
    "article_char_count_review": 3167,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_01",
        "family": "PED",
        "trigger": "maestro"
      }
    ]
  },
  {
    "article_id": "JALEO_1986_WINTER::A3",
    "article_text_for_review": "[from: $ \\underline{\\text{Cambio 16}} $, June 6, 1986 sent by Marysol West, translated by Paco Sevilla] by Lola Díaz Manuela Vargas, bailaora from Sevilla, was born forty-five years ago in the barrio of La Alameda, the center of flamenco in Sevilla at that time. She grew up surrounded by great flamenco artists and learned the art of dance and life without losing rhythm, that is, with verve and authenticity. Are you a bailarina or a bailora? \"I am a bailaora.\" What is the difference? \"A bailaora dances always to guitar palmas pitos and with the voice of the cante.\" When you dance you seem to be in a trance. \"Of course! I have a dance that I enter into, a sort of personal trance. Perhaps this is not understood by those people who try to destroy me and say that I do it because I am showing off, but they are completely wrong, because my body suffers, and when a body suffers it is because life is hitting you hard.\" In what sense? \"Well, I, you could say, have a spirit that makes me do flamenco from deep within and, therefore, even though I want to get it out through my body, with my feet, and by trying to do many movements with my arms, I am unable to do it. It's as though it takes over my body, as if paralyzed one in the same and I have to JALEO - VOLUME IX, No. 4 \"Of course. With life and the fight for my dance, I now don't enter through the same door.\" And now you are also among the best dressed and most elegant women in this country... \"Yes, but when I was thirteen I was a ragged little girl, thin and pretty ugly, although I was like a wild animal, a young colt, Then, later, they began to say how well the flamenco dress suited me, how elegant it looked. I don't know if you have it inside you or not, but I didn't\" Is it important for a bailaora like you to be in the Ballet Nacional? \"I was in it for three years. What I didn't want was to be where I was when I was young, in those tablaos to provide amusement for people with a whiskey in their hands and no appreciation for you at all.\" Are you a gypsy? \"On my mother's side.\" Do you feel like a paya or a gypsy? \"I don't feel one or the other. I grew up with gypsies and I am comfortable around them, in spite of the fact that they are difficult and will sometimes disappoint you. But sometimes you even like that about them.\" Why do you like it when they disappoint you? \"Because they are proud, sensitive people and it is wonderful when they offend you, because they do it without realizing it, maybe just because they lack education. But I have never been influenced by racial factors, because, in Sevilla and some other parts of Andalucía, the gypsy is just like anybody else, and not like they say now -- that gypsies are like this or that. At least in the barrio of La Alameda, everybody was equal, some darker, some lighter, and even some who were very white.\" Can you tell a gypsy at first sight? \"I see it immediately, even if they don't sing, dance, or have dark skin!\" How do you tell? \"Pues, I don't know... Maybe their way of looking, The gypsies have a sad, yet proud look in their eyes.\" When you dance on stage are you aware of the audience? \"When I am on stage I see nothing. Now, for example, in the Teatro Monumental, which is huge, it is as if I were in a small box, isolated by the strange atmosphere that the lights produce. Sometimes I say, 'what about the audience?' But, even if they applaud me, I have to be told, because I hear nothing.\" How is that possible? \"I don't know. I am enclosed in my little world and, during those two hours that I may be up there expressing or transmitting something. It is as if I were floating. And, imagine, here I have one bruise and here another; my feet are so swollen that I can't even get my shoes on. But when I go out there I forget everything. It is very beautiful to have that opportunity to live floating for several hours within this constant fighting and suffering that we call life.\" I believe that your present love is a Latin American, as was your ex-husband... \"He is from Uruguay. I met Damian the year I was separated, but he went to live in Switzerland and I didn't see him again until he returned last year.\" Do you prefer Latin Americans? \"I don't know if I have a predilection, but it is true that they have",
    "title": "INTERVIEW WITH MANUELA VARGAS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 819,
    "article_char_count_full": 4269,
    "article_char_count_review": 4269,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1986_WINTER::A4",
    "article_text_for_review": "by Marina Granich There's no one better qualified to give us a rundown on flamenco than Teodoro Morca. Teo, as he likes to be called, believes flamenco has something to offer everyone. He's spent 35 years of his life involved in the art, as an internationally-known flamenco dancer, choreographer and teacher. Los Angles-born, Teo is living proof that you don't have to be born in Barcelona to dance flamenco. He's appeared with many of the major flamenco groups in Spain and has toured Europe and America with his own dance company. Besides receiving the prestigious St. Denis Choreographic Award (1982) in the States, he has been given the Gold Choreographic Award in Spain, as well as various fellowships from the National Endowment for the Arts. He also is a Commissioner for the Washington State Arts Commission. Many of us have seen flamenco, at one time or other, but can you tell us exactly what is flamenco? \"Basically, flamenco is the art, the song, the way of life of the people of Southern Spain, a melting pot of people...very much like the people in the United States in that blend. It's an art form relating to that part of life, very much like our jazz and blues. It was born with people expressing their feelings and emotions, their joys and sorrows, comedies, tragedies, every facet of their life through this art form.\" \"It's an interesting mixture of Eastern and Western cultures. The hands, the upper body, the styling of the dance was very influenced by East Indian dance, Arabic dance.\" \"It's a dance from that is thousands of years old, although the way we know it, it is just a few hundred years old, as far as a performing art. It's an audible form just like our tap dancing, expressing rhythms of life. It's definitely an art form of an oppressed people, that's why it has a built-in drama and emotion.\" What attracts people to flamenco? \"The basic humanness, feeling, drama, emotion. We're all emotional people, we're all dramatic. This flamenco taps something in every person. If you see good flamenco, you see someone becoming themselves in dance, music, song. It's a real immediate thing...It's the immediacy of feeling that really attracts people...\" What can flamenco do for your body? \"Flamenco is very isometric in the sense that it's energy. It's also aerobic. Very few dance forms work the upper body like flamenco and the lower body, as far as that goes. It has such a built-in isometric tension. It works against gravity. That's why it's great for the whole body...Gravity pulls our organs down and my philosophy is that gravity, if you give in to it, has a lot to do with aging. This is where flamenco comes in with that total pull-up, that carriage...you lift the whole body off the ground.\" \"When I was teaching in Los Angeles, I was teaching a bunch of women...some of them were models. One of them told me after she'd had her child, she'd been trying to get her breasts back to their old shape. She noticed that, thanks to the working of the posture and especially the arms and back in flamenco, her breasts were firming up and getting back their uplift.\" \"The whole tension of the way the knees are bent in a unique part of flamenco...We all admire the tight tush, so to speak of flamenco dancer...There's nothing like the stomping of flamenco for cellulite removal...Flamenco deals with parts of the body we very seldom use. The arms being up in those positions does fantastic things for the posture. You end up with so much more class and style.\" Specifically, how does flamenco help posture? You mention posture a lot. \"The flamenco posture is natural. We've lost that. You can see good posture in many so-called primitive people who walk around carring things on their head...If you look at a skeleton, you notice a plump line that goes right through the spine through the center of the hips and down. This is flamenco posture. It's nothing more than getting back to that healthy centered posture, which I feel is one of the secrets of good health. Most of us are slumped, then the whole body goes to pot. You're out of balance.\" How does flamenco rank with other forms of movement or dance as an exercise? \"It ranks very high if done with a basic system of strengthening and stretching, beforehand. Flamenco has that total isometric energy, that cat-like quality that can develop beautiful posture. It's an aesthetic exercise. You can carry flamenco into your daily life... A lot of people jog and stuff, but it doesn't really improve their posture. They're working out in aerobic terms, but it really doesn't improve their looks in the sence of how they carry and present themselves.\" \"Since there's not a lot of leaping and jumping, I think there's less stress on the body than, say, in classical ballet, over a longer period of time. You",
    "title": "YOU...A FLAMENCO DANCER",
    "periodical": "jaleo",
    "issue_id": "JALEO_1986_WINTER",
    "year": 1986,
    "language": "en",
    "article_type": "other",
    "pages": "9",
    "page_number": 9,
    "word_count": 833,
    "article_char_count_full": 4790,
    "article_char_count_review": 4790,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
