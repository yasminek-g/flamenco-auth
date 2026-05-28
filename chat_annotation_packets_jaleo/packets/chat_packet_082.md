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
    "article_id": "JALEO_1980_08::A16",
    "article_text_for_review": "(From: $ \\underline{\\text{Guidepost}} $, September 16, 1966) By Sally Nicholson It's not an easy task for anyone to earn acceptance in the flamenco world and if you're an American aficionado you might as well forget it. One exception to this rule is Donn Pohren, a tall yanqui from Minneapolis, Minnesota who has lived in Spain on and off for the past fourteen years. As a much-respected flamenco guitarist and the authority on the art of flamenco in the English-speaking world, Mr. Pohren's primary concern has always been \"pure\" flamenco. After many years of studying flamenco guitar Donn Pohren launched his professional career performing both with groups and as a soloist in the United States, Mexico and Spain under the artistic name of Daniel Maravilla. In 1958 he settled down for a year in the San Francisco area where he and his Spanish wife, Luise Maravilla, ran a flamenco cafe cantante, the first of its kind in the United States. But, soon homesick for the flamenco ambiente and things Spanish the Pohrens moved back to Madrid the following year when he began work on his two books, The Art of Flamenco and Lives and Legends of Flamenco, both of which have subsequently become veritable \"bibles\" to English-speaking flamencofans. So many flamenco enthusiasts began appealing to Mr. Pohren as to where they might hear flamenco puro that he began mulling over the idea of opening a flamenco center and soon started up a private flamenco club in Madrid where aficionados gathered for juergas and to practice. It wasn't until after the club had been running for almost a year that Mr. Pohren found just what he wanted in the way of a permanent home for his flamenco center. Heading down south the Pohrens discovered the perfect spot for their flamenco center at Finca \"Espartero\". Near Morón de la Frontera, it is right in the heart of flamenco country where cante and baile are not just a business but still a way of life. The house is set in a lovely landscape near the foot of a mountain on one side with a large orchard running from the house down to the Guadaira River on the other. In the orchard is a small spring-water pool for bathing as well as a fine swimming hole in the river. The house itself, originally built as a play refuge for a wealthy senhorito, is large and typically Andalusian with room for ten guests. A terrace runs around the entire length of the house cluster-with all kinds of flowers. The center's season lasts from April 1 through November 30 and the minimum stay is one week but so many requests have come in for some kind of winter schedule that the Pohrens have decided to arrange \"flamenco weekends\" which will run throughout the winter months beginning in October. All phases of flamenco from cante and baile (Luisa Maravilla, Mr. Pohren's wife, instructs the dancing) to guitar accompaniment are taught for those who would like lessons. But the highlight of all the flamenco activity at Finca \"Espartero\" are the juergas where all the oldtimers from around the region join with the guests at the center for allnight sessions of singing and dancing. These juergas are frequently held at the finca itself but just as often the visitor will find himself spending the evening with a group of flamenos at a gypsy's cave, around a campfire or at an allnight bar in nearby Triana -- anywhere, it seems, when the mood strikes. Considering that the center was opened just last April the project has proved to be a great success. It's getting so that reservations in advance are a must (for further information or reservations write to Mr. Pohren at Finca \"Espartero\", Moron de la Frontera, Sevilla). The center's international clientele seems to have been spreading the virtues of the Andalusian country",
    "title": "FINCA ESPARTERO: FLAMENCO AS A WAY OF LIFE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_08",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "19",
    "page_number": 27,
    "word_count": 643,
    "article_char_count_full": 3740,
    "article_char_count_review": 3740,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_08::A17",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nA Book Review by Paco Sevilla (A Way of Life, by Donn E. Pohren, Society of Spanish Studies, Victor Pradera, 46, Madrid-8 Spain. 191 pp.) Can flamenco survive? There is a strong feeling among many knowledgeable flamenco people today that flamenco as we know it cannot endure. Most of the conditions that gave birth to flamenco and nurtured it are gone or going. Can flamenco possibly survive as a theater art with no roots in the daily lives of people; can it continue when it is no longer a way of life? Donn Pohren is one who feels that it cannot. His new book, A Way of Life, describes flamenco as few of us will ever experience it. He takes us away from the world of theaters, chorus line dances, unvarying choreographies and musical arrangements, and high-powered technical virtuosity to show\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_02 | trigger=\"know\"]\n\ne, a way of thinking, feeling, and existing. A Way of Life is, basically, the story of Pohren's years in Morón de la Frontera (roughly, 1968-1973) and his impressions of the people and events that were associated with his flamenco center at the Finca Espartero. Those who visited the Finca will read this book with nostalgia (perhaps not always pleasant -- Donn pulls no punches in describing the guests); those of us who were aware of the Finca and know many of the people involved, but never went to Morón, will enjoy learning more about what actually occurred there; those who never heard of Donn Pohren, the Finca Espartero, or Morón de la Frontera, will enjoy reading about a unique flamenco event and will learn a great deal about the \"flamenco\" life style. Beyond the story and the detailed descriptions of some of the characters involved, there is a great deal to be learned from Donn's views on flamenco. As an example, in the preface (page 9), he says: \"We might begin this study by exploding the popular myth that flamenco is the tragic expression of an oppressed people, moving blackly across life's stage amidst great wailing and gnashing of teeth. Poets and other tragedians have successfully presented flamenco in this light, but the fact is that nothing could be further from the truth. I would estimate that within the r\n\n[ENDING CONTEXT]\n\nclear statement to an ambiguous or vacillating one. I would hope that others would look under the surface for the truths that lie in most of his observations. One thing that I feel would have improved this book considerably would have been the ommision of many of Donn's philosophical asides that are quite irrelevant to flamenco or to the flow of the writing. For example, after an interesting description of how flamenco artists seldom managed to hang on to their earnings long enough to get money home to their families, he adds a value judgement that is really unnecessary and out of place,\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "A WAY OF LIFE",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_08",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "20-21",
    "page_number": 28,
    "word_count": 1066,
    "article_char_count_full": 6263,
    "article_char_count_review": 2957,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_02",
        "family": "COMM",
        "trigger": "know"
      }
    ]
  },
  {
    "article_id": "JALEO_1980_08::A18",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nWhen Donn philosophizes about what is happening to flamenco due to the social changes in Spain, he is on firmer ground and makes a strong case. Referring to those who prefer modern flamenco, he writes, \"'Flamenco must keep up with the times' they say, when in reality the phrase should go, 'Flamenco is being destroyed by the times.'\" (pg. 76) Speaking of Anzonini and Paco de Valdepeñas he says, \"They are the last of their breed. The spirit that drove them on (and on) has nearly disappeared in this more'reasonable' age of materialism and regular hours.\" (pg.102) In the last of his explanatory notes, he sums up his message for those who have not distilled it from the rest of his writing. He finishes with, \"That the end of Spanish complacency occurred simultaneously with the end of the\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_04 | trigger=\"Many\"]\n\ndrove them on (and on) has nearly disappeared in this more'reasonable' age of materialism and regular hours.\" (pg.102) In the last of his explanatory notes, he sums up his message for those who have not distilled it from the rest of his writing. He finishes with, \"That the end of Spanish complacency occurred simultaneously with the end of the flamenco way of life is no coincidence. Both were struck down by the same inevitable causes.\" (pg. 191) Many who perform flamenco professionally (foreign or Spanish) do so without realizing their dependence on those who have lived the flamenco way of life. Flamenco, no matter how it is modernized, altered, or jazzed-up, depends upon its tradition, its roots, for its unique character. The flamenco tree appears to be flourishing at the moment, but there can be no denying that root-rot is well established. A Way of Life (in English) may be ordered from: Society of Spanish Studies c/o Sunrise Press P.O. Box 742 Chandler, Az 85224 U.S.A. Send $6.95 for soft bound, or $10.95 for hardbound plus $1.50 for postage and handling ($0.50 for each additional book). In Europe: Order from Spain (see address at beginning of review) for equivalent of 460 pesetas (soft-bound) or 720 pesetas (hardbound). SKIRTS - PART I By Marta del Cid Maybe you have experienced flamenco a number of times or maybe all it took was one exposure, but it has caught you somewhere deep inside and won't leave you alone. You are no longer content to be just an observer but feel you must participate in some way, so you make a decision, based on your own inclinations and capabilities, as to what area of the art you will actively pursue and search out instruction. If you are starting study of the baile you will find that you will\n\n[ENDING CONTEXT]\n\nwith him shine also. For example, the granddaughter of Manuel Torre, in a marvelously inspired interpretation, made us think for a moment that the situation would change. But unfortunately it continued in the same vein and the artists limited themselves to putting on the same old record as they have learned to do in those festivales where they believe they will get nothing out of it and that's that. This was, in brief, the XII Gazpacho Andaluz, of which I would liked to have made some good comments about the cante, but since since there was none, I limit myself to what has been said.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "COSTUMING FOR FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_08",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "22-29",
    "page_number": 30,
    "word_count": 2557,
    "article_char_count_full": 14350,
    "article_char_count_review": 3362,
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
    "article_id": "JALEO_1980_09::A1",
    "article_text_for_review": "A TRIBUTE FROM STUDENTS AND FRIENDS by Mark Boroush and Christine Scott Serrano came to Detroit in 1969 in the wake of a traumatic period in his life. The unexpected death of his first wife (an accomplished flamenco dancer in her own right) left him distraught and depressed. He all but stopped performing and broke many of his contacts with managers, recording companies, and critics. He came to Detroit where a friend (and well-known guitar maker) provided him with some space to begin anew. It was a start in a new place which no one could wish upon another. Yet it was a start for what, over the next ten years, proved to be an enormous blessing for the art of flamenco in Detroit. Serrano was born in 1936 and grew up in the Andalusian city of Córdoba. From the very beginning the influences of music and flamenco were strong upon him. His father was a guitarist, known professionally as Antonio el del Lunar. His mother was a flamenco singer -- Niña de la Sierra. He first became interested in the guitar at 9 and began the study of it soon after under the strict direction of his father (his only teacher ever). He apparently kept at it because by the time he was 13 he had turned professional and had made a considerable reputation for himself as an accompanist in Córdoba. Indeed, to this day he remains a favorite son of the city -- along with the great bullfighters Manolete and El Córdobés. And he continued to be remembered by a clock tower in the town square which chimes the hours with a recording of his guitar playing seguirias. Nino Ricardo and Ramon Montoya, as Serrano remembers, were the preeminent guitarists during the time he was growing and first learned to play. He found their music and their styles inspirational. At a later point in his career he traveled widely with Ricardo -- and to this day has many funny stories to tell about that man and their experiences together. Nevertheless, with his father's help, Serrano set out from the very beginning to create his own style and compositions. By the time he was 14 he had moved on from Córdoba to the flamenco circles of Madrid and had recorded his first album. The music he played then was comprised, among other things, by many fast, melodic picado lines and by inventive syncopated versions of the traditional rhythms. It was a style which was received as revolutionary for the time. His popularity grew widely over the next decade -- many critics and aficionados dubbed him \"the young Sabicas\". Much of the reputation he gained over this time was based upon his accompaniment of some of the best singers and dancers of the day. Singers included Manolo Caracol, Antonio Mairena, Fosforito, Fernanda and Bernarda de Utrera, La Paquera; dancers were such as Carmen Amaya, Pastora Imperio, Regla Ortega, Rita Ortega, and La Chunga. An appearance on the Ed Sullivan show and night club engagements in Las Vegas introduced him to American audiences in 1959. Juan was also revolutionary with the guitar in another way -- one that is less well remembered at present. He proudly observes that he was the first in Spain to take the flamenco guitar solo onto the concert stage. Despite earlier solo recordings by Niño Ricardo and Ramón Montoya, neither of these performed in a concert setting in Spain with the guitar alone. Serrano was the first to do this in Spain when his manager arranged such a solo concert in 1957. Thereafter many young guitarists broke the tradition and followed suit. In doing this, Serrano says that he was after the opportunity to be more creative and to pursue the expressive harmonies and feelings of the guitar to a larger degree. He felt the need to move out of the artistic structure imposed by the dancer and singer, though, in all this, he remembers, he could never forget his father's admonition to never lose touch with the traditional flamenco rhythms. Serrano and his family chose to remain in New York at the close of a tour in 1962. With the aid and counsel of his newly discovered friend, Theodore Bikel, he was able, not long after, to record his first album in the U.S.A. \"Ole La Mano\", as it was called, and was an enormous artistic and commercial success. It was the first of over 20 albums to follow for Electra, RCA, and Audiofidelity. Over the next several years he became widely sought after as a concert soloist. Managed by the late impresario, Sol Hurok, he toured widely in the United States, Japan, the Philippines and South America. The Serrano apartment became a meeting place for the major flamenco artists of the area, including among them, Sabicas and Carmen Amaya. In 1965, Serrano was awarded a gold medallion by the Spanish Academy of Fine Arts for spreading the music of Spain throughout the world -- the only flamenco guitarist to date to receive this honor. Then, in the late 1960's, at perhaps the pinnacle of his success, he and his wife chose to leave the cold of New York and the demands of many tours and concerts for Miami to teach and have time for family. But events were to have things otherwise. With the sudden passing of his wife in 1969 and the return of his son and daughter to Spain, Serrano was left to start very much over again.",
    "title": "JUAN SERRANO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_09",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "3,6",
    "page_number": 3,
    "word_count": 916,
    "article_char_count_full": 5183,
    "article_char_count_review": 5183,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1980_09::A2",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDear Jaleo, During the 4th of July weekend there are plenty of fireworks everywhere. But, at the home of Joan and Larry Temo of Akron, Ohio, there was a great display of flamenco fireworks. I'm referring to the three day juerga where there was much talent and super food and liquid refreshment. And the flamencos came from far and near: Joan Weber, dancer from St. Paul; Greg Wolfe from Minneapolis; Mark Boroush, Lee Miller, and Christine Scott from Michigan; from Atlanta, Georgia, came Martha Sid-Ahmed; others came from Columbus, Cleveland, and Akron. Good fortune brought a singer, José Luis Giménez, from Columbus. It was a fine juerga with many nice people. I'm sure everybody enjoyed the three full days. Sincerely Joe Bubas West Mifflin, PA Dear Jaleo, I admit that some of the things I\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"Jaleo\"]\n\nt. Paul; Greg Wolfe from Minneapolis; Mark Boroush, Lee Miller, and Christine Scott from Michigan; from Atlanta, Georgia, came Martha Sid-Ahmed; others came from Columbus, Cleveland, and Akron. Good fortune brought a singer, José Luis Giménez, from Columbus. It was a fine juerga with many nice people. I'm sure everybody enjoyed the three full days. Sincerely Joe Bubas West Mifflin, PA Dear Jaleo, I admit that some of the things I have written in Jaleo are controversial. However, they are intentionally controversial with the purpose of stirring flamenco minds to activity. It's better to have someone to fight with than to have no-one at all. I would like to hear some feedback on my articles by the readers. Some of you readers could become writers and tell us of your flamenco lives. Others could write emotional or even logical articles. If you like Jaleo as much as I do, why not participate? Guillermo Salazar Denver, Colorado Dear Jaleo: I hope your August issue stirs up a flurry of response, both pro and con on the Moron question. I can definitely sympathize with Paco's evaluation of Pohren's strong stands on commercialism-certainly preferable to being a jelly-fish. And I am ceratinly grateful for the wonderful photos of Joselero singing and dancing, Paco del Gastor looking uproariously cross-eyed, and all the rest. Having visited Spain extensively in 1965-66, 1970-71, 1972-73 and most recently in 1978, I can agree with Pohren's assessment that it is probably doomed as a lifestyle, and I am thankful that Marcos and I were able to at least catch the tail-end on our\n\n[ENDING CONTEXT]\n\nwritten a flamenco suite for solo guitar and orchestra. To be sure, his most recent recording available in this country dates back to 1972 (entitled \"Flamenquísimo\" on the Audiophile label). But while this is well worth listening to, it doesn't do full justice to the musical artistry he has grown to achieve. To these authors' ears, what is most exciting about Serrano's music at present is that he has found a way to combine the empathetic interpretations, fiery rhythms, and Guitarist Wanted MUST BE EXPERIENCED IN DANCE ACCOMPANIMENT AND BE WILLING TO TRAVEL. CALL VICENTE ROMERO 213/432-3795\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1980_09",
    "year": 1980,
    "language": "en",
    "article_type": "other",
    "pages": "4-8",
    "page_number": 4,
    "word_count": 1943,
    "article_char_count_full": 11254,
    "article_char_count_review": 3209,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "COMM_03",
        "family": "COMM",
        "trigger": "Jaleo"
      }
    ]
  }
]
```
