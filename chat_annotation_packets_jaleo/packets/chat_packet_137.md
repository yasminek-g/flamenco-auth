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
    "article_id": "JALEO_1982_04::A1",
    "article_text_for_review": "(from: Texas Highways, April 1981; sent by Chayito) First, you notice the soft lights, the subdued sounds. Nice, but you expected that. And you see the crisp linen, the silver vase with a single rose on each table, the crystal and fine china that mark Las Canarias at La Mansion Hotel as one of the most elegant places to dine along San Antonio's Riverwalk. Then the music begins. Suddenly, incredible sounds issuing out of one single guitar transport you to Spain, to the heart of the dance in Andalusia, where ruffled petticoats swirl and high heels tap the rhythms of the flamenco. El Curro, source of this illusion, perches on a stool behind a wrought iron rail at Las Canarias. As diners sit entranced, he evokes from his six-string guitar music to conjure up exotic dancers with clicking castanets and clashing cymbals. You hear the sharp hand claps that accent the syncopation and imagine ruffles flouncing as costumed dancers are caught up in the fast, hot measures of the gypsy flamenco. The music ends. El Curro puts down his guitar as applause fills the silence. He bows with a smile. Not bad for a kid who grew up on San Antonio's west side in an area he calls the ghetto. When Curro started playing guitar at 12, he and his six",
    "title": "OF SPAIN TO SAN ANTONIO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "3",
    "page_number": 3,
    "word_count": 221,
    "article_char_count_full": 1240,
    "article_char_count_review": 1240,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_04::A2",
    "article_text_for_review": "FAREWELL FROM PACO SEVILLA With this issue of Jaleo, I am formally leaving the task of editor behind. After four and a half years of being involved in writing about flamenco, I find that I have a great need to devote more time to doing it. I'm not even certain anymore of the validity of writing about flamenco. One certainly does not learn flamenco from reading about it --- although it may be possible to pick up some facts about it. Even studying from a teacher and practicing doesn't seem to result in an understanding of flamenco. As much as I would like to resist falling into an old Spanish cliché, I find that I must admit that the aspiring flamenco artist who was not raised in a flamenco environment in Spain, has little chance of ever understanding even a fraction of what flamenco is about (and perhaps finding that flamenco is, in a sense, nothing at all). The only chance we have of acquiring that small understanding is to spend extended periods of time in Spain. So my present goal is to return to Spain. I will never be a flamenco --- my personality will not permit it, but, perhaps by immersing myself in flamenco for awhile longer, I will interpret the music a little better. (You might be surprised to find that some of Spains top flamenco artists are not \"flamencos\" either.) Jalec has value in its presentation of historical material, gossip about artists, biographies, etc., and as a forum for all of the displaced flamenco mutants of the world (aren't we all just flamenco souls that happened to be conceived in the wrong mothers and in the wrong countries?). I hope to continue to contribute to Jalec in a more minor role and perhaps get back to doing some writing in the future, but I no longer wish to be held responsible for the content, presentation or delivery of the magazine. I have been prevented, to some extent, from revealing to the readers what I feel the nature of this publication should be (or in fact, is) and, therefore, will not be held responsible for any aspect of it in the future. Lastly, one more round of thank you and appreciation on",
    "title": "EDITORIAL",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "4",
    "page_number": 4,
    "word_count": 375,
    "article_char_count_full": 2083,
    "article_char_count_review": 2083,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_04::A3",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nDear Jaleo: A word of thanks for the magazine and the enthusiasm of all involved with the production of the magazine. I live in an area that is practically a vacuum for flamence music and this magazine helps me to keep in touch in no small way. Thank you, Ron Hälina Saskatchewan, Canada Dear Jaleo: We just returned from a very exciting tour of Mew Zealand and introduced flamenco to some beautiful places. We met quite a few aficionados at the University in Auckland and a very fine singer from Cadiz named Leo. We enjoyed a few fiestas together. There has been little flamenco there and we were very much appreciated. We had time to spend at some beautiful beaches and the people were superb and friendly. I enclose a review that we received at a theatre where we were the first people to\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_01 | trigger=\"review\"]\n\nt quite a few aficionados at the University in Auckland and a very fine singer from Cadiz named Leo. We enjoyed a few fiestas together. There has been little flamenco there and we were very much appreciated. We had time to spend at some beautiful beaches and the people were superb and friendly. I enclose a review that we received at a theatre where we were the first people to perform. Abrasas ta all, Teo Morca Bellingham, Washington [Editpr: See review in this issue.] Dear Editor: I have had the excellent fartune of having met some of the world's finest musicians, flamenca and otherwise. I have noticed that they refrain from criticism (sarcastic or otherwise) of other musicians, especially other masters of any instrument. This finesse is part of what may be called \"grace.\" It was obvious to me, from the article \"Punto de Vista\" in the March, 1982, Jaleo, that Mr. Lobóill lacks any such grace. When Mr. Lobóill can contribute one percent of what Pacca de Lucía has to flamenco and to music in general, then he might be worth listening to. Until he does, I can only be disgusted by his pettiness. in your magazine by Jerry Loddill, unless perhaps the\n\n[ENDING CONTEXT]\n\nto think that when someone goes outside the bounds of their pure ethnic forms they are going for the money. Some jazz players have indeed gone into more of a rock-and-roll medium, but I hope that Mr. Loddill is not so misled as to think that is what Paco has done. On the contrary, if Paco needs more money, he will probably have to do more touring in the flamenco concert vein, as he is unlikely to make a great deal of money playing the style of music which Mr. Loddill is concerned about. I doubt, furthermore, that Paco ever had any expectation that his new path would increase his income.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LETTERS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "5-6",
    "page_number": 5,
    "word_count": 1187,
    "article_char_count_full": 6655,
    "article_char_count_review": 2776,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_01",
        "family": "CRIT",
        "trigger": "review"
      }
    ]
  },
  {
    "article_id": "JALEO_1982_04::A4",
    "article_text_for_review": "by George Peters After reading about David Smith in New South Wales, Australia, and his problem with getting flamenco records, I decided to share some of my experiences in searching for records. There is a book out for record collectors called $ \\underline{\\text{The Record Collectors International Dictionary}} $ by Gary S. Felton which lists dealers of new and used records; this has helped me to get some old, out-of-print flamenco records. (Soft cover edition available from Crown Publishers, Inc., One Park Avenue, New York, NY 10016 for about 9.00.) Needless to say, most of the listed dealers carry junk, and you are lucky to get one response out of twenty attempts in a record search. An alternative method is to list your specific want in a paperback magazine called Goldmine (Box 187, Fraser, MI 48026) or in Classic Wax by the same publisher. These magazines can sometimes be found in large magazine stores. Another one is The Record Finder (15394 Warwick Blvd., Newport News, VA 23602). I think the cost of listing your needs is $1.00 for Classic Wax and The Record Finder, and $2.00 for Goldmine. These publications print long lists of records for sale, although flamenco selections are nil. The value here is that private collectors scan the ads and I've had more responses from them than from anywhere else. Here is a list of some of the places in which I have found some success in my search for flamenco records. By the way, I have never been able to get a response from The Musical Heritage Society (often listed in Jaleo as a place to get records). --Record Finders (213) 732-6737 or 931-2757 120 N. Larchmont/5639 Melrose Los Angeles, CA (large selection of Latin and some flamenco; cheap) --Discontinued Records (213) 849-4791 444 S. Victory Boulevard Eurbank, CA (had a large selection of flamenco and would make tape copies for 515-20, but have been taken to court for record piracy and future is uncertain) --Record Annex (213) 469-4465 6715k Hollywood Boulevard Hollywood, CA (various; cheap) --Canoga Park Used Book Store (213) 883-7986 7528 Topanga Canyon Canoga Park, CA 91303 (Sabicas, Montoya, Maravilla; cheap) --The Magic Flute (415) 661-4257 51Dk Frederick St. San Francisco, CA (Montoya, José Greco; cheap) --Round Sound West (714) 436-3131 Box 2248 Leucadia, CA 92024 (Sabicas, José Greco, Montoya, Serrano) --Music Masters (212) 840-195E 25 W. 43rd St. Mew York, NY 1D036 (reissues of Sabicas, Escudero; reasonable prices) --Daytons 824 Broadway New York, NY 10003 (Sabicas, others; very expensive) --Second Hand Rose's 6th Avenue Shop, Inc. 525 Sixth Ave. Wew York, NY 10011 (reasonable) --Ludus Tomalis (212) 989-975E 24 Eighth Ave. New York, NY (good selection; expensive) --Spanish Music Center (212) 582-42E0 Belvedere Hotel 319 W. 48th St. New York, NY 10036 (owner hates flamenco music, but has a great deal of printed music) --C. S. Sierle 88 Boulevard, Suite 203 Passaic Park, NJ 07055 (large selection of Spanish records; reasonable) --Casa Moneo (212) 929-1647 210 W. 14th St. New York, NY 10011 (limited selection of new Spanish records) England seems to be more attuned to flamenco music than are. Guitar Record Center in London has a long list of records in their catalogue (most of Paco Pena's records, at least 15 records by Manitas de Plata, plus others including Sabicas, Lucía, Escudero). Send one pound sterling for a --Also: Paco Peña Toques Flamencos (music and record) available from Musical New Services, Ltd. Guitar House Bimport, Shaftesbury Dorset England Send 4.60 (Record), 5.60 (Book) plus 30p for each item -- a Banker's Draft or International Money Order in sterling --Also: Juan Martín's El Arte Flamenco de la Guitarra (guitar method) with cassette. (price 12.00 sterling plus? for mailing) United Music Publishers, Ltd. 1 Montague London WC1B 5BS, England Paco de Lucía written music is now available from Spain: \"Fantasía Flamenca #1\" and \"#2\" (200 pesetas each) and \"Lo Mejor de Paco de Lucía\" (250 pesetas). Write to: Union Musical Española San Jeronimo 26 Madrid, Spain I've tried for two years now to find Mario Escudero's \"Piesta Flamenca\" (ABC Paramount -- 428), the record that corresponds to the music book transcriptions by Joseph Trotter (Hanson Publications). This is a very elusive record and may have had a limited release. I would appreciate any help that could be given to me in locating a hard copy -- I am willing to pay $100.00 for it, although it may not be available at any price. Please write to me at: 6040 Elba Drive Woodland Hills, CA 91367 Good luck in your record search! FAVIANA DANCING TO MARIA'S GUITAR",
    "title": "FLAMENCO RECORD COLLECTING",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "7-8",
    "page_number": 7,
    "word_count": 762,
    "article_char_count_full": 4603,
    "article_char_count_review": 4603,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1982_04::A5",
    "article_text_for_review": "SPANISH LANGUAGE STUDENTS BITTEN BY FLAMENCO BUG by Mary Mounts Four years ago I was introduced to Martha Sid-Ahmed, who was forming a class in flamenco. Being a Spanish language teacher and being interested in Spanish culture, I leaped at the chance to join Martha's group. Martha was a superb instructor, exuding the spirit and \"el alma de los gitanos.\" This spirit completely permeated and moved one of her younger students to learn not only the dance patterns but also to study and learn how to sing and accompany other dancers with the guitar. Now this student, Maria Temo, is in the eighth grade and is one of my Spanish language students. Martha, our former teacher, is now residing in Georgia. Maria is what every Spanish teacher would like to have in her class. In four months of first year Spanish, my students have been exposed to more Spanish culture than most four-year students, and the credit goes to Maria. When I learned in early October that the University of Akron was planning a foreign language festival, I made plans to have Maria organize a mock juerga. Several periods during the week, Maria would teach different groups of students how to do palmas with her footwork and with her guitar accompaniment. She also taught three other students the dance patterns to bulerías. The industrial arts teacher made a wooden tablado for us. On the day of our performance, I took 19 eager, eighth grade students, a tape recorder, a tablado, and Maria with her guitar to the University of Akron to compete with other schools in the area of dance. One hour before we were to perform, we looked for a place to practice. This practice period proved to be more exciting and rewarding than the actual contest performance. We decided to use the large open entryway of the Student Union Building. We put the tablado in place and all the students took their seats around Maria. The sound of Maria's voice and guitar, and the rhythmic palmas and counter palmas began to float through the entire downstairs area. We were also joined by Fabiana Vidlak, who was also one of Martha's students. Fabiana, accompanied by Maria, improvised a tango. The combination of Fabiana's feet, the guitar, Maria's gitana voice and the castanets was so exciting that soon the entire area and stairway leading to the second floor was surrounded by students who wanted to join in with palmas. The sight and the unique sounds coming from this corner of Akron University was so completely foreign and seemingly authentic, that when university students entered the door on their way to the bookstore or lounge, they seemed to be incredulous. When the impromptu audience responded with joy, delight, and awe, we knew that we were ready for the competition. Needless to say, we did win first place in group competition, and Maria Temo was awarded a first place in individual competition in guitar, voice, and dance. Fabiana was awarded a first place in dance. As I observed everyone enjoying our performances, I thought about Martha and wished that she too, could have enjoyed these moments of joy that she truly inspired. My students were so excited about the juerga, that we presented it in a school assembly. I am delighted that so many of our Akron students, teachers, and other interested adults have had the opportunity to experience flamenco through our small but enthusiastic group of students. TOP: MARIA PULLS MIGUEL BORGSCHULTE ONTO THE TABLADO FOR RUMBA BOTTOM: FAVIANA VIDLAK AND MARIA DANCE SEVILLANAS",
    "title": "FLAMENCOHIO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1982_04",
    "year": 1982,
    "language": "en",
    "article_type": "other",
    "pages": "9",
    "page_number": 9,
    "word_count": 593,
    "article_char_count_full": 3491,
    "article_char_count_review": 3491,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
