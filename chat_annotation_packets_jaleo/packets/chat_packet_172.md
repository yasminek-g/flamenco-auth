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
    "article_id": "JALEO_1983_07::A9",
    "article_text_for_review": "OLD SPANISH DAYS IN SANTA BARBARA Juan Talavera and his Spanish and Flamenco Fiesta Spectacular returns to the historic Loberto Theatre in Santa Barbara for his fourteenth Old Spanish Days Fiesta visit, August 4, 5, and 6. After a busy year of theatrical and concert appearances throughout the United States, Mexico and Canada, the critically acclaimed Spanish and flamenco dance star brings an all new Fiesta program to the Loberto Theatre. A Santa Barbara critic had labeled Juan Talavera as, \"an indispensable Fiesta asset.\" The hard-boiled Los Angeles Times has stated that, \"Juan Talavera is a beautiful sight as a Spanish dancer.\" The Hollywood Drama Logue has stated that, \"Talavera, always brilliant, erupts into a fireworks explosion of flamenco dance.\" The usually stolid Santa Barbara News Press states that, \"Talavera not only entertains. He lends authenticity and professionalism. In the Historic",
    "title": "JUAN TALAVERA",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "13",
    "page_number": 13,
    "word_count": 141,
    "article_char_count_full": 909,
    "article_char_count_review": 909,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A10",
    "article_text_for_review": "[from: Nueva Andalucía, March 24, 1983; translated by Paco Sevilla] by F. Godoy Along with Spring, the flamenco performance of \"Sangre de la Primavera\" arrived and made its debut in Sevilla, in the Teatro-Cine Los Remedios: its creator is a Sevillano who, it seems, is not sufficiently well-known in the land of his birth. We speak of Manolo Marín, whom we found in the location of the debut, in a gathering for the news media before the event; this man is an artist who has travelled at length to the stages of the world, a fighter who, as he told us, has put all his determination and dreams into this work which, for three days, promises to delight the artistic sensitivity of Sevilla. The work, classified as \"theater-ballet,\" can be conceived as such from the moment in which, in spite of being fundamentally based on cuadros of baile, it counts on \"almost a plot,\" as Manolo told us; that is to say, it has a connecting thread that gives it theatrical form. Based fundamentally on fragments of the life and poetry of Lorca, it is a reflection on the vision of the death of the poet from Granada. The company (more than thirty in all) is headed by Manolo Marín, Carmen Albéniz, Currillo de Bormujos, and Maribel. This last artist is a North American bailaora whose inclusion among the stars has awakened the curiosity of the aficionados and the small world of flamenco in general. Manolo, how is it that you have included a foreigner among the principal figures of your ballet? How do you feel about the dedication to the baile flamenco by people who lack roots in our land? \"In the first place, Maribel has the most theatrical role in the performance, because she personifies the figure of death, and to have chosen her is due simply to the fact that she is the most appropriate of my collaborators for this work -- and it is magnificently assumed by her.\" \"With respect to the second question, I believe that the profession of dancer is a question of value or lack of value and that has nothing to do with nationality. Undoubtedly, ※※※※ マノロ・マリン MANOLO MARIN (RIGHT) IN JAPAN \"SANGRE DE LA PRIMAVERA,\" A GOOD FLAMENCO-BALLET (Sevilla newspaper, March 24, 1983) Yesterday the flamenco performance of \"Sangre de Prima-vera\" made its successful debut; the work was inspired by the poetry and death of Federico García Lorca and a poem by Antonio Machado. The direction of the show, which was presented to the public in the Los Remedios Theater, was in the hands of bailaor and choreographer, Manolo Marín. The performance of the North American bailaora, Mary Elizabeth Weisinger, artistically called \"Maribel,\" was one of the greatest attractions of the work. Maribel did not defraud the audience and received numerous ovations. Besides Manolo Marín and Maribel, there were also Carmen Albéniz and Currillo de Bormujos. On the guitar, José Manuel Cruz, José Antonio Vargas, Luis del Carmen, and Juan Nogales. In the cante, Antonio Saavedra, Juan José de Alcalá, and Antonio de la Malena. The show had the special collaboration of the national prize winning guitarist, Riqueni. The debut was crowned by success. The dance of Manolo Marín and Carmen Albéniz stood out, but without detracting for a moment from Maribel, a bailaor who was not born in Triana, but seems to have gathered all of the \"sal y gracia\" of this land, she passed, with great success, the test of fire. The \"fin de fiesta\" (grand finale) with performance by all the company was brilliant and well-applauded. The audience A premium string designed especially for the top line of flamenco guitars—the choice of many leading guitarists, classical as well as flamenco. At your local dealer or contact: Antonio David Inc., 204 West 55th Street, N Y C. 10019—(212) 757-3255 (212) 757-4412 LOS ANGELES, CALIFORNIA Group classes from Aug. 27 - Sept. 3 Advanced/Professional Beginning/Intermediate Performance to be arranged. Contact: Leo Markus (213) 851-9409 1218 N. Gardner, Los Angeles, CA 90046 MINNEAPOLIS, MINNESOTA Group classes on September 16, 17, 18, 21, 22, 23, 24 Beginner/Intermediate: 7:30 PM Intermediate/Advanced: 9:30 PM (Weekend classes will be held during the day) Performance sponsored by The Flamenco Society of Minnesota on Sunday, Sept. 25 Contact: Barbara Roche (612) 377-1123",
    "title": "MANOLO MARIN",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "poem",
    "pages": "14-16",
    "page_number": 14,
    "word_count": 715,
    "article_char_count_full": 4263,
    "article_char_count_review": 4263,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A11",
    "article_text_for_review": "COSTUMES AND THE DANCE This article is in the category of \"food-for-thought,\" because, when you write about as large a subject as costumes and costuming for dance, specifically flamenco dance, then there are surely as many opinions as there are colors in the rainbow. I have always been fascinated by all facets of dance, including the history and all of the wonders of ancient tradition, with the ever-changing evolution to the present day. Almost every culture in the world that dances, whether as a performing art, ritual, festival or personal expression of that culture, has given great importance to the costume as body cover, and as an extension of the body expressing movement and becoming at one with the dancer's interpretation and feeling. Since the time that flamenco dance became a spectacle art, a performing art, the costumes for both men and women have gone through many subtle and not so subtle changes in style and use. It was not too many years ago, before all the marvelous \"wash and wear\" materials, paper nylons and polyester gabardine, that costumes for women were cotton, heavily starched, and usually heavy in body of the material. This was also a time, back in the early tablaos and café-cantantes of the early 20th century, when the famous dancers La Malena, La Macarrona, La Tanguera, and others moved in a flamenco style very different from that which you see today. The costumes of the male dancers of the same period were usually made of a fairly stiff alpaca or wool, which had very little give in movement if not cut right; the style of the male flamenco dancer was also very different from what we see today. Early photos of the famous male dancers, El Estampio, Antonio de Bilbao, Frasquillo, and others, showed them in the traditional three piece \"traje corto\" of high pants, way above the waist, a short vest and heavy short jacket with a high neck shirt. In a way this is similar to what is worn today, although I will talk of the differences later in the article. For both male and female dancer, the very cut of their costumes and the materials they were made from influenced their movements and style. It seems obvious that the earlier female dancers moved slower in the heavy cotton costumes, especially the bata de cola, using them as part of the body, not something to just kick around or lift like a \"shmata\" (although a Jewish word, it describes a type of tropo or rag beautifully). Another emphasis in the earlier dancer was the use of the upper body more as a total expression of the dance and the dancer's feelings. Arms, hands, torso and the \"aire\" of the upper body were expressed by the dancer of the past and the costume was not only to cover the body, but to be as an integral part of the dancer's expression. Footwork was at a minimum for the early female dancers and, if they did lift the bata to show the expression of the footwork, it was done discretely and with style, as was the art of using the fan. The bata was in the category of total artistic expression, flowing with the dance, the use of the legs and hips at one with the compás, making the costume and artist appear as one, not just a beautiful body with a beautiful cover. Pilar López was one of the last of the Spanish-Flamenco dancers who could do a 12 to 15 minute soleares without more than two or three redobles and completely cover the entire range of feeling, emotion, aire and gracia of the dance, yet never once grab the bata. It flowed artistically with every nuance and dynamic of the cante and music of the guitar. She made an art out of the movement of the bata de cola. The woman who changed the movement of the bata forever was Carmen Amaya. She had such dynamics and strength that she could put the heaviest and longest bata anywhere she pleased with one incredible movement of her body. She had such terrific footwork that she would lift her bata in a way that was always in relation to her individual moves and originality. It would be totally integrated into her dancing. She was also one of the first women to wear pants on stage; she could do it because they fit her figure and artistic tamperament. Carmen Amaya inspired many imitations. Many tried to wear even longer batas, kicking and thrashing at them to get them out of the way of their feet and eventually having to \"carry\" the bata for most of the dance. Much of this has prevailed to this day.",
    "title": "MORCA SOBRE EL BAILE: COSTUMES",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 788,
    "article_char_count_full": 4392,
    "article_char_count_review": 4392,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1983_07::A12",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nMANOLO SANLÚCAR Summer Guitar Course Manolo Sanlucar, one of Spain's formeost flamenco guitarists, has announced the Second International Sanlucar Flamenco Guitar Course to be held from August 1 to 14 in Sanlucar de Barrameda, on the Atlantic coast of the province of Cádiz in Andalucía. last year students from more than a dozen countries participated in the summer course, sharing in Sanlúcar's unique approach to flamenco guitar technique and music. Students were able to exchange ideas with guitarists from other backgrounds and nations as well as acquaint themselves with Andalucía and its rich flamenco heritage. This year's course will be a 2 week intensive course and will be offered for 25,000 pesetas (approx. $200) for the classes and 25,000 for room and board (dorm style, with 3 meals a\n\n[EVIDENCE WINDOW 1 | retrieval_hint=PED_03 | trigger=\"Translation\"]\n\nthe summer course, sharing in Sanlúcar's unique approach to flamenco guitar technique and music. Students were able to exchange ideas with guitarists from other backgrounds and nations as well as acquaint themselves with Andalucía and its rich flamenco heritage. This year's course will be a 2 week intensive course and will be offered for 25,000 pesetas (approx. $200) for the classes and 25,000 for room and board (dorm style, with 3 meals a day). Translation into English will be provided. A limited amount of financial assistance will be available. Auditors will be accepted for 8,000 pesetas. To be admitted as a performer, a student must have some background in guitar, though not necessarily flamenco guitar For further information and registration materials, contact Irene Kessel, 32 Arcadia Rd., Natick, MA 01760, (617) 555-2305. Transcribed by Peter Raine 0000 4444 7777 12121212 111111 12121212 111111 12121212 111111 111111 1 0000 4444 77\n\n[ENDING CONTEXT]\n\n699 700 701 702 703 704 705 706 707 708 709 710 711 712 713 714 715 716 717 718 719 720 721 722 723 724 725 726 727 728 729 730 731 732 733 734 735 736 737 738 739 740 741 742 743 744 745 746 747 748 749 750 751 752 753 754 755 756 757 758 759 760 761 762 763 764 765 766 767 768 769 770 771 772 773 774 775 776 777 778 779 780 781 782 783 784 785 786 787 788 789 790 791 792 793 794 795 796 797 798 799 800 801 802 803 804 805 806 807 808 809 810 811 812 813 814 815 816 817 818 819 820 821 822 823 824 825 826 827 828 829 830 831 832 833 834 835 836 837 838 839 840 841 842 843 844 845 846 847 848\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "STRUCTURE UP CLOSE: TREMOLO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "18-19",
    "page_number": 18,
    "word_count": 2172,
    "article_char_count_full": 10574,
    "article_char_count_review": 2583,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "PED_03",
        "family": "PED",
        "trigger": "Translation"
      }
    ]
  },
  {
    "article_id": "JALEO_1983_07::A13",
    "article_text_for_review": "The recent articles in Jaleo on flamenco dance notation have made me think of the system I was taught by Luisa Pericet. She is a great teacher. Her seven-year program amounted to a division of bolero and zapateado steps as well as the required and optional choreographies into a truly progressive sequence. A requirement along the way was that we keep up our notebooks which we had to turn over at the exams. The exercises and the first few choreographies were distributed to us in written form, but after that everyone was on his own. I usually spent the last few minutes of each class learning to write down the new material correctly. The classical dances were fairly easy because the steps all had names; so did the castanet sounds. The flamenco dances were more cumbersome, but Luisa's system made them less unwieldy than they might have been. She had isolated the most common patterns used in flamenco beginning with the remate, \"golpe derecho, 2 golpes izquierdos, golpe derecho.\" The Shah of Iran writes it for me it is #1 con D (erecho). First, of course, we learned the components of footwork: planta, talon, punto, taco, golpe.",
    "title": "PAULA DURBIN ON DANCE: DANCE NOTATION",
    "periodical": "jaleo",
    "issue_id": "JALEO_1983_07",
    "year": 1983,
    "language": "en",
    "article_type": "other",
    "pages": "20",
    "page_number": 20,
    "word_count": 197,
    "article_char_count_full": 1138,
    "article_char_count_review": 1138,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
