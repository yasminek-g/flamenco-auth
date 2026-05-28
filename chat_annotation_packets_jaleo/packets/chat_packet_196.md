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
    "article_id": "JALEO_1984_08::A10",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n(translated and edited by Paco Sevilla) On the occasion of its 25th anniversary (officially, September 25, 1983), the Cátedra de Flamencologia de Jerez de la Frontera published a pamphlet describing a little about its activities and history. We have translated some of the more interesting parts here: The Cátedra de Flamencología y Estudios Folkloricos Andaluces is an academic institution that has as its objective the study, investigation, salvation, preservation, promotion, defense, and dissemination of the art of flamenca and the genuine folklore of Andalucía. It developed out of several other groups -- the Grupo Atalaya, made up of young writers and artists, the Centro Cultural Jerezano, and as a continuation at a higher level of the Peña Artística y del Folklore that was active in\n\n[EVIDENCE WINDOW 1 | retrieval_hint=AUTH_04 | trigger=\"close\"]\n\ntedra was officially founded on September 24, 1958, as an autonomous and special section of the Centra Cultural Jerezana - (later called the Ateneo de Jerez); in 1960, it first called itself \"Cátedra\", and it began to have a proper and independent life as an academic corporation. In 1973. When the Cátedra was founded, there was a need to save and restore the flamenco dances and songs, as well as much of our other folklore, which were dangerously close to the point of disappearing. So, for the first time in Andalucía there could be a serious, permanent and continuous means of studying this material. The main governing body is The General Assembly, headed by a \"Consejc Rectar\" (Governing Council) and assisted by a \"Consejo Asesor\" (Consulting Council), bath of which are divided into different areas of work. At the same time, the Cátedra is divided into four specialized branches: Aula de Cante \"Manuel Tarre,\" Aula de Baile \"Juana la Macarrona,\" Aula de Guitarra \"Javier Molina,\" and Aula de Folklore Andaluz. As a special section, with its own board of trustees, there\n\n[ENDING CONTEXT]\n\nof the Catedra\" as an independent cultural center. 1974-Domecq donates the present building to the Cátedra and Museo. Debut of the \"Concierte de Jerez,\" promoted by the Cátedra, composed by Benito Lauret, with the Symphonic Orchestra of Jerez, and Manuel Morao as guitar soloist. 1975-Beginning of round table discussion called \"Cátedra Viva\" with the participation of peñas, artists, and aficionados. 1976-Homage to the Jerez cantaora Antonia Suárez, Debut of \"Retablo flamenco\" with Parrilla de Jerez on guitar and Alejandro Villatoro on piano. 1977-National Assembly of Flamenco Organizations.\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "LA CATEDRA DE FLAMENCOLOGIA-25 YEARS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "17",
    "page_number": 17,
    "word_count": 1138,
    "article_char_count_full": 7271,
    "article_char_count_review": 2699,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "AUTH_04",
        "family": "AUTH",
        "trigger": "close"
      }
    ]
  },
  {
    "article_id": "JALEO_1984_08::A11",
    "article_text_for_review": "OF THE ARABIAN AND THE ANDALUCIAN \"Macama Jonda\" by Angel Fernandez Caballero [from: El País, August 13, 1983; translated by the Shah of Iran] Author: José Meredia Maya Cantaores: Antonia, la Negra; Luis Heredia, el Pímaco; Jaime Heredia, el Parrón Guitarristas: Paco Cortés, Pedro C., el Niño de Jero; Miguel Ochando Flauta: Rafael Carretero Orquesta de Música Andaluzia de Tetuán, con el cantor Chekera Ariola, I-205400, Barcelona, 1983 Lately, a musical show with the same name as this record-cassette \"Macama Jonda\" has appeared on some Spanish stages. \"Macama\" in arabic means meeting or encounter, from which information the reader can gather an idea of the intentions of this work, expressed by the author when he writes, \"We wish to show the possibilities of encounters between men and between peoples, symbolized by a wedding of an Andalucian man and a Moorish woman from Tetuans. This allows us to show certain fundamental traits common to both peoples. This recording is the soundtrack of a show held in the Manuel de Falla Auditnium in Granada. Naturally, even though the soundtrack is good, the retention of the audible part alone of a show intended for theater, in which the dance forms a gzeater part than the cante, only a ceztain portion reaches us, and not the greatest portion at that. $ ^{2} $ Personally, I am one of those who believe, as I have written before, that the influence of Arabic music over flamenco has been traditionally ovez-rated, and has oot been so important as some claim. $ ^{3} $ These attempts to give us both cultures that are already formed, function at a superficial level, as we do not wish to become serious and transcendental. $ ^{3} $ From the purely flamenco point of view, the recording is pleasant. Of course, an act expect to be elevated to paradise, since the individuals who make it possible are not extraordinary. In certain theatrical productions, specifically those of Madrid, the show included the participation of Enrique Morente, but in the record-cassette he is not present. La Negra, El Pálaco, and 81 Parrón perform. Some of the songs are choral numbers, which lowers even more the flamenco quality. The lyrics by the Heredia/Maya attempt, and usually achieve, that simplicity peculiar to folk lyrics and handily lead to the overall intention. For example:",
    "title": "RECORD REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "18",
    "page_number": 18,
    "word_count": 386,
    "article_char_count_full": 2320,
    "article_char_count_review": 2320,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  },
  {
    "article_id": "JALEO_1984_08::A12",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\n[From: Minneapolis Star and Tribune, January 27, 1984] by Mike Steele From the late 1920's to the early '60s, Spanish dance was undeniably hot stuff - white hot, passionate, sensual, colorful and rhythmically exciting with its strumming guitars, clacking castanets and stomping feet. Thare was a time when Spanish dance troupes could fill the Metropolitan Opera dn you couldn't turn on your old Motorola without seeing the haughty Jose Greco and his steamy partner, Nana Lorca, flying across the screen. Since then, for a variety of reasons - the vagaries of TV taste, the rise of modern dance, the flooding of the field with mediocre opportunists, internecline battles among flamenco purists, Spanish classical dancers and the more theatrically oriented wing of the movement - Spanish dance has\n\n[EVIDENCE WINDOW 1 | retrieval_hint=COMM_03 | trigger=\"clubs\"]\n\norola without seeing the haughty Jose Greco and his steamy partner, Nana Lorca, flying across the screen. Since then, for a variety of reasons - the vagaries of TV taste, the rise of modern dance, the flooding of the field with mediocre opportunists, internecline battles among flamenco purists, Spanish classical dancers and the more theatrically oriented wing of the movement - Spanish dance has declined in popularity. The famed New York flamenco clubs have virtually all closed. Spanish dance on television is rare. While several dancers and teachers are sprinkled around the country, only a handful of troupes exist, maybe no more than four or five. One of those, Zorongo Flamenco, happens to be quartered in Minneapolis and, if it isn't exactly doing the Johnny Carson circuit and becoming a household word, it performs regularly in clubs and on tour. (Its tours have included places as diverse as Alaska, a few years ago, and Yugoslavia last year.) The heart of Zorongo Flamenco is Suzanne and Michael Hauser, the latter the son of modern dance choreographer Nancy Hauser. Suzanne, under the name Susana, has been the troupe's leading dancer-choreographer since the company came into being, first as Trio Flamenco in 1976, then as Zorongo Flamenco four years later. Suzanne has been dancing since she was 3. Her guitarist husband is\n\n[ENDING CONTEXT]\n\nyearning for rebirth, searching for the reasons for her barrenness - counterpointed by the macho arrogance of her husband. (It's amazing how much about relationships on stage can be picked up by listening to counterpoints in rhythm or sudden syncopations.) Solos bring out the characters' essence while ensemble numbers, especially dances for the laundresses (which carry on too long) and later fertility rites, create the atmosphere for the play. There are strong solos for both leads and another for Sergio Bahamondes as the third man in the triangle. The male dances are bold and strong with fast\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "ZORONGO FLAMENCO",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "poem",
    "pages": "19-20",
    "page_number": 19,
    "word_count": 1546,
    "article_char_count_full": 9248,
    "article_char_count_review": 2963,
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
    "article_id": "JALEO_1984_08::A13",
    "article_text_for_review": "[BEGINNING CONTEXT]\n\nCAROLA GOYA AND MATEO IN \"CONCERT OF THE CASTANETS\" (from: $ \\underline{\\text{Dance Diary}} $, June 22, 1984; sent by Mary Poeltl) by Jennie Schulman As Matteo has stated: \"Contrary to popular belief, castanets are not indigenous to Spain.\" In one of the most fascinating demonstrations ever seen at the American Museum of Natural History, Matteo set about enlightening everyone on the history and variety of castanets utilized by civilizations from ancient Egypt, Greece, Rome and Africa and carried into our own century from ancient and baroque times. And if anyone knows the a to z of castanets it is Matteo. His book, \"Woods That Dance,\" is about the only definitive one on the subject. Unfortunately, it has been too long out of print. Currently, after eighteen years of research, Carola Goya\n\n[EVIDENCE WINDOW 1 | retrieval_hint=CRIT_03 | trigger=\"classic\"]\n\nhere has long been a crying need. Carola Goya was the first to appear with symphony orchestras as castanetist and Matteo has conceived a way of scoring for castanets which was illustrated by slides and it proved a precise and clear means; one that wouldn't be at all difficult to absorb, even for neophytes. What was to remarkable about \"Concert of the Castanets?\" Everything! In addition to illustrating the use of castanets in flamenco and Spanish classical dance, the presentation showed us the variety of music that the so-called \"wooden clappers\" can transcend and the endless nuances that can be achieved. The team, assisted by guest artists Ellen Sulides, Robert Sinclair, Juan Antonio Altamirano, Marc Saint-Germain, Jerane Michel and Liliana Morales, illustrated how castanets can be utilized in accompanying everything from Scott Joplin to Johann Strauss; Louis Gottschalk to Enrique Granados. One of the highlights was the interpretation of \"Weiner Bonbons\" (Vienna Bonbons), a first time in history, by castanets, with soprano toned castanets played by Carola Goya, mezzo by Ellen Sulides, baritone by Robert Sinclair, bass tones by Juan Antonio Altamirano and tenor by Matteo. To say that the audience flipped is putting it mildly. But then everything performed on the program had them galvanized. Since this was a program covering the wide range of castanets, the aforementioned players refrained from actual dancing except, of course, for the dancing of the arms and the upper torso. Still the program was not without dance. Jerane Michel performed her own version of the \"Intermezzo\" fron Enrique Granados' opera \"Goyescas\" and she danced this with classic finesse and exquisite form. Liliana Morales danced the \"Jota Aragonesa\" glowingly. This was not the balletic form seen of late but seemed rough hewn by comparison. Still it was authentic, performed precisely in the manner that you would see it danced when visiting the Aragon province of Northern Spain. The two ladies also concluded the program with the traditional \"Sevillanas\". But not before Carola Goya and Matteo strutted their way through Scott Joplin's \"Maple Leaf Rag,\" accompanied by castanets, which was a joy to behold. We have long known of Carola Goya's and Matteo's devotion to Spanish and other forms of ethnic dance, bu\n\n[ENDING CONTEXT]\n\nand dedicated myself to co-زاتف وکف وکفات. I was a large, rather plain looking girl and they laughed at me. This you never forget, because of the age at whirh it happened. \"What I want to do now is to create an authentic Andalucian ballet, one that would be purely flamenco and \"salvaje\" [wild, primitive]. I will not die without seeing a first class performance of the art of Andalucía.\" Flamenco Guitar For Sale 1932 Santos Hernandez Excellent Condition $3,500 Tel. 408/733-1115 FLAMENCO FENCING ORNAMENTAL IRON SAFEGUARD FENCE CO. SERVING NORTH COUNTY 619/745-4846 CA Contractor's Licence #374198\n\n[NOTE: Review text constructed from beginning/end context plus selected trigger windows. Retrieval hints are not labels.]",
    "title": "REVIEWS",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "other",
    "pages": "21-26",
    "page_number": 21,
    "word_count": 1566,
    "article_char_count_full": 9616,
    "article_char_count_review": 3937,
    "article_text_was_truncated": true,
    "review_strategy": "head_tail_windows",
    "retrieval_hints": [
      {
        "window": 1,
        "retrieval_hint": "CRIT_03",
        "family": "CRIT",
        "trigger": "classic"
      }
    ]
  },
  {
    "article_id": "JALEO_1984_08::A14",
    "article_text_for_review": "Correction Jaleo Mar./April '84 page 21...Roberto Reyes (doing palmas) the dancer is Aurora, a popular bailaora married to Basil Georges (both with the Jose Molina ballet). Chiquito de Triana (one of the greatest payo cantaores) is married to Leo Amaya...father of another great flamenco artist Chuni Amaya living in Mexico...Chiquito's cante had been described in Jáleo December '78 with the greatest longendary guitarist Esteban de Sanlucar. Juani Amaya (niece of Diego del Gastor) now leading lady of Mario Maya had been in Jaleo '81 page 16. On the New York scene La Tata (Jaleo, Mar/Apr '84 pg 20) has been dancing with great success at Meson Asturias as guest artist...the cantaor is Pepe de Málaga, guitarist Reynaldo Rincon. Restaurante \"San Rafael\" in Weehawken, New Jersey, near Manhattan, will have Reynaldo Rincón (guitarist) and Dominico Caro (cantaor) in the near future...both have been on Jaleo covers! Spanish Dance Arts Company has been busy this summer. After their successful show in Yonkers, New York they had a repeat show at Prospect Park, Brooklyn, New York...both Carlota and Melinda in the meantime presented their own staged shows with Roberto Lorca as their guest artist... Carlota and Lorca with Arturo Martinez on guitar appeared at the Sunrise Mall, Massapequa, Long Island, New York, July 25. Melinda Marquez and Company performed at Morgan Park, Glen Cove, Long Island, on August 19. Melinda's complement included dancers Roberto Lorca and Nita Angeletti, cantaor Dominico Caro and guitarists Paco Juanas and Arturo Martinez...The Morgan Park concert at the Gazebo on the beach, overlooking the bay at City of Glen Cove, in the north of Nassau county was probably the most beautiful concert I have attended in a long time. The City Fathers of Glen Cove were beaming with pride--they brought a beautiful show to their shores--something the other communities could not do. ONE HUNDRED BRILLIANTLY COSTUMED SINGERS AND DANCERS FROM SPAIN PRESENTED MUSIC FROM ZARZUELAS (SPANISH LIGHT OPERAS) AT SUMMERFARE IN PURCHASE, N.Y.",
    "title": "RYSS REPORT",
    "periodical": "jaleo",
    "issue_id": "JALEO_1984_08",
    "year": 1984,
    "language": "en",
    "article_type": "article",
    "pages": "27",
    "page_number": 27,
    "word_count": 325,
    "article_char_count_full": 2053,
    "article_char_count_review": 2053,
    "article_text_was_truncated": false,
    "review_strategy": "full",
    "retrieval_hints": []
  }
]
```
