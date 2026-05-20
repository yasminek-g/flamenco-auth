import pandas as pd
from pathlib import Path
from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer
from umap import UMAP
from hdbscan import HDBSCAN
import re

INPUT_FILE = Path("bertopic_articles.csv")
OUTPUT_DIR = Path("bertopic_outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

MIN_WORDS = 100

def clean_text(text):
    text = str(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# ------------------------------------------------------------
# Load and filter
# ------------------------------------------------------------

df = pd.read_csv(INPUT_FILE, sep=";", encoding="utf-8-sig")
df["text"] = df["text"].fillna("").apply(clean_text)

model_df = df[df["word_count"] >= MIN_WORDS].copy()
model_df = model_df.reset_index(drop=True)

docs = model_df["text"].tolist()

print(f"Total articles: {len(df)}")
print(f"Articles used for BERTopic, >= {MIN_WORDS} words: {len(model_df)}")
print(f"Articles excluded as too short: {len(df) - len(model_df)}")

# ------------------------------------------------------------
# Model setup
# ------------------------------------------------------------

# Good general English model; all-MiniLM is fast and stable.
embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

# UMAP controls dimensionality reduction.
umap_model = UMAP(
    n_neighbors=15,
    n_components=5,
    min_dist=0.0,
    metric="cosine",
    random_state=42
)

# HDBSCAN controls clustering.
# min_cluster_size controls how large topics must be.
hdbscan_model = HDBSCAN(
    min_cluster_size=12,
    min_samples=5,
    metric="euclidean",
    prediction_data=True
)

# Remove common English stopwords.
custom_stopwords = [
    "flamenco", "jaleo", "spanish", "spain", "sevilla", "seville",
    "madrid", "dance", "dancer", "dancers", "dancing", "danced",
    "guitar", "guitarist", "cante", "baile", "el", "la", "los", "las",
    "juan", "paco", "jose", "maria",
    "td", "tr", "table", "colspan", "html",
    "page", "continued", "cont", "issue"
]

vectorizer_model = CountVectorizer(
    stop_words="english",
    min_df=3,
    ngram_range=(1, 2),
    max_df=0.85
)

# Add custom stopwords to built-in English stopwords
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
vectorizer_model.set_params(
    stop_words=list(ENGLISH_STOP_WORDS.union(custom_stopwords))
)

# Improves topic keyword representations after clustering.
representation_model = KeyBERTInspired(
    top_n_words=10,
    random_state=42
)

topic_model = BERTopic(
    embedding_model=embedding_model,
    umap_model=umap_model,
    hdbscan_model=hdbscan_model,
    vectorizer_model=vectorizer_model,
    representation_model=representation_model,
    calculate_probabilities=False,
    verbose=True
)

# ------------------------------------------------------------
# Fit model
# ------------------------------------------------------------

topics, probs = topic_model.fit_transform(docs)

model_df["topic"] = topics

# ------------------------------------------------------------
# Save outputs
# ------------------------------------------------------------

topic_info = topic_model.get_topic_info()
topic_info.to_csv(OUTPUT_DIR / "bertopic_topic_info.csv", sep=";", index=False, encoding="utf-8-sig")

model_df.to_csv(OUTPUT_DIR / "bertopic_article_topics.csv", sep=";", index=False, encoding="utf-8-sig")

# Save representative docs per topic
rep_docs = topic_model.get_representative_docs()

rep_rows = []
for topic_id, docs_for_topic in rep_docs.items():
    for i, doc in enumerate(docs_for_topic):
        rep_rows.append({
            "topic": topic_id,
            "representative_doc_rank": i + 1,
            "representative_doc": doc[:1500]
        })

pd.DataFrame(rep_rows).to_csv(
    OUTPUT_DIR / "bertopic_representative_docs.csv",
    sep=";",
    index=False,
    encoding="utf-8-sig"
)

topic_model.save(str(OUTPUT_DIR / "jaleo_bertopic_model"), serialization="pickle")

print()
print("Created:")
print(OUTPUT_DIR / "bertopic_topic_info.csv")
print(OUTPUT_DIR / "bertopic_article_topics.csv")
print(OUTPUT_DIR / "bertopic_representative_docs.csv")
print(OUTPUT_DIR / "jaleo_bertopic_model")
print()
print(topic_info.head(20).to_string(index=False))