import pandas as pd
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt

user_communities = pd.read_csv('../Data/CSV/user_community_mapping.csv')
reviews = pd.read_csv('../Data/CSV/reviews_with_sentiment.csv')

reviews = reviews.rename(columns={"username_of_creator": "username"})
df = reviews.merge(user_communities, on="username", how="inner")

community_text = (
    df.groupby("community_id")["review_content"]
      .apply(lambda x: " ".join(x.dropna().astype(str)))
)

stopwords = STOPWORDS.union({"movie", "film", "one", "like", "the"})

for comm, text in community_text.items():
    wc = WordCloud(
        width=800, height=400,
        background_color="white",
        stopwords=stopwords
    ).generate(text)

    plt.figure(figsize=(10,6))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(f"Word Cloud for Community {comm}")
    plt.show()