import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

reviews = pd.read_csv('/Users/Mari.Piiriste/TESTING/Social-Graphs-and-Interactions-1/Project/Data/CSV/reviews_with_sentiment.csv')
reviews_clean = reviews.dropna(subset=["rating", "sentiment_score"])

calculate_avg_sentiment = (
    reviews_clean.groupby("moviename")
           .agg(
               avg_sentiment=("sentiment_score", "mean"),
               n_reviews=("sentiment_score", "size")
           )
           .reset_index()
)

calculate_avg_sentiment["avg_sentiment"] = calculate_avg_sentiment["avg_sentiment"].round(2)

movies = pd.read_csv('/Users/Mari.Piiriste/TESTING/Social-Graphs-and-Interactions-1/Project/Data/CSV/movies.csv')
movies["genre"] = movies["genres"].str.split("|").str[0].str.strip()

movie_data = movies[["movie_name", "rating", "genre"]].rename(
    columns={"movie_name": "moviename"}
)

movies_with_sentiment = calculate_avg_sentiment.merge(
    movie_data,
    on="moviename",
    how="inner"  # keep all movies with reviews
)
clean_dataset = movies_with_sentiment.dropna(subset=["rating", "avg_sentiment"])

clean_dataset.to_csv("movie_sentiment_and_rating.csv", index=False)

# Pearson correlation coefficient

correlation = clean_dataset["avg_sentiment"].corr(clean_dataset["rating"], method="pearson")
print("Pearson correlation:", correlation.__round__(4))

# Scatter plot
x = clean_dataset["rating"]
y = clean_dataset["avg_sentiment"]

plt.figure(figsize=(10,7))
plt.scatter(x, y, alpha=0.3, s=20)
m, b = np.polyfit(x, y, 1)  # slope (m) and intercept (b)
plt.plot(x, m*x + b, color="pink", linewidth=2, label="Trend line")

plt.text(
    0.05, 0.95,
    f"r = {correlation:.3f}",
    transform=plt.gca().transAxes,
    fontsize=12,
    verticalalignment="top",
    bbox=dict(facecolor="white", alpha=0.7, edgecolor="none")
)
plt.xlabel("Movie Rating in Letterbox")
plt.ylabel("Average Review Sentiment")
plt.title("Sentiment Score vs. Official Rating")
plt.grid(True)
plt.show()


# genre sentiment
genre_sentiment = (
      movies_with_sentiment.groupby("genre")
                 .agg(
                      avg_sentiment=("avg_sentiment", "mean"),
                     n_movies=("avg_sentiment", "size")
                  )
                  .reset_index()
 )

print("genres", genre_sentiment)


# calculating the Spearman correlation coefficient
corr_spearman = reviews["rating"].corr(reviews["sentiment_score"], method="spearman")
print("Spearman correlation:", corr_spearman.__round__(3))

# plot the sentiment score vs review the user gave
plt.figure(figsize=(12,6))
sns.boxplot(x=reviews["rating"], y=reviews["sentiment_score"])

plt.text(
    0.02, 0.95,
    f"r = {corr_spearman:.3f}",
    transform=plt.gca().transAxes,
    fontsize=12,
    verticalalignment="top",
    bbox=dict(facecolor="white", alpha=0.7, edgecolor="none")
)
plt.xlabel("User Rating (1–10)")
plt.ylabel("Sentiment Score")
plt.title("Sentiment Score Distribution by User Rating")
plt.show()
