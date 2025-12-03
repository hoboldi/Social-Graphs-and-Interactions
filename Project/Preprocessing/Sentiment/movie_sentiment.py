import pandas as pd
import matplotlib.pyplot as plt

reviews = pd.read_csv('/Users/Mari.Piiriste/TESTING/Social-Graphs-and-Interactions-1/Project/Data/CSV/reviews_with_sentiment.csv')

calculate_avg_sentiment = (
    reviews.groupby("moviename")
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

movies_with_sentiment.to_csv("movie_sentiment_and_rating.csv", index=False)


# Scatter plot
plt.figure(figsize=(10,7))
plt.scatter(movies_with_sentiment["rating"],
            movies_with_sentiment["avg_sentiment"], alpha=0.25, s=12)

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