import pickle
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib
import re
import requests
import os, re
from collections import Counter
from typing import Iterable, Dict, Tuple, Optional, Union

df = pd.read_csv('/Users/Mari.Piiriste/TESTING/Social-Graphs-and-Interactions-1/Project/Data/CSV/reviews.csv')

# removing numbers and punctuations
def tokenize(text: str):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return text.split()


def download_labmt_wordlist():
    labmt_df = pd.read_csv('/Users/Mari.Piiriste/TESTING/Social-Graphs-and-Interactions-1/Project/Data/CSV/Hedonometer.csv')

    labmt = {
        row["Word"].lower(): float(row["Happiness Score"])
        for _, row in labmt_df.iterrows()
    }
    return labmt

labmt = download_labmt_wordlist()


def calculate_sentiment(
    tokens: Iterable[str],
    labmt: Dict[str, float],
    stop_window: Tuple[float, float] = (4.0, 6.0),
    return_detail: bool = False,
) -> Union[Optional[float], Tuple[Optional[float], int, int]]:

    # Count only words that exist in LabMT
    freq = Counter(t for t in tokens if t in labmt)
    if not freq:
        return (None, 0, 0) if return_detail else None

    low, high = stop_window
    kept: Dict[str, int] = {}
    removed = 0

    for word, count in freq.items():
        score = labmt[word]
        if low <= score <= high:
            removed += count
        else:
            kept[word] = count

    total = sum(kept.values())
    if total == 0:
        return (None, 0, removed) if return_detail else None

    score = sum(labmt[word] * count for word, count in kept.items()) / total
    score = round(score, 2)
    return (score, total, removed) if return_detail else score


def compute_sentiment(text: str):
    if not isinstance(text, str):
        return None

    tokens = tokenize(text)
    return calculate_sentiment(tokens, labmt)

df["sentiment_score"] = df["review_content"].apply(compute_sentiment)

df.to_csv('/Users/Mari.Piiriste/TESTING/Social-Graphs-and-Interactions-1/Project/Data/CSV/reviews_with_sentiment.csv',
          index=False)
