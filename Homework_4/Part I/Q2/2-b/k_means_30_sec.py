import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import spacy
from sklearn.model_selection import train_test_split
import string
from string import punctuation
import collections
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
import tweepy
import requests
import json
from sklearn.cluster import KMeans
from sklearn.metrics import adjusted_rand_score
from sklearn.decomposition import PCA
from sklearn.decomposition import TruncatedSVD
from scipy import sparse as sp
from datetime import datetime

api_key = "aYBeKs5wulGvf3Fhl4RxY8gd5"
api_key_secret = "0Qxju33AbDTY9q7vWcecD1iONYnlrJv7LW81FOx1LoWFSrYGyP"
access_token = "1467550430058012676-dLPf4PcFgSf0chLvGSg0ca8sDFVBHb"
access_token_secret = "jLAj1H1RSYpGaIEaZnEvaRnoYi5NIDVP8BEIofZy5K5wH"
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAKpvbgEAAAAAWOzxBggW%2FaHPawUMJvfQSpCGuo0%3D5tsOpR8Oyq0l56ctwiVYFsB7o45x0vhvjBYZ6Kj1vFdx1YgHR6'


#code referred from : https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Filtered-Stream/filtered_stream.py

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print("Get rules", json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(rule):
    # You can adjust the rules if needed
    val = rule.split(" ")
    sample_rules = []
    for v in val:
        sample_rules.append({"value":v})
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_tweets(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )

    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

    i = 0
    doc = []
    d1 = datetime.now()
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            clean_tweet = json_response["data"]["text"].lower().strip()
            clean_tweet = re.sub("@[A-Za-z0-9_]+","", clean_tweet)
            clean_tweet = re.sub("#[A-Za-z0-9_]+","", clean_tweet)
            clean_tweet = re.sub(r'http\S+', '', clean_tweet)
            doc.append(clean_tweet)
            d2 = datetime.now()
            diff = d2-d1
            if diff.seconds >= 30 :
                i+=1
                clustering(doc)
                d1 = datetime.now()
                if i == 3 :
                    exit()



def clustering(doc):
    vectorizer = TfidfVectorizer(stop_words='english')
    X = vectorizer.fit_transform(doc)

    true_k = 3
    model = KMeans(n_clusters=true_k, init='k-means++', max_iter=100, n_init=1)
    model.fit(X)

    predict = model.predict(X)
    print(predict)

    clf = TruncatedSVD(2)
    X_pca = clf.fit_transform(X)

    plt.scatter(x=X_pca[:,0], y=X_pca[:,1],c=predict)
    plt.title("Clusters")
    colors = ['#DF2020', '#81DF20', '#2095DF']
    # legend_elements = [Line2D([0], [0], marker='o', color='w', label='Cluster {}'.format(i+1), 
    #            markerfacecolor=mcolor, markersize=5) for i, mcolor in enumerate(colors)]
    # plt.legend(handles=legend_elements, loc='upper right')  
    plt.title('Clustering on Tweets\n' + datetime.now().strftime("%H:%M:%S:%Z"), loc='left', fontsize=22)
    plt.show()



def main():
    str = input("Enter the word on which clustering needs to be performed : ")
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(str)
    get_tweets(str)


if __name__ == "__main__":
    main()
