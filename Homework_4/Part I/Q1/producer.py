import tweepy
import requests
import json
import re
from kafka import KafkaProducer

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


def get_stream_push_to_kafka(set):
    k_producer = KafkaProducer()
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )

    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            # clean_tweet = re.sub("@[A-Za-z0-9_]+","", json_response["data"]["text"])
            # clean_tweet = re.sub("#[A-Za-z0-9_]+","", clean_tweet)
            data = json_response["data"]["text"]
            k_producer.send('tweets_sentiment', data.encode())


def kafka_producer():
    k_producer = None
    try:
        k_producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'], 
            api_version=(0, 10), 
            linger_ms=10)
    except Exception as ex:
        print("Exception in connecting Kafka", str(ex))
    finally:
        return k_producer

def main():
    str = input("Enter the word to be analyzed : ")
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(str)
    get_stream_push_to_kafka(str)


if __name__ == "__main__":
    main()
