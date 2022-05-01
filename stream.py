import tweepy
from data import TweetData
from threading import Thread
import time
import json
from collections import deque


# the passwords needed to get access in the twitter api
consumer_key = "your consumer key here"
consumer_secret = "your consumer key secret here"
access_token = "your access token here"
access_token_secret = "your access token secret here"
bearer_token = "your bearer token here"

class TweetStream(tweepy.StreamingClient):
    def __init__(self, bearer_token, **kwargs):
        super().__init__(bearer_token, **kwargs)
        self.db = TweetData()
        self.buffer = deque()
        self.buffer_limit = 10

    def create_rules(self, rules):
        for r in rules:
            new_rule = tweepy.StreamRule(value=rules[r], tag=r)
            self.db.init_db(r)
            self.add_rules(new_rule)
            print(new_rule.tag)

    def start_stream(self, autostop=None):
        """

        :param autostop: after "autostop" seconds the connection will stop automatically
        :param rules: the rules for filter streaming data
        :return:
        """
        def auto_stop():
            time.sleep(autostop)
            self.stop_stream()
            print("Auto-disconnecting after " + str(autostop) + " seconds....")

        if autostop is not None:
            t1 = Thread(target=auto_stop)
            t1.start()
        self.filter(tweet_fields=['created_at'], expansions=['author_id', 'referenced_tweets.id'], user_fields=['location', 'created_at', 'public_metrics'])

    def stop_stream(self):
        self.clean_rules()
        self.disconnect()

    def clean_rules(self):
        response = self.get_rules()
        for r in response.data:
            self.delete_rules(r.id)

    def clear_buffer(self):
        while True:
            try:
                item = self.buffer.popleft()  # tweet data
                movies = item['matching_rules']  # movie names
                for i in movies:
                    self.db.insert_db(item, i['tag'])
            except IndexError:
                break

    def on_data(self, raw_data):
        data = json.loads(raw_data)
        self.buffer.append(data)

        if len(self.buffer) > self.buffer_limit:
            t2 = Thread(target=self.clear_buffer)
            t2.start()

    def on_connection_error(self):
        print("Error: connection error!")
        self.disconnect()

    def on_errors(self, errors):
        print("Error detected, reconnecting in a few seconds...")
        self.start_stream()


if __name__ == "__main__":
    _rules = {'doctor strange': 'doctor strange lang:en', 'spider man': 'spider man lang:en', 'Morbius': 'Morbius lang:en'}
    d = TweetStream(bearer_token)
    d.create_rules(_rules)
    d.start_stream(autostop=30)
