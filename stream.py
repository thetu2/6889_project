import tweepy
from data import TweetData
from threading import Thread
import time
import json
from collections import deque


# the passwords needed to get access in the twitter api
consumer_key = "YC7BRyfMoIrFECEt38fwZP9cJ"
consumer_secret = "l8GZHl35sJbgRJtryGruWXzBWTtgSw0RWrUFXS4qsSZGdIRdjS"
access_token = "1238106378243149825-9Vxt86ygz5x76RzaSgx7ubN34TrBKD"
access_token_secret = "AHZO6UJraNBMfAv53aqz52LSz2JNB6CCxKk2E6R8vQ95P"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAIKSbgEAAAAAov8EpD%2Fv0t3lkNldNBf5b8uor3Q%3DMLBvfGbrmzUblqapzMo0y2HdiFqs0yCgrsEQpt5nDM1pQODve3"


class TweetStream(tweepy.StreamingClient):
    def __init__(self, bearer_token, **kwargs):
        super().__init__(bearer_token, **kwargs)
        self.db = TweetData()
        self.buffer = deque()
        self.buffer_limit = 5

    def start_stream(self, rules, autostop=None):
        """

        :param autostop: after "autostop" seconds the connection will stop automatically
        :param rules: the rules for filter streaming data
        :return:
        """
        def auto_stop():
            time.sleep(autostop)
            self.stop_stream()
            print("Auto-disconnecting after " + str(autostop) + " seconds....")

        stream_rules = tweepy.StreamRule(value=rules)
        self.add_rules(stream_rules)
        if autostop is not None:
            t1 = Thread(target=auto_stop)
            t1.start()
        self.filter(tweet_fields=['created_at'], expansions='author_id', user_fields=['location'])

    def stop_stream(self):
        self.clean_rules()
        self.disconnect()

    def clean_rules(self):
        response = self.get_rules()
        for r in response.data:
            self.delete_rules(r.id)

    def on_data(self, raw_data):
        data = json.loads(raw_data)
        self.buffer.append(data)

        if len(self.buffer) > self.buffer_limit:
            t2 = Thread(target=self.clear_buffer)
            t2.start()

    def clear_buffer(self):
        while True:
            try:
                self.db.insert_db(self.buffer.popleft())
            except IndexError:
                break


if __name__ == "__main__":
    rules = 'doctor strange lang:en'
    d = TweetStream(bearer_token)
    d.start_stream(rules, autostop=50)
    r = d.get_rules()
    print(r)
