import tweepy
from data import TweetData
from threading import Thread
import time
import json

# the passwords needed to get access in the twitter api
consumer_key = "p3G1cqeOLxEtVGoYhhRKjSADO"
consumer_secret = "3uSVuDS7iP7sdlsUxVpnBpET0WCBRXr5LnmQl84g2puT3xgQjZ"
access_token = "1508561637367861252-AFUs4bKLlPQ1sOkvRq1xeQH3yBx2N4"
access_token_secret = "Z1VjLxjD2OEquHdtFD0dhqSenVZ48jE0rzCp3svhVyqae"
bearer_token = "AAAAAAAAAAAAAAAAAAAAANxtbgEAAAAArzLC%2FeQ8hn1FEPtHsLdTdceGRDc%3D7jNiD8tfpYHeEEPKCWKnXaqGvATDi0XqJ95EL0Pgt3V17egAY1"


class TweetStream(tweepy.StreamingClient):
    def __init__(self, bearer_token, **kwargs):
        super().__init__(bearer_token, **kwargs)
        self.db = TweetData()

    def start_stream(self, rules, rule_id, autostop=None):
        """

        :param autostop: after "autostop" seconds the connection will stop automatically
        :param rules: the rules for filter streaming data
        :return:
        """
        def auto_stop():
            time.sleep(autostop)
            self.stop_stream()
            print("Auto-disconnecting after " + str(autostop) + " seconds....")

        stream_rules = tweepy.StreamRule(value=rules, id=rule_id)
        self.add_rules(stream_rules)
        if autostop is not None:
            t = Thread(target=auto_stop)
            t.start()
        self.filter()

    def stop_stream(self):
        self.disconnect()

    def on_data(self, raw_data):
        data = json.loads(raw_data)
        self.db.insert_db(data['data'])


if __name__ == "__main__":
    rules = 'Spiderman'
    rule_id = 'movie2'
    d = TweetStream(bearer_token)
    d.start_stream(rules, rule_id, autostop=10)

