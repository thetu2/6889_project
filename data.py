import tweepy
import pymongo

# the passwords needed to get access in the twitter api
consumer_key = "p3G1cqeOLxEtVGoYhhRKjSADO"
consumer_secret = "3uSVuDS7iP7sdlsUxVpnBpET0WCBRXr5LnmQl84g2puT3xgQjZ"
access_token = "1508561637367861252-AFUs4bKLlPQ1sOkvRq1xeQH3yBx2N4"
access_token_secret = "Z1VjLxjD2OEquHdtFD0dhqSenVZ48jE0rzCp3svhVyqae"
bearer_token = "AAAAAAAAAAAAAAAAAAAAANxtbgEAAAAArzLC%2FeQ8hn1FEPtHsLdTdceGRDc%3D7jNiD8tfpYHeEEPKCWKnXaqGvATDi0XqJ95EL0Pgt3V17egAY1"

# initialize the client agent
# client = tweepy.Client(bearer_token, consumer_key, consumer_secret, access_token, access_token_secret, return_type=dict)
# rst = client.search_recent_tweets("Jackson Wang", max_results=20)
# for i in rst['data']:
#     print(i)


class TweetData(object):
    def __init__(self):
        self.db_link = self.get_connection()
        self.db = self.init_db()
        self.client = tweepy.Client(bearer_token,
                                    consumer_key,
                                    consumer_secret,
                                    access_token,
                                    access_token_secret,
                                    return_type=dict)

    def stream_data(self):
        """
        search data from tweepy api through specific keywords
        :return: data-->dic
        """
        rst = self.client.search_recent_tweets("Jackson Wang", max_results=20)
        return rst["data"]

    def filter_data(self):
        """
        select useful data
        :return:
        """

    def get_connection(self):
        """
        get the connection with mongodb
        through the url link
        :return: client
        """
        client = pymongo.MongoClient(
                "mongodb+srv://wz2581:Zwxangel.72700@cluster0.xwejf.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
            )
        return client

    def init_db(self):
        """
        initialize the database "twitter" with collection "records"
        with attributes: time, location, txt
        :return: records_collection
        """
        records_collection = self.db_link["twitter"]["records"]
        return records_collection

    def insert_db(self, data):
        """
        insert new tweets into the database
        :return:
        """
        for i in data:
            self.db.insert_one(i)

    def extract_db(self):
        """
        extract needed data records from database
        :return:
        """

    def clear_db(self):
        """
        clear the database
        :return:
        """


if __name__ == "__main__":
    data = TweetData()
    for i in data.stream_data():
        data.db.insert_one(i)




