import tweepy
import datetime
import pymongo
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy.extra.rate_limiter import RateLimiter
from dateutil import parser
import time


# the passwords needed to get access in the twitter api
consumer_key = "your consumer key here"
consumer_secret = "your consumer key secret here"
access_token = "your access token here"
access_token_secret = "your access token secret here"
bearer_token = "your bearer token here"


class TweetData(object):
    def __init__(self):
        self.db_link = self.get_connection()
        self.collections = {}
        self.db = self.db_link["twitter"]
        self.geo_locator = Nominatim(user_agent="tweets_location")

    def stream_data(self):
        """
        search data from tweepy api through specific keywords
        :return: data-->dic
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

    def init_db(self, movie):
        """
        initialize the database "twitter" with collection "records"
        with attributes: time, location, txt
        :return: records_collection
        """
        records_collection = self.db[movie]
        self.collections[movie] = records_collection
        return records_collection

    def insert_db(self, data, movie):
        """
        insert new tweets into the database
        :return:
        """
        users = {u["id"]: u for u in data['includes']['users']}
        user_id = data['data']['author_id']
        user = users[user_id]

        if 'location' in user:
            location = user['location']
        else:
            location = None

        data['data']['location'] = location
        date = data['data']['created_at']
        if type(date) != datetime.datetime:
            data['data']['created_at'] = parser.parse(date)

        # city = None
        # country = None
        # if location:
        #     raw_data = self.lookup_latitude(location)
        #     if raw_data:
        #         address = self.lookup_location(raw_data)
        #         city = address.get('state', None)
        #         country = address.get('country_code', None)
        #
        # data['data']['city'] = city
        # data['data']['country'] = country

        print(data['data'])
        self.collections[movie].insert_one(data['data'])

    def extract_db_bylocation(self, movie, country):
        """
        extract needed data records from database
        :return:
        """
        query = {"country": country}
        rst = self.db[movie].find(query)
        for i in rst:
            print(i)

    def extract_db_bytime(self, movie, start_time, end_time):
        """
        extract data from database by time range
        :param movie: the name of movie (string)
        :param start_time: the start time of the time range(datetime)
        :param end_time: the end time of the time range(datetime)
        :return: a dictionary of data records
        """
        query = {"created_at": {"$gte": start_time,
                                "$lt": end_time}}

        rst = self.db[movie].find(query)
        output = {}
        for i in rst:
            output[i['_id']] = i
        return output

    def clear_db(self, movie):
        """
        drop the specific movie collection from database
        :param movie: the collection name(string
        :return: None
        """
        if self.collections[movie].drop():
            print("Collection "+movie+" has been dropped...")

    # def lookup_latitude(self, location):
    #     try:
    #         data = self.geo_locator.geocode(location, language='en')
    #     except GeocoderTimedOut as e:
    #         return None
    #     return data
    #
    # def lookup_location(self, data):
    #     latitude = data.raw.get("lat")
    #     longitude = data.raw.get("lon")
    #     location = self.geo_locator.reverse(latitude + "," + longitude, language='en')
    #     address = location.raw['address']
    #     return address


if __name__ == "__main__":
    d = TweetData()
    rst = d.extract_db_bytime("spider man", datetime.datetime(2022, 4, 23, 20, 3, 30),
                              datetime.datetime(2022, 4, 23, 20, 52, 58))
