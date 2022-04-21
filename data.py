import tweepy
import pymongo
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# the passwords needed to get access in the twitter api
consumer_key = "your consumer key here"
consumer_secret = "your consumer key secret here"
access_token = "your access token here"
access_token_secret = "your access token secret here"
bearer_token = "your bearer token here"


class TweetData(object):
    def __init__(self):
        self.db_link = self.get_connection()
        self.db = self.init_db()


    def stream_data(self):
        """
        search data from tweepy api through specific keywords
        :return: data-->dic
        """

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
        users = {u["id"]: u for u in data['includes']['users']}
        user_id = data['data']['author_id']
        user = users[user_id]

        if 'location' in user:
            location = user['location']
        else:
            location = None

        data['data']['location'] = location

        city = None
        country = None
        if location:
            address = self.lookup_location(location)
            if address:
                city = address.get('state', None)
                country = address.get('country_code', None)

        data['data']['city'] = city
        data['data']['country'] = country

        print(data['data'])
        self.db.insert_one(data['data'])

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

    def lookup_location(self, location):
        geo_locator = Nominatim(user_agent="tweets_location")
        try:
            data = geo_locator.geocode(location, language='en')
            if data:
                latitude = data.raw.get("lat")
                longitude = data.raw.get("lon")
                location = geo_locator.reverse(latitude + "," + longitude, language='en')
                address = location.raw['address']
            else:
                address = None
        except GeocoderTimedOut:
            return None
        return address


# if __name__ == "__main__":
