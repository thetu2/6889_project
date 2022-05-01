import time
from datetime import datetime, timedelta
from threading import Thread
from collections import deque
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import numpy as np

from data import TweetData
from utils import clean


class Sentiment(object):
    """
    A container for sentiment values that provides additional related meta-data.

    :param val: The sentiment value.
    :type val: numeric
    :param vol: The sentiment volume.
    :type vol: numeric
    :param start: The beginning of the measurement period.
    :type start: datetime
    :param end: The end of the measurement period.
    :type end: datetime

    :ivar value: The sentiment value.
    :ivar volume: The sentiment volume.
    :ivar start: The beginning of the measurement period.
    :ivar end: The end of the measurement period.
    """
    def __init__(self, val):
        self._val = val

    @property
    def value(self):
        return self._val

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

class TweetFeels(object):
    """
    The controller.

    :param credentials: A list of your 4 credential components.
    :param tracking: A list of keywords to track.
    :param db: A sqlite database to store data. Will be created if it doesn't
               already exist. Will append if it exists.
    :ivar lang: A list of languages to include in tweet gathering.
    :ivar buffer_limit: When the number of s in the buffer hits this limit
                        all tweets in the buffer gets flushed to the database.
    :ivar connected: Tells you if TweetFeels is connected and listening to
                     Twitter.
    :ivar sentiment: The real-time sentiment score.
    :ivar binsize: The fixed observation interval between new sentiment
                   calculations. (default = 60 seconds)
    :ivar factor: The fall-off factor used in real-time sentiment calculation.
                  (default = 0.99)
    """
    #_db_factory = (lambda db: TweetData(db))
    def __init__(self, starttime, endtime,tracking):
        d=TweetData()
        self.start=starttime
        self.end=endtime
        self.tracking = tracking
        self._feels=d.extract_db_bytime(self.tracking, self.start, self.end)
        self.lang = ['en']
        self._sentiment = Sentiment(0)
        self._factor = 0.99

    @property
    def factor(self):
        return self._factor

    @factor.setter  # The fall-off factor used in real-time sentiment calculation
    def factor(self, value):
        assert(value<=1 and value>0)
        self._factor = value

    @property  # overall sentiment score initialization
    def sentiment(self):
        sentiments = self.sentiments(bins=self._feels)
        for s in sentiments:
            pass
        return s

    def sentiments(self, bins, nans=False):
        """
        Provides a generator for sentiment values in ``delta_time`` increments.

        :param start: The start time at which the generator yeilds a value. If
                      not provided, generator will start from beginning of your
                      dataset.
        :type start: datetime
        :param end: The ending datetime of the series. If not provided,
                    generator will not stop until it reaches the end of your
                    dataset.
        :type end: datetime
        :param delta_time: The time length that each sentiment value represents.
                           If not provided, the generator will use the setting
                           configured by :class:`TweetFeels`.
        :type delta_time: timedelta
        :param nans: Determines if a nan will be yielded when no tweets are
                     observed within a bin.
        :type nans: boolean
        """
        self._sentiment = Sentiment(0)
        keys = list(bins.keys())
        # start yielding sentiment values
        sentiment = deque()
        for index in range(len(keys)):
            b=bins.get(keys[index])  # b is dictionary
            try:
                self._sentiment = sentiment.popleft()  # pop the left element
            except IndexError:
                pass

            latest = self._sentiment
            if len(b['text']) > 0:
                latest = self.model_sentiment(
                    b['text'], self._sentiment, self._factor
                    )
                sentiment.append(latest)  # push the grade to the deque
                # self._latest_calc = b.start #change to the next beginning time #changeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee b.start
                # Yield the latest element
                if len(b['text']) == 0 and nans:
                    yield Sentiment(np.nan)  #changeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                else:
                    yield sentiment[-1]

    def calculate_sentiment(self,data):
        _data=data
        t = clean(_data)
        cal_sentiment = SentimentIntensityAnalyzer().polarity_scores(t)
        return cal_sentiment

    def model_sentiment(self, b, s, fo=0.99):
        """
        Defines the real-time sentiment model given a dataframe of tweets.

        :param b: A ``TweetBin`` to calculate the new sentiment value.
        :param b: A dictionary
        :param s: The initial Sentiment to begin calculation.
        :param fo: Fall-off factor
        """
        newval = s.value
        if(len(b)>0):
            try:
                val = self.calculate_sentiment(b)  # the score is based on the followers need to change
                val=val.get('compound')
            except ZeroDivisionError:
                val = 0
            newval = s.value*fo + val*(1-fo)  # has the decreasing factor
        return Sentiment(newval)

if  __name__ == "__main__":
    d = TweetData()
    d.extract_db_bytime("spider man", datetime(2022, 4, 23, 18, 52, 15), datetime(2022, 4, 23, 18, 52, 58))
    spider_feels=TweetFeels(datetime(2022, 4, 23, 20, 2, 15), datetime(2022, 4, 23, 22, 52, 58), tracking='spider man')
    print(spider_feels.sentiment.value)
