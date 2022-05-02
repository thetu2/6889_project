from datetime import datetime
#from threading import Thread
from collections import deque
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import numpy as np
import pandas as pd

from data import TweetData
from utils import clean


class Sentiment(object):
    """
    A container for sentiment values that provides additional related meta-data.

    :param val: The sentiment value.
    :type val: numeric

    :ivar value: The sentiment value.
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

    :param tracking: A keyword to track (string).
    :param starttime: database start time.
    :param endtime: database end time.
    :ivar lang: A list of languages to include in tweet gathering.
    :ivar sentiment: The real-time sentiment score.
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
        :param bins: database extracted by time
        :type bins: dictionary
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
            #print(b['joined_at'],b['followers_count'],b['created_at'])
            if len(b['text']) > 0:
                sentiment_score=self.calculate_sentiment(b['text']).get('compound')
                latest = self.model_sentiment(
                    b['text'], self._sentiment, self._factor
                    )
                sentiment.append(latest)  # push the grade to the deque
                # self._latest_calc = b.start #change to the next beginning time #changeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee b.start
                # Yield the latest element
                if len(b['text']) == 0 and nans:
                    sentiment_score=np.nan
                    yield Sentiment(np.nan)  
                else:
                    yield sentiment[-1]
                outputdf={"created_at":b['created_at'],"joined_at":b['joined_at'],"followers":b['followers_count'],"score":sentiment_score}
                df=pd.DataFrame(outputdf,index=[0])
                #print(df)
                #save real time score to csv
                df.to_csv(self.tracking+'.csv', mode='a', index=False,sep=',',header=False)

    def calculate_sentiment(self,data):
        _data=data
        t = clean(_data)
        cal_sentiment = SentimentIntensityAnalyzer().polarity_scores(t)
        return cal_sentiment

    def model_sentiment(self, b, s, fo=0.99):
        """
        Defines the real-time sentiment model given a dataframe of tweets.

        :param b: A tweet to calculate the new sentiment value.
        :param b: A string
        :param s: The initial Sentiment to begin calculation.
        :param fo: Fall-off factor
        """
        newval = s.value
        if(len(b)>0):
            try:
                val = self.calculate_sentiment(b)  # the score is based on the followers need to change
                val=val.get('compound')
                #print(val)
            except ZeroDivisionError:
                val = 0
            newval = s.value*fo + val*(1-fo)  # has the decreasing factor
        return Sentiment(newval)

if  __name__ == "__main__":
    tracking_list=['spider man', 'morbius', 'batman', 'fantastic beasts', 'northman', 'sonic the hedgehog', 'doctor strange']
    spider_feels=TweetFeels(datetime(2022, 4, 30, 00, 52, 19), datetime(2022, 4, 30, 00, 54, 00), tracking=tracking_list[0])
    print(spider_feels.sentiment.value)
