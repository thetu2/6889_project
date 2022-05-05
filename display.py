import numpy as np
from datetime import datetime
import time
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import random
import datetime

import pyspark
import re
import shlex
import csv
import time
from random import randrange

sc = pyspark.SparkContext(appName="hw1")

#read input CSV files
batman=sc.textFile("movie_sentiment/batman.csv")
fantastic=sc.textFile("movie_sentiment/fantastic beasts.csv")
morbius=sc.textFile("movie_sentiment/morbius.csv")
northman=sc.textFile("movie_sentiment/northman.csv")
sonic=sc.textFile("movie_sentiment/sonic the hedgehog.csv")

#split using comma as delimiters
batman = batman.map(lambda x: x.split(","))
fantastic = fantastic.map(lambda x: x.split(","))
morbius = morbius.map(lambda x: x.split(","))
northman = northman.map(lambda x: x.split(","))
sonic = sonic.map(lambda x: x.split(","))


#get account age, normalize ratings to 0-10
batman=batman.map(lambda x: [x[0], 2022-int(x[1][0:4]), int(x[2]), (float(x[3])+1)*5])
fantastic=fantastic.map(lambda x: [x[0], 2022-int(x[1][0:4]), int(x[2]), (float(x[3])+1)*5])
morbius=morbius.map(lambda x: [x[0], 2022-int(x[1][0:4]), int(x[2]), (float(x[3])+1)*5])
northman=northman.map(lambda x: [x[0], 2022-int(x[1][0:4]), int(x[2]), (float(x[3])+1)*5])
sonic=sonic.map(lambda x: [x[0], 2022-int(x[1][0:4]), int(x[2]), (float(x[3])+1)*5])

# print("batman:", batman.count())
# print("batman:",batman.take(3))
# print("fantastic:", fantastic.count())
# print("fantastic:",fantastic.take(3))

movies=['Batman', 'Fantastic Beasts', 'Morbius', 'Northman', 'Sonic Hedgehog']
window_time=10
end=0
movie_idx={}
movie_ratings={}
curr_tweets= {}
window_start_time=datetime.datetime(2000,1,1,0,0,0,0) #initialize window start time for first window
curr_window_rating={}
curr_window_rating_followers={}
window_rating_avg=[0.0]*len(movies)
cum_avg={}
temp_avg={}
volume_trend={} #tracks # of tweets about movie in all time windows
volume_window={} #tracks # of tweets about movie in the given time window
cum_x=[]

#init storage elements
for movie in movies:
    movie_idx[movie]=1 #which tweet index to read from, starts at 1
    movie_ratings[movie]=[0.0]*4 #ratings for each movie
    curr_window_rating[movie]=[] #current list of ratings in window
    curr_window_rating_followers[movie] = []  # # of follower for current list of ratings in window
    cum_avg[movie]=[] #initialize average as 5.0 for each movie
    temp_avg[movie] = []  # initialize average as 5.0 for each movie
    volume_trend[movie] = []  # initialize average as 5.0 for each movie
    volume_window[movie]=0


#figure setup
fig = plt.figure(figsize=(20,13))
color_batman=['black']*4
color_fant=['grey']*4
color_morbius=['red']*4
color_northman=['green']*4
color_sonic=['blue']*4

batman_l=batman.collect()
fant_l=fantastic.collect()
morb_l=morbius.collect()
north_l=northman.collect()
sonic_l=sonic.collect()
#while (not end):
for _ in range(500):
    #################### Process Tweets ################################
    #######get latest tweets######
    # curr_tweets['Batman']=batman.take(movie_idx['Batman'])[-1]
    # curr_tweets['Fantastic Beasts']=fantastic.take(movie_idx['Fantastic Beasts'])[-1]
    # curr_tweets['Morbius']=batman.take(movie_idx['Morbius'])[-1]
    # curr_tweets['Northman']=fantastic.take(movie_idx['Northman'])[-1]
    # curr_tweets['Sonic Hedgehog']=batman.take(movie_idx['Sonic Hedgehog'])[-1]

    curr_tweets['Batman']=batman_l[movie_idx['Batman']-1]
    curr_tweets['Fantastic Beasts'] = fant_l[movie_idx['Fantastic Beasts'] - 1]
    curr_tweets['Morbius'] = morb_l[movie_idx['Morbius'] - 1]
    curr_tweets['Northman'] = north_l[movie_idx['Northman'] - 1]
    curr_tweets['Sonic Hedgehog'] = sonic_l[movie_idx['Sonic Hedgehog'] - 1]

    # find earliest tweets that has not been processed
    for idx, key in enumerate(movies):
        if (idx==0): #1st index
            earliest_tweet_time=curr_tweets[key][0]
            earliest_movie=key
        else:
            if (curr_tweets[key][0]<earliest_tweet_time):
                earliest_tweet_time = curr_tweets[key][0]
                earliest_movie = key

    movie_idx[earliest_movie] += 1 #earliest tweet has been processed
    if (curr_tweets[earliest_movie][1]<2): #acc between 0-1 yo
        movie_ratings[earliest_movie][0]=curr_tweets[earliest_movie][-1] #assign ratings
    elif (curr_tweets[earliest_movie][1]<5): #acc between 2-4 yo
        movie_ratings[earliest_movie][1] = curr_tweets[earliest_movie][-1]
    elif (curr_tweets[earliest_movie][1]<10): #acc between 5-9 yo
        movie_ratings[earliest_movie][2] = curr_tweets[earliest_movie][-1]
    else: #10+ yo acc
        movie_ratings[earliest_movie][3] = curr_tweets[earliest_movie][-1]

    #print (movie_ratings)

    #######Get window_time second average of tweets########
    tweet_time=datetime.datetime.fromisoformat(curr_tweets[earliest_movie][0])
    time_passed=tweet_time-window_start_time #time passed since window start
    if (time_passed.total_seconds()>window_time): #20 second window closes
        #can use followers as weights
        for idx, movie in enumerate(movies):
            if (curr_window_rating[movie]): # check to make sure len is not zero
                ratings_sum=0
                for idx2, rating in enumerate(curr_window_rating[movie]):
                    ratings_sum+=rating*curr_window_rating_followers[movie][idx2]
                window_rating_avg[idx]=ratings_sum / sum(curr_window_rating_followers[movie]) #calculate weighted average rating in window

            curr_window_rating_followers[movie] = []  # clear follower list
            # Calculate cumulative movie score
            if (not cum_avg[movie]): #initial condition
                cum_avg[movie].append(5.0)
                temp_avg[movie].append(0.0)
            elif (not curr_window_rating[movie]): # no ratings in current time window
                cum_avg[movie].append(cum_avg[movie][-1])
                temp_avg[movie].append(temp_avg[movie][-1])
            else:
                cum_avg[movie].append(cum_avg[movie][-1]*0.8+0.2*window_rating_avg[idx])
                temp_avg[movie].append(window_rating_avg[idx])
            curr_window_rating[movie] = []  # clear movie ratings list
            volume_trend[movie].append(volume_window[movie]) #save tweet count for given movie
            volume_window[movie]=0 # clear tweet count for current window
        window_start_time=tweet_time #window starts at this current tweet's time
        cum_x.append(str(window_start_time))
    else: # not yet 20 seconds, keep adding ratings
        curr_window_rating[earliest_movie].append(curr_tweets[earliest_movie][-1]) #save current rating
        volume_window[earliest_movie]+=1
        #quantize follower count into weights
        if (curr_tweets[earliest_movie][2] <3): #1st quartile
            curr_window_rating_followers[earliest_movie].append(1)
        elif (curr_tweets[earliest_movie][2] <6): #2st quartile
            curr_window_rating_followers[earliest_movie].append(2)
        elif (curr_tweets[earliest_movie][2] <18): #3rd quartile
            curr_window_rating_followers[earliest_movie].append(3)
        else:
            curr_window_rating_followers[earliest_movie].append(4)

    # print(curr_tweets)
    # print("Avg rating: ", window_rating_avg)
    # print("current rating: ", curr_window_rating)
    # print("cum_avg: ", cum_avg)

    #################### Plot Data ################################
    plt.clf() #clear plot contents but keep window open
    ax = fig.add_subplot(231)
    acc_age=['0-1', '2-4', '5-9', '10+']

    x_axis = np.arange(len(acc_age))

    ax.bar(x_axis-0.3, movie_ratings['Batman'], 0.15, color=color_batman, label='Batman') #1st float controls x location of bars, 2nd float controls width of bar
    ax.bar(x_axis - 0.15, movie_ratings['Fantastic Beasts'], 0.15, color=color_fant, label='Fantastic Beasts')
    ax.bar(x_axis + 0.0, movie_ratings['Morbius'], 0.15, color=color_morbius, label='Morbius')
    ax.bar(x_axis + 0.15, movie_ratings['Northman'], 0.15, color=color_northman, label='Northman')
    ax.bar(x_axis + 0.3, movie_ratings['Sonic Hedgehog'], 0.15, color=color_sonic, label='Sonic Hedgehog')

    ax.set_xticks(x_axis, acc_age)
    ax.legend()
    ax.set_title('Real-Time Movie Sentiment Ratings Across Different Account Ages', fontsize=10, fontweight='bold')
    #ax.text(0, 9, 'Tweet time: ' + earliest_tweet_time)
    ax.set_xlabel('Account Age (Years)')
    ax.set_ylabel('Sentiment Rating')
    ax.set_ylim([0,10])

    #################### Plot Data 2 ################################
    ax2 = fig.add_subplot(232)
    colors=['black', 'grey', 'red', 'green', 'blue']
    width=[0.7]*len(movies)
    ax2.bar(['Batman', 'Fantastic Beasts', 'Morbius', 'Northman', 'Sonic'], window_rating_avg, color=colors, width=width)
    ax2.set_title('Average Movie Rating in 10 Second Tumbling Window \n (Follower Weighted)', fontsize=10, fontweight='bold')
    #ax2.text(0, 9, 'Window start time: ' + str(window_start_time))
    ax2.set_xlabel('Movie Title')
    ax2.set_ylabel('Sentiment Rating')
    ax2.set_ylim([0,10])


    #################### Plot Data 3 ################################
    ax3 = fig.add_subplot(236)
    for idx, movie in enumerate(movies):
        ax3.plot(cum_x, cum_avg[movie], color=colors[idx], label=movie )
    ax3.set_title('Cumulative Movie Rating', fontsize=10, fontweight='bold')
    #ax3.text(0, 9, 'Window start time: ' + str(window_start_time))
    ax3.legend()
    ax3.set_xlabel('Time')
    ax3.set_ylabel('Sentiment Rating')
    ax3.set_ylim([0,10])
    #ax3.set_xticks(np.arange(0, len(cum_x) + 1, min(2, len(cum_x))))
    ax3.set_xticks(np.arange(0, len(cum_x) + 1, int(len(cum_x)/3) if int(len(cum_x)/3) else 1))
    plt.xticks(rotation=10)


    #################### Plot Data 4 ################################
    ax4 = fig.add_subplot(235)
    for idx, movie in enumerate(movies):
        ax4.plot(cum_x, temp_avg[movie], color=colors[idx], label=movie )
    ax4.set_title('Average Movie Rating in 10 Second Tumbling Window \n with Historical Data', fontsize=10, fontweight='bold')
    #ax3.text(0, 9, 'Window start time: ' + str(window_start_time))
    ax4.legend()
    ax4.set_xlabel('Time')
    ax4.set_ylabel('Sentiment Rating')
    ax4.set_ylim([0,10])
    #ax3.set_xticks(np.arange(0, len(cum_x) + 1, min(2, len(cum_x))))
    ax4.set_xticks(np.arange(0, len(cum_x) + 1, int(len(cum_x)/3) if int(len(cum_x)/3) else 1))
    plt.xticks(rotation=10)

    #################### Plot Data 5 ################################
    ax5 = fig.add_subplot(234)
    for idx, movie in enumerate(movies):
        ax5.plot(cum_x, volume_trend[movie], color=colors[idx], label=movie )
    ax5.set_title('Tweet Volumes Over Time', fontsize=10, fontweight='bold')
    ax5.legend()
    ax5.set_xlabel('Time')
    ax5.set_ylabel('Number of Tweets/10s')
    #ax5.set_ylim([0,10])
    ax5.set_xticks(np.arange(0, len(cum_x) + 1, int(len(cum_x)/3) if int(len(cum_x)/3) else 1))
    plt.xticks(rotation=10)

    fig.suptitle('Tweet time: ' + earliest_tweet_time+"\n"+'Window start time: ' + str(window_start_time))
    plt.subplots_adjust(hspace=0.3) #increase space btw top and bottom figures
    plt.pause(0.01)
plt.show()
