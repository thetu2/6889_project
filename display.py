import numpy as np
from datetime import datetime
import time
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import random

##TODO: simulatenous graphs

current_ratings={"0-1": [0.73, "na", "4/30/22 13:47", 30], "2-3": [0.64, "na", "4/30/22 13:47", 20], "4-5": [0.85, "na", "4/30/22 13:47", 10], "6+": [0.5, "na", "4/30/22 13:47", 5]}

###################################### plot bar graphs #########################################################
fig = plt.figure(1)
for _ in range(10):
    plt.clf() #clear plot contents but keep window open
    ax = fig.add_subplot(111)
    keys=list(current_ratings.keys()) #get current set of keys
    values=[]
    curr_time=current_ratings[keys[0]][2] #get current time from first key
    for key in keys: #randomly update keys (for demo)
        if (random.uniform(0,1)<0.5):
            rand_change=-0.05
        else:
            rand_change=0.05
        current_ratings[key][0]+=rand_change
        values.append(current_ratings[key][0])

    colors=[]
    for val in values: # color code the max and min ratings
        if (val==max(values)):
            colors.append('green')
        elif (val==min(values)):
            colors.append ('red')
        else:
            colors.append ('grey')

    ax.bar(keys, values, color=colors)
    fig.suptitle('Real-Time Sentiment Ratings Across Different Account Ages', fontsize=11, fontweight='bold')
    ax.set_title ('Current time: ' + curr_time)
    plt.xlabel('Account Age (Years)')
    plt.ylabel('Sentiment Rating')
    plt.ylim([0,1])

    plt.pause(0.7)
plt.show()


###################################### plot line graph #########################################################
#plot setup
x_data, y_data = [], []
figure = plt.figure(2)
line, = plt.plot_date(x_data, y_data, '-')
plt.xlabel('Time')
plt.ylabel('Sentiment Rating')
plt.suptitle('Average Sentiment Rating', fontsize=14, fontweight='bold')
#average calculation setup
tweet_count_total=0 # total number of tweets seen
ratings_avg=0 #overall average rating so far

def update(frame):
    global tweet_count_total
    global tweet_count_window
    global rating_product
    global ratings_avg

    keys = list(current_ratings.keys())
    for key in keys:  # randomly update keys (for demo)
        if (random.uniform(0, 1) < 0.5):
            rand_change = -0.05
        else:
            rand_change = 0.05
        current_ratings[key][0] += rand_change

    rating_product=0 #temporary product of ratings * rating count
    tweet_count_window=0 # number of tweets in current window
    for key in keys:  ## calculate total number of tweets in this time window
        rating_product += current_ratings[key][0] * current_ratings[key][-1]  # ratings * # of ratings
        tweet_count_window += current_ratings[key][-1]  # add to ratings count in current window

    ratings_window = rating_product / tweet_count_window  # for testing only
    print(ratings_window)
    ratings_avg = (ratings_avg * tweet_count_total + rating_product) / (tweet_count_total + tweet_count_window)
    tweet_count_total += tweet_count_window  # new total tweet count

    plt.title('Total Tweets on movie: '+ str(tweet_count_total))

    x_data.append(datetime.now())
    y_data.append(ratings_avg)
    line.set_data(x_data, y_data)
    figure.gca().relim()
    figure.gca().autoscale_view()
    return line,

animation = FuncAnimation(figure, update, interval=200)
plt.show()


