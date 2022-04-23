import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import random



# display: {"location": (score, location, time, number of tweets)} ->updated every 10 seconds
# bar graph with countries on x axis
# moving line graph (x axis (time) moves)

current_ratings={"usa": [0.73, "usa", "22-march, 2022", 30], "canada": [0.64, "canada", "22-march, 2022", 20], "mexico": [0.85, "mexico", "22-march, 2022", 5] }

###################################### plot bar graphs #########################################################
# fig = plt.figure()
# ax = fig.add_subplot(111)
#
# rand_change=0
# for _ in range(10):
#
#     for key in current_ratings:
#         if (random.uniform(0,1)<0.5):
#             rand_change=-0.05
#         else:
#             rand_change=0.05
#         current_ratings[key][0]+=rand_change
#
#     ax.bar(["usa", "canada", "mexico"], [current_ratings["usa"][0], current_ratings["canada"][0], current_ratings["mexico"][0]])
#     ax.set_title ('Real-Time Sentiment Ratings Across Different Countries')
#     plt.xlabel('Country')
#     plt.ylabel('Sentiment Rating')
#     plt.pause(0.5)
# plt.show()


###################################### plot line graph #########################################################
x_data, y_data = [], []
figure = plt.figure()
line, = plt.plot_date(x_data, y_data, '-')
plt.xlabel('Time')
plt.ylabel('Sentiment Rating')
plt.title('Real-Time Overall Sentiment Rating')

def update(frame):
    x_data.append(datetime.now())
    y_data.append(random.uniform(0, 1))
    line.set_data(x_data, y_data)
    figure.gca().relim()
    figure.gca().autoscale_view()
    return line,

animation = FuncAnimation(figure, update, interval=500)

plt.show()
