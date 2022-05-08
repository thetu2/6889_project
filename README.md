# 6889_project
Description:
---
Our project is to create a system that can produce movie ratings based on real-time movie-related tweets on Twitter, therefore, providing users with information about movie selection before watching. <br>
Ideally, our project is able to use proper streaming algorithms and methods to retrieve real-time tweets from Twitter API, and do data processing and sampling with specific topics to filter out the valid tweets information from all the streamed data. After that, the ideal system can produce relatively accurate and reasonable real-time sentiment scores based on valid tweet information through suitable sentiment analysis algorithms. For example, when we type in the movie names we want to watch, the system will then retrieve tweet data from Twitter and display the real-time movie ratings within the range from 1 to 10 in the resulting graph, based on the sampled tweets streamed from Twitter. So naturally, data streaming, data storage, tweet sentiment analysis, and result display are the four most important parts of our system.<br>

Features:
---
Realized real-time tweet streaming based on specific tracking keywords; <br>
Built cloud MongoDB database and movie collections; <br>
Accomplished sentiment analysis on real-time tweets;<br>
Obtained movie rating considering followers, age, and timestamp; <br>
Displayed real-time sentiment score based on pyspark using weighting and averaging.<br>

Directory：
---
```./
├── movie sentiment //real time rating results 
├── README.md
├── data.py         //save tweet to mongodb
├── display.py      //real time display using pyspark
├── sentiment.py    //sentiment analysis 
├── steam.py        //get streaming tweet using tweepy
└── utils.py        //clean tweet (preprocess)

