import tweepy

client = tweepy.Client(bearer_token='bearer token here')

# Replace with your own search query
query = 'fantastic beasts -is:retweet lang:en place_country:US'

tweets = client.search_recent_tweets(query=query, tweet_fields=['created_at'], max_results=20, expansions='author_id', user_fields=['name', 'location'])


# Get users list from the includes object
users = {u["id"]: u for u in tweets.includes['users']}

for tweet in tweets.data:
    if users[tweet.author_id]:
        user = users[tweet.author_id]
        print(user.location, tweet.data, '\n')

