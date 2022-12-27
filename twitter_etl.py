import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
def run_twitter_etl():
    access_key = "tYJp1wgdA5tD7oMN53YMO0UYc"
    access_secret = "4NaMYLgfihyNXHMkDWt3b0WErZ5Qnw9hvnwA8b1wZICUJsQUfg"
    consumer_key = "1576887156794339328-VIP1QAnEEveM6s2iyHDIa99YgOiB6c"
    consumer_secret = "QVmGXRlC6Du4WRQfv073qJaGyF6VQt2bn9oo4oQT95isv"
    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)
    # Creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',
                                # 200 is the maximum allowed count
                                count=200,
                                include_rts = False,
                                # Necessary to keep full_text
                                # otherwise only the first 140 words are e
                                tweet_mode = 'extended'
                                )
    print(tweets)
    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
            
        list.append(refined_tweet)

        df = pd.DataFrame(list)
        df.to_csv('s3://simrah-airflow/refined_tweets.csv')
