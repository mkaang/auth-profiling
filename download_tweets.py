import pandas as pd

import tweepy

from tqdm.auto import tqdm
from glob import glob
from time import sleep

def get_all_tweets(screen_name):
    
    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # Create API object
    api = tweepy.API(auth)
    
    #initialize a list to hold all the tweepy Tweets
    alltweets = []  
    
    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(screen_name = screen_name,count=200)
    
    #save most recent tweets
    alltweets.extend(new_tweets)
    
    #save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1
    
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        #print(f"getting tweets before {oldest}")
        
        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1
        
        #print(f"...{len(alltweets)} tweets downloaded so far")
        print("{:.2f}".format(len(alltweets)/3220), end="\r")
              
    #transform the tweepy tweets into a 2D array that will populate the csv 
    outtweets = [[tweet.id_str, tweet.created_at, tweet.text, tweet.entities, tweet.retweeted] for tweet in alltweets]
    
    pd.DataFrame(data=outtweets).to_csv('tweets/new_' + screen_name + '_tweets.csv', index=False)

if __name__ == '__main__':
    CONSUMER_KEY = None
    CONSUMER_SECRET = None
    ACCESS_TOKEN = None
    ACCESS_TOKEN_SECRET = None

    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    # Create API object
    api = tweepy.API(auth)

    usernames = pd.read_csv('data/accounts_v4.csv', usecols=['username'])
    usernames_exist = [x.split('/')[1][4:-11] for x in glob("tweets/*")]
    usernames_not_exist = set(usernames.username) - set(usernames_exist)

    for userID in tqdm(usernames_not_exist):
        try:
            get_all_tweets(userID)
            sleep(5)
        except:
            pass