import pandas as pd
from tools import preprocess, stem, concat_tweets
from TurkishStemmer import TurkishStemmer


if __name__ == '__main__':
    
    tweets = pd.read_csv("data/tweets.csv")
    labels = pd.read_csv("data/labels.csv")

    df = tweets.merge(labels, on='username', how='left')

    # Drop empty targets
    df.dropna(subset=['age'],inplace=True)
    df.dropna(subset=['gender'],inplace=True)

    # Split data according to users not tweets
    userlist_shuffled = pd.Series(df.username.unique()).sample(frac=1, random_state=26).reset_index(drop=True).tolist()

    mid = 4 * (len(userlist_shuffled) // 5)
    train = df[df.username.isin(userlist_shuffled[:mid])]
    test = df[df.username.isin(userlist_shuffled[mid:])]

    train.reset_index(drop=True, inplace=True)
    test.reset_index(drop=True, inplace=True)

    train["processed_text"] = preprocess(train.entities.tolist(), train.text.tolist())
    test["processed_text"] = preprocess(test.entities.tolist(), test.text.tolist())

    stemmer = TurkishStemmer()

    train["stemmed"] = stem(train["processed_text"].tolist(), stemmer)
    test["stemmed"] = stem(test["processed_text"].tolist(), stemmer)

    train.to_csv("tweets_prep_train.csv", index=False)
    test.to_csv("tweets_prep_test.csv", index=False)

    map_user_tweet_tr_stem = concat_tweets(train, 'stemmed')
    map_user_tweet_ts_stem = concat_tweets(test, 'stemmed')

    map_user_tweet_tr_process = concat_tweets(train, 'processed_text')
    map_user_tweet_ts_process = concat_tweets(test, 'processed_text')

    map_user_tweet_tr = map_user_tweet_tr_stem.merge(map_user_tweet_tr_process, on=["username"])
    map_user_tweet_ts = map_user_tweet_ts_stem.merge(map_user_tweet_ts_process, on=["username"])

    merged_tr = map_user_tweet_tr.merge(train, how="left", left_on=["username"], right_on=["username"])
    merged_ts = map_user_tweet_ts.merge(test, how="left", left_on=["username"], right_on=["username"])






