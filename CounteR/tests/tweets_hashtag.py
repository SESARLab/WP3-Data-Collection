import tweepy

from counterUtilites import setConfig

cfg = setConfig(file="app-config.yml")

bearer_token = cfg["twitter"].get("bearer_token")
access_token = cfg["twitter"].get("access_token")
access_token_secret = cfg["twitter"].get("access_token_secret")
api_key = cfg["twitter"].get("api_key")
api_key_sec = cfg["twitter"].get("api_key_sec")


def printtweetdata(n, ith_tweet):
    print()
    print(f"Tweet {n}:")
    print(f"Username:{ith_tweet[0]}")
    print(f"Description:{ith_tweet[1]}")
    print(f"Location:{ith_tweet[2]}")
    print(f"Following Count:{ith_tweet[3]}")
    print(f"Follower Count:{ith_tweet[4]}")
    print(f"Total Tweets:{ith_tweet[5]}")
    print(f"Retweet Count:{ith_tweet[6]}")
    print(f"Tweet Text:{ith_tweet[7]}")
    print(f"Hashtags Used:{ith_tweet[8]}")


def scrape(words, date_since, numtweet):
    tweets = tweepy.Cursor(api.search_tweets,
                           words, lang="en",
                           since_id=date_since,
                           tweet_mode='extended').items(numtweet)

    list_tweets = [tweet for tweet in tweets]

    i = 1

    for tweet in list_tweets:
        username = tweet.user.screen_name
        description = tweet.user.description
        location = tweet.user.location
        following = tweet.user.friends_count
        followers = tweet.user.followers_count
        totaltweets = tweet.user.statuses_count
        retweetcount = tweet.retweet_count
        hashtags = tweet.entities['hashtags']

        try:
            text = tweet.retweeted_status.full_text
        except AttributeError:
            text = tweet.full_text
        hashtext = list()
        for j in range(0, len(hashtags)):
            hashtext.append(hashtags[j]['text'])

        ith_tweet = [username, description,
                     location, following,
                     followers, totaltweets,
                     retweetcount, text, hashtext]

        printtweetdata(i, ith_tweet)
        i = i + 1


if __name__ == '__main__':
    auth = tweepy.OAuthHandler(api_key, api_key_sec)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    words = 'Programming'
    date_since = "2020-01-01"

    numtweet = 100
    scrape(words, date_since, numtweet)
