import tweepy

from counterUtilites import setConfig

cfg = setConfig(file="app-config.yml")


bearer_token = cfg["twitter"].get("bearer_token")
access_token = cfg["twitter"].get("access_token")
access_token_secret = cfg["twitter"].get("access_token_secret")
api_key = cfg["twitter"].get("api_key")
api_key_sec = cfg["twitter"].get("api_key_sec")

auth = tweepy.OAuthHandler(api_key, api_key_sec)

auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


# screen_name = "geeksforgeeks"
# statuses = api.user_timeline(screen_name=screen_name)
# print(str(len(statuses)) + " number of statuses have been fetched.")

screen_name = 'geeksforgeeks'
# tweets = api.user_timeline(id=user_id, count=10, tweet_mode='extended')
tweets = api.user_timeline(screen_name=screen_name, count=10, tweet_mode='extended')

print(tweets)
