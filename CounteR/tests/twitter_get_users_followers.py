import tweepy

from counterUtilites import setConfig

cfg = setConfig(file="app-config.yml")


bearer_token = cfg["twitter"].get("bearer_token")
access_token = cfg["twitter"].get("access_token")
access_token_secret = cfg["twitter"].get("access_token_secret")


def create_session(token):
    return tweepy.Client(bearer_token=token,
                         wait_on_rate_limit=True,
                         access_token=access_token,
                         access_token_secret=access_token_secret)


def get_twitter_response(screen_name, tweepy_client):
    return tweepy_client.get_users_followers(id=screen_name, max_results=1, user_fields=['id', 'name', 'username'], pagination_token = None)


tweepy_session = create_session(bearer_token)

response = get_twitter_response('ToscaMusk', tweepy_session)
print(response)


# When authenticating requests to the Twitter API v2 endpoints, you must use keys and tokens from a Twitter developer App that is attached to a Project. You can create a project via the developer portal.
# "https://twittercommunity.com/t/when-authenticating-requests-to-the-twitter-api-v2-endpoints-you-must-use-keys-and-tokens-from-a-twitter-developer-app-that-is-attached-to-a-project-you-can-create-a-project-via-the-developer-portal/200690"