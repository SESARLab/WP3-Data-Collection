import json
import os
import sys
import unittest
from http import HTTPStatus
from pathlib import Path

import requests
import tweepy as tw
import counterUtilites as myutil


sys.path.append(str(Path(".").absolute().parent))


tweet_fields = [
    "attachments",
    "author_id",
    "context_annotations",
    "conversation_id",
    "created_at",
    "entities",
    "geo",
    "id",
    "in_reply_to_user_id",
    "lang",
    "public_metrics",
    "possibly_sensitive",
    "referenced_tweets",
    "reply_settings",
    "source",
    "text",
    "withheld",
]

user_fields = [
    "created_at",
    "description",
    "entities",
    "id",
    "location",
    "name",
    "pinned_tweet_id",
    "profile_image_url",
    "protected",
    "public_metrics",
    "url",
    "username",
    "verified",
    "withheld",
]

media_fields = ["preview_image_url", "url", "variants", "duration_ms"]


class TwitterConnector:
    DIRECTORY = "counter/"

    def __init__(
        self,
        access_token=None,
        access_token_secret=None,
        api_key=None,
        api_key_sec=None,
        bearer_token=None,
        account_type=None,
        username=None,
        password=None,
        label=None,
    ):

        self.cfg = myutil.setConfig(file="app-config.yml")

        self.maxResults = self.cfg["twitter"].get("maxResults", 100)
        self.timeout = self.cfg["twitter"].get("timeout", 9000000000000000)

        self.account_type = str(self.cfg["twitter"]["account_type"]) if not account_type else account_type
        self.username = str(self.cfg["twitter"]["username"]) if not username else username
        self.password = str(self.cfg["twitter"]["password"]) if not password else password
        self.label = str(self.cfg["twitter"]["label"]) if not label else label
        self.access_token = str(self.cfg["twitter"]["access_token"]) if not access_token else access_token
        self.access_token_secret = str(self.cfg["twitter"]["access_token_secret"]) \
            if not access_token_secret else access_token_secret
        self.api_key = str(self.cfg["twitter"]["api_key"]) if not api_key else api_key
        self.api_key_sec = str(self.cfg["twitter"]["api_key_sec"]) if not api_key_sec else api_key_sec
        self.bearer_token = str(self.cfg["twitter"]["bearer_token"]) if not bearer_token else bearer_token

        # Authenticate to Twitter
        auth = tw.OAuthHandler(self.api_key, self.api_key_sec)
        auth.set_access_token(self.access_token, self.access_token_secret)

        self.apitw = tw.API(auth, wait_on_rate_limit=True)

        self.client = tw.Client(bearer_token=self.bearer_token,
                                access_token=self.access_token,
                                access_token_secret=self.access_token_secret,
                                wait_on_rate_limit=True,
                                return_type=dict)

        if not os.path.isdir(self.DIRECTORY):
            os.makedirs(self.DIRECTORY)

    def get_user(self, name):
        response = self.client.get_user(username=str(name),
                                        tweet_fields=tweet_fields,
                                        user_fields=user_fields)
        errors = response.get("errors", [])
        if errors:
            error = errors[0]
            error_data = {
                'status_code': HTTPStatus.NOT_FOUND,
                'error_details': error["detail"],
                'message': error["title"]
            }
            raise myutil.CounterCustomError(error_data)
        return response.get('data')

    def getTweet(self, user_id, max_results=10):
        try:
            response = self.client.get_users_tweets(id=user_id,
                                                    max_results=max_results,
                                                    expansions=["attachments.media_keys"],
                                                    media_fields=media_fields,
                                                    user_fields=user_fields,
                                                    tweet_fields=tweet_fields)
            errors = response.get("errors", [])
            if errors:
                error = errors[0]
                error_data = {
                    'status_code': HTTPStatus.NOT_FOUND,
                    'error_details': error["detail"],
                    'message': error["title"]
                }
                raise myutil.CounterCustomError(error_data)
        except requests.exceptions.RequestException as response:
            errors = response.get("errors", [])
            if errors:
                error = errors[0]
                error_data = {
                    'status_code': HTTPStatus.BAD_REQUEST,
                    'error_details': error["detail"],
                    'message': error["title"]
                }
                raise myutil.CounterCustomError(error_data)

        return response

    def getTwitte(self, tag, start, end):
        startDate, endDate = myutil.translateDate(start=start, end=end) if start else None, None
        try:
            query_params = {
                'query': str(tag),
                'max_results': self.maxResults,
                'tweet_fields': ["id", "text"],
                'expansions': ["author_id", "attachments.media_keys"],
                'user_fields': ["name", "username", "profile_image_url"],
                'media_fields': media_fields
            }
            if startDate:
                query_params['start_time'] = startDate.isoformat()
            if endDate:
                query_params['end_time'] = endDate.isoformat()
            tweets = self.client.search_recent_tweets(**query_params)
        except Exception as e:
            response = e.response
            content = json.loads(response.content.decode('utf-8'))
            error_data = {
                'status_code': response.status_code,
                'error_details': content["errors"][0]["message"],
                'message': content["errors"][0]["message"]
            }
            raise myutil.CounterCustomError(error_data)

        return tweets

    def getAuthors(self, ids):
        try:
            authors_data = self.client.get_users(ids=ids)
        except Exception as e:
            response = e.response
            content = json.loads(response.content.decode('utf-8'))
            error_data = {
                'status_code': response.status_code,
                'error_details': content["errors"][0]["message"],
                'message': content["errors"][0]["message"]
            }
            raise myutil.CounterCustomError(error_data)
        return {
            author.get('id'): author for author in authors_data.get('data', [])
        }

    def get_followers(self, name):
        user_id = self.client.get_user(username=str(name)).get('data').get('id')
        try:
            return self.client.get_users_followers(id=user_id)
        except Exception as e:
            response = e.response
            content = json.loads(response.content.decode('utf-8'))
            error_data = {
                'status_code': response.status_code,
                'error_details': content["detail"],
                'message': content["detail"]
            }
            raise myutil.CounterCustomError(error_data)

    def get_friends(self, name):
        try:
            return self.apitw.get_friends(screen_name=name,
                                          count=self.maxResults)
        except Exception as e:
            response = e.response
            content = json.loads(response.content.decode('utf-8'))
            error_data = {
                'status_code': response.status_code,
                'error_details': content["errors"][0]["message"],
                'message': content["errors"][0]["message"]
            }
            raise myutil.CounterCustomError(error_data)

    def get_friendship(self, source, target):
        try:
            return self.apitw.get_friendship(source_screen_name=source,
                                             target_screen_name=target)
        except Exception as e:
            response = e.response
            content = json.loads(response.content.decode('utf-8'))
            error_data = {
                'status_code': response.status_code,
                'error_details': content["errors"][0]["message"],
                'message': content["errors"][0]["message"]
            }
            raise myutil.CounterCustomError(error_data)

    def get_profile_imgs(self, target_user, start, end):
        startDate, endDate = myutil.translateDate(start=start, end=end)
        tweets = []

        tmpTweets = self.apitw.user_timeline(screen_name=str(target_user), count=1)
        while tmpTweets[-1].created_at > startDate:
            tmpTweets = self.apitw.user_timeline(screen_name=str(target_user), max_id=tmpTweets[-1].id)
            # tmpTweets = self.apitw.user_timeline(screen_name=target_user, count=10, include_rts=False, tweet_mode="extended")

        for tweet in tmpTweets:
            if tweet.created_at > endDate or tweet.created_at < startDate:
                break
            if endDate > tweet.created_at > startDate:
                tweets.append(tweet)

        tt = []
        for tweet in tweets:
            del tweet._json["user"]
            tt.append(tweet._json)
        return tt


def obj2json(tmpTweets):
    ret = []
    if tmpTweets.data is None:
        return []
    for data in tmpTweets.data:
        id = int(data["id"])
        text = data["text"]

        attachments = data.get("attachments")

        author_id = data.get("author_id")
        if author_id is not None:
            author_id = int(author_id)

        created_at = data.get("created_at")
        if created_at is not None:
            created_at = str(created_at)

        entities = data.get("entities")
        geo = data.get("geo")

        in_reply_to_user_id = data.get("in_reply_to_user_id")
        if in_reply_to_user_id is not None:
            in_reply_to_user_id = int(in_reply_to_user_id)

        lang = data.get("lang")
        url = data.get("url")
        possibly_sensitive = data.get("possibly_sensitive")

        source = data.get("source")

        ret.append({
            "id": id,
            "text": text,
            "url": url,
            "attachments": attachments,
            "author_id": author_id,
            "created_at": created_at,
            "entities": entities,
            "geo": geo,
            "in_reply_to_user_id": in_reply_to_user_id,
            "lang": lang,
            "possibly_sensitive": possibly_sensitive,
            "source": source,
        })
    return ret
