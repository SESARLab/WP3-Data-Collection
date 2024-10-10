import uuid
from http import HTTPStatus

from flask import request
from flask_restx import Namespace, Resource

import counterUtilites as myutil
from TwitterConnector.TwitterConnector import TwitterConnector
from counterUtilites import strtobool
import constants as const

cfg = myutil.setConfig(file="app-config.yml")
api = Namespace("twitter")
skip_flag = None

cfg = myutil.setConfig(file="app-config.yml")

access_token = cfg["twitter"]["access_token"]
access_token_secret = cfg["twitter"]["access_token_secret"]
bearer_token = cfg["twitter"]["bearer_token"]
api_key = cfg["twitter"]["api_key"]
api_key_sec = cfg["twitter"]["api_key_sec"]
account_type = cfg["twitter"]["account_type"]
username = cfg["twitter"]["username"]
password = cfg["twitter"]["password"]
label = cfg["twitter"]["label"]


def getClient(request):
    global skip_flag
    req_headers = request.headers
    skip_flag = strtobool(req_headers.get("skip_media")) or strtobool(request.args.get('skip_media'))
    return TwitterConnector(
        access_token=access_token,
        access_token_secret=access_token_secret,
        api_key=api_key,
        api_key_sec=api_key_sec,
        bearer_token=bearer_token,
        account_type=account_type,
        username=username,
        password=password,
        label=label)


@api.route("/profile/<string:name>")
class TwitterProfile(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={"name": "Specify the name associated with the twitter"})
    def get(self, name):
        campaign_id = request.headers.get('campaign_id')
        try:
            tw = getClient(request)
            uuiddirname = str(campaign_id) if campaign_id else str(uuid.uuid1())
            user_profile = tw.get_user(name=name)
            user_id = user_profile.get('id')
            tweets = tw.getTweet(user_id)
            tweets_data = tweets.get('data', [])
            for tweet in tweets_data:
                tweet['canonical_url'] = f'https://twitter.com/{name}/status/{tweet["id"]}'
            data = {
                "profile_data": user_profile,
                "tweets": tweets_data
            }

            media = tweets.get("includes", {}).get("media", [])
            myutil.download_twitter_media(user_profile, media, f'{uuiddirname}/media/')
            link = myutil.postProcess(name="profile",
                                      obj=data,
                                      uid=uuiddirname,
                                      skip=True)
            return {
                "profile": data,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitter_error()
            myutil.postProcess(name="failed",
                               obj=error_dict,
                               uid=campaign_id)
            api.abort(error_dict["status_code"], error_dict["message"])
        except KeyError as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, const.NO_INFORMATION_MSG)
        except Exception as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.BAD_REQUEST,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.BAD_REQUEST, e)


@api.route("/search/<string:query>/<string:start>/<string:end>")
@api.route("/search/<string:query>")
class TwitterSearchQuery(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={
                 "query": "Specify the string associated with the twitter",
                 "start": "Specify the start date(YYYYMMDD) associated with the twitter",
                 "end": "Specify the end date(YYYYMMDD) associated with the twitter",
                 "location": "Specify the location associated with the twitter"
             })
    def get(self, query, start=None, end=None):
        campaign_id = request.headers.get('campaign_id')
        try:
            tw = getClient(request)
            uuiddirname = str(campaign_id) if campaign_id else str(uuid.uuid1())
            myutil.createFile(uuiddirname)
            tweets = tw.getTwitte(tag=query, start=start, end=end)
            # process tweets data
            tweets_data = tweets.get('data', {})
            authors_ids = [t["author_id"] for t in tweets_data]
            authors_data = tw.getAuthors(authors_ids)
            tweets_with_authors = []
            for t in tweets_data:
                t["author"] = authors_data[t["author_id"]]
                t["canonical_url"] = f'https://twitter.com/{t["author"].get("username")}/status/{t["id"]}'
                tweets_with_authors.append(t)

            # dowload tweets media
            media = tweets.get("includes", {}).get("media", [])
            myutil.download_twitter_media(None, media, f'{uuiddirname}/media/')
            link = myutil.postProcess(name="tweets",
                                      obj=tweets_with_authors,
                                      uid=uuiddirname,
                                      skip=True)
            return {
                "tweets": tweets_with_authors,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitter_error()
            myutil.postProcess(name="failed",
                               obj=error_dict,
                               uid=campaign_id)
            api.abort(error_dict["status_code"], error_dict["message"])
        except KeyError as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, const.NO_INFORMATION_MSG)
        except Exception as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.BAD_REQUEST,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.BAD_REQUEST, e)


@api.route("/followers/<string:name>")
class TwitterFollowers(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={"name": "Specify the name associated with the twitter"})
    def get(self, name):
        # TODO - not working
        campaign_id = request.headers.get('campaign_id')
        try:
            tw = getClient(request)
            follows = tw.get_followers(name=name)
            tt = [f._json for f in follows]
            link = myutil.postProcess(name="followers",
                                      obj=tt,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "followers": tt,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitter_error()
            myutil.postProcess(name="failed",
                               obj=error_dict,
                               uid=campaign_id)
            api.abort(error_dict["status_code"], error_dict["message"])
        except KeyError as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, const.NO_INFORMATION_MSG)
        except Exception as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.BAD_REQUEST,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.BAD_REQUEST, e)


@api.route("/friends/<string:name>")
class TwitterFriends(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={"name": "Specify the name associated with the twitter"})
    def get(self, name):
        # TODO - not working
        campaign_id = request.headers.get('campaign_id')
        try:
            tw = getClient(request)
            friends = tw.get_friends(name=str(name))
            tt = []
            for f in friends:
                tt.append(f._json)

            link = myutil.postProcess(name="friends",
                                      obj=tt,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "friends": tt,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitter_error()
            myutil.postProcess(name="failed",
                               obj=error_dict,
                               uid=campaign_id)
            api.abort(error_dict["status_code"], error_dict["message"])
        except KeyError as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, const.NO_INFORMATION_MSG)
        except Exception as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.BAD_REQUEST,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.BAD_REQUEST, e)


@api.route("/friendship/<string:source>/<string:target>")
class TwitterFriendship(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={
                 "source": "Specify the source name for friendship",
                 "target": "Specify the target name for friendship"
             })
    def get(self, source, target):
        # TODO - not working
        campaign_id = request.headers.get('campaign_id')
        try:
            data = {"source": {}, "target": {}}
            tw = getClient(request)
            friendships = tw.get_friendship(source=source, target=target)
            idx = 0
            for f in friendships:
                data["source"] = f._json
                if idx == 0:
                    idx = idx + 1
            link = myutil.postProcess(name="relationship",
                                      obj=data,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "relationship": data,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitter_error()
            myutil.postProcess(name="failed",
                               obj=error_dict,
                               uid=campaign_id)
            api.abort(error_dict["status_code"], error_dict["message"])
        except KeyError as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, const.NO_INFORMATION_MSG)
        except Exception as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.BAD_REQUEST,
                                    "message": str(e),
                                    "error_details": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.BAD_REQUEST, e)


# @api.route("/tweet/<string:name>/<string:start>/<string:end>")
# class TwitterTweet(Resource):
#     @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
#                         HTTPStatus.BAD_REQUEST: const.STATUS_400,
#                         HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
#              params={
#                  "name": "Specify the name associated with the twitter",
#                  "start": "Specify the start date(YYYYMMDD) associated with the twitter",
#                  "end": "Specify the end date(YYYYMMDD) associated with the twitter"
#              })
#     def get(self, name, start, end):
#         campaign_id = request.headers.get('campaign_id')
#         try:
#             tw = getClient(request)
#             tweets = tw.get_profile_imgs(name, start, end)
#             link = myutil.postProcess(name="tweets_imgs",
#                                       obj=tweets,
#                                       uid=campaign_id,
#                                       skip=skip_flag)
#             return {
#                 "tweets": tweets,
#                 "zipfile": link
#             }
#         except myutil.CounterCustomError as e:
#             error_dict = e.extract_twitter_error()
#             myutil.postProcess(name="failed",
#                                obj=error_dict,
#                                uid=campaign_id)
#             api.abort(error_dict["status_code"], error_dict["message"])
#         except KeyError as e:
#             myutil.postProcess(name="failed",
#                                obj={"status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
#                                     "message": str(e),
#                                     "error_details": str(e)},
#                                uid=campaign_id)
#             api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, const.NO_INFORMATION_MSG)
#         except Exception as e:
#             myutil.postProcess(name="failed",
#                                obj={"status_code": HTTPStatus.BAD_REQUEST,
#                                     "message": str(e),
#                                     "error_details": str(e)},
#                                uid=campaign_id)
#             api.abort(HTTPStatus.BAD_REQUEST, e)
