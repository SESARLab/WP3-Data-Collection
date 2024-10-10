import json

from flask import request
from flask_restx import Namespace, Resource

import counterUtilites as myutil
from YoutubeConnector.YoutubeCrawler.Youtubecrawler import Youtubecrawler
from counterUtilites import strtobool
from http import HTTPStatus

import constants as const

cfg = myutil.setConfig(file="app-config.yml")
api = Namespace("youtube")

maxVideos = cfg["youtube"].get("maxVideos", 10)
youtube_api_key = cfg["youtube"]['youtube_api_key']


def getHeaders(request):
    global skip_flag
    skip_flag = strtobool(request.headers.get("download_media")) or \
                strtobool(request.args.get('download_media'))

    yapi = request.headers.get("youtube_api")
    if yapi is None or yapi == "":
        youtube_api = youtube_api_key
    return Youtubecrawler(youtube_api)


@api.route("/profile/<string:channel_id>")
class YoutubeProfile(Resource):
    """ Description will be added """

    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={"channel_id": const.NO_CHANNEL_ID_MSG})
    def get(self, channel_id):
        campaign_id = request.headers.get("campaign_id")
        try:
            mex = getHeaders(request)
            profile = mex.get_info_channel_by_id(channel_id)
            link = myutil.postProcess(name="channel",
                                      obj=profile,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "profile": profile,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_google_api_error_profile()
            myutil.postProcess(name="failed",
                               obj=error_dict,
                               uid=campaign_id)
            api.abort(error_dict["status_code"], error_dict["message"])
        except KeyError as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.INTERNAL_SERVER_ERROR,
                                    "message": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.INTERNAL_SERVER_ERROR, const.NO_INFORMATION_MSG)
        except Exception as e:
            myutil.postProcess(name="failed",
                               obj={"status_code": HTTPStatus.BAD_REQUEST,
                                    "message": str(e)},
                               uid=campaign_id)
            api.abort(HTTPStatus.BAD_REQUEST, e)


@api.route("/playlist/<string:playlist_id>")
class YoutubePlaylist(Resource):
    """ Description will be added """

    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={"playlist_id": const.NO_CHANNEL_ID_MSG})
    def get(self, playlist_id):
        campaign_id = request.headers.get("campaign_id")
        try:
            mex = getHeaders(request)
            videos = mex.get_videos_by_playlist_id(playlist_id)
            link = myutil.postProcess(name="playlist",
                                      obj=videos,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "videos": videos,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_google_api_error_playlist()
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


@api.route("/comments/<string:video_id>")
class YoutubeComments(Resource):
    """ Description will be added """

    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={'video_id': const.NO_VIDEO_ID_MSG})
    def get(self, video_id):
        campaign_id = request.headers.get("campaign_id")
        try:
            mex = getHeaders(request)
            comments, replies = mex.get_comment_by_video_id(video_id)  # get info channel by channel id
            link = myutil.postProcess(name="comments",
                                      obj={"comments": comments, "replies": replies},
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "comments": comments,
                "replies": replies,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_google_api_error_comments()
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


@api.route("/channel/search/<string:channel_id>/<string:query>", defaults={"start": "", "end": "", "location": ""})
@api.route("/channel/search/<string:channel_id>/<string:query>/<string:start>/<string:end>", defaults={"location": ""})
@api.route("/channel/search/<string:channel_id>/<string:query>/<string:start>/<string:end>/<string:location>")
class YoutubeChannel(Resource):
    """ Description will be added """

    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={'video_id': const.NO_VIDEO_ID_MSG})
    def get(self, query, channel_id, start, end, location):
        campaign_id = request.headers.get("campaign_id")
        try:
            location, radius = myutil.splitLocation(location)
            mex = getHeaders(request)
            videos = mex.search(query, channel_id, start, end, location, radius)
            result = []
            for v in videos:
                v["comments"], v["replies"] = mex.get_comment_by_video_id(v["id"]["videoId"])
                result.append(v)

            data = json.loads(json.dumps(result, cls=myutil.DateTimeEncoder))
            link = myutil.postProcess(name="youtube",
                                      obj=data,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "youtube": data,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_google_api_error_channel_search()
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


@api.route("/captions/<string:video_id>", defaults={"language": ""})
@api.route("/captions/<string:video_id>/<string:language>")
class YoutubeCaptions(Resource):
    """
    Downloads the captions associated to a video in the given language.
    If no language is given, downloads all the available captions
    """

    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={'video_id': const.NO_VIDEO_ID_MSG, 'language': const.NO_LANG_MSG})
    def get(self, video_id, language):
        campaign_id = request.headers.get("campaign_id")
        try:
            mex = getHeaders(request)
            captions = mex.get_captions(video_id, language)
            data = {
                "video_id": video_id,
                "captions": captions
            }
            link = myutil.postProcess(name="youtube",
                                      obj=data,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "youtube": data,
                "zipfile": link
            }
        except myutil.CounterCustomErrorTranscriptsDisabled as e:
            error_dict = e.extract_no_captions_errors()
            myutil.postProcess(name="failed",
                               obj=error_dict,
                               uid=campaign_id)
            api.abort(error_dict["status_code"], error_dict["message"])

        except myutil.CounterCustomError as e:
            error_dict = e.extract_google_api_error_captions()
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


@api.route('/channel/all_videos/<string:channel_id>')
class ChannelAllVideos(Resource):
    """ Description will be added """

    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={'channel_id': const.NO_CHANNEL_ID_MSG})
    def get(self, channel_id):
        campaign_id = request.headers.get("campaign_id")
        try:
            mex = getHeaders(request)
            videos = mex.get_all_videos_and_comments_for_channel(channel_id)
            link = myutil.postProcess(name="channel_video_and_comments",
                                      obj={"videos": videos},
                                      uid=campaign_id)
            return {
                "videos": videos,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_google_api_error_channel_all_videos()
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


@api.route("/search/<string:video_id>", defaults={"start": "", "end": "", "location": ""})
@api.route("/search/<string:query>", defaults={"start": "", "end": "", "location": ""})
@api.route("/search/<string:query>/<string:start>/<string:end>", defaults={"location": ""})
@api.route("/search/<string:query>/<string:start>/<string:end>/<string:location>")
class YoutubeSearch(Resource):
    """ Description will be added """

    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
             params={
                 "query": "Specify the string",
                 "start": "Specify the start date(YYYYMMDD) associated",
                 "end": "Specify the end date(YYYYMMDD) associated",
                 "location": "Specify the location associated",
             })
    def get(self, query, start, end, location):
        campaign_id = request.headers.get("campaign_id")
        try:
            location, radius = myutil.splitLocation(location)
            mex = getHeaders(request)
            videos = mex.search(query, None, start, end, location, radius)

            result = []
            for v in videos[:maxVideos]:
                data = v.get("id")
                if data:
                    video_id = data.get("videoId")
                    if video_id:
                        data["comments"], data["replies"] = mex.get_comment_by_video_id(video_id)
                        snipet = v.get("snippet", {})
                        data["title"] = snipet.get("title")
                        data["description"] = snipet.get("description")
                        result.append(data)

            data = json.loads(json.dumps(result, cls=myutil.DateTimeEncoder))
            link = myutil.postProcess(name="youtube_search",
                                      obj=data,
                                      uid=campaign_id,
                                      skip=skip_flag)
            return {
                "youtube": data,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_google_api_error_response()
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

#  NOT USED APIS
#
# @api.route("/channelnametoid/<string:channel_name>")
# class YoutubeChannelNameToId(Resource):
#     """ Description will be added """
#
#     @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
#                         HTTPStatus.BAD_REQUEST: const.STATUS_400,
#                         HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500},
#              params={"channel_id": const.NO_CHANNEL_ID_MSG})
#     def get(self, channel_name):
#         campaign_id = request.headers.get("campaign_id")
#         try:
#             mex = getHeaders(request)
#             videos = mex.get_id_by_channel_name(channel_name=channel_name)
#             link = myutil.postProcess(name="channel",
#                                       obj=videos,
#                                       uid=campaign_id,
#                                       skip=skip_flag)
#             return {
#                 "id": videos,
#                 "zipfile": link
#             }
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
