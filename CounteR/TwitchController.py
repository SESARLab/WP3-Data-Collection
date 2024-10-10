import uuid

from flask import request
from TwitchConnector.TwitchConnector import Twichcrawler, TwitchChatIRC
from flask_restx import Namespace, Resource
from http import HTTPStatus

import counterUtilites as myutil
import constants as const

api = Namespace("twitch")

global cfg
cfg = myutil.setConfig(file="app-config.yml")

oauthtoken = cfg['twitch']['oauthtoken']
server = cfg['twitch']['server']
port = cfg['twitch']['port']
nickname = cfg['twitch']['nickname']
client_id = cfg['twitch']['client_id']
client_secret = cfg['twitch']['client_secret']


@api.route("/get_chat/<string:channelname>", defaults={"duration": 60})
@api.route("/get_chat/<string:channelname>/<int:duration>")
class TwitchGetChat(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500})
    def get(self, channelname, duration):
        campaign_id = request.headers.get('campaign_id')
        try:
            twitch_chat_irc = TwitchChatIRC()
            file_name = twitch_chat_irc.listen_live(channelname, timeout=duration)
            twitch_chat_irc.close_connection()
            data = twitch_chat_irc.export_chat(file_name, channelname)

            link = myutil.postProcess(name="chat",
                                      obj=data,
                                      uid=campaign_id)
            return {
                "live": data,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitch_api_error_profile()
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


@api.route("/get_video/<string:channelname>/", defaults={"duration": 60})
@api.route("/get_video/<string:channelname>/<int:duration>")
class TwitchGetVideo(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500})
    def get(self, channelname, duration):
        campaign_id = request.headers.get('campaign_id')
        try:
            obj = Twichcrawler(client_id=client_id,
                               client_secret=client_secret,
                               oauthtoken=oauthtoken,
                               channel=f'#{channelname}',
                               server=server,
                               port=port)

            uuiddirname = str(campaign_id) if campaign_id else str(uuid.uuid1())
            myutil.createFile(uuiddirname)

            dir_name = obj.save_video(uuiddirname, duration)
            info = obj.get_channel_info()
            myutil.pack_twitch_result(uuiddirname, dir_name, info)
            link = myutil.upload_to_hdfs(uuiddirname)
            return {
                "twitch": info,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitch_get_video_api_error_profile()
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


@api.route("/search/<string:channelname>")
class TwitchGetChannels(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500})
    def get(self, channelname):
        campaign_id = request.headers.get('campaign_id')
        try:
            obj = Twichcrawler(client_id=client_id,
                               client_secret=client_secret,
                               oauthtoken=oauthtoken,
                               channel=f'#{channelname}',
                               server=server,
                               port=port)

            results = obj.search_channels(channelname)
            link = myutil.postProcess(name="twitch",
                                      obj=results,
                                      uid=campaign_id)
            return {
                "live": results,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_twitch_get_video_api_error_profile()
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


