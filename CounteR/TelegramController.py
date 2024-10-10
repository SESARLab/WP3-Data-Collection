from http import HTTPStatus

from flask import request
from flask_restx import Namespace, Resource

import TelegramConnector.utility as ut
from TelegramConnector.Telegramcrawler.Telegramcrawler import Telegramcrawler

from telethon.sessions import StringSession
import counterUtilites as myutil
import constants as const

telegram_api_id, telegram_api_hash, phone, username = ut.get_setting_telegram()

cfg = myutil.setConfig(file="app-config.yml")
api = Namespace("telegram")


@api.route("/login/sendcode")
class TelegramLogin(Resource):
    def get(self):
        campaign_id = request.headers.get('campaign_id')
        mex = Telegramcrawler(phone,
                              telegram_api_id,
                              telegram_api_hash,
                              campaign_id=campaign_id)

        mex._client.connect()
        hash_code = mex._client.send_code_request(phone=phone)
        session_file = open("session", "w")
        session_file.write(StringSession.save(mex._client.session))
        session_file.close()
        mex._client.disconnect()

        return {
            "message": f"Wait for code, then go to login/code/{hash_code.phone_code_hash}/<received_code>",
            "hash_code": hash_code.phone_code_hash
        }


@api.route("/login/code/<string:hash_code>/<string:code>")
class TelegramLoginCode(Resource):
    def get(self, code, hash_code):
        campaign_id = request.headers.get('campaign_id')

        mex = Telegramcrawler(phone,
                              telegram_api_id,
                              telegram_api_hash,
                              campaign_id=campaign_id)

        mex._client.connect()
        mex._client.sign_in(phone, code=code, phone_code_hash=hash_code)
        session_file = open("session", "w")
        session_file.write(StringSession.save(mex._client.session))
        session_file.close()
        mex._client.disconnect()

        return {
            "message": "Logged In"
        }


@api.route("/profile/<string:username>")
class TelegramProfile(Resource):
    def get(self, username):
        campaign_id = request.headers.get('campaign_id')
        try:
            mex = Telegramcrawler(phone,
                                  telegram_api_id,
                                  telegram_api_hash,
                                  campaign_id=campaign_id)
            telegram = mex.get_user_bio(username)
            link = mex.dump_to_file_and_upload_to_hdfs(telegram, "bio_data_")
            return {
                "telegram": telegram,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_telegram_profile_error()
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


@api.route("/channel/<string:channel_name>")
class TelegramChannel(Resource):
    def get(self, channel_name):
        campaign_id = request.headers.get('campaign_id')
        try:
            mex = Telegramcrawler(phone,
                                  telegram_api_id,
                                  telegram_api_hash,
                                  list_keyword=[channel_name],
                                  campaign_id=campaign_id)
            telegram = mex.get_channel_description(channel_name)
            link = mex.dump_to_file_and_upload_to_hdfs(telegram, "channel_bio_data_")
            return {
                "telegram": telegram,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_telegram_profile_error()
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


@api.route("/search/<string:query>")
class TelegramSearch(Resource):
    def get(self, query):
        campaign_id = request.headers.get('campaign_id')
        try:
            mex = Telegramcrawler(phone,
                                  telegram_api_id,
                                  telegram_api_hash,
                                  list_keyword=[query],
                                  campaign_id=campaign_id)
            maxResults = cfg["telegram"].get("maxResults", 100)
            limit = request.args.get("limit")
            max_results = int(limit) if limit is not None else maxResults

            telegram = mex.get_channels(max_results)
            link = mex.dump_to_file_and_upload_to_hdfs(telegram, "channel_data_")
            return {
                "telegram": telegram,
                "zipfile": link
            }

        except myutil.CounterCustomError as e:
            error_dict = e.extract_telegram_profile_error()
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


@api.route("/messages/<string:channel_name>", defaults={"savemedia": "1"})
@api.route("/messages/<string:channel_name>/<string:savemedia>")
class TelegramMessages(Resource):
    def get(self, channel_name, savemedia):
        campaign_id = request.headers.get('campaign_id')
        try:
            savemedia = bool(int(savemedia))
        except:
            savemedia = True
        try:
            mex = Telegramcrawler(phone,
                                  telegram_api_id,
                                  telegram_api_hash,
                                  list_keyword=[channel_name],
                                  campaign_id=campaign_id)
            maxResults = cfg["telegram"].get("maxResults", 100)
            maxMediaResults = cfg["telegram"].get("maxMediaResults", 20)
            max_results = int(request.args.get("limit")) if request.args.get("limit") is not None else maxResults
            telegram = mex.get_posts_all(channelname=channel_name,
                                         order=False,
                                         savemedia=savemedia,
                                         limit=max_results,
                                         maxmedialimit=maxMediaResults,)
            link = mex.dump_to_file_and_upload_to_hdfs(telegram, "channel_posts_")
            return {
                "telegram": telegram,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_telegram_profile_error()
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


@api.route("/search/<string:channel_name>/<string:query>")
class TelegramSearchChannel(Resource):
    def get(self, channel_name, query):
        campaign_id = request.headers.get('campaign_id')
        try:
            mex = Telegramcrawler(phone,
                                  telegram_api_id,
                                  telegram_api_hash,
                                  list_keyword=[channel_name],
                                  campaign_id=campaign_id)
            telegram = mex.get_channel_posts(channel_name, limit=None, order=False, savemedia=True, search=query)
            link = mex.dump_to_file_and_upload_to_hdfs(telegram, "channel_posts_")
            return {
                "telegram": telegram,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_telegram_profile_error()
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


@api.route("/comments/<string:channel_name>/<string:message_id>")
class TelegramComments(Resource):
    def get(self, channel_name, message_id):
        campaign_id = request.headers.get('campaign_id')
        try:
            mex = Telegramcrawler(phone,
                                  telegram_api_id,
                                  telegram_api_hash,
                                  campaign_id=campaign_id)
            telegram = mex.get_comments(channel_name, int(message_id), savemedia=True)
            link = mex.dump_to_file_and_upload_to_hdfs(telegram, "post_comments_")
            return {
                "telegram": telegram,
                "zipfile": link
            }
        except myutil.CounterCustomError as e:
            error_dict = e.extract_telegram_profile_error()
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

