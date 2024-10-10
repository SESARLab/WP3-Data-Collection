from http import HTTPStatus

from flask import request
from flask_restx import Namespace, Resource, reqparse, inputs

from DarkWebConnector.DarkWebConnector import DarkWebCrawler

import constants as const

import counterUtilites as myutil

cfg = myutil.setConfig(file="app-config.yml")

skip_flag = None

api = Namespace("darkweb")


parser = reqparse.RequestParser()
parser.add_argument('url', type=inputs.url, help="onion url", required=True)


@api.route("/scan")
class DarkWebScanUrl(Resource):
    @api.doc(responses={HTTPStatus.OK: const.STATUS_200,
                        HTTPStatus.BAD_REQUEST: const.STATUS_400,
                        HTTPStatus.INTERNAL_SERVER_ERROR: const.STATUS_500})
    @api.expect(parser)
    def get(self):
        url = None
        query_strings = request.query_string.decode('utf-8').split("&")
        for query_string in query_strings:
            query_string_data = query_string.split("=")
            if query_string_data[0] == "url":
                url = query_string_data[1]
        if not url:
            return {
                "darweb": None,
                "zipfile": None
            }
        campaign_id = request.headers.get('campaign_id')
        try:
            darweb_crawler = DarkWebCrawler(campaign_id=campaign_id)
            images, text = darweb_crawler.download_content(url, campaign_id)

            data = {
                "images": images,
                "text": text
            }

            link = myutil.postProcess(name="dark_deep_web_content",
                                      obj=data,
                                      uid=campaign_id,
                                      skip=skip_flag)

            return {
                "dark_deep_web_content": data,
                "zipfile": link
            }
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
