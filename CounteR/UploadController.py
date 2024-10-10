from flask import request
from flask_restx import Resource, Namespace
from werkzeug.datastructures import FileStorage

import constants as const

import counterUtilites as myutil
import pkg_resources

cfg = myutil.setConfig(file="app-config.yml")
api = Namespace("manual")

upload_parser = api.parser()
upload_parser.add_argument("file", location="files", type=FileStorage, required=True)


@api.route("/version")
class Version(Resource):
    def get(self):
        pkg_resources.get_distribution("simplegist").version


@api.route("/upload/")
@api.expect(upload_parser)
class Upload(Resource):
    def post(self):
        try:
            return {
                "zipfile": myutil.manualUpload(files=request.files["file"])
            }
        except KeyError as e:
            api.abort(500, e.__doc__, status=const.NO_INFORMATION_MSG, statusCode="500")
        except Exception as e:
            api.abort(400, e.__doc__, status=const.NO_INFORMATION_MSG, statusCode="400")
