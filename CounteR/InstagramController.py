import json
import uuid

from flask import request
from flask_restx import Namespace, Resource

import counterUtilites as myutil
from InstagramConnector.InstagramConnector import InstagramConnector

import constants as const

cfg = myutil.setConfig(file="app-config.yml")

api = Namespace("instagram")


@api.route("/search/<string:tag>")
class MainClass(Resource):
    @api.doc(responses={200: "OK", 400: const.STATUS_400, 500: const.STATUS_500},
             params={"tag": "Specify the string or hashtag associated"})
    def get(self, tag):
        ig = InstagramConnector()
        result = ig.start_campaign_and_wait_response(tag)
        return {
            "instagram": result,
            "zipfile": self.pack_instagram_results(ig, result)
        }

    def pack_instagram_results(self, ig, result):
        name = "instagram"
        jd = json.dumps({"instagram": result}, ensure_ascii=False, indent=4, separators=(',', ': '))
        uid = request.headers.get('campaign_id')

        uuiddirname = str(uid)
        if uid is None or uid == "":
            uuiddirname = str(uuid.uuid1())

        myutil.createFile(uuiddirname)
        with open(f"{str(cfg['localzippath'])}{uuiddirname}/{name}.json", "w", encoding="utf-8") as f:
            f.write(jd)

        ig.download_campaign_images(download_directory=str(cfg["localzippath"]) + uuiddirname)
        myutil.createZip(dirName=uuiddirname)
        return myutil.writeOnHdfs(filename=f"{uuiddirname}.zip")
