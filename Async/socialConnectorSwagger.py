#!/usr/bin/env python
# encoding: utf-8


import ast
import io
import json

import requests
from celery import Celery, states
from celery.result import AsyncResult
from flask import Flask, request, render_template, send_file
from flask_cors import CORS
from flask_restx import Api, Resource, reqparse, inputs
from pywebhdfs.webhdfs import PyWebHdfsClient
from os import getenv

from worker import create_task, cel
import constants as const

FLOWER_HOSTNAME = getenv("FLOWER_HOST")
FLOWER_PORT = getenv("FLOWER_PORT")
HDFS_HOST = getenv("HDFS_HOST", "172.20.28.210")
HDFS_PORT = getenv("HDFS_PORT", 9870)
HDFS_USERNAME = getenv("HDFS_USERNAME", "admin")

celery = Celery(__name__)
app = Flask(__name__)
CORS(app)
api = Api(app)

campaign_space = api.namespace("campaign", description="Manage campaign")
twitter_space = api.namespace("twitter", description="Manage twitter")
telegram_space = api.namespace("telegram", description="Manage telegram")
youtube_space = api.namespace("youtube", description="Manage youtube")
instagram_space = api.namespace("instagram", description="Manage instagram")
twitch_space = api.namespace("twitch", description="Manage twitch")
darkweb_space = api.namespace("darkweb", description="Manage dark web")


def getHdfsClient():
    try:
        return PyWebHdfsClient(host=HDFS_HOST, port=HDFS_PORT, user_name=HDFS_USERNAME)
    except Exception as e:
        print("ERROR = ", e)
        raise e


@campaign_space.route("/<string:id>/downloadzip")
class DownloadClass(Resource):
    def get(self, id):
        try:
            data = requests.get("http://flower:7000/api/task/info/" + id)
            if data.status_code == 200:
                output = json.loads(data.content)
                if output["state"] == "SUCCESS":
                    uid = output["result"].split("/")[-1].split(".")[0]
                    hdfs = getHdfsClient()

                    res = hdfs.read_file("tmp/counter/" + uid + ".zip")
                    return send_file(io.BytesIO(res), download_name=uid + ".zip", as_attachment=True)
                else:
                    return {
                        "error": "Task Running"
                    }, 404
            else:
                return {
                    "error": "Task Not Found"
                }, 404
                # hdfs.delete_file_dir("tmp/counter/"+uid+".zip")
        except KeyError as e:
            api.abort(500, e.__doc__, status=const.NO_INFORMATION_MSG, statusCode="500")
        except Exception as e:
            print("EXCEPTION ERROR = ", e)
            api.abort(400, e.__doc__, status=const.NO_INFORMATION_MSG, statusCode="400")


@app.route("/dashboard")
def hello():
    return render_template("home.html")


@campaign_space.route("/<string:id>")
class Campaign(Resource):
    def get(self, id):
        print(str(request.path))
        result = AsyncResult(id, app=cel)
        return {
            "state": result.state,
            "id": result.id,
            "ready": result.ready(),
            "failed": result.failed(),
            "info": result.info,
            "date_done": str(result.date_done),
            "result": result.get() if result.state == states.SUCCESS else None,
        }


@campaign_space.route("/<string:id>/stop")
class StopCampaign(Resource):
    def get(self, id):
        cel.control.revoke(id, terminate=True)
        return {}


@campaign_space.route("/<string:id>/restart")
class CloneCampaign(Resource):
    def get(self, id):
        request = requests.get(f"http://{FLOWER_HOSTNAME}:{FLOWER_PORT}/api/task/info/{id}")
        if request.status_code == 200:
            args, hook = ast.literal_eval(str(request.json()["args"]))
            task = create_task.delay(args, hook)
            return {
                "campaign_id": task.task_id
            }
        return {
            "error": "Task Not Found"
        }, 404


@twitter_space.route("/followers/<string:name>")
@twitter_space.route("/friends/<string:name>")
@twitter_space.route("/friendship/<string:source>/<string:target>")
@twitter_space.route("/profile/<string:name>")
@twitter_space.route("/search/<string:query>")
@twitter_space.route("/search/<string:query>/<string:start>/<string:end>")
# @twitter_space.route("/tweet/<string:name>/<string:start>/<string:end>")
@telegram_space.route("/channel/<string:channel_name>")
@telegram_space.route("/messages/<string:channel_name>", defaults={"savemedia": "1"})
@telegram_space.route("/messages/<string:channel_name>/<string:savemedia>")
@telegram_space.route("/search/<string:query>")
@telegram_space.route("/search/<string:channel_name>/<string:query>")
@telegram_space.route("/comments/<string:channel_name>/<string:message_id>")
@telegram_space.route("/profile/<string:username>")
# @telegram_space.route('/search/<string:tag>/<string:location>')
# @telegram_space.route("/search/<string:query>/<string:start>/<string:end>")
@youtube_space.route("/profile/<string:channel_id>")
@youtube_space.route("/playlist/<string:playlist_id>")
@youtube_space.route("/comments/<string:video_id>")
@youtube_space.route("/channel/search/<string:channel_id>/<string:query>", defaults={"start": "", "end": "", "location": ""})
@youtube_space.route("/channel/search/<string:channel_id>/<string:query>/<string:start>/<string:end>", defaults={"location": ""})
@youtube_space.route("/channel/search/<string:channel_id>/<string:query>/<string:start>/<string:end>/<string:location>")
@youtube_space.route('/captions/<string:video_id>', defaults={'language': ''})
@youtube_space.route('/captions/<string:video_id>/<string:language>')
@youtube_space.route('/channel/all_videos/<string:channel_id>')
@youtube_space.route("/search/<string:video_id>", defaults={"start": "", "end": "", "location": ""})
@youtube_space.route("/search/<string:query>", defaults={"start": "", "end": "", "location": ""})
@youtube_space.route("/search/<string:query>/<string:start>/<string:end>", defaults={"location": ""})
@youtube_space.route("/search/<string:query>/<string:start>/<string:end>/<string:location>")
# @youtube_space.route("/channelnametoid/<string:channel_name>")
@twitch_space.route("/get_chat/<string:channelname>/<int:duration>")
@twitch_space.route("/get_video/<string:channelname>/<int:duration>")
@twitch_space.route("/search/<string:channelname>")
# @instagram_space.route("/search/<string:tag>")
# @instagram_space.route("/search/<string:tag>/<string:start>/<string:end>")
class ApiBackgorundCaller(Resource):
    def get(self, **kwargs):
        try:
            hook = request.headers["Hook"]
        except:
            hook = None

        task = create_task.delay(request.full_path, hook)
        return {
            "campaign_id": task.task_id
        }


parser = reqparse.RequestParser()
parser.add_argument('url', type=inputs.url, help="onion url", required=True)


@darkweb_space.route("/scan")
class ApiDarkWebBackgorundCaller(Resource):

    @api.expect(parser)
    def get(self, **kwargs):
        try:
            hook = request.headers["Hook"]
        except:
            hook = None

        task = create_task.delay(request.full_path, hook)
        return {
            "campaign_id": task.task_id
        }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
