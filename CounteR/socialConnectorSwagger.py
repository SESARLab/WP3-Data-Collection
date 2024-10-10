#!/usr/bin/env python
# encoding: utf-8
"""
/webhdfs/v1/tmp/counter/c6cbf29c-6d7a-11ec-80af-60a44c72a1cb.zip?op=DELETE&recursive=true
"""
import asyncio
import os
import warnings

import sentry_sdk
from flask import Flask
from flask_restx import Api
from sentry_sdk.integrations.flask import FlaskIntegration

import counterUtilites as myutil
from InstagramController import api as instagram_controller
from TelegramController import api as telegram_controller
from TwitterController import api as twitter_controller
from UploadController import api as upload_controller
from YoutubeController import api as youtube_controller
from TwitchController import api as twitch_controller
from DarkWebController import api as darkweb_controller


app = Flask(__name__)
app.config.SWAGGER_UI_DOC_EXPANSION = "list"
app.app_context().push()
warnings.simplefilter("ignore", UserWarning)
cfg = myutil.setConfig(file="app-config.yml")

apitw = myutil.getApiTwitte()

loop = asyncio.new_event_loop()
telegram_controller.loop = loop
api = Api(
    app,
    version="1.0",
    title="Social Network Connector API",
    description="Social Network Connector API",
)


sentry_sdk.init(
    dsn=cfg["sentry"]
    or "https://a0b4ef6aecde4fa68a59b3a16744bdc9@o4504095788105728.ingest.sentry.io/4504095789416448",

    integrations=[
        FlaskIntegration(),
    ],
    environment=os.getenv("ENVIRONMENT", "production"),
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0,
)


telegram_space = api.add_namespace(telegram_controller)
youtube_space = api.add_namespace(youtube_controller)
instagram_space = api.add_namespace(instagram_controller)
twitter_space = api.add_namespace(twitter_controller)
twitch_space = api.add_namespace(twitch_controller)
upload_space = api.add_namespace(upload_controller)
darkweb_space = api.add_namespace(darkweb_controller)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
