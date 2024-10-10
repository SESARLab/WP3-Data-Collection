import logging
import os
from time import sleep

import requests

import counterUtilites as myutil


class InstagramConnector:
    cfg = myutil.setConfig(file="app-config.yml")
    max_count = cfg["instagram"]["maxResults"]

    BASE_URL = cfg["instagram"]["connector_url"]
    campaign_id = None
    campaign = None

    def __init__(
        self,
        base_url=BASE_URL
    ):
        self.BASE_URL = base_url

    def request_campaign(self, hashtag):

        result = requests.post(f"{self.BASE_URL}/batch?hashtag={hashtag}&max_count={self.max_count}")
        self.campaign_id = result.json()["result"]["id"]
        logging.info(self.campaign_id)
        return self.campaign_id

    def wait_campaign_status(self):
        if self.campaign_id is None:
            print("You should start a campaign first!")
            return None

        while True:
            WAITING_SECONDS = 5
            logging.info("Waiting campaign...retry in %s seconds", WAITING_SECONDS)
            sleep(WAITING_SECONDS)
            result = requests.get(f"{self.BASE_URL}/batch/{self.campaign_id}")
            if result.json()["result"]["completed"]:
                self.campaign = result.json()
                return self.campaign

    def get_campaign_images(self):
        result = requests.get(f"{self.BASE_URL}/batch/{self.campaign_id}/images")
        self.campaign["images"] = result.json()["result"]
        return self.campaign["images"]

    def get_campaign_image(self, image_id, download_directory):
        local_filename = f"{download_directory}{os.sep}{image_id}.jpg"
        logging.info(f"Downloading %s", local_filename)
        url = f"{self.BASE_URL}/batch/{self.campaign_id}/image/{image_id}"
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    def download_campaign_images(self, download_directory="counter"):
        for image in self.get_campaign_images():
            self.get_campaign_image(image["id"], download_directory=download_directory)

    def start_campaign_and_wait_response(self, tag):
        self.request_campaign(tag)
        self.wait_campaign_status()
        # self.download_campaign_images()
        return self.campaign
