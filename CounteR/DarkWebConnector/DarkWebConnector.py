import os
import sys
import uuid
import requests
from pathlib import Path
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

import counterUtilites as myutil
from zipfile import ZipFile

sys.path.append(str(Path(".").absolute().parent))

cfg = myutil.setConfig(file="app-config.yml")
maxResults = cfg["darkweb"]["maxResults"]


class DarkWebCrawler:
    DIRECTORY = "counter/"

    def __init__(
            self,
            campaign_id=None
    ):
        self.uuiddirname = campaign_id if campaign_id is not None else str(uuid.uuid1())
        if not os.path.isdir(self.DIRECTORY):
            os.makedirs(self.DIRECTORY)

        self.path = f"{self.DIRECTORY}{self.uuiddirname}"
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self.proxies = {
            'http': 'socks5h://localhost:9050',
            'https': 'socks5h://localhost:9050'
        }

        self.headers = {
            'User-Agent': UserAgent().random
        }

    def download_content(self, url, campaign_id, download_images=True, download_text=True):
        response = requests.get(url, headers=self.headers, proxies=self.proxies)

        soup = BeautifulSoup(response.text, 'html.parser')
        images = []
        strings = []

        if download_images:
            image_tags = soup.find_all('img')
            for image_tag in image_tags:
                try:
                    img_url = image_tag['src']

                    if img_url.startswith('data:image/svg+xml'):
                        continue

                    if not img_url.startswith('http'):
                        img_url = urljoin(url, img_url)
                    img_url = unquote(img_url.split("url=")[-1].split("&")[0])

                    res = requests.get(img_url, proxies=self.proxies, headers=self.headers)
                    res.raise_for_status()

                    filename = f"{uuid.uuid4()}.jpg"
                    with open(os.path.join(f'{self.path}', filename), 'wb') as f:
                        f.write(res.content)

                    images.append(f"/counter/{campaign_id}/{filename}")

                    if len(images) == maxResults:
                        break

                except Exception as e:
                    print('Error downloading image:', str(e))

        if download_text:
            strings = [string for string in soup.stripped_strings]

        return images, strings
