#!/usr/bin/env python
# encoding: utf-8

import json
import logging
import os
import sys
import uuid
import warnings
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from datetime import timedelta
from http import HTTPStatus
from json import JSONEncoder
from logging.handlers import RotatingFileHandler
from pprint import pprint
from typing import Optional, List, Tuple, Dict
from urllib.error import URLError
from urllib.parse import urlparse
from zipfile import ZipFile

import pendulum
import requests
import tweepy as tw
import yaml
from pytube import YouTube
from pywebhdfs.webhdfs import PyWebHdfsClient

warnings.simplefilter("ignore", UserWarning)


def logger(logPath=None, fileName=None):
    logPath = (os.path.abspath(".") + "/" + logPath if logPath else os.path.abspath(".") + "/logs")
    fileName = (os.path.basename(fileName).split(".")[0] if fileName else os.path.basename(__file__).split(".")[0])

    if not os.path.isdir(logPath):
        os.mkdir(logPath)

    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-8.8s]  %(message)s")
    logger = logging.getLogger()
    logger.propagate = False
    logger.setLevel(logging.DEBUG)  # this is needed to get all levels, and therefore filter on each handler

    # fileHandler = logging.FileHandler("{0}/{1}.log".format(logPath, fileName)) # this is for regular file handler
    fileHandler = RotatingFileHandler("{0}/{1}.log".format(logPath, fileName), maxBytes=(1048576 * 5), backupCount=3)
    fileHandler.setFormatter(logFormatter)
    fileHandler.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    consoleHandler.setLevel(logging.DEBUG)
    logger.handlers.clear()
    logger.addHandler(consoleHandler)

    return logger


class DateTimeEncoder(JSONEncoder):
    """ Instead of letting the default encoder convert datetime to string,
    convert datetime objects into a dict, which can be decoded by the
    DateTimeDecoder
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                "__type__": "datetime",
                "year": obj.year,
                "month": obj.month,
                "day": obj.day,
                "hour": obj.hour,
                "minute": obj.minute,
                "second": obj.second,
                "microsecond": obj.microsecond,
            }
        return JSONEncoder.default(self, obj)


def setConfig(file):
    log = logger(logPath="logs", fileName="counter.log")
    """
        logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-8.8s]  %(message)s",
        handlers=[
            logging.FileHandler("logs/counter.log"),
            logging.StreamHandler()
        ])
    """
    log.debug("start config....")

    global cfg
    with open(file, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    return cfg


def writeOnHdfs(filename):
    try:
        hdfs = PyWebHdfsClient(host=cfg["webhdfs"]["host"], port=cfg["webhdfs"]["port"], user_name=cfg["webhdfs"]["user"])
    except Exception as e:
        print("ERROR = ", e)
        raise e
    try:
        with open(f"{cfg['localzippath']}{filename}", "rb") as file:
            hdfs.create_file(f"/{cfg['webhdfs']['basepath']}{filename}", file_data=file, overwrite=True)
        if os.path.isfile(f"{cfg['localzippath']}{filename}"):
            os.remove(f"{cfg['localzippath']}{filename}")
        link = f"http://{cfg['webhdfs']['host']}:{cfg['webhdfs']['port']}/{cfg['webhdfs']['restapi']}{cfg['webhdfs']['basepath']}{filename}?op=OPEN"
        return link
    except Exception as e:
        print("Error While Opening the file! ", e)
        raise e


def createZip(dirName):
    log = logging.getLogger(__name__)
    log.debug(f"Creating zip of {cfg['localzippath']}{dirName}")
    with ZipFile(f"{cfg['localzippath']}{dirName}.zip", "w") as zipObj:
        # Iterate over all the files in directory
        c = 1
        filenames_count = sum([len(files) for r, d, files in os.walk(f"{cfg['localzippath']}{dirName}")])

        for folderName, subfolders, filenames in os.walk(f"{cfg['localzippath']}{dirName}"):
            for filename in filenames:
                log.error(f"Zipping {dirName}: {c}/{filenames_count}")
                # create complete filepath of file in directory
                filePath = os.path.join(folderName, filename)
                # Add file to zip
                zipObj.write(filePath, filePath)
                c = c + 1
        zipObj.close()
    return


def encodePath(path, uuiddirname):
    dirname = f"{uuiddirname}/"
    for el in path:
        if el[1] is not None and el[1] != "":
            dirname = f"{dirname}{el[0]}/{el[1]}/"
        else:
            dirname = f"{dirname}{el[0]}/"
    createFile(dirname)
    return dirname


def createFile(f):
    os.makedirs(f"{cfg['localzippath']}{f}", exist_ok=True)


def getMediaFromUrl(url, cType, filename):
    try:
        res = requests.get(url, headers={"Content-Type": cType}, stream=True, timeout=10).content
        with open(filename, "wb") as f:
            f.write(res)
    except requests.exceptions.HTTPError as e:
        print(e)
        return


def getImageFromUrl(url, cType, filename):
    getMediaFromUrl(url, cType, filename)

    fn = (filename.split("/")[-1]).split(".")

    if len(fn) > 1 and fn[0].endswith("_normal"):
        removal = "_normal"
        newfilename = remove_last(filename, removal)
        newurl = remove_last(url, removal)
        getMediaFromUrl(newurl, cType, newfilename)


def remove_last(string, to_remove):
    return "".join(string.rsplit(to_remove, 1))


def translateDate(start, end, as_timestamp=False):
    if not start or not end:
        return start, end
    tz = pendulum.timezone("utc")
    dt = datetime.strptime(str(start), "%Y%m%d")
    dt_formatters = ["%Y", "%m", "%d"]
    dt_vals = tuple(map(lambda formatter: int(datetime.strftime(dt, formatter)), dt_formatters))
    startDate = datetime(*dt_vals, tzinfo=tz)
    dt = datetime.strptime(str(end), "%Y%m%d")
    dt_formatters = ["%Y", "%m", "%d"]
    dt_vals = tuple(map(lambda formatter: int(datetime.strftime(dt, formatter)), dt_formatters))
    endDate = datetime(*dt_vals, tzinfo=tz) + timedelta(days=1) + timedelta(seconds=-1)
    if as_timestamp:
        startDate = startDate.timestamp()
        endDate = endDate.timestamp()
    return startDate, endDate


def getApiTwitte():
    access_token = str(cfg["twitter"]["access_token"])
    access_token_secret = str(cfg["twitter"]["access_token_secret"])
    bearer_token = str(cfg["twitter"]["bearer_token"])
    api_key = str(cfg["twitter"]["api_key"])
    api_key_sec = str(cfg["twitter"]["api_key_sec"])

    # Authenticate to Twitter
    auth = tw.OAuthHandler(api_key, api_key_sec)
    auth.set_access_token(access_token, access_token_secret)

    return tw.API(auth, wait_on_rate_limit=True)


def splitLocation(location_radius):
    if location_radius == "":
        return location_radius, ""
    try:
        location, radius = location_radius.rsplit(",", 1)
        return location, radius
    except Exception as e:
        raise e


def postProcess(name, obj, uid=None, skip=False):
    try:
        contents = {name: obj}
        uuiddirname = str(uid) if uid else str(uuid.uuid1())
        createFile(uuiddirname)
        with open(f"{cfg['localzippath']}{uuiddirname}/{name}.json", "w", encoding="utf-8") as f:
            json.dump(obj=contents, fp=f, ensure_ascii=False, indent=4, separators=(",", ": "), default=str)
        if not skip:
            with ThreadPoolExecutor(max_workers=100) as executor:
                flatten = flatten_json(obj, uuiddirname)
                for el in flatten:
                    executor.submit(saveMedia, url=el[1], path=el[0])

        print(f"Creating ZIP FILE: {uuiddirname}.zip")
        createZip(dirName=uuiddirname)
        print(f"Writing ZIP FILE on HDF: {uuiddirname}.zip")
        link = writeOnHdfs(filename=uuiddirname + ".zip")
        return link
    except Exception as e:
        print(e)
        raise e


def manualUpload(files):
    try:
        uuiddirname = str(uuid.uuid1())
        createFile(uuiddirname)
        files.save(f"{cfg['localzippath']}/{uuiddirname}/{files.filename}")
        createZip(dirName=uuiddirname)
        link = writeOnHdfs(filename=uuiddirname + ".zip")
        return link
    except Exception as e:
        print(e)
        raise e


def flatten_json(y, uuiddirname) -> List[Tuple[List[str], str]]:
    out: List[Tuple[List[str], str]] = []

    def flatten(x, name=uuiddirname + "/"):

        # If the Nested key-value
        # pair is of dict type
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "/")

        # If the Nested key-value
        # pair is of list type
        elif type(x) is list:
            for i, a in enumerate(x):
                flatten(a, name + str(i) + "/")

        else:
            names = name[:-1].split("/")
            if names[-1] == "video_id" or names[-1] == "videoId":
                out.append((name[:-1], "https://youtu.be/" + str(x)))
            elif str(x).startswith("http"):
                out.append((name[:-1], x))

    flatten(y)

    print("after flatten", out)

    cleaned_out: Dict[str, List[str]] = dict()
    # cleanup
    # using `url_value` as dictionary key
    for path, url_value in out:
        try:
            parsed_v = urlparse(url_value)

            # HTTPS > HTTP preference
            if parsed_v.scheme == "http":
                print("is http")
                https_parsed_v = deepcopy(parsed_v)
                https_parsed_v._replace(scheme=https_parsed_v.scheme.replace("http", "https"))
                if https_parsed_v.geturl() in cleaned_out:
                    continue

            # substitute HTTP with HTTPS if already present
            elif parsed_v.scheme == "https":
                print("is https")
                http_parsed_v = deepcopy(parsed_v)
                http_parsed_v._replace(scheme=http_parsed_v.scheme.replace("https", "http"))
                if http_parsed_v.geturl() in cleaned_out:
                    cleaned_out.pop(http_parsed_v.geturl())  # remove HTTP version
                cleaned_out[url_value] = path

            # generic case
            else:
                cleaned_out[url_value] = path
        except URLError as url_e:
            print("URL parsing error: passing through", url_e)
            cleaned_out[url_value] = path

    print("After cleanup")
    pprint(cleaned_out)

    return [(path_components, url_value) for url_value, path_components in cleaned_out.items()]


def save_media_youtube(url: str, path: str):
    """ Download a YouTube video by ``url`` to the target ``path`` """
    try:
        os.makedirs(f"{cfg['localzippath']}{path}", exist_ok=True)
        videoid = url.split("/")[-1]
        yt = YouTube(url)
        yt.title = str(videoid)
        yt.streams.filter(progressive=True, file_extension="mp4").order_by("resolution").asc().first() \
            .download(f"{cfg['localzippath']}{path}/{videoid}")
        return True
    except Exception as e:
        print("saveMedia Youtube ERROR: ", e)
        return False


def saveMedia(url: str, path: str, force_scheme: Optional[str] = None):
    try:
        parsed_url = urlparse(url)

        if force_scheme is not None:
            parsed_url._replace(scheme=force_scheme)

        # YouTube link
        if parsed_url.hostname in ["youtu.be" "youtube.com"]:
            return save_media_youtube(url=url, path=path)

        head_res = requests.head(url, timeout=10)
        head_res.raise_for_status()

        cType = head_res.headers["content-type"].split("/")
        etag = url.rsplit("/", 2)[-2]
        imf = url.split("/")
        fname = imf[len(imf) - 1].split(".")

        if len(cType) > 1 and len(fname) > 1 and cType[0] == "image":
            # Image file
            os.makedirs(f"{cfg['localzippath']}{path}", exist_ok=True)
            return getImageFromUrl(
                url=url,
                cType=head_res.headers["content-type"],
                filename=f"{cfg['localzippath']}{path}/{etag}_{fname[0]}.{cType[1]}"
            )
        elif len(cType) > 1 and cType[0] == "video":
            # Video file
            os.makedirs(f"{cfg['localzippath']}{path}", exist_ok=True)
            return getMediaFromUrl(
                url=url,
                cType=head_res.headers["content-type"],
                filename=f"{cfg['localzippath']}{path}/{etag}_{fname[0]}.{cType[1]}"
            )

        else:
            return False
    except Exception as e:
        return False


def strtobool(val):
    """ Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    if val is None:
        return False
    val = val.lower()
    if val in ['y', 'yes', 't', 'true', 'on', '1']:
        return True
    elif val in ['n', 'no', 'f', 'false', 'off', '0']:
        return False
    return False


class CounterCustomError(Exception):
    def __init__(self, error_dict):
        self.error_dict = error_dict

    def extract_google_api_error_resp(self):
        try:
            error_details = json.loads(self.error_dict.content.decode('utf-8'))
            return {
                'status_code': self.error_dict.resp.status,
                'error_details': error_details,
                'message': error_details["error"]["message"]
            }
        except Exception as e:
            return {
                'status_code': HTTPStatus.INTERNAL_SERVER_ERROR,
                'error_details': str(e),
                'message': str(e)
            }

    def extract_google_api_error_response(self):
        try:
            error_details = json.loads(self.error_dict.response.text)
            return {
                'status_code': self.error_dict.response.status_code,
                'error_details': error_details,
                'message': error_details["error"]["message"]
            }
        except Exception as e:
            return {
                'status_code': HTTPStatus.INTERNAL_SERVER_ERROR,
                'error_details': str(e),
                'message': str(e)
            }

    def extract_google_api_error_profile(self):
        return self.extract_google_api_error_resp()

    def extract_google_api_error_playlist(self):
        return self.extract_google_api_error_response()

    def extract_google_api_error_comments(self):
        return self.extract_google_api_error_resp()

    def extract_google_api_error_channel_search(self):
        return self.extract_google_api_error_resp()

    def extract_google_api_error_captions(self):
        return self.extract_google_api_error_resp()

    def extract_google_api_error_channel_all_videos(self):
        return self.extract_google_api_error_resp()

    def extract_telegram_profile_error(self):
        return {
            "message": self.error_dict,
            "status_code": HTTPStatus.NOT_FOUND
        }

    def extract_twitter_error(self):
        try:
            return self.error_dict
        except Exception as e:
            return {
                'status_code': HTTPStatus.INTERNAL_SERVER_ERROR,
                'error_details': str(e),
                'message': str(e)
            }

    def extract_twitch_api_error_profile(self):
        return {
            "message": self,
            "status_code": HTTPStatus.INTERNAL_SERVER_ERROR
        }

    def extract_twitch_get_video_api_error_profile(self):
        try:
            return self.error_dict
        except Exception as e:
            return {
                'status_code': HTTPStatus.INTERNAL_SERVER_ERROR,
                'error_details': str(e),
                'message': str(e)
            }


class CounterCustomErrorTranscriptsDisabled(Exception):
    def __init__(self, error_dict, language=None):
        self.error_dict = error_dict
        self.language = language

    def extract_no_captions_errors(self):
        if self.language:
            message = f"Invalid captions"
            return {
                "message": message,
                "status_code": HTTPStatus.BAD_REQUEST
            }
        return {
            "message": self,
            "status_code": HTTPStatus.BAD_REQUEST
        }


def pack_twitch_result(uuiddirname, result, info):
    name = "twitch"

    jd = json.dumps({"twitch": info}, ensure_ascii=False, indent=4, separators=(',', ': '))

    with open(f"{result}/{name}.json", "w", encoding="utf-8") as f:
        f.write(jd)

    createZip(dirName=uuiddirname)


def upload_to_hdfs(uuiddirname):
    filename = f"{uuiddirname}.zip"
    if os.path.exists(filename):
        return writeOnHdfs(filename=filename)
    return None


def download_media(tweets, uuiddirname):
    for i in tweets:
        entities = i.get("entities", {})
        urls = entities.get("urls", [])
        for url_data in urls:
            images = url_data.get("images", [])
            for image_data in images:
                url = image_data["url"]
                extension = ".jpg" if "format=jpg" in url else ""
                media_key = f"{uuid.uuid4()}{extension}"
                download_file(url, uuiddirname, media_key)


def calculate_video_size_mb(bitrate, duration):
    duration_seconds = duration / 1000
    file_size_bits = bitrate * duration_seconds
    file_size_bytes = file_size_bits / 8
    return file_size_bytes / (1024 * 1024)


def download_twitter_media(profile, attachments, media_dir):
    createFile(media_dir)
    if profile:
        profile_image_url = profile.get('profile_image_url')
        extension = profile_image_url.split(".")[-1]
        download_file(profile_image_url, media_dir, f'profile_image.{extension}')
    for attachment in attachments:
        extension = None
        dowload_video_flag = False
        max_video_size_mb = cfg['twitter']['max_video_size_mb']
        variants = attachment.get('variants')
        if variants:
            dowload_video_flag = True
            filtered_video_list = [
                video for video in variants if 'bit_rate' in video and
                calculate_video_size_mb(video.get('bit_rate'), attachment.get('duration_ms', 0)) < max_video_size_mb]
            max_bitrate_video = max(filtered_video_list, key=lambda x: x['bit_rate'])
            if attachment.get('type') == 'video':
                extension = 'mp4'
            url = max_bitrate_video.get('url', '')
        else:
            url = attachment.get('url', '')
        if not extension:
            extension = url.split(".")[-1]
        media_key = f"{attachment.get('media_key', '')}.{extension}"
        if dowload_video_flag:
            download_video(url, media_dir, media_key)
        else:
            download_file(url, media_dir, media_key)


def download_file(url, uuiddirname, media_key):
    try:
        destination_path = f"counter{os.sep}{uuiddirname}{os.sep}{media_key}"
        response = requests.get(url)
        if response.status_code == HTTPStatus.OK:
            with open(destination_path, 'wb') as file:
                file.write(response.content)
    except Exception as e:
        print(e)


def download_video(url, uuiddirname, media_key):
    try:
        destination_path = f"counter{os.sep}{uuiddirname}{os.sep}{media_key}"
        response = requests.get(url)
        if response.status_code == HTTPStatus.OK:
            with open(destination_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
    except Exception as e:
        print(e)
