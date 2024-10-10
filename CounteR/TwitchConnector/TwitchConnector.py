from http import HTTPStatus

from emoji import demojize
import socket
import time
import re
from datetime import datetime
import pandas as pd
import json
import requests
import random
import names
import os
import datetime as dt

import counterUtilites as myutil

global cfg
cfg = myutil.setConfig(file="app-config.yml")


class Twichcrawler:
    DIRECTORY = "files/"

    def __init__(
        self,
        oauthtoken,
        client_id,
        client_secret,
        channel="",
        server="irc.chat.twitch.tv",
        port=6667
    ):
        self.server = server
        self.port = port
        self.nickname = names.get_first_name() + "%i" % random.randint(10000, 99999)
        self.channel = channel
        self.sock = None
        self.resp = None
        self.now = None
        self.oauthtoken = oauthtoken
        self.client_id = client_id
        self.client_secret = client_secret

    def create_connection(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.server, self.port))
        self.sock.send(f"PASS {self.oauthtoken}\n".encode("utf-8"))
        self.sock.send(f"NICK {self.nickname}\n".encode("utf-8"))
        self.sock.send(f"JOIN {self.channel}\n".encode("utf-8"))
        try:
            self.resp = self.sock.recv(2048).decode("utf-8")
        except:
            raise myutil.CounterCustomError("Connection reset by peer. Try again")

        if self.resp == "":
            raise myutil.CounterCustomError("Warning, connection failed")

    def save_direct(self, timeout):
        self.now = datetime.now()

        timeout_start = time.time()
        with open(f"{self.channel}_{self.now.strftime('%d-%m-%Y_%H-%M-%S')}.log", "w") as fhandle:
            while time.time() < timeout_start + timeout:
                try:
                    self.resp = self.sock.recv(2048).decode("utf-8")
                except Exception as e:
                    raise myutil.CounterCustomError(str(e))

                if self.resp.startswith("PING"):
                    self.sock.send("PONG\n".encode("utf-8"))
                elif len(self.resp) > 0:
                    fhandle.write(f'{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}-{demojize(self.resp)}\n')

    def save_video(self, uuiddirname, seconds):
        channel = self.channel.replace("#", "")
        try:
            os.system(f'streamlink "--twitch-api-header=Authorization={self.oauthtoken} '
                      f'--twitch-disable-ads --retry-max 1" '
                      f'"https://www.twitch.tv/{channel}" 480p --output '
                      f'"counter/{uuiddirname}/{channel}.ts" --hls-duration {dt.timedelta(seconds=seconds)}')
        except Exception as e:
            raise myutil.CounterCustomError(e)
        return f"counter/{uuiddirname}/"

    def export_chat(self):
        data = []
        self.__get_token()
        with open(f'{self.channel}_{self.now.strftime("%d-%m-%Y_%H-%M-%S")}.log', "r", encoding="utf-8") as f:
            lines = f.read().split("\n\n")

            for line in lines:
                if line != "":
                    time_logged = line.split(" - ")[0].strip()
                    time_logged = datetime.strptime(time_logged, "%Y-%m-%d_%H:%M:%S")

                    username_message = line.split(" - ")[1:]
                    username_message = "—".join(username_message).strip()

                    username_message = re.search(":(.*)\!.*@.*\.tmi\.twitch\.tv PRIVMSG #(.*) :(.*)", username_message)

                    if username_message:
                        username = username_message.group(1)
                        channel = username_message.group(2)
                        message = username_message.group(3)

                        reply_usr = "channel message"
                        for u in message.split():
                            if "@" in u:
                                reply_usr = u

                        data.append({
                            "timestamp": str(time_logged),
                            "channel": channel,
                            "username": username,
                            "message": message,
                            "reply_usr": reply_usr,
                        })

        df = pd.DataFrame().from_records(data)
        df.to_csv(f"{self.channel}_{self.now.strftime('%d-%m-%Y_%H-%M-%S')}.csv", index=False, encoding="utf-8",)
        return data

    def __get_token(self):
        try:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
            }

            data = f"client_id={self.client_id}&client_secret={self.client_secret}&grant_type=client_credentials"

            response = requests.post("https://id.twitch.tv/oauth2/token", headers=headers, data=data)
            res = json.loads(response.text)
            self.token = res["access_token"]
        except:
            print("Max retries exceeded with url: /oauth2/token")

    def search_channels(self, query):
        self.now = datetime.now()
        self.__get_token()
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Client-Id": self.client_id
        }

        params = {
            "query": query,
            "live_only": "true",
            "first": "100"
        }

        res = requests.get("https://api.twitch.tv/helix/search/channels", params=params, headers=headers)
        if res.status_code != HTTPStatus.OK:
            error_data = {
                'status_code': res.status_code,
                'error_details': res.content,
                'message': res.reason
            }
            raise myutil.CounterCustomError(error_data)

        res = json.loads(res.text)
        with open(f"{self.now.strftime('%d-%m-%Y %H-%M-%S')}json_channels.json", "w", encoding="utf-8") as outfile:
            json.dump(res, outfile, indent=8)

        return res

    def get_channel_info(self):
        self.__get_token()
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Client-Id": self.client_id,
        }
        params = {
            "login": self.channel.replace("#", "")
        }
        res = requests.get("https://api.twitch.tv/helix/users", params=params, headers=headers)
        if res.status_code != HTTPStatus.OK:
            error_data = {
                'status_code': res.status_code,
                'error_details': res.content,
                'message': res.reason
            }
            raise myutil.CounterCustomError(error_data)

        res = json.loads(res.text)
        user_id = res["data"][0]["id"]
        params = {
            "broadcaster_id": user_id
        }
        res = requests.get(f"https://api.twitch.tv/helix/channels", params=params, headers=headers)
        if res.status_code != HTTPStatus.OK:
            error_data = {
                'status_code': res.status_code,
                'error_details': res.content,
                'message': res.reason
            }
            raise myutil.CounterCustomError(error_data)

        return res.json()["data"][0]


class TwitchChatIRC:
    __HOST = 'irc.chat.twitch.tv'
    # __DEFAULT_NICK = f"{cfg['twitch']['nickname']}"
    # __DEFAULT_PASS = cfg['twitch']['password']
    __DEFAULT_NICK = 'justinfan67420'
    __DEFAULT_PASS = 'SCHMOOPIIE'
    __PORT = 6667

    __PATTERN = re.compile(r'@(.+?(?=\s+:)).*PRIVMSG[^:]*:([^\r\n]*)')
    # __PATTERN = re.compile(r':(.*)\!.*@.*\.tmi\.twitch\.tv PRIVMSG #(.*) :(.*)')

    def __init__(self, nickname=None, password=None):
        # create new socket
        self.__SOCKET = socket.socket()

        # start connection
        self.__SOCKET.connect((self.__HOST, self.__PORT))

        # log in
        self.__send_raw('CAP REQ :twitch.tv/tags')

        nickname = nickname if nickname else self.__DEFAULT_NICK
        password = password if password else self.__DEFAULT_PASS

        self.__send_raw(f'PASS {password}')
        self.__send_raw(f'NICK {nickname}')

    def __send_raw(self, string):
        self.__SOCKET.send((string + '\r\n').encode('utf-8'))

    def __recvall(self, buffer_size):
        data = b''
        while True:
            part = self.__SOCKET.recv(buffer_size)
            data += part
            if len(part) < buffer_size:
                break
        return data.decode('utf-8')  # ,'ignore'

    def __join_channel(self, channel_name):
        self.__send_raw(f'JOIN #{channel_name.lower()}')

    def close_connection(self):
        self.__SOCKET.close()

    def listen_live(self, channel_name, timeout=None, message_timeout=None, buffer_size=4096):
        self.__join_channel(channel_name)
        self.__SOCKET.settimeout(message_timeout)

        readbuffer = ''
        file_name = f"{channel_name}_{datetime.now().strftime('%d-%m-%Y_%H-%M-%S')}.log"
        try:
            timeout_start = time.time()
            with open(file_name, "w") as fhandle:
                while time.time() < timeout_start + timeout:
                    new_info = self.__recvall(buffer_size)
                    readbuffer += new_info

                    if 'PING :tmi.twitch.tv' in readbuffer:
                        self.__send_raw('PONG :tmi.twitch.tv')

                    matches = list(self.__PATTERN.finditer(readbuffer))

                    if matches:
                        fhandle.write(f"{demojize(readbuffer)}\n")
        except Exception as e:
            print('Unknown Error:', e)
            # TODO - not sure we should raise an error
            # raise e
        return file_name

    def export_chat(self, file_name, channel):
        data = []
        with open(file_name, "r", encoding="utf-8") as f:
            lines = f.read().split("\n")
            ids = []
            for line in lines:
                if "@badge-info" in line:
                    try:
                        _data = {
                            "display-name": None,
                            "id": None,
                            "room-id": None,
                            "tmi-sent-ts": None,
                            "user-id": None,
                            "channel": channel,
                            "username": None,
                            "message": None
                        }
                        id = None
                        for i in line.split(";"):
                            if "display-name" in i:
                                _data["display-name"] = i.replace("display-name=", "")
                            elif "user-type=" in i:
                                try:
                                    i = i.split("PRIVMSG")
                                    _data["username"] = i[0].replace("user-type=", "").split(":")[1].split("!")[0].strip()
                                    _data["message"] = i[1].replace(f"#{channel} :", "").strip()
                                except:
                                    pass
                            elif "vip=" in i:
                                try:
                                    i = i.split("PRIVMSG")
                                    _data["username"] = i[0].replace("vip=", "").split(":")[1].split("!")[0].strip()
                                    _data["message"] = i[1].replace(f"#{channel} :", "").strip()
                                except:
                                    pass
                            elif "tmi-sent-ts" in i:
                                _data["tmi-sent-ts"] = i.replace("tmi-sent-ts=", "")
                            elif "room-id" in i:
                                _data["room-id"] = i.replace("room-id=", "")
                            elif "user-id" in i:
                                _data["user-id"] = i.replace("user-id=", "")
                            elif "id" in i:
                                id = i.replace("id=", "")
                                _data["id"] = id

                        if id is not None:
                            if id in ids:
                                continue
                            else:
                                ids.append(id)
                        data.append(_data)
                    except Exception as e:
                        print(e)
                        print(line.split(";"))
                        pass

        df = pd.DataFrame().from_records(data)
        df.to_csv(file_name.replace(".log", '.csv'), index=False, encoding="utf-8",)
        return data
