import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path

from telethon import functions
from telethon.sync import TelegramClient
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.users import GetFullUserRequest

import counterUtilites as myutil
from zipfile import ZipFile
from telethon.sessions import StringSession


sys.path.append(str(Path(".").absolute().parent))

cfg = myutil.setConfig(file="app-config.yml")


class Telegramcrawler:
    DIRECTORY = "counter/"

    def __init__(
        self,
        phone,
        api_id="",
        api_hash="",
        list_keyword=None,
        campaign_id=None
    ):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        session = self.get_session()

        self._client = TelegramClient(StringSession(session),
                                      api_id,
                                      api_hash,
                                      connection_retries=10,
                                      retry_delay=5,
                                      loop=loop)

        self._list_keyword = list_keyword if list_keyword else []
        self._phone = phone
        self._list_channels = []

        self.uuiddirname = campaign_id if campaign_id is not None else str(uuid.uuid1())
        if not os.path.isdir(self.DIRECTORY):
            os.makedirs(self.DIRECTORY)

        path = f"{self.DIRECTORY}{self.uuiddirname}"
        if not os.path.exists(path):
            os.makedirs(path)

    def get_session(self):
        try:
            with open("session") as f:
                return f.readline()
        except EnvironmentError:
            pass
        return ""

    def all_profile_photo(self, user):
        """ Download user profile photos """
        self._client.connect()
        try:
            i = 0
            for photo in self._client.iter_profile_photos(user):
                self._client.download_media(photo, f"{self.DIRECTORY}{self.uuiddirname}/{user}{i}")
                i += 1
        except:
            print("all_profile_photo - user or channel not found")
            pass
        self._client.disconnect()

    def get_user_bio(self, user):
        try:
            self._client.connect()
            user_request = GetFullUserRequest(user)
            full_user = self._client(user_request).full_user
            self.all_profile_photo(user)
            self._client.disconnect()
            return {
                "bio": full_user.about,
                "uid": full_user.id,
                "username": user
            }
        except Exception as e:
            self._client.disconnect()
            raise myutil.CounterCustomError(e)

    def get_channel_description(self, channel):
        try:
            self._client.connect()
            ch = self._client.get_entity(channel)
            ch_full = self._client(GetFullChannelRequest(channel=ch))
            bio = ch_full.full_chat.about  # this is what you need
            self.all_profile_photo(channel)
            self._client.disconnect()
            return {
                "bio": bio,
                "username": channel
            }
        except Exception as e:
            self._client.disconnect()
            raise myutil.CounterCustomError(e)

    def get_channels(self, n_channels):
        try:
            self._client.connect()

            for s in self._list_keyword:
                result = self._client(functions.contacts.SearchRequest(q=s, limit=n_channels))

                for r in result.chats:
                    self._list_channels.append({
                        "id": r.id,
                        "title": r.title,
                        "participants_count": r.participants_count,
                        "username": r.username,
                        "verified": r.verified,
                        "date": r.date.strftime("%m/%d/%Y, %H:%M:%S"),
                    })
            self._client.disconnect()
            return self._list_channels
        except Exception as e:
            self._client.disconnect()
            raise myutil.CounterCustomError(e)

    def get_posts_all(self, channelname, order, savemedia, search=None, limit=None, maxmedialimit=None):
        medialimitcounter = 0
        limitcounter = 0
        try:
            entity_data = {}
            self._client.connect()
            list_mex = []

            post_messages = self._client.iter_messages(channelname,
                                                       limit=limit,
                                                       reverse=order,
                                                       search=search)
            for message in post_messages:
                if limitcounter > limit:
                    break

                text = message.text
                if text is not None:
                    limitcounter += 1

                sender_id = message.sender_id
                if sender_id:
                    try:
                        if sender_id in entity_data:
                            author = entity_data[sender_id]
                        else:
                            sender = self._client.get_entity(sender_id)
                            author = sender.username
                            entity_data[sender_id] = author
                    except:
                        author = None

                list_mex.append({
                    "id": message.id,
                    "text": text,
                    "data": message.date.strftime("%m/%d/%Y, %H:%M:%S"),
                    "author": author,
                    "sender_id": sender_id
                })
                if savemedia and message.media is not None:
                    try:
                        if isinstance(message.media, types.MessageMediaPhoto):
                            # TODO - more research on how to get the size of the photo
                            size = 1
                        elif isinstance(message.media, types.MessageMediaDocument):
                            size = message.media.document.size
                        else:
                            raise AttributeError
                    except AttributeError:
                        print("error get document size")
                        continue

                    if size < cfg["telegram"].get("maxMediaSize", 5000000):
                        self._client.download_media(message.media,
                                                    f"{self.DIRECTORY}{self.uuiddirname}/{channelname}{message.id}")
                        medialimitcounter += 1
                    else:
                        print("size to big")
                if medialimitcounter >= maxmedialimit:
                    break
            self._client.disconnect()
            return list_mex
        except Exception as e:
            self._client.disconnect()
            raise myutil.CounterCustomError(e)

    def get_channel_posts(self, channelname, limit, order, savemedia, search):
        try:
            self._client.connect()

            list_mex = []
            post_messages = self._client.get_messages(channelname,
                                                      limit=limit,
                                                      reverse=order,
                                                      search=search,
                                                      wait_time=180)
            for messages in post_messages:
                if messages.text is not None:
                    list_mex.append({
                        "id": messages.id,
                        "text": messages.text,
                        "data": messages.date.strftime("%m/%d/%Y, %H:%M:%S"),
                        "author": messages.post_author,
                    })

                    if savemedia and messages.media is not None:
                        self._client.download_media(messages.media,
                                                    f"{self.DIRECTORY}{self.uuiddirname}/{channelname}{messages.id}")

            self._client.disconnect()
            return list_mex
        except Exception as e:
            self._client.disconnect()
            raise myutil.CounterCustomError(e)

    def get_comments(self, channelname, messageid, limit=None, order=False, savemedia=True):
        try:
            self._client.connect()
            list_comment = []
            comments = self._client.iter_messages(channelname, reply_to=messageid, reverse=order, limit=limit)
            for comment in comments:
                list_comment.append({
                    "id": comment.id,
                    "text": comment.text,
                    "data": comment.date.strftime("%m/%d/%Y, %H:%M:%S"),
                    "author": comment.from_id.user_id,
                    "replay_post_id": comment.reply_to_msg_id,
                    "id_post": messageid
                })

                if savemedia and comment.media is not None:
                    self._client.download_media(comment.media,
                                                f"{self.DIRECTORY}{self.uuiddirname}/{channelname}_{messageid}_{comment.id}")
            self._client.disconnect()
            return list_comment
        except Exception as e:
            self._client.disconnect()
            raise myutil.CounterCustomError(e)

    def dump_to_file_and_upload_to_hdfs(self, itemToDump, fileName):
        # dump to file
        fileName = f"{fileName}{str(time.time() * 1000)}"
        path = f"{self.DIRECTORY}{self.uuiddirname}/{fileName}.json"

        with open(path, "a", encoding="utf8") as outfile:
            json.dump(itemToDump, outfile, indent=4, ensure_ascii=False)
        with ZipFile(f"{self.DIRECTORY}{self.uuiddirname}.zip", "w", allowZip64=True) as zipObj:
            for folderName, subfolders, filenames in os.walk(self.DIRECTORY + self.uuiddirname):
                for filename in filenames:
                    filePath = os.path.join(folderName, filename)
                    zipObj.write(filePath, filePath)
            zipObj.close()
        
        # upload to hdfs
        return myutil.writeOnHdfs(filename=f"{self.uuiddirname}.zip")
