from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import counterUtilites as myutil

cfg = myutil.setConfig(file="app-config.yml")


telegram_api_id = cfg["telegram"].get("api_id")
telegram_api_hash = cfg["telegram"].get("api_hash")
channel_name = "Testtelegramchannelassist"


def get_session():
    try:
        with open("session") as f:
            return f.readline()
    except EnvironmentError:
        pass
    return ""


session = get_session()
client = TelegramClient(StringSession(session), telegram_api_id, telegram_api_hash)

client.connect()

for message in client.iter_messages(channel_name):
    author = None
    sender_id = message.sender_id
    if sender_id:
        try:
            sender = client.get_entity(sender_id)
            author = sender.username
        except:
            author = None
    print(f"Author: {author} | SenderID: {sender_id} | Message: {message.text}")


client.disconnect()
