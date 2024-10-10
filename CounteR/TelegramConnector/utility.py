import counterUtilites as myutil


def get_setting_telegram():
    config = myutil.setConfig(file="app-config.yml")

    # These example values won't work. You must get your own api_id and
    # api_hash from https://my.telegram.org, under API Development.
    # (1) Use your own values here
    api_id = config["telegram"]["api_id"]
    api_hash = config["telegram"]["api_hash"]

    # (2) Create the client and connect
    phone = config["telegram"]["phone"]
    username = config["telegram"]["username"]

    return api_id, api_hash, phone, username
