from YoutubeCrawler.Youtubecrawler import Youtubecrawler

youtube_api = ""

if __name__ == "__main__":
    list_keyword = ["polvere di stelle"]
    # initialize Youtube crawler object
    mex = Youtubecrawler(youtube_api)

    # mex.get_comment_by_video_id('3uENPFDlfWY') # get list of comments by video id
    # mex.get_playlist_channel_by_id('UCjS3AJDotyaER6SoGbVLVFQ') # get playlist of channel by channel id
    # mex.get_info_channel_by_id('UCjS3AJDotyaER6SoGbVLVFQ')  # get info channel by channel id
    publishedAfter = "2019-09-21T00:00:00Z"  # mandatory
    publishedBefore = "2020-09-22T00:00:00Z"  # mandatory
    location = "41.902782, 12.496366"  # mandatory
    location_radius = "100km"  # mandatory
    channel_id = "UCjS3AJDotyaER6SoGbVLVFQ"  # id canale ligabue

    # 1 API Call - cerco polvere di stelle nel canale di ligabue
    mex.search(
        list_keyword,
        channel_id=channel_id,
        publishedAfter="",
        publishedBefore="",
        location="",
        location_radius="",
    )  # search by keyword list + channel video + time interval + location

    # 2 API Call - cerco polvere di stelle nel canale di ligabue specificando l'intervallo temporale
    mex.search(
        list_keyword,
        channel_id=channel_id,
        publishedAfter=publishedAfter,
        publishedBefore=publishedBefore,
        location="",
        location_radius="",
    )  # search by keyword list + channel video + time interval + location

    # 3 API Call - cerco polvere di stelle nel canale di ligabue specificando l'intervallo temporale e posizione
    # n.b. questa è molto selettiva, con questa query non mi restituisce nulla
    mex.search(
        list_keyword,
        channel_id=channel_id,
        publishedAfter=publishedAfter,
        publishedBefore=publishedBefore,
        location=location,
        location_radius=location_radius,
    )  # search by keyword list + channel video + time interval + location

    # 4 API Call - cerco polvere di stelle specificando l'intervallo temporale
    mex.search(
        list_keyword,
        channel_id="",
        publishedAfter=publishedAfter,
        publishedBefore=publishedBefore,
        location="",
        location_radius="",
    )  # search by keyword list + channel video + time interval + location

    # 5 API Call - cerco polvere di stelle specificando l'intervallo temporale e la posizione
    mex.search(
        list_keyword,
        channel_id="",
        publishedAfter=publishedAfter,
        publishedBefore=publishedBefore,
        location=location,
        location_radius=location_radius,
    )  # search by keyword list + channel video + time interval + location
