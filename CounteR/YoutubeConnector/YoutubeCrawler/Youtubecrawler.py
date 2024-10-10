import json
import sys
from pathlib import Path

import pandas as pd
from googleapiclient.discovery import build
from youtube_api import YouTubeDataAPI, parsers
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptAvailable, NotTranslatable, \
    TranslationLanguageNotAvailable, NoTranscriptFound

import counterUtilites as myutil

sys.path.append(str(Path(".").absolute().parent))

DEVELOPER_KEY = "youtube api"

cfg = myutil.setConfig(file="app-config.yml")

maxRequests = cfg["youtube"].get("maxRequests", 5)
maxResults = cfg["youtube"].get("maxResults", 5)
maxComments = cfg["youtube"].get("maxComments", 100)
maxCaptions = cfg["youtube"].get("maxCaptions", 100)
maxPlaylist = cfg["youtube"].get("maxPlaylist", 100)


class Youtubecrawler:
    def __init__(
        self,
        api_key,
    ):
        self._api_key = api_key
        self._result = pd.DataFrame({})
        self._yt = YouTubeDataAPI(self._api_key)
        self._service = build("youtube", "v3", developerKey=self._api_key)

    def get_video_by_keywords(self, list_keyword):
        searches = self._yt.search(q=" ".join(map(str, list_keyword)), max_results=maxResults)
        print(searches)
        list_searches = []
        for s in searches:
            new_video = {
                "video_id": s["video_id"],
                "channel_title": s["channel_title"],
                "channel_id": s["channel_id"],
                "collection_date": s["collection_date"].strftime("%m/%d/%Y, %H:%M:%S"),
                "video_title": s["video_title"],
                "video_description": s["video_description"],
                "video_thumbnail": s["video_thumbnail"]
            }
            list_searches.append(new_video)
        self._result = pd.DataFrame(searches)
        return list_searches

    def get_info_channel_by_id(self, id):
        ch_request = self._service.channels().list(part="brandingSettings,"
                                                        "contentDetails,"
                                                        "contentOwnerDetails,"
                                                        "id,"
                                                        "localizations,"
                                                        "snippet,"
                                                        "statistics,"
                                                        "status,"
                                                        "topicDetails",
                                                   id=id)
        ch_response = ch_request.execute()
        playlist_id = ch_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        try:
            videos = self._service.playlistItems().list(part=["snippet"],
                                                        playlistId=playlist_id,
                                                        maxResults=maxResults).execute()
        except Exception as e:
            raise myutil.CounterCustomError(e)

        ch_response["videos"] = videos
        return ch_response

    def get_id_by_channel_name(self, channel_name):
        return self._service.search().list(part="id",
                                           q=channel_name,
                                           type="channel",
                                           maxResults="1").execute().get('items')[0].get('id')

    def get_videos_by_playlist_id(self, id):
        try:
            self._yt.get_videos_from_playlist_id(playlist_id=id, parser=parsers.raw_json)
        except Exception as e:
            raise myutil.CounterCustomError(e)

    def get_playlist_channel_by_id(self, id):
        res = self._service.channels().list(id=id, part='contentDetails').execute()
        playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        request = self._service.playlistItems().list(part=["snippet"],
                                                     playlistId=playlist_id,
                                                     maxResults=maxResults)
        response = request.execute()
        return response

    def get_comment_by_video_id(self, video_id):
        nextPage_token = []

        comments_pop = []
        comment_id_pop = []
        reply_count_pop = []
        like_count_pop = []
        comment_author_pop = []
        comment_pubtime_temp = []
        comments_pubtime_pop = []

        replies_pop = []
        replies_like_count_pop = []
        replies_author_pop = []
        replies_parent_id_pop = []
        replies_id_pop = []
        replies_pubtime_pop = []

        comments_temp = []
        comment_id_temp = []
        reply_count_temp = []
        like_count_temp = []
        comment_author_temp = []

        replies_temp = []
        replies_like_count_temp = []
        replies_author_temp = []
        replies_parent_id_temp = []
        replies_id_temp = []
        replies_pubtime_temp = []

        number_of_requests = 0

        while True:
            try:
                response = self._service.commentThreads().list(part="snippet,replies",
                                                               videoId=video_id,
                                                               maxResults=maxComments,
                                                               order="relevance",
                                                               textFormat="plainText",
                                                               pageToken=nextPage_token).execute()
            except Exception as e:
                print("comments disabled")
                return [], []

            nextPage_token = response.get("nextPageToken")
            for item in response["items"]:
                comments_temp.append(item["snippet"]["topLevelComment"]["snippet"]["textDisplay"])
                comment_id_temp.append(item["snippet"]["topLevelComment"]["id"])
                comment_pubtime_temp.append(item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_author_temp.append(item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"])
                reply_count_temp.append(item["snippet"]["totalReplyCount"])
                like_count_temp.append(item["snippet"]["topLevelComment"]["snippet"]["likeCount"])
                if "replies" in item.keys():
                    for reply in item["replies"]["comments"]:
                        replies_author_temp.append(reply["snippet"]["authorDisplayName"])
                        replies_temp.append(reply["snippet"]["textDisplay"])
                        replies_pubtime_temp.append(reply["snippet"]["publishedAt"])
                        replies_like_count_temp.append(reply["snippet"]["likeCount"])
                        replies_parent_id_temp.append(reply["snippet"]["parentId"])
                        replies_id_temp.append(reply["id"])

                comments_pop.extend(comments_temp)
                comment_id_pop.extend(comment_id_temp)
                reply_count_pop.extend(reply_count_temp)
                like_count_pop.extend(like_count_temp)
                comment_author_pop.extend(comment_author_temp)
                comments_pubtime_pop.extend(comment_pubtime_temp)

                replies_pop.extend(replies_temp)
                replies_author_pop.extend(replies_author_temp)
                replies_id_pop.extend(replies_id_temp)
                replies_pubtime_pop.extend(replies_pubtime_temp)
                replies_like_count_pop.extend(replies_like_count_temp)
                replies_parent_id_pop.extend(replies_parent_id_temp)

            number_of_requests += 1

            if nextPage_token is None or number_of_requests > maxRequests:
                output_dict = {
                    "Comment": comments_pop,
                    "Comment ID": comment_id_pop,
                    "Replies": reply_count_pop,
                    "Publication time": comments_pubtime_pop,
                    "Likes": like_count_pop,
                    "Author": comment_author_pop,
                }

                replies_dict = {
                    "Reply": replies_pop,
                    "Reply ID": replies_id_pop,
                    "Publication time": replies_pubtime_pop,
                    "Likes": replies_like_count_pop,
                    "Author": replies_author_pop,
                    "Comment ID": replies_parent_id_pop,
                }

                output_df = pd.DataFrame(output_dict, columns=output_dict.keys())
                replies_df = pd.DataFrame(replies_dict, columns=replies_dict.keys())

                unique_df = output_df.drop_duplicates(subset=["Comment"])
                unique_df_replies = replies_df.drop_duplicates(subset=["Reply"])

                return json.loads(unique_df.to_json(orient="records"))[:maxComments], \
                    json.loads(unique_df_replies.to_json(orient="records"))[:maxComments]

    def search(
        self,
        list_keyword,
        channel_id,
        publishedAfter,
        publishedBefore,
        location,
        location_radius,
    ):
        publishedAfter, publishedBefore = myutil.translateDate(publishedAfter, publishedBefore, as_timestamp=True)
        search_data = {
            "q": list_keyword,
            "max_results": maxResults,
            "parser": parsers.raw_json
        }
        if publishedAfter and publishedBefore:
            search_data.update({
                "published_after": publishedAfter,
                "published_before": publishedBefore,
                "parser": parsers.raw_json_with_datetime
            })
        if location:
            search_data.update({
                "location": location
            })
        if location_radius:
            search_data.update({
                "location_radius": location_radius
            })
        if channel_id:
            search_data.update({
                "channel_id": channel_id
            })
        try:
            return self._yt.search(**search_data)
        except Exception as e:
            raise myutil.CounterCustomError(e)

    def generate_caption(self, transcript, language=None):
        """ Return a caption for a specific transcript """
        return {
            # if language is not set, get the language code from the caption metadata
            "language": language if language else transcript.language_code,
            # fetch the actual caption data
            "transcript": transcript.fetch()[:maxCaptions]
        }

    # Downloads the captions related to a video in a given language.
    # In no language is given, downloads all the available captions.
    # language must be a language code (e.g., 'en', 'de', etc.).
    def get_captions(self, video_id, language):
        try:
            transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
            captions_list = []
            if language:
                transcript = transcript_list.find_transcript([language])
                caption = self.generate_caption(transcript, language)
                captions_list.append(caption)
            else:
                for transcript in transcript_list:
                    caption = self.generate_caption(transcript)
                    captions_list.append(caption)
            return captions_list
        except TranscriptsDisabled as e:
            raise myutil.CounterCustomErrorTranscriptsDisabled(e)
        except NoTranscriptAvailable as e:
            raise myutil.CounterCustomErrorTranscriptsDisabled(e)
        except NotTranslatable as e:
            raise myutil.CounterCustomErrorTranscriptsDisabled(e)
        except TranslationLanguageNotAvailable as e:
            raise myutil.CounterCustomErrorTranscriptsDisabled(e)
        except NoTranscriptFound as e:
            raise myutil.CounterCustomErrorTranscriptsDisabled(e, language)
        except Exception as e:
            raise myutil.CounterCustomError(e)

    def get_all_videos_and_comments_for_channel(self, channel_id):
        videos = []
        next_page_token = None

        # Retrieve the channel's playlist ID
        try:
            channel_response = self._service.channels().list(id=channel_id, part='contentDetails').execute()
        except Exception as e:
            raise myutil.CounterCustomError(e)

        playlist_id = None
        if channel_response.get('items') and len(channel_response['items']) > 0 \
                and channel_response['items'][0].get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads'):
            playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        if not playlist_id:
            print('Error: no upload playlist ID found.')
            print(channel_response)
            return videos

        while True:
            # Retrieve the playlist items
            try:
                playlist_items = self._service.playlistItems().list(playlistId=playlist_id,
                                                                    part='snippet',
                                                                    maxResults=maxResults,
                                                                    pageToken=next_page_token).execute()
            except Exception as e:
                raise myutil.CounterCustomError(e)

            # Add the video IDs to the list
            for item in playlist_items['items']:
                video_id = item['snippet']['resourceId']['videoId']
                comments = self.get_comment_by_video_id(video_id)
                comments_data = {}
                if isinstance(comments, str):
                    comments_data['error'] = comments
                else:
                    comments_data['comments'] = comments[0]
                    comments_data['replies'] = comments[1]
                    comments_data['error'] = None

                videos.append({
                    "videoId": video_id,
                    "videoUrl": "https://www.youtube.com/watch?v=" + video_id,
                    "videoTitle": item['snippet']['title'],
                    "videoDescription": item['snippet']['description'],
                    "videoThumbnail": item['snippet']['thumbnails']['high']['url'],
                    "videoPublishedAt": item['snippet']['publishedAt'],
                    "comments": comments_data
                })

                if len(videos) > maxPlaylist:
                    return videos

            # Check if there are more pages to retrieve
            next_page_token = playlist_items.get('nextPageToken')
            if not next_page_token:
                break

        return videos
