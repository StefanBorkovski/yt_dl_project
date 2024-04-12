import pandas as pd
import requests
import scrapetube
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup

from .aws_helpers import DynamoDBHelper


class ChannelMetadataHelper:
    """Helper class for handling channel metadata operations such as retrieving channel IDs and videos.
    """

    @staticmethod
    def _get_channel_id(channel_url: str) -> str:
        resp = requests.get(channel_url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        return soup.select_one('meta[property="og:url"]')['content'].strip('/').split('/')[-1]
    
    @staticmethod
    def _get_channel_videos(channel_url: str) -> list:
        videos = scrapetube.get_channel(channel_url=channel_url)
        return list(videos)

class ChannelPerformanceUtilities():
    """A utilities class for checking channel performance constraints.
    """

    CHECK_AFTER_N_VIDEOS = 5
    INACTIVE_CHANNELS = []

    @classmethod
    def _check_channel_responses(cls, response_df: pd.DataFrame, input_cfg):

        successfull_dl_df = response_df.loc[response_df['download_status'] == True]

        # Check total downloaded hours
        total_dl_h = round(successfull_dl_df['video_duration'].sum() / 60 / 60, 2)
        if total_dl_h > input_cfg['max_download_H_per_channel']:
            return 'Max hours per channel exceeded'

        # Check total downloaded videos
        total_dl_v = successfull_dl_df.shape[1]
        if total_dl_v > input_cfg['max_downloaded_videos_per_channel']:
            return 'Max downloaded videos per channel exceeded'
        
        # Get download success ratio
        if response_df.shape[0] >= cls.CHECK_AFTER_N_VIDEOS:
            succ_ratio = response_df.shape[0] / total_dl_v
            if succ_ratio < input_cfg['min_successful_download_ration']:
                return 'Minimum success rate not achieved'
        
    @classmethod
    def check_channel_constraints(cls, dynamo_hlp_instance: DynamoDBHelper, channel_id: str, video_id: str, input_cfg: dict) -> bool:
        
        # Check if channel is active
        response = dynamo_hlp_instance.query_items({
            "KeyConditionExpression": Key('channel_id').eq(channel_id),
            "FilterExpression": Key('channel_status').eq('Inactive')
        })
        if response['Count'] > 0:
            cls.INACTIVE_CHANNELS.append(channel_id)
            return False

        # Load past responses
        response = dynamo_hlp_instance.query_items({
            "KeyConditionExpression": Key('channel_id').eq(channel_id),
            "FilterExpression": Key('channel_status').eq('Active'),
            "ProjectionExpression": 'download_status, video_duration'
        })
        if response['Count'] > 0:
            # Check constraints
            constraint_failure_msg = cls._check_channel_responses(pd.DataFrame(response['Items']), input_cfg)
            if constraint_failure_msg is not None:
                # Import download request status
                dynamo_hlp_instance.import_item(
                    item = {
                        "channel_id": channel_id,
                        "video_id": video_id,
                        "update_time": pd.Timestamp.now().strftime('%Y-%m-%d %H:%m:%S'),
                        "download_status": False,  # Sample download success status
                        "channel_status": "Inactive",
                        "reason": constraint_failure_msg
                })
                return False
        # Constraints met
        return True
