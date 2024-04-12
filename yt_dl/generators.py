
import random
from io import StringIO

import pandas as pd
from boto3.dynamodb.conditions import Key
from tqdm.auto import tqdm

from .aws_helpers import DynamoDBHelper, S3Helper
from .channel_utilities import ChannelMetadataHelper


class ReportGenerator:
    """
    Utility class for generating reports based on the current state of the reports table.

    This class provides methods to retrieve data from the reports table, process it, and generate reports.

    """

    SUCC_DOW_REPORT_COLUMNS = ['video_id', 'video_title', 'video_duration', 'video_view_count', 'video_like_count']
    FAILED_DOW_REPORT_COLUMNS = ['video_id', 'reason']

    def __init__(self, dynamo_hlp_instance: DynamoDBHelper, s3_hlp_instance: S3Helper) -> None:

        # Initialize dynamo helper instance 
        self.dynamo_hlp_instance = dynamo_hlp_instance
        # Initialize s3 helper instance
        self.s3_hlp_instance = s3_hlp_instance
        
    
    @classmethod
    def _unpack_responses(cls, reports):
        succ_videos = dict()
        failed_videos = dict()
        reports_csv = pd.DataFrame(reports)
        for channel_id in reports_csv['channel_id'].unique():
            
            channel_csv = reports_csv.loc[reports_csv['channel_id']==channel_id]
            succ_videos[channel_id] = channel_csv.loc[channel_csv['download_status']==True][cls.SUCC_DOW_REPORT_COLUMNS]
            failed_videos[channel_id] = channel_csv.loc[channel_csv['download_status']==True][cls.FAILED_DOW_REPORT_COLUMNS]
        return succ_videos, failed_videos
    
    def _import_response(self, response: pd.DataFrame, key: str) -> None:
        csv_buffer=StringIO()
        response.to_csv(csv_buffer)
        content = csv_buffer.getvalue()
        # Upload the response object
        self.s3_hlp_instance.upload_object(
            body=content,
            bucket='ytdlreports',
            key=key
        )
        
    def _import_dl_responses(self, succ_dl_chs_responses, failed_dl_chs_responses):
        # Import successfully downloaded videos responses
        for channel_id, response in succ_dl_chs_responses.items():
            self._import_response(response, f'{channel_id}/successsfully_downloaded.csv')
        # Import unsuccessfully downloaded videos responses
        for channel_id, response in failed_dl_chs_responses.items():
            self._import_response(response, f'{channel_id}/unsuccesssfully_downloaded.csv')

    def generate_reports(self):
        # Query all table items
        responses = self.dynamo_hlp_instance.query_all_table_items()
        # Unpack the responses by channel
        succ_dl_chs_responses, failed_dl_chs_responses = self._unpack_responses(responses)
        # Import  to S3 bucket
        self._import_dl_responses(succ_dl_chs_responses, failed_dl_chs_responses)


class TaskGenerator():
    """
    Task generator class for extracting video IDs from a channel, organizing them into tasks, 
    checking previous task executions, and skipping tasks that have already been executed.

    This class facilitates the creation and management of tasks related to video extraction processes.
    """
        
    @staticmethod
    def extract_channel_video_urls(channels: list, dynamo_hlp_instance: DynamoDBHelper, shuffle: bool = False) -> list:

        res_tasks_id = list()
        for channel_id, channel_url in tqdm(channels, desc='channels'):
            channel_id = channel_id.replace('@', '') if '@' in channel_id else channel_id
            videos_metadata = ChannelMetadataHelper._get_channel_videos(channel_url)

            for video_metadata in tqdm(videos_metadata, desc='videos', leave=False):
                video_id = video_metadata.get('videoId')

                # Check past responses
                res = dynamo_hlp_instance.query_items({
                    "KeyConditionExpression": Key('channel_id').eq(channel_id) & Key('video_id').eq(video_id),
                    "ProjectionExpression": 'download_status'
                })

                # Skip if already tried
                if res['Count'] > 0:
                    continue

                res_tasks_id.append(video_id)
        
        if shuffle is True:
            random.shuffle(res_tasks_id)
            
        return res_tasks_id
