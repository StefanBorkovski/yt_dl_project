import yt_dlp as youtube_dl

from .aws_helpers import DynamoDBHelper, S3Helper
from .channel_utilities import ChannelPerformanceUtilities
from .utils import load_yt_dl_config
from .video_utilities import VideoMetadataUtilities



class Downloader:
    """
    Class for downloading videos or audio based on preset configuration,
    checking constraints, and generating download responses.
    """

    YT_BASE_URL = 'https://www.youtube.com/watch?v='

    @classmethod
    def run(cls, video_id: str, input_cfg: dict, run_locally: bool = False):
        
        # Initialize dynamo and s3 helper instances
        dynamo_hlp_instance = DynamoDBHelper(table_name = 'VideoChannelInfoTable')
        s3_hlp_instance = S3Helper()

        # Load yt_dl configuration and set random proxy
        yt_dl_cfg = load_yt_dl_config(run_locally=run_locally)
        
        with youtube_dl.YoutubeDL(yt_dl_cfg) as ydl:
            video_url = cls.YT_BASE_URL + video_id
            
            # Get channel ID
            video_metadata = ydl.extract_info(video_url, download = False)
            channel_id = video_metadata['uploader_url'].split('@')[-1].strip()

            # Check current channel performance
            cnsts_passed = ChannelPerformanceUtilities.check_channel_constraints(dynamo_hlp_instance, channel_id, video_id, input_cfg)
            if cnsts_passed == False:
                return
                
            # Check metadata constraints
            cnsts_passed, cnsts_failure_msg, video_srt_content, calc_metadata = VideoMetadataUtilities.check_video_constraints(video_id, input_cfg, video_metadata)
            VideoMetadataUtilities.upload_dl_metadata_report(dynamo_hlp_instance, channel_id, video_id, video_metadata, cnsts_failure_msg, calc_metadata)
            if cnsts_passed == False:
                return 
            
            # Download video
            ydl.download([video_url,])

        # Upload video file
        file_path = f"./data/audio_files/{video_id}.flac" if run_locally else f"/tmp/audio_files/{video_id}.flac"
        s3_hlp_instance.upload_file(
            filename=file_path,
            bucket="ytdldata",
            key=f"{channel_id}/audio_files/{video_id}.flac"
        )
    
        # Upload transcript file
        s3_hlp_instance.upload_object(
            body=video_srt_content.encode('utf-8'),
            bucket="ytdldata",
            key=f"{channel_id}/srt_files/{video_id}.srt"
        )