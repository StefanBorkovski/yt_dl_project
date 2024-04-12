from decimal import Decimal
from typing import Final

import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_wpm.__main__ import (calc_seconds_per_word, calc_speak_time,
                                  normalize_languages, normalize_youtube_id)


class TranscriptHelper:
    """
    A helper class for handling operations related to audio transcripts.

    This class encapsulates various functionalities for processing, manipulating, and managing audio transcripts.
    It provides methods for tasks such as loading, conversion and formatting of transcript data.
    """

    @staticmethod 
    def _convert_to_srt(transcripts: list) -> str:
        def convert_time(seconds: float) -> str:
            hours = int(seconds // 3600)
            seconds %= 3600
            minutes = int(seconds // 60)
            seconds = seconds % 60
            milliseconds = int((seconds - int(seconds)) * 1000)
            return f"{hours:02d}:{minutes:02d}:{int(seconds):02d},{milliseconds:03d}"
        
        def format_transcript(counter: int, transcript: dict) -> str:
            return f"{counter}\n" \
                   f"{convert_time(transcript['start'])} --> {convert_time(transcript['start'] + transcript['duration'])}\n" \
                   f"{transcript['text']}\n\n"

        return ''.join(format_transcript(i + 1, transcript) for i, transcript in enumerate(transcripts))

    @classmethod
    def _get_video_transcript(cls, video_id: str, cap_lng: str) -> tuple:
        try:
            transcripts = YouTubeTranscriptApi().list_transcripts(normalize_youtube_id(video_id))
        except Exception as exc:
            return None, '', exc.cause
        if len(transcripts._generated_transcripts) == 0:
            return None, '', 'Captions unavailable'
        if cap_lng in transcripts._generated_transcripts:
            transcript = transcripts.find_transcript(normalize_languages(cap_lng)).fetch()
            srt_content = cls._convert_to_srt(transcript)
            return transcript, srt_content, ''
        else:
            return None, '', 'Selected caption language not available'


class WordsPerMinuteHelper:
    """
    Helper class for calculating words per minute (WPM) from audio data.

    This class provides methods to analyze audio content and determine the average words per minute spoken in the audio.
    It serves as a tool for estimating the speech rate or transcription speed based on the audio input.
    """

    MAX_ITERATION: Final = 10
    EXIT_WPM_DIFF_THRESHOLD: Final[float] = 1
    INITIAL_APPROXIMATE_WPM: Final[float] = Decimal(180)

    @classmethod
    def get_video_wpm(cls, sequences: list) -> Decimal:
        initial_wpm: Final = Decimal(cls.INITIAL_APPROXIMATE_WPM)
        prev_wpm: Decimal = initial_wpm
        
        inference_spw = calc_seconds_per_word(prev_wpm)
        for i in range(cls.MAX_ITERATION):
            stats = calc_speak_time(sequences, inference_spw=inference_spw)
            diff_wpm = abs(stats.wpm - prev_wpm)
            print(f"iteration={i}, wpm={stats.wpm:.1f}, diff_wpm={diff_wpm:.1f}")
            if diff_wpm < cls.EXIT_WPM_DIFF_THRESHOLD:
                break
            inference_spw = calc_seconds_per_word(stats.wpm)
            prev_wpm = stats.wpm
        return stats.wpm
    

class VideoMetadataUtilities():
    """
    Utility class for validating audio metadata including words per minute (WPM), transcripts, and duration against predefined criteria
    and uploading download report in DynamoDB.

    This class encapsulates methods for verifying audio metadata attributes such as words per minute, transcript content, and duration
    to ensure they meet specific conditions or standards.
    """

    @staticmethod
    def _get_and_check_transcript(video_id: str, cap_lng: str) -> tuple:
        # Get video transcript
        sequences, srt_content, msg = TranscriptHelper._get_video_transcript(video_id, cap_lng)
        if sequences is None:
            return {'status': False, 'msg': msg}, sequences, srt_content
        return {'status': True, 'msg': msg}, sequences, srt_content
    
    @staticmethod
    def _check_wpm(sequences: list, min_wpm: int) -> tuple:
        if sequences is not None:
            # Get WPM
            video_wpm = int(WordsPerMinuteHelper.get_video_wpm(sequences))
            if video_wpm < min_wpm:
                return False, video_wpm
            return True, video_wpm
        else:
            return None, Decimal("0")
    
    @staticmethod
    def _check_duration(video_duration: float, input_cfg: dict) -> tuple:
        # Check video length
        video_duration = Decimal(str(video_duration)) # unit [s]
        if video_duration/60 > input_cfg['min_audio_duration_M'] and video_duration/60 > input_cfg['max_audio_duration_M']:
            return False, video_duration
        return True, video_duration
    
    @staticmethod
    def _get_failure_message(seq_cond: dict, wpm_cond: bool, dur_cond: bool) -> str:
        # Check conditions and create reasons message
        reasons: list = []

        # Check sequences
        if not seq_cond['status']:
            reasons.append(seq_cond['msg'])
        # Check WPM
        if wpm_cond == False:
            reasons.append('Low WPM')
        elif wpm_cond == True:
            pass
        else:
            reasons.append('Unknown WPM')
        # Check duration
        if not dur_cond:
            reasons.append('Too short')

        return ' - '.join(reasons) if len(reasons) > 0 else None

    @classmethod
    def check_video_constraints(cls, video_id: str, input_cfg: dict, video_metadata: dict) -> tuple:

        # Check transcripts
        seq_cond_response, sequences, video_srt_content = cls._get_and_check_transcript(video_id, input_cfg['captions_language'])

        # Check words per minute
        wpm_cond, video_wpm = cls._check_wpm(sequences, input_cfg['min_wpm'])

        # Check video duration
        dur_cond, video_duration = cls._check_duration(video_metadata['duration'], input_cfg)

        # Get failure message
        cnsts_failure_msg = cls._get_failure_message(seq_cond_response, wpm_cond, dur_cond)
        cnsts_passed = False if cnsts_failure_msg else True

        return cnsts_passed, cnsts_failure_msg, video_srt_content, {
            'video_wpm': video_wpm,
            'video_duration': video_duration
        }
        
    @staticmethod
    def upload_dl_metadata_report(dynamo_hlp_instance, channel_id: str, video_id: str, video_metadata: dict, response_msg: str, calc_metadata: dict) -> None:
        # Import download request status
        dynamo_hlp_instance.import_item(
            item = {
                "channel_id": channel_id,
                "video_id": video_id,
                **calc_metadata,
                "video_title": video_metadata['title'],
                "video_view_count": video_metadata['view_count'],
                "video_like_count": video_metadata['like_count'],
                "video_upload_date": pd.Timestamp(video_metadata['upload_date']).strftime('%Y-%m-%d'),
                "update_time": pd.Timestamp.now().strftime('%Y-%m-%d %H:%m:%S'),
                "channel_status": "Active",
                "download_status": response_msg is None,
                "reason": response_msg
            }
        )