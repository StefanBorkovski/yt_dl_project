import time
import os

import ray
from yt_dl import (Downloader, DynamoDBHelper, ReportGenerator, S3Helper,
                   TaskGenerator)


@ray.remote
def distributed_downloader(video_id: str, input_cfg: dict):
    Downloader.run(video_id, input_cfg)


if __name__=='__main__':

    RUN_LOCALLY: bool = False
    USE_RAY: bool = True
    
    # Initialize dynamo and s3 helper instances
    dynamo_hlp_instance = DynamoDBHelper(table_name = 'VideoChannelInfoTable')
    s3_hlp_instance = S3Helper()

    # Load input configuration file
    input_file = s3_hlp_instance.load_object(            
            bucket="ytdlinput", 
            key="input.json"
        )
    
    # Generate tasks
    video_ids = TaskGenerator.extract_channel_video_urls(input_file.get('channels'), dynamo_hlp_instance, shuffle=True)
    print(f'>>> {len(video_ids)} videos will be downloaded!')

    # Load input configuration
    input_cfg = input_file.get('configuration')

    st = time.time()
    run_setup = (USE_RAY, RUN_LOCALLY)
    
    # Run locally using ray
    if run_setup == (True, True):
        ray.init()
        os.makedirs('./data/audio_files') 
        [Downloader.run(video_id, input_cfg=input_cfg, run_locally=RUN_LOCALLY) for video_id in video_ids]
    # Run sequentially
    elif run_setup == (False, True):           
        for video_id in video_ids:
            Downloader.run(video_id, input_cfg=input_cfg, run_locally=RUN_LOCALLY)
    # Run on AWS
    elif run_setup == (True, False):        
        ray.init(address='auto') 
        ray.get([distributed_downloader.remote(video_id, input_cfg=input_cfg) for video_id in video_ids])
    else:
        raise NotImplementedError(f'Configuration (USE_RAY, RUN_LOCALLY): {run_setup}, not implemented!')
    print(f'>>> Time required: {time.time()-st}')

    # Generate reports from the current state of responses table 
    report_gen = ReportGenerator(dynamo_hlp_instance, s3_hlp_instance)
    report_gen.generate_reports()
