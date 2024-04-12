# yt_dl_project

## SET UP BOTO3

- Set up AWS credentials in **AWSCredentials** class in **aws_helpers.py** script.
- Create and place the key file in the **./aws** directory (where Boto3 checks, check documentation).

## CONFIGURE INPUT FILE
- Upload **input.json** from **./data** to the S3 bucket named **'ytdlinput'**.


## FOR RUNNING LOCALLY

- Install requirements from **requirements.txt**
- Create and add **ffmpeg** in the **./dep** directory (**./dep/ffmpeg/bin/ffmpeg.exe**).
- Upload **input.json** from **./data** to the S3 bucket named **'ytdlinput'**.
- Set **RUN_LOCALLY=True** and **USE_RAY** to **True** or **False** if you want to run it using Ray or sequentially.

## FOR RUNNING ON AWS CLUSTER

- For Windows, in WSL CLI run:
  - Create the cluster: 
    ```
    ray up -y /mnt/c/Users/Stefan/Desktop/Personal/Projects/Soniox/yt_dl_project/ray/yt_dl_cluster.yaml
    ```
  - Forward ports locally: 
    ```
    ray dashboard /mnt/c/Users/Stefan/Desktop/Personal/Projects/Soniox/yt_dl_project/ray/yt_dl_cluster.yaml
    ```
  - Submit task: 
    ```
    RAY_ADDRESS='http://localhost:8265' ray job submit --runtime-env-json='{"working_dir": "./", "pip": ["boto3", "boto3-stubs[dynamodb]", "scrapetube", "youtube-transcript-api", "youtube-wpm"]}' -- python ./pipeline.py
    ```