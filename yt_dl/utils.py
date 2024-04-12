import yaml


def get_video_length(time_str):
    return sum(int(x) * 60 ** i for i, x in enumerate(reversed(time_str.split(':'))))

def load_yaml_config(cfg_path: str) -> dict:
    return yaml.safe_load(open(cfg_path))

def load_yt_dl_config(run_locally: bool):
    cfg_name = 'yt_dl_local_config' if run_locally is True else 'yt_dl_remote_config'
    return load_yaml_config(f"yt_dl/configs/{cfg_name}.yaml")