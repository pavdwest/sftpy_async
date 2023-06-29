import yaml


config_file = 'config.staging.pwn.yaml'

with open(config_file, 'r') as file:
    config = yaml.safe_load(file)

batch_size = 10
max_batch_retries = 5
local_downloads_root_folder = config['local_downloads_root_folder']
remote_folder_to_download = config['remote_folder_to_download']
store_files_by_session = config['store_files_by_session']
sftp_host=config['sftp_credentials']['host']
sftp_username=config['sftp_credentials']['username']
sftp_password=config['sftp_credentials']['password']
