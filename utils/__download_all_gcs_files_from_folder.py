import pandas as pd
from google.cloud import storage
from classes.ConfigManagerClass import ConfigManager
from classes.GCS import GCSManager

config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')
gcs_client = storage.Client()
gcs_manager = GCSManager()

gcs_manager.download_all_files_in_gcs_folder(
    bucket_name = config.bucket_name, 
    folder_path = config.wthr_forecast_folderpath, 
    destination_folder = config.primary_gcs_download_folder
)