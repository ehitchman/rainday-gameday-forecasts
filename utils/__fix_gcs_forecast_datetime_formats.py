import pandas as pd
import io

from classes.ConfigManagerClass import ConfigManager
from classes.LoggingClass import LoggingManager
from classes.GCS import GCSManager

from utils.__dateUtilities import convert_datetime_format

config = ConfigManager(yaml_filename = 'config.yaml', yaml_filepath = 'config')
gcs_manager = GCSManager()

bucket_name = config.bucket_name
blobs_list = gcs_manager.list_gcs_blobs(bucket_name=bucket_name)

csvs_to_union_folder_location = 'weather_forecast_csv/5-day forecast/'

for blob in blobs_list:
    if blob.name.startswith(csvs_to_union_folder_location) and blob.name.endswith('.csv') and not blob.name.endswith('/'):
        csv_content = io.StringIO() 
        csv_content.write(blob.download_as_text())
        csv_content.seek(0)
        df = pd.read_csv(csv_content)
        # print('-----')
        # print(f"df dtype: {df.dtypes}")
        # print(df.head(5))


        print('-----')
        forecast_datetimes_corrected = []
        for index, row in df.iterrows():
            forecast_datetime_corrected = convert_datetime_format(row['forecast_datetime'])
            forecast_datetimes_corrected.append(forecast_datetime_corrected)
        df['forecast_datetime_corrected'] = forecast_datetimes_corrected
        df['forecast_datetime_corrected'] = pd.to_datetime(df['forecast_datetime_corrected'])

        print('-----')
        print(f"blobname: {blob.name}")
        print(f"df dtype: {df.dtypes}")
        print(df.head(5))

        # #Write back to GCS
        # gcs_manager.write_df_to_gcs(
        #     df=df,
        #     bucket_name=config.bucket_name,
        #     gcs_bucket_filepath=blob.name,
        #     is_testing_run=False
        # )
    else:
        print("didn't start with the filepath")