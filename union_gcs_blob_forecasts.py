
#%%

import functions_framework

@functions_framework.http
def union_and_write_to_gcs(request=None, is_testing_run=False):

    import pandas as pd
    from modules import load_yaml, list_gcs_blobs, union_gcs_csv_blobs, write_df_to_gcs
    import os
    yaml_data = load_yaml()

    bucket_name='rainday-gameday-bucket'
    gcs_file_name = 'all_historic_forecasts.csv'
    gcs_forecasthistory_bucket_directory = yaml_data['5dayforecast_historic_forecast_csvdir']
    gcs_forecasthistory_filepath = os.path.join(gcs_forecasthistory_bucket_directory, gcs_file_name).replace('\\','/')
    csvs_to_union_folder_location = yaml_data['5dayforecast_csvdir']

    #Get all csvs from bucket and union them together
    blobs_list = list_gcs_blobs(bucket_name=bucket_name)
    unioned_forecasts = union_gcs_csv_blobs(blobs_list=blobs_list,
                                            csvs_to_union_folder_location = csvs_to_union_folder_location)

    #write to GCS (scheduled data transscheduled to move the GCS blob to BQ)
    write_df_to_gcs(df=unioned_forecasts, 
                    bucket_name=bucket_name, 
                    gcs_bucket_filepath=gcs_forecasthistory_filepath, 
                    is_testing_run=False)

    return(f"The combined/unioned forecasts has been saved to GCS bucket: {bucket_name} in location: {gcs_bucket_file_path}")

if __name__ == '__main__':
    union_and_write_to_gcs()
#%%
