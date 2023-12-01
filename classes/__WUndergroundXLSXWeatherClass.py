import pandas as pd
import re

from classes.ConfigManagerClass import ConfigManager
from classes.GCS import GCSManager

class WeatherHistoryProcessor:
    def __init__(self, file_path):
        self.wunderground_file_path = file_path
        self.config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')
        self.gcs_manager = GCSManager()

    @staticmethod
    def _extract_numerical_value(s):
        return float(re.findall(r"[-+]?\d*\.\d+|\d+", s)[0])

    def _process_tab(self, df, capture_date):
        df['Temperature'] = df['Temperature'].apply(self._extract_numerical_value)
        df['Dew Point'] = df['Dew Point'].apply(self._extract_numerical_value)
        df['Humidity'] = df['Humidity'].apply(self._extract_numerical_value)
        df['Wind Speed'] = df['Wind Speed'].apply(self._extract_numerical_value)
        df['Wind Gust'] = df['Wind Gust'].apply(self._extract_numerical_value)
        df['Pressure'] = df['Pressure'].apply(self._extract_numerical_value)
        df['Precip.'] = df['Precip.'].apply(self._extract_numerical_value)

        df['DateTime'] = pd.to_datetime(capture_date + ' ' + df['Time'].astype(str))
        df['Epoch'] = df['DateTime'].apply(lambda dt: int(dt.timestamp()))

        df.rename(columns={
            'Temperature': 'Temperature (°F)',
            'Dew Point': 'Dew Point (°F)',
            'Humidity': 'Humidity (%)',
            'Wind Speed': 'Wind Speed (mph)',
            'Wind Gust': 'Wind Gust (mph)',
            'Pressure': 'Pressure (in)',
            'Precip.': 'Precipitation (in)'
        }, inplace=True)

        # Function to round down to the nearest three hours
        def round_down_to_three_hours(dt):
            rounded_hour = (dt.hour // 3) * 3
            return dt.replace(hour=rounded_hour, minute=0, second=0, microsecond=0)

        # Adjusting the DateTime column
        df['DateTime'] = pd.to_datetime(capture_date + ' ' + df['Time'].astype(str))
        df['DateTime'] = df['DateTime'].apply(round_down_to_three_hours)

        return df

    def process_all_tabs(self):
        # Read all sheets from the Excel file
        all_sheets = pd.read_excel(self.wunderground_file_path, sheet_name=None)

        processed_data = {}
        for sheet_name, df in all_sheets.items():
            capture_date = sheet_name  # Assuming sheet name is the capture date
            processed_data[sheet_name] = self._process_tab(df, capture_date)

        return processed_data
    
    def union_all_tabs(self, processed_data):
        dataframes = processed_data.values()
        df_union = pd.concat(dataframes, ignore_index=True)

        # Group by 'DateTime', and keep the row with the minimum 'Epoch' for each group
        df_union = df_union.sort_values('Epoch').groupby('DateTime').first().reset_index()

        return df_union
    
    def process_all_tabs_bq(self, df_union):
        print("Do something...")
        gcs_filepath = self.config.wthr_historic_unioned_csvpath+'.csv'
        self.gcs_manager.write_df_to_gcs(
            df=df_union, 
            bucket_name = self.config.bucket_name, 
            gcs_bucket_filepath = gcs_filepath
            )
        message = "Looks like the write_df_to_gcs() has completed successfully"
        return message
    
if __name__ == "__main__":
    from classes.ConfigManagerClass import ConfigManager

    config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')

    wthr_processor = WeatherHistoryProcessor(file_path=config.wunderground_weatherhistory_filepath)
    processed_data = wthr_processor.process_all_tabs()
    df_union = wthr_processor.union_all_tabs(processed_data)
    bq_result = wthr_processor.process_all_tabs_bq(df_union)
    print(bq_result)