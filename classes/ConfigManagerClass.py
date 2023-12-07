import os
import yaml
import dotenv

from schemas.bq_schemas import get_bq_schemas

runtime_logger_level = 'DEBUG'

class ConfigManager:
    _instance = None

    def __new__(cls, yaml_filepath=None, yaml_filename=None):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance.initialize_config(yaml_filepath, yaml_filename)
            cls._instance.init_attributes()
            print("Completed initialization of ConfigManager()")
        return cls._instance

    def init_attributes(self):
        print("No initialization attributes set in ConfigManager.  Beginning setting of env and yaml variables")

    def initialize_config(self, yaml_filepath, yaml_filename):
        yaml_full_path = os.path.join(yaml_filepath, yaml_filename)
        self.load_yaml_config(yaml_full_path)
        self.set_env_variables()
        self.set_other_variables()

    def load_yaml_config(self, yaml_full_path):
        with open(yaml_full_path, 'r') as file:
            yaml_config = yaml.safe_load(file)
            self.update_config_from_yaml(yaml_config)

    def set_env_variables(self):
        if self.env_filedir and self.env_filename:
            env_path = os.path.join(self.env_filedir, self.env_filename)
            if os.path.exists(env_path):
                dotenv.load_dotenv(env_path)
                self.update_config_from_env()

    def set_other_variables(self):
        self.update_config_from_other()

    def update_config_from_other(self):
        self.bq_schemas = get_bq_schemas()
        self.bq_schemas_historic_weather = self.bq_schemas['schema_historic_weather']
        self.bq_schemas_historic_forecast = self.bq_schemas['schema_historic_forecast']

    def update_config_from_env(self):
        # # Load and set runtime parameters from environment variables set in .bat
        self.openweathermap_api_key = os.getenv("OPENWEATHERMAP_API_KEY")

    def update_config_from_yaml(self, yaml_config):
        self.env_filename = yaml_config.get('env_filename', 'env.py')
        self.env_filedir = yaml_config.get('env_filedir', '')
        self.is_testing_run = yaml_config.get('is_testing_run', False)

        if os.getenv('RAINDAY_IN_CLOUD_ENVIRONMENT')=='yes':
            self.primary_logging_folder = yaml_config.get('cloud_primary_logging_folder', '/tmp')
            self.log_responses_directory = yaml_config.get('cloud_log_responses_directory', '/tmp/responses')
        else:
            self.primary_logging_folder = yaml_config.get('primary_logging_folder', '/log')
            self.log_responses_directory = yaml_config.get('log_responses_directory', '/log/responses')

        self.gcp_project_name = yaml_config.get('gcp_project_name')

        self.bq_dataset_name = yaml_config.get('bq_dataset_name')
        self.bq_historic_table_name=yaml_config.get('bq_historic_table_name')
        self.bq_forecast_table_name=yaml_config.get('bq_forecast_table_name')

        self.pubsub_project_id = yaml_config.get('pubsub_project_id') 
        self.bucket_name = yaml_config.get('bucket_name')
        self.gcs_credential_filepath = yaml_config.get('gcs_credential_filepath')

        self.response_file_name = yaml_config.get('response_file_name', 'response')
        self.wthr_forecast_csvpath = yaml_config.get('wthr_forecast_csvpath', '')
        self.wthr_forecast_individual_csvpath = yaml_config.get('wthr_forecast_individual_csvpath', 'weather_forecast_csv/most_recent_individual_forecasts/5-day forecast')
        self.wunderground_weatherhistory_filepath = yaml_config.get('wunderground_weatherhistory_filepath')
        self.wthr_historic_unioned_csvpath = yaml_config.get('wthr_historic_unioned_csvpath')
        self.wthr_historic_csvpath = yaml_config.get('wthr_historic_csvpath')
        
        self.wthr_historic_unioned_folderpath = yaml_config.get('wthr_historic_unioned_folderpath')
        self.wthr_historic_unioned_filename = yaml_config.get('wthr_historic_unioned_filename')


        #Users details: dict data type
        self.users_details = yaml_config.get('users_details', [])

        # GCS
        self.forecast_unioned_csvpath = yaml_config.get('forecast_unioned_csvpath')

def main():
    config_manager = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')
    return config_manager

if __name__ == "__main__":
    print("Use config_manager.primary_logging_folder to test config managers presenece")
    config_manager = main()
    config_manager.wthr_historic_unioned_filename