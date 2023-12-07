#bigquery_io.py
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
from classes.LoggingClass import LoggingManager

runtime_logger_level = 'DEBUG'

class BigQueryManager():
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.logging_manager = LoggingManager() 

        self.logger = self.logging_manager.create_logger(
            logger_name='logger_bigquery',
            debug_level=runtime_logger_level,
            stream_logs=True,
            mode='a'
            )

    def create_or_replace_bq_table_from_gcs(
            self,
            project_name, 
            source_bucket_name,
            source_dir_path,
            source_file_name,
            target_dataset_name, 
            target_table_name,
            schema
            ):
        
        self.logger.debug('---------------------------------')
        self.logger.debug(f"project_name: {project_name}")
        self.logger.debug(f"source_bucket_name: {source_bucket_name}")
        self.logger.debug(f"source_dir_path: {source_dir_path}")
        self.logger.debug(f"source_file_name: {source_file_name}")
        self.logger.debug(f"target_dataset_name: {target_dataset_name}")
        self.logger.debug(f"target_table_name: {target_table_name}")
        self.logger.debug(f"schema: {schema}")
        self.logger.debug(f"")

        try:
            gcs_uri = f"gs://{source_bucket_name}/{source_dir_path}/{source_file_name}"
            table_fullqual = f"{project_name}.{target_dataset_name}.{target_table_name}"
            table_ref = bigquery.TableReference.from_string(table_fullqual)

            self.logger.info(f"Source URI from GCS is: {gcs_uri}")
            self.logger.info(f"Target BQ table: {table_fullqual}")

            try: # Check if the table already exists in BQ
                self.bq_client.get_table(table_ref)
                self.logger.info(f"{table_fullqual} already exists so a new one was not created. continuing with job load for {gcs_uri}.")
            except NotFound:
                self.logger.warning(f"{table_fullqual} was not found. Creating new table.")
                table = bigquery.Table(table_ref, schema=schema)
                self.bq_client.create_table(table)
                self.logger.info(f"{table_fullqual} created, continuing with job load.")

            # Configure the external data source and start the BigQuery Load job
            job_config = bigquery.LoadJobConfig(
                autodetect=False,
                schema=schema,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE   
            )
            load_job = self.bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job_result = load_job.result()

            self.logger.info(f"This is the load_job_result: {load_job_result}")
            return load_job_result

        except GoogleCloudError as e:
            self.logger.error(f"Google Cloud Error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            return None