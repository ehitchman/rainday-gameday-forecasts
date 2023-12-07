from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def get_bq_schemas():
    schema_historic_weather = [
        bigquery.SchemaField("forecast_datetime", "DATETIME"),
        bigquery.SchemaField("temp", "FLOAT"), 
        bigquery.SchemaField("temp_humidity", "FLOAT"),
        bigquery.SchemaField("name", "STRING")       
    ]

    #"budget_meta.xlsx"
    schema_historic_forecast = [
        bigquery.SchemaField("capture_date", "DATE"),
        bigquery.SchemaField("forecast_dateunix", "INTEGER"),
        bigquery.SchemaField("forecast_datetime", "STRING"), #should prob be datetime
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("rain_category", "STRING"),
        bigquery.SchemaField("rain_category_value", "INTEGER"),
        bigquery.SchemaField("temp", "FLOAT"),
        bigquery.SchemaField("temp_min", "FLOAT"),
        bigquery.SchemaField("temp_max", "FLOAT"),
        bigquery.SchemaField("temp_humidity", "INTEGER"),
        bigquery.SchemaField("weather_description", "STRING")
    ]

    #Schema dictionary for each known budget file
    bq_schemas = {
        'schema_historic_weather': schema_historic_weather,
        'schema_historic_forecast': schema_historic_forecast,
    }

    #return schemas in dictionary
    return bq_schemas

if __name__ == "__main__":
    bq_schemas = get_bq_schemas()
    print(bq_schemas['schema_historic_weather'])
    