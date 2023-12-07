from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def get_schemas():
    schema_historic_weather = [
        bigquery.SchemaField("Posted Date", "STRING"),
        bigquery.SchemaField("Payee", "STRING"), 
        bigquery.SchemaField("Address", "STRING"),
        bigquery.SchemaField("Amount", "STRING"),
        bigquery.SchemaField("transaction date_year-month", "STRING")       
    ]

    #"budget_meta.xlsx"
    schema_historic_forecast = [
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("classification1", "STRING"),
        bigquery.SchemaField("classification2", "STRING"),
        bigquery.SchemaField("classification3", "STRING"),
        bigquery.SchemaField("classification4", "STRING"),
        bigquery.SchemaField("comments", "STRING"),
        bigquery.SchemaField("filename", "STRING")
        ]

    #Schema dictionary for each known budget file
    schemas = {
        'schema_historic_weather': schema_historic_weather,
        'schema_historic_forecast': schema_historic_forecast,
    }

    #return schemas in dictionary
    return schemas