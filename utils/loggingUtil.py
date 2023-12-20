import pandas as pd
import logging

def log_dataframe_info_and_get_unique_values(df, logger):
    """
    Logs important information about a pandas DataFrame and returns a dictionary
    with unique values for each column.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        dict: A dictionary where each key is a column name and the value is a list
              of unique values from that column.
    """
    unique_values_dict = {}

    # Log the shape of the DataFrame
    logger.warning(f"DataFrame Shape: Rows = {df.shape[0]}, Columns = {df.shape[1]}")

    # Log data types of the DataFrame
    logger.warning("DataFrame Data Types:")
    logger.warning(df.dtypes)

    # Log unique values and nulls for each column, and store unique values in the dictionary
    logger.warning("DataFrame Column Information:")
    for column in df.columns:
        unique_values = df[column].unique()
        unique_values_dict[column] = unique_values
        null_values = df[column].isnull().sum()

        # Log the information
        logger.warning(f"{column}: Unique Values = {len(unique_values)}, Null Values = {null_values}")

        # Log each unique value in the column (optional, can be verbose)
        # for value in unique_values:
        #     logger.info(f"Unique value in {column}: {value}")

    return unique_values_dict