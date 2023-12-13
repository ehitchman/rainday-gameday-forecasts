# Function to convert datetime string to the correct format 'YYYY-MM-DD HH:MM:SS' from '%Y-%m-%d %H:%M' 
def convert_datetime_format(dt_str):
    from datetime import datetime
    try:
        # Parse the datetime string and format it to include the seconds
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        # In case of a formatting error, return the original string
        return dt_str