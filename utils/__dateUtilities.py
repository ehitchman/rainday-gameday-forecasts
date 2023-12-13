# Function to convert datetime string to the correct format 'YYYY-MM-DD HH:MM:SS' from '%Y-%m-%d %H:%M' 
def convert_datetime_format(dt_str):
    from datetime import datetime
    try:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return dt_str