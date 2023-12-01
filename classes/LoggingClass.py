import logging
import inspect
import json
import os

from classes.ConfigManagerClass import ConfigManager

class LoggingManager():
    def __init__(self):
        self.config = ConfigManager(yaml_filepath='config', yaml_filename='config.yaml')
    def create_logger(
            self,
            logger_name='unset_logname', 
            debug_level='DEBUG', 
            mode='w',
            stream_logs = True,
            encoding='UTF-8'
            ):
        
        level_mapping = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'EXCEPTION': logging.ERROR,  # Exception is not a level in Python logging; it's usually logged as an ERROR
        }

        if debug_level.upper() not in level_mapping:
            raise ValueError(f"Invalid debug_level: {debug_level}. Must be one of: {', '.join(level_mapping.keys())}")

        logger = logging.getLogger(logger_name if logger_name else __name__)
        
        # Clear existing handlers
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        logger.setLevel(level_mapping[debug_level.upper()])

        formatter = logging.Formatter('%(asctime)s - %(module)s - %(levelname)s - Name: %(funcName)s - Line: %(lineno)d - %(message)s')

        filehandler_filepath = os.path.join(self.config.primary_logging_folder, f'{logger_name}.log' )
        file_handler = logging.FileHandler(filehandler_filepath, mode=mode, encoding=encoding)


        file_handler.setLevel(level_mapping[debug_level.upper()])
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if stream_logs == True:
            stream_handler = logging.StreamHandler()
            stream_handler.setLevel(level_mapping[debug_level.upper()])
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
        
        return logger

    def log_function_args(self, logger):
        """
        Decorator for logging the arguments of a function as a dictionary.

        Args:
            logger (logging.Logger): The logger object to use for logging the arguments.

        This decorator creates a single log entry containing both positional and
        keyword arguments passed to the function it decorates, formatted as a dictionary.
        It's useful for concise logging and easier tracking of function calls.

        Usage:
            @log_function_args(logger)
            def some_function(arg1, arg2, ...):
                ...

        Note:
            The logger must be configured beforehand to ensure proper logging output.
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                arg_names = inspect.getfullargspec(func).args
                args_dict = {name: value for name, value in zip(arg_names, args)}
                args_dict.update(kwargs)
                return func(*args, **kwargs)    
            return wrapper
        return decorator

    def _truncate_long_strings(self, obj):
        """
        Truncates string values in the object to a maximum length of 50 characters.
        This function is recursively applied to each value in the object.
        """
        if isinstance(obj, dict):
            return {k: self._truncate_long_strings(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._truncate_long_strings(v) for v in obj]
        elif isinstance(obj, str):
            return obj[:50] + '...' if len(obj) > 50 else obj
        else:
            return obj

    def log_as_json(self, logger, obj, indent=2):
        """
        Log an object as a JSON-formatted string, with individual string values truncated.

        Args:
            logger (logging.Logger): The logger object to use for logging the formatted string.
            obj (object): The object to be logged.
            indent (int, optional): The indentation level for the JSON string. Defaults to 2.
        """
        # Serialize the object to a JSON string with custom serialization for long strings
        json_string = json.dumps(obj, indent=indent, default=self._truncate_long_strings)
    
def main():
    logging_manager = LoggingManager() 
    logger = logging_manager.create_logger(
        logger_name='log_sometestlog',
        debug_level='DEBUG', 
        stream_logs=True
        )
    logger.info("This is a test log...")
    return logger

if __name__ == '__main__':
    logging_manager = LoggingManager()
    logger = main()
    logger.info("this is a log in __main__")