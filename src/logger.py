import logging
from src.config import log_path,formatter_format

from logging import StreamHandler,FileHandler,Formatter

def create_logger(module, file_needed=False):
    #Create a new logger for the module being run
    new_logger = logging.getLogger(f'{module}')
    #Adding Stream Handlers to logger
    if not new_logger.handlers:
        new_logger.setLevel(logging.INFO)
        new_handler = StreamHandler()
        new_formatter = Formatter(formatter_format)
        new_handler.setFormatter(new_formatter)
        new_logger.addHandler(new_handler)
    #In case if seperate log file is needed
    if file_needed:
        if not any(isinstance(h, FileHandler) for h in new_logger.handlers):
            new_file_handler = FileHandler(f'{log_path}{module}.log')
            file_formatter = Formatter(formatter_format)
            new_file_handler.setFormatter(file_formatter)
            new_logger.addHandler(new_file_handler)
    return new_logger
