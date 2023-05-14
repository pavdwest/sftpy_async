import os
import sys
import logging

def get_logger(filename: str = 'log.txt') -> logging.Logger:
    if not os.path.exists('logs'):
        os.makedirs('logs')

    formatter = logging.Formatter(
        '%(asctime)s | %(name)s |  %(levelname)s: %(message)s'
    )

    # stdout
    log_stream_handler = logging.StreamHandler()
    log_stream_handler.setFormatter(formatter)
    log_stream_handler.setLevel(logging.INFO)

    # file
    log_file_handler = logging.FileHandler(
        filename=f"logs/{filename}.txt", 
    )
    log_file_handler.setFormatter(formatter)
    log_file_handler.setLevel(logging.DEBUG)

    logging.basicConfig(
        handlers=[
            log_file_handler,
            log_stream_handler,
        ],
        encoding='utf-8', 
        level=logging.INFO
    )

    return logging.getLogger()
