import logging


def configure_logging():
    # Configure the logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create a logger
    logger = logging.getLogger(__name__)

    # Create a file handler
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.INFO)  # Only log messages with level ERROR or higher
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    return logger
