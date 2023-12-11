# pragma: no cover
import logging
from logging.handlers import RotatingFileHandler

def configure_logger(): # pragma: no cover
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler("client_data_processing.log", maxBytes=100000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)