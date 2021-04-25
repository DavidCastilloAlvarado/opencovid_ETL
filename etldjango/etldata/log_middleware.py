from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS
from .utiles import get_client_and_log_resource
import io
import logging
import json


client, _LOG_RESOURCE = get_client_and_log_resource(
    GOOGLE_APPLICATION_CREDENTIALS)


class StackDriverHandler(logging.Handler):

    def __init__(self):
        logging.Handler.__init__(self)

    def emit(self, record):
        """Add record to cloud"""
        self.logger = client.logger('stackdriver.googleapis.com%2Fapp')
        self.log_msg = self.format(record)
        self.logger.log_text(
            self.log_msg, severity=record.levelname, resource=_LOG_RESOURCE)
