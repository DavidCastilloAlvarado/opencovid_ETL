from google.cloud.logging import Resource
from google.cloud import logging as gcp_logging
import io
import json
import os


def get_client_and_log_resource():
    client = gcp_logging.Client()  # .from_service_account_json(path_credentials)
    client.setup_logging()

    _LOG_RESOURCE = Resource(
        type='service_account',
        labels={
            "project_id":  client.project,
        }
    )
    return client, _LOG_RESOURCE
