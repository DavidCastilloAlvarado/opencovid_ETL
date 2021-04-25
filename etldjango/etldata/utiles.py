from google.cloud.logging import Resource
from google.cloud import logging as gcp_logging
import io
import json


def get_client_and_log_resource(path_credentials):
    client = gcp_logging.Client.from_service_account_json(
        path_credentials)
    client.setup_logging()

    client_email = ""
    with io.open(path_credentials, "r", encoding="utf-8") as file:
        credentials_info = json.load(file)
        client_email = credentials_info["client_email"]

    _LOG_RESOURCE = Resource(
        type='service_account',
        labels={
            "email_id":  client_email,
            "project_id":  client.project,
        }
    )
    return client, _LOG_RESOURCE
