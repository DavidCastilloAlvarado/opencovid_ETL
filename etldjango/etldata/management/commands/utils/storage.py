from google.cloud import storage
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
import os
from os import path as pathdir
from pathlib import Path
from etldata.models import Logs_extractor
from .extractor import Data_Extractor


class Bucket_handler(object):
    def __init__(self, project_id):
        self.storage_client = storage.Client(project_id)
        self.origin = "https://storage.googleapis.com/"
        # For slow upload speed
        storage.blob._DEFAULT_CHUNKSIZE = 2097152  # 1024 * 1024 B * 2 = 2 MB
        storage.blob._MAX_MULTIPART_SIZE = 2097152  # 2 MB

    def create_bucket(self, dataset_name):
        """Creates a new bucket. https://cloud.google.com/storage/docs/ """
        try:
            bucket = self.storage_client.create_bucket(dataset_name)
            print('Bucket {} created'.format(bucket.name))
        except:
            print('Bucket {} creation FAIL ... check if already exist'.format(
                dataset_name))

    def upload_blob(self, bucket_name, source_file_name, destination_blob_name):
        """Uploads files to bucket. https://cloud.google.com/storage/docs/ """
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)
            print('File {} uploaded to {}.'.format(source_file_name,
                                                   destination_blob_name))
            destination, status = destination_blob_name, 'ok'
            self.log_register({'e_name': destination.split('/')[-1],
                               'url': destination,
                               'status': status,
                               'mode': 'upload'})
        except:
            status = 'fail'
            print('FAIL: uploading {} to {}.'.format(source_file_name,
                                                     destination_blob_name))
            self.log_register({'e_name': destination.split('/')[-1],
                               'url': destination,
                               'status': status,
                               'mode': 'upload'})

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        # bucket_name = "your-bucket-name"
        # source_blob_name = "storage-object-name"
        # destination_file_name = "local/path/to/file"
        bucket = self.storage_client.bucket(bucket_name)

        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        try:
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_name)
            print("Blob {} downloaded to {}.".format(source_blob_name,
                                                     destination_file_name))
            destination, status = destination_file_name, 'ok'
            self.log_register({'e_name': destination.split('/')[-1],
                               'url': destination,
                               'status': status,
                               'mode': 'download'})
        except:
            print("FAIL: downloading {} to {}.".format(source_blob_name,
                                                       destination_file_name))
            destination, status = destination_file_name, 'fail'
            self.log_register({'e_name': destination.split('/')[-1],
                               'url': destination,
                               'status': status,
                               'mode': 'download'})

    def log_register(self, data):
        Logs_extractor.objects.create(**data)


class GetBucketData(Bucket_handler):
    def get_from_bucket(self, source_name, destination_name, ipress=False):
        # Downloading Links file and names
        # self.download_blob(bucket_name=BUCKET_NAME,
        #                    source_blob_name="data_source/datos_fuentes.csv",
        #                    destination_file_name="temp/datos_fuentes.csv")
        # Downloading GEO data for every Ipress
        if ipress:
            self.download_blob(bucket_name=BUCKET_NAME,
                               source_blob_name="data_source/geo_ipress.csv",
                               destination_file_name="temp/geo_ipress.csv")
        # Loading links and names to donwload from bucket
        #self.handler = Data_Extractor(csv_urls="temp/datos_fuentes.csv")

        # Downloading the rest of the files from bucket
        self.download_blob(bucket_name=BUCKET_NAME,
                           source_blob_name=source_name,
                           destination_file_name=destination_name)
