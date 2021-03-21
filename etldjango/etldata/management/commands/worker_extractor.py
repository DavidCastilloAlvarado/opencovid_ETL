import os
from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from datetime import datetime
from tqdm import tqdm
import time


class Command(BaseCommand):
    help = 'This command download in local and then upload the table to our bucket in GCP'
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def handle(self, *args, **options):
        os.system("mkdir temp")
        self.print_shell('Creating bucket if doesn\' exist ... ')
        self.bucket.create_bucket(BUCKET_NAME)
        self.print_shell('Downloading url and filenames ... ')
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/datos_fuentes.csv",
                                  destination_file_name="temp/datos_fuentes.csv")
        self.handler = Data_Extractor(csv_urls="temp/datos_fuentes.csv")

        #bool_ = options["today"]
        self.print_shell('url and names downloaded')
        self.print_shell('Downloading files from goberment\'s servers  ... ')
        self.extracting_data()
        self.print_shell('Uploading files to Bucket ... ')
        self.uploading_bucket()
        self.print_shell('Work done! ... ')

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def extracting_data(self,):
        status = self.handler.extract_queue()
        self.print_shell('Downloaded: {}'.format(status))

    def uploading_bucket(self):
        TODAY = str(datetime.now().date())
        for file_name in tqdm(self.handler.list_name, total=len(self.handler.list_name)):
            destination = BUCKET_ROOT + "/" + TODAY + "/" + file_name
            self.bucket.upload_blob(bucket_name=BUCKET_NAME,
                                    source_file_name=file_name,
                                    destination_blob_name=destination)
        time.sleep(2)
        os.system("rm temp/*")
