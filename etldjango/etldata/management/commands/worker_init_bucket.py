import os
from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from datetime import datetime
from etldata.models import Logs_extractor
from tqdm import tqdm
import time
import glob


class Command(BaseCommand):
    help = 'Load csv support files to gcp bucket'
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def handle(self, *args, **options):
        self.create_bucket()
        self.uploading_files_to_bucket()
        self.print_shell('Work Done!')

    def create_bucket(self):
        self.print_shell('Creating bucket if doesn\'t exist ... ')
        self.bucket.create_bucket(BUCKET_NAME)

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def uploading_files_to_bucket(self):
        dir_files = glob.glob('data_source/**.csv')
        for file_name in tqdm(dir_files, total=len(dir_files)):
            destination = file_name
            self.bucket.upload_blob(bucket_name=BUCKET_NAME,
                                    source_file_name=file_name,
                                    destination_blob_name=destination)
        time.sleep(2)
        #os.system("rm temp/*")
