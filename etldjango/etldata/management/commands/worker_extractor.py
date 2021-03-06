import os
from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from datetime import datetime
from etldata.models import Logs_extractor
from tqdm import tqdm
import time


class Command(BaseCommand):
    help = 'This command download in local and then upload the raw data to our bucket in GCP'
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def add_arguments(self, parser):
        parser.add_argument(
            'version', type=str, help="v1, v2, ...; full, load: the whole dataset, last: only the latest dates")

    def handle(self, *args, **options):
        version = options["version"]
        assert 'v' in version, "Error in --version argument"
        os.system("mkdir temp")  # Creating temp folder
        self.create_bucket()
        self.downloading_source_csv(version)
        self.print_shell('Downloading files from gobernment\'s servers  ... ')
        self.extracting_data_from_gob_origin()
        self.print_shell('Uploading files to Bucket ... ')
        self.uploading_bucket()
        self.print_shell('Work Done!')

    def create_bucket(self):
        self.print_shell('Creating bucket if doesn\' exist ... ')
        self.bucket.create_bucket(BUCKET_NAME)

    def downloading_source_csv(self, version):
        """
        Function to download the csv file which contain all the url and standar names
        for the the data from the goberment, then 
        read that file and download all the files form source.
        """
        self.print_shell('Downloading url and filenames ... ')
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name='data_source/datos_fuentes_'+version+'.csv',
                                  destination_file_name="temp/datos_fuentes.csv")
        self.handler = Data_Extractor(csv_urls="temp/datos_fuentes.csv")
        self.print_shell('url and names downloaded')

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def extracting_data_from_gob_origin(self,):
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
