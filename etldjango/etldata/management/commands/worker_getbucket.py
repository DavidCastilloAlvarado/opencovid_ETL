import os
from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from etldata.models import DB_sinadef
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "Command for download all files from the bucket"
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def handle(self, *args, **options):
        os.system("mkdir temp")
        self.print_shell("Downloading from bucket ... ")
        TODAY = str(datetime.now().date())
        try:
            self.get_from_bucket(TODAY)
        except:
            self.print_shell("Fail firt chance")
            self.print_shell("Second chance downloading ...")
            TODAY = str(datetime.now().date() -
                        timedelta(days=1))  # a day before
            self.get_from_bucket(TODAY)
        self.print_shell("Work done! ")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def get_from_bucket(self, today):
        # Downloading Links file and names
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/datos_fuentes.csv",
                                  destination_file_name="temp/datos_fuentes.csv")
        # Downloading GEO data for every Ipress
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/geo_ipress.csv",
                                  destination_file_name="temp/geo_ipress.csv")
        # Loading links and names to donwload from bucket
        self.handler = Data_Extractor(csv_urls="temp/datos_fuentes.csv")

        # Downloading the rest of the files from bucket
        for file_name in self.handler.list_name:
            destination = BUCKET_ROOT + "/" + today + "/" + file_name
            self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                      source_blob_name=destination,
                                      destination_file_name=file_name)
