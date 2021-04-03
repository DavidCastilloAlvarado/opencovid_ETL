from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_sinadef, DB_minsa_muertes, DB_positividad
from django.contrib.gis.geos import Point
# from django.utils import timezone
from django.db.models import Sum, Avg, Count, StdDev
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "RESUMEN: Command for create the resumen using the current date in the DB"
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)
    filename = 'poblacion.csv'

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_table(self, table, db, mode):
        if mode == 'full':
            records = table.to_dict(orient='records')
            records = [db(**record) for record in tqdm(records)]
            _ = db.objects.all().delete()
            _ = db.objects.bulk_create(records)
        elif mode == 'last':
            # this is posible because the table is sorter by "-fecha"
            last_record = db.objects.all()[:1]
            last_record = list(last_record)
            if len(last_record) > 0:
                last_date = str(last_record[0].fecha_corte.date())
            else:
                last_date = '2020-05-01'
            table = table.loc[table.fecha_corte > last_date]
            if len(table):
                self.print_shell("Storing new records")
                records = table.to_dict(orient='records')
                records = [db(**record) for record in tqdm(records)]
                _ = db.objects.bulk_create(records)
            else:
                self.print_shell("No new data was found to store")

    def handle(self, *args, **options):
        self.print_shell("Computing epidemiology score")
        # Downloading data from bucket
        self.downloading_source_csv()
        table = self.load_poblacion_file()
        self.print_shell('Work Done!')
        self.query_test_positivos(DB_positividad)

    def downloading_source_csv(self):
        """
        Function to download the csv file which contain all the url and standar names
        for the the data from the goberment, then 
        read that file and download all the files form source.
        """
        self.print_shell('Downloading poblacion.csv ... ')
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/"+self.filename,
                                  destination_file_name="temp/"+self.filename)

    def load_poblacion_file(self):
        table = pd.read_csv('temp/'+self.filename)
        table.rename(columns={
            'Region': 'region',
            'Poblacion': 'poblacion'
        }, inplace=True)
        table.region = table.region.apply(lambda x: normalizer_str(x))
        print(table)
        return table

    def query_test_positivos(self, db):
        query = db.objects.values('fecha')
        query = query.order_by('-fecha')[:7]
        query = query.values('region')
        query = query.annotate(Avg('total'), Avg('total_pos'))
        print(query)
