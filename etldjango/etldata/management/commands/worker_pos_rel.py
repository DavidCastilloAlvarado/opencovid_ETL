from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from datetime import datetime, timedelta
from etldata.models import DB_positividad_relativa, Logs_extractor
from .utils.unicodenorm import normalizer_str
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
from urllib.request import urlopen
import os
import time
import tabula
import re


class Command(BaseCommand):
    help = "Command for store the positive cases  PR, PCR, AG from minsa table"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    file_name = "covidpos.csv"

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def downloading_data_from_bucket(self,):
        last_record = Logs_extractor.objects.filter(status='ok',
                                                    mode='upload',
                                                    e_name=self.file_name)[:1]
        last_record = list(last_record)
        assert len(last_record) > 0, "There are not any file {} in the bucket".format(
            self.file_name)
        last_record = last_record[0]
        source_url = last_record.url
        print(source_url)
        self.bucket.get_from_bucket(source_name=source_url,
                                    destination_name='temp/'+self.file_name)

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
                last_date = str(last_record[0].fecha.date())
            else:
                last_date = '2020-01-01'
            table = table.loc[table.fecha > last_date]
            if len(table):
                self.print_shell("Storing new records")
                records = table.to_dict(orient='records')
                records = [db(**record) for record in tqdm(records)]
                _ = db.objects.bulk_create(records)
            else:
                self.print_shell("No new data was found to store")

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.downloading_data_from_bucket()
        table = self.read_raw_data_format_date()
        table = self.filter_by_date(table, mode)
        table = self.transform_positiv_rel(table)
        self.save_table(table, DB_positividad_relativa, mode)
        self.print_shell("Work Done!")

    def read_raw_data_format_date(self,):
        cols_extr = [
            "DEPARTAMENTO",
            "METODODX",
            "FECHA_RESULTADO",
        ]
        # usecols=cols_extr)
        table = pd.read_csv('temp/'+self.file_name, sep=";", usecols=cols_extr)
        cols = table.columns.tolist()
        table.columns = [normalizer_str(col).lower() for col in cols]
        table.rename(columns={"fecha_resultado": "fecha",
                              "departamento": "region"}, inplace=True)
        # Format date
        table.fecha = table.fecha.apply(
            lambda x: datetime.strptime(str(int(x)), "%Y%m%d") if x == x else x)
        return table

    def filter_by_date(self, table, mode, min_date="2020-03-01"):
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=30)) # test only
            table = table.loc[(table.fecha >= min_date)]
        elif mode == 'last':
            min_date = str(datetime.now().date() - timedelta(days=30))
            table = table.loc[(table.fecha >= min_date)]
        self.print_shell("Records after filter: {}".format(table.shape))
        return table

    def transform_positiv_rel(self, table):
        # pivot table
        table = self.getting_lima_region_and_metropol(table)
        table["count"] = 1
        table = pd.pivot_table(table,
                               values="count",
                               index=['region', 'fecha'],
                               columns=['metododx'], aggfunc=np.sum).fillna(0)
        #table = table.reset_index()
        #table = table.groupby(by=['region', 'fecha']).sum().fillna(0)
        table.columns = [col.lower() for col in table.columns.tolist()]
        table["total"] = table.sum(1)
        table.reset_index(inplace=True)
        table.region = table.region.apply(
            lambda x: normalizer_str(x))
        table.sort_values(by='fecha', inplace=True)
        table.reset_index(inplace=True, drop=True)
        print(table.info())
        print(table["total"].sum(0))
        self.print_shell("Records :{}".format(table.shape))
        return table

    def getting_lima_region_and_metropol(self, table):
        def transform_region(x):
            if x['region'] == 'LIMA':
                return 'LIMA METROPOLITANA'
            else:
                return x['region']
        table['region'] = table.apply(transform_region, axis=1)
        return table
