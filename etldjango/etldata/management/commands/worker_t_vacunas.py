from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from datetime import datetime, timedelta
from etldata.models import DB_vacunas, Logs_extractor
from .utils.unicodenorm import normalizer_str
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np


class Command(BaseCommand):
    help = "Command for store Vaccines records"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    file_name = "vacunas.csv"

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
                last_date = '2020-05-01'
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
        table = self.format_columns(table)
        table = self.transform_vacunas(table)
        self.save_table(table, DB_vacunas, mode)
        self.print_shell("Work Done!")

    def read_raw_data_format_date(self,):
        cols_extr = [
            "FECHA_VACUNACION",
            "DEPARTAMENTO",
            "DOSIS",
            "FABRICANTE",
            "PROVINCIA",
            "GRUPO_RIESGO"
        ]
        # usecols=cols_extr)
        table = pd.read_csv('temp/'+self.file_name, usecols=cols_extr)

        table.rename(columns={"FECHA_VACUNACION": "fecha"}, inplace=True)
        # Format date
        table.fecha = table.fecha.apply(
            lambda x: datetime.strptime(str(int(x)), "%Y%m%d") if x == x else x)
        return table

    def filter_by_date(self, table, mode, min_date="2020-03-01"):
        max_date_table = table.fecha.max()
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=30)) # test only
            table = table.loc[(table.fecha >= min_date) &
                              (table.fecha < max_date_table)]
        elif mode == 'last':
            min_date = str(datetime.now().date() - timedelta(days=30))
            table = table.loc[(table.fecha >= min_date) &
                              (table.fecha < max_date_table)]
        self.print_shell("Records after filter: {}".format(table.shape))
        return table

    def format_columns(self, table):
        table.rename(columns={
            "DEPARTAMENTO": 'region',
            "DOSIS": 'dosis',
            "FABRICANTE": 'fabricante',
            "PROVINCIA": 'provincia',
            "GRUPO_RIESGO": 'grupo_riesgo',
        }, inplace=True)
        return table

    def transform_vacunas(self, table):
        # normalize words
        table.region = table.region.apply(
            lambda x: normalizer_str(x))
        table.grupo_riesgo = table.grupo_riesgo.apply(
            lambda x: normalizer_str(x))
        table["cantidad"] = 1
        cols = ['fecha', 'region', 'fabricante',
                'provincia', 'dosis', 'grupo_riesgo']
        table = table.groupby(by=cols).sum()
        table.sort_values(by='fecha', inplace=True)
        table.reset_index(inplace=True)
        table = self.getting_lima_region_and_metropol(table)
        print(table.tail(20))
        print(table.info())
        print(table.grupo_riesgo.unique())
        return table

    def getting_lima_region_and_metropol(self, table):
        def transform_region(x):
            if x['region'] == 'LIMA':
                if x['provincia'] == 'LIMA':
                    return 'LIMA METROPOLITANA'
                else:
                    return 'LIMA REGION'
            else:
                return x['region']
        table['region'] = table.apply(transform_region, axis=1)
        return table.drop(columns=['provincia'])
