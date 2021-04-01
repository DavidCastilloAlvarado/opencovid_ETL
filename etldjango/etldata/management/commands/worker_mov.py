from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from .utils.rt_factory import Generator_RT
from .utils.unicodenorm import normalizer_str
from datetime import datetime, timedelta
from etldata.models import DB_movilidad, Logs_extractor
from django_pandas.io import read_frame
# from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import time
import os


class Command(BaseCommand):
    help = "Command for create the report about population movility"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    file_name = 'mov_report.zip'

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last; full, load: the whole dataset, last: only the latest dates")

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.print_shell("Transforming Movility table ...")
        self.downloading_data_from_bucket()
        self.unzip_data()
        table = self.read_raw_table_norm_columns()
        table = self.filter_date(table, mode,)
        table = self.fix_data(table)
        table = self.transform_rollermean(table)
        self.save_table(table, DB_movilidad, mode)
        self.print_shell("Work Done!")

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
                last_date = str(last_record[0].fecha.date())
            else:
                last_date = '2020-12-01'
            table = table.loc[table.fecha > last_date]
            if len(table):
                self.print_shell("Storing new records")
                records = table.to_dict(orient='records')
                records = [db(**record) for record in tqdm(records)]
                _ = db.objects.bulk_create(records)
            else:
                self.print_shell("No new data was found to store")

    def unzip_data(self,):
        os.system(
            "unzip -p temp/" + self.file_name + " 2021_PE_Region_Mobility_Report.csv >temp/PE.csv")
        os.system("rm temp/" + self.file_name)

    def read_raw_table_norm_columns(self,):
        table = pd.read_csv("temp/PE.csv")
        columns = [
            "sub_region_1",
            "date",
            "retail_and_recreation_percent_change_from_baseline",
            "grocery_and_pharmacy_percent_change_from_baseline",
            "parks_percent_change_from_baseline",
            "transit_stations_percent_change_from_baseline",
            "workplaces_percent_change_from_baseline",
            "residential_percent_change_from_baseline",
        ]
        table = table[columns]
        table.rename(columns={
            "sub_region_1": "region",
            "date": "fecha",
            "retail_and_recreation_percent_change_from_baseline": "comercial_recreacion",
            "grocery_and_pharmacy_percent_change_from_baseline": "supermercados_farmacias",
            "parks_percent_change_from_baseline": "parques",
            "transit_stations_percent_change_from_baseline": "estaciones_de_transito",
            "workplaces_percent_change_from_baseline": "lugares_de_trabajo",
            "residential_percent_change_from_baseline": "residencia",
        }, inplace=True)
        return table

    def filter_date(self, table, mode, min_date="2021-01-01"):
        table = table.dropna(subset=["region", "fecha"])
        table['fecha'] = table['fecha'].apply(
            lambda x: datetime.strptime(str(x), "%Y-%m-%d"))
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=10))
            table = table.loc[(table.fecha >= min_date)]
        elif mode == 'last':
            min_date = str(datetime.now().date() - timedelta(days=30))
            # max_date = str(datetime.now().date())
            table = table.loc[(table.fecha >= min_date)]
        return table

    def fix_data(self, table):
        table["region"] = table["region"].apply(
            lambda x: normalizer_str(str(x)).upper())

        table["region"] = table["region"].apply(lambda x:
                                                "MUNICIPALIDAD METROPOLITANA DE LIMA" if x == "METROPOLITAN MUNICIPALITY OF LIMA" else x)
        return table

    def transform_rollermean(self, table, n_roll=7):
        table = table.groupby(["region", "fecha", ]).mean()
        table = table.groupby(["region", ])
        table_roll = pd.DataFrame()
        for region in table:
            table_roll = table_roll.append(region[1]
                                           .sort_values(by="fecha")
                                           .fillna(method="backfill")
                                           .rolling(n_roll, center=True).mean()
                                           .dropna())
        table_roll = table_roll.reset_index()
        print(table_roll.head())
        return table_roll
