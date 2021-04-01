from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_capacidad_oxi, Logs_extractor
from django.contrib.gis.geos import Point
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "OXI: Command for transform the tables and upload to the data base"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    file_name_oxi = "O2.csv"

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last; full: load the whole dataset, last: only the latest dates")

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

    def downloading_data_from_bucket(self, file_name=None):
        last_record = Logs_extractor.objects.filter(status='ok',
                                                    mode='upload',
                                                    e_name=file_name)[:1]
        last_record = list(last_record)
        assert len(last_record) > 0, "There are not any file {} in the bucket".format(
            file_name)
        last_record = last_record[0]
        source_url = last_record.url
        print(source_url)
        self.bucket.get_from_bucket(source_name=source_url,
                                    destination_name='temp/'+file_name)

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.print_shell("Transforming data UCI to load in DB UCI... ")
        # Downloading data from bucket
        # self.downloading_data_from_bucket(file_name=self.file_name_oxi)
        # Transform UCI
        table = self.read_raw_oxi_table(filename=self.file_name_oxi)
        table = self.filter_oxi_by_date(table, mode)
        table = self.format_columns_drop_duplicates(table)
        table = self.transform_oxi(table)
        self.save_table(table, DB_capacidad_oxi, mode)
        self.print_shell("Work Done!")

    def read_raw_oxi_table(self, filename):
        columns_ext = [
            "FECHACORTE",
            "CODIGO",
            "REGION"
        ]

        columns_val_oxi = ["VOL_DISPONIBLE",
                           "PRODUCCION_DIA_OTR",
                           "PRODUCCION_DIA_GEN",
                           "PRODUCCION_DIA_ISO",
                           "PRODUCCION_DIA_CRIO",
                           "PRODUCCION_DIAPLA",
                           "VOL_CONSUMO_DIA",
                           "CONSUMO_DIA_CRIO",
                           "CONSUMO_DIA_PLA",
                           "CONSUMO_DIA_ISO",
                           "CONSUMO_DIA_GEN",
                           "CONSUMO_DIA_OTR"]
        # usecols=columns_ext)
        table = pd.read_csv('temp/'+filename,
                            sep="|",
                            usecols=columns_ext+columns_val_oxi)

        table.rename(columns={"FECHACORTE": "fecha_corte"}, inplace=True)
        return table

    def filter_oxi_by_date(self, table, mode, min_date="2020-04-01"):
        table.fecha_corte = table.fecha_corte.apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d"))
        # Seleccionando fecha máxima
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=10))
            table = table.loc[(table.fecha_corte >= min_date)]
        elif mode == 'last':
            min_date = str(datetime.now().date() - timedelta(days=30))
            # max_date = str(datetime.now().date())
            table = table.loc[(table.fecha_corte >= min_date)]
        return table

    def format_columns_drop_duplicates(self, table):
        table.drop_duplicates(inplace=True)
        table.rename(columns={
            'CODIGO': 'codigo',
            'REGION': 'region',
            "VOL_DISPONIBLE": 'vol_tk_disp',
            "PRODUCCION_DIA_OTR": 'prod_dia_otro',
            "PRODUCCION_DIA_GEN": 'prod_dia_generador',
            "PRODUCCION_DIA_ISO": 'prod_dia_iso',
            "PRODUCCION_DIA_CRIO": 'prod_dia_crio',
            "PRODUCCION_DIAPLA": 'prod_dia_planta',
            "VOL_CONSUMO_DIA": 'consumo_vol_tk',
            "CONSUMO_DIA_CRIO": 'consumo_dia_crio',
            "CONSUMO_DIA_PLA": 'consumo_dia_pla',
            "CONSUMO_DIA_ISO": 'consumo_dia_iso',
            "CONSUMO_DIA_GEN": 'consumo_dia_gen',
            "CONSUMO_DIA_OTR": 'consumo_dia_otro',
        }, inplace=True)
        self.disponible = ['vol_tk_disp', 'prod_dia_otro',
                           'prod_dia_generador', 'prod_dia_iso',
                           'prod_dia_crio', 'prod_dia_planta', ]
        self.consumo = ['consumo_vol_tk', 'consumo_dia_crio',
                        'consumo_dia_pla', 'consumo_dia_iso',
                        'consumo_dia_gen', 'consumo_dia_otro', ]
        return table

    def transform_oxi(self, table,):
        table = table.groupby(["codigo", "fecha_corte"]).last()
        table.reset_index(inplace=True)
        table.drop(columns=["codigo"], inplace=True)
        table = table.groupby(["region", "fecha_corte"]).sum()
        table.reset_index(inplace=True)
        table["m3_disp"] = table[self.disponible].sum(1)
        table["m3_consumo"] = table[self.consumo].sum(1)
        # Sum for the whole country
        temp = table.groupby(["fecha_corte"]).sum()
        temp = temp.reset_index()
        temp['region'] = "PERU"
        table = table.append(temp)
        print(table.tail())
        print(table.describe())
        print(table.info())
        self.print_shell("Records: {}".format(table.shape))
        return table
