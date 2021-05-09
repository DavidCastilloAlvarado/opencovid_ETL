from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
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
        self.print_shell("Oxy table data processing")
        # Downloading data from bucket
        self.downloading_data_from_bucket(file_name=self.file_name_oxi)
        # Transform oxi
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
            "REGION",
            "PROVINCIA",
        ]
        self.disponible = ['CIL_VOL_DISP_M3_DU24',
                           'PLAN_OPER_PROD_DIA_M3',
                           'TAN_OPER_CANT_REC_DIA_M3',
                           ]
        self.consumo = ['CIL_NU_CIL_CONSUM_CU24',
                        'PLAN_OPER_CONS_DIA_M3',
                        'TAN_OPER_CONS_DIA_M3',
                        'CON_PROM_PROD_DIA_ANT',
                        ]
        # usecols=columns_ext)
        table = pd.read_csv('temp/'+filename,
                            sep="|",
                            usecols=columns_ext+self.disponible+self.consumo)

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
        table.dropna(subset=['REGION'], inplace=True)
        table.rename(columns={
            'CODIGO': 'codigo',
            'REGION': 'region',
            'PROVINCIA': 'provincia',
            # CONSUMO
            "CIL_VOL_DISP_M3_DU24": 'vol_tk_disp',
            # "": 'prod_dia_otro',
            # "": 'prod_dia_generador',
            # "": 'prod_dia_iso',
            "TAN_OPER_CANT_REC_DIA_M3": 'prod_dia_crio',
            "PLAN_OPER_PROD_DIA_M3": 'prod_dia_planta',
            # CONSUMO
            "CIL_NU_CIL_CONSUM_CU24": 'consumo_vol_tk',
            "TAN_OPER_CONS_DIA_M3": 'consumo_dia_crio',
            "PLAN_OPER_CONS_DIA_M3": 'consumo_dia_pla',
            # "": 'consumo_dia_iso',
            "CON_PROM_PROD_DIA_ANT": 'consumo_dia_gen',
            # "": 'consumo_dia_otro',
        }, inplace=True)
        self.disponible = ['vol_tk_disp',
                           'prod_dia_crio', 'prod_dia_planta', ]
        self.consumo = ['consumo_vol_tk', 'consumo_dia_crio',
                        'consumo_dia_pla',
                        'consumo_dia_gen', ]
        return table

    def transform_oxi(self, table,):
        table = self.getting_lima_region_and_metropol(table)
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

    def getting_lima_region_and_metropol(self, table):
        table.region = table.region.apply(
            lambda x: normalizer_str(x).upper())
        table.provincia = table.provincia.apply(
            lambda x: normalizer_str(x).upper())

        def transform_region(x):
            if 'LIMA' in x['region']:
                if x['region'] == 'LIMA REGION':
                    return x['region']
                elif 'LIMA METROP' in x['region']:
                    if x['provincia'] == 'LIMA':
                        return 'LIMA METROPOLITANA'
                    elif x['provincia'] == 'CALLAO':
                        return 'CALLAO'
            else:
                return x['region']

        table['region'] = table.apply(transform_region, axis=1)
        return table.drop(columns=['provincia'])
