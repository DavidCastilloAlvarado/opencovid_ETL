from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_capacidad_hosp, Logs_extractor
from django.contrib.gis.geos import Point
# from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "Hospital capacity: Command for transform the tables and upload to the data base"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    file_name_uci = "UCI_VENT.csv"

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
        self.downloading_data_from_bucket(file_name=self.file_name_uci)
        # Transform UCI
        table, colsvals = self.read_raw_uci_table(filename=self.file_name_uci)
        table = self.filter_uci_by_date(table, mode)
        table = self.format_columns_drop_duplicates(table, colsvals)
        table = self.transform_uci(table)
        self.save_table(table, DB_capacidad_hosp, mode)
        self.print_shell("Work Done!")

    def read_raw_uci_table(self, filename):
        columns_ext = ["FECHACORTE",
                       "CODIGO",
                       "REGION",
                       "PROVINCIA",
                       ]
        columns_measure = ["ZC_UCI_AACT_CAM_TOTAL",
                           "ZC_UCI_AACT_CAM_TOT_DISP",
                           "ZC_UCI_AACT_CAM_TOT_OCUP",
                           #
                           "ZC_UCI_ADUL_CAM_TOTAL",
                           "ZC_UCI_ADUL_CAM_TOT_DISP",
                           "ZC_UCI_ADUL_CAM_TOT_OCUP",
                           #
                           "ZC_UCI_NEONATAL_CAM_TOTAL",
                           "ZC_UCI_NEONATAL_CAM_TOT_DISP",
                           "ZC_UCI_NEONATAL_CAM_TOT_OCUP",
                           #
                           "ZC_UCI_PEDIA_CAM_TOTAL",
                           "ZC_UCI_PEDIA_CAM_TOT_DISP",
                           "ZC_UCI_PEDIA_CAM_TOT_OCUP",
                           # Camas covid ## No UCI
                           "ZC_HOSP_ADUL_CAM_TOTAL",
                           "ZC_HOSP_ADUL_CAM_TOT_DISP",
                           "ZC_HOSP_ADUL_CAM_TOT_OCUP",
                           #
                           "ZC_EMER_ADUL_CAM_TOTAL",
                           "ZC_EMER_ADUL_CAM_TOT_DISP",
                           "ZC_EMER_ADUL_CAM_TOT_OCUP",
                           #
                           "ZC_UCIN_CIA_CAM_TOTAL",
                           "ZC_UCIN_CIA_CAM_TOT_DISP",
                           "ZC_UCIN_CIA_CAM_TOT_OCUP",
                           #
                           "ZC_UCIN_CIP_CAM_TOTAL",
                           "ZC_UCIN_CIP_CAM_TOT_DISP",
                           "ZC_UCIN_CIP_CAM_TOT_OCUP",
                           #####
                           ]
        # usecols=columns_ext)
        uci_table = pd.read_csv('temp/'+filename, sep="|",
                                usecols=columns_ext+columns_measure)
        uci_table.columns = [normalizer_str(label).replace(
            " ", "_") for label in uci_table.columns.tolist()]
        uci_table.rename(columns={"FECHACORTE": "fecha_corte",
                                  "CODIGO": 'codigo',
                                  "REGION": 'region',
                                  "PROVINCIA": 'provincia'}, inplace=True)
        return uci_table, columns_measure

    def filter_uci_by_date(self, table, mode, min_date="2020-04-01"):
        table.fecha_corte = table.fecha_corte.apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d"))
        # Seleccionando fecha m??xima
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=10))
            table = table.loc[(table.fecha_corte >= min_date)]
        elif mode == 'last':
            min_date = str(datetime.now().date() - timedelta(days=30))
            # max_date = str(datetime.now().date())
            table = table.loc[(table.fecha_corte >= min_date)]
        return table

    def format_columns_drop_duplicates(self, table, colsvals):
        table.drop_duplicates(inplace=True)
        table[colsvals] = table[colsvals].fillna(0)
        table['uci_zc_cama_disp'] = table['ZC_UCI_AACT_CAM_TOT_DISP'] +\
            table['ZC_UCI_ADUL_CAM_TOT_DISP'] +\
            table['ZC_UCI_NEONATAL_CAM_TOT_DISP'] +\
            table['ZC_UCI_PEDIA_CAM_TOT_DISP']

        table['uci_zc_cama_ocup'] = table['ZC_UCI_AACT_CAM_TOT_OCUP'] +\
            table['ZC_UCI_ADUL_CAM_TOT_OCUP'] +\
            table['ZC_UCI_NEONATAL_CAM_TOT_OCUP'] +\
            table['ZC_UCI_PEDIA_CAM_TOT_OCUP']

        table['uci_zc_cama_total'] = table['ZC_UCI_AACT_CAM_TOTAL'] +\
            table['ZC_UCI_ADUL_CAM_TOTAL'] +\
            table['ZC_UCI_NEONATAL_CAM_TOTAL'] +\
            table['ZC_UCI_PEDIA_CAM_TOTAL']

        table['uci_znc_cama_ocup'] = table['ZC_HOSP_ADUL_CAM_TOT_OCUP'] +\
            table['ZC_EMER_ADUL_CAM_TOT_OCUP'] +\
            table['ZC_UCIN_CIA_CAM_TOT_OCUP'] +\
            table['ZC_UCIN_CIP_CAM_TOT_OCUP']

        table['uci_znc_cama_disp'] = table['ZC_HOSP_ADUL_CAM_TOT_DISP'] +\
            table['ZC_EMER_ADUL_CAM_TOT_DISP'] +\
            table['ZC_UCIN_CIA_CAM_TOT_DISP'] +\
            table['ZC_UCIN_CIP_CAM_TOT_DISP']

        table['uci_znc_cama_total'] = table['ZC_HOSP_ADUL_CAM_TOTAL'] +\
            table['ZC_EMER_ADUL_CAM_TOTAL'] +\
            table['ZC_UCIN_CIA_CAM_TOTAL'] +\
            table['ZC_UCIN_CIP_CAM_TOTAL']

        table.drop(columns=colsvals, inplace=True)
        return table

    def transform_uci(self, table,):
        table = self.getting_lima_region_and_metropol(table)
        table = table.groupby(["codigo", "fecha_corte"]).last()
        table.reset_index(inplace=True)
        table.drop(columns=["codigo"], inplace=True)
        table = table.groupby(["region", "fecha_corte"]).sum()
        table.reset_index(inplace=True)
        temp = table.groupby(["fecha_corte"]).sum()
        temp = temp.reset_index()
        temp['region'] = "PERU"
        table = table.append(temp)
        table = table.astype(float, errors='ignore')
        print(table.tail())
        print(table.info())
        print(table.describe())
        self.print_shell("Records: {}".format(table.shape))
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
