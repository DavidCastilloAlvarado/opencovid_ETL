from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler, GetBucketData
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from .utils.unicodenorm import normalizer_str
from etldata.models import DB_uci, Logs_extractor
from django.contrib.gis.geos import Point
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "UCI+OXI-GEO: Command for transform the tables and upload to the data base"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    file_name_uci = "UCI_VENT.csv"
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
                last_date = '2020-12-01'
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
                                    destination_name='temp/'+file_name, ipress=True)

    def handle(self, *args, **options):
        mode = options["mode"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.print_shell("UCI table + OXI table data processing ... ")
        # Downloading data from bucket
        self.downloading_data_from_bucket(file_name=self.file_name_uci)
        self.downloading_data_from_bucket(file_name=self.file_name_oxi)
        # Transform UCI
        table_uci, dropcols = self.read_raw_uci_table(
            filename=self.file_name_uci)
        table_uci = self.filter_uci_by_date(table_uci)
        table_uci = self.format_uci_drop_duplicates(table_uci, dropcols)
        table_uci = self.transform_uci(table_uci)
        # Transform OXI
        table_oxi = self.read_raw_oxi_table(filename=self.file_name_oxi)
        table_oxi = self.filter_oxi_by_date(table_oxi)
        table_oxi = self.transform_oxi(table_oxi)
        # Merge data
        total_table = self.transform_merge(table_uci, table_oxi)
        # Saving data in table
        self.save_table(total_table, DB_uci, mode)
        self.print_shell("Work Done!")

    def read_raw_oxi_table(self, filename):
        columns_ext = [
            "FECHACORTE",
            "CODIGO",
        ]

        columns_val_oxi = ["CIL_VOL_DISP_M3_DU24",
                           "PLAN_OPER_PROD_DIA_M3",
                           "TAN_OPER_CI_VOL_TOT_M3",
                           "TAN_OPER_COI_VOL_TOT_M3",
                           "CON_PROM_PROD_DIA_ANT", ]

        o2_table = pd.read_csv('temp/'+filename,
                               sep="|",
                               usecols=columns_ext+columns_val_oxi)
        cols = o2_table.columns.tolist()
        o2_table.columns = [normalizer_str(label).replace(
            " ", "_").lower() for label in cols]
        o2_table.rename(columns={"fechacorte": "fecha_corte"}, inplace=True)
        return o2_table

    def filter_oxi_by_date(self, table):
        # Cambio formato de fecha
        table.fecha_corte = table.fecha_corte.apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d"))
        # Solo la fecha maxima
        fecha_max = table.fecha_corte.max()
        table = table.loc[table.fecha_corte == fecha_max]
        return table

    def transform_oxi(self, table,):
        table.drop_duplicates(inplace=True)
        table = table.set_index(["fecha_corte", "codigo"])
        table["serv_o2_cant"] = table.sum(axis=1)
        table = table[["serv_o2_cant"]]
        table["serv_oxi"] = table.apply(
            lambda x: True if x["serv_o2_cant"] > 0 else False, axis=1)
        table.reset_index(inplace=True)
        print(table.info())
        self.print_shell("Records: {}".format(table.shape))
        return table

    def read_raw_uci_table(self, filename):
        columns_ext = ["FECHACORTE",
                       "CODIGO",
                       ]
        columns_measure = ["ZC_UCI_AACT_CAM_TOTAL",
                           "ZC_UCI_AACT_CAM_TOT_DISP",
                           "ZC_UCI_ADUL_CAM_TOTAL",
                           "ZC_UCI_ADUL_CAM_TOT_DISP",
                           "ZC_UCI_NEONATAL_CAM_TOTAL",
                           "ZC_UCI_NEONATAL_CAM_TOT_DISP",
                           "ZC_UCI_PEDIA_CAM_TOTAL",
                           "ZC_UCI_PEDIA_CAM_TOT_DISP",
                           #################
                           "ZC_HOSP_ADUL_CAM_TOTAL",
                           "ZC_HOSP_ADUL_CAM_TOT_DISP",
                           "ZC_EMER_ADUL_CAM_TOTAL",
                           "ZC_EMER_ADUL_CAM_TOT_DISP",
                           #
                           "ZC_UCIN_CIA_CAM_TOTAL",
                           "ZC_UCIN_CIA_CAM_TOT_DISP",
                           "ZC_UCIN_CIP_CAM_TOTAL",
                           "ZC_UCIN_CIP_CAM_TOT_DISP",
                           #####
                           ]
        # usecols=columns_ext)
        uci_table = pd.read_csv('temp/'+filename, sep="|",
                                usecols=columns_ext+columns_measure)
        uci_table.columns = [normalizer_str(label).replace(
            " ", "_") for label in uci_table.columns.tolist()]
        uci_table.rename(columns={"FECHACORTE": "fecha_corte",
                                  'CODIGO': 'codigo', }, inplace=True)
        return uci_table, columns_measure

    def filter_uci_by_date(self, table):
        table.fecha_corte = table.fecha_corte.apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d"))
        # Seleccionando fecha máxima - 2
        min_date = str(table.fecha_corte.max() - timedelta(days=1))
        table = table.loc[table.fecha_corte >= min_date]
        return table

    def format_uci_drop_duplicates(self, table, dropcols):
        table.drop_duplicates(inplace=True)
        table[dropcols] = table[dropcols].fillna(0)
        table['serv_uci_left'] = table['ZC_UCI_AACT_CAM_TOT_DISP'] + \
            table['ZC_UCI_ADUL_CAM_TOT_DISP']
        # table['ZC_UCI_NEONATAL_CAM_TOT_DISP'] + \
        # table['ZC_UCI_PEDIA_CAM_TOT_DISP']

        table['serv_uci_total'] = table['ZC_UCI_AACT_CAM_TOTAL'] + \
            table['ZC_UCI_ADUL_CAM_TOTAL']
        # table['ZC_UCI_NEONATAL_CAM_TOTAL'] + \
        # table['ZC_UCI_PEDIA_CAM_TOTAL']

        table['serv_nc_left'] = table['ZC_EMER_ADUL_CAM_TOT_DISP'] +\
            table['ZC_HOSP_ADUL_CAM_TOT_DISP'] +\
            table['ZC_UCIN_CIA_CAM_TOT_DISP'] +\
            table['ZC_UCIN_CIP_CAM_TOT_DISP']

        table['serv_nc_total'] = table['ZC_HOSP_ADUL_CAM_TOTAL'] +\
            table['ZC_EMER_ADUL_CAM_TOTAL'] +\
            table['ZC_UCIN_CIA_CAM_TOTAL'] +\
            table['ZC_UCIN_CIP_CAM_TOTAL']

        table.drop(columns=dropcols, inplace=True)
        return table

    def transform_uci(self, table):
        # Sort values by fecha_corte
        table.sort_values(by="fecha_corte", inplace=True)
        # Group by codigo and take the last register (last date)
        table = table.groupby(["codigo"]).last()
        table.reset_index(inplace=True)
        # Same date for every record
        table["fecha_corte"] = table.fecha_corte.max()
        # Sum all the uci services
        table["serv_uci"] = table.apply(
            lambda x: True if x["serv_uci_total"] + x["serv_nc_total"] > 0 else False, axis=1)
        print(table.info())
        self.print_shell("Records: {}".format(table.shape))
        return table

    def transform_merge(self, uci, oxi):
        # Loading geo data
        ipress = pd.read_csv("temp/geo_ipress.csv")
        ipress.columns = [normalizer_str(col).lower()
                          for col in ipress.columns.tolist()]
        # Merge UCI + OXI
        oxi["fecha_corte"] = uci.fecha_corte.max()
        total_table = uci.merge(oxi.set_index("codigo"),
                                on=["codigo", "fecha_corte"],
                                how="outer")
        total_table["serv_oxi"] = total_table["serv_oxi"].fillna(False)
        total_table["serv_o2_cant"] = total_table["serv_o2_cant"].fillna(0)
        total_table["serv_uci"] = total_table["serv_uci"].fillna(False)
        # Merge UCI + OXI + GEODATA
        total_table = total_table.merge(ipress.set_index("codigo"),
                                        on=["codigo"],
                                        how="left")
        # print(total_table.columns)
        # print(total_table.head())
        # Fill NAN values
        total_table.distrito = total_table.distrito.fillna("")
        total_table = total_table.fillna(0)
        print(total_table.isnull().sum())
        total_table["location"] = total_table.apply(
            lambda x: Point(x['longitude'], x['latitude']), axis=1)
        total_table.drop(columns=['longitude', 'latitude'], inplace=True)
        print(total_table.info())
        print(total_table.loc[total_table.direccion != total_table.direccion])
        print(total_table['serv_uci_total'].sum())
        print(total_table['serv_uci_left'].sum())
        print(total_table['serv_nc_total'].sum())
        print(total_table['serv_nc_left'].sum())
        self.print_shell("Records oxi + uci: {}".format(total_table.shape))
        return total_table
