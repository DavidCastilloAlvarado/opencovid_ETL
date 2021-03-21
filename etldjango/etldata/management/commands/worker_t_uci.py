from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from etldata.models import DB_uci
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import os
import time
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "UCI+OXI: Command for transform the tables and upload to the data base"
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def add_arguments(self, parser):
        parser.add_argument(
            'init', type=str, help="yes/no; yes to load the whole db, no for the lastest record")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_table(self, table, db, init):
        records = table.to_dict(orient='records')
        records = [db(**record) for record in tqdm(records)]
        if init == "yes":
            _ = db.objects.all().delete()
        _ = db.objects.bulk_create(records)

    def handle(self, *args, **options):
        init = options["init"]
        assert init in ['yes', 'no'], "Error in --init argument"
        self.print_shell("Transforming data UCI to load in DB UCI... ")
        uci = self.transform_uci("temp/UCI_VENT.csv")
        oxi = self.transform_oxi("temp/O2.csv")
        self.transform_merge(uci, oxi, init)
        self.print_shell("Work Done!")

    def transform_uci(self, filename,):
        # Abriendo archivo
        uci_table = pd.read_csv(filename, sep="|")
        uci_table.columns = [label.replace(
            " ", "_").upper() for label in uci_table.columns.tolist()]
        uci_table.rename(columns={"FECHACORTE": "FECHA_CORTE"}, inplace=True)
        # Seleccionando ultima fecha
        uci_table.FECHA_CORTE = uci_table.FECHA_CORTE.apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d"))
        # Seleccionando fecha máxima
        uci_table_td = uci_table.loc[uci_table.FECHA_CORTE ==
                                     uci_table.FECHA_CORTE.max()]
        # Selección de columnas
        # Se verifica que existen códigos des hospitales únicos
        columns_ext = ["FECHA_CORTE",
                       "CÓDIGO",
                       "VENTILADORES_UCI_ZC_TOTAL",
                       "VENTILADORES_UCI_ZC_DISPONIBLE",
                       "CAMAS_ZC_TOTAL_OPERATIVO",
                       "CAMAS_ZC_DISPONIBLE"]
        columns_new = ["fecha_corte",
                       "CODIGO",
                       "serv_uci_total",
                       "serv_uci_left",
                       "serv_nc_total",
                       "serv_nc_left"]
        uci_vent = uci_table_td.loc[:, columns_ext]
        uci_vent.columns = columns_new
        uci_vent["serv_uci"] = uci_vent.apply(
            lambda x: True if x["serv_uci_total"] + x["serv_nc_total"] > 0 else False, axis=1)
        print(uci_vent.head())
        self.print_shell("Records: {}".format(uci_vent.shape))
        return uci_vent

    def transform_oxi(self, filename,):
        o2_table = pd.read_csv(filename, sep="|")
        o2_table.columns = [label.replace(
            " ", "_").upper() for label in o2_table.columns.tolist()]
        o2_table.rename(columns={"FECHACORTE": "FECHA_CORTE"}, inplace=True)
        # Cambio formato de fecha
        o2_table.FECHA_CORTE = o2_table.FECHA_CORTE.apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d"))
        # Solo la fecha maxima
        fecha_max = o2_table.FECHA_CORTE.max()
        o2_table_td = o2_table.loc[o2_table.FECHA_CORTE == fecha_max]
        # Se verifica que existen códigos des hospitales únicos
        columns_ext = [
            "CODIGO",
        ]

        columns_val = ["VOL_DISPONIBLE",
                       "PRODUCCION_DIA_OTR",
                       "PRODUCCION_DIA_GEN",
                       "PRODUCCION_DIA_ISO",
                       "PRODUCCION_DIA_CRIO",
                       "PRODUCCION_DIAPLA"]

        columns_new = [
            "CODIGO",
            "serv_o2_cant"]

        o2 = o2_table_td.loc[:, columns_ext]
        o2["serv_o2_cant"] = o2_table_td[columns_val].sum(axis=1)
        o2.columns = columns_new
        o2["serv_oxi"] = o2.apply(
            lambda x: True if x["serv_o2_cant"] > 0 else False, axis=1)
        print(o2.head())
        self.print_shell("Records: {}".format(o2.shape))
        return o2

    def transform_merge(self, uci, oxi, init="yes"):
        # Loading geo data
        ipress = pd.read_csv("temp/geo_ipress.csv")
        # Merge UCI + OXI
        oxi["fecha_corte"] = uci.fecha_corte.max()
        total_table = uci.merge(oxi.set_index(
            "CODIGO"), on=["CODIGO", "fecha_corte"], how="outer")
        total_table["serv_oxi"] = total_table["serv_oxi"].fillna(False)
        total_table["serv_o2_cant"] = total_table["serv_o2_cant"].fillna(0)
        total_table["serv_uci"] = total_table["serv_uci"].fillna(False)
        # Merge UCI + OXI + GEODATA
        total_table = total_table.merge(ipress.set_index(
            "CODIGO"), on=["CODIGO"], how="outer")
        print(total_table.columns)
        print(total_table.head())
        # Fill NAN values
        total_table.DISTRITO = total_table.DISTRITO.fillna("")
        total_table = total_table.fillna(0)
        print(total_table.isnull().sum())
        # Loading to dataBase
        self.save_table(total_table, DB_uci, init)
        self.print_shell("Records oxi + uci: {}".format(total_table.shape))
