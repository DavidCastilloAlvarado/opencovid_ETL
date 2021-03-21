from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from .utils.rt_factory import Generator_RT
from .utils.unicodenorm import normalizer_str
from datetime import datetime, timedelta
from etldata.models import DB_movilidad
from django_pandas.io import read_frame
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import time
import os


class Command(BaseCommand):
    help = "Command downloads pdf report from Minsa, for positive cases"
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_table(self, table, db,):
        records = table.to_dict(orient='records')
        records = [db(**record) for record in tqdm(records)]
        _ = db.objects.all().delete()
        _ = db.objects.bulk_create(records)

    def handle(self, *args, **options):
        self.print_shell("Transforming Movility table ...")
        self.transform_and_load_mov()
        self.print_shell("Work Done!")

    def transform_and_load_mov(self):
        os.system(
            "unzip -p temp/mov_report.zip 2020_PE_Region_Mobility_Report.csv >temp/PE.csv")
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
            "retail_and_recreation_percent_change_from_baseline": "comercial_recreación",
            "grocery_and_pharmacy_percent_change_from_baseline": "supermercados_farmacias",
            "parks_percent_change_from_baseline": "parques",
            "transit_stations_percent_change_from_baseline": "estaciones_de_tránsito",
            "workplaces_percent_change_from_baseline": "lugares_de_trabajo",
            "residential_percent_change_from_baseline": "residencia",
        }, inplace=True)
        table["region"] = table["region"].apply(
            lambda x: normalizer_str(str(x)).upper())
        table["region"] = table["region"].apply(lambda x:
                                                "MUNICIPALIDAD METROPOLITANA DE LIMA" if x == "METROPOLITAN MUNICIPALITY OF LIMA" else x)
        table['fecha'] = table['fecha'].apply(
            lambda x: datetime.strptime(str(x), "%Y-%m-%d"))
        table = table.loc[table.fecha > "2020-01-01"]
        table = table.groupby(["fecha", "region"]).mean().sort_values(
            by="fecha").reset_index().dropna(subset=["region"]).fillna(method="backfill")
        print(table.head())
        self.save_table(table, DB_movilidad)
        self.print_shell("Done!")
