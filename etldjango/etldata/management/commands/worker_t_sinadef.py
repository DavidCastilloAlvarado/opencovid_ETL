import os
from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from datetime import datetime, timedelta
from etldata.models import DB_sinadef
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
# datetime.now(tz=timezone.utc)  # you can use this value


class Command(BaseCommand):
    help = "SINADEF: Command for transform the tables and upload to the data base"
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def add_arguments(self, parser):
        parser.add_argument(
            'init', type=str, help="yes/no; yes to load the whole db, no for the latest record")

    def handle(self, *args, **options):
        init = options["init"]
        assert init in ['yes', 'no'], "Error in --init argument"
        self.print_shell(
            "Transforming data SINADEF to consume in DB sinadef... ")
        self.transform_sinadef("temp/sinadef.csv", init)
        self.print_shell("Work done! ")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_table(self, table, db):
        records = table.to_dict(orient='records')
        records = [db(**record) for record in tqdm(records)]
        _ = db.objects.bulk_create(records)

    def transform_sinadef(self, filename, init='no'):
        #ipress = pd.read_csv("temp/geo_ipress.csv")
        sinadef = pd.read_csv(
            filename, sep=";", encoding='latin-1', header=2).iloc[:, 0:31]
        sinadef.FECHA = sinadef.FECHA.apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d"))
        # CALCULO DE FALLECIDOS
        list_ = ["NO SE CONOCE", 'SIN REGISTRO']
        col_extr = [
            "PAIS DOMICILIO",
            "DEPARTAMENTO DOMICILIO",
            "FECHA",
        ]

        # Filtros
        sinadef2018 = sinadef.loc[(sinadef.FECHA >= '2018-1-1') & (
            sinadef.FECHA < '2019-1-1') & (sinadef["MUERTE VIOLENTA"].isin(list_))]
        sinadef2019 = sinadef.loc[(sinadef.FECHA >= '2019-1-1') & (
            sinadef.FECHA < '2020-1-1') & (sinadef["MUERTE VIOLENTA"].isin(list_))]
        sinadef2 = sinadef.loc[(sinadef.FECHA >= "2020-01-01")
                               & (sinadef["MUERTE VIOLENTA"].isin(list_))]

        def preprocess(df, col_extr):
            df = df[col_extr]
            # df.head()
            # agrupando segun fecha y domicilio
            df = df.groupby(
                ["DEPARTAMENTO DOMICILIO", "FECHA"]).count().reset_index()
            # pivot table

            df = pd.pivot_table(df, values='PAIS DOMICILIO', index=['FECHA'],
                                columns=['DEPARTAMENTO DOMICILIO'], aggfunc=np.sum).fillna(0)[:-1]
            df["PERU"] = df.sum(1)
            df["PERU_roll"] = df[["PERU"]].rolling(7, center=True).mean()
            df.dropna(inplace=True)
            # df = df[:-3] # Con esto se ha eliminado los ultimos 3 días que estaban como nulos debido a la media movil centrada
            df.reset_index(inplace=True)
            return df
        if init == 'yes':
            # DB_sinadef.objects.all().delete()
            sinadef2 = preprocess(sinadef2, col_extr)
            columns = [label.replace(" ", "_")
                       for label in sinadef2.columns.tolist()]
            sinadef2018 = preprocess(sinadef2018, col_extr)
            sinadef2019 = preprocess(sinadef2019, col_extr)

            sinadef2.columns = columns
            sinadef2018.columns = columns
            sinadef2019.columns = columns
            # test if all fields are correct
            _ = DB_sinadef(**sinadef2.sample(1).to_dict(orient='records')[0])
            _ = DB_sinadef.objects.all().delete()
            self.save_table(sinadef2018, DB_sinadef)
            self.save_table(sinadef2019, DB_sinadef)
            self.save_table(sinadef2, DB_sinadef)
            #self.print_shell("N records: {}".format(columns))

        elif init == 'no':
            sinadef2 = preprocess(sinadef2, col_extr).iloc[-2:-1]
            columns = [label.replace(" ", "_")
                       for label in sinadef2.columns.tolist()]
            sinadef2.columns = columns
            assert not DB_sinadef.objects.filter(
                FECHA=sinadef2["FECHA"].tolist()[0]).exists(), "Day record already exist"
            self.save_table(sinadef2, DB_sinadef)
            #self.print_shell("N records: {}".format(columns))
