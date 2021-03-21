from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import Bucket_handler
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from .utils.rt_factory import Generator_RT
from .utils.unicodenorm import normalizer_str
from datetime import datetime, timedelta
from etldata.models import DB_rt, DB_positividad
from django_pandas.io import read_frame
#from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import time


class Command(BaseCommand):
    help = "Command downloads pdf report from Minsa, for positive cases"
    bucket = Bucket_handler(project_id=GCP_PROJECT_ID)

    def add_arguments(self, parser):
        parser.add_argument(
            'init', type=str, help="test/db; test to load from a recorder csv or db to load from our db records")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def save_table(self, table, db,):
        records = table.to_dict(orient='records')
        records = [db(**record) for record in tqdm(records)]
        _ = db.objects.all().delete()
        _ = db.objects.bulk_create(records)

    def handle(self, *args, **options):
        init = options["init"]
        assert init in [
            'test', 'db'], "Error in --init argument must be test or db"
        self.print_shell("RT calculation started ... ")
        if init == "test":
            self.load_from_bucket_test()
        elif init == "db":
            self.load_from_DB()
        self.print_shell("Work Done!")

    def load_from_bucket_test(self):
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/feed_rt.csv",
                                  destination_file_name="temp/feed_rt.csv")
        rt_score = Generator_RT(path="temp/", name_file="feed_rt.csv", sep=";")
        rt_score = rt_score.final_results
        rt_score.region = rt_score.region.apply(
            lambda x: normalizer_str(x).upper())
        print(rt_score.head(20))
        self.save_table(rt_score, DB_rt)

    def load_from_DB(self):
        all_pos = DB_positividad.objects.values_list(
            'fecha', 'region', 'Total_pos').all()  # [:20]
        positividad = read_frame(all_pos)
        columns = [
            "fecha",
            "region",
            # "PCR_pos",
            # "PR_pos",
            # "AG_pos",
            # "Total",
            "Total_pos"
        ]
        positividad = positividad[columns]
        positividad.rename(columns={"fecha": "Fecha",
                                    "region": "REGION",
                                    # "PCR_pos": "cum_pos_pcr",
                                    # "PR_pos": "cum_pos_zero",
                                    # "AG_pos": "cum_pos_ag",
                                    # "Total": "cum_total_muestras",
                                    "Total_pos": "cum_pos_total"
                                    }, inplace=True)
        positividad.Fecha = positividad.Fecha.apply(
            lambda x: x.date().strftime("%Y-%m-%d"))
        positividad.to_csv("temp/rt_feed.csv", index=False)
        #positividad = pd.read_csv("temp/rt_feed.csv")
        # print(positividad.Fecha[0])
        # print(positividad.dtypes)
        rt_score = Generator_RT(path="temp/", name_file="rt_feed.csv")
        rt_score = rt_score.final_results
        rt_score.region = rt_score.region.apply(
            lambda x: normalizer_str(x).upper())
        print(rt_score.head(20))
        # print(rt_score.dtypes)
        self.save_table(rt_score, DB_rt)
