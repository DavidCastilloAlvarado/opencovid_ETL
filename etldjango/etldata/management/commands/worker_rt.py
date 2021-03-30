from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from .utils.rt_factory import Generator_RT
from .utils.unicodenorm import normalizer_str
from datetime import datetime, timedelta
from etldata.models import DB_rt, DB_positividad_relativa
from django_pandas.io import read_frame
# from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import time


class Command(BaseCommand):
    help = "Command downloads pdf report from Minsa, for positive cases"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    temp_file = 'positive_acum.csv'

    def add_arguments(self, parser):
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")

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
                last_date = '2020-01-01'
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
        self.print_shell("RT calculation started ... ")
        table = self.load_data_from_db()
        table = self.acumulate_records(table)
        table = self.rt_compute()
        self.save_table(table, DB_rt, mode)
        self.print_shell("Work done!")
        # if init == "test":
        #     self.load_from_bucket_test()
        # elif init == "db":
        #     self.load_from_DB()
        # self.print_shell("Work Done!")
        # KeyError: "[('TUMBES', Timestamp('2021-02-21 00:00:00'))\n ('TUMBES', Timestamp('2021-02-20 00:00:00'))] not found in axi

    def load_data_from_db(self, months_before=6):
        min_date = str(datetime.now().date() -
                       timedelta(days=int(30*months_before)))
        query = DB_positividad_relativa.objects.filter(fecha__gt=min_date)
        query = pd.DataFrame.from_records(query
                                          .values('fecha', 'region', 'total'))
        # query.fecha = query.fecha.apply(lambda x: x.date())
        # print(query.info())
        return query

    def acumulate_records(self, table):
        # table.sort_values(by='fecha', inplace=True)
        table = table.groupby(["region", ])
        table_acum = pd.DataFrame()
        for region in table:
            temp = region[1].sort_values(by="fecha").fillna(method="backfill")
            min_ = temp.fecha.min()
            max_ = temp.fecha.max()
            totaldatelist = pd.date_range(start=min_, end=max_).tolist()
            totaldatelist = pd.DataFrame(data={"fecha": totaldatelist})
            temp = totaldatelist.merge(temp.set_index("fecha"),
                                       on=["fecha"],
                                       how="outer")
            temp = temp.fillna(value={
                "region": region[0],
                "total": 1e-6,
            })
            # temp.reset_index(inplace=True)
            temp = temp.sort_values(by="fecha")
            temp["cum_pos_total"] = temp["total"].cumsum()
            temp.fecha = temp.fecha.apply(lambda x: x.date())
            table_acum = table_acum.append(temp, ignore_index=True)
        # print(table_acum.info())
        print(table_acum.head())
        table_acum.to_csv('temp/' + self.temp_file, index=False)

    def load_from_bucket_test(self):
        self.bucket.download_blob(bucket_name=BUCKET_NAME,
                                  source_blob_name="data_source/feed_rt.csv",
                                  destination_file_name="temp/feed_rt.csv")
        rt_score = Generator_RT(
            path="temp/", name_file="feed_rt.csv", sep=";")
        rt_score = rt_score.final_results
        rt_score.region = rt_score.region.apply(
            lambda x: normalizer_str(x).upper())
        print(rt_score.head(20))
        self.save_table(rt_score, DB_rt)

    def load_from_DB(self):
        all_pos = DB_positividad.objects.values_list(
            'fecha', 'region', 'Total_pos').all()  # [:5000]
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
        positividad.Fecha = positividad.Fecha.apply(
            lambda x: datetime.strptime(str(x), "%Y-%m-%d"))

        """
        # Filling data with backfill method
        positividad = pd.pivot_table(
            positividad, values="cum_pos_total", index="Fecha", columns='REGION').reset_index()
        totaldatelist = pd.date_range(
            start=positividad.Fecha.min(), end=positividad.Fecha.max()).tolist()
        totaldatelist = pd.DataFrame(data={"Fecha": totaldatelist})

        # print(totaldatelist.dtypes)
        # print(positividad.dtypes)

        positividad = totaldatelist.merge(
            positividad.set_index("Fecha"), on=["Fecha"], how="outer")
        positividad = positividad.sort_values(by="Fecha", ascending=False).reset_index(
            drop=True).fillna(method="backfill").fillna(method="ffill")
        positividad = pd.melt(positividad, id_vars=[
                              'Fecha'], var_name='REGION', value_name='cum_pos_total')
        """

        positividad.to_csv("temp/rt_feed.csv", index=False)
        # positividad = pd.read_csv("temp/rt_feed.csv")
        # print(positividad.Fecha[0])
        # print(positividad.dtypes)
        # print(positividad.Fecha.unique())
        print(positividad.head())
        rt_score = Generator_RT(path="temp/", name_file="rt_feed.csv")
        rt_score = rt_score.final_results
        rt_score.region = rt_score.region.apply(
            lambda x: normalizer_str(x).upper())
        print(rt_score.head(20))
        # print(rt_score.dtypes)
        self.save_table(rt_score, DB_rt)

    def rt_compute(self):
        rt_table = Generator_RT(path="temp/", name_file=self.temp_file)
        cols = rt_table.final_results.columns.tolist()
        rt_table.final_results.columns = [col.lower() for col in cols]
        print(rt_table.final_results.info())
        return rt_table.final_results
