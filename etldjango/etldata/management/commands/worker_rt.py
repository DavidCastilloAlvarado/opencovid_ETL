from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from .utils.rt_factory import Generator_RT
from .utils.unicodenorm import normalizer_str
from datetime import datetime, timedelta
from etldata.models import DB_rt, DB_positividad_relativa
#from django_pandas.io import read_frame
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
            #_ = db.objects.all().delete()
            # this is posible because the table is sorter by "-fecha"
            last_record = db.objects.all()[:1]
            last_record = list(last_record)
            if len(last_record) > 0:
                last_date = str(last_record[0].date.date())
            else:
                last_date = '2020-01-01'
            table = table.loc[table.date > last_date]
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
        table = self.filter_data_by_date(table, mode)
        self.acumulate_records(table)
        table = self.rt_compute()
        self.save_table(table, DB_rt, mode)
        self.print_shell("Work done!")
        # if init == "test":
        #     self.load_from_bucket_test()
        # elif init == "db":
        #     self.load_from_DB()
        # self.print_shell("Work Done!")

    def load_data_from_db(self, months_before=12):
        min_date = str(datetime.now().date() -
                       timedelta(days=int(30*months_before)))
        query = DB_positividad_relativa.objects.filter(fecha__gt=min_date)
        query = pd.DataFrame.from_records(query
                                          .values('fecha', 'region', 'total'))
        # query.fecha = query.fecha.apply(lambda x: x.date())
        # print(query.info())
        return query

    def date_table_factory(self, fechas_orig):
        min_ = fechas_orig.min()
        max_ = fechas_orig.max()
        totaldatelist = pd.date_range(start=min_, end=max_).tolist()
        totaldatelist = pd.DataFrame(data={"fecha": totaldatelist})
        return totaldatelist

    def acumulate_records(self, table):
        # table.sort_values(by='fecha', inplace=True)
        table = table.groupby(["region", ])
        table_acum = pd.DataFrame()
        for region in table:
            temp = region[1].sort_values(by="fecha")
            totaldatelist = self.date_table_factory(temp.fecha)
            temp = totaldatelist.merge(temp.set_index("fecha"),
                                       on=["fecha"],
                                       how="outer")
            # if region[0] == 'PUNO':
            #     print(temp.loc[(temp.region != temp.region) |
            #                    ((temp.fecha > '2020-09-28') & (temp.fecha < '2020-10-03')) |
            #                    (temp.total == 0)])
            # else:
            #     continue
            temp = temp.sort_values(by="fecha")
            temp = temp.reset_index(drop=True)
            temp = temp.fillna(method="ffill")
            # temp = temp.fillna(value={
            #     "region": region[0],
            #     "total": 1e-6,
            # })
            # if region[0] == 'SAN MARTIN':
            #     print(temp.head(20))
            #     print(temp.loc[(temp.total != temp.total) |
            #                    (temp.total == 0)])

            # temp.reset_index(inplace=True)
            #temp = temp.sort_values(by="fecha")
            temp["cum_pos_total"] = temp["total"].cumsum()
            #temp.fecha = temp.fecha.apply(lambda x: x.date())
            table_acum = table_acum.append(temp, ignore_index=True)
        # print(table_acum.info())
        print(table_acum.head())
        table_acum.to_csv('temp/' + self.temp_file, index=False)

    def filter_data_by_date(self, table, mode):
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=10))
            table = table
        elif mode == 'last':
            min_date = str(datetime.now().date() - timedelta(days=30))
            # max_date = str(datetime.now().date())
            table = table.loc[(table.fecha >= min_date)]
        return table

    def rt_compute(self):
        rt_table = Generator_RT(path="temp/", name_file=self.temp_file)
        cols = rt_table.final_results.columns.tolist()
        rt_table.final_results.columns = [col.lower() for col in cols]
        print(rt_table.final_results.info())
        return rt_table.final_results
