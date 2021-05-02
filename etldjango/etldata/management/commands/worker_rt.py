from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GOOGLE_APPLICATION_CREDENTIALS, GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from .utils.rt_factory import Generator_RT
from .utils.unicodenorm import normalizer_str
from datetime import datetime, timedelta
from etldata.models import DB_rt, DB_positividad
from django.db.models import F, Sum, Avg, Count, StdDev, Max, Q
#from django_pandas.io import read_frame
# from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import time
import logging
logger = logging.getLogger('StackDriverHandler')


class Command(BaseCommand):
    help = "Command to calculate RT score over the positive cases acum"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)
    temp_file = 'positive_acum.csv'

    def add_arguments(self, parser):
        """
        - for reboot the dataset for the last 12months
        $python manage.py worker_rt full
        - for reboot the dataset for the last 6 months
        $python manage.py worker_rt full --m 6
        - for update the dataset using the last month
        $python manage.py worker_rt last
        - for update the dataset using the las 6 months
        $python manage.py worker_rt last --m 6
        """
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")
        parser.add_argument(
            '--m', type=int, help="months, months for analysis, only available in last")

    def print_shell(self, text):
        self.stdout.write(self.style.SUCCESS(text))

    def error_rt(self, table):
        vector = table.loc[(table.region == 'PERU') & (table.ml <= 0.05)]
        if len(vector) > 10:
            return True
        return False

    def save_table(self, table, db, mode):
        if mode == 'full' and self.error_rt(table):
            self.print_shell("Storing new records - FULL")
            records = table.to_dict(orient='records')
            records = [db(**record) for record in tqdm(records)]
            _ = db.objects.all().delete()
            _ = db.objects.bulk_create(records)
        elif mode == 'last' and self.error_rt(table):
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
        else:
            logger.warning('RT wasnt saved - bad calculation')
            self.print_shell(
                "No new data was found to store - Error in rt Perú values")

    def handle(self, *args, **options):
        mode = options["mode"]
        months = options["m"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.print_shell("RT calculation started ... ")
        table = self.load_data_from_db()
        table = self.fix_daily_records(table)
        table = self.filter_data_by_date(table, mode, months)
        self.save_table_to_csv(table)
        table = self.rt_compute()
        table = self.format_region_field(table)
        self.save_table(table, DB_rt, mode)
        self.print_shell("Work Done!")

    def load_data_from_db(self, months_before=6):
        min_date = str(datetime.now().date() -
                       timedelta(days=int(30*months_before)))
        max_date = str(datetime.now().date() -
                       timedelta(days=1))
        query = DB_positividad.objects
        query = query.filter(fecha__gt=min_date, fecha__lt=max_date)
        query = query.annotate(total_posit=F(
            'pcr_pos') + F('pr_pos') + F('ag_pos'))
        query = pd.DataFrame.from_records(query
                                          .values('fecha', 'region', 'total_posit'))
        # query.fecha = query.fecha.apply(lambda x: x.date())
        #print(query.loc[query.region == 'PIURA'])
        return query

    def date_table_factory(self, fechas_orig, region_name):
        min_ = fechas_orig.min()
        max_ = fechas_orig.max()
        totaldatelist = pd.date_range(start=min_, end=max_).tolist()
        totaldatelist = pd.DataFrame(data={"fecha": totaldatelist})
        totaldatelist['region'] = region_name
        return totaldatelist

    def fix_daily_records(self, table):
        # table.sort_values(by='fecha', inplace=True)
        table = table.groupby(["region", ])
        table_acum = pd.DataFrame()
        for region in table:
            region_name = region[0]
            temp = region[1].sort_values(by="fecha")
            temp = self.drop_bad_records(temp)
            totaldatelist = self.date_table_factory(temp.fecha, region_name)
            temp = totaldatelist.merge(temp,
                                       on=["fecha", 'region'],
                                       how="left")
            temp = temp.sort_values(by="fecha")
            temp["total_posit"] = temp["total_posit"].interpolate(method='linear',
                                                                  limit_direction='forward',
                                                                  axis=0)
            temp = temp.reset_index(drop=True)
            temp.dropna(inplace=True)
            #temp = temp.fillna(method="ffill")

            #temp["cum_pos_total"] = temp["total"].cumsum()
            #temp.fecha = temp.fecha.apply(lambda x: x.date())
            table_acum = table_acum.append(temp, ignore_index=True)
        table_acum.rename(columns={'total_posit': 'cum_pos_total'},
                          inplace=True)
        print(table_acum.info())
        print(table_acum.tail())
        print(table_acum.loc[table_acum.region == 'LIMA REGION'])
        return table_acum

    def save_table_to_csv(self, table):
        table.to_csv('temp/' + self.temp_file, index=False)

    def filter_data_by_date(self, table, mode, months):
        months = 6 if not months else months
        min_date = str(datetime.now().date() -
                       timedelta(days=int(months*30)))
        if mode == 'full':
            # max_date = str(datetime.now().date() - timedelta(days=10))
            table = table.loc[(table.fecha >= min_date)]
        elif mode == 'last':
            # max_date = str(datetime.now().date())
            table = table.loc[(table.fecha >= min_date)]
        return table

    def rt_compute(self):
        rt_table = Generator_RT(path="temp/", name_file=self.temp_file)
        cols = rt_table.final_results.columns.tolist()
        rt_table.final_results.columns = [col.lower() for col in cols]
        print(rt_table.final_results.info())
        return rt_table.final_results

    def format_region_field(self, table):
        table.region = table.region.apply(
            lambda x: 'LIMA METROPOLITANA' if x == 'LIMA' else x.upper())
        return table

    def drop_bad_records(self, table):
        for _ in range(4):
            table["diff"] = table["total_posit"].diff()
            table["diff"] = table["diff"].apply(
                lambda x: np.nan if x < 0 else x)
            table.dropna(subset=["diff"], inplace=True)
        table.drop(columns=["diff"], inplace=True)
        return table
