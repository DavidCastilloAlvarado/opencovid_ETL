from django.core.management.base import BaseCommand, CommandError
from etldjango.settings import GCP_PROJECT_ID, BUCKET_NAME, BUCKET_ROOT
from .utils.storage import GetBucketData
from .utils.extractor import Data_Extractor
from .utils.urllibmod import urlretrieve
from .utils.rt_factory import Generator_RT
from .utils.unicodenorm import normalizer_str
from datetime import datetime, timedelta
from etldata.models import DB_rt, DB_positividad_salida, DB_positividad
from django.db.models import F, Sum, Avg, Count, StdDev, Max, Q
#from django_pandas.io import read_frame
# from django.utils import timezone
from tqdm import tqdm
import pandas as pd
import numpy as np
import time


class Command(BaseCommand):
    help = "Command to extract daily positivity from the acumulated db, also create roller mean data"
    bucket = GetBucketData(project_id=GCP_PROJECT_ID)

    def add_arguments(self, parser):
        """
        - for reboot the dataset for the last 12months
        $python manage.py worker_positividad full
        - for update the dataset using the las 6 months
        $python manage.py worker_positividad last --m 6
        """
        parser.add_argument(
            'mode', type=str, help="full/last , full: the whole external dataset. last: only the latest records")
        parser.add_argument(
            '--m', type=int, help="months, months for analysis, only available in last")

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
        months = options["m"]
        assert mode in ['full', 'last'], "Error in --mode argument"
        self.print_shell("Positivity calculation started ... ")
        months = self.get_months_from_args(mode, months)
        table = self.load_data_from_db(months)
        table = self.getting_daily_data_from_acum_table(table)
        self.save_table(table, DB_positividad_salida, mode)
        self.print_shell("Work Done!")

    def get_months_from_args(self, mode, months):
        if months:
            return months + 1
        elif mode == 'full':
            return 6
        elif mode == 'last':
            return 1

    def load_data_from_db(self, months_before=12):
        min_date = str(datetime.now().date() -
                       timedelta(days=int(30*months_before)))
        query = DB_positividad.objects
        query = query.values('fecha', 'region',
                             'pcr_total', 'pr_total', 'ag_total',
                             'pcr_pos', 'pr_pos', 'ag_pos')
        query = query.filter(fecha__gt=min_date)
        query = query.annotate(total_test=F('pcr_total') + F('pr_total') + F('ag_total'),
                               total_pos=F('pcr_pos') + F('pr_pos') + F('ag_pos'))
        query = pd.DataFrame.from_records(query)
        print(query.head())
        print(query.info())
        return query

    def drop_bad_records(self, table):
        cols = table.columns.tolist()
        for _ in range(5):
            temp = table.diff()
            table = table.join(temp, rsuffix='_diff')
            table = table.applymap(
                lambda x: np.nan if x < 0 else x)
            table.dropna(inplace=True)
            table = table[cols]
        return table[cols]

    def date_table_factory(self, fechas_orig, region_name):
        min_ = fechas_orig.min()
        max_ = fechas_orig.max()
        totaldatelist = pd.date_range(start=min_, end=max_).tolist()
        totaldatelist = pd.DataFrame(data={"fecha": totaldatelist})
        totaldatelist['region'] = region_name
        return totaldatelist

    def getting_daily_data_from_acum_table(self, table, n_roll=7):
        table = table.groupby(["region", ])
        table_total = pd.DataFrame()
        for region in table:
            region_name = region[0]
            # print(region_name)
            temp = region[1].sort_values(by="fecha")
            temp = temp.set_index(['fecha', 'region'])
            temp = self.drop_bad_records(temp)
            temp = temp.reset_index()
            totaldatelist = self.date_table_factory(temp.fecha, region_name)
            temp = totaldatelist.merge(temp,
                                       on=["fecha", 'region'],
                                       how="left")
            # print(temp.tail())
            #temp = temp.groupby(["fecha", "region"]).last()

            temp = temp.set_index(['fecha', 'region'])
            #temp = temp.sort_values(by="fecha")
            temp = temp.apply(pd.to_numeric, errors='ignore')
            temp = temp.interpolate(method='linear',
                                    limit_direction='forward',
                                    axis=0)
            temp = temp.diff()
            #temp = temp.applymap(lambda x: np.nan if x < 0 else x)
            #temp = temp.fillna(method="ffill")
            #temp = temp.dropna()
            temp_roll = temp.rolling(n_roll).mean()
            temp = temp.join(temp_roll, rsuffix='_roll')
            temp = temp.reset_index()
            temp = temp.dropna()
            # print(temp.tail(30))
            # temp = temp.reset_index()
            # temp.fecha = temp.fecha.apply(lambda x: x.date())
            table_total = table_total.append(temp, ignore_index=True)
        # print(table_acum.info())
        print(table_total.head())
        print(table_total.isnull().sum())
        print(table_total.info())
        return table_total
